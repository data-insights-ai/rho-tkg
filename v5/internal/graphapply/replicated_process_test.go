package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

const processFrameBytes = 2 << 20
const processQueueBytes = 8 << 20
const processPacketCount = 128
const processDeliveries = 4096
const processStderrBytes = 64 << 10

const (
	processCampaign byte = iota + 1
	processPropose
	processStep
	processRead
	processLookup
	processProject
	processClose
	processTick
	processStateOp
	processPublish
	processOffers
	processBuild
	processSeal
	processManifest
	processNext
	processBeginImport
	processAppend
	processVerify
	processPrepare
	processActivate
	processFeedback
	processImportClose
	processHold
	processHeldProject
	processHoldClose
)

// IPC IDs identify this process event only. No storage/send/prepared capability
// is serialized. The snapshot case opens bounded child-local sender/import/read owners.
const (
	statGeneration = iota
	statCheckpoint
	statTerm
	statVote
	statCommit
	statAuditIndex
	statAuditTerm
	statViews
	statViewBytes
	statExports
	statExportImageBytes
	statPrepared
	statPreparedBytes
	statClaims
	statClaimBytes
	statClaimImageBytes
	statImportImageBytes
	statVerifierImageBytes
	statActiveBytes
	statActiveRecords
	statStagedBytes
	statStagedRecords
	statPinnedBytes
	statImport
	statVerified
	statVerifier
	statHeldRows
	statHeldBytes
	statCount
)

type processRequest struct {
	incarnation   [16]byte
	sequence      uint64
	op            byte
	index, entity uint64
	data          []byte
	packet        replica.Packet
}
type processResponse struct {
	incarnation             [16]byte
	stats                   [statCount]uint64
	cutID, manifestID       [32]byte
	handles                 []uint64
	aux                     []byte
	ready                   bool
	causeMask               uint16
	readGeneration          uint64
	sequence, applied       uint64
	code                    byte
	detail                  string
	packets                 []replica.Packet
	reads                   []replica.ReadResult
	image, outcome, changes []byte
	fact                    processFact
}

func processEncoding(emit func(*boundedWriter)) ([]byte, error) {
	size := boundedWriter{max: processFrameBytes, sizing: true}
	emit(&size)
	if size.err != nil {
		return nil, size.err
	}
	w := boundedWriter{max: processFrameBytes, b: make([]byte, 0, size.n)}
	emit(&w)
	return w.b, w.err
}
func processPacket(w *boundedWriter, p replica.Packet) {
	w.u64(p.From)
	w.u64(p.To)
	if p.Snapshot {
		w.tag(1)
	} else {
		w.tag(0)
	}
	w.field(p.Payload)
}
func processDecodePacket(c *graphCursor) replica.Packet {
	p := replica.Packet{From: c.u64(), To: c.u64()}
	flag := c.tag()
	if flag > 1 {
		c.err = errCorrupt
	}
	p.Snapshot = flag == 1
	p.Payload = c.field(processFrameBytes)
	return p
}
func encodeProcessRequest(r processRequest) ([]byte, error) {
	return processEncoding(func(w *boundedWriter) {
		w.add(r.incarnation[:])
		w.u64(r.sequence)
		w.tag(r.op)
		w.u64(r.index)
		w.u64(r.entity)
		w.field(r.data)
		processPacket(w, r.packet)
	})
}
func decodeProcessRequest(b []byte) (processRequest, error) {
	c := graphCursor{b: b}
	r := processRequest{incarnation: c.array(), sequence: c.u64(), op: c.tag(), index: c.u64(), entity: c.u64()}
	r.data = c.field(processFrameBytes)
	r.packet = processDecodePacket(&c)
	if r.sequence == 0 || r.op < processCampaign || r.op > processHoldClose || len(c.b) != 0 {
		c.err = errCorrupt
	}
	return r, c.err
}
func encodeProcessResponse(r processResponse) ([]byte, error) {
	if len(r.handles) > 2 || len(r.packets) > processPacketCount || len(r.reads) > 16 || len(r.fact.Labels) > 16 || len(r.detail) > 4096 {
		return nil, errLimit
	}
	return processEncoding(func(w *boundedWriter) {
		w.add(r.incarnation[:])
		for _, v := range r.stats {
			w.u64(v)
		}
		w.add(r.cutID[:])
		w.add(r.manifestID[:])
		w.u64(r.readGeneration)
		w.u32(len(r.handles))
		for _, id := range r.handles {
			w.u64(id)
		}
		w.field(r.aux)
		if r.ready {
			w.tag(1)
		} else {
			w.tag(0)
		}
		w.u64(r.sequence)
		w.u64(r.applied)
		w.tag(r.code)
		w.u16(r.causeMask)
		w.text(r.detail)
		w.u32(len(r.packets))
		for _, p := range r.packets {
			processPacket(w, p)
		}
		w.u32(len(r.reads))
		for _, r := range r.reads {
			w.u64(r.Index)
			w.field(r.Context)
		}
		w.field(r.image)
		w.field(r.outcome)
		w.field(r.changes)
		var flags byte
		if r.fact.Exists {
			flags |= 1
		}
		if r.fact.Active {
			flags |= 2
		}
		w.tag(flags)
		w.u64(r.fact.Life)
		w.u32(len(r.fact.Labels))
		for _, label := range r.fact.Labels {
			w.text(label)
		}
		w.text(r.fact.Answer)
	})
}
func decodeProcessResponse(b []byte) (processResponse, error) {
	c := graphCursor{b: b}
	r := processResponse{incarnation: c.array()}
	for i := range r.stats {
		r.stats[i] = c.u64()
	}
	copy(r.cutID[:], c.take(32))
	copy(r.manifestID[:], c.take(32))
	r.readGeneration = c.u64()
	nh := c.count(2, 8)
	if c.err != nil {
		return processResponse{}, c.err
	}
	r.handles = make([]uint64, nh)
	for i := range r.handles {
		r.handles[i] = c.u64()
	}
	r.aux = c.field(1 << 20)
	flag := c.tag()
	if flag > 1 {
		c.err = errCorrupt
	}
	r.ready = flag == 1
	r.sequence = c.u64()
	r.applied = c.u64()
	r.code = c.tag()
	r.causeMask = c.u16()
	r.detail = string(c.field(4096))
	// Header arrays are bounded BEFORE allocation; all byte fields borrow the
	// bounded frame. Strings additionally own <=16*256+4096+256 bytes. There is
	// no JSON/base64 transport expansion or attacker-sized decoded collection.
	n := c.count(processPacketCount, 21)
	if c.err != nil {
		return processResponse{}, c.err
	}
	r.packets = make([]replica.Packet, n)
	for i := range r.packets {
		r.packets[i] = processDecodePacket(&c)
	}
	n = c.count(16, 12)
	if c.err != nil {
		return processResponse{}, c.err
	}
	r.reads = make([]replica.ReadResult, n)
	for i := range r.reads {
		r.reads[i] = replica.ReadResult{Index: c.u64(), Context: c.field(1024)}
	}
	r.image = c.field(64 << 10)
	r.outcome = c.field(64 << 10)
	r.changes = c.field(1 << 20)
	flags := c.tag()
	if flags > 3 {
		c.err = errCorrupt
	}
	r.fact.Exists = flags&1 != 0
	r.fact.Active = flags&2 != 0
	r.fact.Life = c.u64()
	n = c.count(16, 4)
	if c.err != nil {
		return processResponse{}, c.err
	}
	r.fact.Labels = make([]string, n)
	for i := range r.fact.Labels {
		r.fact.Labels[i] = string(c.field(256))
	}
	r.fact.Answer = string(c.field(256))
	if r.sequence == 0 || r.code > 6 || len(c.b) != 0 {
		c.err = errCorrupt
	}
	return r, c.err
}

func writeProcessFrame(w io.Writer, b []byte) error {
	if len(b) > processFrameBytes {
		return errLimit
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(b))) // #nosec G115 -- bounded by 2 MiB.
	for _, part := range [][]byte{header[:], b} {
		for len(part) > 0 {
			n, err := w.Write(part)
			if err != nil {
				return err
			}
			if n <= 0 {
				return io.ErrShortWrite
			}
			part = part[n:]
		}
	}
	return nil
}
func readProcessFrame(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(header[:])
	if n < 9 || n > processFrameBytes {
		return nil, errLimit
	}
	b := make([]byte, int(n)) // #nosec G115 -- checked <= 2 MiB before allocation.
	_, err := io.ReadFull(r, b)
	return b, err
}

type processStderr struct {
	mu       sync.Mutex
	data     [processStderrBytes]byte
	n        int
	overflow bool
}

func (b *processStderr) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := min(len(p), processStderrBytes-b.n)
	copy(b.data[b.n:], p[:n])
	b.n += n
	b.overflow = b.overflow || n < len(p)
	return len(p), nil // Drain even after the cap, preventing a blocked child pipe.
}
func (b *processStderr) state() (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.data[:b.n]), b.overflow
}

type graphProcess struct {
	cmd                                       *exec.Cmd
	stdin                                     io.WriteCloser
	stdout                                    io.ReadCloser
	stderr                                    processStderr
	sequence                                  uint64
	closed                                    bool
	incarnation                               [16]byte
	maxPinned, maxOffers, maxImports, maxHeld uint64
	closeErr                                  error
	maxFrame                                  int
}

func startGraphProcess(t *testing.T, dir string, id uint64, extraEnv ...string) *graphProcess {
	t.Helper()
	p := &graphProcess{cmd: exec.Command(os.Args[0], "-test.run=^TestReplicatedGraphChild$", "-test.count=1")}
	p.cmd.Env = append(os.Environ(), "RHO_GRAPH_PROCESS_CHILD=1", "RHO_GRAPH_PROCESS_ID="+strconv.FormatUint(id, 10), "RHO_GRAPH_PROCESS_DIR="+dir, "GOMAXPROCS=1")
	p.cmd.Env = append(p.cmd.Env, extraEnv...)
	var err error
	p.stdin, err = p.cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	p.stdout, err = p.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	p.cmd.Stderr = &p.stderr
	if err = p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if p.closed {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := p.shutdown(ctx, t.Failed()); err != nil && !t.Failed() {
			t.Error("child cleanup", err)
		}
	})
	return p
}
func (p *graphProcess) call(t *testing.T, r processRequest) processResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	response, err := p.exchange(ctx, r)
	if err == nil && response.code != 0 {
		err = fmt.Errorf("child tag=%d: %s", response.code, response.detail)
	}
	if err != nil {
		t.Fatalf("child %d RPC: %v", p.cmd.Process.Pid, err)
	}
	return response
}

// No testing.T calls here: cleanup must never Goexit before closing/reaping.
func (p *graphProcess) exchange(ctx context.Context, r processRequest) (processResponse, error) {
	p.sequence++
	r.sequence = p.sequence
	r.incarnation = p.incarnation
	b, err := encodeProcessRequest(r)
	if err != nil {
		return processResponse{}, err
	}
	p.maxFrame = max(p.maxFrame, len(b)+4)
	type result struct {
		r          processResponse
		err        error
		frameBytes int
	}
	ch := make(chan result, 1)
	go func() {
		if err := writeProcessFrame(p.stdin, b); err != nil {
			ch <- result{err: err}
			return
		}
		wire, err := readProcessFrame(p.stdout)
		if err != nil {
			ch <- result{err: err}
			return
		}
		r, err := decodeProcessResponse(wire)
		ch <- result{r: r, err: err, frameBytes: len(wire) + 4}
	}()
	select {
	case got := <-ch:
		p.maxFrame = max(p.maxFrame, got.frameBytes)
		stderr, overflow := p.stderr.state()
		if got.err != nil {
			return processResponse{}, fmt.Errorf("protocol/EOF: %w stderr=%q", got.err, stderr)
		}
		if p.incarnation == ([16]byte{}) {
			p.incarnation = got.r.incarnation
		} else if p.incarnation != got.r.incarnation {
			return processResponse{}, errCorrupt
		}
		p.maxPinned = max(p.maxPinned, got.r.stats[statPinnedBytes])
		p.maxOffers = max(p.maxOffers, got.r.stats[statExports])
		p.maxImports = max(p.maxImports, got.r.stats[statImport])
		p.maxHeld = max(p.maxHeld, got.r.stats[statViews])
		if got.r.sequence != r.sequence || overflow {
			return processResponse{}, fmt.Errorf("%w: sequence=%d/%d stderr=%q overflow=%v", errCorrupt, got.r.sequence, r.sequence, stderr, overflow)
		}

		return got.r, nil
	case <-ctx.Done():
		return processResponse{}, ctx.Err()
	}
}

// Always close both pipes and await Cmd.Wait, including failed Close RPCs.
// The deferred cleanup contains no Fatal/Goexit door. A kill is followed by a
// separate bounded reap wait, even when the RPC context already expired.
func (p *graphProcess) shutdown(ctx context.Context, force bool) (err error) {
	if p.closed {
		return p.closeErr
	}
	p.closed = true
	defer func() {
		closePipe := func(pipe io.Closer) {
			if e := pipe.Close(); e != nil && !errors.Is(e, os.ErrClosed) {
				err = errors.Join(err, e)
			}
		}
		closePipe(p.stdin)
		kill := func() {
			if e := p.cmd.Process.Kill(); e != nil && !errors.Is(e, os.ErrProcessDone) {
				err = errors.Join(err, e)
			}
		}
		if force || err != nil {
			kill()
		}
		ch := make(chan error, 1)
		go func() { ch <- p.cmd.Wait() }()
		select {
		case e := <-ch:
			err = errors.Join(err, e)
		case <-time.After(10 * time.Second):
			err = errors.Join(err, context.DeadlineExceeded)
			kill()
			select {
			case e := <-ch:
				err = errors.Join(err, e)
			case <-time.After(10 * time.Second):
				err = errors.Join(err, fmt.Errorf("child did not reap after kill: %w", context.DeadlineExceeded))
			}
		}
		closePipe(p.stdout)
		p.closeErr = err
	}()
	if !force {
		var response processResponse
		response, err = p.exchange(ctx, processRequest{op: processClose})
		if err == nil && response.code != 0 {
			err = processResponseError(response)
		}
	}
	return err
}

func processNamespace(id uint64) (namespace, [3]uint64, [16]byte) {
	if id <= 3 {
		return namespace{idalloc.GraphID{21}, 7}, [3]uint64{1, 2, 3}, [16]byte{41}
	}
	return namespace{idalloc.GraphID{22}, 9}, [3]uint64{4, 5, 6}, [16]byte{42}
}

type processRemoteError struct {
	mask   uint16
	code   byte
	detail string
}

func (e processRemoteError) Error() string { return fmt.Sprintf("remote tag=%d: %s", e.code, e.detail) }
func (e processRemoteError) Is(target error) bool {
	for i, sentinel := range []error{raftlog.ErrInvalid, replica.ErrInvalid, raftlog.ErrLimit, replica.ErrLimit, errInvalid, errLimit, graphstore.ErrResourceLimit} {
		if target == sentinel && e.mask&(1<<i) != 0 {
			return true
		}
	}
	return false
}
func processCauseMask(err error) uint16 {
	var mask uint16
	for i, sentinel := range []error{raftlog.ErrInvalid, replica.ErrInvalid, raftlog.ErrLimit, replica.ErrLimit, errInvalid, errLimit, graphstore.ErrResourceLimit} {
		if errors.Is(err, sentinel) {
			mask |= 1 << i
		}
	}
	return mask
}
func processResponseError(r processResponse) error {
	if r.code == 0 {
		return nil
	}
	return processRemoteError{r.causeMask, r.code, r.detail}
}

type processSendOwner struct {
	id    uint64
	send  *replica.SnapshotSend
	bound bool
}
type processOwners struct {
	incarnation              [16]byte
	next                     uint64
	sends                    [2]processSendOwner
	imported                 *raftlog.ApplicationImport
	prepared                 *raftlog.PreparedApplicationSnapshot
	importID                 uint64
	held                     *graphReadSession
	heldID                   uint64
	closed                   bool
	closeErr, importCloseErr error
}

func (o *processOwners) id() (uint64, error) {
	if o.next == ^uint64(0) {
		return 0, errLimit
	}
	o.next++
	return o.next, nil
}
func (o *processOwners) sender(send *replica.SnapshotSend, bound bool) (uint64, error) {
	for i := range o.sends {
		if o.sends[i].send == send {
			o.sends[i].bound = o.sends[i].bound || bound
			return o.sends[i].id, nil
		}
	}
	for i := range o.sends {
		if o.sends[i].send == nil {
			id, err := o.id()
			if err != nil {
				return 0, err
			}
			o.sends[i] = processSendOwner{id, send, bound}
			return id, nil
		}
	}
	return 0, errLimit
}
func (o *processOwners) send(id uint64) *replica.SnapshotSend {
	for _, slot := range o.sends {
		if slot.id == id && id != 0 {
			return slot.send
		}
	}
	return nil
}
func (o *processOwners) removeSend(id uint64) {
	for i := range o.sends {
		if o.sends[i].id == id {
			o.sends[i] = processSendOwner{}
		}
	}
}
func (o *processOwners) closeImport() error {
	if o.imported == nil {
		return o.importCloseErr
	}
	var err error
	if o.prepared != nil {
		err = o.prepared.Close()
	}
	err = errors.Join(err, o.imported.Abort())
	o.importCloseErr = err
	o.imported = nil
	o.prepared = nil
	o.importID = 0
	return err
}
func (o *processOwners) close(d *replica.Driver) error {
	if o.closed {
		return o.closeErr
	}
	o.closed = true
	if o.held != nil {
		o.closeErr = o.held.Close()
		o.held = nil
		o.heldID = 0
	}
	o.closeErr = errors.Join(o.closeErr, o.closeImport())
	for i, slot := range o.sends {
		if slot.send != nil {
			var err error
			if slot.bound {
				_, err = d.ReportSnapshotSend(slot.send, false)
			} else {
				err = slot.send.Close()
			}
			o.closeErr = errors.Join(o.closeErr, err)
			o.sends[i] = processSendOwner{}
		}
	}
	return o.closeErr
}
func encodeProcessChunk(chunk raftlog.ApplicationSnapshotChunk) ([]byte, error) {
	if len(chunk.Data) > 256<<10 || len(chunk.After) > 2*1024+11 {
		return nil, errLimit
	}
	return processEncoding(func(w *boundedWriter) {
		w.u32(int(chunk.Version))
		w.add(chunk.CutID[:])
		w.add(chunk.ManifestID[:])
		w.u64(chunk.Sequence)
		w.u64(chunk.Visited)
		w.u64(chunk.VisitedBytes)
		if chunk.Final {
			w.tag(1)
		} else {
			w.tag(0)
		}
		w.field(chunk.After)
		w.field(chunk.Data)
	})
}
func decodeProcessChunk(b []byte) (raftlog.ApplicationSnapshotChunk, error) {
	c := graphCursor{b: b}
	version := c.count(3, 0)
	chunk := raftlog.ApplicationSnapshotChunk{Version: uint32(version)} // #nosec G115 -- version checked <=3.
	copy(chunk.CutID[:], c.take(32))
	copy(chunk.ManifestID[:], c.take(32))
	chunk.Sequence = c.u64()
	chunk.Visited = c.u64()
	chunk.VisitedBytes = c.u64()
	flag := c.tag()
	if flag > 1 {
		c.err = errCorrupt
	}
	chunk.Final = flag == 1
	chunk.After = c.field(2*1024 + 11)
	chunk.Data = c.field(256 << 10)
	if len(c.b) != 0 {
		c.err = errCorrupt
	}
	return chunk, c.err
}
func processFactAt(view graphstate.ReadView, id graphstate.EntityID, l materializerLimits) (processFact, error) {
	entity, err := view.Entity(context.Background(), id)
	if err != nil || !entity.Found {
		return processFact{}, err
	}
	position, err := temporal.RationalPosition(entity.Record.Axis, temporal.RationalInt64(0))
	if err != nil {
		return processFact{}, err
	}
	p, err := graphstate.Project(context.Background(), view, id, position, graphstate.Effective, l.graph.Planner)
	if err != nil {
		return processFact{}, err
	}
	f := processFact{Exists: p.Exists, Active: p.Active, Life: uint64(p.Life), Labels: p.Labels}
	for _, v := range p.Properties {
		if v.Name != "answer" || v.Cardinality != graphstate.ScalarCardinality {
			return processFact{}, errCorrupt
		}
		var ok bool
		f.Answer, ok = v.Scalar.StringValue()
		if !ok {
			return processFact{}, errCorrupt
		}
	}
	return f, nil
}
func processStats(s *raftlog.Store) ([statCount]uint64, [32]byte, [32]byte, error) {
	var stats [statCount]uint64
	index, _, err := s.Checkpoint()
	if err != nil {
		return stats, [32]byte{}, [32]byte{}, err
	}
	hard, _, err := s.InitialState()
	if err != nil {
		return stats, [32]byte{}, [32]byte{}, err
	}
	audit, err := s.ApplicationActivationAudit()
	if err != nil {
		return stats, [32]byte{}, [32]byte{}, err
	}
	usage, err := s.ApplicationTransferUsage()
	if err != nil {
		return stats, [32]byte{}, [32]byte{}, err
	}
	app, err := s.ApplicationUsage()
	if err != nil {
		return stats, [32]byte{}, [32]byte{}, err
	}
	stats = [statCount]uint64{s.ApplicationGeneration(), index, hard.GetTerm(), hard.GetVote(), hard.GetCommit(), audit.Index, audit.Term, uint64(app.Views), uint64(app.ViewBytes), uint64(usage.Exports), uint64(usage.ExportImageBytes), uint64(usage.Prepared), usage.PreparedBytes, uint64(usage.Claims), usage.ClaimBytes, usage.ClaimImageBytes, uint64(usage.ImportImageBytes), uint64(usage.VerifierImageBytes), usage.ActiveBytes, usage.ActiveRecords, usage.StagedBytes, usage.StagedRecords, usage.PinnedLogicalBytes}
	if usage.Import {
		stats[statImport] = 1
	}
	if usage.Verified {
		stats[statVerified] = 1
	}
	if usage.Verifier {
		stats[statVerifier] = 1
	}
	return stats, audit.CutID, audit.ManifestID, nil
} // #nosec G115 -- public counts/image lengths are nonnegative under finite validated fixture policies.
func processSeed(n namespace) ([]byte, error) {
	dir, err := os.MkdirTemp("", "rho-graph-process-seed-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	s, err := raftlog.Open(raftlog.Config{Dir: dir, Create: true, Application: raftlog.DefaultApplicationPolicy(1)})
	if err != nil {
		return nil, err
	}
	if err = graphstore.BootstrapSinglePartition(s, graphstore.Namespace{Graph: graphstate.GraphID(n.graph), Partition: n.partition}, 1); err != nil {
		return nil, errors.Join(err, s.Close())
	}
	_, image, err := s.Checkpoint()
	return image, errors.Join(err, s.Close())
}
func processError(err error) (byte, string) {
	if err == nil {
		return 0, ""
	}
	code := byte(6)
	switch {
	case errors.Is(err, replica.ErrInvalid), errors.Is(err, raftlog.ErrInvalid), errors.Is(err, errInvalid):
		code = 1
	case errors.Is(err, replica.ErrLimit), errors.Is(err, raftlog.ErrLimit), errors.Is(err, errLimit), errors.Is(err, graphstore.ErrResourceLimit):
		code = 2
	case errors.Is(err, replica.ErrUnavailable):
		code = 3
	case errors.Is(err, raftlog.ErrCorrupt), errors.Is(err, errCorrupt):
		code = 4
	case errors.Is(err, raftlog.ErrClosed), errors.Is(err, replica.ErrStopped):
		code = 5
	}
	detail := err.Error()
	if len(detail) > 4096 {
		detail = detail[:4096]
	}
	return code, detail
}

// Six real OS children each own Store + actual materializer + public Driver.
// No direct Install/Persist/Apply graph mutation path exists in this fixture.
func TestReplicatedGraphChild(t *testing.T) {
	if os.Getenv("RHO_GRAPH_PROCESS_CHILD") != "1" {
		t.Skip("subprocess entry")
	}
	id, err := strconv.ParseUint(os.Getenv("RHO_GRAPH_PROCESS_ID"), 10, 64)
	if err != nil || id < 1 || id > 6 {
		t.Fatal("child identity", err)
	}
	n, voters, group := processNamespace(id)
	l := raftlog.DefaultLimits()
	l.CacheBytes = 2 << 20
	l.MemTableBytes = 1 << 20
	l.MaxRetainedBytes = 16 << 20
	l.MaxRetainedEntries = 8192
	p := raftlog.DefaultApplicationPolicy(id)
	p.MaxInstallWrites = 1024
	p.RetainedApplicationBytes = 16 << 20
	p.RetainedApplicationRecords = 8192
	cfg := raftlog.Config{Dir: os.Getenv("RHO_GRAPH_PROCESS_DIR"), Create: true, Limits: l, Application: p, Transfer: raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(n.graph), Partition: n.partition, Group: group}, Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.ApplicationTransferLimits{MaxExports: 2, MaxChunkRows: 64, MaxChunkBytes: 256 << 10, MaxStagedBytes: 16 << 20, MaxStagedRecords: 8192, MaxPinnedLogicalBytes: 64 << 20}}, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 32 << 20, MaxRecords: 16384}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 4096}, Replication: raftlog.ApplicationReplicationConfig{Voters: voters}, SemanticContractID: SemanticContractID()}
	if cfg.Application.RetainedApplicationRecords != 8192 || cfg.Generations.MaxRecords != 16384 || cfg.Limits.MemTableBytes != 1<<20 {
		t.Fatal("fixture policy drift")
	}
	cfg.Create = os.Getenv("RHO_GRAPH_PROCESS_REOPEN") != "1"
	s, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Create {
		seed, e := processSeed(n)
		if e == nil {
			e = s.Initialize(voters[:], seed)
		}
		if e != nil {
			_ = s.Close()
			t.Fatal(e)
		}
	}
	m, err := newMaterializer(s, n, 1, defaultMaterializerLimits())
	if err != nil {
		_ = s.Close()
		t.Fatal(err)
	}
	d, err := replica.Open(replica.Config{ID: id, Store: s, ApplicationMachine: m, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
	if err != nil {
		_ = s.Close()
		t.Fatal(err)
	}
	owners := processOwners{}
	if _, err := io.ReadFull(rand.Reader, owners.incarnation[:]); err != nil {
		_ = d.Close()
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			if err := errors.Join(owners.close(d), d.Close()); err != nil {
				t.Error(err)
			}
		}
	}()
	for {
		wire, err := readProcessFrame(os.Stdin)
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			t.Fatal("child framing", err)
		}
		r, err := decodeProcessRequest(wire)
		if err != nil {
			t.Fatal("child protocol", err)
		}
		if r.incarnation != owners.incarnation && (r.sequence != 1 || r.incarnation != ([16]byte{})) {
			t.Fatal("stale IPC incarnation")
		}
		response := processResponse{sequence: r.sequence, incarnation: owners.incarnation}
		var out replica.Output
		switch r.op {
		case processCampaign:
			out, err = d.Campaign()
		case processPropose:
			out, err = d.Propose(r.data)
		case processStep:
			out, err = d.Step(r.packet)
		case processRead:
			out, err = d.ReadIndex(r.data)
		case processLookup:
			var v *raftlog.ApplicationView
			v, err = s.ApplicationView(d.Applied())
			if err == nil {
				var ident request
				ident, _, err = commandIdentity(r.data)
				if err == nil && ident.ns != n {
					err = errInvalid
				}
				if err == nil {
					var row raftlog.KV
					var found bool
					row, found, err = v.Get(context.Background(), outcomeKey(ident), 1024)
					if err == nil && !found {
						err = errNotInitialized
					}
					if err == nil {
						response.outcome = row.Value
						var o outcome
						o, err = decodeAnyOutcome(row.Value, n)
						if err == nil {
							response.changes, err = s.ApplicationRecord(context.Background(), o.index, false, 1<<20)
						}
					}
				}
				err = errors.Join(err, v.Close())
			}
		case processProject:
			var v *raftlog.ApplicationView
			v, err = s.ApplicationView(r.index)
			if err == nil {
				var c *graphstore.Catalog
				c, _, err = m.catalog(v)
				if err == nil {
					var full *graphstore.ReadView
					full, err = graphstore.OpenReadView(context.Background(), c, m.limits.graph)
					if err == nil {
						var entity graphstate.EntityRead
						entity, err = full.Entity(context.Background(), graphstate.EntityID(r.entity))
						if err == nil && entity.Found {
							var position temporal.Position
							position, err = temporal.RationalPosition(entity.Record.Axis, temporal.RationalInt64(0))
							if err == nil {
								var projection graphstate.Projection
								projection, err = graphstate.Project(context.Background(), full, graphstate.EntityID(r.entity), position, graphstate.Effective, m.limits.graph.Planner)
								response.fact = processFact{Exists: projection.Exists, Active: projection.Active, Life: uint64(projection.Life), Labels: projection.Labels}
								for _, property := range projection.Properties {
									if property.Name != "answer" || property.Cardinality != graphstate.ScalarCardinality {
										err = errCorrupt
										break
									}
									var ok bool
									response.fact.Answer, ok = property.Scalar.StringValue()
									if !ok {
										err = errCorrupt
									}
								}
							}
						}
						err = errors.Join(err, full.Close())
					}
				}
				if err == nil {
					var base raftlog.ApplicationRoot
					base, err = v.Root()
					response.image = base.Image
					response.readGeneration = base.Generation
				}
				err = errors.Join(err, v.Close())
			}
		case processTick:
			out, err = d.Tick()
		case processStateOp:
			_, response.image, err = s.Checkpoint()
		case processPublish:
			err = s.PublishSnapshot()
		case processOffers:
			var sends []*replica.SnapshotSend
			sends, err = d.SnapshotSends()
			if err == nil {
				for _, send := range sends {
					var id uint64
					id, err = owners.sender(send, false)
					if err != nil {
						break
					}
					response.handles = append(response.handles, id)
				}
			}
		case processBuild, processSeal, processManifest, processNext, processFeedback:
			send := owners.send(r.index)
			if send == nil {
				err = errInvalid
				break
			}
			switch r.op {
			case processBuild:
				response.ready, err = send.Build(context.Background(), raftlog.ReadBudget{Rows: 64, Bytes: 256 << 10})
			case processSeal:
				err = d.SealSnapshotSend(send)
			case processManifest:
				var manifest raftlog.ApplicationSnapshotManifest
				manifest, err = send.Manifest()
				if err == nil {
					response.aux, err = raftlog.EncodeApplicationSnapshotManifest(manifest)
				}
			case processNext:
				var chunk raftlog.ApplicationSnapshotChunk
				chunk, err = send.Next(context.Background(), raftlog.ReadBudget{Rows: 64, Bytes: 256 << 10})
				if err == nil {
					response.aux, err = encodeProcessChunk(chunk)
					response.ready = chunk.Final
				}
			case processFeedback:
				out, err = d.ReportSnapshotSend(send, r.entity == 1)
				if err == nil {
					owners.removeSend(r.index)
				}
			}
		case processBeginImport:
			if owners.imported != nil {
				err = errLimit
				break
			}
			var manifest raftlog.ApplicationSnapshotManifest
			manifest, err = raftlog.DecodeApplicationSnapshotManifestForBinding(r.data, p.MaxImageBytes, s.ApplicationBinding())
			if err == nil {
				owners.imported, err = s.BeginApplicationImport(context.Background(), manifest)
			}
			if err == nil {
				owners.importID, err = owners.id()
				response.handles = []uint64{owners.importID}
				if err == nil {
					var status raftlog.ApplicationImportStatus
					status, err = owners.imported.Status()
					response.aux = status.ManifestID[:]
				}
			}
		case processAppend, processVerify, processPrepare, processActivate, processImportClose:
			if r.op == processActivate && r.index == 0 {
				out, err = d.StepApplicationSnapshot(r.packet, nil)
				break
			}
			if owners.imported == nil || owners.importID != r.index {
				err = errInvalid
				break
			}
			switch r.op {
			case processAppend:
				var chunk raftlog.ApplicationSnapshotChunk
				chunk, err = decodeProcessChunk(r.data)
				if err == nil {
					err = owners.imported.Append(context.Background(), chunk)
				}
			case processVerify:
				err = owners.imported.Verify(context.Background())
			case processPrepare:
				owners.prepared, err = owners.imported.Prepare()
			case processActivate:
				out, err = d.StepApplicationSnapshot(r.packet, owners.prepared)
			case processImportClose:
				err = owners.closeImport()
			}
		case processHold:
			if owners.held != nil {
				err = errLimit
				break
			}
			owners.held, err = openGraphRead(context.Background(), m, r.index)
			if err == nil {
				owners.heldID, err = owners.id()
				response.handles = []uint64{owners.heldID}
			}
		case processHeldProject:
			if owners.held == nil || owners.heldID != r.index {
				err = errInvalid
				break
			}
			var view graphstate.ReadView
			view, err = owners.held.ReadView()
			if err == nil {
				response.fact, err = processFactAt(view, graphstate.EntityID(r.entity), m.limits)
			}
			if err == nil {
				var root raftlog.ApplicationRoot
				root, err = owners.held.backend.Root()
				response.image = root.Image
				response.readGeneration = root.Generation
			}
		case processHoldClose:
			if owners.held == nil || owners.heldID != r.index {
				err = errInvalid
				break
			}
			err = owners.held.Close()
			owners.held = nil
			owners.heldID = 0
		case processClose:
			if failure := os.Getenv("RHO_GRAPH_PROCESS_CLOSE_FAILURE"); failure != "" {
				if failure == "eof" {
					if err := os.Stdout.Close(); err != nil {
						t.Fatal(err)
					}
				}
				for {
					time.Sleep(time.Hour)
				} // Parent must kill and reap the actual child.
			}
			err = errors.Join(owners.close(d), d.Close())
			closed = true
		}
		if len(out.SnapshotSends) > 0 {
			for _, send := range out.SnapshotSends {
				var id uint64
				id, e := owners.sender(send, true)
				err = errors.Join(err, e)
				response.handles = append(response.handles, id)
			}
		}
		if !closed {
			var e error
			response.stats, response.cutID, response.manifestID, e = processStats(s)
			if owners.held != nil {
				work := owners.held.Work()
				response.stats[statHeldRows] = uint64(work.Records)
				response.stats[statHeldBytes] = uint64(work.Bytes)
			} // #nosec G115 -- work counters are nonnegative under validated finite source caps.
			err = errors.Join(err, e)
		}
		response.applied = d.Applied()
		response.packets = out.Packets
		response.reads = out.Reads
		response.code, response.detail = processError(err)
		response.causeMask = processCauseMask(err)
		wire, err = encodeProcessResponse(response)
		if err != nil {
			t.Fatal("child output limit", err)
		}
		if err = writeProcessFrame(os.Stdout, wire); err != nil {
			t.Fatal("child output", err)
		}
		if r.op == processClose {
			return
		}
	}
}

func processStoreBytes(dir string) (uint64, error) {
	var total uint64
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if info.Size() < 0 {
				return errCorrupt
			}
			total += uint64(info.Size())
			if total > 256<<20 {
				return errLimit
			}
		}
		return nil
	})
	return total, err
}

func TestReplicatedProcessProtocolBoundaries(t *testing.T) {
	for _, wire := range [][]byte{nil, {0, 0, 0, 9}, {0xff, 0xff, 0xff, 0xff}} {
		_, err := readProcessFrame(bytes.NewReader(wire))
		expected := io.EOF
		if len(wire) == 4 {
			if wire[0] == 0xff {
				expected = errLimit
			}
		}
		if !errors.Is(err, expected) {
			t.Fatal("framing", err, expected)
		}
	}
	bad, err := processEncoding(func(w *boundedWriter) {
		w.add(make([]byte, 16+statCount*8+64+8))
		w.u32(0)
		w.field(nil)
		w.tag(0)
		w.u64(1)
		w.u64(1)
		w.tag(0)
		w.u16(0)
		w.text("")
		w.u32(processPacketCount + 1)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decodeProcessResponse(bad); !errors.Is(err, errLimit) {
		t.Fatal(err)
	}
	if _, err = encodeProcessResponse(processResponse{packets: make([]replica.Packet, processPacketCount+1)}); !errors.Is(err, errLimit) {
		t.Fatal(err)
	}
	stderr := processStderr{}
	if n, err := stderr.Write(bytes.Repeat([]byte{'s'}, processStderrBytes+1)); err != nil || n != processStderrBytes+1 {
		t.Fatal(n, err)
	}
	if text, overflow := stderr.state(); len(text) != processStderrBytes || !overflow {
		t.Fatal(len(text), overflow)
	}
	for _, sentinel := range []error{replica.ErrInvalid, raftlog.ErrLimit, replica.ErrUnavailable, errCorrupt, raftlog.ErrClosed} {
		code, _ := processError(errors.Join(errors.New("wrapped"), sentinel))
		if code == 0 || code == 6 {
			t.Fatal(sentinel, code)
		}
	}
}

func TestReplicatedProcessShutdownReapsFailedCloseRPC(t *testing.T) {
	if os.Getenv("RHO_GRAPH_PROCESS_CHILD") == "1" {
		t.Skip("parent only")
	}
	for _, mode := range []string{"eof", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			p := startGraphProcess(t, filepath.Join(t.TempDir(), "store"), 1, "RHO_GRAPH_PROCESS_CLOSE_FAILURE="+mode)
			p.call(t, processRequest{op: processCampaign}) // Child is live and has opened its real Driver.
			deadline := 5 * time.Second
			if mode == "timeout" {
				deadline = 100 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(t.Context(), deadline)
			defer cancel()
			err := p.shutdown(ctx, false)
			want := io.EOF
			if mode == "timeout" {
				want = context.DeadlineExceeded
			}
			if !errors.Is(err, want) {
				t.Fatal("close failure cause lost", err, want)
			}
			if p.cmd.ProcessState == nil {
				t.Fatal("child not reaped")
			}
			status, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatal("expected actual forced kill", p.cmd.ProcessState)
			}
			if err := p.cmd.Process.Signal(syscall.Signal(0)); !errors.Is(err, os.ErrProcessDone) {
				t.Fatal("child still usable/not reaped", err)
			}
			if _, err := p.stdin.Write([]byte{1}); !errors.Is(err, os.ErrClosed) {
				t.Fatal("stdin leaked", err)
			}
			if _, err := p.stdout.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
				t.Fatal("stdout leaked", err)
			}
			state := p.cmd.ProcessState
			if err := p.shutdown(t.Context(), false); !errors.Is(err, want) || p.cmd.ProcessState != state {
				t.Fatal("repeat shutdown changed cleanup", err)
			}
		})
	}
}
