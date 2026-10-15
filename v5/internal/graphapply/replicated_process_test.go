package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
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
	processGuardRequest
	processArmLeaderKill
	processEntryRecord
	processCertifiedRead
	processCertifiedAction
	processCertifiedObserve
	processSourceServe
	processSourceReply
	processSourceCancel
	processSourceCoordinate
	processSourceObserve
	processSourcePause
	processSourcePausedBarrier
	processSourceCutEvidence
	processSourceCertifiedOpen
	processSourceCertifiedObserve
	processSourceCertifiedClose
	processRangeSnapshotProbe
	processRangeImportProbe
	processRangeEvidenceProbe
	processRangeRun
	processRangeResume
	processRangeCertifiedDetail
	processRangeProgressProbe
	processRangeNativeVoterProbe
	processCertifiedIndexObserve
	processP08BeginAcquire
	processP08ReadAcquire
	processP08FinishAcquire
	processP08Mint
	processP08PrepareGraph
	processP08ReadGrant
	processP08Metadata
	processP08ChangedGraph
	processLifecycleObserve
	processLifecycleRecord
	processPairedConnections
	processPairedReconnect
	processPairedPendingOpen
	processCostObserve
	processCostRecord
	processCostRecords
	processP05Open
	processP05Observe
	processP05Coordinate
	processP05Resume
	processP05Close
	processP05Proposal
	processNativeFaultControl
	processNativeDecisionControl
	processNativeFaultFamilyControl
	processNativeTransitionControl
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
	if r.sequence == 0 || r.op < processCampaign || r.op > processNativeTransitionControl || len(c.b) != 0 {
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
	return decodeProcessResponseForProfile(b, false)
}
func decodeProcessResponseForProfile(b []byte, durableColumns bool) (processResponse, error) {
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
	maxCode := byte(6)
	if durableColumns {
		maxCode = 9
	}
	if r.sequence == 0 || r.code > maxCode || len(c.b) != 0 {
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
	mux                                       *graphProcessMux
	cmd                                       *exec.Cmd
	stdin                                     io.WriteCloser
	stdout                                    io.ReadCloser
	stderr                                    processStderr
	sequence                                  uint64
	closed                                    bool
	durableColumns                            bool // explicit child profile, never inferred from a result code
	incarnation                               [16]byte
	workloadRun                               [16]byte
	maxPinned, maxOffers, maxImports, maxHeld uint64
	closeErr                                  error
	maxFrame                                  int
	// Test-only ordering observations; callbacks must only record, never block
	// or call testing.T, so cleanup retains its unconditional kill/reap path.
	shutdownObserve func(phase string, err error)
}

func startGraphProcess(t *testing.T, dir string, id uint64, extraEnv ...string) *graphProcess {
	t.Helper()
	p := &graphProcess{cmd: exec.Command(os.Args[0], "-test.run=^TestReplicatedGraphChild$", "-test.count=1")}
	p.cmd.Env = append(os.Environ(), "RHO_GRAPH_PROCESS_CHILD=1", "RHO_GRAPH_PROCESS_ID="+strconv.FormatUint(id, 10), "RHO_GRAPH_PROCESS_DIR="+dir, "GOMAXPROCS=1")
	p.cmd.Env = append(p.cmd.Env, extraEnv...)
	// Coverage is an explicit P05 qualification lane. Each incarnation owns a
	// separate profile, including a reopen in the same graph directory.
	conditional, childCover := false, false
	durableProfile, contentionProfile, edgeProfile, feedProfile, selectorsProfile, createUpdateProfile, feedRetainedProfile, immutableColumnsProfile, lateCorrectionProfile := false, false, false, false, false, false, false, false, false
	transferProfile := false
	var workloadRun string
	for _, entry := range p.cmd.Env {
		name, value, _ := strings.Cut(entry, "=")
		switch name {
		case "RHO_GRAPH_DURABLE_COLUMN_PROCESS":
			durableProfile = value == "1"
		case "RHO_GRAPH_NATIVE_WORKLOAD":
			contentionProfile = value == nativeContentionFormat
		case "RHO_GRAPH_WORKLOAD_RUN":
			workloadRun = value
		case "RHO_GRAPH_EDGE_UNIQUE_PROCESS":
			edgeProfile = value == "1"
		case "RHO_GRAPH_FEED_PROCESS":
			feedProfile = value == "1" || value == "2"
		case "RHO_GRAPH_FEED_RETAINED_PROCESS":
			feedRetainedProfile = value == "1"
		case "RHO_GRAPH_SELECTORS_PROCESS":
			selectorsProfile = value == "1" || value == "2"
		case "RHO_GRAPH_CREATE_UPDATE_PROCESS":
			createUpdateProfile = value == "1"
		case nativeImmutableColumnsProcessFlag:
			immutableColumnsProfile = value == "1"
		case nativeLateCorrectionProcessFlag:
			lateCorrectionProfile = value == "1"
		case nativeTransferProcessFlag:
			transferProfile = value == "1"
		case "RHO_GRAPH_CONDITIONAL_PROCESS":
			conditional = value == "1"
		case "RHO_P05_CHILD_COVER":
			childCover = value == "1"
		}
	}
	p.durableColumns = durableProfile || contentionProfile || edgeProfile || feedProfile || selectorsProfile || createUpdateProfile || feedRetainedProfile || immutableColumnsProfile || lateCorrectionProfile || transferProfile
	if contentionProfile {
		raw, err := hex.DecodeString(workloadRun)
		if err != nil || len(raw) != 16 || hex.EncodeToString(raw) != workloadRun {
			t.Fatal("fresh workload run binding", err)
		}
		copy(p.workloadRun[:], raw)
		if p.workloadRun == ([16]byte{}) {
			t.Fatal("zero fresh workload run")
		}
	}
	if conditional && childCover {
		if err := os.MkdirAll(dir, 0750); err != nil {
			t.Fatal(err)
		}
		profile, err := os.CreateTemp(dir, "p05-child-cover-*.out")
		if err != nil {
			t.Fatal(err)
		}
		if err := profile.Close(); err != nil {
			t.Fatal(err)
		}
		p.cmd.Args = append(p.cmd.Args, "-test.coverprofile="+profile.Name())
	}
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
		t.Fatalf("child %d RPC op=%d index=%d entity=%d packet=%d->%d: %v code=%d causes=%x detail=%s", p.cmd.Process.Pid, r.op, r.index, r.entity, r.packet.From, r.packet.To, err, response.code, response.causeMask, response.detail)
	}
	return response
}

// No testing.T calls here: cleanup must never Goexit before closing/reaping.
func (p *graphProcess) exchange(ctx context.Context, r processRequest) (processResponse, error) {
	if p.mux != nil {
		return p.mux.exchange(ctx, r)
	}
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
		r, err := decodeProcessResponseForProfile(wire, p.durableColumns)
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
		kill := func() {
			e := p.cmd.Process.Kill()
			if p.shutdownObserve != nil {
				p.shutdownObserve("kill", e)
			}
			if e != nil && !errors.Is(e, os.ErrProcessDone) {
				err = errors.Join(err, e)
			}
		}
		mustKill := force || err != nil
		if mustKill {
			// EOF is a graceful-exit instruction to the actual child loop. Send
			// SIGKILL before closing stdin when crash/error cleanup requires it.
			kill()
		}
		if p.shutdownObserve != nil {
			p.shutdownObserve("stdin-close", nil)
		}
		closePipe(p.stdin)
		if !mustKill && err != nil {
			// A new pipe-close error still receives the existing kill fallback.
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
	for i, sentinel := range []error{raftlog.ErrInvalid, replica.ErrInvalid, raftlog.ErrLimit, replica.ErrLimit, errInvalid, errLimit, graphstore.ErrResourceLimit, replica.ErrUnavailable, idalloc.ErrInvalid, idalloc.ErrStaleAuthority, idalloc.ErrAlreadyReserved, idalloc.ErrUnknown, idalloc.ErrExhausted, idalloc.ErrPayloadMismatch, idalloc.ErrExpired, idalloc.ErrUnreserved} {
		if target == sentinel && e.mask&(1<<i) != 0 {
			return true
		}
	}
	return false
}
func processCauseMask(err error) uint16 {
	var mask uint16
	for i, sentinel := range []error{raftlog.ErrInvalid, replica.ErrInvalid, raftlog.ErrLimit, replica.ErrLimit, errInvalid, errLimit, graphstore.ErrResourceLimit, replica.ErrUnavailable, idalloc.ErrInvalid, idalloc.ErrStaleAuthority, idalloc.ErrAlreadyReserved, idalloc.ErrUnknown, idalloc.ErrExhausted, idalloc.ErrPayloadMismatch, idalloc.ErrExpired, idalloc.ErrUnreserved} {
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
	err := processRemoteError{r.causeMask, r.code, r.detail}
	if r.code >= 7 && r.code <= 9 {
		return nativeColumnProcessRemoteError{err}
	}
	return err
}

// One exact command only. The fixed hash metadata is diagnostic, never a
// client receipt, persistent cursor or another application implementation.
type leaderKillMaterializer struct {
	m                                       *materializer
	ctx                                     context.Context
	marker                                  string
	armed                                   bool
	hash, rootHash, outcomeHash, changeHash [32]byte
	identity                                [16]byte
	index, term, generation                 uint64
}

func (c *leaderKillMaterializer) SemanticContractID() raftlog.ApplicationSemanticContractID {
	return c.m.SemanticContractID()
}
func (c *leaderKillMaterializer) arm(wire []byte) error {
	if c.armed {
		return errLimit
	}
	r, err := decodeGraphRequest(wire, c.m.limits)
	if err != nil {
		return err
	}
	if r.kind != guardedGraphOperations || r.ns != c.m.ns {
		return errInvalid
	}
	c.hash = sha256.Sum256(wire)
	c.identity = r.identity()
	c.armed = true
	return nil
}
func (c *leaderKillMaterializer) Stage(e replica.Entry, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	b, err := c.m.Stage(e, budget)
	if err != nil {
		return b, err
	}
	if c.armed && sha256.Sum256(e.Data) == c.hash {
		o, err := decodeAnyOutcome(b.Outcome, c.m.ns)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if o.kind != guardedGraphOperations || o.disposition != applied || o.reason != reasonNone || o.identity != c.identity || o.hash != c.hash || o.index != e.Index || len(b.Changes) == 0 {
			return raftlog.ApplicationBatch{}, errInvalid
		}
		c.index, c.term, c.generation = e.Index, e.Term, e.Generation
		c.rootHash = sha256.Sum256(b.Image)
		c.outcomeHash = sha256.Sum256(b.Outcome)
		c.changeHash = sha256.Sum256(b.Changes)
	}
	return b, nil
}
func (c *leaderKillMaterializer) Restore(index uint64, image []byte) error {
	// Real Restore first: Driver has already synchronously installed this batch.
	if err := c.m.Restore(index, image); err != nil {
		return err
	}
	if !c.armed || c.index == 0 || c.index != index {
		return nil
	}
	if err := c.ctx.Err(); err != nil {
		return err
	}
	installed, actual, err := c.m.store.Checkpoint()
	if err != nil {
		return err
	}
	hard, _, err := c.m.store.InitialState()
	if err != nil {
		return err
	}
	term, err := c.m.store.Term(index)
	if err != nil {
		return err
	}
	outcome, err := c.m.store.ApplicationRecord(c.ctx, index, true, 64<<10)
	if err != nil {
		return err
	}
	changes, err := c.m.store.ApplicationRecord(c.ctx, index, false, 1<<20)
	if err != nil {
		return err
	}
	if installed != index || hard.GetCommit() < index || term != c.term || c.m.store.ApplicationGeneration() != c.generation || sha256.Sum256(actual) != c.rootHash || sha256.Sum256(outcome) != c.outcomeHash || sha256.Sum256(changes) != c.changeHash {
		return errCorrupt
	}
	marker, err := processEncoding(func(w *boundedWriter) {
		w.add([]byte("GKM1"))
		w.add(c.identity[:])
		w.add(c.hash[:])
		w.u64(index)
		w.u64(term)
		w.u64(c.generation)
		w.add(c.rootHash[:])
		w.add(c.outcomeHash[:])
		w.add(c.changeHash[:])
	})
	if err != nil {
		return err
	}
	if len(marker) > 256 {
		return errLimit
	}
	f, err := os.OpenFile(c.marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	n, writeErr := f.Write(marker)
	if writeErr == nil && n != len(marker) {
		writeErr = io.ErrShortWrite
	}
	err = errors.Join(writeErr, f.Sync(), f.Close())
	if err != nil {
		return err
	}
	if err := c.ctx.Err(); err != nil {
		return err
	}
	// Driver has not updated Applied, emitted Ready messages or returned Output;
	// child response framing cannot execute until this callback returns.
	return materializerKillSelfForCrashTest()
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
	if _, err := nativeColumnProcessProfile(); err != nil {
		t.Fatal("durable column profile", err)
	}
	if os.Getenv("RHO_GRAPH_CERTIFIED_PROCESS") == "1" {
		certifiedGraphProcessChild(t, id)
		return
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
	machine := &leaderKillMaterializer{m: m, ctx: t.Context(), marker: filepath.Join(filepath.Dir(cfg.Dir), "leader-kill.marker")}
	d, err := replica.Open(replica.Config{ID: id, Store: s, ApplicationMachine: machine, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
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
		case processGuardRequest:
			if owners.held == nil || owners.heldID != r.index {
				err = errInvalid
				break
			}
			var command graphRequest
			command, err = decodeGraphRequest(r.data, m.limits)
			if err == nil && (command.kind != graphOperations || command.ns != n) {
				err = errInvalid
			}
			if err == nil {
				response.aux, err = owners.held.Request(command.id, command.operations, command.revision, command.claims)
			}
		case processArmLeaderKill:
			if id != 1 {
				err = errInvalid
			} else {
				err = machine.arm(r.data)
			}
		case processEntryRecord:
			response.outcome, err = s.ApplicationRecord(context.Background(), r.index, true, 64<<10)
			if err == nil {
				response.changes, err = s.ApplicationRecord(context.Background(), r.index, false, 1<<20)
			}
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

// This test-only child mode keeps every completed proof inside its actual Host.
// Only source-produced proposal target/wire and copied observations cross IPC.
// It adds no remote graph read/planning or proof-import implementation.
const (
	certifiedInitialize uint64 = iota + 1
	certifiedBeginRecipient
	certifiedRecipientTransition
	certifiedReserve
	certifiedInstallGrant
	certifiedOpenRecipient
	certifiedCreateNode
	certifiedCreateCrossEdge
	certifiedHealthyLabel
	certifiedPrepareFence
	certifiedPrepareCertificate
	certifiedRangeSet
	certifiedE18Property
	certifiedE18Correction
	certifiedP08Reserve
	certifiedP08Transfer
	certifiedP08Close
	certifiedLifecycle
	certifiedPairedFaultProperty
	certifiedGraphCost
)

type certifiedProcessOwners struct {
	cost               *nativeGraphCostSpec
	workload           *nativeContentionSpec
	workloadRun        [16]byte
	workloadDeadline   time.Time
	workloadAdmission  nativeContentionAdmission
	fault              *nativeFaultFS
	stableBatch        *nativeBatchOwner
	decisions          *nativeDecisionOwner
	family             *nativeFaultFamilyOwner
	host               *allocationHost
	proof              allocationProof
	readID             allocationReadID
	question           allocationQuery
	ready              bool
	reserve            idalloc.RecipientSession
	recipient          *allocationRecipientHandle
	transferAllocation *nativeTransferAllocator
	incarnation        [16]byte
}

func encodeCertifiedProcessProposal(p allocationProposal) ([]byte, error) {
	scope, err := encodeAllocationScope(p.target)
	if err != nil {
		return nil, err
	}
	return processEncoding(func(w *boundedWriter) { w.field(scope); w.field(p.wire) })
}
func decodeCertifiedProcessProposal(wire []byte) (allocationProposal, error) {
	c := graphCursor{b: wire}
	scope := c.field(256)
	command := c.field(defaultMaterializerLimits().commandBytes)
	if c.err != nil || len(c.b) != 0 {
		return allocationProposal{}, errors.Join(errCorrupt, c.err)
	}
	target, err := decodeAllocationScope(scope)
	return allocationProposal{target: target, wire: command}, err
}
func certifiedProcessNamespace(id uint64) (namespace, [3]uint64) {
	if id <= 3 {
		return namespace{idalloc.GraphID{1}, 3}, [3]uint64{1, 2, 3}
	}
	return namespace{idalloc.GraphID{1}, 8}, [3]uint64{4, 5, 6}
}

func (o *certifiedProcessOwners) event(e allocationEvent) error {
	for _, reply := range e.replies {
		if o.ready || reply.id != o.readID || reply.question != o.question || reply.proof.owner != o.host {
			return errInvalid
		}
		if reply.err != nil {
			return reply.err
		}
		o.proof, o.ready = reply.proof, true
		if reply.question.kind == observeRecipient {
			o.reserve = reply.proof.observation.recipient.session
		}
	}
	if len(e.crossReplies)+len(e.cutReplies)+len(e.capacityReplies)+len(e.snapshotSends) != 0 {
		return errInvalid
	}
	return nil
}
func (o *certifiedProcessOwners) read(r processRequest) (allocationEvent, error) {
	q := allocationQuery{kind: allocationObservationKind(r.entity)}
	copy(q.nonce[:8], o.incarnation[:8])
	binary.BigEndian.PutUint64(q.nonce[8:], r.sequence)
	switch q.kind {
	case observeConfiguration, observeAllocator:
	case observeRecipient:
		if r.index < 1 || r.index > 2 {
			return allocationEvent{}, errInvalid
		}
		q.session.ID[0] = byte(r.index)
	case observeGrant:
		q.session = o.reserve
		var err error
		q.sequence, q.count, err = o.grantParameters(r.data)
		if err != nil {
			return allocationEvent{}, err
		}
	default:
		return allocationEvent{}, errInvalid
	}
	o.ready, o.proof = false, allocationProof{}
	e, err := o.host.Read(q)
	if err == nil {
		o.readID, o.question = e.readID, q
	}
	return e, err
}

func (o *certifiedProcessOwners) action(t *testing.T, r processRequest, init declaredInitCommand) (allocationProposal, error) {
	id := requestID(o.incarnation)
	binary.BigEndian.PutUint64(id[8:], r.sequence)
	h := o.host
	switch r.entity {
	case certifiedNativeFamilyBusiness:
		return nativeFaultFamilyBusiness(o, r)
	case certifiedNativeDecisionBusiness:
		return nativeDecisionBusiness(o, r)
	case certifiedPairedFaultProperty:
		return pairedGraphFaultProperty(o, r, id)
	case certifiedGraphCost:
		if o.workload != nil {
			return allocationProposal{}, errInvalid // fresh factories require their genuine retained condition owner
		}
		return nativeGraphCostProposal(o, r, id)
	case certifiedLifecycle:
		return nativeLifecycleProposal(o, r, id)
	case certifiedP08Reserve:
		request, err := decodeAllocationP08Request(r.data)
		if err != nil {
			return allocationProposal{}, err
		}
		return h.AuthorizeReserve(id, request, o.proof)
	case certifiedP08Close:
		cfg, err := h.currentConfiguration()
		if err != nil {
			return allocationProposal{}, err
		}
		scope, err := temporal.All(cfg.defaultAxis.Axis())
		if err != nil {
			return allocationProposal{}, err
		}
		if len(r.data) != 8 {
			return allocationProposal{}, errInvalid
		}
		life := binary.BigEndian.Uint64(r.data)
		return h.PreparePartitionGraphOperations(id, []graphstate.Operation{{Kind: graphstate.Close, Owner: graphstate.EntityID(r.index), Life: graphstate.LifeID(life), Scope: scope}}, 41, nil)
	case certifiedP08Transfer:
		return h.TransferAllocator(id, o.proof)
	case certifiedPrepareFence:
		return h.PrepareGraphFence(r.index)
	case certifiedPrepareCertificate:
		return h.PrepareGraphCertificate(r.index)
	case certifiedInitialize:
		schemas := partitionWriteSchemas()
		if os.Getenv("RHO_GRAPH_LIFECYCLE_PROCESS") == "1" || o.cost != nil {
			schemas = nativeLifecycleSchemas()
		}
		if os.Getenv("RHO_GRAPH_CONDITIONAL_PROCESS") == "1" {
			schemas = p05Schemas()
		}
		if o.cost != nil && o.cost.Version == 4 {
			schemas = nativeGraphEdgeUniqueSchemas()
		}
		return h.PreparePartitionInitialization(r.index, bootstrapAttemptID(id), schemas, init.maxBlock, graphGenesisBinding(t), *init.routing, o.proof)
	case certifiedBeginRecipient:
		if r.index < 1 || r.index > 2 {
			return allocationProposal{}, errInvalid
		}
		return h.BeginRecipient(id, [16]byte{byte(r.index)}, o.proof)
	case certifiedRecipientTransition:
		if len(r.data) != 1 {
			return allocationProposal{}, errInvalid
		}
		return h.RecipientTransition(id, commandKind(r.data[0]), r.index, o.proof)
	case certifiedReserve:
		sequence, count, err := uint64(0), uint64(0), error(nil)
		if o.workload != nil {
			sequence, count, err = nativeContentionGrantParameters(o, *o.workload, r.data)
		} else {
			sequence, count, err = o.grantParameters(r.data)
		}
		if err != nil {
			return allocationProposal{}, err
		}
		return h.AuthorizeReserve(id, idalloc.GrantRequest{Graph: h.machine.ns.graph, Session: o.reserve, Sequence: sequence, Count: count}, o.proof)
	case certifiedInstallGrant:
		return h.InstallGrant(id, r.index, o.proof)
	case certifiedOpenRecipient:
		var err error
		o.recipient, err = h.OpenRecipient(o.proof)
		return allocationProposal{}, err
	case certifiedE18Property, certifiedE18Correction:
		return graphE18Proposal(o, r, id)
	case certifiedRangeSet:
		return graphE17PropertyProposal(t, o, r, id)
	case certifiedCreateNode, certifiedCreateCrossEdge, certifiedHealthyLabel:
		if o.recipient == nil {
			return allocationProposal{}, errInvalid
		}
		scope, err := temporal.All(graphGenesisBinding(t).Axis())
		if err != nil {
			return allocationProposal{}, err
		}
		owner := graphstate.EntityID(r.index)
		life := graphstate.LifeID(r.index + 1)
		op := graphstate.Operation{Kind: graphstate.CreateNode, Owner: owner, Life: life, Scope: scope, Record: graphstate.EntityRecord{Interpretation: graphstate.InterpretationObservation, TemporalRole: graphstate.TemporalRoleValidity}}
		grant := grantReference{session: o.recipient.session, sequence: 1}
		claims := []freshBinding{{role: entityBinding, id: r.index, grant: grant}, {role: lifeBinding, owner: owner, id: r.index + 1, grant: grant}}
		if h.machine.rangeMoves() && r.index == 1 {
			claims[1].grant.sequence = 2
		}
		switch r.entity {
		case certifiedCreateCrossEdge:
			op = graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: 3, Life: 4, Scope: scope, Record: graphstate.EntityRecord{Type: "cross-edge", Source: 1, Target: 17, Mode: graphstate.LifeBound, Interpretation: graphstate.InterpretationAssertedRelation, TemporalRole: graphstate.TemporalRoleOccurrenceTime}, Binding: graphstate.LifeRecord{SourceLife: 2, TargetLife: 18}}
			claims[0].id, claims[1].owner, claims[1].id = 3, 3, 4
			if h.machine.rangeMoves() {
				claims[0].grant.sequence, claims[1].grant.sequence = 2, 2
			}
		case certifiedHealthyLabel:
			op = graphstate.Operation{Kind: graphstate.AddLabel, Owner: owner, Life: life, Scope: scope, Name: "healthy"}
			claims = nil
		}
		return h.PreparePartitionGraphOperations(id, []graphstate.Operation{op}, 23, claims)
	default:
		return allocationProposal{}, errInvalid
	}
}

func certifiedProcessObservation(ctx context.Context, h *allocationHost, id uint64) (observation certifiedProcessFact, err error) {
	backend, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		return observation, err
	}
	defer func() { err = errors.Join(err, backend.Close()) }()
	cfg, err := h.currentConfiguration()
	if err != nil {
		return observation, err
	}
	configuration, err := cfg.digest()
	if err != nil {
		return observation, err
	}
	l := h.machine.limits
	v, _, err := graphstore.OpenPartitionGraphReadView(ctx, backend, h.machine.declaration, cfg.defaultAxis, configuration, l.catalog, l.graph, graphstore.OwnershipBudget{SourceRows: l.sourceRows, SourceBytes: l.sourceBytes, OutputBytes: l.outputBytes})
	if err != nil {
		return observation, err
	}
	defer func() { err = errors.Join(err, v.Close()) }()
	entity, err := v.Entity(ctx, graphstate.EntityID(id))
	if err != nil {
		return observation, err
	}
	observation.Exists = entity.Found
	if entity.Found {
		point, e := temporal.RationalPosition(cfg.defaultAxis.Axis(), temporal.RationalInt64(0))
		if e != nil {
			return observation, e
		}
		p, e := graphstate.Project(ctx, v, graphstate.EntityID(id), point, graphstate.Effective, l.graph.Planner)
		if e != nil {
			return observation, e
		}
		observation.Active, observation.Life, observation.Labels = p.Active, uint64(p.Life), p.Labels
		observation.Source, observation.Target = uint64(p.Record.Source), uint64(p.Record.Target)
		if entity.Record.Kind == graphstate.Node {
			scope, e := temporal.All(cfg.defaultAxis.Axis())
			if e != nil {
				return observation, e
			}
			page, e := v.IncidentRelationships(ctx, graphstate.IncidentPredicate{Endpoint: graphstate.EntityID(id), Life: p.Life, Window: scope}, 0, graphstate.ReadBudget{Rows: 16, Bytes: 64 << 10})
			if e != nil {
				return observation, e
			}
			if !page.Complete || len(page.Entities) > 16 {
				return observation, errLimit
			}
			for _, edge := range page.Entities {
				observation.Incident = append(observation.Incident, uint64(edge))
			}
		}
	}
	return observation, nil
}

func certifiedGraphProcessChild(t *testing.T, id uint64) {
	t.Helper()
	protocolState, profileErr := nativeProtocolStateProfileFromEnv()
	if profileErr != nil {
		t.Fatal("D-only protocol state profile", profileErr)
	}
	pooledState := false
	if protocolState || os.Getenv("RHO_GRAPH_PROTOCOL_STATE_RECOVERY") != "" {
		selected, err := nativeProtocolStateAgreement()
		if err != nil || !protocolState || selected != rangeMoveSpanSemanticContractID() && selected != rangeMovePooledSemanticContractID() {
			t.Fatal("D-only actual recovery agreement", errors.Join(errInvalid, err))
		}
		pooledState = selected == rangeMovePooledSemanticContractID()
	}
	n, voters := certifiedProcessNamespace(id)
	init := partitionCodecRequest(t, 1, defaultMaterializerLimits())
	if init.declaration.Graph() != graphstate.GraphID(n.graph) {
		t.Fatal("certified fixture graph identity")
	}
	part, found := init.declaration.Partition(n.partition)
	if !found {
		t.Fatal("certified fixture partition")
	}
	l := raftlog.DefaultLimits()
	l.CacheBytes, l.MemTableBytes, l.MaxRetainedBytes, l.MaxRetainedEntries = 2<<20, 1<<20, 16<<20, 8192
	p := raftlog.DefaultApplicationPolicy(id)
	p.MaxImageBytes, p.MaxInstallWrites, p.RetainedApplicationRecords, p.RetainedApplicationBytes = 172, 256, 8192, 16<<20
	agreement := certifiedGraphSemanticContractID()
	materializer := newCertifiedGraphMaterializer
	generations := raftlog.ApplicationGenerationLimits{MaxBytes: 32 << 20, MaxRecords: 16384}
	if os.Getenv("RHO_GRAPH_RANGE_PROCESS") == "1" {
		agreement, materializer = rangeMoveSemanticContractID(), newRangeMoveMaterializer
		// Logical protocol history is admitted separately from unchanged child
		// cache/memtable and aggregate process RSS/time limits.
		l.MaxRetainedBytes, l.MaxRetainedEntries = 256<<20, 65536
		p.RetainedApplicationBytes, p.RetainedApplicationRecords = 256<<20, 65536
		generations = raftlog.ApplicationGenerationLimits{MaxBytes: 512 << 20, MaxRecords: 131072}
	}
	// The explicit fresh conditional profile keeps RANGE quotas when requested,
	// but always uses its distinct semantic ID on create AND same-directory reopen.
	if os.Getenv("RHO_GRAPH_CONDITIONAL_PROCESS") == "1" {
		agreement, materializer = conditionalGraphSemanticContractID(), newConditionalGraphMaterializer
	}
	retention := os.Getenv("RHO_GRAPH_RETENTION_PROCESS") == "1"
	if retention && (os.Getenv("RHO_GRAPH_RANGE_PROCESS") == "1" || os.Getenv("RHO_GRAPH_CONDITIONAL_PROCESS") == "1") {
		t.Fatal("retention profile conflicts with RANGE/P05 profile")
	}
	if retention {
		agreement, materializer = retainedGraphSemanticContractID(), newRetainedGraphMaterializer
	}
	cost, err := nativeGraphCostSpecFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if retention && cost != nil {
		t.Fatal("retention profile conflicts with COST profile")
	}
	if err := nativeGraphCostProfile(cost, &init, &l, &p, &generations); err != nil {
		t.Fatal(err)
	}
	workload, workloadRun, err := nativeContentionSpecFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	var workloadDeadline time.Time
	var workloadAdmission nativeContentionAdmission
	if workload != nil {
		workloadAdmission, err = nativeContentionAdmissionFromEnv(*workload, workloadRun)
		if err != nil {
			t.Fatal("fixed fresh workload deadline", err)
		}
		workloadDeadline = time.Unix(0, workloadAdmission.DeadlineUnixNanos)
	}
	if err := nativeContentionProfile(workload, &init, &l, &p, &generations); err != nil {
		t.Fatal(err)
	}
	if workload != nil {
		// The facade enables only shared diagnostics/schema selection. Business
		// and grant factories above take the distinct authenticated fresh route.
		cost = &nativeGraphCostSpec{Version: 1, Facts: workload.Facts, CrossPercent: workload.CrossPercent, Writers: workload.Writers}
		agreement, materializer = durableColumnGraphSemanticContractID(), newDurableColumnGraphMaterializer
	}
	durableColumns, err := nativeColumnProcessProfile()
	if err != nil {
		t.Fatal(err)
	}
	edgeUnique, err := nativeGraphEdgeUniqueProcessProfile(cost)
	if err != nil {
		t.Fatal(err)
	}
	selectors, err := nativeSelectorsProcessProfile(cost)
	if err != nil {
		t.Fatal("selectors profile", err)
	}
	createUpdate, err := nativeCreateUpdateProcessProfile(cost)
	if err != nil {
		t.Fatal("create/update profile", err)
	}
	immutableColumns, err := nativeImmutableColumnsProcessProfile(cost)
	if err != nil {
		t.Fatal("immutable columns profile", err)
	}
	lateCorrection, err := nativeLateCorrectionProcessProfile(cost)
	if err != nil {
		t.Fatal("late correction profile", err)
	}
	durableColumns = durableColumns || edgeUnique || selectors || createUpdate || immutableColumns || lateCorrection
	if durableColumns {
		agreement, materializer = durableColumnGraphSemanticContractID(), newDurableColumnGraphMaterializer
	}
	feed, err := nativeFeedProcessProfile(cost)
	if err != nil {
		t.Fatal("feed profile", err)
	}
	if feed {
		agreement, materializer = retainedFeedSemanticContractID(), newRetainedFeedMaterializer
	}
	feedRetained, err := nativeFeedRetainedProcessProfile(cost)
	if err != nil {
		t.Fatal("retained-only feed profile", err)
	}
	if feedRetained {
		agreement, materializer = retainedOnlyFeedSemanticContractID(), newRetainedOnlyFeedMaterializer
	}
	transfer, err := nativeTransferProcessProfile(cost)
	if err != nil {
		t.Fatal("pooled transfer workload profile", err)
	}
	if transfer {
		agreement, materializer = rangeMovePooledSemanticContractID(), newRangeMovePooledMaterializer
	}
	if protocolState {
		agreement, materializer = rangeMoveSpanSemanticContractID(), newRangeMoveSpanMaterializer
		l.MaxRetainedBytes, l.MaxRetainedEntries = 256<<20, 131072
		p.RetainedApplicationBytes, p.RetainedApplicationRecords = 256<<20, 131072
		generations = raftlog.ApplicationGenerationLimits{MaxBytes: 512 << 20, MaxRecords: 262144}
	}
	if pooledState {
		agreement, materializer = rangeMovePooledSemanticContractID(), newRangeMovePooledMaterializer
	}
	controls, recovery := raftlog.ApplicationControlConfig{Version: 1}, raftlog.ApplicationRecoveryConfig{Version: 1}
	if retention || durableColumns || workload != nil || feed || feedRetained {
		recovery.Version = 2
	}
	var nativeRetention raftlog.ApplicationRetentionConfig
	contract, err := raftlog.ApplicationContractForPolicyWithRecovery(p, controls, recovery)
	if durableColumns || workload != nil || feed || feedRetained {
		nativeRetention.Version = 1
		contract, err = raftlog.ApplicationContractForPolicyWithRetention(p, controls, recovery, nativeRetention)
	}
	if protocolState {
		recovery.Version, nativeRetention.Version = 3, 1
		contract, err = raftlog.ApplicationContractForPolicyWithRetention(p, controls, recovery, nativeRetention)
	}
	if transfer || pooledState {
		recovery.Version, nativeRetention.Version = 4, 1
		contract, err = raftlog.ApplicationContractForPolicyWithRetention(p, controls, recovery, nativeRetention)
	}
	if err != nil {
		t.Fatal(err)
	}
	create := os.Getenv("RHO_GRAPH_PROCESS_REOPEN") != "1"
	family, familyFS, err := newNativeFaultFamilyOwner(t.Context(), os.Getenv("RHO_GRAPH_PROCESS_DIR"), id, create)
	if err != nil {
		t.Fatal(err)
	}
	if family != nil {
		defer family.close()
	}
	var fault *nativeFaultFS
	var faultFS vfs.FS = familyFS
	switch os.Getenv("RHO_GRAPH_NATIVE_FAULT_PROCESS") {
	case "", "off":
	case "1":
		fault, err = newNativeFaultFS(os.Getenv("RHO_GRAPH_PROCESS_DIR"), id)
		if err != nil {
			t.Fatal(err)
		}
		if familyFS != nil {
			fault.FS = familyFS
		}
		faultFS = fault
		if kind := os.Getenv("RHO_GRAPH_NATIVE_FAULT_START"); kind != "" {
			_, _, transition := nativeTransitionSelector(kind)
			if create || kind != "directory-sync" && kind != "options-rename" && !transition {
				t.Fatal("startup fault requires same-directory reopen with an admitted actual native IO selector")
			}
			arm, encodeErr := json.Marshal(nativeFaultControl{Version: 1, Action: "arm", Cookie: fault.state.Cookie, Sequence: 1, Kind: kind, Nth: 1})
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			if _, err := fault.control(arm); err != nil {
				t.Fatal(err)
			}
		}
	default:
		t.Fatal("unknown native fault process profile")
	}
	stableBatch, stableFS, err := nativeBatchOpen(t.Context(), os.Getenv("RHO_GRAPH_PROCESS_DIR"), id, faultFS)
	if err != nil {
		t.Fatal(err)
	}
	if stableBatch != nil {
		defer stableBatch.close()
		faultFS = stableFS
	}
	s, err := raftlog.Open(raftlog.Config{Dir: os.Getenv("RHO_GRAPH_PROCESS_DIR"), FS: faultFS, Create: create, Limits: l, Application: p, Controls: controls, Recovery: recovery, Retention: nativeRetention, Transfer: raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(n.graph), Partition: n.partition, Group: part.Group}, Contract: contract, Limits: raftlog.ApplicationTransferLimits{MaxExports: 2, MaxChunkRows: 64, MaxChunkBytes: 256 << 10, MaxStagedBytes: 16 << 20, MaxStagedRecords: 8192, MaxPinnedLogicalBytes: 64 << 20}}, Generations: generations, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 4096}, Replication: raftlog.ApplicationReplicationConfig{Voters: voters}, SemanticContractID: agreement})
	if err != nil {
		if family != nil {
			err = errors.Join(err, family.recordOpenRefusal(os.Getenv("RHO_GRAPH_PROCESS_DIR"), err))
		}
		t.Fatal(err)
	}
	if create {
		if err := graphstore.BootstrapBoundOwnership(s, s.ApplicationBinding(), init.declaration, voters); err != nil {
			_ = s.Close()
			t.Fatal(err)
		}
	}
	m, err := materializer(s, n, init.declaration, defaultMaterializerLimits())
	if err != nil {
		_ = s.Close()
		t.Fatal(err)
	}
	h, err := p02OpenAllocationHost(m, id, replica.Config{Store: s, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
	if err != nil {
		_ = s.Close()
		t.Fatal(err)
	}
	defer func() {
		if err := h.Close(); err != nil {
			t.Error(err)
		}
	}()
	o := certifiedProcessOwners{host: h, cost: cost, workload: workload, workloadRun: workloadRun, workloadDeadline: workloadDeadline, workloadAdmission: workloadAdmission, fault: fault, stableBatch: stableBatch, family: family}
	if _, err := io.ReadFull(rand.Reader, o.incarnation[:]); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("RHO_GRAPH_SOURCE_PROCESS") == "1" {
		graphSourceProcessChild(t, h, &o, init)
		return
	}

	for {
		wire, err := readProcessFrame(os.Stdin)
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		r, err := decodeProcessRequest(wire)
		if err != nil {
			t.Fatal(err)
		}
		if r.incarnation != o.incarnation && (r.sequence != 1 || r.incarnation != ([16]byte{})) {
			t.Fatal("certified stale IPC incarnation")
		}
		response := processResponse{incarnation: o.incarnation, sequence: r.sequence}
		var event allocationEvent
		switch r.op {
		case processCampaign:
			event, err = h.Campaign()
		case processTick:
			event, err = h.Tick()
		case processStep:
			event, err = h.Step(r.packet)
		case processCertifiedRead:
			event, err = o.read(r)
		case processCertifiedAction:
			var proposal allocationProposal
			proposal, err = o.action(t, r, init)
			if err == nil && len(proposal.wire) != 0 {
				response.aux, err = encodeCertifiedProcessProposal(proposal)
			}
		case processPropose:
			var proposal allocationProposal
			proposal, err = decodeCertifiedProcessProposal(r.data)
			if err == nil {
				event, err = h.Submit(proposal)
			}
		case processStateOp:
			_, response.image, err = s.Checkpoint()
		case processEntryRecord:
			response.outcome, err = s.ApplicationRecord(t.Context(), r.index, true, 64<<10)
			if err == nil {
				response.changes, err = s.ApplicationRecord(t.Context(), r.index, false, 1<<20)
			}
		case processCertifiedObserve:
			var observation certifiedProcessFact
			observation, err = certifiedProcessObservation(t.Context(), h, r.entity)
			if err == nil {
				response.aux, err = encodeCertifiedProcessFact(observation)
			}
		case processClose:
			err = h.Close()
		default:
			err = errInvalid
		}
		if err == nil {
			err = o.event(event)
		}
		if r.op != processClose {
			var e error
			response.stats, response.cutID, response.manifestID, e = processStats(s)
			err = errors.Join(err, e)
		}
		response.ready, response.applied, response.packets = o.ready, h.driver.Applied(), event.packets
		response.code, response.detail = processError(err)
		response.causeMask = processCauseMask(err)
		wire, err = encodeProcessResponse(response)
		if err != nil {
			t.Fatal(err)
		}
		if err := writeProcessFrame(os.Stdout, wire); err != nil {
			t.Fatal(err)
		}
		if r.op == processClose {
			return
		}
	}
}
