package txnproto

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// These are test-only, trusted-controller IPC frames, not a production RPC API.
// Proof NEVER crosses IPC: actual Host replies remain in the child that owns
// them. That child invokes the real constructor and emits an owned Proposal.
// Inspection Views in replies cannot be submitted as proof. Proposal bytes are
// forwarded unchanged by the crash-only controller, which owns every pipe.
// Two matching single-group cuts are compared together; transporting/assembling
// a global Cut across processes remains an OPEN integration obligation.
const processFrameLimit = 4 << 20

type processRequest struct {
	Sequence uint64
	Op       string
	Tx       Tx
	Query    Query
	Packet   replica.Packet
	Handle   string
	Prior    string
	Proposal *processProposal
	Round    uint64
	Target   uint64
}

type processEvidence struct {
	Handle   string
	Sequence uint64
	Query    Query
	View     View // inspection only; there is no View input in processRequest
	Err      string
}

type processProposal struct {
	Source, Incarnation uint64
	Sequence            uint64
	Handles             []string
	Bytes               []byte
	Hash                [32]byte
}

type processResponse struct {
	Source, Incarnation, Sequence uint64
	Packets                       []replica.Packet
	Evidence                      []processEvidence
	Proposal                      *processProposal
	State                         *Snapshot
	Err                           string
}

func processWrite(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) > processFrameLimit {
		return ErrLimit
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(b)))
	for _, p := range [][]byte{header[:], b} {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n != len(p) {
			return io.ErrShortWrite
		}
	}
	return nil
}

func processRead(r io.Reader, v any) error {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(header[:])
	if n == 0 || n > processFrameLimit {
		return ErrLimit
	}
	b := make([]byte, int(n))
	if _, err := io.ReadFull(r, b); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	return d.Decode(v)
}

// Known sentinels retain errors.Is semantics across this test-only wire. Unknown
// errors remain diagnostics; they cannot silently become an availability result.
func processError(err error) string {
	if err == nil {
		return ""
	}
	for _, sentinel := range []error{ErrInvalid, ErrMismatch, ErrStale, ErrRetry, ErrPending, ErrUnavailable, ErrLimit, replica.ErrUnavailable, replica.ErrInvalid, replica.ErrStopped, raftlog.ErrCorrupt} {
		if errors.Is(err, sentinel) {
			return sentinel.Error()
		}
	}
	return "unexpected: " + err.Error()
}

func processErr(s string) error {
	if s == "" {
		return nil
	}
	for _, sentinel := range []error{ErrInvalid, ErrMismatch, ErrStale, ErrRetry, ErrPending, ErrUnavailable, ErrLimit, replica.ErrUnavailable, replica.ErrInvalid, replica.ErrStopped, raftlog.ErrCorrupt} {
		if s == sentinel.Error() {
			return sentinel
		}
	}
	return errors.New(s)
}

// TestTxnProcessChild reexecutes only in the six children owned by the parent.
// Each child owns one disk-backed Host/Driver, and logical time advances ONLY
// upon controller commands. SIGKILL establishes process-crash evidence: the OS
// cache survives, so this does not establish power-loss or physical-domain safety.
func TestTxnProcessChild(t *testing.T) {
	dir := os.Getenv("RHO_TXN_PROCESS_DIR")
	if dir == "" {
		t.Skip("owned subprocess helper")
	}
	id, err := strconv.ParseUint(os.Getenv("RHO_TXN_PROCESS_ID"), 10, 64)
	if err != nil || id < 1 || id > 6 {
		t.Fatalf("child id: %d %v", id, err)
	}
	group := uint8((id - 1) / 3)
	create := os.Getenv("RHO_TXN_PROCESS_CREATE") == "true"
	s, err := raftlog.Open(raftlog.Config{Dir: dir, Create: create})
	if err != nil {
		t.Fatal(err)
	}
	if create {
		base := uint64(group)*3 + 1
		if err := s.Initialize([]uint64{base, base + 1, base + 2}, nil); err != nil {
			t.Fatal(err)
		}
	}
	h, err := OpenHost(config(group), s, id)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	pid := uint64(os.Getpid())
	proofs := map[string]Proof{}
	pending := map[Query]uint64{}
	if err := processWrite(os.Stdout, processResponse{Source: id, Incarnation: pid}); err != nil {
		t.Fatal(err)
	}
	var sequence uint64
	for {
		var r processRequest
		if err := processRead(os.Stdin, &r); err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			t.Fatal(err)
		}
		if r.Sequence != sequence+1 {
			t.Fatal("nonsequential controller request")
		}
		sequence = r.Sequence
		out := processResponse{Source: id, Incarnation: pid, Sequence: sequence}
		var event Event
		var proposal Proposal
		var e error
		lookup := func(handle string) (Proof, error) {
			p, ok := proofs[handle]
			if !ok {
				return Proof{}, ErrUnavailable
			}
			return p, nil
		}
		switch r.Op {
		case "campaign":
			event, e = h.Campaign()
		case "tick":
			event, e = h.Tick()
		case "transfer":
			event, e = h.TransferLeader(r.Target)
		case "step":
			event, e = h.Step(r.Packet)
		case "read":
			pending[r.Query] = sequence
			event, e = h.Read(r.Query)
			if e != nil {
				delete(pending, r.Query)
			}
		case "checkpoint":
			e = h.SaveCheckpoint()
		case "last":
			// Applied-command diagnostics ONLY, never authoritative evidence.
			h.machine.mu.Lock()
			e = h.machine.last
			h.machine.mu.Unlock()
		case "at":
			p, err := lookup(r.Handle)
			e = err
			if e == nil {
				var c Cut
				c, e = AssembleCut([]uint8{group}, []Proof{p})
				if e == nil {
					var state Snapshot
					state, e = h.At(c)
					out.State = &state
				}
			}
		case "submit":
			if r.Proposal == nil || r.Proposal.Source < 1 || r.Proposal.Source > 6 || r.Proposal.Incarnation == 0 || r.Proposal.Hash != sha256.Sum256(r.Proposal.Bytes) {
				e = ErrInvalid
				break
			}
			var c command
			c, e = decode[command](r.Proposal.Bytes, DefaultLimits().CommandBytes)
			if e == nil {
				// This is an actual source-produced Proposal, NOT a Proof import.
				event, e = h.Submit(Proposal{command: c})
			}
		case "register":
			proposal, e = Register(r.Tx)
		case "local":
			proposal, e = Local(r.Tx)
		case "fence":
			proposal, e = Fence(r.Round)
		case "certify":
			proposal, e = Certify(r.Round)
		case "prepare", "vote", "decide", "resolve":
			p, err := lookup(r.Handle)
			e = err
			if e != nil {
				break
			}
			switch r.Op {
			case "prepare":
				var prior Proof
				if r.Prior != "" {
					prior, e = lookup(r.Prior)
				}
				if e == nil {
					proposal, e = Prepare(p, prior)
				}
			case "vote":
				proposal, e = RecordVote(p)
			case "decide":
				proposal, e = Decide(p, true)
			case "resolve":
				proposal, e = Resolve(p)
			}
		default:
			e = ErrInvalid
		}
		if proposal.command.Kind != "" && e == nil {
			b, err := json.Marshal(proposal.command)
			e = err
			out.Proposal = &processProposal{Source: id, Incarnation: pid, Sequence: sequence, Bytes: b, Hash: sha256.Sum256(b)}
			if r.Handle != "" {
				out.Proposal.Handles = append(out.Proposal.Handles, r.Handle)
			}
			if r.Prior != "" {
				out.Proposal.Handles = append(out.Proposal.Handles, r.Prior)
			}
		}
		out.Err = processError(e)
		out.Packets = event.Packets
		for _, reply := range event.Replies {
			readSequence, ok := pending[reply.Query]
			if !ok {
				t.Fatal("Host reply has no owned pending request")
			}
			delete(pending, reply.Query)
			item := processEvidence{Sequence: readSequence, Query: reply.Query, Err: processError(reply.Err)}
			if reply.Err == nil {
				if len(proofs) >= 256 {
					t.Fatal("proof handle budget")
				}
				item.Handle = fmt.Sprintf("%d/%d/%d", id, pid, readSequence)
				proofs[item.Handle] = reply.Proof // only actual completed Host output
				item.View = reply.Proof.View()
			}
			out.Evidence = append(out.Evidence, item)
		}
		if err := processWrite(os.Stdout, out); err != nil {
			t.Fatal(err)
		}
	}
}

type processChild struct {
	id, incarnation, sequence uint64
	cmd                       *exec.Cmd
	in, out                   *os.File
	log                       *os.File
	done                      chan error
	cancel                    context.CancelFunc
	stopped                   bool
}

type processCluster struct {
	t        *testing.T
	dir      string
	children map[uint64]*processChild
	drop     map[uint64]bool
	replies  map[uint64][]processEvidence
	stage    string
	steps    int
}

func newProcessCluster(t *testing.T) *processCluster {
	t.Helper()
	n := &processCluster{t: t, dir: t.TempDir(), children: map[uint64]*processChild{}, drop: map[uint64]bool{}, replies: map[uint64][]processEvidence{}, stage: "open"}
	t.Cleanup(func() {
		for id := uint64(1); id <= 6; id++ {
			if p := n.children[id]; p != nil {
				n.stop(p)
			}
		}
	})
	for id := uint64(1); id <= 6; id++ {
		n.start(id, true)
	}
	for _, id := range []uint64{1, 4} {
		n.run(id, processRequest{Op: "campaign"})
	}
	return n
}

func (n *processCluster) start(id uint64, create bool) {
	n.t.Helper()
	input, toChild, err := os.Pipe()
	if err != nil {
		n.t.Fatal(err)
	}
	fromChild, output, err := os.Pipe()
	if err != nil {
		input.Close()
		toChild.Close()
		n.t.Fatal(err)
	}
	log, err := os.CreateTemp(n.dir, fmt.Sprintf("replica-%d-*.log", id))
	if err != nil {
		input.Close()
		toChild.Close()
		fromChild.Close()
		output.Close()
		n.t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(n.t.Context(), 2*time.Minute)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestTxnProcessChild$", "-test.timeout=110s")
	cmd.Env = append(os.Environ(), "RHO_TXN_PROCESS_DIR="+filepath.Join(n.dir, fmt.Sprintf("replica-%d", id)), fmt.Sprintf("RHO_TXN_PROCESS_ID=%d", id), fmt.Sprintf("RHO_TXN_PROCESS_CREATE=%t", create))
	cmd.Stdin, cmd.Stdout, cmd.Stderr = input, output, log
	p := &processChild{id: id, cmd: cmd, in: toChild, out: fromChild, log: log, done: make(chan error, 1), cancel: cancel}
	if err := cmd.Start(); err != nil {
		cancel()
		input.Close()
		output.Close()
		toChild.Close()
		fromChild.Close()
		log.Close()
		n.t.Fatal(err)
	}
	input.Close()
	output.Close()
	n.children[id] = p // register cleanup BEFORE any handshake can fail
	go func() { p.done <- cmd.Wait() }()
	if err := p.out.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		n.t.Fatal(err)
	}
	var ready processResponse
	if err := processRead(p.out, &ready); err != nil {
		n.fail(p, "handshake", err)
	}
	if ready.Source != id || ready.Incarnation != uint64(cmd.Process.Pid) || ready.Err != "" {
		n.fail(p, "handshake identity", fmt.Errorf("%+v", ready))
	}
	p.incarnation = ready.Incarnation
	n.t.Logf("stage=%s replica=%d pid=%d create=%t disk=%s", n.stage, id, p.incarnation, create, filepath.Join(n.dir, fmt.Sprintf("replica-%d", id)))
}

func (n *processCluster) fail(p *processChild, op string, err error) {
	n.t.Helper()
	// Bound diagnostic reads even if a dependency produces unexpectedly many logs.
	f, openErr := os.Open(p.log.Name())
	var tail []byte
	if openErr == nil {
		tail, _ = io.ReadAll(io.LimitReader(f, 64<<10))
		f.Close()
	}
	n.t.Fatalf("stage=%s event=%d replica=%d pid=%d op=%s: %v\nchild log: %s", n.stage, n.steps, p.id, p.incarnation, op, err, tail)
}

func (n *processCluster) stop(p *processChild) {
	n.t.Helper()
	if p.stopped {
		return
	}
	p.stopped = true
	// Kill only the exact process this harness started; never a name/pid scan.
	err := p.cmd.Process.Kill()
	if err != nil && !errors.Is(err, os.ErrProcessDone) {
		n.t.Errorf("kill owned replica%d: %v", p.id, err)
	}
	p.cancel()
	p.in.Close()
	p.out.Close()
	select {
	case <-p.done: // Wait reaps the child even when it exited before Kill.
	case <-time.After(10 * time.Second):
		n.t.Errorf("owned replica%d did not reap within deadline", p.id)
	}
	p.log.Close()
}

func (n *processCluster) rpc(id uint64, r processRequest) processResponse {
	n.t.Helper()
	p := n.children[id]
	if p == nil || p.stopped {
		n.t.Fatalf("stage=%s RPC to stopped replica%d", n.stage, id)
	}
	p.sequence++
	r.Sequence = p.sequence
	deadline := time.Now().Add(10 * time.Second)
	if err := p.in.SetWriteDeadline(deadline); err != nil {
		n.fail(p, r.Op, err)
	}
	if err := p.out.SetReadDeadline(deadline); err != nil {
		n.fail(p, r.Op, err)
	}
	if err := processWrite(p.in, r); err != nil {
		n.fail(p, r.Op, err)
	}
	var out processResponse
	if err := processRead(p.out, &out); err != nil {
		n.fail(p, r.Op, err)
	}
	if out.Source != id || out.Incarnation != p.incarnation || out.Sequence != r.Sequence {
		n.fail(p, r.Op, errors.New("response source/incarnation/request mismatch"))
	}
	return out
}

func (n *processCluster) run(id uint64, r processRequest) processResponse {
	n.t.Helper()
	out := n.rpc(id, r)
	if err := processErr(out.Err); err != nil {
		n.fail(n.children[id], r.Op, err)
	}
	n.deliver(id, out)
	return out
}

func (n *processCluster) deliver(id uint64, out processResponse) {
	n.t.Helper()
	n.replies[id] = append(n.replies[id], out.Evidence...)
	queue := out.Packets
	if len(queue) > 0 {
		queue = append(queue, queue[0]) // duplicate one immutable frame per event
	}
	bytesQueued := 0
	for _, p := range queue {
		bytesQueued += len(p.Payload)
	}
	for len(queue) > 0 {
		if len(queue) > 4096 || bytesQueued > 16<<20 || n.steps >= 50000 {
			n.t.Fatalf("stage=%s bounded transport exhausted", n.stage)
		}
		n.steps++
		// Reverse each batch deterministically. No wall-clock scheduling proof.
		j := len(queue) - 1
		p := queue[j]
		queue = queue[:j]
		bytesQueued -= len(p.Payload)
		if p.From < 1 || p.From > 6 || p.To < 1 || p.To > 6 || (p.From-1)/3 != (p.To-1)/3 {
			n.t.Fatal("invalid consensus transport source/group")
		}
		if n.drop[p.From] || n.drop[p.To] || n.children[p.To].stopped {
			continue
		}
		e := n.rpc(p.To, processRequest{Op: "step", Packet: p})
		if err := processErr(e.Err); err != nil {
			n.fail(n.children[p.To], "step", err)
		}
		n.replies[p.To] = append(n.replies[p.To], e.Evidence...)
		queue = append(queue, e.Packets...)
		for _, next := range e.Packets {
			bytesQueued += len(next.Payload)
		}
	}
}

func (n *processCluster) build(id uint64, r processRequest) *processProposal {
	n.t.Helper()
	out := n.run(id, r)
	p := out.Proposal
	if p == nil || p.Source != id || p.Incarnation != n.children[id].incarnation || p.Sequence != out.Sequence || p.Hash != sha256.Sum256(p.Bytes) {
		n.t.Fatalf("stage=%s invalid source-origin proposal", n.stage)
	}
	var handles []string
	if r.Handle != "" {
		handles = append(handles, r.Handle)
	}
	if r.Prior != "" {
		handles = append(handles, r.Prior)
	}
	if !reflect.DeepEqual(p.Handles, handles) {
		n.t.Fatal("proposal does not bind the requested completed-read handles")
	}
	return p
}

func (n *processCluster) submit(id uint64, p *processProposal) {
	n.t.Helper()
	n.run(id, processRequest{Op: "submit", Proposal: p})
}

func (n *processCluster) readResult(id uint64, q Query) (processEvidence, error) {
	n.t.Helper()
	n.replies[id] = nil
	out := n.rpc(id, processRequest{Op: "read", Query: q})
	if err := processErr(out.Err); err != nil {
		return processEvidence{}, err
	}
	n.deliver(id, out)
	for _, p := range n.replies[id] {
		if p.Query == q && p.Sequence == out.Sequence {
			if err := processErr(p.Err); err != nil {
				return p, err
			}
			if p.Handle != fmt.Sprintf("%d/%d/%d", id, n.children[id].incarnation, out.Sequence) || p.View.Group != uint8((id-1)/3) || p.View.Index == 0 {
				n.t.Fatal("read proof handle/request/source mismatch")
			}
			return p, nil
		}
	}
	return processEvidence{}, ErrUnavailable // controller pending, NOT Host rejection
}

func (n *processCluster) read(id uint64, q Query) processEvidence {
	n.t.Helper()
	p, err := n.readResult(id, q)
	if err != nil {
		n.fail(n.children[id], "authoritative read", err)
	}
	return p
}

func (n *processCluster) elect(id uint64) {
	n.t.Helper()
	base := ((id-1)/3)*3 + 1
	for range 80 {
		for peer := base; peer < base+3; peer++ {
			if !n.drop[peer] && !n.children[peer].stopped {
				n.run(peer, processRequest{Op: "tick"})
			}
		}
		for peer := base; peer < base+3; peer++ {
			if n.drop[peer] || n.children[peer].stopped {
				continue
			}
			_, err := n.readResult(peer, Query{Kind: Collection})
			if errors.Is(err, replica.ErrUnavailable) || errors.Is(err, ErrUnavailable) {
				continue
			}
			if err != nil {
				n.fail(n.children[peer], "election read", err)
			}
			if peer == id {
				return
			}
			n.run(peer, processRequest{Op: "transfer", Target: id})
		}
	}
	n.t.Fatalf("stage=%s election to replica%d exhausted", n.stage, id)
}

func (n *processCluster) crashReopen(id uint64) {
	n.t.Helper()
	old := n.children[id]
	n.stop(old)
	n.start(id, false)
	if n.children[id].incarnation == old.incarnation {
		n.t.Fatal("restart reused process incarnation")
	}
	n.elect(id)
}

func (n *processCluster) registration(x Tx) processEvidence {
	n.submit(1, n.build(1, processRequest{Op: "register", Tx: x}))
	return n.read(1, Query{Kind: Registration, TxID: x.ID})
}

func (n *processCluster) prepareA(x Tx) processEvidence {
	r := n.read(1, Query{Kind: Registration, TxID: x.ID})
	n.submit(1, n.build(1, processRequest{Op: "prepare", Handle: r.Handle}))
	return n.read(1, Query{Kind: Prepared, TxID: x.ID})
}

func (n *processCluster) prepareB(x Tx) processEvidence {
	r := n.read(1, Query{Kind: Registration, TxID: x.ID})
	a := n.read(1, Query{Kind: Prepared, TxID: x.ID})
	n.submit(4, n.build(1, processRequest{Op: "prepare", Handle: r.Handle, Prior: a.Handle}))
	return n.read(4, Query{Kind: Prepared, TxID: x.ID})
}

func (n *processCluster) decision(x Tx) processEvidence {
	for _, id := range []uint64{1, 4} {
		p := n.read(id, Query{Kind: Prepared, TxID: x.ID})
		if p.View.Vote == nil || !p.View.Vote.Yes {
			n.t.Fatal("expected YES", p.View)
		}
		n.submit(1, n.build(id, processRequest{Op: "vote", Handle: p.Handle}))
	}
	r := n.read(1, Query{Kind: Registration, TxID: x.ID})
	n.submit(1, n.build(1, processRequest{Op: "decide", Handle: r.Handle}))
	return n.read(1, Query{Kind: Decided, TxID: x.ID})
}

func (n *processCluster) resolve(x Tx, id uint64) {
	d := n.read(1, Query{Kind: Decided, TxID: x.ID})
	n.submit(id, n.build(1, processRequest{Op: "resolve", Handle: d.Handle}))
}

func (n *processCluster) certify(round uint64) [2]processEvidence {
	n.t.Helper()
	var ps [2]processEvidence
	for g, id := range []uint64{1, 4} {
		n.submit(id, n.build(id, processRequest{Op: "fence", Round: round}))
		n.submit(id, n.build(id, processRequest{Op: "certify", Round: round}))
		ps[g] = n.read(id, Query{Kind: Certified, Round: round})
	}
	return ps
}

func (n *processCluster) compare(ps [2]processEvidence, history []whole, round uint64) {
	n.t.Helper()
	if _, ok := serialOrder(history); !ok {
		n.t.Fatal("independent model rejected committed serial history")
	}
	want := modelAt(history, round)
	for g, id := range []uint64{1, 4} {
		p := ps[g]
		if p.View.Kind != Certified || p.View.Group != uint8(g) || p.View.Certificate == nil || p.View.Certificate.Round != round {
			n.t.Fatal("paired scope certificate mismatch", p.View)
		}
		out := n.run(id, processRequest{Op: "at", Handle: p.Handle})
		if out.State == nil || !reflect.DeepEqual(out.State.Values, want[g]) {
			n.t.Fatalf("stage=%s group%d cut%d complete effects: got%+v want%v", n.stage, g, round, out.State, want[g])
		}
	}
}

func processEdge() Tx {
	return tx("process-edge", 0,
		participant(0, 0, Effect{Key: "edge/e", Value: 5}, Effect{Key: "out/e", Value: 5}, Effect{Key: "scalar/a", Value: 7}),
		participant(1, 0, Effect{Key: "in/e", Value: 5}, Effect{Key: "scalar/b", Value: 9}))
}

// Each subtest has exactly TWO logical groups x THREE separate disk/process
// replicas. No extra voter, server binary or controller-owned application state.
func TestTwoGroupsSixProcessesDurableBoundaryRecovery(t *testing.T) {
	for _, stage := range []string{"first-prepare", "both-prepares", "decision-before-response", "A-before-B", "checkpoint-unresolved", "quorum-loss"} {
		t.Run(stage, func(t *testing.T) {
			n := newProcessCluster(t)
			n.stage = stage
			x := processEdge()
			n.registration(x)
			a := n.prepareA(x)
			if a.View.Vote == nil || !a.View.Vote.Yes {
				t.Fatal("first durable prepare was not YES")
			}
			if stage == "first-prepare" || stage == "checkpoint-unresolved" {
				if stage == "checkpoint-unresolved" {
					n.run(1, processRequest{Op: "checkpoint"})
				}
				n.crashReopen(1)
				// Handles are process-bound and must not survive an incarnation.
				bad := n.rpc(1, processRequest{Op: "vote", Handle: a.Handle})
				if !errors.Is(processErr(bad.Err), ErrUnavailable) || bad.Proposal != nil {
					t.Fatal("old process proof handle escaped", bad)
				}
				// Establish retained locking through a real conflicting prepare vote.
				conflict := tx("blocked", 0, participant(0, 0, Effect{Key: "phantom", Value: 99}))
				n.registration(conflict)
				no := n.prepareA(conflict)
				if no.View.Vote == nil || no.View.Vote.Yes || no.View.Vote.Reason != ErrRetry.Error() {
					t.Fatal("unresolved prepare lock lost across process restart", no.View)
				}
			}
			b := n.prepareB(x)
			if b.View.Vote == nil || !b.View.Vote.Yes {
				t.Fatal("second durable prepare was not YES")
			}
			if stage == "both-prepares" {
				n.crashReopen(1)
				n.crashReopen(4)
			}
			if stage == "quorum-loss" {
				// Lose exactly one voter per group, then isolate coordinator A1
				// from its remaining peer. A3/B3 are owned killed processes.
				n.stop(n.children[3])
				n.stop(n.children[6])
				// One-voter loss still permits real quorum authority in BOTH groups.
				n.read(1, Query{Kind: Registration, TxID: x.ID})
				n.read(4, Query{Kind: Prepared, TxID: x.ID})
				n.drop[2] = true
				n.replies[1] = nil
				out := n.rpc(1, processRequest{Op: "read", Query: Query{Kind: Decided, TxID: x.ID}})
				if out.Err != "" {
					t.Fatal("initial leader barrier rejected unexpectedly", out.Err)
				}
				n.deliver(1, out)
				if len(n.replies[1]) != 0 {
					t.Fatal("isolated coordinator emitted decision authority")
				}
				for range 30 {
					n.run(1, processRequest{Op: "tick"})
				}
				// This MUST be the actual Host/Driver error, not a timeout or
				// the controller's 'no reply yet' classification.
				out = n.rpc(1, processRequest{Op: "read", Query: Query{Kind: Collection}})
				if !errors.Is(processErr(out.Err), replica.ErrUnavailable) || len(out.Evidence) != 0 {
					t.Fatal("quorum loss did not explicitly reject fresh authority", out)
				}
				n.submit(4, n.build(4, processRequest{Op: "fence", Round: 1}))
				n.submit(4, n.build(4, processRequest{Op: "certify", Round: 1}))
				if _, err := n.readResult(4, Query{Kind: Certified, Round: 1}); !errors.Is(err, ErrPending) {
					t.Fatal("unresolved participant certified while coordinator unavailable", err)
				}
				delete(n.drop, 2)
				n.start(3, false)
				n.start(6, false)
				n.elect(1)
				n.elect(4)
			}
			d := n.decision(x)
			if d.View.Decision == nil || !d.View.Decision.Commit {
				t.Fatal("durable commit missing", d.View)
			}
			// The harness has withheld this result from its simulated client;
			// the child completed a quorum read so the kill boundary is durable.
			if stage == "decision-before-response" || stage == "checkpoint-unresolved" {
				if stage == "checkpoint-unresolved" {
					n.run(1, processRequest{Op: "checkpoint"})
					n.run(4, processRequest{Op: "checkpoint"})
				}
				n.crashReopen(1)
				n.crashReopen(4)
				recovered := n.read(1, Query{Kind: Decided, TxID: x.ID})
				if !reflect.DeepEqual(recovered.View.Decision, d.View.Decision) {
					t.Fatal("immutable durable decision changed after process kill")
				}
			}
			n.resolve(x, 1)
			if stage == "A-before-B" {
				// Evidence the named boundary with completed quorum reads, not
				// proposal acceptance or drained consensus traffic. Current is
				// local authoritative inspection, NEVER a global certified cut.
				wantA := modelAt([]whole{{x, d.View.Decision.Round}}, d.View.Decision.Round)[0]
				installedA := n.read(1, Query{Kind: Current}).View.State
				pendingB := n.read(4, Query{Kind: Current}).View.State
				if installedA == nil || !reflect.DeepEqual(installedA.Values, wantA) {
					t.Fatalf("A installation boundary: got%+v want%v", installedA, wantA)
				}
				if pendingB == nil || !reflect.DeepEqual(pendingB.Values, map[string]Value{}) {
					t.Fatalf("B exposed pending effects before installation: %+v", pendingB)
				}
				n.crashReopen(1)
				n.crashReopen(4)
			}
			round := d.View.Decision.Round
			// B must not certify a scope containing the half-installed edge.
			n.submit(4, n.build(4, processRequest{Op: "fence", Round: round}))
			n.submit(4, n.build(4, processRequest{Op: "certify", Round: round}))
			if _, err := n.readResult(4, Query{Kind: Certified, Round: round}); !errors.Is(err, ErrPending) {
				t.Fatal("half installation produced certified B scope", err)
			}
			n.resolve(x, 4)
			history := []whole{{x, round}}
			old := n.certify(round)
			n.compare(old, history, round)
			// Exact retained request replay: no extra versions, altered stamp or
			// postings. A changed digest is an applied-command diagnostic only.
			n.registration(x)
			replayed := n.read(1, Query{Kind: Decided, TxID: x.ID})
			if !reflect.DeepEqual(replayed.View.Decision, d.View.Decision) {
				t.Fatal("request replay changed original decision")
			}
			changed := clone(x)
			changed.Participants[0].Effects[0].Value++
			n.submit(1, n.build(1, processRequest{Op: "register", Tx: changed}))
			if err := processErr(n.rpc(1, processRequest{Op: "last"}).Err); !errors.Is(err, ErrMismatch) {
				t.Fatal("changed request digest diagnostic", err)
			}
			bound := n.read(1, Query{Kind: Registration, TxID: x.ID})
			if bound.View.Tx == nil || !reflect.DeepEqual(*bound.View.Tx, x) {
				t.Fatal("mismatched replay changed immutable registration")
			}
			n.compare(old, history, round)
			correction := tx("process-correction", 0,
				participant(0, 1, Effect{Key: "edge/e", Value: 6}, Effect{Key: "out/e", Delete: true}, Effect{Key: "out/new/e", Value: 6}, Effect{Key: "scalar/a", Value: 17}),
				participant(1, 1, Effect{Key: "in/e", Delete: true}, Effect{Key: "in/new/e", Value: 6}, Effect{Key: "scalar/b", Value: 19}))
			n.registration(correction)
			n.prepareA(correction)
			n.prepareB(correction)
			cd := n.decision(correction)
			n.resolve(correction, 1)
			n.resolve(correction, 4)
			history = append(history, whole{correction, cd.View.Decision.Round})
			fresh := n.certify(cd.View.Decision.Round)
			n.compare(fresh, history, cd.View.Decision.Round)
			n.compare(old, history, round) // mutation-then-history; no current fallback
			for _, id := range []uint64{1, 4} {
				n.run(id, processRequest{Op: "checkpoint"})
				n.crashReopen(id)
			}
			// Retained certificates are recovered by NEW request-bound reads;
			// the old proof handles themselves never cross incarnation boundaries.
			old = [2]processEvidence{n.read(1, Query{Kind: Certified, Round: round}), n.read(4, Query{Kind: Certified, Round: round})}
			n.compare(old, history, round)
			fresh = [2]processEvidence{n.read(1, Query{Kind: Certified, Round: cd.View.Decision.Round}), n.read(4, Query{Kind: Certified, Round: cd.View.Decision.Round})}
			n.compare(fresh, history, cd.View.Decision.Round)
			t.Logf("stage=%s oracle=exact serial=true retained-round=%d corrected-round=%d transport-events=%d", stage, round, cd.View.Decision.Round, n.steps)
		})
	}
}
