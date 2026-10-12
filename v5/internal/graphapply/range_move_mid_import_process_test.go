package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Peer identities stay fixed. Replacement occurs at a drained boundary and all
// live lookup paths share this lock; no authenticated association is invented.
func (h *graphProcessHub) peer(id uint64) *graphProcessMux {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.peers[id]
}

func graphE17ReplaceCrashedMux(t *testing.T, n *certifiedProcessNetwork, replacement *graphProcess) {
	t.Helper()
	hub, prior := n.all[1].mux.hub, n.all[6]
	if !prior.closed || prior.cmd.ProcessState == nil || replacement.mux != nil {
		t.Fatal("replacement lacks reaped process boundary")
	}
	mux := &graphProcessMux{p: replacement, id: 6, hub: hub, pending: make(map[uint64]chan graphProcessAnswer), relays: make(map[uint64]context.CancelFunc), relayQueue: make(chan graphProcessRelayJob, processSourceWorkers)}
	hub.mu.Lock()
	if hub.inFlight != 0 || hub.packetBytes != 0 || len(hub.packets) != 0 || hub.peers[6] != prior.mux {
		hub.mu.Unlock()
		t.Fatal("replacement during active process transport")
	}
	hub.peers[6] = mux
	hub.mu.Unlock()
	replacement.mux = mux
	n.all[6], n.groups[1].nodes[6] = replacement, replacement
	n.retired = append(n.retired, prior)
	hub.wg.Go(mux.readLoop)
	for range processSourceWorkers {
		hub.wg.Go(mux.relayLoop)
	}
}

// Inert inspection of real persisted native records, never phase authority.
type graphE17NativeProgress struct {
	status                    rangeMoveStatus
	intent, capture, progress []byte
}

func graphE17NativeMoveProgress(ctx context.Context, h *allocationHost, wire []byte) (body []byte, err error) {
	if h == nil || !h.machine.rangeMoves() || len(wire) != 16 {
		return nil, errInvalid
	}
	var move [16]byte
	copy(move[:], wire)
	if move == ([16]byte{}) {
		return nil, errInvalid
	}
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, view.Close()) }()
	statusRow, found, err := view.Get(ctx, rangeMoveStatusKey(h.machine.ns, move), 1024)
	if err != nil || !found || statusRow.Deleted {
		return nil, errors.Join(errCorrupt, err)
	}
	status, err := decodeRangeMoveStatus(statusRow.Value)
	if err != nil || status.move != move || status.ns != h.machine.ns {
		return nil, errors.Join(errCorrupt, err)
	}
	intent, found, _, err := view.GetControl(ctx, rangeMoveControlKey(h.machine.ns, move, 'I'), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil || !found || sha256.Sum256(intent.Value) != status.intent {
		return nil, errors.Join(errCorrupt, err)
	}
	capture, found, _, err := view.GetControl(ctx, rangeMoveControlKey(h.machine.ns, move, 'C'), raftlog.ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil || !found {
		return nil, errors.Join(errCorrupt, err)
	}
	progress, found, err := view.Get(ctx, rangeMoveProgressKey(h.machine.ns, move), 1<<20)
	if err != nil || !found || progress.Deleted {
		return nil, errors.Join(errCorrupt, err)
	}
	return processEncoding(func(w *boundedWriter) {
		w.field(statusRow.Value)
		w.field(intent.Value)
		w.field(capture.Value)
		w.field(progress.Value)
	})
}

func graphE17ReadNativeProgress(t *testing.T, n *certifiedProcessNetwork, actor uint64, move [16]byte) graphE17NativeProgress {
	t.Helper()
	response := n.all[actor].call(t, processRequest{op: processRangeProgressProbe, data: move[:]})
	c := graphCursor{b: response.aux}
	statusWire := c.field(256)
	p := graphE17NativeProgress{intent: bytes.Clone(c.field(1024)), capture: bytes.Clone(c.field(1024)), progress: bytes.Clone(c.field(1 << 20))}
	var err error
	p.status, err = decodeRangeMoveStatus(statusWire)
	if err != nil || c.err != nil || len(c.b) != 0 || p.status.move != move || sha256.Sum256(p.intent) != p.status.intent {
		t.Fatal("native original progress inspection", err, c.err)
	}
	return p
}

func graphE17DecodeNativeProgress(t *testing.T, p graphE17NativeProgress) rangeMoveProgress {
	t.Helper()
	output, err := graphstate.NewOutputBudget(16 << 20)
	if err != nil {
		t.Fatal(err)
	}
	progress, err := decodeRangeMoveProgress(p.progress, output)
	if err != nil {
		t.Fatal("persisted inert progress", err)
	}
	return progress
}

func graphE17CrashDrain(t *testing.T, n *certifiedProcessNetwork) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := drainGraphProcessMuxes(ctx, n.all); err != nil {
		t.Fatal("bounded process drain", err)
	}
}

func graphE17KillTargetFollower(t *testing.T, p *graphProcess) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	err := p.shutdown(ctx, true)
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatal("deadline is not process-crash evidence", err)
	}
	if _, found := errors.AsType[*exec.ExitError](err); !found || p.cmd.ProcessState == nil {
		t.Fatal("missing actual killed-child reap", err)
	}
	status, found := p.cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !found || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("not actual SIGKILL", p.cmd.ProcessState)
	}
	if err := p.cmd.Process.Signal(syscall.Signal(0)); !errors.Is(err, os.ErrProcessDone) {
		t.Fatal("killed child remains live", err)
	}
	if _, err := p.stdin.Write([]byte{1}); !errors.Is(err, os.ErrClosed) {
		t.Fatal("killed child stdin leak", err)
	}
	if _, err := p.stdout.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
		t.Fatal("killed child stdout leak", err)
	}
	for {
		p.mux.mu.Lock()
		failed, pending, relays := p.mux.err, len(p.mux.pending), len(p.mux.relays)
		p.mux.mu.Unlock()
		if failed != nil {
			if pending != 0 || relays != 0 {
				t.Fatal("dead pipe retained correlations", pending, relays)
			}
			break
		}
		select {
		case <-p.mux.hub.idle:
		case <-ctx.Done():
			t.Fatal("actual dead pipe reader did not retire", ctx.Err())
		}
	}
	t.Logf("mid-import target voter SIGKILL: pid=%d reaped=true signal=%s", p.cmd.Process.Pid, status.Signal())
}

// Scope: actual target voter durable Restore during staged transfer.
// Leaders/coordinator1 and4 remain live with their original authenticated
// streams. This does not prove coordinator/leader/quorum replacement.
func TestGraphRangeMoveSixProcessesTargetVoterRestoresMidImportThenOriginalResume(t *testing.T) {
	n := newCertifiedProcessNetworkProfile(t, true)
	n.e17PublicPhases = 4
	publication, owner := graphE17Placement(t, n, 1, 1)
	otherSource, otherSourceOwner := graphE17Placement(t, n, 1, 2)
	otherTarget, otherTargetOwner := graphE17Placement(t, n, 4, 17)
	if publication.First != 1 || publication.Last != 1 || owner.Partition != 3 || owner.Epoch != 2 {
		t.Fatal("actual narrow source allocation", publication, owner)
	}
	graphE17SubmitLocal(t, n, 1, graphE17Prepare(t, n, 1, certifiedRangeSet, 1, graphE17GrantInput(5, 7)), reasonNone)
	graphE17Cross(t, n, 4, graphE17Prepare(t, n, 4, certifiedRangeSet, 17, graphE17GrantInput(19, 13)))
	graphE17Cross(t, n, 1, graphE17Prepare(t, n, 1, certifiedCreateCrossEdge, 0, nil))
	oldHandle, oldCut := graphE17Open(t, n, 2, graphReadFresh, graphCut{})
	oldFacts := graphE17Observe(t, n, 2, oldHandle)
	graphE17AssertFacts(t, oldFacts, false)
	n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: oldHandle})

	hub := n.all[1].mux.hub
	var mu sync.Mutex
	var captures [2]int
	stopped, resuming := false, false
	hub.mu.Lock()
	hub.beforeRelay = func(ctx context.Context, origin, partition uint64, wire []byte) error {
		request, err := decodeGraphSourceRequest(wire)
		if err != nil {
			return err
		}
		if request.kind != sourceSubmit {
			return nil
		}
		proposal, err := decodeGraphSourceProposal(request.command)
		if err != nil {
			return err
		}
		if !isRangeMoveWire(proposal.wire) {
			return nil
		}
		command, err := decodeRangeMoveCommand(proposal.wire)
		if err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		if command.phase == rangeMoveCapture {
			switch partition {
			case 3:
				captures[0]++
			case 8:
				captures[1]++
			default:
				return errInvalid
			}
		}
		// Preparation1 and the actual first logical record Import2 precede this
		// next source-created command. Inspection below pins all three voters.
		if origin == 1 && partition == 8 && command.phase == rangeMoveImport && command.sequence == 3 && !resuming && !stopped {
			stopped = true
			return io.EOF
		}
		return nil
	}
	hub.mu.Unlock()
	defer func() { hub.mu.Lock(); hub.beforeRelay = nil; hub.mu.Unlock() }()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	response, operationErr := n.all[1].exchange(ctx, processRequest{op: processRangeRun, entity: 1, index: 8, data: graphE17MoveBounds(n.e17MovePages, 8, 16<<10)})
	cancel()
	pending, decodeErr := decodeGraphE17MoveResult(response)
	if !errors.Is(operationErr, graphstore.ErrSourceUnavailable) || !errors.Is(decodeErr, errRangeMovePending) || pending.committed || pending.decision != (rangeMoveStatus{}) || pending.target.phase != rangeMoveImport || pending.target.sequence != 2 {
		t.Fatal("mid-import native operation lost original pending identity", pending, operationErr, decodeErr)
	}
	mu.Lock()
	initialCaptures, actuallyStopped := captures, stopped
	mu.Unlock()
	if !actuallyStopped || initialCaptures != ([2]int{1, 1}) {
		t.Fatal("actual capture/import seam not reached", actuallyStopped, initialCaptures)
	}
	graphE17CrashDrain(t, n)
	var frozen [6]graphE17NativeProgress
	for actor := uint64(1); actor <= 6; actor++ {
		frozen[actor-1] = graphE17ReadNativeProgress(t, n, actor, pending.resume.move)
		if frozen[actor-1].status.intent != pending.resume.intent {
			t.Fatal("native control belongs to another original request", actor)
		}
		first := 0
		if actor > 3 {
			first = 3
		}
		if !reflect.DeepEqual(frozen[actor-1], frozen[first]) {
			t.Fatal("original control/progress differs within voter group", actor)
		}
	}
	progress := graphE17DecodeNativeProgress(t, frozen[5])
	if frozen[5].status.phase != rangeMoveImport || frozen[5].status.sequence != 2 || progress.imported == nil || !progress.imported.PreparationComplete || progress.imported.Sequence == 0 || progress.imported.Rows == 0 || progress.imported.Complete || progress.imported.Sealed || progress.imported.Sequence+progress.imported.PreparationSteps != 2 {
		t.Fatal("kill did not follow durable nonzero logical import page", frozen[5].status, progress.imported)
	}
	graphE17CrashDrain(t, n)
	killed := n.all[6]
	before := killed.call(t, processRequest{op: processStateOp})
	graphE17KillTargetFollower(t, killed)
	replacement := startGraphProcess(t, filepath.Join(n.dir, "6"), 6, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_RANGE_PROCESS=1", "RHO_GRAPH_PROCESS_REOPEN=1")
	// Registered after the generic fallback: the network owns this live
	// replacement's successful Close/Wait even when a later assertion fails.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := replacement.shutdown(ctx, false); err != nil {
			t.Error("replacement graceful cleanup", err)
		}
	})
	graphE17ReplaceCrashedMux(t, n, replacement)
	reopened := replacement.call(t, processRequest{op: processStateOp})
	if reopened.incarnation == before.incarnation || replacement.cmd == killed.cmd || reopened.applied != before.applied || reopened.stats[statGeneration] != before.stats[statGeneration] || !bytes.Equal(reopened.image, before.image) || reopened.ready || reopened.stats[statViews] != 0 || reopened.stats[statPinnedBytes] != 0 {
		t.Fatal("new OS child did not Restore exact staged checkpoint", reopened.applied, before.applied, reopened.stats)
	}
	if restored := graphE17ReadNativeProgress(t, n, 6, pending.resume.move); !reflect.DeepEqual(restored, frozen[5]) {
		t.Fatal("voter restart changed exact original capture/intent/status/progress")
	}
	n.groups[1].event(t, 4, processRequest{op: processTick})
	graphE17CrashDrain(t, n)
	mu.Lock()
	resuming = true
	mu.Unlock()
	attempt := graphE17MoveExchange(t, n.all[1], processRequest{op: processRangeResume, data: response.aux})
	_, completed, err := graphE17CompleteMoveResponse(t, n, attempt)
	if err != nil || !completed.committed || completed.resume != pending.resume || completed.source.phase != rangeMoveFinish || completed.target.phase != rangeMoveFinish || completed.decision.phase != rangeMoveDecide || completed.decision.round == 0 {
		t.Fatal("original authenticated Resume did not complete after voter crash", completed, err)
	}
	graphE17AssertNativeMoveReceipts(t, n, completed)
	mu.Lock()
	finalCaptures := captures
	mu.Unlock()
	if finalCaptures != initialCaptures {
		t.Fatal("Resume recaptured source", initialCaptures, finalCaptures)
	}
	for actor := uint64(1); actor <= 6; actor++ {
		current := graphE17ReadNativeProgress(t, n, actor, completed.resume.move)
		if !bytes.Equal(current.intent, frozen[actor-1].intent) || !bytes.Equal(current.capture, frozen[actor-1].capture) {
			t.Fatal("restart/Resume changed exact immutable controls", actor)
		}
		if graphE17DecodeNativeProgress(t, current).descriptor != progress.descriptor {
			t.Fatal("original frozen captures/manifest replaced", actor)
		}
		p, o := graphE17Placement(t, n, actor, 1)
		if p != publication || o.Partition != 8 || o.Epoch != owner.Epoch+1 || o.Fenced {
			t.Fatal("native placement did not activate", actor, p, o)
		}
		if actor <= 3 {
			p, o = graphE17Placement(t, n, actor, 2)
			if p != otherSource || o != otherSourceOwner {
				t.Fatal("unrelated source allocation changed", actor)
			}
		} else {
			p, o = graphE17Placement(t, n, actor, 17)
			if p != otherTarget || o != otherTargetOwner {
				t.Fatal("unrelated target allocation changed", actor)
			}
		}
	}
	current, currentCut := graphE17Open(t, n, 2, graphReadFresh, graphCut{})
	currentFacts := graphE17Observe(t, n, 2, current)
	n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: current})
	graphE17AssertFacts(t, currentFacts, false)
	if !reflect.DeepEqual(currentFacts, oldFacts) {
		t.Fatal("crash/Resume changed exact original graph")
	}
	graphE17AssertNativeVoters(t, n, currentCut, processRangeNativeVoterProbe, nil)

	decision := graphE17Cross(t, n, 4, graphE17Prepare(t, n, 4, certifiedRangeSet, 1, graphE17GrantInput(20, 8)))

	changed, changedCut := graphE17Open(t, n, 2, graphReadFresh, graphCut{})
	facts := graphE17Observe(t, n, 2, changed)
	n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: changed})
	graphE17AssertFacts(t, facts, true)
	if facts[1].revision != decision.decision.round || facts[1].revision == oldFacts[1].revision || bytes.Equal(facts[1].property, oldFacts[1].property) || !bytes.Equal(facts[1].presence, oldFacts[1].presence) || !bytes.Equal(facts[1].identity, oldFacts[1].identity) {
		t.Fatal("post-crash mutation lost exact current/old revision")
	}
	for _, id := range []uint64{3, 17, 7} {
		if !reflect.DeepEqual(facts[id], oldFacts[id]) {
			t.Fatal("post-crash mutation changed unrelated fact", id)
		}
	}
	graphE17AssertNativeVoters(t, n, changedCut, processRangeNativeVoterProbe, nil)

	at, atCut := graphE17Open(t, n, 6, graphReadAt, oldCut)
	atFacts := graphE17Observe(t, n, 6, at)
	n.all[6].call(t, processRequest{op: processSourceCertifiedClose, index: at})
	graphE17AssertFacts(t, atFacts, false)
	if !reflect.DeepEqual(atFacts, oldFacts) || !reflect.DeepEqual(atCut, oldCut) {
		t.Fatal("reopened old At after crash/mutation substituted current state")
	}
	graphE17CrashDrain(t, n)
	for id, child := range n.all {
		state := child.call(t, processRequest{op: processStateOp})
		if state.stats[statViews] != 0 || state.stats[statPinnedBytes] != 0 || len(state.handles) != 2 || state.handles[0] != 0 || state.handles[1] != 0 {
			t.Fatal("completed crash/Resume retained owners/workers/views/pins", id, state.stats, state.handles)
		}
	}
	if len(n.retired) != 1 || n.retired[0] != killed || !killed.closed || killed.cmd.ProcessState == nil {
		t.Fatal("killed incarnation lost separate reap witness")
	}
	t.Log("one target voter SIGKILL/reap + same-store durable Restore; six final live children retain original successful-reap gate")
}
