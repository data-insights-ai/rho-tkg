package graphapply

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

func TestGraphRangeMoveSixProcessesOwnInvocationCancellationResumesOriginal(t *testing.T) {
	n := newCertifiedProcessNetworkProfile(t, true)
	graphE17SubmitLocal(t, n, 1, graphE17Prepare(t, n, 1, certifiedRangeSet, 1, graphE17GrantInput(5, 7)), reasonNone)
	var baseline [2]uint64
	for i, leader := range n.leaders {
		baseline[i] = n.all[leader].call(t, processRequest{op: processStateOp}).applied
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	hub := n.all[1].mux.hub
	var once sync.Once
	var mu sync.Mutex
	captures := [2]int{}
	stopped := false
	hub.mu.Lock()
	hub.beforeRelay = func(call context.Context, origin, partition uint64, wire []byte) error {
		q, e := decodeGraphSourceRequest(wire)
		if e != nil {
			return e
		}
		if q.kind != sourceSubmit {
			return nil
		}
		p, e := decodeGraphSourceProposal(q.command)
		if e != nil {
			return e
		}
		if !isRangeMoveWire(p.wire) {
			return nil
		}
		c, e := decodeRangeMoveCommand(p.wire)
		if e != nil {
			return e
		}
		mu.Lock()
		if c.phase == rangeMoveCapture {
			if partition == 3 {
				captures[0]++
			} else {
				captures[1]++
			}
		}
		mu.Unlock()
		if origin == 1 && partition == 8 && c.phase == rangeMoveImport && c.sequence == 3 {
			intercepted := false
			once.Do(func() { mu.Lock(); stopped = true; mu.Unlock(); intercepted = true; cancel() })
			if intercepted {
				<-call.Done()
				return call.Err()
			}
		}
		return nil
	}
	hub.mu.Unlock()
	defer func() { hub.mu.Lock(); hub.beforeRelay = nil; hub.mu.Unlock() }()
	attempt := graphE17MoveExchangeContext(t, ctx, n.all[1], processRequest{op: processRangeRun, entity: 1, index: 8, data: graphE17MoveBounds(n.e17MovePages, 8, 16<<10)})
	pending, err := decodeGraphE17MoveResult(attempt.response)
	if !errors.Is(attempt.interruption, context.Canceled) || !errors.Is(err, errRangeMovePending) || !errors.Is(err, context.Canceled) || pending.committed || pending.decision != (rangeMoveStatus{}) {
		t.Fatal("own canceled invocation is not exact terminal Pending", attempt.interruption, pending, err)
	}
	graphE17AssertPendingEnvelope(t, attempt.response, pending)
	mu.Lock()
	initial, hit := captures, stopped
	mu.Unlock()
	if !hit || initial != ([2]int{1, 1}) || pending.target.phase != rangeMoveImport || pending.target.sequence != 2 {
		t.Fatal("actual durable cancellation seam", hit, initial, pending)
	}
	graphE17CrashDrain(t, n)
	var frozen [6]graphE17NativeProgress
	for actor := uint64(1); actor <= 6; actor++ {
		frozen[actor-1] = graphE17ReadNativeProgress(t, n, actor, pending.resume.move)
	}
	_, completed, err := graphE17CompleteMoveResponse(t, n, attempt)
	if err != nil || !completed.committed || completed.resume != pending.resume || completed.source.phase != rangeMoveFinish || completed.target.phase != rangeMoveFinish {
		t.Fatal("original Resume after cancellation failed", completed, err)
	}
	graphE17AssertNativeMoveReceipts(t, n, completed)
	intent, e := decodeRangeMoveIntent(frozen[0].intent)
	if e != nil {
		t.Fatal(e)
	}
	placementCDC, e := boundedEncoding(2048, func(w *boundedWriter) {
		w.add([]byte{'G', 'M', 'L', 1})
		w.add(completed.resume.move[:])
		w.add(completed.resume.intent[:])
		w.u64(intent.publication.RangeID)
		w.u64(intent.source.partition)
		w.u64(intent.target.partition)
		w.u64(intent.epoch)
		w.u64(intent.epoch + 1)
		w.u64(completed.decision.round)
		w.add(completed.decision.digest[:])
	})
	if e != nil {
		t.Fatal(e)
	}
	for group, leader := range n.leaders {
		end := n.all[leader].call(t, processRequest{op: processStateOp}).applied
		if end < baseline[group] || end-baseline[group] > 128 {
			t.Fatal("move record bound")
		}
		effects := 0
		for index := baseline[group] + 1; index <= end; index++ {
			original := n.all[leader].call(t, processRequest{op: processEntryRecord, index: index})
			if len(original.changes) != 0 {
				status, err := decodeRangeMoveStatus(original.outcome)
				if err != nil || status.phase != rangeMoveActivate || status.move != completed.resume.move || !bytes.Equal(original.changes, placementCDC) {
					t.Fatal("canceled/Resume move changed business or placement CDC", leader, index, err)
				}
				effects++
			}
			for id, p := range n.groups[group].nodes {
				r := p.call(t, processRequest{op: processEntryRecord, index: index})
				if !bytes.Equal(r.outcome, original.outcome) || !bytes.Equal(r.changes, original.changes) {
					t.Fatal("cancellation original outcome/CDC voter mismatch", id, index)
				}
			}
		}
		if effects != 1 {
			t.Fatal("canceled Resume duplicate/missing placement effect", group, effects)
		}
	}
	handle, _ := graphE17Open(t, n, 2, graphReadFresh, graphCut{})
	input, e := encodeGraphProcessObserveIDs([]uint64{1})
	if e != nil {
		t.Fatal(e)
	}
	got := n.all[2].call(t, processRequest{op: processRangeCertifiedDetail, index: handle, data: input})
	facts, e := decodeGraphE17Facts(got.aux)
	n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
	if e != nil || len(facts) != 1 || facts[0].valueID != 5 || facts[0].value != 7 || facts[0].provenance != 23 || !facts[0].fact.Active || facts[0].fact.Life != 2 {
		t.Fatal("canceled move lost original graph content", facts, e)
	}
	mu.Lock()
	final := captures
	mu.Unlock()
	if final != initial {
		t.Fatal("cancellation Resume recaptured", initial, final)
	}
	graphE17CrashDrain(t, n)
	for actor := uint64(1); actor <= 6; actor++ {
		p := graphE17ReadNativeProgress(t, n, actor, completed.resume.move)
		if !bytes.Equal(p.intent, frozen[actor-1].intent) || !bytes.Equal(p.capture, frozen[actor-1].capture) {
			t.Fatal("cancellation replaced original immutable controls", actor)
		}
		s := n.all[actor].call(t, processRequest{op: processStateOp})
		if s.stats[statViews] != 0 || s.stats[statPinnedBytes] != 0 || len(s.handles) != 2 || s.handles[0] != 0 || s.handles[1] != 0 {
			t.Fatal("canceled invocation leaked owners", actor, s.stats, s.handles)
		}
	}
}

// Pure private envelopes are inert; malformed/error masks never create a
// completion or authority. Positive wire data comes from the native fixture.
func TestGraphRangeMoveAttemptRejectsUnrelatedPendingCauses(t *testing.T) {
	for _, cause := range []error{errInvalid, errCorrupt, errRemoteOperationFailure, raftlog.ErrInvalid, raftlog.ErrCorrupt, raftlog.ErrClosed, raftlog.ErrPoisoned, replica.ErrUnavailable, replica.ErrStopped} {
		if graphE17PermittedResume(errors.Join(errRangeMovePending, context.Canceled, cause), context.Canceled) {
			t.Fatal("unrelated cause became resumable", cause)
		}
	}
	if graphE17PermittedResume(errRangeMovePending, nil) || graphE17PermittedResume(errors.Join(errRangeMovePending, context.Canceled), nil) {
		t.Fatal("remote-only cancellation became own invocation")
	}
	for _, cause := range []error{errLimit, graphstate.ErrResourceLimit} {
		if !graphE17PermittedResume(errors.Join(errRangeMovePending, cause), nil) {
			t.Fatal("resource Pending lost", cause)
		}
	}
}

func graphE17AssertPendingEnvelope(t *testing.T, response processResponse, original rangeMoveResult) {
	t.Helper()
	for size := range len(response.aux) {
		_, e := decodeGraphE17MoveResult(processResponse{aux: response.aux[:size]})
		if !errors.Is(e, errCorrupt) {
			t.Fatal("truncated private move result", size, e)
		}
	}
	old := bytes.Clone(response.aux)
	copy(old[:4], []byte("GMR1"))
	if _, e := decodeGraphE17MoveResult(processResponse{aux: old}); !errors.Is(e, errCorrupt) {
		t.Fatal("unversioned private frame admitted", e)
	}
	unknown := bytes.Clone(response.aux)
	unknown[len(unknown)-2] |= 1 << 5
	_, e := decodeGraphE17MoveResult(processResponse{aux: unknown})
	if !errors.Is(e, errRemoteOperationFailure) || graphE17PermittedResume(e, context.Canceled) {
		t.Fatal("unknown joined cancellation resumed", e)
	}
	if graphE17OriginalResume(original.resume, original.resume) != nil {
		t.Fatal("actual original identity refused")
	}
	foreign := original.resume
	foreign.intent[0] ^= 1
	if e := graphE17OriginalResume(foreign, original.resume); !errors.Is(e, errInvalid) {
		t.Fatal("foreign pending identity resumed", e)
	}
	if e := graphE17OriginalResume(rangeMoveResume{}, original.resume); !errors.Is(e, errInvalid) {
		t.Fatal("missing terminal pending identity resumed", e)
	}
}
