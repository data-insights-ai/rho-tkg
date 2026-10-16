package graphapply

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// Four finite six-process cells. This does not accept the remaining P01-P09,
// storage/network campaigns, FoundationDB comparison or the whole V2 phase.
func TestPreparedLockP06FirstYESLeaderKillOriginalRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, coordinator := range []uint64{3, 8} {
		for _, commit := range []bool{true, false} {
			t.Run(fmt.Sprintf("coordinator%d/commit%t", coordinator, commit), func(t *testing.T) {
				f := newPairedGraphFaultNetwork(t)
				before := f.observe(4, graphReadFresh, graphCut{}, false)
				for stage := 1; stage <= 2; stage++ {
					p := f.proposal(stage)
					actor := uint64(1)
					if stage == 2 {
						actor = 4
					}
					from := f.indexes()
					result := f.run(actor, p)
					name := []string{"node-a", "node-b"}[stage-1]
					step := f.complete(name, p, result, from, before)
					assertLifecycleView(t, step.After, stage)
					assertLifecycleCDC(t, step.lifecycleStep, stage)
					before = pairedFaultObserver(step)
				}
				old, err := pairedFaultCut(before.Cut)
				if err != nil {
					t.Fatal(err)
				}
				from := f.indexes()
				original := f.proposal(3)
				actor, recoverer := uint64(3), uint64(2)
				if coordinator == 8 {
					actor, recoverer = 5, 4
				}
				gate, clearGate := p06FirstYESGate(f, actor)
				defer clearGate()
				call, cancel := f.beginRun(actor, original)
				defer cancel()
				boundary := f.awaitBoundary(gate)
				prepare, vote, manifest := p06PrepareBoundary(t, boundary, coordinator)
				p06BindBusiness(t, original, manifest)
				f.packetDrain()
				for id := uint64(1); id <= 3; id++ {
					r := f.call(f.n.all[id], processRequest{op: processEntryRecord, index: vote.index})
					if !bytes.Equal(r.outcome, p06ReplyBody(t, boundary.Reply)) || len(r.changes) != 0 {
						t.Fatal("first YES is not the complete original on every A voter", id)
					}
				}
				killed := f.kill(1, "prepared-participant-leader")
				cancel()
				var interrupted graphProcessAnswer
				select {
				case interrupted = <-call:
				case <-time.After(10 * time.Second):
					t.Fatal("original cancelled owner did not retire")
				}
				resume, pending := decodeGraphProcessCoordinatorResult(interrupted.response)
				if !errors.Is(interrupted.err, context.Canceled) || !errors.Is(pending, errCrossProcessPending) || resume.resume.tx != manifest.tx || resume.resume.request != manifest.request.id || resume.decision != (crossStatus{}) {
					t.Fatal("cancellation lacks actual original pending envelope", resume, interrupted.err, pending)
				}
				clearGate()
				f.drain()
				f.elect(1, 2)
				f.reconnect(recoverer, 2)
				// This genuine local endpoint-life mutation must remain fenced after
				// election. Its current read/plan path is covered by the coarse lock.
				blocked := p06LocalProposal(f, 2, certifiedP08Close, 1, allocationP08Words(2))
				refusal := p06SubmitLocal(f, 2, blocked, reasonPreparedConflict)
				var changed *p06LocalRecord
				if !commit {
					// B has not prepared. A real B-local write invalidates the frozen
					// B image; recovery must obtain its real NO, not invent an abort.
					p := p06LocalProposal(f, 4, certifiedP08Close, 17, allocationP08Words(18))
					r := p06SubmitLocal(f, 4, p, reasonNone)
					changed = new(r)
					f.trace.OperationMapping = append(f.trace.OperationMapping, lifecycleOperationMap{Logical: "node-b-close", Round: p06LocalRound(t, r)})
				}
				stopCapture := p06CaptureRecovery(f, recoverer, manifest.tx)
				defer stopCapture()
				r := f.call(f.n.all[recoverer], processRequest{op: processSourceCoordinate, data: original.wire})
				result, err := decodeGraphProcessCoordinatorResult(r)
				if err != nil || result.decision.phase != crossDecide || result.decision.commit != commit || result.resume.tx != manifest.tx || result.resume.request != manifest.request.id || result.decision.digest != manifest.digest {
					t.Fatal("successor did not settle retained original", result, err)
				}
				f.drain()
				captured := stopCapture()
				p06CheckRecoveryPhases(t, captured, prepare, manifest, result.decision, commit)
				f.trace.OperationMapping = append(f.trace.OperationMapping, lifecycleOperationMap{Logical: "edge-create", Coordinator: coordinator, Request: hex.EncodeToString(manifest.request.id[:]), Transaction: hex.EncodeToString(manifest.tx[:]), Round: result.decision.round})
				recovered := p06Observe(f, recoverer, graphReadFresh, graphCut{})
				p06AssertView(t, recovered.View, commit, !commit, false)
				// Native replay receives exact source-produced proposal bytes. A
				// new IPC sequence never changes the original protocol identity.
				duplicates := []p06LocalRecord{p06ReplayPhase(f, prepare, manifest.tx)}
				for _, phase := range captured {
					q := p06SourceProposal(t, phase.Request)
					command, _, err := decodeCrossCommand(q.wire, defaultMaterializerLimits())
					if err != nil {
						t.Fatal(err)
					}
					if command.phase == crossResolve {
						duplicates = append(duplicates, p06ReplayPhase(f, q, manifest.tx))
					}
				}
				if len(duplicates) != 3 {
					t.Fatal("missing exact Prepare/two Resolve duplicate deliveries")
				}
				endpoint := p06LocalProposal(f, 2, certifiedP08Close, 1, allocationP08Words(2))
				closed := p06SubmitLocal(f, 2, endpoint, reasonNone)
				f.trace.OperationMapping = append(f.trace.OperationMapping, lifecycleOperationMap{Logical: "node-close", Round: p06LocalRound(t, closed)})
				current := p06Observe(f, recoverer, graphReadFresh, graphCut{})
				p06AssertView(t, current.View, commit, !commit, true)
				retained := p06Observe(f, recoverer, graphReadAt, old)
				if !reflect.DeepEqual(retained.View, before.View) {
					t.Fatal("old C substituted current or lost graph/index state")
				}
				through := f.indexes()
				records := p06Records(f, from, through)
				p06CheckCDC(t, records, manifest, commit, changed, refusal, closed)
				reopened := f.replace(1)
				p06AssertView(t, p06Observe(f, recoverer, graphReadFresh, graphCut{}).View, commit, !commit, true)
				if got := p06Observe(f, recoverer, graphReadAt, old); !reflect.DeepEqual(got.View, before.View) {
					t.Fatal("rejoined original directory lost C")
				}
				p06WriteArtifact(t, f, p06Artifact{Version: 1, Coordinator: coordinator, Commit: commit, Original: original.wire, Manifest: prepare.wire, Pending: p06ProcessWire(t, interrupted.response), Decision: p06ProcessWire(t, r), FirstYES: boundary, Processes: []pairedFaultProcess{killed, reopened}, Refused: refusal, Changed: changed, Closed: closed, Duplicates: duplicates, Phases: captured, Before: p06Observation{Cut: before.Cut, View: before.View, Native: before.RawObservation}, Recovered: recovered, Current: current, Retained: retained, From: from, Through: through, Records: records})
			})
		}
	}
}
