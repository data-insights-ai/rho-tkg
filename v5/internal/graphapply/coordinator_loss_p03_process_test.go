package graphapply

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// P03 loses a real coordinator leader, rather than killing the submitting
// client. The original client remains alive and receives its own pending reply.
func TestNativeCoordinatorLeaderLossAfterDurableCommitBeforeCallerReply(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, cell := range []struct {
		name                                       string
		partition, leader, caller, next, recoverer uint64
	}{
		{"A", 3, 1, 2, 2, 3},
		{"B", 8, 4, 5, 5, 6},
	} {
		t.Run(cell.name, func(t *testing.T) {
			f := newPairedGraphFaultNetwork(t)
			f.trace.Scope = "P03 actual coordinator leader SIGKILL after durable COMMIT before caller reply; six paired operations; no four-case pair, cost, power-loss or full V2 acceptance"
			before := f.observe(4, graphReadFresh, graphCut{}, false)
			f.assertView(before.View, 0)
			f.trace.InitialCut = before.Cut
			base := f.indexes()
			names := []string{"node-a", "node-b", "edge-create", "precommit-update", "postcommit-update", "zone-loss-update"}
			proposals := make([]allocationProposal, 6)
			results := make([]crossProcessResult, 6)
			for stage := 1; stage <= 4; stage++ {
				p := f.proposal(stage)
				from := f.indexes()
				actor := uint64(4)
				if stage == 1 {
					actor = 1
				}
				result := f.run(actor, p)
				step := f.complete(names[stage-1], p, result, from, before)
				f.assertView(step.After, stage)
				pairedFaultAssertCDC(t, step, stage)
				assertLifecycleCandidates(t, step.RawObservation, stage)
				proposals[stage-1], results[stage-1] = p, result
				before = pairedFaultObserver(step)
			}
			// Source1's recipient handle is a live owner, not restart metadata.
			// Both later commands are genuinely prepared before it can die.
			proposals[4], proposals[5] = f.proposal(5), f.proposal(6)
			future, err := decodePartitionGraphCommand(proposals[5].wire, defaultMaterializerLimits())
			if err != nil || future.request.kind != graphOperations || len(future.request.claims) != 0 {
				t.Fatal("P03 operation6 must be ordinary business, not a captured guard or new allocation", err)
			}
			from := f.indexes()
			entered, release := p03GateCommittedReply(f, cell.caller, cell.partition)
			defer release()
			original := f.n.all[cell.caller]
			call, cancel := f.beginRun(cell.caller, proposals[4])
			defer cancel()
			boundary := f.awaitBoundary(entered)
			decision, scope, committed := p03CommittedDecision(f, boundary, proposals[4], cell.leader, cell.partition)
			command, err := decodePartitionGraphCommand(proposals[4].wire, defaultMaterializerLimits())
			if err != nil {
				t.Fatal(err)
			}
			// All three voters were independently inspected before the leader dies.
			killed := f.kill(cell.leader, "replica")
			cancel()
			var answer graphProcessAnswer
			select {
			case answer = <-call:
			case <-time.After(10 * time.Second):
				t.Fatal("P03 original caller cancellation did not retire its actual worker")
			}
			pending, err := decodeGraphProcessCoordinatorResult(answer.response)
			if !errors.Is(answer.err, context.Canceled) || !errors.Is(err, errCrossProcessPending) ||
				answer.response.incarnation != original.incarnation || answer.response.sequence == 0 ||
				answer.response.code == 0 || pending.decision != (crossStatus{}) ||
				pending.resume.scope != scope || pending.resume.request != command.request.id ||
				pending.resume.tx != decision.tx || pending.resume.business != p03BusinessDigest(proposals[4], decision.ns, t) {
				t.Fatal("P03 missing correlated original pending identity", answer.err, err, pending, answer.response.sequence)
			}
			callerRaw, err := encodeProcessResponse(answer.response)
			if err != nil {
				t.Fatal(err)
			}
			release()
			f.drain()
			if f.n.all[cell.caller] != original || original.closed || original.cmd.ProcessState != nil {
				t.Fatal("P03 substituted a new submitting client")
			}
			f.elect(cell.leader, cell.next)
			p03Reconnect(f, cell.recoverer)
			result := f.run(cell.recoverer, proposals[4])
			if result.decision != decision || result.resume != pending.resume {
				t.Fatal("P03 durable COMMIT became a new decision/identity", decision, result, pending)
			}
			f.mu.Lock()
			down := f.down[cell.leader]
			f.mu.Unlock()
			if !down || f.n.all[cell.leader].cmd.ProcessState == nil {
				t.Fatal("P03 recovery only completed after coordinator rejoin")
			}
			p03AssertDecisionRecords(f, decision, boundary, false)
			step := p03Complete(f, cell.recoverer, names[4], proposals[4], result, from, before)
			f.assertView(step.After, 5)
			pairedFaultAssertCDC(t, step, 5)
			assertLifecycleCandidates(t, step.RawObservation, 3)
			p03OldRead(f, cell.recoverer, 3, "while-coordinator-down")
			p03OldRead(f, cell.recoverer, 4, "while-coordinator-down")
			retry := f.replay(cell.recoverer, names[4], proposals[4], result, base)
			fault := pairedFaultCase{
				Case: "coordinator-leader-loss-after-commit", Operation: names[4],
				Marker:    "actual coordinator" + cell.name + " Decide retained on all voters before SIGKILL; original live caller pending",
				MarkerRaw: boundary.Reply, CallerClassification: "pending", CallerRaw: callerRaw,
				OriginalRequest: step.OriginalRequest, Observer: pairedFaultObserver(step), Retry: &retry,
				ProcessEvidence: []pairedFaultProcess{committed, killed},
			}
			rejoined := f.replace(cell.leader)
			p03AssertDecisionRecords(f, decision, boundary, true)
			fault.ProcessEvidence = append(fault.ProcessEvidence, rejoined)
			rejoinRetry := f.replay(cell.recoverer, names[4], proposals[4], result, base)
			fault.ReopenRetries = append(fault.ReopenRetries, rejoinRetry)
			f.trace.Faults = append(f.trace.Faults, fault)
			results[4] = result
			before = pairedFaultObserver(step)
			// Keep the final ordinary operation on B. Fresh origin6 can bind A2
			// in the A case without resetting origin4's authenticated connection.
			p03Reconnect(f, 6)
			from = f.indexes()
			results[5] = f.run(6, proposals[5])
			last := p03Complete(f, 6, names[5], proposals[5], results[5], from, before)
			f.assertView(last.After, 6)
			pairedFaultAssertCDC(t, last, 6)
			assertLifecycleCandidates(t, last.RawObservation, 3)

			stops, reopens := f.stopAll(), f.reopenAll()
			final := f.observe(4, graphReadFresh, graphCut{}, false)
			f.assertView(final.View, 6)
			cohort := pairedFaultCase{Case: "same-volume-reopen", Operation: names[5],
				Marker:    "all six actual voters stopped and reaped before same-directory reopen",
				MarkerRaw: stops[5].Raw, CallerClassification: "observer-only", CallerRaw: bytes.Clone(f.lastObservationRaw),
				OriginalRequest: last.OriginalRequest, Observer: final, ProcessEvidence: append(stops, reopens...)}
			for stage := 1; stage <= 5; stage++ {
				p03OldRead(f, 4, stage, "after-cohort-reopen")
			}
			for index, p := range proposals {
				actor := uint64(4)
				if index == 0 || index == 4 && cell.partition == 3 {
					actor = 1
				}
				cohort.ReopenRetries = append(cohort.ReopenRetries, f.replay(actor, names[index], p, results[index], base))
			}
			f.trace.Faults = append(f.trace.Faults, cohort)
			inventory := f.inventory(base)
			var effects int
			for _, s := range f.trace.Steps {
				effects += len(s.LogicalCDC)
			}
			if effects != 7 || len(f.trace.Steps) != 6 {
				t.Fatal("P03 incomplete actual six-operation CDC", effects, len(f.trace.Steps))
			}
			for id, child := range f.n.all {
				s := child.call(t, processRequest{op: processStateOp})
				if s.stats[statViews] != 0 || s.stats[statPinnedBytes] != 0 ||
					len(s.handles) != 2 || s.handles[0] != 0 || s.handles[1] != 0 {
					t.Fatal("P03 native read/worker owner leaked", id, s.stats, s.handles)
				}
			}
			f.trace.RawCounts["applicationRecords"] = len(inventory)
			f.trace.RawCounts["componentChanges"] = effects
			f.trace.RawCounts["certifiedReadOperations"] = f.reads
			f.trace.RawCounts["snapshotPageOperations"] = f.snapshotPages
			f.trace.RawCounts["coordinatorPartition"] = cell.partition
			f.trace.RawCounts["completeRawRecords"] = inventory
			wire, err := json.MarshalIndent(f.trace, "", "  ")
			if err != nil || len(wire) > 8<<20 {
				t.Fatal("P03 complete trace encoding/bound", err, len(wire))
			}
			path := filepath.Join(f.n.dir, "native-p03-coordinator-"+cell.name+"-trace.json")
			if err := os.WriteFile(path, append(wire, '\n'), 0o600); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(final.View, last.After) {
				t.Fatal("P03 cohort reopen changed current graph")
			}
			t.Log("P03 actual coordinator fault trace", path)
		})
	}
}
