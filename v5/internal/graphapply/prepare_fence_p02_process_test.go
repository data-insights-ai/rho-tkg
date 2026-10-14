package graphapply

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// Finite P02 schedules use real votes, fences, elections and retained originals.
// They do not certify the complete P02/network/storage campaign family.
func TestPrepareFenceP02ActualOrderingAcrossElection(t *testing.T) {
	for _, schedule := range []string{"delayed-A-prepare", "A-before-B-after", "persisted-fence-before-application"} {
		passed := t.Run(schedule, func(t *testing.T) {
			if schedule == "persisted-fence-before-application" {
				t.Setenv("RHO_GRAPH_P02_STAGE", "1")
			}
			f := newPairedGraphFaultNetwork(t)
			f.trace.Scope = "P02 finite prepare/fence/election and delayed local proposal schedules; no complete P02 family or full V2 acceptance"
			before := f.observe(4, graphReadFresh, graphCut{}, false)
			f.trace.InitialCut = before.Cut
			base := f.indexes()
			for stage := 1; stage <= 2; stage++ {
				actor := uint64(1)
				if stage == 2 {
					actor = 4
				}
				p, from := f.proposal(stage), f.indexes()
				result := f.run(actor, p)
				step := f.complete([]string{"node-a", "node-b"}[stage-1], p, result, from, before)
				f.assertView(step.After, stage)
				pairedFaultAssertCDC(t, step, stage)
				assertLifecycleCandidates(t, step.RawObservation, stage)
				before = pairedFaultObserver(step)
			}
			oldCut, err := pairedFaultCut(before.Cut)
			if err != nil {
				t.Fatal(err)
			}
			old := p06Observe(f, 4, graphReadAt, oldCut)
			if len(oldCut.certificates) != 2 || oldCut.certificates[0].round != oldCut.certificates[1].round {
				t.Fatal("P02 incomplete genuine old cut")
			}
			bound := oldCut.certificates[0].round + 100
			proposal := f.proposal(3)
			artifact := p02FenceArtifact{Schedule: schedule, Old: old}
			from, actor := f.indexes(), uint64(4)
			var phases []p06Phase
			var captures []p06Phase
			var result crossProcessResult
			if schedule != "persisted-fence-before-application" {
				entered, release, finish := p02Phase(f, actor, schedule == "A-before-B-after")
				call, cancel := f.beginRun(actor, proposal)
				defer cancel()
				boundary := f.awaitBoundary(entered)
				original := p06SourceProposal(t, boundary.Request)
				command, _, err := decodeCrossCommand(original.wire, defaultMaterializerLimits())
				if err != nil || command.phase != crossPrepare || command.ns.partition != 3 {
					t.Fatal("P02 actual delayed Prepare boundary", err)
				}
				if schedule == "A-before-B-after" {
					_, vote, _ := p06PrepareBoundary(t, boundary, 8)
					if vote.floor >= bound {
						t.Fatal("P02 A vote was not before the new fence", vote.floor, bound)
					}
					artifact.Fences = append(artifact.Fences, p02Fence(f, 1, bound), p02Fence(f, 4, bound))
					p02ElectLiveLeader(f, 4, 5)
				} else {
					artifact.Fences = append(artifact.Fences, p02Fence(f, 1, bound))
					p02ElectLiveLeader(f, 1, 2)
				}
				release()
				var answer graphProcessAnswer
				select {
				case answer = <-call:
				case <-time.After(10 * time.Second):
					t.Fatal("P02 original did not finish after election")
				}
				pending, err := decodeGraphProcessCoordinatorResult(answer.response)
				unavailable := errors.Is(answer.err, graphstore.ErrSourceUnavailable) || errors.Is(answer.err, replica.ErrUnavailable)
				if !errors.Is(err, errCrossProcessPending) || !unavailable || pending.decision != (crossStatus{}) {
					t.Fatal("P02 original follower route must truthfully return pending", pending, err, answer.err)
				}
				artifact.PendingCaller = p06ProcessWire(t, answer.response)
				f.drain()
				phases, captures, artifact.Encounters = finish()
				if len(phases) < 2 {
					t.Fatal("P02 original delayed Prepare did not reach a genuine voter")
				}
				actor = 6
				p03Reconnect(f, actor)
				_, releaseRecovery, finishRecovery := p02Phase(f, actor, false)
				releaseRecovery()
				result = f.run(actor, proposal)
				if result.resume != pending.resume {
					t.Fatal("P02 recovery changed original qualified identity")
				}
				recovery, sources, encounters := finishRecovery()
				artifact.Encounters = append(artifact.Encounters, encounters...)
				if len(sources) != 0 {
					t.Fatal("P02 recovery replanned instead of retaining the original manifest")
				}
				phases = append(phases, recovery...)
			} else {
				// This ordinary local proposal is genuinely produced before the
				// fence. Its actual installation happens after successor recovery.
				local := p06LocalProposal(f, 1, certifiedP08Close, 1, allocationP08Words(2))
				fence := p06LocalProposal(f, 1, certifiedPrepareFence, bound, nil)
				arm, err := json.Marshal(fence.wire)
				if err != nil {
					t.Fatal(err)
				}
				armPath := filepath.Join(f.n.dir, "1", "p02-fence-arm.json")
				f.packetDrain()
				if err := os.WriteFile(armPath, arm, 0600); err != nil {
					t.Fatal(err)
				}
				baseline := f.n.all[1].call(t, processRequest{op: processStateOp}).applied
				packets, stopHold := p02HoldFenceACK(f, 1, baseline+1)
				defer stopHold()
				encoded, err := encodeCertifiedProcessProposal(fence)
				if err != nil {
					t.Fatal(err)
				}
				f.n.group(1).event(t, 1, processRequest{op: processPropose, data: encoded})
				var ack replica.Packet
				select {
				case ack = <-packets:
				case <-time.After(10 * time.Second):
					t.Fatal("P02 native fence append ACK absent")
				}
				f.packetDrain()
				call := make(chan graphProcessAnswer, 1)
				ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
				defer cancel()
				go func() {
					r, err := f.n.all[1].exchange(ctx, processRequest{op: processStep, packet: ack})
					call <- graphProcessAnswer{response: r, err: err}
				}()
				marker := p02AwaitStage(f, 1, fence)
				if marker.Index != baseline+1 {
					t.Fatal("P02 skipped genuine log predecessor", marker.Index, baseline)
				}
				artifact.Stage = &marker
				artifact.Processes = append(artifact.Processes, f.kill(1, "replica"))
				select {
				case answer := <-call:
					if answer.err == nil {
						t.Fatal("P02 killed Stage RPC unexpectedly completed", answer.response)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("P02 exact blocked RPC not retired")
				}
				stopHold()
				if err := os.Remove(armPath); err != nil {
					t.Fatal(err)
				}
				f.elect(1, 2)
				// Capture this genuine new election entry before the following
				// transaction can cross native log-reclamation boundaries.
				f.records(from)
				// Original retained fence must apply after the new election. No
				// copied marker creates its result or makes a follower certified.
				r := f.n.all[2].call(t, processRequest{op: processEntryRecord, index: marker.Index})
				cut, err := decodeGraphCutResult(r.outcome)
				if err != nil || cut.phase != graphCutFence || cut.status != graphCutApplied || cut.round != bound || cut.index != marker.Index || len(r.changes) != 0 || r.stats[statTerm] <= marker.Term {
					t.Fatal("P02 successor lost original durable fence", cut, err)
				}
				artifact.Fences = append(artifact.Fences, p06LocalRecord{3, marker.Index, fence.wire, r.outcome, r.changes})
				actor = 3
				p03Reconnect(f, actor)
				_, release, finish := p02Phase(f, actor, true)
				// Capture all phases without pausing a second operation.
				release()
				result = f.run(actor, proposal)
				phases, captures, artifact.Encounters = finish()
				if !result.decision.commit {
					t.Fatal("P02 original cross operation aborted after durable fence")
				}
				// Preserve inspected election no-op entries before replacement
				// snapshot sealing can compact the original Raft prefix.
				f.records(from)
				artifact.Processes = append(artifact.Processes, f.replace(1))
				for _, member := range []uint64{1, 2, 3} {
					original := f.n.all[member].call(t, processRequest{op: processEntryRecord, index: marker.Index})
					if !bytes.Equal(original.outcome, r.outcome) || len(original.changes) != 0 || original.stats[statCommit] < marker.Index {
						t.Fatal("P02 retained fence changed across rejoin", member)
					}
				}
				p03Reconnect(f, 4)
				// Retried fence carries the exact same source bytes and no CDC.
				duplicate := p06Submit(f, 2, fence)
				g, err := decodeGraphCutResult(duplicate.Outcome)
				if err != nil || g.round != bound || g.phase != graphCutFence || g.status != graphCutApplied || len(duplicate.CDC) != 0 {
					t.Fatal("P02 original fence replay changed floor/CDC", err)
				}
				artifact.Fences = append(artifact.Fences, duplicate)
				// Retain the ordinary local proposal until the cross operation's
				// exact CDC/export below, then apply it through the genuine Host.
				artifact.DelayedLocal = &p06LocalRecord{Proposal: local.wire}
			}
			artifact.Phases, artifact.SourceCaptures = phases, captures
			encounters, err := json.MarshalIndent(artifact, "", "  ")
			if err != nil || len(encounters) > 8<<20 {
				t.Fatal("P02 encountered phase encoding", err)
			}
			if err := os.WriteFile(filepath.Join(f.n.dir, "p02-encountered-"+schedule+".json"), append(encounters, '\n'), 0600); err != nil {
				t.Fatal(err)
			}
			manifest, votes := p02Votes(f, phases, proposal, result.decision)
			p02BindSources(t, captures, manifest, artifact.Stage)
			if schedule == "A-before-B-after" {
				if votes[0].floor >= bound || votes[1].floor < bound {
					t.Fatal("P02 actual participant ordering/floors", votes, bound)
				}
			} else if votes[0].floor < bound {
				t.Fatal("P02 delayed Prepare voted below durable fence", votes[0], bound)
			}
			// Captured installed rounds and durable read fences are separate.
			// Only these two schedules actually planned before the new fence;
			// the Stage case binds its post-fence source index above instead.
			if schedule != "persisted-fence-before-application" && manifest.parts[0].floor >= bound {
				t.Fatal("P02 proposal did not retain its genuinely earlier captured installed floor", manifest.parts[0].floor, bound)
			}
			step := p03Complete(f, actor, "edge-create", proposal, result, from, before)
			f.assertView(step.After, 3)
			pairedFaultAssertCDC(t, step, 3)
			assertLifecycleCandidates(t, step.RawObservation, 3)
			artifact.Retries = append(artifact.Retries, f.replay(actor, "edge-create", proposal, result, base))
			if artifact.DelayedLocal != nil {
				local := allocationProposal{target: proposal.target, wire: artifact.DelayedLocal.Proposal}
				r := p06SubmitLocal(f, 2, local, reasonNone)
				round := p06LocalRound(t, r)
				if round <= bound || round <= result.decision.round {
					t.Fatal("P02 stale pre-proposal local round", round, bound, result.decision.round)
				}
				p02CheckLocalCloseCDC(t, r, round)
				command, err := decodePartitionGraphCommand(local.wire, defaultMaterializerLimits())
				if err != nil {
					t.Fatal(err)
				}
				f.trace.OperationMapping = append(f.trace.OperationMapping, lifecycleOperationMap{Logical: "node-close", Coordinator: 3, Request: hex.EncodeToString(command.request.id[:]), Round: round})
				artifact.DelayedLocal = &r
			}
			artifact.Current = p06Observe(f, actor, graphReadFresh, graphCut{})
			p06AssertView(t, artifact.Current.View, true, false, artifact.DelayedLocal != nil)
			artifact.Retained = p06Observe(f, actor, graphReadAt, oldCut)
			if !reflect.DeepEqual(artifact.Retained, old) {
				t.Fatal("P02 election/fence substituted current for old cut")
			}
			artifact.Records = p06Records(f, base, f.indexes())
			artifact.Phases, artifact.SourceCaptures, artifact.Trace = phases, captures, f.trace
			cohortBase := f.indexes()
			f.stopAll()
			f.reopenAll()
			// Capture new election no-op entries before Fresh read fences can
			// trigger native Raft log reclamation. The shared ledger retains
			// only inspected originals, never inferred empty-record proofs.
			f.records(cohortBase)
			retained := p06Observe(f, 4, graphReadAt, oldCut)
			current := p06Observe(f, 4, graphReadFresh, graphCut{})
			if !reflect.DeepEqual(retained, old) || !reflect.DeepEqual(current.View, artifact.Current.View) {
				t.Fatal("P02 same-directory reopen lost old/current graph")
			}
			replayActor := uint64(4)
			if schedule == "persisted-fence-before-application" {
				replayActor = 1
			}
			artifact.Retries = append(artifact.Retries, f.replay(replayActor, "edge-create", proposal, result, cohortBase))
			if artifact.DelayedLocal != nil {
				local := allocationProposal{target: proposal.target, wire: artifact.DelayedLocal.Proposal}
				from := f.indexes()
				duplicate := p06Submit(f, 1, local)
				ns, _ := certifiedProcessNamespace(1)
				original, err := decodePartitionGraphOutcome(artifact.DelayedLocal.Outcome, ns)
				replayed, rerr := decodePartitionGraphOutcome(duplicate.Outcome, ns)
				want := original
				want.disposition = requestReplay
				if err != nil || rerr != nil || original.disposition != applied || replayed != want || duplicate.Index <= original.index || len(duplicate.CDC) != 0 {
					t.Fatal("P02 delayed local duplicate changed original/CDC", original, replayed, err, rerr)
				}
				artifact.DelayedLocalReplay = &duplicate
				for _, record := range p06Records(f, from, f.indexes()) {
					if len(record.CDC) != 0 {
						t.Fatal("P02 duplicate local emitted extra CDC")
					}
				}
			}
			postReplay := p06Observe(f, 4, graphReadFresh, graphCut{})
			if !reflect.DeepEqual(postReplay.View, artifact.Current.View) {
				t.Fatal("P02 duplicate originals changed complete graph/postings/census")
			}
			wire, err := json.MarshalIndent(artifact, "", "  ")
			if err != nil || len(wire) > 8<<20 {
				t.Fatal("P02 finite trace encoding", err)
			}
			path := filepath.Join(f.n.dir, "p02-"+schedule+".json")
			if err := os.WriteFile(path, append(wire, '\n'), 0600); err != nil {
				t.Fatal(err)
			}
			t.Log("P02 actual original/fence/election/history artifact", path)
		})
		if !passed {
			return
		}
	}
}
