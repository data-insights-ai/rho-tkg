package graphapply

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestNativeFaultFamilyTornWALRecoversActualOriginalFromDiskAndSyncedImage(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, modeled := range []bool{false, true} {
		name := "torn-wal-current-disk"
		if modeled {
			name = "torn-wal-synced-image"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv("RHO_GRAPH_NATIVE_FAULT_PROCESS", "1")
			t.Setenv("RHO_GRAPH_FAULT_FAMILY_PROCESS", "1")
			t.Setenv("RHO_GRAPH_DURABLE_MODEL", "off")
			if modeled {
				t.Setenv("RHO_GRAPH_DURABLE_MODEL", "1")
			}
			f := newPairedGraphFaultNetwork(t)
			before := nativeFaultSeed(t, f)
			old := p06Observe(f, 6, graphReadFresh, graphCut{})
			cut, err := pairedFaultCut(old.Cut)
			if err != nil {
				t.Fatal(err)
			}
			var capture nativeFaultFamilyReceipt
			if modeled {
				owner := nativeFaultFamilyCall(t, f.n.all[1], nativeFaultFamilyControl{Version: 1, Action: "inspect"})
				capture = nativeFaultFamilyCall(t, f.n.all[1], nativeFaultFamilyControl{Version: 1, Action: "model", Cookie: owner.Cookie})
			}
			proposal, from := f.proposal(3), f.indexes()
			entered, release, err := nativeFaultBeforePhase(t.Context(), f.n.all[5].mux.hub, 5, 3, crossResolve)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			call, cancel := f.beginRun(5, proposal)
			defer cancel()
			boundary := f.awaitBoundary(entered)
			command, cross, err := nativeFaultPhase(boundary.Request)
			if err != nil || !cross || command.phase != crossResolve || len(command.proofs) != 1 {
				t.Fatal("actual decided-before-install boundary", err)
			}
			arena, err := graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := decodeCrossManifest(command.manifest, defaultMaterializerLimits(), arena)
			if err != nil {
				t.Fatal(err)
			}
			p06BindBusiness(t, proposal, manifest)
			decision := f.n.all[4].call(t, processRequest{op: processEntryRecord, index: command.proofs[0].index})
			stored, err := decodeCrossStatus(decision.outcome)
			if err != nil || !stored.commit || stored.tx != manifest.tx || stored.phase != crossDecide || len(decision.changes) != 0 {
				t.Fatal("actual immutable COMMIT missing before tear", err)
			}
			f.packetDrain()
			owner := nativeFaultControlCall(t, f.n.all[1], nativeFaultControl{Version: 1, Action: "inspect"})
			armed := nativeFaultControlCall(t, f.n.all[1], nativeFaultControl{Version: 1, Action: "arm", Cookie: owner.Cookie, Sequence: 1, Kind: "torn-wal-kill", Nth: 1})
			f.mu.Lock()
			f.down[1] = true
			f.mu.Unlock()
			release()
			select {
			case answer := <-call:
				if result, err := decodeGraphProcessCoordinatorResult(answer.response); err == nil && result.decision != (crossStatus{}) {
					t.Fatal("torn installation acknowledged whole completion")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("torn caller did not retire")
			}
			cancel()
			dir := filepath.Join(f.n.dir, "1")
			receipt := readNativeFaultIO(t, dir, armed)
			if receipt.IO.Written == 0 || receipt.IO.Written != receipt.IO.Requested/2 || receipt.IO.Offset < 0 {
				t.Fatal("not a genuine torn native prefix", receipt)
			}
			failed := nativeFaultReap(t, f.n.all[1], 1, dir, receipt)
			f.mu.Lock()
			f.intentionalKills[f.n.all[1]] = true
			f.mu.Unlock()
			var model *nativeFaultFamilyModel
			if modeled {
				actual := nativeFaultFamilyInstallModel(t, f, 1, capture)
				model = &actual
			}
			f.elect(1, 2)
			p03Reconnect(f, 6)
			result := f.run(6, proposal)
			if result.decision != stored {
				t.Fatal("torn recovery changed original first decision", result.decision, stored)
			}
			step := p03Complete(f, 6, "edge-create", proposal, result, from, before)
			f.assertView(step.After, 3)
			pairedFaultAssertCDC(t, step, 3)
			assertLifecycleCandidates(t, step.RawObservation, 3)
			recovered := p06Observe(f, 6, graphReadFresh, graphCut{})
			// Recovery is explicit: a sealed image is installed only after reap.
			// The replacement then uses genuine native Open, never a fake Host.
			t.Setenv("RHO_GRAPH_DURABLE_MODEL", "off")
			var reopened p06Observation
			rejoined := f.replaceBeforeHeal(1, func(p *graphProcess, state processResponse) {
				for _, cert := range cut.certificates {
					if cert.scope.partition == 3 && cert.sourceIndex > state.applied {
						t.Fatal("native synced image lost acknowledged old source prefix")
					}
				}
				reopened = nativeP06InitialLocalOld(f, 1, cut)
				if !reflect.DeepEqual(old, reopened) {
					t.Fatal("native reopen before catch-up changed old cut")
				}
			})
			retained := p06Observe(f, 6, graphReadAt, cut)
			if !reflect.DeepEqual(old, retained) {
				t.Fatal("tear changed held historical answer")
			}
			through := f.indexes()
			writeNativeFaultFamilyArtifact(t, f, nativeFaultFamilyArtifact{nativeFaultArtifact: nativeFaultArtifact{Case: name, Original: proposal.wire, Manifest: command.manifest, Before: old, Retained: retained, Recovered: recovered, Reopened: reopened, Processes: []pairedFaultProcess{failed, rejoined}, From: from, Through: through, Records: p06Records(f, from, through), IO: &receipt}, Model: model})
		})
	}
}

func TestNativeFaultFamilyTwoConflictingOriginalsFenceAndRealTransferExploreCausalOrders(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	recorded := nativeTransitionComposedFromEnv(t)
	// This finite OS-child slice complements the genuine native-host fence-first
	// stale Register/first Prepare schedules in
	// TestRangeMoveRejectsFirstDelayedCrossPrepareAndSettlesInertLateRegistration.
	// Neither source component claims exhaustive arbitrary event permutations.
	for _, firstOriginal := range []int{0, 1} {
		for _, phase := range []string{"registered", "prepared"} {
			for _, fenceFirst := range []bool{true, false} {
				// Once A helps B to its actual ABORT, no unsettled owner remains
				// to block a later first fence. That order is not relabelled pending.
				if firstOriginal == 1 && !fenceFirst {
					continue
				}
				if !recorded.selected(firstOriginal, phase, fenceFirst) {
					continue
				}
				name := fmt.Sprintf("explore-%s-%s-fence-%t", []string{"A", "B"}[firstOriginal], phase, fenceFirst)
				t.Run(name, func(t *testing.T) {
					t.Setenv("RHO_GRAPH_FAULT_FAMILY_PROCESS", "1")
					n := newCertifiedProcessNetworkProfile(t, true)
					// The actual range profile supplies its original split grant;
					// no capacity or publication data is fabricated by this wrapper.
					f := &pairedGraphFaultNetwork{t: t, n: n}
					graphE17SubmitLocal(t, n, 1, graphE17Prepare(t, n, 1, certifiedRangeSet, 1, graphE17GrantInput(5, 7)), reasonNone)
					graphE17Cross(t, n, 4, graphE17Prepare(t, n, 4, certifiedRangeSet, 17, graphE17GrantInput(19, 13)))
					graphE17Cross(t, n, 1, graphE17Prepare(t, n, 1, certifiedCreateCrossEdge, 0, nil))
					held, oldCut := graphE17Open(t, n, 2, graphReadFresh, graphCut{})
					heldClosed := false
					defer func() {
						if !heldClosed {
							n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: held})
						}
					}()
					before, oldFacts := nativeFaultFamilyRangeReadAt(t, n, 2, held, oldCut)
					graphE17AssertFacts(t, oldFacts, false)
					placementBefore := n.all[1].call(t, processRequest{op: processRangeEvidenceProbe, entity: 1}).aux
					factory := func(partition uint64, priority byte) allocationProposal {
						r := n.all[1].call(t, processRequest{op: processCertifiedAction, entity: certifiedNativeFamilyBusiness, index: partition, data: []byte{priority}})
						p, err := decodeCertifiedProcessProposal(r.aux)
						if err != nil || p.target.semantic != rangeMoveSemanticContractID() {
							t.Fatal("genuine range-profile conflict factory", err)
						}
						return p
					}
					coordinatorA, coordinatorB, actorA, actorB := uint64(3), uint64(8), uint64(2), uint64(5)
					if firstOriginal == 1 {
						coordinatorA, coordinatorB, actorA, actorB = 8, 3, 5, 2
					}
					a, b := factory(coordinatorA, 1), factory(coordinatorB, 2)
					ac, err := decodePartitionGraphCommand(a.wire, defaultMaterializerLimits())
					if err != nil {
						t.Fatal(err)
					}
					bc, err := decodePartitionGraphCommand(b.wire, defaultMaterializerLimits())
					if err != nil {
						t.Fatal(err)
					}
					txa, txb := crossTransactionID(namespace{ac.request.ns.graph, coordinatorA}, ac.request.id), crossTransactionID(namespace{bc.request.ns.graph, coordinatorB}, bc.request.id)
					if bytes.Compare(txa[:], txb[:]) >= 0 {
						t.Fatal("declared original priority did not come from actual qualified IDs")
					}
					heldActor, heldProposal, heldTx, heldName := actorA, a, txa, "A"
					if firstOriginal == 1 {
						heldActor, heldProposal, heldTx, heldName = actorB, b, txb, "B"
					}
					from := f.indexes()
					arena, err := graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
					if err != nil {
						t.Fatal(err)
					}
					if err := arena.Reserve(4*nativeDecisionBodyBytes + 4*nativeDecisionFrameBytes); err != nil {
						t.Fatal(err)
					}
					hub := n.all[1].mux.hub
					var mu sync.Mutex
					var manifests [2][]byte
					entered, released := make(chan pairedFaultBoundary, 1), make(chan struct{})
					var once sync.Once
					capture := func(origin uint64, request []byte) (crossCommand, bool, error) {
						command, cross, err := nativeFaultPhase(request)
						if err != nil || !cross || origin != 2 && origin != 5 {
							return command, false, err
						}
						if len(command.manifest) > nativeDecisionBodyBytes {
							return command, false, errLimit
						}
						index := 0
						if len(command.manifest) < 20 {
							return command, false, errInvalid
						}
						switch {
						case bytes.Equal(command.manifest[4:20], txa[:]):
						case bytes.Equal(command.manifest[4:20], txb[:]):
							index = 1
						default:
							return command, false, errInvalid
						}
						mu.Lock()
						defer mu.Unlock()
						if manifests[index] == nil {
							manifests[index] = bytes.Clone(command.manifest)
						} else if !bytes.Equal(manifests[index], command.manifest) {
							return command, false, errInvalid
						}
						return command, true, nil
					}
					hub.mu.Lock()
					if hub.beforeRelay != nil || hub.afterRelay != nil {
						hub.mu.Unlock()
						t.Fatal("exploration relay already owned")
					}
					hub.beforeRelay = func(ctx context.Context, origin, partition uint64, request []byte) error {
						command, cross, err := capture(origin, request)
						if err != nil {
							return err
						}
						if phase == "registered" && cross && origin == heldActor && partition == 3 && command.phase == crossPrepare && bytes.Equal(command.manifest[4:20], heldTx[:]) {
							once.Do(func() { entered <- pairedFaultBoundary{Request: bytes.Clone(request)} })
							return nativeFaultWait(ctx, released)
						}
						return nil
					}
					hub.afterRelay = func(ctx context.Context, origin, partition uint64, request, reply []byte) error {
						command, cross, err := capture(origin, request)
						if err != nil {
							return err
						}
						if phase == "prepared" && cross && origin == heldActor && partition == 3 && command.phase == crossPrepare && bytes.Equal(command.manifest[4:20], heldTx[:]) {
							actual, err := decodeGraphWireResponse(reply)
							if err != nil || actual.err != nil {
								return errors.Join(err, actual.err)
							}
							vote, err := decodeCrossStatus(actual.body)
							if err != nil || !vote.yes || vote.tx != heldTx {
								return errors.Join(errInvalid, err)
							}
							once.Do(func() { entered <- pairedFaultBoundary{Request: bytes.Clone(request), Reply: bytes.Clone(reply)} })
							return nativeFaultWait(ctx, released)
						}
						return nil
					}
					hub.mu.Unlock()
					release := sync.OnceFunc(func() { close(released) })
					detach := sync.OnceFunc(func() { release(); hub.mu.Lock(); hub.beforeRelay, hub.afterRelay = nil, nil; hub.mu.Unlock() })
					defer detach()
					call, cancel := f.beginRun(heldActor, heldProposal)
					defer cancel()
					gate := f.awaitBoundary(entered)
					recorded.perform(t, f, "held", [2]uint64{actorA, actorB}, [2]allocationProposal{a, b}, [2]processResponse{})
					owner := nativeFaultFamilyCall(t, n.all[1], nativeFaultFamilyControl{Version: 1, Action: "inspect"})
					exploration := nativeFaultFamilyExploration{Originals: [2][]byte{a.wire, b.wire}, Gate: gate, Before: before, PlacementBefore: bytes.Clone(placementBefore), MoveOwnerCookie: owner.Cookie, Order: []string{"freeze-A-B", heldName + "-" + phase}}
					fence := func() {
						ctx, stop := context.WithTimeout(t.Context(), 10*time.Second)
						response, transport := n.all[1].exchange(ctx, processRequest{op: processRangeRun, entity: 1, index: 8, data: graphE17MoveBounds(n.e17MovePages, 8, 16<<10)})
						stop()
						result, err := decodeGraphE17MoveResult(response)
						if !errors.Is(err, errRangeMovePending) || errors.Is(transport, context.DeadlineExceeded) || result.committed || result.source != (rangeMoveStatus{}) || result.target != (rangeMoveStatus{}) || result.decision != (rangeMoveStatus{}) || result.resume.request == (requestID{}) {
							t.Fatal("unsettled original allowed actual fence/capture or lost original identity", transport, err, result)
						}
						exploration.MovePending = p06ProcessWire(t, response)
						exploration.Order = append(exploration.Order, "fence-blocked-by-original")
					}
					if fenceFirst {
						fence()
					}
					retireHeld := func() {
						cancel()
						select {
						case answer := <-call:
							pending, err := decodeGraphProcessCoordinatorResult(answer.response)
							if !errors.Is(answer.err, context.Canceled) || !errors.Is(err, errCrossProcessPending) || pending.resume.tx != heldTx || pending.decision != (crossStatus{}) {
								t.Fatal("held caller became timeout/decision authority", answer.err, err)
							}
							exploration.Pending = p06ProcessWire(t, answer.response)
						case <-time.After(10 * time.Second):
							t.Fatal("held original A worker not retired")
						}
					}
					completeOriginal := func(actor uint64, proposal allocationProposal, tx [16]byte, commit bool) processResponse {
						response := n.all[actor].call(t, processRequest{op: processSourceCoordinate, data: proposal.wire})
						result, err := decodeGraphProcessCoordinatorResult(response)
						if err != nil || result.decision.commit != commit || result.decision.tx != tx || result.decision.phase != crossDecide {
							t.Fatal("competing actual original first decision", err, result)
						}
						return response
					}
					var aResponse, bResponse processResponse
					if firstOriginal == 0 {
						bResponse = completeOriginal(actorB, b, txb, false)
						exploration.Order = append(exploration.Order, "B-native-abort")
						if !fenceFirst {
							fence()
						}
						retireHeld()
						release()
						aResponse = completeOriginal(actorA, a, txa, true)
						exploration.Order = append(exploration.Order, "A-native-commit-and-resolve")
					} else {
						retireHeld()
						exploration.Order = append(exploration.Order, "B-caller-canceled")
						release()
						aResponse = completeOriginal(actorA, a, txa, true)
						exploration.Order = append(exploration.Order, "A-native-commit-and-resolve")
						bResponse = completeOriginal(actorB, b, txb, false)
						exploration.Order = append(exploration.Order, "B-native-abort")
					}
					exploration.Results[0] = p06ProcessWire(t, aResponse)
					exploration.Results[1] = p06ProcessWire(t, bResponse)
					detach()
					recorded.perform(t, f, "settled", [2]uint64{actorA, actorB}, [2]allocationProposal{a, b}, [2]processResponse{aResponse, bResponse})
					mu.Lock()
					exploration.Manifests = manifests
					mu.Unlock()
					settledHandle, settledCut := graphE17Open(t, n, 2, graphReadFresh, graphCut{})
					settled, settledFacts := nativeFaultFamilyRangeReadAt(t, n, 2, settledHandle, settledCut)
					n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: settledHandle})
					for id, want := range oldFacts {
						if id == 1 || id == 17 {
							want.fact.Labels = []string{"family-conflict"}
						}
						nativeTransitionComposedAssert(t, recorded, reflect.DeepEqual(settledFacts[id], want), "two-conflict exact graph/typed cells differ")
					}
					exploration.Settled = settled
					pendingResponse, err := decodeProcessResponse(exploration.MovePending)
					if err != nil {
						t.Fatal(err)
					}
					attempt := graphE17MoveExchange(t, n.all[1], processRequest{op: processRangeResume, data: pendingResponse.aux})
					attempt, moved, err := graphE17CompleteMoveResponse(t, n, attempt)
					if err != nil || !moved.committed {
						t.Fatal("same owned factory intent did not complete real transfer", err)
					}
					exploration.MoveComplete = p06ProcessWire(t, attempt.response)
					graphE17AssertNativeMoveReceipts(t, n, moved)
					replay := n.all[1].call(t, processRequest{op: processRangeResume, data: attempt.response.aux})
					replayed, err := decodeGraphE17MoveResult(replay)
					if err != nil || replayed != moved {
						t.Fatal("same owned transfer replay changed original", err)
					}
					exploration.MoveReplay = p06ProcessWire(t, replay)
					recorded.perform(t, f, "transferred", [2]uint64{actorA, actorB}, [2]allocationProposal{a, b}, [2]processResponse{aResponse, bResponse})
					foreign := moved
					foreign.resume.request[0] ^= 0x80
					foreign.resume.move = rangeMoveID(namespace{foreign.resume.scope.graph, foreign.resume.scope.partition}, foreign.resume.request)
					foreign.source, foreign.target, foreign.decision = rangeMoveStatus{}, rangeMoveStatus{}, rangeMoveStatus{}
					foreign.committed = false
					foreignWire, err := encodeGraphE17MoveResult(foreign, errRangeMovePending)
					if err != nil {
						t.Fatal(err)
					}
					beforeRefusal := n.all[1].call(t, processRequest{op: processStateOp}).applied
					refused, refusal := n.all[1].exchange(t.Context(), processRequest{op: processRangeResume, data: foreignWire})
					if !errors.Is(refusal, errInvalid) || len(refused.aux) != 0 || n.all[1].call(t, processRequest{op: processStateOp}).applied != beforeRefusal {
						t.Fatal("inert foreign resume acquired original typed owner", refusal)
					}
					exploration.Order = append(exploration.Order, "native-fence-capture-transfer-finish")
					publication, oldOwner := graphE17Placement(t, n, 1, 1)
					if oldOwner.Partition != 8 || oldOwner.Fenced {
						t.Fatal("range did not physically activate at target")
					}
					for actor := uint64(1); actor <= 6; actor++ {
						response := n.all[actor].call(t, processRequest{op: processRangeEvidenceProbe, entity: 1})
						exploration.PlacementAfter = append(exploration.PlacementAfter, bytes.Clone(response.aux))
						p, o := graphE17Placement(t, n, actor, 1)
						if p != publication || o != oldOwner {
							t.Fatal("real transfer placement differs on native voter", actor)
						}
					}
					handle, currentCut := graphE17Open(t, n, 2, graphReadFresh, graphCut{})
					transferred, facts := nativeFaultFamilyRangeReadAt(t, n, 2, handle, currentCut)
					n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
					nativeTransitionComposedAssert(t, recorded, reflect.DeepEqual(facts, settledFacts), "real range transfer changed logical IDs, exact cells or postings")
					exploration.Transferred = transferred
					graphE17AssertNativeVoters(t, n, currentCut, processRangeNativeVoterProbe, nil)
					heldRead, heldFacts := nativeFaultFamilyRangeReadAt(t, n, 2, held, oldCut)
					nativeTransitionComposedAssert(t, recorded, reflect.DeepEqual(heldFacts, oldFacts) && reflect.DeepEqual(heldRead, before), "held preconflict cut substituted current after transfer")
					exploration.Held = heldRead
					n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: held})
					heldClosed = true
					oldHandle, reopenedCut := graphE17Open(t, n, 5, graphReadAt, oldCut)
					reopened, reopenedFacts := nativeFaultFamilyRangeReadAt(t, n, 5, oldHandle, reopenedCut)
					n.all[5].call(t, processRequest{op: processSourceCertifiedClose, index: oldHandle})
					nativeTransitionComposedAssert(t, recorded, reflect.DeepEqual(reopenedFacts, oldFacts) && reflect.DeepEqual(reopened, before), "new genuine old read lost original state after real transfer")
					exploration.Reopened = reopened
					for _, id := range []uint64{1, 17} {
						nativeTransitionComposedAssert(t, recorded, slices.Equal(settledFacts[id].fact.Labels, []string{"family-conflict"}), "missing exact committed label")
					}
					through := f.indexes()
					records := p06Records(f, from, through)
					aResult, err := decodeGraphProcessCoordinatorResult(aResponse)
					if err != nil {
						t.Fatal(err)
					}
					nativeFaultFamilyAssertConflictCDC(t, records, aResult.decision, moved, oldOwner.Epoch-1)
					writeNativeFaultFamilyArtifact(t, f, nativeFaultFamilyArtifact{nativeFaultArtifact: nativeFaultArtifact{Case: name, From: from, Through: through, Records: records}, Exploration: &exploration, Replay: recorded.finish(t)})
				})
			}
		}
	}
}

func TestNativeFaultFamilySnapshotInterruptionAndAtomicKVPublicationRecoverWholeCut(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, name := range []string{"snapshot-chunk-interruption", "snapshot-atomic-kv-sync"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("RHO_GRAPH_FAULT_FAMILY_PROCESS", "1")
			t.Setenv("RHO_GRAPH_NATIVE_FAULT_PROCESS", "1")
			f := newPairedGraphFaultNetwork(t)
			before := nativeFaultSeed(t, f)
			old := p06Observe(f, 6, graphReadFresh, graphCut{})
			cut, err := pairedFaultCut(old.Cut)
			if err != nil {
				t.Fatal(err)
			}
			prior := f.n.all[3].call(t, processRequest{op: processStateOp})
			processes := []pairedFaultProcess{f.kill(3, "replica")}
			f.n.groups[0].blocked = map[uint64]bool{3: true}
			defer func() { f.n.groups[0].blocked = nil }()
			proposal, from := f.proposal(3), f.indexes()
			capture, err := nativeFaultCaptureManifest(f.n.all[1].mux.hub, 5)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _, _ = capture() }()
			result := f.run(5, proposal)
			manifest, err := capture()
			if err != nil {
				t.Fatal(err)
			}
			published := f.n.all[1].call(t, processRequest{op: processPublish})
			if published.stats[statCheckpoint] <= prior.applied {
				t.Fatal("actual native checkpoint did not pass lagged receiver")
			}
			var notices []nativeFaultSnapshotBoundary
			var actualIO *nativeFaultReceipt
			injected := false
			f.nativeSnapshotHook = func(boundary nativeFaultSnapshotBoundary) bool {
				if boundary.Donor != 1 || boundary.Receiver != 3 {
					t.Fatal("snapshot escaped genuine lagged owner")
				}
				notices = append(notices, boundary)
				if injected {
					return true
				}
				if name == "snapshot-chunk-interruption" && boundary.Phase == "after-append" {
					chunk, err := decodePairedProcessChunk(boundary.Chunk)
					if err != nil || chunk.Final {
						t.Fatal("interruption requires an actual nonterminal persisted chunk", err)
					}
					injected = true
					processes = append(processes, f.kill(3, "snapshot-receiver"))
					f.n.groups[0].blocked = map[uint64]bool{3: true}
					return false
				}
				if name == "snapshot-atomic-kv-sync" && boundary.Phase == "before-activate" {
					injected = true
					owner := nativeFaultControlCall(t, f.n.all[3], nativeFaultControl{Version: 1, Action: "inspect"})
					armed := nativeFaultControlCall(t, f.n.all[3], nativeFaultControl{Version: 1, Action: "arm", Cookie: owner.Cookie, Sequence: 1, Kind: "file-sync", Nth: 1})
					f.mu.Lock()
					f.down[3] = true
					f.mu.Unlock()
					f.n.groups[0].blocked = map[uint64]bool{3: true}
					ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
					response, transport := f.n.all[3].exchange(ctx, processRequest{op: processActivate, index: boundary.Owner, packet: boundary.Packet})
					cancel()
					if transport == nil && response.code == 0 {
						t.Fatal("failed native snapshot sync acknowledged activation")
					}
					dir := filepath.Join(f.n.dir, "3")
					receipt := readNativeFaultIO(t, dir, armed)
					actualIO = &receipt
					failed := nativeFaultReap(t, f.n.all[3], 3, dir, receipt)
					f.mu.Lock()
					f.intentionalKills[f.n.all[3]] = true
					f.mu.Unlock()
					processes = append(processes, failed)
					return false
				}
				return true
			}
			defer func() { f.nativeSnapshotHook = nil }()
			interrupted := f.replaceBeforeHeal(3, func(_ *graphProcess, _ processResponse) { f.n.groups[0].blocked = nil })
			if !injected || interrupted.Ready || interrupted.Action != "reopen-interrupted" {
				t.Fatal("snapshot arm did not interrupt an actual native receiver")
			}
			processes = append(processes, interrupted)
			var reopened p06Observation
			rejoined := f.replaceBeforeHeal(3, func(_ *graphProcess, state processResponse) {
				f.n.groups[0].blocked = nil
				if state.applied < prior.applied {
					t.Fatal("interrupted snapshot lost old acknowledged prefix")
				}
				// Origin2 has not previously used source3. At needs no leader
				// ReadIndex and now reads the replacement's OWN native history.
				f.reconnect(2, 3)
				reopened = p06Observe(f, 2, graphReadAt, cut)
				if !reflect.DeepEqual(old, reopened) {
					t.Fatal("snapshot reopen before catch-up changed actual old source")
				}
			})
			processes = append(processes, rejoined)
			f.nativeSnapshotHook = nil
			if len(notices) < 3 || notices[len(notices)-1].Phase != "after-activate" {
				t.Fatal("actual restarted snapshot did not reach verified native activation")
			}
			step := p03Complete(f, 6, "edge-create", proposal, result, from, before)
			f.assertView(step.After, 3)
			pairedFaultAssertCDC(t, step, 3)
			assertLifecycleCandidates(t, step.RawObservation, 3)
			recovered := p06Observe(f, 6, graphReadFresh, graphCut{})
			retained := p06Observe(f, 6, graphReadAt, cut)
			if !reflect.DeepEqual(old, retained) {
				t.Fatal("snapshot publication substituted current at old cut")
			}
			through := f.indexes()
			writeNativeFaultFamilyArtifact(t, f, nativeFaultFamilyArtifact{nativeFaultArtifact: nativeFaultArtifact{Case: name, Original: proposal.wire, Manifest: manifest, Before: old, Retained: retained, Recovered: recovered, Reopened: reopened, From: from, Through: through, Records: p06Records(f, from, through), Processes: processes, IO: actualIO}, Snapshot: notices})
		})
	}
}

func TestNativeFaultFamilyChildClockExpiryAndRollbackKeepOldAuthorityDead(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	t.Setenv("RHO_GRAPH_FAULT_FAMILY_PROCESS", "1")
	f := newPairedGraphFaultNetwork(t)
	before := nativeFaultSeed(t, f)
	old := p06Observe(f, 6, graphReadFresh, graphCut{})
	cut, err := pairedFaultCut(old.Cut)
	if err != nil {
		t.Fatal(err)
	}
	held, _ := f.open(6, graphReadAt, cut)
	heldClosed := false
	defer func() {
		if !heldClosed {
			f.n.all[6].call(t, processRequest{op: processSourceCertifiedClose, index: held})
		}
	}()
	if got := nativeLifecycleRead(t, f.n, 6, held, 0); !reflect.DeepEqual(got.Entities, old.Native.Entities) {
		t.Fatal("genuine old source lease prerequisite")
	}
	proposal, from := f.proposal(3), f.indexes()
	capture, err := nativeFaultCaptureManifest(f.n.all[1].mux.hub, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = capture() }()
	result := f.run(5, proposal)
	manifest, err := capture()
	if err != nil {
		t.Fatal(err)
	}
	step := p03Complete(f, 5, "edge-create", proposal, result, from, before)
	f.assertView(step.After, 3)
	pairedFaultAssertCDC(t, step, 3)
	one := nativeFaultFamilyCall(t, f.n.all[1], nativeFaultFamilyControl{Version: 1, Action: "inspect"})
	four := nativeFaultFamilyCall(t, f.n.all[4], nativeFaultFamilyControl{Version: 1, Action: "inspect"})
	clocks := []nativeFaultFamilyReceipt{
		nativeFaultFamilyCall(t, f.n.all[4], nativeFaultFamilyControl{Version: 1, Action: "clock", Cookie: four.Cookie, Sequence: 1, OffsetMillis: -24 * 60 * 60 * 1000}),
		nativeFaultFamilyCall(t, f.n.all[1], nativeFaultFamilyControl{Version: 1, Action: "clock", Cookie: one.Cookie, Sequence: 1, OffsetMillis: 120000}),
	}
	// Observe the actual authenticated source refusal as well as the GPR cause
	// propagated through the correlated parent IPC transport.
	arena, err := graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := arena.Reserve(32 << 10); err != nil {
		t.Fatal(err)
	}
	hub := f.n.all[1].mux.hub
	var mu sync.Mutex
	var refusal nativeFaultFamilyExpiry
	hub.mu.Lock()
	if hub.afterRelay != nil {
		hub.mu.Unlock()
		t.Fatal("clock diagnostic relay already owned")
	}
	hub.afterRelay = func(ctx context.Context, origin, partition uint64, request, reply []byte) error {
		if origin != 6 || partition != 3 {
			return nil
		}
		actual, err := decodeGraphWireResponse(reply)
		if err != nil {
			return err
		}
		if !errors.Is(actual.err, graphstore.ErrSourceUnavailable) {
			return nil
		}
		if len(request)+len(reply) > 8192 {
			return errLimit
		}
		mu.Lock()
		defer mu.Unlock()
		if refusal.Reply == nil {
			refusal.Request, refusal.Reply = bytes.Clone(request), bytes.Clone(reply)
		}
		return nil
	}
	hub.mu.Unlock()
	detach := sync.OnceFunc(func() { hub.mu.Lock(); hub.afterRelay = nil; hub.mu.Unlock() })
	defer detach()
	expire := func() []byte {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		response, transport := f.n.all[6].exchange(ctx, processRequest{op: processLifecycleObserve, index: held})
		cancel()
		if !errors.Is(transport, graphstore.ErrSourceUnavailable) || response.code == 0 || len(response.aux) != 0 || errors.Is(transport, context.DeadlineExceeded) || response.incarnation != f.n.all[6].incarnation || response.sequence == 0 {
			t.Fatal("expired held owner returned output or lacked actual correlation", transport)
		}
		return p06ProcessWire(t, response)
	}
	first := expire()
	clocks = append(clocks, nativeFaultFamilyCall(t, f.n.all[1], nativeFaultFamilyControl{Version: 1, Action: "clock", Cookie: one.Cookie, Sequence: 2, OffsetMillis: -24 * 60 * 60 * 1000}))
	second := expire()
	detach()
	mu.Lock()
	actual := refusal
	mu.Unlock()
	if len(actual.Request) == 0 || len(actual.Reply) == 0 {
		t.Fatal("clock jump lacked genuine source-expiry reply")
	}
	actual.Response = first
	expiry := []nativeFaultFamilyExpiry{actual, {Response: second}}
	f.n.all[6].call(t, processRequest{op: processSourceCertifiedClose, index: held})
	heldClosed = true
	clocks = append(clocks, nativeFaultFamilyCall(t, f.n.all[1], nativeFaultFamilyControl{Version: 1, Action: "clock", Cookie: one.Cookie, Sequence: 3}), nativeFaultFamilyCall(t, f.n.all[4], nativeFaultFamilyControl{Version: 1, Action: "clock", Cookie: four.Cookie, Sequence: 2}))
	recovered := p06Observe(f, 6, graphReadFresh, graphCut{})
	retained := p06Observe(f, 6, graphReadAt, cut)
	if !reflect.DeepEqual(old, retained) {
		t.Fatal("new genuine old query changed original state after clock rollback")
	}
	through := f.indexes()
	writeNativeFaultFamilyArtifact(t, f, nativeFaultFamilyArtifact{nativeFaultArtifact: nativeFaultArtifact{Case: "clock-expiry-rollback", Original: proposal.wire, Manifest: manifest, Before: old, Retained: retained, Recovered: recovered, Reopened: retained, From: from, Through: through, Records: p06Records(f, from, through)}, Clocks: clocks, Expiry: expiry})
}

func TestNativeFaultFamilyCorruptedDurableNativeRecordsRefuseBeforeReady(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, suffix := range []string{".log", "manifest"} {
		t.Run(suffix, func(t *testing.T) {
			t.Setenv("RHO_GRAPH_FAULT_FAMILY_PROCESS", "1")
			t.Setenv("RHO_GRAPH_DURABLE_MODEL", "1")
			f := newPairedGraphFaultNetwork(t)
			nativeFaultSeed(t, f)
			old := p06Observe(f, 6, graphReadFresh, graphCut{})
			cut, err := pairedFaultCut(old.Cut)
			if err != nil {
				t.Fatal(err)
			}
			from := f.indexes()
			var requiredImage, requiredOutcome []byte
			for _, cert := range cut.certificates {
				if cert.scope.partition == 3 {
					requiredImage = bytes.Clone(cert.image[:])
					requiredOutcome = bytes.Clone(f.n.all[1].call(t, processRequest{op: processEntryRecord, index: cert.sourceIndex}).outcome)
				}
			}
			owner := nativeFaultFamilyCall(t, f.n.all[1], nativeFaultFamilyControl{Version: 1, Action: "inspect"})
			capture := nativeFaultFamilyCall(t, f.n.all[1], nativeFaultFamilyControl{Version: 1, Action: "model", Cookie: owner.Cookie})
			prior := f.n.all[1]
			killed := f.kill(1, "replica")
			model := nativeFaultFamilyInstallModel(t, f, 1, capture)
			dir := filepath.Join(f.n.dir, "1")
			shadow := nativeDurableShadowPath(dir, prior.cmd.Process.Pid)
			image, digest, err := nativeDurableReadImage(t.Context(), shadow, capture.Policy)
			if err != nil || digest != model.Recovery.CatalogSHA {
				t.Fatal("selected synced image changed", err)
			}
			corruption, err := nativeDurableCorrupt(t.Context(), dir, image, suffix, model.Recovery.WorkBytes, requiredImage, requiredOutcome)
			if err != nil {
				t.Fatal("native record corruption was not applied", err)
			}
			t.Setenv("RHO_GRAPH_DURABLE_MODEL", "off")
			candidate := startGraphProcess(t, dir, 1, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_LIFECYCLE_PROCESS=1", "RHO_GRAPH_PAIRED_PROCESS=1", "RHO_GRAPH_PROCESS_REOPEN=1")
			failed, refusal := nativeFaultFamilyReapOpenRefusal(t, candidate, 1, dir)
			// Reconstruct again from the SAME immutable native synced image;
			// never infer which bytes Open may have changed while refusing it.
			repair, err := nativeDurableRecoverCharged(t.Context(), shadow, f.n.dir, capture.ModelCookie, digest, capture.Policy, corruption.WorkBytes)
			if err != nil {
				t.Fatal("prepaid same-image native repair", err)
			}
			archive := fmt.Sprintf("%s.corrupt.%d", dir, candidate.cmd.Process.Pid)
			if _, err := os.Lstat(archive); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("corrupt archive already owned", err)
			}
			if err := os.Rename(dir, archive); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(repair.Directory, dir); err != nil {
				t.Fatal("native repair install", errors.Join(err, os.Rename(archive, dir)))
			}
			parent, err := os.Open(f.n.dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := errors.Join(parent.Sync(), parent.Close()); err != nil {
				t.Fatal(err)
			}
			f.elect(1, 2)
			var reopened p06Observation
			rejoined := f.replaceBeforeHeal(1, func(_ *graphProcess, state processResponse) {
				for _, cert := range cut.certificates {
					if cert.scope.partition == 3 && cert.sourceIndex > state.applied {
						t.Fatal("repaired native image lost acknowledged prefix")
					}
				}
				reopened = nativeP06InitialLocalOld(f, 1, cut)
				if !reflect.DeepEqual(old, reopened) {
					t.Fatal("native repair changed old cut before catch-up")
				}
			})
			p03Reconnect(f, 6)
			recovered := p06Observe(f, 6, graphReadFresh, graphCut{})
			retained := p06Observe(f, 6, graphReadAt, cut)
			if !reflect.DeepEqual(old, retained) || !reflect.DeepEqual(old.Native, recovered.Native) {
				t.Fatal("corruption/repair changed useful graph")
			}
			name := "durable-native-wal-corruption"
			if suffix == "manifest" {
				name = "durable-native-manifest-corruption"
			}
			through := f.indexes()
			writeNativeFaultFamilyArtifact(t, f, nativeFaultFamilyArtifact{nativeFaultArtifact: nativeFaultArtifact{Case: name, Before: old, Retained: retained, Recovered: recovered, Reopened: reopened, From: from, Through: through, Records: p06Records(f, from, through), Processes: []pairedFaultProcess{killed, failed, rejoined}}, Model: &model, Corruption: &corruption, OpenRefusal: &refusal, Repair: &repair, CorruptArchive: archive})
		})
	}
}

func TestNativeFaultFamilyOldLeaderPacketStaysChargedAcrossGenuineElection(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	f := newPairedGraphFaultNetwork(t)
	before := nativeFaultSeed(t, f)
	old := p06Observe(f, 6, graphReadFresh, graphCut{})
	cut, err := pairedFaultCut(old.Cut)
	if err != nil {
		t.Fatal(err)
	}
	proposal, from := f.proposal(3), f.indexes()
	f.packetDrain()
	scheduler, err := newNativeFaultPacketScheduler(t.Context(), "hold", 1, 3, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	detach, err := attachNativeFaultPacketScheduler(f.n.all[1].mux.hub, scheduler)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := detach(); err != nil {
			t.Error("unfinished old-leader packet", err)
		}
	}()
	capture, err := nativeFaultCaptureManifest(f.n.all[1].mux.hub, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = capture() }()
	call, cancel := f.beginRun(5, proposal)
	defer cancel()
	select {
	case <-scheduler.matched:
	case <-time.After(10 * time.Second):
		t.Fatal("no actual old-leader packet held")
	}
	// Diagnostic ownership is prepaid before making another owned copy.
	arena, err := graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := arena.Reserve(4 * processFrameBytes); err != nil {
		t.Fatal(err)
	}
	scheduler.mu.Lock()
	held := scheduler.held.packet
	held.Payload = bytes.Clone(held.Payload)
	scheduler.mu.Unlock()
	spec := partitionCodecRequest(t, 1, defaultMaterializerLimits())
	ns, _ := certifiedProcessNamespace(1)
	part, _ := spec.declaration.Partition(ns.partition)
	binding := raftlog.ApplicationBinding{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(ns.graph), Partition: ns.partition, Group: part.Group}, SemanticContractID: certifiedGraphSemanticContractID()}
	raw, err := replica.DecodeApplicationPacket(binding, held, processFrameBytes)
	message := new(pb.Message)
	if err == nil {
		err = (proto.UnmarshalOptions{RecursionLimit: 8}).Unmarshal(raw.Payload, message)
	}
	if err != nil || message.GetFrom() != 1 || message.GetTo() != 3 || message.GetTerm() == 0 {
		t.Fatal("held packet did not bind actual old leader", err)
	}
	// A caller cancellation retires only its own response. The genuine held
	// Raft bytes remain transport-owned until their explicit native delivery.
	cancel()
	var pending []byte
	select {
	case answer := <-call:
		result, e := decodeGraphProcessCoordinatorResult(answer.response)
		if e != nil && !errors.Is(e, errCrossProcessPending) {
			t.Fatal("unexpected original classification", e)
		}
		if e != nil && result.decision != (crossStatus{}) {
			t.Fatal("pending invented first decision")
		}
		pending = p06ProcessWire(t, answer.response)
	case <-time.After(10 * time.Second):
		t.Fatal("old-leader caller did not retire")
	}
	manifest, err := capture()
	if err != nil {
		t.Fatal(err)
	}
	// Explicit election messages use the ordinary existing tiny transport;
	// voter1 is isolated for that election while voters2+3 form the quorum.
	f.elect(1, 2)
	leaderBefore := f.n.all[2].call(t, processRequest{op: processStateOp})
	if leaderBefore.stats[statTerm] <= message.GetTerm() {
		t.Fatal("held packet was not from an older actual term")
	}
	if err := scheduler.deliverHeld(f.n.all[1].mux.hub); err != nil {
		t.Fatal("old-leader Step failed", err)
	}
	events, err := detach()
	if err != nil || len(events) != 2 || events[0].Action != "hold" || events[1].Action != "release-oldleader" || events[0].SHA256 != events[1].SHA256 {
		t.Fatal("held packet lost exact ownership/delivery", err)
	}
	f.heal(2)
	p03Reconnect(f, 6)
	result := f.run(6, proposal)
	step := p03Complete(f, 6, "edge-create", proposal, result, from, before)
	f.assertView(step.After, 3)
	pairedFaultAssertCDC(t, step, 3)
	assertLifecycleCandidates(t, step.RawObservation, 3)
	recovered := p06Observe(f, 6, graphReadFresh, graphCut{})
	retained := p06Observe(f, 6, graphReadAt, cut)
	if !reflect.DeepEqual(old, retained) {
		t.Fatal("stale-leader delivery changed old cut")
	}
	leaderAfter := f.n.all[2].call(t, processRequest{op: processStateOp})
	if leaderAfter.stats[statTerm] < leaderBefore.stats[statTerm] {
		t.Fatal("stale packet rolled back native term")
	}
	through := f.indexes()
	writeNativeFaultFamilyArtifact(t, f, nativeFaultFamilyArtifact{nativeFaultArtifact: nativeFaultArtifact{Case: "old-leader-packet", Original: proposal.wire, Manifest: manifest, Before: old, Retained: retained, Recovered: recovered, Reopened: retained, From: from, Through: through, Records: p06Records(f, from, through), PacketEvents: events, PendingCaller: pending}, HeldPacket: &held, HeldTerm: message.GetTerm(), LeaderBefore: p06ProcessWire(t, leaderBefore), LeaderAfter: p06ProcessWire(t, leaderAfter)})
}

func TestNativeFaultFamilyDirectedLossAndUnequalTicksPreserveOriginalAndOldCut(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, name := range []string{"unequal-ticks", "asymmetric-network", "partition-quorum-loss", "coordinator-quorum-loss", "logical-domain-loss"} {
		t.Run(name, func(t *testing.T) {
			f := newPairedGraphFaultNetwork(t)
			before := nativeFaultSeed(t, f)
			old := p06Observe(f, 6, graphReadFresh, graphCut{})
			cut, err := pairedFaultCut(old.Cut)
			if err != nil {
				t.Fatal(err)
			}
			proposal, from := f.proposal(3), f.indexes()
			var ticks []nativeFaultFamilyTick
			var windows []nativeFaultFamilyNetworkWindow
			var processes []pairedFaultProcess
			var unavailable []nativeFaultPartialRead
			var pending []byte
			var healNetwork func() nativeFaultFamilyNetworkWindow
			recoverer := uint64(5)
			switch name {
			case "unequal-ticks":
				// Actor3 receives no ticks; real peers receive unequal vectors
				// below their election thresholds, with normal packet delivery.
				for _, vector := range []struct {
					member uint64
					count  int
				}{{2, 7}, {5, 4}, {6, 1}} {
					ticks = append(ticks, nativeFaultFamilyTicks(t, f, vector.member, vector.count)...)
				}
			case "asymmetric-network":
				finish := nativeFaultFamilyPartition(t, f, [][2]uint64{{1, 2}})
				healNetwork = finish
				ticks = append(ticks, nativeFaultFamilyTicks(t, f, 1, 2)...)
				// Keep the actual one-way failure active through the useful write.
				defer finish()
			case "logical-domain-loss":
				// One actual OS replica in each partition is lost together.
				processes = append(processes, f.kill(2, "domain-a"), f.kill(5, "domain-a"))
				recoverer = 6
			case "partition-quorum-loss", "coordinator-quorum-loss":
				leader, next := uint64(1), uint64(2)
				if name == "coordinator-quorum-loss" {
					leader, next = 4, 5
				}
				finish := nativeFaultFamilyPartition(t, f, [][2]uint64{{leader, leader + 1}, {leader + 1, leader}, {leader, leader + 2}, {leader + 2, leader}})
				defer finish()
				ticks = append(ticks, nativeFaultFamilyTicks(t, f, leader, 24)...)
				command, err := decodePartitionGraphCommand(proposal.wire, defaultMaterializerLimits())
				if err != nil {
					t.Fatal(err)
				}
				tx := crossTransactionID(namespace{command.request.ns.graph, 8}, command.request.id)
				unavailable = append(unavailable, nativeFaultPartialReadCall(t, f, 6, graphReadFresh, [16]byte{}, 0), nativeFaultPartialReadCall(t, f, 6, graphReadAfter, tx, 8))
				call, cancel := f.beginRun(5, proposal)
				select {
				case answer := <-call:
					result, err := decodeGraphProcessCoordinatorResult(answer.response)
					if !errors.Is(err, errCrossProcessPending) || result.decision != (crossStatus{}) || result.resume.tx != tx || errors.Is(answer.err, context.DeadlineExceeded) {
						t.Fatal("quorum loss invented a completed decision", answer.err, err)
					}
					pending = p06ProcessWire(t, answer.response)
				case <-time.After(10 * time.Second):
					cancel()
					t.Fatal("unavailable original did not return bounded pending")
				}
				cancel()
				retained := p06Observe(f, 6, graphReadAt, cut)
				if !reflect.DeepEqual(old, retained) {
					t.Fatal("quorum loss invalidated genuine retained cut")
				}
				windows = append(windows, finish())
				f.elect(leader, next)
				f.heal(next)
				p03Reconnect(f, 6)
				recoverer = 6
			}
			capture, err := nativeFaultCaptureManifest(f.n.all[1].mux.hub, recoverer)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _, _ = capture() }()
			result := f.run(recoverer, proposal)
			if healNetwork != nil {
				windows = append(windows, healNetwork())
			}
			manifest, err := capture()
			if err != nil {
				t.Fatal(err)
			}
			if name == "logical-domain-loss" {
				processes = append(processes, f.replace(2), f.replace(5))
			}
			f.heal(f.n.leaders[0])
			f.heal(f.n.leaders[1])
			step := p03Complete(f, 6, "edge-create", proposal, result, from, before)
			f.assertView(step.After, 3)
			pairedFaultAssertCDC(t, step, 3)
			assertLifecycleCandidates(t, step.RawObservation, 3)
			recovered := p06Observe(f, 6, graphReadFresh, graphCut{})
			retained := p06Observe(f, 6, graphReadAt, cut)
			if !reflect.DeepEqual(old, retained) {
				t.Fatal("network/tick schedule substituted current state")
			}
			through := f.indexes()
			writeNativeFaultFamilyArtifact(t, f, nativeFaultFamilyArtifact{nativeFaultArtifact: nativeFaultArtifact{Case: name, Original: proposal.wire, Manifest: manifest, Before: old, Retained: retained, Recovered: recovered, Reopened: retained, From: from, Through: through, Records: p06Records(f, from, through), Processes: processes, PartialReads: unavailable, PendingCaller: pending}, Ticks: ticks, Network: windows})
		})
	}
}

func TestNativeFaultFamilyOptionsPublicationRenameFailsActualReopen(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	t.Setenv("RHO_GRAPH_NATIVE_FAULT_PROCESS", "1")
	t.Setenv("RHO_GRAPH_FAULT_FAMILY_PROCESS", "1")
	f := newPairedGraphFaultNetwork(t)
	nativeFaultSeed(t, f)
	old := p06Observe(f, 6, graphReadFresh, graphCut{})
	cut, err := pairedFaultCut(old.Cut)
	if err != nil {
		t.Fatal(err)
	}
	from := f.indexes()
	killed := f.kill(1, "replica")
	f.elect(1, 2)
	dir := filepath.Join(f.n.dir, "1")
	candidate := startGraphProcess(t, dir, 1, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_LIFECYCLE_PROCESS=1", "RHO_GRAPH_PROCESS_REOPEN=1", "RHO_GRAPH_PAIRED_PROCESS=1", "RHO_GRAPH_NATIVE_FAULT_START=options-rename")
	failed := nativeFaultReap(t, candidate, 1, dir, nativeFaultReceipt{})
	receipt := readNativeFaultStartupIO(t, dir, 1, uint64(candidate.cmd.Process.Pid))
	if receipt.Kind != "options-rename" || receipt.IO.Destination == "" || receipt.IO.Kind != "rename" {
		t.Fatal("actual native OPTIONS rename never matched")
	}
	var reopened p06Observation
	rejoined := f.replaceBeforeHeal(1, func(_ *graphProcess, _ processResponse) { reopened = nativeP06InitialLocalOld(f, 1, cut) })
	p03Reconnect(f, 6)
	recovered := p06Observe(f, 6, graphReadFresh, graphCut{})
	retained := p06Observe(f, 6, graphReadAt, cut)
	if !reflect.DeepEqual(old, retained) || !reflect.DeepEqual(old, reopened) || !reflect.DeepEqual(old.Native, recovered.Native) {
		t.Fatal("OPTIONS publication failure changed actual graph or old cut")
	}
	through := f.indexes()
	writeNativeFaultFamilyArtifact(t, f, nativeFaultFamilyArtifact{nativeFaultArtifact: nativeFaultArtifact{Case: "options-rename-during-reopen", Before: old, Retained: retained, Recovered: recovered, Reopened: reopened, Processes: []pairedFaultProcess{killed, failed, rejoined}, From: from, Through: through, Records: p06Records(f, from, through), IO: &receipt}})
}

func TestNativeFaultFamilyLeaseClockCannotExtendOrReviveNativeOwner(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	crossEdgeFixture(t, n)
	h := n.hosts[0][0]
	server, err := newGraphSourceServer(t.Context(), h, defaultGraphSourceLimits(h.machine.limits))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.close(); err != nil {
			t.Error(err)
		}
	})
	var offset atomic.Int64
	server.lifetimeNow = func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }
	info, err := server.open(t.Context(), n.read(0, allocationQuery{kind: observeConfiguration}))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := server.lease(info.cookie)
	if err != nil {
		t.Fatal(err)
	}
	caller, cancel := context.WithDeadline(t.Context(), time.Now().Add(10*time.Second))
	defer cancel()
	offset.Store(int64(-24 * time.Hour))
	lease.mu.Lock()
	remaining := lease.remainingLifetime(caller)
	lease.mu.Unlock()
	if remaining <= 0 || remaining > 10*time.Second {
		t.Fatal("wall rollback extended actual monotone caller deadline", remaining)
	}
	offset.Store(int64(2 * time.Minute))
	if _, err := server.root(t.Context(), info.cookie, 172); !errors.Is(err, graphstore.ErrSourceUnavailable) {
		t.Fatal("forward clock did not expire genuine lease", err)
	}
	offset.Store(int64(-24 * time.Hour))
	if _, err := server.root(t.Context(), info.cookie, 172); !errors.Is(err, graphstore.ErrSourceUnavailable) {
		t.Fatal("clock rollback revived canceled native owner", err)
	}
	if lease.ctx.Err() == nil {
		t.Fatal("expiry was not permanent cancellation")
	}
	// A diagnostic callback owns bounded copies, never the live receiver ACK
	// or packet bytes used by native activation. This is a seam guard only.
	manifest, chunk, payload, ack := []byte{1}, []byte{2}, []byte{3}, []byte{4}
	noticed := false
	f := &pairedGraphFaultNetwork{t: t, nativeSnapshotHook: func(boundary nativeFaultSnapshotBoundary) bool {
		noticed = true
		clear(boundary.Manifest)
		clear(boundary.Chunk)
		clear(boundary.Packet.Payload)
		clear(boundary.ResponseWire)
		if len(boundary.Response.aux) != 0 || len(boundary.Response.packets) != 0 {
			t.Fatal("callback received live mutable response authority")
		}
		return true
	}}
	if !f.nativeSnapshotNotice(nativeFaultSnapshotBoundary{Manifest: manifest, Chunk: chunk, Packet: replica.Packet{Payload: payload}, Response: processResponse{incarnation: [16]byte{1}, sequence: 1, aux: ack}}) || !noticed || manifest[0] != 1 || chunk[0] != 2 || payload[0] != 3 || ack[0] != 4 {
		t.Fatal("snapshot diagnostic callback mutated live owner bytes")
	}
}
