package graphapply

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

func crossPlanAt(n *allocationTestNetwork, group int, id requestID, ops []graphstate.Operation, claims []freshBinding, provenance uint64) (crossTransaction, allocationProposal) {
	n.t.Helper()
	sources := []allocationProof{n.read(0, allocationQuery{kind: observeConfiguration}), n.read(1, allocationQuery{kind: observeConfiguration})}
	tx, p, err := n.hosts[group][0].PrepareCrossPartitionGraphOperations(n.t.Context(), id, ops, provenance, claims, sources)
	if err != nil {
		n.t.Fatal(err)
	}
	return tx, p
}
func crossResolveAt(n *allocationTestNetwork, coordinator int, decision crossProof, groups ...int) {
	n.t.Helper()
	for _, group := range groups {
		partition := uint64(3)
		if group == 1 {
			partition = 8
		}
		p, err := n.hosts[coordinator][0].ResolveCrossParticipant(partition, decision)
		if err != nil {
			n.t.Fatal(err)
		}
		n.crossSubmit(group, p)
	}
}
func crossFillOrdinary(n *allocationTestNetwork, group int, scope graphstate.Operation) {
	want := reasonPreparedConflict
	if group == 1 {
		want = reasonRoutingUnknown
	}
	crossFillOrdinaryWithReason(n, group, scope, want)
}
func crossFillOrdinaryWithReason(n *allocationTestNetwork, group int, scope graphstate.Operation, want reason) {
	n.t.Helper()
	h := n.hosts[group][0]
	for range 128 {
		p, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "capacity-only-padding", Scope: scope.Scope}}, 26, nil)
		if err != nil {
			n.t.Fatal(err)
		}
		if err := h.machine.store.AdmitApplication(len(p.wire)); errors.Is(err, raftlog.ErrLimit) {
			for _, voter := range n.hosts[group] {
				if err := voter.machine.store.AdmitApplication(1); !errors.Is(err, raftlog.ErrLimit) {
					n.t.Fatal("ordinary voter headroom remains", err)
				}
			}
			return
		} else if err != nil {
			n.t.Fatal(err)
		}
		n.submitPartitionGraph(group, p, want)
	}
	n.t.Fatal("finite ordinary cliff not reached")
}

func TestCrossPartitionCapacityAbortMirrorsPriorityAndConflictSourceAtOrdinaryCliffs(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, policy := range []string{"bytes", "records"} {
		for _, loserGroup := range []int{0, 1} {
			for _, sourceGroup := range []int{0, 1} {
				t.Run(fmt.Sprintf("%s-loser%d-source%d", policy, loserGroup, sourceGroup), func(t *testing.T) {
					n := newCrossPartitionNetworkWithPolicy(t, func(p *raftlog.ApplicationPolicy) {
						if policy == "bytes" {
							p.MaxInstallBytes = 128 << 10
							p.RetainedApplicationBytes = 384 << 10
						} else {
							p.RetainedApplicationRecords = 768
						}
					})
					ops, claims := crossEdgeFixture(t, n)
					ids := crossRequestsWithLoser(n, loserGroup)
					var txs [2]crossTransaction
					var registrations [2]crossProof
					for group := range 2 {
						tx, p := crossPlanAt(n, group, ids[group], ops, claims, uint64(24+group))
						txs[group] = tx
						registered := n.crossSubmit(group, p)
						registrations[group] = n.crossReadPhase(group, registered.tx, crossRegister)
					}
					first, err := n.hosts[0][0].PrepareCrossParticipant(txs[0], 3, registrations[0], nil)
					if err != nil {
						t.Fatal(err)
					}
					if yes := n.crossSubmit(0, first); !yes.yes {
						t.Fatal(yes)
					}
					firstVote := n.crossReadPhase(0, registrations[0].status.tx, crossPrepare)
					blocked := [2]allocationProposal{}
					blocked[0], err = n.hosts[1][0].PrepareCrossParticipant(txs[1], 3, registrations[1], nil)
					if err != nil {
						t.Fatal(err)
					}
					blocked[1], err = n.hosts[0][0].PrepareCrossParticipant(txs[0], 8, registrations[0], []crossProof{firstVote})
					if err != nil {
						t.Fatal(err)
					}
					proof := n.capacityRead(sourceGroup, blocked[sourceGroup])
					loser, _, coordinator := proof.observation.loser()
					if loser != registrations[loserGroup].status.tx || coordinator != n.hosts[loserGroup][0].machine.ns.partition {
						t.Fatal("partition-specific priority", loser, coordinator)
					}
					encoded, err := encodeCrossCapacityObservation(proof.observation)
					if err != nil || len(encoded) > 512 {
						t.Fatal("unbounded capacity fact", len(encoded), err)
					}
					for _, mutate := range []func(*crossCapacityProof){func(p *crossCapacityProof) { p.observation.nonce[0] ^= 1 }, func(p *crossCapacityProof) { p.observation.source.group[0] ^= 1 }, func(p *crossCapacityProof) { p.observation.prepareHash[0] ^= 1 }, func(p *crossCapacityProof) { p.observation.configuration[0] ^= 1 }, func(p *crossCapacityProof) { p.observation.holderDigest[0] ^= 1 }, func(p *crossCapacityProof) { p.observation.readID.sequence++ }} {
						bad := proof
						mutate(&bad)
						p, d, err := n.hosts[loserGroup][0].AbortCrossTransactionForCapacity(txs[loserGroup], bad)
						if !errors.Is(err, errInvalid) || len(p.wire) != 0 || d != (crossStatus{}) {
							t.Fatal("mismatched completed capacity proof admitted", err)
						}
					}
					crossFillOrdinary(n, loserGroup, ops[0])
					abort, existing, err := n.hosts[loserGroup][0].AbortCrossTransactionForCapacity(txs[loserGroup], proof)
					if err != nil || existing != (crossStatus{}) {
						t.Fatal("protected abort construction", existing, err)
					}
					aborted := n.crossSubmit(loserGroup, abort)
					if aborted.commit || aborted.round != 0 {
						t.Fatal("resource abort committed", aborted)
					}
					decision := n.crossReadPhase(loserGroup, loser, crossDecide)
					crossResolveAt(n, loserGroup, decision, loserGroup)
					for _, voter := range n.hosts[loserGroup] {
						v, err := voter.machine.store.ApplicationView(voter.driver.Applied())
						if err != nil {
							t.Fatal(err)
						}
						owner, _, err := v.RecoveryOwner(t.Context(), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
						if err != nil || owner.ID != ([32]byte{}) {
							t.Fatal("loser retained own unused phases", owner, err)
						}
						if err := v.Close(); err != nil {
							t.Fatal(err)
						}
					}
					// At an ordinary cliff the winner may still lack creation capacity.
					// Terminating the loser proves progress without claiming a COMMIT.
					if after := n.crossReadPhase(loserGroup, loser, crossDecide); after.status != decision.status {
						t.Fatal("abort decision changed", after.status, decision.status)
					}
				})
			}
		}
	}
}

func TestCrossPartitionCapacityAbortCannotReverseCommitAndLatePrepareRecoversAbort(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	ids := crossRequestsWithLoser(n, 1)
	b, bRegister := crossPlanAt(n, 1, ids[1], ops, claims, 24)
	bRegistered := n.crossSubmit(1, bRegister)
	bRegistration := n.crossReadPhase(1, bRegistered.tx, crossRegister)
	var votes []crossProof
	for group, partition := range []uint64{3, 8} {
		p, err := n.hosts[1][0].PrepareCrossParticipant(b, partition, bRegistration, votes)
		if err != nil {
			t.Fatal(err)
		}
		if got := n.crossSubmit(group, p); !got.yes {
			t.Fatal(got)
		}
		votes = append(votes, n.crossReadPhase(group, bRegistered.tx, crossPrepare))
	}
	commit, err := n.hosts[1][0].DecideCrossTransaction(b, votes)
	if err != nil {
		t.Fatal(err)
	}
	if got := n.crossSubmit(1, commit); !got.commit {
		t.Fatal(got)
	}
	bDecision := n.crossReadPhase(1, bRegistered.tx, crossDecide)
	crossResolveAt(n, 1, bDecision, 0)
	a, aRegister := crossPlanAt(n, 0, ids[0], []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "aborted-label", Scope: ops[0].Scope}}, nil, 25)
	aRegistered := n.crossSubmit(0, aRegister)
	aRegistration := n.crossReadPhase(0, aRegistered.tx, crossRegister)
	first, err := n.hosts[0][0].PrepareCrossParticipant(a, 3, aRegistration, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := n.crossSubmit(0, first); !got.yes {
		t.Fatal(got)
	}
	aVote := n.crossReadPhase(0, aRegistered.tx, crossPrepare)
	late, err := n.hosts[0][0].PrepareCrossParticipant(a, 8, aRegistration, []crossProof{aVote})
	if err != nil {
		t.Fatal(err)
	}
	proof := n.capacityRead(1, late)
	p, existing, err := n.hosts[1][0].AbortCrossTransactionForCapacity(b, proof)
	if err != nil || len(p.wire) != 0 || existing != bDecision.status || !existing.commit {
		t.Fatal("capacity abort reversed committed holder", existing, err)
	}
	crossResolveAt(n, 1, bDecision, 1)
	// B's completed graph changes stale A's captured remote base; this is a
	// real durable NO, separate from the earlier resource observation.
	if no := n.crossSubmit(1, late); no.yes || no.why != reasonReadConflict {
		t.Fatal("stale remote base voted YES", no)
	}
	abort, err := n.hosts[0][0].DecideCrossTransaction(a, []crossProof{aVote, n.crossReadPhase(1, aRegistered.tx, crossPrepare)})
	if err != nil {
		t.Fatal(err)
	}
	if got := n.crossSubmit(0, abort); got.commit {
		t.Fatal(got)
	}
	aDecision := n.crossReadPhase(0, aRegistered.tx, crossDecide)
	crossResolveAt(n, 0, aDecision, 0, 1)
	beforeIndex, beforeImage, err := n.hosts[1][0].machine.store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	got := n.crossSubmit(1, late)
	if got.phase != crossResolve || got.commit {
		t.Fatal("late prepare resurrected terminal transaction", got)
	}
	_, afterImage, err := n.hosts[1][0].machine.store.Checkpoint()
	if err != nil || !bytes.Equal(beforeImage, afterImage) || got.index > beforeIndex {
		t.Fatal("late prepare changed graph state", got, err)
	}
	for _, voter := range n.hosts[0] {
		projection, _ := partitionProjection(t, voter, voter.driver.Applied(), 1, graphGenesisBinding(t).Axis())
		if len(projection.Labels) != 0 {
			t.Fatal("ABORT installed graph effects", projection)
		}
	}
}

func TestCrossPartitionCapacityConflictNeedsAvailableCompletedAuthority(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, register := n.crossPlan(ops, claims, 24)
	registered := n.crossSubmit(0, register)
	registration := n.crossReadPhase(0, registered.tx, crossRegister)
	p, err := n.hosts[0][0].PrepareCrossParticipant(tx, 3, registration, nil)
	if err != nil {
		t.Fatal(err)
	}
	event, err := n.hosts[0][0].ReadCrossCapacityConflict(p, [16]byte{99})
	if err != nil || len(event.capacityReplies) != 0 {
		t.Fatal("quorum-free resource authority", err)
	}
	for _, test := range []struct {
		wire []byte
		want error
	}{
		{nil, errCorrupt},
		{append([]byte{'G', 'C', 'C', 1}, make([]byte, 72)...), errInvalid},
	} {
		reply, err := n.hosts[0][0].completeCrossCapacityRead(test.wire, n.hosts[0][0].driver.Applied())
		if !errors.Is(err, test.want) || reply != (crossCapacityReply{}) || len(n.hosts[0][0].capacityPending) != 1 {
			t.Fatal("malformed completion minted authority or consumed the pending question", reply, err)
		}
	}
	abort, decision, err := n.hosts[0][0].AbortCrossTransactionForCapacity(tx, crossCapacityProof{})
	if !errors.Is(err, errInvalid) || len(abort.wire) != 0 || decision != (crossStatus{}) {
		t.Fatal("missing authority authorized abort", err)
	}
	if _, err := n.hosts[0][0].ReadCrossCapacityConflict(register, [16]byte{100}); !errors.Is(err, errInvalid) {
		t.Fatal("non-prepare conflict question", err)
	}
	var absent *allocationHost
	if _, err := absent.ReadCrossCapacityConflict(p, [16]byte{1}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, _, err := absent.AbortCrossTransactionForCapacity(tx, crossCapacityProof{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if err := n.hosts[0][0].Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := n.hosts[0][0].ReadCrossCapacityConflict(p, [16]byte{1}); !errors.Is(err, replica.ErrStopped) {
		t.Fatal(err)
	}
}

func crossInstallFollowerSnapshot(t *testing.T, transport *declaredSnapshotTransport, droppedAt uint64) {
	t.Helper()
	g := transport.g
	cut := g.drivers[0].Applied()
	if err := g.stores[0].PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	first, err := g.stores[0].FirstIndex()
	if err != nil || first <= droppedAt {
		t.Fatal("snapshot not required after log reclaim", first, droppedAt, err)
	}
	transport.dropFollower = false
	pages := 0
	for range 20 {
		out, err := g.drivers[0].Tick()
		if err != nil {
			t.Fatal(err)
		}
		transport.enqueue(out)
		transport.drain()
		if transport.send != nil {
			break
		}
		offers, err := g.drivers[0].SnapshotSends()
		if err != nil {
			t.Fatal(err)
		}
		for _, send := range offers {
			complete := false
			for !complete && pages < 128 {
				complete, err = send.Build(t.Context(), raftlog.ReadBudget{Rows: 8, Bytes: 64 << 10})
				pages++
				if err != nil {
					t.Fatal(err)
				}
			}
			if !complete {
				t.Fatal("capacity snapshot manifest page cap")
			}
			if err := g.drivers[0].SealSnapshotSend(send); err != nil {
				t.Fatal(err)
			}
		}
	}
	if transport.send == nil {
		t.Fatal("actual capacity-ABORT MsgSnap required")
	}
	manifest, err := transport.send.Manifest()
	if err != nil || manifest.Version != 5 || manifest.Contract.Version != 3 || manifest.Index != cut {
		t.Fatal(manifest, err)
	}
	receiver, err := g.stores[2].BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Abort()
	complete := false
	for !complete && pages < 128 {
		chunk, err := transport.send.Next(t.Context(), raftlog.ReadBudget{Rows: 8, Bytes: 64 << 10})
		pages++
		if err != nil {
			t.Fatal(err)
		}
		if err := receiver.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
		complete = chunk.Final
	}
	if !complete {
		t.Fatal("capacity snapshot stream page cap")
	}
	if err := receiver.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared, err := receiver.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if g.drivers[2].Applied() != droppedAt || g.stores[2].ApplicationGeneration() != 1 {
		t.Fatal("dormant capacity snapshot activated")
	}
	activated, err := g.drivers[2].StepApplicationSnapshot(transport.packet, prepared)
	if err != nil {
		t.Fatal(err)
	}
	if g.drivers[2].Applied() != cut || g.stores[2].ApplicationGeneration() != 2 {
		t.Fatal("capacity snapshot bank not activated")
	}
	feedback, err := g.drivers[0].ReportSnapshotSend(transport.send, true)
	if err != nil {
		t.Fatal(err)
	}
	transport.send, transport.packet = nil, replica.Packet{}
	transport.enqueue(feedback)
	transport.enqueue(activated)
	transport.drain()
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if err := receiver.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestCrossPartitionCapacityAbortCauseAndPreparedOwnerSurviveFollowerSnapshotAndReopen(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	ids := crossRequestsWithLoser(n, 0)
	a, aRegister := crossPlanAt(n, 0, ids[0], ops, claims, 24)
	aRegistered := n.crossSubmit(0, aRegister)
	aRegistration := n.crossReadPhase(0, aRegistered.tx, crossRegister)
	_, bRegister := crossPlanAt(n, 1, ids[1], ops, claims, 25)
	bRegistered := n.crossSubmit(1, bRegister)
	first, err := n.hosts[0][0].PrepareCrossParticipant(a, 3, aRegistration, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := n.crossSubmit(0, first); !got.yes {
		t.Fatal(got)
	}
	firstVote := n.crossReadPhase(0, aRegistered.tx, crossPrepare)
	blocked, err := n.hosts[0][0].PrepareCrossParticipant(a, 8, aRegistration, []crossProof{firstVote})
	if err != nil {
		t.Fatal(err)
	}
	proof := n.capacityRead(1, blocked)
	abort, _, err := n.hosts[0][0].AbortCrossTransactionForCapacity(a, proof)
	if err != nil {
		t.Fatal(err)
	}
	g := partitionSnapshotGroup(n, 0)
	droppedAt := g.drivers[2].Applied()
	transport := &declaredSnapshotTransport{t: t, g: g, dropFollower: true, queue: make([]replica.Packet, 0, 128)}
	event, err := n.hosts[0][0].Submit(abort)
	if err != nil {
		t.Fatal(err)
	}
	transport.enqueue(replica.Output{Packets: event.packets, SnapshotSends: event.snapshotSends})
	transport.drain()
	crossInstallFollowerSnapshot(t, transport, droppedAt)
	for _, host := range n.hosts[0] {
		view, err := host.machine.store.ApplicationView(host.driver.Applied())
		if err != nil {
			t.Fatal(err)
		}
		cause, found, _, err := view.GetControl(t.Context(), crossControlKey(host.machine.ns, aRegistered.tx, 'A'), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
		want, encodeErr := encodeCrossCapacityObservation(proof.observation)
		if err != nil || encodeErr != nil || !found || !bytes.Equal(cause.Value, want) {
			t.Fatal("snapshot changed exact capacity-ABORT cause", found, err)
		}
		if err := view.Close(); err != nil {
			t.Fatal(err)
		}
		crossAcceptanceState(t, host, aRegistered.tx, true)
	}
	crossReopenAll(t, n)
	aDecision := n.crossReadPhase(0, aRegistered.tx, crossDecide)
	if aDecision.status.commit {
		t.Fatal("reopen reversed ABORT")
	}
	crossResolveAt(n, 0, aDecision, 0)
	bRegistration := n.crossReadPhase(1, bRegistered.tx, crossRegister)
	b, err := n.hosts[1][0].RecoverCrossTransaction(bRegistration)
	if err != nil {
		t.Fatal(err)
	}
	var votes []crossProof
	for group, partition := range []uint64{3, 8} {
		p, err := n.hosts[1][0].PrepareCrossParticipant(b, partition, bRegistration, votes)
		if err != nil {
			t.Fatal(err)
		}
		if got := n.crossSubmit(group, p); !got.yes {
			t.Fatal(got)
		}
		votes = append(votes, n.crossReadPhase(group, bRegistered.tx, crossPrepare))
	}
	commit, err := n.hosts[1][0].DecideCrossTransaction(b, votes)
	if err != nil {
		t.Fatal(err)
	}
	if got := n.crossSubmit(1, commit); !got.commit {
		t.Fatal(got)
	}
	crossResolveAt(n, 1, n.crossReadPhase(1, bRegistered.tx, crossDecide), 0, 1)
	crossReopenAll(t, n)
	for member := range 3 {
		v, close := crossView(t, n, member, [2]uint64{})
		edge, err := v.Entity(t.Context(), 3)
		close()
		if err != nil || !edge.Found {
			t.Fatal("winner lost after capacity snapshot/reopen", member, edge, err)
		}
	}
}

func TestCrossPartitionQueuedCommitAndCapacityAbortKeepFirstDecisionImmutable(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, policy := range []string{"default", "bytes", "records"} {
		for _, abortFirst := range []bool{false, true} {
			t.Run(fmt.Sprint(policy, "-abort-first-", abortFirst), func(t *testing.T) {
				n := newCrossPartitionNetworkWithPolicy(t, func(p *raftlog.ApplicationPolicy) {
					switch policy {
					case "bytes":
						p.MaxInstallBytes = 128 << 10
						p.RetainedApplicationBytes = 384 << 10
					case "records":
						p.RetainedApplicationRecords = 768
					}
				})
				ops, claims := crossEdgeFixture(t, n)
				ids := crossRequestsWithLoser(n, 1)
				badClaims := append([]freshBinding(nil), claims...)
				badClaims[0].grant.sequence = 2
				a, aRegister := crossPlanAt(n, 0, ids[0], ops, badClaims, 24)
				aRegistered := n.crossSubmit(0, aRegister)
				aRegistration := n.crossReadPhase(0, aRegistered.tx, crossRegister)
				b, bRegister := crossPlanAt(n, 1, ids[1], ops, claims, 25)
				bRegistered := n.crossSubmit(1, bRegister)
				bRegistration := n.crossReadPhase(1, bRegistered.tx, crossRegister)
				bFirst, err := n.hosts[1][0].PrepareCrossParticipant(b, 3, bRegistration, nil)
				if err != nil {
					t.Fatal(err)
				}
				proof := n.capacityRead(0, bFirst)
				abort, _, err := n.hosts[1][0].AbortCrossTransactionForCapacity(b, proof)
				if err != nil {
					t.Fatal(err)
				}
				aFirst, err := n.hosts[0][0].PrepareCrossParticipant(a, 3, aRegistration, nil)
				if err != nil {
					t.Fatal(err)
				}
				if no := n.crossSubmit(0, aFirst); no.yes {
					t.Fatal(no)
				}
				aAbort, err := n.hosts[0][0].DecideCrossTransaction(a, []crossProof{n.crossReadPhase(0, aRegistered.tx, crossPrepare)})
				if err != nil {
					t.Fatal(err)
				}
				n.crossSubmit(0, aAbort)
				crossResolveAt(n, 0, n.crossReadPhase(0, aRegistered.tx, crossDecide), 0)
				if yes := n.crossSubmit(0, bFirst); !yes.yes {
					t.Fatal(yes)
				}
				firstVote := n.crossReadPhase(0, bRegistered.tx, crossPrepare)
				bSecond, err := n.hosts[1][0].PrepareCrossParticipant(b, 8, bRegistration, []crossProof{firstVote})
				if err != nil {
					t.Fatal(err)
				}
				if yes := n.crossSubmit(1, bSecond); !yes.yes {
					t.Fatal(yes)
				}
				commit, err := n.hosts[1][0].DecideCrossTransaction(b, []crossProof{firstVote, n.crossReadPhase(1, bRegistered.tx, crossPrepare)})
				if err != nil {
					t.Fatal(err)
				}
				first, second := commit, abort
				if abortFirst {
					first, second = abort, commit
				}
				if policy != "default" {
					crossFillOrdinaryWithReason(n, 1, ops[0], reasonPreparedConflict)
				}
				e1, err := n.hosts[1][0].Submit(first)
				if err != nil {
					t.Fatal(err)
				}
				e2, err := n.hosts[1][0].Submit(second)
				if err != nil && !errors.Is(err, raftlog.ErrLimit) && !errors.Is(err, raftlog.ErrInvalid) {
					t.Fatal("competing decision stopped healthy driver", err)
				}
				t.Logf("first/second submitted at %s capacity: second=%v, before-apply=%d", policy, err, n.hosts[1][0].driver.Applied())
				e1.packets = append(e1.packets, e2.packets...)
				n.crossPump(1, e1)
				decision := n.crossReadPhase(1, bRegistered.tx, crossDecide)
				if decision.status.commit == abortFirst {
					t.Fatal("second queued decision replaced first", decision.status)
				}
				crossResolveAt(n, 1, decision, 0, 1)
				for member := range 3 {
					v, close := crossView(t, n, member, [2]uint64{})
					edge, err := v.Entity(t.Context(), 3)
					close()
					if err != nil || edge.Found != !abortFirst {
						t.Fatal("queued decision produced wrong graph effects", edge, err)
					}
				}
			})
		}
	}
}
