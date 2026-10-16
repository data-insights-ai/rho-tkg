package graphapply

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type rangeMoveCrossFenceState struct {
	image       []byte
	lock        [16]byte
	owner       raftlog.ApplicationRecoveryOwner
	reservation raftlog.ApplicationRecoveryReservation
}

func rangeMoveCrossFenceStates(t *testing.T, n *allocationTestNetwork) [6]rangeMoveCrossFenceState {
	t.Helper()
	var result [6]rangeMoveCrossFenceState
	for group := range 2 {
		for member, h := range n.hosts[group] {
			_, image, err := h.machine.store.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			q := rangeMoveReplayNativeReader(t, h, 64<<20)
			lock, err := crossLock(q)
			if err != nil {
				t.Fatal(err)
			}
			result[3*group+member] = rangeMoveCrossFenceState{image, lock, rangeMoveAdmissionOwner(t, h), rangeMoveAdmissionReservation(t, h)}
			if err := q.view.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	return result
}

func rangeMoveCrossFenceCells(t *testing.T, n *allocationTestNetwork, member int, label string) [][]byte {
	t.Helper()
	view, close := crossView(t, n, member, [2]uint64{})
	defer close()
	window, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	var result [][]byte
	for _, owner := range []graphstate.EntityID{1, 17} {
		for _, key := range []graphstate.ComponentKey{{Owner: owner, Kind: graphstate.Presence}, {Owner: owner, Life: graphstate.LifeID(owner + 1), Kind: graphstate.Label, Name: label}} {
			page, err := view.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: key, Window: window}, 0, raftlog.ReadBudget{Rows: 64, Bytes: 64 << 10})
			if err != nil || !page.Complete || page.Next != 0 {
				t.Fatal("complete genuine cell state", err)
			}
			wire, err := state.AppendState(nil, page.Data, state.CodecLimits{State: defaultMaterializerLimits().graph.Planner.Component, MaxEncodedBytes: 64 << 10})
			if err != nil {
				t.Fatal(err)
			}
			result = append(result, wire)
		}
	}
	return result
}

func rangeMoveCrossFenceControl(t *testing.T, n *allocationTestNetwork, group int, p allocationProposal) crossStatus {
	t.Helper()
	before := rangeMoveCrossFenceStates(t, n)
	status := n.crossSubmit(group, p)
	after := rangeMoveCrossFenceStates(t, n)
	for index := range before {
		if !bytes.Equal(before[index].image, after[index].image) {
			t.Fatal("cross control changed graph root", index)
		}
	}
	for _, h := range n.hosts[group] {
		cdc, err := h.machine.store.ApplicationRecord(t.Context(), h.driver.Applied(), false, h.machine.store.ApplicationLimits().MaxChangeBytes)
		if err != nil || len(cdc) != 0 {
			t.Fatal("cross control emitted business CDC", err)
		}
	}
	return status
}

func rangeMoveCrossFenceVotes(t *testing.T, n *allocationTestNetwork, coordinator int, tx crossTransaction, registration crossProof, previous []crossProof) []crossProof {
	t.Helper()
	votes := append([]crossProof(nil), previous...)
	for group, partition := range []uint64{3, 8} {
		if group < len(previous) {
			continue
		}
		proposal, err := n.hosts[coordinator][0].PrepareCrossParticipant(tx, partition, registration, votes)
		if err != nil {
			t.Fatal(err)
		}
		status := rangeMoveCrossFenceControl(t, n, group, proposal)
		if !status.yes || status.phase != crossPrepare || status.tx != registration.status.tx || status.digest != tx.digest {
			t.Fatal("genuine old prepare did not vote YES", status)
		}
		votes = append(votes, n.crossReadPhase(group, status.tx, crossPrepare))
	}
	return votes
}

func rangeMoveCrossFenceResume(t *testing.T, c *rangeMoveCoordinator, resume rangeMoveResume) rangeMoveResult {
	t.Helper()
	result := rangeMoveKeeperResume(t, c, resume)
	if result.resume != resume {
		t.Fatal("settled old work replaced original move continuation identity")
	}
	return result
}

func TestRangeMoveWaitsForOriginalRegisteredAndPreparedCrossSettlement(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, phase := range []string{"registered-only", "both-YES"} {
		t.Run(phase, func(t *testing.T) {
			n, c, intent, wires, _ := rangeMoveCoordinatorFixture(t, 64)
			window, err := temporal.All(graphGenesisBinding(t).Axis())
			if err != nil {
				t.Fatal(err)
			}
			label := "settled-before-capture"
			ops := []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: window, Name: label}, {Kind: graphstate.AddLabel, Owner: 17, Life: 18, Scope: window, Name: label}}
			tx, reg := n.crossPlan(ops, nil, 141)
			registered := rangeMoveCrossFenceControl(t, n, 0, reg)
			registration := n.crossReadPhase(0, registered.tx, crossRegister)
			if registered.phase != crossRegister || registered.digest != tx.digest {
				t.Fatal("not actual original registration", registered)
			}
			var votes []crossProof
			if phase == "both-YES" {
				votes = rangeMoveCrossFenceVotes(t, n, 0, tx, registration, nil)
			}
			before := rangeMoveCrossFenceStates(t, n)
			cellsBefore := rangeMoveCrossFenceCells(t, n, 0, label)
			if before[0].owner.ID != crossRecoveryID(n.hosts[0][0].machine.ns, registered.tx) || before[0].owner.Digest != tx.digest {
				t.Fatal("fixture lacks original protected cross owner")
			}
			pending, err := c.Run(t.Context(), intent)
			if !errors.Is(err, errRangeMovePending) || pending.committed || pending.resume.request != intent.id {
				t.Fatal("unsettled original work allowed completed move", pending, err)
			}
			if wires[0].captures+wires[1].captures != 0 || !reflect.DeepEqual(rangeMoveCrossFenceStates(t, n), before) || !reflect.DeepEqual(rangeMoveCrossFenceCells(t, n, 0, label), cellsBefore) {
				t.Fatal("blocked move captured, changed cells, or replaced original owner/credits/locks")
			}
			// Pending carries the original caller intent in this coordinator. No
			// durable move registration or capture is manufactured from refusal.
			for group := range 2 {
				observed := rangeMoveAdmissionRead(t, n, group, pending.resume.move)
				if observed.observation.status.phase != 0 || len(observed.observation.intent)+len(observed.observation.progress) != 0 {
					t.Fatal("unsettled cross owner minted move registration", group)
				}
			}
			votes = rangeMoveCrossFenceVotes(t, n, 0, tx, registration, votes)
			decide, err := n.hosts[0][0].DecideCrossTransaction(tx, votes)
			if err != nil {
				t.Fatal(err)
			}
			committed := rangeMoveCrossFenceControl(t, n, 0, decide)
			if !committed.commit || committed.tx != registered.tx || committed.digest != tx.digest {
				t.Fatal("settlement replaced original TxID/decision", committed)
			}
			decision := n.crossReadPhase(0, registered.tx, crossDecide)
			var resolves [2]allocationProposal
			var originalCDC [2][]byte
			var originalOutcomes [2]crossStatus
			seen := make(map[graphstate.EntityID]bool, 2)
			for group, partition := range []uint64{3, 8} {
				resolves[group], err = n.hosts[0][0].ResolveCrossParticipant(partition, decision)
				if err != nil {
					t.Fatal(err)
				}
				resolved := n.crossSubmit(group, resolves[group])
				originalOutcomes[group] = resolved
				if resolved.phase != crossResolve || !resolved.commit || resolved.tx != registered.tx || resolved.round != decision.status.round || resolved.digest != tx.digest {
					t.Fatal("original settlement diverged", resolved)
				}
				for _, h := range n.hosts[group] {
					cdc, err := h.machine.store.ApplicationRecord(t.Context(), resolved.index, false, h.machine.store.ApplicationLimits().MaxChangeBytes)
					if err != nil || len(cdc) == 0 {
						t.Fatal("original COMMIT omitted CDC", err)
					}
					if originalCDC[group] == nil {
						originalCDC[group] = cdc
					} else if !bytes.Equal(originalCDC[group], cdc) {
						t.Fatal("original CDC differs across voters")
					}
				}
				h := n.hosts[group][0]
				out := outcome{ns: h.machine.ns, kind: graphOperations, identity: resolved.tx, hash: resolved.digest, index: resolved.index, disposition: applied}
				changes, err := decodeTypedChangeEnvelope(originalCDC[group], out, h.machine.limits, true)
				if err != nil || len(changes.entities)+len(changes.lives)+len(changes.values) != 0 {
					t.Fatal("label settlement gained phantom records", err)
				}
				for _, component := range changes.groups {
					key := component.Key
					if key.Kind != graphstate.Label || key.Name != label || key.Life != graphstate.LifeID(key.Owner+1) || (key.Owner != 1 && key.Owner != 17) || seen[key.Owner] || len(component.Changes) != 1 {
						t.Fatal("original CDC exact component set", key)
					}
					seen[key.Owner] = true
					change := component.Changes[0]
					if change.Before().Present() || !change.After().Present() || change.After().Revision().ID() != decision.status.round || change.After().Revision().Provenance() != 141 {
						t.Fatal("original CDC cells/revision changed")
					}
				}
			}
			if len(seen) != 2 || !seen[1] || !seen[17] {
				t.Fatal("partial original cross CDC", seen)
			}
			settledCells := rangeMoveCrossFenceCells(t, n, 0, label)
			for group := range 2 {
				if replay := rangeMoveCrossFenceControl(t, n, group, resolves[group]); replay != originalOutcomes[group] {
					t.Fatal("Resolve replay replaced original outcome", replay, originalOutcomes[group])
				}
			}
			if !reflect.DeepEqual(rangeMoveCrossFenceCells(t, n, 0, label), settledCells) {
				t.Fatal("exact Resolve replay reapplied original cells")
			}
			done := rangeMoveCrossFenceResume(t, c, pending.resume)
			if wires[0].captures+wires[1].captures != 2 || done.resume != pending.resume {
				t.Fatal("resume lost one original capture per source")
			}
			for member := range 3 {
				if !reflect.DeepEqual(rangeMoveCrossFenceCells(t, n, member, label), settledCells) {
					t.Fatal("handoff changed settled original cells/revisions", member)
				}
				view, close := crossView(t, n, member, [2]uint64{})
				assertCertifiedProjection(t, view, 1, true, []string{label})
				assertCertifiedProjection(t, view, 17, true, []string{label})
				close()
			}
			for group := range 2 {
				if replay := rangeMoveCrossFenceControl(t, n, group, resolves[group]); replay != originalOutcomes[group] {
					t.Fatal("post-transfer Resolve replay replaced original outcome", replay)
				}
				for _, h := range n.hosts[group] {
					cdc, err := h.machine.store.ApplicationRecord(t.Context(), originalOutcomes[group].index, false, h.machine.store.ApplicationLimits().MaxChangeBytes)
					if err != nil || !bytes.Equal(cdc, originalCDC[group]) {
						t.Fatal("handoff replaced original retained CDC", err)
					}
				}
			}
			if original := n.crossReadPhase(0, registered.tx, crossDecide); original.status != decision.status {
				t.Fatal("move replaced immutable original decision")
			}
		})
	}
}

func TestRangeMoveRejectsFirstDelayedCrossPrepareAndSettlesInertLateRegistration(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, c, intent, _, _ := rangeMoveCoordinatorFixture(t, 64)
	window, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	lateOps := []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: window, Name: "must-never-appear"}, {Kind: graphstate.AddLabel, Owner: 17, Life: 18, Scope: window, Name: "must-never-appear"}}
	ids := crossRequestsWithLoser(n, 0)
	old, oldRegister := crossPlanAt(n, 0, ids[0], lateOps, nil, 142)
	lateTx, lateRegister := n.crossPlan(lateOps, nil, 144) // Never submitted before transfer.
	// This genuine target registration supplies the capacity owner. Its invalid
	// fresh grant later produces a real native NO/ABORT without changing cells;
	// only the handoff then invalidates the old Prepare's captured graph base.
	handle := n.hosts[0][0].recipients[[16]byte{1}]
	if handle == nil {
		t.Fatal("fixture lacks actual source recipient")
	}
	blockerOps := []graphstate.Operation{{Kind: graphstate.CreateRelationship, Owner: 3, Life: 4, Scope: window, Record: graphstate.EntityRecord{Type: "blocked-edge", Source: 1, Target: 17, Mode: graphstate.LifeBound, Interpretation: graphstate.InterpretationAssertedRelation, TemporalRole: graphstate.TemporalRoleOccurrenceTime}, Binding: graphstate.LifeRecord{SourceLife: 2, TargetLife: 18}}}
	claims := []freshBinding{{role: entityBinding, id: 3, grant: grantReference{session: handle.session, sequence: 2}}, {role: lifeBinding, owner: 3, id: 4, grant: grantReference{session: handle.session, sequence: 2}}}
	blocker, blockerRegister := crossPlanAt(n, 1, ids[1], blockerOps, claims, 143)
	blockerStatus := rangeMoveCrossFenceControl(t, n, 1, blockerRegister)
	blockerRegistration := n.crossReadPhase(1, blockerStatus.tx, crossRegister)
	oldStatus := rangeMoveCrossFenceControl(t, n, 0, oldRegister)
	registration := n.crossReadPhase(0, oldStatus.tx, crossRegister)
	first, err := n.hosts[0][0].PrepareCrossParticipant(old, 3, registration, nil)
	if err != nil {
		t.Fatal(err)
	}
	if status := rangeMoveCrossFenceControl(t, n, 0, first); !status.yes {
		t.Fatal(status)
	}
	yes := n.crossReadPhase(0, oldStatus.tx, crossPrepare)
	latePrepare, err := n.hosts[0][0].PrepareCrossParticipant(old, 8, registration, []crossProof{yes})
	if err != nil {
		t.Fatal(err)
	}
	capacity := n.capacityRead(1, latePrepare)
	abort, existing, err := n.hosts[0][0].AbortCrossTransactionForCapacity(old, capacity)
	if err != nil || existing != (crossStatus{}) {
		t.Fatal("real capacity proof did not authorize original ABORT", existing, err)
	}
	if status := rangeMoveCrossFenceControl(t, n, 0, abort); status.commit || status.round != 0 {
		t.Fatal(status)
	}
	originalAbort := n.crossReadPhase(0, oldStatus.tx, crossDecide)
	resolveSource, err := n.hosts[0][0].ResolveCrossParticipant(3, originalAbort)
	if err != nil {
		t.Fatal(err)
	}
	rangeMoveCrossFenceControl(t, n, 0, resolveSource)
	blockerPrepare, err := n.hosts[1][0].PrepareCrossParticipant(blocker, 3, blockerRegistration, nil)
	if err != nil {
		t.Fatal(err)
	}
	blockerNO := rangeMoveCrossFenceControl(t, n, 0, blockerPrepare)
	if blockerNO.yes || blockerNO.why != reasonInvalid {
		t.Fatal("native unissued grant did not produce genuine NO", blockerNO)
	}
	blockerDecide, err := n.hosts[1][0].DecideCrossTransaction(blocker, []crossProof{n.crossReadPhase(0, blockerStatus.tx, crossPrepare)})
	if err != nil {
		t.Fatal(err)
	}
	if status := rangeMoveCrossFenceControl(t, n, 1, blockerDecide); status.commit {
		t.Fatal(status)
	}
	blockerAbort := n.crossReadPhase(1, blockerStatus.tx, crossDecide)
	for group, partition := range []uint64{3, 8} {
		p, err := n.hosts[1][0].ResolveCrossParticipant(partition, blockerAbort)
		if err != nil {
			t.Fatal(err)
		}
		rangeMoveCrossFenceControl(t, n, group, p)
	}
	beforeMove := rangeMoveCrossFenceCells(t, n, 0, "must-never-appear")
	done, err := runRangeMoveToCompletion(t, c, intent)
	if err != nil || !done.committed {
		t.Fatal("native transfer must complete before late command", done, err)
	}
	beforeLate := rangeMoveCrossFenceCells(t, n, 0, "must-never-appear")
	if !reflect.DeepEqual(beforeLate, beforeMove) {
		t.Fatal("fixture changed cells before delayed Prepare")
	}
	// This is the FIRST delivery to8, not an already-applied Prepare retry.
	// Exact already-completed phases are valid immutable replay, not stale YES.
	no := rangeMoveCrossFenceControl(t, n, 1, latePrepare)
	if no.tx != oldStatus.tx || no.digest != old.digest || no.phase != crossPrepare || no.yes || no.why != reasonReadConflict {
		t.Fatal("first old-base Prepare gained authority after transfer", no)
	}
	for _, snapshot := range rangeMoveCrossFenceStates(t, n) {
		if snapshot.lock != ([16]byte{}) {
			t.Fatal("stale first Prepare acquired YES lock")
		}
	}
	resolveTarget, err := n.hosts[0][0].ResolveCrossParticipant(8, originalAbort)
	if err != nil {
		t.Fatal(err)
	}
	rangeMoveCrossFenceControl(t, n, 1, resolveTarget)
	if original := n.crossReadPhase(0, oldStatus.tx, crossDecide); original.status != originalAbort.status {
		t.Fatal("late first Prepare replaced originally settled ABORT")
	}
	// Registration is inert bookkeeping, not write/prepare authority. The
	// specification does not require it to refuse before validating its first
	// Prepare. This genuine pre-move Register is admitted, then votes stale NO.
	registered := rangeMoveCrossFenceControl(t, n, 0, lateRegister)
	if registered.phase != crossRegister || registered.yes || registered.tx != crossTransactionID(n.hosts[0][0].machine.ns, crossRawRequestKey(t, lateTx)) {
		t.Fatal("late original registration identity changed", registered)
	}
	for _, snapshot := range rangeMoveCrossFenceStates(t, n) {
		if snapshot.lock != ([16]byte{}) {
			t.Fatal("inert late Register acquired YES lock")
		}
	}
	freshRegistration := n.crossReadPhase(0, registered.tx, crossRegister)
	prepare, err := n.hosts[0][0].PrepareCrossParticipant(lateTx, 3, freshRegistration, nil)
	if err != nil {
		t.Fatal(err)
	}
	no = rangeMoveCrossFenceControl(t, n, 0, prepare)
	if no.phase != crossPrepare || no.tx != registered.tx || no.digest != lateTx.digest || no.yes || no.why != reasonReadConflict {
		t.Fatal("inert late Register authorized stale old-owner Prepare", no)
	}
	for _, snapshot := range rangeMoveCrossFenceStates(t, n) {
		if snapshot.lock != ([16]byte{}) {
			t.Fatal("stale Prepare after inert registration acquired YES lock")
		}
	}
	decide, err := n.hosts[0][0].DecideCrossTransaction(lateTx, []crossProof{n.crossReadPhase(0, registered.tx, crossPrepare)})
	if err != nil {
		t.Fatal(err)
	}
	if status := rangeMoveCrossFenceControl(t, n, 0, decide); status.commit {
		t.Fatal("stale old-owner transaction committed", status)
	}
	decision := n.crossReadPhase(0, registered.tx, crossDecide)
	resolve, err := n.hosts[0][0].ResolveCrossParticipant(3, decision)
	if err != nil {
		t.Fatal(err)
	}
	rangeMoveCrossFenceControl(t, n, 0, resolve)
	for _, snapshot := range rangeMoveCrossFenceStates(t, n) {
		if snapshot.lock != ([16]byte{}) || snapshot.owner.ID != ([32]byte{}) {
			t.Fatal("late stale settlement retained lock/completion owner")
		}
	}
	for member := range 3 {
		if !reflect.DeepEqual(rangeMoveCrossFenceCells(t, n, member, "must-never-appear"), beforeLate) {
			t.Fatal("late cross command changed exact cells", member)
		}
		view, close := crossView(t, n, member, [2]uint64{})
		assertCertifiedProjection(t, view, 1, true, nil)
		assertCertifiedProjection(t, view, 17, true, nil)
		if entity, err := view.Entity(t.Context(), 3); err != nil || entity.Found {
			t.Fatal("capacity blocker created phantom relationship", err)
		}
		close()
	}
}
