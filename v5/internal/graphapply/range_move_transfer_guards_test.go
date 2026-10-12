package graphapply

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// These guards close missing evidence in PLAN §5.4. They do not claim an
// observed production defect: every write, floor, capture and original below
// comes from the actual native Host/Driver and its six voters.
func rangeMoveTransferData(t *testing.T, h *allocationHost, key []byte) ([]byte, bool) {
	t.Helper()
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	row, found, err := view.Get(t.Context(), key, min(rangeMoveReplayRecordBytes+len(key), view.ReadLimits().Bytes))
	err = errors.Join(err, view.Close())
	if err != nil || found && row.Deleted {
		t.Fatal("native original data read", found, err)
	}
	return row.Value, found
}

func rangeMoveTransferControl(t *testing.T, h *allocationHost, key []byte) (raftlog.ApplicationControlRecord, bool) {
	t.Helper()
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	row, found, _, err := view.GetControl(t.Context(), key, raftlog.ReadBudget{Rows: min(2, view.ReadLimits().Rows), Bytes: min(2*rangeMoveReplayRecordBytes+4096, view.ReadLimits().Bytes)})
	err = errors.Join(err, view.Close())
	if err != nil {
		t.Fatal("native original control read", err)
	}
	return row, found
}

func rangeMoveTransferInstalledFloor(t *testing.T, h *allocationHost, configuration [32]byte) uint64 {
	t.Helper()
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	base, err := view.RootBounded(t.Context(), 172)
	if err != nil {
		_ = view.Close()
		t.Fatal(err)
	}
	output, err := graphstate.NewOutputBudget(1 << 20)
	if err != nil {
		_ = view.Close()
		t.Fatal(err)
	}
	q := reader{ctx: t.Context(), view: view, ns: h.machine.ns, base: base, limits: h.machine.limits.allocation, arena: output}
	floor, err := readPartitionRoundFloor(&q, configuration)
	err = errors.Join(err, view.Close())
	if err != nil {
		t.Fatal("actual persisted installation floor", err)
	}
	return floor
}

func rangeMoveTransferCapturedFloor(t *testing.T, h *allocationHost, move [16]byte) uint64 {
	t.Helper()
	capture, found := rangeMoveTransferControl(t, h, rangeMoveControlKey(h.machine.ns, move, 'C'))
	if !found {
		t.Fatal("completed move lost genuine capture")
	}
	root, floor, err := moveCaptureRoot(capture.Value, h.machine.scope())
	if err != nil || root.Index == 0 || root.Index >= capture.Index {
		t.Fatal("capture does not identify its genuine predecessor", root.Index, capture.Index, err)
	}
	return floor
}

func TestRangeMoveSuccessfulHandoffCarriesBothCapturedClockFloorsBeforeFresh(t *testing.T) {
	// Each owner is independently the maximum. Ignoring either capture must
	// fail one case; a target-only maximum would not detect a dropped source floor.
	for _, tc := range []struct {
		name   string
		floors [2]uint64
	}{
		{"source-dominates", [2]uint64{1400, 700}},
		{"target-dominates", [2]uint64{700, 1400}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, coordinator, intent, _, _ := rangeMoveCoordinatorFixture(t, 48)
			transport := &certifiedTestTransport{n: n}
			for group, floor := range tc.floors {
				h := n.hosts[group][0]
				if installed := rangeMoveTransferInstalledFloor(t, h, intent.publication.Configuration); installed >= floor {
					t.Fatal("high-floor fixture failed to dominate native installed state", group, installed, floor)
				}
				proposal, err := h.PrepareGraphFence(floor)
				if err != nil {
					t.Fatal("native high-floor admission", err)
				}
				result, err := transport.Submit(t.Context(), h, proposal)
				if err != nil || result.phase != graphCutFence || result.status != graphCutApplied || result.round != floor {
					t.Fatal("genuine quorum fence did not install requested floor", result, err)
				}
			}
			done, err := runRangeMoveToCompletion(t, coordinator, intent)
			if err != nil || !done.committed || done.source.phase != rangeMoveFinish || done.target.phase != rangeMoveFinish || done.decision.phase != rangeMoveDecide {
				t.Fatal("floor guard requires a genuinely completed handoff", done, err)
			}
			// This is deliberately the first observation after Finish, on every
			// target voter. No Fresh/certificate operation can repair a missed floor.
			for member, target := range n.hosts[1] {
				if floor := rangeMoveTransferInstalledFloor(t, target, intent.publication.Configuration); floor < done.decision.round {
					t.Fatal("Finish returned before target persisted handoff floor", member, floor, done.decision.round)
				}
			}
			captured := [2]uint64{
				rangeMoveTransferCapturedFloor(t, n.hosts[0][0], done.resume.move),
				rangeMoveTransferCapturedFloor(t, n.hosts[1][0], done.resume.move),
			}
			if captured != tc.floors {
				t.Fatal("move failed to capture the two genuinely installed high floors", captured, tc.floors)
			}
			if done.decision.floor != max(captured[0], captured[1]) || done.decision.round <= captured[0] || done.decision.round <= captured[1] {
				t.Fatal("handoff decision did not strictly advance both captured clocks", done.decision, captured)
			}
			window, err := temporal.All(graphGenesisBinding(t).Axis())
			if err != nil {
				t.Fatal(err)
			}
			id := n.requestID()
			proposal, err := n.hosts[1][0].PreparePartitionGraphOperations(id, []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: window, Name: "first-target-decision"}}, 1401, nil)
			if err != nil {
				t.Fatal("first native target decision admission", err)
			}
			outcome := n.submitPartitionGraph(1, proposal, reasonNone)
			for member, target := range n.hosts[1] {
				wire, found := rangeMoveTransferData(t, target, graphLocalReceiptKey(target.machine.ns, [16]byte(id)))
				receipt, err := decodeGraphLocalReceipt(wire)
				if err != nil || !found || receipt.scope != intent.target || receipt.identity != [16]byte(id) || receipt.index != outcome.index || receipt.round <= done.decision.round {
					t.Fatal("first post-move target decision reused the handoff clock", member, receipt, done.decision.round, err)
				}
				if floor := rangeMoveTransferInstalledFloor(t, target, intent.publication.Configuration); floor != receipt.round {
					t.Fatal("native target decision did not persist its assigned clock", member, floor, receipt.round)
				}
			}
		})
	}
}

func TestRangeMoveCompletedHandoffCopiesNativeCrossDecisionsAndDedupWithKeeperOffline(t *testing.T) {
	n, _, intent, _, transports := rangeMoveCoordinatorFixture(t, 64)
	source, target := n.hosts[0][0], n.hosts[1][0]
	origin := source.machine.ns
	window, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	collisionID, targetOnlyID := requestID{121}, requestID{122}
	write := func(group int, id requestID, name string) outcome {
		h := n.hosts[group][0]
		owner := graphstate.EntityID(1)
		if group == 1 {
			owner = 17
		}
		proposal, err := h.PreparePartitionGraphOperations(id, []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: owner, Life: graphstate.LifeID(owner + 1), Scope: window, Name: name}}, 121, nil)
		if err != nil {
			t.Fatal("genuine namespace-qualified dedup original", err)
		}
		return n.submitPartitionGraph(group, proposal, reasonNone)
	}
	sourceOutcome := write(0, collisionID, "source-dedup-original")
	write(1, collisionID, "independent-target-dedup-original")
	write(1, targetOnlyID, "target-only-original")
	// Existing IDs need no fresh claims. Both nonempty participants prepare,
	// COMMIT and resolve through the actual native protocol before the move.
	tx, registration := n.crossPlan([]graphstate.Operation{
		{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: window, Name: "source-cross-original"},
		{Kind: graphstate.AddLabel, Owner: 17, Life: 18, Scope: window, Name: "target-cross-original"},
	}, nil, 123)
	decision := n.crossCommit(tx, registration)
	n.crossResolve(decision)
	if decision.status.phase != crossDecide || !decision.status.commit || decision.status.round == 0 {
		t.Fatal("cross-copy guard has no genuine native COMMIT", decision.status)
	}
	type originalRecord struct {
		name    string
		key     []byte
		control bool
		value   []byte
	}
	originals := []originalRecord{
		{name: "ga4 source dedup outcome", key: declaredRequestKey(origin, graphOperations, [16]byte(collisionID))},
		{name: "ga16 source local receipt", key: graphLocalReceiptKey(origin, [16]byte(collisionID))},
		{name: "ga11 resolved cross status", key: crossKey(origin, decision.status.tx)},
		{name: "GP D immutable COMMIT", key: crossControlKey(origin, decision.status.tx, 'D'), control: true},
		{name: "GP R original manifest", key: crossControlKey(origin, decision.status.tx, 'R'), control: true},
		{name: "GP P native YES", key: crossControlKey(origin, decision.status.tx, 'P'), control: true},
		{name: "GP K prepared kernel", key: crossControlKey(origin, decision.status.tx, 'K'), control: true},
	}
	for i := range originals {
		var found bool
		if originals[i].control {
			row, present := rangeMoveTransferControl(t, source, originals[i].key)
			originals[i].value, found = bytes.Clone(row.Value), present
		} else {
			originals[i].value, found = rangeMoveTransferData(t, source, originals[i].key)
		}
		if !found || len(originals[i].value) == 0 {
			t.Fatal("genuine native source original missing", originals[i].name)
		}
	}
	storedStatus, err := decodeCrossStatus(originals[2].value)
	if err != nil || storedStatus.ns != origin || storedStatus.tx != decision.status.tx || storedStatus.phase != crossResolve || !storedStatus.commit || storedStatus.round != decision.status.round {
		t.Fatal("source ga11 did not retain actual cross resolution", storedStatus, err)
	}
	storedDecision, err := decodeCrossStatus(originals[3].value)
	if err != nil || storedDecision != decision.status || !bytes.Equal(originals[4].value, tx.manifest) {
		t.Fatal("source controls do not retain actual COMMIT and original manifest", storedDecision, err)
	}
	// Capture the target's real independent local collision and its same-TxID
	// participant status. A participant status is not a request-ID collision.
	targetOriginals := []originalRecord{
		{key: declaredRequestKey(target.machine.ns, graphOperations, [16]byte(collisionID))},
		{key: graphLocalReceiptKey(target.machine.ns, [16]byte(collisionID))},
		{key: crossKey(target.machine.ns, decision.status.tx)},
		{key: declaredRequestKey(target.machine.ns, graphOperations, [16]byte(targetOnlyID))},
		{key: graphLocalReceiptKey(target.machine.ns, [16]byte(targetOnlyID))},
	}
	for i := range targetOriginals {
		var found bool
		targetOriginals[i].value, found = rangeMoveTransferData(t, target, targetOriginals[i].key)
		if !found || len(targetOriginals[i].value) == 0 {
			t.Fatal("independent native target original missing", i)
		}
	}
	if bytes.Equal(originals[0].value, targetOriginals[0].value) || bytes.Equal(originals[1].value, targetOriginals[1].value) || bytes.Equal(originals[2].value, targetOriginals[2].value) {
		t.Fatal("source and target native original coordinates did not diverge")
	}
	phantomKey := graphLocalReceiptKey(origin, [16]byte(targetOnlyID))
	if _, found := rangeMoveTransferData(t, source, phantomKey); found {
		t.Fatal("target-only collision manufactured source receipt before move")
	}
	sourceWire := &rangeMoveKeeperWire{graphPeerWireTransport: transports[3]}
	transports[3] = sourceWire
	coordinator, err := newRangeMoveCoordinator(source, transports)
	if err != nil {
		t.Fatal(err)
	}
	done, err := runRangeMoveToCompletion(t, coordinator, intent)
	if err != nil || !done.committed || done.source.phase != rangeMoveFinish || done.target.phase != rangeMoveFinish {
		t.Fatal("cross-copy guard requires actual completed target handoff", done, err)
	}
	// The destination's actual completed handoff must answer the frozen originals
	// with the complete original source endpoint offline, not a live keeper fallback.
	sourceWire.allUnavailable = true
	sourceCalls := sourceWire.calls
	for _, original := range originals {
		got, err := coordinator.LookupOriginal(t.Context(), done.resume, origin, original.control, original.key)
		if err != nil || !got.found || got.origin != origin || !bytes.Equal(got.key, original.key) || !bytes.Equal(got.value, original.value) || got.evaluationIndex != target.driver.Applied() || sourceWire.calls != sourceCalls {
			t.Fatal("completed target lost exact cross/dedup original or needed offline keeper", original.name, got, err)
		}
		if original.name == "ga4 source dedup outcome" && got.originalIndex != sourceOutcome.index {
			t.Fatal("copied source dedup lost its original native outcome coordinate", got.originalIndex, sourceOutcome.index)
		}
		for member, voter := range n.hosts[1] {
			_, family, id, valid := rangeMoveReplayOriginal(original.key, original.control)
			if !valid {
				t.Fatal("invalid genuine original key", original.name)
			}
			value, found := rangeMoveTransferData(t, voter, rangeMoveReplayKey(voter.machine.ns, origin.partition, id, family))
			if !found || !bytes.Equal(value, original.value) {
				t.Fatal("native target voter omitted or changed copied original", member, original.name)
			}
		}
	}
	for member, voter := range n.hosts[1] {
		for _, original := range targetOriginals {
			value, found := rangeMoveTransferData(t, voter, original.key)
			if !found || !bytes.Equal(value, original.value) {
				t.Fatal("source copy replaced independent native target original", member, original.key)
			}
		}
	}
	// A same raw receipt ID present only at target is still missing at source.
	// Offline keeper authority must leave the result pending and empty, rather
	// than substituting the target record or declaring a completed absence.
	got, err := coordinator.LookupOriginal(t.Context(), done.resume, origin, false, phantomKey)
	if !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || !reflect.DeepEqual(got, rangeMoveOriginal{}) || sourceWire.calls != sourceCalls+1 {
		t.Fatal("offline source miss became target collision or phantom completed original", got, err)
	}
}
