package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Hold the exact source-produced target proposal after the native coordinator
// consumed a genuine source Fence read authority. Only delivery is delayed;
// this gate never constructs a proof, a status, an intent, or an application view.
type rangeMoveLateFenceGate struct {
	graphPeerWireTransport
	frozen allocationProposal
}

func (g *rangeMoveLateFenceGate) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	request, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	if request.kind == sourceSubmit {
		proposal, err := decodeGraphSourceProposal(request.command)
		if err != nil {
			return nil, err
		}
		command, err := decodeRangeMoveCommand(proposal.wire)
		if err != nil {
			return nil, err
		}
		if command.phase == rangeMoveFence && proposal.target.partition == 8 {
			if len(g.frozen.wire) != 0 {
				return nil, errCorrupt
			}
			g.frozen = proposal
			return nil, io.EOF
		}
	}
	return g.graphPeerWireTransport.RoundTrip(ctx, wire)
}

type rangeMoveLateFenceTargetState struct {
	image     []byte
	placement [3][]byte
	present   [3]bool
}

func rangeMoveLateFenceTargetSnapshot(t *testing.T, h *allocationHost, intent rangeMoveIntent) rangeMoveLateFenceTargetState {
	t.Helper()
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := view.Close(); err != nil {
			t.Error(err)
		}
	}()
	root, err := view.RootBounded(t.Context(), 172)
	if err != nil {
		t.Fatal(err)
	}
	n := graphstore.Namespace{Graph: intent.publication.Graph, Partition: h.machine.ns.partition}
	keys := [][]byte{graphstore.RangePublicationKey(n, intent.publication.RangeID), graphstore.RangeEndKey(n, intent.publication.Last), graphstore.RangeOwnershipKey(n, intent.publication.RangeID)}
	state := rangeMoveLateFenceTargetState{image: bytes.Clone(root.Image)}
	for i, key := range keys {
		row, found, err := view.Get(t.Context(), key, 1024)
		if err != nil {
			t.Fatal(err)
		}
		state.present[i] = found && !row.Deleted
		if state.present[i] {
			state.placement[i] = bytes.Clone(row.Value)
		}
	}
	return state
}

func rangeMoveLateFenceAssertQuiescent(t *testing.T, h *allocationHost, intent rangeMoveIntent, before rangeMoveLateFenceTargetState) {
	t.Helper()
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := view.Close(); err != nil {
			t.Error(err)
		}
	}()
	base, err := view.RootBounded(t.Context(), 172)
	if err != nil {
		t.Fatal(err)
	}
	q := reader{ctx: t.Context(), view: view, ns: h.machine.ns, base: base, limits: h.machine.limits.allocation}
	active, readErr := readRangeMoveActive(&q)
	readErr = errors.Join(readErr, view.Close())
	if readErr != nil {
		t.Fatal("actual target active-state inspection", readErr)
	}
	owner := rangeMoveAdmissionOwner(t, h)
	if active != ([16]byte{}) || owner != (raftlog.ApplicationRecoveryOwner{}) {
		t.Fatalf("late target Fence reacquired admission after completed original abort: partition=%d active=%x recovery=%+v", h.machine.ns.partition, active, owner)
	}
	actual := rangeMoveLateFenceTargetSnapshot(t, h, intent)
	if !bytes.Equal(actual.image, before.image) {
		t.Fatal("completed abort/late Fence changed target graph image")
	}
	for i := range actual.placement {
		if actual.present[i] != before.present[i] || !bytes.Equal(actual.placement[i], before.placement[i]) {
			t.Fatal("completed abort/late Fence changed exact visible placement", i)
		}
	}
}

// Regression: Source Abort0 plus untouched target was treated as completed,
// allowing a source-authorized target Fence already in flight to install an
// orphan admission reservation afterward. A second Resume is not a repair law.
func TestRangeMoveCoordinatorCompletedAbortRejectsDelayedGenuineTargetFence(t *testing.T) {
	n, _, intent, _, transports := rangeMoveCoordinatorFixture(t, 8)
	for group := range 2 {
		for member := range 3 {
			if n.configs[group][member].Dir == "" || n.configs[group][member].FS != nil {
				t.Fatal("fixture is not actual disk-backed native storage")
			}
		}
	}
	wire, err := encodeRangeMoveIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	var before [3]rangeMoveLateFenceTargetState
	for member, h := range n.hosts[1] {
		before[member] = rangeMoveLateFenceTargetSnapshot(t, h, intent)
	}
	gate := &rangeMoveLateFenceGate{graphPeerWireTransport: transports[8]}
	transports[8] = gate
	coordinator, err := newRangeMoveCoordinator(n.hosts[0][0], transports)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := coordinator.Run(t.Context(), intent)
	if !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || pending.committed || pending.source.phase != rangeMoveFence || pending.target.phase != 0 || len(gate.frozen.wire) == 0 {
		t.Fatal("actual source Fence/target in-flight seam not reached", pending, err)
	}
	command, err := decodeRangeMoveCommand(gate.frozen.wire)
	if err != nil || command.phase != rangeMoveFence || command.scope != intent.target || !bytes.Equal(command.intent, wire) || len(command.proofs) != 1 {
		t.Fatal("frozen proposal is not the actual original target Fence", command, err)
	}
	sourceFence := rangeMoveAdmissionRead(t, n, 0, pending.resume.move)
	if !sourceFence.validSeal() || sourceFence.observation.status != pending.source || sourceFence.observation.status.phase != rangeMoveFence || command.proofs[0] != sourceFence.observation.status || sha256.Sum256(sourceFence.observation.intent) != pending.resume.intent {
		t.Fatal("frozen target Fence did not carry the genuine original completed source authority")
	}
	for member, h := range n.hosts[1] {
		rangeMoveLateFenceAssertQuiescent(t, h, intent, before[member])
	}
	abort := rangeMoveAdmissionSubmit(t, n, 0, wire, rangeMoveAbort, 0, nil, nil)
	if abort.phase != rangeMoveAbort || abort.sequence != 0 || abort.result != rangeMoveApplied || abort.intent != pending.resume.intent {
		t.Fatal("genuine source Abort0 did not persist", abort)
	}
	completed, err := coordinator.Resume(t.Context(), pending.resume)
	if !errors.Is(err, errLimit) || errors.Is(err, errRangeMovePending) || completed.committed || completed.resume != pending.resume || completed.source != abort || completed.decision != (rangeMoveStatus{}) {
		t.Fatal("actual Resume did not finish the original pre-COMMIT abort", completed, err)
	}
	// Deliver the frozen bytes only AFTER Resume has reported terminal abort.
	// No refreshed proof, new command, post-delivery Resume, or cleanup vote
	// can turn acceptance of this old Fence into a passing regression.
	pump := nativeGraphTestPump{n: n, group: 1, host: n.hosts[1][0]}
	outcome, err := pump.Submit(t.Context(), gate.frozen)
	if err != nil {
		t.Fatal("delayed native proposal stopped driver instead of original rejection", err)
	}
	late, err := decodeRangeMoveStatus(outcome)
	if err != nil || late.intent != pending.resume.intent || late.move != pending.resume.move {
		t.Fatal("native delayed proposal lost exact original identity", late, err)
	}
	for member, h := range n.hosts[1] {
		rangeMoveLateFenceAssertQuiescent(t, h, intent, before[member])
		cdc, err := h.machine.store.ApplicationRecord(t.Context(), h.driver.Applied(), false, h.machine.store.ApplicationLimits().MaxChangeBytes)
		if err != nil || len(cdc) != 0 {
			t.Fatal("late rejected Fence emitted graph CDC", member, err)
		}
	}
	if late.phase == rangeMoveFence && late.result == rangeMoveApplied {
		t.Fatal("late genuine target Fence returned a new applied admission after terminal abort", late)
	}
	// Required health is a new unrelated business mutation through native
	// admission, not a compensating range Resume after the stale delivery.
	all, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := n.hosts[1][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 17, Life: 18, Scope: all, Name: "after-abort"}}, 73, nil)
	if err != nil {
		t.Fatal("late rejected Fence stranded healthy unrelated target write", err)
	}
	n.submitPartitionGraph(1, mutation, reasonNone)
	var afterHealth [3]rangeMoveLateFenceTargetState
	for member, h := range n.hosts[1] {
		afterHealth[member] = rangeMoveLateFenceTargetSnapshot(t, h, intent)
	}
	crossReopenAll(t, n)
	// Re-deliver the same original bytes after genuine disk-backed reopen.
	// The target must remember rejection without a live coordinator repairing it.
	pump = nativeGraphTestPump{n: n, group: 1, host: n.hosts[1][0]}
	repeatedWire, err := pump.Submit(t.Context(), gate.frozen)
	if err != nil {
		t.Fatal("reopened target stopped on original delayed proposal", err)
	}
	repeated, err := decodeRangeMoveStatus(repeatedWire)
	if err != nil || repeated != late {
		t.Fatal("reopen lost original durable rejection", repeated, late, err)
	}
	for member, h := range n.hosts[1] {
		rangeMoveLateFenceAssertQuiescent(t, h, intent, afterHealth[member])
	}
	for member := range 3 {
		for _, h := range []*allocationHost{n.hosts[0][member], n.hosts[1][member]} {
			if owner := rangeMoveAdmissionOwner(t, h); owner != (raftlog.ApplicationRecoveryOwner{}) {
				t.Fatal("reopen resurrected aborted reservation", member, owner)
			}
		}
		target := n.hosts[1][member]
		view, err := target.machine.store.ApplicationView(target.driver.Applied())
		if err != nil {
			t.Fatal(err)
		}
		base, err := view.RootBounded(t.Context(), 172)
		if err != nil {
			t.Fatal(err)
		}
		q := reader{ctx: t.Context(), view: view, ns: target.machine.ns, base: base, limits: target.machine.limits.allocation}
		active, err := readRangeMoveActive(&q)
		err = errors.Join(err, view.Close())
		if err != nil || active != ([16]byte{}) {
			t.Fatal("reopen resurrected late target fence", member, active, err)
		}
		composite, close := crossView(t, n, member, [2]uint64{})
		assertCertifiedProjection(t, composite, 1, true, nil)
		assertCertifiedProjection(t, composite, 17, true, []string{"after-abort"})
		phantom, err := composite.Entity(t.Context(), 7)
		if err != nil || phantom.Found || phantom.Record.ID != 0 {
			t.Fatal("aborted range fabricated phantom entity7", phantom, err)
		}
		close()
	}
}
