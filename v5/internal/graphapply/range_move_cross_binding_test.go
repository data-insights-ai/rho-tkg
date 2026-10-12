package graphapply

import (
	"errors"
	"io"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestRangeMoveActiveCrossReplayRetainsCapacitySourceBindingValidation(t *testing.T) {
	n, _, intent, _, transports := rangeMoveCoordinatorFixture(t, 64)
	window, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	ops := []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: window, Name: "completed-capacity"}, {Kind: graphstate.AddLabel, Owner: 17, Life: 18, Scope: window, Name: "completed-capacity"}}
	ids := crossRequestsWithLoser(n, 1)
	var txs [2]crossTransaction
	var registrations [2]crossProof
	for group := range 2 {
		tx, proposal := crossPlanAt(n, group, ids[group], ops, nil, 91)
		txs[group] = tx
		status := n.crossSubmit(group, proposal)
		registrations[group] = n.crossReadPhase(group, status.tx, crossRegister)
	}
	first, err := n.hosts[0][0].PrepareCrossParticipant(txs[0], 3, registrations[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	if status := n.crossSubmit(0, first); !status.yes {
		t.Fatal(status)
	}
	firstVote := n.crossReadPhase(0, registrations[0].status.tx, crossPrepare)
	blocked, err := n.hosts[1][0].PrepareCrossParticipant(txs[1], 3, registrations[1], nil)
	if err != nil {
		t.Fatal(err)
	}
	capacity := n.capacityRead(0, blocked)
	abort, existing, err := n.hosts[1][0].AbortCrossTransactionForCapacity(txs[1], capacity)
	if err != nil || existing != (crossStatus{}) {
		t.Fatal(err, existing)
	}
	n.crossSubmit(1, abort)
	aborted := n.crossReadPhase(1, registrations[1].status.tx, crossDecide)
	crossResolveAt(n, 1, aborted, 1)
	second, err := n.hosts[0][0].PrepareCrossParticipant(txs[0], 8, registrations[0], []crossProof{firstVote})
	if err != nil {
		t.Fatal(err)
	}
	if status := n.crossSubmit(1, second); !status.yes {
		t.Fatal(status)
	}
	secondVote := n.crossReadPhase(1, registrations[0].status.tx, crossPrepare)
	decide, err := n.hosts[0][0].DecideCrossTransaction(txs[0], []crossProof{firstVote, secondVote})
	if err != nil {
		t.Fatal(err)
	}
	n.crossSubmit(0, decide)
	crossResolveAt(n, 0, n.crossReadPhase(0, registrations[0].status.tx, crossDecide), 0, 1)
	gate := &rangeMoveAdmissionImportGate{graphPeerWireTransport: transports[8], sequence: 3}
	transports[8] = gate
	coordinator, err := newRangeMoveCoordinator(n.hosts[0][0], transports)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runRangeMoveToCompletion(t, coordinator, intent); !gate.stopped || !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) {
		t.Fatal("actual target did not reach staged move", err)
	}
	h := n.hosts[1][0]
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	if purpose, err := h.machine.VerifyApplicationRecovery(t.Context(), abort.wire, view); err != nil || purpose.Reserve != nil || purpose.Use != nil {
		t.Fatal("genuine completed capacity replay lost zero-purpose", purpose, err)
	}
	command, _, err := decodeCrossCommand(abort.wire, h.machine.limits)
	if err != nil || command.conflict == nil {
		t.Fatal("actual capacity decision omitted conflict", err)
	}
	changed := *command.conflict
	changed.source.group[0] ^= 1
	command.conflict = &changed
	wire, err := encodeCrossCommand(command, h.machine.limits)
	if err != nil {
		t.Fatal("structurally valid changed capacity source", err)
	}
	purpose, err := h.machine.VerifyApplicationRecovery(t.Context(), wire, view)
	if !errors.Is(err, errInvalid) || purpose.Reserve != nil || purpose.Use != nil {
		t.Fatal("active move cross replay omitted full capacity source binding", purpose, err)
	}
}
