package graphapply

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestRangeMoveIntrinsicSourceAdmissionIsTerminalWithoutFenceAndKeepsOriginalRetry(t *testing.T) {
	n, c, intent, wires, _ := rangeMoveCoordinatorFixture(t, 64)
	intent.maxPages = 1024
	var before [6]raftlog.ApplicationRoot
	for group := range 2 {
		for member, h := range n.hosts[group] {
			v, err := h.machine.store.ApplicationView(h.driver.Applied())
			if err != nil {
				t.Fatal(err)
			}
			root, err := v.RootBounded(t.Context(), 172)
			err = errors.Join(err, v.Close())
			if err != nil {
				t.Fatal(err)
			}
			before[3*group+member] = root
		}
	}
	reservation, err := n.hosts[0][0].machine.moveReservation(intent)
	if err != nil || n.hosts[0][0].machine.moveReservationFits(reservation) {
		t.Fatal("breaking admission input must exceed existing immutable source quota", err)
	}
	// Admission recipes reject invalid data before returning any credit. Fence
	// reserves a new recipe; it must never be mistaken for a completion USE.
	if invalid, err := n.hosts[0][0].machine.moveReservation(rangeMoveIntent{}); !errors.Is(err, errInvalid) || !reflect.DeepEqual(invalid, raftlog.ApplicationRecoveryReservation{}) {
		t.Fatal("invalid intent returned completion credits", err)
	}
	for _, phase := range []rangeMovePhase{rangeMoveFence, 0, 255} {
		if rangeMoveCredit(rangeMoveCommand{phase: phase}) != 0 {
			t.Fatal("noncompletion phase selected a protected USE credit", phase)
		}
	}
	q := rangeMoveReplayNativeReader(t, n.hosts[0][0], 16<<20)
	if held, err := n.hosts[0][0].machine.rangeMoveFenceOwner(q, intent); err != nil || held {
		t.Fatal("actual empty native owner blocked source admission", held, err)
	}
	if err := q.view.Close(); err != nil {
		t.Fatal(err)
	}
	if held, err := n.hosts[0][0].machine.rangeMoveFenceOwner(q, intent); held || !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("closed borrowed native view became ownership evidence", held, err)
	}
	refused, err := c.Run(t.Context(), intent)
	if refused.committed || !errors.Is(err, errLimit) || errors.Is(err, errRangeMovePending) || refused.source.phase != rangeMoveAbort || refused.source.result != rangeMoveLimit || refused.source.sequence != 0 || refused.target.phase != 0 {
		t.Fatal("intrinsic admission became an endless pending or acquired a target fence", refused, err)
	}
	if wires[0].captures+wires[1].captures != 0 {
		t.Fatal("rejected original captured data")
	}
	for group := range 2 {
		for member, h := range n.hosts[group] {
			q := rangeMoveReplayNativeReader(t, h, 16<<20)
			root := q.base
			active, err := readRangeMoveActive(q)
			if closeErr := q.view.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
			if err != nil || active != ([16]byte{}) || rangeMoveAdmissionOwner(t, h) != (raftlog.ApplicationRecoveryOwner{}) || !bytes.Equal(root.Image, before[3*group+member].Image) || root.ImageHash != before[3*group+member].ImageHash {
				t.Fatal("intrinsic refusal changed graph or retained admission/recovery owner", group, member, err)
			}
			if group == 1 && root.Index != before[3*group+member].Index {
				t.Fatal("source refusal submitted a target transition")
			}
			if group == 0 {
				cdc, err := h.machine.store.ApplicationRecord(t.Context(), h.driver.Applied(), false, h.machine.store.ApplicationLimits().MaxChangeBytes)
				if err != nil || len(cdc) != 0 {
					t.Fatal("no-effect refusal emitted graph CDC", err)
				}
			}
		}
	}
	retry, err := c.Resume(t.Context(), refused.resume)
	if !errors.Is(err, errLimit) || errors.Is(err, errRangeMovePending) || retry != refused {
		t.Fatal("terminal retry reinterpreted original refusal", retry, err)
	}
	recovered, err := n.hosts[0][0].PrepareRangeMove(t.Context(), intent.id, intent.publication.RangeID, intent.target.partition, intent.maxPages, intent.pageRows, intent.pageBytes)
	if err != nil || recovered != intent {
		t.Fatal("rejected original lost exact native intent", recovered, err)
	}
	output, err := graphstate.NewOutputBudget(8 << 20)
	if err != nil {
		t.Fatal(err)
	}
	intentWire, err := encodeRangeMoveIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := n.hosts[0][0].prepareRangeMoveCommand(intentWire, rangeMoveFence, 3, 0, nil, nil, output)
	if err != nil {
		t.Fatal(err)
	}
	target := n.hosts[1][0]
	root := before[3]
	term, err := target.machine.store.Term(root.Index)
	if err != nil {
		t.Fatal(err)
	}
	// Known framed data is checked at the pure native Stage boundary, never
	// installed or claimed as a consensus/certificate result.
	for _, tc := range []struct {
		name  string
		wire  []byte
		cause error
	}{
		{"foreign actual scope", proposal.wire, errInvalid},
		{"truncated known frame", proposal.wire[:8], errCorrupt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch, err := target.machine.Stage(replica.Entry{Index: root.Index + 1, Generation: root.Generation, Term: term, Data: tc.wire}, target.machine.store.ApplicationBudget())
			if !errors.Is(err, tc.cause) || !reflect.DeepEqual(batch, raftlog.ApplicationBatch{}) {
				t.Fatal("known framed refusal returned effects or lost sentinel", err)
			}
		})
	}
	window, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	healthy, err := n.hosts[0][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: window, Name: "healthy-after-intrinsic-refusal"}}, 173, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(0, healthy, reasonNone)
	crossReopenAll(t, n)
	recovered, err = n.hosts[0][0].PrepareRangeMove(t.Context(), intent.id, intent.publication.RangeID, intent.target.partition, intent.maxPages, intent.pageRows, intent.pageBytes)
	if err != nil || recovered != intent {
		t.Fatal("durable reopen changed original terminal identity", recovered, err)
	}
	for member := range 3 {
		view, closeView := crossView(t, n, member, [2]uint64{n.hosts[0][member].driver.Applied(), n.hosts[1][member].driver.Applied()})
		assertCertifiedProjection(t, view, 1, true, []string{"healthy-after-intrinsic-refusal"})
		assertCertifiedProjection(t, view, 17, true, nil)
		closeView()
	}
	for group := range 2 {
		for _, h := range n.hosts[group] {
			if rangeMoveAdmissionOwner(t, h) != (raftlog.ApplicationRecoveryOwner{}) {
				t.Fatal("reopen reacquired a rejected recovery owner")
			}
		}
	}
}
