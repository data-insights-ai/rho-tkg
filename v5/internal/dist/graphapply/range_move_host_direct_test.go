package graphapply

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
)

func TestRangeMoveNativeReadDirectMatchesCompletedQueryAndCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _, intent, _, _ := rangeMoveCoordinatorFixture(t, 64)
	h := n.hosts[0][0]
	question := rangeMoveReadBudget(h.machine.limits)
	question.move = rangeMoveID(h.machine.ns, intent.id)
	question.nonce = [16]byte{171}
	event, err := h.ReadRangeMove(t.Context(), question)
	if err != nil || event.readID.sequence == 0 {
		t.Fatal("direct native move read", err)
	}
	pump := nativeGraphTestPump{n: n, group: 0, host: h}
	result, err := pump.drive(t.Context(), event)
	if err != nil || len(result.moveReplies) != 1 {
		t.Fatal("actual quorum completion", err)
	}
	reply := result.moveReplies[0]
	if reply.id != event.readID || reply.question != question || reply.err != nil || reply.proof.observation.status != (rangeMoveStatus{}) {
		t.Fatal("completed absence lost correlation", reply.err)
	}
	if _, err := h.exportRangeMoveAuthority(reply.proof); err != nil {
		t.Fatal("native export lost actual completed read", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	question.nonce[0]++
	event, err = h.ReadRangeMove(ctx, question)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	result, err = pump.drive(t.Context(), event)
	if err != nil || len(result.moveReplies) != 1 || !errors.Is(result.moveReplies[0].err, context.Canceled) || result.moveReplies[0].proof.owner != nil {
		t.Fatal("canceled waiter contaminated native event or returned authority", err)
	}
	if h.pendingReadCount() != 0 {
		t.Fatal("completed cancellation retained native read owner")
	}
	tick, err := h.Tick()
	if err != nil {
		t.Fatal("canceled waiter stopped unrelated native Tick", err)
	}
	if _, err := pump.drive(t.Context(), tick); err != nil {
		t.Fatal(err)
	}
}

func TestRangeMoveNativeReadDirectNilInvalidCancelledAndClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _, intent, _, _ := rangeMoveCoordinatorFixture(t, 64)
	h := n.hosts[0][0]
	q := rangeMoveReadBudget(h.machine.limits)
	q.move = rangeMoveID(h.machine.ns, intent.id)
	q.nonce = [16]byte{172}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	page := q
	page.page = 1
	for _, tc := range []struct {
		h     *allocationHost
		ctx   context.Context
		q     rangeMoveQuery
		cause error
	}{
		{nil, t.Context(), q, errInvalid}, {h, nil, q, errInvalid}, {h, t.Context(), rangeMoveQuery{}, errInvalid}, {h, t.Context(), page, errInvalid}, {h, cancelled, q, context.Canceled},
	} {
		if event, err := tc.h.ReadRangeMove(tc.ctx, tc.q); event.readID != (allocationReadID{}) || !errors.Is(err, tc.cause) {
			t.Fatal("native boundary lost zero output/sentinel", event.readID, err)
		}
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if event, err := h.ReadRangeMove(t.Context(), q); event.readID != (allocationReadID{}) || !errors.Is(err, replica.ErrStopped) {
		t.Fatal("closed native read", err)
	}
}
