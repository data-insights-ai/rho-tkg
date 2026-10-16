package graphapply

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

// RED: cancellation at the actual committed-outcome lookup belongs to the
// request. It must not escape on the process pump's fatal native event lane.
func TestGraphProcessCutSubmissionCancellationIsOperationFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	h := n.hosts[0][0]
	baseline := h.driver.Applied()
	proposal, err := h.PrepareGraphFence(7)
	if err != nil {
		t.Fatal(err)
	}
	pump := &nativeGraphTestPump{n: n, group: 0, host: h}
	committed, err := pump.Submit(t.Context(), proposal)
	if err != nil {
		t.Fatal("genuine quorum-committed fence prerequisite", err)
	}
	result, err := decodeGraphCutResult(committed)
	if err != nil || result.phase != graphCutFence || result.status != graphCutApplied || result.round != 7 || result.index != baseline+1 {
		t.Fatal("fixture must persist exactly the prepared fence command", result, err)
	}
	entries, err := h.machine.store.Entries(result.index, result.index+1, 512)
	if err != nil || len(entries) != 1 || !bytes.Equal(entries[0].GetData(), proposal.wire) {
		t.Fatal("fixture command bytes lost their genuine Raft entry", err)
	}
	next, scanned, body, operationErr, eventErr := pollGraphProcessCutSubmission(t.Context(), h, proposal, baseline, 0)
	if next != result.index || scanned != 1 || !bytes.Equal(body, committed) || operationErr != nil || eventErr != nil {
		t.Fatal("live counterpart must return the exact committed GSO", next, scanned, operationErr, eventErr)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // Cancellation reaches the actual ApplicationRecord lookup.
	next, scanned, body, operationErr, eventErr = pollGraphProcessCutSubmission(ctx, h, proposal, baseline, 0)
	if next != result.index || scanned != 1 || body != nil || !errors.Is(operationErr, context.Canceled) || eventErr != nil {
		t.Fatal("cancelled outcome lookup escaped as fatal process event instead of operation refusal", next, scanned, operationErr, eventErr)
	}
	event, err := h.Tick()
	if err != nil {
		t.Fatal("request cancellation poisoned native event processing", err)
	}
	if _, err := pump.drive(t.Context(), event); err != nil {
		t.Fatal("healthy native event counterpart failed after cancellation", err)
	}
	retained, err := h.machine.store.ApplicationRecord(t.Context(), result.index, true, 512)
	if err != nil || !bytes.Equal(retained, committed) {
		t.Fatal("request cancellation changed genuine retained outcome", err)
	}
	next, scanned, body, operationErr, eventErr = pollGraphProcessCutSubmission(t.Context(), h, proposal, baseline, 0)
	if next != result.index || scanned != 1 || !bytes.Equal(body, committed) || operationErr != nil || eventErr != nil {
		t.Fatal("operation cancellation prevented a later exact native lookup", next, scanned, operationErr, eventErr)
	}
}
