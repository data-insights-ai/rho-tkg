package graphapply

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestRangeMoveStagedTargetRejectsQueuedAllocationAndGraphMutators(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _, intent, _, transports := rangeMoveCoordinatorFixture(t, 64)
	home, target := n.hosts[0][0], n.hosts[1][0]
	recipient := n.read(0, allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: [16]byte{2}}}).observation.recipient.session
	request := idalloc.GrantRequest{Graph: home.machine.ns.graph, Session: recipient, Sequence: 2, Count: 16}
	reservation, err := home.AuthorizeReserve(n.requestID(), request, n.read(0, allocationQuery{kind: observeAllocator}))
	if err != nil {
		t.Fatal(err)
	}
	n.submit(0, reservation)
	grant := n.read(0, allocationQuery{kind: observeGrant, session: recipient, sequence: 2, count: 16})
	queued, err := home.InstallGrant(n.requestID(), 8, grant)
	if err != nil {
		t.Fatal(err)
	}
	all, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	graph, err := target.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 17, Life: 18, Scope: all, Name: "must-not-stage"}}, 151, nil)
	if err != nil {
		t.Fatal(err)
	}
	gate := &rangeMoveAdmissionImportGate{graphPeerWireTransport: transports[8], sequence: 3}
	transports[8] = gate
	c, err := newRangeMoveCoordinator(home, transports)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := runRangeMoveToCompletion(t, c, intent)
	if !gate.stopped || !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) {
		t.Fatal("actual target did not reach staged tree", pending, err)
	}
	before := rangeMoveFrozenGuardStateAt(t, target, pending.resume.move)
	owner := rangeMoveAdmissionOwner(t, target)
	grantRefusal := n.submitReason(1, queued, reasonStale)
	graphRefusal := n.submitPartitionGraph(1, graph, reasonPreparedConflict)
	after := rangeMoveFrozenGuardStateAt(t, target, pending.resume.move)
	if !bytes.Equal(before.root.Image, after.root.Image) || !bytes.Equal(before.capture, after.capture) || !bytes.Equal(before.progress, after.progress) || !bytes.Equal(before.status, after.status) || rangeMoveAdmissionOwner(t, target) != owner {
		t.Fatal("queued mutator changed staged baseline/owner/cursor")
	}
	for _, h := range n.hosts[1] {
		for _, index := range []uint64{grantRefusal.index, graphRefusal.index} {
			cdc, e := h.machine.store.ApplicationRecord(t.Context(), index, false, h.machine.store.ApplicationLimits().MaxChangeBytes)
			if e != nil || len(cdc) != 0 {
				t.Fatal("fenced mutator emitted CDC", index, e)
			}
		}
	}
	gate.sequence = 1024
	done := rangeMoveKeeperResume(t, c, pending.resume)
	if !done.committed {
		t.Fatal(done)
	}
	for member := range 3 {
		view, close := crossView(t, n, member, [2]uint64{})
		assertCertifiedProjection(t, view, 1, true, nil)
		assertCertifiedProjection(t, view, 17, true, nil)
		phantom, e := view.Entity(t.Context(), 33)
		if e != nil || phantom.Found {
			t.Fatal("queued grant created graph entity", phantom, e)
		}
		close()
	}
}
