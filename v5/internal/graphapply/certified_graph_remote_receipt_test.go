package graphapply

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func remoteCertifiedReceiptBackend(t *testing.T, c *crossProcessCoordinator) *remoteCertifiedGraphBackend {
	t.Helper()
	l := c.host.machine.limits
	arena, err := graphstate.NewOutputBudget(min(l.outputBytes, l.sourceBytes))
	if err != nil {
		t.Fatal(err)
	}
	budget := newGraphRemoteBudget(l, arena)
	endpoints, err := c.endpoints(budget)
	if err != nil {
		t.Fatal(err)
	}
	return &remoteCertifiedGraphBackend{coordinator: c, endpoints: endpoints, budget: budget, arena: arena}
}

// RED: a genuine completed floor read is not a committed local mutation receipt.
// Generic proof validity must not silently turn its empty receipt into round0.
func TestRemoteCertifiedReceiptRoundRejectsGenuineFloorProofAsLocalReceipt(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, valid := certifiedReceiptIssuer(t, true)
	_, transport := certifiedCoordinator(n)
	nonce, err := graphReadNonce()
	if err != nil {
		t.Fatal(err)
	}
	query := graphCutQuery{kind: 1, nonce: nonce}
	reply, err := transport.Read(t.Context(), n.hosts[0][0], query)
	if err != nil || reply.err != nil {
		t.Fatal("genuine completed floor read prerequisite", err, reply.err)
	}
	if err := checkGraphCutProof(reply.proof, query); err != nil {
		t.Fatal("fixture must preserve native proof provenance", err)
	}
	if _, err := graphReceiptFromLocal(reply.proof); !errors.Is(err, errInvalid) {
		t.Fatal("native receipt constructor must reject floor proof", err)
	}
	c := remoteCertifiedFixture(t, n)
	backend := remoteCertifiedReceiptBackend(t, c)
	if round, err := backend.ReceiptRound(t.Context(), valid); err != nil || round != valid.local.observation.local.round || round == 0 {
		t.Fatal("genuine local commit receipt counterpart", round, err)
	}
	round, err := backend.ReceiptRound(t.Context(), graphCommitReceipt{local: reply.proof})
	if round != 0 || !errors.Is(err, errInvalid) {
		t.Fatal("completed floor proof was accepted as a local commit receipt", round, err)
	}
}

// RED: validating immutable native receipt provenance must not re-export live
// authority through its stopped issuer. Reopened sources check current binding.
func TestRemoteCertifiedReceiptRoundPreservesNativeCommitAfterIssuerCloseReopen(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, receipt := certifiedReceiptIssuer(t, false)
	original := receipt.proof.owner
	want := receipt.proof.status.round
	if original == nil || want == 0 || receipt.remote != nil {
		t.Fatal("fixture must supply original native committed decision provenance")
	}
	crossReopenAll(t, n)
	if original == n.hosts[0][0] {
		t.Fatal("fixture reused original issuer instead of reopening genuine sources")
	}
	if view, err := original.machine.store.ApplicationView(receipt.proof.index); view != nil || !errors.Is(err, raftlog.ErrClosed) {
		if view != nil {
			_ = view.Close()
		}
		t.Fatal("original issuer store must actually be closed", err)
	}
	if _, err := graphReceiptFromDecision(receipt.proof); err != nil {
		t.Fatal("immutable native committed receipt lost provenance after close", err)
	}
	native, _ := certifiedCoordinator(n)
	if round, err := native.receiptRound(t.Context(), receipt); err != nil || round != want {
		t.Fatal("native reopened-source receipt counterpart", round, err)
	}
	c := remoteCertifiedFixture(t, n)
	backend := remoteCertifiedReceiptBackend(t, c)
	if round, err := backend.ReceiptRound(t.Context(), receipt); err != nil || round != want {
		t.Fatal("remote receipt validation depended on stopped original issuer", round, want, err)
	}
	read, err := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadAfter, receipt: receipt}, nil)
	if err != nil || read == nil {
		t.Fatal("remote After lost committed receipt on reopened sources", err)
	}
	t.Cleanup(func() {
		if err := read.Close(); err != nil {
			t.Error(err)
		}
	})
	cut, err := read.Cut()
	if err != nil || len(cut.certificates) != 2 {
		t.Fatal("After needs both genuine source certificates", cut, err)
	}
	for _, certificate := range cut.certificates {
		if certificate.round < want {
			t.Fatal("After certificate precedes original commit", certificate.round, want)
		}
	}
	view := certifiedView(t, read)
	assertCertifiedProjection(t, view, 3, true, nil)
	scope, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	assertCertifiedIncident(t, view, scope, 1, 2, []graphstate.EntityID{3})
	assertCertifiedIncident(t, view, scope, 17, 18, []graphstate.EntityID{3})
	phantom, err := view.Entity(t.Context(), 7)
	if err != nil || phantom.Found {
		t.Fatal("After introduced phantom identity", phantom, err)
	}
}
