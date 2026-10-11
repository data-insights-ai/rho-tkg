package graphapply

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

func remoteCertifiedFixture(t *testing.T, n *allocationTestNetwork) *crossProcessCoordinator {
	t.Helper()
	transports := make(map[uint64]graphPeerWireTransport, 2)
	for g := range 2 {
		h := n.hosts[g][0]
		server, e := newGraphSourceServer(t.Context(), h, defaultGraphSourceLimits(h.machine.limits))
		if e != nil {
			t.Fatal(e)
		}
		pump := &nativeGraphTestPump{n: n, group: g, host: h}
		service, e := newGraphPeerService(h, server, pump)
		if e != nil {
			t.Fatal(e)
		}
		transports[h.machine.ns.partition] = &nativeGraphTestTransport{service: service}
	}
	c, e := newCrossProcessCoordinator(n.hosts[1][1], transports)
	if e != nil {
		t.Fatal(e)
	}
	return c
}

func TestRemoteCertifiedModesPreserveStableScopeAndRefuseFailedPublication(t *testing.T) {
	n := newCertifiedGraphNetwork(t)
	crossEdgeFixture(t, n)
	c := remoteCertifiedFixture(t, n)
	var missing *crossProcessCoordinator
	if r, e := missing.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil); r != nil || !errors.Is(e, errInvalid) {
		t.Fatal(r, e)
	}
	//nolint:staticcheck // Direct nil-context refusal is an invalid-input contract.
	if r, e := c.OpenCertified(nil, graphReadRequest{mode: graphReadFresh}, nil); r != nil || !errors.Is(e, errInvalid) {
		t.Fatal(r, e)
	}
	if r, e := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadStable}, nil); r != nil || !errors.Is(e, replica.ErrUnavailable) {
		t.Fatal(r, e)
	}
	if r, e := c.OpenCertified(t.Context(), graphReadRequest{}, nil); r != nil || !errors.Is(e, errInvalid) {
		t.Fatal(r, e)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if r, e := c.OpenCertified(cancelled, graphReadRequest{mode: graphReadFresh}, nil); r != nil || !errors.Is(e, context.Canceled) {
		t.Fatal(r, e)
	}
	fresh, e := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if e != nil {
		t.Fatal(e)
	}
	cut, e := fresh.Cut()
	if e != nil {
		t.Fatal(e)
	}
	// Both leases are genuinely held. A second read fills them; the third fails
	// without replacing Stable or publishing a partially opened reader.
	second, e := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadAt, cut: cut}, nil)
	if e != nil {
		t.Fatal(e)
	}
	for group := range 2 {
		h := n.hosts[group][0]
		proposal, e := h.PrepareGraphFence(cut.certificates[0].round + 1)
		if e != nil {
			t.Fatal(e)
		}
		pump := &nativeGraphTestPump{n: n, group: group, host: h}
		if _, e := pump.Submit(t.Context(), proposal); e != nil {
			t.Fatal(e)
		}
	}
	if r, e := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil); r != nil || !errors.Is(e, errLimit) {
		t.Fatal("source-cap admission", r, e)
	}
	if !slices.Equal(c.certified.stable.certificates, cut.certificates) {
		t.Fatal("failed capture replaced Stable")
	}
	if e := second.Close(); e != nil {
		t.Fatal(e)
	}
	if e := fresh.Close(); e != nil {
		t.Fatal(e)
	}
	stable, e := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadStable}, nil)
	if e != nil {
		t.Fatal(e)
	}
	got, e := stable.Cut()
	if e != nil || !slices.Equal(got.certificates, cut.certificates) {
		t.Fatal(got, e)
	}
	if e := stable.Close(); e != nil {
		t.Fatal(e)
	}
	if r, e := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadAt, cut: cut}, []uint64{3}); r != nil || !errors.Is(e, errGraphCutScope) {
		t.Fatal("cut scope expanded", r, e)
	}
	scoped := graphCut{certificates: cut.certificates[:1]}
	read, e := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadAt, cut: scoped}, []uint64{3})
	if e != nil {
		t.Fatal(e)
	}
	view, e := read.ReadView()
	if e != nil {
		t.Fatal(e)
	}
	foreign, e := view.Entity(t.Context(), 17)
	if !errors.Is(e, graphstore.ErrScopeUnavailable) || foreign.Found {
		t.Fatal(foreign, e)
	}
	if e := read.Close(); e != nil {
		t.Fatal(e)
	}
}

func TestRemoteCertifiedAfterRetainsAuthenticatedDecisionReceiptAcrossIssuerReopen(t *testing.T) {
	n, nativeReceipt := certifiedReceiptIssuer(t, false)
	c := remoteCertifiedFixture(t, n)
	backend := remoteCertifiedReceiptBackend(t, c)
	// Recover the actual recorded transaction through the genuine native kernel,
	// then obtain its decision through the authenticated remote source path.
	nonce, e := graphReadNonce()
	if e != nil {
		t.Fatal(e)
	}
	authority, e := backend.endpoints[nativeReceipt.proof.status.ns.partition].cross(t.Context(), nativeReceipt.proof.status.tx, crossDecide, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	receipt, e := graphReceiptFromAuthority(authority)
	if e != nil {
		t.Fatal(e)
	}
	copyAuthority := authority
	copyAuthority.question.nonce = nonce
	if _, e := graphReceiptFromAuthority(copyAuthority); !errors.Is(e, errInvalid) {
		t.Fatal("changed request borrowed decision authority", e)
	}
	crossReopenAll(t, n)
	reopened := remoteCertifiedFixture(t, n)
	read, e := reopened.OpenCertified(t.Context(), graphReadRequest{mode: graphReadAfter, receipt: receipt}, nil)
	if e != nil {
		t.Fatal("After depended on old issuing stream", e)
	}
	defer read.Close()
	cut, e := read.Cut()
	if e != nil || len(cut.certificates) != 2 {
		t.Fatal(cut, e)
	}
	for _, cert := range cut.certificates {
		if cert.round < authority.status.round {
			t.Fatal("After omitted immutable decision", cert)
		}
	}
	assertCertifiedProjection(t, certifiedView(t, read), 3, true, nil)
}

func TestRemoteCertifiedFreshResolvesNativeEarlyAbortAndKeepsMissingParticipantAbsent(t *testing.T) {
	n := newCertifiedGraphNetwork(t)
	decision := certifiedEarlyAbort(t, n)
	c := remoteCertifiedFixture(t, n)
	read, e := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if e != nil {
		t.Fatal("remote early Abort closure", e)
	}
	defer read.Close()
	edge, e := certifiedView(t, read).Entity(t.Context(), 3)
	if e != nil || edge.Found {
		t.Fatal("Abort invented graph effects", edge, e)
	}
	actual := n.crossReadPhase(0, decision.status.tx, crossDecide)
	if actual.status != decision.status {
		t.Fatal("read changed immutable Abort")
	}
	for group := range 2 {
		h := n.hosts[group][0]
		if _, e := h.Tick(); e != nil {
			t.Fatal("refusal poisoned native Driver", e)
		}
	}
}

func TestRemoteCertifiedBackendRejectsChangedReceiptAndInvalidWriteWithoutEffects(t *testing.T) {
	n, receipt := certifiedReceiptIssuer(t, true)
	c := remoteCertifiedFixture(t, n)
	backend := remoteCertifiedReceiptBackend(t, c)
	changed := receipt
	changed.local.observation.configuration[0] ^= 1
	if round, e := backend.ReceiptRound(t.Context(), changed); round != 0 || !errors.Is(e, errInvalid) {
		t.Fatal("changed configuration borrowed native receipt", round, e)
	}
	before := n.hosts[0][0].driver.Applied()
	if result, e := backend.Write(t.Context(), 3, 0, 1); result != (graphCutResult{}) || !errors.Is(e, errInvalid) {
		t.Fatal("invalid fence phase reached effects", result, e)
	}
	if n.hosts[0][0].driver.Applied() != before {
		t.Fatal("invalid phase wrote graph/cut state")
	}
	if result, e := backend.Write(t.Context(), 99, graphCutFence, 1); result != (graphCutResult{}) || !errors.Is(e, errGraphCutScope) {
		t.Fatal(result, e)
	}
	native, _ := certifiedCoordinator(n)
	read := certifiedOpen(t, native, graphReadRequest{mode: graphReadFresh}, nil)
	cut, e := read.Cut()
	if e != nil {
		t.Fatal(e)
	}
	cert := cut.certificates[0]
	cert.scope.partition = 99
	source, cfg, _, e := backend.Capture(t.Context(), cert)
	if source != nil || cfg != (genesisAllocationConfig{}) || !errors.Is(e, errGraphCutScope) {
		t.Fatal("unknown authority supplied retained source", source, cfg, e)
	}
}

func TestRemoteCertifiedCutCollectChargesPendingNativeManifestWork(t *testing.T) {
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, p := n.crossPlan(ops, claims, 24)
	n.crossCommit(tx, p)
	c := remoteCertifiedFixture(t, n)
	l := c.host.machine.limits
	arena, e := graphstate.NewOutputBudget(l.outputBytes)
	if e != nil {
		t.Fatal(e)
	}
	budget := newGraphRemoteBudget(l, arena)
	// The genuine stored registration must be read to identify its owner. Its
	// payload alone exceeds this total allowance, independently of any reported
	// work counter or network framing. A tiny scalar reply cannot make it free.
	budget.maxBytes = len(tx.manifest) - 1
	endpoints, e := c.endpoints(budget)
	if e != nil {
		t.Fatal(e)
	}
	b := &remoteCertifiedGraphBackend{c, endpoints, budget, arena}
	o, e := b.Read(t.Context(), 8, graphCutQuery{kind: 1})
	if !errors.Is(e, errLimit) || o != (graphCutObservation{}) {
		t.Fatalf("native pending manifest escaped cumulative allowance: manifest=%d wire charged=%d observation=%+v err=%v", len(tx.manifest), budget.bytes, o, e)
	}
}

func TestRemoteCertifiedNativeTransportFreshAndRetainedAt(t *testing.T) {
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	c := remoteCertifiedFixture(t, n)
	old, e := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if e != nil {
		t.Fatal("remote Fresh", e)
	}
	defer old.Close()
	cut, e := old.Cut()
	if e != nil {
		t.Fatal(e)
	}
	tx, p := n.crossPlan(ops, claims, 24)
	decision := n.crossCommit(tx, p)
	resolve, e := n.hosts[0][0].ResolveCrossParticipant(3, decision)
	if e != nil {
		t.Fatal(e)
	}
	n.crossSubmit(0, resolve)
	fresh, e := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if e != nil {
		t.Fatal("remote closes B", e)
	}
	defer fresh.Close()
	assertCertifiedIncident(t, certifiedView(t, fresh), ops[0].Scope, 1, 2, []graphstate.EntityID{3})
	assertCertifiedIncident(t, certifiedView(t, fresh), ops[0].Scope, 17, 18, []graphstate.EntityID{3})
	assertCertifiedIncident(t, certifiedView(t, old), ops[0].Scope, 1, 2, nil)
	if e := fresh.Close(); e != nil {
		t.Fatal(e)
	}
	at, e := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadAt, cut: cut}, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer at.Close()
	assertCertifiedIncident(t, certifiedView(t, at), ops[0].Scope, 17, 18, nil)
}
