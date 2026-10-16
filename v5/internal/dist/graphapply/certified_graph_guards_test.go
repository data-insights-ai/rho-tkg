package graphapply

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func TestCertifiedGraphCodecsAndPointerHelpersRefuseMissingAuthority(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	var host *allocationHost
	for _, call := range []func() error{func() error { _, e := host.PrepareGraphFence(1); return e }, func() error { _, e := host.PrepareGraphCertificate(1); return e }, func() error { _, e := host.ReadGraphCut(graphCutQuery{}); return e }, func() error { return host.CancelGraphCutRead(allocationReadID{}) }, func() error { _, e := host.AtGraphCertificate(t.Context(), graphCertificate{}); return e }} {
		if e := call(); !errors.Is(e, errInvalid) {
			t.Fatal(e)
		}
	}
	var read *certifiedGraphRead
	if _, e := read.Cut(); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	if _, e := read.ReadView(); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	if e := read.Close(); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	if read.Work() != (graphstore.PageWork{}) {
		t.Fatal(read.Work())
	}
	if _, e := newCertifiedGraphMaterializer(nil, namespace{}, graphstore.OwnershipDeclaration{}, materializerLimits{}); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	if _, e := graphReceiptFromDecision(crossProof{}); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	if _, e := graphReceiptFromLocal(graphCutProof{}); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	var tr *certifiedTestTransport
	c := &certifiedReadCoordinator{transport: tr}
	if _, e := c.Open(t.Context(), graphReadRequest{}, nil); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	if _, e := certifiedScope(graphstore.OwnershipDeclaration{}, nil); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	for _, wire := range [][]byte{nil, {}, []byte("bad"), make([]byte, 512)} {
		if _, e := decodeGraphCutCommand(wire); !errors.Is(e, errCorrupt) {
			t.Fatal(e)
		}
		if _, e := decodeGraphCertificate(wire); !errors.Is(e, errCorrupt) {
			t.Fatal(e)
		}
		if _, e := decodeGraphCutResult(wire); !errors.Is(e, errCorrupt) {
			t.Fatal(e)
		}
		if _, e := decodeGraphLocalReceipt(wire); !errors.Is(e, errCorrupt) {
			t.Fatal(e)
		}
	}
	for _, status := range []byte{0, graphCutApplied, graphCutPending, graphCutStale, graphCutLimit, 99} {
		e := graphResultError(status)
		switch status {
		case graphCutApplied:
			if e != nil {
				t.Fatal(e)
			}
		case graphCutPending:
			if !errors.Is(e, errGraphCutPending) {
				t.Fatal(e)
			}
		case graphCutStale:
			if !errors.Is(e, errInvalid) {
				t.Fatal(e)
			}
		case graphCutLimit:
			if !errors.Is(e, errLimit) {
				t.Fatal(e)
			}
		default:
			if !errors.Is(e, errCorrupt) {
				t.Fatal(e)
			}
		}
	}
	n := newCertifiedGraphNetwork(t)
	h := n.hosts[0][0]
	if _, e := newCertifiedGraphMaterializer(h.machine.store, h.machine.ns, n.d, h.machine.limits); e != nil {
		t.Fatal(e)
	}
	cfg, e := h.currentConfiguration()
	if e != nil {
		t.Fatal(e)
	}
	configuration, e := cfg.digest()
	if e != nil {
		t.Fatal(e)
	}
	base := graphCutCommand{scope: h.machine.scope(), configuration: configuration, round: 1, phase: graphCutFence}
	for _, mutate := range []func(*graphCutCommand){func(c *graphCutCommand) { c.scope.group = [16]byte{} }, func(c *graphCutCommand) { c.scope.semantic = crossPartitionSemanticContractID() }, func(c *graphCutCommand) { c.configuration = [32]byte{} }, func(c *graphCutCommand) { c.round = 0 }, func(c *graphCutCommand) { c.round = math.MaxUint64 }, func(c *graphCutCommand) { c.phase = 0 }} {
		bad := base
		mutate(&bad)
		if _, e := encodeGraphCutCommand(bad); !errors.Is(e, errInvalid) {
			t.Fatal(e)
		}
	}
	for _, round := range []uint64{0, math.MaxUint64} {
		if _, e := h.PrepareGraphFence(round); !errors.Is(e, errInvalid) {
			t.Fatal(e)
		}
	}
	if _, e := h.completeGraphCutRead(nil, h.driver.Applied()); !errors.Is(e, errCorrupt) {
		t.Fatal(e)
	}
	if e := h.CancelGraphCutRead(allocationReadID{}); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	c, tr2 := certifiedCoordinator(n)
	if _, e := c.Open(t.Context(), graphReadRequest{mode: graphReadStable}, nil); !errors.Is(e, replica.ErrUnavailable) {
		t.Fatal(e)
	}
	if _, e := c.Open(t.Context(), graphReadRequest{}, nil); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	for _, scope := range [][]uint64{{3, 3}, {99}, {3, 8, 99}} {
		if _, e := c.Open(t.Context(), graphReadRequest{mode: graphReadFresh}, scope); !errors.Is(e, errInvalid) {
			t.Fatal(e)
		}
	}
	p, e := h.PrepareGraphCertificate(7)
	if e != nil {
		t.Fatal(e)
	}
	result, e := tr2.Submit(t.Context(), h, p)
	if e != nil || result.status != graphCutPending {
		t.Fatal(result, e)
	}
	p, e = h.PrepareGraphFence(7)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = tr2.Submit(t.Context(), h, p); e != nil {
		t.Fatal(e)
	}
	if _, e = tr2.Submit(t.Context(), h, p); e != nil {
		t.Fatal("exact fence retry", e)
	}
	p, e = h.PrepareGraphCertificate(7)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = tr2.Submit(t.Context(), h, p); e != nil {
		t.Fatal(e)
	}
	if _, e = tr2.Submit(t.Context(), h, p); e != nil {
		t.Fatal("exact certificate retry", e)
	}
	nonce, e := graphReadNonce()
	if e != nil {
		t.Fatal(e)
	}
	reply, e := tr2.Read(t.Context(), h, graphCutQuery{kind: 2, round: 7, nonce: nonce})
	if e != nil || reply.err != nil {
		t.Fatal(e, reply.err)
	}
	certificate := reply.proof.observation.certificate
	//nolint:staticcheck // SA1012: nil is an adversarial context input; the contract must refuse it.
	if _, e := h.AtGraphCertificate(nil, certificate); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if v, e := h.AtGraphCertificate(ctx, certificate); !errors.Is(e, context.Canceled) || v != nil {
		t.Fatal(v, e)
	}
	bad := certificate
	bad.sourceIndex = bad.index
	if _, e := encodeGraphCertificate(bad); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	wire, e := encodeGraphCertificate(certificate)
	if e != nil {
		t.Fatal(e)
	}
	wire[4] ^= 1
	if _, e := decodeGraphCertificate(wire); !errors.Is(e, errCorrupt) {
		t.Fatal(e)
	}
	if e := h.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e := h.PrepareGraphFence(1); !errors.Is(e, replica.ErrStopped) {
		t.Fatal(e)
	}
	if _, e := h.ReadGraphCut(graphCutQuery{kind: 1, nonce: [16]byte{1}}); !errors.Is(e, replica.ErrStopped) {
		t.Fatal(e)
	}
	if e := h.CancelGraphCutRead(allocationReadID{}); !errors.Is(e, replica.ErrStopped) {
		t.Fatal(e)
	}
	if _, e := h.AtGraphCertificate(t.Context(), certificate); !errors.Is(e, replica.ErrStopped) {
		t.Fatal(e)
	}
}
func TestCertifiedGraphAggregateBudgetsConstructorCleanupAndCumulativeReads(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	crossEdgeFixture(t, n)
	c, _ := certifiedCoordinator(n)
	read := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, nil)
	cut, e := read.Cut()
	if e != nil {
		t.Fatal(e)
	}
	if e := read.Close(); e != nil {
		t.Fatal(e)
	}
	h := n.hosts[0][0]
	original := h.machine.limits
	for _, mutate := range []func(*materializerLimits){func(l *materializerLimits) { l.sourceRows = 1 }, func(l *materializerLimits) { l.sourceBytes = 172 }, func(l *materializerLimits) { l.outputBytes = 8192 }} {
		for range 20 {
			h.machine.limits = original
			mutate(&h.machine.limits)
			v, e := c.Open(t.Context(), graphReadRequest{mode: graphReadAt, cut: cut}, nil)
			if v != nil || !errors.Is(e, errLimit) && !errors.Is(e, graphstore.ErrResourceLimit) && !errors.Is(e, raftlog.ErrLimit) {
				t.Fatal("unbounded/partial constructor", v, e)
			}
		}
	}
	h.machine.limits = original
	remote := n.hosts[1][0]
	remoteOriginal := remote.machine.limits
	remote.machine.limits.outputBytes = 8192
	if v, e := c.Open(t.Context(), graphReadRequest{mode: graphReadAt, cut: cut}, nil); v != nil || !errors.Is(e, errLimit) {
		t.Fatal("second partition cap widened", v, e)
	}
	remote.machine.limits = remoteOriginal
	read = certifiedOpen(t, c, graphReadRequest{mode: graphReadAt, cut: cut}, nil)
	v := certifiedView(t, read)
	before := read.Work()
	exhausted := false
	for i := range original.sourceRows + 1 {
		id := graphstate.EntityID(16)
		if i%2 == 1 {
			id = 32
		}
		r, e := v.Entity(t.Context(), id)
		if e != nil {
			if !errors.Is(e, graphstore.ErrResourceLimit) && !errors.Is(e, raftlog.ErrLimit) && !errors.Is(e, errLimit) {
				t.Fatal(e)
			}
			if r != (graphstate.EntityRead{}) {
				t.Fatal("partial error output", r)
			}
			exhausted = true
			break
		}
		if r.Found {
			t.Fatal("phantom entity", r)
		}
	}
	if !exhausted || read.Work().Bytes <= before.Bytes || read.Work().Records <= before.Records {
		t.Fatal("fresh per-call quota", read.Work(), before)
	}
	if e := read.Close(); e != nil {
		t.Fatal(e)
	}
	if w := read.Work(); w.Records == 0 || w.Bytes == 0 {
		t.Fatal("close erased work", w)
	}
	if _, e := read.Cut(); !errors.Is(e, graphstore.ErrClosed) {
		t.Fatal(e)
	}
}
func TestCertifiedGraphScopedReaderConstructorDirectAndBorrowLifetime(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	crossEdgeFixture(t, n)
	c, _ := certifiedCoordinator(n)
	read := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, nil)
	cut, e := read.Cut()
	if e != nil {
		t.Fatal(e)
	}
	source, e := n.hosts[1][0].AtGraphCertificate(t.Context(), cut.certificates[1])
	if e != nil {
		t.Fatal(e)
	}
	defer source.Close()
	h := n.hosts[1][0]
	cfg, e := h.currentConfiguration()
	if e != nil {
		t.Fatal(e)
	}
	configuration, e := cfg.digest()
	if e != nil {
		t.Fatal(e)
	}
	l := h.machine.limits
	budget := graphstore.OwnershipBudget{SourceRows: l.sourceRows, SourceBytes: l.sourceBytes, OutputBytes: l.outputBytes}
	arena, e := graphstate.NewOutputBudget(l.outputBytes)
	if e != nil {
		t.Fatal(e)
	}
	view, _, e := graphstore.OpenPartitionScopedReadView(t.Context(), []*raftlog.ApplicationView{source}, []uint64{8}, n.d, cfg.defaultAxis, configuration, l.catalog, l.graph, budget, arena)
	if e != nil {
		t.Fatal(e)
	}
	r, e := view.Entity(t.Context(), 17)
	if e != nil || !r.Found {
		t.Fatal(r, e)
	}
	if r, e := view.Entity(t.Context(), 1); !errors.Is(e, errGraphCutScope) && !errors.Is(e, graphstore.ErrRoutingUnknown) || r.Found {
		t.Fatal("missing primary authority became absence", r, e)
	}
	if e := view.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e := source.Root(); e != nil {
		t.Fatal("borrow closed", e)
	}
	for _, scope := range [][]uint64{nil, {3}, {8, 8}} {
		if v, _, e := graphstore.OpenPartitionScopedReadView(t.Context(), []*raftlog.ApplicationView{source}, scope, n.d, cfg.defaultAxis, configuration, l.catalog, l.graph, budget, arena); e == nil || v != nil {
			t.Fatal("wrong scope admitted", v, e)
		}
	}
}

func TestCertifiedGraphStaleCertificateApplyAndPredecessorBudgetAreNonfatal(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	h := n.hosts[0][0]
	_, tr := certifiedCoordinator(n)
	cfg, e := h.currentConfiguration()
	if e != nil {
		t.Fatal(e)
	}
	configuration, e := cfg.digest()
	if e != nil {
		t.Fatal(e)
	}
	command := graphCutCommand{scope: h.machine.scope(), configuration: configuration, round: 1, phase: graphCutFence}
	command.configuration[0] ^= 1
	wire, e := encodeGraphCutCommand(command)
	if e != nil {
		t.Fatal(e)
	}
	result, e := tr.Submit(t.Context(), h, allocationProposal{target: h.machine.scope(), wire: wire})
	if e != nil || result.status != graphCutStale {
		t.Fatal("changed configuration poisoned application", result, e)
	}
	if _, e := h.Tick(); e != nil {
		t.Fatal("expected refusal stopped driver", e)
	}
	view, e := h.machine.store.ApplicationView(h.driver.Applied())
	if e != nil {
		t.Fatal(e)
	}
	defer view.Close()
	base, e := view.RootBounded(t.Context(), 172)
	if e != nil {
		t.Fatal(e)
	}
	q := reader{ctx: t.Context(), view: view, ns: h.machine.ns, base: base, limits: h.machine.limits.allocation}
	q.arena, _ = graphstate.NewOutputBudget(1)
	if _, _, _, e := readGraphRoundPredecessor(&q, configuration, 1); !errors.Is(e, errLimit) {
		t.Fatal("unadmitted predecessor ownership", e)
	}
	q.arena = nil
	q.limits.readRows = 0
	if _, _, _, e := readGraphRoundPredecessor(&q, configuration, 1); !errors.Is(e, errLimit) {
		t.Fatal(e)
	}
	body := []byte{'G', 'S', 'O', 1, 9, 9}
	body = append(body, make([]byte, 16)...)
	if _, e := decodeGraphCutResult(seal(body)); !errors.Is(e, errCorrupt) {
		t.Fatal(e)
	}
	command.configuration = configuration
	wire, e = encodeGraphCutCommand(command)
	if e != nil {
		t.Fatal(e)
	}
	wire[len(wire)-40] = 0
	wire[len(wire)-33] = 0
	wire = seal(wire[:len(wire)-32])
	if _, e := decodeGraphCutCommand(wire); !errors.Is(e, errCorrupt) {
		t.Fatal(e)
	}
}
func TestCertifiedGraphYESLockRefusesPublicationWithoutStrandingCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, p := n.crossPlan(ops, claims, 24)
	decision := n.crossCommit(tx, p)
	h := n.hosts[0][0]
	before, e := h.machine.observeAllocation(allocationQuery{kind: observeConfiguration, nonce: [16]byte{1}}, h.driver.Applied())
	if e != nil {
		t.Fatal(e)
	}
	proposal, e := h.BeginRecipient(n.requestID(), [16]byte{9}, n.read(0, allocationQuery{kind: observeConfiguration}))
	if e != nil {
		t.Fatal(e)
	}
	event, e := h.Submit(proposal)
	if e != nil {
		t.Fatal(e)
	}
	n.crossPump(0, event)
	wire, e := h.machine.store.ApplicationRecord(t.Context(), h.driver.Applied(), true, 256)
	if e != nil {
		t.Fatal(e)
	}
	out, e := decodeDeclaredOutcome(wire, h.machine.ns)
	if e != nil || out.reason != reasonStale {
		t.Fatal("allocation mutated prepared root", out, e)
	}
	after, e := h.machine.observeAllocation(allocationQuery{kind: observeConfiguration, nonce: [16]byte{2}}, h.driver.Applied())
	if e != nil || after.epoch != before.epoch || after.effect != before.effect {
		t.Fatal("YES publication changed semantic root", before, after, e)
	}
	n.crossResolve(decision)
	c, _ := certifiedCoordinator(n)
	read := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, nil)
	assertCertifiedProjection(t, certifiedView(t, read), 3, true, nil)
}

func TestCertifiedGraphRegistrationOnlyOwnerPreventsClosureWithoutStoppingDriver(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	_, registration := n.crossPlan(ops, claims, 24)
	registered := n.crossSubmit(0, registration)
	h := n.hosts[0][0]
	_, tr := certifiedCoordinator(n)
	p, e := h.PrepareGraphFence(100)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = tr.Submit(t.Context(), h, p); e != nil {
		t.Fatal(e)
	}
	p, e = h.PrepareGraphCertificate(100)
	if e != nil {
		t.Fatal(e)
	}
	result, e := tr.Submit(t.Context(), h, p)
	if e != nil || result.status != graphCutPending {
		t.Fatal("registration completion owner ignored", registered, result, e)
	}
	if _, e := h.Tick(); e != nil {
		t.Fatal("pending poisoned Driver", e)
	}
}

func TestCertifiedGraphApplySourceLimitReturnsTypedOutcomeWithoutPartialFence(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	h := n.hosts[0][0]
	proposal, e := h.PrepareGraphFence(17)
	if e != nil {
		t.Fatal(e)
	}
	view, e := h.machine.store.ApplicationView(h.driver.Applied())
	if e != nil {
		t.Fatal(e)
	}
	defer view.Close()
	base, e := view.RootBounded(t.Context(), 172)
	if e != nil {
		t.Fatal(e)
	}
	q := reader{ctx: t.Context(), view: view, ns: h.machine.ns, base: base, limits: h.machine.limits.allocation}
	q.limits.readRows = 2
	batch, e := h.machine.stageGraphCut(&q, replica.Entry{Index: base.Index + 1, Generation: base.Generation, Data: proposal.wire}, h.machine.store.ApplicationBudget())
	if e != nil || len(batch.Writes) != 0 || len(batch.ControlPuts) != 0 {
		t.Fatal("source budget stopped application or leaked fence", batch, e)
	}
	outcome, e := decodeGraphCutResult(batch.Outcome)
	if e != nil || outcome.status != graphCutLimit {
		t.Fatal("source limit not typed", outcome, e)
	}
}

func TestCertifiedGraphInitialMapCannotBeInventedOrRebound(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	h := n.hosts[0][0]
	cfg, e := h.currentConfiguration()
	if e != nil {
		t.Fatal(e)
	}
	configuration, e := cfg.digest()
	if e != nil {
		t.Fatal(e)
	}
	view, e := h.machine.store.ApplicationView(h.driver.Applied())
	if e != nil {
		t.Fatal(e)
	}
	row, found, e := view.Get(t.Context(), partitionRoundMapKey(h.machine.ns, 0), 1024)
	if e != nil || !found {
		t.Fatal(found, e)
	}
	if e := view.Close(); e != nil {
		t.Fatal(e)
	}
	for _, mutate := range []func([]byte){func(w []byte) { w[4] ^= 1 }, func(w []byte) { binary.BigEndian.PutUint64(w[44:52], math.MaxUint64) }, func(w []byte) { w[52] ^= 1 }} {
		damaged := bytes.Clone(row.Value[:len(row.Value)-32])
		mutate(damaged)
		index := corruptDeclaredKV(t, h.machine.store, raftlog.KV{Key: row.Key, Value: seal(damaged)})
		v, e := h.machine.store.ApplicationView(index)
		if e != nil {
			t.Fatal(e)
		}
		base, e := v.RootBounded(t.Context(), 172)
		if e != nil {
			t.Fatal(e)
		}
		q := reader{ctx: t.Context(), view: v, ns: h.machine.ns, base: base, limits: h.machine.limits.allocation}
		if e := h.machine.checkCertifiedPrerequisites(&q, configuration); !errors.Is(e, errCorrupt) {
			t.Fatal("incomplete/rebound initial map accepted", e)
		}
		if e := v.Close(); e != nil {
			t.Fatal(e)
		}
	}
}

func TestCertifiedGraphAtAppliedAuthorityWithoutPersistedCertificateIsUnavailable(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	issuer := newCertifiedGraphNetwork(t)
	coordinator, _ := certifiedCoordinator(issuer)
	read := certifiedOpen(t, coordinator, graphReadRequest{mode: graphReadFresh}, nil)
	cut, e := read.Cut()
	if e != nil {
		t.Fatal(e)
	}
	cert := cut.certificates[0]
	target := newCertifiedGraphNetwork(t)
	_, transport := certifiedCoordinator(target)
	host := target.hosts[0][0]
	if host.machine.scope() != cert.scope {
		t.Fatal("fixture scope must match genuine certificate")
	}
	for b := uint64(1); host.driver.Applied() < cert.index; b++ {
		p, e := host.PrepareGraphFence(b)
		if e != nil {
			t.Fatal(e)
		}
		out, e := transport.Submit(t.Context(), host, p)
		if e != nil || out.status != graphCutApplied {
			t.Fatal(out, e)
		}
	}
	// Real applied progress satisfies the positional barrier, but no certificate
	// was ever proposed or installed in this current authority's durable store.
	if view, e := host.AtGraphCertificate(t.Context(), cert); view != nil || !errors.Is(e, replica.ErrUnavailable) {
		t.Fatal("applied offset substituted for persisted certificate authority", view, e)
	}
}
