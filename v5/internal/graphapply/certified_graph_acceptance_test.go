package graphapply

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type certifiedTestTransport struct {
	n                      *allocationTestNetwork
	member                 int
	delayResolve           uint64
	denyDecision           bool
	drop                   uint64
	isolated               bool
	beforeSubmit           func(*allocationHost, allocationProposal)
	participantUnavailable uint64
	participantMissing     bool
}

func (t *certifiedTestTransport) Host(ctx context.Context, p uint64) (*allocationHost, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	for g := range 2 {
		if t.n.hosts[g][t.member].machine.ns.partition == p {
			return t.n.hosts[g][t.member], nil
		}
	}
	return nil, errInvalid
}
func (t *certifiedTestTransport) group(h *allocationHost) int {
	for g := range 2 {
		for _, x := range t.n.hosts[g] {
			if x == h {
				return g
			}
		}
	}
	return -1
}
func (t *certifiedTestTransport) pump(ctx context.Context, h *allocationHost, event allocationEvent) ([]graphCutReply, []crossReply, error) {
	group := t.group(h)
	if group < 0 {
		return nil, nil, errInvalid
	}
	queue := event.packets
	cuts, cross := event.cutReplies, event.crossReplies
	for steps := 0; len(queue) > 0; steps++ {
		if e := ctx.Err(); e != nil {
			return nil, nil, e
		}
		if steps >= 1024 {
			return nil, nil, errLimit
		}
		p := queue[0]
		queue = queue[1:]
		if p.To == t.drop || p.From == t.drop || t.isolated && (p.To != 1 || p.From != 1) {
			continue
		}
		if p.Snapshot || p.To < 1 || p.To > 3 {
			return nil, nil, errInvalid
		}
		e, err := t.n.hosts[group][p.To-1].Step(p)
		if err != nil {
			return nil, nil, err
		}
		if len(queue)+len(e.packets) > 128 {
			return nil, nil, errLimit
		}
		queue = append(queue, e.packets...)
		cuts = append(cuts, e.cutReplies...)
		cross = append(cross, e.crossReplies...)
	}
	return cuts, cross, nil
}
func (t *certifiedTestTransport) Read(ctx context.Context, h *allocationHost, q graphCutQuery) (graphCutReply, error) {
	if q.kind == 4 && h.machine.ns.partition == t.participantUnavailable {
		return graphCutReply{}, replica.ErrUnavailable
	}
	if q.kind == 4 && t.participantMissing {
		return graphCutReply{}, nil
	}
	if e := ctx.Err(); e != nil {
		return graphCutReply{}, e
	}
	event, e := h.ReadGraphCut(q)
	if e != nil {
		return graphCutReply{}, e
	}
	cuts, _, e := t.pump(ctx, h, event)
	if e != nil {
		_ = h.CancelGraphCutRead(event.readID)
		return graphCutReply{}, e
	}
	for _, r := range cuts {
		if r.id == event.readID && r.question == q {
			return r, nil
		}
	}
	_ = h.CancelGraphCutRead(event.readID)
	return graphCutReply{}, replica.ErrUnavailable
}
func (t *certifiedTestTransport) Submit(ctx context.Context, h *allocationHost, p allocationProposal) (graphCutResult, error) {
	if t.beforeSubmit != nil {
		t.beforeSubmit(h, p)
	}
	event, e := h.Submit(p)
	if e != nil {
		return graphCutResult{}, e
	}
	if _, _, e = t.pump(ctx, h, event); e != nil {
		return graphCutResult{}, e
	}
	w, e := h.machine.store.ApplicationRecord(ctx, h.driver.Applied(), true, 256)
	if e != nil {
		return graphCutResult{}, e
	}
	return decodeGraphCutResult(w)
}
func (t *certifiedTestTransport) Decision(ctx context.Context, h *allocationHost, tx, nonce [16]byte) (crossReply, error) {
	if t.denyDecision {
		return crossReply{}, replica.ErrUnavailable
	}
	event, e := h.ReadCrossPhase(tx, crossDecide, nonce)
	if e != nil {
		return crossReply{}, e
	}
	_, replies, e := t.pump(ctx, h, event)
	if e != nil {
		return crossReply{}, e
	}
	for _, r := range replies {
		if r.err != nil {
			return r, nil
		}
		if r.proof.id == event.readID && r.proof.question.tx == tx && r.proof.question.nonce == nonce {
			return r, nil
		}
	}
	return crossReply{}, replica.ErrUnavailable
}
func (t *certifiedTestTransport) Resolve(ctx context.Context, h *allocationHost, p allocationProposal) (crossStatus, error) {
	if h.machine.ns.partition == t.delayResolve {
		return crossStatus{}, replica.ErrUnavailable
	}
	event, e := h.Submit(p)
	if e != nil {
		return crossStatus{}, e
	}
	if _, _, e = t.pump(ctx, h, event); e != nil {
		return crossStatus{}, e
	}
	w, e := h.machine.store.ApplicationRecord(ctx, h.driver.Applied(), true, 256)
	if e != nil {
		return crossStatus{}, e
	}
	return decodeCrossStatus(w)
}
func certifiedCoordinator(n *allocationTestNetwork) (*certifiedReadCoordinator, *certifiedTestTransport) {
	tr := &certifiedTestTransport{n: n}
	return &certifiedReadCoordinator{declaration: n.d, transport: tr}, tr
}
func certifiedOpen(t *testing.T, c *certifiedReadCoordinator, r graphReadRequest, scope []uint64) *certifiedGraphRead {
	t.Helper()
	v, e := c.Open(t.Context(), r, scope)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := v.Close(); e != nil {
			t.Error(e)
		}
	})
	return v
}
func certifiedView(t *testing.T, s *certifiedGraphRead) graphstate.ReadView {
	t.Helper()
	v, e := s.ReadView()
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func certifiedPoint(t *testing.T) temporal.Position {
	t.Helper()
	p, e := temporal.RationalPosition(graphGenesisBinding(t).Axis(), temporal.RationalInt64(5))
	if e != nil {
		t.Fatal(e)
	}
	return p
}
func assertCertifiedProjection(t *testing.T, v graphstate.ReadView, id graphstate.EntityID, active bool, labels []string) {
	t.Helper()
	p, e := graphstate.Project(t.Context(), v, id, certifiedPoint(t), graphstate.Effective, defaultMaterializerLimits().graph.Planner)
	if e != nil || p.Active != active || !slices.Equal(p.Labels, labels) {
		t.Fatal("exact projection", id, p, e)
	}
}
func assertCertifiedIncident(t *testing.T, v graphstate.ReadView, scope temporal.Scope, owner graphstate.EntityID, life graphstate.LifeID, want []graphstate.EntityID) {
	t.Helper()
	p, e := v.IncidentRelationships(t.Context(), graphstate.IncidentPredicate{Endpoint: owner, Life: life, Window: scope}, 0, graphstate.ReadBudget{Rows: 100, Bytes: 1 << 20})
	if e != nil || !p.Complete || !slices.Equal(p.Entities, want) {
		t.Fatal("exact endpoint postings", owner, p, e)
	}
}

func TestCertifiedGraphFreshE15WaitsForDelayedParticipantAndAfterIncludesCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	c, tr := certifiedCoordinator(n)
	old := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, nil)
	oldCut, e := old.Cut()
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
	tr.delayResolve = 8
	if v, e := c.Open(t.Context(), graphReadRequest{mode: graphReadFresh}, nil); !errors.Is(e, replica.ErrUnavailable) || v != nil {
		t.Fatal("half transaction admitted", v, e)
	}
	assertCertifiedIncident(t, certifiedView(t, old), ops[0].Scope, 1, 2, nil)
	assertCertifiedIncident(t, certifiedView(t, old), ops[0].Scope, 17, 18, nil)
	tr.delayResolve = 0
	receipt, e := graphReceiptFromDecision(decision)
	if e != nil {
		t.Fatal(e)
	}
	fresh := certifiedOpen(t, c, graphReadRequest{mode: graphReadAfter, receipt: receipt}, nil)
	v := certifiedView(t, fresh)
	assertCertifiedProjection(t, v, 3, true, nil)
	assertCertifiedIncident(t, v, ops[0].Scope, 1, 2, []graphstate.EntityID{3})
	assertCertifiedIncident(t, v, ops[0].Scope, 17, 18, []graphstate.EntityID{3})
	at := certifiedOpen(t, c, graphReadRequest{mode: graphReadAt, cut: oldCut}, nil)
	edge, e := certifiedView(t, at).Entity(t.Context(), 3)
	if e != nil || edge.Found {
		t.Fatal("old cut gained edge", edge, e)
	}
	stable := certifiedOpen(t, c, graphReadRequest{mode: graphReadStable, cut: oldCut}, nil)
	got, e := stable.Cut()
	if e != nil || got.certificates[0].round != oldCut.certificates[0].round {
		t.Fatal(got, e)
	}
}

func TestCertifiedGraphCollectionFenceRaceExcludesHigherInstalledState(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	ops, _ := crossEdgeFixture(t, n)
	c, tr := certifiedCoordinator(n)
	injected := false
	tr.beforeSubmit = func(h *allocationHost, p allocationProposal) {
		command, e := decodeGraphCutCommand(p.wire)
		if e != nil {
			t.Fatal(e)
		}
		if command.phase != graphCutFence || injected {
			return
		}
		injected = true
		// The first collection has ended. This write must get a larger round, while
		// the cut being certified must select the old graph root and old indexes.
		local, e := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "after-collection", Scope: ops[0].Scope}}, 25, nil)
		if e != nil {
			t.Fatal(e)
		}
		n.submitPartitionGraph(0, local, reasonNone)
	}
	old := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, nil)
	assertCertifiedProjection(t, certifiedView(t, old), 1, true, nil)
	if !injected {
		t.Fatal("race was not exercised")
	}
	tr.beforeSubmit = nil
	current := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, nil)
	assertCertifiedProjection(t, certifiedView(t, current), 1, true, []string{"after-collection"})
}
func TestCertifiedGraphPreFenceCommitResolvesBelowReadFence(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, p := n.crossPlan(ops, claims, 24)
	decision := n.crossCommit(tx, p)
	c, tr := certifiedCoordinator(n)
	b := decision.status.round + 100
	for g := range 2 {
		h := n.hosts[g][0]
		p, e := h.PrepareGraphFence(b)
		if e != nil {
			t.Fatal(e)
		}
		r, e := tr.Submit(t.Context(), h, p)
		if e != nil || graphResultError(r.status) != nil {
			t.Fatal(r, e)
		}
	}
	n.crossResolve(decision)
	fresh := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, nil)
	cut, e := fresh.Cut()
	if e != nil || cut.certificates[0].round < b {
		t.Fatal(cut, e)
	}
	assertCertifiedProjection(t, certifiedView(t, fresh), 3, true, nil)
}
func TestCertifiedGraphE16UnresolvedLowPrepareBlocksHigherObservedRound(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, register := n.crossPlan(ops, claims, 24)
	registered := n.crossSubmit(0, register)
	registration := n.crossReadPhase(0, registered.tx, crossRegister)
	prepare, e := n.hosts[0][0].PrepareCrossParticipant(tx, 3, registration, nil)
	if e != nil {
		t.Fatal(e)
	}
	n.crossSubmit(0, prepare)
	yes := n.crossReadPhase(0, registered.tx, crossPrepare)
	for range 3 {
		h := n.hosts[1][0]
		p, e := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 17, Life: 18, Name: "higher", Scope: ops[0].Scope}}, 25, nil)
		if e != nil {
			t.Fatal(e)
		}
		n.submitPartitionGraph(1, p, reasonNone)
	}
	c, tr := certifiedCoordinator(n)
	if v, e := c.Open(t.Context(), graphReadRequest{mode: graphReadFresh}, nil); !errors.Is(e, errGraphCutPending) || v != nil {
		t.Fatal("unresolved low YES ignored", v, e)
	}
	// Certificate application itself must report pending without stopping Raft.
	h := n.hosts[0][0]
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
	r, e := tr.Submit(t.Context(), h, p)
	if e != nil || r.status != graphCutPending {
		t.Fatal("pending APPLY stopped driver", r, e)
	}
	prepare, e = h.PrepareCrossParticipant(tx, 8, registration, []crossProof{yes})
	if e != nil {
		t.Fatal(e)
	}
	no := n.crossSubmit(1, prepare)
	if no.yes {
		t.Fatal("changed base voted YES", no)
	}
	p, e = h.DecideCrossTransaction(tx, []crossProof{yes, n.crossReadPhase(1, registered.tx, crossPrepare)})
	if e != nil {
		t.Fatal(e)
	}
	n.crossSubmit(0, p)
	n.crossResolve(n.crossReadPhase(0, registered.tx, crossDecide))
	fresh := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, nil)
	edge, e := certifiedView(t, fresh).Entity(t.Context(), 3)
	if e != nil || edge.Found {
		t.Fatal("aborted old prepare visible", edge, e)
	}
	assertCertifiedProjection(t, certifiedView(t, fresh), 17, true, []string{"higher"})
}
func TestCertifiedGraphScopedReadCannotExpandAndEmptyPublishedAuthorityIsRetained(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	c, _ := certifiedCoordinator(n)
	h := n.activate(0, [16]byte{1})
	n.acquire(0, h, 1, 16)
	scoped := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, []uint64{3})
	v := certifiedView(t, scoped)
	r, e := v.Entity(t.Context(), 1)
	if e != nil || r.Found {
		t.Fatal("published empty identity unavailable", r, e)
	}
	remote := n.activate(1, [16]byte{2})
	n.acquire(1, remote, 1, 16)
	cut, e := scoped.Cut()
	if e != nil {
		t.Fatal(e)
	}
	if v, e := c.Open(t.Context(), graphReadRequest{mode: graphReadAt, cut: cut}, nil); !errors.Is(e, errGraphCutScope) || v != nil {
		t.Fatal("scope silently expanded", v, e)
	}
	// The remote published owner is absent in the scoped source, so an entity
	// lookup must refuse unknown authority rather than report graph absence.
	if r, e := v.Entity(t.Context(), 17); !errors.Is(e, graphstore.ErrRoutingUnknown) && !errors.Is(e, errGraphCutScope) || r.Found {
		t.Fatal("remote became absence", r, e)
	}
	full := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, nil)
	r, e = certifiedView(t, full).Entity(t.Context(), 17)
	if e != nil || r.Found {
		t.Fatal("current empty owner not authoritative", r, e)
	}
}
func TestCertifiedGraphFollowerBarrierReopenAndHeldCutHistory(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	c, tr := certifiedCoordinator(n)
	tr.drop = 3
	old := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, nil)
	cut, e := old.Cut()
	if e != nil {
		t.Fatal(e)
	}
	follower, e := n.hosts[0][2].AtGraphCertificate(t.Context(), cut.certificates[0])
	if !errors.Is(e, replica.ErrUnavailable) || follower != nil {
		t.Fatal("unapplied certificate served", follower, e)
	}
	tr.drop = 0
	for g := range 2 {
		event, e := n.hosts[g][0].Tick()
		if e != nil {
			t.Fatal(e)
		}
		if _, _, e = tr.pump(t.Context(), n.hosts[g][0], event); e != nil {
			t.Fatal(e)
		}
	}
	tx, p := n.crossPlan(ops, claims, 24)
	decision := n.crossCommit(tx, p)
	n.crossResolve(decision)
	current := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, nil)
	assertCertifiedProjection(t, certifiedView(t, current), 3, true, nil)
	at := certifiedOpen(t, c, graphReadRequest{mode: graphReadAt, cut: cut}, nil)
	r, e := certifiedView(t, at).Entity(t.Context(), 3)
	if e != nil || r.Found {
		t.Fatal("retained source uses current root", r, e)
	}
	if e := old.Close(); e != nil {
		t.Fatal(e)
	}
	if e := at.Close(); e != nil {
		t.Fatal(e)
	}
	if e := current.Close(); e != nil {
		t.Fatal(e)
	}
	crossReopenAll(t, n)
	tr.member = 2
	at = certifiedOpen(t, c, graphReadRequest{mode: graphReadAt, cut: cut}, nil)
	r, e = certifiedView(t, at).Entity(t.Context(), 3)
	if e != nil || r.Found {
		t.Fatal("reopen lost retained certified history", r, e)
	}
}
func TestCertifiedGraphNilCancellationMalformedCutAndCloseRace(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	var c *certifiedReadCoordinator
	if _, e := c.Open(context.Background(), graphReadRequest{}, nil); !errors.Is(e, errInvalid) {
		t.Fatal(e)
	}
	n := newCertifiedGraphNetwork(t)
	c, tr := certifiedCoordinator(n)
	//nolint:staticcheck // SA1012: nil is an adversarial context input; the contract must refuse it.
	if v, e := c.Open(nil, graphReadRequest{}, nil); !errors.Is(e, errInvalid) || v != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if v, e := c.Open(ctx, graphReadRequest{mode: graphReadFresh}, nil); !errors.Is(e, context.Canceled) || v != nil {
		t.Fatal(e)
	}
	read := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, nil)
	cut, e := read.Cut()
	if e != nil {
		t.Fatal(e)
	}
	for _, mutate := range []func(*graphCut){func(c *graphCut) { c.certificates = c.certificates[:1] }, func(c *graphCut) { c.certificates[0].scope.graph[0] ^= 1 }, func(c *graphCut) { c.certificates[0].image[0] ^= 1 }, func(c *graphCut) { c.certificates[0].index++ }, func(c *graphCut) { c.certificates[0].scope.group[0] ^= 1 }, func(c *graphCut) { c.certificates[1].round++ }} {
		bad := graphCut{certificates: slices.Clone(cut.certificates)}
		mutate(&bad)
		if v, e := c.Open(t.Context(), graphReadRequest{mode: graphReadAt, cut: bad}, nil); e == nil || v != nil {
			t.Fatal("forged cut admitted", v, e)
		}
	}
	tr.drop = 2
	h := n.hosts[0][0]
	nonce, e := graphReadNonce()
	if e != nil {
		t.Fatal(e)
	}
	event, e := h.ReadGraphCut(graphCutQuery{kind: 1, nonce: nonce})
	if e != nil {
		t.Fatal(e)
	}
	if e := h.CancelGraphCutRead(event.readID); e != nil {
		t.Fatal(e)
	}
	if _, _, e := tr.pump(t.Context(), h, event); e != nil {
		t.Fatal(e)
	}
	tr.drop = 0
	var wg sync.WaitGroup
	wg.Go(func() { _ = read.Work(); _, _ = read.ReadView() })
	wg.Go(func() { _ = read.Close() })
	wg.Wait()
	if e := read.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e := read.ReadView(); !errors.Is(e, graphstore.ErrClosed) {
		t.Fatal(e)
	}
}

func TestCertifiedGraphAfterLocalReceiptIncludesActualMappedMutation(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	ops, _ := crossEdgeFixture(t, n)
	c, tr := certifiedCoordinator(n)
	h := n.hosts[1][0]
	id := n.requestID()
	p, e := h.PreparePartitionGraphOperations(id, []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 17, Life: 18, Name: "local-receipt", Scope: ops[0].Scope}}, 25, nil)
	if e != nil {
		t.Fatal(e)
	}
	n.submitPartitionGraph(1, p, reasonNone)
	nonce, e := graphReadNonce()
	if e != nil {
		t.Fatal(e)
	}
	r, e := tr.Read(t.Context(), h, graphCutQuery{kind: 3, request: [16]byte(id), nonce: nonce})
	if e != nil || r.err != nil {
		t.Fatal(r.err, e)
	}
	receipt, e := graphReceiptFromLocal(r.proof)
	if e != nil {
		t.Fatal(e)
	}
	read := certifiedOpen(t, c, graphReadRequest{mode: graphReadAfter, receipt: receipt}, nil)
	assertCertifiedProjection(t, certifiedView(t, read), 17, true, []string{"local-receipt"})
	if v, e := c.Open(t.Context(), graphReadRequest{mode: graphReadAfter}, nil); !errors.Is(e, errInvalid) || v != nil {
		t.Fatal("empty receipt asserted transaction", v, e)
	}
}
func assertCertifiedClaim(t *testing.T, v graphstate.ReadView, window temporal.Scope, value int64, want []graphstate.EntityID) {
	t.Helper()
	definition := partitionWriteSchemas()[1]
	p, e := v.UniqueCandidates(t.Context(), graphstate.UniquePredicate{Definition: definition, Value: graphstate.I64(value), Window: window}, 0, graphstate.ReadBudget{Rows: 100, Bytes: 1 << 20})
	if e != nil || !p.Complete {
		t.Fatal(p, e)
	}
	ids := make([]graphstate.EntityID, len(p.Claims))
	for i, c := range p.Claims {
		ids[i] = c.Owner
	}
	if !slices.Equal(ids, want) {
		t.Fatal("exact unique candidates", value, ids, want)
	}
}
func TestCertifiedGraphE18OldCorrectionIndexAndEndpointLivesRemainExact(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, p := n.crossPlan(ops, claims, 24)
	n.crossResolve(n.crossCommit(tx, p))
	grant := claims[0].grant
	set := []graphstate.Operation{{Kind: graphstate.Set, Owner: 1, Life: 2, Name: "p", Value: graphstate.I64(9), ValueID: 5, Scope: ops[0].Scope}, {Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "old-label", Scope: ops[0].Scope}}
	tx, p = n.crossPlan(set, []freshBinding{{role: valueBinding, id: 5, grant: grant}}, 25)
	n.crossResolve(n.crossCommit(tx, p))
	c, _ := certifiedCoordinator(n)
	old := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, nil)
	oldCut, e := old.Cut()
	if e != nil {
		t.Fatal(e)
	}
	// Correction moves the unique posting and the label, then closes the remote
	// endpoint life and cascades graph presence. Both old roots must survive.
	change := []graphstate.Operation{{Kind: graphstate.Set, Owner: 1, Life: 2, Name: "p", Value: graphstate.I64(10), ValueID: 6, Scope: ops[0].Scope}, {Kind: graphstate.RemoveLabel, Owner: 1, Life: 2, Name: "old-label", Scope: ops[0].Scope}, {Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "new-label", Scope: ops[0].Scope}, {Kind: graphstate.Correct, Owner: 17, Life: 18, Scope: ops[0].Scope, Present: false}}
	tx, p = n.crossPlan(change, []freshBinding{{role: valueBinding, id: 6, grant: grant}}, 26)
	n.crossResolve(n.crossCommit(tx, p))
	current := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, nil)
	for _, sample := range []struct {
		read *certifiedGraphRead
		old  bool
	}{{old, true}, {certifiedOpen(t, c, graphReadRequest{mode: graphReadAt, cut: oldCut}, nil), true}, {current, false}} {
		v := certifiedView(t, sample.read)
		label := "new-label"
		oldOwners, newOwners := []graphstate.EntityID(nil), []graphstate.EntityID{1}
		if sample.old {
			label = "old-label"
			oldOwners, newOwners = newOwners, oldOwners
		}
		assertCertifiedProjection(t, v, 1, true, []string{label})
		assertCertifiedProjection(t, v, 3, sample.old, nil)
		assertCertifiedClaim(t, v, ops[0].Scope, 9, []graphstate.EntityID{1})
		assertCertifiedEffectiveClaim(t, v, ops[0].Scope, 9, oldOwners)
		assertCertifiedEffectiveClaim(t, v, ops[0].Scope, 10, newOwners)
		assertCertifiedClaim(t, v, ops[0].Scope, 10, newOwners)
		assertCertifiedClaim(t, v, ops[0].Scope, 999, nil)
		binding, e := v.Life(t.Context(), 3, 4)
		if e != nil || !binding.Found || binding.Record.SourceLife != 2 || binding.Record.TargetLife != 18 {
			t.Fatal("immutable old endpoint lives", binding, e)
		}
		// Canonical and declared posting doors both retain the edge binding; visibility
		// comes from the same retained primary/components, rather than current state.
		for _, ep := range []struct {
			id   graphstate.EntityID
			life graphstate.LifeID
		}{{1, 2}, {17, 18}, {1, 0}, {17, 0}} {
			assertCertifiedIncident(t, v, ops[0].Scope, ep.id, ep.life, []graphstate.EntityID{3})
		}
	}
}

// Candidate indexes preserve a declared superset; exact matches hydrate through
// the same retained cut rather than borrowing current component state.
func assertCertifiedEffectiveClaim(t *testing.T, v graphstate.ReadView, window temporal.Scope, value int64, want []graphstate.EntityID) {
	t.Helper()
	p, e := v.UniqueCandidates(t.Context(), graphstate.UniquePredicate{Definition: partitionWriteSchemas()[1], Value: graphstate.I64(value), Window: window}, 0, graphstate.ReadBudget{Rows: 100, Bytes: 1 << 20})
	if e != nil || !p.Complete {
		t.Fatal(p, e)
	}
	var ids []graphstate.EntityID
	for _, claim := range p.Claims {
		page, e := v.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: claim.Key, Window: window}, 0, graphstate.ReadBudget{Rows: 100, Bytes: 1 << 20})
		if e != nil || !page.Complete {
			t.Fatal(page, e)
		}
		for _, piece := range page.Data.Pieces() {
			if !piece.Cell().Present() {
				continue
			}
			ref := piece.Cell().Value()
			r, e := v.Value(t.Context(), graphstate.ValueID(ref.ID()))
			if e != nil || !r.Found {
				t.Fatal(r, e)
			}
			same, e := r.Value.Equal(graphstate.I64(value), defaultMaterializerLimits().graph.Planner)
			if e != nil {
				t.Fatal(e)
			}
			if same && !slices.Contains(ids, claim.Owner) {
				ids = append(ids, claim.Owner)
			}
		}
	}
	if !slices.Equal(ids, want) {
		t.Fatal("exact effective candidates", value, ids, want)
	}
}

func TestCertifiedGraphQuorumLossProducesNoCutAndBoundsCancelledRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	c, tr := certifiedCoordinator(n)
	tr.isolated = true
	bounded := false
	for i := range 40 {
		read, e := c.Open(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
		if read != nil {
			t.Fatal("quorum loss published a cut")
		}
		if errors.Is(e, replica.ErrLimit) || errors.Is(e, errLimit) {
			bounded = true
			if i > 32 {
				t.Fatal("cancelled Driver queue grew beyond32", i)
			}
			break
		}
		if !errors.Is(e, replica.ErrUnavailable) {
			t.Fatal("wrong quorum failure", e)
		}
	}
	if !bounded {
		t.Fatal("cancellation reset outstanding Driver quota")
	}
	tr.isolated = false
	h := n.hosts[0][0]
	event, e := h.Tick()
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e := tr.pump(t.Context(), h, event); e != nil {
		t.Fatal(e)
	}
	read := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, nil)
	cut, e := read.Cut()
	if e != nil || len(cut.certificates) != 2 {
		t.Fatal("quorum recovery did not release cancelled requests", cut, e)
	}
}
