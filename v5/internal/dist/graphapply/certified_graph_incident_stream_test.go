package graphapply

import (
	"context"
	"errors"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestCertifiedIncidentStreamRetainsCutAndCommitsOnlyHydratedChunks(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	second := ops[0]
	second.Owner, second.Life = 5, 6
	ops = append(ops, second)
	for _, claim := range slices.Clone(claims) {
		if claim.role == entityBinding {
			claim.id = 5
		} else {
			claim.owner, claim.id = 5, 6
		}
		claims = append(claims, claim)
	}
	tx, p := n.crossPlan(ops, claims, 24)
	decision := n.crossCommit(tx, p)
	n.crossResolve(decision)
	native, _ := certifiedCoordinator(n)
	pinned := certifiedOpen(t, native, graphReadRequest{mode: graphReadFresh}, nil)
	cut, err := pinned.Cut()
	if err != nil {
		t.Fatal(err)
	}
	remote := remoteCertifiedFixture(t, n)
	at := certifiedPoint(t)
	point, err := temporal.Point(at)
	if err != nil {
		t.Fatal(err)
	}
	predicate := graphstate.IncidentPredicate{Endpoint: 1, Life: 2, Window: point}

	// Direct composite API parity reuses this genuine native retained view.
	direct, err := pinned.full.IncidentRelationshipsChunk(t.Context(), predicate, 0, raftlog.ReadBudget{Rows: 512, Bytes: 4 << 20}, 1)
	if err != nil || direct.Complete || !slices.Equal(direct.Entities, []graphstate.EntityID{3}) {
		t.Fatal(direct, err)
	}
	capsule, err := pinned.full.ExportIncidentContinuation(t.Context(), predicate, direct.Next)
	if err != nil {
		t.Fatal(err)
	}
	imported, err := pinned.full.ImportIncidentContinuation(t.Context(), predicate, capsule)
	if err != nil {
		t.Fatal(err)
	}
	following, err := pinned.full.IncidentRelationshipsChunk(t.Context(), predicate, imported, raftlog.ReadBudget{Rows: 512, Bytes: 4 << 20}, 1)
	if err != nil || !following.Complete || !slices.Equal(following.Entities, []graphstate.EntityID{5}) {
		t.Fatal(following, err)
	}
	l := defaultMaterializerLimits()
	grant, err := incidentGrant(l, 1)
	if err != nil {
		t.Fatal(err)
	}
	var budget certifiedIncidentBudget
	for range 5 {
		budget = budget.add(grant)
	}
	uncertain := false
	open := func(ctx context.Context, r graphReadRequest, scope []uint64) (*certifiedGraphRead, error) {
		if r.mode != graphReadAt || !slices.Equal(r.cut.certificates, cut.certificates) {
			t.Fatal("cut substituted", r)
		}
		if uncertain {
			uncertain = false
			return nil, graphstore.ErrSourceUnavailable
		}
		return remote.OpenCertified(ctx, r, scope)
	}
	stream, err := openCertifiedIncidentStream(t.Context(), open, l, cut, nil, predicate, at, graphstate.Effective, 1, time.Now().Add(time.Minute), budget)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	initial := stream.Charged()
	if _, err := stream.Next(t.Context(), [16]byte{255}, 1, nil); !errors.Is(err, errInvalid) {
		t.Fatal("foreign request accepted", err)
	}
	if _, err := stream.Next(t.Context(), stream.request, 2, nil); !errors.Is(err, errInvalid) {
		t.Fatal("skipped sequence accepted", err)
	}
	if stream.Charged() != initial {
		t.Fatal("invalid sequence dispatched")
	}
	rejected := errors.New("consumer rejected hydrated chunk")
	stream.hydrateChunk = func(ctx context.Context, view graphstate.ReadView, chunk *certifiedIncidentChunk) error {
		entity, err := view.Entity(ctx, chunk.rows[0].Record.ID)
		if err != nil || !entity.Found || entity.View != view.Identity() {
			t.Fatal("hook lost same live read", entity, err)
		}
		_ = stream.Charged()
		return rejected
	}
	if chunk, err := stream.Next(t.Context(), stream.request, 1, nil); chunk != nil || !errors.Is(err, rejected) {
		t.Fatal(chunk, err)
	}
	stream.hydrateChunk = nil
	if stream.sequence != 0 || stream.started || stream.last != nil || stream.Charged().invocations != 2 {
		t.Fatal("failed hydration committed or refunded")
	}
	first, err := stream.Next(t.Context(), stream.request, 1, nil)
	if err != nil || first.complete || len(first.rows) != 1 || first.rows[0].Record.ID != 3 || !first.rows[0].Active || first.rows[0].Endpoints[1].ID != 17 || first.work.Records == 0 {
		t.Fatal(first, err)
	}
	charged := stream.Charged()
	replay, err := stream.Next(t.Context(), stream.request, 1, nil)
	if err != nil || replay != first || stream.Charged() != charged {
		t.Fatal("genuine replay changed work", replay, err)
	}
	// Mutate the remote endpoint after the old certified cut. Substituting a
	// current view during the next chunk would hide relationship5.
	mutationTx, mutationProposal := n.crossPlan([]graphstate.Operation{{Kind: graphstate.Close, Owner: 17, Life: 18, Scope: ops[0].Scope}}, nil, 25)
	mutationDecision := n.crossCommit(mutationTx, mutationProposal)
	n.crossResolve(mutationDecision)

	current := certifiedOpen(t, native, graphReadRequest{mode: graphReadFresh}, nil)
	if _, err := current.full.ImportIncidentContinuation(t.Context(), predicate, capsule); !errors.Is(err, graphstore.ErrInvalid) {
		t.Fatal("composite current root accepted old capsule", err)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	uncertain = true
	if chunk, err := stream.Next(t.Context(), stream.request, 2, nil); chunk != nil || !errors.Is(err, graphstore.ErrSourceUnavailable) {
		t.Fatal(chunk, err)
	}
	if stream.sequence != 1 || stream.last != first || stream.Charged().invocations != 4 {
		t.Fatal("uncertain invocation advanced/refunded")
	}
	last, err := stream.Next(t.Context(), stream.request, 2, nil)
	if err != nil || !last.complete || len(last.rows) != 1 || last.rows[0].Record.ID != 5 || !last.rows[0].Active || !last.rows[0].Endpoints[1].Active {
		t.Fatal("retained hydrated set", last, err)
	}
	if stream.Charged() != budget {
		t.Fatal("cumulative ledgers reset", stream.Charged(), budget)
	}
	if _, err := stream.Next(t.Context(), stream.request, 1, nil); !errors.Is(err, errInvalid) {
		t.Fatal("older replay accepted", err)
	}
	if _, err := stream.Next(t.Context(), stream.request, 3, nil); !errors.Is(err, errInvalid) {
		t.Fatal("new EOF fabricated", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Next(t.Context(), stream.request, 2, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("closed owner replayed", err)
	}

	// Physical owner loss starts a new genuine query at the original At cut;
	// this constructor does not accept the old owner request or last emitted ID.
	fresh, err := remote.OpenIncidentStream(t.Context(), cut, nil, predicate, at, graphstate.Effective, 1, time.Now().Add(time.Minute), grant.add(grant))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	beginning, err := fresh.Next(t.Context(), fresh.request, 1, nil)
	if err != nil || len(beginning.rows) != 1 || beginning.rows[0].Record.ID != 3 {
		t.Fatal("new owner skipped genuine prefix", beginning, err)
	}

	// Consumer callbacks can inspect charges, while recursive Next refuses.
	if _, err := fresh.Next(t.Context(), fresh.request, 1, func(*certifiedIncidentChunk) error {
		if fresh.Charged().invocations != 2 {
			t.Fatal("callback charge unavailable")
		}
		if _, err := fresh.Next(t.Context(), fresh.request, 2, nil); !errors.Is(err, errLimit) {
			t.Fatal("recursive Next accepted", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	finalCharge := fresh.Charged()
	if chunk, err := fresh.Next(t.Context(), fresh.request, 2, nil); chunk != nil || !errors.Is(err, errLimit) || fresh.sequence != 1 || fresh.Charged() != finalCharge {
		t.Fatal("exhausted invocation advanced/refunded", chunk, err)
	}

	if err := fresh.Close(); err != nil {
		t.Fatal(err)
	}
	closing, err := remote.OpenIncidentStream(t.Context(), cut, nil, predicate, at, graphstate.Effective, 1, time.Now().Add(time.Minute), grant.add(grant))
	if err != nil {
		t.Fatal(err)
	}
	if chunk, err := closing.Next(t.Context(), closing.request, 1, func(*certifiedIncidentChunk) error {
		_ = closing.Charged()
		return closing.Close()
	}); chunk != nil || !errors.Is(err, context.Canceled) || closing.sequence != 0 {
		t.Fatal("callback Close committed or deadlocked", chunk, err)
	}
	if _, err := incidentProduct(math.MaxInt, 2); !errors.Is(err, errLimit) {
		t.Fatal("overflow admitted", err)
	}
	exhausted := budget.subtract(grant)
	exhausted.decodedCells = 0
	if exhausted.admit(grant) {
		t.Fatal("independent decode allowance bypassed")
	}
}
