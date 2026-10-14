package core

import (
	"context"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRowDoorsAgreeWithResolverOnAnUntimedUpdate (backlog 62, phase A): a
// later version written without a valid-from (ValidFrom 0, version > 0) starts
// where the resolver starts it (its update instant), so the doors that judge
// a materialized row — NodeMatchesValidTime / RelMatchesValidTime and
// NodeInterval / RelInterval (behind RelateNodes / RelateRels) — must give the
// same start as NodeAt / RelAt and the effective timeline. Break cases: an
// instant between the mint and the update (the row is not valid there), the
// update instant itself (it is), and the interval's start.
func TestRowDoorsAgreeWithResolverOnAnUntimedUpdate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	a, err := g.Nodes.Add(ctx, []string{"N"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(ctx, []string{"N"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r0, err := g.Rels.Add(ctx, "R", a, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	now, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	// The update lands 60 s after the creates, so an instant between them
	// separates the mint instant from the update instant.
	if _, err := g.Temporal.AdvanceClock(now + 60_000); err != nil {
		t.Fatal(err)
	}
	// An untimed later version with a valid end (for NodeInterval): the update,
	// then the end set in place (an update carrying tkg_valid_to alone is a
	// close that keeps the version's start, backlog 65).
	end := now + 120_000
	if _, err := g.Nodes.Update(ctx, a.ID(), map[string]any{"v": int64(1)}); err != nil {
		t.Fatal(err)
	}
	n1, err := g.Nodes.UpdateInPlace(ctx, a.ID(), map[string]any{"tkg_valid_to": end})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels.Update(ctx, r0.ID(), map[string]any{"v": int64(1)}); err != nil {
		t.Fatal(err)
	}
	r1, err := g.Rels.UpdateInPlace(ctx, r0.ID(), map[string]any{"tkg_valid_to": end})
	if err != nil {
		t.Fatal(err)
	}
	if n1.Version() == 0 || n1.Temporal().ValidFrom != 0 || r1.Version() == 0 || r1.Temporal().ValidFrom != 0 {
		t.Fatalf("setup: want untimed later versions, got node v%d vf %d, rel v%d vf %d",
			n1.Version(), n1.Temporal().ValidFrom, r1.Version(), r1.Temporal().ValidFrom)
	}

	nodeStart := n1.Temporal().TxFrom
	relStart := r1.Temporal().TxFrom
	between := now + 30_000

	for _, c := range []struct {
		at   types.Instant
		want bool
	}{{between, false}, {nodeStart - 1, false}, {nodeStart, true}} {
		at, err := g.Temporal.NodeAt(a.ID(), c.at)
		if err != nil {
			t.Fatalf("NodeAt(%d): %v", c.at, err)
		}
		if resolverSays := at.Version() == n1.Version(); resolverSays != c.want {
			t.Fatalf("NodeAt(%d) = v%d: setup wrong", c.at, at.Version())
		}
		if got := g.Temporal.NodeMatchesValidTime(n1, storepkg.QueryOpts{ValidAt: c.at}); got != c.want {
			t.Errorf("NodeMatchesValidTime(v%d, %d) = %v, NodeAt says %v", n1.Version(), c.at, got, c.want)
		}
	}
	for _, c := range []struct {
		at   types.Instant
		want bool
	}{{between, false}, {relStart - 1, false}, {relStart, true}} {
		at, err := g.Temporal.RelAt(r0.ID(), c.at)
		if err != nil {
			t.Fatalf("RelAt(%d): %v", c.at, err)
		}
		if resolverSays := at.Version() == r1.Version(); resolverSays != c.want {
			t.Fatalf("RelAt(%d) = v%d: setup wrong", c.at, at.Version())
		}
		if got := g.Temporal.RelMatchesValidTime(r1, storepkg.QueryOpts{ValidAt: c.at}); got != c.want {
			t.Errorf("RelMatchesValidTime(v%d, %d) = %v, RelAt says %v", r1.Version(), c.at, got, c.want)
		}
	}

	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	nsegs, err := g.Temporal.NodeEffectiveTimeline(a.ID(), pin)
	if err != nil || len(nsegs) != 2 {
		t.Fatalf("NodeEffectiveTimeline: %d segments, %v", len(nsegs), err)
	}
	if s, _, err := g.Temporal.NodeInterval(n1); err != nil || s != nsegs[1].ValidFrom {
		t.Errorf("NodeInterval(v%d) start %d (%v), timeline segment starts %d", n1.Version(), s, err, nsegs[1].ValidFrom)
	}
	rsegs, err := g.Temporal.RelEffectiveTimeline(r0.ID(), pin)
	if err != nil || len(rsegs) != 2 {
		t.Fatalf("RelEffectiveTimeline: %d segments, %v", len(rsegs), err)
	}
	if s, _, err := g.Temporal.RelInterval(r1); err != nil || s != rsegs[1].ValidFrom {
		t.Errorf("RelInterval(v%d) start %d (%v), timeline segment starts %d", r1.Version(), s, err, rsegs[1].ValidFrom)
	}
}
