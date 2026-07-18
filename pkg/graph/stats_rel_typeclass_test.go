package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRelPropertyTypeClassCounts_ExactPartition is the rule-2 mirror + adversarial
// gate for the rel type-class counters (BACKLOG 5B): an EXACT partition of a rel
// type's current rels by value class, maintained incrementally across add / replace /
// delete / node-cascade-delete on both backends. The mixed-class case (some numeric,
// some string under one type+key) is the whole point — it makes an ORDER BY unsound.
func TestRelPropertyTypeClassCounts_ExactPartition(t *testing.T) {
	eachBackend(t, func(t *testing.T, g *graphpkg.Graph) {
		ctx := context.Background()
		a, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
		b, _ := g.Nodes().Add(ctx, []string{"P"}, nil)

		// 5 numeric-weight + 3 string-weight + 2 no-weight KNOWS rels.
		numeric := make([]types.RelID, 0, 5)
		for i := 0; i < 5; i++ {
			r, err := g.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"weight": int64(i)})
			if err != nil {
				t.Fatalf("add numeric %d: %v", i, err)
			}
			numeric = append(numeric, r.ID())
		}
		for i := 0; i < 3; i++ {
			if _, err := g.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"weight": "hi"}); err != nil {
				t.Fatalf("add string %d: %v", i, err)
			}
		}
		for i := 0; i < 2; i++ {
			if _, err := g.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), nil); err != nil {
				t.Fatalf("add empty %d: %v", i, err)
			}
		}

		c, err := g.Stats().RelPropertyTypeClassCounts("KNOWS", "weight")
		if err != nil {
			t.Fatalf("counts: %v", err)
		}
		if c.Numeric != 5 || c.String != 3 || c.Missing != 2 {
			t.Fatalf("counts = %+v, want Numeric=5 String=3 Missing=2 (mixed → unsound ordering)", c)
		}

		// Delete a numeric rel → Numeric drops to 4.
		if err := g.Rels().Delete(ctx, numeric[0]); err != nil {
			t.Fatalf("delete rel: %v", err)
		}
		if c, _ := g.Stats().RelPropertyTypeClassCounts("KNOWS", "weight"); c.Numeric != 4 {
			t.Fatalf("after delete Numeric = %d, want 4", c.Numeric)
		}

		// Node-cascade delete (b's deletion removes ALL remaining KNOWS rels via the
		// read-free deleteRelByInfo path — the badger memoized-contribution seam).
		if err := g.Nodes().Delete(ctx, b.ID()); err != nil {
			t.Fatalf("cascade delete node b: %v", err)
		}
		c2, _ := g.Stats().RelPropertyTypeClassCounts("KNOWS", "weight")
		if c2.Numeric != 0 || c2.String != 0 || c2.Missing != 0 {
			t.Fatalf("after cascade = %+v, want all zero (every KNOWS rel gone — no drift)", c2)
		}
	})
}

// TestRelPropertyTypeClassCounts_Reopen proves the badger counters + memoized
// contributions rebuild from loadIndexes at open (so deletes after reopen still
// decrement precisely — the "survives restart" rule).
func TestRelPropertyTypeClassCounts_Reopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	g, err := graphpkg.New(graphpkg.Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	a, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
	b, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
	var first types.RelID
	for i := 0; i < 4; i++ {
		r, _ := g.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"weight": int64(i)})
		if i == 0 {
			first = r.ID()
		}
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	g2, err := graphpkg.New(graphpkg.Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer g2.Close()

	if c, _ := g2.Stats().RelPropertyTypeClassCounts("KNOWS", "weight"); c.Numeric != 4 {
		t.Fatalf("after reopen Numeric = %d, want 4 (counters not rebuilt)", c.Numeric)
	}
	// A delete after reopen must decrement — proves the contribution sidecar was
	// rebuilt too (else the read-free delete would find no contribution and drift).
	if err := g2.Rels().Delete(ctx, first); err != nil {
		t.Fatalf("delete after reopen: %v", err)
	}
	if c, _ := g2.Stats().RelPropertyTypeClassCounts("KNOWS", "weight"); c.Numeric != 3 {
		t.Fatalf("after post-reopen delete Numeric = %d, want 3 (contribution not rebuilt → drift)", c.Numeric)
	}
}

// TestRelPropertyTypeClassCounts_TieredDeclines proves tiered declines the capability
// (rel property indexes — the whole rel-ordering path — are RAM-only per-shard).
func TestRelPropertyTypeClassCounts_TieredDeclines(t *testing.T) {
	ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Machine"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: ts})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if _, err := g.Stats().RelPropertyTypeClassCounts("KNOWS", "weight"); !errors.Is(err, store.ErrCapabilityNotSupported) {
		t.Fatalf("tiered RelPropertyTypeClassCounts err = %v, want ErrCapabilityNotSupported", err)
	}
}
