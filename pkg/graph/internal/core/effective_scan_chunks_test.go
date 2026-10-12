package core

import (
	"context"
	"testing"
	"time"

	badgerpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	shardedpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 55, review round: a scan reads effScanChunk entities per hold of
// the read lock and hands them to fn after it; no read snapshot stays open
// while fn runs (a read transaction's mark would keep every version it can
// see from compaction and value-log GC for as long as fn takes), and an
// Admin().Reset from fn between chunks leaves nothing for the next chunk.

const escRels = effScanChunk + 44 // two chunks

// escGraph opens a graph on a badger-backed store holding escRels "L"
// relationships, each updated once (a history row), flushed.
func escGraph(t *testing.T, sharded bool, dir string) (*Core, func() *Core) {
	t.Helper()
	open := func() *Core {
		var cfg Config
		if sharded {
			st, err := shardedpkg.New(shardedpkg.Config{InMemory: true, BaseSlot: 0, SlotCount: 2, CacheCapacity: 1})
			if err != nil {
				t.Fatalf("sharded.New: %v", err)
			}
			cfg.Store = st
		} else {
			st, err := badgerpkg.New(badgerpkg.Config{Dir: dir, CacheCapacity: 1, FlushInterval: time.Hour})
			if err != nil {
				t.Fatalf("badger.New: %v", err)
			}
			cfg.Store = st
		}
		cfg.AllowReset = true
		g, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = g.Close() })
		return g
	}
	g := open()
	ctx := context.Background()
	s, err := g.Nodes.Add(ctx, []string{"Ref"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	e, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < escRels; i++ {
		r, err := g.Rels.AddByID(ctx, ccType, s.ID(), e.ID(), map[string]any{"x": int64(i)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"x": int64(-i)}); err != nil {
			t.Fatal(err)
		}
	}
	estFlush(t, g)
	return g, open
}

// TestEffectiveScanHoldsNoReadTxnDuringFn: on badger and sharded, while fn
// runs between and within chunks no read snapshot transaction is open.
func TestEffectiveScanHoldsNoReadTxnDuringFn(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		g, _ := escGraph(t, sharded, t.TempDir())
		pin, err := g.Temporal.NowTx()
		if err != nil {
			t.Fatal(err)
		}
		seen, open := map[types.RelID]bool{}, int64(0)
		if err := g.Temporal.ForEachRelEffectiveByType(ccType, pin, func(s temporal.RelSegment) bool {
			seen[s.Rel.ID()] = true
			open = max(open, badgerpkg.OpenReadTxnsForTest())
			return true
		}); err != nil {
			t.Fatal(err)
		}
		if len(seen) != escRels || open != 0 {
			t.Fatalf("sharded=%v: %d relationships streamed (want %d), up to %d read transactions open while fn ran (want 0)", sharded, len(seen), escRels, open)
		}
	}
}

// TestEffectiveScanResetFromFn: a store reopened without a flush since (its
// caches' flush epochs back at zero), a scan whose fn calls Admin().Reset
// at the first segment: the first chunk was read before the reset and
// streams; the second chunk reads after it and streams nothing (the per-ID
// path reads every later entity as unknown).
func TestEffectiveScanResetFromFn(t *testing.T) {
	dir := t.TempDir()
	g, open := escGraph(t, false, dir)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g = open()
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	streamed := map[types.RelID]bool{}
	if err := g.Temporal.ForEachRelEffectiveByType(ccType, pin, func(s temporal.RelSegment) bool {
		if len(streamed) == 0 {
			if err := g.Admin.Reset(); err != nil {
				t.Fatalf("Reset: %v", err)
			}
		}
		streamed[s.Rel.ID()] = true
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(streamed) != effScanChunk {
		t.Fatalf("%d relationships streamed around a Reset from fn, want the %d of the first chunk", len(streamed), effScanChunk)
	}
}
