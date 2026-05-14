package core

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/badger"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func containsSubstring(s, sub string) bool {
	return strings.Contains(s, sub)
}

// ─── Issue 3: Graph validation — reject negative ValidationLimits ─────────────

func TestNew_NegativeValidationLimits(t *testing.T) {
	t.Parallel()

	fields := []struct {
		name string
		cfg  Config
	}{
		{"MaxLabelsPerNode", Config{Validation: ValidationLimits{MaxLabelsPerNode: -1}}},
		{"MaxPropertiesPerEntity", Config{Validation: ValidationLimits{MaxPropertiesPerEntity: -1}}},
		{"MaxPropertyKeyLength", Config{Validation: ValidationLimits{MaxPropertyKeyLength: -1}}},
		{"MaxPropertyValueSize", Config{Validation: ValidationLimits{MaxPropertyValueSize: -1}}},
		{"MaxNameLength", Config{Validation: ValidationLimits{MaxNameLength: -1}}},
	}

	for _, tc := range fields {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(tc.cfg)
			if err == nil {
				t.Fatalf("New(%s=-1) should return error", tc.name)
			}
			if got := err.Error(); got != "graph: validation limits must not be negative" {
				t.Errorf("unexpected error: %s", got)
			}
		})
	}

	// Zero and positive values should succeed.
	t.Run("zero_ok", func(t *testing.T) {
		g, err := New(Config{Store: memory.New()})
		if err != nil {
			t.Fatalf("New(zero defaults) error: %v", err)
		}
		g.Close()
	})
	t.Run("positive_ok", func(t *testing.T) {
		g, err := New(Config{Store: memory.New(), Validation: ValidationLimits{
			MaxLabelsPerNode:       10,
			MaxPropertiesPerEntity: 100,
			MaxPropertyKeyLength:   64,
			MaxPropertyValueSize:   1024,
			MaxNameLength:          128,
		}})
		if err != nil {
			t.Fatalf("New(positive) error: %v", err)
		}
		g.Close()
	})
}

func TestNew_TypedNilStoreRejected(t *testing.T) {
	t.Parallel()

	var typedNilStore *memory.Store
	g, err := New(Config{Store: typedNilStore})
	if !errors.Is(err, ErrNilStore) {
		t.Fatalf("New(typed nil store) = (%v, %v), want ErrNilStore", g, err)
	}
	if g != nil {
		t.Fatalf("New(typed nil store) returned graph %v", g)
	}
}

// ─── Issue 4: badger.Store config — reject invalid values ──────────────────────

func TestNewBadgerStore_InvalidConfig(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		cfg  badger.Config
		want string
	}{
		{"negative_flush", badger.Config{InMemory: true, FlushInterval: -time.Second}, "FlushInterval must not be negative"},
		{"negative_gc", badger.Config{InMemory: true, GCInterval: -time.Second}, "GCInterval must not be negative"},
		{"gc_ratio_zero_negative", badger.Config{InMemory: true, GCDiscardRatio: -0.5}, "GCDiscardRatio must be in (0, 1),"},
		{"gc_ratio_one", badger.Config{InMemory: true, GCDiscardRatio: 1.0}, "GCDiscardRatio must be in (0, 1),"},
		{"gc_ratio_above_one", badger.Config{InMemory: true, GCDiscardRatio: 1.5}, "GCDiscardRatio must be in (0, 1),"},
		{"empty_dir", badger.Config{InMemory: false, Dir: ""}, "Dir required when InMemory is false"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bs, err := badger.New(tc.cfg)
			if err == nil {
				bs.Close()
				t.Fatalf("badger.New(%s) should return error", tc.name)
			}
			if got := err.Error(); !containsSubstring(got, tc.want) {
				t.Errorf("error = %q, want substring %q", got, tc.want)
			}
		})
	}
}

// ─── Issue 5: Auth level — reject fractional float64 ──────────────────────────

func TestExtractProvenance_FractionalAuthLevel(t *testing.T) {
	t.Parallel()

	t.Run("5.9_rejected", func(t *testing.T) {
		t.Parallel()
		_, _, _, _, _, err := extractProvenance(map[string]any{
			"tkg_auth_level": float64(5.9),
		})
		if err == nil {
			t.Fatal("expected error for fractional auth level 5.9")
		}
		if got := err.Error(); got != "graph: tkg_auth_level 5.9 is not an integer" {
			t.Errorf("unexpected error: %s", got)
		}
	})

	t.Run("5.0_accepted", func(t *testing.T) {
		t.Parallel()
		_, _, _, level, _, err := extractProvenance(map[string]any{
			"tkg_auth_level": float64(5.0),
		})
		if err != nil {
			t.Fatalf("unexpected error for 5.0: %v", err)
		}
		if level != 5 {
			t.Errorf("auth level = %d, want 5", level)
		}
	})

	t.Run("0.1_rejected", func(t *testing.T) {
		t.Parallel()
		_, _, _, _, _, err := extractProvenance(map[string]any{
			"tkg_auth_level": float64(0.1),
		})
		if err == nil {
			t.Fatal("expected error for fractional auth level 0.1")
		}
	})
}

// ─── Issue 1: tiered.Store — CreateTemporalIndex ───────────────────────────────

func TestTieredStore_CreateTemporalIndex_Store(t *testing.T) {
	t.Parallel()
	g, _ := newTestTieredGraph(t)

	// Create nodes with "Case" (reference) label.
	n, err := g.Nodes.Add(context.Background(), []string{"Case"}, map[string]any{"name": "C1"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	_ = n

	// Create temporal index on "Case".
	if err := g.Index.CreateTemporal("Case"); err != nil {
		t.Fatalf("CreateTemporalIndex(Case): %v", err)
	}

	// tiered.Store swallows storepkg.ErrTemporalIndexExists (creates across shards idempotently).
	// Second call should succeed without error.
	if err := g.Index.CreateTemporal("Case"); err != nil {
		t.Errorf("duplicate CreateTemporalIndex should be idempotent, got: %v", err)
	}

	// Verify the index actually works by checking temporal query returns the node.
	nodes, err := g.Nodes.ByLabel("Case", storepkg.QueryOpts{ValidAt: types.Instant(time.Now().UnixMilli())})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if len(nodes) != 1 {
		t.Errorf("nodes = %d, want 1 (temporal index should be live)", len(nodes))
	}
}

// ─── Issue 1: tiered.Store — DropTemporalIndex ─────────────────────────────────

func TestTieredStore_DropTemporalIndex_Store(t *testing.T) {
	t.Parallel()
	g, _ := newTestTieredGraph(t)
	g.Nodes.Add(context.Background(), []string{"Case"}, nil)

	if err := g.Index.CreateTemporal("Case"); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	if err := g.Index.DeleteTemporal("Case"); err != nil {
		t.Fatalf("DropTemporalIndex: %v", err)
	}

	// Double-drop.
	err := g.Index.DeleteTemporal("Case")
	if !errors.Is(err, storepkg.ErrTemporalIndexNotFound) {
		t.Errorf("double DropTemporalIndex err = %v, want storepkg.ErrTemporalIndexNotFound", err)
	}
}

// ─── Issue 1: tiered.Store — CreateHighFrequencyIndex ──────────────────────────

func TestTieredStore_CreateHighFrequencyIndex_Store(t *testing.T) {
	t.Parallel()
	g, _ := newTestTieredGraph(t)
	g.Nodes.Add(context.Background(), []string{"Case"}, nil)

	if err := g.Index.CreateHighFrequency("Case", time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}

	// tiered.Store swallows storepkg.ErrTemporalIndexExists from shard-level duplicates.
	if err := g.Index.CreateHighFrequency("Case", time.Hour); err != nil {
		t.Errorf("duplicate CreateHighFrequencyIndex should be idempotent, got: %v", err)
	}
}

// ─── Issue 1: tiered.Store — DropHighFrequencyIndex ────────────────────────────

func TestTieredStore_DropHighFrequencyIndex_Store(t *testing.T) {
	t.Parallel()
	g, _ := newTestTieredGraph(t)
	g.Nodes.Add(context.Background(), []string{"Case"}, nil)

	if err := g.Index.CreateHighFrequency("Case", time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if err := g.Index.DeleteHighFrequency("Case"); err != nil {
		t.Fatalf("DropHighFrequencyIndex: %v", err)
	}

	// Double-drop.
	err := g.Index.DeleteHighFrequency("Case")
	if !errors.Is(err, storepkg.ErrTemporalIndexNotFound) {
		t.Errorf("double DropHighFrequencyIndex err = %v, want storepkg.ErrTemporalIndexNotFound", err)
	}
}

// ─── Issue 1 + 6: tiered.Store — RemoveNodeLabelTokenWithHistory ───────────────

func TestTieredStore_RemoveNodeLabelTokenWithHistory(t *testing.T) {
	t.Parallel()
	g, _ := newTestTieredGraph(t)

	n, err := g.Nodes.Add(context.Background(), []string{"Case", "User"}, map[string]any{"name": "test"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Remove "User" label via Graph API (uses RemoveNodeLabelTokenWithHistory internally).
	if err := g.Nodes.RemoveLabel(context.Background(), id, "User"); err != nil {
		t.Fatalf("RemoveNodeLabel: %v", err)
	}

	// Verify history was written atomically.
	hist, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(hist) != 1 {
		t.Fatalf("history len = %d, want 1", len(hist))
	}

	// Verify current node has only one label.
	got, err := g.Nodes.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	labels := g.Nodes.Labels(got)
	if len(labels) != 1 || labels[0] != "Case" {
		t.Errorf("labels = %v, want [Case]", labels)
	}
}

// ─── Issue 6: RemoveNodeLabel via Graph — crash-consistency (memory.Store) ─────

func TestRemoveNodeLabel_AtomicHistory(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, err := g.Nodes.Add(context.Background(), []string{"A", "B"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	if err := g.Nodes.RemoveLabel(context.Background(), id, "B"); err != nil {
		t.Fatalf("RemoveNodeLabel: %v", err)
	}

	hist, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(hist) != 1 {
		t.Fatalf("history entries = %d, want 1", len(hist))
	}

	// History entry should have version 0 (the original).
	if hist[0].Version() != 0 {
		t.Errorf("history version = %d, want 0", hist[0].Version())
	}

	// Current node should be version 1.
	got, err := g.Nodes.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.Version() != 1 {
		t.Errorf("current version = %d, want 1", got.Version())
	}
}

// ─── Issue 2: Batch concurrency — lock not leaked under normal path ───────────

func TestBatchExecute_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	// Run batch execute.
	b, _ := NewBatchBuilder(g)
	b.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Created != 1 {
		t.Errorf("Created = %d, want 1", result.Created)
	}

	// Verify lock is released: concurrent read should succeed immediately.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := g.Nodes.Count()
		if err != nil {
			t.Errorf("NodeCount after batch: %v", err)
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent read deadlocked after batch Execute")
	}
}
