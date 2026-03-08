package graph

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
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
		g, err := New(Config{Store: NewMemoryStore()})
		if err != nil {
			t.Fatalf("New(zero defaults) error: %v", err)
		}
		g.Close()
	})
	t.Run("positive_ok", func(t *testing.T) {
		g, err := New(Config{Store: NewMemoryStore(), Validation: ValidationLimits{
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

// ─── Issue 4: BadgerStore config — reject invalid values ──────────────────────

func TestNewBadgerStore_InvalidConfig(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		cfg  BadgerStoreConfig
		want string
	}{
		{"negative_flush", BadgerStoreConfig{InMemory: true, FlushInterval: -time.Second}, "FlushInterval must not be negative"},
		{"negative_gc", BadgerStoreConfig{InMemory: true, GCInterval: -time.Second}, "GCInterval must not be negative"},
		{"gc_ratio_zero_negative", BadgerStoreConfig{InMemory: true, GCDiscardRatio: -0.5}, "GCDiscardRatio must be in (0, 1),"},
		{"gc_ratio_one", BadgerStoreConfig{InMemory: true, GCDiscardRatio: 1.0}, "GCDiscardRatio must be in (0, 1),"},
		{"gc_ratio_above_one", BadgerStoreConfig{InMemory: true, GCDiscardRatio: 1.5}, "GCDiscardRatio must be in (0, 1),"},
		{"empty_dir", BadgerStoreConfig{InMemory: false, Dir: ""}, "Dir required when InMemory is false"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bs, err := NewBadgerStore(tc.cfg)
			if err == nil {
				bs.Close()
				t.Fatalf("NewBadgerStore(%s) should return error", tc.name)
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

// ─── Issue 2: Batch panic lock-leak ───────────────────────────────────────────

// panicStore is a Store that panics on PutNodesBatch to test batch panic recovery.
type panicStore struct {
	*MemoryStore
}

func (ps *panicStore) PutNodesBatch(_ []*types.Node) error {
	panic("test panic in PutNodesBatch")
}

func TestBatchExecute_PanicRecovery(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	ps := &panicStore{MemoryStore: ms}
	g, err := New(Config{Store: ps})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })

	b := NewBatchBuilder(g)
	_, err = b.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// Execute should panic; recover and verify lock released.
	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic from PutNodesBatch")
			}
		}()
		b.Execute()
	}()

	// Lock must be released: this should not deadlock.
	done := make(chan struct{})
	go func() {
		g.mu.Lock()
		g.mu.Unlock()
		close(done)
	}()

	select {
	case <-done:
		// success — lock was released
	case <-time.After(2 * time.Second):
		t.Fatal("g.mu.Lock() deadlocked — batch panic leaked the lock")
	}

	// txEventBuffer must be nil after panic cleanup.
	g.mu.Lock()
	if g.txEventBuffer != nil {
		t.Error("txEventBuffer not nil after panic recovery")
	}
	g.mu.Unlock()
}

// ─── Issue 1: BadgerStore — DropTemporalIndex ─────────────────────────────────

func TestBadgerStore_DropTemporalIndex(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Create a label token's index.
	putTestNode(t, bs, 100, 1, nil)
	if err := bs.CreateTemporalIndex(1); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	// Drop it.
	if err := bs.DropTemporalIndex(1); err != nil {
		t.Fatalf("DropTemporalIndex: %v", err)
	}

	// Double-drop returns ErrTemporalIndexNotFound.
	err := bs.DropTemporalIndex(1)
	if !errors.Is(err, ErrTemporalIndexNotFound) {
		t.Errorf("double DropTemporalIndex err = %v, want ErrTemporalIndexNotFound", err)
	}
}

// ─── Issue 1: BadgerStore — CreateHighFrequencyIndex ──────────────────────────

func TestBadgerStore_CreateHighFrequencyIndex(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Succeed first time.
	if err := bs.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}

	// Duplicate returns ErrTemporalIndexExists.
	err := bs.CreateHighFrequencyIndex(1, time.Hour)
	if !errors.Is(err, ErrTemporalIndexExists) {
		t.Errorf("duplicate CreateHighFrequencyIndex err = %v, want ErrTemporalIndexExists", err)
	}

	// Conflict with existing temporal index on different label.
	putTestNode(t, bs, 200, 2, nil)
	if err := bs.CreateTemporalIndex(2); err != nil {
		t.Fatalf("CreateTemporalIndex(2): %v", err)
	}
	err = bs.CreateHighFrequencyIndex(2, time.Hour)
	if !errors.Is(err, ErrTemporalIndexExists) {
		t.Errorf("HF conflict with temporal err = %v, want ErrTemporalIndexExists", err)
	}
}

// ─── Issue 1: BadgerStore — DropHighFrequencyIndex ────────────────────────────

func TestBadgerStore_DropHighFrequencyIndex(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if err := bs.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}

	// Drop.
	if err := bs.DropHighFrequencyIndex(1); err != nil {
		t.Fatalf("DropHighFrequencyIndex: %v", err)
	}

	// Double-drop.
	err := bs.DropHighFrequencyIndex(1)
	if !errors.Is(err, ErrTemporalIndexNotFound) {
		t.Errorf("double DropHighFrequencyIndex err = %v, want ErrTemporalIndexNotFound", err)
	}

	// Re-create after drop.
	if err := bs.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("re-create after drop: %v", err)
	}
}

// ─── Issue 1 + 6: BadgerStore — RemoveNodeLabelTokenWithHistory ───────────────

func TestBadgerStore_RemoveNodeLabelTokenWithHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	primary := uint16(1)
	extra := uint16(2)
	n := putTestNode(t, bs, 100, primary, []uint16{extra})

	// Build updated node with label removed.
	prevVersion := n.Version()
	prevState := n.DeepCopy()
	updated := n.DeepCopy()
	updated.RemoveLabelTokenRaw(extra)
	updated.SetVersion(prevVersion + 1)

	if err := bs.RemoveNodeLabelTokenWithHistory(
		snowflake.ID(100), extra, updated, prevVersion, prevState,
	); err != nil {
		t.Fatalf("RemoveNodeLabelTokenWithHistory: %v", err)
	}

	// Verify history entry exists.
	hist, err := bs.GetNodeVersion(snowflake.ID(100), prevVersion)
	if err != nil {
		t.Fatalf("GetNodeVersion(%d): %v", prevVersion, err)
	}
	if hist.Version() != prevVersion {
		t.Errorf("history version = %d, want %d", hist.Version(), prevVersion)
	}

	// Verify current node has updated version.
	got, err := bs.GetNode(snowflake.ID(100))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.Version() != prevVersion+1 {
		t.Errorf("current version = %d, want %d", got.Version(), prevVersion+1)
	}

	// Verify label index updated — node should not appear for the removed label.
	byLabel, err := bs.NodesByLabel(extra, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel(%d): %v", extra, err)
	}
	for _, node := range byLabel {
		if node.InternalID().SnowflakeID() == snowflake.ID(100) {
			t.Error("node still in label index after RemoveNodeLabelTokenWithHistory")
		}
	}
}

// ─── Issue 1: TieredStore — CreateTemporalIndex ───────────────────────────────

func TestTieredStore_CreateTemporalIndex_Store(t *testing.T) {
	t.Parallel()
	g, _ := newTestTieredGraph(t)

	// Create nodes with "Case" (reference) label.
	n, err := g.AddNode([]string{"Case"}, map[string]any{"name": "C1"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	_ = n

	// Create temporal index on "Case".
	if err := g.CreateTemporalIndex("Case"); err != nil {
		t.Fatalf("CreateTemporalIndex(Case): %v", err)
	}

	// TieredStore swallows ErrTemporalIndexExists (creates across shards idempotently).
	// Second call should succeed without error.
	if err := g.CreateTemporalIndex("Case"); err != nil {
		t.Errorf("duplicate CreateTemporalIndex should be idempotent, got: %v", err)
	}

	// Verify the index actually works by checking temporal query returns the node.
	nodes, err := g.NodesByLabel("Case", QueryOpts{ValidAt: types.Instant(time.Now().UnixMilli())})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if len(nodes) != 1 {
		t.Errorf("nodes = %d, want 1 (temporal index should be live)", len(nodes))
	}
}

// ─── Issue 1: TieredStore — DropTemporalIndex ─────────────────────────────────

func TestTieredStore_DropTemporalIndex_Store(t *testing.T) {
	t.Parallel()
	g, _ := newTestTieredGraph(t)
	g.AddNode([]string{"Case"}, nil)

	if err := g.CreateTemporalIndex("Case"); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	if err := g.DropTemporalIndex("Case"); err != nil {
		t.Fatalf("DropTemporalIndex: %v", err)
	}

	// Double-drop.
	err := g.DropTemporalIndex("Case")
	if !errors.Is(err, ErrTemporalIndexNotFound) {
		t.Errorf("double DropTemporalIndex err = %v, want ErrTemporalIndexNotFound", err)
	}
}

// ─── Issue 1: TieredStore — CreateHighFrequencyIndex ──────────────────────────

func TestTieredStore_CreateHighFrequencyIndex_Store(t *testing.T) {
	t.Parallel()
	g, _ := newTestTieredGraph(t)
	g.AddNode([]string{"Case"}, nil)

	if err := g.CreateHighFrequencyIndex("Case", time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}

	// TieredStore swallows ErrTemporalIndexExists from shard-level duplicates.
	if err := g.CreateHighFrequencyIndex("Case", time.Hour); err != nil {
		t.Errorf("duplicate CreateHighFrequencyIndex should be idempotent, got: %v", err)
	}
}

// ─── Issue 1: TieredStore — DropHighFrequencyIndex ────────────────────────────

func TestTieredStore_DropHighFrequencyIndex_Store(t *testing.T) {
	t.Parallel()
	g, _ := newTestTieredGraph(t)
	g.AddNode([]string{"Case"}, nil)

	if err := g.CreateHighFrequencyIndex("Case", time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if err := g.DropHighFrequencyIndex("Case"); err != nil {
		t.Fatalf("DropHighFrequencyIndex: %v", err)
	}

	// Double-drop.
	err := g.DropHighFrequencyIndex("Case")
	if !errors.Is(err, ErrTemporalIndexNotFound) {
		t.Errorf("double DropHighFrequencyIndex err = %v, want ErrTemporalIndexNotFound", err)
	}
}

// ─── Issue 1: TieredStore — SaveLabelRegistry_Deprecated ──────────────────────

func TestTieredStore_SaveLabelRegistry_Deprecated(t *testing.T) {
	t.Parallel()
	// In-memory TieredStore: SaveLabelRegistry is a no-op (returns nil).
	ts := newTestTieredStore(t)
	reg := newLabelRegistry()
	reg.GetOrCreate("Test")

	if err := ts.SaveLabelRegistry(reg); err != nil {
		t.Fatalf("SaveLabelRegistry (in-memory): %v", err)
	}
}

// ─── Issue 1: TieredStore — SaveRelTypeRegistry_Deprecated ────────────────────

func TestTieredStore_SaveRelTypeRegistry_Deprecated(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	reg := newRelTypeRegistry()
	reg.GetOrCreate("KNOWS")

	if err := ts.SaveRelTypeRegistry(reg); err != nil {
		t.Fatalf("SaveRelTypeRegistry (in-memory): %v", err)
	}
}

// ─── Issue 1 + 6: TieredStore — RemoveNodeLabelTokenWithHistory ───────────────

func TestTieredStore_RemoveNodeLabelTokenWithHistory(t *testing.T) {
	t.Parallel()
	g, _ := newTestTieredGraph(t)

	n, err := g.AddNode([]string{"Case", "User"}, map[string]any{"name": "test"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.InternalID().SnowflakeID()

	// Remove "User" label via Graph API (uses RemoveNodeLabelTokenWithHistory internally).
	if err := g.RemoveNodeLabel(id, "User"); err != nil {
		t.Fatalf("RemoveNodeLabel: %v", err)
	}

	// Verify history was written atomically.
	hist, err := g.GetNodeHistory(id)
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(hist) != 1 {
		t.Fatalf("history len = %d, want 1", len(hist))
	}

	// Verify current node has only one label.
	got, err := g.GetNode(id)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	labels := g.NodeLabels(got)
	if len(labels) != 1 || labels[0] != "Case" {
		t.Errorf("labels = %v, want [Case]", labels)
	}
}

// ─── Issue 6: RemoveNodeLabel via Graph — crash-consistency (MemoryStore) ─────

func TestRemoveNodeLabel_AtomicHistory(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, err := g.AddNode([]string{"A", "B"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.InternalID().SnowflakeID()

	if err := g.RemoveNodeLabel(id, "B"); err != nil {
		t.Fatalf("RemoveNodeLabel: %v", err)
	}

	hist, err := g.GetNodeHistory(id)
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
	got, err := g.GetNode(id)
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
	b := NewBatchBuilder(g)
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
		_, err := g.NodeCount()
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

// ─── Issue 1: BadgerStore — CreateTemporalIndex (direct store test) ───────────

func TestBadgerStore_CreateTemporalIndex(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 100, 1, nil)

	if err := bs.CreateTemporalIndex(1); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	// Duplicate.
	err := bs.CreateTemporalIndex(1)
	if !errors.Is(err, ErrTemporalIndexExists) {
		t.Errorf("duplicate CreateTemporalIndex err = %v, want ErrTemporalIndexExists", err)
	}
}
