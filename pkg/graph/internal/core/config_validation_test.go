package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func containsSubstring(s, sub string) bool {
	return strings.Contains(s, sub)
}

// ─── Graph validation — reject negative ValidationLimits ──────────────────────

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

// ─── badger.Store config — reject invalid values ───────────────────────────────

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

// ─── Auth level — reject fractional float64 ────────────────────────────────────

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

// ─── tiered.Store — temporal and high-frequency index DDL ────────────────────

// TestTieredStore_TemporalIndexDDL: on tiered, CreateTemporal makes HasTemporal
// true and a duplicate create is idempotent (tiered swallows the shard-level
// ErrTemporalIndexExists); DeleteTemporal makes it false and a second delete is
// ErrTemporalIndexNotFound. A high-frequency index does not count for
// HasTemporal, still occupies the label's one temporal-index slot, and has the
// same idempotent create and NotFound double delete.
func TestTieredStore_TemporalIndexDDL(t *testing.T) {
	t.Parallel()
	hasTemporal := func(t *testing.T, g *Core, want bool) {
		t.Helper()
		got, err := g.Index.HasTemporal("Case")
		if err != nil || got != want {
			t.Fatalf("HasTemporal(Case) = %v, %v; want %v", got, err, want)
		}
	}
	t.Run("temporal", func(t *testing.T) {
		t.Parallel()
		g, _ := newTestTieredGraph(t)
		if _, err := g.Nodes.Add(context.Background(), []string{"Case"}, map[string]any{"name": "C1"}); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		hasTemporal(t, g, false)
		if err := g.Index.CreateTemporal("Case"); err != nil {
			t.Fatalf("CreateTemporal: %v", err)
		}
		hasTemporal(t, g, true)
		if err := g.Index.CreateTemporal("Case"); err != nil {
			t.Errorf("duplicate CreateTemporal should be idempotent, got: %v", err)
		}
		if err := g.Index.DeleteTemporal("Case"); err != nil {
			t.Fatalf("DeleteTemporal: %v", err)
		}
		hasTemporal(t, g, false)
		if err := g.Index.DeleteTemporal("Case"); !errors.Is(err, storepkg.ErrTemporalIndexNotFound) {
			t.Errorf("double DeleteTemporal err = %v, want storepkg.ErrTemporalIndexNotFound", err)
		}
	})
	t.Run("high-frequency", func(t *testing.T) {
		t.Parallel()
		g, _ := newTestTieredGraph(t)
		if _, err := g.Nodes.Add(context.Background(), []string{"Case"}, nil); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if err := g.Index.CreateHighFrequency("Case", time.Hour); err != nil {
			t.Fatalf("CreateHighFrequency: %v", err)
		}
		hasTemporal(t, g, false)
		if err := g.Index.CreateTemporal("Case"); !errors.Is(err, storepkg.ErrTemporalIndexExists) {
			t.Errorf("CreateTemporal beside a high-frequency index = %v; want ErrTemporalIndexExists", err)
		}
		if err := g.Index.CreateHighFrequency("Case", time.Hour); err != nil {
			t.Errorf("duplicate CreateHighFrequency should be idempotent, got: %v", err)
		}
		if err := g.Index.DeleteHighFrequency("Case"); err != nil {
			t.Fatalf("DeleteHighFrequency: %v", err)
		}
		if err := g.Index.DeleteHighFrequency("Case"); !errors.Is(err, storepkg.ErrTemporalIndexNotFound) {
			t.Errorf("double DeleteHighFrequency err = %v, want storepkg.ErrTemporalIndexNotFound", err)
		}
	})
}

// ─── RemoveNodeLabel — the prior version goes to history ──────────────────────

// TestRemoveNodeLabel_HistoryKeepsPriorVersion: Nodes.RemoveLabel writes the
// prior version (v0, both labels) as the one history row and the new current
// row (v1, the remaining label) through RemoveNodeLabelTokenWithHistory, on
// memory and tiered. The history and current write are one store call; their
// atomicity is the store's contract, not asserted here.
func TestRemoveNodeLabel_HistoryKeepsPriorVersion(t *testing.T) {
	t.Parallel()
	backends := []struct {
		name string
		open func(t *testing.T) *Core
	}{
		{"memory", newTestGraph},
		{"tiered", func(t *testing.T) *Core { g, _ := newTestTieredGraph(t); return g }},
	}
	for _, be := range backends {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g := be.open(t)
			ctx := context.Background()
			n, err := g.Nodes.Add(ctx, []string{"Case", "User"}, map[string]any{"name": "test"})
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}
			id := n.ID()
			if err := g.Nodes.RemoveLabel(ctx, id, "User"); err != nil {
				t.Fatalf("RemoveNodeLabel: %v", err)
			}
			hist, err := g.Nodes.History(id)
			if err != nil {
				t.Fatalf("GetNodeHistory: %v", err)
			}
			if len(hist) != 1 {
				t.Fatalf("history len = %d, want 1", len(hist))
			}
			if v, labels := hist[0].Version(), strings.Join(g.Nodes.Labels(hist[0]), ","); v != 0 || labels != "Case,User" {
				t.Errorf("history row = v%d [%s]; want v0 [Case,User]", v, labels)
			}
			got, err := g.Nodes.Get(ctx, id)
			if err != nil {
				t.Fatalf("GetNode: %v", err)
			}
			if v, labels := got.Version(), strings.Join(g.Nodes.Labels(got), ","); v != 1 || labels != "Case" {
				t.Errorf("current row = v%d [%s]; want v1 [Case]", v, labels)
			}
		})
	}
}

// ─── Batch concurrency — lock not leaked ──────────────────────────────────────

// TestBatchExecute_ConcurrentAccess: after a successful and after a failing
// Execute (ErrBatchFailed), a write and a Count from another goroutine finish
// within 2 s with the expected count. A leaked graph lock blocks the write.
func TestBatchExecute_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	ctx := context.Background()

	probe := func(phase string, want int) {
		t.Helper()
		done := make(chan error, 1)
		go func() {
			if _, err := g.Nodes.Add(ctx, []string{"Probe"}, nil); err != nil {
				done <- fmt.Errorf("Add: %w", err)
				return
			}
			n, err := g.Nodes.Count()
			if err == nil && n != want {
				err = fmt.Errorf("Count = %d, want %d", n, want)
			}
			done <- err
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s: %v", phase, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: write after batch Execute blocked (lock leaked)", phase)
		}
	}

	b, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	if _, err := b.AddNode([]string{"Person"}, map[string]any{"name": "Alice"}); err != nil {
		t.Fatalf("batch AddNode: %v", err)
	}
	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Created != 1 {
		t.Errorf("Created = %d, want 1", result.Created)
	}
	probe("after a successful Execute", 2)

	b, err = NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	if err := b.UpdateNode(types.NodeID(999999), map[string]any{"name": "Ghost"}); err != nil {
		t.Fatalf("queue UpdateNode: %v", err)
	}
	if _, err := b.Execute(); !errors.Is(err, ErrBatchFailed) {
		t.Fatalf("Execute on a missing node = %v; want ErrBatchFailed", err)
	}
	probe("after a failing Execute", 3)
}
