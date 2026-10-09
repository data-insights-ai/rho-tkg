package core

// A relationship delete must not take the relationship out of its rel-type
// temporal envelope: the deleted row's history stays on the store, and a
// rolled-back GraphTx delete restores the current row through the history-trim
// path, which never re-adds the history versions to the envelope. If the
// delete purges the envelope, the restored relationship's past version is
// pruned away from valid-time queries on a store that carries the index
// (badger), while memory — whose delete leaves the envelope alone — and a
// store without the index still answer.

import (
	"context"
	"slices"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestRelTemporalIndex_RolledBackDeleteKeepsPastVersion(t *testing.T) {
	for _, backend := range []struct {
		name  string
		index bool
		open  func(t *testing.T) storepkg.Store
	}{
		{"memory", true, func(*testing.T) storepkg.Store { return memory.New() }},
		{"badger without index", false, openInMemoryBadgerStore},
		{"badger with index", true, openInMemoryBadgerStore},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			c, err := New(Config{Store: backend.open(t)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Close() })
			a, err := c.Nodes.Add(ctx, []string{"Host"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			b, err := c.Nodes.Add(ctx, []string{"Host"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if backend.index {
				if err := c.Index.CreateRelTemporal("HOP"); err != nil {
					t.Fatalf("CreateRelTemporal: %v", err)
				}
			}
			moved, err := c.Rels.Add(ctx, "HOP", a, b, map[string]any{types.ShadowValidFrom: types.Instant(1000)})
			if err != nil {
				t.Fatal(err)
			}
			// Two-phase: [1000, ...) then moved to [5000, ...).
			if _, err := c.Temporal.SetRelVersionInterval(ctx, moved.ID(), 5000, 0, nil); err != nil {
				t.Fatal(err)
			}
			tx, err := c.BeginTx()
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.DeleteRelationship(moved.ID()); err != nil {
				t.Fatalf("tx delete: %v", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatalf("rollback: %v", err)
			}
			at, err := c.Temporal.RelsByTypeAt("HOP", 1500)
			if err != nil {
				t.Fatal(err)
			}
			if len(at) != 1 || at[0].ID() != moved.ID() {
				t.Errorf("RelsByTypeAt(1500) after rolled-back delete = %d rows, want the moved row's past version", len(at))
			}
			byType, err := c.Rels.ByType("HOP", storepkg.QueryOpts{ValidAt: 1500})
			if err != nil {
				t.Fatal(err)
			}
			if len(byType) != 1 || byType[0].ID() != moved.ID() {
				t.Errorf("ByType{ValidAt:1500} after rolled-back delete = %d rows, want 1", len(byType))
			}
			now, err := c.Temporal.RelsByTypeAt("HOP", 6000)
			if err != nil {
				t.Fatal(err)
			}
			if ids := relIDsOf(now); !slices.Equal(ids, []types.RelID{moved.ID()}) {
				t.Errorf("RelsByTypeAt(6000) = %v, want [%d]", ids, moved.ID())
			}
		})
	}
}

func openInMemoryBadgerStore(t *testing.T) storepkg.Store {
	t.Helper()
	bs, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	return bs
}
