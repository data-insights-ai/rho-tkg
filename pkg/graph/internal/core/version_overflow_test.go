package core

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestVersionOverflowRejectsVersionedMutationsBeforeWrap(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, g *Core)
	}{
		{
			name: "node update",
			run: func(t *testing.T, g *Core) {
				n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "old"})
				if err != nil {
					t.Fatal(err)
				}
				forceStoredNodeVersion(t, g, n.ID(), math.MaxUint32)

				if _, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{"name": "new"}); !errors.Is(err, ErrVersionOverflow) {
					t.Fatalf("Update max-version node = %v, want ErrVersionOverflow", err)
				}
				assertNodeVersionAndProperty(t, g, n.ID(), math.MaxUint32, "name", "old")
			},
		},
		{
			name: "node compare-and-set",
			run: func(t *testing.T, g *Core) {
				n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"state": "old"})
				if err != nil {
					t.Fatal(err)
				}
				forceStoredNodeVersion(t, g, n.ID(), math.MaxUint32)

				ok, err := g.Nodes.CompareAndSetProperty(context.Background(), n.ID(), "state", "old", "new")
				if !errors.Is(err, ErrVersionOverflow) {
					t.Fatalf("CompareAndSetProperty max-version node = (%v, %v), want ErrVersionOverflow", ok, err)
				}
				if ok {
					t.Fatal("CompareAndSetProperty reported success despite version overflow")
				}
				assertNodeVersionAndProperty(t, g, n.ID(), math.MaxUint32, "state", "old")
			},
		},
		{
			name: "node add label",
			run: func(t *testing.T, g *Core) {
				n, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				forceStoredNodeVersion(t, g, n.ID(), math.MaxUint32)

				if err := g.Nodes.AddLabel(context.Background(), n.ID(), "OverflowLabel"); !errors.Is(err, ErrVersionOverflow) {
					t.Fatalf("AddLabel max-version node = %v, want ErrVersionOverflow", err)
				}
				if _, ok := g.labels.Lookup("OverflowLabel"); ok {
					t.Fatal("AddLabel registered a new label before detecting version overflow")
				}
				got, err := g.Nodes.Get(context.Background(), n.ID())
				if err != nil {
					t.Fatal(err)
				}
				if got.Version() != math.MaxUint32 || g.Nodes.HasLabel(got, "OverflowLabel") {
					t.Fatalf("node changed after AddLabel overflow: version=%d labels=%v", got.Version(), g.Nodes.Labels(got))
				}
			},
		},
		{
			name: "node remove label",
			run: func(t *testing.T, g *Core) {
				n, err := g.Nodes.Add(context.Background(), []string{"Person", "Case"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				forceStoredNodeVersion(t, g, n.ID(), math.MaxUint32)

				if err := g.Nodes.RemoveLabel(context.Background(), n.ID(), "Case"); !errors.Is(err, ErrVersionOverflow) {
					t.Fatalf("RemoveLabel max-version node = %v, want ErrVersionOverflow", err)
				}
				got, err := g.Nodes.Get(context.Background(), n.ID())
				if err != nil {
					t.Fatal(err)
				}
				if got.Version() != math.MaxUint32 || !g.Nodes.HasLabel(got, "Case") {
					t.Fatalf("node changed after RemoveLabel overflow: version=%d labels=%v", got.Version(), g.Nodes.Labels(got))
				}
			},
		},
		{
			name: "relationship update",
			run: func(t *testing.T, g *Core) {
				start, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				end, err := g.Nodes.Add(context.Background(), []string{"Place"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				r, err := g.Rels.Add(context.Background(), "VISITED", start, end, map[string]any{"state": "old"})
				if err != nil {
					t.Fatal(err)
				}
				forceStoredRelVersion(t, g, r.ID(), math.MaxUint32)

				if _, err := g.Rels.Update(context.Background(), r.ID(), map[string]any{"state": "new"}); !errors.Is(err, ErrVersionOverflow) {
					t.Fatalf("Update max-version relationship = %v, want ErrVersionOverflow", err)
				}
				assertRelVersionAndProperty(t, g, r.ID(), math.MaxUint32, "state", "old")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newTestGraph(t)
			tc.run(t, g)
		})
	}
}

func TestVersionOverflowPropagatesThroughTxAndBatch(t *testing.T) {
	t.Run("transaction", func(t *testing.T) {
		g := newTestGraph(t)
		n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "old"})
		if err != nil {
			t.Fatal(err)
		}
		forceStoredNodeVersion(t, g, n.ID(), math.MaxUint32)

		tx, err := g.BeginTx()
		if err != nil {
			t.Fatal(err)
		}

		if _, err := tx.UpdateNode(n.ID(), map[string]any{"name": "new"}); !errors.Is(err, ErrVersionOverflow) {
			_ = tx.Rollback()
			t.Fatalf("tx.UpdateNode max-version node = %v, want ErrVersionOverflow", err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		assertNodeVersionAndProperty(t, g, n.ID(), math.MaxUint32, "name", "old")
	})

	t.Run("batch", func(t *testing.T) {
		g := newTestGraph(t)
		n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "old"})
		if err != nil {
			t.Fatal(err)
		}
		forceStoredNodeVersion(t, g, n.ID(), math.MaxUint32)

		b, err := NewBatchBuilder(g)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.UpdateNode(n.ID(), map[string]any{"name": "new"}); err != nil {
			t.Fatal(err)
		}
		result, err := b.Execute()
		if !errors.Is(err, ErrBatchFailed) {
			t.Fatalf("Batch Execute = (%+v, %v), want ErrBatchFailed", result, err)
		}
		if result == nil || len(result.Errors) != 1 || !errors.Is(result.Errors[0].Err, ErrVersionOverflow) {
			t.Fatalf("Batch errors = %+v, want one ErrVersionOverflow", result)
		}
		assertNodeVersionAndProperty(t, g, n.ID(), math.MaxUint32, "name", "old")
	})
}

func forceStoredNodeVersion(t *testing.T, g *Core, id types.NodeID, version uint32) {
	t.Helper()
	n, err := g.store.GetNode(id)
	if err != nil {
		t.Fatal(err)
	}
	n.SetVersion(version)
	if err := g.store.ReplaceNode(n); err != nil {
		t.Fatal(err)
	}
}

func forceStoredRelVersion(t *testing.T, g *Core, id types.RelID, version uint32) {
	t.Helper()
	r, err := g.store.GetRelationship(id)
	if err != nil {
		t.Fatal(err)
	}
	r.SetVersion(version)
	if err := g.store.ReplaceRelationship(r); err != nil {
		t.Fatal(err)
	}
}

func assertNodeVersionAndProperty(t *testing.T, g *Core, id types.NodeID, version uint32, key string, want any) {
	t.Helper()
	got, err := g.Nodes.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version() != version {
		t.Fatalf("node version = %d, want %d", got.Version(), version)
	}
	if gotVal, ok := got.GetProperty(key); !ok || gotVal != want {
		t.Fatalf("node property %q = %v (ok=%v), want %v", key, gotVal, ok, want)
	}
	history, err := g.Nodes.History(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Fatalf("history entries after rejected overflow = %d, want 0", len(history))
	}
}

func assertRelVersionAndProperty(t *testing.T, g *Core, id types.RelID, version uint32, key string, want any) {
	t.Helper()
	got, err := g.Rels.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version() != version {
		t.Fatalf("relationship version = %d, want %d", got.Version(), version)
	}
	if gotVal, ok := got.GetProperty(key); !ok || gotVal != want {
		t.Fatalf("relationship property %q = %v (ok=%v), want %v", key, gotVal, ok, want)
	}
	history, err := g.Rels.History(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Fatalf("relationship history entries after rejected overflow = %d, want 0", len(history))
	}
}
