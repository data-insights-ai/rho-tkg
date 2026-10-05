package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Every door that changes a relationship — its row, its properties, its
// validity, its existence — must move Rels().RelMutationEpoch on every
// backend. A consumer keys caches of relationship-derived state (statistics,
// plans, snapshots) on it; a write that leaves it unchanged makes such a cache
// serve the state before the write. Found by sigma-tkgd (C3q findings 1 and
// 2): a property write or remove moved it on neither memory nor badger, and a
// relationship delete did not move it on badger.

type relEpochDoor struct {
	name string
	run  func(ctx context.Context, g *graphpkg.Graph, r *types.Relationship) error
}

func relEpochDoors() []relEpochDoor {
	return []relEpochDoor{
		{"Rels.Add", func(ctx context.Context, g *graphpkg.Graph, r *types.Relationship) error {
			start, err := g.Nodes().Get(ctx, r.StartNodeID())
			if err != nil {
				return err
			}
			end, err := g.Nodes().Get(ctx, r.EndNodeID())
			if err != nil {
				return err
			}
			_, err = g.Rels().Add(ctx, "T", start, end, nil)
			return err
		}},
		{"Rels.SetProperty", func(ctx context.Context, g *graphpkg.Graph, r *types.Relationship) error {
			return g.Rels().SetProperty(ctx, r.ID(), "w", int64(9))
		}},
		{"Rels.DeleteProperty", func(ctx context.Context, g *graphpkg.Graph, r *types.Relationship) error {
			return g.Rels().DeleteProperty(ctx, r.ID(), "w")
		}},
		{"Rels.CompareAndSetProperty", func(ctx context.Context, g *graphpkg.Graph, r *types.Relationship) error {
			_, err := g.Rels().CompareAndSetProperty(ctx, r.ID(), "w", int64(1), int64(2))
			return err
		}},
		{"Rels.Update", func(ctx context.Context, g *graphpkg.Graph, r *types.Relationship) error {
			_, err := g.Rels().Update(ctx, r.ID(), map[string]any{"w": int64(3)})
			return err
		}},
		{"Rels.UpdateInPlace", func(ctx context.Context, g *graphpkg.Graph, r *types.Relationship) error {
			_, err := g.Rels().UpdateInPlace(ctx, r.ID(), map[string]any{"w": int64(4)})
			return err
		}},
		{"Rels.CloseVersion", func(ctx context.Context, g *graphpkg.Graph, r *types.Relationship) error {
			now, err := g.Temporal().NowTx()
			if err != nil {
				return err
			}
			return g.Rels().CloseVersion(ctx, r.ID(), now+1000)
		}},
		{"Temporal.SetRelVersionInterval", func(ctx context.Context, g *graphpkg.Graph, r *types.Relationship) error {
			now, err := g.Temporal().NowTx()
			if err != nil {
				return err
			}
			_, err = g.Temporal().SetRelVersionInterval(ctx, r.ID(), now, 0, map[string]any{"w": int64(5)})
			return err
		}},
		{"Rels.Delete", func(ctx context.Context, g *graphpkg.Graph, r *types.Relationship) error {
			return g.Rels().Delete(ctx, r.ID())
		}},
		{"Nodes.Delete (cascade)", func(ctx context.Context, g *graphpkg.Graph, r *types.Relationship) error {
			return g.Nodes().Delete(ctx, r.StartNodeID())
		}},
		{"Tx.SetRelationshipProperty", func(_ context.Context, g *graphpkg.Graph, r *types.Relationship) error {
			return g.Tx().Run(func(tx *graphpkg.GraphTx) error { return tx.SetRelationshipProperty(r.ID(), "w", int64(6)) })
		}},
		{"Tx.DeleteRelationshipProperty", func(_ context.Context, g *graphpkg.Graph, r *types.Relationship) error {
			return g.Tx().Run(func(tx *graphpkg.GraphTx) error { return tx.DeleteRelationshipProperty(r.ID(), "w") })
		}},
		{"Tx.UpdateRelationship", func(_ context.Context, g *graphpkg.Graph, r *types.Relationship) error {
			return g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				_, err := tx.UpdateRelationship(r.ID(), map[string]any{"w": int64(7)})
				return err
			})
		}},
		{"Tx.DeleteRelationship", func(_ context.Context, g *graphpkg.Graph, r *types.Relationship) error {
			return g.Tx().Run(func(tx *graphpkg.GraphTx) error { return tx.DeleteRelationship(r.ID()) })
		}},
		{"Batch.UpdateRelationship", func(_ context.Context, g *graphpkg.Graph, r *types.Relationship) error {
			_, err := g.Batch().Run(func(b *graphpkg.BatchBuilder) error {
				return b.UpdateRelationship(r.ID(), map[string]any{"w": int64(8)})
			})
			return err
		}},
		{"Batch.DeleteRelationship", func(_ context.Context, g *graphpkg.Graph, r *types.Relationship) error {
			_, err := g.Batch().Run(func(b *graphpkg.BatchBuilder) error { return b.DeleteRelationship(r.ID()) })
			return err
		}},
		{"Batch.DeleteNode (cascade)", func(_ context.Context, g *graphpkg.Graph, r *types.Relationship) error {
			_, err := g.Batch().Run(func(b *graphpkg.BatchBuilder) error { return b.DeleteNode(r.EndNodeID()) })
			return err
		}},
	}
}

func TestRelMutationEpochMovesOnEveryRelationshipWrite(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		layouts := [][2]string{{"A", "B"}}
		if b.tiered {
			layouts = append(layouts, [2]string{"Ref", "B"}) // cross-shard
		}
		for _, layout := range layouts {
			for _, door := range relEpochDoors() {
				t.Run(layout[0]+"->"+layout[1]+"/"+door.name, func(t *testing.T) {
					start, err := g.Nodes().Add(ctx, []string{layout[0]}, nil)
					if err != nil {
						t.Fatalf("Add start: %v", err)
					}
					end, err := g.Nodes().Add(ctx, []string{layout[1]}, nil)
					if err != nil {
						t.Fatalf("Add end: %v", err)
					}
					r, err := g.Rels().Add(ctx, "T", start, end, map[string]any{"w": int64(1)})
					if err != nil {
						t.Fatalf("Add rel: %v", err)
					}
					before := g.Rels().RelMutationEpoch()
					if err := door.run(ctx, g, r); err != nil {
						t.Fatalf("%s: %v", door.name, err)
					}
					if after := g.Rels().RelMutationEpoch(); after == before {
						t.Fatalf("%s left RelMutationEpoch at %d", door.name, after)
					}
				})
			}
		}
	})
}

// A rolled-back transaction rewrites the relationship rows it touched, so the
// epoch must move again at the rollback: a reader that sampled the epoch
// between the transaction's write and the rollback holds state the rollback
// discards.
func TestRelMutationEpochMovesOnRollback(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, _ storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		start, err := g.Nodes().Add(ctx, []string{"A"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		end, err := g.Nodes().Add(ctx, []string{"B"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels().Add(ctx, "T", start, end, map[string]any{"w": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		tx, err := g.Tx().Begin()
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.SetRelationshipProperty(r.ID(), "w", int64(2)); err != nil {
			t.Fatal(err)
		}
		mid := g.Rels().RelMutationEpoch()
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if g.Rels().RelMutationEpoch() == mid {
			t.Fatalf("Rollback left RelMutationEpoch at %d", mid)
		}
		got, err := g.Rels().Get(ctx, r.ID())
		if err != nil {
			t.Fatal(err)
		}
		if v, _ := got.GetProperty("w"); v != int64(1) {
			t.Fatalf("w after rollback = %v, want 1", v)
		}
	})
}

// The cached relationship columns are keyed on the same counters: a property
// write must not leave a column scan serving the value before the write
// (two-phase: the first scan caches the column at w=1, the write moves w to 2,
// the second scan must report 2 and must not report 1).
func TestRelColumnsFollowPropertyWritesAndDeletes(t *testing.T) {
	eachBackend(t, func(t *testing.T, g *graphpkg.Graph) {
		ctx := context.Background()
		a, _ := g.Nodes().Add(ctx, []string{"City"}, nil)
		b, _ := g.Nodes().Add(ctx, []string{"City"}, nil)
		keep, err := g.Rels().Add(ctx, "ROAD", a, b, map[string]any{"weight": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		gone, err := g.Rels().Add(ctx, "ROAD", b, a, map[string]any{"weight": int64(5)})
		if err != nil {
			t.Fatal(err)
		}
		scan := func() map[types.RelID]int64 {
			out := map[types.RelID]int64{}
			ok, err := g.ScanRelColumns("ROAD", []string{"weight"}, graphpkg.QueryOpts{},
				func(batch *graphpkg.RelColumnBatch) bool {
					ii := 0
					for i := range batch.IDs {
						if batch.Null[0][i] {
							out[batch.IDs[i]] = -1
						} else {
							out[batch.IDs[i]] = batch.Ints[0][ii]
						}
						ii++
					}
					return true
				})
			if err != nil || !ok {
				t.Fatalf("ScanRelColumns ok=%v err=%v", ok, err)
			}
			return out
		}
		if got := scan(); got[keep.ID()] != 1 || got[gone.ID()] != 5 || len(got) != 2 {
			t.Fatalf("first scan = %v", got)
		}
		if err := g.Rels().SetProperty(ctx, keep.ID(), "weight", int64(2)); err != nil {
			t.Fatal(err)
		}
		if err := g.Rels().Delete(ctx, gone.ID()); err != nil {
			t.Fatal(err)
		}
		got := scan()
		if len(got) != 1 || got[keep.ID()] != 2 {
			t.Fatalf("scan after the writes = %v, want only %d -> 2", got, keep.ID())
		}
		if err := g.Rels().DeleteProperty(ctx, keep.ID(), "weight"); err != nil {
			t.Fatal(err)
		}
		if got := scan(); len(got) != 1 || got[keep.ID()] != -1 {
			t.Fatalf("scan after the property removal = %v, want %d absent", got, keep.ID())
		}
	})
}
