package core

import (
	"context"
	"sync"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// degreeOracle counts a type's degree stats from its relationships.
func degreeOracle(t *testing.T, g *Core, typeName string) storepkg.RelTypeDegreeStats {
	t.Helper()
	out := map[types.NodeID]int64{}
	in := map[types.NodeID]int64{}
	var rels int64
	if err := g.Rels.ForEachByType(typeName, storepkg.QueryOpts{}, func(r *types.Relationship) bool {
		rels++
		out[r.StartNodeID()]++
		in[r.EndNodeID()]++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	st := storepkg.DegreeStatsFromCounts(rels, out, in)
	st.Exact = true
	return st
}

// TestTrackRelTypeDegrees (sigma-tkgd round 5 request 5, built in round 7
// request 2): a tracked type's RelTypeDegreeStats equal a count of its
// relationships after every step, and after creates through every recording
// door (Add, AddByID, AddByIDIfAbsent, Import, a transaction, AddRelationships,
// a Batch) they are applied, not counted again; a delete, an update, a
// rolled-back transaction and a write of another type in the same stamp
// stripe count again; an untracked type and an untracked-again type count
// as before. Memory and badger.
func TestTrackRelTypeDegrees(t *testing.T) {
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var st storepkg.Store = memory.New()
			if backend == "badger" {
				bs, err := badger.New(badger.Config{InMemory: true})
				if err != nil {
					t.Fatal(err)
				}
				st = bs
			}
			g, err := New(Config{Store: st})
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			var nodes []*types.Node
			for range 12 {
				n, err := g.Nodes.Add(ctx, []string{"N"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				nodes = append(nodes, n)
			}
			if err := g.Stats.TrackRelTypeDegrees("T"); err != nil {
				t.Fatal(err)
			}
			tr := &g.relDegreeTrack
			step := func(name string, wantApplied bool) {
				t.Helper()
				counts, applies := tr.counts.Load(), tr.applies.Load()
				got, err := g.Stats.RelTypeDegreeStats("T")
				if err != nil {
					t.Fatal(err)
				}
				if want := degreeOracle(t, g, "T"); got != want {
					t.Fatalf("%s: stats %+v, want %+v", name, got, want)
				}
				applied := tr.applies.Load() > applies && tr.counts.Load() == counts
				if applied != wantApplied {
					t.Fatalf("%s: applied=%v (counts %d->%d, applies %d->%d), want applied=%v", name, applied, counts, tr.counts.Load(), applies, tr.applies.Load(), wantApplied)
				}
			}
			mustRel := func(r *types.Relationship, err error) *types.Relationship {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
				return r
			}
			r0 := mustRel(g.Rels.Add(ctx, "T", nodes[0], nodes[1], nil))
			step("first count", false)
			mustRel(g.Rels.Add(ctx, "T", nodes[0], nodes[2], nil))
			mustRel(g.Rels.AddByID(ctx, "T", nodes[0].ID(), nodes[3].ID(), nil))
			step("Add, AddByID", true)
			if _, created, err := g.Rels.AddByIDIfAbsent(ctx, "T", nodes[4].ID(), nodes[3].ID(), nil); err != nil || !created {
				t.Fatal(created, err)
			}
			mustRel(g.Rels.Import(ctx, g.Rels.NextID(), "T", nodes[5], nodes[3], nil))
			step("AddByIDIfAbsent, Import", true)
			tx, err := g.BeginTx()
			if err != nil {
				t.Fatal(err)
			}
			mustRel(tx.AddRelationship("T", nodes[6], nodes[0], nil))
			if _, err := tx.AddRelationships("T", []RelCreate{{StartID: nodes[0].ID(), EndID: nodes[7].ID()}, {StartID: nodes[8].ID(), EndID: nodes[0].ID()}}); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			step("a transaction with AddRelationships", true)
			b, err := NewBatchBuilder(g)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := b.AddRelationship("T", nodes[9], nodes[0], nil); err != nil {
				t.Fatal(err)
			}
			if _, err := b.Execute(); err != nil {
				t.Fatal(err)
			}
			step("a batch", true)
			step("no write", false) // the cached base answers: neither counted nor applied

			if err := g.Rels.Delete(ctx, r0.ID()); err != nil {
				t.Fatal(err)
			}
			step("after a delete", false)
			mustRel(g.Rels.Add(ctx, "T", nodes[10], nodes[11], nil))
			step("a create after the recount", true)
			if _, err := g.Rels.Update(ctx, nodesRel(t, g, nodes[0]), map[string]any{"w": int64(1)}); err != nil {
				t.Fatal(err)
			}
			step("after an update", false)
			tx, err = g.BeginTx()
			if err != nil {
				t.Fatal(err)
			}
			mustRel(tx.AddRelationship("T", nodes[1], nodes[2], nil))
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			step("after a rolled-back transaction", false)
			// A type in T's stamp stripe: its write moves T's stamp too.
			other := ""
			for i := 0; other == ""; i++ {
				name := "U" + string(rune('a'+i%26)) + string(rune('a'+i/26))
				mustRel(g.Rels.Add(ctx, name, nodes[1], nodes[2], nil))
				tokT, _ := g.relTypes.Lookup("T")
				tokU, _ := g.relTypes.Lookup(name)
				if tokU%256 == tokT%256 && tokU != tokT {
					other = name
				}
				g.Stats.RelTypeDegreeStats("T") // keep T's base current between the probes
			}
			mustRel(g.Rels.Add(ctx, "T", nodes[3], nodes[4], nil))
			mustRel(g.Rels.Add(ctx, other, nodes[3], nodes[4], nil))
			step("after a write of a type in T's stripe", false)

			if err := g.Stats.UntrackRelTypeDegrees("T"); err != nil {
				t.Fatal(err)
			}
			mustRel(g.Rels.Add(ctx, "T", nodes[5], nodes[6], nil))
			step("untracked", false)
			if tr.any.Load() {
				t.Fatal("untracking the only type left tracking on")
			}
			if err := g.Stats.TrackRelTypeDegrees(""); err == nil {
				t.Fatal("empty type name tracked")
			}
		})
	}
}

// nodesRel returns the ID of a T relationship starting at n.
func nodesRel(t *testing.T, g *Core, n *types.Node) types.RelID {
	t.Helper()
	var id types.RelID
	if err := g.Rels.ForEachByType("T", storepkg.QueryOpts{}, func(r *types.Relationship) bool {
		if r.StartNodeID() == n.ID() {
			id = r.ID()
			return false
		}
		return true
	}); err != nil || id == 0 {
		t.Fatalf("no T relationship from %d: %v", n.ID(), err)
	}
	return id
}

// TestTrackRelTypeDegrees_ConcurrentWriters: four goroutines create and delete
// relationships of a tracked type while two read its stats; once they stop,
// the stats equal a count (run under -race).
func TestTrackRelTypeDegrees_ConcurrentWriters(t *testing.T) {
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var st storepkg.Store = memory.New()
			if backend == "badger" {
				bs, err := badger.New(badger.Config{InMemory: true})
				if err != nil {
					t.Fatal(err)
				}
				st = bs
			}
			g, err := New(Config{Store: st})
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			if err := g.Stats.TrackRelTypeDegrees("T"); err != nil {
				t.Fatal(err)
			}
			var nodes []*types.Node
			for range 20 {
				n, err := g.Nodes.Add(ctx, []string{"N"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				nodes = append(nodes, n)
			}
			var wg sync.WaitGroup
			stop := make(chan struct{})
			for w := range 4 {
				wg.Go(func() {
					for i := range 60 {
						s := (w*5 + i) % 20
						r, err := g.Rels.Add(ctx, "T", nodes[s], nodes[(s+1+i%19)%20], nil)
						if err != nil {
							t.Error(err)
							return
						}
						if i%7 == 0 {
							if err := g.Rels.Delete(ctx, r.ID()); err != nil {
								t.Error(err)
								return
							}
						}
					}
				})
			}
			var readers sync.WaitGroup
			for range 2 {
				readers.Go(func() {
					for {
						select {
						case <-stop:
							return
						default:
						}
						if _, err := g.Stats.RelTypeDegreeStats("T"); err != nil {
							t.Error(err)
							return
						}
					}
				})
			}
			wg.Wait()
			close(stop)
			readers.Wait()
			got, err := g.Stats.RelTypeDegreeStats("T")
			if err != nil {
				t.Fatal(err)
			}
			if want := degreeOracle(t, g, "T"); got != want {
				t.Fatalf("after the writers: %+v, want %+v", got, want)
			}
		})
	}
}
