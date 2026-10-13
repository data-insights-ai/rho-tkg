package graph_test

import (
	"bytes"
	"context"
	"math"
	"reflect"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRelPlannerStatsMemoBounded is backlog 53 (ai-soc) at the graph doors:
// the relationship planner statistics (presence count, min/max, type-class
// counts) stay exact through every door that removes or replaces a current
// row. After each break case every counter equals a full recount of the
// current relationships (the oracle). The heap bound (RAM per distinct
// statistic, not per relationship) and the store-only seams are the
// same-named test in pkg/graph/store/badger.
func TestRelPlannerStatsMemoBounded(t *testing.T) {
	type fixture struct {
		g    *graphpkg.Graph
		a, b types.NodeID
		old  types.NodeID
		rels []types.RelID
	}
	steps := []struct {
		name string
		run  func(t *testing.T, f *fixture) *graphpkg.Graph // returns the graph to check
	}{
		{"delete", func(t *testing.T, f *fixture) *graphpkg.Graph {
			mustOK(t, f.g.Rels().Delete(ctx0, f.rels[0]))
			mustOK(t, f.g.Rels().Delete(ctx0, f.rels[5])) // the max w holder: min/max rescan
			return f.g
		}},
		{"replace in place with another class", func(t *testing.T, f *fixture) *graphpkg.Graph {
			_, err := f.g.Rels().UpdateInPlace(ctx0, f.rels[1], map[string]any{"w": "now a string", "score": math.NaN()})
			mustOK(t, err)
			return f.g
		}},
		{"update with history to another class", func(t *testing.T, f *fixture) *graphpkg.Graph {
			_, err := f.g.Rels().Update(ctx0, f.rels[2], map[string]any{"w": true, "kind": int64(9)})
			mustOK(t, err)
			return f.g
		}},
		{"delete property in a tx", func(t *testing.T, f *fixture) *graphpkg.Graph {
			tx, err := f.g.Tx().Begin()
			mustOK(t, err)
			mustOK(t, tx.DeleteRelationshipProperty(f.rels[3], "w"))
			mustOK(t, tx.Commit())
			return f.g
		}},
		{"tx rollback restores a deleted rel", func(t *testing.T, f *fixture) *graphpkg.Graph {
			tx, err := f.g.Tx().Begin()
			mustOK(t, err)
			mustOK(t, tx.DeleteRelationship(f.rels[4]))
			mustOK(t, tx.Rollback())
			return f.g
		}},
		{"cascade delete", func(t *testing.T, f *fixture) *graphpkg.Graph {
			mustOK(t, f.g.Nodes().Delete(ctx0, f.b))
			return f.g
		}},
		{"retention purge", func(t *testing.T, f *fixture) *graphpkg.Graph {
			_, err := f.g.Admin().PurgeExpiredNodes(ctx0, adminpkg.PurgePolicy{Label: "Old", Mode: adminpkg.PurgeByAge, Before: farFuture})
			mustOK(t, err)
			return f.g
		}},
		{"replica apply", func(t *testing.T, f *fixture) *graphpkg.Graph {
			replica := relStatsReplicaOf(t, f.g, func() {
				mustOK(t, f.g.Rels().Delete(ctx0, f.rels[0]))
				_, err := f.g.Rels().Update(ctx0, f.rels[1], map[string]any{"w": "s"})
				mustOK(t, err)
				_, err = f.g.Rels().UpdateInPlace(ctx0, f.rels[2], map[string]any{"kind": false})
				mustOK(t, err)
				mustOK(t, f.g.Nodes().Delete(ctx0, f.b))
			})
			return replica
		}},
		{"IO.Import", func(t *testing.T, f *fixture) *graphpkg.Graph {
			mustOK(t, f.g.Rels().Delete(ctx0, f.rels[0]))
			_, err := f.g.Rels().Update(ctx0, f.rels[1], map[string]any{"w": "s"})
			mustOK(t, err)
			var snap bytes.Buffer
			mustOK(t, f.g.IO().Export(&snap))
			imported, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 9, BadgerInMemory: true})
			mustOK(t, err)
			t.Cleanup(func() { _ = imported.Close() })
			mustOK(t, imported.IO().Import(&snap, tkgio.ImportOptions{}))
			// A delete and a replace after the import decrement what the import counted.
			mustOK(t, imported.Rels().Delete(ctx0, f.rels[6]))
			_, err = imported.Rels().UpdateInPlace(ctx0, f.rels[7], map[string]any{"w": "t"})
			mustOK(t, err)
			return imported
		}},
	}

	seed := func(t *testing.T, cfg graphpkg.Config) *fixture {
		g, err := graphpkg.New(cfg)
		mustOK(t, err)
		t.Cleanup(func() { _ = g.Close() })
		f := &fixture{g: g}
		f.a = mustAdd(t, g, []string{"P"}, nil).ID()
		f.b = mustAdd(t, g, []string{"P"}, nil).ID()
		f.old = mustAdd(t, g, []string{"Old"}, nil).ID()
		ends := []types.NodeID{f.b, f.old, f.b}
		for i := 0; i < 24; i++ {
			typ := "KNOWS"
			if i%4 == 3 {
				typ = "LIKES"
			}
			r, err := g.Rels().AddByID(ctx0, typ, f.a, ends[i%3], relStatsProps(i))
			mustOK(t, err)
			f.rels = append(f.rels, r.ID())
		}
		assertRelStatsMatchRecount(t, "seed", g)
		return f
	}

	for _, backend := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{SnowflakeNodeID: 7, ChangeLog: true, AllowRetentionPurge: true}},
		{"badger", graphpkg.Config{SnowflakeNodeID: 7, BadgerInMemory: true, ChangeLog: true, AllowRetentionPurge: true}},
	} {
		for _, step := range steps {
			t.Run(backend.name+"/"+step.name, func(t *testing.T) {
				f := seed(t, backend.cfg)
				assertRelStatsMatchRecount(t, step.name, step.run(t, f))
			})
		}
	}

	t.Run("badger/reopen", func(t *testing.T) {
		dir := t.TempDir()
		cfg := graphpkg.Config{SnowflakeNodeID: 7, BadgerDir: dir}
		f := seed(t, cfg)
		mustOK(t, f.g.Rels().Delete(ctx0, f.rels[0]))
		mustOK(t, f.g.Close())
		g2, err := graphpkg.New(cfg)
		mustOK(t, err)
		t.Cleanup(func() { _ = g2.Close() })
		assertRelStatsMatchRecount(t, "after reopen", g2)
		// Deletes and replaces after the reopen decrement what the open rebuilt.
		mustOK(t, g2.Rels().Delete(ctx0, f.rels[5]))
		_, err = g2.Rels().UpdateInPlace(ctx0, f.rels[1], map[string]any{"w": "x"})
		mustOK(t, err)
		mustOK(t, g2.Nodes().Delete(ctx0, f.old))
		assertRelStatsMatchRecount(t, "after reopen + writes", g2)
	})
}

var ctx0 = context.Background()

func mustOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// relStatsProps gives relationship i a few properties of every class from small
// value domains, so the number of distinct statistics stays fixed as i grows.
func relStatsProps(i int) map[string]any {
	return map[string]any{
		"w":     int64(i % 6),
		"kind":  []string{"x", "y", "z"}[i%3],
		"score": float64(i%4) / 2,
		"tags":  []string{"t"},
	}
}

// relStatsReplicaOf bootstraps a read-only replica of primary from a snapshot,
// runs mutate on the primary, and applies the primary's change feed to the
// replica.
func relStatsReplicaOf(t *testing.T, primary *graphpkg.Graph, mutate func()) *graphpkg.Graph {
	t.Helper()
	var snap bytes.Buffer
	mustOK(t, primary.IO().Export(&snap))
	lsn0, err := primary.Replication().LastCommittedLSN()
	mustOK(t, err)
	replica, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 8, BadgerInMemory: true, ReadOnlyReplica: true})
	mustOK(t, err)
	t.Cleanup(func() { _ = replica.Close() })
	mustOK(t, replica.IO().Import(&snap, tkgio.ImportOptions{}))
	mustOK(t, replica.Replication().SetAppliedLSN(lsn0))
	mutate()
	var recs []store.ChangeRecord
	mustOK(t, primary.Replication().ForEachChange(lsn0, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}))
	_, err = replica.Replication().ApplyChanges(recs)
	mustOK(t, err)
	return replica
}

// assertRelStatsMatchRecount is the oracle: every relationship planner
// statistic equals a full recount of the current relationships.
func assertRelStatsMatchRecount(t *testing.T, step string, g *graphpkg.Graph) {
	t.Helper()
	for _, typ := range []string{"KNOWS", "LIKES"} {
		want := recountRelStats(t, g, typ)
		for _, key := range []string{"w", "kind", "score", "tags", "absent"} {
			w := want[key]
			c, err := g.Stats().RelPropertyTypeClassCounts(typ, key)
			mustOK(t, err)
			got := [types.NumPropertyTypeClasses]int64{c.Numeric, c.NaN, c.String, c.Bool, c.Other}
			if got != w.classes {
				t.Errorf("%s: %s.%s type classes = %v, recount %v", step, typ, key, got, w.classes)
			}
			s, err := g.Stats().RelPropertyStats(typ, key)
			mustOK(t, err)
			if s.Count != w.count {
				t.Errorf("%s: %s.%s Count = %d, recount %d", step, typ, key, s.Count, w.count)
			}
			if w.mixed {
				continue // min/max of a mixed-family key depend on visit order
			}
			if !reflect.DeepEqual(s.Min, w.min) || !reflect.DeepEqual(s.Max, w.max) {
				t.Errorf("%s: %s.%s Min/Max = %v/%v, recount %v/%v", step, typ, key, s.Min, s.Max, w.min, w.max)
			}
		}
	}
}

type relStatsRecount struct {
	classes  [types.NumPropertyTypeClasses]int64
	count    int64
	min, max any
	family   string
	mixed    bool
}

func recountRelStats(t *testing.T, g *graphpkg.Graph, typ string) map[string]*relStatsRecount {
	t.Helper()
	out := map[string]*relStatsRecount{}
	at := func(k string) *relStatsRecount {
		if out[k] == nil {
			out[k] = &relStatsRecount{}
		}
		return out[k]
	}
	for _, k := range []string{"w", "kind", "score", "tags", "absent"} {
		at(k)
	}
	err := g.Rels().ForEachByType(typ, store.QueryOpts{}, func(r *types.Relationship) bool {
		r.ForEachPropertyTypeClass(func(k string, c types.PropertyTypeClass) bool {
			at(k).classes[c]++
			return true
		})
		r.ForEachIndexablePropertyValueKey(func(k, _ string) bool {
			e := at(k)
			e.count++
			v, _ := r.GetProperty(k)
			var fam string
			var less func(x, y any) bool
			switch v.(type) {
			case int64:
				fam, less = "n", func(x, y any) bool { return x.(int64) < y.(int64) }
			case float64:
				if math.IsNaN(v.(float64)) {
					return true
				}
				fam, less = "f", func(x, y any) bool { return x.(float64) < y.(float64) }
			case string:
				fam, less = "s", func(x, y any) bool { return x.(string) < y.(string) }
			default:
				return true // unordered family: no min/max
			}
			switch {
			case e.family == "":
				e.family, e.min, e.max = fam, v, v
			case e.family != fam:
				e.mixed = true
			default:
				if less(v, e.min) {
					e.min = v
				}
				if less(e.max, v) {
					e.max = v
				}
			}
			return true
		})
		return true
	})
	if err != nil {
		t.Fatalf("recount %s: %v", typ, err)
	}
	return out
}
