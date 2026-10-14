package tiered

import (
	"sync/atomic"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	snowflakepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/snowflake"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// moveRace is one reference node a (properties k="v", k2="w", one history version)
// with one relationship a→b (one history version), all on the reference
// shard or, when archived, a and the relationship on the archive.
type moveRace struct {
	ts      *Store
	label   uint16
	relType uint16
	a       types.NodeID
	rel     types.RelID
}

func newMoveRace(t *testing.T, archived bool) moveRace {
	t.Helper()
	ts := newTestTieredStore(t)
	caseTok, _, _ := installDefaultTestLabelRegistry(t, ts)
	gen := tieredNodeGen(t)
	a := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := a.SetProperty("k", "v"); err != nil {
		t.Fatal(err)
	}
	if err := a.SetProperty("k2", "w"); err != nil {
		t.Fatal(err)
	}
	b := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	for _, n := range []*types.Node{a, b} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	a2 := a.DeepCopy()
	a2.SetVersion(a.Version() + 1)
	if err := ts.ReplaceNodeWithHistory(a2, a.Version(), a); err != nil {
		t.Fatal(err)
	}
	const relType = 1
	r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), relType, a.ID(), b.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	r2 := r.DeepCopy()
	r2.SetVersion(r.Version() + 1)
	if err := ts.ReplaceRelWithHistory(r2, r.Version(), r); err != nil {
		t.Fatal(err)
	}
	if archived {
		if err := ts.ArchiveNode(a.ID()); err != nil {
			t.Fatal(err)
		}
	}
	return moveRace{ts: ts, label: caseTok, relType: relType, a: a.ID(), rel: r.ID()}
}

func countNode(ns []*types.Node, id types.NodeID) (c int) {
	for _, n := range ns {
		if n.ID() == id {
			c++
		}
	}
	return c
}

func countRel(rs []*types.Relationship, id types.RelID) (c int) {
	for _, r := range rs {
		if r.ID() == id {
			c++
		}
	}
	return c
}

func countID[T comparable](ids []T, id T) (c int) {
	for _, x := range ids {
		if x == id {
			c++
		}
	}
	return c
}

// TestRefArchiveReadersSeeMovedEntityOnce: an ArchiveNode or RestoreNode that
// runs between a door's reference shard read and its archive read must not
// make the moved node (or its relationship) appear twice or vanish: each door
// answers what it answers with no move in flight. The move runs concurrently
// and the hook waits a bounded time for it, so a door that holds moves off
// still finishes (and the move then runs after it).
//
// A DocValues fold returns its epoch snapshot; an answer whose epoch moved is
// one the caller discards (Gate 2), so only an answer with an unchanged epoch
// is checked.
func TestRefArchiveReadersSeeMovedEntityOnce(t *testing.T) {
	all := QueryOpts{Depth: DepthAll}
	docValues := func(m moveRace, multi bool) (int, bool, error) {
		c := 0
		fn := func(id types.NodeID, _ []any, _ []bool) bool {
			if id == m.a {
				c++
			}
			return true
		}
		var gen uint64
		var ok bool
		var err error
		if multi {
			gen, ok, err = m.ts.ForEachDocValuesMulti([]uint16{m.label}, []string{"k"}, fn)
		} else {
			gen, ok, err = m.ts.ForEachDocValues(m.label, []string{"k"}, fn)
		}
		if err != nil || !ok || m.ts.NodeMutationEpoch() != gen {
			return 0, false, err // declined, or an answer the caller discards
		}
		return c, true, nil
	}
	plain := func(f func(m moveRace) (int, error)) func(m moveRace) (int, bool, error) {
		return func(m moveRace) (int, bool, error) {
			n, err := f(m)
			return n, true, err
		}
	}
	for _, tc := range []struct {
		name    string
		measure func(m moveRace) (n int, checked bool, err error)
	}{
		{"NodesByLabel", plain(func(m moveRace) (int, error) {
			ns, err := m.ts.NodesByLabel(m.label, all)
			return countNode(ns, m.a), err
		})},
		{"NodesByLabelAndProperty", plain(func(m moveRace) (int, error) {
			ns, err := m.ts.NodesByLabelAndProperty(m.label, "k", "v", all)
			return countNode(ns, m.a), err
		})},
		{"NodesByLabelAndProperties", plain(func(m moveRace) (int, error) {
			ns, err := m.ts.NodesByLabelAndProperties(m.label, map[string]any{"k": "v", "k2": "w"}, all)
			return countNode(ns, m.a), err
		})},
		{"AllNodes", plain(func(m moveRace) (int, error) {
			ns, err := m.ts.AllNodes(all)
			return countNode(ns, m.a), err
		})},
		{"AllNodeIDs", plain(func(m moveRace) (int, error) {
			ids, err := m.ts.AllNodeIDs(all)
			return countID(ids, m.a), err
		})},
		{"ForEachNodeID", plain(func(m moveRace) (int, error) {
			c := 0
			err := m.ts.ForEachNodeID(func(id types.NodeID) bool {
				if id == m.a {
					c++
				}
				return true
			})
			return c, err
		})},
		{"NodeCount", plain(func(m moveRace) (int, error) { return m.ts.NodeCount() })},
		{"NodeCountByLabel", plain(func(m moveRace) (int, error) { return m.ts.NodeCountByLabel(m.label) })},
		{"NodeCountByLabelAndPropertyKey", plain(func(m moveRace) (int, error) {
			return m.ts.NodeCountByLabelAndPropertyKey(m.label, "k")
		})},
		{"NodePropertyTypeClassCounts", plain(func(m moveRace) (int, error) {
			c, err := m.ts.NodePropertyTypeClassCounts(m.label, "k")
			return int(c.String), err
		})},
		{"ForEachDocValues", func(m moveRace) (int, bool, error) { return docValues(m, false) }},
		{"ForEachDocValuesMulti", func(m moveRace) (int, bool, error) { return docValues(m, true) }},
		{"AllNodeHistoryIDsFrom", plain(func(m moveRace) (int, error) {
			ids, err := m.ts.AllNodeHistoryIDsFrom(0, 0)
			return countID(ids, m.a), err
		})},
		{"GetNode", plain(func(m moveRace) (int, error) {
			if _, err := m.ts.GetNode(m.a); err != nil {
				return 0, nil
			}
			return 1, nil
		})},
		{"RelationshipsByType", plain(func(m moveRace) (int, error) {
			rs, err := m.ts.RelationshipsByType(m.relType, all)
			return countRel(rs, m.rel), err
		})},
		{"AllRelationships", plain(func(m moveRace) (int, error) {
			rs, err := m.ts.AllRelationships(all)
			return countRel(rs, m.rel), err
		})},
		{"AllRelIDs", plain(func(m moveRace) (int, error) {
			ids, err := m.ts.AllRelIDs(all)
			return countID(ids, m.rel), err
		})},
		{"ForEachRelID", plain(func(m moveRace) (int, error) {
			c := 0
			err := m.ts.ForEachRelID(func(id types.RelID) bool {
				if id == m.rel {
					c++
				}
				return true
			})
			return c, err
		})},
		{"RelationshipCount", plain(func(m moveRace) (int, error) { return m.ts.RelationshipCount() })},
		{"RelCountByType", plain(func(m moveRace) (int, error) { return m.ts.RelCountByType(m.relType) })},
		{"AllRelHistoryIDsFrom", plain(func(m moveRace) (int, error) {
			ids, err := m.ts.AllRelHistoryIDsFrom(0, 0)
			return countID(ids, m.rel), err
		})},
		{"GetRelationship", plain(func(m moveRace) (int, error) {
			if _, err := m.ts.GetRelationship(m.rel); err != nil {
				return 0, nil
			}
			return 1, nil
		})},
	} {
		for _, mv := range []struct {
			name     string
			archived bool
		}{{"ArchiveNode", false}, {"RestoreNode", true}} {
			t.Run(tc.name+"/"+mv.name, func(t *testing.T) {
				m := newMoveRace(t, mv.archived)
				want, _, err := tc.measure(m)
				if err != nil {
					t.Fatal(err)
				}
				moved := make(chan error, 1)
				var fired atomic.Bool
				m.ts.fanoutRefReadHook = func() {
					if !fired.CompareAndSwap(false, true) {
						return
					}
					go func() {
						if mv.archived {
							moved <- m.ts.RestoreNode(m.a)
						} else {
							moved <- m.ts.ArchiveNode(m.a)
						}
					}()
					select {
					case err := <-moved:
						moved <- err
					case <-time.After(250 * time.Millisecond):
					}
				}
				got, checked, err := tc.measure(m)
				if err != nil {
					t.Fatal(err)
				}
				if !fired.Load() {
					t.Skip("door never reached its archive read (answered from the reference shard)")
				}
				if err := <-moved; err != nil {
					t.Fatalf("%s: %v", mv.name, err)
				}
				if checked && got != want {
					t.Fatalf("%s during the read: got %d, want %d (no move in flight)", mv.name, got, want)
				}
			})
		}
	}
}

// BenchmarkTieredGetNodeMoveLock: GetNode on a reference node (answered by the
// reference shard) and on an archived one (reference miss, then archive).
func BenchmarkTieredGetNodeMoveLock(b *testing.B) {
	for _, archived := range []bool{false, true} {
		name := "Reference"
		if archived {
			name = "Archived"
		}
		b.Run(name, func(b *testing.B) {
			ts, err := New(Config{InMemory: true, RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
			if err != nil {
				b.Fatal(err)
			}
			defer ts.Close()
			reg := registrypkg.NewLabelRegistry()
			ts.SetLabelRegistry(reg)
			caseTok, _ := reg.GetOrCreate("Case")
			gen, err := snowflake.NewNode(7, snowflake.WithEpoch(snowflakepkg.Epoch), snowflake.WithMicroseconds(),
				snowflake.WithNodeBits(5), snowflake.WithStepBits(10))
			if err != nil {
				b.Fatal(err)
			}
			n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
			if err := ts.PutNode(n); err != nil {
				b.Fatal(err)
			}
			if archived {
				if err := ts.ArchiveNode(n.ID()); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := ts.GetNode(n.ID()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
