package badger

import (
	"fmt"
	"math"
	"reflect"
	"runtime"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRelPlannerStatsMemoBounded is backlog 53 at the store seam (the graph
// door cases live in pkg/graph under the same name). "heap" measures the heap
// the relationship planner statistics retain — stats on minus stats off, same
// writes, flushed before measuring — at 10 K and 100 K relationships: the
// growth must be a constant, not an amount per relationship. The door cases
// reach the store-only delete seams (batch delete, the tiered split-write
// legs, purge by descriptor, foreign incoming stubs, adjacent purge, cascade,
// exact erasure, and the batch and cascade re-read fallback after a replace in
// the prefetch window); after each step every counter equals a full recount
// of the current relationships.
func TestRelPlannerStatsMemoBounded(t *testing.T) {
	if testing.Short() && !raceEnabled {
		t.Skip("long tier: release gate")
	}
	t.Run("heap", func(t *testing.T) {
		// The least of three: the measure is the process heap, and leftover
		// background work of earlier tests (badger compaction) only ever adds
		// to one run (130 MB once in the long tier, 25 Oct; LONGTIER).
		var small, large int64
		perRel := math.Inf(1)
		for range 3 {
			s, l := storeRelStatsRetainedHeap(t, 10_000), storeRelStatsRetainedHeap(t, 100_000)
			if p := float64(l-s) / 90_000; p < perRel {
				small, large, perRel = s, l, p
			}
		}
		t.Logf("planner-stat heap: %d B at 10K rels, %d B at 100K rels, growth %.1f B per rel", small, large, perRel)
		if perRel > 8 {
			t.Fatalf("planner-stat heap grows %.1f B per relationship written (10K: %d B, 100K: %d B); want a constant (≤ 8 B/rel)", perRel, small, large)
		}
	})

	const knows = 3
	a, b, c := types.NodeID(snowflake.ID(11)), types.NodeID(snowflake.ID(12)), types.NodeID(snowflake.ID(13))
	rel := func(id int64, start, end types.NodeID, props map[string]any) *types.Relationship {
		r := types.NewRelationship(types.RelID(snowflake.ID(id)), knows, start, end)
		for k, v := range props {
			if err := r.SetProperty(k, v); err != nil {
				t.Fatalf("SetProperty %s: %v", k, err)
			}
		}
		return r
	}
	ok := func(t *testing.T, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		name  string
		steps []func(t *testing.T, bs *Store)
	}{
		{"batch delete", []func(*testing.T, *Store){
			func(t *testing.T, bs *Store) { ok(t, bs.DeleteRelationshipsBatch([]types.RelID{100, 105})) },
		}},
		{"split-write legs: put, replace with another class, delete", []func(*testing.T, *Store){
			func(t *testing.T, bs *Store) {
				ok(t, bs.PutRelEntityAndOut(rel(200, a, c, map[string]any{"w": int64(99)})))
			},
			func(t *testing.T, bs *Store) { ok(t, bs.ReplaceRelationship(rel(200, a, c, map[string]any{"w": "s"}))) },
			func(t *testing.T, bs *Store) { _, err := bs.DeleteRelEntityAndOut(200); ok(t, err) },
		}},
		{"split-write leg purged by descriptor", []func(*testing.T, *Store){
			func(t *testing.T, bs *Store) {
				ok(t, bs.PutRelEntityAndOut(rel(201, a, c, map[string]any{"w": int64(-1)})))
			},
			func(t *testing.T, bs *Store) {
				ok(t, bs.PurgeRelationshipByInfo(storecontract.PurgedRel{ID: 201, TypeToken: knows, StartID: a, EndID: c}))
			},
		}},
		{"foreign incoming stub", []func(*testing.T, *Store){
			func(t *testing.T, bs *Store) {
				ok(t, bs.PutRelationshipForeignIncoming(rel(202, types.NodeID(snowflake.ID(999)), a, map[string]any{"w": int64(50)})))
			},
			func(t *testing.T, bs *Store) { ok(t, bs.DeleteRelationshipForeignIncoming(202)) },
		}},
		{"purge adjacent rels", []func(*testing.T, *Store){
			func(t *testing.T, bs *Store) { _, err := bs.PurgeAdjacentRelsForNode(b); ok(t, err) },
		}},
		{"node cascade", []func(*testing.T, *Store){
			func(t *testing.T, bs *Store) { ok(t, bs.DeleteNodeCascade(b)) },
		}},
		{"batch delete after a replace in the prefetch window", []func(*testing.T, *Store){
			func(t *testing.T, bs *Store) {
				replaceInPrefetchWindow(t, bs, rel(100, a, b, map[string]any{"w": "now a string"}))
				ok(t, bs.DeleteRelationshipsBatch([]types.RelID{100, 102}))
			},
		}},
		{"cascade after a replace in the prefetch window", []func(*testing.T, *Store){
			func(t *testing.T, bs *Store) {
				replaceInPrefetchWindow(t, bs, rel(102, a, b, map[string]any{"w": int64(77)}))
				ok(t, bs.DeleteNodeCascade(b))
			},
		}},
		{"exact erasure", []func(*testing.T, *Store){
			func(t *testing.T, bs *Store) {
				_, err := bs.ExactErase(storecontract.ExactErasureRequest{RelIDs: []types.RelID{100, 103}, Bounds: badgerExactErasureBounds})
				ok(t, err)
			},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bs := newTestBadgerStoreInMemory(t)
			for _, id := range []types.NodeID{a, b, c} {
				ok(t, bs.PutNode(types.NewNode(id, 1, nil)))
			}
			for i := int64(0); i < 12; i++ {
				end := b
				if i%2 == 1 {
					end = c
				}
				ok(t, bs.PutRelationship(rel(100+i, a, end, storeRelStatsProps(int(i)))))
			}
			assertStoreRelStatsMatchRecount(t, "seed", bs, knows)
			for i, step := range tc.steps {
				step(t, bs)
				assertStoreRelStatsMatchRecount(t, tc.name+" step "+string(rune('1'+i)), bs, knows)
			}
		})
	}
}

// replaceInPrefetchWindow arms the prefetch->lock hook so the next batch delete
// or cascade sees r replace its prefetched row: the locked re-read fallback
// must then decrement the replacement, not the prefetched row.
func replaceInPrefetchWindow(t *testing.T, bs *Store, r *types.Relationship) {
	t.Helper()
	bs.relDeletePrefetchTestHook = func() {
		bs.relDeletePrefetchTestHook = nil
		if err := bs.ReplaceRelationship(r); err != nil {
			t.Errorf("replace in the prefetch window: %v", err)
		}
	}
}

// TestRelPlannerStatsMemoBoundedConcurrentDoors races replace, delete, batch
// delete and put on the same relationships; afterwards every counter equals a
// recount (backlog 53). A delete whose prefetched row went stale takes the
// locked re-read fallback, so a fallback that loses the row drifts here.
func TestRelPlannerStatsMemoBoundedConcurrentDoors(t *testing.T) {
	const knows, rels = 3, 40
	nodes := []types.NodeID{11, 12, 13, 14}
	mk := func(id int64, i int) *types.Relationship {
		r := types.NewRelationship(types.RelID(snowflake.ID(id)), knows, nodes[id%2], nodes[2+id%2])
		for k, v := range storeRelStatsProps(i) {
			if err := r.SetProperty(k, v); err != nil {
				t.Fatal(err)
			}
		}
		return r
	}
	for round := 0; round < 20; round++ {
		bs := newTestBadgerStoreInMemory(t)
		for _, id := range nodes {
			if err := bs.PutNode(types.NewNode(id, 1, nil)); err != nil {
				t.Fatal(err)
			}
		}
		for i := int64(0); i < rels; i++ {
			if err := bs.PutRelationship(mk(100+i, int(i))); err != nil {
				t.Fatal(err)
			}
		}
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := int64(0); i < rels; i++ {
					id := 100 + (i*int64(g+1))%rels
					// Errors are expected (a rel another goroutine deleted); the
					// recount below is the assertion.
					switch g % 4 {
					case 0:
						_ = bs.ReplaceRelationship(mk(id, int(i)+g))
					case 1:
						_ = bs.DeleteRelationship(types.RelID(id))
					case 2:
						_ = bs.DeleteRelationshipsBatch([]types.RelID{types.RelID(id), types.RelID(100 + (id+1)%rels)})
					case 3:
						_ = bs.PutRelationship(mk(id, int(i)))
					}
				}
			}(g)
		}
		wg.Wait()
		assertStoreRelStatsMatchRecount(t, fmt.Sprintf("round %d", round), bs, knows)
	}
}

func storeRelStatsProps(i int) map[string]any {
	return map[string]any{
		"w":     int64(i % 6),
		"kind":  []string{"x", "y", "z"}[i%3],
		"score": float64(i%4) / 2,
		"tags":  []string{"t"},
	}
}

func storeRelStatsRetainedHeap(t *testing.T, n int) int64 {
	t.Helper()
	return storeRelRetainedHeap(t, n, false) - storeRelRetainedHeap(t, n, true)
}

// storeRelRetainedHeap writes n relationships into a store that never flushes
// (no periodic flush, no pending bound), so the badger engine allocates the
// same in both modes, and returns the live heap growth.
func storeRelRetainedHeap(t *testing.T, n int, disableStats bool) int64 {
	t.Helper()
	before := storeLiveHeap()
	bs, err := New(Config{InMemory: true, FlushInterval: 1<<63 - 1, MaxPendingWrites: -1, DisablePlannerStats: disableStats})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	a, b := types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))
	for _, id := range []types.NodeID{a, b} {
		if err := bs.PutNode(types.NewNode(id, 1, nil)); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	for i := 0; i < n; i++ {
		r := types.NewRelationship(types.RelID(snowflake.ID(int64(1000+i))), 3, a, b)
		for k, v := range storeRelStatsProps(i) {
			if err := r.SetProperty(k, v); err != nil {
				t.Fatalf("SetProperty: %v", err)
			}
		}
		if err := bs.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship %d: %v", i, err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	after := storeLiveHeap()
	runtime.KeepAlive(bs)
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return after - before
}

func storeLiveHeap() int64 {
	var ms runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&ms)
	return int64(ms.HeapAlloc) // #nosec G115 -- heap size fits int64
}

// assertStoreRelStatsMatchRecount is the oracle: presence count, min/max and
// type-class counts equal a recount of the store's current relationships.
func assertStoreRelStatsMatchRecount(t *testing.T, step string, bs *Store, relType uint16) {
	t.Helper()
	rels, err := bs.RelationshipsByType(relType, QueryOpts{})
	if err != nil {
		t.Fatalf("%s: RelationshipsByType: %v", step, err)
	}
	type recount struct {
		classes    [types.NumPropertyTypeClasses]int64
		count      int64
		min, max   float64
		numeric    bool // a numeric value was seen
		nonNumeric bool // an indexable non-numeric value was seen: min/max not checked
	}
	want := map[string]*recount{}
	at := func(k string) *recount {
		if want[k] == nil {
			want[k] = &recount{}
		}
		return want[k]
	}
	for _, r := range rels {
		r.ForEachPropertyTypeClass(func(k string, c types.PropertyTypeClass) bool {
			at(k).classes[c]++
			return true
		})
		r.ForEachIndexablePropertyValueKey(func(k, _ string) bool {
			e := at(k)
			e.count++
			v, _ := r.GetProperty(k)
			var f float64
			switch x := v.(type) {
			case int64:
				f = float64(x)
			case float64:
				if math.IsNaN(x) {
					return true
				}
				f = x
			default:
				e.nonNumeric = true
				return true
			}
			if !e.numeric {
				e.numeric, e.min, e.max = true, f, f
			}
			e.min, e.max = math.Min(e.min, f), math.Max(e.max, f)
			return true
		})
	}
	for _, key := range []string{"w", "kind", "score", "tags"} {
		w := at(key)
		c, err := bs.RelPropertyTypeClassCounts(relType, key)
		if err != nil {
			t.Fatalf("%s: RelPropertyTypeClassCounts: %v", step, err)
		}
		if got := [types.NumPropertyTypeClasses]int64{c.Numeric, c.NaN, c.String, c.Bool, c.Other}; got != w.classes {
			t.Errorf("%s: %s type classes = %v, recount %v", step, key, got, w.classes)
		}
		s, err := bs.RelPropertyStats(relType, key)
		if err != nil {
			t.Fatalf("%s: RelPropertyStats: %v", step, err)
		}
		if s.Count != w.count {
			t.Errorf("%s: %s Count = %d, recount %d", step, key, s.Count, w.count)
		}
		if !w.numeric || w.nonNumeric {
			continue // min/max checked on numeric-only keys (a mixed key's depend on visit order)
		}
		if !reflect.DeepEqual(numericAsFloat(s.Min), w.min) || !reflect.DeepEqual(numericAsFloat(s.Max), w.max) {
			t.Errorf("%s: %s Min/Max = %v/%v, recount %v/%v", step, key, s.Min, s.Max, w.min, w.max)
		}
	}
}

func numericAsFloat(v any) any {
	switch x := v.(type) {
	case int64:
		return float64(x)
	case float64:
		return x
	}
	return v
}

// BenchmarkRelPlannerStatsWritePath is the write-path cost of the relationship
// planner statistics (backlog 53) per door: put, replace, delete, with the
// statistics on. Run: go test -run '^$' -bench BenchmarkRelPlannerStatsWritePath
// -benchmem ./pkg/graph/store/badger/
func BenchmarkRelPlannerStatsWritePath(b *testing.B) {
	a, z := types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))
	rel := func(i int, props map[string]any) *types.Relationship {
		r := types.NewRelationship(types.RelID(snowflake.ID(int64(1000+i))), 3, a, z)
		for k, v := range props {
			if err := r.SetProperty(k, v); err != nil {
				b.Fatal(err)
			}
		}
		return r
	}
	open := func(b *testing.B) *Store {
		bs, err := New(Config{InMemory: true, FlushInterval: 1<<63 - 1})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = bs.Close() })
		for _, id := range []types.NodeID{a, z} {
			if err := bs.PutNode(types.NewNode(id, 1, nil)); err != nil {
				b.Fatal(err)
			}
		}
		return bs
	}
	b.Run("put", func(b *testing.B) {
		bs := open(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := bs.PutRelationship(rel(i, storeRelStatsProps(i))); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("replace", func(b *testing.B) {
		bs := open(b)
		if err := bs.PutRelationship(rel(0, storeRelStatsProps(0))); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := bs.ReplaceRelationship(rel(0, storeRelStatsProps(i+1))); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("delete", func(b *testing.B) {
		bs := open(b)
		for i := 0; i < b.N; i++ {
			if err := bs.PutRelationship(rel(i, storeRelStatsProps(i))); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := bs.DeleteRelationship(types.RelID(snowflake.ID(int64(1000 + i)))); err != nil {
				b.Fatal(err)
			}
		}
	})
}
