package badger

import (
	"math"
	"reflect"
	"runtime"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRelPlannerStatsMemoBounded is backlog 53 at the store seam (the graph
// door cases live in pkg/graph under the same name). "heap" measures the heap
// the relationship planner statistics retain — stats on minus stats off, same
// writes, nothing flushed so the two stores differ only by the statistics — at
// 10 K and 100 K relationships: the growth must be a constant, not an amount
// per relationship. The door cases reach the store-only delete seams (batch
// delete, the tiered split-write legs, purge by descriptor, foreign incoming
// stubs, adjacent purge, cascade); after each step every counter equals a
// full recount of the current relationships.
func TestRelPlannerStatsMemoBounded(t *testing.T) {
	t.Run("heap", func(t *testing.T) {
		small := storeRelStatsRetainedHeap(t, 10_000)
		large := storeRelStatsRetainedHeap(t, 100_000)
		perRel := float64(large-small) / 90_000
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
