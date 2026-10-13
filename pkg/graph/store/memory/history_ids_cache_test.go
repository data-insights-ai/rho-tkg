package memory

import (
	"reflect"
	"runtime"
	"slices"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// cursorPageBytes is the heap allocated per 64-ID page of a history-ID cursor
// walk (after one warm page), averaged over 50 pages from the middle of the set.
func cursorPageBytes(t *testing.T, page func(after int64, limit int) int) float64 {
	t.Helper()
	if got := page(0, 64); got != 64 {
		t.Fatalf("warm page = %d IDs, want 64", got)
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	const pages = 50
	for i := 0; i < pages; i++ {
		if got := page(int64(1000+i*64), 64); got != 64 {
			t.Fatalf("page %d = %d IDs, want 64", i, got)
		}
	}
	runtime.ReadMemStats(&after)
	return float64(after.TotalAlloc-before.TotalAlloc) / pages
}

// A history-ID cursor page costs O(page + log n), not O(n): the bytes
// allocated per 64-ID page at 100,000 history IDs stay within 2x of those at
// 10,000 (copying and sorting every ID per page — the quadratic full walk —
// allocates 10x more). Node and rel doors.
func TestHistoryIDsCursorPageCostIndependentOfStoreSize(t *testing.T) {
	perPage := map[string][2]float64{}
	for i, n := range []int{10_000, 100_000} {
		ms := New()
		seedNodeHistory(t, ms, n)
		seedRelHistory(t, ms, n)
		node := cursorPageBytes(t, func(after int64, limit int) int {
			ids, err := ms.AllNodeHistoryIDsFrom(types.NodeID(snowflake.ID(after)), limit)
			if err != nil {
				t.Fatal(err)
			}
			return len(ids)
		})
		rel := cursorPageBytes(t, func(after int64, limit int) int {
			ids, err := ms.AllRelHistoryIDsFrom(types.RelID(snowflake.ID(after)), limit)
			if err != nil {
				t.Fatal(err)
			}
			return len(ids)
		})
		nb, rb := perPage["node"], perPage["rel"]
		nb[i], rb[i] = node, rel
		perPage["node"], perPage["rel"] = nb, rb
	}
	for door, b := range perPage {
		t.Logf("%s: %.0f B per page at 10 K IDs, %.0f B at 100 K", door, b[0], b[1])
		if b[1] > 2*b[0]+1024 {
			t.Errorf("%s history-ID page allocates %.0f B at 100 K IDs vs %.0f B at 10 K: a page costs O(n)", door, b[1], b[0])
		}
	}
}

// historyKeysOracle is the sorted key set of the history maps, read directly
// (independent of any cursor cache).
func historyKeysOracle(ms *Store) ([]types.NodeID, []types.RelID) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	var nodes []types.NodeID
	for id := range ms.nodeHistory {
		nodes = append(nodes, id)
	}
	var rels []types.RelID
	for id := range ms.relHistory {
		rels = append(rels, id)
	}
	slices.Sort(nodes)
	slices.Sort(rels)
	return nodes, rels
}

// assertHistoryCursorsMatch: the unpaginated doors and a 7-ID paged walk equal
// the map's key set (nil and empty alike).
func assertHistoryCursorsMatch(t *testing.T, ms *Store, step string) {
	t.Helper()
	wantN, wantR := historyKeysOracle(ms)
	gotN, err := ms.AllNodeHistoryIDs()
	if err != nil {
		t.Fatalf("%s: AllNodeHistoryIDs: %v", step, err)
	}
	gotR, err := ms.AllRelHistoryIDs()
	if err != nil {
		t.Fatalf("%s: AllRelHistoryIDs: %v", step, err)
	}
	if len(gotN)+len(wantN) > 0 && !reflect.DeepEqual(gotN, wantN) {
		t.Fatalf("%s: node history IDs %v, want %v", step, gotN, wantN)
	}
	if len(gotR)+len(wantR) > 0 && !reflect.DeepEqual(gotR, wantR) {
		t.Fatalf("%s: rel history IDs %v, want %v", step, gotR, wantR)
	}
	if paged := pageThroughNodeHistory(t, ms, 7); len(paged)+len(wantN) > 0 && !reflect.DeepEqual(paged, wantN) {
		t.Fatalf("%s: paged node walk %v, want %v", step, paged, wantN)
	}
}

// Every change of the history key set after a cached walk reaches the cursor:
// a new ID (also between cached ones), an ID removed by every removal door
// (Truncate keep 0, TrimFrom, Compact keep 0, ExactErase), a remove + add
// pair that keeps the count, Clear, and a version appended to an existing ID
// (no change). The returned page is the caller's: mutating it does not change
// the next answer.
func TestHistoryIDsCursorFollowsKeySetChanges(t *testing.T) {
	ms := New()
	nid := func(i int64) types.NodeID { return types.NodeID(snowflake.ID(i)) }
	rid := func(i int64) types.RelID { return types.RelID(snowflake.ID(i)) }
	putN := func(i int64, v uint32) {
		t.Helper()
		n := types.NewNode(nid(i), 1, nil)
		n.SetVersion(v)
		if err := ms.PutNodeVersion(nid(i), v, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", i, err)
		}
	}
	putR := func(i int64, v uint32) {
		t.Helper()
		r := types.NewRelationship(rid(i), 1, nid(i), nid(i+1))
		r.SetVersion(v)
		if err := ms.PutRelVersion(rid(i), v, r); err != nil {
			t.Fatalf("PutRelVersion(%d): %v", i, err)
		}
	}
	for i := int64(2); i <= 100; i += 2 {
		putN(i, 0)
		putR(i, 0)
	}
	steps := []struct {
		name string
		do   func() error
	}{
		{"new ID between cached ones", func() error { putN(51, 0); putR(51, 0); return nil }},
		{"new ID above the last", func() error { putN(1001, 0); putR(1001, 0); return nil }},
		{"version appended to an existing ID", func() error { putN(10, 1); putR(10, 1); return nil }},
		{"TruncateNodeHistory keep 0", func() error { return ms.TruncateNodeHistory(nid(4), 0) }},
		{"TruncateRelHistory keep 0", func() error { return ms.TruncateRelHistory(rid(4), 0) }},
		{"TrimNodeHistoryFrom 0", func() error { return ms.TrimNodeHistoryFrom(nid(6), 0) }},
		{"TrimRelHistoryFrom 0", func() error { return ms.TrimRelHistoryFrom(rid(6), 0) }},
		{"CompactNodeHistory keep 0", func() error { return ms.CompactNodeHistory(nid(8), 0, nil) }},
		{"CompactRelHistory keep 0", func() error { return ms.CompactRelHistory(rid(8), 0, nil) }},
		{"remove one, add one (same count)", func() error {
			if err := ms.TruncateNodeHistory(nid(12), 0); err != nil {
				return err
			}
			putN(13, 0)
			if err := ms.TruncateRelHistory(rid(12), 0); err != nil {
				return err
			}
			putR(13, 0)
			return nil
		}},
		{"ExactErase of a node with history", func() error {
			n := types.NewNode(nid(2001), 1, nil)
			if err := ms.PutNode(n); err != nil {
				return err
			}
			putN(2001, 0)
			assertHistoryCursorsMatch(t, ms, "ExactErase setup") // cache the set holding 2001
			_, err := ms.ExactErase(storecontract.ExactErasureRequest{NodeIDs: []types.NodeID{nid(2001)}, Bounds: memoryExactErasureBounds})
			return err
		}},
		{"Clear", ms.Clear},
		{"new ID after Clear", func() error { putN(7, 0); return nil }},
	}
	assertHistoryCursorsMatch(t, ms, "seeded")
	for _, s := range steps {
		assertHistoryCursorsMatch(t, ms, "before "+s.name) // the cursor caches the current set
		if err := s.do(); err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		assertHistoryCursorsMatch(t, ms, s.name)
	}

	page, err := ms.AllNodeHistoryIDsFrom(0, 0)
	if err != nil || len(page) != 1 {
		t.Fatalf("final page = %v, %v; want one ID", page, err)
	}
	page[0] = nid(999_999)
	assertHistoryCursorsMatch(t, ms, "after the caller mutated a returned page")
}

// Concurrent cursor walks while a writer adds and removes history IDs: every
// walk is strictly ascending (run under -race for the cache's publication).
func TestHistoryIDsCursorConcurrentReadersAndWriter(t *testing.T) {
	ms := New()
	seedNodeHistory(t, ms, 500)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				var cursor types.NodeID
				for {
					page, err := ms.AllNodeHistoryIDsFrom(cursor, 37)
					if err != nil {
						t.Error(err)
						return
					}
					for _, id := range page {
						if id <= cursor {
							t.Errorf("walk not ascending: %d after %d", id, cursor)
							return
						}
						cursor = id
					}
					if len(page) < 37 {
						break
					}
				}
			}
		}()
	}
	for i := int64(501); i <= 1500; i++ {
		id := types.NodeID(snowflake.ID(i))
		if err := ms.PutNodeVersion(id, 0, types.NewNode(id, 1, nil)); err != nil {
			t.Fatal(err)
		}
		if i%3 == 0 {
			if err := ms.TruncateNodeHistory(types.NodeID(snowflake.ID(i-250)), 0); err != nil {
				t.Fatal(err)
			}
		}
	}
	close(stop)
	wg.Wait()
	assertHistoryCursorsMatch(t, ms, "after the concurrent writer")
}
