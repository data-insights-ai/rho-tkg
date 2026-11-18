package badger

import (
	"fmt"
	"testing"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestScanLargerThanCache_CorrectAndCachePreserving pins the scan-resistance
// behavior (perf/scan-cache-config): label scans whose cardinality exceeds
// the entity-cache capacity must (a) return every row, decoded correctly, on
// repeated passes — the no-fill path reads badger directly and must not
// depend on cache state; and (b) leave the cache useful for point reads —
// the pre-change fill-on-miss behavior evicted the whole cache once per
// scan pass (sequential-LRU pathology: 100% steady-state miss rate,
// measured as a 12x per-row regression just past capacity).
func TestScanLargerThanCache_CorrectAndCachePreserving(t *testing.T) {
	t.Parallel()
	bs, err := New(Config{InMemory: true, CacheCapacity: 8})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { bs.Close() })

	const n = 50
	const label = uint16(1)
	for i := 1; i <= n; i++ {
		node := types.NewNode(types.NodeID(i), label, nil)
		if err := node.SetProperty("k", fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("prop %d: %v", i, err)
		}
		if err := bs.PutNode(node); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	// Flush so entries are clean (evictable) — the regime the cliff lives in.
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Warm the cache with point reads of the first nodes; record which of
	// them it holds. A scan that filled the cache on miss would evict them.
	bs.nodeCache.ResetForTest()
	for i := 1; i <= 4; i++ {
		if _, err := bs.GetNode(types.NodeID(i)); err != nil {
			t.Fatalf("warm get %d: %v", i, err)
		}
	}
	var warm []int64
	for i := 1; i <= n; i++ {
		if _, st := bs.nodeCache.Peek(types.NodeID(i).SnowflakeID()); st == indexpkg.CacheHit {
			warm = append(warm, int64(i))
		}
	}
	if len(warm) == 0 {
		t.Fatal("warm point reads left nothing in the cache")
	}
	warmLen := bs.nodeCache.Len()

	// Two full passes: the second exercises the steady state where the
	// cache holds only a fraction of the label's nodes.
	for pass := 1; pass <= 2; pass++ {
		nodes, err := bs.NodesByLabel(label, QueryOpts{})
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if len(nodes) != n {
			t.Fatalf("pass %d: %d nodes, want %d", pass, len(nodes), n)
		}
		for _, nd := range nodes {
			want := fmt.Sprintf("v%d", int64(nd.ID()))
			if got, ok := nd.GetProperty("k"); !ok || got != want {
				t.Fatalf("pass %d node %d: property k = %v (%v), want %s", pass, nd.ID(), got, ok, want)
			}
			if !nd.IsFrozen() {
				t.Fatalf("pass %d node %d: scan row not frozen", pass, nd.ID())
			}
		}
	}

	// The scans left the cache as the point reads made it: same size, and the
	// warm nodes still answer from it (no miss).
	if got := bs.nodeCache.Len(); got != warmLen {
		t.Fatalf("cache Len after scans = %d, want %d (the scans filled the cache)", got, warmLen)
	}
	misses := bs.nodeCache.Misses()
	for _, id := range warm {
		if _, err := bs.GetNode(types.NodeID(id)); err != nil {
			t.Fatalf("get warm %d after scans: %v", id, err)
		}
	}
	if d := bs.nodeCache.Misses() - misses; d != 0 {
		t.Fatalf("point reads of warm nodes %v after scans missed %d times, want 0", warm, d)
	}

	// Point reads after the scans must still return correct rows — the
	// scans must not have corrupted or poisoned cache state.
	for i := 1; i <= n; i++ {
		nd, err := bs.GetNode(types.NodeID(i))
		if err != nil {
			t.Fatalf("get %d after scans: %v", i, err)
		}
		if got, ok := nd.GetProperty("k"); !ok || got != fmt.Sprintf("v%d", i) {
			t.Fatalf("get %d: property k = %v (%v)", i, got, ok)
		}
	}
}
