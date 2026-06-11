package badger

import (
	"fmt"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestCacheBudgetBytes_BoundsCacheUnderMixedPayloads pins the end-to-end
// byte-budget behavior (enterprise-scale ceiling 4): with
// Config.CacheBudgetBytes set, point reads over entities far exceeding the
// budget must (a) stay correct, and (b) leave the entity cache's accounted
// bytes within the budget once entries are clean — a count capacity alone
// cannot do this when payloads vary 100B-64KB.
func TestCacheBudgetBytes_BoundsCacheUnderMixedPayloads(t *testing.T) {
	t.Parallel()
	const budget = int64(64 * 1024)
	bs, err := New(Config{InMemory: true, CacheBudgetBytes: budget})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { bs.Close() })

	// Mixed payloads: alternating ~100B and ~16KB nodes; 40 nodes ≈ 320KB
	// resident — 5x the budget.
	const n = 40
	const label = uint16(1)
	big := strings.Repeat("x", 16*1024)
	for i := 1; i <= n; i++ {
		node := types.NewNode(types.NodeID(i), label, nil)
		val := fmt.Sprintf("v%d", i)
		if i%2 == 0 {
			val = big
		}
		if err := node.SetProperty("k", val); err != nil {
			t.Fatalf("prop %d: %v", i, err)
		}
		if err := bs.PutNode(node); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Point-read every node twice (the filling path) — correctness must
	// not depend on whether the entry survived the budget.
	for pass := 1; pass <= 2; pass++ {
		for i := 1; i <= n; i++ {
			nd, err := bs.GetNode(types.NodeID(i))
			if err != nil {
				t.Fatalf("pass %d get %d: %v", pass, i, err)
			}
			want := fmt.Sprintf("v%d", i)
			if i%2 == 0 {
				want = big
			}
			if got, ok := nd.GetProperty("k"); !ok || got != want {
				t.Fatalf("pass %d node %d: wrong property (ok=%v)", pass, i, ok)
			}
		}
		if got := bs.nodeCache.Bytes(); got > budget {
			t.Fatalf("pass %d: cache holds %dB, budget %dB", pass, got, budget)
		}
	}
	// The budget must have actually evicted — all 40 nodes cannot fit.
	if kept := bs.nodeCache.Len(); kept >= n {
		t.Fatalf("cache kept all %d entries despite budget", kept)
	}
}

// TestCacheBudgetBytes_DirtyWritesExceedThenFlushSheds pins the soft-limit
// contract at the store level: unflushed writes are never evicted (the
// budget may be exceeded under write pressure), and the flush cycle brings
// the cache back within budget.
func TestCacheBudgetBytes_DirtyWritesExceedThenFlushSheds(t *testing.T) {
	t.Parallel()
	const budget = int64(32 * 1024)
	bs, err := New(Config{InMemory: true, CacheBudgetBytes: budget})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { bs.Close() })

	big := strings.Repeat("x", 8*1024)
	const n = 16 // ~128KB dirty — 4x budget
	for i := 1; i <= n; i++ {
		node := types.NewNode(types.NodeID(i), 1, nil)
		if err := node.SetProperty("k", big); err != nil {
			t.Fatalf("prop %d: %v", i, err)
		}
		if err := bs.PutNode(node); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if got := bs.nodeCache.Bytes(); got <= budget {
		t.Fatalf("expected dirty cache over budget, got %dB", got)
	}
	if kept := bs.nodeCache.Len(); kept != n {
		t.Fatalf("dirty entries evicted: %d remain, want %d", kept, n)
	}

	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := bs.nodeCache.Bytes(); got > budget {
		t.Fatalf("post-flush cache still over budget: %dB", got)
	}

	// Every node must still read back correctly after the shed.
	for i := 1; i <= n; i++ {
		nd, err := bs.GetNode(types.NodeID(i))
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if got, ok := nd.GetProperty("k"); !ok || got != big {
			t.Fatalf("node %d: wrong property after shed (ok=%v)", i, ok)
		}
	}
}
