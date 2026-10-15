package memory

import (
	"fmt"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkPostingsWalk (backlog 72): one full walk over a value key of n
// members in pages of 1,024 through the composite door, as sigma's ListDerived
// does. ns/op is one whole walk.
func BenchmarkPostingsWalk(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("composite/n=%d", n), func(b *testing.B) {
			ms, _, _ := fillOneKey(b, n)
			values := map[string]any{"a": int64(1), "b": "x"}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var after types.EntityID
				seen := 0
				for {
					page, err := ms.NodesByLabelAndProperties(1, values, QueryOpts{After: after, Limit: 1024})
					if err != nil {
						b.Fatal(err)
					}
					if len(page) == 0 {
						break
					}
					seen += len(page)
					after = types.EntityID(page[len(page)-1].ID())
				}
				if seen != n {
					b.Fatalf("walk saw %d of %d", seen, n)
				}
			}
		})
	}
}

// BenchmarkPostingsWrite (backlog 72): the cost of one row write that moves a
// node between two value keys of the property and composite indexes, next to
// a key of 1,000,000 members. The moved node's ID lies inside the big key's ID
// range, so the sorted insert lands mid-key.
func BenchmarkPostingsWrite(b *testing.B) {
	const n = 1_000_000
	ms, _, _ := fillOneKey(b, n)
	id := types.NodeID(snowflake.ID(int64(n/2)<<15 | 2))
	first := types.NewNode(id, 1, nil)
	_ = first.SetProperty("a", int64(2))
	_ = first.SetProperty("b", "x")
	if err := ms.PutNode(first); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		node := types.NewNode(id, 1, nil)
		if i%2 == 0 {
			_ = node.SetProperty("a", int64(1))
		} else {
			_ = node.SetProperty("a", int64(2))
		}
		_ = node.SetProperty("b", "x")
		if err := ms.ReplaceNode(node); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPostingsCreateIndex (backlog 72): declaring a property index and a
// composite index over 1,000,000 existing nodes of one value key (the 3-phase
// build: snapshot, backfill, merge). ns/op is one create; the drop is untimed.
func BenchmarkPostingsCreateIndex(b *testing.B) {
	const n = 1_000_000
	ms := New()
	for i := 0; i < n; i++ {
		node := types.NewNode(types.NodeID(snowflake.ID(int64(i+1)<<15)), 1, nil)
		_ = node.SetProperty("a", int64(1))
		_ = node.SetProperty("b", "x")
		if err := ms.PutNode(node); err != nil {
			b.Fatal(err)
		}
	}
	b.Run("property", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := ms.CreatePropertyIndex(1, "a"); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			if err := ms.DropPropertyIndex(1, "a"); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
	})
	b.Run("composite", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := ms.CreateCompositePropertyIndex(1, []string{"a", "b"}); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			if err := ms.DropCompositePropertyIndex(1, []string{"a", "b"}); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
	})
}
