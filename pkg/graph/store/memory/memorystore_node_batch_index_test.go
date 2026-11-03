package memory

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestPutNodesBatchIndexStateEqualsPutNode (BATCHIDX): the memory store's
// batch door, which adds the batch to the property indexes at once, leaves
// them as PutNode of each node does (the badger twin:
// badgerstore_node_batch_index_test.go).
func TestPutNodesBatchIndexStateEqualsPutNode(t *testing.T) {
	t.Parallel()
	mk := func() *Store {
		ms := New()
		for _, ix := range []struct {
			key  string
			opts storecontract.PropertyIndexOptions
		}{{"v", storecontract.PropertyIndexOptions{}}, {"c", storecontract.PropertyIndexOptions{RangeCounts: true}}, {"l", storecontract.PropertyIndexOptions{Elements: true}}} {
			if err := ms.CreatePropertyIndexWithOptions(5, ix.key, ix.opts); err != nil {
				t.Fatal(err)
			}
		}
		return ms
	}
	seq, bat := mk(), mk()
	r := rand.New(rand.NewPCG(7, 7))
	next := int64(1 << 24)
	for round := 0; round < 6; round++ {
		nodes := make([]*types.Node, 1+r.IntN(600))
		for i := range nodes {
			next += int64(1 + r.IntN(2))
			id := next
			if r.IntN(5) == 0 {
				id = int64(1 + r.IntN(1<<19)) // below the others; unique by round
				id = id*8 + int64(round)
			}
			nodes[i] = types.NewNode(types.NodeID(snowflake.ID(id)), []uint16{5, 6}[r.IntN(2)], nil)
			val := func() any {
				switch r.IntN(4) {
				case 0:
					return fmt.Sprint("s", r.IntN(3))
				case 1:
					return int64(r.IntN(4))
				case 2:
					return float64(r.IntN(4))
				default:
					return fmt.Sprintf("u%06d", r.IntN(1_000_000))
				}
			}
			_ = nodes[i].SetProperty("v", val())
			_ = nodes[i].SetProperty("c", val())
			_ = nodes[i].SetProperty("l", []any{val(), val()})
		}
		slices.SortFunc(nodes, func(a, b *types.Node) int { return int(a.ID() - b.ID()) })
		nodes = slices.CompactFunc(nodes, func(a, b *types.Node) bool { return a.ID() == b.ID() })
		for _, n := range nodes {
			if err := seq.PutNode(n); err != nil {
				t.Fatal(err)
			}
		}
		if err := bat.PutNodesBatch(nodes); err != nil {
			t.Fatal(err)
		}
		if ws, gs := memIdxState(seq), memIdxState(bat); ws != gs {
			t.Fatalf("round %d: PutNodesBatch index state differs from PutNode\nPutNode:\n%s\nPutNodesBatch:\n%s", round, ws, gs)
		}
	}
}

func memIdxState(ms *Store) string {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	var b strings.Builder
	for _, key := range []string{"v", "c", "l"} {
		pi := ms.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: 5, PropertyKey: key}]
		vks := make([]string, 0, len(pi.Entries))
		for vk := range pi.Entries {
			vks = append(vks, vk)
		}
		slices.Sort(vks)
		for _, vk := range vks {
			fmt.Fprintf(&b, "%s %q %v\n", key, vk, slices.Collect(pi.Entries[vk].All()))
		}
		ids, ok := pi.PrefixNodeIDs("u1")
		slices.Sort(ids)
		rg, rok := pi.RangeNodeIDs(0.5, 2, true, true)
		slices.Sort(rg)
		c, cok := pi.RangeCardinality(-1, 3, true, true)
		fmt.Fprintf(&b, "%s prefix %v %v range %v %v card %d %v\n", key, ok, ids, rok, rg, c, cok)
	}
	return b.String()
}
