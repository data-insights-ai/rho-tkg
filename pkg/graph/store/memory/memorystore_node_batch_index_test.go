package memory

import (
	"fmt"
	"math"
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
// badgerstore_node_batch_index_test.go). Indexes on two labels, nodes with
// one or both, one index still building (Mutated), values with NaN and
// integers past 2^53, IDs ascending and below the others, handed out of order.
func TestPutNodesBatchIndexStateEqualsPutNode(t *testing.T) {
	t.Parallel()
	mk := func() *Store {
		ms := New()
		for _, label := range []uint16{5, 6} {
			for _, ix := range []struct {
				key  string
				opts storecontract.PropertyIndexOptions
			}{{"v", storecontract.PropertyIndexOptions{}}, {"c", storecontract.PropertyIndexOptions{RangeCounts: true}}, {"l", storecontract.PropertyIndexOptions{Elements: true}}} {
				if err := ms.CreatePropertyIndexWithOptions(label, ix.key, ix.opts); err != nil {
					t.Fatal(err)
				}
			}
		}
		ms.mu.Lock()
		ms.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: 6, PropertyKey: "c"}].Mutated = make(map[snowflake.ID]struct{})
		ms.mu.Unlock()
		return ms
	}
	for seed := uint64(1); seed <= 5; seed++ {
		seq, bat := mk(), mk()
		r := rand.New(rand.NewPCG(seed, 7))
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
				var extra []uint16
				if r.IntN(3) == 0 {
					extra = []uint16{6}
				}
				nodes[i] = types.NewNode(types.NodeID(snowflake.ID(id)), []uint16{5, 6, 9}[r.IntN(3)], extra)
				val := func() any {
					switch r.IntN(6) {
					case 0:
						return fmt.Sprint("s", r.IntN(3))
					case 1:
						return int64(r.IntN(4))
					case 2:
						return float64(r.IntN(4))
					case 3:
						return math.NaN()
					case 4:
						return int64(1)<<53 + int64(1+r.IntN(3)) // imprecise as a float64 sort key
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
			if r.IntN(2) == 0 {
				r.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
			}
			for _, n := range nodes {
				if err := seq.PutNode(n); err != nil {
					t.Fatal(err)
				}
			}
			if err := bat.PutNodesBatch(nodes); err != nil {
				t.Fatal(err)
			}
			if ws, gs := memIdxState(seq), memIdxState(bat); ws != gs {
				t.Fatalf("seed %d round %d: PutNodesBatch index state differs from PutNode\nPutNode:\n%s\nPutNodesBatch:\n%s", seed, round, ws, gs)
			}
		}
	}
}

func memIdxState(ms *Store) string {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	var b strings.Builder
	for _, label := range []uint16{5, 6} {
		for _, key := range []string{"v", "c", "l"} {
			pi := ms.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: label, PropertyKey: key}]
			vks := make([]string, 0, len(pi.Entries))
			for vk := range pi.Entries {
				vks = append(vks, vk)
			}
			slices.Sort(vks)
			for _, vk := range vks {
				fmt.Fprintf(&b, "%d/%s %q %v\n", label, key, vk, slices.Collect(pi.Entries[vk].All()))
			}
			if pi.Mutated != nil {
				m := make([]snowflake.ID, 0, len(pi.Mutated))
				for id := range pi.Mutated {
					m = append(m, id)
				}
				slices.Sort(m)
				fmt.Fprintf(&b, "%d/%s Mutated %v\n", label, key, m)
			}
			ids, ok := pi.PrefixNodeIDs("u1")
			slices.Sort(ids)
			rg, rok := pi.RangeNodeIDs(0.5, 2, true, true)
			slices.Sort(rg)
			c, cok := pi.RangeCardinality(-1, 3, true, true)
			fmt.Fprintf(&b, "%d/%s prefix %v %v range %v %v card %d %v\n", label, key, ok, ids, rok, rg, c, cok)
		}
	}
	return b.String()
}
