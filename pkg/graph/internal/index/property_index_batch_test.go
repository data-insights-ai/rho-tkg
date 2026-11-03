package index

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestAddNodesBatchParsesEachValueOnce (BATCHIDX, ai-soc 3 Nov: property-index
// inserts 106 ms of a 219 ms cut, addOrdered 34 ms): a batch of 1,000 new
// nodes holding one value, added to an index of 1,000,000 entries of it,
// parses the value's numeric sort key once and inserts no key one by one:
// the IDs ascend past the posting list's last, so they are appended. Before,
// each node took its own AddKey: 1,000 parses and an insert per ID into the
// posting list and the numeric bucket.
func TestAddNodesBatchParsesEachValueOnce(t *testing.T) {
	const existing, batch = 1_000_000, 1_000
	for _, tc := range []struct {
		name  string
		value any
		idx   func() *PropertyIndex
	}{
		{"number", int64(7), NewPropertyIndex},
		{"number/rangeCounts", int64(7), func() *PropertyIndex { return NewPropertyIndexWith(true, false) }},
		{"string", "seven", NewPropertyIndex},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pi := tc.idx()
			vk := PropertyValueKey(tc.value)
			for id := snowflake.ID(1); id <= existing; id++ {
				pi.AddKey(id, vk)
			}
			nodes := make([]*types.Node, batch)
			ids := make([]snowflake.ID, batch)
			for i := range nodes {
				ids[i] = snowflake.ID(existing + 1 + i)
				nodes[i] = types.NewNode(types.NodeID(ids[i]), 10, nil)
				if err := nodes[i].SetProperty("v", tc.value); err != nil {
					t.Fatal(err)
				}
			}
			indexes := map[PropertyIndexKey]*PropertyIndex{{LabelToken: 10, PropertyKey: "v"}: pi}
			hooks := &hookCounts{}
			testHooks = hooks
			AddNodesToPropertyIndexes(indexes, nodes, ids)
			testHooks = nil
			if hooks.numericParse != 1 || hooks.chunkInsert != 0 {
				t.Errorf("%d nodes of one value: %d sort-key parses, %d one-by-one inserts; want 1, 0",
					batch, hooks.numericParse, hooks.chunkInsert)
			}
			if got := pi.Entries[vk].Len(); got != existing+batch {
				t.Fatalf("posting list holds %d, want %d", got, existing+batch)
			}
		})
	}
}

// TestAddNodesBatchEqualsSequentialAdds (BATCHIDX): random batches leave
// every index kind (plain, range counts, Elements, each also mid-build) in the
// state n sequential AddNodeToPropertyIndexes calls leave: the same entries,
// ordered views, counts, imprecision count, Mutated set, and the same answers
// to prefix, range and cardinality queries. Batches mix repeated and distinct
// strings, numbers of several types sharing a sort key (i:1, f64:1), integers
// past 2^53, NaN, lists with repeated elements, nodes without the label or
// the property, IDs ascending past the index and IDs below it.
func TestAddNodesBatchEqualsSequentialAdds(t *testing.T) {
	t.Parallel()
	for seed := uint64(1); seed <= 40; seed++ {
		r := rand.New(rand.NewPCG(seed, 0x5eed))
		mk := func() map[PropertyIndexKey]*PropertyIndex {
			m := map[PropertyIndexKey]*PropertyIndex{
				{LabelToken: 10, PropertyKey: "v"}:  NewPropertyIndex(),
				{LabelToken: 10, PropertyKey: "c"}:  NewPropertyIndexWith(true, false),
				{LabelToken: 10, PropertyKey: "l"}:  NewPropertyIndexWith(false, true),
				{LabelToken: 11, PropertyKey: "v"}:  NewPropertyIndex(),
				{LabelToken: 11, PropertyKey: "c"}:  NewPropertyIndexWith(true, false),
				{LabelToken: 11, PropertyKey: "l"}:  NewPropertyIndexWith(false, true),
				{LabelToken: 12, PropertyKey: "v"}:  NewPropertyIndex(),
				{LabelToken: 12, PropertyKey: "nx"}: NewPropertyIndex(),
			}
			for k, pi := range m {
				if k.LabelToken == 11 { // mid-build: the write path tracks what it touched
					pi.Mutated = make(map[snowflake.ID]struct{})
				}
			}
			return m
		}
		seq, bat := mk(), mk()
		used := make(map[snowflake.ID]bool)
		var maxID snowflake.ID
		nextIDs := func(n int) []snowflake.ID {
			ids := make([]snowflake.ID, 0, n)
			mode := r.IntN(4)
			for len(ids) < n {
				var id snowflake.ID
				switch {
				case mode < 2: // ascending past every ID so far (the snowflake write)
					maxID += snowflake.ID(1 + r.IntN(3))
					id = maxID
				default: // anywhere, below the index's IDs too
					id = snowflake.ID(1 + r.IntN(int(maxID)+n*4))
				}
				if used[id] {
					continue
				}
				used[id] = true
				maxID = max(maxID, id)
				ids = append(ids, id)
			}
			if mode == 1 { // ascending, handed over out of order
				r.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
			}
			return ids
		}
		for round := 0; round < 8; round++ {
			n := 1 + r.IntN(300)
			if round == 0 {
				n = 1 + r.IntN(1500) // past a chunk of 512
			}
			ids := nextIDs(n)
			nodes := make([]*types.Node, n)
			for i, id := range ids {
				nodes[i] = randomBatchNode(t, r, id)
			}
			for i, nd := range nodes {
				AddNodeToPropertyIndexes(seq, nd, ids[i])
			}
			AddNodesToPropertyIndexes(bat, nodes, ids)
			for key, want := range seq {
				got := bat[key]
				if ws, gs := propertyIndexState(want), propertyIndexState(got); ws != gs {
					t.Fatalf("seed %d round %d index %v: batch state differs from sequential\nsequential:\n%s\nbatch:\n%s", seed, round, key, ws, gs)
				}
				if err := checkChunks(&got.strKeys); err != nil {
					t.Fatalf("seed %d round %d index %v strKeys: %v", seed, round, key, err)
				}
				if err := checkChunks(&got.numKeys); err != nil {
					t.Fatalf("seed %d round %d index %v numKeys: %v", seed, round, key, err)
				}
				for vk, p := range got.Entries {
					if err := checkChunks(&p.s); err != nil {
						t.Fatalf("seed %d round %d index %v %q: %v", seed, round, key, vk, err)
					}
				}
			}
			if round == 5 { // a build ends: both continue as served indexes
				for k := range seq {
					seq[k].Mutated, bat[k].Mutated = nil, nil
				}
			}
		}
	}
}

// randomBatchNode is a node of label 10, 11, 12 or none, with random values
// under v, c (scalars), l (lists) and nx (absent most of the time).
func randomBatchNode(t *testing.T, r *rand.Rand, id snowflake.ID) *types.Node {
	t.Helper()
	label := uint16(9 + r.IntN(4)) // 9: no index
	n := types.NewNode(types.NodeID(id), label, nil)
	if r.IntN(5) == 0 {
		n = types.NewNode(types.NodeID(id), label, []uint16{uint16(10 + r.IntN(3))})
	}
	scalar := func() any {
		switch r.IntN(9) {
		case 0:
			return "s" + fmt.Sprint(r.IntN(4)) // repeated strings
		case 1:
			return fmt.Sprintf("u%06d", r.IntN(1_000_000)) // distinct strings
		case 2:
			return int64(r.IntN(5)) // repeated integers
		case 3:
			return float64(r.IntN(5)) // the same sort keys as type f64
		case 4:
			return r.Float64() * 1000 // distinct floats
		case 5:
			return int64(1)<<53 + int64(1+r.IntN(3)) // past 2^53: imprecise
		case 6:
			return math.NaN()
		case 7:
			return int32(r.IntN(5) - 2)
		default:
			return r.IntN(2) == 0
		}
	}
	set := func(k string, v any) {
		_ = n.SetProperty(k, v) // a value the node refuses is simply absent
	}
	if r.IntN(6) != 0 {
		set("v", scalar())
	}
	if r.IntN(6) != 0 {
		set("c", scalar())
	}
	if r.IntN(5) != 0 {
		list := make([]any, r.IntN(5))
		for i := range list {
			list[i] = scalar()
		}
		set("l", list)
	} else if r.IntN(2) == 0 {
		set("l", scalar())
	}
	if r.IntN(10) == 0 {
		set("nx", scalar())
	}
	return n
}

// propertyIndexState renders everything an index holds or answers.
func propertyIndexState(pi *PropertyIndex) string {
	var b strings.Builder
	vks := make([]string, 0, len(pi.Entries))
	for vk := range pi.Entries {
		vks = append(vks, vk)
	}
	slices.Sort(vks)
	for _, vk := range vks {
		fmt.Fprintf(&b, "E %q n=%d %v\n", vk, pi.Entries[vk].Len(), slices.Collect(pi.Entries[vk].All()))
	}
	fmt.Fprintf(&b, "strKeys n=%d %q\n", pi.strKeys.n, chunkKeys(&pi.strKeys))
	fmt.Fprintf(&b, "numKeys n=%d %v\n", pi.numKeys.n, chunkKeys(&pi.numKeys))
	if pi.numKeys.chunkCounts != nil {
		var cnts []int64
		for _, k := range chunkKeys(&pi.numKeys) {
			cnts = append(cnts, pi.numKeys.countRange(k, k, true, true))
		}
		fmt.Fprintf(&b, "numCounts %v total=%d\n", cnts, pi.numKeys.countRange(math.Inf(-1), math.Inf(1), true, true))
	}
	nks := make([]float64, 0, len(pi.numBuckets))
	for k := range pi.numBuckets {
		nks = append(nks, k)
	}
	slices.Sort(nks)
	for _, k := range nks {
		fmt.Fprintf(&b, "B %v %v\n", k, slices.Collect(pi.numBuckets[k].All()))
	}
	fmt.Fprintf(&b, "imprecise=%d\n", pi.numImpreciseCount)
	if pi.Mutated != nil {
		m := make([]snowflake.ID, 0, len(pi.Mutated))
		for id := range pi.Mutated {
			m = append(m, id)
		}
		slices.Sort(m)
		fmt.Fprintf(&b, "Mutated %v\n", m)
	}
	for _, p := range []string{"", "s", "u0", "u5"} {
		ids, ok := pi.PrefixNodeIDs(p)
		slices.Sort(ids)
		fmt.Fprintf(&b, "prefix %q %v %v\n", p, ok, ids)
	}
	for _, rg := range [][2]float64{{math.Inf(-1), math.Inf(1)}, {0, 2}, {-1, 1}, {1, 500}, {1 << 53, 1<<53 + 4}} {
		ids, ok := pi.RangeNodeIDs(rg[0], rg[1], true, true)
		slices.Sort(ids)
		c, cok := pi.RangeCardinality(rg[0], rg[1], true, false)
		fmt.Fprintf(&b, "range %v %v %v card %d %v\n", rg, ok, ids, c, cok)
	}
	return b.String()
}

func chunkKeys[T cmpOrdered](o *sortedChunks[T]) []T {
	var out []T
	for _, c := range o.chunks {
		out = append(out, c...)
	}
	return out
}

type cmpOrdered interface {
	~int | ~int64 | ~float64 | ~string
}

// checkChunks verifies a sortedChunks' invariants: keys strictly ascending
// across chunks, no empty chunk, none above the split cap, n the key count.
func checkChunks[T cmpOrdered](o *sortedChunks[T]) error {
	total := 0
	var prev T
	for i, c := range o.chunks {
		if len(c) == 0 || len(c) > 2*orderedKeyChunk {
			return fmt.Errorf("chunk %d holds %d keys", i, len(c))
		}
		for j, k := range c {
			if (i > 0 || j > 0) && !(prev < k) {
				return fmt.Errorf("chunk %d key %d: %v after %v", i, j, k, prev)
			}
			prev = k
		}
		total += len(c)
	}
	if total != o.n {
		return fmt.Errorf("n=%d, %d keys held", o.n, total)
	}
	if o.chunkCounts != nil && (len(o.cnt) != len(o.chunks) || len(o.tot) != len(o.chunks)) {
		return fmt.Errorf("counts cover %d/%d chunks of %d", len(o.cnt), len(o.tot), len(o.chunks))
	}
	return nil
}
