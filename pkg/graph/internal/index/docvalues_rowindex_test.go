package index

import (
	"cmp"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The hashed lookup finds exactly the members a binary search over the sorted
// vector finds, at the same ordinal, and nothing else: below and above the
// table threshold, for dense runs of IDs (consecutive snowflakes) and sparse
// ones, for probes between, below and above the members, for both entity kinds.
func TestRowIndexAgreesWithBinarySearch(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	for _, n := range []int{1, 2, rowIndexMinRows - 1, rowIndexMinRows, rowIndexMinRows + 1, 1000, 20000} {
		for _, dense := range []bool{true, false} {
			raw := make([]int64, n)
			next := int64(1 << 40)
			for i := range raw {
				step := int64(1)
				if !dense {
					step = 1 + rng.Int64N(1<<20)
				}
				next += step
				raw[i] = next
			}
			rng.Shuffle(len(raw), func(i, j int) { raw[i], raw[j] = raw[j], raw[i] })
			nodeIDs := make([]types.NodeID, n)
			relIDs := make([]types.RelID, n)
			for i, v := range raw {
				nodeIDs[i] = types.NodeID(snowflake.ID(v))
				relIDs[i] = types.RelID(snowflake.ID(v))
			}
			nodes := BuildDocValues(1, nodeIDs, nil, func(types.NodeID, string) (any, bool) { return nil, false }, nil)
			rels := BuildDocValues(1, relIDs, nil, func(types.RelID, string) (any, bool) { return nil, false }, nil)
			sorted := slices.Clone(raw)
			slices.Sort(sorted)
			probes := append(slices.Clone(raw), sorted[0]-1, sorted[n-1]+1, 0, -5)
			for i := 0; i+1 < n; i++ {
				if sorted[i+1]-sorted[i] > 1 {
					probes = append(probes, sorted[i]+1)
				}
			}
			for _, p := range probes {
				wantOrd, wantOK := slices.BinarySearch(sorted, p)
				if !wantOK {
					wantOrd = -1
				}
				gotOrd, gotOK := nodes.lookup(types.NodeID(snowflake.ID(p)))
				if gotOK != wantOK || (wantOK && gotOrd != wantOrd) {
					t.Fatalf("n=%d dense=%v node probe %d: (%d,%v), want (%d,%v)", n, dense, p, gotOrd, gotOK, wantOrd, wantOK)
				}
				gotOrd, gotOK = rels.lookup(types.RelID(snowflake.ID(p)))
				if gotOK != wantOK || (wantOK && gotOrd != wantOrd) {
					t.Fatalf("n=%d dense=%v rel probe %d: (%d,%v), want (%d,%v)", n, dense, p, gotOrd, gotOK, wantOrd, wantOK)
				}
			}
			if n >= rowIndexMinRows && uint64(len(nodes.rows.slots)) < uint64(2*n) {
				t.Fatalf("n=%d: %d slots, want at least 2n", n, len(nodes.rows.slots))
			}
			if n < rowIndexMinRows && nodes.rows.slots != nil {
				t.Fatalf("n=%d: a table was built below the threshold", n)
			}
		}
	}
}

// The first lookups race to build the table; every goroutine sees the same
// answers (run under -race).
func TestRowIndexConcurrentFirstLookups(t *testing.T) {
	const n = 5000
	ids := make([]types.NodeID, n)
	for i := range ids {
		ids[i] = types.NodeID(snowflake.ID(int64(1000 + 3*i)))
	}
	vals := map[types.NodeID]map[string]any{}
	for i, id := range ids {
		vals[id] = map[string]any{"v": int64(i)}
	}
	l := BuildLabelDocValues(1, ids, []string{"v"}, getter(vals), nil)
	snap, ok := l.NewPointSnapshot([]string{"v"})
	if !ok {
		t.Fatal("no snapshot")
	}
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, p := make([]any, 1), make([]bool, 1)
			for i := w; i < n; i += 3 {
				if !snap.Row(ids[i], v, p) || !p[0] || v[0] != int64(i) {
					t.Errorf("row %d: %v %v", i, v[0], p[0])
					return
				}
				if snap.Row(types.NodeID(snowflake.ID(int64(1001+3*i))), v, p) {
					t.Errorf("a non-member next to row %d was found", i)
					return
				}
			}
		}()
	}
	wg.Wait()
	if !slices.IsSortedFunc(l.IDs(), func(a, b types.NodeID) int { return cmp.Compare(a, b) }) {
		t.Fatal("the vector lost its order")
	}
}
