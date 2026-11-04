package badger

import (
	"errors"
	"fmt"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// countScanReads installs scanReadHook for the test and returns its counts,
// indexed by scanRead.
func countScanReads(t *testing.T) *[4]int {
	t.Helper()
	var c [4]int
	scanReadHook = func(r scanRead) { c[r]++ }
	t.Cleanup(func() { scanReadHook = nil })
	return &c
}

func lendCostNodeIDs(ids ...int64) []types.NodeID {
	out := make([]types.NodeID, len(ids))
	for i, id := range ids {
		out[i] = types.NodeID(snowflake.ID(id))
	}
	return out
}

func lendCostRange(from, to int64) []int64 {
	var ids []int64
	for id := from; id <= to; id++ {
		ids = append(ids, id)
	}
	return ids
}

// TestLendSortedSparseMissesGetNotSeek (LENDCOST, ai-soc 4 Nov: LendByIDs
// 115 -> 629 s on v4.103.0): a lent row the cache missed is read by a point
// Get unless the next ID lies within seekSteps of it or, with lendProbeMin
// IDs left, a probe tries a run; only a run creates and seeks the iterator,
// which then steps, and a target it cannot step to costs at most seekSteps
// Next calls before the run ends and the probes back off.
func TestLendSortedSparseMissesGetNotSeek(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)
	const n = 4096
	for i := int64(1); i <= n; i++ {
		putTestNode(t, bs, i, 1, nil)
		putTestRel(t, bs, 1_000_000+i, 3, 1, i)
	}
	// Spread rows: adjacent keys 1,000 IDs apart (rows of different
	// milliseconds).
	spread := func(k int64) int64 { return 1_000_000 + 1000*k }
	spreadIDs := func(from, to, every int64) []int64 {
		var ids []int64
		for k := from; k < to; k += every {
			ids = append(ids, spread(k))
		}
		return ids
	}
	for k := int64(0); k < 600; k++ {
		putTestNode(t, bs, spread(k), 1, nil)
		putTestRel(t, bs, 1_000_000+spread(k), 3, 1, spread(k))
	}
	sparse40 := make([]int64, 40)
	for i := range sparse40 {
		sparse40[i] = 1 + 100*int64(i)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	type want struct{ iter, seek, maxNext, get int }
	cases := []struct {
		name string
		ids  []int64
		want want
	}{
		{"sparse", []int64{1, 1001, 2001, 3001}, want{0, 0, 0, 4}},
		{"dense", lendCostRange(1, 64), want{1, 1, 63, 0}},
		{"dense then sparse", append(lendCostRange(1, 8), 2000, 3000, 4000), want{1, 1, 7 + seekSteps, 3}},
		{"sparse then dense", append([]int64{100}, lendCostRange(2000, 2007)...), want{1, 1, 7, 1}},
		{"two dense runs", append(lendCostRange(10, 13), lendCostRange(3000, 3003)...), want{1, 2, 6 + seekSteps, 0}},
		{"sparse, probed once", sparse40, want{1, 1, seekSteps, 39}},
		{"spread run, probed", spreadIDs(0, 40, 1), want{1, 1, 39, 0}},
		{"spread sparse, probe backs off", spreadIDs(0, 600, 6), want{1, 2, 2 * seekSteps, 98}},
		{"duplicates", []int64{1, 1, 2001, 2001}, want{0, 0, 0, 4}},
		{"descending", []int64{3000, 2000, 1999, 1000}, want{0, 0, 0, 4}},
		{"spread sparse below lendProbeMin", spreadIDs(0, 6*(lendProbeMin-1), 6), want{0, 0, 0, lendProbeMin - 1}},
	}
	for _, c := range cases {
		for _, kind := range []string{"nodes", "rels"} {
			t.Run(c.name+"/"+kind, func(t *testing.T) {
				bs.nodeCache.ResetForTest()
				bs.relCache.ResetForTest()
				counts := countScanReads(t)
				var lent int
				if kind == "nodes" {
					out := make([]*types.Node, len(c.ids))
					if err := bs.LendNodesSorted(lendCostNodeIDs(c.ids...), out); err != nil {
						t.Fatal(err)
					}
					for _, nd := range out {
						if nd != nil {
							lent++
						}
					}
				} else {
					rids := make([]types.RelID, len(c.ids))
					for i, id := range c.ids {
						rids[i] = types.RelID(snowflake.ID(1_000_000 + id))
					}
					out := make([]*types.Relationship, len(c.ids))
					if err := bs.LendRelationshipsSorted(rids, out); err != nil {
						t.Fatal(err)
					}
					for _, r := range out {
						if r != nil {
							lent++
						}
					}
				}
				if lent != len(c.ids) {
					t.Fatalf("lent %d of %d rows", lent, len(c.ids))
				}
				got := want{counts[scanReadIter], counts[scanReadSeek], counts[scanReadNext], counts[scanReadGet]}
				if got.iter != c.want.iter || got.seek != c.want.seek || got.get != c.want.get || got.maxNext > c.want.maxNext {
					t.Fatalf("%d misses: %d iterators, %d seeks, %d nexts, %d gets; want %d iterators, %d seeks, at most %d nexts, %d gets",
						len(c.ids), got.iter, got.seek, got.maxNext, got.get, c.want.iter, c.want.seek, c.want.maxNext, c.want.get)
				}
			})
		}
	}
}

// TestLendSortedLayeredAgreesWithPointReads: over a store whose rows span L0,
// L3 and L1 or L2, the memtable and the value log, with replaced, deleted and never
// written IDs, LendNodesSorted and LendRelationshipsSorted lend exactly what
// LendNode and LendRelationship read per ID, for sparse, dense, mixed,
// duplicate, descending and four-ID batches, and fill no cache.
func TestLendSortedLayeredAgreesWithPointReads(t *testing.T) {
	const n, stride = 1024, 2 // odd IDs written, even IDs never
	badgerOptionsHook = layeredOptions(8<<10, 4)
	t.Cleanup(func() { badgerOptionsHook = nil })
	dir := t.TempDir()
	ro, levels := buildLayeredLendStore(t, dir, layeredShape{n: n, stride: stride, rels: true, valueThreshold: 1 << 10,
		rowBytes: func(j int64) int {
			if j%3 == 0 {
				return 1500 // a value-log value
			}
			return 300
		}}, true)
	t.Logf("tables per level L0..L3: %v", levels)
	// Compaction timing decides whether L1 or L2 holds the middle rounds;
	// the bench, with more rows, asserts all four.
	if len(levels) != 4 || levels[0] == 0 || levels[1]+levels[2] == 0 || levels[3] == 0 {
		t.Fatalf("tables per level %v: want L0, L3 and L1 or L2 populated", levels)
	}
	if _, vlog := ro.db.Size(); vlog < n/3*1500 {
		t.Fatalf("value log holds %d bytes, want the 1,500-byte rows there", vlog)
	}
	if err := ro.Close(); err != nil {
		t.Fatal(err)
	}
	bs, err := New(layeredLendConfig(dir, 1<<10, false))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	// Newer versions in the memtable over every level, and deletes.
	for j := int64(0); j < n; j += 7 {
		nd := types.NewNode(layeredNodeID(j, stride), 1, nil)
		if err := nd.SetProperty("p", fmt.Sprint("memtable-", j)); err != nil {
			t.Fatal(err)
		}
		if err := bs.ReplaceNode(nd); err != nil {
			t.Fatal(err)
		}
	}
	for _, j := range []int64{10, 11, 501, 1001, n - 1} { // none a start node (j%layeredRounds != 0)
		if err := bs.DeleteRelationship(layeredRelID(j, stride)); err != nil {
			t.Fatal(err)
		}
		if err := bs.DeleteNode(layeredNodeID(j, stride)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}

	rows := func(js ...int64) []int64 { return js }
	var all, sparse, mixed, sigma []int64
	for j := int64(0); j < n; j++ {
		all = append(all, j)
		if j%97 == 5 {
			sparse = append(sparse, j)
		}
		if j%300 < 6 {
			mixed = append(mixed, j)
		}
		if j%500 == 3 {
			sigma = append(sigma, j)
		}
	}
	desc := make([]int64, 0, 64)
	for j := int64(700); j > 636; j-- {
		desc = append(desc, j)
	}
	batches := map[string][]int64{
		"all":        all,
		"sparse":     sparse,
		"mixed":      mixed,
		"sigma":      sigma[:4],
		"duplicates": rows(3, 3, 4, 4, 4, 900, 900, 1500),
		"descending": desc,
	}
	// IDs: row j's own, an ID between rows (never written) and one past the end.
	toIDs := func(js []int64) []int64 {
		var ids []int64
		for _, j := range js {
			ids = append(ids, int64(layeredNodeID(j, stride)))
			if j%5 == 0 {
				ids = append(ids, int64(layeredNodeID(j, stride))+1)
			}
		}
		return append(ids, int64(layeredNodeID(n+10, stride)))
	}
	for name, js := range batches {
		ids := toIDs(js)
		t.Run(name+"/nodes", func(t *testing.T) {
			nids := lendCostNodeIDs(ids...)
			bs.nodeCache.ResetForTest()
			out := make([]*types.Node, len(nids))
			if err := bs.LendNodesSorted(nids, out); err != nil {
				t.Fatal(err)
			}
			if bs.nodeCache.Len() != 0 {
				t.Fatalf("the lend filled the node cache: %d rows", bs.nodeCache.Len())
			}
			var lent int
			for i, id := range nids {
				want, err := bs.LendNode(id)
				if (err == nil) != (out[i] != nil) {
					t.Fatalf("node %d: lent %v, LendNode err %v", id, out[i], err)
				}
				if out[i] == nil {
					continue
				}
				lent++
				gp, _ := out[i].GetProperty("p")
				wp, _ := want.GetProperty("p")
				if out[i].ID() != id || !out[i].IsFrozen() || gp != wp {
					t.Fatalf("node %d: lent ID %d frozen %v p %.20v, want p %.20v", id, out[i].ID(), out[i].IsFrozen(), gp, wp)
				}
			}
			if lent == 0 {
				t.Fatal("no row lent")
			}
		})
		t.Run(name+"/rels", func(t *testing.T) {
			rids := make([]types.RelID, len(ids))
			for i, id := range ids {
				rids[i] = types.RelID(snowflake.ID(1<<40 - 1 + id))
			}
			bs.relCache.ResetForTest()
			out := make([]*types.Relationship, len(rids))
			if err := bs.LendRelationshipsSorted(rids, out); err != nil {
				t.Fatal(err)
			}
			if bs.relCache.Len() != 0 {
				t.Fatalf("the lend filled the rel cache: %d rows", bs.relCache.Len())
			}
			var lent int
			for i, id := range rids {
				want, err := bs.LendRelationship(id)
				if (err == nil) != (out[i] != nil) {
					t.Fatalf("rel %d: lent %v, LendRelationship err %v", id, out[i], err)
				}
				if out[i] == nil {
					continue
				}
				lent++
				gp, _ := out[i].GetProperty("p")
				wp, _ := want.GetProperty("p")
				if out[i].ID() != id || !out[i].IsFrozen() || gp != wp || out[i].EndNodeID() != want.EndNodeID() {
					t.Fatalf("rel %d: lent ID %d frozen %v p %.20v, want p %.20v", id, out[i].ID(), out[i].IsFrozen(), gp, wp)
				}
			}
			if lent == 0 {
				t.Fatal("no row lent")
			}
		})
	}
}

// TestLendSortedRepinsAfterFlushBetweenIDs: a flush that lands between two
// cold IDs of one call, releasing the second row's newer version from the
// cache, re-pins the read: the call lends the flushed version, for a dense
// and a sparse batch, nodes and rels.
func TestLendSortedRepinsAfterFlushBetweenIDs(t *testing.T) {
	for _, ids := range [][]int64{{1, 2, 60}, {1, 30, 60}} {
		t.Run(fmt.Sprint(ids), func(t *testing.T) {
			bs := newTestBadgerStoreInMemory(t)
			for i := int64(1); i <= 64; i++ {
				nd := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
				_ = nd.SetProperty("p", "old")
				if err := bs.PutNode(nd); err != nil {
					t.Fatal(err)
				}
				r := types.NewRelationship(types.RelID(snowflake.ID(1000+i)), 3, 1, types.NodeID(snowflake.ID(i)))
				_ = r.SetProperty("p", "old")
				if err := bs.PutRelationship(r); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			bs.nodeCache.ResetForTest()
			bs.relCache.ResetForTest()
			last := ids[len(ids)-1]
			fired := false
			scanReadHook = func(scanRead) {
				if fired {
					return
				}
				fired = true
				nd := types.NewNode(types.NodeID(snowflake.ID(last)), 1, nil)
				_ = nd.SetProperty("p", "new")
				r := types.NewRelationship(types.RelID(snowflake.ID(1000+last)), 3, 1, types.NodeID(snowflake.ID(last)))
				_ = r.SetProperty("p", "new")
				if err := bs.ReplaceNode(nd); err != nil {
					t.Error(err)
				}
				if err := bs.ReplaceRelationship(r); err != nil {
					t.Error(err)
				}
				if err := bs.Flush(); err != nil {
					t.Error(err)
				}
				bs.nodeCache.EvictForTest(snowflake.ID(last))
				bs.relCache.EvictForTest(snowflake.ID(1000 + last))
			}
			t.Cleanup(func() { scanReadHook = nil })
			out := make([]*types.Node, len(ids))
			if err := bs.LendNodesSorted(lendCostNodeIDs(ids...), out); err != nil {
				t.Fatal(err)
			}
			if p, _ := out[len(ids)-1].GetProperty("p"); p != "new" {
				t.Fatalf("node %d lent p %v after the flush, want new", last, p)
			}
			fired = false
			rids := make([]types.RelID, len(ids))
			for i, id := range ids {
				rids[i] = types.RelID(snowflake.ID(1000 + id))
			}
			rout := make([]*types.Relationship, len(ids))
			if err := bs.LendRelationshipsSorted(rids, rout); err != nil {
				t.Fatal(err)
			}
			if p, _ := rout[len(ids)-1].GetProperty("p"); p != "new" {
				t.Fatalf("rel %d lent p %v after the flush, want new", 1000+last, p)
			}
		})
	}
}

// TestLendSortedRowMissingOnEveryPath: a listed row removed under the store
// (as badger reports a block that fails its checksum) is ErrCorruptWire on
// each read path of lendAfterMiss, never another row's or "not found": by
// Get, by a step in a run, by the Seek that starts a run and by a probe.
func TestLendSortedRowMissingOnEveryPath(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)
	for i := int64(1); i <= 400; i++ {
		putTestNode(t, bs, i, 1, nil)
		putTestRel(t, bs, 1000+i, 3, 1, i)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	const gone = 200
	if err := bs.db.Update(func(txn *badgerv4.Txn) error {
		if err := txn.Delete(storepkg.NodeKey(snowflake.ID(gone))); err != nil {
			return err
		}
		return txn.Delete(storepkg.RelKey(snowflake.ID(1000 + gone)))
	}); err != nil {
		t.Fatal(err)
	}
	probe := make([]int64, lendProbeMin)
	for i := range probe {
		probe[i] = gone + 5*int64(i)
	}
	for name, c := range map[string]struct {
		ids  []int64
		path scanRead // the read that reaches gone
	}{
		"get":   {[]int64{100, gone, 300}, scanReadGet},
		"step":  {[]int64{gone - 2, gone - 1, gone, gone + 1}, scanReadNext},
		"seek":  {[]int64{100, gone, gone + 1}, scanReadSeek},
		"probe": {probe, scanReadSeek},
	} {
		t.Run(name, func(t *testing.T) {
			bs.nodeCache.ResetForTest()
			bs.relCache.ResetForTest()
			counts := countScanReads(t)
			err := bs.LendNodesSorted(lendCostNodeIDs(c.ids...), make([]*types.Node, len(c.ids)))
			if !errors.Is(err, storecontract.ErrCorruptWire) {
				t.Fatalf("nodes: got %v, want ErrCorruptWire", err)
			}
			if counts[c.path] == 0 {
				t.Fatalf("nodes: no read of kind %d: %v", c.path, *counts)
			}
			rids := make([]types.RelID, len(c.ids))
			for i, id := range c.ids {
				rids[i] = types.RelID(snowflake.ID(1000 + id))
			}
			err = bs.LendRelationshipsSorted(rids, make([]*types.Relationship, len(rids)))
			if !errors.Is(err, storecontract.ErrCorruptWire) {
				t.Fatalf("rels: got %v, want ErrCorruptWire", err)
			}
		})
	}
}

// TestLendSortedBadgerRereadsWithoutFill (LENDCOST round 2): plain badger
// keeps round 7's scan rule, so a second lend of the same cold IDs reads
// them again (tiered's lend fills instead, TestTieredLendSortedFillsTheCache).
func TestLendSortedBadgerRereadsWithoutFill(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)
	for i := int64(1); i <= 4000; i++ {
		putTestNode(t, bs, i, 1, nil)
		putTestRel(t, bs, 10_000+i, 3, 1, i)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	bs.nodeCache.ResetForTest()
	bs.relCache.ResetForTest()
	ids := []int64{1, 1001, 2001, 3001}
	rids := make([]types.RelID, len(ids))
	for i, id := range ids {
		rids[i] = types.RelID(snowflake.ID(10_000 + id))
	}
	for call := 1; call <= 2; call++ {
		counts := countScanReads(t)
		if err := bs.LendNodesSorted(lendCostNodeIDs(ids...), make([]*types.Node, len(ids))); err != nil {
			t.Fatal(err)
		}
		if err := bs.LendRelationshipsSorted(rids, make([]*types.Relationship, len(rids))); err != nil {
			t.Fatal(err)
		}
		if counts[scanReadGet] != 2*len(ids) || bs.nodeCache.Len() != 0 || bs.relCache.Len() != 0 {
			t.Fatalf("call %d: %d gets, caches %d nodes %d rels; want %d gets, empty caches",
				call, counts[scanReadGet], bs.nodeCache.Len(), bs.relCache.Len(), 2*len(ids))
		}
	}
}
