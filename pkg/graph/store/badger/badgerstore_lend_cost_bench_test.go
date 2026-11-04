package badger

import (
	"encoding/hex"
	"math/rand/v2"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// layeredOptions shrinks badger's levels so a store of a few MB spreads its
// tables over L0 to L3 (LENDCOST): four levels, a base level of `size`
// growing mult x per level, tables of `size`, L0 compacted at one table.
func layeredOptions(size int64, mult int) func(badgerv4.Options) badgerv4.Options {
	return func(o badgerv4.Options) badgerv4.Options {
		return o.WithMaxLevels(4).WithBaseLevelSize(size).WithLevelSizeMultiplier(mult).
			WithBaseTableSize(size).WithNumLevelZeroTables(1)
	}
}

// layeredLendConfig is the layered store's config: no periodic flush or GC.
func layeredLendConfig(dir string, valueThreshold int64, readOnly bool) Config {
	return Config{Dir: dir, MemTableSize: 8 << 20, ValueThreshold: valueThreshold, ReadOnly: readOnly,
		FlushInterval: 1<<63 - 1, GCInterval: 1<<63 - 1}
}

// layeredNodeID is row j's node ID: rows lie stride IDs apart (stride 1:
// consecutive IDs; 1<<22: one snowflake millisecond apart, adjacent keys).
func layeredNodeID(j, stride int64) types.NodeID {
	return types.NodeID(snowflake.ID(1 + j*stride))
}

// layeredRounds is the number of write rounds of buildLayeredLendStore.
const layeredRounds = 8

// layeredRelID is row j's relationship ID, from node j-j%layeredRounds
// (written in round 0) to node j.
func layeredRelID(j, stride int64) types.RelID {
	return types.RelID(snowflake.ID(1<<40 + j*stride))
}

// layeredShape is what buildLayeredLendStore writes: n nodes (and with rels
// n relationships) stride IDs apart, row j holding about rowBytes(j) bytes.
type layeredShape struct {
	n, stride      int64
	rowBytes       func(j int64) int
	rels           bool
	valueThreshold int64
}

// buildLayeredLendStore writes the shape's rows (incompressible) in
// layeredRounds rounds, round r the rows j with j%layeredRounds == r so
// every level spans the whole key range, closing after each so the next
// open compacts the round down; it waits for L0 to empty before every round
// but the last, which stays in L0. It returns the store reopened (read-only when readOnly) and the
// tables per level. badgerOptionsHook must be a layeredOptions for the call.
func buildLayeredLendStore(tb testing.TB, dir string, sh layeredShape, readOnly bool) (*Store, []int) {
	tb.Helper()
	rng := rand.New(rand.NewPCG(uint64(sh.n), uint64(sh.stride)))
	for r := int64(0); r < layeredRounds; r++ {
		bs, err := New(layeredLendConfig(dir, sh.valueThreshold, false))
		if err != nil {
			tb.Fatal(err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for r < layeredRounds-1 && bs.db.Levels()[0].NumTables > 0 {
			if time.Now().After(deadline) {
				tb.Fatal("L0 not compacted within 30 s")
			}
			time.Sleep(20 * time.Millisecond)
		}
		for j := r; j < sh.n; j += layeredRounds {
			fill := make([]byte, sh.rowBytes(j)/2)
			for i := range fill {
				fill[i] = byte(rng.Uint32())
			}
			nd := types.NewNode(layeredNodeID(j, sh.stride), 1, nil)
			if err := nd.SetProperty("p", hex.EncodeToString(fill)); err != nil {
				tb.Fatal(err)
			}
			if err := bs.PutNode(nd); err != nil {
				tb.Fatal(err)
			}
			if sh.rels {
				rel := types.NewRelationship(layeredRelID(j, sh.stride), 3, layeredNodeID(j-j%layeredRounds, sh.stride), layeredNodeID(j, sh.stride))
				if err := rel.SetProperty("p", hex.EncodeToString(fill)); err != nil {
					tb.Fatal(err)
				}
				if err := bs.PutRelationship(rel); err != nil {
					tb.Fatal(err)
				}
			}
			if j%4096 == r {
				if err := bs.Flush(); err != nil {
					tb.Fatal(err)
				}
			}
		}
		if err := bs.Close(); err != nil {
			tb.Fatal(err)
		}
	}
	bs, err := New(layeredLendConfig(dir, sh.valueThreshold, readOnly))
	if err != nil {
		tb.Fatal(err)
	}
	levels := make([]int, 0, 4)
	for _, l := range bs.db.Levels() {
		levels = append(levels, l.NumTables)
	}
	return bs, levels
}

// lendCostIDs is call c's batch: k IDs from rows `gap` rows apart, the
// batches walking the store from different starting rows.
func lendCostIDs(c, k, gap, n, stride int64) []types.NodeID {
	span := (k-1)*gap + 1
	start := (c * 7919) % (n - span)
	ids := make([]types.NodeID, k)
	for i := range k {
		ids[i] = layeredNodeID(start+i*gap, stride)
	}
	return ids
}

// getNodeForBench is the Txn.Get baseline: one point read and the decode
// LendNodesSorted does, frozen, not cached.
func getNodeForBench(bs *Store, txn *badgerv4.Txn, nid types.NodeID) (*types.Node, error) {
	id := nid.SnowflakeID()
	item, err := txn.Get(storepkg.NodeKey(id))
	if err != nil {
		return nil, err
	}
	var n *types.Node
	err = item.Value(func(val []byte) error {
		var w storepkg.NodeWire
		if err := storepkg.SafeUnmarshal(val, &w); err != nil {
			return err
		}
		d, err := bs.decodeNodeWireForKey(w, id)
		n = d
		return err
	})
	if err != nil {
		return nil, err
	}
	n.Freeze()
	return n, nil
}

// BenchmarkLendSorted (LENDCOST, ai-soc 4 Nov): LendNodesSorted against a
// Txn.Get per ID in one read transaction and against a DB.View per ID
// (v4.102.0's lend path), over a read-only store of 64 Ki nodes of ~512 B
// whose tables span L0 to L3, every row out of the entity cache. Batches of
// 4 (sigma's ~4.3 IDs per call) and 64 IDs, sparse (rows 1,000 apart) and
// dense (adjacent rows); "spread" stores the rows one snowflake millisecond
// (1<<22 IDs) apart, so adjacent rows have adjacent keys but far IDs.
func BenchmarkLendSorted(b *testing.B) {
	for _, st := range []struct {
		name   string
		stride int64
	}{{"ids", 1}, {"spread", 1 << 22}} {
		b.Run(st.name, func(b *testing.B) {
			const n = 64 << 10
			badgerOptionsHook = layeredOptions(128<<10, 4)
			bs, levels := buildLayeredLendStore(b, b.TempDir(), layeredShape{n: n, stride: st.stride, rowBytes: func(int64) int { return 512 }}, true)
			badgerOptionsHook = nil
			defer func() { _ = bs.Close() }()
			b.Logf("tables per level L0..L3: %v", levels)
			for l, c := range levels {
				if c == 0 {
					b.Fatalf("level %d holds no table: %v", l, levels)
				}
			}
			bs.nodeCache.ResetForTest()
			for _, shape := range []struct {
				name string
				k    int64
				gap  int64
			}{{"sparse4", 4, 1000}, {"sparse64", 64, 1000}, {"dense4", 4, 1}, {"dense64", 64, 1}} {
				out := make([]*types.Node, shape.k)
				b.Run(shape.name+"/lend", func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						ids := lendCostIDs(int64(i), shape.k, shape.gap, n, st.stride)
						if err := bs.LendNodesSorted(ids, out); err != nil {
							b.Fatal(err)
						}
						if out[0] == nil {
							b.Fatal("row not lent")
						}
					}
				})
				b.Run(shape.name+"/get1txn", func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						ids := lendCostIDs(int64(i), shape.k, shape.gap, n, st.stride)
						txn, pin := bs.newReadTxn()
						for j, id := range ids {
							nd, err := getNodeForBench(bs, txn, id)
							if err != nil {
								b.Fatal(err)
							}
							out[j] = nd
						}
						bs.endReadTxn(txn, pin, true)
					}
				})
				b.Run(shape.name+"/viewperid", func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						ids := lendCostIDs(int64(i), shape.k, shape.gap, n, st.stride)
						for j, id := range ids {
							if err := bs.view(func(txn *badgerv4.Txn) error {
								nd, err := getNodeForBench(bs, txn, id)
								out[j] = nd
								return err
							}); err != nil {
								b.Fatal(err)
							}
						}
					}
				})
			}
			if bs.nodeCache.Len() != 0 {
				b.Fatalf("the bench filled the node cache: %d rows", bs.nodeCache.Len())
			}
		})
	}
}
