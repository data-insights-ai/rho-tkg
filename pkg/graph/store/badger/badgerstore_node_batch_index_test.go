package badger

import (
	"bytes"
	"fmt"
	"iter"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// batchIdxStore is a store with property indexes in RAM on label A (plain,
// range counts, Elements) and on disk on label D (scalar, Elements, member
// entries in 0x0C), and a composite on (v, c) of each label. allDisk keeps
// every label's indexes on disk (PropertyIndexOnDisk; scalar ones only).
func batchIdxStore(t *testing.T, allDisk bool) (bs *Store, a, d uint16) {
	t.Helper()
	labels := registrypkg.NewLabelRegistry()
	a, _ = labels.GetOrCreate("A")
	d, _ = labels.GetOrCreate("D")
	bs = newFlushParkStore(t, func(cfg *Config) {
		cfg.FlushInterval = time.Hour
		cfg.MaxPendingWrites = -1 // every entry stays queued for the comparison
		cfg.PropertyKeyRegistry = registrypkg.NewPropertyKeyRegistry()
		cfg.LabelNames = labels
		if allDisk {
			cfg.PropertyIndexOnDisk = true
		} else {
			cfg.PropertyIndexOnDiskLabels = []string{"D"}
		}
	})
	type ixDef struct {
		label uint16
		key   string
		opts  storecontract.PropertyIndexOptions
	}
	defs := []ixDef{
		{a, "v", storecontract.PropertyIndexOptions{}},
		{a, "c", storecontract.PropertyIndexOptions{RangeCounts: true}},
		{a, "l", storecontract.PropertyIndexOptions{Elements: true}},
		{d, "v", storecontract.PropertyIndexOptions{}},
		{d, "l", storecontract.PropertyIndexOptions{Elements: true}},
	}
	if allDisk {
		defs = []ixDef{{a, "v", storecontract.PropertyIndexOptions{}}, {a, "c", storecontract.PropertyIndexOptions{}}, {d, "v", storecontract.PropertyIndexOptions{}}}
	}
	for _, ix := range defs {
		if err := bs.CreatePropertyIndexWithOptions(ix.label, ix.key, ix.opts); err != nil {
			t.Fatalf("index %d/%s: %v", ix.label, ix.key, err)
		}
	}
	for _, label := range []uint16{a, d} {
		if err := bs.CreateCompositePropertyIndex(label, []string{"v", "c"}); err != nil {
			t.Fatalf("composite %d: %v", label, err)
		}
	}
	return bs, a, d
}

// TestPutNodesBatchIndexesOnce (BATCHIDX, ai-soc 3 Nov: AddNode 16 us a row at
// 3.47 M nodes, property-index inserts 106 ms of a 219 ms cut): PutNodesBatch
// maintains the property indexes, in RAM and on disk, once for the batch, not
// once per node. Before: 1,000 per-node adds for 1,000 nodes.
func TestPutNodesBatchIndexesOnce(t *testing.T) {
	bs, a, d := batchIdxStore(t, false)
	nodes := make([]*types.Node, 1000)
	for i := range nodes {
		label := a
		if i%2 == 1 {
			label = d
		}
		nodes[i] = types.NewNode(types.NodeID(snowflake.ID(1000+i)), label, nil)
		if err := nodes[i].SetProperty("v", int64(i%7)); err != nil {
			t.Fatal(err)
		}
	}
	var perNode, batch int
	testHookPropIdxAdd = func(s *Store, isBatch bool) {
		if s != bs {
			return
		}
		if isBatch {
			batch++
		} else {
			perNode++
		}
	}
	defer func() { testHookPropIdxAdd = nil }()
	if err := bs.PutNodesBatch(nodes); err != nil {
		t.Fatal(err)
	}
	if perNode != 0 || batch != 1 {
		t.Errorf("PutNodesBatch of %d nodes: %d per-node and %d batch property-index adds; want 0 and 1", len(nodes), perNode, batch)
	}
}

// TestPutNodesBatchIndexStateEqualsPutNode (BATCHIDX): random batches through
// PutNodesBatch leave the RAM indexes (entries, Mutated while an index builds,
// prefix, range and cardinality answers), the on-disk entries queued and the
// on-disk ID bounds as PutNode of each node does.
func TestPutNodesBatchIndexStateEqualsPutNode(t *testing.T) {
	t.Parallel()
	for seed := uint64(1); seed <= 8; seed++ {
		allDisk := seed%2 == 0
		seq, a, d := batchIdxStore(t, allDisk)
		bat, _, _ := batchIdxStore(t, allDisk)
		midBuild := indexpkg.PropertyIndexKey{LabelToken: a, PropertyKey: "c"}
		midBuildDisk := indexpkg.PropertyIndexKey{LabelToken: d, PropertyKey: "v"}
		for _, s := range []*Store{seq, bat} {
			s.idxMu.Lock()
			s.propertyIndexes[midBuild].Mutated = make(map[snowflake.ID]struct{})
			s.propertyIndexes[midBuildDisk].Mutated = make(map[snowflake.ID]struct{})
			s.idxMu.Unlock()
		}
		r := rand.New(rand.NewPCG(seed, 0xba7c))
		next := int64(1 << 20)
		var low int64 = 1 << 19
		for round := 0; round < 5; round++ {
			n := 1 + r.IntN(700)
			nodes := make([]*types.Node, n)
			for i := range nodes {
				var id int64
				if r.IntN(4) == 0 {
					low -= int64(1 + r.IntN(5)) // below every ID so far
					id = low
				} else {
					next += int64(1 + r.IntN(3))
					id = next
				}
				label := []uint16{a, d, a, d, 77}[r.IntN(5)]
				nodes[i] = types.NewNode(types.NodeID(snowflake.ID(id)), label, nil)
				if r.IntN(6) == 0 {
					nodes[i] = types.NewNode(types.NodeID(snowflake.ID(id)), label, []uint16{[]uint16{a, d}[r.IntN(2)]})
				}
				val := func() any {
					switch r.IntN(5) {
					case 0:
						return fmt.Sprint("s", r.IntN(3))
					case 1:
						return fmt.Sprintf("u%06d", r.IntN(1_000_000))
					case 2:
						return int64(r.IntN(4))
					case 3:
						return float64(r.IntN(4))
					default:
						return r.Float64()
					}
				}
				for _, k := range []string{"v", "c"} {
					if r.IntN(5) != 0 {
						_ = nodes[i].SetProperty(k, val())
					}
				}
				if r.IntN(3) != 0 {
					_ = nodes[i].SetProperty("l", []any{val(), val(), val()})
				}
			}
			for _, nd := range nodes {
				if err := seq.PutNode(nd); err != nil {
					t.Fatal(err)
				}
			}
			if err := bat.PutNodesBatch(nodes); err != nil {
				t.Fatal(err)
			}
			if ws, gs := batchIdxState(seq), batchIdxState(bat); ws != gs {
				t.Fatalf("seed %d (all on disk %v) round %d: PutNodesBatch index state differs from PutNode\nPutNode:\n%s\nPutNodesBatch:\n%s", seed, allDisk, round, ws, gs)
			}
		}
	}
}

// batchIdxState renders the store's property-index state: per RAM index its
// entries, Mutated and query answers; per on-disk index its Mutated; the
// queued 0x0A keys; the on-disk ID bounds.
func batchIdxState(bs *Store) string {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	var b bytes.Buffer
	keys := make([]indexpkg.PropertyIndexKey, 0, len(bs.propertyIndexes))
	for k := range bs.propertyIndexes {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(x, y indexpkg.PropertyIndexKey) int {
		if x.LabelToken != y.LabelToken {
			return int(x.LabelToken) - int(y.LabelToken)
		}
		return bytes.Compare([]byte(x.PropertyKey), []byte(y.PropertyKey))
	})
	for _, k := range keys {
		pi := bs.propertyIndexes[k]
		fmt.Fprintf(&b, "index %v onDisk=%v\n", k, pi.OnDisk())
		vks := make([]string, 0, len(pi.Entries))
		for vk := range pi.Entries {
			vks = append(vks, vk)
		}
		slices.Sort(vks)
		for _, vk := range vks {
			fmt.Fprintf(&b, "  E %q %v\n", vk, slices.Collect(pi.Entries[vk].All()))
		}
		if pi.Mutated != nil {
			m := make([]snowflake.ID, 0, len(pi.Mutated))
			for id := range pi.Mutated {
				m = append(m, id)
			}
			slices.Sort(m)
			fmt.Fprintf(&b, "  Mutated %v\n", m)
		}
		for _, p := range []string{"", "s", "u1"} {
			ids, ok := pi.PrefixNodeIDs(p)
			slices.Sort(ids)
			fmt.Fprintf(&b, "  prefix %q %v %v\n", p, ok, ids)
		}
		for _, rg := range [][2]float64{{-1, 10}, {0.25, 2}} {
			ids, ok := pi.RangeNodeIDs(rg[0], rg[1], true, true)
			slices.Sort(ids)
			c, cok := pi.RangeCardinality(rg[0], rg[1], true, true)
			fmt.Fprintf(&b, "  range %v %v %v card %d %v\n", rg, ok, ids, c, cok)
		}
	}
	ckeys := make([]indexpkg.CompositeIndexKey, 0, len(bs.compositeIndexes))
	for k := range bs.compositeIndexes {
		ckeys = append(ckeys, k)
	}
	slices.SortFunc(ckeys, func(x, y indexpkg.CompositeIndexKey) int {
		return bytes.Compare([]byte(fmt.Sprint(x)), []byte(fmt.Sprint(y)))
	})
	for _, k := range ckeys {
		ci := bs.compositeIndexes[k]
		var lines []string
		ci.Range(func(vk string, n int, ids iter.Seq[snowflake.ID]) bool {
			lines = append(lines, fmt.Sprintf("  C %q %d %v", vk, n, slices.Collect(ids)))
			return true
		})
		slices.Sort(lines)
		fmt.Fprintf(&b, "composite %v onDisk=%v keys=%d\n", k, ci.OnDisk(), ci.KeyCount())
		for _, l := range lines {
			fmt.Fprintln(&b, l)
		}
	}
	bs.wbMu.Lock()
	var disk []string
	for k, op := range bs.pending {
		if len(k) > 0 && (k[0] == storepkg.KeyPropertyIndex || k[0] == storepkg.KeyEntityIndex) {
			disk = append(disk, fmt.Sprintf("%x %d %x", k, op.opType, op.value))
		}
	}
	bs.wbMu.Unlock()
	slices.Sort(disk)
	fmt.Fprintf(&b, "disk %d entries\n", len(disk))
	for _, e := range disk {
		fmt.Fprintf(&b, "  %s\n", e)
	}
	fmt.Fprintf(&b, "propIDs %d..%d\n", bs.propIDs.lo.Load(), bs.propIDs.hi.Load())
	return b.String()
}
