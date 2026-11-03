package tiered

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// lendBatchFixture is a store whose rows sit on every shard a lend routes to:
// the reference shard, the archive, two closed cold event shards (and an
// empty one between them) and the hot shard, with deleted rows and IDs no
// shard holds. rels by name: "ref" between reference nodes, "arch" on the
// archived node, "cold", "cold2" and "hot" on their event shards, "cross" and
// "cross2" minted in the hot window between nodes of each cold shard (stored
// with the start node, found by the probe of the other event shards, one of
// them only after a miss on the other), "mixed" from a hot node to a
// reference node.
type lendBatchFixture struct {
	ts                *Store
	nodes             map[string]types.NodeID
	rels              map[string]types.RelID
	cold, empty       *EventShard
	cold2, hot        *EventShard
	liveN, liveR      []string // names that must lend a row
	missingN, missing []string // names that must lend nil (deleted or never written)
}

func newLendBatchFixture(t *testing.T, mint bool) *lendBatchFixture {
	t.Helper()
	cfg := mintCfg(t.TempDir(), mint)
	cfg.MaxOpenColdShards = 8
	ts := openMintStore(t, cfg)
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	f := &lendBatchFixture{ts: ts, nodes: map[string]types.NodeID{}, rels: map[string]types.RelID{}}
	node := func(name string, tok uint16) {
		id := types.NodeID(ng.Generate())
		n := types.NewNode(id, tok, nil)
		if err := n.SetProperty("v", name); err != nil {
			t.Fatal(err)
		}
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode %s: %v", name, err)
		}
		f.nodes[name] = id
	}
	rel := func(name, from, to string) {
		id := types.RelID(rg.Generate())
		r := types.NewRelationship(id, 1, f.nodes[from], f.nodes[to])
		if err := r.SetProperty("v", name); err != nil {
			t.Fatal(err)
		}
		if err := ts.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship %s: %v", name, err)
		}
		f.rels[name] = id
	}

	// The first window: becomes the closed cold shard.
	node("refA", mintCaseTok)
	node("refB", mintCaseTok)
	node("refDel", mintCaseTok)
	node("refArch", mintCaseTok)
	node("coldA", mintSignalTok)
	node("coldB", mintSignalTok)
	node("coldDel", mintSignalTok)
	f.nodes["coldMissing"] = types.NodeID(ng.Generate())
	rel("ref", "refA", "refB")
	rel("refDel", "refA", "refB")
	rel("cold", "coldA", "coldB")
	rel("coldDel", "coldA", "coldB")
	if !mint {
		rel("arch", "refArch", "refArch")
	}
	f.rels["coldMissing"] = types.RelID(rg.Generate())
	coldName := hotShardName(ts)
	rotate(t, ts)
	emptyName := hotShardName(ts) // a window without rows: a probe skips it
	rotate(t, ts)

	// The third window: a second closed cold shard.
	node("cold2A", mintSignalTok)
	node("cold2B", mintSignalTok)
	rel("cold2", "cold2A", "cold2B")
	cold2Name := hotShardName(ts)
	rotate(t, ts)

	// The second window: the hot shard.
	node("hotA", mintSignalTok)
	node("hotB", mintSignalTok)
	node("hotDel", mintSignalTok)
	f.nodes["hotMissing"] = types.NodeID(ng.Generate())
	rel("hot", "hotA", "hotB")
	rel("hotDel", "hotA", "hotB")
	rel("cross", "coldA", "coldB")
	rel("cross2", "cold2A", "cold2B")
	rel("mixed", "hotA", "refA")
	f.rels["hotMissing"] = types.RelID(rg.Generate())

	for _, name := range []string{"refDel", "coldDel", "hotDel"} {
		if err := ts.DeleteRelationship(f.rels[name]); err != nil {
			t.Fatalf("DeleteRelationship %s: %v", name, err)
		}
		if err := ts.DeleteNode(f.nodes[name]); err != nil {
			t.Fatalf("DeleteNode %s: %v", name, err)
		}
	}
	if err := ts.ArchiveNode(f.nodes["refArch"]); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	demoteToCold(ts, coldName)
	demoteToCold(ts, emptyName)
	demoteToCold(ts, cold2Name)
	closeColdStores(t, ts)
	ts.mu.RLock()
	f.cold, f.empty, f.cold2, f.hot = ts.eventShards[coldName], ts.eventShards[emptyName], ts.eventShards[cold2Name], ts.hotShard
	ts.mu.RUnlock()
	if !f.empty.noRows.Load() {
		t.Fatal("the empty cold shard is not marked as holding no rows")
	}

	f.liveN = []string{"refA", "refB", "refArch", "coldA", "coldB", "cold2A", "cold2B", "hotA", "hotB"}
	f.missingN = []string{"refDel", "coldDel", "hotDel", "coldMissing", "hotMissing"}
	f.liveR = []string{"ref", "cold", "cold2", "hot", "cross", "cross2", "mixed"}
	if !mint {
		f.liveR = append(f.liveR, "arch")
	}
	f.missing = []string{"refDel", "coldDel", "hotDel", "coldMissing", "hotMissing"}
	return f
}

func (f *lendBatchFixture) nodeIDs(names []string) []types.NodeID {
	ids := make([]types.NodeID, len(names))
	for i, n := range names {
		ids[i] = f.nodes[n]
	}
	return ids
}

func (f *lendBatchFixture) relIDs(names []string) []types.RelID {
	ids := make([]types.RelID, len(names))
	for i, n := range names {
		ids[i] = f.rels[n]
	}
	return ids
}

// lendInputs are the shapes a caller hands the batch: ascending with
// duplicates (the capability's contract) and a duplicate-free permutation
// out of order.
func lendInputs[ID ~int64](all []ID) map[string][]ID {
	sorted := slices.Clone(all)
	slices.Sort(sorted)
	dups := append(slices.Clone(sorted), sorted[0], sorted[len(sorted)/2], sorted[len(sorted)-1])
	slices.Sort(dups)
	unsorted := slices.Clone(sorted)
	slices.Reverse(unsorted)
	unsorted[0], unsorted[len(unsorted)/2] = unsorted[len(unsorted)/2], unsorted[0]
	return map[string][]ID{"sorted with duplicates": dups, "unsorted": unsorted}
}

func nodeRowKey(n *types.Node) string {
	if n == nil {
		return "<nil>"
	}
	v, _ := n.GetProperty("v")
	return fmt.Sprintf("%d %v v%d frozen=%v", n.ID(), v, n.Version(), n.IsFrozen())
}

func relRowKey(r *types.Relationship) string {
	if r == nil {
		return "<nil>"
	}
	v, _ := r.GetProperty("v")
	return fmt.Sprintf("%d %d->%d %v v%d frozen=%v", r.ID(), r.StartNodeID(), r.EndNodeID(), v, r.Version(), r.IsFrozen())
}

// TestTieredLendSortedAgreesWithLend: the tiered batch lend answers what
// LendNode / LendRelationship answer per ID, on every shard a route reaches
// (reference, archive, a closed cold shard opened read-only, the hot shard,
// the probe of other event shards), nil for deleted and never-written IDs, in
// the caller's order, sorted or not; a stale out slot is overwritten.
func TestTieredLendSortedAgreesWithLend(t *testing.T) {
	t.Parallel()
	for _, mint := range []bool{false, true} {
		t.Run(fmt.Sprintf("mint=%v", mint), func(t *testing.T) {
			t.Parallel()
			f := newLendBatchFixture(t, mint)
			lb, ok := any(f.ts).(storecontract.EntityLendBatchCapability)
			if !ok {
				t.Fatal("tiered Store does not implement store.EntityLendBatchCapability")
			}
			stale := types.NewNode(1, mintCaseTok, nil)
			staleRel := types.NewRelationship(1, 1, 1, 1)

			allN := f.nodeIDs(append(slices.Clone(f.liveN), f.missingN...))
			for shape, ids := range lendInputs(allN) {
				closeColdStores(t, f.ts)
				out := make([]*types.Node, len(ids))
				for i := range out {
					out[i] = stale
				}
				if err := lb.LendNodesSorted(ids, out); err != nil {
					t.Fatalf("%s: LendNodesSorted: %v", shape, err)
				}
				closeColdStores(t, f.ts)
				for i, id := range ids {
					want, err := f.ts.LendNode(id)
					if errors.Is(err, ErrNodeNotFound) {
						want, err = nil, nil
					}
					if err != nil {
						t.Fatalf("LendNode(%d): %v", id, err)
					}
					if got, w := nodeRowKey(out[i]), nodeRowKey(want); got != w {
						t.Errorf("%s: node %d at %d: batch %s, LendNode %s", shape, id, i, got, w)
					}
				}
			}
			// The fixture is not trivially empty: each live name lends its row.
			ids := f.nodeIDs(f.liveN)
			out := make([]*types.Node, len(ids))
			if err := lb.LendNodesSorted(ids, out); err != nil {
				t.Fatal(err)
			}
			for i, name := range f.liveN {
				if v, _ := out[i].GetProperty("v"); out[i] == nil || v != name {
					t.Errorf("node %s lent %s", name, nodeRowKey(out[i]))
				}
			}

			allR := f.relIDs(append(slices.Clone(f.liveR), f.missing...))
			for shape, ids := range lendInputs(allR) {
				closeColdStores(t, f.ts)
				out := make([]*types.Relationship, len(ids))
				for i := range out {
					out[i] = staleRel
				}
				if err := lb.LendRelationshipsSorted(ids, out); err != nil {
					t.Fatalf("%s: LendRelationshipsSorted: %v", shape, err)
				}
				closeColdStores(t, f.ts)
				for i, id := range ids {
					want, err := f.ts.LendRelationship(id)
					if errors.Is(err, ErrRelNotFound) {
						want, err = nil, nil
					}
					if err != nil {
						t.Fatalf("LendRelationship(%d): %v", id, err)
					}
					if got, w := relRowKey(out[i]), relRowKey(want); got != w {
						t.Errorf("%s: rel %d at %d: batch %s, LendRelationship %s", shape, id, i, got, w)
					}
				}
			}
			rids := f.relIDs(f.liveR)
			rout := make([]*types.Relationship, len(rids))
			if err := lb.LendRelationshipsSorted(rids, rout); err != nil {
				t.Fatal(err)
			}
			for i, name := range f.liveR {
				if v, _ := rout[i].GetProperty("v"); rout[i] == nil || v != name {
					t.Errorf("rel %s lent %s", name, relRowKey(rout[i]))
				}
			}

			if err := lb.LendNodesSorted(ids, out[:1]); err == nil {
				t.Fatal("LendNodesSorted with fewer out slots than IDs: no error")
			}
			if err := lb.LendRelationshipsSorted(rids, rout[:1]); err == nil {
				t.Fatal("LendRelationshipsSorted with fewer out slots than IDs: no error")
			}
			if err := lb.LendNodesSorted(nil, nil); err != nil {
				t.Fatalf("empty LendNodesSorted: %v", err)
			}
			if err := f.ts.Close(); err != nil {
				t.Fatal(err)
			}
			if err := lb.LendNodesSorted(ids, out); !errors.Is(err, storecontract.ErrStoreClosed) {
				t.Fatalf("LendNodesSorted after Close: %v", err)
			}
			if err := lb.LendRelationshipsSorted(rids, rout); !errors.Is(err, storecontract.ErrStoreClosed) {
				t.Fatalf("LendRelationshipsSorted after Close: %v", err)
			}
			if err := lb.LendNodesSorted(nil, nil); !errors.Is(err, storecontract.ErrStoreClosed) {
				t.Fatalf("empty LendNodesSorted after Close: %v", err)
			}
			if err := lb.LendRelationshipsSorted(nil, nil); !errors.Is(err, storecontract.ErrStoreClosed) {
				t.Fatalf("empty LendRelationshipsSorted after Close: %v", err)
			}
		})
	}
}

// lendLikeCore lends ids as core's LendByIDs does: one batch call where the
// store has EntityLendBatchCapability, else LendNode per ID.
func lendLikeCore(t *testing.T, ts *Store, ids []types.NodeID) []*types.Node {
	t.Helper()
	out := make([]*types.Node, len(ids))
	if lb, ok := any(ts).(storecontract.EntityLendBatchCapability); ok {
		if err := lb.LendNodesSorted(ids, out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	for i, id := range ids {
		n, err := ts.LendNode(id)
		if err != nil && !errors.Is(err, ErrNodeNotFound) {
			t.Fatal(err)
		}
		out[i] = n
	}
	return out
}

func relLendLikeCore(t *testing.T, ts *Store, ids []types.RelID) []*types.Relationship {
	t.Helper()
	out := make([]*types.Relationship, len(ids))
	if lb, ok := any(ts).(storecontract.EntityLendBatchCapability); ok {
		if err := lb.LendRelationshipsSorted(ids, out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	for i, id := range ids {
		r, err := ts.LendRelationship(id)
		if err != nil && !errors.Is(err, ErrRelNotFound) {
			t.Fatal(err)
		}
		out[i] = r
	}
	return out
}

// TestTieredLendSortedOneCheckoutPerShard: a batch lend pins each shard it
// reads once per call (reference, archive, each event shard), not once per
// ID, and opens no shard it does not need.
func TestTieredLendSortedOneCheckoutPerShard(t *testing.T) {
	t.Parallel()
	f := newLendBatchFixture(t, false)
	var mu sync.Mutex
	counts := map[any]int{}
	f.ts.checkoutHook = func(shard any, held int) {
		if held < 0 {
			return
		}
		mu.Lock()
		counts[shard]++
		mu.Unlock()
	}
	check := func(what string, want map[any]int) {
		t.Helper()
		mu.Lock()
		defer mu.Unlock()
		name := func(k any) string {
			switch k {
			case f.cold:
				return "cold"
			case f.hot:
				return "hot"
			case f.empty:
				return "empty"
			case f.cold2:
				return "cold2"
			}
			return fmt.Sprint(k)
		}
		got, exp := map[string]int{}, map[string]int{}
		for k, v := range counts {
			got[name(k)] = v
		}
		for k, v := range want {
			exp[name(k)] = v
		}
		if fmt.Sprint(got) != fmt.Sprint(exp) {
			t.Errorf("%s: checkouts %v, want %v", what, got, exp)
		}
		clear(counts)
	}

	all := map[any]int{checkoutKeyRef: 1, checkoutKeyArchive: 1, f.cold: 1, f.cold2: 1, f.hot: 1}
	ids := f.nodeIDs(append(slices.Clone(f.liveN), f.missingN...))
	slices.Sort(ids)
	lendLikeCore(t, f.ts, ids)
	check("nodes on every shard", all)

	refOnly := f.nodeIDs([]string{"refA", "refB"})
	slices.Sort(refOnly)
	lendLikeCore(t, f.ts, refOnly)
	check("reference nodes", map[any]int{checkoutKeyRef: 1})

	hotOnly := f.nodeIDs([]string{"hotA", "hotB", "hotMissing"})
	slices.Sort(hotOnly)
	lendLikeCore(t, f.ts, hotOnly)
	check("hot nodes", map[any]int{checkoutKeyRef: 1, checkoutKeyArchive: 1, f.hot: 1})

	rids := f.relIDs(append(slices.Clone(f.liveR), f.missing...))
	slices.Sort(rids)
	relLendLikeCore(t, f.ts, rids)
	check("rels on every shard and the probe", all)

	refRels := f.relIDs([]string{"ref"})
	relLendLikeCore(t, f.ts, refRels)
	check("reference rel", map[any]int{checkoutKeyRef: 1})
}

// TestTieredLendSortedPassesOnAListedIDWithoutRow: a shard that lists a
// relationship ID but lends no row for it (here forged: a copy on the hot
// shard, its cache entry marked deleted) passes the ID on to the next shard,
// as LendRelationship does.
func TestTieredLendSortedPassesOnAListedIDWithoutRow(t *testing.T) {
	t.Parallel()
	f := newLendBatchFixture(t, false)
	cross, err := f.ts.GetRelationship(f.rels["cross"])
	if err != nil {
		t.Fatal(err)
	}
	hot := f.hot.store
	for _, name := range []string{"coldA", "coldB"} { // the copy's endpoints
		n, err := f.ts.GetNode(f.nodes[name])
		if err != nil {
			t.Fatal(err)
		}
		if err := hot.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	if err := hot.PutRelationship(cross); err != nil {
		t.Fatalf("forge the copy on the hot shard: %v", err)
	}
	hot.RelCacheForTest().MarkDeleted(cross.ID().SnowflakeID())
	if !hot.HasRelID(cross.ID().SnowflakeID()) {
		t.Fatal("the hot shard does not list the forged ID")
	}
	want, err := f.ts.LendRelationship(cross.ID())
	if err != nil {
		t.Fatalf("LendRelationship: %v", err)
	}
	out := make([]*types.Relationship, 1)
	if err := f.ts.LendRelationshipsSorted([]types.RelID{cross.ID()}, out); err != nil {
		t.Fatal(err)
	}
	if got, w := relRowKey(out[0]), relRowKey(want); got != w || out[0] == nil {
		t.Fatalf("batch %s, LendRelationship %s", got, w)
	}
}

// TestTieredLendSortedHoldsOnePinAtATime: a relationship no shard holds sends
// the batch's probe over every cold shard with rows; it holds one event shard
// pin at a time, as LendRelationship does, not every cold shard at once (each
// a read-only badger open the cold cap cannot close while pinned). Nodes too.
func TestTieredLendSortedHoldsOnePinAtATime(t *testing.T) {
	t.Parallel()
	cfg := mintCfg(t.TempDir(), false)
	cfg.MaxOpenColdShards = 8
	ts := openMintStore(t, cfg)
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	const coldShards = 5
	var names []string
	var nodeIDs []types.NodeID
	var relIDs []types.RelID
	for range coldShards {
		a := putMintNode(t, ts, types.NodeID(ng.Generate()), mintSignalTok)
		b := putMintNode(t, ts, types.NodeID(ng.Generate()), mintSignalTok)
		r := types.RelID(rg.Generate())
		putMintRel(t, ts, r, 1, a, b)
		nodeIDs = append(nodeIDs, a)
		relIDs = append(relIDs, r)
		names = append(names, hotShardName(ts))
		rotate(t, ts)
	}
	for _, name := range names {
		demoteToCold(ts, name)
	}
	closeColdStores(t, ts)
	missing := types.RelID(rg.Generate())

	var mu sync.Mutex
	held, peak := 0, 0
	ts.checkoutHook = func(_ any, d int) {
		mu.Lock()
		defer mu.Unlock()
		held += d
		peak = max(peak, held)
	}
	measure := func(what string, lend func() error) {
		t.Helper()
		mu.Lock()
		held, peak = 0, 0
		mu.Unlock()
		if err := lend(); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		mu.Lock()
		defer mu.Unlock()
		if held != 0 {
			t.Errorf("%s: %d pins left held", what, held)
		}
		if peak > 2 {
			t.Errorf("%s: %d read pins held at once, want at most 2 (%d cold shards)", what, peak, coldShards)
		}
	}
	measure("a missing relationship", func() error {
		return ts.LendRelationshipsSorted([]types.RelID{missing}, make([]*types.Relationship, 1))
	})
	closeColdStores(t, ts)
	rids := append(slices.Clone(relIDs), missing)
	rout := make([]*types.Relationship, len(rids))
	measure("rels on every cold shard and a missing one", func() error {
		return ts.LendRelationshipsSorted(rids, rout)
	})
	for i, r := range rout[:coldShards] {
		if r == nil || r.ID() != relIDs[i] {
			t.Errorf("rel %d lent %s", relIDs[i], relRowKey(r))
		}
	}
	closeColdStores(t, ts)
	nout := make([]*types.Node, len(nodeIDs))
	measure("nodes on every cold shard", func() error {
		return ts.LendNodesSorted(nodeIDs, nout)
	})
	for i, n := range nout {
		if n == nil || n.ID() != nodeIDs[i] {
			t.Errorf("node %d lent %s", nodeIDs[i], nodeRowKey(n))
		}
	}
}
