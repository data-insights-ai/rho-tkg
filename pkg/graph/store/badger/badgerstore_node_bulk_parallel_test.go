package badger

import (
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// (newTinyCacheBadgerStore — CacheCapacity 1, so post-flush reads MISS the cache and
// go through the badger decode path — is defined in v3056_fixes_test.go.)

// TestCollectNodesBulkParallel_EquivalentToSerial is the correctness gate for
// parallel decode: for the same ID list — including deleted (tombstoned) and absent
// IDs — the parallel collector must return the IDENTICAL ordered set of present nodes
// as the serial forEachNodeBulk, and each node must decode to the same property state.
func TestCollectNodesBulkParallel_EquivalentToSerial(t *testing.T) {
	bs := newTinyCacheBadgerStore(t)
	gen := newTestGen(t, 0)

	const label = uint16(1)
	var ids []types.NodeID
	for i := 0; i < 500; i++ {
		nid := types.NodeID(gen.Generate())
		nd := types.NewNode(nid, label, nil)
		if err := nd.SetProperty("seq", int64(i)); err != nil {
			t.Fatalf("SetProperty: %v", err)
		}
		if err := bs.PutNode(nd); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		ids = append(ids, nid)
	}
	if err := bs.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Delete every 7th node → the ID list now mixes present + tombstoned + absent.
	for i := 0; i < len(ids); i += 7 {
		if err := bs.DeleteNode(ids[i]); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}
	}
	if err := bs.flush(); err != nil {
		t.Fatalf("flush after delete: %v", err)
	}

	// Serial reference.
	var serial []*types.Node
	if err := bs.forEachNodeBulk(ids, func(n *types.Node) bool {
		serial = append(serial, n)
		return true
	}); err != nil {
		t.Fatalf("forEachNodeBulk: %v", err)
	}

	// Parallel candidate.
	parallel, err := bs.collectNodesBulkParallel(ids)
	if err != nil {
		t.Fatalf("collectNodesBulkParallel: %v", err)
	}

	if len(parallel) != len(serial) {
		t.Fatalf("parallel returned %d nodes, serial %d", len(parallel), len(serial))
	}
	for i := range serial {
		if parallel[i].ID() != serial[i].ID() {
			t.Fatalf("order mismatch at %d: parallel=%d serial=%d", i, parallel[i].ID().SnowflakeID(), serial[i].ID().SnowflakeID())
		}
		ps, _ := parallel[i].GetProperty("seq")
		ss, _ := serial[i].GetProperty("seq")
		if ps != ss {
			t.Fatalf("seq mismatch for node %d: parallel=%v serial=%v", parallel[i].ID().SnowflakeID(), ps, ss)
		}
	}
	// Sanity: some nodes were deleted, so the present set is a strict subset.
	if len(parallel) >= len(ids) {
		t.Fatalf("expected fewer than %d present nodes after deletes, got %d", len(ids), len(parallel))
	}
}

// TestNodesByLabel_ParallelPathEquivalence exercises the WIRED path: a scan whose
// candidate count crosses parallelDecodeMinIDs routes through parallel decode, and
// must return the same set as the serial forEachNodeBulk over the same label.
func TestNodesByLabel_ParallelPathEquivalence(t *testing.T) {
	bs := newTinyCacheBadgerStore(t)
	gen := newTestGen(t, 0)

	const labelA, labelB = uint16(1), uint16(2)
	n := parallelDecodeMinIDs + 200 // ensure the wired parallel path triggers
	var aIDs []types.NodeID
	for i := 0; i < n; i++ {
		nid := types.NodeID(gen.Generate())
		nd := types.NewNode(nid, labelA, nil)
		if err := bs.PutNode(nd); err != nil {
			t.Fatalf("PutNode A: %v", err)
		}
		aIDs = append(aIDs, nid)
	}
	// A handful of label-B nodes that must NOT appear in a label-A scan.
	for i := 0; i < 5; i++ {
		if err := bs.PutNode(types.NewNode(types.NodeID(gen.Generate()), labelB, nil)); err != nil {
			t.Fatalf("PutNode B: %v", err)
		}
	}
	if err := bs.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Wired path (parallel — len >= parallelDecodeMinIDs, Limit 0).
	got, err := bs.NodesByLabel(labelA, store.QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	// Reference set via the serial door.
	want := make([]*types.Node, 0, len(aIDs))
	if err := bs.forEachNodeBulk(aIDs, func(nd *types.Node) bool {
		if nd.HasLabelTokenRaw(labelA) {
			want = append(want, nd)
		}
		return true
	}); err != nil {
		t.Fatalf("forEachNodeBulk: %v", err)
	}
	if len(got) != len(want) || len(got) != n {
		t.Fatalf("parallel scan returned %d, serial %d, want %d", len(got), len(want), n)
	}
	for i := range want {
		if got[i].ID() != want[i].ID() {
			t.Fatalf("order mismatch at %d", i)
		}
	}
}

// TestBulkNodePropGetter_ParallelBranch exercises bulkNodePropGetter's parallel
// materialization branch (candidate count >= parallelDecodeMinIDs) — the DocValues
// cold-build node fetch — and asserts it returns the correct per-node property, i.e.
// the parallel decode wired into the column build agrees with the stored values.
func TestBulkNodePropGetter_ParallelBranch(t *testing.T) {
	bs := newTinyCacheBadgerStore(t)
	gen := newTestGen(t, 0)

	const label = uint16(1)
	n := parallelDecodeMinIDs + 100 // cross the parallel threshold
	ids := make([]types.NodeID, 0, n)
	want := make(map[types.NodeID]int64, n)
	for i := 0; i < n; i++ {
		nid := types.NodeID(gen.Generate())
		nd := types.NewNode(nid, label, nil)
		if err := nd.SetProperty("v", int64(i)); err != nil {
			t.Fatalf("SetProperty: %v", err)
		}
		if err := bs.PutNode(nd); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		ids = append(ids, nid)
		want[nid] = int64(i)
	}
	if err := bs.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	getProp := bs.bulkNodePropGetter(ids)
	for _, id := range ids {
		got, ok := getProp(id, "v")
		if !ok {
			t.Fatalf("getProp(%d, v) not found", id.SnowflakeID())
		}
		if got != want[id] {
			t.Fatalf("getProp(%d, v) = %v, want %d", id.SnowflakeID(), got, want[id])
		}
	}
}

// TestCollectNodesBulkParallel_FewerJobsThanWorkers covers the worker clamp
// (workers > len(jobs)) — a handful of cache-missed nodes must decode correctly even
// when there are fewer decode jobs than GOMAXPROCS.
func TestCollectNodesBulkParallel_FewerJobsThanWorkers(t *testing.T) {
	bs := newTinyCacheBadgerStore(t) // CacheCapacity 1 → post-flush reads miss
	gen := newTestGen(t, 0)

	var ids []types.NodeID
	for i := 0; i < 3; i++ {
		nid := types.NodeID(gen.Generate())
		nd := types.NewNode(nid, 1, nil)
		if err := nd.SetProperty("seq", int64(i)); err != nil {
			t.Fatalf("SetProperty: %v", err)
		}
		if err := bs.PutNode(nd); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		ids = append(ids, nid)
	}
	if err := bs.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	got, err := bs.collectNodesBulkParallel(ids)
	if err != nil {
		t.Fatalf("collectNodesBulkParallel: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d nodes, want 3", len(got))
	}
	for i, n := range got {
		if n.ID() != ids[i] {
			t.Fatalf("order mismatch at %d", i)
		}
	}
}

// TestCollectNodesBulkParallel_DecodeErrorSurfaces proves the parallel path does NOT
// silently drop a corrupt node: a decode failure in a worker is propagated as an
// error from collectNodesBulkParallel (the errOnce path), not swallowed into a
// short result. Silently returning fewer nodes would be the wrong-answer class.
func TestCollectNodesBulkParallel_DecodeErrorSurfaces(t *testing.T) {
	bs := newTinyCacheBadgerStore(t)
	gen := newTestGen(t, 0)

	var ids []types.NodeID
	for i := 0; i < 4; i++ {
		nid := types.NodeID(gen.Generate())
		if err := bs.PutNode(types.NewNode(nid, 1, nil)); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		ids = append(ids, nid)
	}
	if err := bs.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Corrupt one node's stored bytes directly, then evict the cache so the read
	// goes through the badger decode path.
	bad := ids[2]
	if err := bs.db.Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storepkg.NodeKey(bad.SnowflakeID()), []byte{0xff, 0xff, 0xff, 0xff})
	}); err != nil {
		t.Fatalf("corrupt write: %v", err)
	}
	bs.nodeCache.ResetForTest()

	if _, err := bs.collectNodesBulkParallel(ids); err == nil {
		t.Fatal("collectNodesBulkParallel over a corrupt node returned nil error — a decode failure must surface, not be silently dropped")
	}
}

// TestAllNodes_ParallelPathEquivalence exercises the wired AllNodes parallel path:
// an unbounded whole-graph scan whose candidate count crosses parallelDecodeMinIDs
// routes through parallel decode and must return the same set as a serial reference.
func TestAllNodes_ParallelPathEquivalence(t *testing.T) {
	bs := newTinyCacheBadgerStore(t)
	gen := newTestGen(t, 0)

	n := parallelDecodeMinIDs + 150
	want := make(map[types.NodeID]struct{}, n)
	for i := 0; i < n; i++ {
		nid := types.NodeID(gen.Generate())
		nd := types.NewNode(nid, uint16(1+i%3), nil) // spread across a few labels
		if err := bs.PutNode(nd); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		want[nid] = struct{}{}
	}
	if err := bs.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	got, err := bs.AllNodes(store.QueryOpts{})
	if err != nil {
		t.Fatalf("AllNodes: %v", err)
	}
	if len(got) != n {
		t.Fatalf("AllNodes returned %d, want %d", len(got), n)
	}
	// Every stored node present, sorted ascending, no dupes.
	var prev types.NodeID
	for i, nd := range got {
		if _, ok := want[nd.ID()]; !ok {
			t.Fatalf("AllNodes returned unexpected node %d", nd.ID().SnowflakeID())
		}
		if i > 0 && nd.ID().SnowflakeID() <= prev.SnowflakeID() {
			t.Fatalf("AllNodes not sorted ascending at %d", i)
		}
		prev = nd.ID()
	}
}
