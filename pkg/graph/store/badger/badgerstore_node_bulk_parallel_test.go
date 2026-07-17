package badger

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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
