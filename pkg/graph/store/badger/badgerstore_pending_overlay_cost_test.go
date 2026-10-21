package badger

import (
	"slices"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Row 102 (ai-soc Flux finding): a per-entity overlay read (history of one rel,
// disk-mode adjacency of one node) visited every buffered op, so a rollback of n
// created rels cost n × pending. The cost probe is pendingScanned: the buffered
// entries the overlay readers visited.

const (
	costHub   = types.NodeID(7)
	costOther = types.NodeID(8)
	costRel   = types.RelID(70)
	costType  = uint16(1)
)

// newPendingCostStore is a disk-adjacency store that never flushes on its own,
// holding the target entities' rows plus unrelated buffered ops until pending
// holds at least p entries.
func newPendingCostStore(t *testing.T, p int) *Store {
	t.Helper()
	bs, err := New(Config{InMemory: true, AdjacencyIndexOnDisk: true, FlushInterval: time.Hour, MaxPendingWrites: -1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { bs.Close() })
	for _, nid := range []types.NodeID{costHub, costOther} {
		if err := bs.PutNode(types.NewNode(nid, 1, nil)); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	r := types.NewRelationship(costRel, costType, costHub, costOther)
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	for v := uint32(1); v <= 2; v++ {
		r := types.NewRelationship(costRel, costType, costHub, costOther)
		r.SetVersion(v)
		if err := bs.PutRelVersion(costRel, v, r); err != nil {
			t.Fatalf("PutRelVersion: %v", err)
		}
	}
	// Unrelated pending ops: rels between other nodes (rel row, adjacency, type
	// index keys) with one history version each — the keyspaces the readers filter.
	next := int64(1_000_000)
	for pendingLen(bs) < p {
		a, b := types.NodeID(next), types.NodeID(next+1)
		for _, nid := range []types.NodeID{a, b} {
			if err := bs.PutNode(types.NewNode(nid, 1, nil)); err != nil {
				t.Fatalf("PutNode: %v", err)
			}
		}
		ur := types.NewRelationship(types.RelID(next+2), costType, a, b)
		if err := bs.PutRelationship(ur); err != nil {
			t.Fatalf("PutRelationship: %v", err)
		}
		hr := types.NewRelationship(ur.ID(), costType, a, b)
		hr.SetVersion(1)
		if err := bs.PutRelVersion(ur.ID(), 1, hr); err != nil {
			t.Fatalf("PutRelVersion: %v", err)
		}
		next += 3
	}
	return bs
}

func costRelHistory(t *testing.T, bs *Store) (versions int, scanned uint64) {
	t.Helper()
	before := bs.PendingScannedForTest()
	h, err := bs.GetRelHistory(costRel)
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	return len(h), bs.PendingScannedForTest() - before
}

func costHubAdjacency(t *testing.T, bs *Store) (rids []types.RelID, scanned uint64) {
	t.Helper()
	before := bs.PendingScannedForTest()
	bs.idxMu.RLock()
	rids, err := bs.adjacentRelIDsSnapshotLocked(costHub, 0, false)
	bs.idxMu.RUnlock()
	if err != nil {
		t.Fatalf("adjacentRelIDsSnapshotLocked: %v", err)
	}
	slices.Sort(rids)
	return rids, bs.PendingScannedForTest() - before
}

// TestPendingOverlayCostIndependentOfPendingSize: one entity's history read and
// disk-mode adjacency read visit the same number of buffered entries whether
// 1 K or 64 K unrelated ops are pending, and still see the entity's own rows.
func TestPendingOverlayCostIndependentOfPendingSize(t *testing.T) {
	const maxOwn = 16 // the target's own buffered rows, far below either P
	type cost struct{ hist, adj uint64 }
	costs := map[int]cost{}
	for _, p := range []int{1 << 10, 1 << 16} {
		bs := newPendingCostStore(t, p)
		n, hs := costRelHistory(t, bs)
		if n != 2 {
			t.Fatalf("P=%d: own pending history rows: got %d versions, want 2", p, n)
		}
		rids, as := costHubAdjacency(t, bs)
		if !slices.Equal(rids, []types.RelID{costRel}) {
			t.Fatalf("P=%d: own pending adjacency: got %v, want [%d]", p, rids, costRel)
		}
		t.Logf("P=%d (pending %d): history read scanned %d, adjacency read scanned %d", p, pendingLen(bs), hs, as)
		costs[p] = cost{hs, as}
	}
	small, large := costs[1<<10], costs[1<<16]
	if large.hist != small.hist || large.hist > maxOwn {
		t.Errorf("history read scanned %d at P=1K, %d at P=64K: want equal and <= %d", small.hist, large.hist, maxOwn)
	}
	if large.adj != small.adj || large.adj > maxOwn {
		t.Errorf("adjacency read scanned %d at P=1K, %d at P=64K: want equal and <= %d", small.adj, large.adj, maxOwn)
	}
}

// TestPendingOverlayCost_FlushingPendingPrecedence: rows parked in flushing stay
// visible; a pending delete over a flushing set hides the row, a pending set
// over a flushing delete shows it.
func TestPendingOverlayCost_FlushingPendingPrecedence(t *testing.T) {
	bs := newPendingCostStore(t, 1<<12)
	parkPendingIntoFlushing(t, bs)
	if n, _ := costRelHistory(t, bs); n != 2 {
		t.Fatalf("flushing-only history: got %d versions, want 2", n)
	}
	if rids, _ := costHubAdjacency(t, bs); !slices.Equal(rids, []types.RelID{costRel}) {
		t.Fatalf("flushing-only adjacency: got %v, want [%d]", rids, costRel)
	}
	// Pending delete over a flushing set.
	if err := bs.TruncateRelHistory(costRel, 0); err != nil {
		t.Fatalf("TruncateRelHistory: %v", err)
	}
	if err := bs.DeleteRelationship(costRel); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	if n, _ := costRelHistory(t, bs); n != 0 {
		t.Fatalf("pending delete over flushing set: got %d versions, want 0", n)
	}
	if rids, _ := costHubAdjacency(t, bs); len(rids) != 0 {
		t.Fatalf("pending delete over flushing set: adjacency %v, want empty", rids)
	}
	// Park the deletes, then a pending set over the flushing delete.
	bs.wbMu.Lock()
	bs.flushing = nil // the parked sets "committed" nowhere: only the deletes matter now
	bs.wbMu.Unlock()
	parkPendingIntoFlushing(t, bs)
	r := types.NewRelationship(costRel, costType, costHub, costOther)
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	hr := types.NewRelationship(costRel, costType, costHub, costOther)
	hr.SetVersion(1)
	if err := bs.PutRelVersion(costRel, 1, hr); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}
	if n, _ := costRelHistory(t, bs); n != 1 {
		t.Fatalf("pending set over flushing delete: got %d versions, want 1", n)
	}
	if rids, _ := costHubAdjacency(t, bs); !slices.Equal(rids, []types.RelID{costRel}) {
		t.Fatalf("pending set over flushing delete: adjacency %v, want [%d]", rids, costRel)
	}
}

// TestPendingOverlayCost_DeleteInPendingAndEmpty: a deleted key in pending hides
// the committed row; an empty buffer scans nothing.
func TestPendingOverlayCost_DeleteInPendingAndEmpty(t *testing.T) {
	bs := newPendingCostStore(t, 1<<12)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if n, s := costRelHistory(t, bs); n != 2 || s != 0 {
		t.Fatalf("empty pending: %d versions, scanned %d; want 2, 0", n, s)
	}
	if rids, s := costHubAdjacency(t, bs); !slices.Equal(rids, []types.RelID{costRel}) || s != 0 {
		t.Fatalf("empty pending: adjacency %v, scanned %d; want [%d], 0", rids, s, costRel)
	}
	if err := bs.TruncateRelHistory(costRel, 1); err != nil {
		t.Fatalf("TruncateRelHistory: %v", err)
	}
	if err := bs.DeleteRelationship(costRel); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	if n, _ := costRelHistory(t, bs); n != 1 {
		t.Fatalf("pending delete over committed: got %d versions, want 1", n)
	}
	if rids, _ := costHubAdjacency(t, bs); len(rids) != 0 {
		t.Fatalf("pending delete over committed: adjacency %v, want empty", rids)
	}
}
