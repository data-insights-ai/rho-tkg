package sharded

import (
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func newMemStoreLog(t *testing.T, base, count uint8) *Store {
	t.Helper()
	st, err := New(Config{InMemory: true, BaseSlot: base, SlotCount: count, ChangeLog: true})
	if err != nil {
		t.Fatalf("New(ChangeLog): %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// TestChangeLogFeedCrossShard: mutations across shards draw store-global LSNs in
// ONE total order; the merged feed yields them gapless and ascending.
func TestChangeLogFeedCrossShard(t *testing.T) {
	st := newMemStoreLog(t, 0, 4)
	if !st.ChangeLogEnabled() {
		t.Fatalf("ChangeLogEnabled = false, want true")
	}

	// Nodes across all four slots, then rels across slots — every mutation lands
	// on a different shard but must draw a strictly increasing store-global LSN.
	var mutations int
	a := mkNodeID(0, 1)
	b := mkNodeID(1, 1)
	putNode(t, st, a, 10)
	putNode(t, st, b, 10)
	putNode(t, st, mkNodeID(2, 1), 10)
	putNode(t, st, mkNodeID(3, 1), 10)
	mutations += 4
	for slot := uint8(0); slot < 4; slot++ {
		putRel(t, st, mkRelID(slot, 1), 5, a, b)
		mutations++
	}

	last, err := st.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}
	if last != uint64(mutations) {
		t.Fatalf("LastCommittedLSN = %d, want %d", last, mutations)
	}

	// The merged feed is gapless, ascending, one record per mutation.
	var lsns []uint64
	if err := st.ForEachChange(0, func(rec storecontract.ChangeRecord) bool {
		lsns = append(lsns, rec.LSN)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if len(lsns) != mutations {
		t.Fatalf("feed length = %d, want %d", len(lsns), mutations)
	}
	for i, lsn := range lsns {
		if lsn != uint64(i+1) {
			t.Fatalf("LSN gap/misorder at %d: got %d, want %d", i, lsn, i+1)
		}
	}

	// ChangeFeed materializes the same records; afterLSN skips a prefix.
	feed, err := st.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(feed) != mutations {
		t.Fatalf("ChangeFeed length = %d, want %d", len(feed), mutations)
	}
	tail, err := st.ChangeFeed(uint64(mutations-2), 0)
	if err != nil {
		t.Fatalf("ChangeFeed(after): %v", err)
	}
	if len(tail) != 2 {
		t.Fatalf("ChangeFeed after N-2 = %d, want 2", len(tail))
	}
}

// TestChangeLogDisabledByDefault: without Config.ChangeLog the feed doors are inert.
func TestChangeLogDisabledByDefault(t *testing.T) {
	st := newMemStore(t, 0, 2) // no ChangeLog
	putNode(t, st, mkNodeID(0, 1), 10)
	if st.ChangeLogEnabled() {
		t.Fatalf("ChangeLogEnabled = true, want false")
	}
	if lsn, err := st.LastCommittedLSN(); err != nil || lsn != 0 {
		t.Fatalf("LastCommittedLSN = %d, %v; want 0, nil", lsn, err)
	}
	n := 0
	if err := st.ForEachChange(0, func(storecontract.ChangeRecord) bool { n++; return true }); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if n != 0 {
		t.Fatalf("disabled feed yielded %d records", n)
	}
}

// TestChangeLogReseedAfterReopen: LSNs continue strictly above the persisted max
// after a close/reopen — the shared allocator reseeds from every shard's durable
// LastLSNKey at open (no reuse of a committed LSN).
func TestChangeLogReseedAfterReopen(t *testing.T) {
	dir := t.TempDir()
	st, err := New(Config{Dir: dir, BaseSlot: 0, SlotCount: 4, ChangeLog: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	a := mkNodeID(0, 1)
	b := mkNodeID(1, 1)
	if err := st.PutNode(types.NewNode(a, 10, nil)); err != nil {
		t.Fatalf("PutNode a: %v", err)
	}
	if err := st.PutNode(types.NewNode(b, 10, nil)); err != nil {
		t.Fatalf("PutNode b: %v", err)
	}
	before, err := st.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}
	if before == 0 {
		t.Fatalf("expected nonzero LSN before reopen")
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	st2, err := New(Config{Dir: dir, BaseSlot: 0, SlotCount: 4, ChangeLog: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = st2.Close() }()
	// A new mutation must draw an LSN strictly above the pre-close max.
	if err := st2.PutNode(types.NewNode(mkNodeID(2, 1), 10, nil)); err != nil {
		t.Fatalf("post-reopen PutNode: %v", err)
	}
	after, err := st2.LastCommittedLSN()
	if err != nil {
		t.Fatalf("post-reopen LastCommittedLSN: %v", err)
	}
	if after <= before {
		t.Fatalf("LSN not monotonic across reopen: before %d, after %d (reused LSN?)", before, after)
	}
}
