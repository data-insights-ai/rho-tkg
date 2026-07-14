package sharded

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Test helpers ---

// mkID builds a snowflake ID carrying the given slot in the node field. The
// layout is time(48) | node(5) | seq(10); slot occupies bits 10..14. seq varies
// the value so distinct (slot, n) pairs are distinct IDs. n must be >= 1 (a zero
// ID is rejected by validation).
func mkID(slot uint8, n int64) snowflake.ID {
	return snowflake.ID((n << 15) | (int64(slot) << 10))
}

func mkNodeID(slot uint8, n int64) types.NodeID { return types.NodeID(mkID(slot, n)) }
func mkRelID(slot uint8, n int64) types.RelID   { return types.RelID(mkID(slot, n)) }

func newMemStore(t *testing.T, base, count uint8) *Store {
	t.Helper()
	st, err := New(Config{InMemory: true, BaseSlot: base, SlotCount: count})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func putNode(t *testing.T, st *Store, id types.NodeID, label uint16) *types.Node {
	t.Helper()
	n := types.NewNode(id, label, nil)
	if err := st.PutNode(n); err != nil {
		t.Fatalf("PutNode(%d): %v", id, err)
	}
	return n
}

func putRel(t *testing.T, st *Store, id types.RelID, typ uint16, start, end types.NodeID) *types.Relationship {
	t.Helper()
	r := types.NewRelationship(id, typ, start, end)
	if err := st.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship(%d): %v", id, err)
	}
	return r
}

func nodeIDSet(nodes []*types.Node) map[snowflake.ID]struct{} {
	m := make(map[snowflake.ID]struct{}, len(nodes))
	for _, n := range nodes {
		m[n.ID().SnowflakeID()] = struct{}{}
	}
	return m
}

func relIDSet(rels []*types.Relationship) map[snowflake.ID]struct{} {
	m := make(map[snowflake.ID]struct{}, len(rels))
	for _, r := range rels {
		m[r.ID().SnowflakeID()] = struct{}{}
	}
	return m
}

func assertNodeSet(t *testing.T, got []*types.Node, want ...types.NodeID) {
	t.Helper()
	set := nodeIDSet(got)
	if len(set) != len(want) {
		t.Fatalf("node set size: got %d %v, want %d %v", len(set), keysNode(got), len(want), want)
	}
	for _, w := range want {
		if _, ok := set[w.SnowflakeID()]; !ok {
			t.Fatalf("node set missing %d; got %v", w, keysNode(got))
		}
	}
}

func assertRelSet(t *testing.T, got []*types.Relationship, want ...types.RelID) {
	t.Helper()
	set := relIDSet(got)
	if len(set) != len(want) {
		t.Fatalf("rel set size: got %d, want %d %v", len(set), len(want), want)
	}
	for _, w := range want {
		if _, ok := set[w.SnowflakeID()]; !ok {
			t.Fatalf("rel set missing %d", w)
		}
	}
}

func keysNode(nodes []*types.Node) []snowflake.ID {
	out := make([]snowflake.ID, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.ID().SnowflakeID())
	}
	return out
}

func mustSorted(t *testing.T, nodes []*types.Node) {
	t.Helper()
	for i := 1; i < len(nodes); i++ {
		if nodes[i-1].ID().SnowflakeID() > nodes[i].ID().SnowflakeID() {
			t.Fatalf("nodes not ID-ascending at %d: %v", i, keysNode(nodes))
		}
	}
}

// --- (a) point CRUD + history round-trips per slot + cross-slot ---

func TestPointCRUDPerSlotAndCrossSlot(t *testing.T) {
	st := newMemStore(t, 0, 4)

	// Nodes spread across all four slots.
	ids := []types.NodeID{mkNodeID(0, 1), mkNodeID(1, 1), mkNodeID(2, 1), mkNodeID(3, 1)}
	for _, id := range ids {
		putNode(t, st, id, 10)
	}
	for _, id := range ids {
		got, err := st.GetNode(id)
		if err != nil {
			t.Fatalf("GetNode(%d): %v", id, err)
		}
		if got.ID() != id {
			t.Fatalf("GetNode(%d) returned %d", id, got.ID())
		}
		// Point read must be a mutable, independent copy.
		if err := got.SetProperty("k", "v"); err != nil {
			t.Fatalf("point-read node should be mutable: %v", err)
		}
	}

	// History round-trip: version the node on slot 2.
	target := mkNodeID(2, 1)
	prev, _ := st.GetNode(target)
	cur := types.NewNode(target, 10, nil)
	cur.SetVersion(1)
	if err := st.ReplaceNodeWithHistory(cur, prev.Version(), prev); err != nil {
		t.Fatalf("ReplaceNodeWithHistory: %v", err)
	}
	hist, err := st.GetNodeHistory(target)
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(hist) == 0 {
		t.Fatalf("expected history for %d", target)
	}
	v0, err := st.GetNodeVersion(target, 0)
	if err != nil || v0 == nil {
		t.Fatalf("GetNodeVersion(0): %v", err)
	}
}

// --- (b) ErrSlotNotLocal on unclaimed-slot IDs ---

func TestErrSlotNotLocal(t *testing.T) {
	// Claim slots [2,4): shards for slots 2 and 3 only.
	st := newMemStore(t, 2, 2)

	unclaimed := mkNodeID(0, 1) // slot 0 not claimed
	if err := st.PutNode(types.NewNode(unclaimed, 10, nil)); !errors.Is(err, ErrSlotNotLocal) {
		t.Fatalf("PutNode unclaimed slot: want ErrSlotNotLocal, got %v", err)
	}
	if _, err := st.GetNode(unclaimed); !errors.Is(err, ErrSlotNotLocal) {
		t.Fatalf("GetNode unclaimed slot: want ErrSlotNotLocal, got %v", err)
	}
	if err := st.DeleteNode(unclaimed); !errors.Is(err, ErrSlotNotLocal) {
		t.Fatalf("DeleteNode unclaimed slot: want ErrSlotNotLocal, got %v", err)
	}
	unclaimedRel := mkRelID(5, 1)
	if _, err := st.GetRelationship(unclaimedRel); !errors.Is(err, ErrSlotNotLocal) {
		t.Fatalf("GetRelationship unclaimed slot: want ErrSlotNotLocal, got %v", err)
	}

	// A claimed slot works.
	claimed := mkNodeID(2, 1)
	if err := st.PutNode(types.NewNode(claimed, 10, nil)); err != nil {
		t.Fatalf("PutNode claimed slot: %v", err)
	}
}

// --- (c) label/type folds: exact ID-sorted union incl. pagination straddle ---

func TestLabelFoldExactSetAndPagination(t *testing.T) {
	st := newMemStore(t, 0, 4)
	// Label 10 members spread across slots so a paginated window straddles shards.
	var members []types.NodeID
	for slot := uint8(0); slot < 4; slot++ {
		for n := int64(1); n <= 3; n++ {
			id := mkNodeID(slot, n)
			putNode(t, st, id, 10)
			members = append(members, id)
		}
	}
	// A decoy node with a different label must NOT appear.
	putNode(t, st, mkNodeID(1, 99), 20)

	all, err := st.NodesByLabel(10, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	assertNodeSet(t, all, members...)
	mustSorted(t, all)

	// Pagination: page size 5 across the 12-member union, straddling shards.
	seen := make(map[snowflake.ID]struct{})
	var after types.EntityID
	pages := 0
	for {
		page, perr := st.NodesByLabel(10, QueryOpts{Limit: 5, After: after})
		if perr != nil {
			t.Fatalf("NodesByLabel page: %v", perr)
		}
		if len(page) == 0 {
			break
		}
		mustSorted(t, page)
		for _, n := range page {
			if _, dup := seen[n.ID().SnowflakeID()]; dup {
				t.Fatalf("duplicate across pages: %d", n.ID())
			}
			seen[n.ID().SnowflakeID()] = struct{}{}
		}
		after = types.EntityID(page[len(page)-1].ID().SnowflakeID())
		pages++
		if pages > 10 {
			t.Fatalf("pagination did not terminate")
		}
	}
	if len(seen) != len(members) {
		t.Fatalf("paginated union size %d, want %d", len(seen), len(members))
	}
}

func TestTypeFoldExactSet(t *testing.T) {
	st := newMemStore(t, 0, 4)
	// Nodes to be endpoints (all on slot 0 for simplicity).
	a := mkNodeID(0, 1)
	b := mkNodeID(0, 2)
	putNode(t, st, a, 10)
	putNode(t, st, b, 10)
	// Rels of type 5 spread across rel slots.
	var rels []types.RelID
	for slot := uint8(0); slot < 4; slot++ {
		id := mkRelID(slot, 100)
		putRel(t, st, id, 5, a, b)
		rels = append(rels, id)
	}
	// Decoy type.
	putRel(t, st, mkRelID(2, 200), 6, a, b)

	got, err := st.RelationshipsByType(5, QueryOpts{})
	if err != nil {
		t.Fatalf("RelationshipsByType: %v", err)
	}
	assertRelSet(t, got, rels...)
}

// --- (d) adjacency folds: rels on shard X between nodes on shards Y,Z ---

func TestAdjacencyFoldsCrossShard(t *testing.T) {
	st := newMemStore(t, 0, 4)
	// Endpoints on different shards.
	y := mkNodeID(1, 1) // start on slot 1
	z := mkNodeID(2, 1) // end on slot 2
	putNode(t, st, y, 10)
	putNode(t, st, z, 10)
	// Two rels FROM y TO z, both stored on slot 3 (neither endpoint's slot).
	r1 := mkRelID(3, 1)
	r2 := mkRelID(3, 2)
	putRel(t, st, r1, 5, y, z)
	putRel(t, st, r2, 7, y, z)
	// A rel FROM z TO y stored on slot 0.
	r3 := mkRelID(0, 1)
	putRel(t, st, r3, 5, z, y)

	// Outgoing from y = {r1, r2}.
	out, err := st.OutgoingRelationships(y, 0)
	if err != nil {
		t.Fatalf("OutgoingRelationships(y): %v", err)
	}
	assertRelSet(t, out, r1, r2)

	// Outgoing from y filtered by type 5 = {r1}.
	out5, err := st.OutgoingRelationships(y, 5)
	if err != nil {
		t.Fatalf("OutgoingRelationships(y,5): %v", err)
	}
	assertRelSet(t, out5, r1)

	// Incoming to z = {r1, r2}.
	inZ, err := st.IncomingRelationships(z, 0)
	if err != nil {
		t.Fatalf("IncomingRelationships(z): %v", err)
	}
	assertRelSet(t, inZ, r1, r2)

	// Incoming to y = {r3}.
	inY, err := st.IncomingRelationships(y, 0)
	if err != nil {
		t.Fatalf("IncomingRelationships(y): %v", err)
	}
	assertRelSet(t, inY, r3)

	// ForNodes batch.
	m, err := st.OutgoingRelationshipsForNodes([]types.NodeID{y, z}, 0)
	if err != nil {
		t.Fatalf("OutgoingRelationshipsForNodes: %v", err)
	}
	assertRelSet(t, m[y], r1, r2)
	assertRelSet(t, m[z], r3)

	// Missing endpoint node on rel put fails ErrNodeNotFound.
	missing := mkNodeID(1, 999)
	badRel := types.NewRelationship(mkRelID(3, 50), 5, y, missing)
	if err := st.PutRelationship(badRel); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("PutRelationship with missing endpoint: want ErrNodeNotFound, got %v", err)
	}
}

// --- (e) counts/stats folds vs per-shard sums ---

func TestCountsFolds(t *testing.T) {
	st := newMemStore(t, 0, 4)
	putNode(t, st, mkNodeID(0, 1), 10)
	putNode(t, st, mkNodeID(1, 1), 10)
	putNode(t, st, mkNodeID(2, 1), 20)
	putNode(t, st, mkNodeID(3, 1), 10)

	if n, _ := st.NodeCount(); n != 4 {
		t.Fatalf("NodeCount = %d, want 4", n)
	}
	if n, _ := st.NodeCountByLabel(10); n != 3 {
		t.Fatalf("NodeCountByLabel(10) = %d, want 3", n)
	}
	if n, _ := st.NodeCountByLabel(20); n != 1 {
		t.Fatalf("NodeCountByLabel(20) = %d, want 1", n)
	}

	a := mkNodeID(0, 1)
	b := mkNodeID(1, 1)
	putRel(t, st, mkRelID(2, 1), 5, a, b)
	putRel(t, st, mkRelID(3, 1), 5, a, b)
	if n, _ := st.RelationshipCount(); n != 2 {
		t.Fatalf("RelationshipCount = %d, want 2", n)
	}
	if n, _ := st.RelCountByType(5); n != 2 {
		t.Fatalf("RelCountByType(5) = %d, want 2", n)
	}
}

// --- (g) Clear resets counts and preserves the catalog ---

func TestClearResetsCounts(t *testing.T) {
	st := newMemStore(t, 0, 3)
	putNode(t, st, mkNodeID(0, 1), 10)
	putNode(t, st, mkNodeID(1, 1), 10)
	putNode(t, st, mkNodeID(2, 1), 10)
	if n, _ := st.NodeCount(); n != 3 {
		t.Fatalf("pre-clear NodeCount = %d", n)
	}
	if err := st.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if n, _ := st.NodeCount(); n != 0 {
		t.Fatalf("post-clear NodeCount = %d, want 0", n)
	}
	// Routing still works (catalog intact) — a put/get round-trips.
	putNode(t, st, mkNodeID(1, 5), 10)
	if _, err := st.GetNode(mkNodeID(1, 5)); err != nil {
		t.Fatalf("post-clear GetNode: %v", err)
	}
}

// --- (h) frozen scan pointers vs mutable point-read copies ---

func TestFrozenScanMutablePointRead(t *testing.T) {
	st := newMemStore(t, 0, 2)
	id := mkNodeID(1, 1)
	putNode(t, st, id, 10)

	// Scan returns FROZEN pointers — mutation must be rejected.
	scan, err := st.NodesByLabel(10, QueryOpts{})
	if err != nil || len(scan) != 1 {
		t.Fatalf("NodesByLabel: %v (n=%d)", err, len(scan))
	}
	if err := scan[0].SetProperty("k", "v"); err == nil {
		t.Fatalf("scan row should be frozen (mutation rejected)")
	}

	// Point read returns a mutable copy.
	pt, err := st.GetNode(id)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if err := pt.SetProperty("k", "v"); err != nil {
		t.Fatalf("point read should be mutable: %v", err)
	}
}

// --- (f) catalog: create->reopen round-trip; conflicting-config fails closed ---

func TestCatalogReopenRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := New(Config{Dir: dir, BaseSlot: 0, SlotCount: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	putNode(t, st, mkNodeID(0, 1), 10)
	putNode(t, st, mkNodeID(3, 1), 10)
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen with the SAME config — must round-trip.
	st2, err := New(Config{Dir: dir, BaseSlot: 0, SlotCount: 4})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = st2.Close() }()
	if _, err := st2.GetNode(mkNodeID(3, 1)); err != nil {
		t.Fatalf("post-reopen GetNode: %v", err)
	}
	if n, _ := st2.NodeCount(); n != 2 {
		t.Fatalf("post-reopen NodeCount = %d, want 2", n)
	}
}

func TestCatalogConflictWrongSlotCount(t *testing.T) {
	dir := t.TempDir()
	st, err := New(Config{Dir: dir, BaseSlot: 0, SlotCount: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_ = st.Close()

	// Reopen with a different SlotCount — must fail closed.
	_, err = New(Config{Dir: dir, BaseSlot: 0, SlotCount: 2})
	if !errors.Is(err, ErrCatalogConflict) {
		t.Fatalf("reopen wrong SlotCount: want ErrCatalogConflict, got %v", err)
	}
}

func TestCatalogConflictMissingShardDir(t *testing.T) {
	dir := t.TempDir()
	st, err := New(Config{Dir: dir, BaseSlot: 0, SlotCount: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_ = st.Close()

	// Remove a mapped shard directory (slot 3).
	if err := os.RemoveAll(filepath.Join(dir, shardDirName(3))); err != nil {
		t.Fatalf("remove shard dir: %v", err)
	}
	_, err = New(Config{Dir: dir, BaseSlot: 0, SlotCount: 4})
	if !errors.Is(err, ErrCatalogConflict) {
		t.Fatalf("reopen missing shard dir: want ErrCatalogConflict, got %v", err)
	}
}

// --- config validation ---

func TestConfigRangeValidation(t *testing.T) {
	cases := []Config{
		{InMemory: true, BaseSlot: 0, SlotCount: 0},   // zero count
		{InMemory: true, BaseSlot: 30, SlotCount: 4},  // overflows 32
		{InMemory: true, BaseSlot: 0, SlotCount: 33},  // count > 32
	}
	for i, c := range cases {
		if _, err := New(c); err == nil {
			t.Fatalf("case %d: expected error for invalid config %+v", i, c)
		}
	}
}
