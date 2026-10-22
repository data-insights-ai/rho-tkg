package badger

import (
	"runtime"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Row 95 cuts B + B2 (`git show f94060df:tasks/todo.md` § "R3 sizing"): a shard opened with
// AdjacencyIndexOnDisk builds no transient outIdx/inIdx maps at open (the
// adjacency lives in the out/in keys) and does not seed relRevs (rev 0
// already forces the locked re-read in every prefetch door).

const adjOpenRels = 10000 // rels per measurement; d=1 (two private endpoints per rel)

// seedAdjOpenDir writes 2*rels nodes and, when withRels, one rel per node pair
// (start 2i+1 -> end 2i+2, the ai-soc per-instance shape), then closes cleanly
// so the next open trusts the open image.
func seedAdjOpenDir(t *testing.T, dir string, rels int, withRels bool) {
	t.Helper()
	bs, err := New(Config{Dir: dir, DisableOrdinals: true, MemTableSize: 8 << 20})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for i := 0; i < rels; i++ {
		for _, id := range []int64{int64(2*i + 1), int64(2*i + 2)} {
			if err := bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)); err != nil {
				t.Fatalf("PutNode: %v", err)
			}
		}
		if !withRels {
			continue
		}
		r := types.NewRelationship(types.RelID(snowflake.ID(int64(1_000_000+i))), 7,
			types.NodeID(snowflake.ID(int64(2*i+1))), types.NodeID(snowflake.ID(int64(2*i+2))))
		if err := bs.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship: %v", err)
		}
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// openAllocPerRel returns the bytes allocated during New per rel (the open
// peak, so it includes any transient map): relDir's open minus the same
// nodes' open without rels.
func openAllocPerRel(t *testing.T, relDir, baseDir string, adjOnDisk bool) (float64, *Store) {
	t.Helper()
	open := func(dir string) (*Store, int64) {
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		s, err := New(Config{Dir: dir, DisableOrdinals: true, MemTableSize: 8 << 20, AdjacencyIndexOnDisk: adjOnDisk})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		runtime.ReadMemStats(&m1)
		return s, int64(m1.TotalAlloc - m0.TotalAlloc)
	}
	b, baseAlloc := open(baseDir)
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	s, relAlloc := open(relDir)
	return float64(relAlloc-baseAlloc) / adjOpenRels, s
}

// TestLoadIndexes_AdjOnDisk_NoTransientMaps measures per-rel bytes of an
// opened shard and pins: with the flag, outIdx/inIdx stay empty through the
// open (no transient build) and relRevs is not seeded.
// Break case: flag off still builds and keeps the maps (the default must not
// change). The unclean open is the next test.
func TestLoadIndexes_AdjOnDisk_NoTransientMaps(t *testing.T) {
	relDir, baseDir := t.TempDir(), t.TempDir()
	seedAdjOpenDir(t, relDir, adjOpenRels, true)
	seedAdjOpenDir(t, baseDir, adjOpenRels, false)

	offAlloc, off := openAllocPerRel(t, relDir, baseDir, false)
	offLive := off.RelMapBytesForTest() / adjOpenRels
	off.idxMu.RLock()
	outOff, revOff := len(off.outIdx), len(off.relRevs)
	off.idxMu.RUnlock()
	if err := off.Close(); err != nil {
		t.Fatal(err)
	}
	onAlloc, on := openAllocPerRel(t, relDir, baseDir, true)
	defer on.Close()
	onLive := on.RelMapBytesForTest() / adjOpenRels
	t.Logf("per rel (d=1, %d rels): flag off: open allocates %.0f B, rel maps hold %.0f B; flag on: %.0f B, %.0f B",
		adjOpenRels, offAlloc, offLive, onAlloc, onLive)

	on.idxMu.RLock()
	outN, inN, revN, relN := len(on.outIdx), len(on.inIdx), len(on.relRevs), len(on.relIDs)
	on.idxMu.RUnlock()
	if relN != adjOpenRels {
		t.Fatalf("flag on: %d live rels after open, want %d", relN, adjOpenRels)
	}
	if outN != 0 || inN != 0 {
		t.Errorf("flag on: outIdx %d / inIdx %d entries after open, want 0", outN, inN)
	}
	if revN != 0 {
		t.Errorf("flag on: relRevs seeded with %d entries at open, want 0 (rev 0 falls back)", revN)
	}
	// The open also allocates badger's reads of the rel keys; the transient
	// nested adjacency maps alone add > 500 B/rel at d=1.
	if onAlloc > 0.65*offAlloc {
		t.Errorf("flag on: open allocates %.0f B/rel vs %.0f flag off, want <= 65 %% (no transient out/in maps)", onAlloc, offAlloc)
	}
	// What remains are two ID sets, relIDs and typeIdx: a map[RelID]struct{}
	// slot is 16 B (the trailing struct{} pads) plus a control byte, ~28 B/rel
	// each at this load factor, up to ~2x that right after a table doubles.
	if onLive > 70 {
		t.Errorf("flag on: rel maps hold %.0f B/rel after open, want <= 70", onLive)
	}

	// Break: the default keeps building and keeping the maps.
	if outOff != adjOpenRels || revOff != adjOpenRels {
		t.Errorf("flag off: outIdx %d relRevs %d after open, want %d each", outOff, revOff, adjOpenRels)
	}
}

// TestLoadIndexes_AdjOnDisk_UncleanOpenRepairsDroppedKey: with no trusted
// image, the rebuild still re-adds an out key the rows require, although the
// disk-mode open no longer fills the maps from the keys.
func TestLoadIndexes_AdjOnDisk_UncleanOpenRepairsDroppedKey(t *testing.T) {
	dir := t.TempDir()
	seedAdjOpenDir(t, dir, 3, true)
	rid := snowflake.ID(1_000_001) // rel 1: 3 -> 4
	db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(txn *badgerv4.Txn) error {
		return txn.Delete(storepkg.OutKey(3, 7, 4, rid))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	var s *Store
	decoded := CountRowDecodesForTest(func() {
		if s, err = New(Config{Dir: dir, DisableOrdinals: true, AdjacencyIndexOnDisk: true}); err != nil {
			t.Fatalf("reopen: %v", err)
		}
	})
	defer s.Close()
	if decoded == 0 {
		t.Fatal("open after a raw edit trusted the image; the repair path was not exercised")
	}
	rels, err := s.OutgoingRelationships(types.NodeID(3), 7)
	if err != nil || len(rels) != 1 || rels[0].ID() != types.RelID(rid) {
		t.Fatalf("outgoing of node 3 after repair: %v err %v, want rel %d", rels, err, rid)
	}
	s.idxMu.RLock()
	outN, inN := len(s.outIdx), len(s.inIdx)
	s.idxMu.RUnlock()
	if outN != 0 || inN != 0 {
		t.Errorf("unclean disk-mode open left outIdx %d / inIdx %d entries", outN, inN)
	}
}

// TestRelRevs_LazySeed_PrefetchFallsBack: after a disk-mode open the first
// Replace of a rel reads its row under the lock (prefetch rev is 0), then
// seeds the rev. Break: a concurrent property write between prefetch and lock
// is still detected (no phantom index entry).
func TestRelRevs_LazySeed_PrefetchFallsBack(t *testing.T) {
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	rid := types.RelID(snowflake.ID(100))
	mk := func(status string) *types.Relationship {
		r := types.NewRelationship(rid, 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
		if err := r.SetProperty("status", status); err != nil {
			t.Fatal(err)
		}
		return r
	}
	if err := bs.PutRelationship(mk("A")); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateRelPropertyIndex(5, "status"); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	if bs, err = New(Config{Dir: dir, AdjacencyIndexOnDisk: true}); err != nil {
		t.Fatal(err)
	}
	defer bs.Close()

	pre, err := bs.prefetchRelWithRev(rid)
	if err != nil {
		t.Fatal(err)
	}
	if pre.rev != 0 {
		t.Fatalf("prefetch rev after disk-mode open = %d, want 0 (unseeded)", pre.rev)
	}
	// A prefetched row that differs from the stored one: rev 0 must not trust it.
	stale := pre
	stale.rel = mk("stale")
	bs.idxMu.Lock()
	cur, err := bs.currentRelForPrefetchLocked(rid, stale)
	bs.idxMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := cur.GetProperty("status"); v != "A" {
		t.Fatalf("rev 0 returned the prefetched row (status %v) instead of reading it under the lock", v)
	}

	// Break: the concurrent write lands in the prefetch->lock window of the
	// first Replace after open.
	bs.replaceRelPrefetchTestHook = func() {
		bs.replaceRelPrefetchTestHook = nil
		if err := bs.ReplaceRelationship(mk("B")); err != nil {
			t.Errorf("concurrent replace: %v", err)
		}
	}
	if err := bs.ReplaceRelationship(mk("C")); err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"A", "B"} {
		if got, err := bs.RelationshipsByTypeAndProperty(5, "status", v, QueryOpts{}); err != nil || len(got) != 0 {
			t.Errorf("status=%s still indexed (%d rows, err %v) after replace to C", v, len(got), err)
		}
	}
	if got, err := bs.RelationshipsByTypeAndProperty(5, "status", "C", QueryOpts{}); err != nil || len(got) != 1 {
		t.Errorf("status=C rows %d err %v, want 1", len(got), err)
	}
	bs.idxMu.RLock()
	seeded := bs.relRevs[rid]
	bs.idxMu.RUnlock()
	if seeded == 0 {
		t.Error("rel rev not seeded by its first write after open")
	}
}
