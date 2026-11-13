//go:build measure

// Measurement level (René 4 Nov, `scripts/measure.sh`): the bytes an open allocates
// and the live heap its maps keep are read from process-wide counters, which
// other tests, badger's own goroutines and host load move, so these bounds run
// by hand, not in a gate (MEMGATE). The map entry counts gate in
// TestLoadIndexes_AdjOnDisk_NoTransientMaps.

package badger

import (
	"runtime"
	"testing"
)

// openAllocPerRel returns the bytes allocated during New per rel (the open
// peak, so it includes any transient map): relDir's open minus the same
// nodes' open without rels.
func openAllocPerRel(t *testing.T, relDir, baseDir string, adjOnDisk bool) (float64, *Store) {
	t.Helper()
	open := func(dir string) (*Store, int64) {
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		s := openAdjStore(t, dir, adjOnDisk)
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

// TestLoadIndexes_AdjOnDisk_OpenBytesPerRel measures per-rel bytes of an
// opened shard: with the flag, the open builds no transient outIdx/inIdx
// (it allocates at most 65 % of the flag-off open) and the rel maps keep at
// most 70 B per rel.
func TestLoadIndexes_AdjOnDisk_OpenBytesPerRel(t *testing.T) {
	relDir, baseDir := t.TempDir(), t.TempDir()
	seedAdjOpenDir(t, relDir, adjOpenRels, true)
	seedAdjOpenDir(t, baseDir, adjOpenRels, false)

	offAlloc, off := openAllocPerRel(t, relDir, baseDir, false)
	offLive := off.RelMapBytesForTest() / adjOpenRels
	if err := off.Close(); err != nil {
		t.Fatal(err)
	}
	onAlloc, on := openAllocPerRel(t, relDir, baseDir, true)
	defer on.Close()
	onLive := on.RelMapBytesForTest() / adjOpenRels
	t.Logf("per rel (d=1, %d rels): flag off: open allocates %.0f B, rel maps hold %.0f B; flag on: %.0f B, %.0f B",
		adjOpenRels, offAlloc, offLive, onAlloc, onLive)
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
}
