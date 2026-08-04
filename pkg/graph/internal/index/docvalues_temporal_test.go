package index

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Temporal columns + zone map. The dangerous failure here is not a wrong value —
// it is a SKIPPED BLOCK that should not have been skipped, which silently drops
// rows from an answer. Every probe below is aimed at that.

// tbuild builds a snapshot whose ordinal i has validity bounds from ranges[i].
// ids are 1..n ascending so ordinal == id-1 after the build's sort.
func tbuild(n int, ranges func(i int) (int64, int64)) *LabelDocValues {
	ids := make([]types.NodeID, n)
	for i := range ids {
		ids[i] = types.NodeID(i + 1)
	}
	getProp := func(id types.NodeID, _ string) (any, bool) { return int64(id), true }
	getTemporal := func(id types.NodeID) (int64, int64, bool) {
		f, t := ranges(int(id) - 1)
		return f, t, true
	}
	return BuildLabelDocValues(1, ids, []string{"v"}, getProp, getTemporal)
}

// TestTemporal_NilAccessorMeansNoColumns pins that "no temporal accessor" is
// distinguishable from "bounds are zero". Conflating them would let a reader treat
// an unknown validity as valid-for-all-time and return rows it cannot justify.
func TestTemporal_NilAccessorMeansNoColumns(t *testing.T) {
	ids := []types.NodeID{1, 2}
	l := BuildLabelDocValues(1, ids, []string{"v"},
		func(types.NodeID, string) (any, bool) { return int64(1), true }, nil)
	if l.HasTemporal() {
		t.Fatal("snapshot built with a nil temporal accessor claims to have temporal columns")
	}
	if l.ValidFrom() != nil || l.ValidTo() != nil {
		t.Error("nil accessor still produced validity columns")
	}
	// With no zone map, nothing may EVER be skipped.
	if !l.BlockCanMatch(0, 5000, 6000) {
		t.Error("BlockCanMatch skipped a block on a snapshot with no zone map — " +
			"an absent zone map must never authorise a skip")
	}
}

// TestTemporal_OpenEndedRowBlocksUpperBoundSkip is the sharpest probe here. A row
// with validTo == 0 is OPEN-ENDED — it has no upper bound. If the zone map folds
// that zero into zoneMaxTo as a literal 0, the block looks like it ended at the
// epoch and every query after that instant skips it, dropping live rows.
func TestTemporal_OpenEndedRowBlocksUpperBoundSkip(t *testing.T) {
	// One block. Rows 0..8 are closed and long expired; row 9 is open-ended.
	l := tbuild(10, func(i int) (int64, int64) {
		if i == 9 {
			return 100, 0 // open-ended, still live
		}
		return 100, 200 // expired at 200
	})
	if !l.HasTemporal() {
		t.Fatal("temporal columns not built")
	}
	// A query far in the future: every CLOSED row is irrelevant, but the open-ended
	// row still matches, so the block must NOT be skippable.
	if !l.BlockCanMatch(0, 10_000, 20_000) {
		t.Fatal("block containing an OPEN-ENDED row was skipped for a future query — " +
			"a validTo of 0 means no upper bound, not 'ended at 0'; folding it into " +
			"zoneMaxTo silently drops every live row in the block")
	}
}

// TestTemporal_AllClosedBlockIsSkippable is the other side: without an open-ended
// row the upper-bound skip must actually fire, or the zone map is inert and buys
// nothing. A zone map that never skips passes every correctness test.
func TestTemporal_AllClosedBlockIsSkippable(t *testing.T) {
	l := tbuild(10, func(int) (int64, int64) { return 100, 200 })
	if l.BlockCanMatch(0, 10_000, 20_000) {
		t.Error("a block whose rows all expired at 200 was NOT skipped for a query " +
			"starting at 10000 — the zone map is inert")
	}
	// And it must still match a query that overlaps.
	if !l.BlockCanMatch(0, 150, 250) {
		t.Error("block wrongly skipped for an OVERLAPPING query")
	}
}

// TestTemporal_FutureBlockIsSkippable exercises the lower-bound arm: every row
// starts after the query's upper bound.
func TestTemporal_FutureBlockIsSkippable(t *testing.T) {
	l := tbuild(10, func(int) (int64, int64) { return 9_000, 0 })
	if l.BlockCanMatch(0, 100, 200) {
		t.Error("block whose rows all start at 9000 was not skipped for a query ending at 200")
	}
	// An open-ended QUERY (qTo == 0) has no upper bound, so this arm must not fire.
	if !l.BlockCanMatch(0, 100, 0) {
		t.Error("open-ended query (qTo=0) was treated as ending at 0 and skipped a " +
			"block it must scan")
	}
}

// TestTemporal_SkipNeverDropsAMatchingRow is the exhaustive oracle: over a spread of
// blocks and query windows, a block may only be skipped when NO row in it actually
// matches. This is the property; the individual probes above are the named traps.
func TestTemporal_SkipNeverDropsAMatchingRow(t *testing.T) {
	const n = zoneBlockSize*2 + 500 // three blocks, last one partial
	l := tbuild(n, func(i int) (int64, int64) {
		switch i % 4 {
		case 0:
			return int64(i), 0 // open-ended
		case 1:
			return int64(i), int64(i + 10) // short
		case 2:
			return int64(i), int64(i + 5_000) // long
		default:
			return int64(i + 1_000), int64(i + 1_100) // starts later
		}
	})
	vf, vt := l.ValidFrom(), l.ValidTo()

	for _, q := range [][2]int64{
		{0, 10}, {500, 600}, {5_000, 5_001}, {8_000, 9_000},
		{0, 0}, {100_000, 200_000}, {3_000, 3_000},
	} {
		for start := 0; start < n; start += zoneBlockSize {
			if l.BlockCanMatch(start, q[0], q[1]) {
				continue // scanned — always safe
			}
			// Skipped: prove no row in the block could have matched.
			end := min(start+zoneBlockSize, n)
			for ord := start; ord < end; ord++ {
				f, to := vf[ord], vt[ord]
				matches := (q[1] == 0 || f <= q[1]) && (to == 0 || to > q[0])
				if matches {
					t.Fatalf("query [%d,%d] SKIPPED block at %d, but ordinal %d "+
						"(valid [%d,%d]) matches — the zone map dropped a live row",
						q[0], q[1], start, ord, f, to)
				}
			}
		}
	}
}

// TestTemporal_NodeWithoutMetadataIsNotClaimedValid pins that a node whose accessor
// reports ok=false lands as (0,0) and does NOT get treated as a real bound.
func TestTemporal_NodeWithoutMetadataIsNotClaimedValid(t *testing.T) {
	ids := []types.NodeID{1, 2}
	l := BuildLabelDocValues(1, ids, []string{"v"},
		func(types.NodeID, string) (any, bool) { return int64(1), true },
		func(id types.NodeID) (int64, int64, bool) {
			if id == 1 {
				return 500, 600, true
			}
			return 0, 0, false // no metadata
		})
	if got := l.ValidFrom()[1]; got != 0 {
		t.Errorf("node without metadata got validFrom %d, want 0", got)
	}
	// (0,0) reads as open-ended, so the block can never be skipped on its upper
	// bound — the conservative direction. Assert we did not skip it.
	if !l.BlockCanMatch(0, 10_000, 20_000) {
		t.Error("block containing a metadata-less node was skipped; an unknown " +
			"validity must be conservative, never a licence to drop the row")
	}
}
