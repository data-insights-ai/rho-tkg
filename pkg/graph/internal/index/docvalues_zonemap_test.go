package index

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Zone maps only pay off when the column is CLUSTERED by time. Two shapes:
//
//	clustered — validFrom ascends with ordinal (the natural case: snapshots sort by
//	            NodeID, snowflakes ascend with mint time, and an unset ValidFrom
//	            RESOLVES to mint time, so the column is sorted for free)
//	scattered — validFrom randomised, so every block spans the whole range
func zoneBuild(n int, clustered bool) *LabelDocValues {
	ids := make([]types.NodeID, n)
	for i := range ids {
		ids[i] = types.NodeID(i + 1)
	}
	gp := func(id types.NodeID, _ string) (any, bool) { return int64(id), true }
	gt := func(id types.NodeID) (int64, int64, bool) {
		i := int64(id)
		if clustered {
			return i * 10, i*10 + 5, true
		}
		return (i * 7919) % int64(n*10), ((i * 7919) % int64(n*10)) + 5, true
	}
	return BuildLabelDocValues(1, ids, []string{"v"}, gp, gt)
}

func zoneSkipRatio(l *LabelDocValues, n int, qFrom, qTo int64) float64 {
	blocks, skipped := 0, 0
	for s := 0; s < n; s += zoneBlockSize {
		blocks++
		if !l.BlockCanMatch(s, qFrom, qTo) {
			skipped++
		}
	}
	return float64(skipped) / float64(blocks)
}

// TestZoneMap_SkipsOnlyWhenClustered pins that the zone map is a CLUSTERING BET,
// and pins both sides of it. A zone map that never skips passes every correctness
// test, so the win needs its own assertion; and a reader who assumes it always wins
// will be surprised by backfilled data, so the loss needs one too.
//
// The clustered case is the NATURAL one and not a lucky fixture: a snapshot sorts by
// NodeID, snowflake IDs ascend with mint time, and an unset ValidFrom resolves to
// mint time — so a column of nodes that never set ValidFrom explicitly arrives
// sorted for free.
func TestZoneMap_SkipsOnlyWhenClustered(t *testing.T) {
	const n = 200_000
	cl, sc := zoneBuild(n, true), zoneBuild(n, false)
	qFrom, qTo := int64(n*10/2), int64(n*10/2+n*10/100) // narrow window, ~1% of range

	clustered := zoneSkipRatio(cl, n, qFrom, qTo)
	scattered := zoneSkipRatio(sc, n, qFrom, qTo)
	t.Logf("narrow window: clustered skip=%.1f%%  scattered skip=%.1f%%",
		100*clustered, 100*scattered)

	if clustered < 0.90 {
		t.Errorf("clustered column skipped only %.1f%% of blocks, want >=90%% — the "+
			"zone map is inert on the shape it exists to serve", 100*clustered)
	}
	// Not a bug, but a documented limit: a column whose ValidFrom is scattered (the
	// backfilled/historical case) gets no benefit, because every block spans the
	// whole range. Asserting it keeps the limit honest rather than folklore.
	if scattered > 0.05 {
		t.Errorf("scattered column skipped %.1f%% of blocks; if this now works, the "+
			"documented clustering caveat is stale and should be rewritten", 100*scattered)
	}

	// A point query is the narrowest possible window and must skip at least as much.
	if pt := zoneSkipRatio(cl, n, qFrom, qFrom+1); pt < clustered {
		t.Errorf("point query skipped %.1f%% but a wider window skipped %.1f%% — "+
			"a narrower query can never match more blocks", 100*pt, 100*clustered)
	}
}
