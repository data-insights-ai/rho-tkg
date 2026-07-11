package tiered

import (
	"errors"
	"testing"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// setupPropertyStatsShards mirrors setupBatchDelete but also registers a
// THIRD, SHARED label token ("Shared") that a test can attach as an EXTRA
// label to both a reference-class node (primary "Case") and an event-class
// node (primary "Signal") — routing is decided by the PRIMARY label alone
// (tieredstore_write_node.go's shardForNodeCreate), so a node's extra labels
// never affect placement, but its property-stats accumulator IS updated on
// every label token the node carries (memory/badger's
// adjustNodePropertyKeyCounts loop). This is what lets a single
// (labelToken, propertyKey) pair span a reference shard AND an event shard
// in the SAME query — the scenario ADR-0005 §3.1 requires ("Min/Max spanning
// shards").
func setupPropertyStatsShards(t *testing.T) (ts *Store, caseTok, signalTok, sharedTok uint16) {
	t.Helper()
	ts = newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	var err error
	caseTok, err = reg.GetOrCreate("Case")
	if err != nil {
		t.Fatalf("GetOrCreate Case: %v", err)
	}
	if _, err := reg.GetOrCreate("User"); err != nil {
		t.Fatalf("GetOrCreate User: %v", err)
	}
	signalTok, err = reg.GetOrCreate("Signal")
	if err != nil {
		t.Fatalf("GetOrCreate Signal: %v", err)
	}
	sharedTok, err = reg.GetOrCreate("Shared")
	if err != nil {
		t.Fatalf("GetOrCreate Shared: %v", err)
	}
	return ts, caseTok, signalTok, sharedTok
}

// TestTieredStoreNodePropertyStats_MinMaxSpanningRefAndEventShards is the
// REQUIRED ADR-0005 §3.1 "Min/Max spanning shards" scenario: the SAME
// (labelToken, propertyKey) pair is populated by a reference-shard node
// (carrying the shared label as an EXTRA label alongside primary "Case") AND
// an event-shard node (carrying it alongside primary "Signal"). Min lands on
// the event shard, Max on the reference shard — the fold must combine both,
// not just report whichever shard happens to be checked out first.
func TestTieredStoreNodePropertyStats_MinMaxSpanningRefAndEventShards(t *testing.T) {
	ts, caseTok, signalTok, sharedTok := setupPropertyStatsShards(t)
	gen := tieredNodeGen(t)

	refNode := tieredLabelPropertyNodeWithExtras(t, types.NodeID(gen.Generate()), caseTok, []uint16{sharedTok}, map[string]any{"v": int64(99)})
	if err := ts.PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	evtNode := tieredLabelPropertyNodeWithExtras(t, types.NodeID(gen.Generate()), signalTok, []uint16{sharedTok}, map[string]any{"v": int64(10)})
	if err := ts.PutNode(evtNode); err != nil {
		t.Fatalf("PutNode event: %v", err)
	}

	stats, err := ts.NodePropertyStats(sharedTok, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats: %v", err)
	}
	if stats.Count != 2 {
		t.Fatalf("Count = %d, want 2", stats.Count)
	}
	if stats.Min != int64(10) {
		t.Fatalf("Min = %v, want int64(10) (the event-shard node)", stats.Min)
	}
	if stats.Max != int64(99) {
		t.Fatalf("Max = %v, want int64(99) (the reference-shard node)", stats.Max)
	}
	if stats.NDV < 1 {
		t.Fatalf("NDV = %d, want a positive estimate", stats.NDV)
	}
}

// TestTieredStoreNodePropertyStats_ArchiveShardFold exercises the
// reference-shard + archive-shard fold (mirroring
// TestTieredStoreNodeCountByLabelAndPropertyKey's archive coverage): a node
// stays on refShard, another is moved to refArchive — Count/Min/Max must
// combine both.
func TestTieredStoreNodePropertyStats_ArchiveShardFold(t *testing.T) {
	ts, caseTok, _, _ := setupPropertyStatsShards(t)
	gen := tieredNodeGen(t)

	live := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"v": int64(5)})
	archived := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"v": int64(40)})
	for _, n := range []*types.Node{live, archived} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}
	if err := ts.ArchiveNode(archived.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	stats, err := ts.NodePropertyStats(caseTok, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats: %v", err)
	}
	if stats.Count != 2 {
		t.Fatalf("Count = %d, want 2 (refShard + refArchive)", stats.Count)
	}
	if stats.Min != int64(5) {
		t.Fatalf("Min = %v, want int64(5)", stats.Min)
	}
	if stats.Max != int64(40) {
		t.Fatalf("Max = %v, want int64(40) (the archived node)", stats.Max)
	}
}

// crossShardPropStatsStep is a prime step used to SCATTER the test values in
// TestTieredStoreNodePropertyStats_CrossShardCountsOnce across their decimal
// string encoding. HyperLogLog hashes each value's canonical string key
// (types.IndexablePropertyValueKey via FNV-1a); short, tightly-sequential
// decimal strings ("0","1","2",…) are a known-degenerate input for FNV-1a —
// it fails to avalanche the low-order digit changes into the sketch's
// register-selecting high bits, so a run of consecutive small integers
// undercounts catastrophically (empirically ~8 estimated for 75 truly
// distinct sequential values in this codebase's implementation, an order of
// magnitude off). A moderate prime step scatters the digit patterns enough
// to restore the sketch's normal accuracy (empirically ~75 for 75 distinct
// values at this step) without changing what's actually being tested here —
// the CROSS-SHARD MERGE, not the hash function's own accuracy (which has its
// own seeded regression in hyperloglog_test.go using random strings).
const crossShardPropStatsStep = 97

// TestTieredStoreNodePropertyStats_CrossShardCountsOnce is the REQUIRED
// ADR-0005 §3.1 "same-value-two-shards" scenario: warm and hot event shards
// each carry 50 distinct values with a 25-value OVERLAP (true union = 75
// distinct values). A fold that (incorrectly) SUMS each shard's own
// HyperLogLog Estimate() would report roughly 100 (50+50) — the summation
// trap the ADR calls out. The correct register-max sketch MERGE followed by
// ONE Estimate() call on the combined sketch must land close to the true 75,
// not the naive 100.
func TestTieredStoreNodePropertyStats_CrossShardCountsOnce(t *testing.T) {
	ts, _, signalTok, _ := setupPropertyStatsShards(t)
	gen := tieredNodeGen(t)

	// Warm shard: v in {0, step, 2*step, ..., 49*step}.
	for i := int64(0); i < 50; i++ {
		n := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), signalTok, map[string]any{"v": i * crossShardPropStatsStep})
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode warm %d: %v", i, err)
		}
	}
	forceRotation(t, ts)
	// Hot shard: v in {25*step, ..., 74*step} — a 25-value overlap with the
	// warm shard.
	for i := int64(25); i < 75; i++ {
		n := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), signalTok, map[string]any{"v": i * crossShardPropStatsStep})
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode hot %d: %v", i, err)
		}
	}

	stats, err := ts.NodePropertyStats(signalTok, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats: %v", err)
	}
	if stats.Count != 100 {
		t.Fatalf("Count = %d, want 100 (50 warm + 50 hot live nodes)", stats.Count)
	}
	if stats.Min != int64(0) {
		t.Fatalf("Min = %v, want int64(0)", stats.Min)
	}
	if stats.Max != int64(74*crossShardPropStatsStep) {
		t.Fatalf("Max = %v, want int64(%d)", stats.Max, 74*crossShardPropStatsStep)
	}
	// True union distinct count is 75. A naive per-shard-Estimate() SUM would
	// land near 100 (50+50, ignoring the 25-value overlap entirely) — assert
	// well below that trap and reasonably close to the true union.
	if stats.NDV < 55 || stats.NDV > 90 {
		t.Fatalf("NDV = %d, want roughly the true union (~75), NOT the naive per-shard sum (~100) — got outside [55,90]", stats.NDV)
	}
}

// TestTieredStoreNodePropertyStats_ColdShardFold verifies the fold reaches a
// COLD (lazily-closed) event shard: data written while the shard was hot
// must still be counted after the shard is demoted to cold, exercising the
// checkoutStoreForRead lazy-reopen path (CLAUDE.md "checkout/checkin for
// cold shards").
func TestTieredStoreNodePropertyStats_ColdShardFold(t *testing.T) {
	ts, _, signalTok, _ := setupPropertyStatsShards(t)
	gen := tieredNodeGen(t)

	ts.MuForTest().RLock()
	originName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	n := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), signalTok, map[string]any{"v": int64(7)})
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	forceRotation(t, ts)
	demoteToCold(ts, originName)

	ts.MuForTest().RLock()
	coldES := ts.EventShardsForTest()[originName]
	ts.MuForTest().RUnlock()
	if coldES == nil || coldES.Tier() != TierCold {
		t.Fatal("expected the origin shard to be cold")
	}

	stats, err := ts.NodePropertyStats(signalTok, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats over a cold shard: %v", err)
	}
	if stats.Count != 1 {
		t.Fatalf("Count = %d, want 1 (the cold shard's node)", stats.Count)
	}
	if stats.Min != int64(7) || stats.Max != int64(7) {
		t.Fatalf("Min/Max = %v/%v, want both int64(7)", stats.Min, stats.Max)
	}
	if stats.NDV < 1 {
		t.Fatalf("NDV = %d, want a positive estimate", stats.NDV)
	}
}

// TestTieredStoreNodePropertyStats_EmptyPairZeroValue mirrors
// NodeCountByLabelAndPropertyKey's "unregistered → 0, not an error"
// convention for the richer PropertyStats sibling.
func TestTieredStoreNodePropertyStats_EmptyPairZeroValue(t *testing.T) {
	ts, caseTok, _, _ := setupPropertyStatsShards(t)

	stats, err := ts.NodePropertyStats(caseTok, "missing")
	if err != nil {
		t.Fatalf("NodePropertyStats empty pair: %v", err)
	}
	if stats.Count != 0 || stats.Min != nil || stats.Max != nil || stats.NDV != 0 {
		t.Fatalf("stats = %+v, want zero value", stats)
	}
}

// TestTieredStoreNodePropertyStats_ShardSketchesShareEquivalentPrecision is
// the "precision-match assertion" ADR-0005 §3.1 requires: every shard's
// PropertyStatsAccumulator is built at index.DefaultHLLPrecision, so the
// tiered fold's HyperLogLog.Merge across ref/archive/event shards can never
// hit ErrHLLPrecisionMismatch in practice — this test pins that invariant
// directly against the concrete shards' own sketches, rather than merely
// hoping NodePropertyStats never errors.
func TestTieredStoreNodePropertyStats_ShardSketchesShareEquivalentPrecision(t *testing.T) {
	ts, caseTok, signalTok, _ := setupPropertyStatsShards(t)
	gen := tieredNodeGen(t)

	refNode := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"v": int64(1)})
	if err := ts.PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	evtNode := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), signalTok, map[string]any{"v": int64(2)})
	if err := ts.PutNode(evtNode); err != nil {
		t.Fatalf("PutNode event: %v", err)
	}

	refSketch, _, _, _, err := ts.RefShardForTest().NodePropertyStatsSketch(caseTok, "v")
	if err != nil {
		t.Fatalf("ref NodePropertyStatsSketch: %v", err)
	}
	hotSketch, _, _, _, err := ts.HotShardForTest().Store().NodePropertyStatsSketch(signalTok, "v")
	if err != nil {
		t.Fatalf("hot NodePropertyStatsSketch: %v", err)
	}
	if refSketch == nil || hotSketch == nil {
		t.Fatal("expected both shards to return a populated sketch")
	}
	if refSketch.Precision() != indexpkg.DefaultHLLPrecision {
		t.Fatalf("ref shard sketch precision = %d, want %d", refSketch.Precision(), indexpkg.DefaultHLLPrecision)
	}
	if hotSketch.Precision() != indexpkg.DefaultHLLPrecision {
		t.Fatalf("hot shard sketch precision = %d, want %d", hotSketch.Precision(), indexpkg.DefaultHLLPrecision)
	}
	if err := refSketch.Merge(hotSketch); err != nil {
		t.Fatalf("Merge across shards with matching precision must not error: %v", err)
	}
}

func TestTieredStoreNodePropertyStatsErrors(t *testing.T) {
	ts, caseTok, _, _ := setupPropertyStatsShards(t)

	if _, err := ts.NodePropertyStats(0, "v"); err == nil {
		t.Fatal("token 0 should be rejected")
	}
	if _, err := ts.NodePropertyStats(caseTok, "tkg_version"); err == nil {
		t.Fatal("shadow property key should be rejected")
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := ts.NodePropertyStats(caseTok, "v"); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed store = %v, want ErrStoreClosed", err)
	}
}
