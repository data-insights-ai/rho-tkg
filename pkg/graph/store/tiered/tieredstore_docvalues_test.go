package tiered

import (
	"errors"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// drainTieredDocValues collects ForEachDocValues/ForEachDocValuesMulti output
// into a per-node value map, mirroring pkg/graph/docvalues_test.go's
// drainDocValues so the tiered arm exercises the exact same shape the
// core-level test suite already validates for memory/badger.
func drainTieredDocValues(t *testing.T,
	fold func(fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error)) (map[types.NodeID][]any, uint64, bool) {
	t.Helper()
	out := map[types.NodeID][]any{}
	gen, ok, err := fold(func(id types.NodeID, vals []any, present []bool) bool {
		row := make([]any, len(vals))
		for i := range vals {
			if present[i] {
				row[i] = vals[i]
			}
		}
		out[id] = row
		return true
	})
	if err != nil {
		t.Fatalf("DocValues fold: %v", err)
	}
	return out, gen, ok
}

// TestTieredDocValues_ForEachDocValuesCrossShardExactSet is the required
// cross-shard exact-set battery (ADR-0005 §3.4): a label with members on
// refShard AND on an event shard (via the extra-label mixed-class pattern
// already exercised by TestTieredStoreLabelReadsIncludeMixedClassExtraLabels)
// must emit every member exactly once — no omission, no duplication — and a
// node lacking the label must NOT appear.
func TestTieredDocValues_ForEachDocValuesCrossShardExactSet(t *testing.T) {
	ts, caseTok, signalTok := setupBatchDelete(t)
	gen := tieredNodeGen(t)

	refMember := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"score": int64(10)})
	eventMember := tieredLabelPropertyNodeWithExtras(t, types.NodeID(gen.Generate()), signalTok, []uint16{caseTok}, map[string]any{"score": int64(20)})
	nonMember := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), signalTok, map[string]any{"score": int64(30)})
	for _, n := range []*types.Node{refMember, eventMember, nonMember} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	rows, _, ok := drainTieredDocValues(t, func(fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error) {
		return ts.ForEachDocValues(caseTok, []string{"score"}, fn)
	})
	if !ok {
		t.Fatal("ForEachDocValues declined; want usable")
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2 (exact set: refMember + eventMember, NOT nonMember): %v", len(rows), rows)
	}
	if v, present := rows[refMember.ID()]; !present || v[0].(int64) != 10 {
		t.Fatalf("refMember row = %v, want [10]", v)
	}
	if v, present := rows[eventMember.ID()]; !present || v[0].(int64) != 20 {
		t.Fatalf("eventMember row = %v, want [20]", v)
	}
	if _, present := rows[nonMember.ID()]; present {
		t.Fatal("nonMember (Signal-only, no Case label) must NOT appear in the Case column")
	}
}

// TestTieredDocValues_ForEachDocValuesMultiCrossShardIntersection is the
// required label-intersection battery: a (:Case:Signal)-style pattern must
// match only nodes carrying BOTH labels, wherever they physically live
// (refShard vs. an event shard), and exclude nodes carrying only one.
func TestTieredDocValues_ForEachDocValuesMultiCrossShardIntersection(t *testing.T) {
	ts, caseTok, signalTok := setupBatchDelete(t)
	gen := tieredNodeGen(t)

	refPrimaryBoth := tieredLabelPropertyNodeWithExtras(t, types.NodeID(gen.Generate()), caseTok, []uint16{signalTok}, map[string]any{"score": int64(1)})
	eventPrimaryBoth := tieredLabelPropertyNodeWithExtras(t, types.NodeID(gen.Generate()), signalTok, []uint16{caseTok}, map[string]any{"score": int64(2)})
	caseOnly := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"score": int64(3)})
	signalOnly := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), signalTok, map[string]any{"score": int64(4)})
	for _, n := range []*types.Node{refPrimaryBoth, eventPrimaryBoth, caseOnly, signalOnly} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	rows, _, ok := drainTieredDocValues(t, func(fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error) {
		return ts.ForEachDocValuesMulti([]uint16{caseTok, signalTok}, []string{"score"}, fn)
	})
	if !ok {
		t.Fatal("ForEachDocValuesMulti declined; want usable")
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2 (exact intersection): %v", len(rows), rows)
	}
	if _, present := rows[refPrimaryBoth.ID()]; !present {
		t.Fatal("refPrimaryBoth (both labels, lives on refShard) missing from intersection")
	}
	if _, present := rows[eventPrimaryBoth.ID()]; !present {
		t.Fatal("eventPrimaryBoth (both labels, lives on an event shard) missing from intersection")
	}
	if _, present := rows[caseOnly.ID()]; present {
		t.Fatal("caseOnly must NOT be in the (Case ∩ Signal) intersection")
	}
	if _, present := rows[signalOnly.ID()]; present {
		t.Fatal("signalOnly must NOT be in the (Case ∩ Signal) intersection")
	}
}

// TestTieredDocValues_Gate2StalenessMidScan is the required Gate-2 battery: a
// mutation on ANY shard (here, a brand-new node landing on refShard, injected
// from inside the fn callback partway through the scan) must advance the
// aggregate NodeMutationEpoch so the consumer's post-scan gen comparison
// surfaces a mismatch and falls back rather than trusting a torn aggregate.
func TestTieredDocValues_Gate2StalenessMidScan(t *testing.T) {
	ts, caseTok, _ := setupBatchDelete(t)
	gen := tieredNodeGen(t)

	a := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"score": int64(1)})
	b := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"score": int64(2)})
	for _, n := range []*types.Node{a, b} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	injected := false
	genOut, ok, err := ts.ForEachDocValues(caseTok, []string{"score"}, func(id types.NodeID, vals []any, present []bool) bool {
		if !injected {
			injected = true
			extra := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"score": int64(99)})
			if err := ts.PutNode(extra); err != nil {
				t.Fatalf("mid-scan PutNode: %v", err)
			}
		}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachDocValues: %v", err)
	}
	if !ok {
		t.Fatal("ForEachDocValues declined; want usable")
	}
	if !injected {
		t.Fatal("test bug: the mid-scan mutation callback never fired")
	}
	if ts.NodeMutationEpoch() == genOut {
		t.Fatal("Gate-2: NodeMutationEpoch did not advance after a mid-scan mutation — a torn aggregate would go undetected by the consumer")
	}
}

// TestTieredDocValues_NodeMutationEpochSumsAcrossShards is the direct test for
// the new public NodeMutationEpoch method (Testing Rule 1): a mutation on the
// reference shard AND a mutation on an event shard must each independently
// advance the aggregate SUM, proving the fold is not silently anchored to a
// single shard's counter.
func TestTieredDocValues_NodeMutationEpochSumsAcrossShards(t *testing.T) {
	ts, caseTok, signalTok := setupBatchDelete(t)
	gen := tieredNodeGen(t)

	e0 := ts.NodeMutationEpoch()

	refNode := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"score": int64(1)})
	if err := ts.PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	e1 := ts.NodeMutationEpoch()
	if e1 == e0 {
		t.Fatal("a reference-shard mutation must advance the aggregate NodeMutationEpoch")
	}

	evtNode := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), signalTok, map[string]any{"score": int64(2)})
	if err := ts.PutNode(evtNode); err != nil {
		t.Fatalf("PutNode event: %v", err)
	}
	e2 := ts.NodeMutationEpoch()
	if e2 == e1 {
		t.Fatal("an event-shard mutation must ALSO advance the aggregate NodeMutationEpoch (not just the reference shard's)")
	}
}

// TestTieredDocValues_EmptyLabelDeclines mirrors the single-store "empty
// label" decline contract: a registered-but-unused label token reports
// ok=false (not an error) everywhere on tiered, exactly as it does on a
// single badger/memory store.
func TestTieredDocValues_EmptyLabelDeclines(t *testing.T) {
	ts := newTestTieredStore(t)
	_, userTok, _ := installDefaultTestLabelRegistry(t, ts)

	calls := 0
	gen, ok, err := ts.ForEachDocValues(userTok, []string{"score"}, func(types.NodeID, []any, []bool) bool {
		calls++
		return true
	})
	if err != nil {
		t.Fatalf("ForEachDocValues: %v", err)
	}
	if ok {
		t.Fatal("empty label must decline (ok=false), not report usable-with-zero-rows")
	}
	if gen != 0 {
		t.Fatalf("gen = %d, want 0 on decline", gen)
	}
	if calls != 0 {
		t.Fatalf("fn invoked %d times for an empty label, want 0", calls)
	}
}

// TestTieredDocValues_NumericAndStringColumnTypesPreserved pins the store-
// level contract the cypher sink relies on: numeric values keep their Go
// int64 dynamic type and string values decode as string, across a
// concatenated multi-shard column.
func TestTieredDocValues_NumericAndStringColumnTypesPreserved(t *testing.T) {
	ts, caseTok, _ := setupBatchDelete(t)
	gen := tieredNodeGen(t)

	a := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"city": "berlin", "age": int64(30)})
	b := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"city": "munich", "age": int64(40)})
	c := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"city": "berlin"}) // no age
	for _, n := range []*types.Node{a, b, c} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	rows, _, ok := drainTieredDocValues(t, func(fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error) {
		return ts.ForEachDocValues(caseTok, []string{"city", "age"}, fn)
	})
	if !ok {
		t.Fatal("column path declined; want usable")
	}
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3 (full membership)", len(rows))
	}
	if rows[a.ID()][0] != "berlin" || rows[a.ID()][1].(int64) != 30 {
		t.Fatalf("node a = %v, want [berlin 30]", rows[a.ID()])
	}
	if rows[b.ID()][1].(int64) != 40 {
		t.Fatalf("node b age = %v, want int64 40", rows[b.ID()][1])
	}
	if rows[c.ID()][1] != nil {
		t.Fatalf("node c age = %v, want nil (absent property still a row)", rows[c.ID()][1])
	}
}

// TestTieredDocValues_MatchesPerNodeFallback compares the columnar result
// against the per-node oracle (NodesByLabel + GetProperty) across a mix of
// refShard and event-shard members, including one member missing the
// property — the columnar path and the per-node path must agree exactly.
func TestTieredDocValues_MatchesPerNodeFallback(t *testing.T) {
	ts, caseTok, signalTok := setupBatchDelete(t)
	gen := tieredNodeGen(t)

	nodes := []*types.Node{
		tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"score": int64(1)}),
		tieredLabelPropertyNodeWithExtras(t, types.NodeID(gen.Generate()), signalTok, []uint16{caseTok}, map[string]any{"score": int64(2)}),
		tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"other": "x"}), // no score
	}
	for _, n := range nodes {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	oracle, err := ts.NodesByLabel(caseTok, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel oracle: %v", err)
	}
	wantRows := map[types.NodeID]any{}
	for _, n := range oracle {
		v, _ := n.GetProperty("score") // ok is false → v is nil, matching the column's absent shape
		wantRows[n.ID()] = v
	}

	gotRows, _, ok := drainTieredDocValues(t, func(fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error) {
		return ts.ForEachDocValues(caseTok, []string{"score"}, fn)
	})
	if !ok {
		t.Fatal("ForEachDocValues declined; want usable")
	}
	if len(gotRows) != len(wantRows) {
		t.Fatalf("columnar rows = %d, per-node oracle rows = %d — membership must agree", len(gotRows), len(wantRows))
	}
	for id, wantVal := range wantRows {
		got, present := gotRows[id]
		if !present {
			t.Fatalf("node %d in per-node oracle but missing from columnar result", id)
		}
		if got[0] != wantVal {
			t.Fatalf("node %d score = %v, per-node oracle = %v", id, got[0], wantVal)
		}
	}
}

// TestTieredDocValues_UnbuildableColumnWithNonzeroMembersForcesWholeDecline is
// the correctness safety net for the membership-vs-decline design: when a
// shard has PROVEN-nonzero members (via the O(1) count probe) but its own
// column build declines (mixed numeric/string values for the same key), the
// fold must decline the WHOLE tiered query rather than silently reporting
// ok=true with an incomplete row set. fn must never be invoked.
func TestTieredDocValues_UnbuildableColumnWithNonzeroMembersForcesWholeDecline(t *testing.T) {
	ts, caseTok, _ := setupBatchDelete(t)
	gen := tieredNodeGen(t)

	numeric := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"score": int64(1)})
	stringy := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"score": "two"})
	for _, n := range []*types.Node{numeric, stringy} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	calls := 0
	gen2, ok, err := ts.ForEachDocValues(caseTok, []string{"score"}, func(types.NodeID, []any, []bool) bool {
		calls++
		return true
	})
	if err != nil {
		t.Fatalf("ForEachDocValues: %v", err)
	}
	if ok {
		t.Fatal("a mixed-type column with proven-nonzero members must decline the whole query, not report ok=true")
	}
	if gen2 != 0 {
		t.Fatalf("gen = %d, want 0 on decline", gen2)
	}
	if calls != 0 {
		t.Fatalf("fn invoked %d times on decline — must never emit a partial/incomplete result", calls)
	}
}

// TestTieredDocValues_ColdShardParticipation is the required cold-shard
// battery: an event shard demoted to cold AND fully closed (mirroring
// setupClosedColdReadFanoutShard's pattern) must still contribute its rows —
// ForEachDocValues transiently lazy-opens it, exactly like every other
// tiered read fold (NodeCount, AllNodes, ...).
func TestTieredDocValues_ColdShardParticipation(t *testing.T) {
	ts := newDiskTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	signalTok, err := reg.GetOrCreate("Signal")
	if err != nil {
		t.Fatalf("GetOrCreate Signal: %v", err)
	}
	gen := tieredNodeGen(t)

	coldMember := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), signalTok, map[string]any{"score": int64(42)})
	if err := ts.PutNode(coldMember); err != nil {
		t.Fatalf("PutNode cold: %v", err)
	}

	coldName := ts.HotShardForTest().Name()
	forceRotation(t, ts)
	demoteToCold(ts, coldName)
	cold := ts.EventShardsForTest()[coldName]
	cold.LockShardMuForTest()
	if cold.Store() != nil {
		if err := cold.Store().Close(); err != nil {
			cold.UnlockShardMuForTest()
			t.Fatalf("close cold store: %v", err)
		}
		cold.SetStoreForTest(nil)
	}
	cold.UnlockShardMuForTest()

	hotMember := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), signalTok, map[string]any{"score": int64(99)})
	if err := ts.PutNode(hotMember); err != nil {
		t.Fatalf("PutNode hot: %v", err)
	}

	rows, _, ok := drainTieredDocValues(t, func(fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error) {
		return ts.ForEachDocValues(signalTok, []string{"score"}, fn)
	})
	if !ok {
		t.Fatal("ForEachDocValues declined; want usable")
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2 (cold-closed shard + new hot shard): %v", len(rows), rows)
	}
	if v, present := rows[coldMember.ID()]; !present || v[0].(int64) != 42 {
		t.Fatalf("coldMember row = %v, want [42] — cold-closed shard must be transiently reopened to serve its column", v)
	}
	if v, present := rows[hotMember.ID()]; !present || v[0].(int64) != 99 {
		t.Fatalf("hotMember row = %v, want [99]", v)
	}
}

// TestTieredDocValues_DocValuesSnapshotPointLookup is the required
// point-lookup battery for the random-access side (X5 expand-aggregation
// target): a snapshot spanning refShard and an event shard must resolve a
// member on EITHER shard and correctly report non-membership for a node
// carrying a different label.
func TestTieredDocValues_DocValuesSnapshotPointLookup(t *testing.T) {
	ts, caseTok, signalTok := setupBatchDelete(t)
	gen := tieredNodeGen(t)

	refMember := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"score": int64(10)})
	eventMember := tieredLabelPropertyNodeWithExtras(t, types.NodeID(gen.Generate()), signalTok, []uint16{caseTok}, map[string]any{"score": int64(20)})
	nonMember := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), signalTok, map[string]any{"score": int64(30)})
	for _, n := range []*types.Node{refMember, eventMember, nonMember} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	snap, gen0, ok, err := ts.DocValuesSnapshot(caseTok, []string{"score"})
	if err != nil {
		t.Fatalf("DocValuesSnapshot: %v", err)
	}
	if !ok {
		t.Fatal("DocValuesSnapshot declined; want usable")
	}
	if snap.Epoch() != gen0 {
		t.Fatalf("snap.Epoch() = %d, want %d (returned gen)", snap.Epoch(), gen0)
	}

	vals := make([]any, 1)
	present := make([]bool, 1)
	if !snap.Row(refMember.ID(), vals, present) || !present[0] || vals[0].(int64) != 10 {
		t.Fatalf("refMember Row = (vals=%v, present=%v), want (10, true)", vals, present)
	}
	if !snap.Row(eventMember.ID(), vals, present) || !present[0] || vals[0].(int64) != 20 {
		t.Fatalf("eventMember Row = (vals=%v, present=%v), want (20, true)", vals, present)
	}
	if snap.Row(nonMember.ID(), vals, present) {
		t.Fatal("nonMember (Signal-only) must NOT be reported as a Case-column member")
	}
}

// TestTieredDocValues_ForEachDocValuesIncludesArchivedReferenceEntities closes
// a coverage gap: no prior test exercised the archive-visit block of
// foldDocValues (the refArchive checkout in ForEachDocValues/
// ForEachDocValuesMulti/DocValuesSnapshot, ~:118-130). ArchiveNode moves a
// reference node from refShard to refArchive — this is NOT a delete, so the
// node is still a CURRENT member of its label. Sanity-checked against the
// sibling fold NodeCountByLabelAndPropertyKey (tieredstore_read_bulk.go),
// which ALSO folds in refArchive unconditionally: the two folds agree (no
// divergence to flag) — an archived reference entity counts in both.
func TestTieredDocValues_ForEachDocValuesIncludesArchivedReferenceEntities(t *testing.T) {
	ts, caseTok, _ := setupBatchDelete(t)
	gen := tieredNodeGen(t)

	live := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"score": int64(1)})
	archived := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"score": int64(2)})
	for _, n := range []*types.Node{live, archived} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}
	if err := ts.ArchiveNode(archived.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	// Cross-check: NodeCountByLabelAndPropertyKey (the sibling fold) must
	// agree that both members (live + archived) are counted.
	wantCount, err := ts.NodeCountByLabelAndPropertyKey(caseTok, "score")
	if err != nil {
		t.Fatalf("NodeCountByLabelAndPropertyKey: %v", err)
	}
	if wantCount != 2 {
		t.Fatalf("NodeCountByLabelAndPropertyKey = %d, want 2 (sanity oracle expects archived entities counted)", wantCount)
	}

	rows, _, ok := drainTieredDocValues(t, func(fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error) {
		return ts.ForEachDocValues(caseTok, []string{"score"}, fn)
	})
	if !ok {
		t.Fatal("ForEachDocValues declined; want usable")
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2 (live + archived Case member): %v", len(rows), rows)
	}
	if v, present := rows[live.ID()]; !present || v[0].(int64) != 1 {
		t.Fatalf("live row = %v, want [1]", v)
	}
	if v, present := rows[archived.ID()]; !present || v[0].(int64) != 2 {
		t.Fatalf("archived row = %v, want [2] — archived reference entities must be included (no divergence from NodeCountByLabelAndPropertyKey's archive fold)", v)
	}
}

// TestTieredDocValues_NodeMutationEpochArchiveFold closes the sibling coverage
// gap on NodeMutationEpoch's archive-fold branch: no prior test isolated a
// mutation that touches ONLY refArchive (every existing NodeMutationEpoch
// test only mutated refShard or an event shard). A refShard-only or
// event-shard-only mutation would already move the aggregate SUM even if the
// `if archive := ts.refArchive.Load(); archive != nil { sum +=
// archive.NodeMutationEpoch() }` line were deleted entirely — so this test
// mutates refArchive directly (bypassing the tiered Store's own doors) to
// prove the aggregate genuinely depends on the archive shard's own counter.
func TestTieredDocValues_NodeMutationEpochArchiveFold(t *testing.T) {
	ts, caseTok, _ := setupBatchDelete(t)
	gen := tieredNodeGen(t)

	owner := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"score": int64(1)})
	if err := ts.PutNode(owner); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.ArchiveNode(owner.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("refArchive not opened by ArchiveNode")
	}

	e0 := ts.NodeMutationEpoch()

	// Mutate refArchive DIRECTLY — refShard and every event shard are
	// untouched by this call, so any resulting SUM delta can only have come
	// from refArchive's own NodeMutationEpoch() contribution.
	extra := tieredLabelPropertyNode(t, types.NodeID(gen.Generate()), caseTok, map[string]any{"score": int64(2)})
	if err := archive.PutNode(extra); err != nil {
		t.Fatalf("direct archive PutNode: %v", err)
	}

	e1 := ts.NodeMutationEpoch()
	if e1 == e0 {
		t.Fatal("a refArchive-only mutation must advance the aggregate NodeMutationEpoch (archive-fold branch of NodeMutationEpoch)")
	}
}

// TestTieredDocValues_ClosedStoreErrors checks the sentinel-error contract
// (Testing Rule 4): every DocValues door on a closed tiered store surfaces
// ErrStoreClosed via errors.Is, and NodeMutationEpoch on a nil *Store is a
// safe 0 (mirroring badger/memory's bs==nil / ms==nil convention).
func TestTieredDocValues_ClosedStoreErrors(t *testing.T) {
	ts, caseTok, _ := setupBatchDelete(t)
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, _, err := ts.ForEachDocValues(caseTok, []string{"score"}, func(types.NodeID, []any, []bool) bool { return true }); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("ForEachDocValues on closed store = %v, want ErrStoreClosed", err)
	}
	if _, _, err := ts.ForEachDocValuesMulti([]uint16{caseTok}, []string{"score"}, func(types.NodeID, []any, []bool) bool { return true }); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("ForEachDocValuesMulti on closed store = %v, want ErrStoreClosed", err)
	}
	if _, _, _, err := ts.DocValuesSnapshot(caseTok, []string{"score"}); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("DocValuesSnapshot on closed store = %v, want ErrStoreClosed", err)
	}

	var nilStore *Store
	if got := nilStore.NodeMutationEpoch(); got != 0 {
		t.Fatalf("nil *Store NodeMutationEpoch() = %d, want 0", got)
	}
}
