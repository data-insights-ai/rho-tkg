package graph_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 21e / 20m — re-sharding via export/import.
//
// The sharded backend routes an entity to its shard by a PURE, IMMUTABLE
// function of its own ID (the 5-bit snowflake slot field, ADR-0007). That
// means an entity can never be "moved" to a different shard without changing
// its identity — which would break every relationship pointing at it, its
// hash chain, and external references. So there is no in-place "rebalance"
// primitive, and there does not need to be one: growing a store's SlotCount
// (a superset of the original claimed range) is safe with ZERO new routing
// code, because g.IO().Export / g.IO().Import already reconstructs every
// entity verbatim through the SAME store doors any other write uses — doors
// that route by the entity's own already-fixed slot. A shrink or BaseSlot
// rebase that would drop a non-empty slot fails CLOSED inside Import, with a
// full rollback, because the store door for the dropped slot returns
// ErrSlotNotLocal exactly like it would for any other misrouted write.
//
// These tests are the missing end-to-end proof of that claim (see the
// decision note next to sharded.Config.BaseSlot/SlotCount in sharded.go, and
// docs/adr/0007-horizontal-scaling.md) — not new production code.

// TestShardedResharding_GrowRoundTrip_ExactSet grows a 2-slot store into a
// 4-slot store via export/import and asserts an EXACT match: same live node
// and rel sets (by hash/version/labels — rule 16), full version-history depth
// preserved (multi-version + deleted-with-history), and a post-migration
// VerifyConsistency() report with ZERO ShardMismatches.
func TestShardedResharding_GrowRoundTrip_ExactSet(t *testing.T) {
	ctx := context.Background()

	sourceStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sourceStore: %v", err)
	}
	source, err := graph.New(graph.Config{SnowflakeNodeID: 0, Store: sourceStore})
	if err != nil {
		t.Fatalf("source graph.New: %v", err)
	}
	defer source.Close()

	// Nodes mint on slot 0 (even node field), rels on slot 1 (odd) — every rel
	// is inherently cross-shard from its endpoints under this backend.
	a := mustAdd(t, source, []string{"Thing"}, map[string]any{"n": "a"})
	b := mustAdd(t, source, []string{"Thing"}, map[string]any{"n": "b"})
	c := mustAdd(t, source, []string{"Thing"}, map[string]any{"n": "c"})
	if _, err := source.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"w": int64(1)}); err != nil {
		t.Fatalf("add rel a->b: %v", err)
	}
	rBC, err := source.Rels().AddByID(ctx, "KNOWS", b.ID(), c.ID(), map[string]any{"w": int64(2)})
	if err != nil {
		t.Fatalf("add rel b->c: %v", err)
	}

	// Multi-version history: two updates on 'a'.
	if _, err := source.Nodes().Update(ctx, a.ID(), map[string]any{"n": "a2"}); err != nil {
		t.Fatalf("update a v2: %v", err)
	}
	if _, err := source.Nodes().Update(ctx, a.ID(), map[string]any{"n": "a3"}); err != nil {
		t.Fatalf("update a v3: %v", err)
	}
	if _, err := source.Rels().Update(ctx, rBC.ID(), map[string]any{"w": int64(3)}); err != nil {
		t.Fatalf("update rel b->c: %v", err)
	}

	// Deleted-with-history entity: history must remain queryable (B32) and
	// must survive the migration even though the entity is not "live".
	d := mustAdd(t, source, []string{"Thing"}, map[string]any{"n": "d"})
	if _, err := source.Nodes().Update(ctx, d.ID(), map[string]any{"n": "d2"}); err != nil {
		t.Fatalf("update d: %v", err)
	}
	if err := source.Nodes().Delete(ctx, d.ID()); err != nil {
		t.Fatalf("delete d: %v", err)
	}

	aHistBefore := mustNodeHistory(t, source, a.ID())
	if len(aHistBefore) < 2 {
		t.Fatalf("precondition: node a should have >= 2 history rows (one per update), got %d", len(aHistBefore))
	}
	dHistBefore := mustNodeHistory(t, source, d.ID())
	if len(dHistBefore) < 2 {
		t.Fatalf("precondition: node d should have >= 2 history rows, got %d", len(dHistBefore))
	}

	var buf bytes.Buffer
	if err := source.IO().Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}

	targetStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 4})
	if err != nil {
		t.Fatalf("targetStore: %v", err)
	}
	target, err := graph.New(graph.Config{SnowflakeNodeID: 0, Store: targetStore})
	if err != nil {
		t.Fatalf("target graph.New: %v", err)
	}
	defer target.Close()

	if err := target.IO().Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import into grown topology: %v", err)
	}

	// Exact-set + full-history-parity check (reuses the replica-convergence
	// assertion — Export/Import across a topology change must reproduce the
	// source exactly, the same guarantee a replica bootstrap makes).
	assertConverged(t, "grow round-trip", source, target)

	// Explicit sanity: the migrated history is genuinely multi-version, not
	// coincidentally matching on trivially-short chains.
	aHistAfter := mustNodeHistory(t, target, a.ID())
	if len(aHistAfter) != len(aHistBefore) {
		t.Fatalf("node a history depth: source %d, target %d", len(aHistBefore), len(aHistAfter))
	}
	dHistAfter := mustNodeHistory(t, target, d.ID())
	if len(dHistAfter) != len(dHistBefore) {
		t.Fatalf("node d (deleted) history depth: source %d, target %d", len(dHistBefore), len(dHistAfter))
	}
	if _, err := target.Nodes().Get(ctx, d.ID()); err == nil {
		t.Fatalf("node d should still be deleted (no current row) after migration")
	}

	report, err := targetStore.VerifyConsistency()
	if err != nil {
		t.Fatalf("VerifyConsistency: %v", err)
	}
	if !report.OK() {
		t.Fatalf("post-migration VerifyConsistency not OK: %+v", report)
	}
	if len(report.ShardMismatches) != 0 {
		t.Fatalf("post-migration ShardMismatches: %+v", report.ShardMismatches)
	}
}

// TestShardedResharding_TwoPhaseTemporalSurvival is the rule-15 two-phase
// test for the migration path: an entity in state X at t0, mutated after t0,
// must still resolve to X at t0 AFTER a grow-topology export/import.
func TestShardedResharding_TwoPhaseTemporalSurvival(t *testing.T) {
	ctx := context.Background()

	sourceStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sourceStore: %v", err)
	}
	source, err := graph.New(graph.Config{SnowflakeNodeID: 0, Store: sourceStore})
	if err != nil {
		t.Fatalf("source graph.New: %v", err)
	}
	defer source.Close()

	n, err := source.Nodes().Add(ctx, []string{"Widget"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"state":          "X",
	})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if _, err := source.Nodes().Update(ctx, n.ID(), map[string]any{
		"tkg_valid_from": types.Instant(2000),
		"state":          "Y",
	}); err != nil {
		t.Fatalf("update (state X -> Y at t=2000): %v", err)
	}

	var buf bytes.Buffer
	if err := source.IO().Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}

	targetStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 4})
	if err != nil {
		t.Fatalf("targetStore: %v", err)
	}
	target, err := graph.New(graph.Config{SnowflakeNodeID: 0, Store: targetStore})
	if err != nil {
		t.Fatalf("target graph.New: %v", err)
	}
	defer target.Close()
	if err := target.IO().Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import into grown topology: %v", err)
	}

	before, err := target.Temporal().NodeAt(n.ID(), 1500)
	if err != nil {
		t.Fatalf("NodeAt(t=1500) after migration: %v", err)
	}
	if v, ok := before.Properties().Get("state"); !ok || v != "X" {
		t.Fatalf("NodeAt(t=1500) after migration: state = %v (ok=%v), want X", v, ok)
	}

	after, err := target.Temporal().NodeAt(n.ID(), 2500)
	if err != nil {
		t.Fatalf("NodeAt(t=2500) after migration: %v", err)
	}
	if v, ok := after.Properties().Get("state"); !ok || v != "Y" {
		t.Fatalf("NodeAt(t=2500) after migration: state = %v (ok=%v), want Y", v, ok)
	}
}

// TestShardedResharding_HashChainIntegrityPostMigration asserts every
// migrated entity's hash chain verifies at the GRAPH level (not just
// Import's own internal recompute-and-compare check) after a grow.
func TestShardedResharding_HashChainIntegrityPostMigration(t *testing.T) {
	ctx := context.Background()

	sourceStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sourceStore: %v", err)
	}
	source, err := graph.New(graph.Config{SnowflakeNodeID: 0, Store: sourceStore})
	if err != nil {
		t.Fatalf("source graph.New: %v", err)
	}
	defer source.Close()

	a := mustAdd(t, source, []string{"Thing"}, map[string]any{"n": "a"})
	b := mustAdd(t, source, []string{"Thing"}, map[string]any{"n": "b"})
	for i := 0; i < 3; i++ {
		if _, err := source.Nodes().Update(ctx, a.ID(), map[string]any{"n": i}); err != nil {
			t.Fatalf("update a v%d: %v", i+2, err)
		}
	}
	r, err := source.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatalf("add rel: %v", err)
	}
	if _, err := source.Rels().Update(ctx, r.ID(), map[string]any{"w": int64(2)}); err != nil {
		t.Fatalf("update rel: %v", err)
	}

	var buf bytes.Buffer
	if err := source.IO().Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}

	targetStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 4})
	if err != nil {
		t.Fatalf("targetStore: %v", err)
	}
	target, err := graph.New(graph.Config{SnowflakeNodeID: 0, Store: targetStore})
	if err != nil {
		t.Fatalf("target graph.New: %v", err)
	}
	defer target.Close()
	if err := target.IO().Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import into grown topology: %v", err)
	}

	for _, id := range []types.NodeID{a.ID(), b.ID()} {
		ok, err := target.Hash().VerifyNodeChain(id)
		if err != nil {
			t.Fatalf("VerifyNodeChain(%v): %v", id, err)
		}
		if !ok {
			t.Fatalf("VerifyNodeChain(%v) = false after migration", id)
		}
	}
	if ok, err := target.Hash().VerifyRelChain(r.ID()); err != nil || !ok {
		t.Fatalf("VerifyRelChain(%v) = (%v, %v) after migration, want (true, nil)", r.ID(), ok, err)
	}
}

// TestShardedResharding_UnsafeShrinkFailsClosed proves a shrink that would
// drop a NON-EMPTY slot fails closed via ErrSlotNotLocal, with a genuine
// mid-migration rollback (one entity's slot IS covered by the target and
// gets written successfully before a later entity's slot is found
// uncovered — this is not a pre-check rejecting obviously-bad input, it is
// Import's rollback undoing real partial work), and leaves both the target
// EMPTY and the source untouched.
func TestShardedResharding_UnsafeShrinkFailsClosed(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: SnowflakeNodeID 0 -> node slot 0 (covered by a future 2-slot
	// target). Persisted to disk so a second Core can reopen with a
	// different SnowflakeNodeID and mint into a DIFFERENT slot while sharing
	// the same (now on-disk) registries — avoiding two independently-minted,
	// possibly-divergent in-memory label registries.
	store1, err := sharded.New(sharded.Config{Dir: dir, BaseSlot: 0, SlotCount: 4})
	if err != nil {
		t.Fatalf("store1: %v", err)
	}
	g1, err := graph.New(graph.Config{SnowflakeNodeID: 0, Store: store1})
	if err != nil {
		t.Fatalf("g1 graph.New: %v", err)
	}
	covered := mustAdd(t, g1, []string{"Thing"}, map[string]any{"n": "covered"})
	if err := g1.Close(); err != nil {
		t.Fatalf("g1.Close: %v", err)
	}

	// Phase 2: reopen the SAME store directory with SnowflakeNodeID 1 ->
	// node slot 2, which a fresh 2-slot (base 0) target will NOT cover.
	store2, err := sharded.New(sharded.Config{Dir: dir, BaseSlot: 0, SlotCount: 4})
	if err != nil {
		t.Fatalf("store2 (reopen): %v", err)
	}
	g2, err := graph.New(graph.Config{SnowflakeNodeID: 1, Store: store2})
	if err != nil {
		t.Fatalf("g2 graph.New: %v", err)
	}
	defer g2.Close()
	uncovered := mustAdd(t, g2, []string{"Thing"}, map[string]any{"n": "uncovered"})
	_ = uncovered

	nodesBefore, err := g2.Nodes().All(graph.QueryOpts{})
	if err != nil {
		t.Fatalf("source Nodes().All before: %v", err)
	}
	if len(nodesBefore) != 2 {
		t.Fatalf("precondition: expected 2 source nodes (covered+uncovered), got %d", len(nodesBefore))
	}
	// Sanity: 'covered' really is on a lower snowflake ID than 'uncovered',
	// so export/import processes it FIRST — proving the partial-success case.
	if covered.ID().SnowflakeID() >= uncovered.ID().SnowflakeID() {
		t.Fatalf("precondition: covered node ID must sort before uncovered node ID for the mid-migration proof")
	}

	var buf bytes.Buffer
	if err := g2.IO().Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}

	targetStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("targetStore: %v", err)
	}
	target, err := graph.New(graph.Config{SnowflakeNodeID: 0, Store: targetStore})
	if err != nil {
		t.Fatalf("target graph.New: %v", err)
	}
	defer target.Close()

	err = target.IO().Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{})
	if err == nil {
		t.Fatal("Import into a shrunk topology missing a non-empty slot should fail, got nil")
	}
	if !errors.Is(err, sharded.ErrSlotNotLocal) {
		t.Fatalf("Import error = %v, want errors.Is(..., sharded.ErrSlotNotLocal)", err)
	}

	// Target must be left EMPTY — the 'covered' node genuinely landed on
	// target's shard 0 before the 'uncovered' node's PutNode failed, so this
	// asserts rollback actually deleted it, not merely that nothing was ever
	// written.
	targetNodes, err := target.Nodes().All(graph.QueryOpts{})
	if err != nil {
		t.Fatalf("target Nodes().All after failed import: %v", err)
	}
	if len(targetNodes) != 0 {
		t.Fatalf("target should be empty after rollback, got %d nodes: %+v", len(targetNodes), targetNodes)
	}
	nc, err := targetStore.NodeCount()
	if err != nil {
		t.Fatalf("targetStore.NodeCount: %v", err)
	}
	if nc != 0 {
		t.Fatalf("targetStore.NodeCount after rollback = %d, want 0", nc)
	}

	// Source must be untouched — Export is read-only and Import only ever
	// mutates its own target, but assert it explicitly per the design's spec.
	nodesAfter, err := g2.Nodes().All(graph.QueryOpts{})
	if err != nil {
		t.Fatalf("source Nodes().All after: %v", err)
	}
	if len(nodesAfter) != len(nodesBefore) {
		t.Fatalf("source node count changed: before %d, after %d", len(nodesBefore), len(nodesAfter))
	}
}

// TestShardedResharding_EmptySlotShrinkSucceeds proves the converse of the
// unsafe-shrink test: shrinking to a topology that still covers every slot
// actually IN USE succeeds cleanly, even though the source store nominally
// claimed a wider range.
func TestShardedResharding_EmptySlotShrinkSucceeds(t *testing.T) {
	ctx := context.Background()

	// Source claims 4 slots, but SnowflakeNodeID 0 only ever mints into
	// slots 0 (nodes) and 1 (rels) — slots 2-3 stay empty.
	sourceStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 4})
	if err != nil {
		t.Fatalf("sourceStore: %v", err)
	}
	source, err := graph.New(graph.Config{SnowflakeNodeID: 0, Store: sourceStore})
	if err != nil {
		t.Fatalf("source graph.New: %v", err)
	}
	defer source.Close()

	a := mustAdd(t, source, []string{"Thing"}, map[string]any{"n": "a"})
	b := mustAdd(t, source, []string{"Thing"}, map[string]any{"n": "b"})
	if _, err := source.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"w": int64(1)}); err != nil {
		t.Fatalf("add rel: %v", err)
	}

	var buf bytes.Buffer
	if err := source.IO().Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}

	// Target only claims 2 slots — covers 0 and 1, which is everything that
	// is actually in use.
	targetStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("targetStore: %v", err)
	}
	target, err := graph.New(graph.Config{SnowflakeNodeID: 0, Store: targetStore})
	if err != nil {
		t.Fatalf("target graph.New: %v", err)
	}
	defer target.Close()

	if err := target.IO().Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import into narrower-but-sufficient topology: %v", err)
	}

	assertConverged(t, "empty-slot shrink", source, target)

	report, err := targetStore.VerifyConsistency()
	if err != nil {
		t.Fatalf("VerifyConsistency: %v", err)
	}
	if !report.OK() {
		t.Fatalf("post-migration VerifyConsistency not OK: %+v", report)
	}
}

// TestShardedResharding_BaseSlotRebaseFailsClosed is the BaseSlot analogue
// of the unsafe-shrink test: importing into a topology whose claimed range
// does not cover the source's slots at all (a pure rebase, not a shrink)
// fails closed the same way.
func TestShardedResharding_BaseSlotRebaseFailsClosed(t *testing.T) {
	sourceStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sourceStore: %v", err)
	}
	source, err := graph.New(graph.Config{SnowflakeNodeID: 0, Store: sourceStore})
	if err != nil {
		t.Fatalf("source graph.New: %v", err)
	}
	defer source.Close()

	mustAdd(t, source, []string{"Thing"}, map[string]any{"n": "a"})

	var buf bytes.Buffer
	if err := source.IO().Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}

	// Target claims slots 4-5 (SlotCount 2, BaseSlot 4) — none of the
	// source's slots (0-1) are covered.
	targetStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 4, SlotCount: 2})
	if err != nil {
		t.Fatalf("targetStore: %v", err)
	}
	target, err := graph.New(graph.Config{SnowflakeNodeID: 2, Store: targetStore})
	if err != nil {
		t.Fatalf("target graph.New: %v", err)
	}
	defer target.Close()

	err = target.IO().Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{})
	if err == nil {
		t.Fatal("Import into a rebased topology covering none of the source's slots should fail, got nil")
	}
	if !errors.Is(err, sharded.ErrSlotNotLocal) {
		t.Fatalf("Import error = %v, want errors.Is(..., sharded.ErrSlotNotLocal)", err)
	}

	targetNodes, err := target.Nodes().All(graph.QueryOpts{})
	if err != nil {
		t.Fatalf("target Nodes().All after failed import: %v", err)
	}
	if len(targetNodes) != 0 {
		t.Fatalf("target should be empty after rollback, got %d nodes", len(targetNodes))
	}
}

// TestShardedResharding_ChangeLogSourceWatermarkHandoff exercises Export /
// Import across a topology grow with the SOURCE's change-log enabled: the
// export header's SnapshotLSN must carry over so a receiving graph can
// record it as its applied watermark, and the migrated data must be intact.
func TestShardedResharding_ChangeLogSourceWatermarkHandoff(t *testing.T) {
	ctx := context.Background()

	sourceStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2, ChangeLog: true})
	if err != nil {
		t.Fatalf("sourceStore: %v", err)
	}
	source, err := graph.New(graph.Config{SnowflakeNodeID: 0, Store: sourceStore})
	if err != nil {
		t.Fatalf("source graph.New: %v", err)
	}
	defer source.Close()

	a := mustAdd(t, source, []string{"Thing"}, map[string]any{"n": "a"})
	b := mustAdd(t, source, []string{"Thing"}, map[string]any{"n": "b"})
	if _, err := source.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"w": int64(1)}); err != nil {
		t.Fatalf("add rel: %v", err)
	}

	sourceLSN, err := source.Replication().LastCommittedLSN()
	if err != nil {
		t.Fatalf("source LastCommittedLSN: %v", err)
	}
	if sourceLSN == 0 {
		t.Fatal("precondition: source should have a nonzero committed LSN")
	}

	var buf bytes.Buffer
	if err := source.IO().Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}

	hdr, err := tkgio.HeaderOf(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("HeaderOf: %v", err)
	}
	if hdr.To.LSN != sourceLSN {
		t.Fatalf("export header SnapshotLSN = %d, want source LastCommittedLSN %d", hdr.To.LSN, sourceLSN)
	}

	targetStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 4})
	if err != nil {
		t.Fatalf("targetStore: %v", err)
	}
	target, err := graph.New(graph.Config{SnowflakeNodeID: 0, Store: targetStore, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("target graph.New: %v", err)
	}
	defer target.Close()

	if err := target.IO().Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import into grown topology: %v", err)
	}
	if err := target.Replication().SetAppliedLSN(hdr.To.LSN); err != nil {
		t.Fatalf("SetAppliedLSN: %v", err)
	}

	applied, err := target.Replication().AppliedLSN()
	if err != nil {
		t.Fatalf("AppliedLSN: %v", err)
	}
	if applied != sourceLSN {
		t.Fatalf("target AppliedLSN = %d, want %d", applied, sourceLSN)
	}

	assertConverged(t, "change-log source watermark handoff", source, target)
}
