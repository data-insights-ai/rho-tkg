package sharded

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// S2 battery (ADR-0007): batched doors, cross-shard cascade delete, and the
// VerifyConsistency crash-window diagnosis door. S1 covered point CRUD, folds,
// and history; these tests cover the code salvaged into batch.go / node.go /
// verify.go, which shipped without direct coverage.

// --- Batched node puts ---

func TestPutNodesBatchCrossShard(t *testing.T) {
	st := newMemStore(t, 0, 4)
	// Eight nodes, two per slot — one batch call must land them on four shards.
	var ids []types.NodeID
	var nodes []*types.Node
	for slot := uint8(0); slot < 4; slot++ {
		for n := int64(1); n <= 2; n++ {
			id := mkNodeID(slot, n)
			ids = append(ids, id)
			nodes = append(nodes, types.NewNode(id, 10, nil))
		}
	}
	if err := st.PutNodesBatch(nodes); err != nil {
		t.Fatalf("PutNodesBatch: %v", err)
	}
	for _, id := range ids {
		if _, err := st.GetNode(id); err != nil {
			t.Fatalf("GetNode(%d) after batch: %v", id, err)
		}
	}
	if n, _ := st.NodeCount(); n != len(ids) {
		t.Fatalf("NodeCount = %d, want %d", n, len(ids))
	}

	// Duplicate ID within one batch is rejected wholesale (validate-all-first).
	dupID := mkNodeID(1, 5)
	dupBatch := []*types.Node{types.NewNode(dupID, 10, nil), types.NewNode(dupID, 10, nil)}
	if err := st.PutNodesBatch(dupBatch); err == nil {
		t.Fatalf("PutNodesBatch with in-batch duplicate ID: want error, got nil")
	}
	// The whole batch was rejected before any write — the ID must not exist.
	if _, err := st.GetNode(dupID); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("failed batch must not have written: GetNode = %v", err)
	}

	// Already-present node is rejected with ErrNodeExists.
	if err := st.PutNodesBatch([]*types.Node{types.NewNode(ids[0], 10, nil)}); !errors.Is(err, storecontract.ErrNodeExists) {
		t.Fatalf("PutNodesBatch already-present: want ErrNodeExists, got %v", err)
	}

	// Unclaimed slot fails closed with ErrSlotNotLocal.
	st2 := newMemStore(t, 0, 2)
	if err := st2.PutNodesBatch([]*types.Node{types.NewNode(mkNodeID(3, 1), 10, nil)}); !errors.Is(err, ErrSlotNotLocal) {
		t.Fatalf("PutNodesBatch unclaimed slot: want ErrSlotNotLocal, got %v", err)
	}
}

// TestPutNodesBatchPreEncodedAlignment targets the silent-wrong-answer class the
// ADR flags: the parallel wireBodies array must stay index-aligned with its
// nodes as they are partitioned into per-shard groups. Each node carries a
// DISTINCT TxFrom patched into its own pre-encoded body; if slicing dropped
// alignment, a node would read back another node's TxFrom. A nil body in the
// middle must fall back to encode-at-door (still correct).
func TestPutNodesBatchPreEncodedAlignment(t *testing.T) {
	st := newMemStore(t, 0, 4)
	var nodes []*types.Node
	var wire [][]byte
	txOf := func(i int) types.Instant { return types.Instant(1000 + int64(i)) }
	i := 0
	for slot := uint8(0); slot < 4; slot++ {
		for n := int64(1); n <= 2; n++ {
			nd := types.NewNode(mkNodeID(slot, n), 10, nil)
			nd.SetTemporal(&types.TemporalMetadata{TxFrom: txOf(i)})
			nodes = append(nodes, nd)
			// Node at index 3 uses a nil body: exercises encode-at-door fallback.
			if i == 3 {
				wire = append(wire, nil)
			} else {
				buf, err := storeutil.PreEncodeNodeWireV2WithKeys(nd, st.propKeyReg)
				if err != nil {
					t.Fatalf("pre-encode %d: %v", i, err)
				}
				if err := storeutil.PatchWireTemporalTail(buf, int64(txOf(i)), 0); err != nil {
					t.Fatalf("patch tail %d: %v", i, err)
				}
				wire = append(wire, buf)
			}
			i++
		}
	}
	if err := st.PutNodesBatchPreEncoded(nodes, wire); err != nil {
		t.Fatalf("PutNodesBatchPreEncoded: %v", err)
	}
	// Each node must read back with ITS OWN TxFrom — alignment preserved.
	for idx, nd := range nodes {
		got, err := st.GetNode(nd.ID())
		if err != nil {
			t.Fatalf("GetNode(%d): %v", nd.ID(), err)
		}
		if got.Temporal().TxFrom != txOf(idx) {
			t.Fatalf("node %d (id %d): TxFrom = %d, want %d (alignment broken?)",
				idx, nd.ID(), got.Temporal().TxFrom, txOf(idx))
		}
	}
}

// TestPutNodesBatchPreEncodedLogAlignment exercises the three-array pre-encoded
// door (nodes ‖ wireBodies ‖ logBodies). All three arrays must stay index-aligned
// through per-shard partitioning. Change-log is off on this store, so the log
// bodies are carried but unused by the shard; the assertion is that the entity
// rows still round-trip with their own patched TxFrom (alignment preserved).
func TestPutNodesBatchPreEncodedLogAlignment(t *testing.T) {
	st := newMemStore(t, 0, 4)
	var nodes []*types.Node
	var wire, logs [][]byte
	txOf := func(i int) types.Instant { return types.Instant(2000 + int64(i)) }
	i := 0
	for slot := uint8(0); slot < 4; slot++ {
		for n := int64(1); n <= 2; n++ {
			nd := types.NewNode(mkNodeID(slot, n), 10, nil)
			nd.SetTemporal(&types.TemporalMetadata{TxFrom: txOf(i)})
			nodes = append(nodes, nd)

			wbuf, err := storeutil.PreEncodeNodeWireV2WithKeys(nd, st.propKeyReg)
			if err != nil {
				t.Fatalf("pre-encode wire %d: %v", i, err)
			}
			if err := storeutil.PatchWireTemporalTail(wbuf, int64(txOf(i)), 0); err != nil {
				t.Fatalf("patch wire tail %d: %v", i, err)
			}
			lbuf, err := storeutil.PreEncodeNodePutPayloadV2(nd)
			if err != nil {
				t.Fatalf("pre-encode log %d: %v", i, err)
			}
			if err := storeutil.PatchWireTemporalTail(lbuf, int64(txOf(i)), 0); err != nil {
				t.Fatalf("patch log tail %d: %v", i, err)
			}
			wire = append(wire, wbuf)
			logs = append(logs, lbuf)
			i++
		}
	}
	if err := st.PutNodesBatchPreEncodedLog(nodes, wire, logs); err != nil {
		t.Fatalf("PutNodesBatchPreEncodedLog: %v", err)
	}
	for idx, nd := range nodes {
		got, err := st.GetNode(nd.ID())
		if err != nil {
			t.Fatalf("GetNode(%d): %v", nd.ID(), err)
		}
		if got.Temporal().TxFrom != txOf(idx) {
			t.Fatalf("node %d: TxFrom = %d, want %d (alignment broken?)", idx, got.Temporal().TxFrom, txOf(idx))
		}
	}
}

// --- Batched relationship puts ---

func TestPutRelationshipsBatchCrossShard(t *testing.T) {
	st := newMemStore(t, 0, 4)
	// Endpoints on slots 0 and 1.
	a := mkNodeID(0, 1)
	b := mkNodeID(1, 1)
	putNode(t, st, a, 10)
	putNode(t, st, b, 10)

	// Rels spread across rel slots (co-located with the rel ID's shard), all a->b.
	var rels []*types.Relationship
	var relIDs []types.RelID
	for slot := uint8(0); slot < 4; slot++ {
		rid := mkRelID(slot, 100)
		relIDs = append(relIDs, rid)
		rels = append(rels, types.NewRelationship(rid, 5, a, b))
	}
	if err := st.PutRelationshipsBatch(rels); err != nil {
		t.Fatalf("PutRelationshipsBatch: %v", err)
	}
	// Adjacency reflects the batch: outgoing from a = all four; incoming to b = all four.
	out, err := st.OutgoingRelationships(a, 0)
	if err != nil {
		t.Fatalf("OutgoingRelationships(a): %v", err)
	}
	assertRelSet(t, out, relIDs...)
	in, err := st.IncomingRelationships(b, 0)
	if err != nil {
		t.Fatalf("IncomingRelationships(b): %v", err)
	}
	assertRelSet(t, in, relIDs...)

	// Duplicate rel ID within one batch → ErrRelExists, nothing written.
	dup := mkRelID(2, 200)
	dupBatch := []*types.Relationship{
		types.NewRelationship(dup, 5, a, b),
		types.NewRelationship(dup, 5, a, b),
	}
	if err := st.PutRelationshipsBatch(dupBatch); !errors.Is(err, ErrRelExists) {
		t.Fatalf("PutRelationshipsBatch in-batch dup: want ErrRelExists, got %v", err)
	}
	if _, err := st.GetRelationship(dup); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("failed rel batch must not have written: GetRelationship = %v", err)
	}

	// Missing endpoint → ErrNodeNotFound, whole batch rejected.
	missing := mkNodeID(1, 999)
	if err := st.PutRelationshipsBatch([]*types.Relationship{
		types.NewRelationship(mkRelID(3, 300), 5, a, missing),
	}); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("PutRelationshipsBatch missing endpoint: want ErrNodeNotFound, got %v", err)
	}

	// Already-present rel → ErrRelExists.
	if err := st.PutRelationshipsBatch([]*types.Relationship{
		types.NewRelationship(relIDs[0], 5, a, b),
	}); !errors.Is(err, ErrRelExists) {
		t.Fatalf("PutRelationshipsBatch already-present: want ErrRelExists, got %v", err)
	}
}

// --- Batched deletes ---

func TestDeleteNodesBatchCrossShard(t *testing.T) {
	st := newMemStore(t, 0, 4)
	var ids []types.NodeID
	for slot := uint8(0); slot < 4; slot++ {
		id := mkNodeID(slot, 1)
		putNode(t, st, id, 10)
		ids = append(ids, id)
	}
	// A connected node must NOT be deletable via the unconnected-only batch.
	connected := mkNodeID(0, 5)
	other := mkNodeID(1, 5)
	putNode(t, st, connected, 10)
	putNode(t, st, other, 10)
	putRel(t, st, mkRelID(2, 5), 5, connected, other)
	if err := st.DeleteNodesBatch([]types.NodeID{connected}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNodesBatch connected: want ErrInvalidStoreMutation, got %v", err)
	}
	// The rejected batch wrote nothing — the node survives.
	if _, err := st.GetNode(connected); err != nil {
		t.Fatalf("rejected batch must not delete: GetNode(connected) = %v", err)
	}

	// Unconnected multi-shard delete (with a duplicate ID coalesced).
	del := append(ids, ids[0]) // duplicate ids[0]
	if err := st.DeleteNodesBatch(del); err != nil {
		t.Fatalf("DeleteNodesBatch: %v", err)
	}
	for _, id := range ids {
		if _, err := st.GetNode(id); !errors.Is(err, ErrNodeNotFound) {
			t.Fatalf("GetNode(%d) after batch delete: want ErrNodeNotFound, got %v", id, err)
		}
	}

	// Missing node → ErrNodeNotFound.
	if err := st.DeleteNodesBatch([]types.NodeID{mkNodeID(3, 42)}); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("DeleteNodesBatch missing: want ErrNodeNotFound, got %v", err)
	}
}

func TestDeleteRelationshipsBatchCrossShard(t *testing.T) {
	st := newMemStore(t, 0, 4)
	a := mkNodeID(0, 1)
	b := mkNodeID(1, 1)
	putNode(t, st, a, 10)
	putNode(t, st, b, 10)
	var relIDs []types.RelID
	for slot := uint8(0); slot < 4; slot++ {
		rid := mkRelID(slot, 1)
		putRel(t, st, rid, 5, a, b)
		relIDs = append(relIDs, rid)
	}
	if err := st.DeleteRelationshipsBatch(relIDs); err != nil {
		t.Fatalf("DeleteRelationshipsBatch: %v", err)
	}
	for _, rid := range relIDs {
		if _, err := st.GetRelationship(rid); !errors.Is(err, ErrRelNotFound) {
			t.Fatalf("GetRelationship(%d) after delete: want ErrRelNotFound, got %v", rid, err)
		}
	}
	// Adjacency fully cleaned across shards.
	out, err := st.OutgoingRelationships(a, 0)
	if err != nil {
		t.Fatalf("OutgoingRelationships(a): %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("outgoing adjacency not cleaned: %d remain", len(out))
	}

	// Missing rel → ErrRelNotFound.
	if err := st.DeleteRelationshipsBatch([]types.RelID{mkRelID(2, 99)}); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("DeleteRelationshipsBatch missing: want ErrRelNotFound, got %v", err)
	}
}

// --- Cross-shard cascade delete (ADR-0007 Risk 1) ---

func TestDeleteNodeCascadeCrossShard(t *testing.T) {
	st := newMemStore(t, 0, 4)
	// Hub node on slot 0; neighbors on slots 1 and 2.
	hub := mkNodeID(0, 1)
	n1 := mkNodeID(1, 1)
	n2 := mkNodeID(2, 1)
	putNode(t, st, hub, 10)
	putNode(t, st, n1, 10)
	putNode(t, st, n2, 10)

	// Rels spanning multiple shards, in BOTH directions, plus a self-loop.
	rOut1 := mkRelID(1, 1) // hub -> n1, rel row on slot 1
	rOut2 := mkRelID(3, 1) // hub -> n2, rel row on slot 3
	rIn := mkRelID(2, 1)   // n1 -> hub, rel row on slot 2
	rSelf := mkRelID(0, 9) // hub -> hub, self-loop (appears in both folds)
	putRel(t, st, rOut1, 5, hub, n1)
	putRel(t, st, rOut2, 5, hub, n2)
	putRel(t, st, rIn, 5, n1, hub)
	putRel(t, st, rSelf, 5, hub, hub)

	if err := st.DeleteNodeCascade(hub); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}
	// Hub gone.
	if _, err := st.GetNode(hub); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode(hub) after cascade: want ErrNodeNotFound, got %v", err)
	}
	// Every connected rel gone (self-loop deduped — deleted once, no error).
	for _, rid := range []types.RelID{rOut1, rOut2, rIn, rSelf} {
		if _, err := st.GetRelationship(rid); !errors.Is(err, ErrRelNotFound) {
			t.Fatalf("GetRelationship(%d) after cascade: want ErrRelNotFound, got %v", rid, err)
		}
	}
	// Neighbors survive, and their adjacency to the hub is cleaned.
	for _, nb := range []types.NodeID{n1, n2} {
		if _, err := st.GetNode(nb); err != nil {
			t.Fatalf("neighbor %d must survive: %v", nb, err)
		}
		in, err := st.IncomingRelationships(nb, 0)
		if err != nil {
			t.Fatalf("IncomingRelationships(%d): %v", nb, err)
		}
		if len(in) != 0 {
			t.Fatalf("neighbor %d still has %d incoming rels after cascade", nb, len(in))
		}
	}
	// The store is consistent after the cascade — no dangling references.
	rep, err := st.VerifyConsistency()
	if err != nil {
		t.Fatalf("VerifyConsistency: %v", err)
	}
	if !rep.OK() {
		t.Fatalf("post-cascade store inconsistent: %+v", rep)
	}
}

// --- VerifyConsistency ---

func TestVerifyConsistencyClean(t *testing.T) {
	st := newMemStore(t, 0, 4)
	a := mkNodeID(0, 1)
	b := mkNodeID(1, 1)
	putNode(t, st, a, 10)
	putNode(t, st, b, 10)
	putRel(t, st, mkRelID(2, 1), 5, a, b)
	putRel(t, st, mkRelID(3, 1), 5, b, a)

	rep, err := st.VerifyConsistency()
	if err != nil {
		t.Fatalf("VerifyConsistency: %v", err)
	}
	if !rep.OK() || rep.Total() != 0 {
		t.Fatalf("healthy store reported inconsistent: %+v", rep)
	}
}

func TestVerifyConsistencyDetectsAdjacencyOrphan(t *testing.T) {
	st := newMemStore(t, 0, 4)
	a := mkNodeID(0, 1)
	b := mkNodeID(1, 1)
	putNode(t, st, a, 10)
	putNode(t, st, b, 10)

	// Write ONLY the incoming adjacency leg (no rel row) directly on slot 2's
	// shard — a torn write / cascade residue. shards[2] owns slot base+2 = slot 2.
	orphanRel := mkRelID(2, 77)
	if err := st.shards[2].PutRelIncoming(b.SnowflakeID(), a.SnowflakeID(), 5, orphanRel.SnowflakeID()); err != nil {
		t.Fatalf("PutRelIncoming: %v", err)
	}

	rep, err := st.VerifyConsistency()
	if err != nil {
		t.Fatalf("VerifyConsistency: %v", err)
	}
	if len(rep.AdjacencyOrphans) != 1 {
		t.Fatalf("want 1 adjacency orphan, got %d (%+v)", len(rep.AdjacencyOrphans), rep)
	}
	o := rep.AdjacencyOrphans[0]
	if o.Rel != orphanRel.SnowflakeID() || o.Shard != 2 {
		t.Fatalf("orphan mis-reported: %+v", o)
	}
}

func TestVerifyConsistencyDetectsEndpointOrphan(t *testing.T) {
	st := newMemStore(t, 0, 4)
	start := mkNodeID(0, 1)
	putNode(t, st, start, 10)
	phantom := mkNodeID(1, 999) // never created — fully absent

	// Write a live rel start->phantom via the partial doors (bypassing the
	// endpoint-liveness check PutRelationship enforces). Rel row on slot 3.
	rid := mkRelID(3, 1)
	r := types.NewRelationship(rid, 5, start, phantom)
	if err := st.shards[3].PutRelEntityAndOut(r); err != nil {
		t.Fatalf("PutRelEntityAndOut: %v", err)
	}
	if err := st.shards[3].PutRelIncoming(phantom.SnowflakeID(), start.SnowflakeID(), 5, rid.SnowflakeID()); err != nil {
		t.Fatalf("PutRelIncoming: %v", err)
	}

	rep, err := st.VerifyConsistency()
	if err != nil {
		t.Fatalf("VerifyConsistency: %v", err)
	}
	if len(rep.RelEndpointOrphans) != 1 {
		t.Fatalf("want 1 endpoint orphan, got %d (%+v)", len(rep.RelEndpointOrphans), rep)
	}
	o := rep.RelEndpointOrphans[0]
	if o.Rel != rid.SnowflakeID() || o.Missing != phantom.SnowflakeID() || o.IsStart {
		t.Fatalf("endpoint orphan mis-reported: %+v", o)
	}

	// Negative: a phantom endpoint that has HISTORY (deleted-with-history, B32)
	// is NOT an orphan — its past is still queryable. Give the phantom a
	// history-only row and re-verify: the orphan must clear.
	histNode := types.NewNode(phantom, 10, nil)
	histNode.SetVersion(0)
	phantomShard, err := st.shardForNodeID(phantom)
	if err != nil {
		t.Fatalf("shardForNodeID: %v", err)
	}
	if err := phantomShard.PutNodeVersion(phantom, 0, histNode); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}
	rep2, err := st.VerifyConsistency()
	if err != nil {
		t.Fatalf("VerifyConsistency (2): %v", err)
	}
	if len(rep2.RelEndpointOrphans) != 0 {
		t.Fatalf("endpoint with history must not be an orphan: %+v", rep2.RelEndpointOrphans)
	}
}

func TestVerifyConsistencyDetectsShardMismatch(t *testing.T) {
	st := newMemStore(t, 0, 4)
	// A node whose ID carries slot 2 written directly onto shard 0's badger —
	// slot 2 routes to shard index 2, so shard 0 holding it is a mismatch.
	misrouted := mkNodeID(2, 1)
	if err := st.shards[0].PutNode(types.NewNode(misrouted, 10, nil)); err != nil {
		t.Fatalf("direct PutNode on wrong shard: %v", err)
	}
	rep, err := st.VerifyConsistency()
	if err != nil {
		t.Fatalf("VerifyConsistency: %v", err)
	}
	if len(rep.ShardMismatches) != 1 {
		t.Fatalf("want 1 shard mismatch, got %d (%+v)", len(rep.ShardMismatches), rep)
	}
	m := rep.ShardMismatches[0]
	if m.Shard != 0 || m.ExpectedShard != 2 || m.Slot != 2 || m.Kind != "node" {
		t.Fatalf("shard mismatch mis-reported: %+v", m)
	}
}

// TestNewPartialOpenFailureClosesShards forces a shard to fail opening AFTER the
// anchor is up (a non-anchor shard path pre-occupied by a regular file), so New
// must fail and clean up every already-open shard (closeOpenShards).
func TestNewPartialOpenFailureClosesShards(t *testing.T) {
	dir := t.TempDir()
	// Occupy slot-01's directory path with a regular file so badger cannot open it.
	if err := os.WriteFile(filepath.Join(dir, shardDirName(1)), []byte("x"), 0o600); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	st, err := New(Config{Dir: dir, BaseSlot: 0, SlotCount: 2})
	if err == nil {
		_ = st.Close()
		t.Fatalf("New over a file-occupied shard path: want error, got nil")
	}
}

// TestAdjacencyOrphanToleratedAndPurged exercises the index-orphan tolerance
// paths: an adjacency entry whose rel row is gone (co-located on the rel's shard,
// a torn prior write). The adjacency FOLD must skip it (resolveRelsTolerant), and
// a cascade must purge it rather than fail (isRelNotFound → PurgeOrphanRelationshipIndexes).
func TestAdjacencyOrphanToleratedAndPurged(t *testing.T) {
	st := newMemStore(t, 0, 4)
	n := mkNodeID(0, 1)
	putNode(t, st, n, 10)

	// Orphan: an incoming entry for n pointing at a rel whose row never existed,
	// written on the rel's own shard (slot 1 → shards[1]).
	orphanRel := mkRelID(1, 99)
	phantomStart := mkNodeID(2, 5)
	if err := st.shards[1].PutRelIncoming(n.SnowflakeID(), phantomStart.SnowflakeID(), 5, orphanRel.SnowflakeID()); err != nil {
		t.Fatalf("PutRelIncoming: %v", err)
	}

	// The fold tolerates the orphan and returns no live rels (resolveRelsTolerant).
	in, err := st.IncomingRelationships(n, 0)
	if err != nil {
		t.Fatalf("IncomingRelationships: %v", err)
	}
	if len(in) != 0 {
		t.Fatalf("orphan should be skipped by fold, got %d rels", len(in))
	}

	// Cascade collects the orphan rel ID, finds its row gone (isRelNotFound), and
	// purges the stale index entry so the final node delete succeeds.
	if err := st.DeleteNodeCascade(n); err != nil {
		t.Fatalf("DeleteNodeCascade over orphan: %v", err)
	}
	if _, err := st.GetNode(n); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("node should be gone after cascade: %v", err)
	}
	// The orphan index entry was purged — VerifyConsistency is clean.
	rep, err := st.VerifyConsistency()
	if err != nil {
		t.Fatalf("VerifyConsistency: %v", err)
	}
	if !rep.OK() {
		t.Fatalf("orphan not purged: %+v", rep)
	}
}

// --- PartialBatchError shape ---

func TestPartialBatchErrorTypeAndUnwrap(t *testing.T) {
	inner := ErrInvalidStoreMutation
	pe := &PartialBatchError{
		Op:              "PutNodesBatch",
		CommittedShards: []int{0, 1},
		FailedShard:     2,
		Err:             inner,
	}
	if !errors.Is(pe, ErrInvalidStoreMutation) {
		t.Fatalf("PartialBatchError must unwrap to its inner error")
	}
	if pe.Error() == "" {
		t.Fatalf("PartialBatchError.Error() empty")
	}
	var target *PartialBatchError
	if !errors.As(error(pe), &target) || target.FailedShard != 2 {
		t.Fatalf("errors.As failed: %+v", target)
	}
}
