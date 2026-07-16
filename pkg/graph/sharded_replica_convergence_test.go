package graph_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
)

// The ADR-0007 S3 crown: TOPOLOGY-INDEPENDENT replica convergence. A sharded
// primary with the change-log ON (SnowflakeNodeID 0 → nodes mint on slot 0 /
// shard 0, rels on slot 1 / shard 1, so the store-global feed genuinely MERGES
// records across two shards in one LSN order) converges BYTE-EXACT onto replicas
// with DIFFERENT shard topologies: a single non-sharded badger replica AND a
// 4-shard sharded replica. Records carry entities verbatim; each replica routes
// by its OWN catalog — proving the feed is a topology-independent total order.
func TestShardedReplicaConvergence_TopologyIndependent(t *testing.T) {
	ctx := context.Background()

	primaryStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2, ChangeLog: true})
	if err != nil {
		t.Fatalf("primary sharded.New: %v", err)
	}
	primary, err := graph.New(graph.Config{SnowflakeNodeID: 0, Store: primaryStore})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()

	// Seed vocabulary (labels A, B; rel-type KNOWS) before the snapshot so no
	// post-bootstrap token refetch is needed.
	a := mustAdd(t, primary, []string{"A"}, map[string]any{"n": "a"})
	b := mustAdd(t, primary, []string{"A", "B"}, map[string]any{"n": "b"})
	c := mustAdd(t, primary, []string{"A"}, map[string]any{"n": "c"})
	r1, err := primary.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatalf("seed rel: %v", err)
	}

	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	snapBytes := snap.Bytes()
	lsn0, err := primary.Replication().LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	// Replica 1 — a single non-sharded badger store (topology 1: one shard).
	replicaBadger, err := graph.New(graph.Config{SnowflakeNodeID: 5, BadgerInMemory: true, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("badger replica New: %v", err)
	}
	defer replicaBadger.Close()

	// Replica 2 — a 4-shard sharded store (topology 2: four shards). Foreign IDs
	// on slots 0/1 route to its shards 0/1; slots 2/3 stay empty. No change-log.
	replicaShardedStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 4})
	if err != nil {
		t.Fatalf("sharded replica sharded.New: %v", err)
	}
	replicaSharded, err := graph.New(graph.Config{SnowflakeNodeID: 6, Store: replicaShardedStore, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("sharded replica New: %v", err)
	}
	defer replicaSharded.Close()

	for _, rep := range []*graph.Graph{replicaBadger, replicaSharded} {
		if err := rep.IO().Import(bytes.NewReader(snapBytes), tkgio.ImportOptions{}); err != nil {
			t.Fatalf("replica Import: %v", err)
		}
		if err := rep.Replication().SetAppliedLSN(lsn0); err != nil {
			t.Fatalf("replica SetAppliedLSN: %v", err)
		}
	}
	assertConverged(t, "after bootstrap (badger replica)", primary, replicaBadger)
	assertConverged(t, "after bootstrap (sharded replica)", primary, replicaSharded)

	// Drive every record kind on the primary using only known tokens (A, B, KNOWS).
	if _, err := primary.Nodes().Update(ctx, a.ID(), map[string]any{"n": "a2"}); err != nil {
		t.Fatalf("Update a (with-history): %v", err)
	}
	if _, err := primary.Nodes().UpdateInPlace(ctx, b.ID(), map[string]any{"n": "b2"}); err != nil {
		t.Fatalf("UpdateInPlace b: %v", err)
	}
	if err := primary.Nodes().AddLabel(ctx, a.ID(), "B"); err != nil {
		t.Fatalf("AddLabel a B: %v", err)
	}
	if err := primary.Nodes().RemoveLabel(ctx, b.ID(), "B"); err != nil {
		t.Fatalf("RemoveLabel b B: %v", err)
	}
	if _, err := primary.Rels().AddByID(ctx, "KNOWS", b.ID(), c.ID(), nil); err != nil {
		t.Fatalf("Add rel b->c: %v", err)
	}
	// A rel create that SURVIVES to the end (a->b). Without a co-committed
	// ChangeRelPut record for sharded rel creates this edge would be absent on the
	// replica and assertConverged would fail — the direct guard against the
	// record-free-partial-door bug (the b->c edge below is deleted, so it alone
	// could mask a missing rel-create record).
	rSurvive, err := primary.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"w": int64(9)})
	if err != nil {
		t.Fatalf("Add surviving rel a->b: %v", err)
	}
	_ = rSurvive
	if _, err := primary.Rels().Update(ctx, r1.ID(), map[string]any{"w": int64(2)}); err != nil {
		t.Fatalf("Update rel r1 (with-history): %v", err)
	}
	// Delete a connected node — cascades the b->c rel (node + rel tombstones).
	if err := primary.Nodes().Delete(ctx, c.ID()); err != nil {
		t.Fatalf("Delete c (cascade): %v", err)
	}

	// Tail the store-global feed (merged across shard 0 nodes + shard 1 rels) and
	// apply to BOTH replicas. Assert the feed is gapless in one total order.
	var recs []store.ChangeRecord
	if err := primary.Replication().ForEachChange(lsn0, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("no change records past the snapshot LSN")
	}
	for i, rec := range recs {
		if want := lsn0 + uint64(i) + 1; rec.LSN != want {
			t.Fatalf("cross-shard LSN gap/misorder at index %d: got %d, want %d", i, rec.LSN, want)
		}
	}
	for _, rep := range []*graph.Graph{replicaBadger, replicaSharded} {
		applied, err := rep.Replication().ApplyChanges(recs)
		if err != nil {
			t.Fatalf("ApplyChanges: %v", err)
		}
		if want := recs[len(recs)-1].LSN; applied != want {
			t.Fatalf("applied LSN = %d, want %d", applied, want)
		}
	}
	assertConverged(t, "after tail (badger replica, topology 1)", primary, replicaBadger)
	assertConverged(t, "after tail (sharded replica, topology 4)", primary, replicaSharded)
}
