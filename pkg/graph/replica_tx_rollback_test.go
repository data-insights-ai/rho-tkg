package graph_test

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// A ROLLED-BACK transaction emits NOTHING to the change-log feed — the per-tx
// change-log buffer (store.TxChangeLogScope) buffers the tx's records and DISCARDS
// them on Rollback, mirroring txEventBuffer for events. So a rolled-back tx leaves
// the feed empty past the pre-tx watermark (no create, no reverse delete, no LSN
// burned, no gap), and a replica converges with nothing to apply. This replaces
// the old physical-redo-log behavior where a rolled-back tx shipped its create +
// hard-cascade-delete churn and the replica transiently materialized the phantom.
func TestReplicaConvergence_RolledBackTxEmitsNothing(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	// The pair's two seed nodes carry label "A", so the snapshot registry knows it.
	primary, replica, lsn0 := newReplicaPair(t, "")

	// A transaction that creates a node carrying the already-known "A" label
	// (so the replica can resolve its token without a refetch), then ROLLS BACK.
	tx, err := primary.Tx().Begin()
	if err != nil {
		t.Fatalf("tx Begin: %v", err)
	}
	if _, err := tx.AddNode([]string{"A"}, map[string]any{"n": "phantom"}); err != nil {
		t.Fatalf("tx AddNode: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("tx Rollback: %v", err)
	}

	// The rolled-back tx emitted NOTHING: the feed past the pre-tx watermark is
	// empty and the watermark did not advance (no LSN burned, no gap).
	recs := changesSince(t, primary, lsn0)
	if len(recs) != 0 {
		t.Fatalf("rolled-back tx shipped %d records, want 0 (the per-tx buffer discards them): %v", len(recs), recs)
	}
	if lsn, _ := primary.Replication().LastCommittedLSN(); lsn != lsn0 {
		t.Fatalf("rolled-back tx advanced the watermark %d -> %d (burned an LSN); want unchanged", lsn0, lsn)
	}

	// Nothing to tail, so the replica is already converged.
	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("ApplyChanges (empty): %v", err)
	}
	assertConverged(t, "after rolled-back tx (nothing shipped)", primary, replica)

	pn, _ := primary.Nodes().All(graph.QueryOpts{})
	if len(pn) != 2 {
		t.Fatalf("primary live nodes = %d, want 2 (only the two seed nodes; the phantom rolled back)", len(pn))
	}
	rn, _ := replica.Nodes().All(graph.QueryOpts{})
	if len(rn) != 2 {
		t.Fatalf("replica live nodes = %d, want 2 — the rolled-back phantom never reached the replica", len(rn))
	}
}

// A COMMITTED tx ships its net records (minted at commit time, contiguous LSNs)
// and a replica tailing them converges byte-exactly. Exercises the CommitLogScope
// path: a multi-op tx (create + update + delete, plus a new label token) whose
// records were buffered during the body and emitted atomically at commit.
func TestReplicaConvergence_CommittedTxViaBuffer(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	primary, replica, lsn0 := newReplicaPair(t, "")
	// In-process refetch source so a tx-allocated NEW label token resolves.
	replica.SetReplicationSource(primary.Replication())
	seeds, err := primary.Nodes().All(graph.QueryOpts{})
	if err != nil || len(seeds) == 0 {
		t.Fatalf("seed nodes: %d, %v", len(seeds), err)
	}
	a := seeds[0] // a seed node that came to the replica through the snapshot

	// A multi-op tx that COMMITS: create a node with a brand-new label, update an
	// existing node, then delete the new node.
	tx, err := primary.Tx().Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	newNode, err := tx.AddNode([]string{"Fresh"}, map[string]any{"n": "c"})
	if err != nil {
		t.Fatalf("tx AddNode: %v", err)
	}
	if _, err := tx.UpdateNode(a.ID(), map[string]any{"n": "a2"}); err != nil {
		t.Fatalf("tx UpdateNode: %v", err)
	}
	if err := tx.DeleteNode(newNode.ID()); err != nil {
		t.Fatalf("tx DeleteNode: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Records were emitted at commit with contiguous LSNs.
	recs := changesSince(t, primary, lsn0)
	if len(recs) == 0 {
		t.Fatal("committed tx shipped no records")
	}
	for i := 1; i < len(recs); i++ {
		if recs[i].LSN != recs[i-1].LSN+1 {
			t.Fatalf("committed tx LSNs not contiguous: %d after %d", recs[i].LSN, recs[i-1].LSN)
		}
	}

	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("ApplyChanges: %v", err)
	}
	assertConverged(t, "after committed tx", primary, replica)
}
