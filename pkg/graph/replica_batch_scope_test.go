package graph_test

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
)

// A committed Batch.Execute ships its records through the per-tx change-log scope:
// they are buffered during the batch (which holds the global write lock) and
// emitted with contiguous LSNs minted at CommitLogScope, so a replica tailing them
// converges. (Unlike a tx, a batch is NOT all-or-nothing — it keeps successful ops
// — so the scope is always committed, never discarded.)
func TestReplicaConvergence_CommittedBatchViaBuffer(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	primary, replica, lsn0 := newReplicaPair(t, "")
	// "Batched" is a label token the snapshot does not carry: the replica fetches it
	// from the source while applying.
	replica.SetReplicationSource(primary.Replication())

	// A batch creating several nodes (with a brand-new label token).
	bb, err := primary.Batch().New()
	if err != nil {
		t.Fatalf("Batch().New: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := bb.AddNode([]string{"Batched"}, map[string]any{"i": int64(i)}); err != nil {
			t.Fatalf("bb.AddNode(%d): %v", i, err)
		}
	}
	res, err := bb.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Failed != 0 {
		t.Fatalf("batch Failed = %d, want 0", res.Failed)
	}

	// Records were emitted at batch commit with contiguous LSNs.
	recs := changesSince(t, primary, lsn0)
	if len(recs) != 5 {
		t.Fatalf("batch shipped %d records, want 5 (one per created node)", len(recs))
	}
	for i := 1; i < len(recs); i++ {
		if recs[i].LSN != recs[i-1].LSN+1 {
			t.Fatalf("batch LSNs not contiguous: %d after %d", recs[i].LSN, recs[i-1].LSN)
		}
	}

	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("ApplyChanges: %v", err)
	}
	assertConverged(t, "after committed batch", primary, replica)
}
