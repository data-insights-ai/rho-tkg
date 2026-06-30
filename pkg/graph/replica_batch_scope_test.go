package graph_test

import (
	"bytes"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// A committed Batch.Execute ships its records through the per-tx change-log scope:
// they are buffered during the batch (which holds the global write lock) and
// emitted with contiguous LSNs minted at CommitLogScope, so a replica tailing them
// converges. (Unlike a tx, a batch is NOT all-or-nothing — it keeps successful ops
// — so the scope is always committed, never discarded.)
func TestReplicaConvergence_CommittedBatchViaBuffer(t *testing.T) {
	primary, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()
	mustAdd(t, primary, []string{"Seed"}, map[string]any{"n": "seed"})

	replica, err := graph.New(graph.Config{SnowflakeNodeID: 2, BadgerInMemory: true, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer replica.Close()
	replica.SetReplicationSource(primary.Replication())
	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	lsn0, _ := primary.Replication().LastCommittedLSN()
	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}
	if err := replica.Replication().SetAppliedLSN(lsn0); err != nil {
		t.Fatalf("SetAppliedLSN: %v", err)
	}
	assertConverged(t, "bootstrap", primary, replica)

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
	var recs []store.ChangeRecord
	if err := primary.Replication().ForEachChange(lsn0, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
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
