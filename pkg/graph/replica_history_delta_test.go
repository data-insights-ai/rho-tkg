package graph_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// TestReplicaConvergence_HistoryDeltaPrimary proves that a primary storing
// version history as anchor+delta (B6) replicates byte-identically to a replica
// that stores full snapshots: the delta representation is store-internal and
// never enters the change-log feed, so the replica reconstructs the SAME history
// (depth + per-version content hashes) via its own write path. Crosses the
// anchor boundary (>16 versions) so the primary genuinely stores deltas.
func TestReplicaConvergence_HistoryDeltaPrimary(t *testing.T) {
	ctx := context.Background()
	primary, err := graph.New(graph.Config{
		SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true,
		HistoryDeltaEncoding: true, // <-- primary stores anchor+delta history
	})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()

	// Replica deliberately stores FULL snapshots — cross-format convergence.
	replica, err := graph.New(graph.Config{SnowflakeNodeID: 2, BadgerInMemory: true, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer replica.Close()
	replica.SetReplicationSource(primary.Replication())

	blob := strings.Repeat("unchanging blob payload | ", 8)
	n, err := primary.Nodes().Add(ctx, []string{"Doc"}, map[string]any{
		"blob": blob, "counter": int64(0), "region": "eu-west",
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	for v := 1; v <= 20; v++ { // crosses the anchor boundary at 16
		if _, err := primary.Nodes().Update(ctx, n.InternalID(), map[string]any{
			"counter": int64(v),
			"status":  []string{"active", "pending", "held", "closed"}[v%4],
		}); err != nil {
			t.Fatalf("Update v%d: %v", v, err)
		}
	}

	// Bootstrap the replica from an export snapshot, then tail the feed.
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

	// Tail every change from the very beginning and apply to the replica. (Bootstrap
	// was taken AFTER all writes, so the feed replay is idempotent — apply reproduces
	// identical rows.)
	var recs []store.ChangeRecord
	if err := primary.Replication().ForEachChange(0, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("ApplyChanges: %v", err)
	}

	assertConverged(t, "delta-primary history", primary, replica)
}
