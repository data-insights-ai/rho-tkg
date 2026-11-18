package graph_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
)

// TestReplicaConvergence_HistoryDeltaPrimary proves that a primary storing
// version history as anchor+delta (Config.HistoryDeltaEncoding) replicates
// byte-identically through the change-log feed to a replica that stores full
// snapshots: the delta representation is store-internal and never enters the
// feed, so the replica holds the SAME history (depth + per-version content
// hashes). The replica is bootstrapped right after the create, so all 20 updates
// arrive through ApplyChanges, not Import. Crosses the anchor boundary (>16
// versions) so the primary genuinely stores deltas.
func TestReplicaConvergence_HistoryDeltaPrimary(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
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

	// Bootstrap the replica from an export snapshot holding only the create.
	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	lsn0, err := primary.Replication().LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}
	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}
	if err := replica.Replication().SetAppliedLSN(lsn0); err != nil {
		t.Fatalf("SetAppliedLSN: %v", err)
	}

	for v := 1; v <= 20; v++ { // crosses the anchor boundary at 16
		if _, err := primary.Nodes().Update(ctx, n.InternalID(), map[string]any{
			"counter": int64(v),
			"status":  []string{"active", "pending", "held", "closed"}[v%4],
		}); err != nil {
			t.Fatalf("Update v%d: %v", v, err)
		}
	}

	// Tail the 20 updates from the handoff LSN and apply them.
	recs := changesSince(t, primary, lsn0)
	if len(recs) != 20 {
		t.Fatalf("records past the snapshot = %d, want 20 (one per update)", len(recs))
	}
	applied, err := replica.Replication().ApplyChanges(recs)
	if err != nil {
		t.Fatalf("ApplyChanges: %v", err)
	}
	if want := recs[len(recs)-1].LSN; applied != want {
		t.Fatalf("applied LSN = %d, want %d", applied, want)
	}
	if h := mustNodeHistory(t, replica, n.InternalID()); len(h) < 20 {
		t.Fatalf("replica history depth = %d, want >= 20 (the updates must arrive through the feed)", len(h))
	}

	assertConverged(t, "delta-primary history", primary, replica)
}
