package graph_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
)

// A v2 export stamps the change-log LSN into the header under the same lock as
// the snapshot, and import records it as the replica's initial applied
// watermark — so a bootstrap needs no separate, racy LastCommittedLSN() call.
func TestExport_SnapshotLSN_RecordedAsWatermark(t *testing.T) {
	ctx := context.Background()
	primary, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()
	for i := 0; i < 3; i++ {
		if _, err := primary.Nodes().Add(ctx, []string{"A"}, nil); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	// No writes between Export and this read (single goroutine), so the header's
	// SnapshotLSN equals LastCommittedLSN() here.
	wantLSN, err := primary.Replication().LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}
	if wantLSN == 0 {
		t.Fatal("expected LSN > 0 after writes")
	}

	replica, err := graph.New(graph.Config{SnowflakeNodeID: 2, BadgerInMemory: true, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer replica.Close()
	// Import alone (no manual SetAppliedLSN) must seed the watermark.
	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}
	got, err := replica.Replication().AppliedLSN()
	if err != nil {
		t.Fatalf("AppliedLSN: %v", err)
	}
	if got != wantLSN {
		t.Fatalf("AppliedLSN after import = %d, want SnapshotLSN %d", got, wantLSN)
	}
}

// A source with no change-log exports SnapshotLSN 0; import leaves the watermark
// untouched (0) — and v1 snapshots (no field) behave identically.
func TestExport_NoChangeLog_ZeroWatermark(t *testing.T) {
	ctx := context.Background()
	primary, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()
	if _, err := primary.Nodes().Add(ctx, []string{"A"}, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}

	replica, err := graph.New(graph.Config{SnowflakeNodeID: 2, BadgerInMemory: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer replica.Close()
	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}
	if got, _ := replica.Replication().AppliedLSN(); got != 0 {
		t.Fatalf("AppliedLSN after no-changelog import = %d, want 0", got)
	}
}
