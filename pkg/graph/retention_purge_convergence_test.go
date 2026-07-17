package graph_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// TestRetentionPurge_ReplicaConvergence (ADR-0008 R3) proves the replication
// crown: a range purge emits ONE ChangeRangePurge record (the PREDICATE, not
// per-entity deletes), and a replica reaches the SAME purged state by
// re-executing the predicate against its own LSN-consistent state — no per-entity
// delete records travel, and the replica's retention watermark advances so a
// below-boundary read fails closed there too. Idempotent re-apply.
func TestRetentionPurge_ReplicaConvergence(t *testing.T) {
	ctx := context.Background()

	primary, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, AllowRetentionPurge: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()

	// Seed: 2 surviving machines + 4 events, each event with an edge to a machine.
	m1, _ := primary.Nodes().Add(ctx, []string{"Machine"}, map[string]any{"host": "h1"})
	m2, _ := primary.Nodes().Add(ctx, []string{"Machine"}, nil)
	for i := 0; i < 4; i++ {
		e, err := primary.Nodes().Add(ctx, []string{"Event"}, map[string]any{"seq": int64(i)})
		if err != nil {
			t.Fatalf("add event: %v", err)
		}
		if _, err := primary.Rels().AddByID(ctx, "ON", e.ID(), m1.ID(), nil); err != nil {
			t.Fatalf("add edge: %v", err)
		}
	}

	// Bootstrap the replica from a snapshot taken BEFORE the purge.
	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("export: %v", err)
	}
	lsn0, _ := primary.Replication().LastCommittedLSN()

	replica, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 7, BadgerInMemory: true, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer replica.Close()
	if err := replica.IO().Import(bytes.NewReader(snap.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("replica import: %v", err)
	}
	if err := replica.Replication().SetAppliedLSN(lsn0); err != nil {
		t.Fatalf("replica SetAppliedLSN: %v", err)
	}
	// Replica starts with all 4 events present.
	if ev, _ := replica.Nodes().ByLabel("Event", store.QueryOpts{}); len(ev) != 4 {
		t.Fatalf("replica pre-purge Event count = %d, want 4", len(ev))
	}

	// Purge every Event on the primary (farFuture boundary).
	res, err := primary.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Mode: adminpkg.PurgeByAge, Before: farFuture})
	if err != nil {
		t.Fatalf("primary purge: %v", err)
	}
	if res.NodesPurged != 4 {
		t.Fatalf("primary purged %d events, want 4", res.NodesPurged)
	}

	// Tail the primary feed: it must carry EXACTLY ONE ChangeRangePurge record and
	// NO per-entity node/rel delete records (the predicate, not N deletes).
	var recs []store.ChangeRecord
	var rangePurges, entityDeletes int
	if err := primary.Replication().ForEachChange(lsn0, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		switch rec.Tag {
		case store.ChangeRangePurge:
			rangePurges++
		case store.ChangeNodeDelete, store.ChangeRelDelete:
			entityDeletes++
		}
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if rangePurges != 1 {
		t.Fatalf("feed carried %d ChangeRangePurge records, want exactly 1", rangePurges)
	}
	if entityDeletes != 0 {
		t.Fatalf("feed carried %d per-entity delete records, want 0 (predicate purge)", entityDeletes)
	}

	// Apply to the replica: it re-executes the predicate and reaches the same state.
	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("replica ApplyChanges: %v", err)
	}
	if ev, _ := replica.Nodes().ByLabel("Event", store.QueryOpts{}); len(ev) != 0 {
		t.Fatalf("replica post-apply Event count = %d, want 0 (predicate re-executed)", len(ev))
	}
	if m, _ := replica.Nodes().ByLabel("Machine", store.QueryOpts{}); len(m) != 2 {
		t.Fatalf("replica Machine count = %d, want 2 (survivors)", len(m))
	}
	if in, _ := replica.Rels().Incoming(m1.ID(), "ON"); len(in) != 0 {
		t.Fatalf("replica machine has %d phantom incoming edges, want 0", len(in))
	}
	_ = m2

	// The replica's retention watermark advanced via the record apply (never a
	// replicated MetaSet): a below-boundary temporal scan now fails closed there.
	if _, err := replica.Temporal().NodesAsOf(farFuture - 1); !errors.Is(err, graphpkg.ErrRetentionExpired) {
		t.Fatalf("replica NodesAsOf below watermark err=%v, want ErrRetentionExpired", err)
	}

	// Idempotent re-apply is a no-op.
	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("replica re-ApplyChanges (idempotency): %v", err)
	}
	if ev, _ := replica.Nodes().ByLabel("Event", store.QueryOpts{}); len(ev) != 0 {
		t.Fatalf("replica Event count after re-apply = %d, want 0", len(ev))
	}
}
