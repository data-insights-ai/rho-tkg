package graph_test

import (
	"bytes"
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRetentionPurge_ByValidToReplicaConvergence proves the R5 predicate replicates
// through the SAME crown as R3/ByAge: one ChangeRangePurge record carrying Mode =
// ByValidTo, re-executed by the replica against its own (verbatim-reproduced,
// ValidTo-preserving) state, converges to the identical SELECTIVE outcome — only the
// early-closed events gone, the open one kept. A blind "delete all Events" would
// wrongly remove the open survivor, so this asserts the predicate, not the effect.
func TestRetentionPurge_ByValidToReplicaConvergence(t *testing.T) {
	ctx := context.Background()
	const boundary = types.Instant(5000)

	primary, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, AllowRetentionPurge: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()

	// 3 early-closed events (ValidTo=1000) + 1 open event (kept) + 1 machine.
	m1, _ := primary.Nodes().Add(ctx, []string{"Machine"}, map[string]any{"host": "h1"})
	var openEvent types.NodeID
	for i := 0; i < 3; i++ {
		e, err := primary.Nodes().Add(ctx, []string{"Event"}, map[string]any{
			"seq": int64(i), "tkg_valid_from": types.Instant(100), "tkg_valid_to": types.Instant(1000),
		})
		if err != nil {
			t.Fatalf("add closed event: %v", err)
		}
		if _, err := primary.Rels().AddByID(ctx, "ON", e.ID(), m1.ID(), nil); err != nil {
			t.Fatalf("add edge: %v", err)
		}
	}
	oe, err := primary.Nodes().Add(ctx, []string{"Event"}, map[string]any{"seq": int64(99), "tkg_valid_from": types.Instant(100)})
	if err != nil {
		t.Fatalf("add open event: %v", err)
	}
	openEvent = oe.ID()

	// Bootstrap the replica from a pre-purge snapshot.
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
	if ev, _ := replica.Nodes().ByLabel("Event", store.QueryOpts{}); len(ev) != 4 {
		t.Fatalf("replica pre-purge Event count = %d, want 4", len(ev))
	}

	// ByValidTo purge on the primary: removes the 3 early-closed, keeps the open one.
	res, err := primary.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Mode: adminpkg.PurgeByValidTo, Before: boundary})
	if err != nil {
		t.Fatalf("primary purge: %v", err)
	}
	if res.NodesPurged != 3 {
		t.Fatalf("primary purged %d events, want 3 (early-closed only)", res.NodesPurged)
	}

	// Feed carries exactly one ChangeRangePurge, no per-entity deletes.
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
	if rangePurges != 1 || entityDeletes != 0 {
		t.Fatalf("feed rangePurges=%d entityDeletes=%d, want 1 and 0", rangePurges, entityDeletes)
	}

	// Replica re-executes the ByValidTo predicate → same selective outcome.
	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("replica ApplyChanges: %v", err)
	}
	ev, _ := replica.Nodes().ByLabel("Event", store.QueryOpts{})
	if len(ev) != 1 || ev[0].ID() != openEvent {
		t.Fatalf("replica post-apply Events = %d (want exactly the open survivor)", len(ev))
	}
	if in, _ := replica.Rels().Incoming(m1.ID(), "ON"); len(in) != 0 {
		t.Fatalf("replica machine has %d phantom incoming edges, want 0", len(in))
	}
}
