package graph_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

func newTieredPurgeGraph(t *testing.T, snowID int64, readOnly bool) *graphpkg.Graph {
	t.Helper()
	ts, err := tiered.New(tiered.Config{
		InMemory:      true,
		RefLabels:     []string{"Machine"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
		ChangeLog:     true,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: snowID, Store: ts, AllowRetentionPurge: true, ReadOnlyReplica: readOnly})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// TestRetentionPurge_TieredReplicaConvergence (ADR-0008 R4) is the tiered crown:
// a tiered primary purges events wired to a reference node by BOTH cross-shard
// edge directions (event->machine entity-on-event-shard, machine->event
// entity-on-ref-shard), emits ONE ChangeRangePurge, and a TIERED replica
// re-executes the predicate across ITS shards — cross-shard residue sweep included
// — reaching the same purged, dangle-free state with its watermark advanced.
func TestRetentionPurge_TieredReplicaConvergence(t *testing.T) {
	ctx := context.Background()
	primary := newTieredPurgeGraph(t, 3, false)

	machine, err := primary.Nodes().Add(ctx, []string{"Machine"}, map[string]any{"host": "h"})
	if err != nil {
		t.Fatalf("add machine: %v", err)
	}
	for i := 0; i < 4; i++ {
		e, err := primary.Nodes().Add(ctx, []string{"Event"}, map[string]any{"seq": int64(i)})
		if err != nil {
			t.Fatalf("add event: %v", err)
		}
		// event->machine (entity on the event shard) AND machine->event (entity on
		// the ref shard) — both cross-shard, exercising both residue shapes.
		if _, err := primary.Rels().AddByID(ctx, "ON", e.ID(), machine.ID(), nil); err != nil {
			t.Fatalf("add event->machine: %v", err)
		}
		if _, err := primary.Rels().AddByID(ctx, "SAW", machine.ID(), e.ID(), nil); err != nil {
			t.Fatalf("add machine->event: %v", err)
		}
	}

	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("export: %v", err)
	}
	lsn0, _ := primary.Replication().LastCommittedLSN()

	replica := newTieredPurgeGraph(t, 7, true)
	if err := replica.IO().Import(bytes.NewReader(snap.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("replica import: %v", err)
	}
	if err := replica.Replication().SetAppliedLSN(lsn0); err != nil {
		t.Fatalf("replica SetAppliedLSN: %v", err)
	}
	if ev, _ := replica.Nodes().ByLabel("Event", store.QueryOpts{}); len(ev) != 4 {
		t.Fatalf("replica pre-purge Event count = %d, want 4", len(ev))
	}

	res, err := primary.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Mode: adminpkg.PurgeByAge, Before: farFuture})
	if err != nil {
		t.Fatalf("primary purge: %v", err)
	}
	if res.NodesPurged != 4 {
		t.Fatalf("primary purged %d events, want 4", res.NodesPurged)
	}
	if ev, _ := primary.Nodes().ByLabel("Event", store.QueryOpts{}); len(ev) != 0 {
		t.Fatalf("primary Event count after purge = %d, want 0", len(ev))
	}
	// No dangling edges on the primary's surviving machine (both directions swept).
	if out, _ := primary.Rels().Outgoing(machine.ID(), "SAW"); len(out) != 0 {
		t.Fatalf("primary machine has %d dangling outgoing edges, want 0", len(out))
	}
	if in, _ := primary.Rels().Incoming(machine.ID(), "ON"); len(in) != 0 {
		t.Fatalf("primary machine has %d dangling incoming edges, want 0", len(in))
	}

	// Exactly one predicate record, no per-entity deletes.
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
		t.Fatalf("feed carried %d per-entity delete records, want 0", entityDeletes)
	}

	// The tiered replica re-executes the predicate to the same state.
	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("replica ApplyChanges: %v", err)
	}
	if ev, _ := replica.Nodes().ByLabel("Event", store.QueryOpts{}); len(ev) != 0 {
		t.Fatalf("replica Event count = %d after apply, want 0", len(ev))
	}
	if m, _ := replica.Nodes().ByLabel("Machine", store.QueryOpts{}); len(m) != 1 {
		t.Fatalf("replica Machine count = %d, want 1 (survivor)", len(m))
	}
	if out, _ := replica.Rels().Outgoing(machine.ID(), "SAW"); len(out) != 0 {
		t.Fatalf("replica machine has %d dangling outgoing edges, want 0", len(out))
	}
	if in, _ := replica.Rels().Incoming(machine.ID(), "ON"); len(in) != 0 {
		t.Fatalf("replica machine has %d dangling incoming edges, want 0", len(in))
	}
	if _, err := replica.Temporal().NodesAsOf(farFuture - 1); !errors.Is(err, graphpkg.ErrRetentionExpired) {
		t.Fatalf("replica NodesAsOf below watermark err=%v, want ErrRetentionExpired", err)
	}
}
