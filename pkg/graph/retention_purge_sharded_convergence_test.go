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
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
)

// TestRetentionPurge_ShardedReplicaConvergence (ADR-0008 R4) is the horizontal-
// scaling crown: a SHARDED primary purges across its shards and emits ONE
// store-global ChangeRangePurge record (on the anchor shard); a SHARDED replica
// re-executes the predicate across ITS shards and converges to the same purged
// state — including the cross-shard edge sweep — with no per-entity records.
func TestRetentionPurge_ShardedReplicaConvergence(t *testing.T) {
	ctx := context.Background()

	pStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2, ChangeLog: true})
	if err != nil {
		t.Fatalf("primary sharded.New: %v", err)
	}
	primary, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 0, Store: pStore, AllowRetentionPurge: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()

	m1, _ := primary.Nodes().Add(ctx, []string{"Machine"}, map[string]any{"host": "h"})
	for i := 0; i < 6; i++ {
		e, err := primary.Nodes().Add(ctx, []string{"Event"}, map[string]any{"seq": int64(i)})
		if err != nil {
			t.Fatalf("add event: %v", err)
		}
		if _, err := primary.Rels().AddByID(ctx, "ON", e.ID(), m1.ID(), nil); err != nil {
			t.Fatalf("add edge: %v", err)
		}
	}

	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("export: %v", err)
	}
	lsn0, _ := primary.Replication().LastCommittedLSN()

	rStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("replica sharded.New: %v", err)
	}
	replica, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 6, Store: rStore, ReadOnlyReplica: true})
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
	if ev, _ := replica.Nodes().ByLabel("Event", store.QueryOpts{}); len(ev) != 6 {
		t.Fatalf("replica pre-purge Event count = %d, want 6", len(ev))
	}

	res, err := primary.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Mode: adminpkg.PurgeByAge, Before: farFuture})
	if err != nil {
		t.Fatalf("primary purge: %v", err)
	}
	if res.NodesPurged != 6 {
		t.Fatalf("primary purged %d events, want 6", res.NodesPurged)
	}
	if ev, _ := primary.Nodes().ByLabel("Event", store.QueryOpts{}); len(ev) != 0 {
		t.Fatalf("primary Event count after purge = %d, want 0", len(ev))
	}

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

	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("replica ApplyChanges: %v", err)
	}
	if ev, _ := replica.Nodes().ByLabel("Event", store.QueryOpts{}); len(ev) != 0 {
		t.Fatalf("replica Event count = %d after apply, want 0", len(ev))
	}
	if m, _ := replica.Nodes().ByLabel("Machine", store.QueryOpts{}); len(m) != 1 {
		t.Fatalf("replica Machine count = %d, want 1 (survivor)", len(m))
	}
	if in, _ := replica.Rels().Incoming(m1.ID(), "ON"); len(in) != 0 {
		t.Fatalf("replica machine has %d phantom incoming edges, want 0", len(in))
	}
	if _, err := replica.Temporal().NodesAsOf(farFuture - 1); !errors.Is(err, graphpkg.ErrRetentionExpired) {
		t.Fatalf("replica NodesAsOf below watermark err=%v, want ErrRetentionExpired", err)
	}
}
