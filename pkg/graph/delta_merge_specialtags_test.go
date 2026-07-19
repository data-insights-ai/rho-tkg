package graph_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestDeltaMerge_ForeignIncomingPutAndDeleteMerges is the BACKLOG 12b
// regression for the ChangeForeignIncoming / ChangeForeignIncomingDelete
// tags: ExportSince legitimately emits them (ADR-0010 Model A), but
// captureMergeRecord had no cases for them and fell to the `default` branch,
// misdiagnosing a perfectly valid delta as ErrCorruptExport — a hard
// availability bug for any sharded deployment using cross-machine
// foreign-incoming edges together with delta backups.
func TestDeltaMerge_ForeignIncomingPutAndDeleteMerges(t *testing.T) {
	ctx := context.Background()

	eStore, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2, ChangeLog: true})
	if err != nil {
		t.Fatalf("end sharded.New: %v", err)
	}
	e, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 0, Store: eStore})
	if err != nil {
		t.Fatalf("end New: %v", err)
	}
	defer e.Close()

	end, err := e.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Ada"})
	if err != nil {
		t.Fatalf("Add end: %v", err)
	}
	x, _ := e.Nodes().Add(ctx, []string{"Person"}, nil)
	y, _ := e.Nodes().Add(ctx, []string{"Person"}, nil)
	if _, err := e.Rels().AddByID(ctx, "KNOWS", x.ID(), y.ID(), nil); err != nil {
		t.Fatalf("seed KNOWS: %v", err)
	}

	var full bytes.Buffer
	if err := e.IO().Export(&full); err != nil {
		t.Fatalf("Export: %v", err)
	}
	base, err := tkgio.HeaderOf(bytes.NewReader(full.Bytes()))
	if err != nil {
		t.Fatalf("HeaderOf: %v", err)
	}
	baseCursor := base.To

	target, err := graphpkg.New(graphpkg.Config{Store: mustShardedStore(t)})
	if err != nil {
		t.Fatalf("target New: %v", err)
	}
	defer target.Close()
	if err := target.IO().Import(bytes.NewReader(full.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("target Import: %v", err)
	}

	// Record a cross-machine incoming edge, then delta-merge the PUT.
	edge := store.ForeignIncomingEdge{
		RelID:      types.RelID(snowflake.ID(700003)),
		TypeName:   "KNOWS",
		StartID:    types.NodeID(snowflake.ID(700001)),
		EndID:      end.ID(),
		Properties: map[string]any{"w": int64(7)},
		FromHash:   "aa11",
		ToHash:     "bb22",
		TxFrom:     1234,
		Version:    0,
		AttestTx:   1,
	}
	if err := e.Rels().RecordForeignIncoming(ctx, edge); err != nil {
		t.Fatalf("RecordForeignIncoming: %v", err)
	}

	var delta1 bytes.Buffer
	if err := e.IO().ExportSince(&delta1, baseCursor); err != nil {
		t.Fatalf("ExportSince (put): %v", err)
	}
	if err := target.IO().ImportMerge(bytes.NewReader(delta1.Bytes()), tkgio.MergeOptions{ExpectBase: baseCursor}); err != nil {
		t.Fatalf("ImportMerge (put) misdiagnosed ChangeForeignIncoming: %v", err)
	}

	tin, err := target.Rels().Incoming(end.ID(), "KNOWS")
	if err != nil {
		t.Fatalf("target Incoming: %v", err)
	}
	if len(tin) != 1 || tin[0].ID() != edge.RelID {
		t.Fatalf("target Incoming(END) = %d rels, want the stub %d", len(tin), edge.RelID.SnowflakeID())
	}
	if tin[0].Integrity().ToNodeHash != "bb22" {
		t.Fatalf("merged stub ToNodeHash = %q, want bb22", tin[0].Integrity().ToNodeHash)
	}

	// Now delete the stub and delta-merge the DELETE.
	mid, err := e.IO().Watermark()
	if err != nil {
		t.Fatalf("Watermark mid: %v", err)
	}
	if err := e.Nodes().Delete(ctx, end.ID()); err != nil {
		t.Fatalf("Delete end: %v", err)
	}

	var delta2 bytes.Buffer
	if err := e.IO().ExportSince(&delta2, mid); err != nil {
		t.Fatalf("ExportSince (delete): %v", err)
	}
	if err := target.IO().ImportMerge(bytes.NewReader(delta2.Bytes()), tkgio.MergeOptions{ExpectBase: mid}); err != nil {
		t.Fatalf("ImportMerge (delete) misdiagnosed ChangeForeignIncomingDelete: %v", err)
	}

	if _, err := target.Nodes().Get(ctx, end.ID()); !errors.Is(err, graphpkg.ErrNodeNotFound) {
		t.Fatalf("target Get(end) after merged delete = %v, want ErrNodeNotFound", err)
	}
}

func mustShardedStore(t *testing.T) *sharded.Store {
	t.Helper()
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	return st
}

// TestDeltaMerge_RangePurgeMerges is the BACKLOG 12b regression for the
// ChangeRangePurge tag: a range purge emits ONE predicate record (ADR-0008
// R3), and ExportSince legitimately includes it in a delta stream, but
// captureMergeRecord had no case for it — every delta containing a retention
// purge failed to merge with a misdiagnosed ErrCorruptExport. The fix
// deliberately captures NOTHING for rollback (re-materializing purged rows
// for a possible rollback would undermine retention's own guarantee); this
// test also proves that omission does not corrupt or block the merge.
func TestDeltaMerge_RangePurgeMerges(t *testing.T) {
	ctx := context.Background()

	primary, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, AllowRetentionPurge: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()

	m1, _ := primary.Nodes().Add(ctx, []string{"Machine"}, map[string]any{"host": "h1"})
	for i := 0; i < 4; i++ {
		ev, err := primary.Nodes().Add(ctx, []string{"Event"}, map[string]any{"seq": int64(i)})
		if err != nil {
			t.Fatalf("add event: %v", err)
		}
		if _, err := primary.Rels().AddByID(ctx, "ON", ev.ID(), m1.ID(), nil); err != nil {
			t.Fatalf("add edge: %v", err)
		}
	}

	var full bytes.Buffer
	if err := primary.IO().Export(&full); err != nil {
		t.Fatalf("Export: %v", err)
	}
	base, err := tkgio.HeaderOf(bytes.NewReader(full.Bytes()))
	if err != nil {
		t.Fatalf("HeaderOf: %v", err)
	}
	baseCursor := base.To

	target, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 2, BadgerInMemory: true})
	if err != nil {
		t.Fatalf("target New: %v", err)
	}
	defer target.Close()
	if err := target.IO().Import(bytes.NewReader(full.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("target Import: %v", err)
	}
	if ev, _ := target.Nodes().ByLabel("Event", store.QueryOpts{}); len(ev) != 4 {
		t.Fatalf("target pre-merge Event count = %d, want 4", len(ev))
	}

	res, err := primary.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Mode: adminpkg.PurgeByAge, Before: farFuture})
	if err != nil {
		t.Fatalf("primary purge: %v", err)
	}
	if res.NodesPurged != 4 {
		t.Fatalf("primary purged %d events, want 4", res.NodesPurged)
	}

	var delta bytes.Buffer
	if err := primary.IO().ExportSince(&delta, baseCursor); err != nil {
		t.Fatalf("ExportSince: %v", err)
	}
	if err := target.IO().ImportMerge(bytes.NewReader(delta.Bytes()), tkgio.MergeOptions{ExpectBase: baseCursor}); err != nil {
		t.Fatalf("ImportMerge misdiagnosed ChangeRangePurge: %v", err)
	}

	if ev, _ := target.Nodes().ByLabel("Event", store.QueryOpts{}); len(ev) != 0 {
		t.Fatalf("target post-merge Event count = %d, want 0 (predicate re-executed)", len(ev))
	}
	if m, _ := target.Nodes().ByLabel("Machine", store.QueryOpts{}); len(m) != 1 {
		t.Fatalf("target Machine count = %d, want 1 (survivor)", len(m))
	}
	if in, _ := target.Rels().Incoming(m1.ID(), "ON"); len(in) != 0 {
		t.Fatalf("target machine has %d phantom incoming edges, want 0", len(in))
	}

	// The target's own retention watermark advanced from the merged record.
	if _, err := target.Temporal().NodesAsOf(farFuture - 1); !errors.Is(err, graphpkg.ErrRetentionExpired) {
		t.Fatalf("target NodesAsOf below watermark err=%v, want ErrRetentionExpired", err)
	}
}
