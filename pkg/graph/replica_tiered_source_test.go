package graph_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// newTieredChangeLogPrimary builds a change-log-enabled tiered graph. "Case" is a
// reference label (routes to refShard); any other label (e.g. "Signal") routes to
// the hot event shard — so a rel between them is a CROSS-SHARD relationship whose
// cascade emits per-shard records (ADR-0005 §2.4, Stage F).
func newTieredChangeLogPrimary(t *testing.T) *graph.Graph {
	t.Helper()
	ts, err := tiered.New(tiered.Config{
		InMemory:      true,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
		ChangeLog:     true,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := graph.New(graph.Config{SnowflakeNodeID: 3, Store: ts})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	t.Cleanup(func() { g.Close() })
	return g
}

// A change-log-enabled TIERED primary unlocks the whole replication surface: its
// change feed drives a read-only replica to a byte-exact copy, including a
// CROSS-SHARD cascade delete (reference node + event edge), exercising Stage F's
// per-shard cascade records applied in ascending LSN order.
func TestReplicaConvergence_TieredPrimaryByteExact(t *testing.T) {
	ctx := context.Background()
	primary := newTieredChangeLogPrimary(t)

	// Reference node (refShard) + two event nodes (hot event shard).
	caseA := mustAdd(t, primary, []string{"Case"}, map[string]any{"n": "a"})
	sig1 := mustAdd(t, primary, []string{"Signal"}, map[string]any{"n": "s1"})
	sig2 := mustAdd(t, primary, []string{"Signal"}, map[string]any{"n": "s2"})
	// Cross-shard rel: reference -> event (refShard endpoint, event endpoint).
	rX, err := primary.Rels().AddByID(ctx, "EMITS", caseA.ID(), sig1.ID(), map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatalf("seed cross-shard rel: %v", err)
	}

	// Bootstrap a read-only replica (memory single shard) from a snapshot.
	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	lsn0, err := primary.Replication().LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}
	replica, err := graph.New(graph.Config{SnowflakeNodeID: 4, BadgerInMemory: true, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer replica.Close()
	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("replica Import: %v", err)
	}
	if err := replica.Replication().SetAppliedLSN(lsn0); err != nil {
		t.Fatalf("SetAppliedLSN: %v", err)
	}
	assertConverged(t, "tiered bootstrap", primary, replica)

	// Drive a mix of record kinds using only already-known tokens.
	if _, err := primary.Nodes().Update(ctx, caseA.ID(), map[string]any{"n": "a2"}); err != nil {
		t.Fatalf("Update caseA (with-history): %v", err)
	}
	if _, err := primary.Nodes().UpdateInPlace(ctx, sig1.ID(), map[string]any{"n": "s1b"}); err != nil {
		t.Fatalf("UpdateInPlace sig1: %v", err)
	}
	if _, err := primary.Rels().Update(ctx, rX.ID(), map[string]any{"w": int64(2)}); err != nil {
		t.Fatalf("Update rel (with-history): %v", err)
	}
	// Cross-shard cascade delete: deleting the reference node removes the
	// event-shard edge — records land on both shards with ascending LSNs.
	if err := primary.Nodes().Delete(ctx, caseA.ID()); err != nil {
		t.Fatalf("Delete caseA (cross-shard cascade): %v", err)
	}
	_ = sig2

	// Tail from the handoff LSN and apply, asserting ascending order end-to-end.
	from, err := replica.Replication().AppliedLSN()
	if err != nil {
		t.Fatalf("AppliedLSN: %v", err)
	}
	var recs []store.ChangeRecord
	var prev uint64
	if err := primary.Replication().ForEachChange(from, func(rec store.ChangeRecord) bool {
		if rec.LSN <= prev {
			t.Errorf("feed not ascending: %d after %d", rec.LSN, prev)
		}
		prev = rec.LSN
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("no records past the snapshot — nothing to tail")
	}
	applied, err := replica.Replication().ApplyChanges(recs)
	if err != nil {
		t.Fatalf("ApplyChanges: %v", err)
	}
	if want := recs[len(recs)-1].LSN; applied != want {
		t.Fatalf("applied LSN = %d, want %d", applied, want)
	}

	assertConverged(t, "tiered tail (cross-shard cascade)", primary, replica)

	// The cascaded node's tombstone history must replicate byte-for-byte.
	pHist := mustNodeHistory(t, primary, caseA.ID())
	rHist := mustNodeHistory(t, replica, caseA.ID())
	if len(pHist) != len(rHist) {
		t.Fatalf("cascaded node history depth: primary %d, replica %d", len(pHist), len(rHist))
	}
	for i := range pHist {
		if ph, rh := nodeHash(pHist[i]), nodeHash(rHist[i]); ph != rh {
			t.Fatalf("cascaded node history[%d] hash mismatch: %q vs %q", i, ph, rh)
		}
	}
}

// ExportSince / Watermark / ImportMerge run on a tiered source: a delta stream of
// post-cursor changes merges onto a base to an equivalent graph (ADR-0005 §2 Stage
// F downstream unlock).
func TestExportSince_TieredSource(t *testing.T) {
	ctx := context.Background()
	src := newTieredChangeLogPrimary(t)

	a := mustAdd(t, src, []string{"Case"}, map[string]any{"n": "a"})
	mustAdd(t, src, []string{"Signal"}, map[string]any{"n": "s1"})
	c0, err := src.IO().Watermark()
	if err != nil {
		t.Fatalf("Watermark: %v", err)
	}

	// Full snapshot into a memory base.
	base, err := graph.New(graph.Config{SnowflakeNodeID: 5, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("base New: %v", err)
	}
	defer base.Close()
	var full bytes.Buffer
	if err := src.IO().Export(&full); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if err := base.IO().Import(&full, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("base Import: %v", err)
	}

	// Post-cursor mutations, then a delta merge.
	if _, err := src.Nodes().Update(ctx, a.ID(), map[string]any{"n": "a2"}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	mustAdd(t, src, []string{"Signal"}, map[string]any{"n": "s2"})

	var delta bytes.Buffer
	if err := src.IO().ExportSince(&delta, c0); err != nil {
		t.Fatalf("ExportSince: %v", err)
	}
	if err := base.IO().ImportMerge(bytes.NewReader(delta.Bytes()), tkgio.MergeOptions{ExpectBase: c0}); err != nil {
		t.Fatalf("ImportMerge: %v", err)
	}
	assertConverged(t, "tiered source delta merge", src, base)
}
