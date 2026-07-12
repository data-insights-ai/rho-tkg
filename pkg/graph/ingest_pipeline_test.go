package graph_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// TestIngestPipeline_PublicFacadeSmoke exercises every public method on the
// ingest sub-API through the graph façade end-to-end (Testing Rule 1).
func TestIngestPipeline_PublicFacadeSmoke(t *testing.T) {
	g, err := graph.New(graph.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Sync session: ack ⇒ visible.
	s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true, DeclareLabels: []string{"Person"}})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	n, err := s.AddNode([]string{"Person"}, map[string]any{"name": "Ada"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	tok, err := s.Submit()
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if _, err := g.Nodes().Get(context.Background(), n.ID()); err != nil {
		t.Fatalf("Get after sync ack: %v", err)
	}
	if err := g.Ingest().WaitApplied(tok); err != nil {
		t.Fatalf("WaitApplied: %v", err)
	}
	if g.Ingest().AppliedSeq() < tok.Seq {
		t.Fatalf("AppliedSeq %d < token %d", g.Ingest().AppliedSeq(), tok.Seq)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("session Close: %v", err)
	}

	// nil-safe façade.
	if (*graph.Graph)(nil).Ingest() != nil {
		t.Fatalf("nil graph Ingest() should be nil")
	}
}

// TestIngestPipeline_ReplicaByteExact is the crown acceptance test: a primary
// written ENTIRELY through the ingest pipeline replicates BYTE-EXACT to a
// read-only replica that bootstraps from a snapshot and tails the change-feed.
// Reuses the existing replica-convergence harness (assertConverged) with a
// pipeline-fed source, proving the pipeline's co-committed change-log records
// are indistinguishable from the standalone doors' (invariant: replica
// byte-exactness — §9 item 6).
func TestIngestPipeline_ReplicaByteExact(t *testing.T) {
	ctx := context.Background()

	primary, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()

	// Seed via the pipeline: nodes, rels, an update, and a delete-cascade.
	sess, err := primary.Ingest().NewSession(ingest.IngestOptions{Sync: true, DeclareLabels: []string{"A", "B"}, DeclareRelTypes: []string{"KNOWS"}})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	a, err := sess.AddNode([]string{"A"}, map[string]any{"n": "a"})
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := sess.AddNode([]string{"A", "B"}, map[string]any{"n": "b"})
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	cnode, err := sess.AddNode([]string{"A"}, map[string]any{"n": "c"})
	if err != nil {
		t.Fatalf("AddNode c: %v", err)
	}
	if _, err := sess.AddRelationship("KNOWS", a, b, map[string]any{"w": int64(1)}); err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	if _, err := sess.Submit(); err != nil {
		t.Fatalf("Submit seed: %v", err)
	}

	// A with-history update and a cascade delete, both through the pipeline.
	if err := sess.UpdateNode(a.ID(), map[string]any{"n": "a2"}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	if _, err := sess.AddRelationship("KNOWS", b, cnode, nil); err != nil {
		t.Fatalf("AddRelationship b->c: %v", err)
	}
	if _, err := sess.Submit(); err != nil {
		t.Fatalf("Submit update: %v", err)
	}
	if err := sess.DeleteNode(cnode.ID()); err != nil {
		t.Fatalf("DeleteNode c: %v", err)
	}
	if _, err := sess.Submit(); err != nil {
		t.Fatalf("Submit delete: %v", err)
	}

	// Snapshot + handoff LSN.
	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}

	// Bootstrap a replica from the snapshot at LSN 0 (import records the
	// snapshot LSN as the initial applied watermark), then tail everything.
	replica, err := graph.New(graph.Config{SnowflakeNodeID: 2, BadgerInMemory: true, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer replica.Close()

	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("replica Import: %v", err)
	}
	from, err := replica.Replication().AppliedLSN()
	if err != nil {
		t.Fatalf("AppliedLSN: %v", err)
	}
	var recs []store.ChangeRecord
	if err := primary.Replication().ForEachChange(from, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if len(recs) > 0 {
		if _, err := replica.Replication().ApplyChanges(recs); err != nil {
			t.Fatalf("ApplyChanges: %v", err)
		}
	}

	assertConverged(t, "pipeline-fed replica", primary, replica)
	_ = ctx
}
