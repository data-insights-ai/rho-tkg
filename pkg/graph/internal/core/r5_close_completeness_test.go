// Tests in this file pin R5-F1 from the 2026-05-09 round-5
// maintainability review: every public sub-API entry point must reject
// post-close calls with ErrGraphClosed instead of touching the
// (possibly-closed) store, registries, or indexes. Round 4 covered the
// mutation paths that go through runUnderRLock; round 5 extends the
// guard to reads, queries, hash, stats, admin, IO, index management,
// constraints, BeginTx, and Batch.New.
package core

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	tkgio "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/io"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/temporal"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// closedGraph returns a freshly-created graph that has been Close()d.
// Each test starts from this state so we exercise the post-close
// branch of every sub-API method.
func closedGraph(t *testing.T) *Core {
	t.Helper()
	g := newTestGraph(t)
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return g
}

func TestR5_PostClose_NodeOps_AllReadsReturnErrGraphClosed(t *testing.T) {
	t.Parallel()
	g := closedGraph(t)

	if _, err := g.Nodes.Get(context.Background(), types.NodeID(1)); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Nodes.Get: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Nodes.Get(context.Background(), types.NodeID(1)); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Nodes.GetWithContext: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Nodes.ByLabel("X", storepkg.QueryOpts{}); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Nodes.ByLabel: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Nodes.ByLabelAndProperty("X", "k", "v", storepkg.QueryOpts{}); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Nodes.ByLabelAndProperty: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Nodes.All(storepkg.QueryOpts{}); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Nodes.All: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Nodes.GetByIDs([]types.NodeID{1}); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Nodes.GetByIDs: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Nodes.Count(); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Nodes.Count: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Nodes.CountByLabel("X"); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Nodes.CountByLabel: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Nodes.History(types.NodeID(1)); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Nodes.History: %v, want ErrGraphClosed", err)
	}
}

func TestR5_PostClose_RelOps_AllReadsReturnErrGraphClosed(t *testing.T) {
	t.Parallel()
	g := closedGraph(t)

	if _, err := g.Rels.Get(context.Background(), types.RelID(1)); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Rels.Get: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Rels.Get(context.Background(), types.RelID(1)); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Rels.GetWithContext: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Rels.ByType("X", storepkg.QueryOpts{}); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Rels.ByType: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Rels.Outgoing(types.NodeID(1), ""); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Rels.Outgoing: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Rels.OutgoingForNodes([]types.NodeID{1}, ""); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Rels.OutgoingForNodes: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Rels.Incoming(types.NodeID(1), ""); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Rels.Incoming: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Rels.IncomingForNodes([]types.NodeID{1}, ""); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Rels.IncomingForNodes: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Rels.Count(); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Rels.Count: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Rels.All(storepkg.QueryOpts{}); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Rels.All: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Rels.GetByIDs([]types.RelID{1}); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Rels.GetByIDs: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Rels.CountByType("X"); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Rels.CountByType: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Rels.History(types.RelID(1)); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Rels.History: %v, want ErrGraphClosed", err)
	}
}

func TestR5_PostClose_Stats_ReturnErrGraphClosed(t *testing.T) {
	t.Parallel()
	g := closedGraph(t)

	if _, err := g.Stats.NodeCount(); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Stats.NodeCount: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Stats.RelCount(); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Stats.RelCount: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Stats.AllLabelCounts(); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Stats.AllLabelCounts: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Stats.AllRelTypeCounts(); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Stats.AllRelTypeCounts: %v, want ErrGraphClosed", err)
	}
}

func TestR5_PostClose_Hash_ReturnErrGraphClosed(t *testing.T) {
	t.Parallel()
	g := closedGraph(t)

	if _, err := g.Hash.VerifyNodeChain(types.NodeID(1)); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Hash.VerifyNodeChain: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Hash.VerifyRelChain(types.RelID(1)); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Hash.VerifyRelChain: %v, want ErrGraphClosed", err)
	}
}

func TestR5_PostClose_Index_ReturnErrGraphClosed(t *testing.T) {
	t.Parallel()
	g := closedGraph(t)

	if err := g.Index.CreateProperty("X", "k"); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Index.CreateProperty: %v, want ErrGraphClosed", err)
	}
	if err := g.Index.DropProperty("X", "k"); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Index.DropProperty: %v, want ErrGraphClosed", err)
	}
	if err := g.Index.CreateTemporal("X"); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Index.CreateTemporal: %v, want ErrGraphClosed", err)
	}
	if err := g.Index.DropTemporal("X"); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Index.DropTemporal: %v, want ErrGraphClosed", err)
	}
	if err := g.Index.CreateHighFrequency("X", time.Hour); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Index.CreateHighFrequency: %v, want ErrGraphClosed", err)
	}
	if err := g.Index.DropHighFrequency("X"); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Index.DropHighFrequency: %v, want ErrGraphClosed", err)
	}
	if err := g.Index.CreateVector("X", "embedding", 2, storepkg.DistanceCosine); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Index.CreateVector: %v, want ErrGraphClosed", err)
	}
	if err := g.Index.DropVector("X", "embedding"); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Index.DropVector: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Index.SearchNearest("X", "embedding", []float32{1, 0}, 1, storepkg.QueryOpts{}); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Index.SearchNearest: %v, want ErrGraphClosed", err)
	}
	if err := g.Index.UnregisterProvider("missing"); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Index.UnregisterProvider: %v, want ErrGraphClosed", err)
	}
}

func TestR5_PostClose_Resolve_GetOrCreateReturnErrGraphClosed(t *testing.T) {
	t.Parallel()
	g := closedGraph(t)

	if _, err := g.Resolve.GetOrCreateLabel("X"); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Resolve.GetOrCreateLabel: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Resolve.GetOrCreateRelType("R"); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Resolve.GetOrCreateRelType: %v, want ErrGraphClosed", err)
	}
}

func TestR5_PostClose_NoErrorResolversReturnZeroValues(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("Add node a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add node b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"weight": int64(1)})
	if err != nil {
		t.Fatalf("Add relationship: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if tok, ok := g.Resolve.LookupLabel("Person"); ok || tok != 0 {
		t.Fatalf("LookupLabel after close = (%d, %v), want (0, false)", tok, ok)
	}
	if tok, ok := g.Resolve.LookupRelType("KNOWS"); ok || tok != 0 {
		t.Fatalf("LookupRelType after close = (%d, %v), want (0, false)", tok, ok)
	}
	if labels := g.Nodes.Labels(a); labels != nil {
		t.Fatalf("Nodes.Labels after close = %v, want nil", labels)
	}
	if got := g.Nodes.PrimaryLabel(a); got != "" {
		t.Fatalf("Nodes.PrimaryLabel after close = %q, want empty", got)
	}
	if g.Nodes.HasLabel(a, "Person") {
		t.Fatal("Nodes.HasLabel after close = true, want false")
	}
	if got := g.Rels.Type(r); got != "" {
		t.Fatalf("Rels.Type after close = %q, want empty", got)
	}
	if g.Rels.HasType(r, "KNOWS") {
		t.Fatal("Rels.HasType after close = true, want false")
	}
	if got, ok := g.Resolve.NodeProperty(a, "name"); ok || got != nil {
		t.Fatalf("Resolve.NodeProperty after close = (%v, %v), want (nil, false)", got, ok)
	}
	if got, ok := g.Resolve.RelProperty(r, "weight"); ok || got != nil {
		t.Fatalf("Resolve.RelProperty after close = (%v, %v), want (nil, false)", got, ok)
	}
}

func TestR5_PostClose_IO_ReturnErrGraphClosed(t *testing.T) {
	t.Parallel()
	g := closedGraph(t)

	var out bytes.Buffer
	if err := g.IO.Export(&out); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("IO.Export: %v, want ErrGraphClosed", err)
	}
	if err := g.IO.Import(bytes.NewReader(nil), tkgio.ImportOptions{}); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("IO.Import: %v, want ErrGraphClosed", err)
	}
	if err := g.IO.ImportWithOptions(bytes.NewReader(nil), tkgio.ImportOptions{}); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("IO.ImportWithOptions: %v, want ErrGraphClosed", err)
	}
}

func TestR5_PostClose_Tx_BeginReturnErrGraphClosed(t *testing.T) {
	t.Parallel()
	g := closedGraph(t)

	tx, err := g.BeginTx()
	if !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("BeginTx: %v, want ErrGraphClosed", err)
	}
	if tx != nil {
		t.Errorf("BeginTx returned non-nil tx %v after close", tx)
	}
}

func TestR5_PostClose_Batch_NewReturnErrGraphClosed(t *testing.T) {
	t.Parallel()
	g := closedGraph(t)

	bb, err := NewBatchBuilder(g)
	if !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("NewBatchBuilder: %v, want ErrGraphClosed", err)
	}
	if bb != nil {
		t.Errorf("NewBatchBuilder returned non-nil builder %v after close", bb)
	}
}

// R5-F5: a builder constructed against an open graph but used after
// the graph is closed must reject every queue method with
// ErrGraphClosed. Without the gate, AddNode/AddRelationship would
// keep allocating registry tokens against the closed graph's
// (possibly-closing) registry.
func TestR5_PostClose_BatchBuilder_QueueMethodsReturnErrGraphClosed(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	bb, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := bb.AddNode([]string{"X"}, nil); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("AddNode: %v, want ErrGraphClosed", err)
	}
	// Build minimal endpoint nodes without going through bb (those would
	// also reject post-close); the AddRelationship gate fires before
	// the nil-check on the endpoints.
	a := types.NewNode(types.NodeID(1), 1, nil)
	if _, err := bb.AddRelationship("R", a, a, nil); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("AddRelationship: %v, want ErrGraphClosed", err)
	}
	if err := bb.UpdateNode(types.NodeID(1), map[string]any{"k": "v"}); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("UpdateNode: %v, want ErrGraphClosed", err)
	}
	if err := bb.UpdateRelationship(types.RelID(1), map[string]any{"k": "v"}); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("UpdateRelationship: %v, want ErrGraphClosed", err)
	}
	if err := bb.DeleteNode(types.NodeID(1)); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("DeleteNode: %v, want ErrGraphClosed", err)
	}
	if err := bb.DeleteRelationship(types.RelID(1)); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("DeleteRelationship: %v, want ErrGraphClosed", err)
	}
	if _, err := bb.Execute(); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Execute: %v, want ErrGraphClosed", err)
	}
}

func TestR5_PostClose_Temporal_ReturnErrGraphClosed(t *testing.T) {
	t.Parallel()
	g := closedGraph(t)

	if _, err := g.Temporal.NodeAt(types.NodeID(1), 0); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Temporal.NodeAt: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Temporal.RelAt(types.RelID(1), 0); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Temporal.RelAt: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Temporal.Snapshot(0); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Temporal.Snapshot: %v, want ErrGraphClosed", err)
	}
}

// Constraints.Get is a pure in-memory read; no store contact. After
// Close the in-memory ConstraintSet is still safe to read. Add/Set
// report ErrGraphClosed like other public mutators and leave the set
// unchanged.
func TestR5_PostClose_Constraints_AddSetReturnErrGraphClosed(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	addRelWithinEndpointsConstraint(t, g)
	if got := g.Constraints.Get().Len(); got != 1 {
		t.Fatalf("Constraints.Get().Len before close = %d, want 1", got)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := g.Constraints.Add(temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints}); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("Constraints.Add after close = %v, want ErrGraphClosed", err)
	}
	if err := g.Constraints.Set(temporalpkg.NewConstraintSet()); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("Constraints.Set after close = %v, want ErrGraphClosed", err)
	}
	if err := g.Constraints.Add(temporalpkg.TemporalConstraint{}); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("Constraints.Add invalid after close = %v, want ErrGraphClosed", err)
	}
	if err := g.Constraints.Set(temporalpkg.NewConstraintSet(temporalpkg.TemporalConstraint{})); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("Constraints.Set invalid after close = %v, want ErrGraphClosed", err)
	}
	if got := g.Constraints.Get().Len(); got != 1 {
		t.Fatalf("Constraints.Get().Len after post-close Add/Set = %d, want 1", got)
	}
}

func TestR5_PostClose_EventSettersReturnErrGraphClosed(t *testing.T) {
	t.Parallel()

	g := newTestGraph(t)
	bus := eventspkg.NewEventBus()
	if err := g.Events.SetSync(bus); err != nil {
		t.Fatalf("Events.SetSync before close: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := g.Events.SetSync(nil); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("Events.SetSync(nil) after close = %v, want ErrGraphClosed", err)
	}
	g.mu.RLock()
	gotPublisher := g.events
	g.mu.RUnlock()
	if gotPublisher != bus {
		t.Fatalf("Events.SetSync(nil) after close mutated bus: got %p, want %p", gotPublisher, bus)
	}
	if got := g.Events.GetSync(); got != nil {
		t.Fatalf("Events.GetSync after close = %p, want nil", got)
	}
	if err := g.Events.SetAsync(nil); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("Events.SetAsync(nil) after close = %v, want ErrGraphClosed", err)
	}
	g.mu.RLock()
	gotPublisher = g.events
	g.mu.RUnlock()
	if gotPublisher != bus {
		t.Fatalf("Events.SetAsync(nil) after close mutated bus: got %p, want %p", gotPublisher, bus)
	}

	g2 := closedGraph(t)
	if err := g2.Events.SetSync(eventspkg.NewEventBus()); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("Events.SetSync after close = %v, want ErrGraphClosed", err)
	}
	if got := g2.Events.GetSync(); got != nil {
		t.Fatalf("Events.SetSync after close installed bus %p, want nil", got)
	}
}

func TestR5_PostClose_NoErrorIDAllocatorsReturnZero(t *testing.T) {
	t.Parallel()

	g := closedGraph(t)
	if got := g.Nodes.NextID(); got != 0 {
		t.Fatalf("Nodes.NextID after close = %d, want 0", got)
	}
	if got := g.Rels.NextID(); got != 0 {
		t.Fatalf("Rels.NextID after close = %d, want 0", got)
	}
}
