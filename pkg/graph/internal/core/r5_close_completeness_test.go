// Tests in this file pin R5-F1 from the 2026-05-09 round-5
// maintainability review: every public sub-API entry point must reject
// post-close calls with ErrGraphClosed instead of touching the
// (possibly-closed) store, registries, or indexes. Round 4 covered the
// mutation paths that go through runUnderRLock; round 5 extends the
// guard to reads, queries, hash, stats, admin, IO, index management,
// constraints, BeginTx, and Batch.New.
package core

import (
	"context"
	"errors"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
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

	if _, err := g.Nodes.Get(types.NodeID(1)); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Nodes.Get: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Nodes.GetWithContext(context.Background(), types.NodeID(1)); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Nodes.GetWithContext: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Nodes.ByLabel("X", storepkg.QueryOpts{}); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Nodes.ByLabel: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Nodes.All(storepkg.QueryOpts{}); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Nodes.All: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Nodes.Count(); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Nodes.Count: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Nodes.CountByLabel("X"); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Nodes.CountByLabel: %v, want ErrGraphClosed", err)
	}
}

func TestR5_PostClose_RelOps_AllReadsReturnErrGraphClosed(t *testing.T) {
	t.Parallel()
	g := closedGraph(t)

	if _, err := g.Rels.Get(types.RelID(1)); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Rels.Get: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Rels.ByType("X", storepkg.QueryOpts{}); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Rels.ByType: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Rels.Outgoing(types.NodeID(1), ""); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Rels.Outgoing: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Rels.Incoming(types.NodeID(1), ""); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Rels.Incoming: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Rels.Count(); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Rels.Count: %v, want ErrGraphClosed", err)
	}
	if _, err := g.Rels.All(storepkg.QueryOpts{}); !errors.Is(err, ErrGraphClosed) {
		t.Errorf("Rels.All: %v, want ErrGraphClosed", err)
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
// silently drop post-close mutations rather than racing the lifecycle
// teardown — the constraint set is irrelevant once the underlying
// store is gone.
