package graph

// add_label_test.go — failing tests for missing Graph.AddNodeLabel and
// GraphTx.{AddNodeLabel, RemoveNodeLabel} public APIs.
//
// These tests intentionally reference methods that do not yet exist; they
// must fail to compile until the upstream methods are implemented.
//
// Requirements under test:
//   Graph.AddNodeLabel(id, label) error
//     - adds label to existing node
//     - idempotent when label already present (no error, no version bump)
//     - validates name length (ErrNameTooLong)
//     - rejects empty label names
//     - enforces MaxLabelsPerNode (ErrTooManyLabels)
//     - storepkg.ErrNodeNotFound for unknown id
//     - advances hash chain (new hash, PrevHash == old hash)
//     - writes version history entry
//     - publishes eventspkg.EventNodeUpdate
//
//   GraphTx.AddNodeLabel(id, label) error
//     - applies inside transaction
//     - rollback undoes the add (label gone after Rollback)
//     - Commit persists the added label
//     - storepkg.ErrTxDone after Commit/Rollback
//
//   GraphTx.RemoveNodeLabel(id, label) error
//     - wraps g.removeNodeLabelInternal under tx lock
//     - rollback restores the removed label
//     - returns ErrLastLabel when removing the only label
//     - storepkg.ErrTxDone after Commit/Rollback

import (
	"errors"
	"strings"
	"testing"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
)

// --- Graph.AddNodeLabel ---

func TestAddNodeLabel_AddsExtraLabel(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, nil)
	id := n.ID()

	if err := g.AddNodeLabel(id, "Employee"); err != nil {
		t.Fatalf("AddNodeLabel: %v", err)
	}

	updated, _ := g.GetNode(id)
	if !g.NodeHasLabel(updated, "Employee") {
		t.Error("Employee label missing after AddNodeLabel")
	}
	if !g.NodeHasLabel(updated, "Person") {
		t.Error("Person label should remain after AddNodeLabel")
	}
}

func TestAddNodeLabel_IdempotentIfAlreadyPresent(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person", "Employee"}, nil)
	id := n.ID()

	before, _ := g.GetNode(id)
	beforeVersion := before.Version()

	if err := g.AddNodeLabel(id, "Employee"); err != nil {
		t.Fatalf("AddNodeLabel on existing label should be a no-op, got: %v", err)
	}

	after, _ := g.GetNode(id)
	if after.Version() != beforeVersion {
		t.Errorf("version should not bump on idempotent add: before=%d after=%d",
			beforeVersion, after.Version())
	}
}

func TestAddNodeLabel_EmptyNameRejected(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, nil)
	id := n.ID()

	if err := g.AddNodeLabel(id, ""); err == nil {
		t.Fatal("expected error for empty label name")
	}
}

func TestAddNodeLabel_NameTooLong(t *testing.T) {
	g, _ := New(Config{Validation: ValidationLimits{MaxNameLength: 5}})
	n, _ := g.AddNode([]string{"Short"}, nil)
	id := n.ID()

	err := g.AddNodeLabel(id, strings.Repeat("a", 6))
	if !errors.Is(err, ErrNameTooLong) {
		t.Fatalf("expected ErrNameTooLong, got %v", err)
	}
}

func TestAddNodeLabel_TooManyLabelsRejected(t *testing.T) {
	g, _ := New(Config{Validation: ValidationLimits{MaxLabelsPerNode: 2}})
	n, _ := g.AddNode([]string{"A", "B"}, nil)
	id := n.ID()

	err := g.AddNodeLabel(id, "C")
	if !errors.Is(err, ErrTooManyLabels) {
		t.Fatalf("expected ErrTooManyLabels, got %v", err)
	}
}

func TestAddNodeLabel_NodeNotFound(t *testing.T) {
	g, _ := New(Config{})
	err := g.AddNodeLabel(999, "Person")
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("expected storepkg.ErrNodeNotFound, got %v", err)
	}
}

func TestAddNodeLabel_HashChainAdvances(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"A"}, nil)
	id := n.ID()

	origHash := ""
	if ig := n.Integrity(); ig != nil {
		origHash = ig.Hash
	}

	if err := g.AddNodeLabel(id, "B"); err != nil {
		t.Fatalf("AddNodeLabel: %v", err)
	}

	updated, _ := g.GetNode(id)
	ig := updated.Integrity()
	if ig == nil {
		t.Fatal("updated node missing integrity")
	}
	if ig.Hash == "" || ig.Hash == origHash {
		t.Errorf("hash should advance: orig=%q new=%q", origHash, ig.Hash)
	}
	if ig.PrevHash != origHash {
		t.Errorf("PrevHash = %q, want %q (hash chain)", ig.PrevHash, origHash)
	}
}

func TestAddNodeLabel_WritesHistoryEntry(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"A"}, nil)
	id := n.ID()

	before, _ := g.GetNodeHistory(id)
	if len(before) != 0 {
		t.Fatalf("expected 0 history entries before, got %d", len(before))
	}

	if err := g.AddNodeLabel(id, "B"); err != nil {
		t.Fatalf("AddNodeLabel: %v", err)
	}

	after, _ := g.GetNodeHistory(id)
	if len(after) != 1 {
		t.Fatalf("expected 1 history entry after add, got %d", len(after))
	}
	if after[0].Version() != 0 {
		t.Errorf("history[0].Version() = %d, want 0", after[0].Version())
	}

	cur, _ := g.GetNode(id)
	if cur.Version() != 1 {
		t.Errorf("current.Version() = %d, want 1", cur.Version())
	}
}

func TestAddNodeLabel_NodesByLabelUpdated(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Thing"}, nil)
	id := n.ID()

	if err := g.AddNodeLabel(id, "Tag"); err != nil {
		t.Fatalf("AddNodeLabel: %v", err)
	}

	nodes, _ := g.NodesByLabel("Tag", storepkg.QueryOpts{})
	found := false
	for _, node := range nodes {
		if node.ID() == id {
			found = true
			break
		}
	}
	if !found {
		t.Error("node not found in new label index after AddNodeLabel")
	}
}

func TestAddNodeLabel_PublishesEvent(t *testing.T) {
	g, _ := New(Config{})
	bus := eventspkg.NewEventBus()
	g.SetEventBus(bus)

	n, _ := g.AddNode([]string{"A"}, nil)
	id := n.ID()

	var events []eventspkg.Event
	bus.Subscribe(func(e eventspkg.Event) {
		events = append(events, e)
	})
	events = nil // clear AddNode event

	if err := g.AddNodeLabel(id, "B"); err != nil {
		t.Fatalf("AddNodeLabel: %v", err)
	}

	if len(events) == 0 {
		t.Fatal("expected eventspkg.EventNodeUpdate, got none")
	}
	if events[0].Type != eventspkg.EventNodeUpdate {
		t.Errorf("event type = %v, want eventspkg.EventNodeUpdate", events[0].Type)
	}
}

// --- GraphTx.AddNodeLabel ---

func TestGraphTx_AddNodeLabel_Commit(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"A"}, nil)
	id := n.ID()

	tx := g.BeginTx()
	if err := tx.AddNodeLabel(id, "B"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddNodeLabel: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	updated, _ := g.GetNode(id)
	if !g.NodeHasLabel(updated, "B") {
		t.Error("label B missing after tx.Commit")
	}
}

func TestGraphTx_AddNodeLabel_Rollback(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"A"}, nil)
	id := n.ID()

	tx := g.BeginTx()
	if err := tx.AddNodeLabel(id, "B"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddNodeLabel: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	updated, _ := g.GetNode(id)
	if g.NodeHasLabel(updated, "B") {
		t.Error("label B should be absent after Rollback")
	}
}

func TestGraphTx_AddNodeLabel_RollbackRestoresLabelIndex(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"A"}, nil)
	id := n.ID()

	tx := g.BeginTx()
	if err := tx.AddNodeLabel(id, "B"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddNodeLabel: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Label index must not contain this node under "B" after rollback.
	nodes, _ := g.NodesByLabel("B", storepkg.QueryOpts{})
	for _, nd := range nodes {
		if nd.ID() == id {
			t.Fatal("label index still contains node under B after rollback")
		}
	}
}

func TestGraphTx_RemoveNodeLabel_RollbackRestoresLabelIndex(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"A", "B"}, nil)
	id := n.ID()

	tx := g.BeginTx()
	if err := tx.RemoveNodeLabel(id, "B"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.RemoveNodeLabel: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	nodes, _ := g.NodesByLabel("B", storepkg.QueryOpts{})
	found := false
	for _, nd := range nodes {
		if nd.ID() == id {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("label index missing node under B after rollback")
	}
}

func TestGraphTx_AddNodeLabel_AfterCommitReturnsTxDone(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"A"}, nil)
	id := n.ID()

	tx := g.BeginTx()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := tx.AddNodeLabel(id, "B"); !errors.Is(err, storepkg.ErrTxDone) {
		t.Fatalf("expected storepkg.ErrTxDone, got %v", err)
	}
}

// --- GraphTx.RemoveNodeLabel ---

func TestGraphTx_RemoveNodeLabel_Commit(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"A", "B"}, nil)
	id := n.ID()

	tx := g.BeginTx()
	if err := tx.RemoveNodeLabel(id, "B"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.RemoveNodeLabel: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	updated, _ := g.GetNode(id)
	if g.NodeHasLabel(updated, "B") {
		t.Error("label B should be gone after Commit")
	}
}

func TestGraphTx_RemoveNodeLabel_Rollback(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"A", "B"}, nil)
	id := n.ID()

	tx := g.BeginTx()
	if err := tx.RemoveNodeLabel(id, "B"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.RemoveNodeLabel: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	updated, _ := g.GetNode(id)
	if !g.NodeHasLabel(updated, "B") {
		t.Error("label B should be restored after Rollback")
	}
}

func TestGraphTx_RemoveNodeLabel_LastLabelError(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Solo"}, nil)
	id := n.ID()

	tx := g.BeginTx()
	defer tx.Rollback()

	err := tx.RemoveNodeLabel(id, "Solo")
	if !errors.Is(err, ErrLastLabel) {
		t.Fatalf("expected ErrLastLabel, got %v", err)
	}
}

func TestGraphTx_RemoveNodeLabel_AfterRollbackReturnsTxDone(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"A", "B"}, nil)
	id := n.ID()

	tx := g.BeginTx()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := tx.RemoveNodeLabel(id, "B"); !errors.Is(err, storepkg.ErrTxDone) {
		t.Fatalf("expected storepkg.ErrTxDone, got %v", err)
	}
}
