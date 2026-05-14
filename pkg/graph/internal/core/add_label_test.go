package core

// add_label_test.go — failing tests for missing Graph.Nodes.AddLabel and
// GraphTx.{AddNodeLabel, RemoveNodeLabel} public APIs.
//
// These tests intentionally reference methods that do not yet exist; they
// must fail to compile until the upstream methods are implemented.
//
// Requirements under test:
//   Graph.Nodes.AddLabel(context.Background(), id, label) error
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
//   GraphTx.Nodes.AddLabel(context.Background(), id, label) error
//     - applies inside transaction
//     - rollback undoes the add (label gone after Rollback)
//     - Commit persists the added label
//     - storepkg.ErrTxDone after Commit/Rollback
//
//   GraphTx.Nodes.RemoveLabel(context.Background(), id, label) error
//     - wraps g.removeNodeLabelInternal under tx lock
//     - rollback restores the removed label
//     - returns ErrLastLabel when removing the only label
//     - storepkg.ErrTxDone after Commit/Rollback

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store/memory"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/events"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// --- Graph.Nodes.AddLabel ---

func TestAddNodeLabel_AddsExtraLabel(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	id := n.ID()

	if err := g.Nodes.AddLabel(context.Background(), id, "Employee"); err != nil {
		t.Fatalf("AddNodeLabel: %v", err)
	}

	updated, _ := g.Nodes.Get(context.Background(), id)
	if !g.Nodes.HasLabel(updated, "Employee") {
		t.Error("Employee label missing after AddNodeLabel")
	}
	if !g.Nodes.HasLabel(updated, "Person") {
		t.Error("Person label should remain after AddNodeLabel")
	}
}

func TestAddNodeLabel_IdempotentIfAlreadyPresent(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person", "Employee"}, nil)
	id := n.ID()

	before, _ := g.Nodes.Get(context.Background(), id)
	beforeVersion := before.Version()

	if err := g.Nodes.AddLabel(context.Background(), id, "Employee"); err != nil {
		t.Fatalf("AddNodeLabel on existing label should be a no-op, got: %v", err)
	}

	after, _ := g.Nodes.Get(context.Background(), id)
	if after.Version() != beforeVersion {
		t.Errorf("version should not bump on idempotent add: before=%d after=%d",
			beforeVersion, after.Version())
	}
}

type addLabelCorruptTokenStore struct {
	*memory.Store
}

func TestAddNodeLabel_CorruptFutureTokenRollsBackRegistry(t *testing.T) {
	backing := memory.New()
	id := types.NodeID(424242)
	if err := backing.PutNode(types.NewNode(id, 1, nil)); err != nil {
		t.Fatalf("seed corrupt node: %v", err)
	}

	g, err := New(Config{Store: &addLabelCorruptTokenStore{Store: backing}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if g.storeRowsTrust {
		t.Fatal("wrapper store must be treated as untrusted")
	}

	err = g.Nodes.AddLabel(context.Background(), id, "Corrupt")
	if !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("AddLabel corrupt future token = %v, want ErrInvalidStoreMutation", err)
	}
	if _, ok := g.labels.Lookup("Corrupt"); ok {
		t.Fatal("failed AddLabel left newly allocated label registered")
	}

	done := make(chan error, 1)
	go func() {
		_, err := g.Resolve.GetOrCreateLabel("AfterCorruptAdd")
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("registry usable after corrupt AddLabel: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("registry lock was not released after corrupt AddLabel")
	}
}

func TestAddNodeLabel_EmptyNameRejected(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	id := n.ID()

	if err := g.Nodes.AddLabel(context.Background(), id, ""); err == nil {
		t.Fatal("expected error for empty label name")
	}
}

func TestAddNodeLabel_NameTooLong(t *testing.T) {
	g, _ := New(Config{Validation: ValidationLimits{MaxNameLength: 5}})
	n, _ := g.Nodes.Add(context.Background(), []string{"Short"}, nil)
	id := n.ID()

	err := g.Nodes.AddLabel(context.Background(), id, strings.Repeat("a", 6))
	if !errors.Is(err, ErrNameTooLong) {
		t.Fatalf("expected ErrNameTooLong, got %v", err)
	}
}

func TestAddNodeLabel_TooManyLabelsRejected(t *testing.T) {
	g, _ := New(Config{Validation: ValidationLimits{MaxLabelsPerNode: 2}})
	n, _ := g.Nodes.Add(context.Background(), []string{"A", "B"}, nil)
	id := n.ID()

	err := g.Nodes.AddLabel(context.Background(), id, "C")
	if !errors.Is(err, ErrTooManyLabels) {
		t.Fatalf("expected ErrTooManyLabels, got %v", err)
	}
}

func TestAddNodeLabel_TooManyLabelsDoesNotRegisterRejectedLabel(t *testing.T) {
	g, _ := New(Config{Validation: ValidationLimits{MaxLabelsPerNode: 1}})
	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)

	err := g.Nodes.AddLabel(context.Background(), n.ID(), "Rejected")
	if !errors.Is(err, ErrTooManyLabels) {
		t.Fatalf("AddLabel error = %v, want ErrTooManyLabels", err)
	}
	if _, ok := g.Resolve.LookupLabel("Rejected"); ok {
		t.Fatal("rejected AddLabel registered label token")
	}
}

func TestAddNodeLabel_NodeNotFound(t *testing.T) {
	g, _ := New(Config{})
	err := g.Nodes.AddLabel(context.Background(), 999, "Person")
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("expected storepkg.ErrNodeNotFound, got %v", err)
	}
}

func TestNodeLabelMutationsValidateNameBeforeNodeLookup(t *testing.T) {
	g, _ := New(Config{})

	checks := []struct {
		name string
		run  func() error
	}{
		{name: "add", run: func() error {
			return g.Nodes.AddLabel(context.Background(), types.NodeID(999), " ")
		}},
		{name: "remove", run: func() error {
			return g.Nodes.RemoveLabel(context.Background(), types.NodeID(999), " ")
		}},
	}

	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.run(); !errors.Is(err, ErrEmptyName) {
				t.Fatalf("err = %v, want ErrEmptyName", err)
			}
		})
	}
}

func TestAddNodeLabel_HashChainAdvances(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	id := n.ID()

	origHash := ""
	if ig := n.Integrity(); ig != nil {
		origHash = ig.Hash
	}

	if err := g.Nodes.AddLabel(context.Background(), id, "B"); err != nil {
		t.Fatalf("AddNodeLabel: %v", err)
	}

	updated, _ := g.Nodes.Get(context.Background(), id)
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
	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	id := n.ID()

	before, _ := g.Nodes.History(id)
	if len(before) != 0 {
		t.Fatalf("expected 0 history entries before, got %d", len(before))
	}

	if err := g.Nodes.AddLabel(context.Background(), id, "B"); err != nil {
		t.Fatalf("AddNodeLabel: %v", err)
	}

	after, _ := g.Nodes.History(id)
	if len(after) != 1 {
		t.Fatalf("expected 1 history entry after add, got %d", len(after))
	}
	if after[0].Version() != 0 {
		t.Errorf("history[0].Version() = %d, want 0", after[0].Version())
	}

	cur, _ := g.Nodes.Get(context.Background(), id)
	if cur.Version() != 1 {
		t.Errorf("current.Version() = %d, want 1", cur.Version())
	}
}

func TestAddNodeLabel_NodesByLabelUpdated(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"Thing"}, nil)
	id := n.ID()

	if err := g.Nodes.AddLabel(context.Background(), id, "Tag"); err != nil {
		t.Fatalf("AddNodeLabel: %v", err)
	}

	nodes, _ := g.Nodes.ByLabel("Tag", storepkg.QueryOpts{})
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
	_ = g.Events.SetSync(bus)

	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	id := n.ID()

	var events []eventspkg.Event
	bus.Subscribe(func(e eventspkg.Event) {
		events = append(events, e)
	})
	events = nil // clear AddNode event

	if err := g.Nodes.AddLabel(context.Background(), id, "B"); err != nil {
		t.Fatalf("AddNodeLabel: %v", err)
	}

	if len(events) == 0 {
		t.Fatal("expected eventspkg.EventNodeUpdate, got none")
	}
	if events[0].Type != eventspkg.EventNodeUpdate {
		t.Errorf("event type = %v, want eventspkg.EventNodeUpdate", events[0].Type)
	}
}

// --- GraphTx.Nodes.AddLabel ---

func TestGraphTx_AddNodeLabel_Commit(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	id := n.ID()

	tx, _ := g.BeginTx()
	if err := tx.AddNodeLabel(id, "B"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddNodeLabel: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	updated, _ := g.Nodes.Get(context.Background(), id)
	if !g.Nodes.HasLabel(updated, "B") {
		t.Error("label B missing after tx.Commit")
	}
}

func TestGraphTx_AddNodeLabel_Rollback(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	id := n.ID()

	tx, _ := g.BeginTx()
	if err := tx.AddNodeLabel(id, "B"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddNodeLabel: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	updated, _ := g.Nodes.Get(context.Background(), id)
	if g.Nodes.HasLabel(updated, "B") {
		t.Error("label B should be absent after Rollback")
	}
}

func TestGraphTx_AddNodeLabel_IdempotentDoesNotSnapshot(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	id := n.ID()

	tx, _ := g.BeginTx()
	if err := tx.AddNodeLabel(id, "A"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddNodeLabel existing label: %v", err)
	}
	if len(tx.updatedNodes) != 0 {
		_ = tx.Rollback()
		t.Fatalf("updated node snapshots = %d, want 0 for idempotent add-label", len(tx.updatedNodes))
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	updated, _ := g.Nodes.Get(context.Background(), id)
	if updated.Version() != 0 {
		t.Fatalf("version = %d, want 0 for idempotent add-label", updated.Version())
	}
}

func TestGraphTx_AddNodeLabelFailedTooManyLabelsDoesNotRegisterOnCommit(t *testing.T) {
	g, _ := New(Config{Validation: ValidationLimits{MaxLabelsPerNode: 1}})
	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)

	tx, _ := g.BeginTx()
	err := tx.AddNodeLabel(n.ID(), "Rejected")
	if !errors.Is(err, ErrTooManyLabels) {
		t.Fatalf("tx.AddNodeLabel error = %v, want ErrTooManyLabels", err)
	}
	if len(tx.updatedNodes) != 0 {
		_ = tx.Rollback()
		t.Fatalf("updated node snapshots = %d, want 0 for too-many-labels failure", len(tx.updatedNodes))
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if _, ok := g.Resolve.LookupLabel("Rejected"); ok {
		t.Fatal("rejected tx.AddNodeLabel registered label token after commit")
	}
}

func TestGraphTx_AddNodeLabel_ClosedNodeDoesNotSnapshot(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"A", "B"}, nil)
	id := n.ID()
	closeTime := g.nodeValidFrom(n) + 1000
	if err := g.Nodes.CloseVersion(context.Background(), id, closeTime); err != nil {
		t.Fatalf("CloseVersion: %v", err)
	}

	tx, _ := g.BeginTx()
	defer tx.Rollback()

	err := tx.AddNodeLabel(id, "C")
	if !errors.Is(err, ErrAlreadyClosed) {
		t.Fatalf("tx.AddNodeLabel closed node = %v, want ErrAlreadyClosed", err)
	}
	if len(tx.updatedNodes) != 0 {
		t.Fatalf("updated node snapshots = %d, want 0 for closed add-label", len(tx.updatedNodes))
	}
}

func TestGraphTx_AddNodeLabel_RollbackRestoresLabelIndex(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	id := n.ID()

	tx, _ := g.BeginTx()
	if err := tx.AddNodeLabel(id, "B"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddNodeLabel: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Label index must not contain this node under "B" after rollback.
	nodes, _ := g.Nodes.ByLabel("B", storepkg.QueryOpts{})
	for _, nd := range nodes {
		if nd.ID() == id {
			t.Fatal("label index still contains node under B after rollback")
		}
	}
}

func TestGraphTx_AddNodeLabel_RollbackRestoresMultipleLabelAdds(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	id := n.ID()

	tx, _ := g.BeginTx()
	if err := tx.AddNodeLabel(id, "B"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddNodeLabel(B): %v", err)
	}
	if err := tx.AddNodeLabel(id, "C"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddNodeLabel(C): %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	updated, _ := g.Nodes.Get(context.Background(), id)
	if !g.Nodes.HasLabel(updated, "A") {
		t.Fatal("label A should remain after Rollback")
	}
	if g.Nodes.HasLabel(updated, "B") || g.Nodes.HasLabel(updated, "C") {
		t.Fatalf("labels after Rollback = %#v; want only A", updated.AllLabelTokens())
	}

	for _, label := range []string{"B", "C"} {
		nodes, err := g.Nodes.ByLabel(label, storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("ByLabel(%q): %v", label, err)
		}
		for _, nd := range nodes {
			if nd.ID() == id {
				t.Fatalf("label index still contains node under %s after rollback", label)
			}
		}
	}
}

func TestGraphTx_RemoveNodeLabel_RollbackRestoresLabelIndex(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"A", "B"}, nil)
	id := n.ID()

	tx, _ := g.BeginTx()
	if err := tx.RemoveNodeLabel(id, "B"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.RemoveNodeLabel: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	nodes, _ := g.Nodes.ByLabel("B", storepkg.QueryOpts{})
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

func TestGraphTx_RemoveNodeLabel_RollbackRestoresMultipleLabelRemoves(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"A", "B", "C"}, nil)
	id := n.ID()

	tx, _ := g.BeginTx()
	if err := tx.RemoveNodeLabel(id, "B"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.RemoveNodeLabel(B): %v", err)
	}
	if err := tx.RemoveNodeLabel(id, "C"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.RemoveNodeLabel(C): %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	updated, _ := g.Nodes.Get(context.Background(), id)
	for _, label := range []string{"A", "B", "C"} {
		if !g.Nodes.HasLabel(updated, label) {
			t.Fatalf("label %s should be restored after Rollback", label)
		}
		nodes, err := g.Nodes.ByLabel(label, storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("ByLabel(%q): %v", label, err)
		}
		found := false
		for _, nd := range nodes {
			if nd.ID() == id {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("label index missing node under %s after rollback", label)
		}
	}
}

func TestGraphTx_LabelRollbackRestoresMixedLabelOrder(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"A", "B", "C"}, nil)
	id := n.ID()

	tx, _ := g.BeginTx()
	if err := tx.RemoveNodeLabel(id, "B"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.RemoveNodeLabel(B): %v", err)
	}
	if err := tx.AddNodeLabel(id, "D"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddNodeLabel(D): %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	updated, _ := g.Nodes.Get(context.Background(), id)
	for _, label := range []string{"A", "B", "C"} {
		if !g.Nodes.HasLabel(updated, label) {
			t.Fatalf("label %s should be restored after Rollback", label)
		}
	}
	if g.Nodes.HasLabel(updated, "D") {
		t.Fatal("label D should be absent after Rollback")
	}

	for _, tc := range []struct {
		label string
		want  bool
	}{
		{label: "B", want: true},
		{label: "D", want: false},
	} {
		nodes, err := g.Nodes.ByLabel(tc.label, storepkg.QueryOpts{})
		if err != nil {
			t.Fatalf("ByLabel(%q): %v", tc.label, err)
		}
		found := false
		for _, nd := range nodes {
			if nd.ID() == id {
				found = true
				break
			}
		}
		if found != tc.want {
			t.Fatalf("ByLabel(%q) contains node = %v, want %v", tc.label, found, tc.want)
		}
	}
}

func TestGraphTx_LabelRollbackRestoresPrimaryLabelOrder(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"A", "B", "C"}, nil)
	id := n.ID()

	tx, _ := g.BeginTx()
	if err := tx.RemoveNodeLabel(id, "A"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.RemoveNodeLabel(A): %v", err)
	}
	if err := tx.AddNodeLabel(id, "D"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddNodeLabel(D): %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	updated, _ := g.Nodes.Get(context.Background(), id)
	for _, label := range []string{"A", "B", "C"} {
		if !g.Nodes.HasLabel(updated, label) {
			t.Fatalf("label %s should be restored after Rollback", label)
		}
	}
	if got, want := g.Nodes.PrimaryLabel(updated), "A"; got != want {
		t.Fatalf("primary label after Rollback = %q, want %q", got, want)
	}
	if g.Nodes.HasLabel(updated, "D") {
		t.Fatal("label D should be absent after Rollback")
	}
}

func TestGraphTx_LabelRollbackRestoresTieredPrimaryWithoutClassHop(t *testing.T) {
	g, _ := newTestTieredGraph(t)
	n, err := g.Nodes.Add(context.Background(), []string{"Case", "User", "Signal"}, nil)
	if err != nil {
		t.Fatalf("Nodes.Add: %v", err)
	}
	id := n.ID()

	tx, _ := g.BeginTx()
	if err := tx.RemoveNodeLabel(id, "Case"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.RemoveNodeLabel(Case): %v", err)
	}
	if err := tx.AddNodeLabel(id, "Archived"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddNodeLabel(Archived): %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	updated, err := g.Nodes.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Nodes.Get: %v", err)
	}
	if got, want := g.Nodes.PrimaryLabel(updated), "Case"; got != want {
		t.Fatalf("primary label after Rollback = %q, want %q", got, want)
	}
	for _, label := range []string{"Case", "User", "Signal"} {
		if !g.Nodes.HasLabel(updated, label) {
			t.Fatalf("label %s should be restored after Rollback", label)
		}
	}
	if g.Nodes.HasLabel(updated, "Archived") {
		t.Fatal("label Archived should be absent after Rollback")
	}
}

func TestGraphTx_LabelMutationsValidateNameBeforeNodeLookup(t *testing.T) {
	g, _ := New(Config{})

	checks := []struct {
		name string
		run  func(*GraphTx) error
	}{
		{name: "add", run: func(tx *GraphTx) error {
			return tx.AddNodeLabel(types.NodeID(999), " ")
		}},
		{name: "remove", run: func(tx *GraphTx) error {
			return tx.RemoveNodeLabel(types.NodeID(999), " ")
		}},
	}

	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			tx, _ := g.BeginTx()
			err := check.run(tx)
			if rbErr := tx.Rollback(); rbErr != nil {
				t.Fatalf("Rollback: %v", rbErr)
			}
			if !errors.Is(err, ErrEmptyName) {
				t.Fatalf("err = %v, want ErrEmptyName", err)
			}
		})
	}
}

func TestGraphTx_AddNodeLabel_AfterCommitReturnsTxDone(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	id := n.ID()

	tx, _ := g.BeginTx()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := tx.AddNodeLabel(id, "B"); !errors.Is(err, storepkg.ErrTxDone) {
		t.Fatalf("expected storepkg.ErrTxDone, got %v", err)
	}
}

// --- GraphTx.Nodes.RemoveLabel ---

func TestGraphTx_RemoveNodeLabel_Commit(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"A", "B"}, nil)
	id := n.ID()

	tx, _ := g.BeginTx()
	if err := tx.RemoveNodeLabel(id, "B"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.RemoveNodeLabel: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	updated, _ := g.Nodes.Get(context.Background(), id)
	if g.Nodes.HasLabel(updated, "B") {
		t.Error("label B should be gone after Commit")
	}
}

func TestGraphTx_RemoveNodeLabel_Rollback(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"A", "B"}, nil)
	id := n.ID()

	tx, _ := g.BeginTx()
	if err := tx.RemoveNodeLabel(id, "B"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.RemoveNodeLabel: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	updated, _ := g.Nodes.Get(context.Background(), id)
	if !g.Nodes.HasLabel(updated, "B") {
		t.Error("label B should be restored after Rollback")
	}
}

func TestGraphTx_RemoveNodeLabel_LastLabelError(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"Solo"}, nil)
	id := n.ID()

	tx, _ := g.BeginTx()
	defer tx.Rollback()

	err := tx.RemoveNodeLabel(id, "Solo")
	if !errors.Is(err, ErrLastLabel) {
		t.Fatalf("expected ErrLastLabel, got %v", err)
	}
	if len(tx.updatedNodes) != 0 {
		t.Fatalf("updated node snapshots = %d, want 0 for failed last-label removal", len(tx.updatedNodes))
	}
}

func TestGraphTx_RemoveNodeLabel_LabelNotFoundDoesNotSnapshot(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"A", "B"}, nil)
	id := n.ID()
	if _, err := g.Resolve.GetOrCreateLabel("C"); err != nil {
		t.Fatalf("GetOrCreateLabel C: %v", err)
	}

	tx, _ := g.BeginTx()
	defer tx.Rollback()

	err := tx.RemoveNodeLabel(id, "C")
	if !errors.Is(err, ErrLabelNotFound) {
		t.Fatalf("expected ErrLabelNotFound, got %v", err)
	}
	if len(tx.updatedNodes) != 0 {
		t.Fatalf("updated node snapshots = %d, want 0 for absent-label removal", len(tx.updatedNodes))
	}
}

func TestGraphTx_RemoveNodeLabel_UnknownLabelPrecedesNodeLookup(t *testing.T) {
	g, _ := New(Config{})

	tx, _ := g.BeginTx()
	defer tx.Rollback()

	err := tx.RemoveNodeLabel(types.NodeID(999), "Ghost")
	if !errors.Is(err, ErrLabelNotFound) {
		t.Fatalf("expected ErrLabelNotFound, got %v", err)
	}
	if len(tx.updatedNodes) != 0 {
		t.Fatalf("updated node snapshots = %d, want 0 for unknown-label removal", len(tx.updatedNodes))
	}
}

func TestGraphTx_RemoveNodeLabel_AfterRollbackReturnsTxDone(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"A", "B"}, nil)
	id := n.ID()

	tx, _ := g.BeginTx()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := tx.RemoveNodeLabel(id, "B"); !errors.Is(err, storepkg.ErrTxDone) {
		t.Fatalf("expected storepkg.ErrTxDone, got %v", err)
	}
}
