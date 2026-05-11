package core

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestGraphTx_RollbackDeletes(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx, _ := g.BeginTx()

	n, err := tx.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	n2, err := tx.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	_, err = tx.AddRelationship("KNOWS", n, n2, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Verify everything is gone.
	nc, _ := g.Nodes.Count()
	rc, _ := g.Rels.Count()
	if nc != 0 {
		t.Errorf("node count after rollback: got %d, want 0", nc)
	}
	if rc != 0 {
		t.Errorf("rel count after rollback: got %d, want 0", rc)
	}
}

func TestGraphTx_RollbackRestoresStatsCounters(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	a, err := g.Nodes.Add([]string{"TxStats"}, map[string]any{"v": int64(1)})
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add([]string{"TxStats"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	c, err := g.Nodes.Add([]string{"TxStats"}, nil)
	if err != nil {
		t.Fatalf("AddNode c: %v", err)
	}
	d, err := g.Nodes.Add([]string{"TxStats"}, nil)
	if err != nil {
		t.Fatalf("AddNode d: %v", err)
	}
	updatedRel, err := g.Rels.Add("TX_STATS_UPDATE", a, b, map[string]any{"v": int64(1)})
	if err != nil {
		t.Fatalf("AddRelationship update: %v", err)
	}
	deletedRel, err := g.Rels.Add("TX_STATS_DELETE", c, d, nil)
	if err != nil {
		t.Fatalf("AddRelationship delete: %v", err)
	}
	before := g.Stats.Get()

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.AddNode([]string{"TxStatsCreated"}, nil); err != nil {
		t.Fatalf("tx AddNode: %v", err)
	}
	if _, err := tx.AddRelationship("TX_STATS_CREATE_REL", a, b, nil); err != nil {
		t.Fatalf("tx AddRelationship: %v", err)
	}
	if _, err := tx.UpdateNode(a.ID(), map[string]any{"v": int64(2)}); err != nil {
		t.Fatalf("tx UpdateNode: %v", err)
	}
	if _, err := tx.UpdateRelationship(updatedRel.ID(), map[string]any{"v": int64(2)}); err != nil {
		t.Fatalf("tx UpdateRelationship: %v", err)
	}
	if err := tx.DeleteRelationship(deletedRel.ID()); err != nil {
		t.Fatalf("tx DeleteRelationship: %v", err)
	}
	if err := tx.DeleteNode(d.ID()); err != nil {
		t.Fatalf("tx DeleteNode: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	after := g.Stats.Get()
	if after.NodesAdded != before.NodesAdded ||
		after.NodesRead != before.NodesRead ||
		after.NodesUpdated != before.NodesUpdated ||
		after.NodesDeleted != before.NodesDeleted ||
		after.RelsAdded != before.RelsAdded ||
		after.RelsRead != before.RelsRead ||
		after.RelsUpdated != before.RelsUpdated ||
		after.RelsDeleted != before.RelsDeleted {
		t.Fatalf("stats changed after rollback:\nbefore=%+v\nafter=%+v", before, after)
	}
}

func TestGraphTx_RollbackSnapshotsNodeAndRelWithSameSnowflakeID(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	a, err := g.Nodes.Add([]string{"TxSharedID"}, map[string]any{"node": "before"})
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add([]string{"TxSharedID"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Import(
		context.Background(),
		types.RelID(a.ID().SnowflakeID()),
		"TX_SHARED_ID",
		a,
		b,
		map[string]any{"rel": "before"},
	)
	if err != nil {
		t.Fatalf("Import relationship with same raw ID as node: %v", err)
	}

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.UpdateNode(a.ID(), map[string]any{"node": "after"}); err != nil {
		t.Fatalf("tx UpdateNode: %v", err)
	}
	if _, err := tx.UpdateRelationship(r.ID(), map[string]any{"rel": "after"}); err != nil {
		t.Fatalf("tx UpdateRelationship: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	gotNode, err := g.Nodes.Get(a.ID())
	if err != nil {
		t.Fatalf("Get node after rollback: %v", err)
	}
	if got, _ := gotNode.GetProperty("node"); got != "before" {
		t.Fatalf("node property after rollback = %v, want before", got)
	}
	gotRel, err := g.Rels.Get(r.ID())
	if err != nil {
		t.Fatalf("Get relationship after rollback: %v", err)
	}
	if got, _ := gotRel.GetProperty("rel"); got != "before" {
		t.Fatalf("relationship property after rollback = %v, want before", got)
	}
}

func TestGraphTx_RollbackRestoresRegistries(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	a, err := g.Nodes.Add([]string{"Existing"}, nil)
	if err != nil {
		t.Fatalf("Add node a: %v", err)
	}
	b, err := g.Nodes.Add([]string{"Existing"}, nil)
	if err != nil {
		t.Fatalf("Add node b: %v", err)
	}

	tx, _ := g.BeginTx()
	if _, err := tx.AddNode([]string{"TxOnly"}, nil); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddNode: %v", err)
	}
	if _, err := tx.AddRelationship("TX_REL", a, b, nil); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddRelationship: %v", err)
	}
	if err := tx.AddNodeLabel(a.ID(), "TxExtra"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddNodeLabel: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	if _, ok := g.Resolve.LookupLabel("Existing"); !ok {
		t.Fatal("rollback removed pre-existing label token")
	}
	if _, ok := g.Resolve.LookupLabel("TxOnly"); ok {
		t.Fatal("rollback kept label token created by tx AddNode")
	}
	if _, ok := g.Resolve.LookupLabel("TxExtra"); ok {
		t.Fatal("rollback kept label token created by tx AddNodeLabel")
	}
	if _, ok := g.Resolve.LookupRelType("TX_REL"); ok {
		t.Fatal("rollback kept relationship type token created by tx AddRelationship")
	}
	restored, err := g.Nodes.Get(a.ID())
	if err != nil {
		t.Fatalf("Get node a after rollback: %v", err)
	}
	if g.Nodes.HasLabel(restored, "TxExtra") {
		t.Fatal("rollback left TxExtra label on node")
	}
}

func TestGraphTx_RollbackInvalidatesRelTypeCache(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	a, err := g.Nodes.Add([]string{"Existing"}, nil)
	if err != nil {
		t.Fatalf("Add node a: %v", err)
	}
	b, err := g.Nodes.Add([]string{"Existing"}, nil)
	if err != nil {
		t.Fatalf("Add node b: %v", err)
	}

	const typ = "TX_CACHE_ROLLBACK_REL"
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.AddRelationship(typ, a, b, nil); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddRelationship: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if tok, ok := g.Resolve.LookupRelType(typ); ok || tok != 0 {
		t.Fatalf("LookupRelType after rollback = (%d, %v), want (0, false)", tok, ok)
	}

	rel, err := g.Rels.Add(typ, a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship after rollback: %v", err)
	}
	tok, ok := g.Resolve.LookupRelType(typ)
	if !ok || tok == 0 {
		t.Fatalf("LookupRelType after re-add = (%d, %v), want registered token", tok, ok)
	}
	if got := rel.TypeToken().Value(); got != tok {
		t.Fatalf("relationship type token = %d, registry token = %d", got, tok)
	}
	if got := g.Rels.Type(rel); got != typ {
		t.Fatalf("relationship type resolution = %q, want %q", got, typ)
	}
}

func TestGraphTx_PublicResolverReadsWaitForRollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	a, err := g.Nodes.Add([]string{"Existing"}, nil)
	if err != nil {
		t.Fatalf("Add node a: %v", err)
	}
	b, err := g.Nodes.Add([]string{"Existing"}, nil)
	if err != nil {
		t.Fatalf("Add node b: %v", err)
	}

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.AddNode([]string{"TxOnly"}, nil); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddNode: %v", err)
	}
	if _, err := tx.AddRelationship("TX_REL", a, b, nil); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddRelationship: %v", err)
	}

	labelStarted := make(chan struct{})
	labelDone := make(chan struct{})
	var labelTok uint16
	var labelOK bool
	go func() {
		close(labelStarted)
		labelTok, labelOK = g.Resolve.LookupLabel("TxOnly")
		close(labelDone)
	}()

	relStarted := make(chan struct{})
	relDone := make(chan struct{})
	var relTok uint16
	var relOK bool
	go func() {
		close(relStarted)
		relTok, relOK = g.Resolve.LookupRelType("TX_REL")
		close(relDone)
	}()

	<-labelStarted
	<-relStarted
	assertBlocked := func(name string, done <-chan struct{}) {
		t.Helper()
		select {
		case <-done:
			t.Fatalf("%s returned while the transaction was still active", name)
		case <-time.After(50 * time.Millisecond):
		}
	}
	assertBlocked("LookupLabel", labelDone)
	assertBlocked("LookupRelType", relDone)

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	waitDone := func(name string, done <-chan struct{}) {
		t.Helper()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("%s did not resume after rollback", name)
		}
	}
	waitDone("LookupLabel", labelDone)
	waitDone("LookupRelType", relDone)

	if labelOK || labelTok != 0 {
		t.Fatalf("LookupLabel after rollback = (%d, %v), want (0, false)", labelTok, labelOK)
	}
	if relOK || relTok != 0 {
		t.Fatalf("LookupRelType after rollback = (%d, %v), want (0, false)", relTok, relOK)
	}
}

func TestGraphTx_RollbackEmpty(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx, _ := g.BeginTx()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback empty tx: %v", err)
	}
}

func TestGraphTx_DoubleRollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx, _ := g.BeginTx()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	err := tx.Rollback()
	if !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("double rollback: got %v, want storepkg.ErrTxDone", err)
	}
}

func TestGraphTx_AddAfterRollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx, _ := g.BeginTx()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	_, err := tx.AddNode([]string{"Person"}, nil)
	if !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("AddNode after rollback: got %v, want storepkg.ErrTxDone", err)
	}

	_, err = tx.AddRelationship("KNOWS", nil, nil, nil)
	if !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("AddRelationship after rollback: got %v, want storepkg.ErrTxDone", err)
	}
}

func TestGraphTx_CommitThenRollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx, _ := g.BeginTx()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	err := tx.Rollback()
	if !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("rollback after commit: got %v, want storepkg.ErrTxDone", err)
	}
}

func TestGraphTx_RollbackThenCommit(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx, _ := g.BeginTx()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	err := tx.Commit()
	if !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("commit after rollback: got %v, want storepkg.ErrTxDone", err)
	}
}

func TestGraphTx_RollbackLeavesNoHistory(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx, _ := g.BeginTx()

	n, err := tx.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	nodeID := n.ID()

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// No history should exist for rolled-back entities.
	history, err := g.Nodes.History(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Errorf("rolled-back node has %d history entries, want 0", len(history))
	}
}

func TestGraphTx_UpdateNodeRollbackRestoresHistory(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes.Update(n.ID(), map[string]any{"name": "Committed"}); err != nil {
		t.Fatal(err)
	}
	before, err := g.Nodes.History(n.ID())
	if err != nil {
		t.Fatal(err)
	}
	if len(before) == 0 {
		t.Fatal("expected committed pre-transaction history")
	}

	tx, _ := g.BeginTx()
	if _, err := tx.UpdateNode(n.ID(), map[string]any{"name": "RolledBack"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	after, err := g.Nodes.History(n.ID())
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("history length after rollback = %d, want %d", len(after), len(before))
	}
	for i := range before {
		if before[i].Version() != after[i].Version() {
			t.Fatalf("history[%d].Version = %d, want %d", i, after[i].Version(), before[i].Version())
		}
	}
}

func TestGraphTx_DeleteNodeRollbackRestoresHistory(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tx, _ := g.BeginTx()
	if err := tx.DeleteNode(n.ID()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	history, err := g.Nodes.History(n.ID())
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Fatalf("history length after delete rollback = %d, want 0", len(history))
	}
}

func TestGraphTx_UpdateRelRollbackRestoresHistory(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	a, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add("KNOWS", a, b, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels.Update(r.ID(), map[string]any{"w": int64(2)}); err != nil {
		t.Fatal(err)
	}
	before, err := g.Rels.History(r.ID())
	if err != nil {
		t.Fatal(err)
	}
	if len(before) == 0 {
		t.Fatal("expected committed pre-transaction relationship history")
	}

	tx, _ := g.BeginTx()
	if _, err := tx.UpdateRelationship(r.ID(), map[string]any{"w": int64(3)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	after, err := g.Rels.History(r.ID())
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("relationship history length after rollback = %d, want %d", len(after), len(before))
	}
	for i := range before {
		if before[i].Version() != after[i].Version() {
			t.Fatalf("rel history[%d].Version = %d, want %d", i, after[i].Version(), before[i].Version())
		}
	}
}

func TestGraphTx_CreateUpdateRollbackClearsHistory(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx, _ := g.BeginTx()
	n, err := tx.AddNode([]string{"Person"}, map[string]any{"name": "Temp"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.UpdateNode(n.ID(), map[string]any{"name": "RolledBack"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	history, err := g.Nodes.History(n.ID())
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Fatalf("created node history after rollback = %d, want 0", len(history))
	}
}

func TestGraphTx_CreateThenDelete_Rollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx, _ := g.BeginTx()
	n, err := tx.AddNode([]string{"Person"}, map[string]any{"name": "Temp"})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.DeleteNode(n.ID()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Nothing should exist — create+delete in same tx, rolled back = nothing.
	nc, _ := g.Nodes.Count()
	if nc != 0 {
		t.Errorf("node count: got %d, want 0", nc)
	}
}

func TestGraphTx_UpdateThenDelete_Rollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	nodeID := n.ID()

	tx, _ := g.BeginTx()
	if _, err := tx.UpdateNode(nodeID, map[string]any{"name": "Bob"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.DeleteNode(nodeID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Pre-existing node should be restored to original state.
	got, err := g.Nodes.Get(nodeID)
	if err != nil {
		t.Fatalf("GetNode after rollback: %v", err)
	}
	name, _ := got.GetProperty("name")
	if name != "Alice" {
		t.Errorf("name = %v, want Alice", name)
	}
}

func TestGraphTx_DeleteThenImportSameNodeID_RollbackRestoresOriginal(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	original, err := g.Nodes.Add([]string{"Original"}, map[string]any{"state": "before"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := original.ID()

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if err := tx.DeleteNode(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if _, err := tx.ImportNodeWithID(context.Background(), id, []string{"Replacement"}, map[string]any{"state": "after"}); err != nil {
		t.Fatalf("ImportNodeWithID same ID: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	got, err := g.Nodes.Get(id)
	if err != nil {
		t.Fatalf("GetNode after rollback: %v", err)
	}
	state, _ := got.GetProperty("state")
	if state != "before" {
		t.Fatalf("state after rollback = %v, want before", state)
	}
	if !g.Nodes.HasLabel(got, "Original") {
		t.Fatal("rollback did not restore original label")
	}
	if g.Nodes.HasLabel(got, "Replacement") {
		t.Fatal("rollback left replacement label on original node")
	}
	if _, ok := g.Resolve.LookupLabel("Replacement"); ok {
		t.Fatal("rollback kept replacement label token")
	}
}

func TestGraphTx_DeleteThenImportSameRelID_RollbackRestoresOriginal(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	a, err := g.Nodes.Add([]string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add([]string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	original, err := g.Rels.Add("ORIGINAL_REL", a, b, map[string]any{"state": "before"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	id := original.ID()

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if err := tx.DeleteRelationship(id); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	if _, err := tx.ImportRelationshipWithID(context.Background(), id, "REPLACEMENT_REL", a, b, map[string]any{"state": "after"}); err != nil {
		t.Fatalf("ImportRelationshipWithID same ID: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	got, err := g.Rels.Get(id)
	if err != nil {
		t.Fatalf("GetRelationship after rollback: %v", err)
	}
	state, _ := got.GetProperty("state")
	if state != "before" {
		t.Fatalf("state after rollback = %v, want before", state)
	}
	if typ := g.Rels.Type(got); typ != "ORIGINAL_REL" {
		t.Fatalf("relationship type after rollback = %q, want ORIGINAL_REL", typ)
	}
	if _, ok := g.Resolve.LookupRelType("REPLACEMENT_REL"); ok {
		t.Fatal("rollback kept replacement relationship type token")
	}
}

func TestGraphTx_CreateDeleteImportSameNodeID_RollbackRemovesNode(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	created, err := tx.AddNode([]string{"Temp"}, map[string]any{"state": "created"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := created.ID()
	if err := tx.DeleteNode(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if _, err := tx.ImportNodeWithID(context.Background(), id, []string{"Replacement"}, map[string]any{"state": "replacement"}); err != nil {
		t.Fatalf("ImportNodeWithID same ID: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	if _, err := g.Nodes.Get(id); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("GetNode after rollback = %v, want ErrNodeNotFound", err)
	}
}

func TestGraphTx_CreateDeleteImportSameRelID_RollbackRemovesRelationship(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	a, err := g.Nodes.Add([]string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add([]string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	created, err := tx.AddRelationship("TEMP_REL", a, b, map[string]any{"state": "created"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	id := created.ID()
	if err := tx.DeleteRelationship(id); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	if _, err := tx.ImportRelationshipWithID(context.Background(), id, "REPLACEMENT_REL", a, b, map[string]any{"state": "replacement"}); err != nil {
		t.Fatalf("ImportRelationshipWithID same ID: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	if _, err := g.Rels.Get(id); !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("GetRelationship after rollback = %v, want ErrRelNotFound", err)
	}
}

func TestGraphTx_DeleteRelThenEndpoint_RollbackRestoresRelationship(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	a, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add("KNOWS", a, b, map[string]any{"since": int64(2026)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	tx, _ := g.BeginTx()
	if err := tx.DeleteRelationship(r.ID()); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	if err := tx.DeleteNode(a.ID()); err != nil {
		t.Fatalf("DeleteNode endpoint: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	if _, err := g.Nodes.Get(a.ID()); err != nil {
		t.Fatalf("endpoint a missing after rollback: %v", err)
	}
	if _, err := g.Nodes.Get(b.ID()); err != nil {
		t.Fatalf("endpoint b missing after rollback: %v", err)
	}
	got, err := g.Rels.Get(r.ID())
	if err != nil {
		t.Fatalf("relationship missing after rollback: %v", err)
	}
	if got.StartNodeID() != a.ID() || got.EndNodeID() != b.ID() {
		t.Fatalf("relationship endpoints after rollback = (%d,%d), want (%d,%d)",
			got.StartNodeID(), got.EndNodeID(), a.ID(), b.ID())
	}
}

func TestGraphTx_AfterDone_ReturnsErrTxDone(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	nodeID := n.ID()

	n2, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add("KNOWS", n, n2, nil)
	if err != nil {
		t.Fatal(err)
	}
	relID := r.ID()

	tx, _ := g.BeginTx()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// All mutation methods should return storepkg.ErrTxDone after commit.
	if _, err := tx.UpdateNode(nodeID, map[string]any{"x": int64(1)}); !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("UpdateNode: got %v, want storepkg.ErrTxDone", err)
	}
	if _, err := tx.UpdateRelationship(relID, map[string]any{"x": int64(1)}); !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("UpdateRelationship: got %v, want storepkg.ErrTxDone", err)
	}
	if err := tx.DeleteNode(nodeID); !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("DeleteNode: got %v, want storepkg.ErrTxDone", err)
	}
	if err := tx.DeleteRelationship(relID); !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("DeleteRelationship: got %v, want storepkg.ErrTxDone", err)
	}
}

// TestTxRollbackNoEvents verifies that no events are published when a tx rolls back.
func TestTxRollbackNoEvents(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })

	bus := eventspkg.NewEventBus()
	_ = g.Events.SetSync(bus)

	var count atomic.Int64
	bus.Subscribe(func(e eventspkg.Event) {
		count.Add(1)
	})

	tx, _ := g.BeginTx()

	_, err = tx.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("tx.AddNode: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// No events should have been published.
	if got := count.Load(); got != 0 {
		t.Errorf("events after rollback: got %d, want 0", got)
	}

	// Verify node was actually rolled back.
	nc, _ := g.Nodes.Count()
	if nc != 0 {
		t.Errorf("node count after rollback: got %d, want 0", nc)
	}
}
