package core

import (
	"context"
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestDeleteMutatorsRejectInvalidIDs(t *testing.T) {
	t.Parallel()

	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	type checkCase struct {
		name string
		err  error
	}
	checks := []checkCase{
		{name: "Nodes.Delete zero", err: g.Nodes.Delete(context.Background(), 0)},
		{name: "Nodes.Delete negative", err: g.Nodes.Delete(context.Background(), types.NodeID(-1))},
		{name: "Rels.Delete zero", err: g.Rels.Delete(context.Background(), 0)},
		{name: "Rels.Delete negative", err: g.Rels.Delete(context.Background(), types.RelID(-1))},
	}

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	checks = append(checks,
		checkCase{name: "Tx.DeleteNode zero", err: tx.DeleteNode(0)},
		checkCase{name: "Tx.DeleteNode negative", err: tx.DeleteNode(types.NodeID(-1))},
		checkCase{name: "Tx.DeleteRelationship zero", err: tx.DeleteRelationship(0)},
		checkCase{name: "Tx.DeleteRelationship negative", err: tx.DeleteRelationship(types.RelID(-1))},
	)
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	batch, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	checks = append(checks,
		checkCase{name: "Batch.DeleteNode zero", err: batch.DeleteNode(0)},
		checkCase{name: "Batch.DeleteNode negative", err: batch.DeleteNode(types.NodeID(-1))},
		checkCase{name: "Batch.DeleteRelationship zero", err: batch.DeleteRelationship(0)},
		checkCase{name: "Batch.DeleteRelationship negative", err: batch.DeleteRelationship(types.RelID(-1))},
	)

	for _, check := range checks {
		if !errors.Is(check.err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("%s = %v, want ErrInvalidStoreMutation", check.name, check.err)
		}
	}
}

func TestUpdateMutatorsRejectInvalidIDs(t *testing.T) {
	t.Parallel()

	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	type checkCase struct {
		name string
		err  error
	}
	update := map[string]any{"name": "Ada"}
	checks := []checkCase{
		{name: "Nodes.Update zero", err: errFromNode(g.Nodes.Update(context.Background(), 0, update))},
		{name: "Nodes.Update empty zero", err: errFromNode(g.Nodes.Update(context.Background(), 0, nil))},
		{name: "Nodes.Update negative", err: errFromNode(g.Nodes.Update(context.Background(), types.NodeID(-1), update))},
		{name: "Nodes.UpdateInPlace zero", err: errFromNode(g.Nodes.UpdateInPlace(context.Background(), 0, update))},
		{name: "Nodes.UpdateInPlace empty zero", err: errFromNode(g.Nodes.UpdateInPlace(context.Background(), 0, nil))},
		{name: "Nodes.UpdateInPlace negative", err: errFromNode(g.Nodes.UpdateInPlace(context.Background(), types.NodeID(-1), update))},
		{name: "Nodes.AddLabel zero", err: g.Nodes.AddLabel(context.Background(), 0, "Person")},
		{name: "Nodes.AddLabel negative", err: g.Nodes.AddLabel(context.Background(), types.NodeID(-1), "Person")},
		{name: "Nodes.RemoveLabel zero", err: g.Nodes.RemoveLabel(context.Background(), 0, "Person")},
		{name: "Nodes.RemoveLabel negative", err: g.Nodes.RemoveLabel(context.Background(), types.NodeID(-1), "Person")},
		{name: "Nodes.CompareAndSetProperty zero", err: errFromCAS(g.Nodes.CompareAndSetProperty(context.Background(), 0, "name", nil, "Ada"))},
		{name: "Nodes.CompareAndSetProperty negative", err: errFromCAS(g.Nodes.CompareAndSetProperty(context.Background(), types.NodeID(-1), "name", nil, "Ada"))},
		{name: "Nodes.CloseVersion zero", err: g.Nodes.CloseVersion(context.Background(), 0, 100)},
		{name: "Nodes.CloseVersion negative", err: g.Nodes.CloseVersion(context.Background(), types.NodeID(-1), 100)},
		{name: "Rels.Update zero", err: errFromRel(g.Rels.Update(context.Background(), 0, update))},
		{name: "Rels.Update empty zero", err: errFromRel(g.Rels.Update(context.Background(), 0, nil))},
		{name: "Rels.Update negative", err: errFromRel(g.Rels.Update(context.Background(), types.RelID(-1), update))},
		{name: "Rels.UpdateInPlace zero", err: errFromRel(g.Rels.UpdateInPlace(context.Background(), 0, update))},
		{name: "Rels.UpdateInPlace empty zero", err: errFromRel(g.Rels.UpdateInPlace(context.Background(), 0, nil))},
		{name: "Rels.UpdateInPlace negative", err: errFromRel(g.Rels.UpdateInPlace(context.Background(), types.RelID(-1), update))},
		{name: "Rels.CompareAndSetProperty zero", err: errFromCAS(g.Rels.CompareAndSetProperty(context.Background(), 0, "name", nil, "Ada"))},
		{name: "Rels.CompareAndSetProperty negative", err: errFromCAS(g.Rels.CompareAndSetProperty(context.Background(), types.RelID(-1), "name", nil, "Ada"))},
		{name: "Rels.CloseVersion zero", err: g.Rels.CloseVersion(context.Background(), 0, 100)},
		{name: "Rels.CloseVersion negative", err: g.Rels.CloseVersion(context.Background(), types.RelID(-1), 100)},
	}

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	checks = append(checks,
		checkCase{name: "Tx.UpdateNode zero", err: errFromNode(tx.UpdateNode(0, update))},
		checkCase{name: "Tx.UpdateNode empty zero", err: errFromNode(tx.UpdateNode(0, nil))},
		checkCase{name: "Tx.UpdateNode negative", err: errFromNode(tx.UpdateNode(types.NodeID(-1), update))},
		checkCase{name: "Tx.UpdateRelationship zero", err: errFromRel(tx.UpdateRelationship(0, update))},
		checkCase{name: "Tx.UpdateRelationship empty zero", err: errFromRel(tx.UpdateRelationship(0, nil))},
		checkCase{name: "Tx.UpdateRelationship negative", err: errFromRel(tx.UpdateRelationship(types.RelID(-1), update))},
		checkCase{name: "Tx.AddNodeLabel zero", err: tx.AddNodeLabel(0, "Person")},
		checkCase{name: "Tx.AddNodeLabel negative", err: tx.AddNodeLabel(types.NodeID(-1), "Person")},
		checkCase{name: "Tx.RemoveNodeLabel zero", err: tx.RemoveNodeLabel(0, "Person")},
		checkCase{name: "Tx.RemoveNodeLabel negative", err: tx.RemoveNodeLabel(types.NodeID(-1), "Person")},
	)
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	batch, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	checks = append(checks,
		checkCase{name: "Batch.UpdateNode zero", err: batch.UpdateNode(0, update)},
		checkCase{name: "Batch.UpdateNode empty zero", err: batch.UpdateNode(0, nil)},
		checkCase{name: "Batch.UpdateNode negative", err: batch.UpdateNode(types.NodeID(-1), update)},
		checkCase{name: "Batch.UpdateRelationship zero", err: batch.UpdateRelationship(0, update)},
		checkCase{name: "Batch.UpdateRelationship empty zero", err: batch.UpdateRelationship(0, nil)},
		checkCase{name: "Batch.UpdateRelationship negative", err: batch.UpdateRelationship(types.RelID(-1), update)},
	)

	for _, check := range checks {
		if !errors.Is(check.err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("%s = %v, want ErrInvalidStoreMutation", check.name, check.err)
		}
	}
}

func TestExistingEntityMutatorsValidateInvalidIDBeforePayload(t *testing.T) {
	t.Parallel()

	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	type checkCase struct {
		name string
		err  error
	}
	invalidUpdate := map[string]any{"tkg_version": int64(1)}
	checks := []checkCase{
		{name: "Nodes.Update zero reserved key", err: errFromNode(g.Nodes.Update(context.Background(), 0, invalidUpdate))},
		{name: "Nodes.UpdateInPlace zero reserved key", err: errFromNode(g.Nodes.UpdateInPlace(context.Background(), 0, invalidUpdate))},
		{name: "Nodes.CompareAndSetProperty zero reserved key", err: errFromCAS(g.Nodes.CompareAndSetProperty(context.Background(), 0, "tkg_version", nil, "Ada"))},
		{name: "Rels.Update zero reserved key", err: errFromRel(g.Rels.Update(context.Background(), 0, invalidUpdate))},
		{name: "Rels.UpdateInPlace zero reserved key", err: errFromRel(g.Rels.UpdateInPlace(context.Background(), 0, invalidUpdate))},
		{name: "Rels.CompareAndSetProperty zero reserved key", err: errFromCAS(g.Rels.CompareAndSetProperty(context.Background(), 0, "tkg_type", nil, "KNOWS"))},
	}

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	checks = append(checks,
		checkCase{name: "Tx.UpdateNode zero reserved key", err: errFromNode(tx.UpdateNode(0, invalidUpdate))},
		checkCase{name: "Tx.UpdateRelationship zero reserved key", err: errFromRel(tx.UpdateRelationship(0, invalidUpdate))},
	)
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	batch, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	checks = append(checks,
		checkCase{name: "Batch.UpdateNode zero reserved key", err: batch.UpdateNode(0, invalidUpdate)},
		checkCase{name: "Batch.UpdateRelationship zero reserved key", err: batch.UpdateRelationship(0, invalidUpdate)},
	)

	for _, check := range checks {
		if !errors.Is(check.err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("%s = %v, want ErrInvalidStoreMutation", check.name, check.err)
		}
	}
}

func TestRelationshipCreateMutatorsRejectInvalidEndpointIDs(t *testing.T) {
	t.Parallel()

	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	valid, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add valid node: %v", err)
	}
	zeroNode := types.NewNode(0, 1, nil)
	negativeNode := types.NewNode(types.NodeID(-1), 1, nil)

	type checkCase struct {
		name string
		err  error
	}
	checks := []checkCase{
		{name: "Rels.Add zero start", err: errFromRel(g.Rels.Add(context.Background(), "KNOWS", zeroNode, valid, nil))},
		{name: "Rels.Add negative start", err: errFromRel(g.Rels.Add(context.Background(), "KNOWS", negativeNode, valid, nil))},
		{name: "Rels.AddByID zero start", err: errFromRel(g.Rels.AddByID(context.Background(), "KNOWS", 0, valid.ID(), nil))},
		{name: "Rels.AddByID negative end", err: errFromRel(g.Rels.AddByID(context.Background(), "KNOWS", valid.ID(), types.NodeID(-1), nil))},
		{name: "Rels.AddByIDIfAbsent zero start", err: errFromRelIfAbsent(g.Rels.AddByIDIfAbsent(context.Background(), "KNOWS", 0, valid.ID(), nil))},
		{name: "Rels.AddByIDIfAbsent negative end", err: errFromRelIfAbsent(g.Rels.AddByIDIfAbsent(context.Background(), "KNOWS", valid.ID(), types.NodeID(-1), nil))},
		{name: "Rels.Import zero start", err: errFromRel(g.Rels.Import(context.Background(), g.Rels.NextID(), "KNOWS", zeroNode, valid, nil))},
		{name: "Rels.Import negative start", err: errFromRel(g.Rels.Import(context.Background(), g.Rels.NextID(), "KNOWS", negativeNode, valid, nil))},
	}

	txImportRelID := g.Rels.NextID()
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	checks = append(checks,
		checkCase{name: "Tx.AddRelationship zero start", err: errFromRel(tx.AddRelationship("KNOWS", zeroNode, valid, nil))},
		checkCase{name: "Tx.AddRelationshipByID zero start", err: errFromRel(tx.AddRelationshipByID("KNOWS", 0, valid.ID(), nil))},
		checkCase{name: "Tx.AddRelationshipByIDIfAbsent negative end", err: errFromRelIfAbsent(tx.AddRelationshipByIDIfAbsent("KNOWS", valid.ID(), types.NodeID(-1), nil))},
		checkCase{name: "Tx.ImportRelationshipWithID negative start", err: errFromRel(tx.ImportRelationshipWithID(context.Background(), txImportRelID, "KNOWS", negativeNode, valid, nil))},
	)
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	batch, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	checks = append(checks,
		checkCase{name: "Batch.AddRelationship zero start", err: errFromRel(batch.AddRelationship("KNOWS", zeroNode, valid, nil))},
		checkCase{name: "Batch.AddRelationship negative start", err: errFromRel(batch.AddRelationship("KNOWS", negativeNode, valid, nil))},
	)

	for _, check := range checks {
		if !errors.Is(check.err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("%s = %v, want ErrInvalidStoreMutation", check.name, check.err)
		}
	}
}

func errFromNode(_ *types.Node, err error) error { return err }

func errFromRel(_ *types.Relationship, err error) error { return err }

func errFromRelIfAbsent(_ *types.Relationship, _ bool, err error) error { return err }

func errFromCAS(_ bool, err error) error { return err }
