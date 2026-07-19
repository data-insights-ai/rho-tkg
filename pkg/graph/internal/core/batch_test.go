package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func newTestGraph(t *testing.T) *Core {
	t.Helper()
	g, err := New(Config{Store: memory.New(), AllowReset: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })
	return g
}

func TestNewBatchBuilder_NilCoreReturnsErrNilGraph(t *testing.T) {
	t.Parallel()

	b, err := NewBatchBuilder(nil)
	if !errors.Is(err, ErrNilGraph) {
		t.Fatalf("NewBatchBuilder(nil) = (%v, %v), want ErrNilGraph", b, err)
	}
	if b != nil {
		t.Fatalf("NewBatchBuilder(nil) returned builder %v", b)
	}
}

func TestBatchBuilderAddNode(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b, _ := NewBatchBuilder(g)

	n, err := b.AddNode([]string{"Person", "Actor"}, map[string]any{"name": "Alice", "age": 30})
	if err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	if n == nil {
		t.Fatal("AddNode returned nil node")
	}
	if n.ID() == 0 {
		t.Fatal("AddNode returned zero ID")
	}

	// Check properties.
	v, ok := n.GetProperty("name")
	if !ok || v != "Alice" {
		t.Errorf("name = %v (%v), want Alice", v, ok)
	}

	// Check integrity.
	ig := n.Integrity()
	if ig == nil {
		t.Fatal("AddNode returned node without integrity")
	}
	if ig.Hash == "" {
		t.Error("AddNode returned empty hash")
	}
	if ig.PrevHash != "" {
		t.Error("genesis node should have empty PrevHash")
	}

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Created != 1 {
		t.Fatalf("Created = %d, want 1", result.Created)
	}

	// Check labels after Execute retokenizes the queued node.
	labels := g.Nodes.Labels(n)
	if len(labels) != 2 || labels[0] != "Person" || labels[1] != "Actor" {
		t.Errorf("labels = %v, want [Person Actor]", labels)
	}
}

func TestBatchBuilderAddNodes(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b, _ := NewBatchBuilder(g)

	if err := b.AddNodes([]string{"Bulk", "Imported"}, map[string]any{"kind": "fast"}, 3); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Created != 3 {
		t.Fatalf("Created = %d, want 3", result.Created)
	}
	nodes, err := g.Nodes.ByLabel("Bulk", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabel: %v", err)
	}
	if len(nodes) != 3 {
		t.Fatalf("Bulk nodes = %d, want 3", len(nodes))
	}
	for _, n := range nodes {
		if got, ok := n.GetProperty("kind"); !ok || got != "fast" {
			t.Fatalf("kind = %v (%v), want fast", got, ok)
		}
		labels := g.Nodes.Labels(n)
		if len(labels) != 2 || labels[0] != "Bulk" || labels[1] != "Imported" {
			t.Fatalf("labels = %v, want [Bulk Imported]", labels)
		}
		if ig := n.Integrity(); ig == nil || ig.Hash == "" {
			t.Fatalf("node %d missing integrity", n.ID())
		}
	}
}

func TestBatchBuilderAddNodeInvalidLabels(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b, _ := NewBatchBuilder(g)

	_, err := b.AddNode(nil, nil)
	if !errors.Is(err, ErrNoLabels) {
		t.Fatalf("expected ErrNoLabels, got %v", err)
	}

	_, err = b.AddNode([]string{}, nil)
	if !errors.Is(err, ErrNoLabels) {
		t.Fatalf("expected ErrNoLabels for empty slice, got %v", err)
	}
}

func TestBatchBuilderAddNodeInvalidProps(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b, _ := NewBatchBuilder(g)

	_, err := b.AddNode([]string{"Person"}, map[string]any{"tkg_secret": "bad"})
	if err == nil {
		t.Fatal("expected error for tkg_ prefix property, got nil")
	}
}

func TestBatchBuilderAddRelationship(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b, _ := NewBatchBuilder(g)

	n1, _ := b.AddNode([]string{"Person"}, nil)
	n2, _ := b.AddNode([]string{"Person"}, nil)

	r, err := b.AddRelationship("KNOWS", n1, n2, map[string]any{"since": 2020})
	if err != nil {
		t.Fatalf("AddRelationship returned error: %v", err)
	}
	if r == nil {
		t.Fatal("AddRelationship returned nil")
	}
	if r.ID() == 0 {
		t.Fatal("AddRelationship returned zero ID")
	}

	v, ok := r.GetProperty("since")
	if !ok || v != 2020 {
		t.Errorf("since = %v (%v), want 2020", v, ok)
	}

	ig := r.Integrity()
	if ig == nil || ig.Hash == "" {
		t.Fatal("AddRelationship returned rel without hash")
	}
}

func TestBatchBuilderAddRelationshipDefersNewRelTypeUntilExecute(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b, _ := NewBatchBuilder(g)

	n1, err := b.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode n1: %v", err)
	}
	n2, err := b.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode n2: %v", err)
	}

	const typ = "KNOWS_DEFERRED"
	r, err := b.AddRelationship(typ, n1, n2, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	if _, ok := g.Resolve.LookupRelType(typ); ok {
		t.Fatalf("queued AddRelationship registered new rel type %q before Execute", typ)
	}

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Failed != 0 {
		t.Fatalf("batch failed: %+v", result.Errors)
	}

	tok, ok := g.Resolve.LookupRelType(typ)
	if !ok {
		t.Fatalf("Execute did not register rel type %q", typ)
	}
	if !r.HasTypeTokenRaw(tok) {
		t.Fatalf("returned relationship token = %d, want %d", r.TypeToken().Value(), tok)
	}
	if gotType := g.Rels.Type(r); gotType != typ {
		t.Fatalf("returned relationship type = %q, want %q", gotType, typ)
	}

	got, err := g.Rels.Get(context.Background(), r.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if !got.HasTypeTokenRaw(tok) {
		t.Fatalf("stored relationship token = %d, want %d", got.TypeToken().Value(), tok)
	}
}

func TestBatchBuilderAddRelNilEndpoint(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b, _ := NewBatchBuilder(g)

	n1, _ := b.AddNode([]string{"Person"}, nil)

	_, err := b.AddRelationship("KNOWS", n1, nil, nil)
	if !errors.Is(err, ErrNilNode) {
		t.Fatalf("expected ErrNilNode, got %v", err)
	}

	_, err = b.AddRelationship("KNOWS", nil, n1, nil)
	if !errors.Is(err, ErrNilNode) {
		t.Fatalf("expected ErrNilNode for nil start, got %v", err)
	}
}

func TestBatchBuilderUpdateNodeInvalidKey(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b, _ := NewBatchBuilder(g)

	err := b.UpdateNode(types.NodeID(1), map[string]any{"tkg_version": 5})
	if err == nil {
		t.Fatal("expected error for tkg_ key, got nil")
	}
}

func TestBatchBuilderUpdateAllowsProvenanceKeys(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"score": int64(1)})
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	bNode, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, bNode, map[string]any{"weight": int64(1)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	batch, _ := NewBatchBuilder(g)
	if err := batch.UpdateNode(a.ID(), map[string]any{
		"score":         int64(2),
		"tkg_author_id": "node-updater",
	}); err != nil {
		t.Fatalf("UpdateNode queue: %v", err)
	}
	if err := batch.UpdateRelationship(r.ID(), map[string]any{
		"weight":        int64(2),
		"tkg_author_id": "rel-updater",
	}); err != nil {
		t.Fatalf("UpdateRelationship queue: %v", err)
	}

	result, err := batch.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Failed != 0 {
		t.Fatalf("batch failed: %+v", result.Errors)
	}

	gotNode, err := g.Nodes.Get(context.Background(), a.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if ig := gotNode.Integrity(); ig == nil || ig.AuthorID != "node-updater" {
		t.Fatalf("node integrity = %+v, want AuthorID node-updater", ig)
	}
	if _, ok := gotNode.GetProperty(types.ShadowAuthorID); ok {
		t.Fatal("node stored tkg_author_id as a normal property")
	}

	gotRel, err := g.Rels.Get(context.Background(), r.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if ig := gotRel.Integrity(); ig == nil || ig.AuthorID != "rel-updater" {
		t.Fatalf("relationship integrity = %+v, want AuthorID rel-updater", ig)
	}
	if _, ok := gotRel.GetProperty(types.ShadowAuthorID); ok {
		t.Fatal("relationship stored tkg_author_id as a normal property")
	}
}

func TestBatchBuilderMetadataOnlyUpdatesAreVersioned(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	batch, _ := NewBatchBuilder(g)
	if err := batch.UpdateNode(a.ID(), map[string]any{"tkg_author_id": "node-batch"}); err != nil {
		t.Fatalf("UpdateNode queue: %v", err)
	}
	if err := batch.UpdateRelationship(r.ID(), map[string]any{"tkg_author_id": "rel-batch"}); err != nil {
		t.Fatalf("UpdateRelationship queue: %v", err)
	}

	result, err := batch.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Updated != 2 || result.Failed != 0 {
		t.Fatalf("result = %+v, want Updated=2 Failed=0", result)
	}

	gotNode, err := g.Nodes.Get(context.Background(), a.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if gotNode.Version() != 1 {
		t.Fatalf("node version = %d, want 1", gotNode.Version())
	}
	if ig := gotNode.Integrity(); ig == nil || ig.AuthorID != "node-batch" {
		t.Fatalf("node integrity = %+v, want AuthorID node-batch", ig)
	}
	if _, ok := gotNode.GetProperty(types.ShadowAuthorID); ok {
		t.Fatal("node stored tkg_author_id as a normal property")
	}

	gotRel, err := g.Rels.Get(context.Background(), r.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if gotRel.Version() != 1 {
		t.Fatalf("relationship version = %d, want 1", gotRel.Version())
	}
	if ig := gotRel.Integrity(); ig == nil || ig.AuthorID != "rel-batch" {
		t.Fatalf("relationship integrity = %+v, want AuthorID rel-batch", ig)
	}
	if _, ok := gotRel.GetProperty(types.ShadowAuthorID); ok {
		t.Fatal("relationship stored tkg_author_id as a normal property")
	}
}

func TestBatchBuilderExecuteEmpty(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b, _ := NewBatchBuilder(g)

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute error = %v, want nil", err)
	}
	if result.Created != 0 || result.Updated != 0 || result.Deleted != 0 || result.Failed != 0 {
		t.Errorf("empty batch result = %+v, want all zeros", result)
	}
	if result.Duration < 0 {
		t.Error("Duration should not be negative")
	}
}

func TestBatchBuilderExecuteNodes(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b, _ := NewBatchBuilder(g)

	for i := 0; i < 100; i++ {
		_, err := b.AddNode([]string{"Person"}, map[string]any{"index": i})
		if err != nil {
			t.Fatalf("AddNode(%d) returned error: %v", i, err)
		}
	}

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Created != 100 {
		t.Fatalf("Created = %d, want 100", result.Created)
	}
	if result.Failed != 0 {
		t.Fatalf("Failed = %d, want 0; errors: %v", result.Failed, result.Errors)
	}

	count, _ := g.Nodes.Count()
	if count != 100 {
		t.Fatalf("NodeCount = %d, want 100", count)
	}
}

func TestBatchBuilderExecuteNodesAndRels(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b, _ := NewBatchBuilder(g)

	n1, _ := b.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	n2, _ := b.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})
	n3, _ := b.AddNode([]string{"Company"}, map[string]any{"name": "Acme"})

	_, err := b.AddRelationship("KNOWS", n1, n2, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.AddRelationship("WORKS_AT", n1, n3, nil)
	if err != nil {
		t.Fatal(err)
	}

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Created != 5 { // 3 nodes + 2 rels
		t.Fatalf("Created = %d, want 5", result.Created)
	}
	if result.Failed != 0 {
		t.Fatalf("Failed = %d; errors: %v", result.Failed, result.Errors)
	}

	// Verify relationships are navigable.
	nodeCount, _ := g.Nodes.Count()
	relCount, _ := g.Rels.Count()
	if nodeCount != 3 {
		t.Fatalf("NodeCount = %d, want 3", nodeCount)
	}
	if relCount != 2 {
		t.Fatalf("RelationshipCount = %d, want 2", relCount)
	}

	outgoing, _ := g.Rels.Outgoing(n1.ID(), "")
	if len(outgoing) != 2 {
		t.Fatalf("OutgoingRelationships(n1) = %d, want 2", len(outgoing))
	}
}

func TestBatchBuilderExecuteUpdates(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	// Create a node via normal API.
	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()

	b, _ := NewBatchBuilder(g)
	if err := b.UpdateNode(id, map[string]any{"name": "Bob", "age": 30}); err != nil {
		t.Fatal(err)
	}

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Updated != 1 {
		t.Fatalf("Updated = %d, want 1", result.Updated)
	}

	// Verify update.
	updated, _ := g.Nodes.Get(context.Background(), id)
	v, _ := updated.GetProperty("name")
	if v != "Bob" {
		t.Errorf("name = %v, want Bob", v)
	}
	if updated.Version() != 1 {
		t.Errorf("version = %d, want 1", updated.Version())
	}
}

func TestBatchBuilderUpdateQueuesSnapshotOfUpdateMaps(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	bNode, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Eve"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, bNode, nil)
	if err != nil {
		t.Fatal(err)
	}

	nodeTags := []string{"queued"}
	nodeUpdates := map[string]any{"name": "Bob", "tags": nodeTags}
	relNotes := []string{"queued-rel"}
	relUpdates := map[string]any{"weight": int64(1), "notes": relNotes}

	batch, _ := NewBatchBuilder(g)
	if err := batch.UpdateNode(a.ID(), nodeUpdates); err != nil {
		t.Fatal(err)
	}
	if err := batch.UpdateRelationship(r.ID(), relUpdates); err != nil {
		t.Fatal(err)
	}

	nodeUpdates["name"] = "Mallory"
	nodeUpdates["late"] = "should-not-apply"
	nodeTags[0] = "mutated"
	relUpdates["weight"] = int64(2)
	relUpdates["late"] = "should-not-apply"
	relNotes[0] = "mutated-rel"

	result, err := batch.Execute()
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Updated != 2 {
		t.Fatalf("Updated = %d, want 2", result.Updated)
	}

	updatedNode, err := g.Nodes.Get(context.Background(), a.ID())
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := updatedNode.GetProperty("name"); got != "Bob" {
		t.Fatalf("node name = %v, want queued value Bob", got)
	}
	if got, _ := updatedNode.GetProperty("tags"); !stringSliceEqual(got, []string{"queued"}) {
		t.Fatalf("node tags = %#v, want queued value [queued]", got)
	}
	if got, ok := updatedNode.GetProperty("late"); ok {
		t.Fatalf("late node update applied after queue mutation: %v", got)
	}

	updatedRel, err := g.Rels.Get(context.Background(), r.ID())
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := updatedRel.GetProperty("weight"); got != int64(1) {
		t.Fatalf("rel weight = %v, want queued value 1", got)
	}
	if got, _ := updatedRel.GetProperty("notes"); !stringSliceEqual(got, []string{"queued-rel"}) {
		t.Fatalf("rel notes = %#v, want queued value [queued-rel]", got)
	}
	if got, ok := updatedRel.GetProperty("late"); ok {
		t.Fatalf("late rel update applied after queue mutation: %v", got)
	}
}

func TestBatchBuilderCreateQueuesIsolatedFromReturnedSkeletonMutations(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	batch, _ := NewBatchBuilder(g)
	a, err := batch.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	bNode, err := batch.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	rel, err := batch.AddRelationship("KNOWS", a, bNode, map[string]any{"since": int64(2026)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	if err := a.SetProperty("name", "Mallory"); err != nil {
		t.Fatalf("mutate returned node property: %v", err)
	}
	if err := a.SetProperty("late", "must-not-persist"); err != nil {
		t.Fatalf("mutate returned node late property: %v", err)
	}
	a.SetVersion(99)
	a.AddLabelTokenRaw(65535)
	if ig := a.Integrity(); ig != nil {
		ig.Hash = "tampered-node-hash"
	}
	if err := rel.SetProperty("since", int64(1999)); err != nil {
		t.Fatalf("mutate returned rel property: %v", err)
	}
	if err := rel.SetProperty("late", "must-not-persist"); err != nil {
		t.Fatalf("mutate returned rel late property: %v", err)
	}
	rel.SetVersion(88)
	if ig := rel.Integrity(); ig != nil {
		ig.Hash = "tampered-rel-hash"
		ig.FromNodeHash = "tampered-from"
	}

	result, err := batch.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Created != 3 {
		t.Fatalf("Created = %d, want 3", result.Created)
	}

	storedNode, err := g.Nodes.Get(context.Background(), a.ID())
	if err != nil {
		t.Fatalf("Get node: %v", err)
	}
	if got, _ := storedNode.GetProperty("name"); got != "Alice" {
		t.Fatalf("stored node name = %v, want queued value Alice", got)
	}
	if _, ok := storedNode.GetProperty("late"); ok {
		t.Fatal("stored node includes property added through returned skeleton mutation")
	}
	if storedNode.Version() != 0 {
		t.Fatalf("stored node version = %d, want 0", storedNode.Version())
	}
	if got, _ := a.GetProperty("name"); got != "Alice" {
		t.Fatalf("returned node name after Execute = %v, want committed value Alice", got)
	}
	if _, ok := a.GetProperty("late"); ok {
		t.Fatal("returned node still includes pre-Execute skeleton mutation")
	}
	if a.Version() != 0 {
		t.Fatalf("returned node version = %d, want committed version 0", a.Version())
	}
	if a.Temporal() == nil || a.Temporal().TxFrom == 0 {
		t.Fatal("returned node was not finalised with committed temporal metadata")
	}
	if ok, err := g.Hash.VerifyNodeChain(a.ID()); err != nil || !ok {
		t.Fatalf("VerifyNodeChain = (%v, %v), want (true, nil)", ok, err)
	}
	if err := a.SetProperty("name", "PostExecuteMutation"); err != nil {
		t.Fatalf("post-execute returned node mutation: %v", err)
	}
	storedAfterReturnedMutation, err := g.Nodes.Get(context.Background(), a.ID())
	if err != nil {
		t.Fatalf("Get node after returned mutation: %v", err)
	}
	if got, _ := storedAfterReturnedMutation.GetProperty("name"); got != "Alice" {
		t.Fatalf("post-execute returned node mutation affected store: got %v, want Alice", got)
	}

	storedRel, err := g.Rels.Get(context.Background(), rel.ID())
	if err != nil {
		t.Fatalf("Get relationship: %v", err)
	}
	if got, _ := storedRel.GetProperty("since"); got != int64(2026) {
		t.Fatalf("stored rel since = %v, want queued value 2026", got)
	}
	if _, ok := storedRel.GetProperty("late"); ok {
		t.Fatal("stored rel includes property added through returned skeleton mutation")
	}
	if storedRel.Version() != 0 {
		t.Fatalf("stored rel version = %d, want 0", storedRel.Version())
	}
	if got, _ := rel.GetProperty("since"); got != int64(2026) {
		t.Fatalf("returned rel since after Execute = %v, want committed value 2026", got)
	}
	if _, ok := rel.GetProperty("late"); ok {
		t.Fatal("returned rel still includes pre-Execute skeleton mutation")
	}
	if rel.Version() != 0 {
		t.Fatalf("returned rel version = %d, want committed version 0", rel.Version())
	}
	if rel.Temporal() == nil || rel.Temporal().TxFrom == 0 {
		t.Fatal("returned rel was not finalised with committed temporal metadata")
	}
	if ok, err := g.Hash.VerifyRelChain(rel.ID()); err != nil || !ok {
		t.Fatalf("VerifyRelChain = (%v, %v), want (true, nil)", ok, err)
	}
	if err := rel.SetProperty("since", int64(2030)); err != nil {
		t.Fatalf("post-execute returned rel mutation: %v", err)
	}
	storedRelAfterReturnedMutation, err := g.Rels.Get(context.Background(), rel.ID())
	if err != nil {
		t.Fatalf("Get rel after returned mutation: %v", err)
	}
	if got, _ := storedRelAfterReturnedMutation.GetProperty("since"); got != int64(2026) {
		t.Fatalf("post-execute returned rel mutation affected store: got %v, want 2026", got)
	}
}

func stringSliceEqual(got any, want []string) bool {
	s, ok := got.([]string)
	if !ok || len(s) != len(want) {
		return false
	}
	for i := range s {
		if s[i] != want[i] {
			return false
		}
	}
	return true
}

func TestBatchBuilderExecuteEmptyUpdatesAreNoOps(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	_ = g.Events.SetSync(eventspkg.NewEventBus())

	n1, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode n1: %v", err)
	}
	n2, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode n2: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", n1, n2, map[string]any{"since": int64(2026)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	events := collectEvents(g, eventspkg.EventNodeUpdate)

	b, _ := NewBatchBuilder(g)
	if err := b.UpdateNode(n1.ID(), map[string]any{}); err != nil {
		t.Fatalf("UpdateNode queue: %v", err)
	}
	if err := b.UpdateRelationship(r.ID(), nil); err != nil {
		t.Fatalf("UpdateRelationship queue: %v", err)
	}

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Updated != 0 {
		t.Fatalf("Updated = %d, want 0 for empty update no-ops", result.Updated)
	}
	if gotEvents := drain(events); len(gotEvents) != 0 {
		t.Fatalf("empty batch updates published events: %+v", gotEvents)
	}

	gotNode, err := g.Nodes.Get(context.Background(), n1.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if gotNode.Version() != 0 {
		t.Fatalf("node version = %d, want 0", gotNode.Version())
	}
	gotRel, err := g.Rels.Get(context.Background(), r.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if gotRel.Version() != 0 {
		t.Fatalf("relationship version = %d, want 0", gotRel.Version())
	}
}

func TestBatchBuilderExecuteEmptyUpdatesStillCheckExistence(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	b, _ := NewBatchBuilder(g)
	if err := b.UpdateNode(types.NodeID(1), map[string]any{}); err != nil {
		t.Fatalf("UpdateNode queue: %v", err)
	}
	if err := b.UpdateRelationship(types.RelID(2), nil); err != nil {
		t.Fatalf("UpdateRelationship queue: %v", err)
	}

	result, err := b.Execute()
	if !errors.Is(err, ErrBatchFailed) {
		t.Fatalf("Execute error = %v, want ErrBatchFailed", err)
	}
	if result.Updated != 0 || result.Failed != 2 {
		t.Fatalf("result = %+v, want Updated=0 Failed=2", result)
	}
	if len(result.Errors) != 2 {
		t.Fatalf("len(result.Errors) = %d, want 2", len(result.Errors))
	}
	if !errors.Is(result.Errors[0].Err, storepkg.ErrNodeNotFound) {
		t.Fatalf("node error = %v, want ErrNodeNotFound", result.Errors[0].Err)
	}
	if !errors.Is(result.Errors[1].Err, storepkg.ErrRelNotFound) {
		t.Fatalf("relationship error = %v, want ErrRelNotFound", result.Errors[1].Err)
	}
}

func TestBatchBuilderExecuteDeletes(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	// Create nodes + relationship.
	n1, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	n2, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", n1, n2, nil)

	b, _ := NewBatchBuilder(g)
	b.DeleteRelationship(r.ID())
	b.DeleteNode(n1.ID())

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Deleted != 2 {
		t.Fatalf("Deleted = %d, want 2", result.Deleted)
	}

	nodeCount, _ := g.Nodes.Count()
	if nodeCount != 1 {
		t.Fatalf("NodeCount = %d, want 1", nodeCount)
	}
	relCount, _ := g.Rels.Count()
	if relCount != 0 {
		t.Fatalf("RelationshipCount = %d, want 0", relCount)
	}
}

func TestBatchBuilderExecuteCascadeDeleteCountsRelationshipRows(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	bn, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	c, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode c: %v", err)
	}
	if _, err := g.Rels.Add(context.Background(), "KNOWS", a, bn, nil); err != nil {
		t.Fatalf("AddRelationship out: %v", err)
	}
	if _, err := g.Rels.Add(context.Background(), "LIKES", c, a, nil); err != nil {
		t.Fatalf("AddRelationship in: %v", err)
	}

	batch, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	batch.DeleteNode(a.ID())

	result, err := batch.Execute()
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Deleted != 3 {
		t.Fatalf("Deleted = %d, want 3", result.Deleted)
	}
}

func TestBatchBuilderExecuteMixed(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	// Pre-existing data.
	existing, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Eve"})
	existingID := existing.ID()

	b, _ := NewBatchBuilder(g)

	// Add new nodes.
	n1, _ := b.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	n2, _ := b.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})

	// Add relationship.
	_, err := b.AddRelationship("KNOWS", n1, n2, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Update existing.
	if err := b.UpdateNode(existingID, map[string]any{"name": "Eve Updated"}); err != nil {
		t.Fatal(err)
	}

	// Delete the existing node at the end.
	b.DeleteNode(existingID)

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Created != 3 { // 2 nodes + 1 rel
		t.Errorf("Created = %d, want 3", result.Created)
	}
	if result.Updated != 1 {
		t.Errorf("Updated = %d, want 1", result.Updated)
	}
	if result.Deleted != 1 {
		t.Errorf("Deleted = %d, want 1", result.Deleted)
	}
	if result.Failed != 0 {
		t.Errorf("Failed = %d; errors: %v", result.Failed, result.Errors)
	}

	// Eve deleted, Alice and Bob remain.
	nodeCount, _ := g.Nodes.Count()
	if nodeCount != 2 {
		t.Fatalf("NodeCount = %d, want 2", nodeCount)
	}
}

func TestBatchBuilderExecute1000Nodes(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b, _ := NewBatchBuilder(g)

	for i := 0; i < 1000; i++ {
		_, err := b.AddNode([]string{"Item"}, map[string]any{"index": i})
		if err != nil {
			t.Fatalf("AddNode(%d) returned error: %v", i, err)
		}
	}

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Created != 1000 {
		t.Fatalf("Created = %d, want 1000", result.Created)
	}
	if result.Failed != 0 {
		t.Fatalf("Failed = %d; errors: %v", result.Failed, result.Errors)
	}

	count, _ := g.Nodes.Count()
	if count != 1000 {
		t.Fatalf("NodeCount = %d, want 1000", count)
	}

	// Verify throughput is reasonable.
	if result.Duration <= 0 {
		t.Error("Duration should be positive")
	}
}

func TestBatchBuilderExecuteUpdateRelationship(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n1, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	n2, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", n1, n2, map[string]any{"weight": 1})
	rID := r.ID()

	b, _ := NewBatchBuilder(g)
	if err := b.UpdateRelationship(rID, map[string]any{"weight": 10}); err != nil {
		t.Fatal(err)
	}

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Updated != 1 {
		t.Fatalf("Updated = %d, want 1", result.Updated)
	}

	updated, _ := g.Rels.Get(context.Background(), rID)
	v, _ := updated.GetProperty("weight")
	if v != 10 {
		t.Errorf("weight = %v, want 10", v)
	}
	if updated.Version() != 1 {
		t.Errorf("version = %d, want 1", updated.Version())
	}
}

func TestBatchBuilderUpdateRelInvalidKey(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b, _ := NewBatchBuilder(g)

	err := b.UpdateRelationship(types.RelID(1), map[string]any{"tkg_type": "bad"})
	if err == nil {
		t.Fatal("expected error for tkg_ key, got nil")
	}
}

func TestBatchErrorString(t *testing.T) {
	t.Parallel()
	be := BatchError{Op: "AddNode", ID: types.EntityID(42), Err: storepkg.ErrNodeExists}
	s := be.Error()
	if s == "" {
		t.Fatal("BatchError.Error() returned empty string")
	}
	if !errors.Is(be, storepkg.ErrNodeExists) {
		t.Fatalf("errors.Is(BatchError, ErrNodeExists) = false; err = %v", be)
	}
	if !errors.Is(&be, storepkg.ErrNodeExists) {
		t.Fatalf("errors.Is(&BatchError, ErrNodeExists) = false; err = %v", &be)
	}
}

func TestBatchBuilderPartialFailure(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	// Create a node so we can update it.
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	nID := n.ID()

	b, _ := NewBatchBuilder(g)

	// Valid update.
	if err := b.UpdateNode(nID, map[string]any{"name": "Bob"}); err != nil {
		t.Fatal(err)
	}

	// Update on non-existent node — will fail during Execute.
	if err := b.UpdateNode(types.NodeID(999999), map[string]any{"name": "Ghost"}); err != nil {
		t.Fatal(err)
	}

	// Delete a non-existent relationship — will fail during Execute.
	b.DeleteRelationship(types.RelID(888888))

	result, err := b.Execute()
	if !errors.Is(err, ErrBatchFailed) {
		t.Fatalf("Execute error = %v, want ErrBatchFailed", err)
	}

	if result.Updated != 1 {
		t.Errorf("Updated = %d, want 1", result.Updated)
	}
	if result.Failed != 2 {
		t.Errorf("Failed = %d, want 2", result.Failed)
	}
	if len(result.Errors) != 2 {
		t.Fatalf("len(Errors) = %d, want 2", len(result.Errors))
	}

	// Verify partial success: the valid update was applied.
	updated, _ := g.Nodes.Get(context.Background(), nID)
	v, _ := updated.GetProperty("name")
	if v != "Bob" {
		t.Errorf("name = %v, want Bob (partial failure should not roll back successes)", v)
	}
}

// --- Fix 5: Batch vs Snapshot isolation tests ---

func TestBatchExecuteBlocksSnapshot(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	// Create 50 nodes via batch. Concurrent Snapshot must see 0 or all 50.
	const batchSize = 50
	var wg sync.WaitGroup
	wg.Add(2)

	errCh := make(chan error, 1)

	// Goroutine 1: execute batch.
	go func() {
		defer wg.Done()
		b, _ := NewBatchBuilder(g)
		for range batchSize {
			_, err := b.AddNode([]string{"Item"}, nil)
			if err != nil {
				errCh <- err
				return
			}
		}
		_, err := b.Execute()
		if err != nil {
			errCh <- err
		}
	}()

	// Goroutine 2: take snapshots repeatedly.
	go func() {
		defer wg.Done()
		for range 100 {
			snap, err := g.Temporal.Snapshot(types.Instant(time.Now().UnixMilli() + 60000))
			if err != nil {
				continue
			}
			// Snapshot must see either 0 or batchSize nodes — never a partial batch.
			if snap.NodeCount != 0 && snap.NodeCount != batchSize {
				errCh <- fmt.Errorf("snapshot saw %d nodes, expected 0 or %d (torn read)", snap.NodeCount, batchSize)
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestBatchAndSnapshotConcurrentStress(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	const batches = 5
	const nodesPerBatch = 10
	const snapshotters = 3

	var wg sync.WaitGroup
	errCh := make(chan error, batches+snapshotters)

	// Launch batch goroutines.
	for range batches {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, _ := NewBatchBuilder(g)
			for range nodesPerBatch {
				if _, err := b.AddNode([]string{"Stress"}, nil); err != nil {
					errCh <- err
					return
				}
			}
			if _, err := b.Execute(); err != nil {
				errCh <- err
			}
		}()
	}

	// Launch snapshot goroutines.
	for range snapshotters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				snap, err := g.Temporal.Snapshot(types.Instant(time.Now().UnixMilli() + 60000))
				if err != nil {
					continue
				}
				// Node count must be a multiple of nodesPerBatch (each batch is atomic).
				if snap.NodeCount%nodesPerBatch != 0 {
					errCh <- fmt.Errorf("snapshot saw %d nodes, not a multiple of %d", snap.NodeCount, nodesPerBatch)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

// --- Fix 5: BatchBuilder.Nodes.Add — canonical labels for hash ---

func TestBatchBuilder_AddNode_DuplicateLabelsHash(t *testing.T) {
	// Adding a node via batch with duplicate labels ["A", "B", "A"]
	// should produce the same hash as recomputing with canonical labels.
	// Deduplication happens in types.NewNode, so the hash must use canonical labels.
	t.Parallel()
	g := newTestGraph(t)

	b, _ := NewBatchBuilder(g)
	batchNode, err := b.AddNode([]string{"A", "B", "A"}, map[string]any{"key": "val"})
	if err != nil {
		t.Fatalf("batch AddNode: %v", err)
	}
	if _, err := b.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	batchHash := batchNode.Integrity().Hash

	// Recompute with canonical labels — must match the stored hash.
	canonicalLabels := g.Nodes.Labels(batchNode)
	expectedHash := integrity.ComputeNodeHash(batchNode, canonicalLabels)
	if batchHash != expectedHash {
		t.Errorf("batch hash = %s, recomputed = %s (labels: %v)", batchHash, expectedHash, canonicalLabels)
	}
}

func TestBatchBuilder_AddNode_HashChainVerification(t *testing.T) {
	// Create a node via batch with duplicate labels, execute, verify hash chain.
	t.Parallel()
	g := newTestGraph(t)

	b, _ := NewBatchBuilder(g)
	n, err := b.AddNode([]string{"Person", "Person", "Actor"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("batch AddNode: %v", err)
	}

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Created != 1 {
		t.Fatalf("expected 1 created, got %d", result.Created)
	}

	id := n.ID()
	ok, err := g.Hash.VerifyNodeChain(id)
	if err != nil {
		t.Fatalf("VerifyNodeChain: %v", err)
	}
	if !ok {
		t.Error("hash chain verification failed for batch-created node with duplicate labels")
	}
}

func TestBatchBuilderConcurrentQueueMethods(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b, _ := NewBatchBuilder(g)

	const workers = 64
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := b.AddNode([]string{"Concurrent"}, map[string]any{"i": i}); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent AddNode: %v", err)
	}

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Created != workers {
		t.Fatalf("Created = %d, want %d", result.Created, workers)
	}
}

func TestBatchBuilderAfterExecuteReturnsErrBatchDone(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b, _ := NewBatchBuilder(g)

	if _, err := b.AddNode([]string{"Person"}, nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if _, err := b.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if _, err := b.AddNode([]string{"Person"}, nil); !errors.Is(err, ErrBatchDone) {
		t.Fatalf("AddNode after Execute: got %v, want ErrBatchDone", err)
	}
	if err := b.AddNodes([]string{"Person"}, nil, 1); !errors.Is(err, ErrBatchDone) {
		t.Fatalf("AddNodes after Execute: got %v, want ErrBatchDone", err)
	}
	if _, err := b.AddRelationship("REL", nil, nil, nil); !errors.Is(err, ErrBatchDone) {
		t.Fatalf("AddRelationship after Execute: got %v, want ErrBatchDone", err)
	}
	if err := b.UpdateNode(types.NodeID(1), map[string]any{"k": "v"}); !errors.Is(err, ErrBatchDone) {
		t.Fatalf("UpdateNode after Execute: got %v, want ErrBatchDone", err)
	}
	if err := b.UpdateRelationship(types.RelID(1), map[string]any{"k": "v"}); !errors.Is(err, ErrBatchDone) {
		t.Fatalf("UpdateRelationship after Execute: got %v, want ErrBatchDone", err)
	}
	if err := b.DeleteNode(types.NodeID(1)); !errors.Is(err, ErrBatchDone) {
		t.Fatalf("DeleteNode after Execute: got %v, want ErrBatchDone", err)
	}
	if err := b.DeleteRelationship(types.RelID(1)); !errors.Is(err, ErrBatchDone) {
		t.Fatalf("DeleteRelationship after Execute: got %v, want ErrBatchDone", err)
	}
	if _, err := b.Execute(); !errors.Is(err, ErrBatchDone) {
		t.Fatalf("second Execute: got %v, want ErrBatchDone", err)
	}
}

func TestBatchBuilderExecuteReleasesBuilderLockBeforeSyncEventDispatch(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	bus := eventspkg.NewEventBus()
	_ = g.Events.SetSync(bus)

	b, _ := NewBatchBuilder(g)
	if _, err := b.AddNode([]string{"Person"}, nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	handlerErr := make(chan error, 1)
	bus.Subscribe(func(eventspkg.Event) {
		_, err := b.AddNode([]string{"Late"}, nil)
		handlerErr <- err
	})

	done := make(chan error, 1)
	go func() {
		_, err := b.Execute()
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Execute deadlocked during synchronous event dispatch")
	}

	select {
	case err := <-handlerErr:
		if !errors.Is(err, ErrBatchDone) {
			t.Fatalf("handler AddNode: got %v, want ErrBatchDone", err)
		}
	default:
		t.Fatal("event handler was not called")
	}
}

type recordingEventPublisher struct {
	publishCalls      int
	publishBatchCalls int
	events            []eventspkg.Event
}

func (p *recordingEventPublisher) Publish(e eventspkg.Event) {
	p.publishCalls++
	p.events = append(p.events, e)
}

func (p *recordingEventPublisher) PublishBatch(events ...eventspkg.Event) {
	p.publishBatchCalls++
	p.events = append(p.events, events...)
}

func TestBatchBuilderExecutePublishesBufferedEventsAtomically(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	publisher := &recordingEventPublisher{}
	g.mu.Lock()
	g.events = publisher
	g.mu.Unlock()

	b, _ := NewBatchBuilder(g)
	if _, err := b.AddNode([]string{"Person"}, nil); err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	if _, err := b.AddNode([]string{"Person"}, nil); err != nil {
		t.Fatalf("AddNode B: %v", err)
	}

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Created != 2 {
		t.Fatalf("Created = %d, want 2", result.Created)
	}
	if publisher.publishCalls != 0 {
		t.Fatalf("batch event flush used Publish %d time(s), want PublishBatch", publisher.publishCalls)
	}
	if publisher.publishBatchCalls != 1 {
		t.Fatalf("PublishBatch calls = %d, want 1", publisher.publishBatchCalls)
	}
	if len(publisher.events) != 2 {
		t.Fatalf("published events = %d, want 2", len(publisher.events))
	}
	for i, e := range publisher.events {
		if e.Type != eventspkg.EventNodeCreate {
			t.Fatalf("event %d type = %v, want EventNodeCreate", i, e.Type)
		}
	}
}
