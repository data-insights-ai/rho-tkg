package graph

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/integrity"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func newTestGraph(t *testing.T) *Graph {
	t.Helper()
	g, err := New(Config{Store: NewMemoryStore()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })
	return g
}

func TestBatchBuilderAddNode(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b := NewBatchBuilder(g)

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

	// Check labels.
	labels := g.NodeLabels(n)
	if len(labels) != 2 || labels[0] != "Person" || labels[1] != "Actor" {
		t.Errorf("labels = %v, want [Person Actor]", labels)
	}
}

func TestBatchBuilderAddNodeInvalidLabels(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b := NewBatchBuilder(g)

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
	b := NewBatchBuilder(g)

	_, err := b.AddNode([]string{"Person"}, map[string]any{"tkg_secret": "bad"})
	if err == nil {
		t.Fatal("expected error for tkg_ prefix property, got nil")
	}
}

func TestBatchBuilderAddRelationship(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b := NewBatchBuilder(g)

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

func TestBatchBuilderAddRelNilEndpoint(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b := NewBatchBuilder(g)

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
	b := NewBatchBuilder(g)

	err := b.UpdateNode(types.NodeID(1), map[string]any{"tkg_version": 5})
	if err == nil {
		t.Fatal("expected error for tkg_ key, got nil")
	}
}

func TestBatchBuilderExecuteEmpty(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b := NewBatchBuilder(g)

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
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
	b := NewBatchBuilder(g)

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

	count, _ := g.NodeCount()
	if count != 100 {
		t.Fatalf("NodeCount = %d, want 100", count)
	}
}

func TestBatchBuilderExecuteNodesAndRels(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b := NewBatchBuilder(g)

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
	nodeCount, _ := g.NodeCount()
	relCount, _ := g.RelationshipCount()
	if nodeCount != 3 {
		t.Fatalf("NodeCount = %d, want 3", nodeCount)
	}
	if relCount != 2 {
		t.Fatalf("RelationshipCount = %d, want 2", relCount)
	}

	outgoing, _ := g.OutgoingRelationships(n1.ID(), "")
	if len(outgoing) != 2 {
		t.Fatalf("OutgoingRelationships(n1) = %d, want 2", len(outgoing))
	}
}

func TestBatchBuilderExecuteUpdates(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	// Create a node via normal API.
	n, err := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()

	b := NewBatchBuilder(g)
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
	updated, _ := g.GetNode(id)
	v, _ := updated.GetProperty("name")
	if v != "Bob" {
		t.Errorf("name = %v, want Bob", v)
	}
	if updated.Version() != 1 {
		t.Errorf("version = %d, want 1", updated.Version())
	}
}

func TestBatchBuilderExecuteDeletes(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	// Create nodes + relationship.
	n1, _ := g.AddNode([]string{"Person"}, nil)
	n2, _ := g.AddNode([]string{"Person"}, nil)
	r, _ := g.AddRelationship("KNOWS", n1, n2, nil)

	b := NewBatchBuilder(g)
	b.DeleteRelationship(r.ID())
	b.DeleteNode(n1.ID())

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if result.Deleted != 2 {
		t.Fatalf("Deleted = %d, want 2", result.Deleted)
	}

	nodeCount, _ := g.NodeCount()
	if nodeCount != 1 {
		t.Fatalf("NodeCount = %d, want 1", nodeCount)
	}
	relCount, _ := g.RelationshipCount()
	if relCount != 0 {
		t.Fatalf("RelationshipCount = %d, want 0", relCount)
	}
}

func TestBatchBuilderExecuteMixed(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	// Pre-existing data.
	existing, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Eve"})
	existingID := existing.ID()

	b := NewBatchBuilder(g)

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
	nodeCount, _ := g.NodeCount()
	if nodeCount != 2 {
		t.Fatalf("NodeCount = %d, want 2", nodeCount)
	}
}

func TestBatchBuilderExecute1000Nodes(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	b := NewBatchBuilder(g)

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

	count, _ := g.NodeCount()
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

	n1, _ := g.AddNode([]string{"Person"}, nil)
	n2, _ := g.AddNode([]string{"Person"}, nil)
	r, _ := g.AddRelationship("KNOWS", n1, n2, map[string]any{"weight": 1})
	rID := r.ID()

	b := NewBatchBuilder(g)
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

	updated, _ := g.GetRelationship(rID)
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
	b := NewBatchBuilder(g)

	err := b.UpdateRelationship(types.RelID(1), map[string]any{"tkg_type": "bad"})
	if err == nil {
		t.Fatal("expected error for tkg_ key, got nil")
	}
}

func TestBatchErrorString(t *testing.T) {
	t.Parallel()
	be := BatchError{Op: "AddNode", ID: types.EntityID(42), Err: ErrNodeExists}
	s := be.Error()
	if s == "" {
		t.Fatal("BatchError.Error() returned empty string")
	}
}

func TestBatchBuilderPartialFailure(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	// Create a node so we can update it.
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	nID := n.ID()

	b := NewBatchBuilder(g)

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
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
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
	updated, _ := g.GetNode(nID)
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
		b := NewBatchBuilder(g)
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
			snap, err := g.Snapshot(types.Instant(time.Now().UnixMilli() + 60000))
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
			b := NewBatchBuilder(g)
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
				snap, err := g.Snapshot(types.Instant(time.Now().UnixMilli() + 60000))
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

// --- Fix 5: BatchBuilder.AddNode — canonical labels for hash ---

func TestBatchBuilder_AddNode_DuplicateLabelsHash(t *testing.T) {
	// Adding a node via batch with duplicate labels ["A", "B", "A"]
	// should produce the same hash as recomputing with canonical labels.
	// Deduplication happens in types.NewNode, so the hash must use canonical labels.
	t.Parallel()
	g := newTestGraph(t)

	b := NewBatchBuilder(g)
	batchNode, err := b.AddNode([]string{"A", "B", "A"}, map[string]any{"key": "val"})
	if err != nil {
		t.Fatalf("batch AddNode: %v", err)
	}
	batchHash := batchNode.Integrity().Hash

	// Recompute with canonical labels — must match the stored hash.
	canonicalLabels := g.NodeLabels(batchNode)
	expectedHash := integrity.ComputeNodeHash(batchNode, canonicalLabels)
	if batchHash != expectedHash {
		t.Errorf("batch hash = %s, recomputed = %s (labels: %v)", batchHash, expectedHash, canonicalLabels)
	}
}

func TestBatchBuilder_AddNode_HashChainVerification(t *testing.T) {
	// Create a node via batch with duplicate labels, execute, verify hash chain.
	t.Parallel()
	g := newTestGraph(t)

	b := NewBatchBuilder(g)
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
	ok, err := g.VerifyNodeHashChain(id)
	if err != nil {
		t.Fatalf("VerifyNodeHashChain: %v", err)
	}
	if !ok {
		t.Error("hash chain verification failed for batch-created node with duplicate labels")
	}
}
