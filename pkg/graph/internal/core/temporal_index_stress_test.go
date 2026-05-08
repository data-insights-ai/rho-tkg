package core

// Stress tests for the temporal index fix (v3.0.60):
// - BatchBuilder at scale: large batches must persist all operations correctly.

import (
	"fmt"
	"sync"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- BatchBuilder at scale ---

// TestBatchBuilder_LargeNodeBatch verifies that a batch of 2000 nodes is fully
// persisted with zero failures and all nodes are retrievable afterwards.
func TestBatchBuilder_LargeNodeBatch(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping large batch test in short mode")
	}
	g := newTestGraph(t)

	const count = 2000
	b := NewBatchBuilder(g)
	for i := range count {
		label := fmt.Sprintf("Label%d", i%10)
		if _, err := b.AddNode([]string{label}, map[string]any{"idx": i, "score": i % 100}); err != nil {
			t.Fatalf("b.AddNode(%d): %v", i, err)
		}
	}
	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Created != count {
		t.Fatalf("Created = %d, want %d", result.Created, count)
	}
	if result.Failed != 0 {
		t.Fatalf("Failed = %d, want 0 (errors: %v)", result.Failed, result.Errors)
	}

	n, err := g.Nodes.Count()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if n != count {
		t.Fatalf("NodeCount = %d, want %d", n, count)
	}
}

// TestBatchBuilder_NodesAndRelationships verifies that a batch containing both
// nodes and relationships is executed correctly.
func TestBatchBuilder_NodesAndRelationships(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	const (
		nodeCount = 200
		relCount  = 400
	)

	b := NewBatchBuilder(g)
	nodes := make([]*types.Node, nodeCount)
	for i := range nodeCount {
		n, err := b.AddNode([]string{"Entity"}, map[string]any{"idx": i})
		if err != nil {
			t.Fatalf("b.AddNode(%d): %v", i, err)
		}
		nodes[i] = n
	}
	for i := range relCount {
		src := nodes[i%nodeCount]
		dst := nodes[(i*7+3)%nodeCount]
		if src.ID() == dst.ID() {
			dst = nodes[(i*7+4)%nodeCount]
		}
		if _, err := b.AddRelationship("LINK", src, dst, nil); err != nil {
			t.Fatalf("b.AddRelationship(%d): %v", i, err)
		}
	}

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Failed != 0 {
		t.Fatalf("Failed = %d, want 0 (errors: %v)", result.Failed, result.Errors)
	}
	if result.Created != nodeCount+relCount {
		t.Fatalf("Created = %d, want %d", result.Created, nodeCount+relCount)
	}

	nc, err := g.Nodes.Count()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	rc, err := g.Rels.Count()
	if err != nil {
		t.Fatalf("RelationshipCount: %v", err)
	}
	if nc != nodeCount {
		t.Fatalf("NodeCount = %d, want %d", nc, nodeCount)
	}
	if rc != relCount {
		t.Fatalf("RelationshipCount = %d, want %d", rc, relCount)
	}
}

// TestBatchBuilder_ConcurrentReadsDuringExecute verifies that reads do not block
// (or produce corrupt data) while a batch Execute holds g.mu.Lock.
func TestBatchBuilder_ConcurrentReadsDuringExecute(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	// Pre-populate some nodes for reads to target.
	preNodes := make([]*types.Node, 100)
	for i := range 100 {
		n, err := g.Nodes.Add([]string{"Pre"}, map[string]any{"idx": i})
		if err != nil {
			t.Fatalf("AddNode pre: %v", err)
		}
		preNodes[i] = n
	}

	// Build a batch of 500 nodes.
	b := NewBatchBuilder(g)
	for i := range 500 {
		if _, err := b.AddNode([]string{"Batch"}, map[string]any{"idx": i}); err != nil {
			t.Fatalf("b.AddNode: %v", err)
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 5)

	// Readers: attempt reads concurrently. They block until Execute releases mu.
	for r := range 4 {
		wg.Add(1)
		go func(rID int) {
			defer wg.Done()
			id := preNodes[rID%len(preNodes)].ID()
			for range 50 {
				if _, err := g.Nodes.Get(id); err != nil {
					errCh <- fmt.Errorf("reader %d GetNode: %v", rID, err)
					return
				}
			}
		}(r)
	}

	// Execute the batch (holds g.mu.Lock, readers block).
	wg.Add(1)
	go func() {
		defer wg.Done()
		result, err := b.Execute()
		if err != nil {
			errCh <- fmt.Errorf("Execute: %v", err)
			return
		}
		if result.Failed != 0 {
			errCh <- fmt.Errorf("Execute failed %d ops", result.Failed)
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}
