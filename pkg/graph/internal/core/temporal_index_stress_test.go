package core

// BatchBuilder under load: a mixed node and relationship batch persists every
// operation, and reads running across Execute see the batch whole or not at all.

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- BatchBuilder at scale ---

// TestBatchBuilder_NodesAndRelationships verifies that a batch containing both
// nodes and relationships is executed correctly.
func TestBatchBuilder_NodesAndRelationships(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	const (
		nodeCount = 200
		relCount  = 400
	)

	b, _ := NewBatchBuilder(g)
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

// TestBatchBuilder_ConcurrentReadsDuringExecute: readers that are already
// reading when Execute starts, and keep reading until it returns, read the
// pre-existing nodes with their own values and see the batch either not at
// all or whole (CountByLabel("Batch") is 0 or batchSize, never a part).
func TestBatchBuilder_ConcurrentReadsDuringExecute(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	const preCount, batchSize, readers = 20, 100, 4
	preNodes := make([]*types.Node, preCount)
	for i := range preCount {
		n, err := g.Nodes.Add(context.Background(), []string{"Pre"}, map[string]any{"idx": int64(i)})
		if err != nil {
			t.Fatalf("AddNode pre: %v", err)
		}
		preNodes[i] = n
	}

	b, _ := NewBatchBuilder(g)
	for i := range batchSize {
		if _, err := b.AddNode([]string{"Batch"}, map[string]any{"idx": int64(i)}); err != nil {
			t.Fatalf("b.AddNode: %v", err)
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, readers)
	started := make(chan struct{}, readers)
	executed := make(chan struct{})

	read := func(rID int) error {
		want := rID % preCount
		n, err := g.Nodes.Get(context.Background(), preNodes[want].ID())
		if err != nil {
			return fmt.Errorf("reader %d GetNode: %v", rID, err)
		}
		if v, _ := n.GetProperty("idx"); v != int64(want) {
			return fmt.Errorf("reader %d GetNode idx = %v (%T), want %d", rID, v, v, want)
		}
		c, err := g.Nodes.CountByLabel("Batch")
		if err != nil {
			return fmt.Errorf("reader %d CountByLabel: %v", rID, err)
		}
		if c != 0 && c != batchSize {
			return fmt.Errorf("reader %d saw a partial batch: %d of %d nodes", rID, c, batchSize)
		}
		return nil
	}
	for r := range readers {
		wg.Add(1)
		go func(rID int) {
			defer wg.Done()
			for first := true; ; first = false {
				if err := read(rID); err != nil {
					errCh <- err
					if first {
						started <- struct{}{}
					}
					return
				}
				if first {
					started <- struct{}{}
				}
				select {
				case <-executed:
					return
				default:
				}
			}
		}(r)
	}

	// Execute only once every reader is inside its read loop, so the reads
	// overlap it; readers stop after it returns.
	for range readers {
		<-started
	}
	result, err := b.Execute()
	close(executed)
	wg.Wait()
	close(errCh)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Failed != 0 || result.Created != batchSize {
		t.Fatalf("Execute: Created = %d, Failed = %d, want %d and 0", result.Created, result.Failed, batchSize)
	}
	for err := range errCh {
		t.Error(err)
	}
	if c, err := g.Nodes.CountByLabel("Batch"); err != nil || c != batchSize {
		t.Fatalf("CountByLabel(Batch) after Execute = %d, %v; want %d", c, err, batchSize)
	}
}

// TestBatchBuilder_LargeNodeBatch verifies that a batch of 2000 nodes is fully
// persisted with zero failures and all nodes are retrievable afterwards.
func TestBatchBuilder_LargeNodeBatch(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	g := newTestGraph(t)

	const count = 2000
	b, _ := NewBatchBuilder(g)
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
