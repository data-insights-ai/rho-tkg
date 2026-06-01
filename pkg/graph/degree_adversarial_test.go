package graph_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	graphpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// assertDegreeMatches asserts IncomingDegree(hub, typ) == len(Incoming) == want.
func assertDegreeIn(t *testing.T, g *graphpkg.Graph, hub types.NodeID, typ string, want int) {
	t.Helper()
	d, err := g.Rels().IncomingDegree(hub, typ)
	if err != nil {
		t.Fatalf("IncomingDegree(%q): %v", typ, err)
	}
	rels, err := g.Rels().Incoming(hub, typ)
	if err != nil {
		t.Fatalf("Incoming(%q): %v", typ, err)
	}
	if d != want || d != len(rels) {
		t.Errorf("IncomingDegree(%q)=%d len(Incoming)=%d want %d", typ, d, len(rels), want)
	}
}

// TestRelDegreeChurn drives add/delete/re-add cycles across interleaved types
// and asserts the degree==len(materialized) invariant holds at every step.
func TestRelDegreeChurn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	hub, _ := g.Nodes().Add(ctx, []string{"H"}, nil)

	addKnows := func(n int) []types.RelID {
		ids := make([]types.RelID, 0, n)
		for i := 0; i < n; i++ {
			s, _ := g.Nodes().Add(ctx, []string{"S"}, nil)
			r, err := g.Rels().AddByID(ctx, "KNOWS", s.ID(), hub.ID(), nil)
			if err != nil {
				t.Fatalf("add KNOWS: %v", err)
			}
			ids = append(ids, r.ID())
		}
		return ids
	}

	knowsIDs := addKnows(10)
	assertDegreeIn(t, g, hub.ID(), "KNOWS", 10)

	// Delete 4 → 6 remain.
	for _, id := range knowsIDs[:4] {
		if err := g.Rels().Delete(ctx, id); err != nil {
			t.Fatalf("delete: %v", err)
		}
	}
	assertDegreeIn(t, g, hub.ID(), "KNOWS", 6)

	// Add a different type; KNOWS unchanged, total grows.
	for i := 0; i < 3; i++ {
		s, _ := g.Nodes().Add(ctx, []string{"S"}, nil)
		if _, err := g.Rels().AddByID(ctx, "FOLLOWS", s.ID(), hub.ID(), nil); err != nil {
			t.Fatalf("add FOLLOWS: %v", err)
		}
	}
	assertDegreeIn(t, g, hub.ID(), "KNOWS", 6)
	assertDegreeIn(t, g, hub.ID(), "FOLLOWS", 3)
	assertDegreeIn(t, g, hub.ID(), "", 9)

	// Re-add 2 KNOWS (delete must not leave the index permanently shrunk).
	addKnows(2)
	assertDegreeIn(t, g, hub.ID(), "KNOWS", 8)
	assertDegreeIn(t, g, hub.ID(), "", 11)

	// Delete every remaining edge → degree 0 (not an error).
	for _, typ := range []string{"KNOWS", "FOLLOWS"} {
		rels, _ := g.Rels().Incoming(hub.ID(), typ)
		for _, r := range rels {
			if err := g.Rels().Delete(ctx, r.ID()); err != nil {
				t.Fatalf("delete %s: %v", typ, err)
			}
		}
	}
	assertDegreeIn(t, g, hub.ID(), "", 0)
	if d, err := g.Rels().IncomingDegree(hub.ID(), "KNOWS"); err != nil || d != 0 {
		t.Errorf("drained IncomingDegree(KNOWS) = (%d,%v), want (0,nil)", d, err)
	}
}

// TestRelDegreeSelfLoopRejected verifies a rejected self-loop add does not leak
// into the degree counters (the failed write must not touch the adjacency index).
func TestRelDegreeSelfLoopRejected(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	n, _ := g.Nodes().Add(ctx, []string{"N"}, nil)
	if _, err := g.Rels().AddByID(ctx, "SELF", n.ID(), n.ID(), nil); !errors.Is(err, graphpkg.ErrSelfLoop) {
		t.Fatalf("self-loop add err = %v, want ErrSelfLoop", err)
	}
	// The rejected add must leave both degrees at zero.
	if d, err := g.Rels().IncomingDegree(n.ID(), ""); err != nil || d != 0 {
		t.Errorf("IncomingDegree after rejected self-loop = (%d,%v), want (0,nil)", d, err)
	}
	if d, err := g.Rels().OutgoingDegree(n.ID(), ""); err != nil || d != 0 {
		t.Errorf("OutgoingDegree after rejected self-loop = (%d,%v), want (0,nil)", d, err)
	}
}

// TestRelDegreeMissingNode asserts degree on a deleted/nonexistent node returns
// ErrNodeNotFound (matching the adjacency contract), not a silent zero.
func TestRelDegreeMissingNode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	n, _ := g.Nodes().Add(ctx, []string{"N"}, nil)
	id := n.ID()
	if err := g.Nodes().Delete(ctx, id); err != nil {
		t.Fatalf("delete node: %v", err)
	}
	if _, err := g.Rels().IncomingDegree(id, ""); !errors.Is(err, graphpkg.ErrNodeNotFound) {
		t.Errorf("IncomingDegree(deleted) err = %v, want ErrNodeNotFound", err)
	}
	if _, err := g.Rels().OutgoingDegree(id, "KNOWS"); !errors.Is(err, graphpkg.ErrNodeNotFound) {
		t.Errorf("OutgoingDegree(deleted) err = %v, want ErrNodeNotFound", err)
	}
}

// TestRelDegreeConcurrent hammers a hub with concurrent adds, deletes, and
// degree reads. Run with -race; the final degree must be exact.
func TestRelDegreeConcurrent(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	hub, _ := g.Nodes().Add(ctx, []string{"H"}, nil)
	const n = 200

	// Concurrent adds, collecting rel IDs.
	var mu sync.Mutex
	ids := make([]types.RelID, 0, n)
	stop := make(chan struct{})

	// Reader goroutines exercise concurrent degree reads during writes.
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = g.Rels().IncomingDegree(hub.ID(), "E")
				}
			}
		}()
	}

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			s, err := g.Nodes().Add(ctx, []string{"S"}, nil)
			if err != nil {
				return
			}
			r, err := g.Rels().AddByID(ctx, "E", s.ID(), hub.ID(), nil)
			if err != nil {
				return
			}
			mu.Lock()
			ids = append(ids, r.ID())
			mu.Unlock()
		}()
	}
	wg.Wait()

	if got, err := g.Rels().IncomingDegree(hub.ID(), "E"); err != nil || got != n {
		t.Fatalf("after concurrent add IncomingDegree = (%d,%v), want %d", got, err, n)
	}

	// Concurrent deletes of the first half.
	half := n / 2
	var dwg sync.WaitGroup
	dwg.Add(half)
	for i := 0; i < half; i++ {
		id := ids[i]
		go func() {
			defer dwg.Done()
			_ = g.Rels().Delete(ctx, id)
		}()
	}
	dwg.Wait()
	close(stop)
	readers.Wait()

	want := n - half
	got, err := g.Rels().IncomingDegree(hub.ID(), "E")
	if err != nil || got != want {
		t.Fatalf("after concurrent delete IncomingDegree = (%d,%v), want %d", got, err, want)
	}
	rels, _ := g.Rels().Incoming(hub.ID(), "E")
	if len(rels) != want {
		t.Errorf("degree %d != len(Incoming) %d after churn", got, len(rels))
	}
}
