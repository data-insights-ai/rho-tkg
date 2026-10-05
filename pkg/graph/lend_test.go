package graph_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// lendsWithoutCopy reports whether a backend's Lend hands out the stored
// row itself (memory, badger, tiered) rather than a frozen copy (sharded).
func lendsWithoutCopy(b storeBackend) bool { return b.name != "sharded" }

// Lend returns the current row, frozen, equal to Get's copy; two lends of an
// unchanged entity return the same pointer where the store lends its row;
// a write leaves an earlier lent row as it was (two-phase: lent at w=1,
// written to w=2, the old row still says 1, a new lend says 2).
func TestLendNodeAndRelationship(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		layouts := [][2]string{{"A", "B"}}
		if b.tiered {
			layouts = append(layouts, [2]string{"Ref", "B"})
		}
		for _, layout := range layouts {
			t.Run(layout[0]+"->"+layout[1], func(t *testing.T) {
				start, err := g.Nodes().Add(ctx, []string{layout[0]}, map[string]any{"w": int64(1)})
				if err != nil {
					t.Fatal(err)
				}
				end, err := g.Nodes().Add(ctx, []string{layout[1]}, nil)
				if err != nil {
					t.Fatal(err)
				}
				rel, err := g.Rels().Add(ctx, "T", start, end, map[string]any{"w": int64(1)})
				if err != nil {
					t.Fatal(err)
				}

				n1, err := g.Nodes().Lend(ctx, start.ID())
				if err != nil {
					t.Fatalf("Nodes.Lend: %v", err)
				}
				r1, err := g.Rels().Lend(ctx, rel.ID())
				if err != nil {
					t.Fatalf("Rels.Lend: %v", err)
				}
				if !n1.IsFrozen() || !r1.IsFrozen() {
					t.Fatal("lent rows must be frozen")
				}
				if n1.ID() != start.ID() || r1.ID() != rel.ID() || r1.StartNodeID() != start.ID() || r1.EndNodeID() != end.ID() {
					t.Fatal("lent rows carry the wrong identity")
				}
				if err := n1.SetProperty("w", int64(7)); !errors.Is(err, types.ErrFrozenNode) {
					t.Fatalf("SetProperty on a lent node: %v, want ErrFrozenNode", err)
				}
				if err := r1.SetProperty("w", int64(7)); !errors.Is(err, types.ErrFrozenRelationship) {
					t.Fatalf("SetProperty on a lent relationship: %v, want ErrFrozenRelationship", err)
				}
				if thawed := n1.DeepCopy(); thawed.IsFrozen() || thawed.SetProperty("w", int64(7)) != nil {
					t.Fatal("DeepCopy of a lent node must be mutable")
				}
				if lendsWithoutCopy(b) {
					n2, _ := g.Nodes().Lend(ctx, start.ID())
					r2, _ := g.Rels().Lend(ctx, rel.ID())
					if n2 != n1 || r2 != r1 {
						t.Fatalf("%s: a second lend of an unchanged entity returned another object (a copy)", b.name)
					}
				}

				if err := g.Nodes().SetProperty(ctx, start.ID(), "w", int64(2)); err != nil {
					t.Fatal(err)
				}
				if err := g.Rels().SetProperty(ctx, rel.ID(), "w", int64(2)); err != nil {
					t.Fatal(err)
				}
				if v, _ := n1.GetProperty("w"); v != int64(1) {
					t.Fatalf("the earlier lent node changed under a write: w=%v, want 1", v)
				}
				if v, _ := r1.GetProperty("w"); v != int64(1) {
					t.Fatalf("the earlier lent relationship changed under a write: w=%v, want 1", v)
				}
				n3, err := g.Nodes().Lend(ctx, start.ID())
				if err != nil {
					t.Fatal(err)
				}
				r3, err := g.Rels().Lend(ctx, rel.ID())
				if err != nil {
					t.Fatal(err)
				}
				if v, _ := n3.GetProperty("w"); v != int64(2) {
					t.Fatalf("lend after the write: node w=%v, want 2", v)
				}
				if v, _ := r3.GetProperty("w"); v != int64(2) {
					t.Fatalf("lend after the write: relationship w=%v, want 2", v)
				}

				if err := g.Rels().Delete(ctx, rel.ID()); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Rels().Lend(ctx, rel.ID()); !errors.Is(err, graphpkg.ErrRelNotFound) {
					t.Fatalf("Rels.Lend after delete: %v, want ErrRelNotFound", err)
				}
				if err := g.Nodes().Delete(ctx, start.ID()); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Nodes().Lend(ctx, start.ID()); !errors.Is(err, graphpkg.ErrNodeNotFound) {
					t.Fatalf("Nodes.Lend after delete: %v, want ErrNodeNotFound", err)
				}
				if v, _ := n3.GetProperty("w"); v != int64(2) {
					t.Fatal("a lent row must stay readable after its entity is deleted")
				}
			})
		}
	})
}

func TestLendRejectsBadInput(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, _ storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		for _, id := range []int64{0, -1} {
			if _, err := g.Nodes().Lend(ctx, types.NodeID(id)); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("Nodes.Lend(%d): %v, want ErrInvalidStoreMutation", id, err)
			}
			if _, err := g.Rels().Lend(ctx, types.RelID(id)); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("Rels.Lend(%d): %v, want ErrInvalidStoreMutation", id, err)
			}
		}
		// Phantoms: IDs this graph minted (so local to every backend's
		// routing) that no current row has.
		phantomNode, phantomRel := g.Nodes().NextID(), g.Rels().NextID()
		if _, err := g.Nodes().Lend(ctx, phantomNode); !errors.Is(err, graphpkg.ErrNodeNotFound) {
			t.Fatalf("Nodes.Lend(phantom): %v, want ErrNodeNotFound", err)
		}
		if _, err := g.Rels().Lend(ctx, phantomRel); !errors.Is(err, graphpkg.ErrRelNotFound) {
			t.Fatalf("Rels.Lend(phantom): %v, want ErrRelNotFound", err)
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := g.Nodes().Lend(canceled, types.NodeID(1)); !errors.Is(err, context.Canceled) {
			t.Fatalf("Nodes.Lend(canceled): %v", err)
		}
		if _, err := g.Rels().Lend(canceled, types.RelID(1)); !errors.Is(err, context.Canceled) {
			t.Fatalf("Rels.Lend(canceled): %v", err)
		}
		n, err := g.Nodes().Add(ctx, []string{"A"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Nodes().Lend(ctx, n.ID()); !errors.Is(err, graphpkg.ErrGraphClosed) {
			t.Fatalf("Nodes.Lend after Close: %v, want ErrGraphClosed", err)
		}
		if _, err := g.Rels().Lend(ctx, types.RelID(1)); !errors.Is(err, graphpkg.ErrGraphClosed) {
			t.Fatalf("Rels.Lend after Close: %v, want ErrGraphClosed", err)
		}
	})
	var nilGraph *graphpkg.Graph
	if _, err := nilGraph.Nodes().Lend(context.Background(), 1); !errors.Is(err, graphpkg.ErrNilGraph) {
		t.Fatalf("nil graph Nodes.Lend: %v", err)
	}
}

// A store wrapper is not trusted to hand out its rows: Lend makes the frozen
// copy Get makes, so a wrapper's own GetNode override is honoured.
type getNodeOverride struct {
	*memory.Store
	gets int
}

func (s *getNodeOverride) GetNode(id types.NodeID) (*types.Node, error) {
	s.gets++
	return s.Store.GetNode(id)
}

func TestLendThroughAWrapperIsAFrozenCopy(t *testing.T) {
	st := &getNodeOverride{Store: memory.New()}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, Store: st})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()
	n, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	before := st.gets
	a, err := g.Nodes().Lend(ctx, n.ID())
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes().Lend(ctx, n.ID())
	if err != nil {
		t.Fatal(err)
	}
	if st.gets != before+2 {
		t.Fatalf("Lend through a wrapper must read through its GetNode: %d calls, want 2", st.gets-before)
	}
	if a == b || !a.IsFrozen() {
		t.Fatal("Lend through a wrapper must return a fresh frozen copy each time")
	}
}

// A relationship of a sealed segment (ADR-0011, memory) is decoded on lend.
func TestLendSealedSegmentRelationship(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 4, Store: memory.New(), RelSegments: []graphpkg.RelSegmentSpec{{
		Type:    "HOP",
		Columns: []graphpkg.SegmentColumn{{Name: "w", Kind: graphpkg.SegmentInt64}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()
	a, _ := g.Nodes().Add(ctx, []string{"A"}, nil)
	b, _ := g.Nodes().Add(ctx, []string{"A"}, nil)
	r, err := g.Rels().Add(ctx, "HOP", a, b, map[string]any{"w": int64(3)})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Admin().SealRelSegments("HOP"); err != nil {
		t.Fatal(err)
	}
	got, err := g.Rels().Lend(ctx, r.ID())
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := got.GetProperty("w"); v != int64(3) || !got.IsFrozen() || got.StartNodeID() != a.ID() {
		t.Fatalf("sealed lend = w %v frozen %v", v, got.IsFrozen())
	}
}

// Lent rows are read by many goroutines while writers replace them.
func TestLendConcurrentWithWrites(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, _ storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		a, _ := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"i": int64(0)})
		bnode, _ := g.Nodes().Add(ctx, []string{"A"}, nil)
		r, err := g.Rels().Add(ctx, "T", a, bnode, map[string]any{"i": int64(0)})
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					n, err := g.Nodes().Lend(ctx, a.ID())
					if err != nil {
						t.Error(err)
						return
					}
					rel, err := g.Rels().Lend(ctx, r.ID())
					if err != nil {
						t.Error(err)
						return
					}
					if _, ok := n.GetProperty("i"); !ok {
						t.Error("lent node lost its property")
					}
					if _, ok := rel.GetProperty("i"); !ok {
						t.Error("lent relationship lost its property")
					}
				}
			}()
		}
		for i := 1; i <= 50; i++ {
			if err := g.Nodes().SetProperty(ctx, a.ID(), "i", int64(i)); err != nil {
				t.Fatal(err)
			}
			if err := g.Rels().SetProperty(ctx, r.ID(), "i", int64(i)); err != nil {
				t.Fatal(err)
			}
		}
		wg.Wait()
	})
}

func BenchmarkNodeGetVersusLend(b *testing.B) {
	for _, backend := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{SnowflakeNodeID: 0}},
		{"badger", graphpkg.Config{SnowflakeNodeID: 0, BadgerInMemory: true}},
	} {
		b.Run(backend.name, func(b *testing.B) {
			g, err := graphpkg.New(backend.cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer g.Close()
			ctx := context.Background()
			props := map[string]any{"name": "a name", "age": int64(40), "city": "Linz", "score": 1.5}
			n, err := g.Nodes().Add(ctx, []string{"Person"}, props)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("Get", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := g.Nodes().Get(ctx, n.ID()); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("Lend", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := g.Nodes().Lend(ctx, n.ID()); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
