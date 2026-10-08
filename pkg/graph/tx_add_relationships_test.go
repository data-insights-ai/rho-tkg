package graph_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	temporalpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func relsAdded(t *testing.T, g *graphpkg.Graph) int64 {
	t.Helper()
	st, err := g.Stats().Get()
	if err != nil {
		t.Fatal(err)
	}
	return st.RelsAdded
}

// txRelEndpoints creates n nodes outside any transaction. On tiered every
// second node is a reference node, so half the relationships cross shards.
func txRelEndpoints(t *testing.T, b storeBackend, g *graphpkg.Graph, n int) []*types.Node {
	t.Helper()
	nodes := make([]*types.Node, n)
	for i := range nodes {
		label := "P"
		if b.tiered && i%2 == 1 {
			label = "Ref"
		}
		nd, err := g.Nodes().Add(context.Background(), []string{label}, map[string]any{"i": int64(i)})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = nd
	}
	return nodes
}

// txRelSpecs links nodes[i] to nodes[(i+1) % len] once per element.
func txRelSpecs(nodes []*types.Node, n int) []graphpkg.RelCreate {
	specs := make([]graphpkg.RelCreate, n)
	for i := range specs {
		specs[i] = graphpkg.RelCreate{
			StartID: nodes[i%len(nodes)].ID(),
			EndID:   nodes[(i+1)%len(nodes)].ID(),
			Props:   map[string]any{"i": int64(i), "w": float64(i) / 2},
		}
	}
	return specs
}

// AddRelationships creates one relationship per element, in order, with
// AddRelationshipByID's shape: the type, endpoints and properties, its own ID
// and hash chain, the endpoints' hashes, a strictly increasing transaction
// time, a create event each after commit, the graph's add counter; visible in
// both adjacency directions and the type scan inside the transaction.
// Relationships of another type are untouched.
func TestGraphTxAddRelationships(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		nodes := txRelEndpoints(t, b, g, 4)
		other, err := g.Rels().AddByID(ctx, "OTHER", nodes[0].ID(), nodes[1].ID(), nil)
		if err != nil {
			t.Fatal(err)
		}
		var mu sync.Mutex
		var created []types.EntityID
		bus := events.NewEventBus()
		bus.Subscribe(func(e events.Event) {
			if e.Type == events.EventRelCreate {
				mu.Lock()
				created = append(created, e.EntityID)
				mu.Unlock()
			}
		})
		if err := g.Events().SetSync(bus); err != nil {
			t.Fatal(err)
		}
		before := relsAdded(t, g)

		specs := txRelSpecs(nodes, 5)
		specs[2].Props["tkg_valid_from"] = types.Instant(1000)
		var rels []*types.Relationship
		err = g.Tx().Run(func(tx *graphpkg.GraphTx) error {
			var err error
			rels, err = tx.AddRelationships("KNOWS", specs)
			if err != nil {
				return err
			}
			// Visible inside the transaction, both directions and by type.
			out, err := tx.OutgoingRels(nodes[0].ID(), "KNOWS")
			if err != nil || len(out) != 2 {
				t.Fatalf("outgoing KNOWS of node 0 inside the tx: %d, %v; want 2", len(out), err)
			}
			in, err := tx.IncomingRels(nodes[1].ID(), "KNOWS")
			if err != nil || len(in) != 2 {
				t.Fatalf("incoming KNOWS of node 1 inside the tx: %d, %v; want 2", len(in), err)
			}
			byType, err := g.Rels().ByType("KNOWS", storepkg.QueryOpts{})
			if err != nil || len(byType) != 5 {
				t.Fatalf("KNOWS by type inside the tx: %d, %v; want 5", len(byType), err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(rels) != 5 {
			t.Fatalf("%d relationships, want 5", len(rels))
		}
		seen := map[types.RelID]bool{other.ID(): true}
		var lastTx types.Instant
		for i, r := range rels {
			if seen[r.ID()] {
				t.Fatalf("relationship %d reuses an ID", i)
			}
			seen[r.ID()] = true
			got, err := g.Rels().Get(ctx, r.ID())
			if err != nil {
				t.Fatalf("relationship %d after commit: %v", i, err)
			}
			if got.StartNodeID() != specs[i].StartID || got.EndNodeID() != specs[i].EndID {
				t.Fatalf("relationship %d endpoints %d->%d, want %d->%d", i, got.StartNodeID(), got.EndNodeID(), specs[i].StartID, specs[i].EndID)
			}
			if typ := g.Rels().Type(got); typ != "KNOWS" {
				t.Fatalf("relationship %d type %q", i, typ)
			}
			if v, _ := got.GetProperty("i"); v != int64(i) {
				t.Fatalf("relationship %d: i = %v, want %d (rels' order)", i, v, i)
			}
			if _, ok := got.GetProperty("tkg_valid_from"); ok && i != 2 {
				t.Fatalf("relationship %d carries a stored shadow key", i)
			}
			ok, err := g.Hash().VerifyRelChain(r.ID())
			if err != nil || !ok {
				t.Fatalf("relationship %d hash chain: %v, %v", i, ok, err)
			}
			start, _ := g.Nodes().Get(ctx, specs[i].StartID)
			end, _ := g.Nodes().Get(ctx, specs[i].EndID)
			ig := got.Integrity()
			if ig == nil || ig.FromNodeHash != start.Integrity().Hash || ig.ToNodeHash != end.Integrity().Hash {
				t.Fatalf("relationship %d endpoint hashes %+v", i, ig)
			}
			txFrom := got.Temporal().TxFrom
			if txFrom <= lastTx {
				t.Fatalf("relationship %d TxFrom %d not after %d", i, txFrom, lastTx)
			}
			lastTx = txFrom
		}
		if vf := rels[2].Temporal().ValidFrom; vf != 1000 {
			t.Fatalf("relationship 2 ValidFrom = %d, want 1000", vf)
		}
		if vf := rels[1].Temporal().ValidFrom; vf != 0 {
			t.Fatalf("relationship 1 ValidFrom = %d, want unset", vf)
		}
		if others, _ := g.Rels().ByType("OTHER", storepkg.QueryOpts{}); len(others) != 1 {
			t.Fatalf("OTHER relationships: %d, want 1", len(others))
		}
		if got := relsAdded(t, g) - before; got != 5 {
			t.Fatalf("RelsAdded moved by %d, want 5", got)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(created) != 5 {
			t.Fatalf("%d create events, want 5", len(created))
		}
		for i, r := range rels {
			if created[i] != types.EntityID(r.ID()) {
				t.Fatalf("event %d names %d, want %d", i, created[i], r.ID())
			}
		}
	})
}

// The endpoint hashes AddRelationships stores are the ones AddRelationshipByID
// stores for the same endpoints (the content hash covers the relationship's own
// ID, so it differs; TestGraphTxAddRelationships verifies each chain), and the
// returned rows behave like the single door's.
func TestGraphTxAddRelationshipsMatchesAddRelationshipByID(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		nodes := txRelEndpoints(t, b, g, 3)
		specs := txRelSpecs(nodes, 3)
		var one, many []*types.Relationship
		err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
			for _, s := range specs {
				r, err := tx.AddRelationshipByID("T", s.StartID, s.EndID, s.Props)
				if err != nil {
					return err
				}
				one = append(one, r)
			}
			var err error
			many, err = tx.AddRelationships("T", specs)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		for i := range specs {
			a, c := one[i].Integrity(), many[i].Integrity()
			if a.FromNodeHash != c.FromNodeHash || a.ToNodeHash != c.ToNodeHash || a.PrevHash != c.PrevHash {
				t.Fatalf("relationship %d: AddRelationshipByID %+v, AddRelationships %+v", i, *a, *c)
			}
			if one[i].IsFrozen() != many[i].IsFrozen() {
				t.Fatalf("relationship %d: frozen %v against the single door's %v", i, many[i].IsFrozen(), one[i].IsFrozen())
			}
		}
	})
}

// Rollback removes every relationship AddRelationships created and the type it
// introduced; the counter returns to its value at Begin; the transaction is
// done afterwards.
func TestGraphTxAddRelationshipsRollback(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		nodes := txRelEndpoints(t, b, g, 3)
		keep, err := g.Rels().AddByID(ctx, "KEPT", nodes[0].ID(), nodes[1].ID(), nil)
		if err != nil {
			t.Fatal(err)
		}
		before := relsAdded(t, g)
		tx, err := g.Tx().Begin()
		if err != nil {
			t.Fatal(err)
		}
		rels, err := tx.AddRelationships("FRESH", txRelSpecs(nodes, 4))
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		for i, r := range rels {
			if _, err := g.Rels().Get(ctx, r.ID()); !errors.Is(err, graphpkg.ErrRelNotFound) {
				t.Fatalf("relationship %d after rollback: %v, want ErrRelNotFound", i, err)
			}
		}
		if _, err := g.Rels().Get(ctx, keep.ID()); err != nil {
			t.Fatalf("a relationship from before the transaction: %v", err)
		}
		if fresh, _ := g.Rels().ByType("FRESH", storepkg.QueryOpts{}); len(fresh) != 0 {
			t.Fatalf("%d FRESH relationships after rollback", len(fresh))
		}
		if out, _ := g.Rels().Outgoing(nodes[0].ID(), ""); len(out) != 1 {
			t.Fatalf("node 0 has %d outgoing relationships after rollback, want 1", len(out))
		}
		if got := relsAdded(t, g); got != before {
			t.Fatalf("RelsAdded %d after rollback, want %d", got, before)
		}
		if _, err := tx.AddRelationships("FRESH", txRelSpecs(nodes, 1)); !errors.Is(err, graphpkg.ErrTxDone) {
			t.Fatalf("AddRelationships after rollback: %v, want ErrTxDone", err)
		}
	})
}

// A bad element fails the whole call before any relationship exists: an
// invalid property value, a reserved key, a missing endpoint, a non-positive
// endpoint ID, a malformed type name. Nothing is created (the type registry's
// rollback is checked in core, TestAddRelationshipsInternalRestoresType).
func TestGraphTxAddRelationshipsRejectsBeforeCreating(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		nodes := txRelEndpoints(t, b, g, 3)
		ghost := g.Nodes().NextID() // minted, never stored
		for _, tc := range []struct {
			name     string
			typeName string
			bad      graphpkg.RelCreate
			want     error
		}{
			{"value", "BAD", graphpkg.RelCreate{StartID: nodes[0].ID(), EndID: nodes[1].ID(), Props: map[string]any{"v": struct{}{}}}, nil},
			{"reserved key", "BAD", graphpkg.RelCreate{StartID: nodes[0].ID(), EndID: nodes[1].ID(), Props: map[string]any{"tkg_x": int64(1)}}, types.ErrReservedPrefix},
			{"missing endpoint", "BAD", graphpkg.RelCreate{StartID: nodes[0].ID(), EndID: ghost}, graphpkg.ErrNodeNotFound},
			{"zero endpoint", "BAD", graphpkg.RelCreate{StartID: 0, EndID: nodes[1].ID()}, storepkg.ErrInvalidStoreMutation},
			{"empty type", "", graphpkg.RelCreate{StartID: nodes[0].ID(), EndID: nodes[1].ID()}, nil},
		} {
			t.Run(tc.name, func(t *testing.T) {
				specs := txRelSpecs(nodes, 3)
				specs[1] = tc.bad
				err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
					rels, err := tx.AddRelationships(tc.typeName, specs)
					if rels != nil {
						t.Fatalf("%d relationships returned with the error", len(rels))
					}
					return err
				})
				if err == nil {
					t.Fatal("AddRelationships accepted a bad element")
				}
				if tc.want != nil && !errors.Is(err, tc.want) {
					t.Fatalf("err = %v, want %v", err, tc.want)
				}
				if n, _ := g.Rels().Count(); n != 0 {
					t.Fatalf("%d relationships exist after the refusal", n)
				}
			})
		}
	})
}

// A self-loop is refused unless the graph allows self-loops, before anything
// is created.
func TestGraphTxAddRelationshipsSelfLoop(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	n, err := g.Nodes().Add(context.Background(), []string{"P"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	m, err := g.Nodes().Add(context.Background(), []string{"P"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		_, err := tx.AddRelationships("LOOP", []graphpkg.RelCreate{{StartID: n.ID(), EndID: m.ID()}, {StartID: n.ID(), EndID: n.ID()}})
		return err
	})
	if !errors.Is(err, graphpkg.ErrSelfLoop) {
		t.Fatalf("err = %v, want ErrSelfLoop", err)
	}
	if c, _ := g.Rels().Count(); c != 0 {
		t.Fatalf("%d relationships after the refusal", c)
	}
}

// With a temporal constraint the call takes AddRelationshipByID's path per
// element: a relationship outside its endpoints' validity is refused at that
// element, the earlier ones exist until the transaction rolls back.
func TestGraphTxAddRelationshipsConstraint(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		if err := g.Constraints().Add(temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints}); err != nil {
			t.Fatal(err)
		}
		var nodes []*types.Node
		for range 2 {
			n, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"tkg_valid_from": types.Instant(1000)})
			if err != nil {
				t.Fatal(err)
			}
			nodes = append(nodes, n)
		}
		specs := []graphpkg.RelCreate{
			{StartID: nodes[0].ID(), EndID: nodes[1].ID(), Props: map[string]any{"tkg_valid_from": types.Instant(2000)}},
			{StartID: nodes[1].ID(), EndID: nodes[0].ID(), Props: map[string]any{"tkg_valid_from": types.Instant(3000)}},
			{StartID: nodes[0].ID(), EndID: nodes[1].ID(), Props: map[string]any{"tkg_valid_from": types.Instant(500)}},
		}
		tx, err := g.Tx().Begin()
		if err != nil {
			t.Fatal(err)
		}
		rels, err := tx.AddRelationships("T", specs)
		if !errors.Is(err, temporalpkg.ErrTemporalConstraint) {
			t.Fatalf("err = %v, want ErrTemporalConstraint", err)
		}
		if len(rels) != 2 {
			t.Fatalf("%d relationships before the violation, want 2", len(rels))
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if c, _ := g.Rels().Count(); c != 0 {
			t.Fatalf("%d relationships after rollback", c)
		}
		err = g.Tx().Run(func(tx *graphpkg.GraphTx) error {
			_, err := tx.AddRelationships("T", specs[:2])
			return err
		})
		if err != nil {
			t.Fatalf("valid elements: %v", err)
		}
		if c, _ := g.Rels().Count(); c != 2 {
			t.Fatalf("%d relationships, want 2", c)
		}
	})
}

// An empty call creates nothing and is not an error.
func TestGraphTxAddRelationshipsEmpty(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	err = g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		rels, err := tx.AddRelationships("T", nil)
		if rels != nil || err != nil {
			t.Fatalf("empty call: %v, %v", rels, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
