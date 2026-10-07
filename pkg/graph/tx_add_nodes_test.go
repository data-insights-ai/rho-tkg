package graph_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func nodesAdded(t *testing.T, g *graphpkg.Graph) int64 {
	t.Helper()
	st, err := g.Stats().Get()
	if err != nil {
		t.Fatal(err)
	}
	return st.NodesAdded
}

func txAddNodesProps(n int) []map[string]any {
	props := make([]map[string]any, n)
	for i := range props {
		props[i] = map[string]any{"i": int64(i), "name": "n"}
	}
	return props
}

// AddNodes creates one node per element, in order, with AddNode's shape: its
// labels and properties, its own ID and hash chain, a strictly increasing
// transaction time, a create event each after commit, the graph's add counter.
// Nodes created before the call and a node of another label are untouched.
func TestGraphTxAddNodes(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		var mu sync.Mutex
		var created []types.EntityID
		bus := events.NewEventBus()
		bus.Subscribe(func(e events.Event) {
			if e.Type == events.EventNodeCreate {
				mu.Lock()
				created = append(created, e.EntityID)
				mu.Unlock()
			}
		})
		if err := g.Events().SetSync(bus); err != nil {
			t.Fatal(err)
		}
		other, err := g.Nodes().Add(ctx, []string{"Other"}, map[string]any{"i": int64(0)})
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		created = nil
		mu.Unlock()
		before := nodesAdded(t, g)

		props := txAddNodesProps(5)
		props[2]["tkg_valid_from"] = types.Instant(1000)
		var nodes []*types.Node
		err = g.Tx().Run(func(tx *graphpkg.GraphTx) error {
			var err error
			nodes, err = tx.AddNodes([]string{"Person", "Agent"}, props)
			if err != nil {
				return err
			}
			// Visible inside the transaction.
			n, err := tx.GetNode(nodes[4].ID())
			if err != nil || n == nil {
				t.Fatalf("GetNode inside the tx: %v", err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(nodes) != 5 {
			t.Fatalf("%d nodes, want 5", len(nodes))
		}
		seen := map[types.NodeID]bool{other.ID(): true}
		var lastTx types.Instant
		for i, n := range nodes {
			if seen[n.ID()] {
				t.Fatalf("node %d reuses an ID", i)
			}
			seen[n.ID()] = true
			got, err := g.Nodes().Get(ctx, n.ID())
			if err != nil {
				t.Fatalf("node %d after commit: %v", i, err)
			}
			if v, _ := got.GetProperty("i"); v != int64(i) {
				t.Fatalf("node %d: i = %v, want %d (props' order)", i, v, i)
			}
			if _, ok := got.GetProperty("tkg_valid_from"); ok && i != 2 {
				t.Fatalf("node %d carries a stored shadow key", i)
			}
			labels := g.Nodes().Labels(got)
			if !slices.Equal(labels, []string{"Person", "Agent"}) {
				t.Fatalf("node %d labels %v", i, labels)
			}
			ok, err := g.Hash().VerifyNodeChain(n.ID())
			if err != nil || !ok {
				t.Fatalf("node %d hash chain: %v, %v", i, ok, err)
			}
			txFrom := got.Temporal().TxFrom
			if txFrom <= lastTx {
				t.Fatalf("node %d TxFrom %d not after %d", i, txFrom, lastTx)
			}
			lastTx = txFrom
		}
		if vf := nodes[2].Temporal().ValidFrom; vf != 1000 {
			t.Fatalf("node 2 ValidFrom = %d, want 1000", vf)
		}
		if vf := nodes[1].Temporal().ValidFrom; vf != 0 {
			t.Fatalf("node 1 ValidFrom = %d, want unset", vf)
		}
		people, err := g.Nodes().ByLabel("Person", storepkg.QueryOpts{})
		if err != nil || len(people) != 5 {
			t.Fatalf("Person nodes: %d, %v; want 5", len(people), err)
		}
		if others, _ := g.Nodes().ByLabel("Other", storepkg.QueryOpts{}); len(others) != 1 {
			t.Fatalf("Other nodes: %d, want 1", len(others))
		}
		if got := nodesAdded(t, g) - before; got != 5 {
			t.Fatalf("NodesAdded moved by %d, want 5", got)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(created) != 5 {
			t.Fatalf("%d create events, want 5", len(created))
		}
		for i, n := range nodes {
			if created[i] != types.EntityID(n.ID()) {
				t.Fatalf("event %d names %d, want %d", i, created[i], n.ID())
			}
		}
	})
}

// Rollback removes every node AddNodes created, and the label it introduced;
// the counter returns to its value at Begin.
func TestGraphTxAddNodesRollback(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		keep, err := g.Nodes().Add(ctx, []string{"Kept"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		before := nodesAdded(t, g)
		tx, err := g.Tx().Begin()
		if err != nil {
			t.Fatal(err)
		}
		nodes, err := tx.AddNodes([]string{"Fresh"}, txAddNodesProps(4))
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		for i, n := range nodes {
			if _, err := g.Nodes().Get(ctx, n.ID()); !errors.Is(err, graphpkg.ErrNodeNotFound) {
				t.Fatalf("node %d after rollback: %v, want ErrNodeNotFound", i, err)
			}
		}
		if _, err := g.Nodes().Get(ctx, keep.ID()); err != nil {
			t.Fatalf("a node from before the transaction: %v", err)
		}
		if fresh, _ := g.Nodes().ByLabel("Fresh", storepkg.QueryOpts{}); len(fresh) != 0 {
			t.Fatalf("%d Fresh nodes after rollback", len(fresh))
		}
		if got := nodesAdded(t, g); got != before {
			t.Fatalf("NodesAdded %d after rollback, want %d", got, before)
		}
		if _, err := tx.AddNodes([]string{"Fresh"}, txAddNodesProps(1)); !errors.Is(err, graphpkg.ErrTxDone) {
			t.Fatalf("AddNodes after rollback: %v, want ErrTxDone", err)
		}
	})
}

// A bad element fails the whole call before any node exists: an invalid
// property value, a reserved key, a bad label. Nothing is created (the label
// registry's rollback is checked in core, TestAddNodesInternalRestoresLabels).
func TestGraphTxAddNodesRejectsBeforeCreating(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		for _, tc := range []struct {
			name   string
			labels []string
			bad    map[string]any
			want   error
		}{
			{"value", []string{"Bad"}, map[string]any{"v": struct{}{}}, nil},
			{"reserved key", []string{"Bad"}, map[string]any{"tkg_x": int64(1)}, types.ErrReservedPrefix},
			{"empty label", []string{""}, nil, nil},
		} {
			t.Run(tc.name, func(t *testing.T) {
				props := txAddNodesProps(3)
				if tc.bad != nil {
					props[1] = tc.bad
				}
				err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
					nodes, err := tx.AddNodes(tc.labels, props)
					if nodes != nil {
						t.Fatalf("%d nodes returned with the error", len(nodes))
					}
					return err
				})
				if err == nil {
					t.Fatal("AddNodes accepted a bad element")
				}
				if tc.want != nil && !errors.Is(err, tc.want) {
					t.Fatalf("err = %v, want %v", err, tc.want)
				}
				if n, _ := g.Nodes().Count(); n != 0 {
					t.Fatalf("%d nodes exist after the refusal", n)
				}
			})
		}
	})
}

// With a unique constraint the call takes AddNode's path per element: a value
// repeated within the call or taken before is refused with ErrUniqueViolation
// at that element, the earlier ones exist until the transaction rolls back.
func TestGraphTxAddNodesUniqueConstraint(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		label := "User"
		if b.tiered {
			label = "Ref" // tiered keeps unique constraints on reference labels
		}
		if err := g.Constraints().CreateUnique(ctx, label, "email"); err != nil {
			if errors.Is(err, storepkg.ErrCapabilityNotSupported) {
				t.Skip("no unique constraints on this backend")
			}
			t.Fatal(err)
		}
		props := []map[string]any{{"email": "a@x"}, {"email": "b@x"}, {"email": "a@x"}}
		tx, err := g.Tx().Begin()
		if err != nil {
			t.Fatal(err)
		}
		nodes, err := tx.AddNodes([]string{label}, props)
		if !errors.Is(err, graphpkg.ErrUniqueViolation) {
			t.Fatalf("err = %v, want ErrUniqueViolation", err)
		}
		if len(nodes) != 2 {
			t.Fatalf("%d nodes before the violation, want 2", len(nodes))
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if n, _ := g.Nodes().Count(); n != 0 {
			t.Fatalf("%d nodes after rollback", n)
		}
		ok := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
			_, err := tx.AddNodes([]string{label}, props[:2])
			return err
		})
		if ok != nil {
			t.Fatalf("distinct values: %v", ok)
		}
		if users, _ := g.Nodes().ByLabel(label, storepkg.QueryOpts{}); len(users) != 2 {
			t.Fatalf("%d users, want 2", len(users))
		}
	})
}

// An empty call creates nothing and is not an error.
func TestGraphTxAddNodesEmpty(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	err = g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		nodes, err := tx.AddNodes([]string{"P"}, nil)
		if nodes != nil || err != nil {
			t.Fatalf("empty call: %v, %v", nodes, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := g.Nodes().Count(); n != 0 {
		t.Fatalf("%d nodes", n)
	}
}
