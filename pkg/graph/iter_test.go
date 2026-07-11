package graph_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Go 1.23+ range-over-func iterator probes . Every probe runs against
// BOTH in-tree stores — memory and badger — through the same core routing;
// Iter/OutgoingIter/IncomingIter wrap the existing ForEach machinery with no
// new scan paths, so parity with ForEach/All is the primary correctness
// contract.

func iterBackends(t *testing.T, run func(t *testing.T, g *graph.Graph)) {
	t.Helper()
	backends := []struct {
		name string
		cfg  graph.Config
	}{
		{name: "memory", cfg: graph.Config{SnowflakeNodeID: 3}},
		{name: "badger", cfg: graph.Config{SnowflakeNodeID: 3, BadgerInMemory: true, CacheCapacity: 8}},
	}
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			g, err := graph.New(b.cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })
			run(t, g)
		})
	}
}

// seedIterNodes creates n nodes split across two labels (mixed-label
// adversarial shape) and returns them in creation order.
func seedIterNodes(t *testing.T, g *graph.Graph, n int) []*types.Node {
	t.Helper()
	ctx := context.Background()
	nodes := make([]*types.Node, n)
	for i := 0; i < n; i++ {
		label := "Person"
		if i%3 == 0 {
			label = "Org"
		}
		nd, err := g.Nodes().Add(ctx, []string{label}, map[string]any{"idx": int64(i)})
		if err != nil {
			t.Fatalf("Add node %d: %v", i, err)
		}
		nodes[i] = nd
	}
	return nodes
}

// seedIterRels creates n relationships split across two types between a
// small pool of endpoint nodes (mixed-type adversarial shape).
func seedIterRels(t *testing.T, g *graph.Graph, n int) (endpoints []*types.Node, rels []*types.Relationship) {
	t.Helper()
	ctx := context.Background()
	endpoints = make([]*types.Node, 5)
	for i := range endpoints {
		nd, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"i": int64(i)})
		if err != nil {
			t.Fatalf("Add endpoint %d: %v", i, err)
		}
		endpoints[i] = nd
	}
	rels = make([]*types.Relationship, n)
	for i := 0; i < n; i++ {
		typeName := "KNOWS"
		if i%3 == 0 {
			typeName = "LIKES"
		}
		start := endpoints[i%len(endpoints)]
		end := endpoints[(i+1)%len(endpoints)]
		r, err := g.Rels().Add(ctx, typeName, start, end, map[string]any{"idx": int64(i)})
		if err != nil {
			t.Fatalf("Add rel %d: %v", i, err)
		}
		rels[i] = r
	}
	return endpoints, rels
}

func nodeIDSet(nodes []*types.Node) map[types.NodeID]bool {
	set := make(map[types.NodeID]bool, len(nodes))
	for _, n := range nodes {
		set[n.ID()] = true
	}
	return set
}

func relIDSet(rels []*types.Relationship) map[types.RelID]bool {
	set := make(map[types.RelID]bool, len(rels))
	for _, r := range rels {
		set[r.ID()] = true
	}
	return set
}

// TestIter_NodeParity pins that Nodes().Iter's collected ID set equals both
// ForEach's and All's, over ~200 mixed-label nodes on both backends.
func TestIter_NodeParity(t *testing.T) {
	iterBackends(t, func(t *testing.T, g *graph.Graph) {
		seeded := seedIterNodes(t, g, 200)
		want := nodeIDSet(seeded)

		all, err := g.Nodes().All(graph.QueryOpts{})
		if err != nil {
			t.Fatalf("All: %v", err)
		}
		if len(all) != len(want) {
			t.Fatalf("All returned %d rows, want %d", len(all), len(want))
		}

		var forEachIDs []types.NodeID
		if err := g.Nodes().ForEach(graph.QueryOpts{}, func(n *types.Node) bool {
			forEachIDs = append(forEachIDs, n.ID())
			return true
		}); err != nil {
			t.Fatalf("ForEach: %v", err)
		}
		if len(forEachIDs) != len(want) {
			t.Fatalf("ForEach returned %d rows, want %d", len(forEachIDs), len(want))
		}

		// Compared as SETS, not ordered sequences: the plain (unindexed,
		// non-temporal, unpaginated) scan primitive underlying ForEach makes
		// no ordering guarantee across independent calls (e.g. the in-memory
		// store's ForEachNodeID ranges a Go map), so two separate ForEach-
		// shaped scans of the SAME data are not guaranteed to agree on row
		// order — only on the row SET, which is what this pins.
		var iterIDs []types.NodeID
		for n, err := range g.Nodes().Iter(context.Background(), graph.QueryOpts{}) {
			if err != nil {
				t.Fatalf("Iter: %v", err)
			}
			iterIDs = append(iterIDs, n.ID())
		}
		if len(iterIDs) != len(forEachIDs) {
			t.Fatalf("Iter yielded %d rows, ForEach yielded %d", len(iterIDs), len(forEachIDs))
		}
		gotSet := make(map[types.NodeID]bool, len(iterIDs))
		for _, id := range iterIDs {
			gotSet[id] = true
		}
		if len(gotSet) != len(want) {
			t.Fatalf("Iter yielded %d distinct IDs, want %d", len(gotSet), len(want))
		}
		for id := range want {
			if !gotSet[id] {
				t.Fatalf("Iter did not yield seeded node %v", id)
			}
		}
		for id := range gotSet {
			if !want[id] {
				t.Fatalf("Iter yielded unexpected ID %v not among seeded nodes", id)
			}
		}
	})
}

// TestIter_RelParity mirrors TestIter_NodeParity for relationships
// (Testing Rule 2 — Node/Rel parity).
func TestIter_RelParity(t *testing.T) {
	iterBackends(t, func(t *testing.T, g *graph.Graph) {
		_, seeded := seedIterRels(t, g, 200)
		want := relIDSet(seeded)

		all, err := g.Rels().All(graph.QueryOpts{})
		if err != nil {
			t.Fatalf("All: %v", err)
		}
		if len(all) != len(want) {
			t.Fatalf("All returned %d rows, want %d", len(all), len(want))
		}

		var forEachIDs []types.RelID
		if err := g.Rels().ForEach(graph.QueryOpts{}, func(r *types.Relationship) bool {
			forEachIDs = append(forEachIDs, r.ID())
			return true
		}); err != nil {
			t.Fatalf("ForEach: %v", err)
		}
		if len(forEachIDs) != len(want) {
			t.Fatalf("ForEach returned %d rows, want %d", len(forEachIDs), len(want))
		}

		// Compared as SETS, not ordered sequences — see the comment in
		// TestIter_NodeParity.
		var iterIDs []types.RelID
		for r, err := range g.Rels().Iter(context.Background(), graph.QueryOpts{}) {
			if err != nil {
				t.Fatalf("Iter: %v", err)
			}
			iterIDs = append(iterIDs, r.ID())
		}
		if len(iterIDs) != len(forEachIDs) {
			t.Fatalf("Iter yielded %d rows, ForEach yielded %d", len(iterIDs), len(forEachIDs))
		}
		gotSet := make(map[types.RelID]bool, len(iterIDs))
		for _, id := range iterIDs {
			gotSet[id] = true
		}
		if len(gotSet) != len(want) {
			t.Fatalf("Iter yielded %d distinct IDs, want %d", len(gotSet), len(want))
		}
		for id := range want {
			if !gotSet[id] {
				t.Fatalf("Iter did not yield seeded rel %v", id)
			}
		}
		for id := range gotSet {
			if !want[id] {
				t.Fatalf("Iter yielded unexpected ID %v not among seeded rels", id)
			}
		}
	})
}

// TestOutgoingIncomingIter_Parity pins that OutgoingIter/IncomingIter yield
// the same rows, same order, as ForEachOutgoing/ForEachIncoming on a hub
// node.
func TestOutgoingIncomingIter_Parity(t *testing.T) {
	iterBackends(t, func(t *testing.T, g *graph.Graph) {
		ctx := context.Background()
		hub, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"hub": true})
		if err != nil {
			t.Fatalf("Add hub: %v", err)
		}
		const n = 30
		for i := 0; i < n; i++ {
			s, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"idx": int64(i)})
			if err != nil {
				t.Fatalf("Add spoke %d: %v", i, err)
			}
			if _, err := g.Rels().Add(ctx, "KNOWS", hub, s, nil); err != nil {
				t.Fatalf("Add KNOWS %d: %v", i, err)
			}
			if _, err := g.Rels().Add(ctx, "LIKES", s, hub, nil); err != nil {
				t.Fatalf("Add LIKES %d: %v", i, err)
			}
		}

		var wantOut []types.RelID
		if err := g.Rels().ForEachOutgoing(hub.ID(), "KNOWS", func(r *types.Relationship) bool {
			wantOut = append(wantOut, r.ID())
			return true
		}); err != nil {
			t.Fatalf("ForEachOutgoing: %v", err)
		}
		var gotOut []types.RelID
		for r, err := range g.Rels().OutgoingIter(context.Background(), hub.ID(), "KNOWS") {
			if err != nil {
				t.Fatalf("OutgoingIter: %v", err)
			}
			gotOut = append(gotOut, r.ID())
		}
		if len(gotOut) != len(wantOut) || len(gotOut) != n {
			t.Fatalf("OutgoingIter yielded %d rows, ForEachOutgoing %d, want %d", len(gotOut), len(wantOut), n)
		}
		for i := range wantOut {
			if gotOut[i] != wantOut[i] {
				t.Fatalf("outgoing row %d: OutgoingIter %v, ForEachOutgoing %v", i, gotOut[i], wantOut[i])
			}
		}

		var wantIn []types.RelID
		if err := g.Rels().ForEachIncoming(hub.ID(), "LIKES", func(r *types.Relationship) bool {
			wantIn = append(wantIn, r.ID())
			return true
		}); err != nil {
			t.Fatalf("ForEachIncoming: %v", err)
		}
		var gotIn []types.RelID
		for r, err := range g.Rels().IncomingIter(context.Background(), hub.ID(), "LIKES") {
			if err != nil {
				t.Fatalf("IncomingIter: %v", err)
			}
			gotIn = append(gotIn, r.ID())
		}
		if len(gotIn) != len(wantIn) || len(gotIn) != n {
			t.Fatalf("IncomingIter yielded %d rows, ForEachIncoming %d, want %d", len(gotIn), len(wantIn), n)
		}
		for i := range wantIn {
			if gotIn[i] != wantIn[i] {
				t.Fatalf("incoming row %d: IncomingIter %v, ForEachIncoming %v", i, gotIn[i], wantIn[i])
			}
		}
	})
}

// TestIter_NodeFrozenContract pins the fallback-path frozen-row contract:
// ForEach's own doc says a Limit/After (or temporal) opts forces the
// All-backed fallback, and on a trusted backend All returns shared FROZEN
// rows (unlike the plain fast path, which hands back independent mutable
// copies via GetNode — see Iter's doc comment). Limit is set here
// specifically to force that fallback path.
func TestIter_NodeFrozenContract(t *testing.T) {
	iterBackends(t, func(t *testing.T, g *graph.Graph) {
		ctx := context.Background()
		want, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Ada"})
		if err != nil {
			t.Fatalf("Add: %v", err)
		}

		var got *types.Node
		for n, err := range g.Nodes().Iter(ctx, graph.QueryOpts{Limit: 100}) {
			if err != nil {
				t.Fatalf("Iter: %v", err)
			}
			if n.ID() == want.ID() {
				got = n
				break
			}
		}
		if got == nil {
			t.Fatal("Iter did not yield the seeded node")
		}
		if err := got.SetProperty("name", "Mutated"); !errors.Is(err, types.ErrFrozenNode) {
			t.Fatalf("SetProperty on yielded row = %v, want ErrFrozenNode", err)
		}
		// DeepCopy is the documented thaw operation.
		mutable := got.DeepCopy()
		if err := mutable.SetProperty("name", "Renamed"); err != nil {
			t.Fatalf("SetProperty on DeepCopy: %v", err)
		}
	})
}

// TestIter_RelFrozenContract mirrors TestIter_NodeFrozenContract for
// relationships (Testing Rule 2 — Node/Rel parity). Limit forces the same
// All-backed fallback path that yields frozen rows on a trusted backend.
func TestIter_RelFrozenContract(t *testing.T) {
	iterBackends(t, func(t *testing.T, g *graph.Graph) {
		ctx := context.Background()
		a, _ := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Alice"})
		b, _ := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Bob"})
		want, err := g.Rels().Add(ctx, "KNOWS", a, b, map[string]any{"since": int64(2026)})
		if err != nil {
			t.Fatalf("Add: %v", err)
		}

		var got *types.Relationship
		for r, err := range g.Rels().Iter(ctx, graph.QueryOpts{Limit: 100}) {
			if err != nil {
				t.Fatalf("Iter: %v", err)
			}
			if r.ID() == want.ID() {
				got = r
				break
			}
		}
		if got == nil {
			t.Fatal("Iter did not yield the seeded relationship")
		}
		if err := got.SetProperty("since", 2027); !errors.Is(err, types.ErrFrozenRelationship) {
			t.Fatalf("SetProperty on yielded row = %v, want ErrFrozenRelationship", err)
		}
		mutable := got.DeepCopy()
		if err := mutable.SetProperty("since", 2027); err != nil {
			t.Fatalf("SetProperty on DeepCopy: %v", err)
		}
	})
}

// TestIter_NodeCtxCancelMidIteration pins that cancelling ctx during a real
// backend scan yields exactly one (nil, ctx.Err()) then stops.
func TestIter_NodeCtxCancelMidIteration(t *testing.T) {
	iterBackends(t, func(t *testing.T, g *graph.Graph) {
		seedIterNodes(t, g, 50)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var okCount, errCount int
		var lastErr error
		for n, err := range g.Nodes().Iter(ctx, graph.QueryOpts{}) {
			if err != nil {
				errCount++
				lastErr = err
				if n != nil {
					t.Fatalf("error row: node = %v, want nil", n)
				}
				continue
			}
			okCount++
			if okCount == 5 {
				cancel()
			}
		}
		if okCount != 5 {
			t.Fatalf("got %d successful rows before cancel, want 5", okCount)
		}
		if errCount != 1 {
			t.Fatalf("got %d error yields, want exactly 1", errCount)
		}
		if !errors.Is(lastErr, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", lastErr)
		}
	})
}

// TestIter_RelCtxCancelMidIteration mirrors TestIter_NodeCtxCancelMidIteration
// for relationships.
func TestIter_RelCtxCancelMidIteration(t *testing.T) {
	iterBackends(t, func(t *testing.T, g *graph.Graph) {
		seedIterRels(t, g, 50)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var okCount, errCount int
		var lastErr error
		for r, err := range g.Rels().Iter(ctx, graph.QueryOpts{}) {
			if err != nil {
				errCount++
				lastErr = err
				if r != nil {
					t.Fatalf("error row: rel = %v, want nil", r)
				}
				continue
			}
			okCount++
			if okCount == 5 {
				cancel()
			}
		}
		if okCount != 5 {
			t.Fatalf("got %d successful rows before cancel, want 5", okCount)
		}
		if errCount != 1 {
			t.Fatalf("got %d error yields, want exactly 1", errCount)
		}
		if !errors.Is(lastErr, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", lastErr)
		}
	})
}

// TestIter_ConcurrentWriterRace exercises Iter with a concurrent writer —
// run with -race. Iter's relaxed isolation means the writer's new rows are
// neither guaranteed to be seen nor guaranteed to be excluded; the assertion
// here is only that the scan completes without error, crash, or a data race
// (concurrent structural mutation must never corrupt a frozen row already in
// flight).
func TestIter_ConcurrentWriterRace(t *testing.T) {
	iterBackends(t, func(t *testing.T, g *graph.Graph) {
		seedIterNodes(t, g, 100)

		var wg sync.WaitGroup
		stop := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"w": int64(i)}); err != nil {
					return
				}
				i++
			}
		}()

		count := 0
		for n, err := range g.Nodes().Iter(context.Background(), graph.QueryOpts{}) {
			if err != nil {
				close(stop)
				wg.Wait()
				t.Fatalf("Iter: %v", err)
			}
			if n == nil {
				close(stop)
				wg.Wait()
				t.Fatal("Iter yielded a nil row with nil error")
			}
			count++
		}
		close(stop)
		wg.Wait()
		if count < 100 {
			t.Fatalf("Iter saw %d rows, want at least the 100 seeded", count)
		}
	})
}
