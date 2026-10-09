package core

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Shared fixtures for the node caller-instant tests (Nodes.DeleteWithTx,
// Nodes.UpdateWithTx and their GraphTx / batch / ingest twins). They reuse the
// backends of tx_backfill_rel_helpers_test.go: memory, badger (in-memory),
// sharded and tiered; on tiered a "Ref" node lives on the reference shard and
// its "Ev" neighbours on the hot event shard, so a cascade crosses shards.

// nodeSnap is everything a refused node door must leave untouched.
type nodeSnap struct {
	cur    *types.Node
	curErr error
	hist   []*types.Node
}

func snapNode(t *testing.T, g *Core, id types.NodeID) nodeSnap {
	t.Helper()
	cur, curErr := g.Nodes.Get(context.Background(), id)
	if curErr != nil && !errors.Is(curErr, storepkg.ErrNodeNotFound) {
		t.Fatalf("Nodes.Get(%v): %v", id, curErr)
	}
	hist, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("Nodes.History(%v): %v", id, err)
	}
	hist = append([]*types.Node(nil), hist...)
	sort.Slice(hist, func(i, j int) bool { return hist[i].Version() < hist[j].Version() })
	return nodeSnap{cur: cur, curErr: curErr, hist: hist}
}

func nodeTemporalCopy(n *types.Node) types.TemporalMetadata {
	if tm := n.Temporal(); tm != nil {
		return *tm
	}
	return types.TemporalMetadata{}
}

func nodePropsMap(n *types.Node) map[string]any {
	out := map[string]any{}
	for _, p := range n.Properties() {
		out[p.Key] = p.Value
	}
	return out
}

// assertNodeUnchanged fails unless after equals before: same current row
// (version, every temporal stamp, properties), same history length and the
// same stamps on every history row.
func assertNodeUnchanged(t *testing.T, phase string, before, after nodeSnap) {
	t.Helper()
	if (before.curErr == nil) != (after.curErr == nil) {
		t.Fatalf("[%s] current node presence changed: before err=%v, after err=%v", phase, before.curErr, after.curErr)
	}
	if before.cur != nil {
		if before.cur.Version() != after.cur.Version() {
			t.Fatalf("[%s] current node version %d -> %d", phase, before.cur.Version(), after.cur.Version())
		}
		if b, a := nodeTemporalCopy(before.cur), nodeTemporalCopy(after.cur); !reflect.DeepEqual(b, a) {
			t.Fatalf("[%s] current node temporal changed:\n before %+v\n after  %+v", phase, b, a)
		}
		if b, a := nodePropsMap(before.cur), nodePropsMap(after.cur); !reflect.DeepEqual(b, a) {
			t.Fatalf("[%s] current node properties changed: %v -> %v", phase, b, a)
		}
	}
	if len(before.hist) != len(after.hist) {
		t.Fatalf("[%s] node history length %d -> %d", phase, len(before.hist), len(after.hist))
	}
	for i := range before.hist {
		b, a := before.hist[i], after.hist[i]
		if b.Version() != a.Version() || !reflect.DeepEqual(nodeTemporalCopy(b), nodeTemporalCopy(a)) {
			t.Fatalf("[%s] node history row %d changed:\n before v%d %+v\n after  v%d %+v",
				phase, i, b.Version(), nodeTemporalCopy(b), a.Version(), nodeTemporalCopy(a))
		}
	}
}

// txbNodeChain returns a node's history plus its current row (if any), by version.
func txbNodeChain(t *testing.T, g *Core, id types.NodeID) []*types.Node {
	t.Helper()
	s := snapNode(t, g, id)
	chain := append([]*types.Node(nil), s.hist...)
	if s.cur != nil {
		chain = append(chain, s.cur)
	}
	sort.Slice(chain, func(i, j int) bool { return chain[i].Version() < chain[j].Version() })
	return chain
}

// hoodSnap is a node and every relationship of its neighbourhood: what a
// refused cascade delete must leave untouched.
type hoodSnap struct {
	node nodeSnap
	rels map[types.RelID]relSnap
}

func snapHood(t *testing.T, g *Core, id types.NodeID, rels ...types.RelID) hoodSnap {
	t.Helper()
	h := hoodSnap{node: snapNode(t, g, id), rels: make(map[types.RelID]relSnap, len(rels))}
	for _, r := range rels {
		h.rels[r] = snapRel(t, g, r)
	}
	return h
}

func assertHoodUnchanged(t *testing.T, g *Core, phase string, before hoodSnap, id types.NodeID) {
	t.Helper()
	assertNodeUnchanged(t, phase, before.node, snapNode(t, g, id))
	for rid, b := range before.rels {
		assertRelUnchanged(t, fmt.Sprintf("%s rel %d", phase, rid), b, snapRel(t, g, rid))
	}
}

// txbNodeIDs returns the ids of rows, sorted.
func txbNodeIDs(rows []*types.Node) []types.NodeID {
	out := make([]types.NodeID, 0, len(rows))
	for _, n := range rows {
		out = append(out, n.ID())
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func txbAssertNodeIDSet(t *testing.T, phase string, got []*types.Node, want ...types.NodeID) {
	t.Helper()
	w := append([]types.NodeID(nil), want...)
	sort.Slice(w, func(i, j int) bool { return w[i] < w[j] })
	if g := txbNodeIDs(got); fmt.Sprint(g) != fmt.Sprint(w) {
		t.Fatalf("[%s] node set %v; want exactly %v", phase, g, w)
	}
}

// txbAddNode adds a node through the plain door.
func txbAddNode(t *testing.T, g *Core, label string, props map[string]any) *types.Node {
	t.Helper()
	n, err := g.Nodes.Add(context.Background(), []string{label}, props)
	if err != nil {
		t.Fatalf("Nodes.Add(%s): %v", label, err)
	}
	return n
}

// txbAddRelByID adds a LINK s -> e through the plain door.
func txbAddRelByID(t *testing.T, g *Core, s, e types.NodeID, props map[string]any) *types.Relationship {
	t.Helper()
	r, err := g.Rels.AddByID(context.Background(), "LINK", s, e, props)
	if err != nil {
		t.Fatalf("Rels.AddByID: %v", err)
	}
	return r
}
