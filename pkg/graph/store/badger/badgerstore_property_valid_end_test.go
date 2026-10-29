package badger

import (
	"fmt"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestPropertyIndexValidEnd_RefineGuards (VALIDENDPRUNE): a refined bound
// prunes its entry; a refine is skipped when a row of its node was written
// after the lookup read its sequence, or the store was cleared since; a
// history row written alone makes the node's entries unknown again.
func TestPropertyIndexValidEnd_RefineGuards(t *testing.T) {
	t.Parallel()
	names := registrypkg.NewLabelRegistry()
	d, _ := names.GetOrCreate("D")
	bs, err := New(Config{InMemory: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), LabelNames: names,
		PropertyIndexOnDiskLabels: []string{"D"}, PropertyIndexValidEnd: []storecontract.PropertyIndexValidEnd{{Label: "D", KeyPrefix: "ov_"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	if err := bs.CreatePropertyIndex(d, "ov_h"); err != nil {
		t.Fatal(err)
	}
	row := func(id int64) *types.Node {
		n := types.NewNode(types.NodeID(id), d, nil)
		if err := n.SetProperty("ov_h", "a"); err != nil {
			t.Fatal(err)
		}
		return n
	}
	put := func(id int64) {
		t.Helper()
		if err := bs.PutNode(row(id)); err != nil {
			t.Fatal(err)
		}
	}
	lookup := func(bound types.Instant) ([]*types.Node, []storecontract.ValidEndProbe) {
		t.Helper()
		nodes, probes, err := bs.NodesByLabelAndPropertyKeysValidEnd(d, "ov_h", []string{types.IndexablePropertyValueKey("a")},
			QueryOpts{ValidEndAfter: bound, ValidEndAfterSet: true})
		if err != nil {
			t.Fatal(err)
		}
		if len(probes) != len(nodes) {
			t.Fatalf("%d probes for %d nodes", len(probes), len(nodes))
		}
		return nodes, probes
	}
	ids := func(bound types.Instant) string {
		t.Helper()
		nodes, _ := lookup(bound)
		var out []types.NodeID
		for _, n := range nodes {
			out = append(out, n.ID())
		}
		return fmt.Sprint(out)
	}
	put(1)
	put(2)
	nodes, probes := lookup(500)
	if len(nodes) != 2 || probes[0].Known || probes[1].Known {
		t.Fatalf("new entries: %d nodes, probes %+v, want 2 without a bound", len(nodes), probes)
	}
	for i, n := range nodes {
		bs.RefineNodePropertyValidEnd(n, probes[i], 100)
	}
	if got := ids(500); got != "[]" {
		t.Fatalf("refined to 100, bound 500: %s, want both pruned", got)
	}
	nodes, probes = lookup(50)
	if len(nodes) != 2 || !probes[0].Known || probes[0].Bound != 100 {
		t.Fatalf("refined to 100, bound 50: %d nodes, probes %+v, want both with bound 100", len(nodes), probes)
	}

	if err := bs.ReplaceNode(row(1)); err != nil { // after the lookup: its refine is stale
		t.Fatal(err)
	}
	before := bs.PropertyIndexValidEndStats()
	bs.RefineNodePropertyValidEnd(nodes[0], probes[0], 100)
	if st := bs.PropertyIndexValidEndStats(); st.RefineSkipped != before.RefineSkipped+1 || st.Refined != before.Refined {
		t.Errorf("refine after a write: %+v, before %+v: want it skipped", st, before)
	}
	if got := ids(500); got != "[1]" {
		t.Errorf("node 1 rewritten: %s, want it read again, node 2 pruned", got)
	}

	nodes, probes = lookup(500)
	bs.RefineNodePropertyValidEnd(nodes[0], probes[0], 100)
	if got := ids(500); got != "[]" {
		t.Fatalf("node 1 refined again: %s, want both pruned", got)
	}
	hist := row(2)
	hist.SetVersion(1)
	if err := bs.PutNodeVersion(2, 1, hist); err != nil { // a history row alone
		t.Fatal(err)
	}
	if got := ids(500); got != "[2]" {
		t.Errorf("a history row of node 2 written: %s, want node 2 read again", got)
	}

	nodes, probes = lookup(50)
	if err := bs.Clear(); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreatePropertyIndex(d, "ov_h"); err != nil {
		t.Fatal(err)
	}
	put(3)
	before = bs.PropertyIndexValidEndStats()
	bs.RefineNodePropertyValidEnd(nodes[0], probes[0], 100)
	if st := bs.PropertyIndexValidEndStats(); st.RefineSkipped != before.RefineSkipped+1 {
		t.Errorf("refine read before Clear: %+v, before %+v: want it skipped", st, before)
	}
	if got := ids(0); got != "[3]" {
		t.Errorf("after Clear: %s, want only node 3", got)
	}
}
