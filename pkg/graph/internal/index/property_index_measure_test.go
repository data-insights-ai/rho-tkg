//go:build measure

// Measurement level (René 4 Nov, `make measure`): testing.AllocsPerRun reads
// the process-wide malloc count, and this package runs parallel tests, so the
// bound runs by hand, not in a gate.

package index

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestIndexMaintenanceBuildsIndexedKeysOnly (IDXKEYLAZY, ai-soc's BO A/B
// kpdl: GC mark +20 s with DerivedPage's index on disk): adding a node or a
// relationship to the indexes builds the value key of an indexed property
// only. Before, each of a row's properties got one per label (a copy of the
// value), indexed or not: 20 unindexed properties cost 20 allocations or more.
func TestIndexMaintenanceBuildsIndexedKeysOnly(t *testing.T) {
	const unindexed = 20
	n := types.NewNode(1, 7, nil)
	r := types.NewRelationship(2, 3, 1, 1)
	for _, set := range []func(string, any) error{n.SetProperty, r.SetProperty} {
		if err := set("k", "indexed"); err != nil {
			t.Fatal(err)
		}
		for i := range unindexed {
			if err := set(string(rune('a'+i))+"p", "a value nobody indexes"); err != nil {
				t.Fatal(err)
			}
		}
	}
	nodes := map[PropertyIndexKey]*PropertyIndex{{LabelToken: 7, PropertyKey: "k"}: NewPropertyIndex()}
	rels := map[RelPropertyIndexKey]*PropertyIndex{{RelTypeToken: 3, PropertyKey: "k"}: NewPropertyIndex()}
	for name, add := range map[string]func(){
		"node": func() { AddNodeToPropertyIndexes(nodes, n, 1) },
		"rel":  func() { AddRelToPropertyIndexes(rels, r, 2) },
	} {
		if allocs := testing.AllocsPerRun(50, add); allocs >= unindexed/2 {
			t.Errorf("%s: %.0f allocations per add with %d unindexed properties", name, allocs, unindexed)
		}
	}
	if !nodes[PropertyIndexKey{LabelToken: 7, PropertyKey: "k"}].Contains(1) || !rels[RelPropertyIndexKey{RelTypeToken: 3, PropertyKey: "k"}].Contains(2) {
		t.Fatal("the indexed property was not indexed")
	}
}
