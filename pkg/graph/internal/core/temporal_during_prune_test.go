package core

import (
	"context"
	"sort"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestNodesByLabelPropertyDuring_PruneEquivalence is the rule-17 guarantee for
// the Step-1 wiring: the labeled interval door NodesByLabelPropertyDuring must
// return the identical result whether or not the valid-time envelope index
// exists. The prune is overlap-sound (the During door matches on overlap), so it
// may narrow candidates but never change the answer. Two-phase (rule 15) and
// exact-set (rule 16): a phantom-future node MUST be pruned yet an open-ended
// node whose OLDER version overlaps MUST survive (envelope-union soundness).
func TestNodesByLabelPropertyDuring_PruneEquivalence(t *testing.T) {
	t.Parallel()
	backends := []struct {
		name     string
		newStore func(t *testing.T) storepkg.MandatoryStore
	}{
		{"memory", func(t *testing.T) storepkg.MandatoryStore { return memory.New() }},
		{"badger", func(t *testing.T) storepkg.MandatoryStore {
			bs, err := badger.New(badger.Config{InMemory: true})
			if err != nil {
				t.Fatalf("badger.New: %v", err)
			}
			return bs
		}},
	}
	for _, be := range backends {
		be := be
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			runDuringPruneEquivalence(t, be.newStore)
		})
	}
}

// buildDuringPropScenario populates g with "Doc" nodes all carrying region="eu"
// but with diverging valid-time envelopes, returning nodeID→name.
//
//   - A: valid_from 1000 → (update) valid_from 8000, open-ended. Envelope
//     [1000,∞); older tile [1000,8000) overlaps early windows (union soundness).
//   - C: valid [1000,3000). Bounded — pruned past 3000.
//   - B: valid [5000,6000). Phantom future — pruned for early windows.
func buildDuringPropScenario(t *testing.T, g *Core) map[types.NodeID]string {
	t.Helper()
	ctx := context.Background()
	names := make(map[types.NodeID]string)
	add := func(name string, props map[string]any) *types.Node {
		props["region"] = "eu"
		n, err := g.Nodes.Add(ctx, []string{"Doc"}, props)
		if err != nil {
			t.Fatalf("Add(%s): %v", name, err)
		}
		names[n.ID()] = name
		return n
	}
	a := add("A", map[string]any{"tkg_valid_from": types.Instant(1000)})
	if _, err := g.Nodes.Update(ctx, a.ID(), map[string]any{"tkg_valid_from": types.Instant(8000), "region": "eu"}); err != nil {
		t.Fatalf("Update(A): %v", err)
	}
	add("C", map[string]any{"tkg_valid_from": types.Instant(1000), "tkg_valid_to": types.Instant(3000)})
	add("B", map[string]any{"tkg_valid_from": types.Instant(5000), "tkg_valid_to": types.Instant(6000)})
	return names
}

type duringProbe struct {
	name       string
	start, end types.Instant
	want       []string
}

var duringProbes = []duringProbe{
	// [1200,1800): C [1000,3000) and A's older tile [1000,8000) overlap; B pruned.
	{"early", 1200, 1800, []string{"A", "C"}},
	// [5200,5800): B [5000,6000) and A [1000,8000) overlap; C pruned.
	{"phantom-window", 5200, 5800, []string{"A", "B"}},
	// [8500,9000): only A's head [8000,∞) overlaps; B and C pruned.
	{"open-head", 8500, 9000, []string{"A"}},
}

func runDuringPruneEquivalence(t *testing.T, newStore func(t *testing.T) storepkg.MandatoryStore) {
	gNoIdx, err := New(Config{Store: newStore(t)})
	if err != nil {
		t.Fatalf("New(no index): %v", err)
	}
	defer gNoIdx.Close()
	namesNoIdx := buildDuringPropScenario(t, gNoIdx)

	gIdx, err := New(Config{Store: newStore(t)})
	if err != nil {
		t.Fatalf("New(index): %v", err)
	}
	defer gIdx.Close()
	namesIdx := buildDuringPropScenario(t, gIdx)
	if err := gIdx.Index.CreateTemporal("Doc"); err != nil {
		t.Fatalf("CreateTemporal(Doc): %v", err)
	}
	if gIdx.temporalCandidates == nil {
		t.Fatal("temporalCandidates capability not wired — prune path untested")
	}

	// White-box: the prune actually fires for an interval filter and drops the
	// phantom-future node B at the early window.
	tok, ok := gIdx.labels.Lookup("Doc")
	if !ok {
		t.Fatal("Doc label token not found")
	}
	allIDs := make([]types.NodeID, 0, len(namesIdx))
	for id := range namesIdx {
		allIDs = append(allIDs, id)
	}
	kept, pruned := gIdx.temporalCandidates.PruneTemporalCandidates(tok, allIDs, storepkg.QueryOpts{ValidStart: 1200, ValidEnd: 1800})
	if !pruned {
		t.Fatal("PruneTemporalCandidates returned ok=false for an interval filter — prune never fires")
	}
	keptNames := map[string]bool{}
	for _, id := range kept {
		keptNames[namesIdx[id]] = true
	}
	if keptNames["B"] {
		t.Errorf("prune@[1200,1800) kept phantom-future B: %v", keptNames)
	}
	if !keptNames["A"] || !keptNames["C"] {
		t.Errorf("prune@[1200,1800) dropped an overlapping node: %v (A,C must survive)", keptNames)
	}

	query := func(g *Core, names map[types.NodeID]string, start, end types.Instant) []string {
		got, err := g.Temporal.NodesByLabelPropertyDuring("Doc", "region", "eu", start, end)
		if err != nil {
			t.Fatalf("NodesByLabelPropertyDuring: %v", err)
		}
		out := make([]string, 0, len(got))
		for _, n := range got {
			out = append(out, names[n.ID()])
		}
		sort.Strings(out)
		return out
	}
	for _, p := range duringProbes {
		p := p
		t.Run(p.name, func(t *testing.T) {
			oracle := query(gNoIdx, namesNoIdx, p.start, p.end)
			withIdx := query(gIdx, namesIdx, p.start, p.end)
			if !eqStrs(oracle, p.want) {
				t.Errorf("index-absent = %v, want %v", oracle, p.want)
			}
			if !eqStrs(withIdx, p.want) {
				t.Errorf("index-present = %v, want %v", withIdx, p.want)
			}
			if !eqStrs(oracle, withIdx) {
				t.Errorf("PRUNE CHANGED THE ANSWER: %v != %v", oracle, withIdx)
			}
		})
	}
}
