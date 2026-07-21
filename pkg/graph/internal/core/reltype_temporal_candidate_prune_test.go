package core

import (
	"context"
	"sort"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// relDocProbe mirrors docProbe (temporal_candidate_prune_test.go) for the
// rel-type-keyed mirror (BACKLOG 21c).
type relDocProbe struct {
	name string
	opts storepkg.QueryOpts
	want []string
}

// buildRelDocScenario is the relationship-type mirror of buildDocScenario: the
// same adversarial A/B/C/D lifecycle shape, but as "Knows" relationships between
// two fixed nodes instead of "Doc" nodes. See buildDocScenario's doc comment for
// the per-entity rationale (open-envelope union soundness trap on D, phantom
// window pruning on B/C, two-phase mutate-then-query on A/D).
func buildRelDocScenario(t *testing.T, g *Core) map[types.RelID]string {
	t.Helper()
	ctx := context.Background()
	names := make(map[types.RelID]string)

	start, err := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "start"})
	if err != nil {
		t.Fatalf("Add(start): %v", err)
	}
	end, err := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "end"})
	if err != nil {
		t.Fatalf("Add(end): %v", err)
	}

	add := func(name string, props map[string]any) *types.Relationship {
		r, err := g.Rels.Add(ctx, "Knows", start, end, props)
		if err != nil {
			t.Fatalf("Add(%s): %v", name, err)
		}
		names[r.ID()] = name
		return r
	}
	update := func(r *types.Relationship, name string, props map[string]any) {
		if _, err := g.Rels.Update(ctx, r.ID(), props); err != nil {
			t.Fatalf("Update(%s): %v", name, err)
		}
	}

	a := add("A", map[string]any{"tkg_valid_from": types.Instant(1000), "v": "0"})
	update(a, "A", map[string]any{"tkg_valid_from": types.Instant(2000), "v": "1"})

	add("C", map[string]any{"tkg_valid_from": types.Instant(1000), "tkg_valid_to": types.Instant(3000)})

	add("B", map[string]any{"tkg_valid_from": types.Instant(5000), "tkg_valid_to": types.Instant(6000)})

	d := add("D", map[string]any{"tkg_valid_from": types.Instant(1000), "v": "0"})
	update(d, "D", map[string]any{"tkg_valid_from": types.Instant(8000), "v": "1"})

	return names
}

var relDocProbes = []relDocProbe{
	{"point@1500", storepkg.QueryOpts{ValidAt: 1500}, []string{"A", "C", "D"}},
	{"point@2500", storepkg.QueryOpts{ValidAt: 2500}, []string{"A", "C", "D"}},
	{"point@4000", storepkg.QueryOpts{ValidAt: 4000}, []string{"A", "D"}},
	{"point@5500", storepkg.QueryOpts{ValidAt: 5500}, []string{"A", "B", "D"}},
	{"point@8500", storepkg.QueryOpts{ValidAt: 8500}, []string{"A", "D"}},
	{"interval@1200-1800", storepkg.QueryOpts{ValidStart: 1200, ValidEnd: 1800}, []string{"A", "C", "D"}},
}

func queryRelDocNames(t *testing.T, g *Core, names map[types.RelID]string, opts storepkg.QueryOpts) []string {
	t.Helper()
	got, err := g.Rels.ByType("Knows", opts)
	if err != nil {
		t.Fatalf("ByType(Knows, %+v): %v", opts, err)
	}
	out := make([]string, 0, len(got))
	for _, r := range got {
		out = append(out, names[r.ID()])
	}
	sort.Strings(out)
	return out
}

// TestRelTypeTemporalCandidatePruneEquivalence is the rel-side mirror of
// TestTemporalCandidatePruneEquivalence (BACKLOG 21c): a temporal ByType query
// MUST return the identical result whether or not a rel-type valid-time
// envelope index exists.
func TestRelTypeTemporalCandidatePruneEquivalence(t *testing.T) {
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
		// BaseSlot 0 / SlotCount 2 covers the legacy dual-generator IDs (nodes
		// slot 0, rels slot 1) at SnowflakeNodeID=0 — see
		// temporal_candidate_prune_test.go's node-side sharded case.
		{"sharded", func(t *testing.T) storepkg.MandatoryStore {
			st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				t.Fatalf("sharded.New: %v", err)
			}
			return st
		}},
	}
	for _, be := range backends {
		be := be
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			runRelPruneEquivalence(t, be.newStore)
		})
	}
}

func runRelPruneEquivalence(t *testing.T, newStore func(t *testing.T) storepkg.MandatoryStore) {
	gNoIdx, err := New(Config{Store: newStore(t)})
	if err != nil {
		t.Fatalf("New(no index): %v", err)
	}
	defer gNoIdx.Close()
	namesNoIdx := buildRelDocScenario(t, gNoIdx)

	gIdx, err := New(Config{Store: newStore(t)})
	if err != nil {
		t.Fatalf("New(index): %v", err)
	}
	defer gIdx.Close()
	namesIdx := buildRelDocScenario(t, gIdx)
	if err := gIdx.Index.CreateRelTemporal("Knows"); err != nil {
		t.Fatalf("CreateRelTemporal(Knows): %v", err)
	}

	if gIdx.relTypeTemporalCandidates == nil {
		t.Fatal("relTypeTemporalCandidates capability not wired — prune path untested")
	}

	tok, ok := gIdx.relTypes.Lookup("Knows")
	if !ok {
		t.Fatal("Knows rel-type token not found")
	}
	allIDs := make([]types.RelID, 0, len(namesIdx))
	for id := range namesIdx {
		allIDs = append(allIDs, id)
	}
	kept, pruned := gIdx.relTypeTemporalCandidates.PruneRelTypeTemporalCandidates(tok, allIDs, storepkg.QueryOpts{ValidAt: 4000})
	if !pruned {
		t.Fatal("PruneRelTypeTemporalCandidates returned ok=false with a live envelope index — prune never fires")
	}
	keptNames := make(map[string]bool)
	for _, id := range kept {
		keptNames[namesIdx[id]] = true
	}
	if keptNames["B"] || keptNames["C"] {
		t.Errorf("prune@4000 kept a provably-non-overlapping rel: kept set %v (B/C must be dropped)", keptNames)
	}
	if !keptNames["A"] || !keptNames["D"] {
		t.Errorf("prune@4000 dropped an open-envelope rel: kept set %v (A/D must survive)", keptNames)
	}

	for _, p := range relDocProbes {
		gotNoIdx := queryRelDocNames(t, gNoIdx, namesNoIdx, p.opts)
		gotIdx := queryRelDocNames(t, gIdx, namesIdx, p.opts)
		wantSorted := append([]string(nil), p.want...)
		sort.Strings(wantSorted)
		if !eqStrs(gotNoIdx, wantSorted) {
			t.Errorf("%s no-index = %v, want %v", p.name, gotNoIdx, wantSorted)
		}
		if !eqStrs(gotIdx, wantSorted) {
			t.Errorf("%s with-index = %v, want %v", p.name, gotIdx, wantSorted)
		}
		if !eqStrs(gotNoIdx, gotIdx) {
			t.Errorf("%s index/no-index diverge: no-index=%v index=%v", p.name, gotNoIdx, gotIdx)
		}
	}
}

// TestRelTypeTemporalCandidatePruneTieredDeclines pins the rel-side mirror of
// TestTemporalCandidatePruneTieredDeclines: the tiered store does not implement
// RelTypeTemporalCandidateCapability, so core wires
// c.relTypeTemporalCandidates = nil and every rel-type temporal scan takes the
// full-history fold. A CreateRelTemporal call against tiered must therefore fail
// closed with ErrCapabilityNotSupported.
func TestRelTypeTemporalCandidatePruneTieredDeclines(t *testing.T) {
	t.Parallel()
	g, _ := newTestTieredGraph(t)

	if g.relTypeTemporalCandidates != nil {
		t.Fatal("tiered store must DECLINE RelTypeTemporalCandidateCapability (got non-nil)")
	}
	if err := g.Index.CreateRelTemporal("Knows"); err == nil {
		t.Fatal("CreateRelTemporal on tiered store: want ErrCapabilityNotSupported, got nil")
	}
}
