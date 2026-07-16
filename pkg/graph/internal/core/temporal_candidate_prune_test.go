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

// docProbe is one temporal ByLabel query and the exact set of node names it must
// return. The set is asserted directly (not merely cross-checked between the two
// runs) so a bug that corrupts BOTH the index-present and index-absent path
// identically cannot slip through an equivalence-only comparison.
type docProbe struct {
	name string
	opts storepkg.QueryOpts
	want []string
}

// buildDocScenario populates g with a family of "Doc" nodes whose version
// lifecycles diverge, returning nodeID→name so a query result can be projected to
// a name set. The scenario is deliberately adversarial for the B4 valid-time
// ENVELOPE prune:
//
//   - A: valid_from 1000 → (update) valid_from 2000, open-ended. Envelope [1000,∞):
//     the two-phase case — at t=1500 the resolver must return v0's state, at
//     t=2500 v1's; the open envelope keeps A for every probe.
//   - C: valid [1000,3000). Bounded envelope — MUST be pruned past t=3000.
//   - B: valid [5000,6000). A "phantom window" far in the future — MUST be pruned
//     at every early probe.
//   - D: valid_from 1000 → (update) valid_from 8000, both open-ended (the domain
//     REJECTS updating a valid-to-closed node — ErrAlreadyClosed — so a
//     multi-version node's non-final versions are always open, hence its envelope
//     is always open [minFrom,∞)). Resolver windows tile to [1000,8000) and
//     [8000,∞). The soundness trap: at t=4000 the CURRENT version [8000,∞) does
//     NOT match, but v0's [1000,8000) does — an envelope tracking only the newest
//     version would WRONGLY prune D. The open union [1000,∞) keeps it.
func buildDocScenario(t *testing.T, g *Core) map[types.NodeID]string {
	t.Helper()
	ctx := context.Background()
	names := make(map[types.NodeID]string)

	add := func(name string, props map[string]any) *types.Node {
		n, err := g.Nodes.Add(ctx, []string{"Doc"}, props)
		if err != nil {
			t.Fatalf("Add(%s): %v", name, err)
		}
		names[n.ID()] = name
		return n
	}
	update := func(n *types.Node, name string, props map[string]any) {
		if _, err := g.Nodes.Update(ctx, n.ID(), props); err != nil {
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

// docProbes are the queries run identically against an index-absent and an
// index-present graph. Point probes use ValidAt; the interval probe uses
// ValidStart/ValidEnd (predicate-anywhere-in-interval).
var docProbes = []docProbe{
	{"point@1500", storepkg.QueryOpts{ValidAt: 1500}, []string{"A", "C", "D"}}, // A v0, C, D v0 [1000,8000)
	{"point@2500", storepkg.QueryOpts{ValidAt: 2500}, []string{"A", "C", "D"}}, // A v1, C to 3000, D v0
	{"point@4000", storepkg.QueryOpts{ValidAt: 4000}, []string{"A", "D"}},      // C & B pruned; D kept via v0 (union soundness)
	{"point@5500", storepkg.QueryOpts{ValidAt: 5500}, []string{"A", "B", "D"}}, // B phantom live; C pruned; D v0
	{"point@8500", storepkg.QueryOpts{ValidAt: 8500}, []string{"A", "D"}},      // D via v1 [8000,∞); B & C pruned
	{"interval@1200-1800", storepkg.QueryOpts{ValidStart: 1200, ValidEnd: 1800}, []string{"A", "C", "D"}},
}

func queryDocNames(t *testing.T, g *Core, names map[types.NodeID]string, opts storepkg.QueryOpts) []string {
	t.Helper()
	got, err := g.Nodes.ByLabel("Doc", opts)
	if err != nil {
		t.Fatalf("ByLabel(Doc, %+v): %v", opts, err)
	}
	out := make([]string, 0, len(got))
	for _, n := range got {
		out = append(out, names[n.ID()])
	}
	sort.Strings(out)
	return out
}

func eqStrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestTemporalCandidatePruneEquivalence is the rule-17 guarantee for B4: a
// temporal ByLabel query MUST return the identical result whether or not a
// valid-time envelope index exists — the prune is a candidate narrower, never an
// answer changer. It also pins the exact expected set per probe (rule 16) so a
// silent double-corruption cannot pass an equivalence-only check, and exercises
// the envelope-union soundness trap (D) and phantom-window pruning (B/C) with a
// two-phase mutate-then-query shape (rule 15).
func TestTemporalCandidatePruneEquivalence(t *testing.T) {
	t.Parallel()
	backends := []struct {
		name     string
		newStore func(t *testing.T) storepkg.Store
	}{
		{"memory", func(t *testing.T) storepkg.Store { return memory.New() }},
		{"badger", func(t *testing.T) storepkg.Store {
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
			runPruneEquivalence(t, be.newStore)
		})
	}
}

// TestTemporalCandidatePruneTieredDeclines pins the Stage-4 contract: the tiered
// store does NOT implement TemporalCandidateCapability (its per-shard envelopes
// cannot be soundly folded into one store-global envelope without a cross-shard
// pass), so core wires c.temporalCandidates = nil and every temporal scan takes
// the full-history fold. The query must still return the correct at-time result —
// declining the accelerator must never change the answer.
func TestTemporalCandidatePruneTieredDeclines(t *testing.T) {
	t.Parallel()
	g, _ := newTestTieredGraph(t)
	ctx := context.Background()

	// "Case" is a reference label on the test tiered store.
	n, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{"tkg_valid_from": types.Instant(1000), "v": "0"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"tkg_valid_from": types.Instant(5000), "v": "1"}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := g.Index.CreateTemporal("Case"); err != nil {
		t.Fatalf("CreateTemporal: %v", err)
	}

	if g.temporalCandidates != nil {
		t.Fatal("tiered store must DECLINE TemporalCandidateCapability (got non-nil) — the prune would need a cross-shard envelope fold")
	}

	// Full-fold path must still answer the two-phase query correctly: at t=2000 the
	// node's v0 window [1000,5000) holds.
	at2000, err := g.Nodes.ByLabel("Case", storepkg.QueryOpts{ValidAt: 2000})
	if err != nil {
		t.Fatalf("ByLabel@2000: %v", err)
	}
	if len(at2000) != 1 || at2000[0].ID() != n.ID() {
		t.Errorf("ByLabel@2000 = %d nodes, want the single Case node (v0 window)", len(at2000))
	}
	// At t=500, before any version's valid-from — must be empty.
	at500, err := g.Nodes.ByLabel("Case", storepkg.QueryOpts{ValidAt: 500})
	if err != nil {
		t.Fatalf("ByLabel@500: %v", err)
	}
	if len(at500) != 0 {
		t.Errorf("ByLabel@500 = %d nodes, want 0 (before valid-from)", len(at500))
	}
}

// runPruneEquivalence builds the identical Doc scenario on two graphs from the
// same backend — one without a temporal index (the no-prune correctness oracle),
// one with — and asserts they agree with each other AND with the pinned expected
// sets, plus that the prune path genuinely fires.
func runPruneEquivalence(t *testing.T, newStore func(t *testing.T) storepkg.Store) {
	// Run 1 — NO temporal index. PruneTemporalCandidates returns ok=false, so
	// every candidate is resolved. This is the correctness oracle.
	gNoIdx, err := New(Config{Store: newStore(t)})
	if err != nil {
		t.Fatalf("New(no index): %v", err)
	}
	defer gNoIdx.Close()
	namesNoIdx := buildDocScenario(t, gNoIdx)

	// Run 2 — WITH a temporal index on "Doc". The envelope prune fires.
	gIdx, err := New(Config{Store: newStore(t)})
	if err != nil {
		t.Fatalf("New(index): %v", err)
	}
	defer gIdx.Close()
	namesIdx := buildDocScenario(t, gIdx)
	if err := gIdx.Index.CreateTemporal("Doc"); err != nil {
		t.Fatalf("CreateTemporal(Doc): %v", err)
	}

	// Confirm the capability is actually wired for the index run — otherwise this
	// test silently degrades to comparing two identical no-prune paths.
	if gIdx.temporalCandidates == nil {
		t.Fatal("temporalCandidates capability not wired on memory store — prune path untested")
	}

	// White-box: prove the prune path is actually EXERCISED (returns ok=true and
	// drops ids) — the equivalence assertions above would still pass if
	// PruneTemporalCandidates were a no-op returning ok=false, because the resolver
	// rejects out-of-window nodes regardless. At ValidAt=4000, B [5000,6000) and
	// C [1000,3000) have envelopes that cannot overlap, while A and D are open.
	tok, ok := gIdx.labels.Lookup("Doc")
	if !ok {
		t.Fatal("Doc label token not found")
	}
	allIDs := make([]types.NodeID, 0, len(namesIdx))
	for id := range namesIdx {
		allIDs = append(allIDs, id)
	}
	kept, pruned := gIdx.temporalCandidates.PruneTemporalCandidates(tok, allIDs, storepkg.QueryOpts{ValidAt: 4000})
	if !pruned {
		t.Fatal("PruneTemporalCandidates returned ok=false with a live envelope index — prune never fires")
	}
	keptNames := make(map[string]bool)
	for _, id := range kept {
		keptNames[namesIdx[id]] = true
	}
	if keptNames["B"] || keptNames["C"] {
		t.Errorf("prune@4000 kept a provably-non-overlapping node: kept set %v (B/C must be dropped)", keptNames)
	}
	if !keptNames["A"] || !keptNames["D"] {
		t.Errorf("prune@4000 dropped an open-envelope node: kept set %v (A/D must survive)", keptNames)
	}

	for _, p := range docProbes {
		p := p
		t.Run(p.name, func(t *testing.T) {
			oracle := queryDocNames(t, gNoIdx, namesNoIdx, p.opts)
			pruned := queryDocNames(t, gIdx, namesIdx, p.opts)

			if !eqStrs(oracle, p.want) {
				t.Errorf("index-absent oracle = %v, want %v", oracle, p.want)
			}
			if !eqStrs(pruned, p.want) {
				t.Errorf("index-present result = %v, want %v", pruned, p.want)
			}
			if !eqStrs(oracle, pruned) {
				t.Errorf("PRUNE CHANGED THE ANSWER: index-absent %v != index-present %v", oracle, pruned)
			}

			// Rule 17: the NAMED temporal door (NodesByLabelAt) must agree with the
			// generic ByLabel{ValidAt} door for point probes — both funnel the B4
			// prune. Interval probes have no named single-instant equivalent.
			if p.opts.ValidAt != 0 {
				namedOracle := namedDocNames(t, gNoIdx, namesNoIdx, p.opts.ValidAt)
				namedPruned := namedDocNames(t, gIdx, namesIdx, p.opts.ValidAt)
				if !eqStrs(namedOracle, p.want) {
					t.Errorf("NodesByLabelAt index-absent = %v, want %v", namedOracle, p.want)
				}
				if !eqStrs(namedPruned, p.want) {
					t.Errorf("NodesByLabelAt index-present = %v, want %v", namedPruned, p.want)
				}
				if !eqStrs(namedPruned, pruned) {
					t.Errorf("NAMED vs GENERIC door diverged: NodesByLabelAt %v != ByLabel %v", namedPruned, pruned)
				}
			}
		})
	}
}

func namedDocNames(t *testing.T, g *Core, names map[types.NodeID]string, at types.Instant) []string {
	t.Helper()
	got, err := g.Temporal.NodesByLabelAt("Doc", at)
	if err != nil {
		t.Fatalf("NodesByLabelAt(Doc, %d): %v", at, err)
	}
	out := make([]string, 0, len(got))
	for _, n := range got {
		out = append(out, names[n.ID()])
	}
	sort.Strings(out)
	return out
}
