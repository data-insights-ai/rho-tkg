package graphstore

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

func fullRetractionFixture(t *testing.T, n int) *fullFixture {
	t.Helper()
	f := newFullFixture(t, GraphLimits{})
	whole := f.span(t, 0, 20)
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 1, Scope: whole}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 1, Scope: whole}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 3, Life: 1, Scope: whole})
	// Modest, separately bounded setup calls do not widen the measured Close's
	// unchanged catalog/planner/source limits.
	for j := range n {
		f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: graphstate.EntityID(100 + j), Life: 1, Scope: whole, Record: graphstate.EntityRecord{Type: "BOUND", Source: 1, Target: 2, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 1, TargetLife: 1}})
	}
	f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: 999, Life: 1, Scope: whole, Record: graphstate.EntityRecord{Type: "UNRELATED", Source: 3, Target: 2, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 1, TargetLife: 1}})
	return f
}
func fullRetractionPlan(t *testing.T, f *fullFixture) (GraphEffects, error) {
	t.Helper()
	c := f.catalog(t, f.index)
	defer c.view.Close()
	r, _ := state.NewRevision(f.index+1, f.index+1000)
	effects, err := StageOperations(t.Context(), c, []graphstate.Operation{{Kind: graphstate.Close, Owner: 1, Life: 1, Scope: f.span(t, 10, 20)}}, r, f.limits)
	if c.fullViews != 0 || c.stages != 0 || c.stageBytes != 0 || c.records != 0 {
		t.Fatal("Close leaked private handles")
	}
	return effects, err
}
func TestFullHighDegreePureNodeRetractionAvoidsUniquenessExpansion(t *testing.T) {
	for _, n := range []int{64, 256} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			f := fullRetractionFixture(t, n)
			old := f.index
			effects, err := fullRetractionPlan(t, f)
			if err != nil {
				t.Fatal("actual Full node Close exceeds unchanged bounded reads", n, err)
			}
			if len(effects.Delta.Entities)+len(effects.Delta.Lives)+len(effects.Delta.Values) != 0 || len(effects.Groups) != 1 || effects.Groups[0].Key != (graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence}) {
				t.Fatal("Close generated incident effects", effects)
			}
			for _, dep := range effects.Dependencies {
				if dep.Kind == graphstate.IncidentDependency || dep.Kind == graphstate.PrefixDependency || dep.Kind == graphstate.UniquenessDependency {
					t.Fatal("Close expanded constraints", dep)
				}
			}
			f.install(t, effects)
			for j := range n {
				id := graphstate.EntityID(100 + j)
				before := f.projection(t, old, id, 15, graphstate.Effective)
				effective := f.projection(t, f.index, id, 15, graphstate.Effective)
				declared := f.projection(t, f.index, id, 15, graphstate.Declared)
				earlier := f.projection(t, f.index, id, 5, graphstate.Effective)
				if !before.Exists || !before.Active || effective.Active || !effective.Exists || !declared.Active || !earlier.Active || declared.Record != before.Record {
					t.Fatal("historical/declared/effective set diverged", id, before, effective, declared, earlier)
				}
			}
			unrelated := f.projection(t, f.index, 999, 15, graphstate.Effective)
			phantom := f.projection(t, f.index, 9999, 15, graphstate.Effective)
			if !unrelated.Active || phantom.Exists || phantom.Active {
				t.Fatal("Close leaked unrelated/phantom effects", unrelated, phantom)
			}
			if err := f.db.ScrubApplication(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// This diagnostic fingerprint includes root, exact physical KV writes and CDC,
// omitting dependency/source-work/owned counters. It is NOT a self-contained
// semantic parity assertion: later physical formats may change these bytes.
// The separately retained baseline/fixed receipt compares one exact format.
func TestFullRetractionEffectFingerprint(t *testing.T) {
	f := fullRetractionFixture(t, 4)
	effects, err := fullRetractionPlan(t, f)
	if err != nil {
		t.Fatal(err)
	}
	image, err := EncodeRoot(effects.Root)
	if err != nil {
		t.Fatal(err)
	}
	out := append([]byte(nil), image...)
	appendField := func(b []byte) { out = binary.BigEndian.AppendUint64(out, uint64(len(b))); out = append(out, b...) }
	for _, kv := range effects.Writes {
		appendField(kv.Key)
		appendField(kv.Value)
	}
	for _, group := range effects.Groups {
		changes, err := state.AppendChanges(nil, group.Owned.Axis(), group.Changes, state.CodecLimits{})
		if err != nil {
			t.Fatal(err)
		}
		appendField(changes)
	}
	t.Logf("logical-effect-sha256=%x dependencies=%d source-records=%d source-bytes=%d", sha256.Sum256(out), len(effects.Dependencies), effects.Work.Records, effects.Work.Bytes)
}
