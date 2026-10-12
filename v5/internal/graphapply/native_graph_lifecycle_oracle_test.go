package graphapply

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// This literal fixture model never observes native floor/fence/planner guards.
// It describes external graph answers and exact changed components, not how
// either substrate chooses physical records, rounds or request encodings.
func checkLifecycleIDMap(ids []lifecycleIDMap) error {
	if len(ids) != 9 {
		return errInvalid
	}
	names := map[string]bool{}
	numbers := map[uint64]bool{}
	for _, id := range ids {
		if id.Logical == "" || id.Raw == 0 || id.Graph == "" || names[id.Logical] || numbers[id.Raw] {
			return errInvalid
		}
		if id.Role != "entity" && id.Role != "life" && id.Role != "value" || id.Role == "life" && id.Owner == "" || id.Role != "life" && id.Owner != "" {
			return errInvalid
		}
		names[id.Logical], numbers[id.Raw] = true, true
	}
	for _, id := range ids {
		if id.Role == "life" {
			found := false
			for _, owner := range ids {
				if owner.Role == "entity" && owner.Logical == id.Owner && owner.Graph == id.Graph {
					found = true
				}
			}
			if !found {
				return errInvalid
			}
		}
	}
	return nil
}
func lifecyclePresent(life, operation string, provenance uint64) lifecycleCell {
	return lifecycleCell{Present: true, Kind: "life", Reference: life, RevisionOperation: operation, Provenance: provenance}
}
func lifecycleAbsent(operation string, provenance uint64) lifecycleCell {
	return lifecycleCell{Kind: "absent", RevisionOperation: operation, Provenance: provenance}
}
func literalLifecycleView(n int) lifecycleView {
	v := lifecycleView{ObservationScope: "graph-complete-in-issued-range", CensusBounds: [2]uint64{1, 32}, CensusIDs: []string{}, Entities: []lifecycleEntity{}, Lives: []lifecycleLife{}, Postings: []lifecyclePosting{}, PhantomCount: 2, PhantomFound: []bool{false, false}}
	if n >= 1 {
		a := lifecycleEntity{ID: "node-a", Kind: "node", Presence: lifecyclePresent("life-a1", "node-a", 1), Effective: true, EffectiveLife: "life-a1"}
		if n >= 5 {
			a.Presence = lifecycleAbsent("node-close", 5)
			a.Effective = false
			a.EffectiveLife = ""
		}
		if n >= 7 {
			a.Presence = lifecyclePresent("life-a2", "node-reopen", 7)
			a.Effective = true
			a.EffectiveLife = "life-a2"
		}
		v.Entities = append(v.Entities, a)
		v.Lives = append(v.Lives, lifecycleLife{Owner: "node-a", Life: "life-a1"})
		if n >= 7 {
			v.Lives = append(v.Lives, lifecycleLife{Owner: "node-a", Life: "life-a2"})
		}
	}
	if n >= 2 {
		v.Entities = append(v.Entities, lifecycleEntity{ID: "node-b", Kind: "node", Presence: lifecyclePresent("life-b1", "node-b", 2), Effective: true, EffectiveLife: "life-b1"})
		v.Lives = append(v.Lives, lifecycleLife{Owner: "node-b", Life: "life-b1"})
	}
	if n >= 3 {
		e := lifecycleEntity{ID: "edge", Kind: "relationship", Type: "edge", Mode: "life-bound", Source: "node-a", Target: "node-b", Presence: lifecyclePresent("life-edge", "edge-create", 3), Effective: n < 5, EffectiveLife: "life-edge", Property: lifecycleCell{Present: true, Kind: "i64", Reference: "value5", I64: 5, RevisionOperation: "edge-create", Provenance: 3}}
		if n >= 4 {
			e.Property = lifecycleCell{Present: true, Kind: "i64", Reference: "value6", I64: 6, RevisionOperation: "edge-update", Provenance: 4}
		}
		if n >= 6 {
			e.Presence = lifecycleAbsent("edge-close", 6)
			e.EffectiveLife = ""
		}
		v.Entities = append(v.Entities, e)
		v.Lives = append(v.Lives, lifecycleLife{Owner: "edge", Life: "life-edge", SourceLife: "life-a1", TargetLife: "life-b1"})
	}
	for _, p := range []lifecyclePosting{{Endpoint: "node-a"}, {Endpoint: "node-a", Life: "life-a1"}, {Endpoint: "node-a", Life: "life-a2"}, {Endpoint: "node-b"}, {Endpoint: "node-b", Life: "life-b1"}} {
		p.Declared, p.Effective = []string{}, []string{}
		if n >= 3 && p.Life != "life-a2" {
			if n < 6 {
				p.Declared = []string{"edge"}
			}
			if n < 5 {
				p.Effective = []string{"edge"}
			}
		}
		v.Postings = append(v.Postings, p)
	}
	for _, entity := range v.Entities {
		v.CensusIDs = append(v.CensusIDs, entity.ID)
	}
	slices.Sort(v.CensusIDs)
	return v
}
func assertLifecycleView(t *testing.T, got lifecycleView, n int) {
	t.Helper()
	want := literalLifecycleView(n)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("exact seven-step lifecycle view at%d\ngot=%+v\nwant=%+v", n, got, want)
	}
}
func assertLifecycleCandidates(t *testing.T, raw nativeLifecycleObservation, n int) {
	t.Helper()
	if len(raw.Postings) != 5 {
		t.Fatal("lifecycle posting query omitted", n)
	}
	for _, p := range raw.Postings {
		var want []uint64
		if n >= 3 && (p.Endpoint != 1 || p.Life != 7) {
			want = []uint64{3}
		}
		if !slices.Equal(p.Candidates, want) {
			t.Fatal("exact actual retained endpoint candidate set", n, p, want)
		}
	}
}
func literalLifecycleCDC(t *testing.T, n int) ([]lifecycleChange, []lifecycleEntity, []lifecycleLife, []lifecycleValue) {
	t.Helper()
	all, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	scope, err := temporal.AppendScope(nil, all, defaultMaterializerLimits().catalog.Temporal)
	if err != nil {
		t.Fatal(err)
	}
	change := func(owner, life, kind, name string, before, after lifecycleCell) lifecycleChange {
		allScope, err := lifecycleCanonicalScope(scope, all.Axis(), defaultMaterializerLimits().catalog.Temporal)
		if err != nil {
			t.Fatal(err)
		}
		return lifecycleChange{Owner: owner, Life: life, Component: kind, Name: name, Scope: scope, AllScope: allScope, Before: before, After: after}
	}
	absent := lifecycleAbsent("", 0)
	switch n {
	case 1:
		return []lifecycleChange{change("node-a", "", "presence", "", absent, lifecyclePresent("life-a1", "node-a", 1))}, []lifecycleEntity{{ID: "node-a", Kind: "node"}}, []lifecycleLife{{Owner: "node-a", Life: "life-a1"}}, nil
	case 2:
		return []lifecycleChange{change("node-b", "", "presence", "", absent, lifecyclePresent("life-b1", "node-b", 2))}, []lifecycleEntity{{ID: "node-b", Kind: "node"}}, []lifecycleLife{{Owner: "node-b", Life: "life-b1"}}, nil
	case 3:
		return []lifecycleChange{change("edge", "", "presence", "", absent, lifecyclePresent("life-edge", "edge-create", 3)), change("edge", "life-edge", "property", "p", absent, lifecycleCell{Present: true, Kind: "i64", Reference: "value5", I64: 5, RevisionOperation: "edge-create", Provenance: 3})}, []lifecycleEntity{{ID: "edge", Kind: "relationship", Type: "edge", Mode: "life-bound", Source: "node-a", Target: "node-b"}}, []lifecycleLife{{Owner: "edge", Life: "life-edge", SourceLife: "life-a1", TargetLife: "life-b1"}}, []lifecycleValue{{Reference: "value5", Kind: "i64", I64: 5}}
	case 4:
		return []lifecycleChange{change("edge", "life-edge", "property", "p", lifecycleCell{Present: true, Kind: "i64", Reference: "value5", I64: 5, RevisionOperation: "edge-create", Provenance: 3}, lifecycleCell{Present: true, Kind: "i64", Reference: "value6", I64: 6, RevisionOperation: "edge-update", Provenance: 4})}, nil, nil, []lifecycleValue{{Reference: "value6", Kind: "i64", I64: 6}}
	case 5:
		return []lifecycleChange{change("node-a", "", "presence", "", lifecyclePresent("life-a1", "node-a", 1), lifecycleAbsent("node-close", 5))}, nil, nil, nil
	case 6:
		return []lifecycleChange{change("edge", "", "presence", "", lifecyclePresent("life-edge", "edge-create", 3), lifecycleAbsent("edge-close", 6))}, nil, nil, nil
	case 7:
		return []lifecycleChange{change("node-a", "", "presence", "", lifecycleAbsent("node-close", 5), lifecyclePresent("life-a2", "node-reopen", 7))}, nil, []lifecycleLife{{Owner: "node-a", Life: "life-a2"}}, nil
	}
	t.Fatal("unknown lifecycle step", n)
	return nil, nil, nil, nil
}
func assertLifecycleCDC(t *testing.T, got lifecycleStep, n int) {
	t.Helper()
	changes, entities, lives, values := literalLifecycleCDC(t, n)
	if !reflect.DeepEqual(got.LogicalCDC, changes) || !reflect.DeepEqual(got.NewEntities, entities) || !reflect.DeepEqual(got.NewLives, lives) || !reflect.DeepEqual(got.NewValues, values) {
		t.Fatal("complete actual lifecycle CDC differs", n, got.LogicalCDC, got.NewEntities, got.NewLives, got.NewValues)
	}
}
func TestNativeGraphLifecycleLiteralOracleRejectsCurrentAndMissingEffects(t *testing.T) {
	old, current := literalLifecycleView(3), literalLifecycleView(7)
	if reflect.DeepEqual(old, current) {
		t.Fatal("literal oracle accepts current as old")
	}
	mutation := literalLifecycleView(3)
	mutation.Postings[4].Effective = nil
	if reflect.DeepEqual(old, mutation) {
		t.Fatal("literal oracle misses incoming edge")
	}
	mutation = literalLifecycleView(7)
	mutation.Entities[2].Property = lifecycleCell{}
	if reflect.DeepEqual(current, mutation) {
		t.Fatal("literal oracle drops retained closed-edge property")
	}
	ids := lifecycleIDs()
	ids[1].Owner = "phantom"
	if !errors.Is(checkLifecycleIDMap(ids), errInvalid) {
		t.Fatal("mapping admitted foreign life owner")
	}
}
