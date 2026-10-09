package graphstate

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func planIncident(t testing.TB, v ReadView, revision uint64, ops ...Operation) (Delta, error) {
	t.Helper()
	r, err := state.NewRevision(revision, 0)
	if err != nil {
		t.Fatal(err)
	}
	return Plan(t.Context(), v, ops, r, Limits{})
}

func commitIncident(t testing.TB, v *fixtureView, revision uint64, ops ...Operation) Delta {
	t.Helper()
	d, err := planIncident(t, v, revision, ops...)
	if err != nil {
		t.Fatal(err)
	}
	v.install(t, d)
	v.id[0]++
	return d
}

func projectIncident(t testing.TB, v *fixtureView, id EntityID, axis temporal.Axis, at int64, mode Visibility, active bool, life LifeID, code string) Projection {
	t.Helper()
	p, err := Project(t.Context(), v, id, testPosition(t, axis, at), mode, Limits{})
	if err != nil || !p.Exists || p.Active != active || p.Life != life || p.Record != v.entities[id] || len(p.Labels) != 0 {
		t.Fatalf("unexpected exact projection for %d: %+v, %v", id, p, err)
	}
	assertOwnedScalar(t, p, "code", String(code), active)
	return p
}

func incidentPrefixes(d Delta) []lifeKey {
	out := []lifeKey{}
	seen := map[lifeKey]bool{}
	for _, dep := range d.Dependencies {
		if dep.Kind == PrefixDependency {
			key := lifeKey{dep.Prefix.Owner, dep.Prefix.Life}
			if !seen[key] {
				seen[key] = true
				out = append(out, key)
			}
		}
	}
	return out
}

func TestIdentityReferenceEndpointMutationDoesNotReadForeignAxis(t *testing.T) {
	for _, endpoint := range []EntityID{1, 2} {
		t.Run(map[EntityID]string{1: "source", 2: "target"}[endpoint], func(t *testing.T) {
			v := newFixtureView(t)
			axis, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{2}, Profile: temporal.ProfileIntegerZ, Version: 1, Reference: "observation", CanonicalUnit: "ordinal"}, temporal.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			nodeScope, edgeScope := testSpan(t, v.axis, 0, 20), testSpan(t, axis, 0, 20)
			v.defs[ownerSchemaKey{Relationship, "code"}] = PropertyDefinition{"code", Relationship, ScalarString, ScalarCardinality, UniqueScalar}
			commitIncident(t, v, 1,
				Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: nodeScope},
				Operation{Kind: CreateNode, Owner: 2, Life: 1, Scope: nodeScope},
			)
			commitIncident(t, v, 2,
				Operation{Kind: CreateRelationship, Owner: 3, Life: 1, Scope: edgeScope, Record: EntityRecord{Type: "OBS", Source: 1, Target: 2, Mode: IdentityReference}},
				Operation{Kind: Set, Owner: 3, Life: 1, Scope: edgeScope, Name: "code", Value: String("before"), ValueID: 1},
				Operation{Kind: CreateRelationship, Owner: 4, Life: 1, Scope: edgeScope, Record: EntityRecord{Type: "OBS", Source: 1, Target: 2, Mode: IdentityReference}},
				Operation{Kind: Set, Owner: 4, Life: 1, Scope: edgeScope, Name: "code", Value: String("other"), ValueID: 2},
			)
			old := v.clone()
			d := commitIncident(t, v, 3,
				Operation{Kind: Close, Owner: endpoint, Life: 1, Scope: testSpan(t, v.axis, 10, 20)},
				Operation{Kind: Reopen, Owner: endpoint, Life: 2, Scope: testSpan(t, v.axis, 12, 20)},
			)
			if got := incidentPrefixes(d); !reflect.DeepEqual(got, []lifeKey{{endpoint, 1}, {endpoint, 2}}) {
				t.Fatalf("endpoint change invalidated unrelated relationship properties: %v", got)
			}
			for _, mode := range []Visibility{Declared, Effective} {
				for _, view := range []*fixtureView{old, v} {
					p := projectIncident(t, view, 3, axis, 15, mode, true, 1, "before")
					if p.Endpoints != ([2]EndpointStatus{{ID: 1}, {ID: 2}}) {
						t.Fatalf("foreign endpoint axis was silently mapped: %+v", p.Endpoints)
					}
				}
			}
			for _, check := range []struct {
				view   *fixtureView
				at     int64
				active bool
				life   LifeID
			}{{old, 15, true, 1}, {v, 11, false, 0}, {v, 15, true, 2}, {v, 5, true, 1}} {
				p, err := Project(t.Context(), check.view, endpoint, testPosition(t, v.axis, check.at), Effective, Limits{})
				if err != nil || !p.Exists || p.Active != check.active || p.Life != check.life || len(p.Properties) != 0 || len(p.Labels) != 0 {
					t.Fatalf("endpoint historical lifecycle changed: %+v, %v", p, err)
				}
			}
			duplicate, err := planIncident(t, v, 4, Operation{Kind: Set, Owner: 4, Life: 1, Scope: edgeScope, Name: "code", Value: String("before"), ValueID: 3})
			if !errors.Is(err, ErrUniqueOverlap) || !reflect.DeepEqual(duplicate, Delta{}) {
				t.Fatalf("direct identity-reference mutation bypassed uniqueness: %+v, %v", duplicate, err)
			}
			commitIncident(t, v, 5, Operation{Kind: Set, Owner: 3, Life: 1, Scope: testSpan(t, axis, 10, 20), Name: "code", Value: String("after"), ValueID: 3})
			projectIncident(t, old, 3, axis, 15, Effective, true, 1, "before")
			projectIncident(t, v, 3, axis, 15, Effective, true, 1, "after")
			projectIncident(t, v, 3, axis, 5, Effective, true, 1, "before")
			projectIncident(t, v, 4, axis, 15, Effective, true, 1, "other")
			commitIncident(t, v, 6,
				Operation{Kind: Close, Owner: 3, Life: 1, Scope: edgeScope},
				Operation{Kind: Set, Owner: 4, Life: 1, Scope: edgeScope, Name: "code", Value: String("before"), ValueID: 4},
			)
			duplicate, err = planIncident(t, v, 7, Operation{Kind: Correct, Owner: 3, Life: 1, Scope: edgeScope, Present: true})
			if !errors.Is(err, ErrUniqueOverlap) || !reflect.DeepEqual(duplicate, Delta{}) {
				t.Fatalf("direct identity-reference restoration bypassed uniqueness: %+v, %v", duplicate, err)
			}
			projectIncident(t, v, 3, axis, 5, Effective, false, 0, "")
			projectIncident(t, old, 3, axis, 5, Effective, true, 1, "before")
		})
	}
}

type incidentFaultView struct {
	ReadView
	fault   string
	failure error
}

func (v incidentFaultView) IncidentRelationships(_ context.Context, _ IncidentPredicate, _ Cursor, _ ReadBudget) (EntityPage, error) {
	return EntityPage{View: v.Identity(), Version: 1, Entities: []EntityID{3}, Complete: true}, nil
}

func (v incidentFaultView) Entity(ctx context.Context, id EntityID) (EntityRead, error) {
	if id == 3 && v.fault == "entity error" {
		return EntityRead{}, v.failure
	}
	r, err := v.ReadView.Entity(ctx, id)
	if id == 3 {
		switch v.fault {
		case "missing entity":
			r.Found = false
		case "node candidate":
			r.Record.Kind = Node
		case "foreign endpoint":
			r.Record.Source, r.Record.Target = 2, 2
		}
	}
	return r, err
}

func (v incidentFaultView) Life(ctx context.Context, id EntityID, life LifeID) (LifeRead, error) {
	if id == 3 && v.fault == "life error" {
		return LifeRead{}, v.failure
	}
	r, err := v.ReadView.Life(ctx, id, life)
	if id == 3 && v.fault == "missing life" {
		r.Found = false
	}
	return r, err
}

func TestEndpointInvalidationRejectsContradictoryIncidentMetadata(t *testing.T) {
	v := newFixtureView(t)
	whole := testSpan(t, v.axis, 0, 20)
	commitIncident(t, v, 1,
		Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: whole},
		Operation{Kind: CreateNode, Owner: 2, Life: 1, Scope: whole},
		Operation{Kind: CreateRelationship, Owner: 3, Life: 1, Scope: whole, Record: EntityRecord{Type: "BOUND", Source: 1, Target: 2, Mode: LifeBound}, Binding: LifeRecord{SourceLife: 1, TargetLife: 1}},
	)
	failure := errors.New("incident metadata unavailable")
	for _, fault := range []string{"entity error", "missing entity", "node candidate", "foreign endpoint", "life error", "missing life"} {
		t.Run(fault, func(t *testing.T) {
			want := ErrContradictoryRead
			if fault == "entity error" || fault == "life error" {
				want = failure
			}
			d, err := planIncident(t, incidentFaultView{v, fault, failure}, 2, Operation{Kind: Close, Owner: 1, Life: 1, Scope: whole})
			if !errors.Is(err, want) || !reflect.DeepEqual(d, Delta{}) {
				t.Fatalf("untrusted incident candidate accepted: %+v, %v", d, err)
			}
		})
	}
}

func TestLifeBoundEndpointInvalidatesOnlyImmutableBinding(t *testing.T) {
	for _, shape := range []string{"source", "target", "self-loop"} {
		t.Run(shape, func(t *testing.T) {
			v := newFixtureView(t)
			whole, future := testSpan(t, v.axis, 0, 20), testSpan(t, v.axis, 10, 20)
			v.defs[ownerSchemaKey{Relationship, "code"}] = PropertyDefinition{"code", Relationship, ScalarString, ScalarCardinality, UniqueScalar}
			source, target := EntityID(1), EntityID(2)
			switch shape {
			case "target":
				source, target = 2, 1
			case "self-loop":
				target = 1
			}
			record := EntityRecord{Type: "BOUND", Source: source, Target: target, Mode: LifeBound}
			commitIncident(t, v, 1,
				Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: whole},
				Operation{Kind: CreateNode, Owner: 2, Life: 1, Scope: whole},
				Operation{Kind: CreateNode, Owner: 8, Life: 1, Scope: whole},
				Operation{Kind: CreateRelationship, Owner: 3, Life: 1, Scope: whole, Record: record, Binding: LifeRecord{SourceLife: 1, TargetLife: 1}},
				Operation{Kind: Set, Owner: 3, Life: 1, Scope: whole, Name: "code", Value: String("stale"), ValueID: 1},
				Operation{Kind: CreateRelationship, Owner: 4, Life: 1, Scope: whole, Record: record, Binding: LifeRecord{SourceLife: 1, TargetLife: 1}},
				Operation{Kind: Set, Owner: 4, Life: 1, Scope: whole, Name: "code", Value: String("old"), ValueID: 2},
			)
			old := v.clone()
			commitIncident(t, v, 2,
				Operation{Kind: Close, Owner: 1, Life: 1, Scope: future},
				Operation{Kind: Reopen, Owner: 1, Life: 2, Scope: future},
				Operation{Kind: Close, Owner: 4, Life: 1, Scope: future},
			)
			binding := LifeRecord{SourceLife: 1, TargetLife: 1}
			if source == 1 {
				binding.SourceLife = 2
			}
			if target == 1 {
				binding.TargetLife = 2
			}
			commitIncident(t, v, 3,
				Operation{Kind: Reopen, Owner: 4, Life: 2, Scope: future, Binding: binding},
				Operation{Kind: Set, Owner: 4, Life: 2, Scope: future, Name: "code", Value: String("new"), ValueID: 3},
				Operation{Kind: CreateRelationship, Owner: 5, Life: 1, Scope: future, Record: EntityRecord{Type: "OTHER", Source: 2, Target: 8, Mode: LifeBound}, Binding: LifeRecord{SourceLife: 1, TargetLife: 1}},
				Operation{Kind: Set, Owner: 5, Life: 1, Scope: future, Name: "code", Value: String("stale"), ValueID: 4},
			)
			beforeClose := v.clone()
			d := commitIncident(t, v, 4, Operation{Kind: Close, Owner: 1, Life: 2, Scope: testSpan(t, v.axis, 12, 14)})
			if got := incidentPrefixes(d); !reflect.DeepEqual(got, []lifeKey{{1, 2}, {4, 2}}) {
				t.Fatalf("nonmatching endpoint life entered uniqueness footprint: %v", got)
			}
			for _, check := range []struct {
				view   *fixtureView
				id     EntityID
				at     int64
				mode   Visibility
				active bool
				life   LifeID
				code   string
			}{
				{old, 3, 13, Effective, true, 1, "stale"},
				{old, 4, 13, Effective, true, 1, "old"},
				{beforeClose, 4, 13, Effective, true, 2, "new"},
				{v, 3, 13, Effective, false, 1, ""},
				{v, 3, 13, Declared, true, 1, "stale"},
				{v, 4, 13, Effective, false, 2, ""},
				{v, 4, 13, Declared, true, 2, "new"},
				{v, 4, 15, Effective, true, 2, "new"},
				{v, 4, 5, Effective, true, 1, "old"},
				{v, 5, 13, Effective, true, 1, "stale"},
			} {
				projectIncident(t, check.view, check.id, v.axis, check.at, check.mode, check.active, check.life, check.code)
			}
			// Restoring the old endpoint life reactivates its retained edge claim;
			// omitting all incident checks would silently accept this conflict.
			commitIncident(t, v, 5, Operation{Kind: Close, Owner: 1, Life: 2, Scope: testSpan(t, v.axis, 10, 12)})
			d, err := planIncident(t, v, 6, Operation{Kind: Correct, Owner: 1, Life: 1, Scope: testSpan(t, v.axis, 10, 12), Present: true})
			if !errors.Is(err, ErrUniqueOverlap) || !reflect.DeepEqual(d, Delta{}) {
				t.Fatalf("matching old binding failed to check uniqueness: %+v, %v", d, err)
			}
			projectIncident(t, v, 3, v.axis, 11, Effective, false, 1, "")
			projectIncident(t, old, 3, v.axis, 11, Effective, true, 1, "stale")
		})
	}
}
