package graphstate

import (
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestV1DeclarationsRemainIndependentOfEqualSupportAndLifecycle(t *testing.T) {
	v := newFixtureView(t)
	point, err := temporal.Point(testPosition(t, v.axis, 0))
	if err != nil {
		t.Fatal(err)
	}
	half := testSpan(t, v.axis, 0, 1)
	equal, err := point.SameSupport(half, temporal.Limits{})
	if err != nil || !equal {
		t.Fatal(equal, err)
	}
	commitOps(t, v, 1,
		Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: point, Record: EntityRecord{Interpretation: InterpretationOccurrence, TemporalRole: TemporalRoleOccurrenceTime}},
		Operation{Kind: CreateNode, Owner: 2, Life: 1, Scope: half, Record: EntityRecord{Interpretation: InterpretationState, TemporalRole: TemporalRoleValidity}},
		Operation{Kind: CreateRelationship, Owner: 3, Life: 1, Scope: point, Record: EntityRecord{Type: "PRECEDES", Source: 1, Target: 2, Mode: IdentityReference, Interpretation: InterpretationAssertedRelation, TemporalRole: TemporalRoleCausalOrder}},
	)
	old := v.clone()
	commitOps(t, v, 2, Operation{Kind: Close, Owner: 1, Life: 1, Scope: point}, Operation{Kind: Close, Owner: 3, Life: 1, Scope: point})
	commitOps(t, v, 3, Operation{Kind: Reopen, Owner: 1, Life: 2, Scope: half}, Operation{Kind: Reopen, Owner: 3, Life: 2, Scope: half})
	for _, snap := range []*fixtureView{old, v} {
		for id, want := range map[EntityID]EntityRecord{1: old.entities[1], 2: old.entities[2], 3: old.entities[3]} {
			p, err := Project(t.Context(), snap, id, testPosition(t, v.axis, 0), Effective, Limits{})
			if err != nil || !p.Active || p.Record != want {
				t.Fatal(id, p, err)
			}
			p, err = Project(t.Context(), snap, id, testPosition(t, v.axis, 1), Effective, Limits{})
			if err != nil || p.Active || p.Record != want {
				t.Fatal("point persisted or declaration lost", id, p, err)
			}
		}
	}
	for i := Interpretation(0); i <= InterpretationAssertedRelation; i++ {
		if !i.Valid() {
			t.Fatal(i)
		}
	}
	for r := TemporalRole(0); r <= TemporalRoleCausalOrder; r++ {
		if !r.Valid() {
			t.Fatal(r)
		}
	}
	if Interpretation(255).Valid() || TemporalRole(255).Valid() {
		t.Fatal("unknown declaration admitted")
	}
}

func TestV1DeclarationRefusalsLeaveInputAndDeltaExact(t *testing.T) {
	v := newFixtureView(t)
	scope := testSpan(t, v.axis, 0, 10)
	rev, _ := state.NewRevision(1, 0)
	for _, r := range []EntityRecord{{Interpretation: 255}, {TemporalRole: 255}} {
		before := v.clone()
		d, err := Plan(t.Context(), v, []Operation{{Kind: CreateNode, Owner: 1, Life: 1, Scope: scope, Record: r}}, rev, Limits{})
		if !errors.Is(err, ErrUnsupported) || !reflect.DeepEqual(d, Delta{}) || !reflect.DeepEqual(v, before) {
			t.Fatal(d, err)
		}
	}
	commitOps(t, v, 1, Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: scope})
	for _, kind := range []OperationKind{Reopen, Close, Correct, AddLabel, Set} {
		d, err := Plan(t.Context(), v, []Operation{{Kind: kind, Owner: 1, Life: 1, Scope: scope, Record: EntityRecord{Interpretation: InterpretationState}}}, rev, Limits{})
		if !errors.Is(err, ErrInvalidInput) || !reflect.DeepEqual(d, Delta{}) {
			t.Fatal(kind, d, err)
		}
	}
	broken := v.clone()
	r := broken.entities[1]
	r.TemporalRole = 255
	broken.entities[1] = r
	p, err := Project(t.Context(), broken, 1, testPosition(t, v.axis, 1), Effective, Limits{})
	if !errors.Is(err, ErrContradictoryRead) || !reflect.DeepEqual(p, Projection{}) {
		t.Fatal(p, err)
	}
}
