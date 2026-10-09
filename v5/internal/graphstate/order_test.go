package graphstate

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestDeterministicPlanAndTypedSetEnumeration(t *testing.T) {
	v := newFixtureView(t)
	all, _ := temporal.All(v.axis)
	v.defs[ownerSchemaKey{Node, "numbers"}] = PropertyDefinition{"numbers", Node, ScalarI64, SetCardinality, UniqueMembers}
	v.defs[ownerSchemaKey{Node, "scopes"}] = PropertyDefinition{"scopes", Node, ScalarScope, SetCardinality, UniqueNone}
	ops := []Operation{{Kind: CreateNode, Owner: 1, Life: 1, Scope: all}, {Kind: Add, Owner: 1, Life: 1, Scope: all, Name: "numbers", Value: I64(2), ValueID: 1}, {Kind: Add, Owner: 1, Life: 1, Scope: all, Name: "numbers", Value: I64(10), ValueID: 2}, {Kind: Add, Owner: 1, Life: 1, Scope: all, Name: "scopes", Value: ScopeValue(testSpan(t, v.axis, 20, 30)), ValueID: 3}, {Kind: Add, Owner: 1, Life: 1, Scope: all, Name: "scopes", Value: ScopeValue(testSpan(t, v.axis, 0, 10)), ValueID: 4}}
	r, _ := state.NewRevision(1, 0)
	first, err := Plan(t.Context(), v, ops, r, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for range 10 {
		again, err := Plan(t.Context(), v, ops, r, Limits{})
		if err != nil || !reflect.DeepEqual(first, again) {
			t.Fatal("unstable plan dependencies", err)
		}
	}
	v.install(t, first)
	var baseline Projection
	for i := range 10 {
		p, err := Project(t.Context(), v, 1, testPosition(t, v.axis, 1), Effective, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			baseline = p
		} else if !reflect.DeepEqual(baseline, p) {
			t.Fatal("page/map order affected typed set enumeration")
		}
		for _, property := range p.Properties {
			last := ""
			for j, value := range property.Members {
				key, err := value.EqualityKey(Limits{})
				if err != nil || j > 0 && key < last {
					t.Fatal(key, last, err)
				}
				last = key
			}
		}
	}
}
func TestMalformedMembershipAndBindingCannotProject(t *testing.T) {
	v := newFixtureView(t)
	all, _ := temporal.All(v.axis)
	v.defs[ownerSchemaKey{Node, "tags"}] = PropertyDefinition{"tags", Node, ScalarString, SetCardinality, UniqueNone}
	commitOps(t, v, 1, Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: all}, Operation{Kind: Add, Owner: 1, Life: 1, Scope: all, Name: "tags", Value: String("x"), ValueID: 1})
	for _, key := range []ComponentKey{{Owner: 1, Life: 1, Kind: SetMember, Name: "tags", Member: 1}, {Owner: 1, Life: 1, Kind: Label, Name: "Label"}} {
		bad := v.clone()
		s, _ := state.New(v.axis, state.Limits{})
		ref, _ := state.NewValueRef(1, 1)
		rev, _ := state.NewRevision(2, 0)
		result, _ := s.Set(all, ref, rev, state.Limits{})
		bad.components[key] = result.State()
		if _, err := Project(t.Context(), bad, 1, testPosition(t, v.axis, 1), Effective, Limits{}); !errors.Is(err, ErrContradictoryRead) {
			t.Fatal(err)
		}
	}
	bad := v.clone()
	bad.lives[lifeKey{1, 1}] = LifeRecord{Owner: 1, Life: 1, SourceLife: 1}
	if _, err := Project(t.Context(), bad, 1, testPosition(t, v.axis, 1), Effective, Limits{}); !errors.Is(err, ErrContradictoryRead) {
		t.Fatal(err)
	}
}
func TestCoreValidationErrorsAndAccessorContracts(t *testing.T) {
	v := newFixtureView(t)
	all, _ := temporal.All(v.axis)
	r, _ := state.NewRevision(1, 0)
	for _, op := range []Operation{{Kind: CreateNode, Owner: 0, Life: 1, Scope: all}, {Kind: CreateNode, Owner: 1, Life: 0, Scope: all}, {Kind: OperationKind(99), Owner: 1, Life: 1, Scope: all}, {Kind: CreateRelationship, Owner: 1, Life: 1, Scope: all, Record: EntityRecord{Type: " "}}, {Kind: AddLabel, Owner: 1, Life: 1, Scope: all, Name: ""}} {
		if _, err := Plan(t.Context(), v, []Operation{op}, r, Limits{}); !errors.Is(err, ErrInvalidInput) {
			t.Fatal(err)
		}
	}
	if _, err := Plan(t.Context(), v, []Operation{{Kind: CreateNode, Owner: 1, Life: 1, Scope: all}}, state.Revision{}, Limits{}); !errors.Is(err, state.ErrInvalidRevision) {
		t.Fatal(err)
	}
	var absentContext context.Context
	if _, err := Plan(absentContext, v, []Operation{{Kind: CreateNode, Owner: 1, Life: 1, Scope: all}}, r, Limits{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
	if _, err := Project(context.Background(), v, 1, testPosition(t, v.axis, 0), Visibility(99), Limits{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
	if _, err := Project(context.Background(), v, 1, temporal.Position{}, Effective, Limits{}); !errors.Is(err, temporal.ErrInvalidPosition) {
		t.Fatal(err)
	}
	if _, err := Project(context.Background(), v, 1, testPosition(t, v.axis, 0), Effective, Limits{MaxPages: -1}); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
	if p, err := Project(context.Background(), v, 99, testPosition(t, v.axis, 0), Effective, Limits{}); err != nil || p.Exists {
		t.Fatal(p, err)
	}
	if _, err := (Scalar{}).Equal(Null(), Limits{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
	if _, err := Null().Equal(Scalar{}, Limits{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
	if _, err := Null().Equal(Null(), Limits{MaxReadBytes: -1}); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
	for _, s := range []Scalar{Null(), Bool(true), I64(-2)} {
		if _, err := s.EqualityKey(Limits{MaxReadBytes: -1}); !errors.Is(err, ErrInvalidInput) {
			t.Fatal(err)
		}
	}
	if _, err := String("long").EqualityKey(Limits{MaxReadBytes: 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := I64(2).EqualityKey(Limits{MaxReadBytes: 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
}
