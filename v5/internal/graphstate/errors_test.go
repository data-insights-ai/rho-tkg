package graphstate

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type faultView struct {
	ReadView
	stage   string
	failure error
}

func (v faultView) Entity(c context.Context, id EntityID) (EntityRead, error) {
	if v.stage == "entity" {
		return EntityRead{}, v.failure
	}
	return v.ReadView.Entity(c, id)
}
func (v faultView) Life(c context.Context, id EntityID, l LifeID) (LifeRead, error) {
	if v.stage == "life" {
		return LifeRead{}, v.failure
	}
	return v.ReadView.Life(c, id, l)
}
func (v faultView) Property(c context.Context, owner EntityKind, n string) (PropertyRead, error) {
	if v.stage == "schema" {
		return PropertyRead{}, v.failure
	}
	return v.ReadView.Property(c, owner, n)
}
func (v faultView) Value(c context.Context, id ValueID) (ValueRead, error) {
	if v.stage == "value" {
		return ValueRead{}, v.failure
	}
	return v.ReadView.Value(c, id)
}
func (v faultView) ValueIdentity(c context.Context, x Scalar) (ValueRead, error) {
	if v.stage == "identity" {
		return ValueRead{}, v.failure
	}
	return v.ReadView.ValueIdentity(c, x)
}
func (v faultView) ComponentPage(c context.Context, q ComponentQuery, k Cursor, b ReadBudget) (ComponentPage, error) {
	if v.stage == "component" {
		return ComponentPage{}, v.failure
	}
	return v.ReadView.ComponentPage(c, q, k, b)
}
func (v faultView) ComponentKeys(c context.Context, q KeyPredicate, k Cursor, b ReadBudget) (KeyPage, error) {
	if v.stage == "prefix" {
		return KeyPage{}, v.failure
	}
	return v.ReadView.ComponentKeys(c, q, k, b)
}
func (v faultView) UniqueCandidates(c context.Context, q UniquePredicate, k Cursor, b ReadBudget) (ClaimPage, error) {
	if v.stage == "unique" {
		return ClaimPage{}, v.failure
	}
	return v.ReadView.UniqueCandidates(c, q, k, b)
}
func (v faultView) IncidentRelationships(c context.Context, q IncidentPredicate, k Cursor, b ReadBudget) (EntityPage, error) {
	if v.stage == "incident" {
		return EntityPage{}, v.failure
	}
	return v.ReadView.IncidentRelationships(c, q, k, b)
}
func TestReadErrorsPropagateAtEveryLayer(t *testing.T) {
	v := newFixtureView(t)
	all, _ := temporal.All(v.axis)
	v.defs[ownerSchemaKey{Node, "x"}] = PropertyDefinition{"x", Node, ScalarString, ScalarCardinality, UniqueScalar}
	commitOps(t, v, 1, Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: all}, Operation{Kind: Set, Owner: 1, Life: 1, Scope: all, Name: "x", Value: String("a"), ValueID: 1})
	failure := errors.New("injected storage failure")
	r, _ := state.NewRevision(2, 0)
	for _, stage := range []string{"entity", "life", "schema", "value", "component", "prefix"} {
		if _, err := Project(t.Context(), faultView{v, stage, failure}, 1, testPosition(t, v.axis, 5), Effective, Limits{}); !errors.Is(err, failure) {
			t.Fatal(stage, err)
		}
	}
	for _, stage := range []string{"entity", "life", "schema", "value", "identity", "component", "prefix", "unique"} {
		if _, err := Plan(t.Context(), faultView{v, stage, failure}, []Operation{{Kind: Set, Owner: 1, Life: 1, Scope: all, Name: "x", Value: String("b"), ValueID: 2}}, r, Limits{}); stage != "value" && !errors.Is(err, failure) {
			t.Fatal(stage, err)
		}
	}
	if _, err := Plan(t.Context(), faultView{v, "incident", failure}, []Operation{{Kind: Close, Owner: 1, Life: 1, Scope: all}}, r, Limits{}); !errors.Is(err, failure) {
		t.Fatal(err)
	}
}
func TestFailClosedRecordSchemaAndValueShapes(t *testing.T) {
	v := newFixtureView(t)
	all, _ := temporal.All(v.axis)
	v.defs[ownerSchemaKey{Node, "x"}] = PropertyDefinition{"x", Node, ScalarString, ScalarCardinality, UniqueNone}
	commitOps(t, v, 1, Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: all}, Operation{Kind: Set, Owner: 1, Life: 1, Scope: all, Name: "x", Value: String("a"), ValueID: 1})
	for _, change := range []func(*fixtureView){
		func(v *fixtureView) { r := v.entities[1]; r.Kind = EntityKind(99); v.entities[1] = r },
		func(v *fixtureView) {
			r := v.entities[1]
			r.Kind = Relationship
			r.Source = 1
			r.Target = 1
			r.Type = "R"
			r.Mode = ReferenceMode(99)
			v.entities[1] = r
		},
		func(v *fixtureView) { v.lives[lifeKey{1, 1}] = LifeRecord{Owner: 2, Life: 1} },
		func(v *fixtureView) {
			s := v.components[ComponentKey{Owner: 1, Life: 1, Kind: ScalarProperty, Name: "x"}]
			v.components[ComponentKey{Owner: 1, Life: 1, Kind: ComponentKind(99), Name: "x"}] = s
		},
		func(v *fixtureView) { v.values[1] = I64(1) },
		func(v *fixtureView) {
			d := v.defs[ownerSchemaKey{Node, "x"}]
			d.Cardinality = SetCardinality
			v.defs[ownerSchemaKey{Node, "x"}] = d
		},
	} {
		bad := v.clone()
		change(bad)
		if _, err := Project(t.Context(), bad, 1, testPosition(t, bad.axis, 5), Effective, Limits{}); !errors.Is(err, ErrContradictoryRead) && !errors.Is(err, ErrSchemaMismatch) {
			t.Fatal(err)
		}
	}
	r, _ := state.NewRevision(2, 0)
	for _, def := range []PropertyDefinition{{"x", EntityKind(99), ScalarString, ScalarCardinality, UniqueNone}, {"wrong", Node, ScalarString, ScalarCardinality, UniqueNone}, {"x", Node, ScalarKind(99), ScalarCardinality, UniqueNone}} {
		bad := v.clone()
		bad.defs[ownerSchemaKey{Node, "x"}] = def
		if _, err := Plan(t.Context(), bad, []Operation{{Kind: Set, Owner: 1, Life: 1, Scope: all, Name: "x", Value: String("b"), ValueID: 2}}, r, Limits{}); !errors.Is(err, ErrSchemaMismatch) {
			t.Fatal(err)
		}
	}
	bad := v.clone()
	d := bad.defs[ownerSchemaKey{Node, "x"}]
	d.Unique = UniqueMode(99)
	bad.defs[ownerSchemaKey{Node, "x"}] = d
	if _, err := Plan(t.Context(), bad, []Operation{{Kind: Set, Owner: 1, Life: 1, Scope: all, Name: "x", Value: String("b"), ValueID: 2}}, r, Limits{}); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	for _, op := range []Operation{{Kind: Set, Owner: 1, Life: 1, Scope: all, Name: "missing", Value: String("b"), ValueID: 2}, {Kind: Set, Owner: 1, Life: 2, Scope: all, Name: "x", Value: String("b"), ValueID: 2}, {Kind: CreateNode, Owner: 1, Life: 1, Scope: all}} {
		_, err := Plan(t.Context(), v, []Operation{op}, r, Limits{})
		if !errors.Is(err, ErrSchemaMismatch) && !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrAlreadyExists) {
			t.Fatal(err)
		}
	}
	if _, err := Plan(t.Context(), v, []Operation{{Kind: Set, Owner: 1, Life: 1, Scope: all, Name: "x", Value: String("b"), ValueID: 1}}, r, Limits{}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatal("rebound payload ID", err)
	}
}
func TestUnboundedForeignAxisAndCoverageRevisionSplits(t *testing.T) {
	v := newFixtureView(t)
	all, _ := temporal.All(v.axis)
	otherDesc := v.axis.Descriptor()
	otherDesc.Reference = "foreign"
	foreign, err := temporal.NewAxis(otherDesc, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	v.pageHook = func(_ ComponentQuery, _ Cursor, p ComponentPage) ComponentPage {
		p.Owned, _ = temporal.All(foreign)
		p.Data, _ = state.New(foreign, state.Limits{})
		return p
	}
	r, _ := state.NewRevision(1, 0)
	if _, err := Plan(t.Context(), v, []Operation{{Kind: CreateNode, Owner: 1, Life: 1, Scope: all}}, r, Limits{}); !errors.Is(err, temporal.ErrAxisMismatch) {
		t.Fatal("infinity bounds bypassed axis", err)
	}
	v = newFixtureView(t)
	commitOps(t, v, 1, Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: testSpan(t, v.axis, 0, 10)})
	commitOps(t, v, 2, Operation{Kind: Correct, Owner: 1, Life: 1, Scope: testSpan(t, v.axis, 3, 7), Present: true})
	v.defs[ownerSchemaKey{Node, "x"}] = PropertyDefinition{"x", Node, ScalarString, ScalarCardinality, UniqueNone}
	commitOps(t, v, 3, Operation{Kind: Set, Owner: 1, Life: 1, Scope: testSpan(t, v.axis, 0, 10), Name: "x", Value: String("a"), ValueID: 1})
	assertActive(t, v, 1, 5, Effective, true)
}
