package graphstate

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

// Owner and name identify independent schemas without changing property names.
type ownerSchemaKey struct {
	owner EntityKind
	name  string
}

type ownerSchemaView struct {
	*fixtureView
	definitions map[ownerSchemaKey]PropertyDefinition
	wrongOwner  bool
}

// The view looks up the exact immutable owner/name pair supplied by the caller.
func (v *ownerSchemaView) Property(_ context.Context, owner EntityKind, name string) (PropertyRead, error) {
	definition, found := v.definitions[ownerSchemaKey{owner, name}]
	if found && v.wrongOwner {
		if definition.Owner == Node {
			definition.Owner = Relationship
		} else {
			definition.Owner = Node
		}
	}
	return PropertyRead{View: v.id, Version: 1, Found: found, Record: definition}, nil
}

func (v *ownerSchemaView) cloneOwned() *ownerSchemaView {
	return &ownerSchemaView{fixtureView: v.clone(), definitions: maps.Clone(v.definitions), wrongOwner: v.wrongOwner}
}

func commitOwnedSchema(t testing.TB, v *ownerSchemaView, sequence uint64, ops ...Operation) {
	t.Helper()
	revision, err := state.NewRevision(sequence, sequence+100)
	if err != nil {
		t.Fatal(err)
	}
	delta, err := Plan(t.Context(), v, ops, revision, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	v.install(t, delta)
	v.id[0]++
}

func ownedSchemaProjection(t testing.TB, v *ownerSchemaView, owner EntityID, at int64) Projection {
	t.Helper()
	projection, err := Project(t.Context(), v, owner, testPosition(t, v.axis, at), Effective, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if !projection.Exists || !projection.Active || projection.Life != 1 {
		t.Fatalf("owner %d has unexpected lifecycle: %+v", owner, projection)
	}
	return projection
}

func assertOwnedScalar(t testing.TB, projection Projection, name string, want Scalar, present bool) {
	t.Helper()
	if !present {
		if len(projection.Properties) != 0 {
			t.Fatalf("unexpected properties: %+v", projection.Properties)
		}
		return
	}
	if len(projection.Properties) != 1 {
		t.Fatalf("exact property set: %+v", projection.Properties)
	}
	property := projection.Properties[0]
	equal, err := property.Scalar.Equal(want, Limits{})
	if err != nil || !equal || property.Name != name || property.Cardinality != ScalarCardinality || len(property.Members) != 0 {
		t.Fatalf("want scalar %s=%s, got %+v (%v)", name, want.Render(), property, err)
	}
}

func assertOwnedBoolSet(t testing.TB, projection Projection, name string, want bool) {
	t.Helper()
	if len(projection.Properties) != 1 {
		t.Fatalf("exact property set: %+v", projection.Properties)
	}
	property := projection.Properties[0]
	if property.Name != name || property.Cardinality != SetCardinality || len(property.Members) != 1 {
		t.Fatalf("want singleton bool set %s, got %+v", name, property)
	}
	value, ok := property.Members[0].BoolValue()
	if !ok || value != want {
		t.Fatalf("want bool member %v, got %+v", want, property.Members)
	}
}

func assertOwnedStringSet(t testing.TB, projection Projection, name, want string) {
	t.Helper()
	if len(projection.Properties) != 1 {
		t.Fatalf("exact property set: %+v", projection.Properties)
	}
	property := projection.Properties[0]
	if property.Name != name || property.Cardinality != SetCardinality || len(property.Members) != 1 || property.Scalar.Kind() != ScalarInvalid {
		t.Fatalf("want singleton string set %s, got %+v", name, property)
	}
	value, ok := property.Members[0].StringValue()
	if !ok || value != want {
		t.Fatalf("want string member %s, got %+v", want, property.Members)
	}
}

func TestOwnerQualifiedSchemaSameNameTypesCardinalitiesAndHistory(t *testing.T) {
	v := &ownerSchemaView{fixtureView: newFixtureView(t), definitions: map[ownerSchemaKey]PropertyDefinition{
		{Node, "shared"}:         {"shared", Node, ScalarString, ScalarCardinality, UniqueScalar},
		{Relationship, "shared"}: {"shared", Relationship, ScalarBool, SetCardinality, UniqueMembers},
	}}
	whole, middle := testSpan(t, v.axis, 0, 10), testSpan(t, v.axis, 3, 7)
	commitOwnedSchema(t, v, 1,
		Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: whole},
		Operation{Kind: CreateNode, Owner: 2, Life: 1, Scope: whole},
		Operation{Kind: Set, Owner: 1, Life: 1, Scope: whole, Name: "shared", Value: String("before"), ValueID: 1},
		Operation{Kind: CreateRelationship, Owner: 3, Life: 1, Scope: whole, Record: EntityRecord{Type: "R", Source: 1, Target: 2, Mode: LifeBound}, Binding: LifeRecord{SourceLife: 1, TargetLife: 1}},
		Operation{Kind: Add, Owner: 3, Life: 1, Scope: whole, Name: "shared", Value: Bool(true), ValueID: 2},
	)
	old := v.cloneOwned()
	commitOwnedSchema(t, v, 2,
		Operation{Kind: Set, Owner: 1, Life: 1, Scope: middle, Name: "shared", Value: String("after"), ValueID: 3},
		Operation{Kind: Remove, Owner: 3, Life: 1, Scope: middle, Name: "shared", Value: Bool(true), ValueID: 4},
		Operation{Kind: Add, Owner: 3, Life: 1, Scope: middle, Name: "shared", Value: Bool(false), ValueID: 5},
	)
	assertOwnedScalar(t, ownedSchemaProjection(t, old, 1, 5), "shared", String("before"), true)
	assertOwnedBoolSet(t, ownedSchemaProjection(t, old, 3, 5), "shared", true)
	assertOwnedScalar(t, ownedSchemaProjection(t, v, 1, 5), "shared", String("after"), true)
	assertOwnedBoolSet(t, ownedSchemaProjection(t, v, 3, 5), "shared", false)
	assertOwnedScalar(t, ownedSchemaProjection(t, v, 1, 8), "shared", String("before"), true)
	assertOwnedBoolSet(t, ownedSchemaProjection(t, v, 3, 8), "shared", true)
	assertOwnedScalar(t, ownedSchemaProjection(t, v, 2, 5), "shared", Scalar{}, false)
}

func TestOwnerQualifiedSchemaUniquenessDoesNotMixNodeAndRelationship(t *testing.T) {
	v := &ownerSchemaView{fixtureView: newFixtureView(t), definitions: map[ownerSchemaKey]PropertyDefinition{
		{Node, "token"}:         {"token", Node, ScalarString, ScalarCardinality, UniqueScalar},
		{Relationship, "token"}: {"token", Relationship, ScalarString, ScalarCardinality, UniqueScalar},
	}}
	whole, middle := testSpan(t, v.axis, 0, 10), testSpan(t, v.axis, 3, 7)
	commitOwnedSchema(t, v, 1,
		Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: whole},
		Operation{Kind: CreateNode, Owner: 2, Life: 1, Scope: whole},
		Operation{Kind: CreateRelationship, Owner: 3, Life: 1, Scope: whole, Record: EntityRecord{Type: "R", Source: 1, Target: 2, Mode: LifeBound}, Binding: LifeRecord{SourceLife: 1, TargetLife: 1}},
		Operation{Kind: CreateRelationship, Owner: 4, Life: 1, Scope: whole, Record: EntityRecord{Type: "R", Source: 1, Target: 2, Mode: LifeBound}, Binding: LifeRecord{SourceLife: 1, TargetLife: 1}},
		Operation{Kind: Set, Owner: 1, Life: 1, Scope: whole, Name: "token", Value: String("same"), ValueID: 1},
		Operation{Kind: Set, Owner: 3, Life: 1, Scope: whole, Name: "token", Value: String("same"), ValueID: 2},
	)
	old := v.cloneOwned()
	revision, err := state.NewRevision(2, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, owner := range []EntityID{2, 4} {
		delta, err := Plan(t.Context(), v, []Operation{{Kind: Set, Owner: owner, Life: 1, Scope: middle, Name: "token", Value: String("same"), ValueID: 3}}, revision, Limits{})
		if !errors.Is(err, ErrUniqueOverlap) || len(delta.Patches) != 0 || len(delta.Values) != 0 {
			t.Fatalf("owner %d duplicate must fail atomically: %+v, %v", owner, delta, err)
		}
	}
	commitOwnedSchema(t, v, 3,
		Operation{Kind: Unset, Owner: 1, Life: 1, Scope: middle, Name: "token"},
		Operation{Kind: Set, Owner: 2, Life: 1, Scope: middle, Name: "token", Value: String("same"), ValueID: 3},
	)
	assertOwnedScalar(t, ownedSchemaProjection(t, old, 1, 5), "token", String("same"), true)
	assertOwnedScalar(t, ownedSchemaProjection(t, old, 2, 5), "token", Scalar{}, false)
	assertOwnedScalar(t, ownedSchemaProjection(t, v, 1, 5), "token", Scalar{}, false)
	assertOwnedScalar(t, ownedSchemaProjection(t, v, 2, 5), "token", String("same"), true)
	assertOwnedScalar(t, ownedSchemaProjection(t, v, 3, 5), "token", String("same"), true)
	assertOwnedScalar(t, ownedSchemaProjection(t, v, 4, 5), "token", Scalar{}, false)
}

func TestOwnerQualifiedSchemaSameValueDifferentCardinalitySupersetAndHistory(t *testing.T) {
	v := &ownerSchemaView{fixtureView: newFixtureView(t), definitions: map[ownerSchemaKey]PropertyDefinition{
		{Node, "shared"}:         {"shared", Node, ScalarString, ScalarCardinality, UniqueScalar},
		{Relationship, "shared"}: {"shared", Relationship, ScalarString, SetCardinality, UniqueMembers},
	}}
	whole, middle := testSpan(t, v.axis, 0, 10), testSpan(t, v.axis, 3, 7)
	commitOwnedSchema(t, v, 1,
		Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: whole},
		Operation{Kind: CreateNode, Owner: 2, Life: 1, Scope: whole},
		Operation{Kind: CreateRelationship, Owner: 3, Life: 1, Scope: whole, Record: EntityRecord{Type: "R", Source: 1, Target: 2, Mode: LifeBound}, Binding: LifeRecord{SourceLife: 1, TargetLife: 1}},
		Operation{Kind: Set, Owner: 1, Life: 1, Scope: whole, Name: "shared", Value: String("before"), ValueID: 1},
		Operation{Kind: Add, Owner: 3, Life: 1, Scope: whole, Name: "shared", Value: String("before"), ValueID: 2},
	)
	old := v.cloneOwned()
	// The provider deliberately returns foreign-owner candidates as a complete
	// superset. A same-value revision must not mistake their cardinality for ours.
	commitOwnedSchema(t, v, 2, Operation{Kind: Set, Owner: 1, Life: 1, Scope: middle, Name: "shared", Value: String("before"), ValueID: 3})
	commitOwnedSchema(t, v, 3,
		Operation{Kind: Set, Owner: 1, Life: 1, Scope: middle, Name: "shared", Value: String("after"), ValueID: 3},
		Operation{Kind: Remove, Owner: 3, Life: 1, Scope: middle, Name: "shared", Value: String("before"), ValueID: 4},
		Operation{Kind: Add, Owner: 3, Life: 1, Scope: middle, Name: "shared", Value: String("after"), ValueID: 5},
	)
	assertOwnedScalar(t, ownedSchemaProjection(t, old, 1, 5), "shared", String("before"), true)
	assertOwnedStringSet(t, ownedSchemaProjection(t, old, 3, 5), "shared", "before")
	assertOwnedScalar(t, ownedSchemaProjection(t, v, 1, 5), "shared", String("after"), true)
	assertOwnedStringSet(t, ownedSchemaProjection(t, v, 3, 5), "shared", "after")
	assertOwnedScalar(t, ownedSchemaProjection(t, v, 1, 8), "shared", String("before"), true)
	assertOwnedStringSet(t, ownedSchemaProjection(t, v, 3, 8), "shared", "before")
}

type malformedOwnerCandidates struct {
	*ownerSchemaView
	missingOwner bool
}

func (v malformedOwnerCandidates) UniqueCandidates(_ context.Context, p UniquePredicate, _ Cursor, _ ReadBudget) (ClaimPage, error) {
	owner, kind, member := EntityID(1), SetMember, ValueID(1)
	if p.Definition.Owner == Relationship {
		owner, kind, member = 3, ScalarProperty, 0
	}
	if v.missingOwner {
		owner = 99
		if p.Definition.Cardinality == ScalarCardinality {
			kind, member = ScalarProperty, 0
		} else {
			kind, member = SetMember, 1
		}
	}
	claim := UniqueClaim{Owner: owner, Life: 1, Key: ComponentKey{Owner: owner, Life: 1, Kind: kind, Name: p.Definition.Name, Member: member}}
	return ClaimPage{View: v.id, Version: 1, Claims: []UniqueClaim{claim}, Complete: true}, nil
}

func TestOwnerQualifiedSchemaRejectsMalformedOwnKindCandidates(t *testing.T) {
	v := &ownerSchemaView{fixtureView: newFixtureView(t), definitions: map[ownerSchemaKey]PropertyDefinition{
		{Node, "shared"}:         {"shared", Node, ScalarString, ScalarCardinality, UniqueScalar},
		{Relationship, "shared"}: {"shared", Relationship, ScalarString, SetCardinality, UniqueMembers},
	}}
	whole := testSpan(t, v.axis, 0, 10)
	commitOwnedSchema(t, v, 1,
		Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: whole},
		Operation{Kind: CreateNode, Owner: 2, Life: 1, Scope: whole},
		Operation{Kind: CreateRelationship, Owner: 3, Life: 1, Scope: whole, Record: EntityRecord{Type: "R", Source: 1, Target: 2, Mode: LifeBound}, Binding: LifeRecord{SourceLife: 1, TargetLife: 1}},
		Operation{Kind: Set, Owner: 1, Life: 1, Scope: whole, Name: "shared", Value: String("before"), ValueID: 1},
		Operation{Kind: Add, Owner: 3, Life: 1, Scope: whole, Name: "shared", Value: String("before"), ValueID: 2},
	)
	revision, err := state.NewRevision(2, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, missing := range []bool{false, true} {
		for _, op := range []Operation{
			{Kind: Set, Owner: 1, Life: 1, Scope: whole, Name: "shared", Value: String("before"), ValueID: 3},
			{Kind: Add, Owner: 3, Life: 1, Scope: whole, Name: "shared", Value: String("before"), ValueID: 3},
		} {
			delta, err := Plan(t.Context(), malformedOwnerCandidates{v, missing}, []Operation{op}, revision, Limits{})
			if !errors.Is(err, ErrContradictoryRead) || !reflect.DeepEqual(delta, Delta{}) {
				t.Fatalf("malformed same-kind or unknown-owner candidate must fail atomically: %+v, %v", delta, err)
			}
		}
	}
	assertOwnedScalar(t, ownedSchemaProjection(t, v, 1, 5), "shared", String("before"), true)
	assertOwnedStringSet(t, ownedSchemaProjection(t, v, 3, 5), "shared", "before")
}

func TestOwnerQualifiedSchemaRejectsWrongOwnerProviderAtEveryLayer(t *testing.T) {
	v := &ownerSchemaView{fixtureView: newFixtureView(t), definitions: map[ownerSchemaKey]PropertyDefinition{
		{Node, "shared"}:         {"shared", Node, ScalarString, ScalarCardinality, UniqueNone},
		{Relationship, "shared"}: {"shared", Relationship, ScalarString, ScalarCardinality, UniqueNone},
	}}
	whole := testSpan(t, v.axis, 0, 10)
	commitOwnedSchema(t, v, 1,
		Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: whole},
		Operation{Kind: CreateNode, Owner: 2, Life: 1, Scope: whole},
		Operation{Kind: CreateRelationship, Owner: 3, Life: 1, Scope: whole, Record: EntityRecord{Type: "R", Source: 1, Target: 2, Mode: LifeBound}, Binding: LifeRecord{SourceLife: 1, TargetLife: 1}},
		Operation{Kind: Set, Owner: 1, Life: 1, Scope: whole, Name: "shared", Value: String("before"), ValueID: 1},
		Operation{Kind: Set, Owner: 3, Life: 1, Scope: whole, Name: "shared", Value: String("before"), ValueID: 2},
	)
	bad := v.cloneOwned()
	bad.wrongOwner = true
	revision, err := state.NewRevision(2, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, owner := range []EntityID{1, 3} {
		delta, err := Plan(t.Context(), bad, []Operation{{Kind: Set, Owner: owner, Life: 1, Scope: whole, Name: "shared", Value: String("after"), ValueID: 3}}, revision, Limits{})
		if !errors.Is(err, ErrSchemaMismatch) || !reflect.DeepEqual(delta, Delta{}) {
			t.Fatalf("wrong-owner Plan must fail closed: %+v, %v", delta, err)
		}
		projection, err := Project(t.Context(), bad, owner, testPosition(t, bad.axis, 5), Effective, Limits{})
		if !errors.Is(err, ErrSchemaMismatch) || !reflect.DeepEqual(projection, Projection{}) {
			t.Fatalf("wrong-owner Project must fail closed: %+v, %v", projection, err)
		}
		assertOwnedScalar(t, ownedSchemaProjection(t, v, owner, 5), "shared", String("before"), true)
	}
}

func TestOwnerQualifiedSchemaDependenciesRetainKindIncludingAbsentSchema(t *testing.T) {
	v := &ownerSchemaView{fixtureView: newFixtureView(t), definitions: map[ownerSchemaKey]PropertyDefinition{
		{Node, "shared"}:         {"shared", Node, ScalarString, ScalarCardinality, UniqueNone},
		{Relationship, "shared"}: {"shared", Relationship, ScalarBool, SetCardinality, UniqueNone},
	}}
	limits, err := (Limits{}).resolve()
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []EntityKind{Node, Relationship} {
		for _, name := range []string{"shared", "missing"} {
			engine, err := start(t.Context(), v, limits, state.Revision{})
			if err != nil {
				t.Fatal(err)
			}
			definition, err := engine.property(kind, name)
			absent := name == "missing"
			if absent && !errors.Is(err, ErrSchemaMismatch) || !absent && (err != nil || definition.Owner != kind) {
				t.Fatalf("%d/%s: %+v, %v", kind, name, definition, err)
			}
			if len(engine.delta.Dependencies) != 1 {
				t.Fatal("exact schema dependency set", engine.delta.Dependencies)
			}
			dependency := engine.delta.Dependencies[0]
			if dependency.Kind != SchemaDependency || dependency.OwnerKind != kind || dependency.Name != name || dependency.Absent != absent || dependency.View != v.id || dependency.Version != 1 {
				t.Fatalf("schema qualification lost: %+v", dependency)
			}
		}
	}
	for _, kind := range []EntityKind{0, 99} {
		engine, err := start(t.Context(), v, limits, state.Revision{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := engine.property(kind, "shared"); !errors.Is(err, ErrInvalidInput) || len(engine.delta.Dependencies) != 0 {
			t.Fatalf("invalid owner kind must not become a schema read: %v", err)
		}
	}
}
