package graphstate

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func commitOps(t testing.TB, v *fixtureView, revision uint64, ops ...Operation) Delta {
	t.Helper()
	r, _ := state.NewRevision(revision, 0)
	d, err := Plan(context.Background(), v, ops, r, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	v.install(t, d)
	return d
}
func assertActive(t testing.TB, v *fixtureView, id EntityID, at int64, mode Visibility, want bool) Projection {
	t.Helper()
	p, err := Project(context.Background(), v, id, testPosition(t, v.axis, at), mode, Limits{})
	if err != nil || p.Active != want {
		t.Fatal(id, at, p.Active, err)
	}
	return p
}
func TestFinalOverlaySwapAndRestoredLifeUniqueness(t *testing.T) {
	v := newFixtureView(t)
	all, _ := temporal.All(v.axis)
	v.defs["email"] = PropertyDefinition{"email", Node, ScalarString, ScalarCardinality, UniqueScalar}
	commitOps(t, v, 1, Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: all}, Operation{Kind: CreateNode, Owner: 2, Life: 1, Scope: all}, Operation{Kind: Set, Owner: 1, Life: 1, Scope: all, Name: "email", Value: String("a"), ValueID: 1}, Operation{Kind: Set, Owner: 2, Life: 1, Scope: all, Name: "email", Value: String("b"), ValueID: 2})
	old := v.clone()
	d := commitOps(t, v, 2, Operation{Kind: Set, Owner: 1, Life: 1, Scope: all, Name: "email", Value: String("b"), ValueID: 3}, Operation{Kind: Set, Owner: 2, Life: 1, Scope: all, Name: "email", Value: String("a"), ValueID: 4})
	if len(d.Patches) != 2 {
		t.Fatal(d.Patches)
	}
	before := assertActive(t, old, 1, 5, Effective, true)
	after := assertActive(t, v, 1, 5, Effective, true)
	if before.Properties[0].Scalar.Render() != "a" || after.Properties[0].Scalar.Render() != "b" {
		t.Fatal("current substituted for history")
	}
	r, _ := state.NewRevision(3, 0)
	if _, err := Plan(t.Context(), v, []Operation{{Kind: Set, Owner: 1, Life: 1, Scope: all, Name: "email", Value: String("a"), ValueID: 5}}, r, Limits{}); !errors.Is(err, ErrUniqueOverlap) {
		t.Fatal(err)
	}
	closeScope := testSpan(t, v.axis, 10, 100)
	commitOps(t, v, 4, Operation{Kind: Close, Owner: 1, Life: 1, Scope: closeScope}, Operation{Kind: Set, Owner: 2, Life: 1, Scope: closeScope, Name: "email", Value: String("b"), ValueID: 6})
	r, _ = state.NewRevision(5, 0)
	if _, err := Plan(t.Context(), v, []Operation{{Kind: Correct, Owner: 1, Life: 1, Scope: closeScope, Present: true}}, r, Limits{}); !errors.Is(err, ErrUniqueOverlap) {
		t.Fatal("restoration ignored retained property", err)
	}
	guard := false
	for _, dep := range d.Dependencies {
		if dep.Kind == UniquenessDependency {
			guard = true
		}
	}
	if !guard {
		t.Fatal("missing uniqueness predicate footprint")
	}
}
func TestLifeBoundAndIdentityReferenceObservations(t *testing.T) {
	v := newFixtureView(t)
	all, _ := temporal.All(v.axis)
	event, _ := temporal.Point(testPosition(t, v.axis, 30))
	sameTime, _ := temporal.Point(testPosition(t, v.axis, 12))
	commitOps(t, v, 1, Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: all}, Operation{Kind: CreateNode, Owner: 2, Life: 1, Scope: all}, Operation{Kind: CreateRelationship, Owner: 3, Life: 1, Scope: all, Record: EntityRecord{Type: "STATE", Source: 1, Target: 2, Mode: LifeBound}, Binding: LifeRecord{SourceLife: 1, TargetLife: 1}}, Operation{Kind: CreateRelationship, Owner: 4, Life: 1, Scope: event, Record: EntityRecord{Type: "OBSERVATION", Source: 1, Target: 2, Mode: IdentityReference}}, Operation{Kind: CreateNode, Owner: 5, Life: 1, Scope: sameTime}, Operation{Kind: CreateNode, Owner: 6, Life: 1, Scope: sameTime})
	old := v.clone()
	commitOps(t, v, 2, Operation{Kind: Close, Owner: 1, Life: 1, Scope: testSpan(t, v.axis, 20, 100)}, Operation{Kind: Reopen, Owner: 1, Life: 2, Scope: testSpan(t, v.axis, 25, 100)})
	assertActive(t, v, 3, 30, Effective, false)
	assertActive(t, v, 3, 30, Declared, true)
	obs := assertActive(t, v, 4, 30, Effective, true)
	if !obs.Endpoints[0].Known || obs.Endpoints[0].Life != 2 || obs.Endpoints[0].BoundLife != 0 {
		t.Fatal(obs.Endpoints)
	}
	assertActive(t, old, 3, 30, Effective, true)
	assertActive(t, v, 5, 12, Effective, true)
	assertActive(t, v, 6, 12, Effective, true)
	assertActive(t, v, 5, 13, Effective, false)
	commitOps(t, v, 3, Operation{Kind: Correct, Owner: 1, Life: 1, Scope: testSpan(t, v.axis, 20, 25), Present: true})
	assertActive(t, v, 3, 22, Effective, true)
	assertActive(t, v, 3, 30, Effective, false)
}
func TestRelationshipUniquenessRespectsEffectiveMasks(t *testing.T) {
	v := newFixtureView(t)
	all, _ := temporal.All(v.axis)
	v.defs["code"] = PropertyDefinition{"code", Relationship, ScalarString, ScalarCardinality, UniqueScalar}
	commitOps(t, v, 1, Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: all}, Operation{Kind: CreateNode, Owner: 2, Life: 1, Scope: all}, Operation{Kind: CreateNode, Owner: 3, Life: 1, Scope: all}, Operation{Kind: CreateRelationship, Owner: 4, Life: 1, Scope: all, Record: EntityRecord{Type: "R", Source: 1, Target: 2, Mode: LifeBound}, Binding: LifeRecord{SourceLife: 1, TargetLife: 1}}, Operation{Kind: Set, Owner: 4, Life: 1, Scope: all, Name: "code", Value: String("x"), ValueID: 1})
	future := testSpan(t, v.axis, 10, 100)
	commitOps(t, v, 2, Operation{Kind: Close, Owner: 1, Life: 1, Scope: future}, Operation{Kind: CreateRelationship, Owner: 5, Life: 1, Scope: future, Record: EntityRecord{Type: "R", Source: 2, Target: 3, Mode: LifeBound}, Binding: LifeRecord{SourceLife: 1, TargetLife: 1}}, Operation{Kind: Set, Owner: 5, Life: 1, Scope: future, Name: "code", Value: String("x"), ValueID: 2})
	r, _ := state.NewRevision(3, 0)
	if _, err := Plan(t.Context(), v, []Operation{{Kind: Correct, Owner: 1, Life: 1, Scope: future, Present: true}}, r, Limits{}); !errors.Is(err, ErrUniqueOverlap) {
		t.Fatal("restored endpoint bypassed relationship uniqueness", err)
	}
	// Declared properties remain writable while endpoint masking hides the edge.
	commitOps(t, v, 4, Operation{Kind: Set, Owner: 4, Life: 1, Scope: future, Name: "code", Value: String("y"), ValueID: 3})
	assertActive(t, v, 4, 20, Effective, false)
}
func TestTypedValuesAndPassiveScopeProperty(t *testing.T) {
	v := newFixtureView(t)
	all, _ := temporal.All(v.axis)
	v.defs["period"] = PropertyDefinition{"period", Node, ScalarScope, ScalarCardinality, UniqueNone}
	period := testSpan(t, v.axis, 50, 60)
	commitOps(t, v, 1, Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: all}, Operation{Kind: Set, Owner: 1, Life: 1, Scope: all, Name: "period", Value: ScopeValue(period), ValueID: 1})
	projected := assertActive(t, v, 1, 10, Effective, true)
	stored, ok := projected.Properties[0].Scalar.Scope()
	same, err := stored.SameSupport(period, temporal.Limits{})
	if !ok || err != nil || !same {
		t.Fatal(stored, err)
	}
	assertActive(t, v, 1, 55, Effective, true)
	zero, err := F64(math.Copysign(0, -1))
	if err != nil {
		t.Fatal(err)
	}
	positive, _ := F64(0)
	if equal, err := zero.Equal(positive, Limits{}); err != nil || !equal {
		t.Fatal(err)
	}
	if n, _ := zero.Float64Value(); math.Signbit(n) {
		t.Fatal("signed zero not canonical")
	}
	one, _ := F64(1)
	if equal, err := one.Equal(I64(1), Limits{}); err != nil || equal {
		t.Fatal("coerced I64 to F64", err)
	}
	for _, f := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := F64(f); !errors.Is(err, ErrInvalidInput) {
			t.Fatal(err)
		}
	}
	if s, ok := String("").StringValue(); !ok || s != "" {
		t.Fatal(s, ok)
	}
	if b, ok := Bool(true).BoolValue(); !ok || !b {
		t.Fatal(b, ok)
	}
	if n, ok := I64(math.MinInt64).Int64Value(); !ok || n != math.MinInt64 {
		t.Fatal(n, ok)
	}
	for _, s := range []Scalar{Null(), String("x"), Bool(false), I64(1), one, ScopeValue(period)} {
		if _, err := s.EqualityKey(Limits{}); err != nil {
			t.Fatal(err)
		}
		_ = s.Render()
	}
	if _, err := (Scalar{}).EqualityKey(Limits{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatal(err)
	}
}
func TestPageContradictionsCancellationAndBudgets(t *testing.T) {
	for _, test := range []struct {
		name string
		hook func(ComponentQuery, Cursor, ComponentPage) ComponentPage
		want error
	}{
		{"wrong view", func(_ ComponentQuery, _ Cursor, p ComponentPage) ComponentPage { p.View = ViewID{9}; return p }, ErrInvalidView},
		{"empty owned", func(q ComponentQuery, _ Cursor, p ComponentPage) ComponentPage {
			p.Owned, _ = temporal.Empty(q.Window.Axis())
			return p
		}, ErrIncompleteRead},
		{"bad complete cursor", func(_ ComponentQuery, _ Cursor, p ComponentPage) ComponentPage { p.Next = 1; return p }, ErrContradictoryRead},
		{"no progress", func(_ ComponentQuery, _ Cursor, p ComponentPage) ComponentPage { p.Complete = false; return p }, ErrIncompleteRead},
		{"context overlaps owned", func(q ComponentQuery, _ Cursor, p ComponentPage) ComponentPage {
			s, _ := state.New(q.Window.Axis(), state.Limits{})
			ref, _ := state.NewValueRef(1, 0)
			rev, _ := state.NewRevision(1, 0)
			r, _ := s.Set(q.Window, ref, rev, state.Limits{})
			p.MergeContext = r.State().Pieces()
			return p
		}, ErrContradictoryRead},
	} {
		t.Run(test.name, func(t *testing.T) {
			v := newFixtureView(t)
			v.pageHook = test.hook
			r, _ := state.NewRevision(1, 0)
			if _, err := Plan(t.Context(), v, []Operation{{Kind: CreateNode, Owner: 1, Life: 1, Scope: testSpan(t, v.axis, 0, 10)}}, r, Limits{}); !errors.Is(err, test.want) {
				t.Fatal(err)
			}
		})
	}
	v := newFixtureView(t)
	all, _ := temporal.All(v.axis)
	r, _ := state.NewRevision(1, 0)
	for _, l := range []Limits{{MaxOperations: -1}, {MaxPages: -1}, {MaxRows: -1}, {MaxReadBytes: -1}, {MaxDeltaBytes: -1}, {MaxDependencies: -1}, {MaxNameBytes: -1}} {
		if err := l.Validate(); !errors.Is(err, ErrInvalidInput) {
			t.Fatal(err)
		}
	}
	for _, l := range []Limits{{MaxReadBytes: 1}, {MaxDeltaBytes: 1}, {MaxDependencies: 1}, {MaxRows: 1}} {
		if _, err := Plan(t.Context(), v, []Operation{{Kind: CreateNode, Owner: 1, Life: 1, Scope: all}}, r, l); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(l, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Plan(ctx, v, []Operation{{Kind: CreateNode, Owner: 1, Life: 1, Scope: all}}, r, Limits{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := Plan(t.Context(), nil, []Operation{{Kind: CreateNode, Owner: 1, Life: 1, Scope: all}}, r, Limits{}); !errors.Is(err, ErrNilView) {
		t.Fatal(err)
	}
	var typedNil *fixtureView
	if _, err := Plan(t.Context(), typedNil, []Operation{{Kind: CreateNode, Owner: 1, Life: 1, Scope: all}}, r, Limits{}); !errors.Is(err, ErrNilView) {
		t.Fatal(err)
	}
	if _, err := Project(t.Context(), v, 1, testPosition(t, v.axis, 256), Effective, Limits{Component: state.Limits{Temporal: temporal.Limits{MaxMagnitudeBits: 1}}}); !errors.Is(err, temporal.ErrResourceLimit) {
		t.Fatal("missing entity skipped query validation", err)
	}
}
