package graphstate

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func retainedScope(t *testing.T, id byte, bytes int) Scalar {
	t.Helper()
	a, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{id}, Profile: temporal.ProfileIntegerZ, Version: 1, Reference: strings.Repeat("r", bytes), CanonicalUnit: "us"}, temporal.Limits{MaxDescriptorBytes: 40000})
	if err != nil {
		t.Fatal(err)
	}
	scope, err := temporal.All(a)
	if err != nil {
		t.Fatal(err)
	}
	return ScopeValue(scope)
}
func retainedLimits(read, delta int) Limits {
	l := Limits{MaxReadBytes: read, MaxDeltaBytes: delta}
	l.Component.Temporal.MaxDescriptorBytes = 40000
	return l
}
func retainedFixture(t *testing.T, values ...Scalar) (*fixtureView, []Operation) {
	t.Helper()
	v := newFixtureView(t)
	all, _ := temporal.All(v.axis)
	v.defs[ownerSchemaKey{Node, "scalar"}] = PropertyDefinition{Name: "scalar", Owner: Node, Type: ScalarScope, Cardinality: ScalarCardinality}
	v.defs[ownerSchemaKey{Node, "set"}] = PropertyDefinition{Name: "set", Owner: Node, Type: ScalarScope, Cardinality: SetCardinality}
	ops := []Operation{{Kind: CreateNode, Owner: 1, Life: 1, Scope: all}}
	for j, value := range values {
		kind, name := Set, "scalar"
		if j > 0 {
			kind, name = Add, "set"
		}
		ops = append(ops, Operation{Kind: kind, Owner: 1, Life: 1, Scope: all, Name: name, Value: value, ValueID: ValueID(j + 1)})
	}
	return v, ops
}
func TestScopeValueRetainedBytesDoNotChangeEqualityKey(t *testing.T) {
	value := retainedScope(t, 2, 32000)
	l := retainedLimits(8192, 8192)
	key, err := value.EqualityKey(l)
	if err != nil || len(key) != 54 {
		t.Fatal("typed key changed", len(key), err)
	}
	if same, err := value.Equal(value, l); err != nil || !same {
		t.Fatal(same, err)
	}
	scope, _ := value.Scope()
	want := len(key) + axisBytes(scope.Axis())

	if n := value.retainedBytes(len(key)); n != want {
		t.Fatal(n, want)
	}
	// Per-value and aggregate limits share the same charge/output ledgers.
	resolved, _ := retainedLimits(want, want).resolve()
	for _, budget := range []int{want, want - 1} {
		resolved.MaxReadBytes = budget
		resolved.MaxDeltaBytes = budget
		e := engine{ctx: t.Context(), view: newFixtureView(t), id: ViewID{1}, graph: GraphID{1}, limits: resolved}
		readErr := e.charge(1, want)
		deltaErr := e.output(want)
		if budget == want {
			if readErr != nil || deltaErr != nil {
				t.Fatal(readErr, deltaErr)
			}
		} else {
			if !errors.Is(readErr, ErrResourceLimit) || !errors.Is(deltaErr, ErrResourceLimit) {
				t.Fatal(readErr, deltaErr)
			}
		}
	}

}
func TestPlanRetainedScopeValueRefusesAtomically(t *testing.T) {
	rev, _ := state.NewRevision(1, 1)
	cases := []struct {
		name      string
		values    []Scalar
		limits    Limits
		aggregate bool
	}{
		{"per-value-read", []Scalar{retainedScope(t, 2, 32000)}, retainedLimits(8192, 1<<20), false},
		{"per-value-delta", []Scalar{retainedScope(t, 2, 32000)}, retainedLimits(1<<20, 8192), false},
		{"aggregate-read", []Scalar{retainedScope(t, 2, 5000), retainedScope(t, 3, 5000)}, retainedLimits(8192, 1<<20), true},
		{"aggregate-delta", []Scalar{retainedScope(t, 2, 2500), retainedScope(t, 3, 2500)}, retainedLimits(1<<20, 8192), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.aggregate {
				for _, value := range c.values {
					v, ops := retainedFixture(t, value)
					if _, err := Plan(t.Context(), v, ops, rev, c.limits); err != nil {
						t.Fatal("individual value should fit", err)
					}
				}
			}
			v, ops := retainedFixture(t, c.values...)
			d, err := Plan(t.Context(), v, ops, rev, c.limits)
			if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(d, Delta{}) {
				t.Fatalf("over-budget delta exposed: %+v %v", d, err)
			}
			if len(v.entities) != 0 || len(v.values) != 0 || len(v.components) != 0 {
				t.Fatal("failed plan changed read view")
			}
		})
	}
}
func TestPlanScopeValueReferencesChargeDefinitions(t *testing.T) {
	value := retainedScope(t, 2, 64)
	v, ops := retainedFixture(t, value)
	rev, _ := state.NewRevision(1, 1)
	l := retainedLimits(1<<20, 1<<20)
	d, err := Plan(t.Context(), v, ops, rev, l)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := value.EqualityKey(l)
	scope, _ := value.Scope()
	want := uint64(len(key) + axisBytes(scope.Axis()))
	found := false
	for _, patch := range d.Patches {
		if patch.Key.Kind == ScalarProperty {
			for _, piece := range patch.State.Pieces() {
				if piece.Cell().Present() {
					found = true
					if piece.Cell().Value().PayloadBytes() != want {
						t.Fatal("reference omits definition")
					}
				}
			}
		}
	}
	if !found {
		t.Fatal("no scalar patch")
	}
}
func TestProjectScalarAndSetScopeValuesRefuseAtomically(t *testing.T) {
	rev, _ := state.NewRevision(1, 1)
	for _, values := range [][]Scalar{{retainedScope(t, 2, 32000)}, {retainedScope(t, 2, 5000), retainedScope(t, 3, 5000)}} {
		v, ops := retainedFixture(t, values...)
		d, err := Plan(t.Context(), v, ops, rev, retainedLimits(1<<20, 1<<20))
		if err != nil {
			t.Fatal(err)
		}
		v.install(t, d)
		for _, l := range []Limits{retainedLimits(8192, 1<<20), retainedLimits(1<<20, 8192)} {
			got, err := Project(t.Context(), v, 1, testPosition(t, v.axis, 1), Effective, l)
			if !errors.Is(err, ErrResourceLimit) || !reflect.DeepEqual(got, Projection{}) {
				t.Fatalf("over-budget projection exposed: %+v %v", got, err)
			}
		}
		got, err := Project(t.Context(), v, 1, testPosition(t, v.axis, 1), Effective, retainedLimits(1<<20, 1<<20))
		if err != nil || len(got.Properties) != len(values) {
			t.Fatal("valid projection", got, err)
		}
	}
}
