package graphstate

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestMutationPlacementIsStrictBeforeLookup(t *testing.T) {
	a, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{1}, Profile: temporal.ProfileIntegerZ, Version: 1, Reference: "fixture", CanonicalUnit: "microsecond"}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	empty, _ := temporal.Empty(a)
	unplaced, _ := temporal.Unplaced(a)
	r, _ := state.NewRevision(1, 0)
	for _, test := range []struct {
		scope temporal.Scope
		want  error
	}{{temporal.Scope{}, ErrValidityRequired}, {empty, ErrEmptyMutation}, {unplaced, ErrUnsupported}} {
		if _, err := Plan(context.Background(), nil, []Operation{{Kind: CreateNode, Owner: 1, Life: 1, Scope: test.scope}}, r, Limits{}); !errors.Is(err, test.want) {
			t.Fatal(err)
		}
	}
}

// TestEndIsItsOwnCellKind: End writes a Presence cell that never merges with an
// adjacent Retract (Close) cell of the same revision and is told apart by
// IsEndCell; a caller revision already carrying EndProvenance is refused, and
// the kind after End is unknown.
func TestEndIsItsOwnCellKind(t *testing.T) {
	v := newFixtureView(t)
	all, _ := temporal.All(v.axis)
	commitOps(t, v, 1, Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: all})
	span := func(from, to int64) temporal.Scope { return testSpan(t, v.axis, from, to) }
	tail := func(from int64) temporal.Scope {
		s, err := temporal.Span(v.axis, mustFinite(t, testPosition(t, v.axis, from)), temporal.PositiveInfinity(), temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	d := commitOps(t, v, 2,
		Operation{Kind: Close, Owner: 1, Life: 1, Scope: span(10, 20)},
		Operation{Kind: End, Owner: 1, Life: 1, Scope: tail(20)})
	var retract, end int
	for _, p := range d.Patches {
		if p.Key.Kind != Presence {
			continue
		}
		for _, pc := range p.State.Pieces() {
			switch {
			case pc.Cell().Present():
			case IsEndCell(pc.Cell()):
				end++
			default:
				retract++
			}
		}
	}
	if retract != 1 || end != 1 {
		t.Fatalf("adjacent Retract and End cells: retract=%d end=%d (must stay two pieces)", retract, end)
	}
	r, _ := state.NewRevision(3, EndProvenance)
	if _, err := Plan(context.Background(), v, []Operation{{Kind: End, Owner: 1, Life: 1, Scope: tail(50)}}, r, Limits{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("provenance bit taken by the caller: %v", err)
	}
	r, _ = state.NewRevision(3, 0)
	if _, err := Plan(context.Background(), v, []Operation{{Kind: End + 1, Owner: 1, Life: 1, Scope: tail(50)}}, r, Limits{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("unknown kind after End: %v", err)
	}
}

func mustFinite(t testing.TB, p temporal.Position) temporal.Bound {
	b, err := temporal.FiniteBound(p, true)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
