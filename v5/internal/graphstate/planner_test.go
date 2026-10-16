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

// TestEndIsItsOwnCellKind: End writes the LifeEnd component (present, the life's
// handle) besides the Presence retraction; Close never writes LifeEnd, so the
// two stay distinguishable; the kind after End is unknown.
func TestEndIsItsOwnCellKind(t *testing.T) {
	v := newFixtureView(t)
	all, _ := temporal.All(v.axis)
	commitOps(t, v, 1, Operation{Kind: CreateNode, Owner: 1, Life: 1, Scope: all})
	tail, err := temporal.Span(v.axis, mustFinite(t, testPosition(t, v.axis, 20)), temporal.PositiveInfinity(), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	count := func(d Delta, kind ComponentKind) (n int) {
		for _, p := range d.Patches {
			if p.Key.Kind != kind {
				continue
			}
			for _, pc := range p.State.Pieces() {
				if pc.Cell().Present() && LifeID(pc.Cell().Value().ID()) == 1 {
					n++
				}
			}
		}
		return n
	}
	d := commitOps(t, v, 2, Operation{Kind: Close, Owner: 1, Life: 1, Scope: testSpan(t, v.axis, 10, 20)})
	if count(d, LifeEnd) != 0 {
		t.Fatal("Close wrote a LifeEnd cell")
	}
	d = commitOps(t, v, 3, Operation{Kind: End, Owner: 1, Life: 1, Scope: tail})
	if count(d, LifeEnd) != 1 {
		t.Fatalf("End wrote no LifeEnd cell: %+v", d.Patches)
	}
	assertActive(t, v, 1, 30, Effective, false)
	r, _ := state.NewRevision(4, 0)
	if _, err := Plan(context.Background(), v, []Operation{{Kind: End + 1, Owner: 1, Life: 1, Scope: tail}}, r, Limits{}); !errors.Is(err, ErrInvalidInput) {
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
