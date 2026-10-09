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
