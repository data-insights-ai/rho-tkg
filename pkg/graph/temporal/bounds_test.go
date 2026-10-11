package temporal

import (
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A prior Ops implementation still satisfies the unchanged interface. New
// typed correction calls must refuse its missing optional capability cleanly.
type oldBoundsOps struct{ Ops }

func TestBoundsOptionalCapabilityRefusesPriorOps(t *testing.T) {
	a := New(oldBoundsOps{})
	r := types.TemporalRange{From: types.TemporalEndpoint{Value: 0, Finite: true}}
	if _, err := a.SetNodeVersionBounds(t.Context(), 1, r, nil); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatal(err)
	}
	if _, err := a.SetRelVersionBounds(t.Context(), 1, r, nil); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatal(err)
	}
}
