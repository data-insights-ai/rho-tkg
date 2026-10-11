package store

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestQueryOptsExplicitZeroAndTypedWindow(t *testing.T) {
	if (QueryOpts{}).HasValidAt() || (QueryOpts{}).HasValidRange() {
		t.Fatal("legacy defaults active")
	}
	if !(QueryOpts{ValidAtSet: true}).HasValidAt() || !(QueryOpts{ValidAt: -1}).HasValidAt() {
		t.Fatal("explicit/signed point inactive")
	}
	window := types.TemporalRange{To: types.TemporalEndpoint{Value: 0, Finite: true}}
	opts := QueryOpts{ValidWindow: new(window)}
	if !opts.HasValidRange() || ValidateQueryOpts(opts) != nil {
		t.Fatal("explicit open-start/finite0 window invalid")
	}
	invalid := types.TemporalRange{From: types.TemporalEndpoint{Value: 0, Finite: true}, To: types.TemporalEndpoint{Value: 0, Finite: true}}
	if err := ValidateQueryOpts(QueryOpts{ValidWindow: new(invalid)}); !errors.Is(err, ErrInvalidTimeRange) {
		t.Fatal("invalid typed window accepted", err)
	}
}
