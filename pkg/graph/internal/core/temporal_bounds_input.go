package core

import (
	"fmt"
	"maps"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func updateValidStart(t updateTemporal) (types.Instant, bool) {
	if t.validEndpoints != 0 {
		return startKey(types.TemporalEndpoint{Value: t.validFrom, Finite: t.validEndpoints&types.EndpointFromFinite != 0}), true
	}
	return t.validFrom, t.hasValidFrom && t.validFrom != 0
}

// The explicit range is a reserved typed input. Scalar keys keep their old
// zero defaults, so mixing the two representations is refused before mutation.
func extractTemporalWithBounds(props map[string]any) (vf, vt, ca, tf types.Instant, presence types.EndpointPresence, filtered map[string]any, err error) {
	value, has := props["tkg_valid_bounds"]
	if !has {
		vf, vt, ca, tf, filtered, err = extractTemporal(props)
		return
	}
	r, ok := value.(types.TemporalRange)
	if !ok {
		err = fmt.Errorf("%w: tkg_valid_bounds requires types.TemporalRange", ErrInvalidTimeRange)
		return
	}
	if e := r.Validate(); e != nil {
		err = fmt.Errorf("%w: %v", ErrInvalidTimeRange, e)
		return
	}
	if _, has := props["tkg_valid_from"]; has {
		err = fmt.Errorf("%w: mixed typed/scalar valid endpoints", ErrInvalidTimeRange)
		return
	}
	if _, has := props["tkg_valid_to"]; has {
		err = fmt.Errorf("%w: mixed typed/scalar valid endpoints", ErrInvalidTimeRange)
		return
	}
	clean := maps.Clone(props)
	delete(clean, "tkg_valid_bounds")
	_, _, ca, tf, filtered, err = extractTemporal(clean)
	if err != nil {
		return
	}
	var tm types.TemporalMetadata
	if err = tm.SetValidBounds(r); err != nil {
		return
	}
	vf, vt, presence = tm.ValidFrom, tm.ValidTo, tm.ValidEndpoints
	return
}
