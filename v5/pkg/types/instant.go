// Package types contains public graph compatibility value types.
package types

import "github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"

// Instant is the signed int64 millisecond codec used for default-axis
// compatibility. Its value carries no axis identity, clock or epoch. The caller
// must supply the axis explicitly when binding it to a temporal position.
// Zero is an ordinary coordinate, never absence or infinity. Its finite codec
// range does not narrow the mathematical domain of an axis.
type Instant int64

// InstantPosition binds value to the caller's explicitly supplied millisecond
// axis, preserving its reference and Z/Q profile. This selects neither a global
// default nor a Unix origin. It performs no unit or reference-system conversion
// and supplies no Q×N microstep. Invalid limits, invalid axis, descriptor budget,
// unit and profile refusals precede coordinate/output-budget checks, in that order.
// MaxValueBytes bounds the resulting position's canonical encoding.
func InstantPosition(axis temporal.Axis, value Instant, l temporal.Limits) (temporal.Position, error) {
	if err := validateInstantAxis(axis, l); err != nil {
		return temporal.Position{}, err
	}
	return temporal.ConvertUnits(temporal.RationalInt64(int64(value)), "millisecond", axis, l)
}

// InstantFromPosition encodes an exact integral millisecond coordinate only
// when its identity and complete definition match the caller's expected axis.
// It neither converts existing axes nor discards a microstep. General exact Q
// fractions and wide Z/Q coordinates remain valid but refuse this finite codec.
// Refusal precedence is expected-axis admission (as in InstantPosition), source
// validation/numeric encoding (as in temporal.InstantMillis), then axis mismatch.
// MaxValueBytes bounds the eight-byte result, not a temporary position encoding.
func InstantFromPosition(p temporal.Position, axis temporal.Axis, l temporal.Limits) (Instant, error) {
	if err := validateInstantAxis(axis, l); err != nil {
		return 0, err
	}
	value, err := temporal.InstantMillis(p, l)
	if err != nil {
		return 0, err
	}
	if p.Axis().Descriptor() != axis.Descriptor() || p.Axis().DefinitionHash() != axis.DefinitionHash() {
		return 0, temporal.ErrAxisMismatch
	}
	return Instant(value), nil
}

func validateInstantAxis(axis temporal.Axis, l temporal.Limits) error {
	if err := axis.Validate(l); err != nil {
		return err
	}
	d := axis.Descriptor()
	if d.CanonicalUnit != "millisecond" {
		return temporal.ErrExplicitMappingRequired
	}
	if d.Profile != temporal.ProfileIntegerZ && d.Profile != temporal.ProfileRationalQ {
		return temporal.ErrIncompatibleDomain
	}
	return nil
}
