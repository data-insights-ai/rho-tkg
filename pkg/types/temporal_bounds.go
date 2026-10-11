package types

import "errors"

// TemporalEndpoint distinguishes a finite Unix millisecond, including zero,
// from an unbounded endpoint. Value must be zero when Finite is false.
// An unbounded From is negative infinity; an unbounded To is positive infinity.
// This type does not represent an unknown date.
type TemporalEndpoint struct {
	Value  Instant
	Finite bool
}

// TemporalRange is a nonempty half-open valid-time interval.
type TemporalRange struct {
	From TemporalEndpoint
	To   TemporalEndpoint
}

var (
	// ErrInvalidTemporalRange reports malformed finite/unbounded endpoints.
	ErrInvalidTemporalRange = errors.New("types: invalid temporal range")
	// ErrNilTemporalMetadata reports a mutation of absent metadata.
	ErrNilTemporalMetadata = errors.New("types: nil temporal metadata")
)

// Validate rejects noncanonical infinity values and empty/reversed ranges.
func (r TemporalRange) Validate() error {
	if (!r.From.Finite && r.From.Value != 0) || (!r.To.Finite && r.To.Value != 0) ||
		(r.From.Finite && r.To.Finite && r.From.Value >= r.To.Value) {
		return ErrInvalidTemporalRange
	}
	return nil
}

// Contains reports whether at is inside this valid-time interval.
func (r TemporalRange) Contains(at Instant) bool {
	return r.Validate() == nil && (!r.From.Finite || r.From.Value <= at) && (!r.To.Finite || at < r.To.Value)
}

// Overlaps reports whether this interval and other share a valid instant.
func (r TemporalRange) Overlaps(other TemporalRange) bool {
	return r.Validate() == nil && other.Validate() == nil &&
		(!r.From.Finite || !other.To.Finite || r.From.Value < other.To.Value) &&
		(!other.From.Finite || !r.To.Finite || other.From.Value < r.To.Value)
}

// EndpointPresence is the persisted interpretation of valid endpoints. Zero
// preserves legacy numeric-zero defaults. A new explicit range always sets
// EndpointsExplicit; its finite endpoints carry the corresponding bits.
type EndpointPresence uint8

// Presence bits distinguish explicit ranges from legacy scalar defaults.
const (
	EndpointsExplicit EndpointPresence = 1 << iota
	EndpointFromFinite
	EndpointToFinite
)

// HasValidBounds reports whether the metadata carries explicit typed bounds.
func (tm *TemporalMetadata) HasValidBounds() bool {
	return tm != nil && tm.ValidEndpoints&EndpointsExplicit != 0
}

// ValidBounds reports the explicit typed bounds, or the legacy scalar shape.
// For legacy metadata the false result means an absent start still needs the
// entity's effective-start fallback, rather than negative infinity.
func (tm *TemporalMetadata) ValidBounds() (TemporalRange, bool) {
	if tm == nil {
		return TemporalRange{}, false
	}
	if tm.HasValidBounds() {
		return TemporalRange{From: TemporalEndpoint{tm.ValidFrom, tm.ValidEndpoints&EndpointFromFinite != 0}, To: TemporalEndpoint{tm.ValidTo, tm.ValidEndpoints&EndpointToFinite != 0}}, true
	}
	return TemporalRange{From: TemporalEndpoint{tm.ValidFrom, tm.ValidFrom != 0}, To: TemporalEndpoint{tm.ValidTo, tm.ValidTo != 0}}, false
}

// SetValidBounds stores canonical explicit typed valid-time bounds.
func (tm *TemporalMetadata) SetValidBounds(r TemporalRange) error {
	if tm == nil {
		return ErrNilTemporalMetadata
	}
	if err := r.Validate(); err != nil {
		return err
	}
	p := EndpointsExplicit
	if r.From.Finite {
		p |= EndpointFromFinite
	}
	if r.To.Finite {
		p |= EndpointToFinite
	}
	tm.ValidFrom, tm.ValidTo, tm.ValidEndpoints = r.From.Value, r.To.Value, p
	return nil
}

// ValidateValidBounds validates explicit presence without changing legacy rows.
func (tm *TemporalMetadata) ValidateValidBounds() error {
	if tm == nil {
		return nil
	}
	if tm.ValidEndpoints == 0 {
		return nil
	}
	if !tm.HasValidBounds() || tm.ValidEndpoints & ^(EndpointsExplicit|EndpointFromFinite|EndpointToFinite) != 0 {
		return ErrInvalidTemporalRange
	}
	r, _ := tm.ValidBounds()
	return r.Validate()
}
