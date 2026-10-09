// Package temporal defines exact temporal coordinates and their axis identity.
// Machine encodings never bound the semantic integer or rational domain.
package temporal

import "errors"

var (
	// ErrInvalidLimits identifies overrides outside the permitted resource policy.
	ErrInvalidLimits = errors.New("temporal: invalid resource limits")
	// ErrResourceLimit identifies a value or operation exceeding its active budget.
	ErrResourceLimit = errors.New("temporal: resource limit exceeded")
	// ErrInvalidValue identifies malformed numbers or a negative natural microstep.
	ErrInvalidValue = errors.New("temporal: invalid coordinate value")
	// ErrZeroDenominator identifies a fraction with an undefined denominator.
	ErrZeroDenominator = errors.New("temporal: zero denominator")
	// ErrInvalidAxis identifies an absent or malformed axis definition.
	ErrInvalidAxis = errors.New("temporal: invalid axis")
	// ErrUnknownProfile identifies a domain profile with no native implementation.
	ErrUnknownProfile = errors.New("temporal: unknown profile")
	// ErrUnknownVersion identifies an unsupported descriptor or codec version.
	ErrUnknownVersion = errors.New("temporal: unknown version")
	// ErrIncompatibleDomain identifies a coordinate shape unsuitable for its axis.
	ErrIncompatibleDomain = errors.New("temporal: incompatible coordinate domain")
	// ErrAxisMismatch identifies different axis identities or conflicting definitions.
	ErrAxisMismatch = errors.New("temporal: axis identity or definition mismatch")
	// ErrInvalidPosition identifies an absent or structurally invalid position.
	ErrInvalidPosition = errors.New("temporal: invalid position")
	// ErrUnsupportedPredicate identifies an operation unavailable on the profile.
	ErrUnsupportedPredicate = errors.New("temporal: predicate unsupported by profile")
	// ErrNoPredecessor identifies a position without an immediate predecessor.
	ErrNoPredecessor = errors.New("temporal: position has no immediate predecessor")
	// ErrInvalidEncoding identifies malformed or noncanonical persisted bytes.
	ErrInvalidEncoding = errors.New("temporal: invalid canonical encoding")
)
