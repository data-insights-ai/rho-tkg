package types

import (
	"errors"
	"fmt"
)

// TemporalValue is a storage-typed temporal property value: a temporal
// KIND plus the canonical ISO-8601 rendering. It exists so engines layered
// on the graph (a query engine with typed temporals) can round-trip the
// temporal-ness of a value through storage EXACTLY — before this type,
// temporals were stored as plain strings and readers had to GUESS from the
// string shape, which mistyped genuine string properties that merely look
// like dates ('2024-01-01') and broke their literal equality.
//
// The graph layer treats the value as opaque: validation checks shape
// only; ordering/arithmetic semantics live in the engine. The ISO string
// is the single source of truth — Kind says how to interpret it.
type TemporalValue struct {
	Kind TemporalKind
	// Value is the canonical ISO-8601 rendering (e.g. "2024-01-01",
	// "12:30:00Z", "2024-01-01T12:30:00+02:00", "P1DT2H").
	Value string
}

// TemporalKind enumerates the temporal categories a TemporalValue carries.
// The numeric values are part of the WIRE format (stored in property
// rows) — append only, never renumber.
type TemporalKind uint8

// TemporalKind wire values. Mirrors the openCypher temporal taxonomy.
const (
	TemporalDate          TemporalKind = 0 // calendar date
	TemporalLocalTime     TemporalKind = 1 // time of day, no zone
	TemporalTime          TemporalKind = 2 // time of day with zone offset
	TemporalLocalDateTime TemporalKind = 3 // date + time, no zone
	TemporalDateTime      TemporalKind = 4 // date + time with zone
	TemporalDuration      TemporalKind = 5 // ISO-8601 duration
)

// maxTemporalKind guards wire decoding — values above this fail closed.
const maxTemporalKind = TemporalDuration

// ErrInvalidTemporalValue is returned when a TemporalValue fails shape
// validation (unknown kind or empty rendering).
var ErrInvalidTemporalValue = errors.New("types: invalid temporal property value")

// maxTemporalValueLen caps the ISO rendering length — generous for any
// legal ISO-8601 temporal, small enough to bound hostile input.
const maxTemporalValueLen = 128

// Validate checks the value's shape (kind range, non-empty bounded
// rendering). Content-level ISO validity is the producing engine's
// responsibility — the graph stores what the engine rendered.
func (t TemporalValue) Validate() error {
	if t.Kind > maxTemporalKind {
		return fmt.Errorf("%w: unknown kind %d", ErrInvalidTemporalValue, t.Kind)
	}
	if t.Value == "" {
		return fmt.Errorf("%w: empty rendering", ErrInvalidTemporalValue)
	}
	if len(t.Value) > maxTemporalValueLen {
		return fmt.Errorf("%w: rendering exceeds %d bytes", ErrInvalidTemporalValue, maxTemporalValueLen)
	}
	return nil
}

// String returns the ISO rendering — fmt/%v of a TemporalValue prints
// exactly the storage form, mirroring the engine-side convention.
func (t TemporalValue) String() string { return t.Value }
