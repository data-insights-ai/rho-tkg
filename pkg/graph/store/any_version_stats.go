package store

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// Statistics types of sigma-tkgd round 9 (R9-1 to R9-3).

// RangeAnyVersionCount is RangeCardinalityAnyVersion's answer: the entities
// a numeric range over every version recorded at a pin visits, and their
// rows recorded at the pin whose value is in range (an entity holding a
// value in range on two versions counts once in Entities, twice in Rows).
type RangeAnyVersionCount struct {
	Entities int64
	Rows     int64
}

// CountSegment is one segment of a count timeline: Count members hold over
// the valid instants [From, To). From == math.MinInt64 is an open start
// (types.UnboundedValidFrom), To == 0 an open end.
type CountSegment struct {
	From, To types.Instant
	Count    int64
}

// CountTimelineOpts tunes CountTimelineByLabel / CountTimelineByType.
type CountTimelineOpts struct {
	// UntimedFromMinusInf: an entity's first segment that starts at an
	// untimed create (its first version written without a valid-from,
	// valid from its TxFrom on a UntimedTxFrom store) holds from -inf, as an
	// UntimedUnbounded store stores it.
	UntimedFromMinusInf bool
	// MaxSegments, above zero, declines (exact=false) a timeline of more
	// segments instead of building it.
	MaxSegments int
	// CurrentRows counts the current rows instead of the state at a pin (the
	// pin must be 0, else ErrInvalidTimeRange): each member's head row, the
	// row ForEachByLabel(label, QueryOpts{}) / ForEachByType hands out, over
	// its own valid interval [valid-from, valid-to): an unset valid-from is
	// the row's derived start (its TxFrom), an unset valid-to an open end;
	// with UntimedFromMinusInf a row that starts at an untimed create holds
	// from -inf. A member's earlier versions and a deleted entity count
	// nowhere (an unpinned read takes each member's current row as it is).
	// Not cached: one pass over the members' current rows per call.
	CurrentRows bool
}

// ValidHull is the hull of the valid instants at which a label's (type's)
// members hold (ValidHullByLabel / ValidHullByType): the least instant
// From and the end of the latest span, with explicit flags for the empty
// hull and the open end (no sentinel stands for either).
type ValidHull struct {
	// Empty: no member holds at any valid instant; the other fields are zero.
	Empty bool
	// From is the earliest instant at which a member holds;
	// types.UnboundedValidFrom (math.MinInt64) when one holds from -inf.
	From types.Instant
	// To ends the hull when OpenEnd is false: no member holds at To or
	// after; 0 when OpenEnd.
	To types.Instant
	// OpenEnd: some member holds with no end (+inf).
	OpenEnd bool
}

// PropertyValueStats states the values of an indexed property: Distinct
// values as the index keys them, by stored width (types.IndexablePropertyValueKey:
// an int64 3 and a float64 3.0 are two keys although ByLabelAndProperty finds
// both for 3; a diagnostics statistic, not an equality count),
// Rows carrying an indexed value, the heaviest value's rows (MaxRows) and
// that value (MaxValue; nil when Rows is 0).
type PropertyValueStats struct {
	Distinct int64
	Rows     int64
	MaxRows  int64
	MaxValue any
}
