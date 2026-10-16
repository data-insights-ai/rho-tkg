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
