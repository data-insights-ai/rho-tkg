package temporal

import (
	"context"
)

// RelEffectiveTimelinesByProperties returns, for every tuple of property
// values, the effective timelines at pin.Tx of the relationships of typeName
// whose rows carry those values (row 105, sigma O1-3): result index i holds
// the segments whose row carries keys[j] = tuples[i][j] for every j (the
// property equality of every equality door, types.EqualIndexValues: 3 matches
// 3.0, NaN matches nothing, list membership does not count) and that meet
// pin's valid-time filter, grouped by relationship ascending by ID, segments
// ascending; nil for a tuple with no match. The result is aligned with tuples
// (unsorted and repeated tuples allowed, each answer stands on its own). A
// segment is kept whole, as RelsEffectivePage keeps it: it is not clipped to
// the window. Each relationship's segments are its RelEffectiveTimeline(id,
// pin.Tx) segments, so a relationship whose values changed answers under each
// tuple only with the segments that carried that tuple. DECLARED view: not
// masked by endpoint validity.
//
// Pin: pin.Tx is required (a pin at or below 0 answers nil for every tuple:
// nothing recorded yet); the valid-time filter is Pin's (none, a point
// ValidAt, [ValidStart, ValidEnd), or the open [ValidStart, inf)). The pin is
// gated before any row is read: ErrTxPinTooNew, ErrHistoryCompacted, or
// ErrRetentionExpired (the type's watermark) refuse the whole call.
//
// Cost: one call per probe set, not per history version or per type member.
// A (typeName, key) relationship property index must be declared on at least
// one of keys (Index().CreateRelProperty); the driver is the indexed key whose
// tuple values have the fewest current members (Rels().CountByTypePropertyValue,
// O(1) per value on memory, badger and sharded; a store without exact counts,
// tiered, takes the first indexed key in keys order, so name the most
// selective key first there). The candidates are the relationships whose rows
// ever carried a driver value of some tuple, inside the valid window, from the
// index's transaction-time membership sidecar in ONE enumeration for all
// tuples (one shard fan-out on tiered and sharded); those first recorded after
// pin.Tx are pruned. Each candidate's timeline is read once and each segment
// is routed to the tuples whose values its row carries (the other keys are
// checked on the row, no index needed). So a probe (estate, device) with
// device indexed reads about the device's relationships in the window, while
// a probe by a low-selectivity key alone, e.g. (action), reads every
// relationship that carried the action in the window: O(A), inherent. RAM:
// the windowed enumeration opens a window view per probed driver value the
// first time it is read (about 110-145 bytes per posting of that value instead
// of 25, kept current by every write afterwards); a call without a valid
// filter reads the plain sidecar and opens no view.
//
// Errors: keys must be 1..4 distinct valid property keys (shadow keys
// refused, types.ErrReservedPrefix; an empty key, ErrEmptyName; a wrong count
// or a duplicate, ErrInvalidStoreMutation), every tuple must hold len(keys)
// values (ErrInvalidStoreMutation), every value a scalar with an index key (a
// nil, list or map value is types.ErrUnsupportedValueType). No declared index
// on any key (or one still being built, or an Elements index) is
// ErrIndexNotFound: a cost contract, the call never folds the type's history.
// An unknown type answers nil for every tuple; empty tuples answer an empty
// result and read nothing. ErrInvalidTimeRange for a conflicting or reversed
// valid filter, ErrNilContext, ctx's error (checked between candidates),
// ErrGraphClosed.
func (a *API) RelEffectiveTimelinesByProperties(ctx context.Context, typeName string, keys []string, tuples [][]any, pin Pin) ([][]RelSegment, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RelEffectiveTimelinesByProperties(ctx, typeName, keys, tuples, pin)
}
