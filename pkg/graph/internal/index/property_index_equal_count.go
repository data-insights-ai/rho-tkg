package index

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// EqualCount counts the entities whose value equals value under the property
// equality of the lookup doors (types.EqualIndexValues: numbers by exact value
// across every stored width, no string/number/boolean crossing, nil and NaN
// count 0), from the index's per-value sets of those keys (their sizes; no ID
// is read), so it equals the number of rows ByLabelAndProperty returns.
// exact=false where the index cannot say: a nil index, an index still being
// built (Mutated is set: the backfill has not landed), an Elements index (a
// list's elements share the keys of scalars), a temporal value (equal instants
// may be stored in different renderings), a list, a map or another unindexed
// type.
func (pi *PropertyIndex) EqualCount(value any) (count int64, exact bool) {
	if pi == nil || pi.elements || pi.Mutated != nil {
		return 0, false
	}
	switch value.(type) {
	case types.TemporalValue:
		return 0, false
	case nil:
		return 0, true
	}
	keys := types.EqualIndexKeys(value)
	if keys == nil && types.IndexablePropertyValueKey(value) == "" {
		return 0, false // a list, map or other unindexed value
	}
	for _, k := range keys {
		count += int64(pi.Entries[k].Len())
	}
	return count, true
}
