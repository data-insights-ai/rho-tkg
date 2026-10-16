package index

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
)

// ValueStats is a property index's value statistics (sigma-tkgd round 9
// R9-3): Distinct value keys, Rows (postings), the heaviest value key's
// postings (MaxRows), that key (MaxKey, the smallest key string among
// equals) and one entity holding it (Sample), from which the caller reads
// the value.
type ValueStats struct {
	Distinct int64
	Rows     int64
	MaxRows  int64
	MaxKey   string
	Sample   snowflake.ID
}

// ValueStats returns the index's value statistics, exact=false where the
// index cannot say (nil, still being built, an Elements index: a list's
// elements are no row value). Computed by one pass over the value keys at
// the first call after a mutation and kept until the next (AddKey,
// removeKey and the purge drop it): the write path pays one pointer store.
// Callers hold the store's index lock (shared at least); the cache is
// atomic because shared holders may compute it at once.
func (pi *PropertyIndex) ValueStats() (ValueStats, bool) {
	if !pi.serves() {
		return ValueStats{}, false
	}
	if s := pi.valueStats.Load(); s != nil {
		return *s, true
	}
	var s ValueStats
	for vk, set := range pi.Entries {
		n := int64(set.Len())
		if n == 0 {
			continue
		}
		s.Distinct++
		s.Rows += n
		if n > s.MaxRows || (n == s.MaxRows && vk < s.MaxKey) {
			s.MaxRows, s.MaxKey = n, vk
		}
	}
	if s.MaxRows > 0 {
		for id := range pi.Entries[s.MaxKey].All() {
			s.Sample = id
			break
		}
	}
	pi.valueStats.Store(&s)
	return s, true
}
