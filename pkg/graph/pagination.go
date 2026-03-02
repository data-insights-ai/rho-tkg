package graph

import (
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// paginateIDs applies cursor-based pagination to a sorted slice of snowflake IDs.
// The input slice must be sorted in ascending order.
//
//   - after == 0: start from the beginning.
//   - after > 0: skip all IDs <= after (keyset cursor).
//   - limit == 0: return all remaining IDs.
//   - limit > 0: return at most limit IDs.
//
// Returns nil if the result is empty.
func paginateIDs(ids []snowflake.ID, after snowflake.ID, limit int) []snowflake.ID {
	if len(ids) == 0 {
		return nil
	}

	start := 0
	if after > 0 {
		// Binary search for the first ID > after.
		start = sort.Search(len(ids), func(i int) bool {
			return ids[i] > after
		})
	}

	if start >= len(ids) {
		return nil
	}

	remaining := ids[start:]
	if limit > 0 && limit < len(remaining) {
		remaining = remaining[:limit]
	}

	return remaining
}
