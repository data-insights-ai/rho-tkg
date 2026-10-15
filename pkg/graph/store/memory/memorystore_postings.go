package memory

import (
	"cmp"
	"iter"
	"slices"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// idsAfter yields the IDs of an ascending ID slice above after: the label- and
// type-scan fallbacks' candidates in the shape of an index posting list's
// After (backlog 72), so both feed one filter loop.
func idsAfter[T interface{ SnowflakeID() snowflake.ID }](ids []T, after types.EntityID) iter.Seq[snowflake.ID] {
	return func(yield func(snowflake.ID) bool) {
		a := after.SnowflakeID()
		i, found := slices.BinarySearchFunc(ids, a, func(x T, t snowflake.ID) int {
			return cmp.Compare(x.SnowflakeID(), t)
		})
		if found {
			i++
		}
		for _, id := range ids[i:] {
			if !yield(id.SnowflakeID()) {
				return
			}
		}
	}
}

// resultCap is the starting capacity of a page: the limit, else the number of
// candidates.
func resultCap(limit, candidates int) int {
	if limit > 0 && limit < candidates {
		return limit
	}
	return candidates
}
