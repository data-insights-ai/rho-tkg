package graph

import (
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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

// paginateNodes applies cursor-based pagination to a slice of nodes sorted
// ascending by snowflake ID. Same semantics as paginateIDs.
func paginateNodes(nodes []*types.Node, after snowflake.ID, limit int) []*types.Node {
	if len(nodes) == 0 {
		return nil
	}
	start := 0
	if after > 0 {
		start = sort.Search(len(nodes), func(i int) bool {
			return nodes[i].ID().SnowflakeID() > after
		})
	}
	if start >= len(nodes) {
		return nil
	}
	out := nodes[start:]
	if limit > 0 && limit < len(out) {
		out = out[:limit]
	}
	return out
}

// paginateRels applies cursor-based pagination to a slice of relationships
// sorted ascending by snowflake ID. Same semantics as paginateIDs.
func paginateRels(rels []*types.Relationship, after snowflake.ID, limit int) []*types.Relationship {
	if len(rels) == 0 {
		return nil
	}
	start := 0
	if after > 0 {
		start = sort.Search(len(rels), func(i int) bool {
			return rels[i].ID().SnowflakeID() > after
		})
	}
	if start >= len(rels) {
		return nil
	}
	out := rels[start:]
	if limit > 0 && limit < len(out) {
		out = out[:limit]
	}
	return out
}
