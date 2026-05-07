package storeutil

import (
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// PaginateIDs applies cursor-based pagination to a sorted slice of snowflake IDs.
// The input slice must be sorted in ascending order.
//
//   - after == 0: start from the beginning.
//   - after > 0: skip all IDs <= after (keyset cursor).
//   - limit == 0: return all remaining IDs.
//   - limit > 0: return at most limit IDs.
//
// Returns nil if the result is empty.
func PaginateIDs(ids []snowflake.ID, after types.EntityID, limit int) []snowflake.ID {
	if len(ids) == 0 {
		return nil
	}
	afterRaw := after.SnowflakeID()
	start := 0
	if afterRaw > 0 {
		// Binary search for the first ID > after.
		start = sort.Search(len(ids), func(i int) bool {
			return ids[i] > afterRaw
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

// PaginateNodes applies cursor-based pagination to a slice of nodes sorted
// ascending by snowflake ID. Same semantics as PaginateIDs.
func PaginateNodes(nodes []*types.Node, after types.EntityID, limit int) []*types.Node {
	if len(nodes) == 0 {
		return nil
	}
	afterRaw := after.SnowflakeID()
	start := 0
	if afterRaw > 0 {
		start = sort.Search(len(nodes), func(i int) bool {
			return nodes[i].ID().SnowflakeID() > afterRaw
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

// PaginateRels applies cursor-based pagination to a slice of relationships
// sorted ascending by snowflake ID. Same semantics as PaginateIDs.
func PaginateRels(rels []*types.Relationship, after types.EntityID, limit int) []*types.Relationship {
	if len(rels) == 0 {
		return nil
	}
	afterRaw := after.SnowflakeID()
	start := 0
	if afterRaw > 0 {
		start = sort.Search(len(rels), func(i int) bool {
			return rels[i].ID().SnowflakeID() > afterRaw
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

// ToNodeIDs wraps a raw snowflake.ID slice into a typed types.NodeID slice.
// Used at boundaries where storage maps return raw IDs but the downstream API
// takes typed IDs. New allocation; preserves order.
func ToNodeIDs(ids []snowflake.ID) []types.NodeID {
	if len(ids) == 0 {
		return nil
	}
	out := make([]types.NodeID, len(ids))
	for i, id := range ids {
		out[i] = types.NodeID(id)
	}
	return out
}

// ToRelIDs wraps a raw snowflake.ID slice into a typed types.RelID slice.
func ToRelIDs(ids []snowflake.ID) []types.RelID {
	if len(ids) == 0 {
		return nil
	}
	out := make([]types.RelID, len(ids))
	for i, id := range ids {
		out[i] = types.RelID(id)
	}
	return out
}

// PaginateNodeIDs is the typed equivalent of PaginateIDs for []types.NodeID.
// Same semantics: input must be sorted ascending by underlying snowflake ID;
// after == 0 starts from the beginning; limit == 0 returns all remaining.
func PaginateNodeIDs(ids []types.NodeID, after types.EntityID, limit int) []types.NodeID {
	if len(ids) == 0 {
		return nil
	}
	afterRaw := after.SnowflakeID()
	start := 0
	if afterRaw > 0 {
		start = sort.Search(len(ids), func(i int) bool {
			return ids[i].SnowflakeID() > afterRaw
		})
	}
	if start >= len(ids) {
		return nil
	}
	out := ids[start:]
	if limit > 0 && limit < len(out) {
		out = out[:limit]
	}
	return out
}

// SortNodesByID sorts nodes by snowflake.ID for deterministic output.
// Order is time-dominant (ms timestamp in high bits) with nodeField and step as tiebreakers.
func SortNodesByID(nodes []*types.Node) {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID() < nodes[j].ID()
	})
}

// SortRelsByID sorts relationships by snowflake.ID for deterministic output.
// Order is time-dominant (ms timestamp in high bits) with nodeField and step as tiebreakers.
func SortRelsByID(rels []*types.Relationship) {
	sort.Slice(rels, func(i, j int) bool {
		return rels[i].ID() < rels[j].ID()
	})
}

// PaginateRelIDs is the typed equivalent of PaginateIDs for []types.RelID.
func PaginateRelIDs(ids []types.RelID, after types.EntityID, limit int) []types.RelID {
	if len(ids) == 0 {
		return nil
	}
	afterRaw := after.SnowflakeID()
	start := 0
	if afterRaw > 0 {
		start = sort.Search(len(ids), func(i int) bool {
			return ids[i].SnowflakeID() > afterRaw
		})
	}
	if start >= len(ids) {
		return nil
	}
	out := ids[start:]
	if limit > 0 && limit < len(out) {
		out = out[:limit]
	}
	return out
}
