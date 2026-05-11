package tiered

import (
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// stripDepth returns opts with the Depth field cleared.
// Used when forwarding to single-shard queries (Depth is Store-level).
// All other fields — Limit, After, ValidAt, ValidStart, ValidEnd — are preserved
// so per-shard calls respect pagination and temporal filters.
func stripDepth(opts QueryOpts) QueryOpts {
	opts.Depth = 0
	return opts
}

func validateDepth(depth ShardDepth) error {
	return storecontract.ValidateShardDepth(depth)
}

func validateQueryOpts(opts QueryOpts) error {
	return storecontract.ValidateQueryOpts(opts)
}

// Internal slice converters bridging typed entity IDs and raw snowflake.ID
// for cross-shard merge helpers (mergeIDSlices, applyIDPagination).
func nodeIDsToRaw(ids []types.NodeID) []snowflake.ID {
	if len(ids) == 0 {
		return nil
	}
	out := make([]snowflake.ID, len(ids))
	for i, id := range ids {
		out[i] = id.SnowflakeID()
	}
	return out
}

func relIDsToRaw(ids []types.RelID) []snowflake.ID {
	if len(ids) == 0 {
		return nil
	}
	out := make([]snowflake.ID, len(ids))
	for i, id := range ids {
		out[i] = id.SnowflakeID()
	}
	return out
}

func rawToNodeIDs(ids []snowflake.ID) []types.NodeID {
	if len(ids) == 0 {
		return nil
	}
	out := make([]types.NodeID, len(ids))
	for i, id := range ids {
		out[i] = types.NodeID(id)
	}
	return out
}

func rawToRelIDs(ids []snowflake.ID) []types.RelID {
	if len(ids) == 0 {
		return nil
	}
	out := make([]types.RelID, len(ids))
	for i, id := range ids {
		out[i] = types.RelID(id)
	}
	return out
}

// --- Merge helpers ---
// Standard k-way merge of pre-sorted slices. For Phase 3a with 2 shards,
// this is a simple 2-way merge.

func mergeNodeSlices(slices [][]*types.Node) []*types.Node {
	if len(slices) == 0 {
		return nil
	}
	if len(slices) == 1 {
		return slices[0]
	}

	// 2-way merge for the common case.
	total := 0
	for _, s := range slices {
		total += len(s)
	}
	result := make([]*types.Node, 0, total)

	// Flatten and sort (simple approach for small shard counts).
	for _, s := range slices {
		result = append(result, s...)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID().SnowflakeID() < result[j].ID().SnowflakeID()
	})
	return result
}

func mergeRelSlices(slices [][]*types.Relationship) []*types.Relationship {
	if len(slices) == 0 {
		return nil
	}
	if len(slices) == 1 {
		return slices[0]
	}

	total := 0
	for _, s := range slices {
		total += len(s)
	}
	result := make([]*types.Relationship, 0, total)
	for _, s := range slices {
		result = append(result, s...)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID().SnowflakeID() < result[j].ID().SnowflakeID()
	})
	return result
}

func mergeIDSlices(slices [][]snowflake.ID) []snowflake.ID {
	if len(slices) == 0 {
		return nil
	}
	if len(slices) == 1 {
		return slices[0]
	}

	total := 0
	for _, s := range slices {
		total += len(s)
	}
	result := make([]snowflake.ID, 0, total)
	for _, s := range slices {
		result = append(result, s...)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})
	return result
}

// --- Pagination helpers ---
// Apply After/Limit from QueryOpts to already-merged, sorted slices.

func applyNodePagination(nodes []*types.Node, opts QueryOpts) []*types.Node {
	if opts.After != 0 {
		afterRaw := opts.After.SnowflakeID()
		i := sort.Search(len(nodes), func(i int) bool {
			return nodes[i].ID().SnowflakeID() > afterRaw
		})
		nodes = nodes[i:]
	}
	if opts.Limit > 0 && len(nodes) > opts.Limit {
		nodes = nodes[:opts.Limit]
	}
	if len(nodes) == 0 {
		return nil
	}
	return nodes
}

func applyRelPagination(rels []*types.Relationship, opts QueryOpts) []*types.Relationship {
	if opts.After != 0 {
		afterRaw := opts.After.SnowflakeID()
		i := sort.Search(len(rels), func(i int) bool {
			return rels[i].ID().SnowflakeID() > afterRaw
		})
		rels = rels[i:]
	}
	if opts.Limit > 0 && len(rels) > opts.Limit {
		rels = rels[:opts.Limit]
	}
	if len(rels) == 0 {
		return nil
	}
	return rels
}

func applyIDPagination(ids []snowflake.ID, opts QueryOpts) []snowflake.ID {
	if opts.After != 0 {
		afterRaw := opts.After.SnowflakeID()
		i := sort.Search(len(ids), func(i int) bool {
			return ids[i] > afterRaw
		})
		ids = ids[i:]
	}
	if opts.Limit > 0 && len(ids) > opts.Limit {
		ids = ids[:opts.Limit]
	}
	if len(ids) == 0 {
		return nil
	}
	return ids
}
