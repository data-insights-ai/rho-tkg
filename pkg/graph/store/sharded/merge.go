package sharded

import (
	"sort"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Merge + pagination helpers. Each shard returns ID-sorted results; the sharded
// store concatenates the per-shard slices, re-sorts by snowflake ID for a stable
// global order, and applies pagination AFTER the merge (mirroring tiered).

func mergeSortNodes(slices [][]*types.Node) []*types.Node {
	var out []*types.Node
	for _, s := range slices {
		out = append(out, s...)
	}
	storeutil.SortNodesByID(out)
	return out
}

func mergeSortRels(slices [][]*types.Relationship) []*types.Relationship {
	var out []*types.Relationship
	for _, s := range slices {
		out = append(out, s...)
	}
	storeutil.SortRelsByID(out)
	return out
}

func mergeSortNodeIDs(slices [][]types.NodeID) []types.NodeID {
	var out []types.NodeID
	for _, s := range slices {
		out = append(out, s...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SnowflakeID() < out[j].SnowflakeID() })
	return out
}

func mergeSortRelIDs(slices [][]types.RelID) []types.RelID {
	var out []types.RelID
	for _, s := range slices {
		out = append(out, s...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SnowflakeID() < out[j].SnowflakeID() })
	return out
}

// stripPagination clears Limit/After so per-shard folds return every matching
// row; pagination is applied globally after the merge. Temporal filters and
// NoSort are preserved (they push down soundly per shard).
func stripPagination(opts QueryOpts) QueryOpts {
	opts.Limit = 0
	opts.After = 0
	return opts
}

func paginateNodes(nodes []*types.Node, opts QueryOpts) []*types.Node {
	return storeutil.PaginateNodes(nodes, opts.After, opts.Limit)
}

func paginateRels(rels []*types.Relationship, opts QueryOpts) []*types.Relationship {
	return storeutil.PaginateRels(rels, opts.After, opts.Limit)
}

func paginateNodeIDs(ids []types.NodeID, opts QueryOpts) []types.NodeID {
	return storeutil.PaginateNodeIDs(ids, opts.After, opts.Limit)
}

func paginateRelIDs(ids []types.RelID, opts QueryOpts) []types.RelID {
	return storeutil.PaginateRelIDs(ids, opts.After, opts.Limit)
}
