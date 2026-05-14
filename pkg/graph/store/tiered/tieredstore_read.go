package tiered

import (
	"container/heap"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/storeutil"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
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

// Internal slice converters for history scans that deduplicate raw snowflake IDs.
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
// Standard k-way merge of pre-sorted slices. The common one/two-shard cases
// avoid heap setup; larger fan-in uses a heap so large scans do not pay
// O(N log N) sort cost after every shard already returned sorted data.

func mergeNodeSlices(slices [][]*types.Node) []*types.Node {
	return mergeSortedSlices(slices, func(a, b *types.Node) bool {
		return a.ID().SnowflakeID() < b.ID().SnowflakeID()
	})
}

func mergeRelSlices(slices [][]*types.Relationship) []*types.Relationship {
	return mergeSortedSlices(slices, func(a, b *types.Relationship) bool {
		return a.ID().SnowflakeID() < b.ID().SnowflakeID()
	})
}

func mergeNodeIDSlices(slices [][]types.NodeID) []types.NodeID {
	return mergeSortedSlices(slices, func(a, b types.NodeID) bool {
		return a.SnowflakeID() < b.SnowflakeID()
	})
}

func mergeRelIDSlices(slices [][]types.RelID) []types.RelID {
	return mergeSortedSlices(slices, func(a, b types.RelID) bool {
		return a.SnowflakeID() < b.SnowflakeID()
	})
}

func mergeSortedSlices[T any](slices [][]T, less func(a, b T) bool) []T {
	if len(slices) == 0 {
		return nil
	}
	var first []T
	nonEmpty := 0
	total := 0
	for _, s := range slices {
		if len(s) == 0 {
			continue
		}
		if nonEmpty == 0 {
			first = s
		}
		nonEmpty++
		total += len(s)
	}
	switch nonEmpty {
	case 0:
		return nil
	case 1:
		return first
	case 2:
		return mergeTwoSortedSlices(slices, less, total)
	}

	h := mergeHeap[T]{less: less}
	h.items = make([]mergeItem[T], 0, nonEmpty)
	for sliceIdx, s := range slices {
		if len(s) == 0 {
			continue
		}
		h.items = append(h.items, mergeItem[T]{
			value:    s[0],
			sliceIdx: sliceIdx,
			itemIdx:  0,
		})
	}
	heap.Init(&h)

	result := make([]T, 0, total)
	for h.Len() > 0 {
		item := heap.Pop(&h).(mergeItem[T])
		result = append(result, item.value)
		nextIdx := item.itemIdx + 1
		if nextIdx < len(slices[item.sliceIdx]) {
			heap.Push(&h, mergeItem[T]{
				value:    slices[item.sliceIdx][nextIdx],
				sliceIdx: item.sliceIdx,
				itemIdx:  nextIdx,
			})
		}
	}
	return result
}

func mergeTwoSortedSlices[T any](slices [][]T, less func(a, b T) bool, total int) []T {
	var a, b []T
	for _, s := range slices {
		if len(s) == 0 {
			continue
		}
		if a == nil {
			a = s
		} else {
			b = s
			break
		}
	}

	result := make([]T, 0, total)
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if less(b[j], a[i]) {
			result = append(result, b[j])
			j++
		} else {
			result = append(result, a[i])
			i++
		}
	}
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

type mergeItem[T any] struct {
	value    T
	sliceIdx int
	itemIdx  int
}

type mergeHeap[T any] struct {
	items []mergeItem[T]
	less  func(a, b T) bool
}

func (h mergeHeap[T]) Len() int { return len(h.items) }

func (h mergeHeap[T]) Less(i, j int) bool {
	left, right := h.items[i], h.items[j]
	if h.less(left.value, right.value) {
		return true
	}
	if h.less(right.value, left.value) {
		return false
	}
	return left.sliceIdx < right.sliceIdx
}

func (h mergeHeap[T]) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *mergeHeap[T]) Push(x any) { h.items = append(h.items, x.(mergeItem[T])) }

func (h *mergeHeap[T]) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[:n-1]
	return item
}

// --- Pagination helpers ---
// Apply After/Limit from QueryOpts to already-merged, sorted slices.

func applyNodePagination(nodes []*types.Node, opts QueryOpts) []*types.Node {
	return storeutil.PaginateNodes(nodes, opts.After, opts.Limit)
}

func applyRelPagination(rels []*types.Relationship, opts QueryOpts) []*types.Relationship {
	return storeutil.PaginateRels(rels, opts.After, opts.Limit)
}

func applyNodeIDPagination(ids []types.NodeID, opts QueryOpts) []types.NodeID {
	return storeutil.PaginateNodeIDs(ids, opts.After, opts.Limit)
}

func applyRelIDPagination(ids []types.RelID, opts QueryOpts) []types.RelID {
	return storeutil.PaginateRelIDs(ids, opts.After, opts.Limit)
}
