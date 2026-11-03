package index

import (
	"slices"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// testHooks counts hot-path work for tests (BATCHIDX); nil in production.
// Set only by a test that does not call t.Parallel: a parallel test's
// index work would count in it, and its writes would race.
var testHooks *hookCounts

type hookCounts struct {
	numericParse int // numericSortKey calls
	chunkInsert  int // sortedChunks.insert calls
}

// AddNodesToPropertyIndexes is AddNodeToPropertyIndexes for a batch of new
// nodes, nodes[i] under ids[i], applied per index at once (BATCHIDX, ai-soc's
// cut: 106 ms of 219 in per-node index inserts): each distinct value key is
// looked up and its numeric sort key parsed once, its IDs go into the posting
// list sorted (appended when they ascend past its last, the snowflake write),
// new values enter the ordered views sorted, a range count moves once per
// value. The index ends in the state the per-node adds leave. Caller must
// hold the store's write lock.
func AddNodesToPropertyIndexes(indexes map[PropertyIndexKey]*PropertyIndex, nodes []*types.Node, ids []snowflake.ID) {
	for key, idx := range indexes {
		if idx == nil || idx.onDisk {
			continue
		}
		var keys map[string][]snowflake.ID
		for i, n := range nodes {
			if !n.HasLabelTokenRaw(key.LabelToken) {
				continue
			}
			id := ids[i]
			if !idx.elements {
				if vk, _ := n.IndexablePropertyValueKey(key.PropertyKey); vk != "" {
					if keys == nil {
						keys = make(map[string][]snowflake.ID)
					}
					keys[vk] = append(keys[vk], id)
				}
				continue
			}
			n.ForEachPropertyIndexKey(key.PropertyKey, true, func(vk string) bool {
				if vk != "" {
					if keys == nil {
						keys = make(map[string][]snowflake.ID)
					}
					keys[vk] = append(keys[vk], id)
				}
				return true
			})
		}
		idx.addKeys(keys)
	}
}

// addKeys applies AddKey(id, vk) for every id of keys[vk], per value key at
// once (see AddNodesToPropertyIndexes). keys' slices are sorted in place.
func (pi *PropertyIndex) addKeys(keys map[string][]snowflake.ID) {
	if pi == nil || len(keys) == 0 {
		return
	}
	if pi.Entries == nil {
		pi.Entries = make(map[string]*IDPostings)
	}
	ordered := pi.orderedViews()
	var newStrs []string
	var nums map[float64][]snowflake.ID
	for vk, ids := range keys {
		pairs := len(ids) // AddKey calls: each notes its value's precision
		ids = sortedDistinct(ids)
		set := pi.Entries[vk]
		if set == nil {
			set = &IDPostings{}
			pi.Entries[vk] = set
			if ordered {
				if s, ok := stringSortKey(vk); ok {
					newStrs = append(newStrs, s)
				}
			}
		}
		set.s.putSorted(ids)
		if pi.Mutated != nil {
			for _, id := range ids {
				pi.Mutated[id] = struct{}{}
			}
		}
		if !ordered {
			continue
		}
		k, ok := numericSortKey(vk)
		if !ok {
			continue
		}
		if isNumericImprecise(vk) {
			pi.numImpreciseCount += pairs
		}
		if nums == nil {
			nums = make(map[float64][]snowflake.ID)
		}
		nums[k] = append(nums[k], ids...) // value keys of one sort key (i:1, f64:1) share a bucket
	}
	pi.valueStats.Store(nil)
	if len(newStrs) > 0 {
		slices.Sort(newStrs)
		pi.strKeys.insertSorted(newStrs)
	}
	if len(nums) > 0 {
		pi.addOrderedKeys(nums)
	}
}

// addOrderedKeys is addOrdered for each id of nums[k], per sort key at once.
func (pi *PropertyIndex) addOrderedKeys(nums map[float64][]snowflake.ID) {
	if pi.numBuckets == nil {
		pi.numBuckets = make(map[float64]*IDPostings)
	}
	var newKeys []float64
	for k, ids := range nums {
		ids = sortedDistinct(ids)
		bucket := pi.numBuckets[k]
		if bucket == nil {
			bucket = &IDPostings{}
			pi.numBuckets[k] = bucket
			if !pi.rangeCounts {
				newKeys = append(newKeys, k)
			}
		}
		added := bucket.s.putSorted(ids)
		if pi.rangeCounts && added > 0 {
			pi.numKeys.addCount(k, int64(added))
		}
	}
	if len(newKeys) > 0 {
		slices.Sort(newKeys)
		pi.numKeys.insertSorted(newKeys)
	}
}

// sortedDistinct returns ids ascending without repeats, sorting in place
// only when they are not so already.
func sortedDistinct(ids []snowflake.ID) []snowflake.ID {
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			slices.Sort(ids)
			return slices.Compact(ids)
		}
	}
	return ids
}
