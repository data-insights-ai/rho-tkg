package index

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// testHooks counts hot-path work for tests (BATCHIDX); nil in production.
// Set only by a test that does not run in parallel.
var testHooks *hookCounts

type hookCounts struct {
	numericParse int // numericSortKey calls
	chunkInsert  int // sortedChunks.insert calls
}

// AddNodesToPropertyIndexes is AddNodeToPropertyIndexes for a batch of new
// nodes, nodes[i] under ids[i]. Caller must hold the store's write lock.
func AddNodesToPropertyIndexes(indexes map[PropertyIndexKey]*PropertyIndex, nodes []*types.Node, ids []snowflake.ID) {
	for i, n := range nodes {
		AddNodeToPropertyIndexes(indexes, n, ids[i])
	}
}
