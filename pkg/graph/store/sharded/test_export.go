package sharded

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
)

// ShardPendingWritesForTest returns each slot's pending write-buffer size, in
// slot order. Exported solely for tests; not for production use.
func (s *Store) ShardPendingWritesForTest() []int {
	out := make([]int, len(s.shards))
	for i, shard := range s.shards {
		out[i] = shard.PendingWriteCount()
	}
	return out
}

// ShardForIDForTest returns the slot's badger shard id routes to (the shard
// every door of id reads), so tests can flush it or evict its cache entries.
// Exported solely for tests; not for production use.
func (s *Store) ShardForIDForTest(id snowflake.ID) (*badger.Store, error) {
	return s.shardForID(id)
}
