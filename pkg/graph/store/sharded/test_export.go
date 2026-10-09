package sharded

// ShardPendingWritesForTest returns each slot's pending write-buffer size, in
// slot order. Exported solely for tests; not for production use.
func (s *Store) ShardPendingWritesForTest() []int {
	out := make([]int, len(s.shards))
	for i, shard := range s.shards {
		out[i] = shard.PendingWriteCount()
	}
	return out
}
