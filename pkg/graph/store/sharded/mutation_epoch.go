package sharded

// NodeMutationEpoch / RelMutationEpoch expose a monotonic mutation counter that
// advances on every node/rel write, folded across all slots. A consumer keys a
// read cache on these to invalidate it after writes. Each per-slot badger shard
// keeps its own monotonic epoch; the SUM across shards is itself monotonic
// (a write increments exactly one shard's counter, so the sum increments), so it
// changes on every mutation regardless of which slot took the write. Without
// this the sharded backend defaulted to a constant 0, so an epoch-keyed consumer
// cache never invalidated and served STALE reads after writes on a multi-lane
// deployment. Mirrors the tiered store's cross-shard epoch fold.

func (s *Store) NodeMutationEpoch() uint64 {
	if s == nil {
		return 0
	}
	var sum uint64
	for _, shard := range s.shards {
		if shard != nil {
			sum += shard.NodeMutationEpoch()
		}
	}
	return sum
}

func (s *Store) RelMutationEpoch() uint64 {
	if s == nil {
		return 0
	}
	var sum uint64
	for _, shard := range s.shards {
		if shard != nil {
			sum += shard.RelMutationEpoch()
		}
	}
	return sum
}
