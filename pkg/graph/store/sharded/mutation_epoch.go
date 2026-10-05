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

// MaxNodeOrdinal / MaxRelOrdinal report the shared dense-ordinal allocator
// (store.OrdinalCapability): every shard draws from it, so ordinals are
// unique across slots. All shards stay open for the store's life, so an
// entity's ordinal is stable while the store is open. 0 after Close.
func (s *Store) MaxNodeOrdinal() uint32 {
	if s == nil || s.checkOpen() != nil {
		return 0
	}
	return s.ordinals.MaxNodeOrdinal()
}

func (s *Store) MaxRelOrdinal() uint32 {
	if s == nil || s.checkOpen() != nil {
		return 0
	}
	return s.ordinals.MaxRelOrdinal()
}

// HasOrdinals is true: every slot numbers its entities from the shared allocator.
func (s *Store) HasOrdinals() bool { return s != nil }
