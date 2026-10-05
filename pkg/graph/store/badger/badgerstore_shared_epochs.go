package badger

import "sync/atomic"

// SharedMutationEpochs are a node and a relationship mutation counter that
// several stores advance together (Config.SharedMutationEpochs). Each store
// still keeps its own epochs, which its column caches are keyed on; the
// shared counters move once per own-epoch bump of any store they were handed
// to, so their owner (the tiered store) reports epochs that move on every
// write of every shard. A sum of the open shards' own epochs does not: a
// shard that took a write and was closed before the next read contributes
// nothing, and the sum can return to a value a reader already holds.
//
// The zero value is ready to use. Methods are safe for concurrent use.
type SharedMutationEpochs struct {
	nodes atomic.Uint64
	rels  atomic.Uint64
}

// NodeMutationEpoch returns the shared node counter; 0 for nil.
func (e *SharedMutationEpochs) NodeMutationEpoch() uint64 {
	if e == nil {
		return 0
	}
	return e.nodes.Load()
}

// RelMutationEpoch returns the shared relationship counter; 0 for nil.
func (e *SharedMutationEpochs) RelMutationEpoch() uint64 {
	if e == nil {
		return 0
	}
	return e.rels.Load()
}

func (e *SharedMutationEpochs) node() {
	if e != nil {
		e.nodes.Add(1)
	}
}

func (e *SharedMutationEpochs) rel() {
	if e != nil {
		e.rels.Add(1)
	}
}
