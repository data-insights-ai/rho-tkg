package badger

import (
	"math"
	"sync"
	"sync/atomic"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Dense ordinals (store.OrdinalCapability) on the badger store. Ordinals are
// not persisted: loadIndexes numbers the stored nodes and relationships 1..N
// in ID order when the store opens, every write of a new ID takes the next
// one, and the store keeps ID → ordinal maps in RAM (a leaf lock, ordMu, that
// is never held while another lock is taken). Every row the store hands out
// carries its entity's ordinal: the cached rows are built with it, a decoded
// row looks it up. A history row of an entity that has no current row any
// more carries 0, as does every row when Config.DisableOrdinals is set (the
// tiered store sets it: a cold shard that closes and reopens would renumber
// its entities while the store stays open).

// OrdinalAllocator hands out node and relationship ordinals. Several stores
// that must not hand out the same ordinal (the sharded store's slots) share
// one through Config.SharedOrdinals. The zero value is ready.
type OrdinalAllocator struct {
	nodes atomic.Uint32
	rels  atomic.Uint32
}

func (a *OrdinalAllocator) nextNode() uint32 { return nextOrdinal(&a.nodes) }
func (a *OrdinalAllocator) nextRel() uint32  { return nextOrdinal(&a.rels) }

// MaxNodeOrdinal returns the largest node ordinal handed out; 0 for nil.
func (a *OrdinalAllocator) MaxNodeOrdinal() uint32 {
	if a == nil {
		return 0
	}
	return a.nodes.Load()
}

// MaxRelOrdinal returns the largest relationship ordinal handed out.
func (a *OrdinalAllocator) MaxRelOrdinal() uint32 {
	if a == nil {
		return 0
	}
	return a.rels.Load()
}

func nextOrdinal(c *atomic.Uint32) uint32 {
	for {
		cur := c.Load()
		if cur == math.MaxUint32 {
			return 0
		}
		if c.CompareAndSwap(cur, cur+1) {
			return cur + 1
		}
	}
}

// ordinals is the store's ID → ordinal state.
type ordinals struct {
	mu       sync.RWMutex
	nodes    map[types.NodeID]uint32
	rels     map[types.RelID]uint32
	alloc    *OrdinalAllocator
	disabled bool
}

func newOrdinals(cfg Config) *ordinals {
	o := &ordinals{alloc: cfg.SharedOrdinals, disabled: cfg.DisableOrdinals}
	if o.alloc == nil {
		o.alloc = &OrdinalAllocator{}
	}
	o.nodes = make(map[types.NodeID]uint32)
	o.rels = make(map[types.RelID]uint32)
	return o
}

func (o *ordinals) node(id types.NodeID, assign bool) uint32 {
	if o == nil || o.disabled {
		return 0
	}
	o.mu.RLock()
	ord, ok := o.nodes[id]
	o.mu.RUnlock()
	if ok || !assign {
		return ord
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if ord, ok := o.nodes[id]; ok {
		return ord
	}
	ord = o.alloc.nextNode()
	if ord != 0 {
		o.nodes[id] = ord
	}
	return ord
}

func (o *ordinals) rel(id types.RelID, assign bool) uint32 {
	if o == nil || o.disabled {
		return 0
	}
	o.mu.RLock()
	ord, ok := o.rels[id]
	o.mu.RUnlock()
	if ok || !assign {
		return ord
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if ord, ok := o.rels[id]; ok {
		return ord
	}
	ord = o.alloc.nextRel()
	if ord != 0 {
		o.rels[id] = ord
	}
	return ord
}

func (o *ordinals) dropNode(id types.NodeID) {
	if o == nil || o.disabled {
		return
	}
	o.mu.Lock()
	delete(o.nodes, id)
	o.mu.Unlock()
}

func (o *ordinals) dropRel(id types.RelID) {
	if o == nil || o.disabled {
		return
	}
	o.mu.Lock()
	delete(o.rels, id)
	o.mu.Unlock()
}

func (o *ordinals) reset() {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.nodes = make(map[types.NodeID]uint32)
	o.rels = make(map[types.RelID]uint32)
	o.mu.Unlock()
}

var _ storecontract.OrdinalCapability = (*Store)(nil)

// MaxNodeOrdinal returns the largest node ordinal handed out (shared with
// the other stores of a SharedOrdinals allocator); 0 when disabled or closed.
func (bs *Store) MaxNodeOrdinal() uint32 {
	if bs == nil || bs.ords == nil || bs.ords.disabled || bs.dbClosed.Load() {
		return 0
	}
	return bs.ords.alloc.MaxNodeOrdinal()
}

// MaxRelOrdinal returns the largest relationship ordinal handed out.
func (bs *Store) MaxRelOrdinal() uint32 {
	if bs == nil || bs.ords == nil || bs.ords.disabled || bs.dbClosed.Load() {
		return 0
	}
	return bs.ords.alloc.MaxRelOrdinal()
}

// HasOrdinals is false when the store was opened with DisableOrdinals.
func (bs *Store) HasOrdinals() bool { return bs != nil && bs.ords != nil && !bs.ords.disabled }
