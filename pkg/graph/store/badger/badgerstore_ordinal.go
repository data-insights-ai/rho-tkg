package badger

import (
	"math"
	"sync"
	"sync/atomic"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Dense ordinals (store.OrdinalCapability). Not persisted: loadIndexes numbers
// stored nodes and rels 1..N in ID order at open, each new ID takes the next, and
// ID-to-ordinal maps live in RAM (leaf lock ordMu, never held while taking another
// lock). Rows handed out carry their entity's ordinal; history rows of an entity
// without a current row carry 0, as does every row with Config.DisableOrdinals
// (tiered sets it: a reopened cold shard would renumber).

// OrdinalAllocator hands out node and rel ordinals; stores that must not collide
// (sharded slots) share one via Config.SharedOrdinals. Zero value is ready.
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
	// foreign resolves an endpoint this store holds no ordinal for
	// (Config.ForeignNodeOrdinal); nil = 0.
	foreign func(types.NodeID) uint32
}

func newOrdinals(cfg Config) *ordinals {
	o := &ordinals{alloc: cfg.SharedOrdinals, disabled: cfg.DisableOrdinals, foreign: cfg.ForeignNodeOrdinal}
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

// NodeOrdinal returns the dense ordinal of a node this store holds a current row
// for, else 0 (or with DisableOrdinals). Reads this store's map only, never
// Config.ForeignNodeOrdinal.
func (bs *Store) NodeOrdinal(id types.NodeID) uint32 {
	if bs == nil {
		return 0
	}
	return bs.ords.node(id, false)
}

// endpointNodeOrdinal is this store's ordinal for a rel endpoint, else
// Config.ForeignNodeOrdinal for a node held elsewhere.
func (bs *Store) endpointNodeOrdinal(id types.NodeID) uint32 {
	if ord := bs.ords.node(id, false); ord != 0 {
		return ord
	}
	return bs.foreignNodeOrdinal(id)
}

// foreignNodeOrdinal asks Config.ForeignNodeOrdinal; 0 without one.
func (bs *Store) foreignNodeOrdinal(id types.NodeID) uint32 {
	o := bs.ords
	if o == nil || o.disabled || o.foreign == nil {
		return 0
	}
	return o.foreign(id)
}

// relEndpointOrdinals returns the ordinals of r's endpoints, the values a
// current row the store keeps or hands out carries.
func (bs *Store) relEndpointOrdinals(r *types.Relationship) (start, end uint32) {
	start = bs.endpointNodeOrdinal(r.StartNodeID())
	if r.EndNodeID() == r.StartNodeID() {
		return start, start
	}
	return start, bs.endpointNodeOrdinal(r.EndNodeID())
}

// HasOrdinals is false when the store was opened with DisableOrdinals.
func (bs *Store) HasOrdinals() bool { return bs != nil && bs.ords != nil && !bs.ords.disabled }

// columnOrdinalCache holds ordinals of a column snapshot's rows per label and rel
// type, keyed by the snapshot: snapshots are immutable and an entity keeps its
// ordinal while current, so they never change.
type columnOrdinalCache struct {
	nodes map[uint16]snapshotOrdinals[types.NodeID]
	rels  map[uint16]snapshotOrdinals[types.RelID]
	// multi: a label intersection's snapshot, by its cache key (round 6
	// request 11, ForEachDocValuesMultiOrdinal).
	multi map[string]snapshotOrdinals[types.NodeID]
}

type snapshotOrdinals[T indexpkg.EntityID] struct {
	col  *indexpkg.DocValues[T]
	ords []uint32
	// starts/ends: a relationship snapshot's endpoints' ordinals, aligned
	// like ords (nil for a label's snapshot).
	starts, ends []uint32
}

// labelColumnOrdinals returns the ordinals aligned with col.IDs(), from the
// cache when col is the snapshot they were read for.
func (bs *Store) labelColumnOrdinals(token uint16, col *indexpkg.LabelDocValues) []uint32 {
	bs.docMu.Lock()
	c, ok := bs.colOrds.nodes[token]
	bs.docMu.Unlock()
	if ok && (c.col == col || indexpkg.SameIDs(c.col, col)) {
		// A patched snapshot shares its predecessor's ID vector: same rows, same ordinals.
		if c.col != col {
			bs.docMu.Lock()
			bs.colOrds.nodes[token] = snapshotOrdinals[types.NodeID]{col: col, ords: c.ords}
			bs.docMu.Unlock()
		}
		return c.ords
	}
	ords := bs.ords.nodeOrdinals(col.IDs(), make([]uint32, 0, col.Len()))
	bs.docMu.Lock()
	if bs.colOrds.nodes == nil {
		bs.colOrds.nodes = make(map[uint16]snapshotOrdinals[types.NodeID])
	}
	bs.colOrds.nodes[token] = snapshotOrdinals[types.NodeID]{col: col, ords: ords}
	bs.docMu.Unlock()
	return ords
}

// multiColumnOrdinals is labelColumnOrdinals for a label intersection's
// snapshot, keyed by the intersection's cache key.
func (bs *Store) multiColumnOrdinals(key string, col *indexpkg.LabelDocValues) []uint32 {
	bs.docMu.Lock()
	c, ok := bs.colOrds.multi[key]
	bs.docMu.Unlock()
	if ok && (c.col == col || indexpkg.SameIDs(c.col, col)) {
		if c.col != col {
			bs.docMu.Lock()
			bs.colOrds.multi[key] = snapshotOrdinals[types.NodeID]{col: col, ords: c.ords}
			bs.docMu.Unlock()
		}
		return c.ords
	}
	ords := bs.ords.nodeOrdinals(col.IDs(), make([]uint32, 0, col.Len()))
	bs.docMu.Lock()
	if bs.colOrds.multi == nil {
		bs.colOrds.multi = make(map[string]snapshotOrdinals[types.NodeID])
	}
	bs.colOrds.multi[key] = snapshotOrdinals[types.NodeID]{col: col, ords: ords}
	bs.docMu.Unlock()
	return ords
}

// relColumnOrdinals is labelColumnOrdinals for a rel type's snapshot, with its
// endpoints' ordinals (starts, ends: aligned with its IDs), read once per snapshot.
func (bs *Store) relColumnOrdinals(token uint16, col *indexpkg.DocValues[types.RelID], startIDs, endIDs []int64) (ords, starts, ends []uint32) {
	bs.docMu.Lock()
	c, ok := bs.colOrds.rels[token]
	bs.docMu.Unlock()
	if ok && c.col == col {
		return c.ords, c.starts, c.ends
	}
	ords = bs.ords.relOrdinals(col.IDs(), make([]uint32, 0, col.Len()))
	starts = bs.endpointOrdinals(startIDs)
	ends = bs.endpointOrdinals(endIDs)
	bs.docMu.Lock()
	if bs.colOrds.rels == nil {
		bs.colOrds.rels = make(map[uint16]snapshotOrdinals[types.RelID])
	}
	bs.colOrds.rels[token] = snapshotOrdinals[types.RelID]{col: col, ords: ords, starts: starts, ends: ends}
	bs.docMu.Unlock()
	return ords, starts, ends
}

// endpointOrdinals returns the node ordinals of the raw node IDs of a
// relationship snapshot's endpoint column.
func (bs *Store) endpointOrdinals(raw []int64) []uint32 {
	out := make([]uint32, len(raw))
	o := bs.ords
	if o == nil || o.disabled {
		return out
	}
	o.mu.RLock()
	for i, id := range raw {
		out[i] = o.nodes[types.NodeID(id)]
	}
	o.mu.RUnlock()
	for i, id := range raw {
		if out[i] == 0 {
			out[i] = bs.foreignNodeOrdinal(types.NodeID(id))
		}
	}
	return out
}

// nodeOrdinals appends the ordinals of ids to out (0 for none, or all when
// ordinals are off) under one read lock: the column scans' batch fill.
func (o *ordinals) nodeOrdinals(ids []types.NodeID, out []uint32) []uint32 {
	if o == nil || o.disabled {
		return append(out, make([]uint32, len(ids))...)
	}
	o.mu.RLock()
	for _, id := range ids {
		out = append(out, o.nodes[id])
	}
	o.mu.RUnlock()
	return out
}

// relOrdinals is nodeOrdinals for relationships.
func (o *ordinals) relOrdinals(ids []types.RelID, out []uint32) []uint32 {
	if o == nil || o.disabled {
		return append(out, make([]uint32, len(ids))...)
	}
	o.mu.RLock()
	for _, id := range ids {
		out = append(out, o.rels[id])
	}
	o.mu.RUnlock()
	return out
}

// adjacentOrdinals returns, for each adjacency entry, the relationship's and
// the other endpoint's ordinal (pairs, in order), under one read lock.
func (o *ordinals) adjacentOrdinals(metas []adjMeta) []uint32 {
	out := make([]uint32, 2*len(metas))
	if o == nil || o.disabled {
		return out
	}
	o.mu.RLock()
	for i, m := range metas {
		out[2*i] = o.rels[m.rel]
		out[2*i+1] = o.nodes[m.other]
	}
	o.mu.RUnlock()
	return out
}
