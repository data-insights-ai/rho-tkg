package badger

import (
	"slices"
	"strings"
	"sync/atomic"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

var _ storecontract.PropertyIndexValidEndCapability = (*Store)(nil)

// Valid-end bounds in on-disk property index entries (VALIDENDPRUNE, sigma's
// paged-row probes).
//
// An on-disk definition listed in Config.PropertyIndexValidEnd keeps in the
// value of each 0x0A entry of its nodes an upper bound on the node's effective
// valid end (storeutil.EncodeValidEndBound; empty: unknown), so a lookup with
// QueryOpts.ValidEndAfter leaves out the entries bounded below the bound
// without reading their rows. Entries are shared per property key, so a bound
// is the node's, whatever label's definition wrote it.
//
// A bound is never below its node's end at the current state:
//   - a write of a node's current row writes its listed entries unknown in the
//     row's batch (maintainPropertyIndexesAdd writes every entry empty, and
//     maintainPropertyIndexesUpdate keeps them: validEndEntryKeysLocked);
//   - a write of history rows alone writes them unknown in its batch, read
//     from the node's current row (noteValidEndPruneLocked, under wbMu);
//   - only a read sets a bound (RefineNodePropertyValidEnd): the end it
//     resolved from the node's timeline, and only if no row of the node was
//     written since the sequence it read before the lookup (the node's stripe;
//     a write bumps it under wbMu in the batch that buffers its rows, and the
//     refine checks it under wbMu).
//
// A definition's bounds are trusted with its built marker
// (PropertyIndexValidEndBuiltKey): set by its create or by the writable open
// that lists it (after writing its key's bounded entries unknown), deleted by
// a writable open that does not list it or by its drop. The first raises the
// format marker to 14: a release that does not reset the bounds refuses the
// store.

// validEndStripeCount is the number of write-sequence stripes a refine
// checks; two nodes sharing a stripe only skip a refine.
const validEndStripeCount = 4096

// validEndState is a store's pruning state.
type validEndState struct {
	listing []storecontract.PropertyIndexValidEnd
	trusted map[indexpkg.PropertyIndexKey]struct{}      // listed, on disk, marker present; under idxMu
	defs    atomic.Pointer[[]indexpkg.PropertyIndexKey] // trusted, read under wbMu without idxMu
	seq     atomic.Uint64                               // bumped per node row op while a definition is trusted
	floor   atomic.Uint64                               // Clear: a probe read below it is stale
	stripes [validEndStripeCount]atomic.Uint64          // the sequence of each stripe's last node row op

	pruned, unknown, refined, skipped atomic.Uint64
}

func validEndStripe(id snowflake.ID) int {
	return int(uint64(id) % validEndStripeCount) // #nosec G115 -- a modulus of the ID's bits
}

// validEndListed reports whether the property index on key is listed in
// PropertyIndexValidEnd, its label named as propIdxOnDiskWanted names it.
func (bs *Store) validEndListed(key indexpkg.PropertyIndexKey) bool {
	if len(bs.validEnd.listing) == 0 {
		return false
	}
	reg := bs.labelNames.Load()
	if reg == nil {
		reg = bs.propDiskNames
	}
	if reg == nil {
		return false
	}
	label := reg.Resolve(key.LabelToken)
	for _, l := range bs.validEnd.listing {
		if l.Label == label && strings.HasPrefix(key.PropertyKey, l.KeyPrefix) {
			return true
		}
	}
	return false
}

// refreshValidEndDefsLocked drops a trusted definition gone or in RAM and
// publishes the rest for the wbMu sections. Caller holds idxMu (write).
func (bs *Store) refreshValidEndDefsLocked() {
	var defs []indexpkg.PropertyIndexKey
	for key := range bs.validEnd.trusted {
		if !bs.propertyIndexes[key].OnDisk() {
			delete(bs.validEnd.trusted, key)
			continue
		}
		defs = append(defs, key)
	}
	if len(defs) == 0 {
		bs.validEnd.defs.Store(nil)
		return
	}
	bs.validEnd.defs.Store(&defs)
}

// validEndEntryOps are the set ops of n's entries in the trusted definitions,
// each with value (empty: unknown), one per entry key.
func (bs *Store) validEndEntryOps(defs []indexpkg.PropertyIndexKey, n *types.Node, id snowflake.ID, value []byte) []writeOp {
	var ops []writeOp
	for _, def := range defs {
		if !n.HasLabelTokenRaw(def.LabelToken) {
			continue
		}
		valueKey, _ := n.IndexablePropertyValueKey(def.PropertyKey)
		if valueKey == "" {
			continue
		}
		op, ok := bs.propertyIndexDiskOp(def.PropertyKey, valueKey, id, writeOpSet)
		if !ok || slices.ContainsFunc(ops, func(o writeOp) bool { return string(o.key) == string(op.key) }) {
			continue
		}
		op.value = value
		ops = append(ops, op)
	}
	return ops
}

// validEndEntryKeysLocked are the keys of n's entries in the trusted
// definitions (nil without any): an update writes them again, unknown, though
// their value did not change. Caller holds idxMu.
func (bs *Store) validEndEntryKeysLocked(n *types.Node, id snowflake.ID) map[string]struct{} {
	defs := bs.validEnd.defs.Load()
	if defs == nil {
		return nil
	}
	var keys map[string]struct{}
	for _, op := range bs.validEndEntryOps(*defs, n, id, nil) {
		if keys == nil {
			keys = make(map[string]struct{})
		}
		keys[string(op.key)] = struct{}{}
	}
	return keys
}

// noteValidEndPruneLocked bumps the stripe of every node whose row ops are
// buffered and returns ops with, for each node with history ops but no
// current-row op among them, its trusted entries written unknown (a
// current-row write maintains its entries itself). The node's current row is
// read from the cache (an unflushed row is never evicted) or badger, without
// idxMu (a caller may hold it). Caller holds wbMu.
func (bs *Store) noteValidEndPruneLocked(ops []writeOp) []writeOp {
	defs := bs.validEnd.defs.Load()
	if defs == nil {
		return ops
	}
	var current, history []snowflake.ID
	var opKeys map[string]struct{}
	for _, op := range ops {
		key := op.key
		switch {
		case len(key) == storepkg.SizeNodeKey && key[0] == storepkg.KeyNode:
			current = append(current, storepkg.ParseIDFromKey(key, 1))
		case len(key) == storepkg.SizeHistKey && key[0] == storepkg.KeyHistNode:
			history = append(history, storepkg.ParseIDFromKey(key, 1))
		default:
			continue
		}
		bs.validEnd.stripes[validEndStripe(storepkg.ParseIDFromKey(key, 1))].Store(bs.validEnd.seq.Add(1))
	}
	var extra []writeOp
	for _, id := range history {
		if slices.Contains(current, id) {
			continue
		}
		n := bs.validEndCurrentRow(id)
		if n == nil {
			continue
		}
		if opKeys == nil {
			opKeys = make(map[string]struct{}, len(ops))
			for _, op := range ops {
				opKeys[string(op.key)] = struct{}{}
			}
		}
		for _, op := range bs.validEndEntryOps(*defs, n, id, nil) {
			if _, dup := opKeys[string(op.key)]; !dup {
				opKeys[string(op.key)] = struct{}{}
				extra = append(extra, op)
			}
		}
	}
	if extra == nil {
		return ops
	}
	return append(ops[:len(ops):len(ops)], extra...)
}

// validEndCurrentRow is id's current row: the write buffer's, the flush's in
// progress, the cache's, else badger's; nil without one or on a read error.
// Takes no store lock but the cache's. Caller holds wbMu.
func (bs *Store) validEndCurrentRow(id snowflake.ID) *types.Node {
	key := storepkg.NodeKey(id)
	for _, buf := range []map[string]writeOp{bs.pending, bs.flushing} {
		if op, ok := buf[string(key)]; ok {
			if op.opType == writeOpDelete {
				return nil
			}
			return bs.validEndDecodeRow(id, op.value)
		}
	}
	switch n, status := bs.nodeCache.GetNoPromote(id); status {
	case indexpkg.CacheHit:
		return n
	case indexpkg.CacheDeleted:
		return nil
	}
	var n *types.Node
	_ = bs.view(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			n = bs.validEndDecodeRow(id, val)
			return nil
		})
	})
	return n
}

// validEndDecodeRow decodes a current node row's value, nil on an error.
func (bs *Store) validEndDecodeRow(id snowflake.ID, val []byte) *types.Node {
	w := getPooledNodeWire()
	defer putPooledNodeWire(w)
	if err := storepkg.SafeUnmarshal(val, w); err != nil {
		return nil
	}
	n, err := bs.decodeNodeWireForKey(*w, id)
	if err != nil {
		return nil
	}
	return n
}

// NodesByLabelAndPropertyKeysValidEnd implements
// store.PropertyIndexValidEndCapability: NodesByLabelAndPropertyKeys without
// the entries a trusted definition bounds below opts.ValidEndAfter, a probe per
// node. A definition not trusted (in RAM, not listed, unmarked, building)
// answers NodesByLabelAndPropertyKeys with nil probes: nothing to refine.
func (bs *Store) NodesByLabelAndPropertyKeysValidEnd(labelToken uint16, propKey string, valueKeys []string, opts QueryOpts) ([]*types.Node, []storecontract.ValidEndProbe, error) {
	bound := opts.ValidEndAfter
	opts.ValidEndAfter, opts.ValidEndAfterSet = 0, false
	if err := bs.checkOpen(); err != nil {
		return nil, nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, nil, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propKey); err != nil {
		return nil, nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, nil, err
	}
	match := storepkg.DistinctIndexKeyMatch(valueKeys)
	targetKeys := match.Keys()
	if len(targetKeys) == 0 {
		return nil, nil, nil
	}
	seq := bs.validEnd.seq.Load() // before any row is read
	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propKey}
	bs.idxMu.RLock()
	idx, ok := bs.propertyIndexes[key]
	_, trusted := bs.validEnd.trusted[key]
	if !ok || !trusted || !idx.OnDisk() || idx.Mutated != nil {
		bs.idxMu.RUnlock()
		nodes, err := bs.NodesByLabelAndPropertyKeys(labelToken, propKey, valueKeys, opts)
		return nodes, nil, err
	}
	bounds := make(map[types.NodeID]types.Instant)
	nids, err := bs.propertyIndexDiskKeysCandidatesLocked(propKey, targetKeys, bounds)
	bs.idxMu.RUnlock()
	if err != nil || len(nids) == 0 {
		return nil, nil, err
	}
	storepkg.SortNodeIDs(nids)
	nids = slices.Compact(nids)
	kept := nids[:0]
	for _, nid := range nids {
		if b, known := bounds[nid]; known && b < bound {
			if types.EntityID(nid) > opts.After {
				bs.validEnd.pruned.Add(1)
			}
			continue
		}
		kept = append(kept, nid)
	}
	nodes, err := bs.fetchNodesByLabelPropertyIDs(labelToken, propKey, match, false, kept, opts)
	if err != nil {
		return nil, nil, err
	}
	probes := make([]storecontract.ValidEndProbe, len(nodes))
	for i, n := range nodes {
		b, known := bounds[n.ID()]
		probes[i] = storecontract.ValidEndProbe{Bound: b, Known: known, Seq: seq}
		if !known {
			bs.validEnd.unknown.Add(1)
		}
	}
	return nodes, probes, nil
}

// RefineNodePropertyValidEnd implements store.PropertyIndexValidEndCapability:
// n's entries in the trusted definitions take end, unless a row of n was
// buffered after probe.Seq was read or the store was cleared since. Index
// entries only: no row, stamp or change record. A read-only or closed store
// refines nothing.
func (bs *Store) RefineNodePropertyValidEnd(n *types.Node, probe storecontract.ValidEndProbe, end types.Instant) {
	if n == nil || bs.checkWritable() != nil {
		return
	}
	defs := bs.validEnd.defs.Load()
	if defs == nil {
		return
	}
	id := n.ID().SnowflakeID()
	ops := bs.validEndEntryOps(*defs, n, id, storepkg.EncodeValidEndBound(end))
	if len(ops) == 0 {
		return
	}
	bs.wbMu.Lock()
	if probe.Seq < bs.validEnd.floor.Load() || bs.validEnd.stripes[validEndStripe(id)].Load() > probe.Seq {
		bs.wbMu.Unlock()
		bs.validEnd.skipped.Add(1)
		return
	}
	for _, op := range ops {
		bs.putPendingLocked(op)
	}
	bs.wbMu.Unlock()
	bs.validEnd.refined.Add(1)
}

// PropertyIndexValidEndStats implements store.PropertyIndexValidEndCapability.
func (bs *Store) PropertyIndexValidEndStats() storecontract.PropertyIndexValidEndStats {
	return storecontract.PropertyIndexValidEndStats{
		Pruned:        bs.validEnd.pruned.Load(),
		UnknownRead:   bs.validEnd.unknown.Load(),
		Refined:       bs.validEnd.refined.Load(),
		RefineSkipped: bs.validEnd.skipped.Load(),
	}
}

// validEndResetOps writes unknown every entry of propertyKey that carries a
// bound: a definition listed again trusts none it did not maintain.
func (bs *Store) validEndResetOps(txn *badgerv4.Txn, propertyKey string) []writeOp {
	tok, ok := bs.propKeyTokenFor(propertyKey)
	if !ok {
		return nil
	}
	var ops []writeOp
	it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: storepkg.PropertyIndexTokenPrefix(tok)})
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		if it.Item().ValueSize() > 0 {
			ops = append(ops, writeOp{opType: writeOpSet, key: it.Item().KeyCopy(nil), value: []byte{}})
		}
	}
	return ops
}
