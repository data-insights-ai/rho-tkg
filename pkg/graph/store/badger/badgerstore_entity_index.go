package badger

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The 0x0C entity index (RELDISK, storeutil/entity_index_key.go): the entries
// of the property indexes kept on disk that the 0x0A keyspace does not hold.
//
//   - A relationship definition on disk (RelPropertyIndexOnDiskTypes) keeps its
//     current entries (EntityIndexRelCurrent, the RAM value maps' answer) and
//     its member entries (EntityIndexRelMember, the membership sidecar's).
//   - A node definition on disk (PropertyIndexOnDisk, PropertyIndexOnDiskLabels)
//     keeps its current entries in 0x0A and its member entries here
//     (EntityIndexNodeMember) once built (propDiskDef.members).
//
// Every door writes a definition's entries in the batch of the row it writes,
// under idxMu.Lock: a current row's current entry and member entry, a history
// row's member entry, a removed current row's current-entry delete. A member
// entry is never deleted by a door: a pinned read needs it as long as a row
// carrying the value exists (history included); a retention purge, an exact
// erasure, a drop and Clear delete them. A reader takes, per entity, the
// lowest transaction time of its member entries (indexpkg.MergeFirstTx), so a
// door may write one again (the same bytes) or with a later time, and a read
// at a pin the membership doors serve from disk builds no sidecar.

// relDiskDef is one relationship definition kept on disk (Store.relDiskDefs).
type relDiskDef struct {
	key indexpkg.RelPropertyIndexKey
	idx *indexpkg.PropertyIndex
}

// refreshRelDiskDefsLocked rebuilds relDiskDefs from relPropertyIndexes.
// Caller holds idxMu.
func (bs *Store) refreshRelDiskDefsLocked() {
	var defs []relDiskDef
	for key, idx := range bs.relPropertyIndexes {
		if idx.OnDisk() {
			defs = append(defs, relDiskDef{key, idx})
		}
	}
	bs.relDiskDefs = defs
}

// relIdxOnDiskWanted reports whether a property index on relTypeToken keeps
// its entries on disk (RelPropertyIndexOnDiskTypes), the type named by the
// graph's registry or else the one the open read. A type no registry names
// stays in RAM.
func (bs *Store) relIdxOnDiskWanted(relTypeToken uint16) bool {
	if len(bs.relDiskTypes) == 0 {
		return false
	}
	reg := bs.relTypeNames.Load()
	if reg == nil {
		reg = bs.relDiskNames
	}
	if reg == nil {
		return false
	}
	_, ok := bs.relDiskTypes[reg.Resolve(relTypeToken)]
	return ok
}

// entityIndexValuePrefix is the value prefix of (kind, scope, propertyKey,
// valueKey); ok=false when the value has no payload or the property key no
// token (callers skip the entry, as propertyIndexDiskOp).
func (bs *Store) entityIndexValuePrefix(kind byte, scope uint16, propertyKey, valueKey string) ([]byte, bool) {
	tok, ok := bs.propKeyTokenFor(propertyKey)
	if !ok {
		return nil, false
	}
	b := make([]byte, 0, storepkg.EntityIndexHeadLen+storepkg.EntityIndexValueLen(valueKey)+storepkg.EntityIndexTailLen(kind))
	b = storepkg.AppendEntityIndexHead(b, kind, scope, tok)
	return storepkg.AppendEntityIndexValue(b, valueKey)
}

// entityIndexOp appends the op of one entry to ops.
func (bs *Store) entityIndexOp(ops []writeOp, kind byte, scope uint16, propertyKey, valueKey string, id snowflake.ID, txFrom types.Instant, opType writeOpType) []writeOp {
	prefix, ok := bs.entityIndexValuePrefix(kind, scope, propertyKey, valueKey)
	if !ok {
		return ops
	}
	key := storepkg.AppendEntityIndexEntry(prefix, kind, id, int64(txFrom))
	if opType == writeOpDelete {
		return append(ops, writeOp{opType: writeOpDelete, key: key})
	}
	return append(ops, writeOp{opType: writeOpSet, key: key, value: []byte{}})
}

// relIndexDiskOpsLocked returns the entries of r's on-disk definitions a door
// writes: for a current row (set) its current and member entries, for a
// history row (current false) its member entries, for a removed current row
// (delete) its current-entry deletes. Each definition being created records
// the id of a current row (Mutated). Caller holds idxMu.Lock (a history row:
// idxMu, R or W).
func (bs *Store) relIndexDiskOpsLocked(r *types.Relationship, id snowflake.ID, opType writeOpType, current bool) []writeOp {
	if len(bs.relDiskDefs) == 0 || r == nil {
		return nil
	}
	tok := r.TypeToken().Value()
	txFrom, _, _ := r.TxStamps()
	var ops []writeOp
	for _, def := range bs.relDiskDefs {
		if def.key.RelTypeToken != tok {
			continue
		}
		valueKey, _ := r.IndexablePropertyValueKey(def.key.PropertyKey)
		if valueKey != "" {
			if current {
				ops = bs.entityIndexOp(ops, storepkg.EntityIndexRelCurrent, tok, def.key.PropertyKey, valueKey, id, 0, opType)
			}
			if opType == writeOpSet {
				ops = bs.entityIndexOp(ops, storepkg.EntityIndexRelMember, tok, def.key.PropertyKey, valueKey, id, txFrom, writeOpSet)
			}
		}
		if current && def.idx.Mutated != nil {
			def.idx.Mutated[id] = struct{}{}
		}
	}
	return ops
}

// relEntriesOfLocked returns deletes of id's entries on disk, current and
// member, found by walking every on-disk definition's: the removal of a row
// that cannot be read (corruption only), O(index size). Caller holds idxMu.
func (bs *Store) relEntriesOfLocked(id snowflake.ID) []writeOp {
	var ops []writeOp
	for _, def := range bs.relDiskDefs {
		for _, head := range bs.entityIndexHeads(true, def.key.RelTypeToken, def.key.PropertyKey) {
			kind := head[1]
			_ = bs.entityIndexRangeLocked(head, head, nil, func(key []byte) {
				if _, eid, _, ok := storepkg.EntityIndexEntryOf(key, kind); ok && eid == id {
					ops = append(ops, writeOp{opType: writeOpDelete, key: slices.Clone(key)})
				}
			})
		}
	}
	return ops
}

// nodeMemberOpsLocked returns the member entries of n's on-disk node
// definitions whose members are on disk, for a row n (current or history).
// Caller holds idxMu.Lock.
func (bs *Store) nodeMemberOpsLocked(n *types.Node, id snowflake.ID) []writeOp {
	if !bs.nodeMembersOnDisk || n == nil {
		return nil
	}
	txFrom, _, _ := n.TxStamps()
	var ops []writeOp
	for _, def := range bs.propDiskDefs {
		if !def.members || !n.HasLabelTokenRaw(def.key.LabelToken) {
			continue
		}
		if valueKey, _ := n.IndexablePropertyValueKey(def.key.PropertyKey); valueKey != "" {
			ops = bs.entityIndexOp(ops, storepkg.EntityIndexNodeMember, def.key.LabelToken, def.key.PropertyKey, valueKey, id, txFrom, writeOpSet)
		}
	}
	return ops
}

// relMembersOnDiskLocked reports whether the relationship definition on key
// answers its membership from 0x0C. Caller holds idxMu.
func (bs *Store) relMembersOnDiskLocked(key indexpkg.RelPropertyIndexKey) bool {
	return bs.relPropertyIndexes[key].OnDisk()
}

// nodeMembersOnDiskLocked reports whether the node definition on key answers
// its membership from 0x0C. Caller holds idxMu.
func (bs *Store) nodeMembersOnDiskLocked(key indexpkg.PropertyIndexKey) bool {
	_, ok := bs.nodeMemberDisk[key]
	return ok
}

// entityIndexBuildingLocked reports whether a relationship definition kept on
// disk is being created. Caller holds idxMu.
func (bs *Store) entityIndexBuildingLocked() bool {
	for _, def := range bs.relDiskDefs {
		if def.idx.Mutated != nil {
			return true
		}
	}
	return false
}

// dropUnchangedEntityOps drops from ops what an update writes again for a
// value its old row carried: the delete and set of a current entry (the
// entry stays as it is, PROPREWRITE) and the member entry of the new row (the
// old row's member entry, as low or lower, stays). from splits the old row's
// ops (current-entry deletes) from the new row's. Not while a definition is
// created (its backfill skips the rows a door marked).
func (bs *Store) dropUnchangedEntityOps(ops []writeOp, from int, oldTx, newTx types.Instant) []writeOp {
	if from == 0 || from == len(ops) || bs.entityIndexBuildingLocked() {
		return ops
	}
	removed := make(map[string]struct{}, from)
	for _, op := range ops[:from] {
		removed[string(op.key)] = struct{}{}
	}
	keepMember := newTx < oldTx // the new row lowers the bound (0, unknown, is lowest)
	var unchanged map[string]struct{}
	for _, op := range ops[from:] {
		if op.key[1] != storepkg.EntityIndexRelCurrent {
			continue
		}
		if _, ok := removed[string(op.key)]; ok {
			if unchanged == nil {
				unchanged = make(map[string]struct{})
			}
			unchanged[string(op.key[:len(op.key)-8])] = struct{}{} // the value prefix
		}
	}
	if unchanged == nil {
		return ops
	}
	kept := ops[:0]
	for _, op := range ops {
		switch op.key[1] {
		case storepkg.EntityIndexRelCurrent:
			if _, ok := unchanged[string(op.key[:len(op.key)-8])]; ok {
				continue
			}
		case storepkg.EntityIndexRelMember:
			if _, ok := unchanged[string(memberCurrentPrefix(op.key))]; ok && !keepMember {
				continue
			}
		}
		kept = append(kept, op)
	}
	return kept
}

// memberCurrentPrefix maps a relationship member entry to the value prefix of
// the current entry of the same definition and value.
func memberCurrentPrefix(key []byte) []byte {
	p := slices.Clone(key[:len(key)-16])
	p[1] = storepkg.EntityIndexRelCurrent
	return p
}

// entityIndexWalkLocked visits once each live entry of kind under the value
// prefixes (exact: a longer raw value is not visited): the persisted keys'
// newest version and the write buffer's, resolved per key (lesson 57). The
// walk takes every version (AllVersions) and stops at each prefix's end, so
// deleted entries outside the prefixes are never walked (docs/badger.md rule
// 12). Caller holds idxMu (R or W).
func (bs *Store) entityIndexWalkLocked(kind byte, prefixes [][]byte, fn func(key []byte)) error {
	if len(prefixes) == 0 {
		return nil
	}
	slices.SortFunc(prefixes, bytes.Compare)
	prefixes = slices.CompactFunc(prefixes, bytes.Equal)
	tail := storepkg.EntityIndexTailLen(kind)
	ov := pendingKeyOverlay[struct{}]{}
	bs.rangePendingPrefixes(prefixes, func(k string, op writeOp) { ov.note(k, op, struct{}{}) })
	err := bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.AllVersions = true
		opts.Prefix = prefixes[0][:storepkg.EntityIndexHeadLen]
		it := txn.NewIterator(opts)
		defer it.Close()
		var prev []byte
		for _, p := range prefixes {
			for it.Seek(p); it.ValidForPrefix(p); it.Next() {
				item := it.Item()
				key := item.Key()
				if len(key) != len(p)+tail {
					break // a longer raw value (0x00 0xFF after p): its entries sort after p's
				}
				if bytes.Equal(key, prev) {
					continue // an older version of the key just decided
				}
				prev = append(prev[:0], key...)
				if item.IsDeletedOrExpired() || ov.overrides(key) {
					continue
				}
				fn(key)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("graph: entity index scan: %w", err)
	}
	for k := range ov.sets {
		fn([]byte(k))
	}
	return nil
}

// entityIndexRangeLocked visits once each live entry under within whose first
// len(lo) bytes lie in [lo, hi] (hi nil: every entry under within), resolved
// with the write buffer per key. Caller holds idxMu (R or W).
func (bs *Store) entityIndexRangeLocked(within, lo, hi []byte, fn func(key []byte)) error {
	in := func(k []byte) bool {
		if !bytes.HasPrefix(k, within) || len(k) < len(lo) || bytes.Compare(k[:len(lo)], lo) < 0 {
			return false
		}
		return hi == nil || bytes.Compare(k[:len(hi)], hi) <= 0
	}
	ov := pendingKeyOverlay[struct{}]{}
	bs.rangePending(func(k string, op writeOp) {
		if in([]byte(k)) {
			ov.note(k, op, struct{}{})
		}
	})
	err := bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.AllVersions = true
		opts.Prefix = within
		it := txn.NewIterator(opts)
		defer it.Close()
		var prev []byte
		for it.Seek(lo); it.ValidForPrefix(within); it.Next() {
			item := it.Item()
			key := item.Key()
			if hi != nil && bytes.Compare(key[:min(len(hi), len(key))], hi) > 0 {
				break
			}
			if bytes.Equal(key, prev) {
				continue
			}
			prev = append(prev[:0], key...)
			if item.IsDeletedOrExpired() || ov.overrides(key) {
				continue
			}
			fn(key)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("graph: entity index range scan: %w", err)
	}
	for k := range ov.sets {
		fn([]byte(k))
	}
	return nil
}

// txMemberFold folds member entries: each entity once, with the lowest
// transaction time of its entries (indexpkg.MergeFirstTx).
type txMemberFold map[snowflake.ID]types.Instant

func (f txMemberFold) add(kind byte) func(key []byte) {
	return func(key []byte) {
		_, id, tx, ok := storepkg.EntityIndexEntryOf(key, kind)
		if !ok {
			return
		}
		if prev, seen := f[id]; seen {
			f[id] = indexpkg.MergeFirstTx(prev, types.Instant(tx))
		} else {
			f[id] = types.Instant(tx)
		}
	}
}

// foldedTxMembers returns f's members in ascending ID order.
func foldedTxMembers[ID cmp.Ordered](f txMemberFold, conv func(snowflake.ID) ID) []indexpkg.TxMember[ID] {
	out := make([]indexpkg.TxMember[ID], 0, len(f))
	for id, tx := range f {
		out = append(out, indexpkg.TxMember[ID]{ID: conv(id), FirstTx: tx})
	}
	slices.SortFunc(out, func(a, b indexpkg.TxMember[ID]) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// memberQuery selects the values of a membership lookup: valueKeys (nil:
// every value), or with numeric every value whose magnitude lies in [lo, hi]
// and NaN (index.PropertyTxMembers.MembersInRange).
type memberQuery struct {
	valueKeys []string
	numeric   bool
	lo, hi    float64
}

// diskTxMembersLocked answers a membership lookup of the definition (kind,
// scope, propertyKey) from its member entries. Caller holds idxMu (R or W).
func (bs *Store) diskTxMembersLocked(kind byte, scope uint16, propertyKey string, q memberQuery) (txMemberFold, error) {
	f := txMemberFold{}
	tok, ok := bs.propKeyTokenFor(propertyKey)
	if !ok {
		return f, nil
	}
	head := storepkg.AppendEntityIndexHead(nil, kind, scope, tok)
	switch {
	case q.numeric:
		if q.lo <= q.hi { // else (an empty range or a NaN bound) the NaN values only
			lo, hi := storepkg.EntityIndexNumericBounds(head, q.lo, q.hi)
			if err := bs.entityIndexRangeLocked(head, lo, hi, f.add(kind)); err != nil {
				return nil, err
			}
		}
		nan := storepkg.EntityIndexNaNBound(head)
		if err := bs.entityIndexRangeLocked(head, nan, nan, f.add(kind)); err != nil {
			return nil, err
		}
	case q.valueKeys == nil:
		if err := bs.entityIndexRangeLocked(head, head, nil, f.add(kind)); err != nil {
			return nil, err
		}
	default:
		var prefixes [][]byte
		for _, vk := range q.valueKeys {
			if p, ok := storepkg.AppendEntityIndexValue(slices.Clone(head), vk); ok {
				prefixes = append(prefixes, p)
			}
		}
		if err := bs.entityIndexWalkLocked(kind, prefixes, f.add(kind)); err != nil {
			return nil, err
		}
	}
	return f, nil
}

// purgedNodeMemberOpsLocked returns deletes of the member entries of nid's
// rows, current and history: a retention purge removes them all, and an
// entry left would name a value of a row that no longer exists. Caller holds
// idxMu.Lock.
func (bs *Store) purgedNodeMemberOpsLocked(nid types.NodeID) ([]writeOp, error) {
	if !bs.nodeMembersOnDisk {
		return nil, nil
	}
	id := nid.SnowflakeID()
	rows, err := bs.getNodeHistoryByPrefix(storepkg.HistNodePrefix(id))
	if err != nil {
		return nil, fmt.Errorf("graph: purge member entries of node %d: %w", id, err)
	}
	if n, err := bs.getNodeLocked(nid); err == nil {
		rows = append(rows, n)
	}
	var ops []writeOp
	for _, n := range rows {
		for _, op := range bs.nodeMemberOpsLocked(n, id) {
			ops = append(ops, writeOp{opType: writeOpDelete, key: op.key})
		}
	}
	return ops, nil
}

// purgedRelMemberOpsLocked is purgedNodeMemberOpsLocked for a relationship
// of a type with a definition on disk (its current entries go with its
// current row, maintainRelPropertyIndexesPurge). Caller holds idxMu.Lock.
func (bs *Store) purgedRelMemberOpsLocked(rid types.RelID, relType uint16) ([]writeOp, error) {
	if !slices.ContainsFunc(bs.relDiskDefs, func(d relDiskDef) bool { return d.key.RelTypeToken == relType }) {
		return nil, nil
	}
	id := rid.SnowflakeID()
	rows, err := bs.getRelHistoryByPrefix(storepkg.HistRelPrefix(id))
	if err != nil {
		return nil, fmt.Errorf("graph: purge member entries of relationship %d: %w", id, err)
	}
	if r, err := bs.getRelLocked(rid); err == nil {
		rows = append(rows, r)
	}
	var ops []writeOp
	for _, r := range rows {
		for _, op := range bs.relIndexDiskOpsLocked(r, id, writeOpSet, false) {
			ops = append(ops, writeOp{opType: writeOpDelete, key: op.key})
		}
	}
	return ops, nil
}

// relDiskMembers answers a membership lookup of the relationship definition
// on key from its member entries when it is kept on disk: handled=false when
// it is not (its sidecar answers), ErrIndexNotFound while it is created.
func (bs *Store) relDiskMembers(key indexpkg.RelPropertyIndexKey, q memberQuery) (members []indexpkg.TxMember[types.RelID], handled bool, err error) {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	idx := bs.relPropertyIndexes[key]
	if !idx.OnDisk() {
		return nil, false, nil
	}
	if idx.Mutated != nil {
		return nil, true, ErrIndexNotFound
	}
	f, err := bs.diskTxMembersLocked(storepkg.EntityIndexRelMember, key.RelTypeToken, key.PropertyKey, q)
	if err != nil {
		return nil, true, err
	}
	return foldedTxMembers(f, func(id snowflake.ID) types.RelID { return types.RelID(id) }), true, nil
}

// nodeDiskMembers is relDiskMembers for a node definition whose member
// entries are on disk (nodeMemberDisk).
func (bs *Store) nodeDiskMembers(key indexpkg.PropertyIndexKey, q memberQuery) (members []indexpkg.TxMember[types.NodeID], handled bool, err error) {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	if _, ok := bs.nodeMemberDisk[key]; !ok {
		return nil, false, nil
	}
	if bs.propertyIndexes[key].Mutated != nil {
		return nil, true, ErrIndexNotFound
	}
	f, err := bs.diskTxMembersLocked(storepkg.EntityIndexNodeMember, key.LabelToken, key.PropertyKey, q)
	if err != nil {
		return nil, true, err
	}
	return foldedTxMembers(f, func(id snowflake.ID) types.NodeID { return types.NodeID(id) }), true, nil
}

// emitTxMembers hands members to fn outside every store lock (fn re-enters
// the store); false stops.
func emitTxMembers[ID cmp.Ordered](members []indexpkg.TxMember[ID], err error, fn func(ID, types.Instant) bool) error {
	if err != nil {
		return err
	}
	for _, m := range members {
		if !fn(m.ID, m.FirstTx) {
			return nil
		}
	}
	return nil
}

// pageTxMembers keeps the members above after, at most limit (none for
// limit <= 0), of members in ascending ID order: the paged lookup's answer.
func pageTxMembers[ID cmp.Ordered](members []indexpkg.TxMember[ID], after ID, limit int) []indexpkg.TxMember[ID] {
	i, _ := slices.BinarySearchFunc(members, after, func(m indexpkg.TxMember[ID], id ID) int { return cmp.Compare(m.ID, id) })
	if i < len(members) && members[i].ID == after {
		i++
	}
	members = members[i:]
	return members[:min(max(limit, 0), len(members))]
}

// relCurrentCandidatesLocked returns the relationships (unsorted, possibly
// repeated) whose current entries under the definition (relTypeToken,
// propertyKey) carry one of valueKeys. Caller holds idxMu (R or W).
func (bs *Store) relCurrentCandidatesLocked(relTypeToken uint16, propertyKey string, valueKeys []string) ([]types.RelID, error) {
	var prefixes [][]byte
	for _, vk := range valueKeys {
		if p, ok := bs.entityIndexValuePrefix(storepkg.EntityIndexRelCurrent, relTypeToken, propertyKey, vk); ok {
			prefixes = append(prefixes, p)
		}
	}
	var rids []types.RelID
	err := bs.entityIndexWalkLocked(storepkg.EntityIndexRelCurrent, prefixes, func(key []byte) {
		if _, id, _, ok := storepkg.EntityIndexEntryOf(key, storepkg.EntityIndexRelCurrent); ok {
			rids = append(rids, types.RelID(id))
		}
	})
	return rids, err
}

// entityBuildTarget is one definition an entity-index build fills: a
// relationship definition (rel, scope its type), a node definition's member
// entries (scope its label), or a composite node definition's current entries
// (composite its keys, head its entries' head; COMPDISK).
type entityBuildTarget struct {
	rel         bool
	scope       uint16
	propertyKey string
	composite   []string
	head        []byte
}

// scanEntityIndexEntries reads every stored row of the targets' kind, current
// and history (the write buffer first, then a badger view: scanPropTxRows),
// and hands fn the set op of each entry the rows give the targets: a
// relationship definition's current entries from current rows, a composite's
// from current rows, and every other target's member entries from every row
// (no history row is read when every target is a composite). The targets are
// all relationship or all node definitions. It holds no store lock.
func (bs *Store) scanEntityIndexEntries(targets []entityBuildTarget, fn func(op writeOp) error) error {
	if len(targets) == 0 {
		return nil
	}
	byScope := make(map[uint16][]entityBuildTarget, len(targets))
	for _, t := range targets {
		byScope[t.scope] = append(byScope[t.scope], t)
	}
	emit := func(t entityBuildTarget, valueKey string, id snowflake.ID, txFrom types.Instant, current bool) error {
		kind := storepkg.EntityIndexNodeMember
		if t.rel {
			kind = storepkg.EntityIndexRelMember
			if current {
				for _, op := range bs.entityIndexOp(nil, storepkg.EntityIndexRelCurrent, t.scope, t.propertyKey, valueKey, id, 0, writeOpSet) {
					if err := fn(op); err != nil {
						return err
					}
				}
			}
		}
		for _, op := range bs.entityIndexOp(nil, kind, t.scope, t.propertyKey, valueKey, id, txFrom, writeOpSet) {
			if err := fn(op); err != nil {
				return err
			}
		}
		return nil
	}
	if targets[0].rel {
		want := func(relType int) bool { return relType > 0 && relType <= 0xFFFF && len(byScope[uint16(relType)]) > 0 } // #nosec G115 -- range checked
		return bs.scanPropTxRows(storepkg.KeyRel, storepkg.KeyHistRel, func(id snowflake.ID, hist bool, version uint64, raw []byte, local localAnchorFunc) error {
			r, err := bs.decodeScannedRel(want, id, hist, version, raw, local)
			if r == nil || err != nil {
				return err
			}
			txFrom, _, _ := r.TxStamps()
			for _, t := range byScope[r.TypeToken().Value()] {
				if valueKey, _ := r.IndexablePropertyValueKey(t.propertyKey); valueKey != "" {
					if err := emit(t, valueKey, id, txFrom, !hist); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	want := func(w *storepkg.NodeWire) bool {
		has := func(label int) bool { return label > 0 && label <= 0xFFFF && len(byScope[uint16(label)]) > 0 } // #nosec G115 -- range checked
		return has(w.PrimaryLabel) || slices.ContainsFunc(w.ExtraLabels, has)
	}
	histKind := byte(0) // composites only: current rows give every entry, so no history row is read
	for _, t := range targets {
		if t.composite == nil {
			histKind = storepkg.KeyHistNode
		}
	}
	return bs.scanPropTxRows(storepkg.KeyNode, histKind, func(id snowflake.ID, hist bool, version uint64, raw []byte, local localAnchorFunc) error {
		n, err := bs.decodeScannedNode(want, id, hist, version, raw, local)
		if n == nil || err != nil {
			return err
		}
		txFrom, _, _ := n.TxStamps()
		for i := 0; i < n.LabelTokenCount(); i++ {
			for _, t := range byScope[n.LabelTokenRawAt(i)] {
				if t.composite != nil {
					if vk, ok := indexpkg.NodeCompositeValueKey(t.composite, n); ok && !hist {
						if err := fn(compositeDiskEntry(t.head, vk, id, writeOpSet)); err != nil {
							return err
						}
					}
				} else if valueKey, _ := n.IndexablePropertyValueKey(t.propertyKey); valueKey != "" {
					if err := emit(t, valueKey, id, txFrom, !hist); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}

// entityIndexKeysUnderLocked returns the keys under each head, buffered sets
// and persisted, for deletion (a drop, a rebuild's stale entries). Caller
// holds idxMu.
func (bs *Store) entityIndexKeysUnderLocked(heads ...[]byte) ([][]byte, error) {
	var keys [][]byte
	for _, head := range heads {
		if err := bs.entityIndexRangeLocked(head, head, nil, func(key []byte) { keys = append(keys, slices.Clone(key)) }); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

// entityIndexHeads returns the heads of a definition's entries: a
// relationship definition's current and member heads, a node definition's
// member head; nil without a property-key token.
func (bs *Store) entityIndexHeads(rel bool, scope uint16, propertyKey string) [][]byte {
	tok, ok := bs.propKeyTokenFor(propertyKey)
	if !ok {
		return nil
	}
	if rel {
		return [][]byte{
			storepkg.AppendEntityIndexHead(nil, storepkg.EntityIndexRelCurrent, scope, tok),
			storepkg.AppendEntityIndexHead(nil, storepkg.EntityIndexRelMember, scope, tok),
		}
	}
	return [][]byte{storepkg.AppendEntityIndexHead(nil, storepkg.EntityIndexNodeMember, scope, tok)}
}

// entityIndexDropOpsLocked returns the deletes of a definition's 0x0C entries
// and its built marker. Caller holds idxMu.Lock.
func (bs *Store) entityIndexDropOpsLocked(rel bool, scope uint16, propertyKey string) ([]writeOp, error) {
	kind := storepkg.EntityIndexNodeMember
	if rel {
		kind = storepkg.EntityIndexRelMember
	}
	ops := []writeOp{{opType: writeOpDelete, key: storepkg.EntityIndexBuiltKey(kind, scope, propertyKey)}}
	keys, err := bs.entityIndexKeysUnderLocked(bs.entityIndexHeads(rel, scope, propertyKey)...)
	if err != nil {
		return nil, fmt.Errorf("graph: drop property index entries on disk: %w", err)
	}
	for _, k := range keys {
		ops = append(ops, writeOp{opType: writeOpDelete, key: k})
	}
	return ops, nil
}

// createRelPropertyIndexOnDiskLocked is CreateRelPropertyIndex for a type
// kept on disk, entered under idxMu.Lock with key absent (released here). It
// installs the definition so every door writes its entries from now on
// (Mutated records the current rows written), deletes the entries an earlier
// definition left, scans every relationship row without a lock, and merges
// under the lock: a current entry only for a relationship alive and not
// written meanwhile, every member entry (a union, sound). A scan that
// straddled a purge, an erasure or a drop (propTxGen) is redone.
func (bs *Store) createRelPropertyIndexOnDiskLocked(key indexpkg.RelPropertyIndexKey) error {
	liveIdx := indexpkg.NewPropertyIndexOnDisk()
	liveIdx.Mutated = make(map[snowflake.ID]struct{})
	bs.relPropertyIndexes[key] = liveIdx
	bs.refreshRelDiskDefsLocked()
	stale, err := bs.entityIndexDropOpsLocked(true, key.RelTypeToken, key.PropertyKey)
	if err != nil {
		delete(bs.relPropertyIndexes, key)
		bs.refreshRelDiskDefsLocked()
		bs.idxMu.Unlock()
		return err
	}
	bs.appendOps(stale...)
	gen := bs.propTxGen
	bs.idxMu.Unlock()

	abort := func(err error) error {
		bs.idxMu.Lock()
		deleteRelPropertyIndexIfCurrent(bs.relPropertyIndexes, key, liveIdx)
		bs.refreshRelDiskDefsLocked()
		bs.idxMu.Unlock()
		return err
	}
	target := []entityBuildTarget{{rel: true, scope: key.RelTypeToken, propertyKey: key.PropertyKey}}
	for attempt := 0; attempt < propTxBuildAttempts; attempt++ {
		var ops []writeOp
		if err := bs.scanEntityIndexEntries(target, func(op writeOp) error { ops = append(ops, op); return nil }); err != nil {
			return abort(fmt.Errorf("graph: create relationship property index: %w", err))
		}
		bs.idxMu.Lock()
		if err := requireRelPropertyIndexCurrentForCreate(bs.relPropertyIndexes, key, liveIdx); err != nil {
			bs.refreshRelDiskDefsLocked()
			bs.idxMu.Unlock()
			return err
		}
		if bs.propTxGen != gen {
			gen = bs.propTxGen
			bs.idxMu.Unlock()
			continue
		}
		kept := ops[:0]
		for _, op := range ops {
			if op.key[1] == storepkg.EntityIndexRelCurrent {
				_, id, _, _ := storepkg.EntityIndexEntryOf(op.key, storepkg.EntityIndexRelCurrent)
				if _, mutated := liveIdx.Mutated[id]; mutated {
					continue // written or deleted during the scan: its door wrote the entry
				}
			}
			kept = append(kept, op)
		}
		liveIdx.Mutated = nil
		bs.persistRelPropertyIndexDefs()
		kept = append(kept, writeOp{opType: writeOpSet, key: storepkg.EntityIndexBuiltKey(storepkg.EntityIndexRelMember, key.RelTypeToken, key.PropertyKey), value: []byte{1}})
		bs.appendOpsRaising(storepkg.EntityIndexStoreFormatVersion, kept...)
		bs.idxMu.Unlock()
		return bs.flushIfNeeded()
	}
	return abort(fmt.Errorf("graph: create relationship property index: raced %d purges or drops: %w", propTxBuildAttempts, ErrIndexNotFound))
}

// entityIndexOpen is an open's plan for the 0x0C entries (planEntityIndexOpen):
// which definitions answer from them and what the open writes.
type entityIndexOpen struct {
	marked   map[string]struct{} // the built markers present
	keep     map[string]struct{} // the markers this open keeps
	canBuild bool
	targets  []entityBuildTarget // the definitions this open builds
	stale    [][]byte            // their entries before the build
	mark     []writeOp           // built markers to set (with the format marker)
	level    uint16              // the format marker the marks raise to
}

// planEntityIndexOpen reads the 0x0C built markers at open. A definition
// whose entries are trusted (its marker present, kept by every writable open
// since its build) answers from them; an untrusted one a writable open with a
// property-key registry builds (commitEntityIndexOpen), and otherwise it is
// answered as without 0x0C for this open.
func (bs *Store) planEntityIndexOpen(txn *badgerv4.Txn) *entityIndexOpen {
	p := &entityIndexOpen{marked: make(map[string]struct{}), keep: make(map[string]struct{}),
		canBuild: !bs.readOnly && bs.propKeyReg.Load() != nil}
	it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: storepkg.EntityIndexBuiltPrefix})
	for it.Rewind(); it.Valid(); it.Next() {
		p.marked[string(it.Item().Key())] = struct{}{}
	}
	it.Close()
	return p
}

// place decides one definition: true when it answers from 0x0C this open.
func (p *entityIndexOpen) place(bs *Store, txn *badgerv4.Txn, rel bool, scope uint16, propertyKey string) bool {
	kind := storepkg.EntityIndexNodeMember
	if rel {
		kind = storepkg.EntityIndexRelMember
	}
	k := storepkg.EntityIndexBuiltKey(kind, scope, propertyKey)
	if _, ok := p.marked[string(k)]; ok {
		p.keep[string(k)] = struct{}{}
		return true
	}
	if !p.canBuild {
		return false
	}
	for _, head := range bs.entityIndexHeads(rel, scope, propertyKey) {
		it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: head})
		for it.Rewind(); it.Valid(); it.Next() {
			p.stale = append(p.stale, it.Item().KeyCopy(nil))
		}
		it.Close()
	}
	p.targets = append(p.targets, entityBuildTarget{rel: rel, scope: scope, propertyKey: propertyKey})
	p.mark = append(p.mark, writeOp{opType: writeOpSet, key: k, value: []byte{1}})
	p.level = max(p.level, storepkg.EntityIndexStoreFormatVersion)
	return true
}

// placeComposite is place for a composite node definition of a label kept on
// disk (COMPDISK): true when its entries answer from 0x0C this open, never
// without a property-key registry (no head).
func (p *entityIndexOpen) placeComposite(bs *Store, txn *badgerv4.Txn, key indexpkg.CompositeIndexKey, keys []string) bool {
	head, ok := bs.compositeDiskHead(key.LabelToken, keys)
	if !ok {
		return false
	}
	k := compositeDiskBuiltKey(key)
	if _, ok := p.marked[string(k)]; ok {
		p.keep[string(k)] = struct{}{}
		return true
	}
	if !p.canBuild {
		return false
	}
	it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: head})
	for it.Rewind(); it.Valid(); it.Next() {
		p.stale = append(p.stale, it.Item().KeyCopy(nil))
	}
	it.Close()
	p.targets = append(p.targets, entityBuildTarget{scope: key.LabelToken, composite: keys, head: head})
	p.mark = append(p.mark, writeOp{opType: writeOpSet, key: k, value: []byte{1}})
	p.level = storepkg.CompositeDiskStoreFormatVersion
	return true
}

// commitEntityIndexOpen builds the planned definitions' entries from every
// row and writes them, after deleting the entries they had, with their
// markers last, in one WriteBatch (a crash keeps a prefix without the
// markers: the next open builds again, docs/badger.md rule 1); a writable open
// also deletes the markers of definitions it did not keep on disk.
func (bs *Store) commitEntityIndexOpen(p *entityIndexOpen) error {
	if p == nil || bs.readOnly {
		return nil
	}
	var unmark [][]byte
	for k := range p.marked {
		if _, ok := p.keep[k]; !ok {
			unmark = append(unmark, []byte(k))
		}
	}
	if len(p.targets) == 0 && len(unmark) == 0 {
		return nil
	}
	wb := bs.db.NewWriteBatch()
	defer wb.Cancel()
	for _, key := range append(p.stale, unmark...) {
		if err := wb.Delete(key); err != nil {
			return fmt.Errorf("graph: entity-index build at open: %w", err)
		}
	}
	set := func(op writeOp) error { return wb.SetEntry(badgerv4.NewEntry(op.key, op.value)) }
	var rels, nodes []entityBuildTarget
	for _, t := range p.targets {
		if t.rel {
			rels = append(rels, t)
		} else {
			nodes = append(nodes, t)
		}
	}
	for _, ts := range [][]entityBuildTarget{rels, nodes} {
		if err := bs.scanEntityIndexEntries(ts, set); err != nil {
			return fmt.Errorf("graph: entity-index build at open: %w", err)
		}
	}
	if len(p.mark) > 0 {
		v := bs.raisedFormatMarker(p.level)
		p.mark = append(p.mark, writeOp{opType: writeOpSet, key: storepkg.WireFormatVersionKey, value: formatMarkerValue(v)})
	}
	for _, op := range p.mark {
		if err := set(op); err != nil {
			return fmt.Errorf("graph: entity-index build at open: %w", err)
		}
	}
	if bs.dbClosed.Load() {
		return fmt.Errorf("graph: entity-index build at open: %w", badgerv4.ErrDBClosed)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("graph: entity-index build at open: %w", err)
	}
	return nil
}

// nodeReplacedMemberOpsLocked returns the member entries of prev, the row a
// with-history door demotes, that old, the current row it replaces (whose
// entries exist), did not give: none when prev is old's copy, as the doors
// write it. Caller holds idxMu.Lock.
func (bs *Store) nodeReplacedMemberOpsLocked(prev, old *types.Node, id snowflake.ID) []writeOp {
	return newEntityOps(bs.nodeMemberOpsLocked(prev, id), bs.nodeMemberOpsLocked(old, id))
}

// relReplacedMemberOpsLocked is nodeReplacedMemberOpsLocked for a
// relationship. Caller holds idxMu.Lock.
func (bs *Store) relReplacedMemberOpsLocked(prev, old *types.Relationship, id snowflake.ID) []writeOp {
	return newEntityOps(bs.relIndexDiskOpsLocked(prev, id, writeOpSet, false), bs.relIndexDiskOpsLocked(old, id, writeOpSet, false))
}

// newEntityOps returns the ops of ops whose key have does not hold.
func newEntityOps(ops, have []writeOp) []writeOp {
	if len(ops) == 0 {
		return nil
	}
	held := make(map[string]struct{}, len(have))
	for _, op := range have {
		held[string(op.key)] = struct{}{}
	}
	var out []writeOp
	for _, op := range ops {
		if _, ok := held[string(op.key)]; !ok {
			out = append(out, op)
		}
	}
	return out
}
