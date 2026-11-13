package badger

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
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

// relCurrentOp appends the op of one relationship current entry
// (EntityIndexRelCurrent) to ops. The member entries go through entityMemberOp.
func (bs *Store) relCurrentOp(ops []writeOp, scope uint16, propertyKey, valueKey string, id snowflake.ID, opType writeOpType) []writeOp {
	prefix, ok := bs.entityIndexValuePrefix(storepkg.EntityIndexRelCurrent, scope, propertyKey, valueKey)
	if !ok {
		return ops
	}
	key := storepkg.AppendEntityIndexEntry(prefix, storepkg.EntityIndexRelCurrent, id, 0, 0)
	if opType == writeOpDelete {
		return append(ops, writeOp{opType: writeOpDelete, key: key})
	}
	return append(ops, writeOp{opType: writeOpSet, key: key, value: []byte{}})
}

// relIndexDiskOpsLocked returns the entries of r's on-disk definitions a door
// writes: for a current row (set) its current and member entries, for a
// history row (current false) its member entries, for a removed current row
// (delete) its current-entry deletes. A member entry carries the row's span as
// it stands (relMemberRow); a door that moves the row (an update, a delete)
// writes it again with what the move gives (relReplacedMemberOpsLocked, the
// tombstone's). Each definition being created records the id of a current row
// (Mutated). Caller holds idxMu.Lock (a history row: idxMu, R or W).
func (bs *Store) relIndexDiskOpsLocked(r *types.Relationship, id snowflake.ID, opType writeOpType, current bool) []writeOp {
	if len(bs.relDiskDefs) == 0 || r == nil {
		return nil
	}
	tok := r.TypeToken().Value()
	var ops []writeOp
	var row memberRow
	built := false
	for _, def := range bs.relDiskDefs {
		if def.key.RelTypeToken != tok {
			continue
		}
		valueKey, _ := r.IndexablePropertyValueKey(def.key.PropertyKey)
		if valueKey != "" {
			if current {
				ops = bs.relCurrentOp(ops, tok, def.key.PropertyKey, valueKey, id, opType)
			}
			if opType == writeOpSet {
				if !built {
					row, built = bs.relMemberRow(r, nil), true
				}
				ops = bs.entityMemberOp(ops, storepkg.EntityIndexRelSpan, tok, def.key.PropertyKey, valueKey, id, row)
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
// definitions whose members are on disk, for a row n (current or history),
// each with the row's span as it stands (nodeMemberRow). Caller holds
// idxMu.Lock.
func (bs *Store) nodeMemberOpsLocked(n *types.Node, id snowflake.ID) []writeOp {
	return bs.nodeMemberOpsNextLocked(n, id, nil)
}

// nodeMemberOpsNextLocked is nodeMemberOpsLocked for a row next replaced: a
// row an update demoted ends at next's start (memberRow.lateTo,
// RowInfo.EndedBy); next nil: the row as it stands.
func (bs *Store) nodeMemberOpsNextLocked(n *types.Node, id snowflake.ID, next *types.Node) []writeOp {
	if !bs.nodeMembersOnDisk || n == nil {
		return nil
	}
	var ops []writeOp
	var row memberRow
	built := false
	for _, def := range bs.propDiskDefs {
		if !def.members || !n.HasLabelTokenRaw(def.key.LabelToken) {
			continue
		}
		forEachDiskValueKey(n, def.key.PropertyKey, def.elements, func(valueKey string) {
			if !built {
				row, built = bs.nodeMemberRow(n, next), true
			}
			ops = bs.entityMemberOp(ops, storepkg.EntityIndexNodeSpan, def.key.LabelToken, def.key.PropertyKey, valueKey, id, row)
		})
	}
	return ops
}

// nodeMemberOpsKeysLocked is nodeMemberOpsLocked with n's value keys already
// built (nodeDiskKeysLocked, PAGECOST).
func (bs *Store) nodeMemberOpsKeysLocked(n *types.Node, id snowflake.ID, keys nodeDiskKeys) []writeOp {
	if !bs.nodeMembersOnDisk || n == nil {
		return nil
	}
	var ops []writeOp
	var row memberRow
	built := false
	for _, k := range keys {
		def := bs.propDiskDefs[k.def]
		if def.members {
			if !built {
				row, built = bs.nodeMemberRow(n, nil), true
			}
			ops = bs.entityMemberOp(ops, storepkg.EntityIndexNodeSpan, def.key.LabelToken, def.key.PropertyKey, k.valueKey, id, row)
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
// entry stays as it is, PROPREWRITE). The new row's member entry is never
// dropped: it is its own key (its version) and carries its own span
// (RELDISKWIN). from splits the old row's ops (current-entry deletes) from the
// new row's. Not while a definition is created (its backfill skips the rows a
// door marked).
func (bs *Store) dropUnchangedEntityOps(ops []writeOp, from int) []writeOp {
	if from == 0 || from == len(ops) || bs.entityIndexBuildingLocked() {
		return ops
	}
	removed := make(map[string]struct{}, from)
	for _, op := range ops[:from] {
		removed[string(op.key)] = struct{}{}
	}
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
		if op.key[1] == storepkg.EntityIndexRelCurrent {
			if _, ok := unchanged[string(op.key[:len(op.key)-8])]; ok {
				continue
			}
		}
		kept = append(kept, op)
	}
	return kept
}

// dropSameRelMemberOps drops from ops[from:] (the new row r's) the member
// entries old, the row r replaces in place, already wrote with the same key and
// the same value: r has old's transaction time, version and span, so a value
// both carry has the entry it had (PROPREWRITE: a rewrite adds a badger version
// every lookup of the value visits until compaction drops it). An update that
// gives the row another stamp or span, or a with-history door (a new version),
// writes the entry. Not while a definition is created (its backfill skips the
// rows a door marked).
func (bs *Store) dropSameRelMemberOps(ops []writeOp, from int, old, r *types.Relationship, id snowflake.ID) []writeOp {
	if from == len(ops) || len(bs.relDiskDefs) == 0 || old == nil || bs.entityIndexBuildingLocked() ||
		bs.relMemberRow(old, nil) != bs.relMemberRow(r, nil) {
		return ops
	}
	return dropHeldOps(ops, from, bs.relIndexDiskOpsLocked(old, id, writeOpSet, false))
}

// dropHeldOps returns ops with the member entry sets from index from on that
// have holds a set of the same key removed (and so of the same value: the
// caller's guarantee).
func dropHeldOps(ops []writeOp, from int, have []writeOp) []writeOp {
	if len(have) == 0 {
		return ops
	}
	held := make(map[string]struct{}, len(have))
	for _, op := range have {
		held[string(op.key)] = struct{}{}
	}
	kept := ops[:from]
	for _, op := range ops[from:] {
		if op.opType == writeOpSet && op.key[0] == storepkg.KeyEntityIndex && (storepkg.EntityIndexIsSpan(op.key[1]) || storepkg.EntityIndexIsTime(op.key[1])) {
			if _, ok := held[string(op.key)]; ok {
				continue
			}
		}
		kept = append(kept, op)
	}
	return kept
}

// entityIndexWalkLocked visits once each live entry of kind under the value
// prefixes (exact: a longer raw value is not visited): the persisted keys'
// newest version and the write buffer's, resolved per key (lesson 57). The
// walk takes every version (AllVersions) and stops at each prefix's end, so
// deleted entries outside the prefixes are never walked (docs/badger.md rule
// 12). Caller holds idxMu (R or W).
func (bs *Store) entityIndexWalkLocked(kind byte, prefixes [][]byte, fn func(key []byte)) error {
	return bs.entityIndexWalkLockedWork(kind, prefixes, fn, nil)
}

func (bs *Store) entityIndexWalkLockedWork(kind byte, prefixes [][]byte, fn func(key []byte), work *storecontract.PropertyMemberWork) error {
	return bs.entityIndexWalkCore(kind, prefixes, func(key, _ []byte) { fn(key) }, false, nil, work)
}

func (bs *Store) entityIndexWalkCore(kind byte, prefixes [][]byte, fn func(key, val []byte), values bool, buffered func(key, val []byte), work *storecontract.PropertyMemberWork) error {
	if len(prefixes) == 0 {
		return nil
	}
	slices.SortFunc(prefixes, bytes.Compare)
	prefixes = slices.CompactFunc(prefixes, bytes.Equal)
	tail := storepkg.EntityIndexTailLen(kind)
	ov := pendingKeyOverlay[[]byte]{}
	bs.rangePendingPrefixes(prefixes, func(k string, op writeOp) {
		if work != nil {
			work.OverlayOpsInspected++
		}
		var val []byte
		if values && op.opType == writeOpSet {
			val = slices.Clone(op.value)
		}
		noteMemberOverlay(&ov, k, op, val, work)
	})
	if buffered != nil {
		for k, val := range ov.sets {
			if work != nil {
				work.OverlaySetsEmitted++
			}
			buffered([]byte(k), val)
		}
	}
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
				if work != nil {
					work.IteratorSteps++
				}
				if bs.entityIndexWalkTestHook != nil {
					bs.entityIndexWalkTestHook()
				}
				item := it.Item()
				key := item.Key()
				if len(key) != len(p)+tail {
					break // a longer raw value (0x00 0xFF after p): its entries sort after p's
				}
				if bytes.Equal(key, prev) {
					continue // an older version of the key just decided
				}
				noteIndexEntryVisited()
				prev = append(prev[:0], key...)
				if item.IsDeletedOrExpired() || ov.overrides(key) {
					continue
				}
				if work != nil {
					work.DiskEntriesEmitted++
				}
				if !values {
					fn(key, nil)
					continue
				}
				if err := item.Value(func(val []byte) error { fn(key, val); return nil }); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("graph: entity index scan: %w", err)
	}
	if buffered != nil {
		return nil // the caller has the buffer's sets
	}
	for k, val := range ov.sets {
		if work != nil {
			work.OverlaySetsEmitted++
		}
		fn([]byte(k), val)
	}
	return nil
}

// entityIndexRangeLocked visits once each live entry under within whose first
// len(lo) bytes lie in [lo, hi] (hi nil: every entry under within), resolved
// with the write buffer per key. Caller holds idxMu (R or W).
func (bs *Store) entityIndexRangeLocked(within, lo, hi []byte, fn func(key []byte)) error {
	return bs.entityIndexRangeLockedWork(within, lo, hi, fn, nil)
}

func (bs *Store) entityIndexRangeLockedWork(within, lo, hi []byte, fn func(key []byte), work *storecontract.PropertyMemberWork) error {
	in := func(k []byte) bool {
		if !bytes.HasPrefix(k, within) || len(k) < len(lo) || bytes.Compare(k[:len(lo)], lo) < 0 {
			return false
		}
		return hi == nil || bytes.Compare(k[:len(hi)], hi) <= 0
	}
	ov := pendingKeyOverlay[struct{}]{}
	bs.rangePending(func(k string, op writeOp) {
		if work != nil {
			work.OverlayOpsInspected++
		}
		if in([]byte(k)) {
			noteMemberOverlay(&ov, k, op, struct{}{}, work)
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
			if work != nil {
				work.IteratorSteps++
			}
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
			if work != nil {
				work.DiskEntriesEmitted++
			}
			fn(key)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("graph: entity index range scan: %w", err)
	}
	for k := range ov.sets {
		if work != nil {
			work.OverlaySetsEmitted++
		}
		fn([]byte(k))
	}
	return nil
}

			noteIndexEntryVisited()
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
	work      *storecontract.PropertyMemberWork
	valueKeys []string
	numeric   bool
	lo, hi    float64
	// window: with one value key, the members whose rows may meet the window
	// (a definition in the span layout narrows them, diskWindowMembersLocked;
	// one in the layout before it answers every member of the value).
	window *memberWindow
}

// diskTxMembersLocked answers a membership lookup of the definition (kind,
// scope, propertyKey) from its member entries. Caller holds idxMu (R or W).
func (bs *Store) diskTxMembersLocked(kind byte, scope uint16, propertyKey string, q memberQuery) (txMemberFold, error) {
	if q.window != nil && len(q.valueKeys) == 1 && storepkg.EntityIndexIsSpan(kind) {
		return bs.diskWindowMembersLocked(kind, scope, propertyKey, q.valueKeys[0], *q.window, q.work)
	}
	f := txMemberFold{}
	if q.work != nil {
		defer func() { q.work.FoldMembersPeak = uint64(len(f)) }()
	}
	tok, ok := bs.propKeyTokenFor(propertyKey)
	if !ok {
		return f, nil
	}
	head := storepkg.AppendEntityIndexHead(nil, kind, scope, tok)
	switch {
	case q.numeric:
		if q.lo <= q.hi { // else (an empty range or a NaN bound) the NaN values only
			lo, hi := storepkg.EntityIndexNumericBounds(head, q.lo, q.hi)
			if err := bs.entityIndexRangeLockedWork(head, lo, hi, f.add(kind), q.work); err != nil {
				return nil, err
			}
		}
		nan := storepkg.EntityIndexNaNBound(head)
		if err := bs.entityIndexRangeLockedWork(head, nan, nan, f.add(kind), q.work); err != nil {
			return nil, err
		}
	case q.valueKeys == nil:
		if err := bs.entityIndexRangeLockedWork(head, head, nil, f.add(kind), q.work); err != nil {
			return nil, err
		}
	default:
		var prefixes [][]byte
		for _, vk := range q.valueKeys {
			if p, ok := storepkg.AppendEntityIndexValue(slices.Clone(head), vk); ok {
				prefixes = append(prefixes, p)
			}
		}
		if err := bs.entityIndexWalkLockedWork(kind, prefixes, f.add(kind), q.work); err != nil {
			return nil, err
		}
	}
	return f, nil
}

// purgedNodeMemberOpsLocked returns deletes of the member entries of nid's
// rows, current and history: a retention purge removes them all, and an
// entry left would name a value of a row that no longer exists. A row's
// time-ordered entries (option B) are found by every end the row may have
// been written with (purgeLateTos): its own, the one a replacing row gave it,
// the one before a delete. Caller holds idxMu.Lock.
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
	axis := bs.validAxis.Get()
	infos := make([]indexpkg.RowInfo, len(rows))
	for i, n := range rows {
		infos[i] = indexpkg.NodeRowInfo(axis, n)
	}
	var keys [][]byte
	for i, n := range rows {
		row, lateTos := memberRowOf(infos[i], infos[i].Span), purgeLateTos(infos, i)
		for _, def := range bs.propDiskDefs {
			if !def.members || !n.HasLabelTokenRaw(def.key.LabelToken) {
				continue
			}
			forEachDiskValueKey(n, def.key.PropertyKey, def.elements, func(valueKey string) {
				keys = append(keys, bs.memberPurgeKeys(storepkg.EntityIndexNodeSpan, def.key.LabelToken, def.key.PropertyKey, valueKey, id, row, lateTos)...)
			})
		}
	}
	return purgeDeletes(keys), nil
}

// purgeDeletes returns the deletes of keys, each once.
func purgeDeletes(keys [][]byte) []writeOp {
	seen := make(map[string]struct{}, len(keys))
	ops := make([]writeOp, 0, len(keys))
	for _, k := range keys {
		if _, dup := seen[string(k)]; dup {
			continue
		}
		seen[string(k)] = struct{}{}
		ops = append(ops, writeOp{opType: writeOpDelete, key: k})
	}
	return ops
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
	axis := bs.validAxis.Get()
	infos := make([]indexpkg.RowInfo, len(rows))
	for i, r := range rows {
		infos[i] = indexpkg.RelRowInfo(axis, r)
	}
	var keys [][]byte
	for i, r := range rows {
		row, lateTos := memberRowOf(infos[i], infos[i].Span), purgeLateTos(infos, i)
		tok := r.TypeToken().Value()
		for _, def := range bs.relDiskDefs {
			if def.key.RelTypeToken != tok {
				continue
			}
			if valueKey, _ := r.IndexablePropertyValueKey(def.key.PropertyKey); valueKey != "" {
				keys = append(keys, bs.memberPurgeKeys(storepkg.EntityIndexRelSpan, tok, def.key.PropertyKey, valueKey, id, row, lateTos)...)
			}
		}
	}
	return purgeDeletes(keys), nil
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
	if q.work != nil {
		q.work.Supported = true
	}
	if idx.Mutated != nil {
		return nil, true, ErrIndexNotFound
	}
	f, err := bs.diskTxMembersLocked(bs.memberKind(true, key.RelTypeToken, key.PropertyKey), key.RelTypeToken, key.PropertyKey, q)
	if err != nil {
		return nil, true, err
	}
	members = foldedTxMembers(f, func(id snowflake.ID) types.RelID { return types.RelID(id) })
	if q.work != nil {
		q.work.MemberSliceLen = uint64(len(members))
		q.work.MemberSliceCap = uint64(cap(members))
	}
	return members, true, nil
}

// nodeDiskMembers is relDiskMembers for a node definition whose member
// entries are on disk (nodeMemberDisk).
func (bs *Store) nodeDiskMembers(key indexpkg.PropertyIndexKey, q memberQuery) (members []indexpkg.TxMember[types.NodeID], handled bool, err error) {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	if _, ok := bs.nodeMemberDisk[key]; !ok {
		return nil, false, nil
	}
	if q.work != nil {
		q.work.Supported = true
	}
	if bs.propertyIndexes[key].Mutated != nil {
		return nil, true, ErrIndexNotFound
	}
	f, err := bs.diskTxMembersLocked(bs.memberKind(false, key.LabelToken, key.PropertyKey), key.LabelToken, key.PropertyKey, q)
	if err != nil {
		return nil, true, err
	}
	members = foldedTxMembers(f, func(id snowflake.ID) types.NodeID { return types.NodeID(id) })
	if q.work != nil {
		q.work.MemberSliceLen = uint64(len(members))
		q.work.MemberSliceCap = uint64(cap(members))
	}
	return members, true, nil
}

// relDiskMembersAfter is relDiskMembers for one value's page: the at most limit
// lowest members above after, read from the cursor on.
func (bs *Store) relDiskMembersAfter(key indexpkg.RelPropertyIndexKey, valueKey string, after types.RelID, limit int) (members []indexpkg.TxMember[types.RelID], handled bool, err error) {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	idx := bs.relPropertyIndexes[key]
	if !idx.OnDisk() {
		return nil, false, nil
	}
	if idx.Mutated != nil {
		return nil, true, ErrIndexNotFound
	}
	f, err := bs.diskTxMembersAfterLocked(bs.memberKind(true, key.RelTypeToken, key.PropertyKey), key.RelTypeToken, key.PropertyKey, valueKey, snowflake.ID(after), limit)
	if err != nil {
		return nil, true, err
	}
	return pageTxMembers(foldedTxMembers(f, func(id snowflake.ID) types.RelID { return types.RelID(id) }), after, limit), true, nil
}

// nodeDiskMembersAfter is relDiskMembersAfter for a node definition whose
// member entries are on disk (nodeMemberDisk).
func (bs *Store) nodeDiskMembersAfter(key indexpkg.PropertyIndexKey, valueKey string, after types.NodeID, limit int) (members []indexpkg.TxMember[types.NodeID], handled bool, err error) {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	if _, ok := bs.nodeMemberDisk[key]; !ok {
		return nil, false, nil
	}
	if bs.propertyIndexes[key].Mutated != nil {
		return nil, true, ErrIndexNotFound
	}
	f, err := bs.diskTxMembersAfterLocked(bs.memberKind(false, key.LabelToken, key.PropertyKey), key.LabelToken, key.PropertyKey, valueKey, after.SnowflakeID(), limit)
	if err != nil {
		return nil, true, err
	}
	return pageTxMembers(foldedTxMembers(f, func(id snowflake.ID) types.NodeID { return types.NodeID(id) }), after, limit), true, nil
}

// diskTxMembersAfterLocked folds the member entries of one value from the ID
// above after on. Entries sort by value, ID, tx, so the walk seeks past the
// cursor and stops at the limit+1st live ID on disk, its steps about limit
// times the entries per ID. The fold may hold more than limit members, every
// buffered one above the cursor: the caller's cut keeps the lowest limit, each
// complete, since the walk covered every ID up to its last and a buffered ID
// above that sorts after limit disk members. Caller holds idxMu (R or W).
func (bs *Store) diskTxMembersAfterLocked(kind byte, scope uint16, propertyKey, valueKey string, after snowflake.ID, limit int) (txMemberFold, error) {
	f := txMemberFold{}
	p, ok := bs.entityIndexValuePrefix(kind, scope, propertyKey, valueKey)
	if !ok || limit <= 0 {
		return f, nil
	}
	tail := storepkg.EntityIndexTailLen(kind)
	add := f.add(kind)
	ov := pendingKeyOverlay[struct{}]{}
	bs.rangePendingPrefixes([][]byte{p}, func(k string, op writeOp) { ov.note(k, op, struct{}{}) })
	err := bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.AllVersions = true
		opts.Prefix = p
		it := txn.NewIterator(opts)
		defer it.Close()
		var prev []byte
		ids, lastID := 0, snowflake.ID(0)
		for it.Seek(binary.BigEndian.AppendUint64(slices.Clone(p), uint64(after)+1)); it.ValidForPrefix(p); it.Next() { // #nosec G115 -- the key's ID bits
			if bs.entityIndexWalkTestHook != nil {
				bs.entityIndexWalkTestHook()
			}
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
			_, id, _, ok := storepkg.EntityIndexEntryOf(key, kind)
			if !ok {
				continue
			}
			if id != lastID {
				if ids == limit {
					return nil
				}
				ids++
				lastID = id
			}
			add(key)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: entity index page scan: %w", err)
	}
	for k := range ov.sets {
		add([]byte(k))
	}
	return f, nil
}

// emitTxMembers hands members to fn outside every store lock (fn re-enters
// the store); false stops.
func emitTxMembers[ID cmp.Ordered](members []indexpkg.TxMember[ID], err error, fn func(ID, types.Instant) bool) error {
	if err != nil {
		return err
	}
	for _, m := range members {
		if !fn(m.ID, m.FirstTx) {
			noteIndexEntryVisited()
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
// entries (scope its label; elements, an Elements definition's per list
// element, ELEMDISK), or a composite node definition's current entries
// (composite its keys, head its entries' head; COMPDISK).
type entityBuildTarget struct {
	rel         bool
	scope       uint16
	propertyKey string
	elements    bool
	composite   []string
	head        []byte
}

// scanEntityIndexEntries reads every stored row of the targets' kind, current
// and history (the write buffer first, then a badger view: scanPropTxRows),
// and hands fn the set op of each entry the rows give the targets: a
// relationship definition's current entries from current rows, a composite's
// from current rows, and every other target's member entries from every row
// (no history row is read when every target is a composite), each with the
// row's span: a current row's as stored, a history row's ended by the row that
// replaced it (memberScanGroup). The targets are all relationship or all node
// definitions. It holds no store lock.
func (bs *Store) scanEntityIndexEntries(targets []entityBuildTarget, fn func(op writeOp) error) error {
	if len(targets) == 0 {
		return nil
	}
	byScope := make(map[uint16][]entityBuildTarget, len(targets))
	for _, t := range targets {
		byScope[t.scope] = append(byScope[t.scope], t)
	}
	axis := bs.validAxis.Get()
	rel := targets[0].rel
	memberKind := storepkg.EntityIndexNodeSpan
	if rel {
		memberKind = storepkg.EntityIndexRelSpan
	}
	group := &memberScanGroup{emit: func(p memberPiece, id snowflake.ID, row memberRow) error {
		for _, op := range bs.entityMemberOp(nil, memberKind, p.t.scope, p.t.propertyKey, p.valueKey, id, row) {
			if err := fn(op); err != nil {
				return err
			}
		}
		return nil
	}}
	if rel {
		want := func(relType int) bool { return relType > 0 && relType <= 0xFFFF && len(byScope[uint16(relType)]) > 0 } // #nosec G115 -- range checked
		group.cur = func(id snowflake.ID) (indexpkg.RowInfo, bool, error) {
			r, err := bs.scanCurrentRel(want, id)
			if r == nil || err != nil {
				return indexpkg.RowInfo{}, false, err
			}
			return indexpkg.RelRowInfo(axis, r), true, nil
		}
		err := bs.scanPropTxRows(storepkg.KeyRel, storepkg.KeyHistRel, func(id snowflake.ID, hist bool, version uint64, raw []byte, local localAnchorFunc) error {
			r, err := bs.decodeScannedRel(want, id, hist, version, raw, local)
			if r == nil || err != nil {
				return err
			}
			var pieces []memberPiece
			for _, t := range byScope[r.TypeToken().Value()] {
				valueKey, _ := r.IndexablePropertyValueKey(t.propertyKey)
				if valueKey == "" {
					continue
				}
				if !hist {
					for _, op := range bs.relCurrentOp(nil, t.scope, t.propertyKey, valueKey, id, writeOpSet) {
						if err := fn(op); err != nil {
							return err
						}
					}
				}
				pieces = append(pieces, memberPiece{t, valueKey})
			}
			return group.row(id, indexpkg.RelRowInfo(axis, r), !hist, pieces)
		})
		if err != nil {
			return err
		}
		return group.flush()
	}
	want := func(w *storepkg.NodeWire) bool {
		has := func(label int) bool { return label > 0 && label <= 0xFFFF && len(byScope[uint16(label)]) > 0 } // #nosec G115 -- range checked
		return has(w.PrimaryLabel) || slices.ContainsFunc(w.ExtraLabels, has)
	}
	group.cur = func(id snowflake.ID) (indexpkg.RowInfo, bool, error) {
		n, err := bs.scanCurrentNode(want, id)
		if n == nil || err != nil {
			return indexpkg.RowInfo{}, false, err
		}
		return indexpkg.NodeRowInfo(axis, n), true, nil
	}
	histKind := byte(0) // composites only: current rows give every entry, so no history row is read
	for _, t := range targets {
		if t.composite == nil {
			histKind = storepkg.KeyHistNode
		}
	}
	err := bs.scanPropTxRows(storepkg.KeyNode, histKind, func(id snowflake.ID, hist bool, version uint64, raw []byte, local localAnchorFunc) error {
		n, err := bs.decodeScannedNode(want, id, hist, version, raw, local)
		if n == nil || err != nil {
			return err
		}
		var pieces []memberPiece
		for i := 0; i < n.LabelTokenCount(); i++ {
			for _, t := range byScope[n.LabelTokenRawAt(i)] {
				if t.composite != nil {
					if vk, ok := indexpkg.NodeCompositeValueKey(t.composite, n); ok && !hist {
						if err := fn(compositeDiskEntry(t.head, vk, id, writeOpSet)); err != nil {
							return err
						}
					}
					continue
				}
				forEachDiskValueKey(n, t.propertyKey, t.elements, func(valueKey string) {
					pieces = append(pieces, memberPiece{t, valueKey})
				})
			}
		}
		return group.row(id, indexpkg.NodeRowInfo(axis, n), !hist, pieces)
	})
	if err != nil {
		return err
	}
	return group.flush()
}

// scanCurrentRel reads the current row of id as a build scan does (the write
// buffer first, then the store): nil when it has none or no wanted type.
func (bs *Store) scanCurrentRel(want func(relType int) bool, id snowflake.ID) (*types.Relationship, error) {
	key := storepkg.RelKey(id)
	if op, ok := bs.lookupPending(string(key)); ok {
		if op.opType == writeOpDelete || len(op.value) == 0 {
			return nil, nil
		}
		return bs.decodeScannedRel(want, id, false, 0, op.value, nil)
	}
	var r *types.Relationship
	err := bs.view(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(key)
		if errors.Is(err, badgerv4.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) (err error) {
			r, err = bs.decodeScannedRel(want, id, false, 0, val, nil)
			return err
		})
	})
	return r, err
}

// scanCurrentNode is scanCurrentRel for a node row.
func (bs *Store) scanCurrentNode(want func(w *storepkg.NodeWire) bool, id snowflake.ID) (*types.Node, error) {
	key := storepkg.NodeKey(id)
	if op, ok := bs.lookupPending(string(key)); ok {
		if op.opType == writeOpDelete || len(op.value) == 0 {
			return nil, nil
		}
		return bs.decodeScannedNode(want, id, false, 0, op.value, nil)
	}
	var n *types.Node
	err := bs.view(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(key)
		if errors.Is(err, badgerv4.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) (err error) {
			n, err = bs.decodeScannedNode(want, id, false, 0, val, nil)
			return err
		})
	})
	return n, err
}

// memberPiece is one (definition, value) a scanned row gives an entry for.
type memberPiece struct {
	t        entityBuildTarget
	valueKey string
}

// memberScanRow is a history row waiting for the rows after it.
type memberScanRow struct {
	ri     indexpkg.RowInfo
	pieces []memberPiece
}

// memberScanGroup derives the member entries of a build scan row by row, as
// the membership sidecar's build does (indexpkg.RowScan): a current row's entry
// as it stands, a history row's ended by the row that replaced it
// (RowInfo.LateSpan) when that row is among the next indexpkg.ReplacingRowWindow
// rows of the entity, whatever they carry. The scan visits every current row
// first, then one entity's history rows after another in version order, so a
// history row waits in rows until its window is full or the entity ends, which
// bounds what the build holds to a window per entity; when the entity ends
// with a replaced open row whose window runs past the history rows, the
// current row is read once (cur) as the last row of the window. A scan in
// another order (the write buffer's rows) only finds fewer replacing rows: a
// row keeps its own span, sound.
type memberScanGroup struct {
	id   snowflake.ID
	rows []memberScanRow
	emit func(p memberPiece, id snowflake.ID, row memberRow) error
	cur  func(id snowflake.ID) (indexpkg.RowInfo, bool, error)
}

// row takes one scanned row of entity id: a current row's entries go out at
// once, a history row waits.
func (g *memberScanGroup) row(id snowflake.ID, ri indexpkg.RowInfo, current bool, pieces []memberPiece) error {
	if current {
		for _, p := range pieces {
			if err := g.emit(p, id, memberRowOf(ri, ri.Span)); err != nil {
				return err
			}
		}
		return nil
	}
	if len(g.rows) > 0 && id != g.id {
		if err := g.flush(); err != nil {
			return err
		}
	}
	g.id = id
	g.rows = append(g.rows, memberScanRow{ri: ri, pieces: pieces})
	for len(g.rows) > indexpkg.ReplacingRowWindow {
		if err := g.resolve(0, nil); err != nil {
			return err
		}
		g.rows = g.rows[1:]
	}
	return nil
}

// resolve emits the entries of rows[i] with the rows after it (and extra,
// rows after those) as its window.
func (g *memberScanGroup) resolve(i int, extra []indexpkg.RowInfo) error {
	r := g.rows[i]
	if len(r.pieces) == 0 {
		return nil
	}
	after := make([]indexpkg.RowInfo, 0, indexpkg.ReplacingRowWindow)
	for _, x := range g.rows[i+1:] {
		after = append(after, x.ri)
	}
	after = append(after, extra...)
	late := r.ri.LateSpan(after)
	for _, p := range r.pieces {
		if err := g.emit(p, g.id, memberRowOf(r.ri, late)); err != nil {
			return err
		}
	}
	return nil
}

// flush ends the entity: every waiting row is resolved against the rows after
// it, the current row among them when its window runs past the history.
func (g *memberScanGroup) flush() error {
	var curRow []indexpkg.RowInfo
	curRead := false
	for i := range g.rows {
		r := g.rows[i]
		if len(r.pieces) > 0 && r.ri.Replaced() && r.ri.Span.To == math.MaxInt64 && len(g.rows)-i-1 < indexpkg.ReplacingRowWindow && g.cur != nil {
			if !curRead {
				curRead = true
				ri, ok, err := g.cur(g.id)
				if err != nil {
					return err
				}
				if ok {
					curRow = []indexpkg.RowInfo{ri}
				}
			}
			if err := g.resolve(i, curRow); err != nil {
				return err
			}
			continue
		}
		if err := g.resolve(i, nil); err != nil {
			return err
		}
	}
	g.rows = g.rows[:0]
	return nil
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
// relationship definition's current head, then the member heads of both
// layouts (the span kind and the one before RELDISKWIN: a store built before
// holds the latter until an open builds it again); nil without a property-key
// token.
func (bs *Store) entityIndexHeads(rel bool, scope uint16, propertyKey string) [][]byte {
	tok, ok := bs.propKeyTokenFor(propertyKey)
	if !ok {
		return nil
	}
	var heads [][]byte
	if rel {
		heads = append(heads, storepkg.AppendEntityIndexHead(nil, storepkg.EntityIndexRelCurrent, scope, tok))
	}
	return append(heads, bs.memberHeads(rel, scope, propertyKey)...)
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
	liveIdx := indexpkg.NewPropertyIndexOnDisk(false)
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
		kept = append(kept, writeOp{opType: writeOpSet, key: storepkg.EntityIndexBuiltKey(storepkg.EntityIndexRelMember, key.RelTypeToken, key.PropertyKey), value: []byte{storepkg.EntityIndexBuiltSpan}})
		bs.appendOpsRaising(storepkg.MemberSpanStoreFormatVersion, kept...)
		bs.idxMu.Unlock()
		return bs.flushIfNeeded()
	}
	return abort(fmt.Errorf("graph: create relationship property index: raced %d purges or drops: %w", propTxBuildAttempts, ErrIndexNotFound))
}

// entityIndexOpen is an open's plan for the 0x0C entries (planEntityIndexOpen):
// which definitions answer from them and what the open writes.
type entityIndexOpen struct {
	marked   map[string]byte     // the built markers present, with their layout (EntityIndexBuiltLegacy, EntityIndexBuiltSpan)
	keep     map[string]struct{} // the markers this open keeps
	canBuild bool
	targets  []entityBuildTarget // the definitions this open builds
	stale    [][]byte            // the heads of their entries before the build
	mark     []writeOp           // built markers to set (with the format marker)
	level    uint16              // the format marker the marks raise to
}

// planEntityIndexOpen reads the 0x0C built markers at open. A definition
// whose entries are trusted (its marker present, kept by every writable open
// since its build, in the span layout) answers from them; an untrusted one, or
// one built before RELDISKWIN (marker value 1: its member entries carry no
// span), a writable open with a property-key registry builds
// (commitEntityIndexOpen), and otherwise it is answered as without 0x0C for
// this open, except that a read-only open reads a definition of the earlier
// layout as it is (place: every windowed lookup answers every member of the
// value, the broad answer it gave).
func (bs *Store) planEntityIndexOpen(txn *badgerv4.Txn) (*entityIndexOpen, error) {
	p := &entityIndexOpen{marked: make(map[string]byte), keep: make(map[string]struct{}),
		canBuild: !bs.readOnly && bs.propKeyReg.Load() != nil}
	it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: storepkg.EntityIndexBuiltPrefix, PrefetchValues: false})
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		layout := storepkg.EntityIndexBuiltLegacy // a marker of v4.92.0 to v4.107: its value is 1
		if err := it.Item().Value(func(v []byte) error {
			if len(v) > 0 && v[0] >= storepkg.EntityIndexBuiltSpan {
				layout = storepkg.EntityIndexBuiltSpan
			}
			return nil
		}); err != nil {
			return nil, fmt.Errorf("graph: read entity-index built marker: %w", err)
		}
		p.marked[string(it.Item().Key())] = layout
	}
	return p, nil
}

// place decides one definition: true when it answers from 0x0C this open. A
// marker of the span layout is trusted. One of the earlier layout is trusted by
// a read-only open only, which records it (memberLegacy) and reads it as it
// is; a writable open builds it again as the span layout. Without a marker a
// writable open with a registry builds it.
func (p *entityIndexOpen) place(bs *Store, rel bool, scope uint16, propertyKey string, elements bool) bool {
	kind := storepkg.EntityIndexNodeMember
	if rel {
		kind = storepkg.EntityIndexRelMember
	}
	k := storepkg.EntityIndexBuiltKey(kind, scope, propertyKey)
	if layout, ok := p.marked[string(k)]; ok {
		if layout == storepkg.EntityIndexBuiltSpan {
			p.keep[string(k)] = struct{}{}
			return true
		}
		if bs.readOnly {
			p.keep[string(k)] = struct{}{}
			if bs.memberLegacy == nil {
				bs.memberLegacy = make(map[memberDef]struct{})
			}
			bs.memberLegacy[memberDef{rel, scope, propertyKey}] = struct{}{}
			return true
		}
	}
	if !p.canBuild {
		return false
	}
	p.stale = append(p.stale, bs.entityIndexHeads(rel, scope, propertyKey)...)
	p.targets = append(p.targets, entityBuildTarget{rel: rel, scope: scope, propertyKey: propertyKey, elements: elements})
	p.mark = append(p.mark, writeOp{opType: writeOpSet, key: k, value: []byte{storepkg.EntityIndexBuiltSpan}})
	p.level = max(p.level, storepkg.MemberSpanStoreFormatVersion)
	return true
}

// placeComposite is place for a composite node definition of a label kept on
// disk (COMPDISK): true when its entries answer from 0x0C this open, never
// without a property-key registry (no head).
func (p *entityIndexOpen) placeComposite(bs *Store, key indexpkg.CompositeIndexKey, keys []string) bool {
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
	p.stale = append(p.stale, head)
	p.targets = append(p.targets, entityBuildTarget{scope: key.LabelToken, composite: keys, head: head})
	p.mark = append(p.mark, writeOp{opType: writeOpSet, key: k, value: []byte{1}})
	p.level = max(p.level, storepkg.CompositeDiskStoreFormatVersion)
	return true
}

// entityIndexOpenCommitHook, set only by tests, runs as commitEntityIndexOpen
// starts writing.
var entityIndexOpenCommitHook func()

// entityIndexOpenBeforeMarkHook, set only by tests, runs after the entries of
// commitEntityIndexOpen's batch were added and before its markers are: an
// error it returns fails the open there, as a crash would end it.
var entityIndexOpenBeforeMarkHook func() error

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
	if entityIndexOpenCommitHook != nil {
		entityIndexOpenCommitHook()
	}
	wb := bs.db.NewWriteBatch()
	defer wb.Cancel()
	for _, key := range unmark {
		if err := wb.Delete(key); err != nil {
			return fmt.Errorf("graph: entity-index build at open: %w", err)
		}
	}
	// The planned definitions' earlier entries, streamed into the batch: one
	// key at a time, never all of them (STALEBUILD).
	if err := bs.view(func(txn *badgerv4.Txn) error {
		for _, head := range p.stale {
			it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: head})
			for it.Rewind(); it.Valid(); it.Next() {
				if err := wb.Delete(it.Item().KeyCopy(nil)); err != nil {
					it.Close()
					return err
				}
			}
			it.Close()
		}
		return nil
	}); err != nil {
		return fmt.Errorf("graph: entity-index build at open: %w", err)
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
	if entityIndexOpenBeforeMarkHook != nil {
		if err := entityIndexOpenBeforeMarkHook(); err != nil {
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
// with-history door demotes, written again as next, the row that replaces it,
// ends it (RELDISKWIN: lateTo is next's start for a row replaced open, last
// the demoted row's TxTo). The entry exists, the row's own key, written when
// the row was; this is its rewrite, one set per value, no read. Its
// time-ordered entries move with it (option B): the late axis's entry at old's
// end is deleted when the end changed, and a widened row gains its recent
// axis's entry. A value prev carries that old, the current row it replaces,
// did not still gets its entry, and an entry of old that prev does not give (a
// demoted row that is not old's copy: another value, version or transaction
// time) is deleted with its time-ordered entries: no row carries it any more,
// and a retention purge finds an entry by the rows it removes. Caller holds
// idxMu.Lock.
func (bs *Store) nodeReplacedMemberOpsLocked(old, prev, next *types.Node, id snowflake.ID) []writeOp {
	if !bs.nodeMembersOnDisk || prev == nil {
		return nil
	}
	row := bs.nodeMemberRow(prev, next)
	var oldRow memberRow
	sameRow := false
	if old != nil {
		oldRow = bs.nodeMemberRow(old, nil)
		sameRow = oldRow.txFrom == row.txFrom && oldRow.version == row.version
	}
	var ops []writeOp
	for _, def := range bs.propDiskDefs {
		if !def.members {
			continue
		}
		var kept []string // the value keys prev gives under def
		if prev.HasLabelTokenRaw(def.key.LabelToken) {
			forEachDiskValueKey(prev, def.key.PropertyKey, def.elements, func(valueKey string) { kept = append(kept, valueKey) })
		}
		var had []string // the value keys old gave under def
		if old != nil && old.HasLabelTokenRaw(def.key.LabelToken) {
			forEachDiskValueKey(old, def.key.PropertyKey, def.elements, func(valueKey string) { had = append(had, valueKey) })
		}
		for _, valueKey := range had {
			if !sameRow || !slices.Contains(kept, valueKey) {
				ops = bs.entityMemberDelOp(ops, storepkg.EntityIndexNodeSpan, def.key.LabelToken, def.key.PropertyKey, valueKey, id, oldRow)
			}
		}
		for _, valueKey := range kept {
			if sameRow && slices.Contains(had, valueKey) {
				ops = bs.entityMemberOp(ops, storepkg.EntityIndexNodeSpan, def.key.LabelToken, def.key.PropertyKey, valueKey, id, row, oldRow)
			} else {
				ops = bs.entityMemberOp(ops, storepkg.EntityIndexNodeSpan, def.key.LabelToken, def.key.PropertyKey, valueKey, id, row)
			}
		}
	}
	return ops
}

// relReplacedMemberOpsLocked is nodeReplacedMemberOpsLocked for a
// relationship. Caller holds idxMu.Lock.
func (bs *Store) relReplacedMemberOpsLocked(old, prev, next *types.Relationship, id snowflake.ID) []writeOp {
	if len(bs.relDiskDefs) == 0 || prev == nil {
		return nil
	}
	tok := prev.TypeToken().Value()
	row := bs.relMemberRow(prev, next)
	var oldRow memberRow
	sameRow := false
	if old != nil {
		oldRow = bs.relMemberRow(old, nil)
		sameRow = oldRow.txFrom == row.txFrom && oldRow.version == row.version
	}
	var ops []writeOp
	for _, def := range bs.relDiskDefs {
		if def.key.RelTypeToken != tok {
			continue
		}
		prevValue, _ := prev.IndexablePropertyValueKey(def.key.PropertyKey)
		oldValue := ""
		if old != nil {
			oldValue, _ = old.IndexablePropertyValueKey(def.key.PropertyKey)
		}
		if oldValue != "" && (!sameRow || oldValue != prevValue) {
			ops = bs.entityMemberDelOp(ops, storepkg.EntityIndexRelSpan, tok, def.key.PropertyKey, oldValue, id, oldRow)
		}
		if prevValue != "" {
			if sameRow && oldValue == prevValue {
				ops = bs.entityMemberOp(ops, storepkg.EntityIndexRelSpan, tok, def.key.PropertyKey, prevValue, id, row, oldRow)
			} else {
				ops = bs.entityMemberOp(ops, storepkg.EntityIndexRelSpan, tok, def.key.PropertyKey, prevValue, id, row)
			}
		}
	}
	return ops
}

// relTombMemberOpsLocked returns the member entries of tomb, the tombstone of
// a deleted relationship row, as its delete stamps it: the same span entry
// with the tombstone's span, its late axis's entry moved from the row's end
// before the delete (a delete that wrote ValidTo ended an open row) and, for a
// widened tombstone, its recent axis's entry (option B). Caller holds
// idxMu.Lock.
func (bs *Store) relTombMemberOpsLocked(tomb *types.Relationship, id snowflake.ID) []writeOp {
	if len(bs.relDiskDefs) == 0 || tomb == nil {
		return nil
	}
	tok := tomb.TypeToken().Value()
	row := bs.relMemberRow(tomb, nil)
	var ops []writeOp
	for _, def := range bs.relDiskDefs {
		if def.key.RelTypeToken != tok {
			continue
		}
		if valueKey, _ := tomb.IndexablePropertyValueKey(def.key.PropertyKey); valueKey != "" {
			ops = bs.entityMemberOp(ops, storepkg.EntityIndexRelSpan, tok, def.key.PropertyKey, valueKey, id, row, preDeleteRow(row))
		}
	}
	return ops
}

// nodeTombMemberOpsLocked is relTombMemberOpsLocked for a node's tombstone.
func (bs *Store) nodeTombMemberOpsLocked(tomb *types.Node, id snowflake.ID) []writeOp {
	if !bs.nodeMembersOnDisk || tomb == nil {
		return nil
	}
	row := bs.nodeMemberRow(tomb, nil)
	var ops []writeOp
	for _, def := range bs.propDiskDefs {
		if !def.members || !tomb.HasLabelTokenRaw(def.key.LabelToken) {
			continue
		}
		forEachDiskValueKey(tomb, def.key.PropertyKey, def.elements, func(valueKey string) {
			ops = bs.entityMemberOp(ops, storepkg.EntityIndexNodeSpan, def.key.LabelToken, def.key.PropertyKey, valueKey, id, row, preDeleteRow(row))
		})
	}
	return ops
}

// relRewrittenTimeOpsLocked returns the deletes of the time-ordered entries of
// old that r, the row an update writes in old's place with old's transaction
// time and version (an in-place rewrite), replaces with other keys: r's span
// entry overwrote old's (the same key), so old's time-ordered entries name a
// row that is gone, and a retention purge, which finds entries by the rows it
// removes, would miss them (option B). Caller holds idxMu.Lock.
func (bs *Store) relRewrittenTimeOpsLocked(old, r *types.Relationship, id snowflake.ID) []writeOp {
	if len(bs.relDiskDefs) == 0 || old == nil || r == nil || old.TypeToken() != r.TypeToken() {
		return nil
	}
	oldRow, row := bs.relMemberRow(old, nil), bs.relMemberRow(r, nil)
	if oldRow == row || oldRow.txFrom != row.txFrom || oldRow.version != row.version {
		return nil
	}
	tok := r.TypeToken().Value()
	var ops []writeOp
	for _, def := range bs.relDiskDefs {
		if def.key.RelTypeToken != tok {
			continue
		}
		valueKey, _ := r.IndexablePropertyValueKey(def.key.PropertyKey)
		if oldValue, _ := old.IndexablePropertyValueKey(def.key.PropertyKey); valueKey == "" || oldValue != valueKey {
			continue
		}
		if p, ok := bs.entityIndexValuePrefix(storepkg.EntityIndexRelTime, tok, def.key.PropertyKey, valueKey); ok {
			ops = memberTimeDelOps(ops, p, id, oldRow, &row)
		}
	}
	return ops
}

// nodeRewrittenTimeOpsLocked is relRewrittenTimeOpsLocked for a node.
func (bs *Store) nodeRewrittenTimeOpsLocked(old, n *types.Node, id snowflake.ID) []writeOp {
	if !bs.nodeMembersOnDisk || old == nil || n == nil {
		return nil
	}
	oldRow, row := bs.nodeMemberRow(old, nil), bs.nodeMemberRow(n, nil)
	if oldRow == row || oldRow.txFrom != row.txFrom || oldRow.version != row.version {
		return nil
	}
	var ops []writeOp
	for _, def := range bs.propDiskDefs {
		if !def.members || !old.HasLabelTokenRaw(def.key.LabelToken) || !n.HasLabelTokenRaw(def.key.LabelToken) {
			continue
		}
		var had []string
		forEachDiskValueKey(old, def.key.PropertyKey, def.elements, func(valueKey string) { had = append(had, valueKey) })
		forEachDiskValueKey(n, def.key.PropertyKey, def.elements, func(valueKey string) {
			if !slices.Contains(had, valueKey) {
				return
			}
			if p, ok := bs.entityIndexValuePrefix(storepkg.EntityIndexNodeTime, def.key.LabelToken, def.key.PropertyKey, valueKey); ok {
				ops = memberTimeDelOps(ops, p, id, oldRow, &row)
			}
		})
	}
	return ops
}
