package badger

import (
	"bytes"
	"fmt"
	"math"
	"slices"

	badgerv4 "github.com/dgraph-io/badger/v4"

	snowflake "github.com/bds421/rho-snowflake-2026"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Member entries with a span (RELDISKWIN, storeutil/entity_index_key.go): the
// on-disk membership of a definition keeps, per row that carries a value, one
// entry whose key is (value, ID, TxFrom, version) and whose 32-byte value is
// the row's valid-time envelope: from, allTo, lateTo, last. A reader folds the
// entries of a member (indexpkg.DiskMember) into the posting the RAM sidecar
// keeps, so the windowed door (store.RelPropertyTxMemberWindowCapability)
// narrows the members to those whose envelope meets the window, as the sidecar
// does (indexpkg.PropertyTxMembers.MembersDuring).
//
// Writes, all in the batch of the row and under idxMu.Lock, no read:
//
//   - a create, a put and a history version write the row's entry
//     (relIndexDiskOpsLocked, nodeMemberOpsLocked): lateTo its stored end;
//   - an update writes the demoted row's entry again with lateTo the new row's
//     start (RowInfo.EndedBy: an open row ends where its replacement starts)
//     and last its TxTo (relReplacedMemberOpsLocked, nodeReplacedMemberOpsLocked);
//   - a delete writes the deleted row's entry again as its tombstone stamps it:
//     allTo reopened when the delete wrote the ValidTo (RowInfo.AnyPinSpan),
//     lateTo the tombstone's end, last its DeletedAt.
//
// Every other write can only narrow a row's derived interval, so an entry that
// misses it stays a sound superset (the header of
// index/property_tx_members_window.go). The version is in the key because
// Instant is ms: a create and an update of one entity in one ms share value, ID
// and TxFrom, and the newer entry would overwrite a wider one.

// memberDef names one definition kept in 0x0C: a relationship type's or a
// node label's property.
type memberDef struct {
	rel         bool
	scope       uint16
	propertyKey string
}

// memberKind returns the kind of the member entries of the definition the
// reads walk: the span kind, or the member kind before RELDISKWIN for a
// definition a read-only open found in that layout (memberLegacy).
func (bs *Store) memberKind(rel bool, scope uint16, propertyKey string) byte {
	kind := storepkg.EntityIndexNodeMember
	if rel {
		kind = storepkg.EntityIndexRelMember
	}
	if _, legacy := bs.memberLegacy[memberDef{rel, scope, propertyKey}]; legacy {
		return kind
	}
	return storepkg.EntityIndexSpanKind(kind)
}

// memberRow is what a row's member entry records: the key's transaction time
// and version and the value's envelope.
type memberRow struct {
	txFrom                    types.Instant
	version                   uint32
	from, allTo, lateTo, last types.Instant
}

// memberRowOf returns ri's member row; late is the span the row answers at
// pins at or after its last stamp (ri.Span, or ri.EndedBy(next) for a row an
// update demoted).
func memberRowOf(ri indexpkg.RowInfo, late indexpkg.Span) memberRow {
	return memberRow{txFrom: ri.TxFrom, version: ri.Version, from: ri.Span.From, allTo: ri.AnyPinSpan().To, lateTo: late.To, last: ri.Last()}
}

// relMemberRow returns r's member row; next the row that replaced r (nil: r as
// it stands).
func (bs *Store) relMemberRow(r, next *types.Relationship) memberRow {
	axis := bs.validAxis.Get()
	ri := indexpkg.RelRowInfo(axis, r)
	late := ri.Span
	if next != nil {
		late = ri.EndedBy(indexpkg.RelRowInfo(axis, next))
	}
	return memberRowOf(ri, late)
}

// nodeMemberRow is relMemberRow for a node row.
func (bs *Store) nodeMemberRow(n, next *types.Node) memberRow {
	axis := bs.validAxis.Get()
	ri := indexpkg.NodeRowInfo(axis, n)
	late := ri.Span
	if next != nil {
		late = ri.EndedBy(indexpkg.NodeRowInfo(axis, next))
	}
	return memberRowOf(ri, late)
}

// memberEntry returns the key and value of row's entry of kind (a span kind)
// under the value prefix.
func memberEntry(prefix []byte, kind byte, id snowflake.ID, row memberRow) (key, value []byte) {
	key = storepkg.AppendEntityIndexEntry(prefix, kind, id, int64(row.txFrom), row.version)
	value = storepkg.AppendEntityIndexSpanValue(make([]byte, 0, storepkg.EntityIndexSpanValueLen),
		int64(row.from), int64(row.allTo), int64(row.lateTo), int64(row.last))
	return key, value
}

// widened reports whether row answers a wider span at pins before its last
// stamp than after (allTo > lateTo: an open row an update demoted, a tombstone
// whose delete wrote ValidTo): such a row has an entry on the recent axis too.
func (row memberRow) widened() bool { return row.allTo > row.lateTo }

// timePrefix returns the value prefix of the time-ordered kind that goes with
// the span kind's value prefix p (the same payload under its own head).
func timePrefix(p []byte) []byte {
	tp := slices.Clone(p)
	tp[1] = storepkg.EntityIndexTimeKind(tp[1])
	return tp
}

// memberTimeKeys returns row's time-ordered keys under the time kind's value
// prefix tp: the late axis's, and the recent axis's for a widened row (nil
// otherwise).
func memberTimeKeys(tp []byte, id snowflake.ID, row memberRow) (late, recent []byte) {
	late = storepkg.AppendEntityIndexTimeEntry(slices.Clip(tp), storepkg.EntityIndexAxisLate, int64(row.lateTo), int64(row.from), id, int64(row.txFrom), row.version)
	if row.widened() {
		recent = storepkg.AppendEntityIndexTimeEntry(slices.Clip(tp), storepkg.EntityIndexAxisRecent, int64(row.last), int64(row.from), id, int64(row.txFrom), row.version)
	}
	return late, recent
}

// memberTimeOps appends the sets of row's time-ordered entries (value the span
// entry's) to ops.
func memberTimeOps(ops []writeOp, tp []byte, id snowflake.ID, row memberRow, value []byte) []writeOp {
	late, recent := memberTimeKeys(tp, id, row)
	ops = append(ops, writeOp{opType: writeOpSet, key: late, value: value})
	if recent != nil {
		ops = append(ops, writeOp{opType: writeOpSet, key: recent, value: value})
	}
	return ops
}

// memberTimeDelOps appends the deletes of was's time-ordered entries, except
// the keys of row (nil: none), which the caller sets.
func memberTimeDelOps(ops []writeOp, tp []byte, id snowflake.ID, was memberRow, row *memberRow) []writeOp {
	wl, wr := memberTimeKeys(tp, id, was)
	var late, recent []byte
	if row != nil {
		late, recent = memberTimeKeys(tp, id, *row)
	}
	for _, k := range [][]byte{wl, wr} {
		if k != nil && !bytes.Equal(k, late) && !bytes.Equal(k, recent) {
			ops = append(ops, writeOp{opType: writeOpDelete, key: k})
		}
	}
	return ops
}

// entityMemberOp appends the set of row's member entry of kind (a span kind)
// for valueKey to ops, with its time-ordered entries (option B); was are rows
// of the same entity and value whose time-ordered entries row's replace (the
// row before a demotion, a delete or an in-place rewrite): their keys that row
// does not write again are deleted.
func (bs *Store) entityMemberOp(ops []writeOp, kind byte, scope uint16, propertyKey, valueKey string, id snowflake.ID, row memberRow, was ...memberRow) []writeOp {
	prefix, ok := bs.entityIndexValuePrefix(kind, scope, propertyKey, valueKey)
	if !ok {
		return ops
	}
	key, value := memberEntry(prefix, kind, id, row)
	ops = append(ops, writeOp{opType: writeOpSet, key: key, value: value})
	tp := timePrefix(prefix)
	for _, w := range was {
		ops = memberTimeDelOps(ops, tp, id, w, &row)
	}
	return memberTimeOps(ops, tp, id, row, value)
}

// entityMemberDelOp appends the delete of row's member entry of kind (a span
// kind) of id for valueKey, and of its time-ordered entries.
func (bs *Store) entityMemberDelOp(ops []writeOp, kind byte, scope uint16, propertyKey, valueKey string, id snowflake.ID, row memberRow) []writeOp {
	prefix, ok := bs.entityIndexValuePrefix(kind, scope, propertyKey, valueKey)
	if !ok {
		return ops
	}
	ops = append(ops, writeOp{opType: writeOpDelete, key: storepkg.AppendEntityIndexEntry(prefix, kind, id, int64(row.txFrom), row.version)})
	return memberTimeDelOps(ops, timePrefix(prefix), id, row, nil)
}

// preDeleteRow returns the row a tombstone's row was before its delete: a
// delete that wrote ValidTo ended an open row (lateTo was allTo), any other
// left the span as it was.
func preDeleteRow(tomb memberRow) memberRow {
	row := tomb
	row.lateTo = row.allTo
	return row
}

// memberPurgeKeys returns every key a row's entries for valueKey may hold
// (option B, a retention purge, which must leave none): the span entry, the
// late axis's entry at each of lateTos (the row's standing end, its end as
// a replacing row ended it, its end before a delete) and the recent axis's.
func (bs *Store) memberPurgeKeys(kind byte, scope uint16, propertyKey, valueKey string, id snowflake.ID, row memberRow, lateTos []types.Instant) [][]byte {
	prefix, ok := bs.entityIndexValuePrefix(kind, scope, propertyKey, valueKey)
	if !ok {
		return nil
	}
	keys := [][]byte{storepkg.AppendEntityIndexEntry(slices.Clone(prefix), kind, id, int64(row.txFrom), row.version)}
	tp := timePrefix(prefix)
	for _, to := range lateTos {
		keys = append(keys, storepkg.AppendEntityIndexTimeEntry(slices.Clip(tp), storepkg.EntityIndexAxisLate, int64(to), int64(row.from), id, int64(row.txFrom), row.version))
	}
	return append(keys, storepkg.AppendEntityIndexTimeEntry(slices.Clip(tp), storepkg.EntityIndexAxisRecent, int64(row.last), int64(row.from), id, int64(row.txFrom), row.version))
}

// purgeLateTos returns the ends a member entry of rows[i] may have been
// written with: its stored end, its any-pin end (before a delete that wrote
// ValidTo) and its end as each other row of the entity may have ended it
// (RowInfo.EndedBy), distinct.
func purgeLateTos(rows []indexpkg.RowInfo, i int) []types.Instant {
	ri := rows[i]
	tos := []types.Instant{ri.Span.To}
	add := func(to types.Instant) {
		if !slices.Contains(tos, to) {
			tos = append(tos, to)
		}
	}
	add(ri.AnyPinSpan().To)
	for j, next := range rows {
		if j != i {
			add(ri.EndedBy(next).To)
		}
	}
	return tos
}

// memberWindow is a windowed membership lookup: the valid-time window [start,
// end) at transaction time pin (0: no pin).
type memberWindow struct {
	start, end, pin types.Instant
}

// diskWindowMembersLocked answers a windowed lookup of the definition (kind, a
// span kind, scope, propertyKey) for valueKey from its time-ordered entries
// (RELDISKWIN option B): the members with a row that may be valid somewhere in
// the window at the pin, each with the lowest transaction time of those rows.
// Two seeks, as the membership sidecar's two views (late, recent):
//
//   - the late axis from the window's start on: the rows whose lateTo is past
//     the start, each kept when it meets the window at the pin
//     (indexpkg.RowMeets);
//   - with a pin (0 < pin < MaxInt64), the recent axis from its newest row down
//     to the first whose last stamp is at or before the pin: the widened rows
//     written after the pin, each kept when its any-pin span meets the window.
//
// A row that meets is on one of them: at pins at or after its last stamp it
// answers its late span, which ends past the start; at an earlier pin its
// any-pin span, the late span unless the row is widened (then it is on the
// recent axis). The rows of a member are judged one at a time, so the answer
// is a subset of the sidecar's (which joins a member's rows into one envelope)
// and a superset of what a resolver matches. The walk never visits a row that
// ended before the window, nor an unwidened one written after the pin; it
// visits the rows that start after the window's end (the late axis is
// ordered by end, not start), the widened rows written after an old pin, and
// the open rows (genuine candidates whose start the read checks). The write
// buffer's entries under each axis are judged the same way. An empty or
// reversed window answers nothing without reading. Caller holds idxMu (R or W).
func (bs *Store) diskWindowMembersLocked(kind byte, scope uint16, propertyKey, valueKey string, w memberWindow, work *storecontract.PropertyMemberWork) (txMemberFold, error) {
	out := txMemberFold{}
	if w.start >= w.end {
		return out, nil
	}
	p, ok := bs.entityIndexValuePrefix(storepkg.EntityIndexTimeKind(kind), scope, propertyKey, valueKey)
	if !ok {
		return out, nil
	}
	judge := func(key, val []byte) {
		_, _, _, id, tx, _, ok := storepkg.EntityIndexTimeEntryOf(key)
		if !ok {
			return
		}
		from, allTo, lateTo, last, ok := storepkg.ParseEntityIndexSpanValue(val)
		if ok && !indexpkg.RowMeets(types.Instant(from), types.Instant(allTo), types.Instant(lateTo), types.Instant(last), w.start, w.end, w.pin) {
			return
		}
		if prev, seen := out[id]; seen {
			out[id] = indexpkg.MergeFirstTx(prev, types.Instant(tx))
		} else {
			out[id] = types.Instant(tx)
		}
	}
	// The late axis: lateTo > start.
	late := append(slices.Clip(p), storepkg.EntityIndexAxisLate)
	seek := storepkg.AppendEntityIndexTimeSeek(slices.Clip(p), storepkg.EntityIndexAxisLate, int64(w.start)+1)
	if err := bs.entityIndexSeekLocked(late, seek, nil, judge, work); err != nil {
		return nil, err
	}
	if w.pin != 0 && w.pin < math.MaxInt64 {
		// The recent axis: last > pin, newest first.
		recent := append(slices.Clip(p), storepkg.EntityIndexAxisRecent)
		past := func(key []byte) bool {
			_, last, _, _, _, _, ok := storepkg.EntityIndexTimeEntryOf(key)
			return ok && types.Instant(last) <= w.pin
		}
		if err := bs.entityIndexSeekLocked(recent, recent, past, judge, work); err != nil {
			return nil, err
		}
	}
	if work != nil {
		work.FoldMembersPeak = uint64(len(out))
		work.MemberSliceLen = uint64(len(out))
		work.MemberSliceCap = uint64(len(out))
	}
	return out, nil
}

// entityIndexSeekLocked visits once each live time-ordered entry under prefix
// (a value prefix and axis) from seek on, up to the first key past reports
// (nil: the prefix's end), with its value: the persisted keys' newest version
// and the write buffer's, resolved per key (lesson 57); a buffered entry
// outside [seek, past) is not visited. The walk takes every version
// (AllVersions) and counts each loop entry (IteratorSteps, the boundary
// included), its seek (IteratorSeeks) and each live persisted entry it hands
// on (DiskEntriesEmitted). Caller holds idxMu (R or W).
func (bs *Store) entityIndexSeekLocked(prefix, seek []byte, past func(key []byte) bool, fn func(key, val []byte), work *storecontract.PropertyMemberWork) error {
	tail := storepkg.EntityIndexTimeTailLen - 1 // after the axis
	in := func(k []byte) bool {
		return len(k) == len(prefix)+tail && bytes.Compare(k, seek) >= 0 && (past == nil || !past(k))
	}
	ov := pendingKeyOverlay[[]byte]{}
	bs.rangePendingPrefixes([][]byte{prefix}, func(k string, op writeOp) {
		if work != nil {
			work.OverlayOpsInspected++
		}
		var val []byte
		if op.opType == writeOpSet {
			val = slices.Clone(op.value)
		}
		noteMemberOverlay(&ov, k, op, val, work)
	})
	err := bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.AllVersions = true
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		if work != nil {
			work.IteratorSeeks++
		}
		var prev []byte
		for it.Seek(seek); it.ValidForPrefix(prefix); it.Next() {
			if work != nil {
				work.IteratorSteps++
			}
			if bs.entityIndexWalkTestHook != nil {
				bs.entityIndexWalkTestHook()
			}
			item := it.Item()
			key := item.Key()
			if len(key) != len(prefix)+tail {
				continue // not an entry of this layout: skipped, never decoded
			}
			if past != nil && past(key) {
				break
			}
			if bytes.Equal(key, prev) {
				continue // an older version of the key just decided
			}
			prev = append(prev[:0], key...)
			if item.IsDeletedOrExpired() || ov.overrides(key) {
				continue
			}
			if work != nil {
				work.DiskEntriesEmitted++
			}
			if err := item.Value(func(val []byte) error { fn(key, val); return nil }); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("graph: entity index seek: %w", err)
	}
	for k, val := range ov.sets {
		if !in([]byte(k)) {
			continue
		}
		if work != nil {
			work.OverlaySetsEmitted++
		}
		fn([]byte(k), val)
	}
	return nil
}

// memberHeads returns the heads of every kind of a definition's member
// entries, the span kind, its time-ordered kind (option B) and the one before
// RELDISKWIN, for the deletes of a
// drop, a rebuild and the corruption-path walk (entityIndexHeads adds a
// relationship definition's current head).
func (bs *Store) memberHeads(rel bool, scope uint16, propertyKey string) [][]byte {
	tok, ok := bs.propKeyTokenFor(propertyKey)
	if !ok {
		return nil
	}
	kinds := []byte{storepkg.EntityIndexNodeSpan, storepkg.EntityIndexNodeTime, storepkg.EntityIndexNodeMember}
	if rel {
		kinds = []byte{storepkg.EntityIndexRelSpan, storepkg.EntityIndexRelTime, storepkg.EntityIndexRelMember}
	}
	heads := make([][]byte, 0, len(kinds))
	for _, k := range kinds {
		heads = append(heads, storepkg.AppendEntityIndexHead(nil, k, scope, tok))
	}
	return heads
}
