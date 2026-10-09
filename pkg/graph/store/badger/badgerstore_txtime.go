package badger

import (
	"errors"
	"math"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// TransactionTimeQueryCapability (NodeAsOf / RelAsOf / NodesAsOf / RelsAsOf).
//
// The mandatory-store fallback in core/txtime.go answers a transaction-time
// read by MATERIALIZING the entire version history of an entity (every version
// decoded + deep-copied) and then linear-scanning it. These native methods
// replace that with a bounded REVERSE scan: the history keys are ordered by
// (entity, ascending version), so a reverse iterator visits versions
// newest-first and stops as soon as the as-of rule is decided — O(versions
// newer than the query, plus the one row that held the current slot) instead
// of O(all versions). Adding the four methods directly to *Store auto-enables
// the native path via nativeTransactionTimeQuery (core.go), no wiring change.
//
// Selection semantics are storeutil.SelectAsOfWithCurrent's (shared with the
// memory backend and the core fallback; see selectAsOfScan). Temporal
// "rewinding" (TxTo / DeletedAt that post-date txTime) is applied by the core
// layer, which also deep-copies the returned entity (txTimeQueryCopy is true
// for non-memory natives), so these methods return the raw selected version.

// reverseScanHistoryVersion walks one entity's version history newest-first and
// invokes consider(version, valueBytes) for each version in DESCENDING version
// order until consider returns stop=true (or the history is exhausted). It
// merges the badger reverse iterator with the pending write overlay: a pending
// SET overrides badger for the same key, a pending DELETE hides a badger key.
//
// CONTRACT: val is only valid for the DURATION of the consider call (for
// persisted rows it is badger's internal buffer, surfaced inside an
// Item().Value closure) — a callback that retains bytes past its return MUST
// copy them. This is what lets the tail-peek classification walk versions
// with zero per-version allocations.
//
// prefix is the 9-byte per-entity history prefix (HistNodePrefix / HistRelPrefix).
func (bs *Store) reverseScanHistoryVersion(prefix []byte, consider func(version uint64, val []byte) (stop bool, err error)) error {
	// Overlay BEFORE the View — the lesson-64 commit-window ordering. The
	// previous shape captured the overlay INSIDE the View callback, i.e. AFTER
	// badger assigned the transaction's snapshot instant: a concurrent flush()
	// that committed a parked row and cleared `flushing` between the snapshot
	// instant and the overlay read left the row in NEITHER view — dropped. The
	// gap is nanoseconds when the goroutine runs uninterrupted (the old
	// comment's "back-to-back" argument), but a descheduled goroutine widens
	// it arbitrarily under load — a probability argument, not a correctness
	// one. Capturing first makes it structural: a row committed after the
	// capture was still in `flushing` at capture time (flush clears strictly
	// after commit) and is in the overlay; a row committed before the View is
	// in the snapshot.
	pendingSets, pendingDeletes := bs.pendingHistoryVersionOverlay(prefix, 0)
	return bs.db.View(func(txn *badgerv4.Txn) error {
		return bs.reverseScanHistoryVersionInTxnOverlay(txn, prefix, pendingSets, pendingDeletes, consider)
	})
}

// historyOverlaySnapshot is a frozen, whole-store copy of every buffered
// (pending + flushing) history-version write op, captured ONCE via
// snapshotHistoryOverlay(). NodesAsOf/RelsAsOf's bulk scan captures this
// BEFORE opening its shared badger transaction and threads it through every
// per-entity resolution call, instead of each call independently re-reading
// the live pending/flushing buffers via bs.pendingHistoryVersionOverlay.
//
// Why this matters: a background flush() clears a version from pending only
// AFTER it commits to badger (badgerstore_flush.go's wb.Flush() then
// bs.flushing = nil). If a bulk scan's shared txn is opened at T0 and some
// entity's overlay read happens later at T2, a flush that commits at T1
// (T0 < T1 < T2) makes that version invisible to BOTH: the txn (opened
// before the commit) and the now-drained live overlay (cleared after it) —
// the exact "lesson 64" commit-window gap getNodeHistoryByPrefix closes by
// capturing its overlay before opening its (per-call) View. NodesAsOf's
// single-shared-txn-for-many-entities shape reopens that gap unless the
// overlay is ALSO captured once, at the same instant as the txn snapshot.
type historyOverlaySnapshot struct {
	entries map[string][]byte
	deletes map[string]struct{}
}

// snapshotHistoryOverlay captures every buffered history-version write op
// (pending + flushing, across every entity) in one wbMu critical section.
// Call ONCE per bulk scan, immediately before opening the scan's shared
// badger transaction, so both are consistent with the same instant.
func (bs *Store) snapshotHistoryOverlay() historyOverlaySnapshot {
	snap := historyOverlaySnapshot{
		entries: make(map[string][]byte),
		deletes: make(map[string]struct{}),
	}
	bs.rangePending(func(k string, op writeOp) {
		if len(k) != storepkg.SizeHistKey {
			return
		}
		if op.opType == writeOpDelete {
			delete(snap.entries, k)
			snap.deletes[k] = struct{}{}
			return
		}
		cp := make([]byte, len(op.value))
		copy(cp, op.value)
		snap.entries[k] = cp
		delete(snap.deletes, k)
	})
	return snap
}

// forPrefix filters the frozen snapshot down to one entity's overlay,
// matching pendingHistoryVersionOverlay's per-prefix, per-startVersion
// filtering contract exactly — the only difference is the source is the
// frozen snapshot instead of a live re-read of the write buffer.
func (s historyOverlaySnapshot) forPrefix(prefix []byte, startVersion uint32) (map[string][]byte, map[string]struct{}) {
	prefixStr := string(prefix)
	entries := make(map[string][]byte, len(s.entries))
	deletes := make(map[string]struct{}, len(s.deletes))
	for k, v := range s.entries {
		if len(k) < len(prefixStr) || k[:len(prefixStr)] != prefixStr {
			continue
		}
		if historyVersionFromKey([]byte(k)) < uint64(startVersion) {
			continue
		}
		entries[k] = v
	}
	for k := range s.deletes {
		if len(k) < len(prefixStr) || k[:len(prefixStr)] != prefixStr {
			continue
		}
		if historyVersionFromKey([]byte(k)) < uint64(startVersion) {
			continue
		}
		deletes[k] = struct{}{}
	}
	return entries, deletes
}

// reverseScanHistoryVersionInTxnSnapshot is reverseScanHistoryVersionInTxn's
// bulk-scan sibling: it takes its pending-overlay view from an
// ALREADY-CAPTURED historyOverlaySnapshot instead of live-reading the write
// buffer, closing the commit-window gap documented on historyOverlaySnapshot.
func (bs *Store) reverseScanHistoryVersionInTxnSnapshot(txn *badgerv4.Txn, prefix []byte, snap historyOverlaySnapshot, consider func(version uint64, val []byte) (stop bool, err error)) error {
	pendingSets, pendingDeletes := snap.forPrefix(prefix, 0)
	return bs.reverseScanHistoryVersionInTxnOverlay(txn, prefix, pendingSets, pendingDeletes, consider)
}

// reverseScanHistoryVersionInTxnOverlay is the shared scan body: reverse-walks
// prefix's badger history keys through txn, merged with the caller-supplied
// pending overlay (either a live per-call read or a frozen bulk-scan
// snapshot — see the two callers above).
func (bs *Store) reverseScanHistoryVersionInTxnOverlay(txn *badgerv4.Txn, prefix []byte, pendingSets map[string][]byte, pendingDeletes map[string]struct{}, consider func(version uint64, val []byte) (stop bool, err error)) error {
	pendKeys := make([]string, 0, len(pendingSets))
	for k := range pendingSets {
		pendKeys = append(pendKeys, k)
	}
	// Descending key order == descending version (big-endian version suffix).
	sort.Sort(sort.Reverse(sort.StringSlice(pendKeys)))

	// PrefetchValues=false: prefetch eagerly COPIES every visited item's whole
	// value into an iterator-owned buffer — one alloc+memcpy per walked
	// version, which re-dominates the walk once classification is a 24-byte
	// tail peek. On-demand Item().Value reads hand the closure the underlying
	// bytes without that per-item staging copy.
	opts := badgerv4.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Reverse = true
	it := txn.NewIterator(opts)
	defer it.Close()

	// Seek to the highest possible key for this entity: prefix + all-0xff
	// version. With Reverse=true the iterator then walks versions high→low.
	maxKey := make([]byte, storepkg.SizeHistKey)
	copy(maxKey, prefix)
	for i := len(prefix); i < storepkg.SizeHistKey; i++ {
		maxKey[i] = 0xff
	}
	it.Seek(maxKey)

	// nextBadgerKey advances to the next well-formed badger history key for
	// this prefix and returns it (descending).
	nextBadgerKey := func() (string, bool) {
		for it.ValidForPrefix(prefix) {
			k := it.Item().Key()
			if len(k) != storepkg.SizeHistKey {
				it.Next()
				continue
			}
			return string(k), true
		}
		return "", false
	}

	bKey, bValid := nextBadgerKey()
	pi := 0
	for {
		pValid := pi < len(pendKeys)
		if !bValid && !pValid {
			return nil
		}

		// Choose the lexicographically-larger key (descending version).
		var chooseKey string
		usePending, consumeBadger := false, false
		switch {
		case bValid && pValid:
			switch {
			case pendKeys[pi] > bKey:
				chooseKey, usePending = pendKeys[pi], true
			case pendKeys[pi] < bKey:
				chooseKey, consumeBadger = bKey, true
			default: // same key — pending wins, both advance
				chooseKey, usePending, consumeBadger = pendKeys[pi], true, true
			}
		case pValid:
			chooseKey, usePending = pendKeys[pi], true
		default:
			chooseKey, consumeBadger = bKey, true
		}

		advance := func() {
			if usePending {
				pi++
			}
			if consumeBadger {
				it.Next()
				bKey, bValid = nextBadgerKey()
			}
		}

		if _, deleted := pendingDeletes[chooseKey]; deleted {
			advance()
			continue
		}

		// val is handed to consider WITHOUT a defensive copy — for the badger
		// arm it is only valid inside the Item().Value closure. The consider
		// contract (see reverseScanHistoryVersion) requires callbacks to copy
		// any bytes they retain; the tail-peek classification makes most
		// versions a bounded byte-peek, so an unconditional per-version copy
		// here would dominate the walk again.
		version := historyVersionFromKey([]byte(chooseKey))
		var stop bool
		if usePending {
			var err error
			stop, err = consider(version, pendingSets[chooseKey])
			if err != nil {
				return err
			}
		} else {
			if err := it.Item().Value(func(v []byte) error {
				var cErr error
				stop, cErr = consider(version, v)
				return cErr
			}); err != nil {
				return err
			}
		}
		advance()
		if stop {
			return nil
		}
	}
}

// asOfPick is the outcome of one entity's as-of selection over its history
// reverse scan: the current row answers (fromCurrent), a history row answers
// (version + its raw bytes, copied), or the entity is absent (!found).
type asOfPick struct {
	found       bool
	fromCurrent bool
	version     uint64
	raw         []byte
}

// historyTxWindow returns a history row's TxFrom/TxTo, peeking the fixed v2
// temporal tail when it can (no decode) and decoding the temporal block
// otherwise (delta rows, legacy v1 rows).
type historyTxWindow func(version uint64, val []byte) (txFrom, txTo int64, err error)

// historyTemporal decodes a history row's temporal block (only for the rows
// the write-run and tombstone rules inspect); nil when the row has none.
type historyTemporal func(version uint64, val []byte) (*types.TemporalMetadata, error)

func (bs *Store) nodeTxWindow(id snowflake.ID) historyTxWindow {
	return func(version uint64, val []byte) (int64, int64, error) {
		if storepkg.HistoryValueKindOf(val) == storepkg.HistoryFull {
			if tf, tt, ok := storepkg.PeekWireTemporalTail(val); ok {
				return tf, tt, nil
			}
		}
		n, err := bs.historyNodeTemporal(id, version, val)
		if err != nil {
			return 0, 0, err
		}
		tm := n.Temporal()
		if tm == nil {
			return 0, 0, nil
		}
		return int64(tm.TxFrom), int64(tm.TxTo), nil
	}
}

func (bs *Store) relTxWindow(id snowflake.ID) historyTxWindow {
	return func(version uint64, val []byte) (int64, int64, error) {
		if storepkg.HistoryValueKindOf(val) == storepkg.HistoryFull {
			if tf, tt, ok := storepkg.PeekWireTemporalTail(val); ok {
				return tf, tt, nil
			}
		}
		r, err := bs.historyRelTemporal(id, version, val)
		if err != nil {
			return 0, 0, err
		}
		tm := r.Temporal()
		if tm == nil {
			return 0, 0, nil
		}
		return int64(tm.TxFrom), int64(tm.TxTo), nil
	}
}

func (bs *Store) nodeRowTemporal(id snowflake.ID) historyTemporal {
	return func(version uint64, val []byte) (*types.TemporalMetadata, error) {
		n, err := bs.historyNodeTemporal(id, version, val)
		if err != nil {
			return nil, err
		}
		return n.Temporal(), nil
	}
}

func (bs *Store) relRowTemporal(id snowflake.ID) historyTemporal {
	return func(version uint64, val []byte) (*types.TemporalMetadata, error) {
		r, err := bs.historyRelTemporal(id, version, val)
		if err != nil {
			return nil, err
		}
		return r.Temporal(), nil
	}
}

// liveHistoryKeyExists reports whether a history key is present: a buffered
// write (pending, or flushing mid-commit) decides, else a badger point read of
// the key (no value read). Same commit-window ordering as GetNodeVersion: the
// buffer is consulted before the read transaction opens (lesson 64).
func (bs *Store) liveHistoryKeyExists(key []byte) (bool, error) {
	if op, ok := bs.lookupPending(string(key)); ok {
		return op.opType != writeOpDelete, nil
	}
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		_, err := txn.Get(key)
		return err
	})
	if errors.Is(err, badgerv4.ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, err
}

// historyKeyExistsWithPresence is liveHistoryKeyExists behind the entity's
// history presence (HasNodeHistory / HasRelHistory, a RAM lookup): an entity
// without history rows has no key to read, so the point read is skipped.
func (bs *Store) historyKeyExistsWithPresence(has bool, err error) func(key []byte) (bool, error) {
	return func(key []byte) (bool, error) {
		if err != nil || !has {
			return false, err
		}
		return bs.liveHistoryKeyExists(key)
	}
}

// snapshotHistoryKeyExists is liveHistoryKeyExists for the bulk as-of scans,
// by version: the overlay captured once before the shared transaction decides,
// else the shared transaction's point read. limit/known come from
// bulkPresence.bound: a version above the highest version the entity can hold
// at the scan's snapshot has no key, so nothing is read.
func (bs *Store) snapshotHistoryKeyExists(txn *badgerv4.Txn, overlay historyOverlaySnapshot, kind byte, id snowflake.ID, limit int64, known bool) func(version uint64) (bool, error) {
	if known && limit < 0 {
		return noHistoryKeyExists
	}
	return func(version uint64) (bool, error) {
		if known && version > uint64(limit) {
			return false, nil
		}
		if bs.bulkAsOfKeyProbeTestHook != nil {
			bs.bulkAsOfKeyProbeTestHook()
		}
		key := storepkg.HistNodeKey(id, version)
		if kind == storepkg.KeyHistRel {
			key = storepkg.HistRelKey(id, version)
		}
		if _, ok := overlay.entries[string(key)]; ok { // a map index: string(key) does not allocate
			return true, nil
		}
		if _, ok := overlay.deletes[string(key)]; ok {
			return false, nil
		}
		_, err := txn.Get(key)
		if errors.Is(err, badgerv4.ErrKeyNotFound) {
			return false, nil
		}
		return err == nil, err
	}
}

func noHistoryKeyExists(uint64) (bool, error) { return false, nil }

// asOfRow is one visited history row of selectAsOfScan (raw copied: the scan's
// value is only valid inside its callback).
type asOfRow struct {
	version uint64
	tf, tt  int64
	raw     []byte
}

// selectAsOfScan applies storeutil.SelectAsOfWithCurrent's rule to one
// entity's history, visited newest-version-first by scan (the
// reverseScanHistoryVersion contract), and the live current row (cur, nil
// when none). It stops as soon as the rule is decided:
//
//   - current arm (cur recorded by txTime, TxTo == 0): exists(curVersion+1)
//     decides whether any row sits above the current version (versions are
//     allocated densely); if one does, the versions above cur's are visited
//     and the first recorded at or after cur and by txTime outranks cur,
//     together with the rest of its write (the run sharing its TxFrom, cur
//     included when the run reaches it);
//   - history arm: the first version recorded by txTime is the newest; the run
//     of rows sharing its TxFrom is collected, and the scan continues to the
//     first lower row whose TxTo is at or after its TxFrom (the row that held
//     the current slot): the entity is absent if it is a tombstone deleted
//     after the newest was recorded and by txTime. A hard delete stamps TxTo ==
//     DeletedAt, so only a row whose TxTo lies in that window is decoded.
//
// The answer of a run is its highest row whose own valid interval is open at
// txTime (decoded per row, newest first), else its newest row. Equivalence
// with SelectAsOfWithCurrent is pinned by the badger as-of equivalence tests
// over generated chains.
func selectAsOfScan(scan func(consider func(version uint64, val []byte) (bool, error)) error, cur *types.TemporalMetadata, curVersion uint32, txTime types.Instant, window historyTxWindow, temporal historyTemporal, exists func(version uint64) (bool, error)) (asOfPick, error) {
	// answer picks within a run (newest first); withCur appends the current
	// row as the run's last member.
	answer := func(run []asOfRow, withCur bool) (asOfPick, *types.TemporalMetadata, error) {
		for _, r := range run {
			tm, err := temporal(r.version, r.raw)
			if err != nil {
				return asOfPick{}, nil, err
			}
			if tm != nil && storepkg.OwnOpenAtTxTime(tm, txTime) {
				return asOfPick{found: true, version: r.version, raw: r.raw}, tm, nil
			}
		}
		if withCur && storepkg.OwnOpenAtTxTime(cur, txTime) {
			return asOfPick{found: true, fromCurrent: true}, cur, nil
		}
		tm, err := temporal(run[0].version, run[0].raw)
		return asOfPick{found: true, version: run[0].version, raw: run[0].raw}, tm, err
	}
	retracted := func(tm *types.TemporalMetadata) bool {
		return tm != nil && ((tm.TxTo != 0 && tm.TxTo <= txTime) || (tm.DeletedAt != 0 && tm.DeletedAt <= txTime))
	}

	if cur != nil && cur.TxFrom > 0 && cur.TxFrom <= txTime && cur.TxTo == 0 {
		if curVersion == math.MaxUint32 {
			return asOfPick{found: true, fromCurrent: true}, nil
		}
		if above, err := exists(uint64(curVersion) + 1); err != nil {
			return asOfPick{}, err
		} else if !above {
			return asOfPick{found: true, fromCurrent: true}, nil
		}
		// Every version above cur's is visited: a row there carrying a
		// retraction (TxTo at or after its TxFrom) belongs to an earlier life
		// of the ID (a re-import), and then none of them answers for cur.
		var run []asOfRow
		broken, earlierLife := false, false
		err := scan(func(version uint64, val []byte) (bool, error) {
			if version <= uint64(curVersion) {
				return true, nil
			}
			tf, tt, err := window(version, val)
			if err != nil {
				return false, err
			}
			if tt != 0 && tt >= tf {
				earlierLife = true
				return true, nil
			}
			switch {
			case len(run) == 0:
				if tf >= int64(cur.TxFrom) && types.Instant(tf) <= txTime {
					run = append(run, asOfRow{version: version, tf: tf, tt: tt, raw: append([]byte(nil), val...)})
				}
			case !broken && tf == run[0].tf:
				run = append(run, asOfRow{version: version, tf: tf, tt: tt, raw: append([]byte(nil), val...)})
			default:
				broken = true
			}
			return false, nil
		})
		if err != nil {
			return asOfPick{}, err
		}
		if len(run) == 0 || earlierLife {
			return asOfPick{found: true, fromCurrent: true}, nil
		}
		pick, tm, err := answer(run, !broken && int64(cur.TxFrom) == run[0].tf)
		if err != nil {
			return asOfPick{}, err
		}
		if retracted(tm) {
			return asOfPick{}, nil
		}
		return pick, nil
	}

	var (
		run          []asOfRow
		inRun        = true
		slotDecided  bool
		lifeEnded    bool
		newestTxFrom int64
	)
	err := scan(func(version uint64, val []byte) (bool, error) {
		tf, tt, err := window(version, val)
		if err != nil {
			return false, err
		}
		if len(run) == 0 {
			if tf == 0 || types.Instant(tf) > txTime {
				return false, nil // recorded after the pin — keep scanning older versions
			}
			run = append(run, asOfRow{version: version, tf: tf, tt: tt, raw: append([]byte(nil), val...)})
			newestTxFrom = tf
			return false, nil
		}
		if inRun && tf == newestTxFrom {
			run = append(run, asOfRow{version: version, tf: tf, tt: tt, raw: append([]byte(nil), val...)})
		} else {
			inRun = false
		}
		if !slotDecided && tt != 0 && tt >= tf {
			slotDecided = true
			if tt > newestTxFrom && types.Instant(tt) <= txTime {
				tm, err := temporal(version, val)
				if err != nil {
					return false, err
				}
				if tm != nil && tm.DeletedAt != 0 && int64(tm.DeletedAt) > newestTxFrom && tm.DeletedAt <= txTime {
					lifeEnded = true
				}
			}
		}
		return slotDecided && !inRun, nil
	})
	if err != nil {
		return asOfPick{}, err
	}
	if len(run) == 0 || lifeEnded {
		return asOfPick{}, nil
	}
	pick, tm, err := answer(run, false)
	if err != nil {
		return asOfPick{}, err
	}
	if retracted(tm) {
		return asOfPick{}, nil
	}
	return pick, nil
}

// NodeAsOf returns the node version visible at txTime without materializing the
// node's full history. See the file header and selectAsOfScan for the rule.
func (bs *Store) NodeAsOf(nid types.NodeID, txTime types.Instant) (*types.Node, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}

	// Current row: cache-backed (GetNode), no pending check needed — current
	// rows are written through the cache synchronously before the badger commit.
	current, err := bs.GetNode(nid)
	if err != nil && !errors.Is(err, ErrNodeNotFound) {
		return nil, err
	}
	id := nid.SnowflakeID()
	prefix := storepkg.HistNodePrefix(id)
	scan := func(consider func(version uint64, val []byte) (bool, error)) error {
		return bs.reverseScanHistoryVersion(prefix, consider)
	}
	exists := func(version uint64) (bool, error) {
		return bs.historyKeyExistsWithPresence(bs.HasNodeHistory(nid))(storepkg.HistNodeKey(id, version))
	}
	return bs.nodeAsOfPick(id, current, txTime, scan, exists)
}

// nodeAsOfPick runs selectAsOfScan for one node and materializes the winner:
// the current row (GetNode / getNodeInTxn already returned a copy) or the
// winning history row, fully reconstructed (its anchor point-read if it is a
// delta).
func (bs *Store) nodeAsOfPick(id snowflake.ID, current *types.Node, txTime types.Instant, scan func(consider func(version uint64, val []byte) (bool, error)) error, exists func(version uint64) (bool, error)) (*types.Node, error) {
	var cur *types.TemporalMetadata
	var curVersion uint32
	if current != nil {
		cur, curVersion = current.Temporal(), current.Version()
	}
	pick, err := selectAsOfScan(scan, cur, curVersion, txTime, bs.nodeTxWindow(id), bs.nodeRowTemporal(id), exists)
	if err != nil {
		return nil, err
	}
	switch {
	case !pick.found:
		return nil, ErrVersionNotFound
	case pick.fromCurrent:
		return current, nil
	}
	return bs.decodeHistoryNodeValue(id, pick.version, pick.raw)
}

// RelAsOf returns the relationship version visible at txTime. Mirrors NodeAsOf.
func (bs *Store) RelAsOf(rid types.RelID, txTime types.Instant) (*types.Relationship, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}

	current, err := bs.GetRelationship(rid)
	if err != nil && !errors.Is(err, ErrRelNotFound) {
		return nil, err
	}
	id := rid.SnowflakeID()
	prefix := storepkg.HistRelPrefix(id)
	scan := func(consider func(version uint64, val []byte) (bool, error)) error {
		return bs.reverseScanHistoryVersion(prefix, consider)
	}
	exists := func(version uint64) (bool, error) {
		return bs.historyKeyExistsWithPresence(bs.HasRelHistory(rid))(storepkg.HistRelKey(id, version))
	}
	return bs.relAsOfPick(id, current, txTime, scan, exists)
}

// relAsOfPick mirrors nodeAsOfPick for relationships.
func (bs *Store) relAsOfPick(id snowflake.ID, current *types.Relationship, txTime types.Instant, scan func(consider func(version uint64, val []byte) (bool, error)) error, exists func(version uint64) (bool, error)) (*types.Relationship, error) {
	var cur *types.TemporalMetadata
	var curVersion uint32
	if current != nil {
		cur, curVersion = current.Temporal(), current.Version()
	}
	pick, err := selectAsOfScan(scan, cur, curVersion, txTime, bs.relTxWindow(id), bs.relRowTemporal(id), exists)
	if err != nil {
		return nil, err
	}
	switch {
	case !pick.found:
		return nil, ErrVersionNotFound
	case pick.fromCurrent:
		return current, nil
	}
	return bs.decodeHistoryRelValue(id, pick.version, pick.raw)
}

// nodeAsOfInTxn is NodeAsOf's body reading through an ALREADY-OPEN read
// transaction and an ALREADY-CAPTURED overlay snapshot instead of opening/
// reading its own — used by NodesAsOf's single-transaction bulk scan
// (BACKLOG 18k). Same selection rule and same error contract as NodeAsOf
// (ErrVersionNotFound on no visible version). See historyOverlaySnapshot for
// why the overlay must be pre-captured rather than live-read per entity.
//
// Winner decode: the default (non-delta) path decodes from the copied raw
// bytes alone, no txn needed. Under opt-in HistoryDeltaEncoding a delta
// winner's anchor read opens its OWN nested transaction (legal — badger read
// txns nest fine — just not yet folded into the shared txn; BACKLOG 18k design
// section 6a defers that elimination as a follow-up since delta mode is
// opt-in/default-off).
func (bs *Store) nodeAsOfInTxn(snap *scanSnapshot, nid types.NodeID, txTime types.Instant, overlay historyOverlaySnapshot, presence *bulkPresence) (*types.Node, error) {
	current, err := bs.getNodeInTxn(snap, nid)
	if err != nil && !errors.Is(err, ErrNodeNotFound) {
		return nil, err
	}
	id := nid.SnowflakeID()
	prefix := storepkg.HistNodePrefix(id)
	scan := func(consider func(version uint64, val []byte) (bool, error)) error {
		return bs.reverseScanHistoryVersionInTxnSnapshot(snap.anyTxn(), prefix, overlay, consider)
	}
	limit, known := presence.bound(id)
	return bs.nodeAsOfPick(id, current, txTime, scan, bs.snapshotHistoryKeyExists(snap.anyTxn(), overlay, storepkg.KeyHistNode, id, limit, known))
}

// relAsOfInTxn mirrors nodeAsOfInTxn for relationships.
func (bs *Store) relAsOfInTxn(snap *scanSnapshot, rid types.RelID, txTime types.Instant, overlay historyOverlaySnapshot, presence *bulkPresence) (*types.Relationship, error) {
	current, err := bs.getRelInTxn(snap, rid)
	if err != nil && !errors.Is(err, ErrRelNotFound) {
		return nil, err
	}
	id := rid.SnowflakeID()
	prefix := storepkg.HistRelPrefix(id)
	scan := func(consider func(version uint64, val []byte) (bool, error)) error {
		return bs.reverseScanHistoryVersionInTxnSnapshot(snap.anyTxn(), prefix, overlay, consider)
	}
	limit, known := presence.bound(id)
	return bs.relAsOfPick(id, current, txTime, scan, bs.snapshotHistoryKeyExists(snap.anyTxn(), overlay, storepkg.KeyHistRel, id, limit, known))
}

// NodesAsOf returns every node version visible at txTime: the union of live
// nodes (current + history) and history-only nodes (deleted but with history),
// running the per-entity NodeAsOf selection on each. Misses are omitted; returns
// nil, nil when no node existed at txTime. Mirrors memory NodesAsOf.
//
// Single-snapshot consistency (BACKLOG 18k): Phase 2 runs inside ONE badger
// read transaction AND one continuous bs.idxMu.RLock() hold, so it observes ONE
// consistent point-in-time view with NO concurrent writer able to interleave —
// every writer (PutNode/ReplaceNode/DeleteNode/PutNodeVersion/...) takes
// idxMu.Lock() around both its cache mutation AND its pending-write-buffer
// append (see badgerstore_flush.go's lock-ordering note), so holding idxMu.RLock()
// for the WHOLE scan excludes writers from the cache, the pending overlay, AND
// (via the shared badger transaction) badger's own persisted state, for the
// scan's entire duration — not just per-entity. For a past or NowTx txTime this
// is unobservable either way — every concurrent write has TxFrom > txTime and is
// excluded by classifyVersionAtTxTime regardless of snapshot/lock timing. For a
// FUTURE txTime (rare — normal usage pins the present or the past) this is the
// deliberate, tested, documented contract: a concurrent write racing the scan is
// EITHER fully excluded (blocks on idxMu until the scan completes, then applies)
// OR fully preceded it — NEVER visible to some entities and not others within one
// NodesAsOf call. This is an explicit, named trade: NodesAsOf/RelsAsOf are bulk,
// infrequent, admin/reporting-style doors, not a hot per-mutation path, so
// blocking writers for the scan's duration is the right trade for a genuine
// consistency guarantee rather than a documentation-only promise (see
// TestNodesAsOf_SingleSnapshotConsistencyUnderConcurrentWrite, which caught the
// torn-read case a cache-hit-without-this-lock would otherwise allow).
func (bs *Store) NodesAsOf(txTime types.Instant) ([]*types.Node, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}

	// Phase 1: collect candidate IDs, UNLOCKED. ForEachDeletedNodeID opens its
	// OWN paged transactions internally, so it must run BEFORE Phase 2's long
	// idxMu.RLock() hold — nesting a second idxMu.RLock() from the SAME
	// goroutine inside that hold would self-deadlock once a writer is queued
	// (sync.RWMutex is not reentrant, lesson 9). A candidate list collected
	// slightly before Phase 2's lock is fine — CLAUDE.md's own two-phase
	// "collect IDs, then process" convention already accepts this; the
	// consistency guarantee below is about VALUES read during Phase 2, not
	// about the candidate SET being perfectly synced to one instant.
	bs.idxMu.RLock()
	liveIDs := make([]types.NodeID, 0, len(bs.nodeIDs))
	for nid := range bs.nodeIDs {
		liveIDs = append(liveIDs, nid)
	}
	bs.idxMu.RUnlock()

	var deletedIDs []types.NodeID
	if err := bs.ForEachDeletedNodeID(func(nid types.NodeID) bool {
		deletedIDs = append(deletedIDs, nid)
		return true
	}); err != nil {
		return nil, err
	}

	// Phase 2: one shared read transaction AND one continuous idxMu.RLock hold
	// for the whole scan — see the doc comment above for why both are needed.
	// The history-version overlay is ALSO captured once here, BEFORE opening
	// the shared txn (same ordering discipline as getNodeHistoryByPrefix's
	// lesson-64 fix) — see historyOverlaySnapshot's doc comment for why a
	// per-entity live overlay re-read would reopen that exact commit-window
	// gap across this scan's long real-time duration.
	// The history presence set is built here, before idxMu is taken: a build is
	// a key-only scan of the whole history keyspace and must not run inside the
	// scan's long RLock hold (it would keep every writer out meanwhile). A set
	// that is still unbuilt under the lock (Clear in between) is not used; the
	// scan then probes keys as before (bulkPresence).
	if err := bs.ensureHistoryPresenceBuilt(&bs.histNodePresence, storepkg.KeyHistNode); err != nil {
		return nil, err
	}

	result := make([]*types.Node, 0, len(liveIDs)+len(deletedIDs))
	bs.idxMu.RLock()
	presence := bs.beginBulkPresence(&bs.histNodePresence) // before the overlay capture
	overlay := bs.snapshotHistoryOverlay()
	idx := 0
	snap := newScanSnapshot(bs.db, bs.nodeCache.FlushEpoch)
	err := func() error {
		defer snap.close()
		for _, nid := range liveIDs {
			if bs.bulkAsOfScanTestHook != nil {
				bs.bulkAsOfScanTestHook(idx)
			}
			idx++
			n, err := bs.nodeAsOfInTxn(snap, nid, txTime, overlay, &presence)
			if errors.Is(err, ErrVersionNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			result = append(result, n)
		}
		for _, nid := range deletedIDs {
			if bs.bulkAsOfScanTestHook != nil {
				bs.bulkAsOfScanTestHook(idx)
			}
			idx++
			n, err := bs.nodeAsOfInTxn(snap, nid, txTime, overlay, &presence)
			if errors.Is(err, ErrVersionNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			result = append(result, n)
		}
		return nil
	}()
	bs.idxMu.RUnlock()
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, nil
	}
	storepkg.SortNodesByID(result)
	return result, nil
}

// RelsAsOf returns every relationship version visible at txTime. Mirrors
// NodesAsOf, including its single-snapshot-consistency contract for future pins
// (BACKLOG 18k).
func (bs *Store) RelsAsOf(txTime types.Instant) ([]*types.Relationship, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}

	bs.idxMu.RLock()
	liveIDs := make([]types.RelID, 0, len(bs.relIDs))
	for rid := range bs.relIDs {
		liveIDs = append(liveIDs, rid)
	}
	bs.idxMu.RUnlock()

	var deletedIDs []types.RelID
	if err := bs.ForEachDeletedRelID(func(rid types.RelID) bool {
		deletedIDs = append(deletedIDs, rid)
		return true
	}); err != nil {
		return nil, err
	}

	if err := bs.ensureHistoryPresenceBuilt(&bs.histRelPresence, storepkg.KeyHistRel); err != nil {
		return nil, err
	}

	result := make([]*types.Relationship, 0, len(liveIDs)+len(deletedIDs))
	bs.idxMu.RLock()
	presence := bs.beginBulkPresence(&bs.histRelPresence) // before the overlay capture
	overlay := bs.snapshotHistoryOverlay()
	idx := 0
	snap := newScanSnapshot(bs.db, bs.relCache.FlushEpoch)
	err := func() error {
		defer snap.close()
		for _, rid := range liveIDs {
			if bs.bulkAsOfScanTestHook != nil {
				bs.bulkAsOfScanTestHook(idx)
			}
			idx++
			r, err := bs.relAsOfInTxn(snap, rid, txTime, overlay, &presence)
			if errors.Is(err, ErrVersionNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			result = append(result, r)
		}
		for _, rid := range deletedIDs {
			if bs.bulkAsOfScanTestHook != nil {
				bs.bulkAsOfScanTestHook(idx)
			}
			idx++
			r, err := bs.relAsOfInTxn(snap, rid, txTime, overlay, &presence)
			if errors.Is(err, ErrVersionNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			result = append(result, r)
		}
		return nil
	}()
	bs.idxMu.RUnlock()
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, nil
	}
	storepkg.SortRelsByID(result)
	return result, nil
}
