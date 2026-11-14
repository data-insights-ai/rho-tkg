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
// These native methods answer a transaction-time read with a bounded REVERSE
// scan instead of the mandatory-store fallback in core/txtime.go (which
// materializes the entity's whole version history): the history keys are
// ordered by (entity, ascending version), so a reverse iterator visits
// versions newest-first and stops as soon as the as-of rule is decided —
// O(versions newer than the query, plus the one row that held the current
// slot). Declaring the four methods on *Store enables the native path via
// nativeTransactionTimeQuery (core.go).
//
// Selection semantics are storeutil.SelectAsOfWithCurrent's (shared with the
// memory backend and the core fallback; see selectAsOfScanTallied). Temporal
// "rewinding" (TxTo / DeletedAt that post-date txTime) is applied by the core
// layer, which also deep-copies the returned entity (txTimeQueryCopy is true
// for non-memory natives), so these methods return the raw selected version.

// reverseScanHistoryVersionTallied walks one entity's version history newest-first and
// invokes consider(version, valueBytes) for each version in DESCENDING version
// order until consider returns stop=true (or the history is exhausted). It
// merges the badger reverse iterator with the pending write overlay: a pending
// SET overrides badger for the same key, a pending DELETE hides a badger key.
//
// CONTRACT: val is only valid for the DURATION of the consider call (for
// persisted rows it is badger's internal buffer, surfaced inside an
// Item().Value closure) — a callback that retains bytes past its return MUST
// copy them. Classification can inspect an unretained row without copying
// its full value; the selector admits values that must outlive the callback.
//
// prefix is the 9-byte per-entity history prefix (HistNodePrefix / HistRelPrefix).
func (bs *Store) reverseScanHistoryVersionTallied(prefix []byte, consider func(version uint64, val []byte) (stop bool, err error), t *storecontract.ReadTally) error {
	// Overlay BEFORE the View — the commit-window ordering. Capturing
	// the overlay INSIDE the View callback, i.e. AFTER badger assigned the
	// transaction's snapshot instant, would let a concurrent flush() that
	// committed a parked row and cleared `flushing` between the snapshot
	// instant and the overlay read leave the row in NEITHER view. Capturing
	// first makes it structural: a row committed after the capture was still in
	// `flushing` at capture time (flush clears strictly after commit) and is in
	// the overlay; a row committed before the View is in the snapshot.
	pendingSets, pendingDeletes, err := bs.pendingHistoryVersionOverlayTallied(prefix, 0, t)
	if err != nil {
		return err
	}
	return bs.view(func(txn *badgerv4.Txn) error {
		return bs.reverseScanHistoryVersionInTxnOverlay(txn, prefix, pendingSets, pendingDeletes, consider, t)
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
// the commit-window gap. The overlay must therefore be captured
// once, at the same instant as the txn snapshot.
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
	return bs.reverseScanHistoryVersionInTxnOverlay(txn, prefix, pendingSets, pendingDeletes, consider, nil)
}

// reverseScanHistoryVersionInTxnOverlay is the shared scan body: reverse-walks
// prefix's badger history keys through txn, merged with the caller-supplied
// pending overlay (either a live per-call read or a frozen bulk-scan
// snapshot — see the two callers above).
func (bs *Store) reverseScanHistoryVersionInTxnOverlay(txn *badgerv4.Txn, prefix []byte, pendingSets map[string][]byte, pendingDeletes map[string]struct{}, consider func(version uint64, val []byte) (stop bool, err error), t *storecontract.ReadTally) error {
	// Logical key-list slots and the reverse seek key, before allocation.
	if err := t.Buffer(16*len(pendingSets) + storepkg.SizeHistKey); err != nil {
		return err
	}
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
	opts.Prefix = prefix
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
	nextBadgerKey := func() (string, bool, error) {
		for it.ValidForPrefix(prefix) {
			if err := t.History(1); err != nil {
				return "", false, err
			}
			noteIndexEntryVisited()
			k := it.Item().Key()
			if len(k) != storepkg.SizeHistKey {
				it.Next()
				continue
			}
			if err := t.Buffer(len(k)); err != nil {
				return "", false, err
			}
			return string(k), true, nil
		}
		return "", false, nil
	}

	bKey, bValid, err := nextBadgerKey()
	if err != nil {
		return err
	}
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

		advance := func() error {
			if usePending {
				pi++
			}
			if consumeBadger {
				it.Next()
				bKey, bValid, err = nextBadgerKey()
				return err
			}
			return nil
		}

		if _, deleted := pendingDeletes[chooseKey]; deleted {
			if err := advance(); err != nil {
				return err
			}
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
			if err := t.History(1); err != nil {
				return err
			}
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
		if stop {
			return nil
		}
		if err := advance(); err != nil {
			return err
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

// historyTxWindow returns a history row's TxFrom/TxTo, decoding the fixed v2
// tail when it can and the temporal block otherwise (delta and legacy rows).
type historyTxWindow func(version uint64, val []byte) (txFrom, txTo int64, err error)

// historyTemporal decodes a history row's temporal block (only for the rows
// the write-run and tombstone rules inspect); nil when the row has none.
type historyTemporal func(version uint64, val []byte) (*types.TemporalMetadata, error)

// asOfTxTail admits a full-row tail decode attempt before reading its stored
// scalar values. A legacy row falls back to a separately admitted decoder.
func (bs *Store) asOfTxTail(val []byte, t *storecontract.ReadTally) (int64, int64, bool, error) {
	if storepkg.HistoryValueKindOf(val) != storepkg.HistoryFull {
		return 0, 0, false, nil
	}
	if err := t.DecodedHistory(len(val)); err != nil {
		return 0, 0, false, err
	}
	tf, tt, ok := storepkg.PeekWireTemporalTail(val)
	if ok {
		bs.historyMetaDecodes.Add(1)
	}
	return tf, tt, ok, nil
}

func (bs *Store) nodeTxWindow(id snowflake.ID, t *storecontract.ReadTally) historyTxWindow {
	return func(version uint64, val []byte) (int64, int64, error) {
		if tf, tt, ok, err := bs.asOfTxTail(val, t); ok || err != nil {
			return tf, tt, err
		}
		n, err := bs.historyNodeTemporalTallied(id, version, val, t)
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

func (bs *Store) relTxWindow(id snowflake.ID, t *storecontract.ReadTally) historyTxWindow {
	return func(version uint64, val []byte) (int64, int64, error) {
		if tf, tt, ok, err := bs.asOfTxTail(val, t); ok || err != nil {
			return tf, tt, err
		}
		r, err := bs.historyRelTemporalTallied(id, version, val, t)
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

func (bs *Store) nodeRowTemporal(id snowflake.ID, t *storecontract.ReadTally) historyTemporal {
	return func(version uint64, val []byte) (*types.TemporalMetadata, error) {
		n, err := bs.historyNodeTemporalTallied(id, version, val, t)
		if err != nil {
			return nil, err
		}
		return n.Temporal(), nil
	}
}

func (bs *Store) relRowTemporal(id snowflake.ID, t *storecontract.ReadTally) historyTemporal {
	return func(version uint64, val []byte) (*types.TemporalMetadata, error) {
		r, err := bs.historyRelTemporalTallied(id, version, val, t)
		if err != nil {
			return nil, err
		}
		return r.Temporal(), nil
	}
}

// liveHistoryKeyExists reports whether a history key is present: a buffered
// write (pending, or flushing mid-commit) decides, else a badger point read of
// the key (no value read). Same commit-window ordering as GetNodeVersion: the
// buffer is consulted before the read transaction opens.
func (bs *Store) liveHistoryKeyExists(key []byte, t *storecontract.ReadTally) (bool, error) {
	if err := t.Candidates(1); err != nil {
		return false, err
	}
	if op, ok := bs.lookupPending(string(key)); ok {
		return op.opType != writeOpDelete, nil
	}
	err := bs.view(func(txn *badgerv4.Txn) error {
		_, err := txn.Get(key)
		return err
	})
	if errors.Is(err, badgerv4.ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, err
}

// liveHistoryAbove answers selectAsOfScan's "may a history row lie above v"
// for the point as-of doors: no history (the presence set, a RAM lookup) or
// the set's upper bound of the top version at or below v answers no without
// a read; else the key v+1 answers yes, and when it is absent the scan decides
// (a gap above v: history compaction trims the oldest rows, which after
// bounded corrections lie directly above the current row; this is the
// rule of the graph's nodeHasHistoryAbove). key returns the history key of a
// version.
func (bs *Store) liveHistoryAbove(p *historyPresence, id snowflake.ID, has bool, err error, key func(version uint64) []byte, t *storecontract.ReadTally) func(v uint64) (bool, error) {
	return func(v uint64) (bool, error) {
		if err != nil || !has {
			return false, err
		}
		if top, ok := bs.historyTopBound(p, id); ok {
			return uint64(top) > v, nil
		}
		if ok, err := bs.liveHistoryKeyExists(key(v+1), t); err != nil || ok {
			return ok, err
		}
		return true, nil
	}
}

// snapshotHistoryAbove is liveHistoryAbove for the bulk as-of scans:
// limit/known come from bulkPresence.bound, an upper bound of the highest
// version the entity can hold at the scan's snapshot (none: no read); else
// the overlay captured once before the shared transaction, then the shared
// transaction's point read of v+1, and when it is absent the scan decides.
func (bs *Store) snapshotHistoryAbove(txn *badgerv4.Txn, overlay historyOverlaySnapshot, kind byte, id snowflake.ID, limit int64, known bool) func(v uint64) (bool, error) {
	if known {
		return func(v uint64) (bool, error) {
			return limit >= 0 && uint64(limit) > v, nil // #nosec G115 -- limit >= 0 checked first
		}
	}
	return func(v uint64) (bool, error) {
		if bs.bulkAsOfKeyProbeTestHook != nil {
			bs.bulkAsOfKeyProbeTestHook()
		}
		key := storepkg.HistNodeKey(id, v+1)
		if kind == storepkg.KeyHistRel {
			key = storepkg.HistRelKey(id, v+1)
		}
		if _, ok := overlay.entries[string(key)]; ok { // a map index: string(key) does not allocate
			return true, nil
		}
		if _, ok := overlay.deletes[string(key)]; !ok {
			_, err := txn.Get(key)
			if err == nil {
				return true, nil
			}
			if !errors.Is(err, badgerv4.ErrKeyNotFound) {
				return false, err
			}
		}
		return true, nil // v+1 absent: a row may sit above a gap, the scan decides
	}
}

// asOfRow is one visited history row of selectAsOfScanTallied (raw copied: the scan's
// value is only valid inside its callback).
type asOfRow struct {
	version uint64
	tf, tt  int64
	raw     []byte
}

// selectAsOfScanTallied applies storeutil.SelectAsOfWithCurrent's rule to one
// entity's history, visited newest-version-first by scan (the
// reverseScanHistoryVersionTallied contract), and the live current row (cur, nil
// when none). It stops as soon as the rule is decided:
//
//   - current arm (cur recorded by txTime, TxTo == 0): above(curVersion)
//     says whether a row may sit above the current version (false: none,
//     decided without the scan); if one may, the versions above cur's are visited
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
func selectAsOfScanTallied(a types.ValidAxis, scan func(consider func(version uint64, val []byte) (bool, error)) error, cur *types.TemporalMetadata, curVersion uint32, txTime types.Instant, window historyTxWindow, temporal historyTemporal, above func(v uint64) (bool, error), t *storecontract.ReadTally) (asOfPick, error) {
	retain := func(run []asOfRow, version uint64, tf, tt int64, val []byte) ([]asOfRow, error) {
		// A row's logical run slot (three scalars and a byte-slice header), plus
		// its raw value: admit both before append copies either.
		if err := t.Buffer(48 + len(val)); err != nil {
			return nil, err
		}
		raw := append([]byte(nil), val...)
		noteHistoryBytesCopied(len(raw))
		return append(run, asOfRow{version: version, tf: tf, tt: tt, raw: raw}), nil
	}

	// answer picks within a run (newest first); withCur appends the current
	// row as the run's last member.
	answer := func(run []asOfRow, withCur bool) (asOfPick, *types.TemporalMetadata, error) {
		for _, r := range run {
			tm, err := temporal(r.version, r.raw)
			if err != nil {
				return asOfPick{}, nil, err
			}
			if tm != nil && storepkg.OwnOpenAtTxTime(a, tm, txTime) {
				return asOfPick{found: true, version: r.version, raw: r.raw}, tm, nil
			}
		}
		if withCur && storepkg.OwnOpenAtTxTime(a, cur, txTime) {
			return asOfPick{found: true, fromCurrent: true}, cur, nil
		}
		tm, err := temporal(run[0].version, run[0].raw)
		return asOfPick{found: true, version: run[0].version, raw: run[0].raw}, tm, err
	}
	retracted := func(tm *types.TemporalMetadata) bool {
		return tm != nil && ((tm.TxTo != 0 && tm.TxTo <= txTime) || (tm.DeletedAt != 0 && tm.DeletedAt <= txTime))
	}
	// void: an excise's void piece is never an as-of answer and the rule
	// reads as if it were absent (storeutil.SelectAsOfWithCurrent). It
	// carries no TxTo, so only a row without one is decoded.
	void := func(version uint64, val []byte, tt int64) (bool, error) {
		if tt != 0 {
			return false, nil
		}
		tm, err := temporal(version, val)
		return storepkg.IsVoid(tm), err
	}

	if cur != nil && cur.TxFrom > 0 && cur.TxFrom <= txTime && cur.TxTo == 0 {
		if curVersion == math.MaxUint32 {
			return asOfPick{found: true, fromCurrent: true}, nil
		}
		if above, err := above(uint64(curVersion)); err != nil {
			return asOfPick{}, err
		} else if !above {
			return asOfPick{found: true, fromCurrent: true}, nil
		}
		// Every version above cur's is visited: a row there carrying a
		// retraction (TxTo at or after its TxFrom) belongs to an earlier life
		// of the ID (a re-import stored before re-imports kept the earlier life's history), and then none of
		// them answers for cur.
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
			if v, err := void(version, val, tt); err != nil || v {
				return false, err
			}
			switch {
			case len(run) == 0:
				if tf >= int64(cur.TxFrom) && types.Instant(tf) <= txTime {
					run, err = retain(run, version, tf, tt, val)
					if err != nil {
						return false, err
					}
				}
			case !broken && tf == run[0].tf:
				run, err = retain(run, version, tf, tt, val)
				if err != nil {
					return false, err
				}
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
			if v, err := void(version, val, tt); err != nil || v {
				return false, err
			}
			run, err = retain(run, version, tf, tt, val)
			if err != nil {
				return false, err
			}
			newestTxFrom = tf
			return false, nil
		}
		if inRun && tf == newestTxFrom {
			if v, err := void(version, val, tt); err != nil {
				return false, err
			} else if v {
				return false, nil
			}
			run, err = retain(run, version, tf, tt, val)
			if err != nil {
				return false, err
			}
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
	return bs.NodeAsOfMetered(nid, txTime, nil)
}

var _ storecontract.TransactionTimeQueryMeteredCapability = (*Store)(nil)

// NodeAsOfMetered admits the current and history probes, overlay captures,
// reverse walk, metadata/winner decodes and retained raw buffers before the
// native work. A delta's anchor has its own admitted point read and copy.
// Refusal returns a *store.ReadBudgetError and no row; nil t is NodeAsOf.
func (bs *Store) NodeAsOfMetered(nid types.NodeID, txTime types.Instant, t *storecontract.ReadTally) (*types.Node, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}

	// Current row: cache-backed (GetNode), no pending check needed — current
	// rows are written through the cache synchronously before the badger commit.
	// Read it BEFORE the history scan: a with-history door publishes the moved
	// row to history before it changes the cache (publishMoveLocked), so this
	// order sees the row at least once.
	current, err := bs.asOfCurrentNode(nid, t)
	if err != nil && !errors.Is(err, ErrNodeNotFound) {
		return nil, err
	}
	bs.afterAsOfCurrentRead()
	id := nid.SnowflakeID()
	prefix := storepkg.HistNodePrefix(id)
	scan := func(consider func(version uint64, val []byte) (bool, error)) error {
		return bs.reverseScanHistoryVersionTallied(prefix, consider, t)
	}
	has, err := bs.asOfHistoryPresence(&bs.histNodePresence, storepkg.KeyHistNode, id, t)
	if t != nil && err != nil {
		return nil, err
	}
	above := bs.liveHistoryAbove(&bs.histNodePresence, id, has, err, func(v uint64) []byte { return storepkg.HistNodeKey(id, v) }, t)
	return bs.nodeAsOfPickTallied(id, current, txTime, scan, above, t)
}

// asOfCurrentNode is the current-row read of a point as-of door: GetNode, or
// under a tally the metered lend (counted before its decode on a cache miss)
// copied as GetNode copies.
func (bs *Store) asOfCurrentNode(nid types.NodeID, t *storecontract.ReadTally) (*types.Node, error) {
	if t == nil {
		return bs.GetNode(nid)
	}
	if err := t.Candidates(1); err != nil {
		return nil, err
	}
	n, err := bs.prefetchNodeTallied(nid, t)
	if err != nil || n == nil {
		return nil, err
	}
	if err := t.Buffer(8); err != nil {
		return nil, err
	}
	return n.DeepCopy(), nil
}

// asOfCurrentRel mirrors asOfCurrentNode for relationships.
func (bs *Store) asOfCurrentRel(rid types.RelID, t *storecontract.ReadTally) (*types.Relationship, error) {
	if t == nil {
		return bs.GetRelationship(rid)
	}
	if err := t.Candidates(1); err != nil {
		return nil, err
	}
	r, err := bs.prefetchRelTallied(rid, t)
	if err != nil || r == nil {
		return nil, err
	}
	if err := t.Buffer(8); err != nil {
		return nil, err
	}
	return r.DeepCopy(), nil
}

// afterAsOfCurrentRead runs asOfAfterCurrentTestHook (tests only) between a
// point as-of door's current-row read and its history scan.
func (bs *Store) afterAsOfCurrentRead() {
	if h := bs.asOfAfterCurrentTestHook; h != nil {
		h()
	}
}

// nodeAsOfPick runs selectAsOfScan for one node and materializes the winner:
// the current row (GetNode / getNodeInTxn already returned a copy) or the
// winning history row, fully reconstructed (its anchor point-read if it is a
// delta).
func (bs *Store) nodeAsOfPick(id snowflake.ID, current *types.Node, txTime types.Instant, scan func(consider func(version uint64, val []byte) (bool, error)) error, above func(v uint64) (bool, error)) (*types.Node, error) {
	return bs.nodeAsOfPickTallied(id, current, txTime, scan, above, nil)
}

// nodeAsOfPickTallied is nodeAsOfPick counting the winning history row's
// stored bytes (and a delta's anchor) in t before its decode.
func (bs *Store) nodeAsOfPickTallied(id snowflake.ID, current *types.Node, txTime types.Instant, scan func(consider func(version uint64, val []byte) (bool, error)) error, above func(v uint64) (bool, error), t *storecontract.ReadTally) (*types.Node, error) {
	var cur *types.TemporalMetadata
	var curVersion uint32
	if current != nil {
		cur, curVersion = current.Temporal(), current.Version()
	}
	pick, err := selectAsOfScanTallied(bs.validAxis.Get(), scan, cur, curVersion, txTime, bs.nodeTxWindow(id, t), bs.nodeRowTemporal(id, t), above, t)
	if err != nil {
		return nil, err
	}
	switch {
	case !pick.found:
		return nil, ErrVersionNotFound
	case pick.fromCurrent:
		return current, nil
	}
	return bs.decodeHistoryNodeValueTallied(id, pick.version, pick.raw, t)
}

// RelAsOf returns the relationship version visible at txTime. Mirrors NodeAsOf.
func (bs *Store) RelAsOf(rid types.RelID, txTime types.Instant) (*types.Relationship, error) {
	return bs.RelAsOfMetered(rid, txTime, nil)
}

// RelAsOfMetered is NodeAsOfMetered for relationships.
func (bs *Store) RelAsOfMetered(rid types.RelID, txTime types.Instant, t *storecontract.ReadTally) (*types.Relationship, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}

	current, err := bs.asOfCurrentRel(rid, t)
	if err != nil && !errors.Is(err, ErrRelNotFound) {
		return nil, err
	}
	bs.afterAsOfCurrentRead() // current row first, then history — see NodeAsOf
	id := rid.SnowflakeID()
	prefix := storepkg.HistRelPrefix(id)
	scan := func(consider func(version uint64, val []byte) (bool, error)) error {
		return bs.reverseScanHistoryVersionTallied(prefix, consider, t)
	}
	has, err := bs.asOfHistoryPresence(&bs.histRelPresence, storepkg.KeyHistRel, id, t)
	if t != nil && err != nil {
		return nil, err
	}
	above := bs.liveHistoryAbove(&bs.histRelPresence, id, has, err, func(v uint64) []byte { return storepkg.HistRelKey(id, v) }, t)
	return bs.relAsOfPickTallied(id, current, txTime, scan, above, t)
}

// relAsOfPick mirrors nodeAsOfPick for relationships.
func (bs *Store) relAsOfPick(id snowflake.ID, current *types.Relationship, txTime types.Instant, scan func(consider func(version uint64, val []byte) (bool, error)) error, above func(v uint64) (bool, error)) (*types.Relationship, error) {
	return bs.relAsOfPickTallied(id, current, txTime, scan, above, nil)
}

// relAsOfPickTallied mirrors nodeAsOfPickTallied for relationships.
func (bs *Store) relAsOfPickTallied(id snowflake.ID, current *types.Relationship, txTime types.Instant, scan func(consider func(version uint64, val []byte) (bool, error)) error, above func(v uint64) (bool, error), t *storecontract.ReadTally) (*types.Relationship, error) {
	var cur *types.TemporalMetadata
	var curVersion uint32
	if current != nil {
		cur, curVersion = current.Temporal(), current.Version()
	}
	pick, err := selectAsOfScanTallied(bs.validAxis.Get(), scan, cur, curVersion, txTime, bs.relTxWindow(id, t), bs.relRowTemporal(id, t), above, t)
	if err != nil {
		return nil, err
	}
	switch {
	case !pick.found:
		return nil, ErrVersionNotFound
	case pick.fromCurrent:
		return current, nil
	}
	return bs.decodeHistoryRelValueTallied(id, pick.version, pick.raw, t)
}

// nodeAsOfInTxn is NodeAsOf's body reading through an ALREADY-OPEN read
// transaction and an ALREADY-CAPTURED overlay snapshot instead of opening/
// reading its own — used by NodesAsOf's single-transaction bulk scan.
// Same selection rule and same error contract as NodeAsOf
// (ErrVersionNotFound on no visible version). See historyOverlaySnapshot for
// why the overlay must be pre-captured rather than live-read per entity.
//
// Winner decode: the default (non-delta) path decodes from the copied raw
// bytes alone, no txn needed. Under opt-in HistoryDeltaEncoding a delta
// winner's anchor read opens its OWN nested transaction (legal — badger read
// txns nest fine — it is not folded into the shared txn since delta mode is
// opt-in).
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
	return bs.nodeAsOfPick(id, current, txTime, scan, bs.snapshotHistoryAbove(snap.anyTxn(), overlay, storepkg.KeyHistNode, id, limit, known))
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
	return bs.relAsOfPick(id, current, txTime, scan, bs.snapshotHistoryAbove(snap.anyTxn(), overlay, storepkg.KeyHistRel, id, limit, known))
}

// NodesAsOf returns every node version visible at txTime: the union of live
// nodes (current + history) and history-only nodes (deleted but with history),
// running the per-entity NodeAsOf selection on each. Misses are omitted; returns
// nil, nil when no node existed at txTime. Mirrors memory NodesAsOf.
//
// Single-snapshot consistency: Phase 2 runs inside ONE badger
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
// FUTURE txTime this is the contract: a concurrent write racing the scan is
// EITHER fully excluded (blocks on idxMu until the scan completes, then applies)
// OR fully preceded it — NEVER visible to some entities and not others within one
// NodesAsOf call. NodesAsOf/RelsAsOf are bulk, infrequent, admin/reporting-style
// doors, so blocking writers for the scan's duration is the accepted trade (see
// TestNodesAsOf_SingleSnapshotConsistencyUnderConcurrentWrite).
func (bs *Store) NodesAsOf(txTime types.Instant) ([]*types.Node, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}

	// Phase 1: collect candidate IDs, UNLOCKED. ForEachDeletedNodeID opens its
	// OWN paged transactions internally, so it must run BEFORE Phase 2's long
	// idxMu.RLock() hold — nesting a second idxMu.RLock() from the SAME
	// goroutine inside that hold would self-deadlock once a writer is queued
	// (sync.RWMutex is not reentrant). A candidate list collected
	// slightly before Phase 2's lock is fine — the two-phase "collect IDs, then
	// process" convention (docs/architecture.md "History-aware merging")
	// accepts this; the consistency guarantee below is about VALUES read during
	// Phase 2, not about the candidate SET being perfectly synced to one
	// instant.
	bs.idxMu.RLock()
	liveIDs := make([]types.NodeID, 0, bs.nodeIDs.Len())
	bs.nodeIDs.Ascend(func(id int64) bool { liveIDs = append(liveIDs, types.NodeID(id)); return true })
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
	// commit-window fix) — see historyOverlaySnapshot's doc comment for why a
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
	if bs.bulkAsOfOverlayTestHook != nil {
		bs.bulkAsOfOverlayTestHook()
	}
	idx := 0
	snap := newScanSnapshot(bs, bs.nodeCache.FlushEpoch)
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
// NodesAsOf, including its single-snapshot-consistency contract for future pins.
func (bs *Store) RelsAsOf(txTime types.Instant) ([]*types.Relationship, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}

	bs.idxMu.RLock()
	liveIDs := make([]types.RelID, 0, bs.relIDs.Len())
	bs.relIDs.Ascend(func(id int64) bool { liveIDs = append(liveIDs, types.RelID(id)); return true })
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
	if bs.bulkAsOfOverlayTestHook != nil {
		bs.bulkAsOfOverlayTestHook()
	}
	idx := 0
	snap := newScanSnapshot(bs, bs.relCache.FlushEpoch)
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

// asOfHistoryPresence uses the same entity-scoped admitted probe as an
// effective scan. An unmetered call retains the presence cache's usual build.
func (bs *Store) asOfHistoryPresence(p *historyPresence, kind byte, id snowflake.ID, t *storecontract.ReadTally) (bool, error) {
	if t == nil {
		return bs.hasHistory(p, kind, id, bs.probeHistoryPresence)
	}
	s := &readSnapshot{bs: bs, tally: t}
	defer s.Close()
	return bs.hasHistoryTallied(p, kind, id, s.probe, t)
}
