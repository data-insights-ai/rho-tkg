package badger

import (
	"errors"
	"sort"

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
// (entity, ascending version) and version tracks TxFrom monotonically, so a
// reverse iterator visits versions newest-first and stops at the first version
// visible at the query time — O(versions newer than the query) instead of
// O(all versions). Adding the four methods directly to *Store auto-enables the
// native path via nativeTransactionTimeQuery (core.go), no wiring change.
//
// Selection semantics mirror the memory backend exactly (memorystore_history.go
// nodeAsOfLocked / nodeMatchesTxTime): the current row wins iff it is open in
// transaction time and already committed at txTime; otherwise the visible
// version is the highest-TxFrom history version with TxFrom <= txTime that was
// not yet superseded at txTime. Temporal "rewinding" (TxTo / DeletedAt that
// post-date txTime) is applied by the core layer, which also deep-copies the
// returned entity (txTimeQueryCopy is true for non-memory natives), so these
// methods return the raw selected version.

// txTimeMatchesCurrent reports whether a current entity row is the version
// visible at txTime: open in tx-time (TxTo == 0) and already committed
// (0 < TxFrom <= txTime). Mirrors memory's nodeMatchesTxTime / relMatchesTxTime.
func txTimeMatchesCurrent(tm *types.TemporalMetadata, txTime types.Instant) bool {
	return tm != nil && tm.TxFrom > 0 && tm.TxFrom <= txTime && tm.TxTo == 0
}

// txTimeVersionVerdict classifies one history version during the reverse scan.
type txTimeVersionVerdict int

const (
	txTimeSkip    txTimeVersionVerdict = iota // not committed yet at txTime (or no temporal) — keep scanning older
	txTimeVisible                             // this version is visible at txTime — done
	txTimeHidden                              // first committed-by-txTime version, but superseded — entity not visible, done
)

// classifyVersionAtTxTime decides a version's fate against txTime. The reverse
// scan visits versions in descending TxFrom order; the FIRST version with
// TxFrom <= txTime is decisive because transaction-time intervals of an entity's
// versions are contiguous and non-overlapping (a superseding version's TxFrom
// equals the superseded version's TxTo), so no older version can cover txTime if
// this one does not. That is why the scan can stop early instead of materializing
// the whole chain like the fallback.
func classifyVersionAtTxTime(tm *types.TemporalMetadata, txTime types.Instant) txTimeVersionVerdict {
	if tm == nil || tm.TxFrom == 0 || tm.TxFrom > txTime {
		return txTimeSkip
	}
	if tm.TxTo == 0 || tm.TxTo > txTime {
		return txTimeVisible
	}
	return txTimeHidden
}

// reverseScanHistoryVersion walks one entity's version history newest-first and
// invokes consider(version, valueBytes) for each version in DESCENDING version
// order until consider returns stop=true (or the history is exhausted). It
// merges the badger reverse iterator with the pending write overlay: a pending
// SET overrides badger for the same key, a pending DELETE hides a badger key.
//
// prefix is the 9-byte per-entity history prefix (HistNodePrefix / HistRelPrefix).
func (bs *Store) reverseScanHistoryVersion(prefix []byte, consider func(version uint64, val []byte) (stop bool, err error)) error {
	return bs.db.View(func(txn *badgerv4.Txn) error {
		return bs.reverseScanHistoryVersionInTxn(txn, prefix, consider)
	})
}

// reverseScanHistoryVersionInTxn is reverseScanHistoryVersion's body reading
// through an ALREADY-OPEN read transaction instead of opening its own — used by
// a single-entity caller (NodeAsOf/RelAsOf via the wrapper above) that opens its
// OWN transaction per call, so a live per-call overlay read is safe: nothing can
// commit-and-drain the overlay between this call's overlay read and its own
// txn's snapshot instant, because both happen back-to-back under the SAME call.
// The bulk multi-entity scan (NodesAsOf/RelsAsOf, BACKLOG 18k) must NOT use this
// path — see reverseScanHistoryVersionInTxnSnapshot.
func (bs *Store) reverseScanHistoryVersionInTxn(txn *badgerv4.Txn, prefix []byte, consider func(version uint64, val []byte) (stop bool, err error)) error {
	pendingSets, pendingDeletes := bs.pendingHistoryVersionOverlay(prefix, 0)
	return bs.reverseScanHistoryVersionInTxnOverlay(txn, prefix, pendingSets, pendingDeletes, consider)
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

	opts := badgerv4.DefaultIteratorOptions
	opts.PrefetchValues = true
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

		var val []byte
		if usePending {
			val = pendingSets[chooseKey]
		} else {
			if err := it.Item().Value(func(v []byte) error {
				val = append([]byte(nil), v...)
				return nil
			}); err != nil {
				return err
			}
		}

		stop, err := consider(historyVersionFromKey([]byte(chooseKey)), val)
		if err != nil {
			return err
		}
		advance()
		if stop {
			return nil
		}
	}
}

// NodeAsOf returns the node version visible at txTime without materializing the
// node's full history. See the file header for the algorithm and semantics.
func (bs *Store) NodeAsOf(nid types.NodeID, txTime types.Instant) (*types.Node, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}

	// Current-row arm: cache-backed (GetNode), no pending check needed — current
	// rows are written through the cache synchronously before the badger commit.
	current, err := bs.GetNode(nid)
	if err != nil && !errors.Is(err, ErrNodeNotFound) {
		return nil, err
	}
	if current != nil && txTimeMatchesCurrent(current.Temporal(), txTime) {
		return current, nil // GetNode already returned a DeepCopy
	}

	// History arm: reverse-scan for the visible superseded version. Classify by
	// temporal metadata only — a delta carries the full temporal block in its Meta,
	// so no anchor read is needed during the scan; the winning version is fully
	// reconstructed (point-reading its anchor if it is a delta) after the scan.
	id := nid.SnowflakeID()
	var winnerVersion uint64
	var winnerRaw []byte
	found := false
	scanErr := bs.reverseScanHistoryVersion(storepkg.HistNodePrefix(id), func(version uint64, val []byte) (bool, error) {
		n, err := bs.historyNodeTemporal(id, version, val)
		if err != nil {
			return false, err
		}
		switch classifyVersionAtTxTime(n.Temporal(), txTime) {
		case txTimeVisible:
			winnerRaw = append([]byte(nil), val...)
			winnerVersion = version
			found = true
			return true, nil
		case txTimeHidden:
			return true, nil // decisive: entity not visible at txTime
		default:
			return false, nil // keep scanning older versions
		}
	})
	if scanErr != nil {
		return nil, scanErr
	}
	if !found {
		return nil, ErrVersionNotFound
	}
	return bs.decodeHistoryNodeValue(id, winnerVersion, winnerRaw)
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
	if current != nil && txTimeMatchesCurrent(current.Temporal(), txTime) {
		return current, nil
	}

	id := rid.SnowflakeID()
	var winnerVersion uint64
	var winnerRaw []byte
	found := false
	scanErr := bs.reverseScanHistoryVersion(storepkg.HistRelPrefix(id), func(version uint64, val []byte) (bool, error) {
		r, err := bs.historyRelTemporal(id, version, val)
		if err != nil {
			return false, err
		}
		switch classifyVersionAtTxTime(r.Temporal(), txTime) {
		case txTimeVisible:
			winnerRaw = append([]byte(nil), val...)
			winnerVersion = version
			found = true
			return true, nil
		case txTimeHidden:
			return true, nil
		default:
			return false, nil
		}
	})
	if scanErr != nil {
		return nil, scanErr
	}
	if !found {
		return nil, ErrVersionNotFound
	}
	return bs.decodeHistoryRelValue(id, winnerVersion, winnerRaw)
}

// nodeAsOfInTxn is NodeAsOf's body reading through an ALREADY-OPEN read
// transaction and an ALREADY-CAPTURED overlay snapshot instead of opening/
// reading its own — used by NodesAsOf's single-transaction bulk scan
// (BACKLOG 18k). Same selection algorithm and same error contract as NodeAsOf
// (ErrVersionNotFound on no visible version). See historyOverlaySnapshot for
// why the overlay must be pre-captured rather than live-read per entity.
func (bs *Store) nodeAsOfInTxn(txn *badgerv4.Txn, nid types.NodeID, txTime types.Instant, overlay historyOverlaySnapshot) (*types.Node, error) {
	current, err := bs.getNodeInTxn(txn, nid)
	if err != nil && !errors.Is(err, ErrNodeNotFound) {
		return nil, err
	}
	if current != nil && txTimeMatchesCurrent(current.Temporal(), txTime) {
		return current, nil
	}

	id := nid.SnowflakeID()
	var winnerVersion uint64
	var winnerRaw []byte
	found := false
	scanErr := bs.reverseScanHistoryVersionInTxnSnapshot(txn, storepkg.HistNodePrefix(id), overlay, func(version uint64, val []byte) (bool, error) {
		n, err := bs.historyNodeTemporal(id, version, val)
		if err != nil {
			return false, err
		}
		switch classifyVersionAtTxTime(n.Temporal(), txTime) {
		case txTimeVisible:
			winnerRaw = append([]byte(nil), val...)
			winnerVersion = version
			found = true
			return true, nil
		case txTimeHidden:
			return true, nil
		default:
			return false, nil
		}
	})
	if scanErr != nil {
		return nil, scanErr
	}
	if !found {
		return nil, ErrVersionNotFound
	}
	// Winner decode: the default (non-delta) path decodes from winnerRaw alone,
	// no txn needed. Under opt-in HistoryDeltaEncoding a delta winner's anchor
	// read opens its OWN nested transaction here (legal — badger read txns
	// nest fine — just not yet folded into the shared txn; BACKLOG 18k design
	// section 6a defers that elimination as a follow-up since delta mode is
	// opt-in/default-off).
	return bs.decodeHistoryNodeValue(id, winnerVersion, winnerRaw)
}

// relAsOfInTxn mirrors nodeAsOfInTxn for relationships.
func (bs *Store) relAsOfInTxn(txn *badgerv4.Txn, rid types.RelID, txTime types.Instant, overlay historyOverlaySnapshot) (*types.Relationship, error) {
	current, err := bs.getRelInTxn(txn, rid)
	if err != nil && !errors.Is(err, ErrRelNotFound) {
		return nil, err
	}
	if current != nil && txTimeMatchesCurrent(current.Temporal(), txTime) {
		return current, nil
	}

	id := rid.SnowflakeID()
	var winnerVersion uint64
	var winnerRaw []byte
	found := false
	scanErr := bs.reverseScanHistoryVersionInTxnSnapshot(txn, storepkg.HistRelPrefix(id), overlay, func(version uint64, val []byte) (bool, error) {
		r, err := bs.historyRelTemporal(id, version, val)
		if err != nil {
			return false, err
		}
		switch classifyVersionAtTxTime(r.Temporal(), txTime) {
		case txTimeVisible:
			winnerRaw = append([]byte(nil), val...)
			winnerVersion = version
			found = true
			return true, nil
		case txTimeHidden:
			return true, nil
		default:
			return false, nil
		}
	})
	if scanErr != nil {
		return nil, scanErr
	}
	if !found {
		return nil, ErrVersionNotFound
	}
	return bs.decodeHistoryRelValue(id, winnerVersion, winnerRaw)
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
	result := make([]*types.Node, 0, len(liveIDs)+len(deletedIDs))
	bs.idxMu.RLock()
	overlay := bs.snapshotHistoryOverlay()
	idx := 0
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		for _, nid := range liveIDs {
			if bs.bulkAsOfScanTestHook != nil {
				bs.bulkAsOfScanTestHook(idx)
			}
			idx++
			n, err := bs.nodeAsOfInTxn(txn, nid, txTime, overlay)
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
			n, err := bs.nodeAsOfInTxn(txn, nid, txTime, overlay)
			if errors.Is(err, ErrVersionNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			result = append(result, n)
		}
		return nil
	})
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

	result := make([]*types.Relationship, 0, len(liveIDs)+len(deletedIDs))
	bs.idxMu.RLock()
	overlay := bs.snapshotHistoryOverlay()
	idx := 0
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		for _, rid := range liveIDs {
			if bs.bulkAsOfScanTestHook != nil {
				bs.bulkAsOfScanTestHook(idx)
			}
			idx++
			r, err := bs.relAsOfInTxn(txn, rid, txTime, overlay)
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
			r, err := bs.relAsOfInTxn(txn, rid, txTime, overlay)
			if errors.Is(err, ErrVersionNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			result = append(result, r)
		}
		return nil
	})
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
