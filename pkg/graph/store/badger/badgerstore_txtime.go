package badger

import (
	"errors"
	"fmt"
	"sort"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
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
	// Pending overlay for every version of this entity (startVersion 0).
	pendingSets, pendingDeletes := bs.pendingHistoryVersionOverlay(prefix, 0)
	pendKeys := make([]string, 0, len(pendingSets))
	for k := range pendingSets {
		pendKeys = append(pendKeys, k)
	}
	// Descending key order == descending version (big-endian version suffix).
	sort.Sort(sort.Reverse(sort.StringSlice(pendKeys)))

	return bs.db.View(func(txn *badgerv4.Txn) error {
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
	})
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

	// History arm: reverse-scan for the visible superseded version.
	id := nid.SnowflakeID()
	var result *types.Node
	scanErr := bs.reverseScanHistoryVersion(storepkg.HistNodePrefix(id), func(version uint64, val []byte) (bool, error) {
		var w storepkg.NodeWire
		if err := msgpack.Unmarshal(val, &w); err != nil {
			return false, fmt.Errorf("graph: unmarshal node version: %w", err)
		}
		n, err := bs.decodeNodeHistoryWireForKey(w, id, version)
		if err != nil {
			return false, fmt.Errorf("graph: decode node version: %w", err)
		}
		switch classifyVersionAtTxTime(n.Temporal(), txTime) {
		case txTimeVisible:
			result = n
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
	if result == nil {
		return nil, ErrVersionNotFound
	}
	return result, nil
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
	var result *types.Relationship
	scanErr := bs.reverseScanHistoryVersion(storepkg.HistRelPrefix(id), func(version uint64, val []byte) (bool, error) {
		var w storepkg.RelWire
		if err := msgpack.Unmarshal(val, &w); err != nil {
			return false, fmt.Errorf("graph: unmarshal rel version: %w", err)
		}
		r, err := bs.decodeRelHistoryWireForKey(w, id, version)
		if err != nil {
			return false, fmt.Errorf("graph: decode rel version: %w", err)
		}
		switch classifyVersionAtTxTime(r.Temporal(), txTime) {
		case txTimeVisible:
			result = r
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
	if result == nil {
		return nil, ErrVersionNotFound
	}
	return result, nil
}

// NodesAsOf returns every node version visible at txTime: the union of live
// nodes (current + history) and history-only nodes (deleted but with history),
// running the per-entity NodeAsOf selection on each. Misses are omitted; returns
// nil, nil when no node existed at txTime. Mirrors memory NodesAsOf.
func (bs *Store) NodesAsOf(txTime types.Instant) ([]*types.Node, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}

	bs.idxMu.RLock()
	liveIDs := make([]types.NodeID, 0, len(bs.nodeIDs))
	for nid := range bs.nodeIDs {
		liveIDs = append(liveIDs, nid)
	}
	bs.idxMu.RUnlock()

	result := make([]*types.Node, 0, len(liveIDs))
	for _, nid := range liveIDs {
		n, err := bs.NodeAsOf(nid, txTime)
		if errors.Is(err, ErrVersionNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		result = append(result, n)
	}

	var iterErr error
	if err := bs.ForEachDeletedNodeID(func(nid types.NodeID) bool {
		n, err := bs.NodeAsOf(nid, txTime)
		if errors.Is(err, ErrVersionNotFound) {
			return true
		}
		if err != nil {
			iterErr = err
			return false
		}
		result = append(result, n)
		return true
	}); err != nil {
		return nil, err
	}
	if iterErr != nil {
		return nil, iterErr
	}

	if len(result) == 0 {
		return nil, nil
	}
	storepkg.SortNodesByID(result)
	return result, nil
}

// RelsAsOf returns every relationship version visible at txTime. Mirrors NodesAsOf.
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

	result := make([]*types.Relationship, 0, len(liveIDs))
	for _, rid := range liveIDs {
		r, err := bs.RelAsOf(rid, txTime)
		if errors.Is(err, ErrVersionNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		result = append(result, r)
	}

	var iterErr error
	if err := bs.ForEachDeletedRelID(func(rid types.RelID) bool {
		r, err := bs.RelAsOf(rid, txTime)
		if errors.Is(err, ErrVersionNotFound) {
			return true
		}
		if err != nil {
			iterErr = err
			return false
		}
		result = append(result, r)
		return true
	}); err != nil {
		return nil, err
	}
	if iterErr != nil {
		return nil, iterErr
	}

	if len(result) == 0 {
		return nil, nil
	}
	storepkg.SortRelsByID(result)
	return result, nil
}
