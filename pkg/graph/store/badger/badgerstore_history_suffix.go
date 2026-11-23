package badger

import (
	"cmp"
	"fmt"
	"slices"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

var _ storecontract.HistorySuffixCapability = (*Store)(nil)

// nodeHistoryOrdered answers from the history-stamps sidecar's ordered bit
// (historyStamps): true when the node's history rows carry no DeletedAt and
// TxFrom never falls as the version rises, or when it has none. An ID
// awaiting its per-ID read and a probe-only store answer false without
// reading rows; for the former, stamp is its write generation and pending
// true (NodeHistoryNewestFirst resolves it when its walk reads every row).
// Nothing is charged: the answer reads no row (the sidecar's one build is the
// store's, as for NodeHistoryStamps).
func (bs *Store) nodeHistoryOrdered(id types.NodeID) (ordered, pending bool, stamp uint64, err error) {
	if bs.historyPresenceProbeOnly {
		return false, false, 0, nil
	}
	p := &bs.histNodeStamps
	if !p.built.Load() {
		if err := bs.buildHistoryStamps(p, storepkg.KeyHistNode); err != nil {
			return false, false, 0, err
		}
	}
	sid := id.SnowflakeID()
	p.mu.RLock()
	defer p.mu.RUnlock()
	if _, v, ok := p.has.get(sid); ok {
		return v.ordered, false, 0, nil
	}
	stamp, unresolved := p.unknown[sid]
	return !unresolved, unresolved, stamp, nil
}

// NodeHistoryNewestFirst visits the node's history skeletons by falling
// version with the sidecar's ordered bit (nodeHistoryOrdered;
// store.HistorySuffixCapability): the buffered rows (captured by reference
// before the badger view, pendingHistoryRefs) merged into a reverse walk of
// the committed keys, every visited row charged History(1), its stored bytes
// and a 16-byte buffer before it is decoded, as the ascending list charges
// it, whether it is buffered or committed; rows not visited are not charged. The walk ends when visit returns false.
func (bs *Store) NodeHistoryNewestFirst(id types.NodeID, t *storecontract.ReadTally, visit func(storecontract.VersionTemporalMeta, bool) bool) (bool, error) {
	if err := bs.checkOpen(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateNodeID(id); err != nil {
		return false, err
	}
	ordered, pending, stamp, err := bs.nodeHistoryOrdered(id)
	if err != nil {
		return false, err
	}
	// An ID awaiting its per-ID read: a walk that reads every row (the
	// visitor never stops it) has folded them all, newest first, and resolves
	// the ID with that fold, so the next walk is proven without a second read.
	fold := stampEntry{ordered: true}
	var prevTx types.Instant
	var rows, stopped bool
	err = bs.nodeHistoryNewestFirst(id, t, func(m storecontract.VersionTemporalMeta) bool {
		if pending {
			if m.Temporal == nil {
				fold.ordered = false
			} else {
				tm := m.Temporal
				fold.ordered = fold.ordered && tm.DeletedAt == 0 && (!rows || tm.TxFrom <= prevTx)
				prevTx = tm.TxFrom
				fold.from, fold.to = storecontract.FoldTxStamps(fold.from, fold.to, tm.TxFrom, tm.TxTo, tm.DeletedAt)
			}
			if !rows {
				fold.top = clampTop(uint64(m.Version))
			}
			rows = true
		}
		stopped = !visit(m, ordered)
		return !stopped
	})
	if err == nil && pending && !stopped {
		bs.histNodeStamps.resolve(id.SnowflakeID(), stamp, fold, rows)
	}
	return ordered, err
}

func (bs *Store) nodeHistoryNewestFirst(id types.NodeID, t *storecontract.ReadTally, visit func(storecontract.VersionTemporalMeta) bool) error {
	prefix := storepkg.HistNodePrefix(id.SnowflakeID())
	// The buffered rows, captured by reference before the badger view and
	// charged and decoded only when visited, as a committed row is below.
	overlay, overlayDeletes, err := bs.pendingHistoryRefs(prefix, nil, nil, nil)
	if err != nil {
		return err
	}
	type pending struct {
		version uint64
		raw     []byte
	}
	pend := make([]pending, 0, len(overlay))
	for k, raw := range overlay {
		pend = append(pend, pending{version: historyVersionFromKey([]byte(k)), raw: raw})
	}
	slices.SortFunc(pend, func(a, b pending) int { return cmp.Compare(b.version, a.version) })
	metaOf := func(raw []byte) (storecontract.VersionTemporalMeta, error) {
		bs.historyMetaDecodes.Add(1)
		return historyTemporalMetaOf(raw, true)
	}
	done := false
	// emit visits the buffered rows at or above version (all: below 0).
	emit := func(above uint64, all bool) error {
		for len(pend) > 0 && !done && (all || pend[0].version >= above) {
			raw := pend[0].raw
			pend = pend[1:]
			if err := t.DecodedHistory(len(raw)); err != nil {
				return err
			}
			if err := t.Buffer(16); err != nil {
				return err
			}
			m, err := metaOf(raw)
			if err != nil {
				return fmt.Errorf("graph: decode pending history temporal meta: %w", err)
			}
			done = !visit(m)
		}
		return nil
	}
	if bs.historyScanTestHook != nil {
		bs.historyScanTestHook()
	}
	err = bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Reverse = true
		opts.Prefix = prefix // tables without the key are skipped, not iterated
		it := txn.NewIterator(opts)
		defer it.Close()
		seek := append(slices.Clone(prefix), 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
		for it.Seek(seek); !done && it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) == storepkg.SizeHistKey {
				if err := emit(historyVersionFromKey(key), false); err != nil || done {
					return err // a buffered row at or above this key ended the walk
				}
			}
			if err := t.History(1); err != nil {
				return err
			}
			noteIndexEntryVisited()
			if len(key) != storepkg.SizeHistKey {
				continue
			}
			if _, deleted := overlayDeletes[string(key)]; deleted {
				continue
			}
			if _, buffered := overlay[string(key)]; buffered {
				continue // emitted above: the buffered row wins
			}
			if err := t.Decoded(int(it.Item().ValueSize())); err != nil {
				return err
			}
			if err := t.Buffer(16); err != nil {
				return err
			}
			bs.validEndValueFetches.Add(1)
			if err := it.Item().Value(func(val []byte) error {
				bs.validEndFetchedBytes.Add(int64(len(val)))
				m, err := metaOf(val)
				if err != nil {
					return err
				}
				done = !visit(m)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("graph: scan history temporal meta newest first: %w", err)
	}
	return emit(0, true)
}
