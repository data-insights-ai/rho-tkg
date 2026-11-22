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

// NodeHistoryOrdered answers from the history-stamps sidecar's ordered bit
// (store.HistorySuffixCapability; historyStamps): true when the node's
// history rows carry no DeletedAt and TxFrom never falls as the version
// rises, or when it has none. An ID awaiting its per-ID read and a probe-only
// store answer false without reading rows. t is not charged: the answer reads
// no row (the sidecar's one build is the store's, as for NodeHistoryStamps).
func (bs *Store) NodeHistoryOrdered(id types.NodeID, _ *storecontract.ReadTally) (bool, error) {
	if err := bs.checkOpen(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateNodeID(id); err != nil {
		return false, err
	}
	if bs.historyPresenceProbeOnly {
		return false, nil
	}
	p := &bs.histNodeStamps
	if !p.built.Load() {
		if err := bs.buildHistoryStamps(p, storepkg.KeyHistNode); err != nil {
			return false, err
		}
	}
	sid := id.SnowflakeID()
	p.mu.RLock()
	defer p.mu.RUnlock()
	if _, v, ok := p.has.get(sid); ok {
		return v.ordered, nil
	}
	_, unresolved := p.unknown[sid]
	return !unresolved, nil
}

// NodeHistoryNewestFirst visits the node's history skeletons by falling
// version (store.HistorySuffixCapability): the buffered rows (captured
// before the badger view, as historyTemporalMetaByPrefixTallied) merged into
// a reverse walk of the committed keys, each committed row charged
// History(1), its ValueSize and a 16-byte buffer before its value is fetched,
// as the ascending list charges it. The walk ends when visit returns false.
func (bs *Store) NodeHistoryNewestFirst(id types.NodeID, t *storecontract.ReadTally, visit func(storecontract.VersionTemporalMeta) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(id); err != nil {
		return err
	}
	prefix := storepkg.HistNodePrefix(id.SnowflakeID())
	overlay, overlayDeletes, err := bs.pendingHistoryVersionOverlayTallied(prefix, 0, t)
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
