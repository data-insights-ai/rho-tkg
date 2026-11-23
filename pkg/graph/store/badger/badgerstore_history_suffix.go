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
// awaiting its per-ID read is resolved by that read, each of its rows charged
// to t as the ascending list charges it (historyStampEntry), so the bit holds
// again after a write the sidecar could not fold (a cascade demoting the
// current row). A probe-only store answers false without reading rows. A
// known bit costs nothing (the sidecar's one build is the store's, as for
// NodeHistoryStamps).
func (bs *Store) nodeHistoryOrdered(id types.NodeID, t *storecontract.ReadTally) (bool, error) {
	if bs.historyPresenceProbeOnly {
		return false, nil
	}
	e, present, err := bs.historyStampEntry(&bs.histNodeStamps, storepkg.KeyHistNode, id.SnowflakeID(), t)
	return !present || e.ordered, err
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
	ordered, err := bs.nodeHistoryOrdered(id, t)
	if err != nil {
		return false, err
	}
	return ordered, bs.nodeHistoryNewestFirst(id, t, func(m storecontract.VersionTemporalMeta) bool { return visit(m, ordered) })
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
