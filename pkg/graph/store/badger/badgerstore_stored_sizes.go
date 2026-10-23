package badger

import (
	"errors"
	"fmt"
	"sort"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

var _ storecontract.NodeStoredSizeCapability = (*Store)(nil)

// NodeStoredSizes is store.NodeStoredSizeCapability: the current row's stored
// length (the write buffer's value when one is pending) and every history
// row's, merged with the write buffer as the history reads merge it.
func (bs *Store) NodeStoredSizes(nid types.NodeID) ([]storecontract.StoredRow, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	id := nid.SnowflakeID()
	var out []storecontract.StoredRow
	cur, err := bs.LendNode(nid)
	switch {
	case errors.Is(err, ErrNodeNotFound):
	case err != nil:
		return nil, err
	default:
		key := storepkg.NodeKey(id)
		size := 0
		if op, found := bs.lookupPending(string(key)); found && op.opType != writeOpDelete {
			size = len(op.value)
		} else if err := bs.view(func(txn *badgerv4.Txn) error {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			size = int(item.ValueSize())
			return nil
		}); err != nil {
			return nil, fmt.Errorf("graph: stored size of node: %w", err)
		}
		out = append(out, storecontract.StoredRow{Version: cur.Version(), Current: true, Bytes: size})
	}

	prefix := storepkg.HistNodePrefix(id)
	overlay, overlayDeletes := bs.pendingHistoryVersionOverlay(prefix, 0) // before the View, as the history reads
	rowOf := func(key, raw []byte) (storecontract.StoredRow, error) {
		m, err := historyTemporalMetaOf(raw, true)
		if err != nil {
			return storecontract.StoredRow{}, err
		}
		return storecontract.StoredRow{
			Version: uint32(historyVersionFromKey(key)), // #nosec G115 -- versions are uint32 in the API
			Bytes:   len(raw),
			Delta:   storepkg.HistoryValueKindOf(raw) == storepkg.HistoryDelta,
			Void:    m.Temporal != nil && m.Temporal.Void,
		}, nil
	}
	byKey := map[string]storecontract.StoredRow{}
	if err := bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) != storepkg.SizeHistKey {
				continue
			}
			if _, deleted := overlayDeletes[string(key)]; deleted {
				continue
			}
			if _, pending := overlay[string(key)]; pending {
				continue
			}
			if err := it.Item().Value(func(val []byte) error {
				r, err := rowOf(key, val)
				byKey[string(key)] = r
				return err
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("graph: stored sizes of node history: %w", err)
	}
	for k, raw := range overlay {
		r, err := rowOf([]byte(k), raw)
		if err != nil {
			return nil, fmt.Errorf("graph: stored size of pending history: %w", err)
		}
		byKey[k] = r
	}
	hist := make([]storecontract.StoredRow, 0, len(byKey))
	for _, r := range byKey {
		hist = append(hist, r)
	}
	sort.Slice(hist, func(i, j int) bool { return hist[i].Version < hist[j].Version })
	return append(out, hist...), nil
}
