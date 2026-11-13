package badger

import (
	"bytes"
	"fmt"
	"slices"

	badgerv4 "github.com/dgraph-io/badger/v4"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.NodePropertyKeysIDsCapability = (*Store)(nil)

// propDiskInexactPrefix starts the inexact markers: one per property key whose
// on-disk entries may hold keys no live definition wrote, left when a
// definition was dropped while another one still indexed the key. Present
// until the key's entries are purged or rebuilt; while it is, only a row
// recheck answers from those entries, so the ID door declines.
var propDiskInexactPrefix = storepkg.MetaKey("property_index_on_disk_inexact/")

func propDiskInexactKey(propertyKey string) []byte {
	return append(append([]byte(nil), propDiskInexactPrefix...), propertyKey...)
}

// NodeIDsByLabelAndPropertyKeys implements
// storecontract.NodePropertyKeysIDsCapability: NodesByLabelAndPropertyKeys'
// IDs from the index, without a row read (ELEMPROBE). It answers when the
// lookup names no time coordinate, the index is published, and its keys are
// exact for the definition: an index in RAM holds the definition's own keys;
// an on-disk scalar definition is not exact while an on-disk Elements
// definition shares its property key, whose element keys would match a list
// value the scalar definition does not. A candidate counts when the node
// carries the label (a deleted node carries none).
func (bs *Store) NodeIDsByLabelAndPropertyKeys(labelToken uint16, propKey string, valueKeys []string, opts QueryOpts) ([]types.NodeID, bool, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, false, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, false, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propKey); err != nil {
		return nil, false, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, false, err
	}
	if opts.HasTimeCoordinate() {
		return nil, false, nil
	}
	targetKeys := storepkg.DistinctIndexKeyMatch(valueKeys).Keys()

	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	idx, ok := bs.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propKey}]
	if !ok || idx.Mutated != nil {
		return nil, false, nil
	}
	if idx.OnDisk() && !idx.Elements() && bs.elementsOnDiskSharesLocked(propKey) {
		return nil, false, nil
	}
	if _, inexact := bs.propDiskInexact[propKey]; idx.OnDisk() && inexact {
		return nil, false, nil // a dropped definition's entries are among its keys
	}
	// Under a read budget each candidate is admitted as the walk produces it
	// (R2), before it is collected: a read past its Candidates limit stops at
	// the entry that passes it, not after the whole key was gathered.
	var admit func(int) error
	if m, _ := opts.ReadMeter(); m != nil {
		admit = opts.AddCandidates
	}
	var nids []types.NodeID
	if !idx.OnDisk() {
		for id := range idx.PostingsOfKeys(targetKeys).After(0) {
			if admit != nil {
				if err := admit(1); err != nil {
					return nil, false, err
				}
			}
			nids = append(nids, types.NodeID(id))
		}
	} else {
		var err error
		if nids, err = bs.propertyIndexDiskKeysCandidatesAdmitLocked(propKey, targetKeys, nil, admit); err != nil {
			return nil, false, err
		}
		storepkg.SortNodeIDs(nids)
		nids = slices.Compact(nids)
	}
	nids, err := bs.keepLabelledLocked(labelToken, nids)
	if err != nil {
		return nil, false, err
	}
	return storepkg.PaginateNodeIDs(nids, opts.After, opts.Limit), true, nil
}

// elementsOnDiskSharesLocked reports whether an on-disk Elements definition
// of any label indexes propKey. Caller holds idxMu.
func (bs *Store) elementsOnDiskSharesLocked(propKey string) bool {
	for k, idx := range bs.propertyIndexes {
		if k.PropertyKey == propKey && idx.OnDisk() && idx.Elements() {
			return true
		}
	}
	return false
}

// keepLabelledLocked keeps the sorted nids that carry the label: from
// labelIdx, or with the label index on disk from the pending writes and one
// iterator seeking each node's label key. Caller holds idxMu.
func (bs *Store) keepLabelledLocked(token uint16, nids []types.NodeID) ([]types.NodeID, error) {
	if !bs.labelOnDisk {
		set := bs.labelIdx[token]
		return slices.DeleteFunc(nids, func(nid types.NodeID) bool {
			_, ok := set[nid]
			return !ok
		}), nil
	}
	if len(nids) == 0 {
		return nids, nil
	}
	out := nids[:0]
	err := bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for _, nid := range nids {
			key := storepkg.LabelIndexKey(token, nid.SnowflakeID())
			if op, ok := bs.lookupPending(string(key)); ok {
				if op.opType == writeOpSet {
					out = append(out, nid)
				}
				continue
			}
			if it.Seek(key); it.Valid() && bytes.Equal(it.Item().Key(), key) {
				out = append(out, nid)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: label index membership: %w", err)
	}
	return out, nil
}
