package badger

import (
	"errors"
	"fmt"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Startup validates every canonical current row but retains only schema-sized
// definitions/counters. It does not build and then discard corpus-sized maps.
// Advanced indexes remain explicitly unsupported in this disk-only profile;
// silently opening them empty would change answers and is forbidden.
func (bs *Store) loadDiskIdentityIndexes() error {
	var definitions []propIdxDef
	var rawNodes, validNodes, rawRels, validRels int64
	var backfill bool
	writes := bs.db.NewWriteBatch()
	defer func() { writes.Cancel() }()
	queued := 0
	flush := func() error {
		if queued == 0 {
			return nil
		}
		if err := writes.Flush(); err != nil {
			return err
		}
		writes.Cancel()
		writes = bs.db.NewWriteBatch()
		queued = 0
		return nil
	}
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		for _, key := range [][]byte{storeutil.RelPropIndexDefsKey, storeutil.TemporalIndexDefsKey, storeutil.RelTypeTemporalIndexDefsKey, storeutil.HighFrequencyIndexDefsKey, storeutil.VectorIndexDefsKey, storeutil.CompositeIndexDefsKey} {
			item, err := txn.Get(key)
			if errors.Is(err, badgerv4.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			var entries []any
			if err := item.Value(func(raw []byte) error { return storeutil.SafeUnmarshal(raw, &entries) }); err != nil {
				return err
			}
			if len(entries) > 0 {
				return fmt.Errorf("%w: disk entity identity cannot open resident index definition %q", storecontract.ErrCapabilityNotSupported, key)
			}
		}
		if item, err := txn.Get(storeutil.PropIndexDefsKey); err == nil {
			if err := item.Value(func(raw []byte) error { return storeutil.SafeUnmarshal(raw, &definitions) }); err != nil {
				return err
			}
		} else if !errors.Is(err, badgerv4.ErrKeyNotFound) {
			return err
		}
		for _, definition := range definitions {
			if err := storecontract.ValidateLabelToken(definition.LabelToken); err != nil {
				return err
			}
			if err := storecontract.ValidateIndexPropertyKey(definition.PropertyKey); err != nil {
				return err
			}
			key := indexpkg.PropertyIndexKey{LabelToken: definition.LabelToken, PropertyKey: definition.PropertyKey}
			bs.propertyIndexes[key] = indexpkg.NewPropertyIndexWith(definition.RangeCounts)
		}
		_, markerErr := txn.Get(storeutil.PropertyIndexOnDiskBuiltKey)
		backfill = errors.Is(markerErr, badgerv4.ErrKeyNotFound)
		if markerErr != nil && !backfill {
			return markerErr
		}
		if backfill && len(definitions) > 0 && (bs.readOnly || bs.propKeyReg.Load() == nil) {
			return fmt.Errorf("%w: disk property index bootstrap requires a writable store and property-key registry", storecontract.ErrIndexNotFound)
		}
		for _, kind := range []byte{storeutil.KeyNode, storeutil.KeyRel} {
			options := badgerv4.DefaultIteratorOptions
			options.PrefetchValues = true
			it := txn.NewIterator(options)
			prefix := []byte{kind}
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				key := it.Item().Key()
				if len(key) != storeutil.SizeNodeKey {
					continue
				}
				id := storeutil.ParseIDFromKey(key, 1)
				if kind == storeutil.KeyNode {
					rawNodes++
				} else {
					rawRels++
				}
				err := it.Item().Value(func(raw []byte) error {
					if kind == storeutil.KeyRel {
						var wire storeutil.RelWire
						if err := storeutil.SafeUnmarshal(raw, &wire); err != nil {
							return err
						}
						r, err := bs.decodeRelWireForKey(wire, id)
						if err != nil {
							return err
						}
						validRels++
						bs.getOrCreateTypeCounter(uint16(r.TypeToken())).Add(1)
						return nil
					}
					var wire storeutil.NodeWire
					if err := storeutil.SafeUnmarshal(raw, &wire); err != nil {
						return err
					}
					n, err := bs.decodeNodeWireForKey(wire, id)
					if err != nil {
						return err
					}
					validNodes++
					for i := 0; i < n.LabelTokenCount(); i++ {
						bs.getOrCreateLabelCounter(n.LabelTokenRawAt(i)).Add(1)
					}
					if backfill {
						for _, definition := range definitions {
							if !n.HasLabelTokenRaw(definition.LabelToken) {
								continue
							}
							value, exists := n.IndexablePropertyValueKey(definition.PropertyKey)
							if !exists {
								continue
							}
							op, exists := bs.propertyIndexDiskOp(definition.PropertyKey, value, id, writeOpSet)
							if !exists {
								return fmt.Errorf("%w: missing property key %q", ErrInvalidStoreMutation, definition.PropertyKey)
							}
							if err := writes.Set(op.key, op.value); err != nil {
								return err
							}
							queued++
							if queued >= 1024 {
								if err := flush(); err != nil {
									return err
								}
							}
						}
					}
					return nil
				})
				if err != nil {
					it.Close()
					// Corrupt source rows and I/O errors are never turned into
					// absent identities in this profile.
					return fmt.Errorf("graph: disk identity row %d: %w", id, err)
				}
			}
			it.Close()
		}
		nodes, err := getCounter(txn, counterNodeCountKey)
		if err != nil {
			return err
		}
		nodes, err = reconcilePersistedCounter("node", nodes, validNodes, rawNodes, bs.logger)
		if err != nil {
			return err
		}
		rels, err := getCounter(txn, counterRelCountKey)
		if err != nil {
			return err
		}
		rels, err = reconcilePersistedCounter("relationship", rels, validRels, rawRels, bs.logger)
		if err != nil {
			return err
		}
		bs.nodeCount.Store(nodes)
		bs.relCount.Store(rels)
		lsn, err := seedLogLSN(txn)
		if err != nil {
			return err
		}
		if bs.logSeqSource != nil {
			bs.logSeqSource.Observe(lsn)
		} else {
			bs.logSeq.Store(lsn)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	if backfill && !bs.readOnly {
		return bs.db.Update(func(txn *badgerv4.Txn) error {
			return txn.Set(storeutil.PropertyIndexOnDiskBuiltKey, []byte{1})
		})
	}
	return nil
}
