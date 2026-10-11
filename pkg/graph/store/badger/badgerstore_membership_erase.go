package badger

import (
	"bytes"
	"encoding/binary"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Resolve auxiliary cleanup before destroying any source row. Per-ID reverse
// prefixes bind each referenced posting to the requested kind and identity.
func (bs *Store) historicalMembershipEraseOps(node bool, id types.EntityID) ([]writeOp, error) {
	if !bs.historicalMembershipOnDisk {
		return nil, nil
	}
	kinds := []byte{diskMemberRelType, diskMemberRelProperty}
	if node {
		kinds = []byte{diskMemberNodeLabel, diskMemberNodeProperty}
	}
	seen := map[string]writeOp{}
	for _, reversePrefix := range []byte{diskMembershipReversePrefix, diskMembershipCompactReversePrefix} {
		for _, kind := range kinds {
			prefix := make([]byte, 10)
			prefix[0], prefix[1] = reversePrefix, kind
			binary.BigEndian.PutUint64(prefix[2:], uint64(id)) // #nosec G115 — positive erasure identity validated by its mutation door
			overlay := map[string]writeOp{}
			bs.rangePending(func(key string, op writeOp) {
				if bytes.HasPrefix([]byte(key), prefix) {
					overlay[key] = op
				}
			})
			visit := func(key, value []byte) error {
				var forward []byte
				var err error
				if reversePrefix == diskMembershipCompactReversePrefix {
					forward, err = compactReverseMembershipForward(key, value, kind, id)
				} else {
					forward, err = legacyReverseMembershipForward(key, value, kind, id)
				}
				if err != nil {
					return err
				}
				seen[string(key)] = writeOp{opType: writeOpDelete, key: bytes.Clone(key)}
				seen[string(forward)] = writeOp{opType: writeOpDelete, key: forward}
				return nil
			}
			if err := bs.db.View(func(txn *badgerv4.Txn) error {
				opts := badgerv4.DefaultIteratorOptions
				opts.PrefetchValues = true
				opts.Prefix = prefix
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
					key := it.Item().KeyCopy(nil)
					if _, ok := overlay[string(key)]; ok {
						continue
					}
					value, err := it.Item().ValueCopy(nil)
					if err != nil {
						return err
					}
					if err := visit(key, value); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return nil, err
			}
			for key, op := range overlay {
				if op.opType != writeOpDelete {
					if err := visit([]byte(key), op.value); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	ops := make([]writeOp, 0, len(seen))
	for _, op := range seen {
		ops = append(ops, op)
	}
	return ops, nil
}
