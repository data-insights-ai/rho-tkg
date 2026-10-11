package badger

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// ForEachNodeHistoricalCandidate implements ordered historical candidates.
func (bs *Store) ForEachNodeHistoricalCandidate(token uint16, property, value string, after types.NodeID, fn func(types.NodeID, types.Instant) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	if err := storecontract.ValidateLabelToken(token); err != nil {
		return err
	}
	if after < 0 {
		return storecontract.ErrInvalidQueryCursor
	}
	if !bs.historicalMembershipOnDisk {
		return ErrIndexNotFound
	}
	kind := diskMemberNodeLabel
	if property != "" {
		if err := storecontract.ValidateIndexPropertyKey(property); err != nil {
			return err
		}
		bs.idxMu.RLock()
		usable := bs.nodePropIndexUsableLocked(indexpkg.PropertyIndexKey{LabelToken: token, PropertyKey: property})
		bs.idxMu.RUnlock()
		if !usable {
			return ErrIndexNotFound
		}
		kind = diskMemberNodeProperty
	}
	return bs.forEachDiskMembership(diskMembershipScope(kind, token, property, value), types.EntityID(after), func(id types.EntityID, first types.Instant) bool { return fn(types.NodeID(id), first) })
}

// ForEachRelHistoricalCandidate is the relationship mirror.
func (bs *Store) ForEachRelHistoricalCandidate(token uint16, property, value string, after types.RelID, fn func(types.RelID, types.Instant) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	if err := storecontract.ValidateRelTypeToken(token); err != nil {
		return err
	}
	if after < 0 {
		return storecontract.ErrInvalidQueryCursor
	}
	if !bs.historicalMembershipOnDisk {
		return ErrIndexNotFound
	}
	kind := diskMemberRelType
	if property != "" {
		if err := storecontract.ValidateIndexPropertyKey(property); err != nil {
			return err
		}
		bs.idxMu.RLock()
		usable := bs.relPropIndexUsableLocked(indexpkg.RelPropertyIndexKey{RelTypeToken: token, PropertyKey: property})
		bs.idxMu.RUnlock()
		if !usable {
			return ErrIndexNotFound
		}
		kind = diskMemberRelProperty
	}
	return bs.forEachDiskMembership(diskMembershipScope(kind, token, property, value), types.EntityID(after), func(id types.EntityID, first types.Instant) bool { return fn(types.RelID(id), first) })
}

// The pending overlay is captured before the committed snapshot. Each row's
// smallest acquisition stamp sorts first, so one candidate does not require
// materializing all values or every history stamp of that entity.
func (bs *Store) forEachDiskMembership(scope []byte, after types.EntityID, fn func(types.EntityID, types.Instant) bool) error {
	if !bs.historicalMembershipReady.Load() {
		return ErrIndexNotFound
	}
	overlay := map[string]writeOp{}
	bs.rangePending(func(key string, op writeOp) {
		if bytes.HasPrefix([]byte(key), scope) {
			overlay[key] = op
		}
	})
	keys := make([]string, 0, len(overlay))
	for key := range overlay {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	seek := make([]byte, len(scope)+16)
	copy(seek, scope)
	if after >= 0 {
		binary.BigEndian.PutUint64(seek[len(scope):], uint64(after))
	}
	at := 0
	return bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = scope
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(seek)
		last := after
		visit := func(key, value []byte) (bool, error) {
			id, stamp, err := diskMembershipRecord(key, scope)
			if err != nil {
				return false, err
			}
			if len(value) != 1 || value[0] != 1 {
				return false, fmt.Errorf("graph: corrupt historical membership value")
			}
			if id <= last {
				return true, nil
			}
			last = id
			return fn(id, stamp), nil
		}
		jump := func() {
			if last <= after {
				return
			}
			if last == types.EntityID(math.MaxInt64) {
				for it.ValidForPrefix(scope) {
					it.Next()
				}
				at = len(keys)
				return
			}
			binary.BigEndian.PutUint64(seek[len(scope):], uint64(last+1)) // #nosec G115 — positive candidate ID, MaxInt64 handled above
			clear(seek[len(scope)+8:])
			it.Seek(seek)
			at, _ = slices.BinarySearch(keys, string(seek))
		}
		for it.ValidForPrefix(scope) || at < len(keys) {
			var diskKey []byte
			if it.ValidForPrefix(scope) {
				diskKey = it.Item().Key()
			}
			if at < len(keys) && (diskKey == nil || bytes.Compare([]byte(keys[at]), diskKey) <= 0) {
				key := keys[at]
				op := overlay[key]
				at++
				if diskKey != nil && bytes.Equal([]byte(key), diskKey) {
					it.Next()
				}
				if op.opType == writeOpDelete {
					continue
				}
				keep, err := visit([]byte(key), op.value)
				if err != nil || !keep {
					return err
				}
				jump()
				continue
			}
			value, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			key := bytes.Clone(diskKey)
			it.Next()
			keep, err := visit(key, value)
			if err != nil || !keep {
				return err
			}
			jump()
		}
		return nil
	})
}
