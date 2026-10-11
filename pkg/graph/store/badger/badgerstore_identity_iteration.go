package badger

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"
	"strings"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// currentKeyOverlay captures matching keys, with newer pending ops
// replacing flushing ops. Values are immutable encoded buffers. Capture before
// opening the disk snapshot: a completed flush cannot disappear from both.
func (bs *Store) currentKeyOverlay(prefix []byte, size int) map[string]writeOp {
	var overlay map[string]writeOp
	bs.rangePending(func(key string, op writeOp) {
		if len(key) != size || !strings.HasPrefix(key, string(prefix)) {
			return
		}
		if overlay == nil {
			overlay = make(map[string]writeOp)
		}
		overlay[key] = op
	})
	return overlay
}

// forEachCurrentRow merges bounded buffered keys with one ordered disk cursor.
// Callbacks run without index locks, so they may perform store reads or writes.
// They must not close/reset this store: maintenance must follow the iteration.
// The snapshot owns immutable values until its callback returns. An explicit
// unbounded slice API still allocates its requested output, not an extra ID set.
func (bs *Store) forEachCurrentRow(kind byte, after snowflake.ID, fn func(snowflake.ID, []byte) (bool, error)) error {
	return bs.forEachIdentityKey([]byte{kind}, storeutil.SizeNodeKey, 1, after, false, fn)
}

func (bs *Store) forEachIdentityKey(prefix []byte, size, idOffset int, after snowflake.ID, locked bool, fn func(snowflake.ID, []byte) (bool, error)) error {
	if err := bs.beginIdentityRead(); err != nil {
		return err
	}
	defer bs.identityReaders.Done()
	if !locked {
		bs.idxMu.RLock()
	}
	overlay := bs.currentKeyOverlay(prefix, size)
	txn, it, err := bs.newIdentityIterator()
	if !locked {
		bs.idxMu.RUnlock()
	}
	if err != nil {
		return err
	}
	defer txn.Discard()
	defer it.Close()
	keys := make([]string, 0, len(overlay))
	for key := range overlay {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	start := make([]byte, size)
	copy(start, prefix)
	binary.BigEndian.PutUint64(start[idOffset:], uint64(after)) // #nosec G115 -- validated positive ID or zero cursor
	it.Seek(start)
	at := 0
	emit := func(key string, raw []byte) (bool, error) {
		id := storeutil.ParseIDFromKey([]byte(key), idOffset)
		if id <= after {
			return true, nil
		}
		if err := bs.beginIdentityCallback(); err != nil {
			return false, err
		}
		defer bs.endIdentityCallback()
		return fn(id, raw)
	}
	for it.ValidForPrefix(prefix) || at < len(keys) {
		var diskKey []byte
		if it.ValidForPrefix(prefix) {
			diskKey = it.Item().Key()
			if len(diskKey) != size {
				it.Next()
				continue
			}
		}
		if at < len(keys) && (diskKey == nil || bytes.Compare([]byte(keys[at]), diskKey) <= 0) {
			key := keys[at]
			at++
			if bytes.Equal([]byte(key), diskKey) {
				it.Next()
			}
			op := overlay[key]
			if op.opType == writeOpDelete {
				continue
			}
			more, err := emit(key, op.value)
			if err != nil || !more {
				return err
			}
			continue
		}
		key := string(diskKey)
		// Item.Value holds a value-log read lock. Release it before a callback
		// can write/rotate the value log or reenter another store operation.
		raw, err := it.Item().ValueCopy(nil)
		if err != nil {
			return err
		}
		more, err := emit(key, raw)
		if err != nil || !more {
			return err
		}
		it.Next()
	}
	return nil
}

func (bs *Store) beginIdentityRead() error {
	bs.identityReadMu.Lock()
	defer bs.identityReadMu.Unlock()
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if bs.db.IsClosed() {
		return badgerv4.ErrDBClosed
	}
	if bs.identityResetting {
		return IdentityCallbackMaintenanceError{Operation: "iteration during Clear"}
	}
	bs.identityReaders.Add(1)
	return nil
}

func (bs *Store) beginIdentityCallback() error {
	bs.identityReadMu.Lock()
	defer bs.identityReadMu.Unlock()
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if bs.identityResetting {
		return IdentityCallbackMaintenanceError{Operation: "callback during Clear"}
	}
	bs.identityCallbacks++
	return nil
}

func (bs *Store) endIdentityCallback() {
	bs.identityReadMu.Lock()
	bs.identityCallbacks--
	bs.identityReadMu.Unlock()
}

func (bs *Store) beginIdentityReset() error {
	bs.identityReadMu.Lock()
	if bs.identityCallbacks > 0 || bs.identityResetting {
		bs.identityReadMu.Unlock()
		return IdentityCallbackMaintenanceError{Operation: "Clear"}
	}
	if err := bs.checkOpen(); err != nil {
		bs.identityReadMu.Unlock()
		return err
	}
	bs.identityResetting = true
	bs.identityReadMu.Unlock()
	bs.identityReaders.Wait()
	return nil
}

func (bs *Store) endIdentityReset() {
	bs.identityReadMu.Lock()
	bs.identityResetting = false
	bs.identityReadMu.Unlock()
}

// Badger's NewIterator panics if an externally closed DB crosses construction.
// Store.Close is excluded by the admitted reader lifetime; a direct DB close
// is still translated precisely. Unrelated panics are never swallowed.
func (bs *Store) newIdentityIterator() (txn *badgerv4.Txn, it *badgerv4.Iterator, err error) {
	defer func() {
		if cause := recover(); cause != nil {
			if failure, ok := cause.(error); ok && errors.Is(failure, badgerv4.ErrDBClosed) {
				if txn != nil {
					txn.Discard()
				}
				txn, it, err = nil, nil, failure
				return
			}
			panic(cause)
		}
	}()
	if bs.db.IsClosed() {
		return nil, nil, badgerv4.ErrDBClosed
	}
	txn = bs.db.NewTransaction(false)
	options := badgerv4.DefaultIteratorOptions
	options.PrefetchValues = false
	it = txn.NewIterator(options)
	return txn, it, nil
}

func (bs *Store) forEachCurrentNodeRow(after types.NodeID, fn func(*types.Node) (bool, error)) error {
	return bs.forEachCurrentRow(storeutil.KeyNode, after.SnowflakeID(), func(id snowflake.ID, raw []byte) (bool, error) {
		var wire storeutil.NodeWire
		if err := storeutil.SafeUnmarshal(raw, &wire); err != nil {
			return false, err
		}
		n, err := bs.decodeNodeWireForKey(wire, id)
		if err != nil {
			return false, err
		}
		n.Freeze()
		return fn(n)
	})
}

func (bs *Store) forEachCurrentRelRow(after types.RelID, fn func(*types.Relationship) (bool, error)) error {
	return bs.forEachCurrentRelRowLocked(after, false, fn)
}

func (bs *Store) forEachCurrentRelRowLocked(after types.RelID, locked bool, fn func(*types.Relationship) (bool, error)) error {
	return bs.forEachIdentityKey([]byte{storeutil.KeyRel}, storeutil.SizeRelKey, 1, after.SnowflakeID(), locked, func(id snowflake.ID, raw []byte) (bool, error) {
		var wire storeutil.RelWire
		if err := storeutil.SafeUnmarshal(raw, &wire); err != nil {
			return false, err
		}
		r, err := bs.decodeRelWireForKey(wire, id)
		if err != nil {
			return false, err
		}
		r.Freeze()
		return fn(r)
	})
}

func (bs *Store) forEachDiskTypeRel(token uint16, opts QueryOpts, fn func(*types.Relationship) (bool, error)) error {
	prefix := []byte{storeutil.KeyRelType, byte(token >> 8), byte(token)}
	return bs.forEachIdentityKey(prefix, storeutil.SizeRelTypeIdx, 3, types.RelID(opts.After).SnowflakeID(), false, func(id snowflake.ID, _ []byte) (bool, error) {
		r, err := bs.prefetchRelScan(types.RelID(id))
		if errors.Is(err, ErrRelNotFound) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if uint16(r.TypeToken()) != token || storeutil.HasTemporalFilter(opts) && !storeutil.MatchesTemporalFilter(id, r.Temporal(), opts) {
			return true, nil
		}
		return fn(r)
	})
}

func (bs *Store) forEachDiskLabelNode(token uint16, opts QueryOpts, fn func(*types.Node) (bool, error)) error {
	prefix := storeutil.LabelIndexKey(token, 0)[:3]
	return bs.forEachDiskNodeKey(prefix, opts, func(n *types.Node) bool { return n.HasLabelTokenRaw(token) }, fn)
}

func (bs *Store) forEachDiskNodeKey(prefix []byte, opts QueryOpts, predicate func(*types.Node) bool, fn func(*types.Node) (bool, error)) error {
	return bs.forEachIdentityKey(prefix, len(prefix)+8, len(prefix), types.NodeID(opts.After).SnowflakeID(), false, func(id snowflake.ID, _ []byte) (bool, error) {
		n, err := bs.prefetchNodeScan(types.NodeID(id))
		if errors.Is(err, ErrNodeNotFound) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if !predicate(n) || storeutil.HasTemporalFilter(opts) && !storeutil.MatchesTemporalFilter(id, n.Temporal(), opts) {
			return true, nil
		}
		return fn(n)
	})
}

func (bs *Store) diskNodesByProperty(token uint16, property, valueKey string, opts QueryOpts) ([]*types.Node, error) {
	predicate := func(n *types.Node) bool {
		value, exists := n.IndexablePropertyValueKey(property)
		return n.HasLabelTokenRaw(token) && exists && value == valueKey
	}
	bs.idxMu.RLock()
	index := bs.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: token, PropertyKey: property}]
	indexed := index != nil && index.Mutated == nil
	propertyToken, known := bs.propKeyTokenFor(property)
	bs.idxMu.RUnlock()
	prefix := storeutil.LabelIndexKey(token, 0)[:3]
	if indexed {
		payload, indexable := storeutil.PropertyIndexValueBytes(valueKey)
		if !known || !indexable {
			return nil, nil
		}
		prefix = storeutil.PropertyIndexValuePrefix(propertyToken, payload)
	}
	var out []*types.Node
	err := bs.forEachDiskNodeKey(prefix, opts, predicate, func(n *types.Node) (bool, error) {
		out = append(out, n)
		return opts.Limit == 0 || len(out) < opts.Limit, nil
	})
	return out, err
}
