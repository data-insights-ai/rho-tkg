package badger

import (
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Entry sizes at the write door (KEYSIZE, VALUESIZE, docs/badger.md section 2,
// "One entry and its limits"). badger refuses a key above 65,000 bytes and a
// value above ValueLogFileSize, in memory one above ValueThreshold
// (txn.go:352-371), but only when the flush commits: an entry it refuses was
// acknowledged, then failed every flush, and nothing written after it
// persisted. A staged flush adds 9 bytes to a key and 1 to a value
// (badgerstore_flush_staged.go). So every door checks the entries it will hand
// badger against the limits less that overhead, before any mutation.

// ErrEntryTooLarge is storecontract.ErrEntryTooLarge, refused at every write door.
var ErrEntryTooLarge = storecontract.ErrEntryTooLarge

const (
	badgerMaxKeyBytes    = 65_000 // txn.go:352
	stagedKeyOverhead    = 9      // KeyFlushStage + sequence
	stagedValueOverhead  = 1      // op byte
	changeRecordOverhead = 1      // change-log tag byte (EncodeChangeValue)
)

// entryLimits returns the largest key and value a door may hand badger.
func entryLimits(cfg Config) (maxKey, maxValue int) {
	def := badgerv4.DefaultOptions("")
	limit := def.ValueLogFileSize
	if cfg.ValueLogFileSize > 0 {
		limit = cfg.ValueLogFileSize
	}
	if cfg.InMemory {
		limit = def.ValueThreshold
	}
	return badgerMaxKeyBytes - stagedKeyOverhead, int(limit) - stagedValueOverhead
}

func (bs *Store) checkEntryValue(what string, n int) error {
	if n > bs.maxEntryValue {
		return fmt.Errorf("%w: %s of %d bytes, the store holds at most %d (ValueLogFileSize; in memory 1 MB)", ErrEntryTooLarge, what, n, bs.maxEntryValue)
	}
	return nil
}

func (bs *Store) checkChangePayload(payload []byte) error {
	return bs.checkEntryValue("change-log record", len(payload)+changeRecordOverhead)
}

// checkNodeIndexKeysLocked checks the on-disk property index keys a write of n
// would make (the only rho keys that carry a value, verbatim for strings,
// property_index_key.go). Caller holds idxMu. It interns no token: a token is
// always 2 bytes, so a placeholder gives the key's length.
func (bs *Store) checkNodeIndexKeysLocked(n *types.Node, id snowflake.ID) error {
	if !bs.propIdxOnDisk || len(bs.propertyIndexes) == 0 {
		return nil
	}
	var tooLarge error
	for i := 0; i < n.LabelTokenCount() && tooLarge == nil; i++ {
		label := n.LabelTokenRawAt(i)
		n.ForEachIndexablePropertyValueKey(func(propertyKey, valueKey string) bool {
			if _, ok := bs.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: label, PropertyKey: propertyKey}]; !ok {
				return true
			}
			payload, ok := storepkg.PropertyIndexValueBytes(valueKey)
			if !ok {
				return true
			}
			if l := len(storepkg.PropertyIndexEntryKey(0, payload, id)); l > bs.maxEntryKey {
				tooLarge = fmt.Errorf("%w: property index key for %q of %d bytes, the store holds at most %d", ErrEntryTooLarge, propertyKey, l, bs.maxEntryKey)
				return false
			}
			return true
		})
	}
	return tooLarge
}

// cascadeCheckBelow: a cascade record can reach a store limit below this only
// (about 9 bytes per cascaded relationship), so above it the check is skipped.
const cascadeCheckBelow = 64 << 20

// checkCascadeRecordLocked bounds the ChangeNodeDelete record of a cascade
// delete of nid: its ID plus one ID (at most 9 bytes in msgpack) per
// relationship. Caller holds idxMu.Lock, so the adjacency is the set the
// cascade deletes.
func (bs *Store) checkCascadeRecordLocked(nid types.NodeID) error {
	if !bs.logEnabled.Load() || bs.maxEntryValue >= cascadeCheckBelow {
		return nil
	}
	out, err := bs.adjacentRelIDsSnapshotLocked(nid, 0, false)
	if err != nil {
		return err
	}
	in, err := bs.adjacentRelIDsSnapshotLocked(nid, 0, true)
	if err != nil {
		return err
	}
	return bs.checkEntryValue("cascade delete record", 64+9*(len(out)+len(in))+changeRecordOverhead)
}

// checkedPayload passes a built change-log payload through checkChangePayload.
func (bs *Store) checkedPayload(payload []byte, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	return payload, bs.checkChangePayload(payload)
}
