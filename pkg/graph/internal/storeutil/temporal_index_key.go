package storeutil

import (
	"encoding/binary"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// KeyTemporalIndex is the disk-resident RAW INTERVAL ENTRY LOG backing the opt-in
// badger.Config.TemporalIndexOnDisk rebuild accelerator.
//
// Unlike the KeyPropertyIndex / label / adjacency on-disk modes (which move
// an in-memory index OFF the RAM heap and answer LIVE reads from the
// persisted keyspace instead), the maxTo-augmented TemporalIndex
// (pkg/graph/internal/index/temporal_index.go) always stays fully resident
// in RAM at runtime — its stabbing/overlap queries walk an implicit balanced
// BST augmentation (subMax) that has no on-disk analogue, so there is no
// "answer reads from disk" mode for it. TemporalIndexOnDisk instead targets
// the ONE-TIME rebuild-at-open cost: reconstructing a label's TemporalIndex
// on open previously required a full node fetch+decode (a Badger point-get
// plus msgpack decode of the ENTIRE row, including arbitrarily large
// properties) for every node carrying that label, just to extract two int64
// fields (from, to). This keyspace instead persists exactly those two
// fields plus the entity ID, written transactionally alongside the node row
// whenever a node carrying an actively-temporal-indexed label is
// created/updated/deleted, so a rebuild becomes one compact prefix
// iteration per label instead of N random point reads with full-row decode.
//
// Layout: KeyTemporalIndex(1B) | labelToken(2B BE) |
// orderPreservingInt64(from)(8B BE) | nodeID(8B BE) = 19B.
// Value: raw big-endian TO instant (8B) — never used as a sort/comparison
// key, only read back, so no order-preserving encoding is needed for it.
//
// The FROM component is order-preserving-encoded (sign-bit-flipped two's
// complement — the standard trick, mirrored from
// orderPreservingFloat64Bits's float variant in property_index_key.go) so a
// plain prefix iteration over one label's sub-keyspace visits entries in
// EXACTLY the (From ASC, ID ASC) order TemporalIndex.Entries requires:
// loadIndexesScan streams straight from iteration order into
// TemporalIndex.AddKnownAbsent, no separate sort or augmentation-build pass
// beyond the existing lazy sortIfDirty on first query.
const KeyTemporalIndex byte = 0x0B

// SizeTemporalIndexEntryKey is the fixed total key length of one entry:
// KeyTemporalIndex(1) + labelToken(2) + from(8) + nodeID(8).
const SizeTemporalIndexEntryKey = 1 + 2 + 8 + 8

// orderPreservingInt64Bits maps an int64 to a uint64 whose UNSIGNED ordering
// matches the int64's natural ordering: flipping the sign bit moves the
// negative half (whose two's-complement pattern is numerically larger as an
// unsigned value) below the non-negative half, exactly mirroring
// orderPreservingFloat64Bits's IEEE-754 sign-flip for floats.
func orderPreservingInt64Bits(v int64) uint64 {
	return uint64(v) ^ (1 << 63)
}

// orderPreservingInt64Value reverses orderPreservingInt64Bits.
func orderPreservingInt64Value(bits uint64) int64 {
	return int64(bits ^ (1 << 63)) // #nosec G115 -- reversing the sign-flip round-trips exactly
}

// TemporalIndexEntryKey builds the full on-disk key for one
// (labelToken, from, nodeID) entry.
func TemporalIndexEntryKey(labelToken uint16, from types.Instant, id snowflake.ID) []byte {
	b := make([]byte, SizeTemporalIndexEntryKey)
	b[0] = KeyTemporalIndex
	PutUint16(b, 1, labelToken)
	binary.BigEndian.PutUint64(b[3:11], orderPreservingInt64Bits(int64(from)))
	PutUint64(b, 11, int64(id))
	return b
}

// TemporalIndexEntryValue encodes the TO instant as the entry's value bytes.
func TemporalIndexEntryValue(to types.Instant) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(to)) // #nosec G115 -- intentional int64->uint64 for binary encoding
	return b
}

// TemporalIndexEntryValueDecode decodes a value written by
// TemporalIndexEntryValue. Defensively returns 0 for a malformed (too-short)
// value rather than panicking — callers only reach this via a trusted
// internal keyspace, but a corrupt/truncated row must fail soft, not crash.
func TemporalIndexEntryValueDecode(val []byte) types.Instant {
	if len(val) < 8 {
		return 0
	}
	return types.Instant(binary.BigEndian.Uint64(val)) // #nosec G115 -- reverses TemporalIndexEntryValue's cast
}

// TemporalIndexTokenPrefix returns the 3-byte iteration prefix for every
// entry indexed under labelToken, in (From ASC, ID ASC) order.
func TemporalIndexTokenPrefix(labelToken uint16) []byte {
	b := make([]byte, 1+2)
	b[0] = KeyTemporalIndex
	PutUint16(b, 1, labelToken)
	return b
}

// TemporalIndexNodeIDFromKey extracts the trailing 8-byte node ID from a
// well-formed temporal-index entry key.
func TemporalIndexNodeIDFromKey(key []byte) snowflake.ID {
	return ParseIDFromKey(key, len(key)-8)
}

// TemporalIndexFromFromKey decodes the FROM instant embedded in a
// well-formed temporal-index entry key (bytes [3:11]).
func TemporalIndexFromFromKey(key []byte) types.Instant {
	return types.Instant(orderPreservingInt64Value(binary.BigEndian.Uint64(key[3:11])))
}
