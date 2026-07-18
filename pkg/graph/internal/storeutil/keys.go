// Package storeutil hosts shared helpers used by the Store backend
// implementations: the binary key encoding for the Badger backend, the
// msgpack wire format, pagination and temporal-filter helpers. The public
// Store contract (interface, QueryOpts, sentinel errors) lives in
// pkg/graph/store; this package only contains backend-internal helpers.
package storeutil

import (
	"encoding/binary"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// Chokepoint invariant for raw snowflake.ID: this file (binary key encoding)
// is one of a small set of files that legitimately consume raw snowflake.ID
// values. The rest of pkg/graph flows typed (NodeID / RelID / EntityID); the
// raw form is needed here only because keys are big-endian uint64 byte slices
// and the snowflake bits are the canonical sort order on disk. Other Tier D
// raw-ID files: wire.go (msgpack on-disk format), lru.go (type-agnostic
// cache), entity_locks.go (type-agnostic lock pool).
//
// Key prefix tags — single-byte, non-overlapping, fixed-width keys.
// All snowflake IDs are stored as big-endian uint64 (cast from int64) for correct
// sort order. Tokens are stored as big-endian uint16.
const (
	KeyNode      byte = 0x01 // + 8B nodeID                              =  9B
	KeyRel       byte = 0x02 // + 8B relID                               =  9B
	KeyLabel     byte = 0x03 // + 2B labelToken + 8B nodeID              = 11B
	KeyRelType   byte = 0x04 // + 2B relTypeToken + 8B relID             = 11B
	KeyOut       byte = 0x05 // + 8B start + 2B type + 8B end + 8B rel   = 27B
	KeyIn        byte = 0x06 // + 8B end + 2B type + 8B start + 8B rel   = 27B
	KeyHistNode  byte = 0x07 // + 8B nodeID + 8B version              = 17B
	KeyHistRel   byte = 0x08 // + 8B relID + 8B version               = 17B
	KeyChangeLog byte = 0x09 // + 8B LSN                               =  9B
	KeyMeta      byte = 0x0F // + variable (rare, only registry keys)
)

// Key size constants — byte counts for the Badger key layout.
const (
	SizeNodeKey    = 1 + 8             // 9B
	SizeRelKey     = 1 + 8             // 9B
	SizeLabelIdx   = 1 + 2 + 8         // 11B
	SizeRelTypeIdx = 1 + 2 + 8         // 11B
	SizeAdjacency  = 1 + 8 + 2 + 8 + 8 // 27B
	SizeHistKey    = 1 + 8 + 8         // 17B
	SizeChangeLog  = 1 + 8             // 9B
)

// PutUint64 writes v as big-endian at buf[off..off+8].
// Snowflake IDs are int64 but stored as uint64 for correct big-endian sort order.
func PutUint64(buf []byte, off int, v int64) {
	binary.BigEndian.PutUint64(buf[off:], uint64(v)) // #nosec G115 — intentional int64→uint64 for binary key encoding
}

// PutUint16 writes v as big-endian at buf[off..off+2].
func PutUint16(buf []byte, off int, v uint16) {
	binary.BigEndian.PutUint16(buf[off:], v)
}

// --- Entity keys ---

// NodeKey returns the 9-byte key for a node entity.
func NodeKey(id snowflake.ID) []byte {
	b := make([]byte, SizeNodeKey)
	b[0] = KeyNode
	PutUint64(b, 1, int64(id))
	return b
}

// RelKey returns the 9-byte key for a relationship entity.
func RelKey(id snowflake.ID) []byte {
	b := make([]byte, SizeRelKey)
	b[0] = KeyRel
	PutUint64(b, 1, int64(id))
	return b
}

// --- Label index keys ---

// LabelIndexKey returns the 11-byte key for a label→node index entry.
func LabelIndexKey(token uint16, nodeID snowflake.ID) []byte {
	b := make([]byte, SizeLabelIdx)
	b[0] = KeyLabel
	PutUint16(b, 1, token)
	PutUint64(b, 3, int64(nodeID))
	return b
}

// --- RelType index keys ---

// RelTypeIndexKey returns the 11-byte key for a relType→rel index entry.
func RelTypeIndexKey(token uint16, relID snowflake.ID) []byte {
	b := make([]byte, SizeRelTypeIdx)
	b[0] = KeyRelType
	PutUint16(b, 1, token)
	PutUint64(b, 3, int64(relID))
	return b
}

// --- Adjacency keys ---

// OutKey returns the 27-byte key for an outgoing adjacency entry.
func OutKey(startID snowflake.ID, relType uint16, endID snowflake.ID, relID snowflake.ID) []byte {
	b := make([]byte, SizeAdjacency)
	b[0] = KeyOut
	PutUint64(b, 1, int64(startID))
	PutUint16(b, 9, relType)
	PutUint64(b, 11, int64(endID))
	PutUint64(b, 19, int64(relID))
	return b
}

// InKey returns the 27-byte key for an incoming adjacency entry.
func InKey(endID snowflake.ID, relType uint16, startID snowflake.ID, relID snowflake.ID) []byte {
	b := make([]byte, SizeAdjacency)
	b[0] = KeyIn
	PutUint64(b, 1, int64(endID))
	PutUint16(b, 9, relType)
	PutUint64(b, 11, int64(startID))
	PutUint64(b, 19, int64(relID))
	return b
}

// --- History keys ---

// HistNodeKey returns the 17-byte key for a node history entry.
func HistNodeKey(nodeID snowflake.ID, version uint64) []byte {
	b := make([]byte, SizeHistKey)
	b[0] = KeyHistNode
	PutUint64(b, 1, int64(nodeID))
	binary.BigEndian.PutUint64(b[9:], version)
	return b
}

// HistRelKey returns the 17-byte key for a relationship history entry.
func HistRelKey(relID snowflake.ID, version uint64) []byte {
	b := make([]byte, SizeHistKey)
	b[0] = KeyHistRel
	PutUint64(b, 1, int64(relID))
	binary.BigEndian.PutUint64(b[9:], version)
	return b
}

// HistNodePrefix returns the 9-byte prefix for scanning all node version entries.
func HistNodePrefix(nodeID snowflake.ID) []byte {
	b := make([]byte, 1+8)
	b[0] = KeyHistNode
	PutUint64(b, 1, int64(nodeID))
	return b
}

// HistRelPrefix returns the 9-byte prefix for scanning all relationship version entries.
func HistRelPrefix(relID snowflake.ID) []byte {
	b := make([]byte, 1+8)
	b[0] = KeyHistRel
	PutUint64(b, 1, int64(relID))
	return b
}

// --- Change-log keys ---

// ChangeLogKey returns the 9-byte key for a change-log record at the given LSN.
// The LSN is the monotonic cluster commit sequence; big-endian encoding makes a
// prefix scan over KeyChangeLog yield records in ascending LSN (commit) order.
func ChangeLogKey(lsn uint64) []byte {
	b := make([]byte, SizeChangeLog)
	b[0] = KeyChangeLog
	binary.BigEndian.PutUint64(b[1:], lsn)
	return b
}

// ChangeLogPrefix returns the 1-byte prefix for scanning all change-log records.
func ChangeLogPrefix() []byte {
	return []byte{KeyChangeLog}
}

// ChangeLogLSNFromKey extracts the LSN from a change-log key. Returns
// (0, false) when the key is not a well-formed KeyChangeLog key — callers must
// check the bool before trusting the LSN.
func ChangeLogLSNFromKey(key []byte) (uint64, bool) {
	if len(key) != SizeChangeLog || key[0] != KeyChangeLog {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[1:]), true
}

// --- Meta keys ---

// MetaKey returns a variable-length key for metadata entries.
func MetaKey(name string) []byte {
	b := make([]byte, 1+len(name))
	b[0] = KeyMeta
	copy(b[1:], name)
	return b
}

// PropIndexDefsKey is the Badger key for persisting property index definitions.
var PropIndexDefsKey = MetaKey("prop_indexes")

// RelPropIndexDefsKey is the Badger key for persisting relationship property
// index definitions (K3b). Only the definitions are persisted; the RAM value
// maps are rebuilt from current relationship state at open, mirroring the
// node property index's non-disk rebuild path.
var RelPropIndexDefsKey = MetaKey("rel_prop_indexes")

// TemporalIndexDefsKey is the Badger key for persisting temporal index label tokens.
var TemporalIndexDefsKey = MetaKey("temporal_index_defs")

// HighFrequencyIndexDefsKey is the Badger key for persisting high-frequency temporal index definitions.
var HighFrequencyIndexDefsKey = MetaKey("high_frequency_index_defs")

// VectorIndexDefsKey is the Badger key for persisting vector index definitions.
var VectorIndexDefsKey = MetaKey("vector_index_defs")

// CompositeIndexDefsKey is the Badger key for persisting composite property
// index definitions (label token + declared ordered key list). Entries are
// always RAM-only and rebuilt from current node state on reopen — there is
// no on-disk composite-index keyspace equivalent to PropertyIndexOnDisk (v1
// scope; see docs/query-planners.md "Composite property indexes").
var CompositeIndexDefsKey = MetaKey("composite_index_defs")

// WireFormatVersionKey is the Badger key for the store-level on-disk format
// marker: a single big-endian uint16 holding the highest wire format version
// any row in this store may carry. Stamped at open when absent; a marker
// greater than CurrentWireFormatVersion makes open fail closed with
// store.ErrWireFormatVersionUnsupported. Absence means "pre-versioning
// store" and is equivalent to version 1.
var WireFormatVersionKey = MetaKey("wire_format_version")

// HistoryAnchorIntervalKey is the Badger key for the persisted anchor interval used
// by HistoryDeltaEncoding. The interval is baked into the on-disk delta layout, so a
// mismatch between the stored marker and the configured interval fails closed at open
// (a delta reconstructed against the wrong anchor is a silent misread).
var HistoryAnchorIntervalKey = MetaKey("history_anchor_interval")

// PropertyIndexOnDiskBuiltKey marks that the persisted 0x0A property-index
// keyspace has been backfilled from current node state at least once (a
// single byte, value irrelevant — presence is the signal). Absent means a
// fresh store, or an existing store having PropertyIndexOnDisk turned on for
// the first time: the open path must scan current node state for every
// existing property-index definition and write the corresponding disk rows
// before serving reads, exactly once (mirrors the wire_format_version marker
// pattern — stamped after a successful backfill so subsequent opens with the
// flag still on skip the rescan and trust the keyspace ongoing maintenance
// already kept in sync).
var PropertyIndexOnDiskBuiltKey = MetaKey("property_index_on_disk_built")

// TemporalIndexOnDiskBuiltKey marks that the persisted 0x0B temporal-index
// raw-entry keyspace has been backfilled from current node state at least
// once (a single byte, value irrelevant — presence is the signal). Absent
// means a fresh store, or an existing store having TemporalIndexOnDisk
// turned on for the first time: the open path must fall back to the
// (slower) full-node-fetch rebuild for every existing temporal index
// definition, collecting the corresponding 0x0B rows as it goes, and commit
// them plus this marker in one WriteBatch — mirroring the
// PropertyIndexOnDiskBuiltKey pattern exactly. Subsequent opens with the
// flag still on trust the keyspace (kept in sync by ongoing write-path
// maintenance) and stream straight from it instead.
var TemporalIndexOnDiskBuiltKey = MetaKey("temporal_index_on_disk_built")

// LastLSNKey is the Badger key for the durable change-log watermark: a single
// big-endian uint64 holding the highest LSN committed to the change-log. It is
// written in the SAME WriteBatch as the change-log records it covers, so after
// a crash the marker and the maximum KeyChangeLog key are always consistent.
// Absent (or zero) means an empty change-log; the LSN allocator seeds from 0.
var LastLSNKey = MetaKey("last_lsn")

// --- Parser functions ---

// ParseIDFromKey extracts the 8-byte big-endian snowflake.ID at the given offset.
// The uint64→int64 cast reverses the encoding in PutUint64.
func ParseIDFromKey(key []byte, offset int) snowflake.ID {
	return snowflake.ID(binary.BigEndian.Uint64(key[offset:])) // #nosec G115 — inverse of PutUint64
}

// ParseRelIDFromAdjKey extracts the relationship ID from the last 8 bytes of
// a 27-byte adjacency key (OutKey or InKey).
func ParseRelIDFromAdjKey(key []byte) snowflake.ID {
	return snowflake.ID(binary.BigEndian.Uint64(key[19:])) // #nosec G115 — inverse of PutUint64
}
