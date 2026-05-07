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
	KeyNode     byte = 0x01 // + 8B nodeID                              =  9B
	KeyRel      byte = 0x02 // + 8B relID                               =  9B
	KeyLabel    byte = 0x03 // + 2B labelToken + 8B nodeID              = 11B
	KeyRelType  byte = 0x04 // + 2B relTypeToken + 8B relID             = 11B
	KeyOut      byte = 0x05 // + 8B start + 2B type + 8B end + 8B rel   = 27B
	KeyIn       byte = 0x06 // + 8B end + 2B type + 8B start + 8B rel   = 27B
	KeyHistNode byte = 0x07 // + 8B nodeID + 8B version              = 17B
	KeyHistRel  byte = 0x08 // + 8B relID + 8B version               = 17B
	KeyMeta     byte = 0x0F // + variable (rare, only registry keys)
)

// --- Key sizes ---

const (
	SizeNodeKey    = 1 + 8             // 9B
	SizeRelKey     = 1 + 8             // 9B
	SizeLabelIdx   = 1 + 2 + 8         // 11B
	SizeRelTypeIdx = 1 + 2 + 8         // 11B
	SizeAdjacency  = 1 + 8 + 2 + 8 + 8 // 27B
	SizeHistKey    = 1 + 8 + 8         // 17B
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

// TemporalIndexDefsKey is the Badger key for persisting temporal index label tokens.
var TemporalIndexDefsKey = MetaKey("temporal_index_defs")

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
