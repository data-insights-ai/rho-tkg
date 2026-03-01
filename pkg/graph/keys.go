package graph

import "encoding/binary"

// Key prefix tags — single-byte, non-overlapping, fixed-width keys.
// All snowflake IDs are stored as big-endian uint64 (cast from int64) for correct
// sort order. Tokens are stored as big-endian uint16.
const (
	keyNode    byte = 0x01 // + 8B nodeID                              =  9B
	keyRel     byte = 0x02 // + 8B relID                               =  9B
	keyLabel   byte = 0x03 // + 2B labelToken + 8B nodeID              = 11B
	keyRelType byte = 0x04 // + 2B relTypeToken + 8B relID             = 11B
	keyOut     byte = 0x05 // + 8B start + 2B type + 8B end + 8B rel   = 27B
	keyIn      byte = 0x06 // + 8B end + 2B type + 8B start + 8B rel   = 27B
	keyHistNode byte = 0x07 // + 8B nodeID + 8B version              = 17B
	keyHistRel  byte = 0x08 // + 8B relID + 8B version               = 17B
	keyMeta     byte = 0x0F // + variable (rare, only registry keys)
)

// --- Key sizes ---

const (
	sizeNodeKey    = 1 + 8             // 9B
	sizeRelKey     = 1 + 8             // 9B
	sizeLabelIdx   = 1 + 2 + 8         // 11B
	sizeRelTypeIdx = 1 + 2 + 8         // 11B
	sizeAdjacency  = 1 + 8 + 2 + 8 + 8 // 27B
	sizeHistKey    = 1 + 8 + 8         // 17B
)

// putUint64 writes v as big-endian at buf[off..off+8].
// Snowflake IDs are int64 but stored as uint64 for correct big-endian sort order.
func putUint64(buf []byte, off int, v int64) {
	binary.BigEndian.PutUint64(buf[off:], uint64(v)) // #nosec G115 — intentional int64→uint64 for binary key encoding
}

// putUint16 writes v as big-endian at buf[off..off+2].
func putUint16(buf []byte, off int, v uint16) {
	binary.BigEndian.PutUint16(buf[off:], v)
}

// --- Entity keys ---

// nodeKey returns the 9-byte key for a node entity.
func nodeKey(id int64) []byte {
	b := make([]byte, sizeNodeKey)
	b[0] = keyNode
	putUint64(b, 1, id)
	return b
}

// relKey returns the 9-byte key for a relationship entity.
func relKey(id int64) []byte {
	b := make([]byte, sizeRelKey)
	b[0] = keyRel
	putUint64(b, 1, id)
	return b
}

// --- Label index keys ---

// labelIndexKey returns the 11-byte key for a label→node index entry.
func labelIndexKey(token uint16, nodeID int64) []byte {
	b := make([]byte, sizeLabelIdx)
	b[0] = keyLabel
	putUint16(b, 1, token)
	putUint64(b, 3, nodeID)
	return b
}

// --- RelType index keys ---

// relTypeIndexKey returns the 11-byte key for a relType→rel index entry.
func relTypeIndexKey(token uint16, relID int64) []byte {
	b := make([]byte, sizeRelTypeIdx)
	b[0] = keyRelType
	putUint16(b, 1, token)
	putUint64(b, 3, relID)
	return b
}

// --- Adjacency keys ---

// outKey returns the 27-byte key for an outgoing adjacency entry.
func outKey(startID int64, relType uint16, endID int64, relID int64) []byte {
	b := make([]byte, sizeAdjacency)
	b[0] = keyOut
	putUint64(b, 1, startID)
	putUint16(b, 9, relType)
	putUint64(b, 11, endID)
	putUint64(b, 19, relID)
	return b
}

// inKey returns the 27-byte key for an incoming adjacency entry.
func inKey(endID int64, relType uint16, startID int64, relID int64) []byte {
	b := make([]byte, sizeAdjacency)
	b[0] = keyIn
	putUint64(b, 1, endID)
	putUint16(b, 9, relType)
	putUint64(b, 11, startID)
	putUint64(b, 19, relID)
	return b
}

// --- History keys ---

// histNodeKey returns the 17-byte key for a node history entry.
func histNodeKey(nodeID int64, version uint64) []byte {
	b := make([]byte, sizeHistKey)
	b[0] = keyHistNode
	putUint64(b, 1, nodeID)
	binary.BigEndian.PutUint64(b[9:], version)
	return b
}

// histRelKey returns the 17-byte key for a relationship history entry.
func histRelKey(relID int64, version uint64) []byte {
	b := make([]byte, sizeHistKey)
	b[0] = keyHistRel
	putUint64(b, 1, relID)
	binary.BigEndian.PutUint64(b[9:], version)
	return b
}

// histNodePrefix returns the 9-byte prefix for scanning all node version entries.
func histNodePrefix(nodeID int64) []byte {
	b := make([]byte, 1+8)
	b[0] = keyHistNode
	putUint64(b, 1, nodeID)
	return b
}

// histRelPrefix returns the 9-byte prefix for scanning all relationship version entries.
func histRelPrefix(relID int64) []byte {
	b := make([]byte, 1+8)
	b[0] = keyHistRel
	putUint64(b, 1, relID)
	return b
}

// --- Meta keys ---

// metaKey returns a variable-length key for metadata entries.
func metaKey(name string) []byte {
	b := make([]byte, 1+len(name))
	b[0] = keyMeta
	copy(b[1:], name)
	return b
}

// --- Parser functions ---

// parseIDFromKey extracts the 8-byte big-endian int64 at the given offset.
// The uint64→int64 cast reverses the encoding in putUint64.
func parseIDFromKey(key []byte, offset int) int64 {
	return int64(binary.BigEndian.Uint64(key[offset:])) // #nosec G115 — inverse of putUint64
}

// parseRelIDFromAdjKey extracts the relationship ID from the last 8 bytes of
// a 27-byte adjacency key (outKey or inKey).
func parseRelIDFromAdjKey(key []byte) int64 {
	return int64(binary.BigEndian.Uint64(key[19:])) // #nosec G115 — inverse of putUint64
}
