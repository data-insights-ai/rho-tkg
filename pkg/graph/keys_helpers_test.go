package graph

import "encoding/binary"

// Key prefix tags used only in tests — future key types for history and temporal
// indexing that are not yet integrated into any Store implementation.
const (
	keyHistNode byte = 0x07 // + 8B nodeID + 8B version                 = 17B
	keyHistRel  byte = 0x08 // + 8B relID + 8B version                  = 17B
	keyTempNode byte = 0x09 // + 8B validFrom + 8B nodeID               = 17B
	keyTempRel  byte = 0x0A // + 8B validFrom + 8B relID                = 17B
)

// Key sizes for test-only key types.
const (
	sizeHistKey = 1 + 8 + 8 // 17B
	sizeTempIdx = 1 + 8 + 8 // 17B
)

// --- Prefix builders (test-only) ---

// labelIndexPrefix returns the 3-byte prefix for scanning all nodes with a label.
func labelIndexPrefix(token uint16) []byte {
	b := make([]byte, 3)
	b[0] = keyLabel
	putUint16(b, 1, token)
	return b
}

// relTypeIndexPrefix returns the 3-byte prefix for scanning all rels of a type.
func relTypeIndexPrefix(token uint16) []byte {
	b := make([]byte, 3)
	b[0] = keyRelType
	putUint16(b, 1, token)
	return b
}

// outPrefix returns the 9-byte prefix for all outgoing rels from a node.
func outPrefix(startID int64) []byte {
	b := make([]byte, 1+8)
	b[0] = keyOut
	putUint64(b, 1, startID)
	return b
}

// outTypedPrefix returns the 11-byte prefix for outgoing rels of a specific type.
func outTypedPrefix(startID int64, relType uint16) []byte {
	b := make([]byte, 1+8+2)
	b[0] = keyOut
	putUint64(b, 1, startID)
	putUint16(b, 9, relType)
	return b
}

// inPrefix returns the 9-byte prefix for all incoming rels to a node.
func inPrefix(endID int64) []byte {
	b := make([]byte, 1+8)
	b[0] = keyIn
	putUint64(b, 1, endID)
	return b
}

// inTypedPrefix returns the 11-byte prefix for incoming rels of a specific type.
func inTypedPrefix(endID int64, relType uint16) []byte {
	b := make([]byte, 1+8+2)
	b[0] = keyIn
	putUint64(b, 1, endID)
	putUint16(b, 9, relType)
	return b
}

// --- History keys (test-only) ---

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

// --- Temporal index keys (test-only) ---

// tempNodeKey returns the 17-byte key for a temporal node index entry.
func tempNodeKey(validFrom int64, nodeID int64) []byte {
	b := make([]byte, sizeTempIdx)
	b[0] = keyTempNode
	putUint64(b, 1, validFrom)
	putUint64(b, 9, nodeID)
	return b
}

// tempRelKey returns the 17-byte key for a temporal relationship index entry.
func tempRelKey(validFrom int64, relID int64) []byte {
	b := make([]byte, sizeTempIdx)
	b[0] = keyTempRel
	putUint64(b, 1, validFrom)
	putUint64(b, 9, relID)
	return b
}

// --- Parser functions (test-only) ---

// parseNodeIDFromLabelIdx extracts the node ID from the last 8 bytes of
// an 11-byte label index key.
func parseNodeIDFromLabelIdx(key []byte) int64 {
	return int64(binary.BigEndian.Uint64(key[3:])) // #nosec G115 — inverse of putUint64
}

// parseRelIDFromTypeIdx extracts the relationship ID from the last 8 bytes of
// an 11-byte reltype index key.
func parseRelIDFromTypeIdx(key []byte) int64 {
	return int64(binary.BigEndian.Uint64(key[3:])) // #nosec G115 — inverse of putUint64
}
