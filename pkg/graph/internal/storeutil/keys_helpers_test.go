package storeutil

import (
	"encoding/binary"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// Key prefix tags used only in tests — future key types for temporal
// indexing that are not yet integrated into any Store implementation.
const (
	keyTempNode byte = 0x09 // + 8B validFrom + 8B nodeID               = 17B
	keyTempRel  byte = 0x0A // + 8B validFrom + 8B relID                = 17B
)

// Key sizes for test-only key types.
const (
	sizeTempIdx = 1 + 8 + 8 // 17B
)

// --- Prefix builders (test-only) ---

// labelIndexPrefix returns the 3-byte prefix for scanning all nodes with a label.
func labelIndexPrefix(token uint16) []byte {
	b := make([]byte, 3)
	b[0] = KeyLabel
	PutUint16(b, 1, token)
	return b
}

// relTypeIndexPrefix returns the 3-byte prefix for scanning all rels of a type.
func relTypeIndexPrefix(token uint16) []byte {
	b := make([]byte, 3)
	b[0] = KeyRelType
	PutUint16(b, 1, token)
	return b
}

// outPrefix returns the 9-byte prefix for all outgoing rels from a node.
func outPrefix(startID snowflake.ID) []byte {
	b := make([]byte, 1+8)
	b[0] = KeyOut
	PutUint64(b, 1, int64(startID))
	return b
}

// outTypedPrefix returns the 11-byte prefix for outgoing rels of a specific type.
func outTypedPrefix(startID snowflake.ID, relType uint16) []byte {
	b := make([]byte, 1+8+2)
	b[0] = KeyOut
	PutUint64(b, 1, int64(startID))
	PutUint16(b, 9, relType)
	return b
}

// inPrefix returns the 9-byte prefix for all incoming rels to a node.
func inPrefix(endID snowflake.ID) []byte {
	b := make([]byte, 1+8)
	b[0] = KeyIn
	PutUint64(b, 1, int64(endID))
	return b
}

// inTypedPrefix returns the 11-byte prefix for incoming rels of a specific type.
func inTypedPrefix(endID snowflake.ID, relType uint16) []byte {
	b := make([]byte, 1+8+2)
	b[0] = KeyIn
	PutUint64(b, 1, int64(endID))
	PutUint16(b, 9, relType)
	return b
}

// --- Temporal index keys (test-only) ---

// tempNodeKey returns the 17-byte key for a temporal node index entry.
func tempNodeKey(validFrom int64, nodeID snowflake.ID) []byte {
	b := make([]byte, sizeTempIdx)
	b[0] = keyTempNode
	PutUint64(b, 1, validFrom)
	PutUint64(b, 9, int64(nodeID))
	return b
}

// tempRelKey returns the 17-byte key for a temporal relationship index entry.
func tempRelKey(validFrom int64, relID snowflake.ID) []byte {
	b := make([]byte, sizeTempIdx)
	b[0] = keyTempRel
	PutUint64(b, 1, validFrom)
	PutUint64(b, 9, int64(relID))
	return b
}

// --- Parser functions (test-only) ---

// parseNodeIDFromLabelIdx extracts the node ID from the last 8 bytes of
// an 11-byte label index key.
func parseNodeIDFromLabelIdx(key []byte) snowflake.ID {
	return snowflake.ID(binary.BigEndian.Uint64(key[3:])) // #nosec G115 — inverse of PutUint64
}

// parseRelIDFromTypeIdx extracts the relationship ID from the last 8 bytes of
// an 11-byte reltype index key.
func parseRelIDFromTypeIdx(key []byte) snowflake.ID {
	return snowflake.ID(binary.BigEndian.Uint64(key[3:])) // #nosec G115 — inverse of PutUint64
}
