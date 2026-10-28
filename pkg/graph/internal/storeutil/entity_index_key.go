package storeutil

import (
	"encoding/binary"
	"strings"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// Disk-resident entity property index (RELDISK, badger.Config.
// RelPropertyIndexOnDiskTypes and the node indexes kept on disk).
//
// Layout: KeyEntityIndex(1B) | kind(1B) | scopeToken(2B BE) | propKeyToken(2B BE)
// | payload | entityID(8B BE) [| txFrom(8B BE), member kinds]
//
// scopeToken is the relationship type or the label of the definition, so a
// definition owns its prefix (its head) whole. Kinds:
//
//   - EntityIndexRelCurrent: one entry per relationship and current value,
//     deleted when the value changes (the relationship mirror of 0x0A).
//   - EntityIndexRelMember / EntityIndexNodeMember: one entry per value a row
//     of the entity ever carried and per transaction time of such a row,
//     append-only. A reader takes, per entity, the lowest transaction time
//     (0, unknown, lowest of all: index.MergeFirstTx): the membership sidecar's
//     answer (store.RelPropertyTxMembershipCapability), read from disk.
//
// payload is 0x0A's numeric payload (PropIdxDomainNumeric, 18 bytes, ordered
// by magnitude) or, for a string, bool or temporal value, PropIdxDomainRaw
// followed by the value key with each 0x00 byte written 0x00 0xFF and a 0x00
// terminator. The terminator ends a value's entries at its own: an equality
// walk visits no longer value that extends it (0x0A's unterminated raw payload
// needs node ID bounds for that, PROPSTR), and the order of values is kept.
const (
	// KeyEntityIndex is the badger key-space prefix tag of the RELDISK index.
	KeyEntityIndex byte = 0x0C

	EntityIndexRelCurrent byte = 0x01
	EntityIndexRelMember  byte = 0x02
	EntityIndexNodeMember byte = 0x03
)

// EntityIndexBuiltPrefix starts the built markers of the definitions kept in
// 0x0C: one per definition whose entries an open built and every open since
// maintained (a writable open that keeps it in RAM deletes it).
var EntityIndexBuiltPrefix = MetaKey("entity_index_built/")

// EntityIndexBuiltKey is the built marker of the definition on (scopeToken,
// propertyKey) whose entries are of kind (EntityIndexRelMember for a
// relationship definition, its current entries included;
// EntityIndexNodeMember for a node definition).
func EntityIndexBuiltKey(kind byte, scopeToken uint16, propertyKey string) []byte {
	b := make([]byte, 0, len(EntityIndexBuiltPrefix)+3+len(propertyKey))
	b = append(b, EntityIndexBuiltPrefix...)
	b = append(b, kind)
	b = binary.BigEndian.AppendUint16(b, scopeToken)
	return append(b, propertyKey...)
}

// EntityIndexHeadLen is the length of a definition's head: tag, kind, scope
// token and property-key token.
const EntityIndexHeadLen = 1 + 1 + 2 + 2

// EntityIndexTailLen is the length of what follows an entry's value prefix:
// the entity ID, and for a member kind its transaction time.
func EntityIndexTailLen(kind byte) int {
	if kind == EntityIndexRelCurrent {
		return 8
	}
	return 16
}

// AppendEntityIndexHead appends the head of a definition's entries of kind.
func AppendEntityIndexHead(dst []byte, kind byte, scopeToken, propKeyToken uint16) []byte {
	dst = append(dst, KeyEntityIndex, kind)
	dst = binary.BigEndian.AppendUint16(dst, scopeToken)
	return binary.BigEndian.AppendUint16(dst, propKeyToken)
}

// AppendEntityIndexValue appends the payload of valueKey (a canonical
// types.IndexablePropertyValueKey); ok=false (dst unchanged) when it has none.
func AppendEntityIndexValue(dst []byte, valueKey string) ([]byte, bool) {
	n := len(dst)
	out, ok := appendPropertyIndexValueBytes(dst, valueKey)
	if !ok {
		return dst[:n], false
	}
	if out[n] != PropIdxDomainRaw {
		return out, true
	}
	// Escape the raw bytes in place of the verbatim ones, then terminate.
	out = append(out[:n+1], escapeEntityIndexRaw(nil, valueKey)...)
	return append(out, 0x00), true
}

// EntityIndexValueLen is the length AppendEntityIndexValue gives valueKey's
// payload, without building it (0 for a value key without a type tag, which
// has none; a malformed number's length is that of a number's).
func EntityIndexValueLen(valueKey string) int {
	colon := strings.IndexByte(valueKey, ':')
	if colon < 0 {
		return 0
	}
	if _, isNumeric := propNumericSubtypeTags[valueKey[:colon]]; isNumeric {
		return PropIdxNumericPayloadLen
	}
	return 1 + len(valueKey) + strings.Count(valueKey, "\x00") + 1
}

func escapeEntityIndexRaw(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		dst = append(dst, s[i])
		if s[i] == 0x00 {
			dst = append(dst, 0xFF)
		}
	}
	return dst
}

// AppendEntityIndexEntry appends an entry's tail to its value prefix: the
// entity ID, and for a member kind the row's transaction time.
func AppendEntityIndexEntry(prefix []byte, kind byte, id snowflake.ID, txFrom int64) []byte {
	prefix = binary.BigEndian.AppendUint64(prefix, uint64(id)) // #nosec G115 -- int64 bits as uint64, the key order of IDs
	if kind == EntityIndexRelCurrent {
		return prefix
	}
	return binary.BigEndian.AppendUint64(prefix, uint64(txFrom)) // #nosec G115 -- int64 bits as uint64; readers take the minimum, not the key order
}

// EntityIndexEntryOf splits an entry of kind into its value prefix, entity ID
// and transaction time (0 for the current kind); ok=false when key is too
// short to be one.
func EntityIndexEntryOf(key []byte, kind byte) (prefix []byte, id snowflake.ID, txFrom int64, ok bool) {
	tail := EntityIndexTailLen(kind)
	if len(key) < EntityIndexHeadLen+1+tail {
		return nil, 0, 0, false
	}
	end := len(key) - tail
	id = snowflake.ID(binary.BigEndian.Uint64(key[end:])) // #nosec G115 -- the key's ID bits back to int64
	if tail == 16 {
		txFrom = int64(binary.BigEndian.Uint64(key[end+8:])) // #nosec G115 -- the key's instant bits back to int64
	}
	return key[:end], id, txFrom, true
}

// EntityIndexNumericBounds returns the [lo, hi] bounds of a numeric range scan
// over [min, max] under head, exact (the membership sidecar's range: no reader
// re-checks the value); -0 bounds as 0, the value every stored zero has. A
// key's first len(lo) bytes compare against them. Callers pass min <= max.
func EntityIndexNumericBounds(head []byte, min, max float64) (lo, hi []byte) {
	bound := func(f float64) []byte {
		if f == 0 {
			f = 0 // -0 to 0
		}
		b := append(append([]byte(nil), head...), PropIdxDomainNumeric)
		return binary.BigEndian.AppendUint64(b, orderPreservingFloat64Bits(f))
	}
	return bound(min), bound(max)
}

// EntityIndexNaNBound is the bound under head of every NaN entry: NaN's sort
// key sorts after every number's.
func EntityIndexNaNBound(head []byte) []byte {
	b := append(append([]byte(nil), head...), PropIdxDomainNumeric)
	return binary.BigEndian.AppendUint64(b, propNumericSortBitsForNaN)
}
