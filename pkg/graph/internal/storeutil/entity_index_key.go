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
// | payload | entityID(8B BE) [| txFrom(8B BE), member kinds [| version(4B BE),
// span kinds]]
//
// A composite node definition's head names every key (COMPDISK): KeyEntityIndex
// | EntityIndexNodeComposite | label(2B BE) | first key's token(2B BE) | key
// count(1B) | the other keys' tokens (2B BE each), so two composites of a label
// never share a prefix; its payload is the tuple's value key (index.
// NodeCompositeValueKey) as is: its parts are length-prefixed, so no tuple's
// key begins another's and a tuple's entries are its prefix and an ID.
//
// scopeToken is the relationship type or the label of the definition, so a
// definition owns its prefix (its head) whole. Kinds:
//
//   - EntityIndexRelCurrent: one entry per relationship and current value,
//     deleted when the value changes (the relationship mirror of 0x0A).
//   - EntityIndexNodeComposite: one entry per node and current tuple of a
//     composite node index kept on disk, deleted when the tuple changes.
//   - EntityIndexRelMember / EntityIndexNodeMember: one entry per value a row
//     of the entity ever carried and per transaction time of such a row,
//     append-only, no value. A reader takes, per entity, the lowest transaction
//     time (0, unknown, lowest of all: index.MergeFirstTx): the membership
//     sidecar's answer (store.RelPropertyTxMembershipCapability), read from
//     disk. The layout of format markers 13 to 16 (built marker value 1): a
//     store of that layout is read as it is (a windowed lookup answers every
//     member of the value) and a writable open builds it again as the span
//     kinds.
//   - EntityIndexRelSpan / EntityIndexNodeSpan (format marker 17, built marker
//     value 2, RELDISKWIN): the member entries of one row each, keyed by the
//     row's version too (Instant is ms: a create and an update of one entity in
//     one ms share value, ID and transaction time), whose 32-byte value is
//     from(8) | allTo(8) | lateTo(8) | last(8), big-endian: the row's valid-time
//     envelope as the membership sidecar keeps it (index.PropertyTxMembers):
//     [from, allTo) the any-pin span (a tombstone whose delete wrote ValidTo
//     reopens to math.MaxInt64), [from, lateTo) the span at pins at or after
//     last (the row's stored end, or for a row an update replaced open the
//     replacing row's start), last the newest stamp of the row. A reader folds
//     them per entity (index.DiskMember) and a windowed lookup answers the
//     members whose envelope meets the window, as the sidecar does.
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
	// EntityIndexRelMember and EntityIndexNodeMember are the member entries of
	// the layout before RELDISKWIN (no value, no version in the key). They also
	// name the built marker of a definition (EntityIndexBuiltKey).
	EntityIndexRelMember  byte = 0x02
	EntityIndexNodeMember byte = 0x03
	// EntityIndexNodeComposite (COMPDISK) is a composite node index's current
	// entry.
	EntityIndexNodeComposite byte = 0x04
	// EntityIndexRelSpan and EntityIndexNodeSpan (RELDISKWIN) are the member
	// entries that carry a row's valid-time envelope.
	EntityIndexRelSpan  byte = 0x05
	EntityIndexNodeSpan byte = 0x06
)

// The value of a definition's built marker: the layout of its member entries.
const (
	// EntityIndexBuiltLegacy: member entries without a span (format 13 to 16).
	EntityIndexBuiltLegacy byte = 1
	// EntityIndexBuiltSpan: member entries of the span kinds (format 17).
	EntityIndexBuiltSpan byte = 2
)

// EntityIndexSpanKind returns the span kind that replaces the member kind
// (EntityIndexRelMember, EntityIndexNodeMember); any other kind as it is.
func EntityIndexSpanKind(kind byte) byte {
	switch kind {
	case EntityIndexRelMember:
		return EntityIndexRelSpan
	case EntityIndexNodeMember:
		return EntityIndexNodeSpan
	}
	return kind
}

// EntityIndexIsSpan reports whether kind is a span kind.
func EntityIndexIsSpan(kind byte) bool {
	return kind == EntityIndexRelSpan || kind == EntityIndexNodeSpan
}

// EntityIndexIsNodeMember reports whether kind is a node member kind (either
// layout).
func EntityIndexIsNodeMember(kind byte) bool {
	return kind == EntityIndexNodeMember || kind == EntityIndexNodeSpan
}

// EntityIndexSpanValueLen is the length of a span entry's value.
const EntityIndexSpanValueLen = 32

// AppendEntityIndexSpanValue appends a span entry's value: from, allTo, lateTo,
// last (index.DiskMember).
func AppendEntityIndexSpanValue(dst []byte, from, allTo, lateTo, last int64) []byte {
	dst = binary.BigEndian.AppendUint64(dst, uint64(from))   // #nosec G115 -- int64 bits as uint64
	dst = binary.BigEndian.AppendUint64(dst, uint64(allTo))  // #nosec G115 -- int64 bits as uint64
	dst = binary.BigEndian.AppendUint64(dst, uint64(lateTo)) // #nosec G115 -- int64 bits as uint64
	return binary.BigEndian.AppendUint64(dst, uint64(last))  // #nosec G115 -- int64 bits as uint64
}

// ParseEntityIndexSpanValue splits a span entry's value; ok=false when v is
// not EntityIndexSpanValueLen bytes (a reader treats such an entry as meeting
// every window).
func ParseEntityIndexSpanValue(v []byte) (from, allTo, lateTo, last int64, ok bool) {
	if len(v) != EntityIndexSpanValueLen {
		return 0, 0, 0, 0, false
	}
	from = int64(binary.BigEndian.Uint64(v))        // #nosec G115 -- the value's instant bits back to int64
	allTo = int64(binary.BigEndian.Uint64(v[8:]))   // #nosec G115 -- the value's instant bits back to int64
	lateTo = int64(binary.BigEndian.Uint64(v[16:])) // #nosec G115 -- the value's instant bits back to int64
	last = int64(binary.BigEndian.Uint64(v[24:]))   // #nosec G115 -- the value's instant bits back to int64
	return from, allTo, lateTo, last, true
}

// EntityIndexBuiltPrefix starts the built markers of the definitions kept in
// 0x0C: one per definition whose entries an open built and every open since
// maintained (a writable open that keeps it in RAM deletes it).
var EntityIndexBuiltPrefix = MetaKey("entity_index_built/")

// EntityIndexBuiltKey is the built marker of the definition on (scopeToken,
// propertyKey) whose entries are of kind (EntityIndexRelMember for a
// relationship definition, its current entries included;
// EntityIndexNodeMember for a node definition; a span kind's definition keeps
// its legacy kind's marker). Its value is EntityIndexBuiltLegacy or
// EntityIndexBuiltSpan.
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
// the entity ID, for a member kind its transaction time, for a span kind the
// row's version too.
func EntityIndexTailLen(kind byte) int {
	switch kind {
	case EntityIndexRelCurrent, EntityIndexNodeComposite:
		return 8
	case EntityIndexRelSpan, EntityIndexNodeSpan:
		return 20
	}
	return 16
}

// AppendEntityIndexCompositeHead appends the head of a composite node
// definition's entries over keyTokens (2..4, declared order).
func AppendEntityIndexCompositeHead(dst []byte, label uint16, keyTokens []uint16) []byte {
	dst = AppendEntityIndexHead(dst, EntityIndexNodeComposite, label, keyTokens[0])
	dst = append(dst, byte(len(keyTokens))) // #nosec G115 -- 2..4 keys
	for _, tok := range keyTokens[1:] {
		dst = binary.BigEndian.AppendUint16(dst, tok)
	}
	return dst
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
// entity ID, for a member kind the row's transaction time, for a span kind the
// row's version too (the other kinds ignore version).
func AppendEntityIndexEntry(prefix []byte, kind byte, id snowflake.ID, txFrom int64, version uint32) []byte {
	prefix = binary.BigEndian.AppendUint64(prefix, uint64(id)) // #nosec G115 -- int64 bits as uint64, the key order of IDs
	if kind == EntityIndexRelCurrent || kind == EntityIndexNodeComposite {
		return prefix
	}
	prefix = binary.BigEndian.AppendUint64(prefix, uint64(txFrom)) // #nosec G115 -- int64 bits as uint64; readers take the minimum, not the key order
	if EntityIndexIsSpan(kind) {
		prefix = binary.BigEndian.AppendUint32(prefix, version)
	}
	return prefix
}

// EntityIndexEntryOf splits an entry of kind into its value prefix, entity ID
// and transaction time (0 for the current kind; a span kind's version is the
// last 4 bytes of the key); ok=false when key is too short to be one.
func EntityIndexEntryOf(key []byte, kind byte) (prefix []byte, id snowflake.ID, txFrom int64, ok bool) {
	tail := EntityIndexTailLen(kind)
	if len(key) < EntityIndexHeadLen+1+tail {
		return nil, 0, 0, false
	}
	end := len(key) - tail
	id = snowflake.ID(binary.BigEndian.Uint64(key[end:])) // #nosec G115 -- the key's ID bits back to int64
	if tail >= 16 {
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
