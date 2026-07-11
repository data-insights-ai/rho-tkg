package storeutil

import (
	"encoding/binary"
	"math"
	"strconv"
	"strings"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// Disk-resident property-value index (opt-in, badger.Config.PropertyIndexOnDisk).
//
// Layout: KeyPropertyIndex(1B) | propKeyToken(2B BE) | payload(var) | nodeID(8B BE)
//
// payload is domain-tagged:
//
//   - PropIdxDomainNumeric(1B) | sortKey(8B BE) | subtypeTag(1B) | rawBits(8B BE)
//     — fixed PropIdxNumericPayloadLen bytes. sortKey is the order-preserving
//     IEEE-754 sign-flip encoding of the value's float64 MAGNITUDE, so every
//     numeric width (int/uint/float, any size) shares ONE ordered domain and a
//     byte-range scan spans every numeric type uniformly — mirroring the
//     in-memory ordered view's semantics (property_index_range.go: RangeNodeIDs
//     conflates all numeric subtypes into one float64-keyed bucket space for
//     range queries; same over-selecting, ulp-widened contract). The
//     subtypeTag+rawBits trailer disambiguates EXACT equality across types
//     that happen to share a numeric magnitude: int64(5), uint64(5), and
//     float64(5.0) are distinct stored values (matching the RAM index's
//     type-prefixed Entries-map equality), so their full payloads differ even
//     though their sortKey prefix — and therefore their position in a range
//     scan — is identical.
//
//   - PropIdxDomainRaw(1B) | rawBytes(var) — the canonical
//     types.IndexablePropertyValueKey string's bytes, VERBATIM. Used for
//     string, bool, and TemporalValue values: none of them support range
//     scans, so byte order doesn't need to encode magnitude, and the
//     canonical value key already prevents cross-type collisions via its own
//     "s:"/"b:"/"tv:" prefix. String length is bounded by the property's own
//     ValidationLimits.MaxPropertyValueSize check at WRITE time (property
//     values are validated before they ever reach index maintenance) — the
//     codec itself enforces no additional limit and does not need to, since a
//     Badger key has no size ceiling this library imposes.
//
// Both domain payloads are self-describing and DETERMINISTIC: the same
// property value always encodes to the same bytes, so an equality lookup is a
// direct prefix scan (no stored-value round-trip needed) and a numeric range
// scan needs only the sortKey portion of the prefix.
const (
	// KeyPropertyIndex is the badger key-space prefix tag for the persisted
	// property-value index.
	KeyPropertyIndex byte = 0x0A

	// PropIdxDomainNumeric marks a fixed-length order-preserving numeric payload.
	PropIdxDomainNumeric byte = 0x01
	// PropIdxDomainRaw marks a variable-length raw (string/bool/temporal) payload.
	PropIdxDomainRaw byte = 0x02
)

// PropIdxNumericPayloadLen is the fixed payload length for the numeric
// domain: domain(1) + sortKey(8) + subtypeTag(1) + rawBits(8).
const PropIdxNumericPayloadLen = 1 + 8 + 1 + 8

// PropIdxNumericSortBoundLen is the length of the prefix used to bound a
// numeric range scan: KeyPropertyIndex(1) + propKeyToken(2) + domain(1) + sortKey(8).
const PropIdxNumericSortBoundLen = 1 + 2 + 1 + 8

// PropIdxNumericEntryKeyLen is the fixed total key length of a numeric-domain
// entry: KeyPropertyIndex(1) + propKeyToken(2) + payload(PropIdxNumericPayloadLen) + nodeID(8).
const PropIdxNumericEntryKeyLen = 1 + 2 + PropIdxNumericPayloadLen + 8

// Numeric subtype tags disambiguate exact equality when two different Go
// numeric types share a sort-key magnitude (e.g. int64(5) vs uint64(5)).
const (
	propSubtypeInt byte = 1 + iota
	propSubtypeInt8
	propSubtypeInt16
	propSubtypeInt32
	propSubtypeInt64
	propSubtypeUint
	propSubtypeUint8
	propSubtypeUint16
	propSubtypeUint32
	propSubtypeUint64
	propSubtypeFloat32
	propSubtypeFloat64
)

var propNumericSubtypeTags = map[string]byte{
	"i": propSubtypeInt, "i8": propSubtypeInt8, "i16": propSubtypeInt16,
	"i32": propSubtypeInt32, "i64": propSubtypeInt64,
	"u": propSubtypeUint, "u8": propSubtypeUint8, "u16": propSubtypeUint16,
	"u32": propSubtypeUint32, "u64": propSubtypeUint64,
	"f32": propSubtypeFloat32, "f64": propSubtypeFloat64,
}

// orderPreservingFloat64Bits maps a float64 to a uint64 whose UNSIGNED
// ordering matches the float's natural ordering — the standard IEEE-754
// sign-flip trick (lesson 25's "float bit-pattern" case, applied here to KEY
// bytes rather than the hash/equality contract): negative values flip every
// bit (reversing their magnitude order and moving them below all positive
// values), non-negative values flip only the sign bit. Because the caller
// always derives magnitude from an already-canonicalized value key (+0/-0
// collapse to the same "f64:0"/"f32:0" string upstream, in
// types.IndexablePropertyValueKey), this function never needs to reconcile
// +0 and -0 itself.
func orderPreservingFloat64Bits(f float64) uint64 {
	bits := math.Float64bits(f)
	if bits&(1<<63) != 0 {
		return ^bits
	}
	return bits | (1 << 63)
}

// propNumericSortBitsForNaN sorts after every finite value. NaN is excluded
// from range scans by construction (callers only ever bound scans with finite
// — or infinite but still less-than-all-ones — magnitudes), so this only
// needs to be a stable, deterministic placement so equality lookups for NaN
// still resolve to one key.
const propNumericSortBitsForNaN = math.MaxUint64

// PropertyIndexValueBytes returns the domain-tagged payload for a property
// value's canonical value key vk (see types.IndexablePropertyValueKey /
// types.Node.ForEachIndexablePropertyValueKey). ok=false when vk is "" (the
// value type is not indexable) or malformed (defensive — every real vk has a
// colon separating its type tag from its payload).
func PropertyIndexValueBytes(valueKey string) (payload []byte, ok bool) {
	if valueKey == "" {
		return nil, false
	}
	colon := strings.IndexByte(valueKey, ':')
	if colon < 0 {
		return nil, false
	}
	tag, rest := valueKey[:colon], valueKey[colon+1:]
	if subtype, isNumeric := propNumericSubtypeTags[tag]; isNumeric {
		return propertyIndexNumericBytes(subtype, tag, rest)
	}
	// Raw domain: string / bool / TemporalValue — verbatim vk bytes.
	out := make([]byte, 1+len(valueKey))
	out[0] = PropIdxDomainRaw
	copy(out[1:], valueKey)
	return out, true
}

func propertyIndexNumericBytes(subtype byte, tag, payload string) ([]byte, bool) {
	var mag float64
	var bits uint64
	switch tag {
	case "i", "i8", "i16", "i32", "i64":
		n, err := strconv.ParseInt(payload, 10, 64)
		if err != nil {
			return nil, false
		}
		mag = float64(n)
		bits = uint64(n) // #nosec G115 -- two's complement bit pattern, not a value cast
	case "u", "u8", "u16", "u32", "u64":
		n, err := strconv.ParseUint(payload, 10, 64)
		if err != nil {
			return nil, false
		}
		mag = float64(n)
		bits = n
	case "f32":
		if payload == "nan" {
			return propertyIndexNumericOut(subtype, propNumericSortBitsForNaN, uint64(math.Float32bits(float32(math.NaN())))), true
		}
		f, err := strconv.ParseFloat(payload, 32)
		if err != nil {
			return nil, false
		}
		mag = f
		bits = uint64(math.Float32bits(float32(f)))
	case "f64":
		if payload == "nan" {
			return propertyIndexNumericOut(subtype, propNumericSortBitsForNaN, math.Float64bits(math.NaN())), true
		}
		f, err := strconv.ParseFloat(payload, 64)
		if err != nil {
			return nil, false
		}
		mag = f
		bits = math.Float64bits(f)
	default:
		return nil, false
	}
	return propertyIndexNumericOut(subtype, orderPreservingFloat64Bits(mag), bits), true
}

func propertyIndexNumericOut(subtype byte, sortBits, rawBits uint64) []byte {
	out := make([]byte, PropIdxNumericPayloadLen)
	out[0] = PropIdxDomainNumeric
	binary.BigEndian.PutUint64(out[1:9], sortBits)
	out[9] = subtype
	binary.BigEndian.PutUint64(out[10:18], rawBits)
	return out
}

// PropertyIndexEntryKey builds the full on-disk key for one (propKeyToken,
// value, nodeID) entry.
func PropertyIndexEntryKey(propKeyToken uint16, payload []byte, nodeID snowflake.ID) []byte {
	b := make([]byte, 1+2+len(payload)+8)
	b[0] = KeyPropertyIndex
	PutUint16(b, 1, propKeyToken)
	copy(b[3:], payload)
	PutUint64(b, 3+len(payload), int64(nodeID))
	return b
}

// PropertyIndexValuePrefix returns the key prefix identifying every entry for
// one (propKeyToken, exact value) pair — an equality lookup scans this prefix
// and collects the trailing 8-byte node IDs.
func PropertyIndexValuePrefix(propKeyToken uint16, payload []byte) []byte {
	b := make([]byte, 1+2+len(payload))
	b[0] = KeyPropertyIndex
	PutUint16(b, 1, propKeyToken)
	copy(b[3:], payload)
	return b
}

// PropertyIndexTokenPrefix returns the key prefix for every entry indexed
// under propKeyToken, across every domain and value — used by the
// corruption-path brute-force purge scan and the token-level
// rebuild-on-enable scan.
func PropertyIndexTokenPrefix(propKeyToken uint16) []byte {
	b := make([]byte, 1+2)
	b[0] = KeyPropertyIndex
	PutUint16(b, 1, propKeyToken)
	return b
}

// PropertyIndexNumericDomainPrefix returns the 4-byte prefix bounding the
// numeric domain's sub-keyspace for one propKeyToken (tag+token+domain).
func PropertyIndexNumericDomainPrefix(propKeyToken uint16) []byte {
	b := make([]byte, 1+2+1)
	b[0] = KeyPropertyIndex
	PutUint16(b, 1, propKeyToken)
	b[3] = PropIdxDomainNumeric
	return b
}

// PropertyIndexNumericSortBound returns the (prefix+sortKey) byte bound used
// to Seek/compare during a numeric range scan: KeyPropertyIndex |
// propKeyToken | PropIdxDomainNumeric | sortKey(8B). Callers compare a
// candidate key's first PropIdxNumericSortBoundLen bytes against this bound.
func PropertyIndexNumericSortBound(propKeyToken uint16, magnitude float64) []byte {
	prefix := PropertyIndexNumericDomainPrefix(propKeyToken)
	out := make([]byte, len(prefix)+8)
	copy(out, prefix)
	binary.BigEndian.PutUint64(out[len(prefix):], orderPreservingFloat64Bits(magnitude))
	return out
}

// PropertyIndexNumericRangeBounds returns the [lo, hi] byte bounds for a
// numeric range scan over [min, max]. Bounds are WIDENED by one ulp on each
// side before encoding — mirroring the in-memory ordered view's
// over-selecting contract (property_index_range.go's RangeNodeIDs) — so
// callers MUST post-filter candidates with an exact comparison against the
// original inclusivity flags.
func PropertyIndexNumericRangeBounds(propKeyToken uint16, min, max float64) (lo, hi []byte) {
	lo = PropertyIndexNumericSortBound(propKeyToken, math.Nextafter(min, math.Inf(-1)))
	hi = PropertyIndexNumericSortBound(propKeyToken, math.Nextafter(max, math.Inf(1)))
	return lo, hi
}

// PropertyIndexNodeIDFromKey extracts the trailing 8-byte node ID from a
// well-formed property-index entry key (any domain — the node ID is always
// the last 8 bytes, regardless of the variable-length payload before it).
func PropertyIndexNodeIDFromKey(key []byte) snowflake.ID {
	return ParseIDFromKey(key, len(key)-8)
}
