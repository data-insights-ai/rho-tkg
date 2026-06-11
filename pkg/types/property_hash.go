package types

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
)

// Property hash type tags mirror the persisted wire tags used by storeutil.
// They are part of the integrity-hash byte layout; changing them invalidates
// existing node and relationship hash chains.
const (
	propertyHashTypeUnknown    byte = 0
	propertyHashTypeBool       byte = 1
	propertyHashTypeInt        byte = 2
	propertyHashTypeInt8       byte = 3
	propertyHashTypeInt16      byte = 4
	propertyHashTypeInt32      byte = 5
	propertyHashTypeInt64      byte = 6
	propertyHashTypeUint       byte = 7
	propertyHashTypeUint8      byte = 8
	propertyHashTypeUint16     byte = 9
	propertyHashTypeUint32     byte = 10
	propertyHashTypeUint64     byte = 11
	propertyHashTypeFloat32    byte = 12
	propertyHashTypeFloat64    byte = 13
	propertyHashTypeString     byte = 14
	propertyHashTypeSliceStr   byte = 15
	propertyHashTypeSliceInt   byte = 16
	propertyHashTypeSliceInt64 byte = 17
	propertyHashTypeSliceF64   byte = 18
	propertyHashTypeSliceByte  byte = 19
	propertyHashTypeSliceBool  byte = 20
	propertyHashTypeSliceAny   byte = 21
	propertyHashTypeMapStrAny  byte = 22
	propertyHashTypeMapStrStr  byte = 23
	propertyHashTypeSliceF32   byte = 24
	propertyHashTypeCustom     byte = 25
	propertyHashTypeTemporal   byte = 26
)

const propertyHashNilContainerLen = ^uint32(0)

// PropertyHashTypeTag returns the stable type tag used by property integrity
// hashing. The values intentionally match the graph store wire tags.
func PropertyHashTypeTag(v any) byte {
	switch v.(type) {
	case bool:
		return propertyHashTypeBool
	case int:
		return propertyHashTypeInt
	case int8:
		return propertyHashTypeInt8
	case int16:
		return propertyHashTypeInt16
	case int32:
		return propertyHashTypeInt32
	case int64:
		return propertyHashTypeInt64
	case uint:
		return propertyHashTypeUint
	case uint8:
		return propertyHashTypeUint8
	case uint16:
		return propertyHashTypeUint16
	case uint32:
		return propertyHashTypeUint32
	case uint64:
		return propertyHashTypeUint64
	case float32:
		return propertyHashTypeFloat32
	case float64:
		return propertyHashTypeFloat64
	case string:
		return propertyHashTypeString
	case []string:
		return propertyHashTypeSliceStr
	case []int:
		return propertyHashTypeSliceInt
	case []int64:
		return propertyHashTypeSliceInt64
	case []float32:
		return propertyHashTypeSliceF32
	case []float64:
		return propertyHashTypeSliceF64
	case []byte:
		return propertyHashTypeSliceByte
	case []bool:
		return propertyHashTypeSliceBool
	case []any:
		return propertyHashTypeSliceAny
	case map[string]any:
		return propertyHashTypeMapStrAny
	case map[string]string:
		return propertyHashTypeMapStrStr
	case TemporalValue:
		return propertyHashTypeTemporal
	default:
		if _, _, ok := RegisteredPropertyStructWireType(v); ok {
			return propertyHashTypeCustom
		}
		return propertyHashTypeUnknown
	}
}

// AppendPropertyValueHashBytes appends the typed binary representation of a
// single property value used by node and relationship integrity hashes.
func AppendPropertyValueHashBytes(buf []byte, v any) []byte {
	return appendPropertyValueHashBytes(buf, v, 0)
}

func appendPropertyValueHashBytes(buf []byte, v any, depth int) []byte {
	if depth > maxPropertyDepth {
		panic("types: AppendPropertyValueHashBytes: property value exceeds maximum nesting depth")
	}
	tag := PropertyHashTypeTag(v)
	buf = append(buf, tag)
	if v == nil {
		return buf
	}
	if tag == propertyHashTypeUnknown {
		panic(fmt.Sprintf("types: AppendPropertyValueHashBytes: unsupported type %T", v))
	}

	switch val := v.(type) {
	case bool:
		if val {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	case int:
		buf = binary.BigEndian.AppendUint64(buf, uint64(int64(val))) // #nosec G115
	case int8:
		buf = append(buf, byte(val)) // #nosec G115
	case int16:
		buf = binary.BigEndian.AppendUint16(buf, uint16(val)) // #nosec G115
	case int32:
		buf = binary.BigEndian.AppendUint32(buf, uint32(val)) // #nosec G115
	case int64:
		buf = binary.BigEndian.AppendUint64(buf, uint64(val)) // #nosec G115
	case uint:
		buf = binary.BigEndian.AppendUint64(buf, uint64(val))
	case uint8:
		buf = append(buf, val)
	case uint16:
		buf = binary.BigEndian.AppendUint16(buf, val)
	case uint32:
		buf = binary.BigEndian.AppendUint32(buf, val)
	case uint64:
		buf = binary.BigEndian.AppendUint64(buf, val)
	case float32:
		// Hash by raw IEEE-754 bit pattern. Preserves ±0 sign-bit distinction
		// (deliberate — see TestAppendPropertyValue_Float64_PositiveNegativeZero)
		// and NaN payload bits (deliberate — see
		// TestAppendPropertyValue_Float32_NaN_BitsPreserved). PropertyValueEqual
		// treats all NaN as equal for CAS short-circuit purposes, so a no-op
		// SetProperty that replaces one NaN bit pattern with another will not
		// trigger a version bump; but a fresh write that stores a different bit
		// pattern produces a distinct hash. Callers exchanging data with
		// systems that canonicalize NaN bits must canonicalize at the boundary.
		buf = binary.BigEndian.AppendUint32(buf, math.Float32bits(val))
	case float64:
		// See float32 comment above; same contract for float64.
		buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(val))
	case string:
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		buf = append(buf, val...)
	case TemporalValue:
		buf = append(buf, byte(val.Kind))
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val.Value))) // #nosec G115
		buf = append(buf, val.Value...)
	case []string:
		if val == nil {
			return appendNilPropertyContainerHash(buf)
		}
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		for _, s := range val {
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(s))) // #nosec G115
			buf = append(buf, s...)
		}
	case []int:
		if val == nil {
			return appendNilPropertyContainerHash(buf)
		}
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		for _, n := range val {
			buf = binary.BigEndian.AppendUint64(buf, uint64(int64(n))) // #nosec G115
		}
	case []int64:
		if val == nil {
			return appendNilPropertyContainerHash(buf)
		}
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		for _, n := range val {
			buf = binary.BigEndian.AppendUint64(buf, uint64(n)) // #nosec G115
		}
	case []float32:
		if val == nil {
			return appendNilPropertyContainerHash(buf)
		}
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		for _, f := range val {
			buf = binary.BigEndian.AppendUint32(buf, math.Float32bits(f))
		}
	case []float64:
		if val == nil {
			return appendNilPropertyContainerHash(buf)
		}
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		for _, f := range val {
			buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(f))
		}
	case []byte:
		if val == nil {
			return appendNilPropertyContainerHash(buf)
		}
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		buf = append(buf, val...)
	case []bool:
		if val == nil {
			return appendNilPropertyContainerHash(buf)
		}
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		for _, b := range val {
			if b {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		}
	case []any:
		if val == nil {
			return appendNilPropertyContainerHash(buf)
		}
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		for _, elem := range val {
			buf = appendPropertyValueHashBytes(buf, elem, depth+1)
		}
	case map[string]any:
		if val == nil {
			return appendNilPropertyContainerHash(buf)
		}
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(k))) // #nosec G115
			buf = append(buf, k...)
			buf = appendPropertyValueHashBytes(buf, val[k], depth+1)
		}
	case map[string]string:
		if val == nil {
			return appendNilPropertyContainerHash(buf)
		}
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(val))) // #nosec G115
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(k))) // #nosec G115
			buf = append(buf, k...)
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(val[k]))) // #nosec G115
			buf = append(buf, val[k]...)
		}
	default:
		if tag == propertyHashTypeCustom {
			typeName, pointer, ok := RegisteredPropertyStructWireType(v)
			if !ok {
				panic(fmt.Sprintf("types: AppendPropertyValueHashBytes: unregistered custom type %T", v))
			}
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(typeName))) // #nosec G115
			buf = append(buf, typeName...)
			if pointer {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
			h, ok := v.(HashableValue)
			if !ok {
				panic(fmt.Sprintf("types: AppendPropertyValueHashBytes: custom type %T does not implement types.HashableValue", v))
			}
			hb := h.HashBytes()
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(hb))) // #nosec G115
			buf = append(buf, hb...)
			return buf
		}
		panic(fmt.Sprintf("types: AppendPropertyValueHashBytes: unsupported type %T", v))
	}
	return buf
}

func appendNilPropertyContainerHash(buf []byte) []byte {
	return binary.BigEndian.AppendUint32(buf, propertyHashNilContainerLen)
}

// AppendHashBytes appends all properties in canonical key order to buf using
// the integrity-hash byte layout. It does not copy property values.
func (ps PropertySlice) AppendHashBytes(buf []byte) []byte {
	for _, p := range ps {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(p.Key))) // #nosec G115
		buf = append(buf, p.Key...)
		buf = AppendPropertyValueHashBytes(buf, p.Value)
	}
	return buf
}

// PropertyValueHashNeedsRecover reports whether hashing v can invoke custom
// user code or hit the unsupported-type panic used to protect the property
// validation invariant. Both cases must be wrapped by the checked hash path.
func PropertyValueHashNeedsRecover(v any) bool {
	return propertyValueHashNeedsRecover(v, 0)
}

func propertyValueHashNeedsRecover(v any, depth int) bool {
	if depth > maxPropertyDepth {
		return true
	}
	switch val := v.(type) {
	case nil,
		bool, string,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		[]string, []int, []int64, []float32, []float64, []byte, []bool,
		map[string]string:
		return false
	case []any:
		for _, elem := range val {
			if propertyValueHashNeedsRecover(elem, depth+1) {
				return true
			}
		}
		return false
	case map[string]any:
		for _, elem := range val {
			if propertyValueHashNeedsRecover(elem, depth+1) {
				return true
			}
		}
		return false
	default:
		return true
	}
}

func propertySliceHashNeedsRecover(ps PropertySlice) bool {
	for _, p := range ps {
		if PropertyValueHashNeedsRecover(p.Value) {
			return true
		}
	}
	return false
}
