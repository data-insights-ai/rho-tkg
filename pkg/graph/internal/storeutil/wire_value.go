package storeutil

// Property type tags + value reconstruction (R5-F9 split out from wire.go).
//
// File layout:
//   - wire.go        — NodeWire / RelWire / PropertyWire types + entity-level
//                      to/from-wire conversion + property slice converters.
//   - wire_value.go  — Property type tag constants, PropertyTypeTag,
//                      reconstructTypedValue, and the int/slice/map helper
//                      functions that round-trip msgpack-decoded values back
//                      to the original Go types.

// Property type tags — used to reconstruct exact Go types after msgpack decoding.
// Covers every type accepted by PropertySlice.Set() (deepCopyValue fast-paths).
const (
	ptUnknown    byte = 0 // fallback: normalize integers only
	ptBool       byte = 1
	ptInt        byte = 2
	ptInt8       byte = 3
	ptInt16      byte = 4
	ptInt32      byte = 5
	ptInt64      byte = 6
	ptUint       byte = 7
	ptUint8      byte = 8
	ptUint16     byte = 9
	ptUint32     byte = 10
	ptUint64     byte = 11
	ptFloat32    byte = 12
	ptFloat64    byte = 13
	ptString     byte = 14
	ptSliceStr   byte = 15
	ptSliceInt   byte = 16
	ptSliceInt64 byte = 17
	ptSliceF64   byte = 18
	ptSliceByte  byte = 19
	ptSliceBool  byte = 20
	ptSliceAny   byte = 21
	ptMapStrAny  byte = 22
	ptMapStrStr  byte = 23
	ptSliceF32   byte = 24
)

// PropertyTypeTag returns the type tag for a property value.
func PropertyTypeTag(v any) byte {
	switch v.(type) {
	case bool:
		return ptBool
	case int:
		return ptInt
	case int8:
		return ptInt8
	case int16:
		return ptInt16
	case int32:
		return ptInt32
	case int64:
		return ptInt64
	case uint:
		return ptUint
	case uint8:
		return ptUint8
	case uint16:
		return ptUint16
	case uint32:
		return ptUint32
	case uint64:
		return ptUint64
	case float32:
		return ptFloat32
	case float64:
		return ptFloat64
	case string:
		return ptString
	case []string:
		return ptSliceStr
	case []int:
		return ptSliceInt
	case []int64:
		return ptSliceInt64
	case []float32:
		return ptSliceF32
	case []float64:
		return ptSliceF64
	case []byte:
		return ptSliceByte
	case []bool:
		return ptSliceBool
	case []any:
		return ptSliceAny
	case map[string]any:
		return ptMapStrAny
	case map[string]string:
		return ptMapStrStr
	default:
		return ptUnknown
	}
}

// reconstructTypedValue reconstructs the exact Go type from a decoded msgpack
// value using the stored type tag. Msgpack destroys type fidelity: []string
// becomes []any, int64 becomes int8 for small values, etc. The type tag
// reverses this loss.
func reconstructTypedValue(v any, tag byte) any {
	if v == nil {
		return nil
	}
	switch tag {
	case ptBool:
		if b, ok := v.(bool); ok {
			return b
		}
	case ptInt:
		return int(toInt64(v))
	case ptInt8:
		return int8(toInt64(v)) // #nosec G115 — original value was int8
	case ptInt16:
		return int16(toInt64(v)) // #nosec G115 — original value was int16
	case ptInt32:
		return int32(toInt64(v)) // #nosec G115 — original value was int32
	case ptInt64:
		return toInt64(v)
	case ptUint:
		return uint(toUint64(v))
	case ptUint8:
		return uint8(toUint64(v)) // #nosec G115 — original value was uint8
	case ptUint16:
		return uint16(toUint64(v)) // #nosec G115 — original value was uint16
	case ptUint32:
		return uint32(toUint64(v)) // #nosec G115 — original value was uint32
	case ptUint64:
		return toUint64(v)
	case ptFloat32:
		if f, ok := v.(float64); ok {
			return float32(f)
		}
		if f, ok := v.(float32); ok {
			return f
		}
	case ptFloat64:
		if f, ok := v.(float64); ok {
			return f
		}
	case ptString:
		if s, ok := v.(string); ok {
			return s
		}
	case ptSliceStr:
		return toStringSlice(v)
	case ptSliceInt:
		return toIntSlice(v)
	case ptSliceInt64:
		return toInt64Slice(v)
	case ptSliceF32:
		return ToFloat32SliceWire(v)
	case ptSliceF64:
		return toFloat64Slice(v)
	case ptSliceByte:
		// msgpack encodes []byte as binary — comes back as []byte.
		if b, ok := v.([]byte); ok {
			return b
		}
	case ptSliceBool:
		return toBoolSlice(v)
	case ptSliceAny:
		if s, ok := v.([]any); ok {
			return normalizeIntegersInSlice(s)
		}
	case ptMapStrAny:
		if m, ok := v.(map[string]any); ok {
			return normalizeIntegersInMap(m)
		}
	case ptMapStrStr:
		return toStringStringMap(v)
	}
	// ptUnknown or unmatched: best-effort integer normalization.
	return normalizeIntegersRecursive(v)
}

// --- Integer conversion helpers ---

// toInt64 converts any msgpack-decoded integer to int64.
func toInt64(v any) int64 {
	switch n := v.(type) {
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	case uint8:
		return int64(n)
	case uint16:
		return int64(n)
	case uint32:
		return int64(n)
	case uint64:
		return int64(n) // #nosec G115 — value fits, came from our serialization
	}
	return 0
}

// toUint64 converts any msgpack-decoded integer to uint64.
func toUint64(v any) uint64 {
	switch n := v.(type) {
	case uint8:
		return uint64(n)
	case uint16:
		return uint64(n)
	case uint32:
		return uint64(n)
	case uint64:
		return n
	case int8:
		return uint64(n) // #nosec G115 — value fits, came from our serialization
	case int16:
		return uint64(n) // #nosec G115 — value fits
	case int32:
		return uint64(n) // #nosec G115 — value fits
	case int64:
		return uint64(n) // #nosec G115 — value fits
	case int:
		return uint64(n) // #nosec G115 — value fits
	}
	return 0
}

// --- Slice reconstruction helpers ---

func toStringSlice(v any) []string {
	switch s := v.(type) {
	case []string:
		return s
	case []any:
		out := make([]string, len(s))
		for i, e := range s {
			out[i], _ = e.(string)
		}
		return out
	}
	return nil
}

func toIntSlice(v any) []int {
	switch s := v.(type) {
	case []int:
		return s
	case []any:
		out := make([]int, len(s))
		for i, e := range s {
			out[i] = int(toInt64(e))
		}
		return out
	}
	return nil
}

func toInt64Slice(v any) []int64 {
	switch s := v.(type) {
	case []int64:
		return s
	case []any:
		out := make([]int64, len(s))
		for i, e := range s {
			out[i] = toInt64(e)
		}
		return out
	}
	return nil
}

func toFloat64Slice(v any) []float64 {
	switch s := v.(type) {
	case []float64:
		return s
	case []any:
		out := make([]float64, len(s))
		for i, e := range s {
			if f, ok := e.(float64); ok {
				out[i] = f
			}
		}
		return out
	}
	return nil
}

// ToFloat32SliceWire reconstructs a []float32 from a msgpack-decoded value.
// Msgpack encodes float32 elements as float32 in a typed slice.
func ToFloat32SliceWire(v any) []float32 {
	switch s := v.(type) {
	case []float32:
		return s
	case []any:
		out := make([]float32, len(s))
		for i, e := range s {
			switch f := e.(type) {
			case float32:
				out[i] = f
			case float64:
				out[i] = float32(f)
			}
		}
		return out
	}
	return nil
}

func toBoolSlice(v any) []bool {
	switch s := v.(type) {
	case []bool:
		return s
	case []any:
		out := make([]bool, len(s))
		for i, e := range s {
			out[i], _ = e.(bool)
		}
		return out
	}
	return nil
}

func toStringStringMap(v any) map[string]string {
	switch m := v.(type) {
	case map[string]string:
		return m
	case map[string]any:
		out := make(map[string]string, len(m))
		for k, val := range m {
			out[k], _ = val.(string)
		}
		return out
	}
	return nil
}

// --- Integer normalization ---

// normalizeIntegersRecursive normalizes msgpack-decoded integers:
// int8/int16/int32 → int64, uint8/uint16/uint32 → uint64.
// Recurses into []any and map[string]any containers.
func normalizeIntegersRecursive(v any) any {
	switch val := v.(type) {
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case uint8:
		return uint64(val)
	case uint16:
		return uint64(val)
	case uint32:
		return uint64(val)
	case []any:
		return normalizeIntegersInSlice(val)
	case map[string]any:
		return normalizeIntegersInMap(val)
	default:
		return v
	}
}

func normalizeIntegersInSlice(s []any) []any {
	out := make([]any, len(s))
	for i, e := range s {
		out[i] = normalizeIntegersRecursive(e)
	}
	return out
}

func normalizeIntegersInMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, val := range m {
		out[k] = normalizeIntegersRecursive(val)
	}
	return out
}
