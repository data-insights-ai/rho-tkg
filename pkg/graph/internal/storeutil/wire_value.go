package storeutil

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strconv"

	"github.com/vmihailenco/msgpack/v5"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

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
	ptCustom     byte = 25
)

const (
	minInt8  = int64(-1 << 7)
	maxInt8  = int64(1<<7 - 1)
	minInt16 = int64(-1 << 15)
	maxInt16 = int64(1<<15 - 1)
	minInt32 = int64(-1 << 31)
	maxInt32 = int64(1<<31 - 1)
	minInt64 = int64(-1 << 63)
	maxInt64 = int64(1<<63 - 1)

	maxUint8  = uint64(1<<8 - 1)
	maxUint16 = uint64(1<<16 - 1)
	maxUint32 = uint64(1<<32 - 1)

	maxFloat32 = 3.40282346638528859811704183484516925440e+38
)

func validatePropertyWireValue(v any, tag byte) error {
	if tag == ptUnknown {
		return nil
	}
	if v == nil {
		return fmt.Errorf("tag %d cannot encode nil; nil uses type tag 0", tag)
	}

	switch tag {
	case ptBool:
		if _, ok := v.(bool); ok {
			return nil
		}
	case ptInt:
		n, ok := wireInt64(v)
		if ok && fitsInt(n) {
			return nil
		}
	case ptInt8:
		return validateWireIntRange(v, minInt8, maxInt8, "int8")
	case ptInt16:
		return validateWireIntRange(v, minInt16, maxInt16, "int16")
	case ptInt32:
		return validateWireIntRange(v, minInt32, maxInt32, "int32")
	case ptInt64:
		if _, ok := wireInt64(v); ok {
			return nil
		}
	case ptUint:
		n, ok := wireUint64(v)
		if ok && fitsUint(n) {
			return nil
		}
	case ptUint8:
		return validateWireUintRange(v, maxUint8, "uint8")
	case ptUint16:
		return validateWireUintRange(v, maxUint16, "uint16")
	case ptUint32:
		return validateWireUintRange(v, maxUint32, "uint32")
	case ptUint64:
		if _, ok := wireUint64(v); ok {
			return nil
		}
	case ptFloat32:
		f, ok := wireFloat64(v)
		if ok && validFloat32WireValue(f) {
			return nil
		}
	case ptFloat64:
		if _, ok := wireFloat64(v); ok {
			return nil
		}
	case ptString:
		if _, ok := v.(string); ok {
			return nil
		}
	case ptSliceStr:
		return validateWireStringSlice(v)
	case ptSliceInt:
		return validateWireIntSlice(v, minInt64, maxInt64, "int")
	case ptSliceInt64:
		return validateWireIntSlice(v, minInt64, maxInt64, "int64")
	case ptSliceF32:
		return validateWireFloat32Slice(v)
	case ptSliceF64:
		return validateWireFloat64Slice(v)
	case ptSliceByte:
		if _, ok := v.([]byte); ok {
			return nil
		}
	case ptSliceBool:
		return validateWireBoolSlice(v)
	case ptSliceAny:
		if _, ok := v.([]any); ok {
			return nil
		}
	case ptMapStrAny:
		if _, ok := v.(map[string]any); ok {
			return nil
		}
	case ptMapStrStr:
		return validateWireStringMap(v)
	case ptCustom:
		if _, ok := v.([]byte); ok {
			return nil
		}
	}

	return fmt.Errorf("tag %d does not match value type %T", tag, v)
}

func validateWireIntRange(v any, min, max int64, name string) error {
	n, ok := wireInt64(v)
	if !ok {
		return fmt.Errorf("%s tag does not match value type %T", name, v)
	}
	if n < min || n > max {
		return fmt.Errorf("%s tag value %d outside [%d,%d]", name, n, min, max)
	}
	return nil
}

func validateWireUintRange(v any, max uint64, name string) error {
	n, ok := wireUint64(v)
	if !ok {
		return fmt.Errorf("%s tag does not match value type %T", name, v)
	}
	if n > max {
		return fmt.Errorf("%s tag value %d exceeds %d", name, n, max)
	}
	return nil
}

func wireInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	case uint8:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		if n > uint64(maxInt64) {
			return 0, false
		}
		return int64(n), true
	case uint:
		u := uint64(n)
		if u > uint64(maxInt64) {
			return 0, false
		}
		return int64(u), true
	}
	return 0, false
}

func wireUint64(v any) (uint64, bool) {
	switch n := v.(type) {
	case uint8:
		return uint64(n), true
	case uint16:
		return uint64(n), true
	case uint32:
		return uint64(n), true
	case uint64:
		return n, true
	case uint:
		return uint64(n), true
	case int8:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	case int16:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	case int32:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	case int64:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	case int:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	}
	return 0, false
}

func wireFloat64(v any) (float64, bool) {
	switch f := v.(type) {
	case float32:
		return float64(f), true
	case float64:
		return f, true
	}
	return 0, false
}

func fitsInt(n int64) bool {
	if strconv.IntSize == 32 {
		return n >= minInt32 && n <= maxInt32
	}
	return true
}

func fitsUint(n uint64) bool {
	if strconv.IntSize == 32 {
		return n <= maxUint32
	}
	return true
}

func validateWireStringSlice(v any) error {
	switch s := v.(type) {
	case []string:
		return nil
	case []any:
		for i, e := range s {
			if _, ok := e.(string); !ok {
				return fmt.Errorf("[]string tag element[%d] has type %T", i, e)
			}
		}
		return nil
	}
	return fmt.Errorf("[]string tag does not match value type %T", v)
}

func validateWireBoolSlice(v any) error {
	switch s := v.(type) {
	case []bool:
		return nil
	case []any:
		for i, e := range s {
			if _, ok := e.(bool); !ok {
				return fmt.Errorf("[]bool tag element[%d] has type %T", i, e)
			}
		}
		return nil
	}
	return fmt.Errorf("[]bool tag does not match value type %T", v)
}

func validateWireIntSlice(v any, min, max int64, name string) error {
	switch s := v.(type) {
	case []int:
		if name == "int" {
			return nil
		}
		return fmt.Errorf("[]%s tag does not match value type %T", name, v)
	case []int64:
		for i, e := range s {
			if e < min || e > max || (name == "int" && !fitsInt(e)) {
				return fmt.Errorf("[]%s tag element[%d]=%d outside target range", name, i, e)
			}
		}
		return nil
	case []any:
		for i, e := range s {
			n, ok := wireInt64(e)
			if !ok {
				return fmt.Errorf("[]%s tag element[%d] has type %T", name, i, e)
			}
			if n < min || n > max || (name == "int" && !fitsInt(n)) {
				return fmt.Errorf("[]%s tag element[%d]=%d outside target range", name, i, n)
			}
		}
		return nil
	}
	return fmt.Errorf("[]%s tag does not match value type %T", name, v)
}

func validateWireFloat32Slice(v any) error {
	switch s := v.(type) {
	case []float32:
		return nil
	case []any:
		for i, e := range s {
			f, ok := wireFloat64(e)
			if !ok {
				return fmt.Errorf("[]float32 tag element[%d] has type %T", i, e)
			}
			if !validFloat32WireValue(f) {
				return fmt.Errorf("[]float32 tag element[%d]=%g outside float32 range", i, f)
			}
		}
		return nil
	}
	return fmt.Errorf("[]float32 tag does not match value type %T", v)
}

func validFloat32WireValue(f float64) bool {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return true
	}
	if f < -maxFloat32 || f > maxFloat32 {
		return false
	}
	return float64(float32(f)) == f
}

func validateWireFloat64Slice(v any) error {
	switch s := v.(type) {
	case []float64:
		return nil
	case []any:
		for i, e := range s {
			if _, ok := wireFloat64(e); !ok {
				return fmt.Errorf("[]float64 tag element[%d] has type %T", i, e)
			}
		}
		return nil
	}
	return fmt.Errorf("[]float64 tag does not match value type %T", v)
}

func validateWireStringMap(v any) error {
	switch m := v.(type) {
	case map[string]string:
		return nil
	case map[string]any:
		for k, val := range m {
			if _, ok := val.(string); !ok {
				return fmt.Errorf("map[string]string tag value for %q has type %T", k, val)
			}
		}
		return nil
	}
	return fmt.Errorf("map[string]string tag does not match value type %T", v)
}

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
		if _, _, ok := types.RegisteredPropertyStructWireType(v); ok {
			return ptCustom
		}
		return ptUnknown
	}
}

func propertyToWire(p types.Property) (PropertyWire, error) {
	tag := PropertyTypeTag(p.Value)
	pw := PropertyWire{Key: p.Key, Type: tag}
	if tag != ptCustom {
		pw.Value = p.Value
		return pw, nil
	}

	typeName, pointer, ok := types.RegisteredPropertyStructWireType(p.Value)
	if !ok {
		return PropertyWire{}, fmt.Errorf("unregistered custom property value %T", p.Value)
	}
	data, err := msgpack.Marshal(p.Value)
	if err != nil {
		return PropertyWire{}, fmt.Errorf("marshal custom property %s: %w", typeName, err)
	}
	decoded, err := reconstructCustomPropertyValue(data, typeName, pointer)
	if err != nil {
		return PropertyWire{}, fmt.Errorf("round-trip custom property %s: %w", typeName, err)
	}
	beforeHashable, ok := p.Value.(types.HashableValue)
	if !ok {
		return PropertyWire{}, fmt.Errorf("%w: custom property %s runtime value %T does not implement HashableValue", types.ErrUnsupportedValueType, typeName, p.Value)
	}
	before, err := hashBytesChecked(beforeHashable)
	if err != nil {
		return PropertyWire{}, fmt.Errorf("custom property %s source hash: %w", typeName, err)
	}
	afterHashable, ok := decoded.(types.HashableValue)
	if !ok {
		return PropertyWire{}, fmt.Errorf("%w: custom property %s decoded value %T does not implement HashableValue", types.ErrUnsupportedValueType, typeName, decoded)
	}
	after, err := hashBytesChecked(afterHashable)
	if err != nil {
		return PropertyWire{}, fmt.Errorf("custom property %s decoded hash: %w", typeName, err)
	}
	if !bytes.Equal(before, after) {
		return PropertyWire{}, fmt.Errorf("custom property %s msgpack round-trip changed hash bytes", typeName)
	}
	pw.Value = data
	pw.CustomType = typeName
	pw.CustomPointer = pointer
	return pw, nil
}

func hashBytesChecked(v types.HashableValue) (hash []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			hash = nil
			err = fmt.Errorf("%w: custom property HashBytes panic: %v", types.ErrUnsupportedValueType, r)
		}
	}()
	return v.HashBytes(), nil
}

// reconstructTypedValue reconstructs the exact Go type from a decoded msgpack
// value using the stored type tag. Msgpack destroys type fidelity: []string
// becomes []any, int64 becomes int8 for small values, etc. The type tag
// reverses this loss.
func reconstructPropertyWireValue(p PropertyWire) (any, error) {
	if p.Type == ptCustom {
		data, ok := p.Value.([]byte)
		if !ok {
			return nil, fmt.Errorf("custom property %q value has type %T, want []byte", p.CustomType, p.Value)
		}
		return reconstructCustomPropertyValue(data, p.CustomType, p.CustomPointer)
	}
	return reconstructTypedValue(p.Value, p.Type), nil
}

func reconstructCustomPropertyValue(data []byte, typeName string, pointer bool) (any, error) {
	if typeName == "" {
		return nil, fmt.Errorf("custom property missing type name")
	}
	target, ok := types.NewRegisteredPropertyStructPointer(typeName)
	if !ok {
		return nil, fmt.Errorf("custom property type %q is not registered", typeName)
	}
	if err := msgpack.Unmarshal(data, target); err != nil {
		return nil, fmt.Errorf("unmarshal custom property %s: %w", typeName, err)
	}
	if pointer {
		return target, nil
	}
	rv := reflect.ValueOf(target)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return nil, fmt.Errorf("custom property %s target has type %T", typeName, target)
	}
	return rv.Elem().Interface(), nil
}

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
		if f, ok := wireFloat64(v); ok {
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
	case uint:
		return int64(n) // #nosec G115 — value fits, checked before reconstruction
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
	case uint:
		return uint64(n)
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
	case []int64:
		out := make([]int, len(s))
		for i, e := range s {
			out[i] = int(e)
		}
		return out
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
			if f, ok := wireFloat64(e); ok {
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
