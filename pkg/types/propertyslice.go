package types

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"time"
)

// ErrReservedPrefix is returned when a property key uses the reserved "tkg_" prefix.
var ErrReservedPrefix = errors.New("types: property key uses reserved tkg_ prefix")

// ErrEmptyPropertyKey is returned when a property key is the empty string
// (also the shape of an undecodable PropertyWire; BACKLOG 15l).
var ErrEmptyPropertyKey = errors.New("types: property key must not be empty")

// ErrUnsupportedValueType is returned when a property value contains a type
// not on the exact allowlist consumed by hashing, copying, and wire encoding.
var ErrUnsupportedValueType = errors.New("types: unsupported property value type")

// ErrUnsupportedMapType is returned when a property value is a map other than
// map[string]any or map[string]string.
var ErrUnsupportedMapType = errors.New("types: unsupported map type — only map[string]any and map[string]string are supported")

// ErrMaxDepthExceeded is returned when a property value exceeds the maximum
// nesting depth.
var ErrMaxDepthExceeded = errors.New("types: property value exceeds maximum nesting depth")

// maxPropertyDepth is the maximum nesting depth for property values.
// Validation and deep-copy functions stop recursing beyond this limit.
const maxPropertyDepth = 32

var (
	propertyTypeBool    = reflect.TypeOf(true)
	propertyTypeString  = reflect.TypeOf("")
	propertyTypeInt     = reflect.TypeOf(int(0))
	propertyTypeInt8    = reflect.TypeOf(int8(0))
	propertyTypeInt16   = reflect.TypeOf(int16(0))
	propertyTypeInt32   = reflect.TypeOf(int32(0))
	propertyTypeInt64   = reflect.TypeOf(int64(0))
	propertyTypeUint    = reflect.TypeOf(uint(0))
	propertyTypeUint8   = reflect.TypeOf(uint8(0))
	propertyTypeUint16  = reflect.TypeOf(uint16(0))
	propertyTypeUint32  = reflect.TypeOf(uint32(0))
	propertyTypeUint64  = reflect.TypeOf(uint64(0))
	propertyTypeFloat32 = reflect.TypeOf(float32(0))
	propertyTypeFloat64 = reflect.TypeOf(float64(0))

	propertyTypeSliceString  = reflect.TypeOf([]string(nil))
	propertyTypeSliceInt     = reflect.TypeOf([]int(nil))
	propertyTypeSliceInt64   = reflect.TypeOf([]int64(nil))
	propertyTypeSliceFloat32 = reflect.TypeOf([]float32(nil))
	propertyTypeSliceFloat64 = reflect.TypeOf([]float64(nil))
	propertyTypeSliceByte    = reflect.TypeOf([]byte(nil))
	propertyTypeSliceBool    = reflect.TypeOf([]bool(nil))
	propertyTypeSliceAny     = reflect.TypeOf([]any(nil))

	propertyTypeTemporalValue = reflect.TypeOf(TemporalValue{})

	propertyTypeMapStringAny    = reflect.TypeOf(map[string]any(nil))
	propertyTypeMapStringString = reflect.TypeOf(map[string]string(nil))
)

// Property is a single key-value property entry.
type Property struct {
	Key   string
	Value any
}

// PropertySlice is a sorted slice of Property entries.
// The sorted invariant is maintained by Set; never modify entries directly.
type PropertySlice []Property

// OwnedPropertySlice is a validated, canonical property slice whose ownership
// can be transferred into an entity without another copy. The unexported field
// prevents fabricating one; use NewOwnedPropertySlice. SetOwnedProperties
// consumes it so reuse cannot alias two entities.
type OwnedPropertySlice struct {
	ps *PropertySlice
}

// Set inserts or overwrites the property at key, keeping the slice sorted.
// Returns ErrReservedPrefix for a "tkg_" key, ErrEmptyPropertyKey for "",
// ErrMaxDepthExceeded beyond maxPropertyDepth (32), and a wrapped
// ErrUnsupportedValueType for any value outside the exact allowlist (unless a
// registered custom struct type). Stores a deep copy. For bulk construction
// prefer NewPropertySlice (O(N log N)) over repeated Set (O(N) each).
func (ps *PropertySlice) Set(key string, value any) error {
	if ps == nil {
		return ErrNilPropertySlice
	}
	if key == "" {
		return ErrEmptyPropertyKey
	}
	if IsShadowKey(key) {
		return fmt.Errorf("%w: %q", ErrReservedPrefix, key)
	}
	// time.Time is sugar: canonicalize to a TemporalValue before validation.
	value, _ = canonicalizeTemporalValue(value)
	if err := ValidatePropertyValue(value); err != nil {
		return fmt.Errorf("%w: %q (got %T)", err, key, value)
	}
	copied, err := copyValidatedPropertyValue(key, value)
	if err != nil {
		return err
	}
	i := (*ps).searchKey(key)
	if i < len(*ps) && (*ps)[i].Key == key {
		(*ps)[i].Value = copied
		return nil
	}
	*ps = append(*ps, Property{})
	copy((*ps)[i+1:], (*ps)[i:])
	(*ps)[i] = Property{Key: key, Value: copied}
	return nil
}

// temporalValueFromTime renders a time.Time as a zoned TemporalDateTime in
// RFC 3339 form (the zone offset round-trips through storage).
func temporalValueFromTime(t time.Time) TemporalValue {
	return TemporalValue{Kind: TemporalDateTime, Value: t.Format(time.RFC3339Nano)}
}

// canonicalizeTemporalValue rewrites a top-level time.Time to a TemporalValue so
// the rest of the pipeline never sees time.Time. A nested time.Time is not
// rewritten and is rejected by the validator (it would have to guess a kind); a
// nested TemporalValue is accepted. Non-time values pass untouched.
func canonicalizeTemporalValue(v any) (any, bool) {
	if tm, ok := v.(time.Time); ok {
		return temporalValueFromTime(tm), true
	}
	return v, false
}

func copyValidatedPropertyValue(key string, value any) (copied any, err error) {
	if isScalarPropertyValue(value) {
		return value, nil
	}
	if tv, ok := value.(TemporalValue); ok {
		return tv, nil // value type — assignment is the deep copy
	}
	defer func() {
		if r := recover(); r != nil {
			copied = nil
			err = fmt.Errorf("%w: %q deep copy panic: %v", ErrUnsupportedValueType, key, r)
		}
	}()
	copied = deepCopyValue(value, 0)
	if err := ValidatePropertyValue(copied); err != nil {
		return nil, fmt.Errorf("%w: %q after deep copy (got %T)", err, key, copied)
	}
	if hasRegisteredPropertyStructTypes() && PropertyValueHashNeedsRecover(value) {
		if err := validateCustomCopyShape(key, value, copied, 0); err != nil {
			return nil, err
		}
	}
	return copied, nil
}

func validateCustomCopyShape(key string, original, copied any, depth int) error {
	if depth > maxPropertyDepth {
		return nil
	}
	origType, origPointer, origOK := RegisteredPropertyStructWireType(original)
	if origOK {
		copiedType, copiedPointer, copiedOK := RegisteredPropertyStructWireType(copied)
		if !copiedOK || copiedType != origType || copiedPointer != origPointer {
			return fmt.Errorf("%w: %q deep copy changed custom property %s pointer=%t to %T", ErrUnsupportedValueType, key, origType, origPointer, copied)
		}
		return nil
	}

	switch orig := original.(type) {
	case []any:
		cp, ok := copied.([]any)
		if !ok || len(cp) != len(orig) {
			return fmt.Errorf("%w: %q deep copy changed []any shape from len %d to %T", ErrUnsupportedValueType, key, len(orig), copied)
		}
		for i := range orig {
			if err := validateCustomCopyShape(key, orig[i], cp[i], depth+1); err != nil {
				return err
			}
		}
	case map[string]any:
		cp, ok := copied.(map[string]any)
		if !ok || len(cp) != len(orig) {
			return fmt.Errorf("%w: %q deep copy changed map[string]any shape from len %d to %T", ErrUnsupportedValueType, key, len(orig), copied)
		}
		for k, origValue := range orig {
			copiedValue, ok := cp[k]
			if !ok {
				return fmt.Errorf("%w: %q deep copy dropped map key %q", ErrUnsupportedValueType, key, k)
			}
			if err := validateCustomCopyShape(key, origValue, copiedValue, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func isScalarPropertyValue(v any) bool {
	switch v.(type) {
	case nil,
		bool, string,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return true
	default:
		return false
	}
}

// searchKey returns the index of the first entry whose Key is >= key, by an
// inline binary search (sort.Search's closure cannot inline and dominated the
// per-row property read).
func (ps PropertySlice) searchKey(key string) int {
	lo, hi := 0, len(ps)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1) // avoid overflow
		if ps[mid].Key < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// Get returns the value for key and whether it was found; reference-type
// values are deep-copied.
func (ps PropertySlice) Get(key string) (any, bool) {
	i := ps.searchKey(key)
	if i < len(ps) && ps[i].Key == key {
		return deepCopyValue(ps[i].Value, 0), true
	}
	return nil, false
}

func (ps PropertySlice) propertyValueEqual(key string, expected any) (bool, bool) {
	i := ps.searchKey(key)
	if i >= len(ps) || ps[i].Key != key {
		return false, false
	}
	return true, PropertyValueEqual(ps[i].Value, expected)
}

func (ps PropertySlice) indexableValueKey(key string) (string, bool) {
	i := ps.searchKey(key)
	if i < len(ps) && ps[i].Key == key {
		return IndexablePropertyValueKey(ps[i].Value), true
	}
	return "", false
}

// forEachIndexKey is the one key iteration every property index consumes: a
// scalar yields its IndexablePropertyValueKey; a list whose key elements
// reports true (Elements index, backlog 63) yields each distinct element key.
// elements is asked for list values only; nil means scalars only.
func (ps PropertySlice) forEachIndexKey(elements func(propertyKey string) bool, fn func(propertyKey, valueKey string) bool) {
	if fn == nil {
		return
	}
	for _, p := range ps {
		if valueKey := IndexablePropertyValueKey(p.Value); valueKey != "" {
			if !fn(p.Key, valueKey) {
				return
			}
			continue
		}
		if elements == nil || !isIndexableList(p.Value) || !elements(p.Key) {
			continue
		}
		key := p.Key
		if !ForEachIndexableElementKey(p.Value, func(valueKey string) bool { return fn(key, valueKey) }) {
			return
		}
	}
}

// propertyIndexKeys calls fn with the index keys of key's value (see
// ForEachIndexableElementKey; elements=false: the scalar key only).
func (ps PropertySlice) propertyIndexKeys(key string, elements bool, fn func(valueKey string) bool) {
	i := ps.searchKey(key)
	if i >= len(ps) || ps[i].Key != key || fn == nil {
		return
	}
	if !elements {
		if vk := IndexablePropertyValueKey(ps[i].Value); vk != "" {
			fn(vk)
		}
		return
	}
	ForEachIndexableElementKey(ps[i].Value, fn)
}

func (ps PropertySlice) hasIndexKey(key string, elements bool, targetKey string) bool {
	i := ps.searchKey(key)
	if i >= len(ps) || ps[i].Key != key {
		return false
	}
	return IndexableValueHasKey(ps[i].Value, elements, targetKey)
}

// isIndexableList reports whether v is a list an Elements index expands.
func isIndexableList(v any) bool {
	switch v.(type) {
	case []any, []string, []int, []int64, []float32, []float64, []bool:
		return true
	}
	return false
}

// ForEachIndexableElementKey calls fn with the keys an Elements index takes
// from v (backlog 63): a scalar's key (a one-element list), else each distinct
// indexable scalar element of a list ([]any, []string, []int, []int64,
// []float32, []float64, []bool) in first-occurrence order. Keyless elements
// (nil, nested lists, maps) are skipped; []byte is a blob. Returns false when
// fn stopped the iteration.
func ForEachIndexableElementKey(v any, fn func(valueKey string) bool) bool {
	if fn == nil {
		return true
	}
	if vk := IndexablePropertyValueKey(v); vk != "" {
		return fn(vk)
	}
	switch s := v.(type) {
	case []any:
		return forEachElementKey(s, fn)
	case []string:
		return forEachElementKey(s, fn)
	case []int:
		return forEachElementKey(s, fn)
	case []int64:
		return forEachElementKey(s, fn)
	case []float32:
		return forEachElementKey(s, fn)
	case []float64:
		return forEachElementKey(s, fn)
	case []bool:
		return forEachElementKey(s, fn)
	}
	return true
}

// elementKeyDedupLinear is the list length up to which duplicate keys are found
// by linear scan (no map).
const elementKeyDedupLinear = 16

func forEachElementKey[T any](s []T, fn func(valueKey string) bool) bool {
	var small [elementKeyDedupLinear]string
	seen := small[:0]
	var many map[string]struct{}
	for _, e := range s {
		vk := IndexablePropertyValueKey(e)
		if vk == "" {
			continue
		}
		switch {
		case many != nil:
			if _, dup := many[vk]; dup {
				continue
			}
			many[vk] = struct{}{}
		case slices.Contains(seen, vk):
			continue
		case len(seen) < elementKeyDedupLinear:
			seen = append(seen, vk)
		default:
			many = make(map[string]struct{}, len(s))
			for _, k := range seen {
				many[k] = struct{}{}
			}
			many[vk] = struct{}{}
		}
		if !fn(vk) {
			return false
		}
	}
	return true
}

// IndexableValueHasKey reports whether v carries targetKey as an index key: its
// scalar key equals it or, with elements, one of its element keys does. An
// empty targetKey never matches.
func IndexableValueHasKey(v any, elements bool, targetKey string) bool {
	if targetKey == "" {
		return false
	}
	if vk := IndexablePropertyValueKey(v); vk != "" || !elements {
		return vk == targetKey
	}
	found := false
	ForEachIndexableElementKey(v, func(vk string) bool {
		found = vk == targetKey
		return !found
	})
	return found
}

func (ps PropertySlice) float32SliceCopy(key string) ([]float32, bool) {
	i := ps.searchKey(key)
	if i >= len(ps) || ps[i].Key != key {
		return nil, false
	}
	switch v := ps[i].Value.(type) {
	case []float32:
		if v == nil {
			return nil, true
		}
		out := make([]float32, len(v))
		copy(out, v)
		return out, true
	case []float64:
		if v == nil {
			return nil, true
		}
		out := make([]float32, len(v))
		for i, f := range v {
			out[i] = float32(f)
		}
		return out, true
	case []any:
		if v == nil {
			return nil, true
		}
		out := make([]float32, len(v))
		for i, elem := range v {
			switch f := elem.(type) {
			case float32:
				out[i] = f
			case float64:
				out[i] = float32(f)
			default:
				return nil, false
			}
		}
		return out, true
	default:
		return nil, false
	}
}

// IndexablePropertyValueKey computes a canonical key for values that can be
// indexed (float keys follow PropertyValueEqual for signed zero and NaN, and
// keep float32/float64 separate); "" for slices, maps, structs and other
// non-scalars.
func IndexablePropertyValueKey(v any) string {
	switch val := v.(type) {
	case string:
		return "s:" + val
	case TemporalValue:
		// Type-prefixed so a temporal never collides with the plain string.
		var buf [48]byte
		b := strconv.AppendUint(append(buf[:0], "tv:"...), uint64(val.Kind), 10)
		b = append(b, ':')
		b = append(b, val.Value...)
		return string(b)
	case int:
		return appendIntKey("i:", int64(val))
	case int8:
		return appendIntKey("i8:", int64(val))
	case int16:
		return appendIntKey("i16:", int64(val))
	case int32:
		return appendIntKey("i32:", int64(val))
	case int64:
		return appendIntKey("i64:", val)
	case uint:
		return appendUintKey("u:", uint64(val))
	case uint8:
		return appendUintKey("u8:", uint64(val))
	case uint16:
		return appendUintKey("u16:", uint64(val))
	case uint32:
		return appendUintKey("u32:", uint64(val))
	case uint64:
		return appendUintKey("u64:", val)
	case float32:
		if val == 0 {
			return "f32:0"
		}
		if math.IsNaN(float64(val)) {
			return "f32:nan"
		}
		return appendFloatKey("f32:", float64(val), 32)
	case float64:
		if val == 0 {
			return "f64:0"
		}
		if math.IsNaN(val) {
			return "f64:nan"
		}
		return appendFloatKey("f64:", val, 64)
	case bool:
		if val {
			return "b:true"
		}
		return "b:false"
	default:
		return ""
	}
}

// appendIntKey / appendUintKey / appendFloatKey build a "prefix:number" index
// key in a single allocation (stack buffer sized for the longest output).
func appendIntKey(prefix string, val int64) string {
	var buf [32]byte
	return string(strconv.AppendInt(append(buf[:0], prefix...), val, 10))
}

func appendUintKey(prefix string, val uint64) string {
	var buf [32]byte
	return string(strconv.AppendUint(append(buf[:0], prefix...), val, 10))
}

func appendFloatKey(prefix string, val float64, bitSize int) string {
	var buf [40]byte
	return string(strconv.AppendFloat(append(buf[:0], prefix...), val, 'g', -1, bitSize))
}

// PropertyValueEqual compares two property values with the equality used by
// compare-and-set: concrete types must match and NaNs compare equal, also when
// nested.
//
// Hash divergence by design: the integrity hash keeps the raw IEEE-754 bits, so
// NaNs (and +-0) equal here can hash differently; a no-op CAS between them does
// not advance the hash chain. See pkg/graph/internal/integrity/integrity_test.go.
func PropertyValueEqual(cur, expected any) bool {
	return propertyValueEqual(cur, expected, nil)
}

func propertyValueEqual(cur, expected any, seen map[propertyValueVisit]struct{}) bool {
	if cur == nil || expected == nil {
		return cur == nil && expected == nil
	}
	if reflect.TypeOf(cur) != reflect.TypeOf(expected) {
		return false
	}

	switch curVal := cur.(type) {
	case float32:
		return float32PropertyValueEqual(curVal, expected.(float32))
	case float64:
		return float64PropertyValueEqual(curVal, expected.(float64))
	case []float32:
		expectedVal := expected.([]float32)
		if (curVal == nil) != (expectedVal == nil) || len(curVal) != len(expectedVal) {
			return false
		}
		for i := range curVal {
			if !float32PropertyValueEqual(curVal[i], expectedVal[i]) {
				return false
			}
		}
		return true
	case []float64:
		expectedVal := expected.([]float64)
		if (curVal == nil) != (expectedVal == nil) || len(curVal) != len(expectedVal) {
			return false
		}
		for i := range curVal {
			if !float64PropertyValueEqual(curVal[i], expectedVal[i]) {
				return false
			}
		}
		return true
	case []any:
		expectedVal := expected.([]any)
		if (curVal == nil) != (expectedVal == nil) || len(curVal) != len(expectedVal) {
			return false
		}
		var alreadySeen bool
		seen, alreadySeen = markPropertyValueVisit(reflect.ValueOf(curVal), reflect.ValueOf(expectedVal), seen)
		if alreadySeen {
			return true
		}
		for i := range curVal {
			if !propertyValueEqual(curVal[i], expectedVal[i], seen) {
				return false
			}
		}
		return true
	case map[string]any:
		expectedVal := expected.(map[string]any)
		if (curVal == nil) != (expectedVal == nil) || len(curVal) != len(expectedVal) {
			return false
		}
		var alreadySeen bool
		seen, alreadySeen = markPropertyValueVisit(reflect.ValueOf(curVal), reflect.ValueOf(expectedVal), seen)
		if alreadySeen {
			return true
		}
		for k, curElem := range curVal {
			expectedElem, ok := expectedVal[k]
			if !ok || !propertyValueEqual(curElem, expectedElem, seen) {
				return false
			}
		}
		return true
	default:
		return reflectPropertyValueEqualSeen(reflect.ValueOf(cur), reflect.ValueOf(expected), seen)
	}
}

type propertyValueVisit struct {
	typ      reflect.Type
	cur      uintptr
	expected uintptr
}

func reflectPropertyValueEqualSeen(cur, expected reflect.Value, seen map[propertyValueVisit]struct{}) bool {
	if !cur.IsValid() || !expected.IsValid() {
		return !cur.IsValid() && !expected.IsValid()
	}
	if cur.Type() != expected.Type() {
		return false
	}

	switch cur.Kind() {
	case reflect.Bool:
		return cur.Bool() == expected.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return cur.Int() == expected.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return cur.Uint() == expected.Uint()
	case reflect.Float32:
		return float32PropertyValueEqual(float32(cur.Float()), float32(expected.Float()))
	case reflect.Float64:
		return float64PropertyValueEqual(cur.Float(), expected.Float())
	case reflect.String:
		return cur.String() == expected.String()
	case reflect.Pointer, reflect.Interface:
		if cur.IsNil() || expected.IsNil() {
			return cur.IsNil() && expected.IsNil()
		}
		if cur.Kind() == reflect.Pointer {
			var alreadySeen bool
			seen, alreadySeen = markPropertyValueVisit(cur, expected, seen)
			if alreadySeen {
				return true
			}
		}
		return reflectPropertyValueEqualSeen(cur.Elem(), expected.Elem(), seen)
	case reflect.Struct:
		for i := range cur.NumField() {
			if !reflectPropertyValueEqualSeen(cur.Field(i), expected.Field(i), seen) {
				return false
			}
		}
		return true
	case reflect.Slice, reflect.Array:
		if cur.Kind() == reflect.Slice && (cur.IsNil() || expected.IsNil()) {
			return cur.IsNil() && expected.IsNil()
		}
		if cur.Len() != expected.Len() {
			return false
		}
		if cur.Kind() == reflect.Slice {
			var alreadySeen bool
			seen, alreadySeen = markPropertyValueVisit(cur, expected, seen)
			if alreadySeen {
				return true
			}
		}
		for i := range cur.Len() {
			if !reflectPropertyValueEqualSeen(cur.Index(i), expected.Index(i), seen) {
				return false
			}
		}
		return true
	case reflect.Map:
		if cur.IsNil() || expected.IsNil() {
			return cur.IsNil() && expected.IsNil()
		}
		if cur.Len() != expected.Len() {
			return false
		}
		var alreadySeen bool
		seen, alreadySeen = markPropertyValueVisit(cur, expected, seen)
		if alreadySeen {
			return true
		}
		iter := cur.MapRange()
		for iter.Next() {
			expectedValue := expected.MapIndex(iter.Key())
			if !expectedValue.IsValid() || !reflectPropertyValueEqualSeen(iter.Value(), expectedValue, seen) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func markPropertyValueVisit(cur, expected reflect.Value, seen map[propertyValueVisit]struct{}) (map[propertyValueVisit]struct{}, bool) {
	if cur.Type() != expected.Type() {
		return seen, false
	}
	switch cur.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice:
		if cur.IsNil() || expected.IsNil() {
			return seen, false
		}
	default:
		return seen, false
	}

	visit := propertyValueVisit{
		typ:      cur.Type(),
		cur:      cur.Pointer(),
		expected: expected.Pointer(),
	}
	if _, ok := seen[visit]; ok {
		return seen, true
	}
	if seen == nil {
		seen = make(map[propertyValueVisit]struct{}, 4)
	}
	seen[visit] = struct{}{}
	return seen, false
}

func float32PropertyValueEqual(a, b float32) bool {
	return a == b || (math.IsNaN(float64(a)) && math.IsNaN(float64(b)))
}

func float64PropertyValueEqual(a, b float64) bool {
	return a == b || (math.IsNaN(a) && math.IsNaN(b))
}

// DeepCopy returns an independent copy; reference-type values are cloned.
func (ps PropertySlice) DeepCopy() PropertySlice {
	if ps == nil {
		return nil
	}
	cp := make(PropertySlice, len(ps))
	for i, p := range ps {
		cp[i] = Property{Key: p.Key, Value: deepCopyValue(p.Value, 0)}
	}
	return cp
}

// ValidatePropertyValue recursively checks that v contains only allowlisted
// exact types; it rejects beyond maxPropertyDepth.
func ValidatePropertyValue(v any) error {
	if v == nil {
		return nil
	}
	if isScalarPropertyValue(v) {
		return nil
	}
	if tv, ok := v.(TemporalValue); ok {
		return tv.Validate()
	}
	// time.Time is sugar, canonicalized to a TemporalValue before storage.
	if _, ok := v.(time.Time); ok {
		return nil
	}
	return validateReflectValue(reflect.ValueOf(v), 0)
}

// validateReflectValue accepts only types that hashing, deep-copy and wire
// reconstruction handle losslessly; containers recurse with a depth counter.
func validateReflectValue(rv reflect.Value, depth int) error {
	if depth > maxPropertyDepth {
		return ErrMaxDepthExceeded
	}

	switch rv.Kind() {
	// Exact scalar types supported by hash/copy/wire.
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		if isAllowedPropertyScalarType(rv.Type()) {
			return nil
		}
		return fmt.Errorf("%w: %s", ErrUnsupportedValueType, rv.Type())

	// Containers — recurse only where the supported shape can hide any values.
	case reflect.Interface:
		if rv.IsNil() {
			return nil
		}
		// Interface unwrapping is not a nesting level — just type unwrapping.
		return validateReflectValue(rv.Elem(), depth)

	case reflect.Slice:
		if !isAllowedPropertySliceType(rv.Type()) {
			return fmt.Errorf("%w: %s", ErrUnsupportedValueType, rv.Type())
		}
		if rv.IsNil() {
			return nil
		}
		if rv.Type() != propertyTypeSliceAny {
			return nil
		}
		for i := range rv.Len() {
			if err := validateReflectValue(rv.Index(i), depth+1); err != nil {
				return err
			}
		}
		return nil

	case reflect.Map:
		mt := rv.Type()
		if mt != propertyTypeMapStringAny && mt != propertyTypeMapStringString {
			return fmt.Errorf("%w: %s", ErrUnsupportedMapType, mt)
		}
		if rv.IsNil() {
			return nil
		}
		if mt == propertyTypeMapStringAny {
			iter := rv.MapRange()
			for iter.Next() {
				if err := validateReflectValue(iter.Value(), depth+1); err != nil {
					return err
				}
			}
		}
		return nil

	// Pointer and Struct are accepted if the type has been registered via
	// RegisterPropertyStructType (e.g. for spatial geometry types). This is
	// the opt-in extension point; unregistered structs/pointers are rejected.
	case reflect.Pointer, reflect.Struct:
		// TemporalValue is accepted at any depth (nested temporals round-trip
		// via a reversible wire envelope; a nested plain string is not reinterpreted).
		if rv.Kind() == reflect.Struct && rv.Type() == propertyTypeTemporalValue {
			tv, ok := rv.Interface().(TemporalValue)
			if !ok {
				return ErrUnsupportedValueType
			}
			return tv.Validate()
		}
		if isRegisteredPropertyStructType(rv) {
			return nil
		}
		return ErrUnsupportedValueType

	// Everything else (Array, Chan, Func, UnsafePointer, etc.) is rejected.
	default:
		return ErrUnsupportedValueType
	}
}

func isAllowedPropertyScalarType(t reflect.Type) bool {
	switch t {
	case propertyTypeBool,
		propertyTypeString,
		propertyTypeInt,
		propertyTypeInt8,
		propertyTypeInt16,
		propertyTypeInt32,
		propertyTypeInt64,
		propertyTypeUint,
		propertyTypeUint8,
		propertyTypeUint16,
		propertyTypeUint32,
		propertyTypeUint64,
		propertyTypeFloat32,
		propertyTypeFloat64:
		return true
	default:
		return false
	}
}

func isAllowedPropertySliceType(t reflect.Type) bool {
	switch t {
	case propertyTypeSliceString,
		propertyTypeSliceInt,
		propertyTypeSliceInt64,
		propertyTypeSliceFloat32,
		propertyTypeSliceFloat64,
		propertyTypeSliceByte,
		propertyTypeSliceBool,
		propertyTypeSliceAny:
		return true
	default:
		return false
	}
}

// deepCopyValue clones known reference types; unknown types shallow-copy (safe
// for immutables). Stops at maxPropertyDepth.
func deepCopyValue(v any, depth int) any {
	if v == nil {
		return nil
	}
	if depth > maxPropertyDepth {
		return v // safety fallback: return as-is
	}

	switch val := v.(type) {
	// Scalar types — immutable, return as-is without reflect overhead.
	case bool, string,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return val

	// TemporalValue is immutable: assignment is the deep copy.
	case TemporalValue:
		return val

	// Common slice types.
	case []string:
		if val == nil {
			return val
		}
		cp := make([]string, len(val))
		copy(cp, val)
		return cp
	case []int:
		if val == nil {
			return val
		}
		cp := make([]int, len(val))
		copy(cp, val)
		return cp
	case []int64:
		if val == nil {
			return val
		}
		cp := make([]int64, len(val))
		copy(cp, val)
		return cp
	case []float32:
		if val == nil {
			return val
		}
		cp := make([]float32, len(val))
		copy(cp, val)
		return cp
	case []float64:
		if val == nil {
			return val
		}
		cp := make([]float64, len(val))
		copy(cp, val)
		return cp
	case []byte:
		if val == nil {
			return val
		}
		cp := make([]byte, len(val))
		copy(cp, val)
		return cp
	case []bool:
		if val == nil {
			return val
		}
		cp := make([]bool, len(val))
		copy(cp, val)
		return cp
	case []any:
		if val == nil {
			return val
		}
		cp := make([]any, len(val))
		for i, elem := range val {
			cp[i] = deepCopyValue(elem, depth+1)
		}
		return cp

	// Common map types.
	case map[string]any:
		if val == nil {
			return val
		}
		cp := make(map[string]any, len(val))
		for k, elem := range val {
			cp[k] = deepCopyValue(elem, depth+1)
		}
		return cp
	case map[string]string:
		if val == nil {
			return val
		}
		cp := make(map[string]string, len(val))
		for k, elem := range val {
			cp[k] = elem
		}
		return cp

	default:
		// Registered custom types own their deep copy; the registration check
		// stops fabricated invalid slices from invoking unregistered user code.
		if dc, ok := v.(DeepCopier); ok && isRegisteredPropertyValue(v) {
			return preserveDeepCopyPointerShape(v, dc.DeepCopyValue())
		}
		// Reflect fallback for any other slice or map type.
		return reflectCopyValue(v, depth)
	}
}

func isRegisteredPropertyValue(v any) bool {
	if v == nil {
		return false
	}
	return isRegisteredPropertyStructType(reflect.ValueOf(v))
}

func hasRegisteredPropertyStructTypes() bool {
	propertyStructRegistryMu.RLock()
	hasTypes := len(propertyStructRegistry) != 0
	propertyStructRegistryMu.RUnlock()
	return hasTypes
}

func preserveDeepCopyPointerShape(original, copied any) any {
	if original == nil || copied == nil {
		return copied
	}
	origT := reflect.TypeOf(original)
	copiedT := reflect.TypeOf(copied)
	if origT.Kind() != reflect.Pointer {
		if copiedT.Kind() != reflect.Pointer {
			return copied
		}
		copiedV := reflect.ValueOf(copied)
		if copiedV.IsNil() || !copiedT.Elem().AssignableTo(origT) {
			return copied
		}
		return copiedV.Elem().Interface()
	}
	if copiedT == origT {
		return copied
	}
	elemT := origT.Elem()
	if !copiedT.AssignableTo(elemT) {
		return copied
	}
	ptr := reflect.New(elemT)
	ptr.Elem().Set(reflect.ValueOf(copied))
	return ptr.Interface()
}

// reflectCopyValue clones unknown slice and map types via reflection, deep-copying
// elements. Unsupported types never reach it (Set rejects them).
func reflectCopyValue(v any, depth int) any {
	if depth > maxPropertyDepth {
		return v // safety fallback: return as-is
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice:
		if rv.IsNil() {
			return v
		}
		cp := reflect.MakeSlice(rv.Type(), rv.Len(), rv.Len())
		for i := range rv.Len() {
			elem := deepCopyValue(rv.Index(i).Interface(), depth+1)
			if elemValue, ok := reflectValueForType(elem, cp.Index(i).Type()); ok {
				cp.Index(i).Set(elemValue)
			} else {
				cp.Index(i).Set(rv.Index(i))
			}
		}
		return cp.Interface()
	case reflect.Map:
		if rv.IsNil() {
			return v
		}
		cp := reflect.MakeMapWithSize(rv.Type(), rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			elem := deepCopyValue(iter.Value().Interface(), depth+1)
			if elemValue, ok := reflectValueForType(elem, rv.Type().Elem()); ok {
				cp.SetMapIndex(iter.Key(), elemValue)
			} else {
				cp.SetMapIndex(iter.Key(), iter.Value())
			}
		}
		return cp.Interface()
	default:
		return v
	}
}

func reflectValueForType(value any, target reflect.Type) (reflect.Value, bool) {
	if value == nil {
		return reflect.Zero(target), true
	}
	rv := reflect.ValueOf(value)
	if rv.Type().AssignableTo(target) {
		return rv, true
	}
	if rv.Type().ConvertibleTo(target) {
		return rv.Convert(target), true
	}
	return reflect.Value{}, false
}

// Delete removes the property at key.
// Returns true if the key was found and removed, false if it was not present.
// Returns an error if key has the reserved "tkg_" prefix.
func (ps *PropertySlice) Delete(key string) (bool, error) {
	if ps == nil {
		return false, ErrNilPropertySlice
	}
	if IsShadowKey(key) {
		return false, fmt.Errorf("%w: %q", ErrReservedPrefix, key)
	}
	i := (*ps).searchKey(key)
	if i >= len(*ps) || (*ps)[i].Key != key {
		return false, nil
	}
	copy((*ps)[i:], (*ps)[i+1:])
	(*ps)[len(*ps)-1] = Property{}
	*ps = (*ps)[:len(*ps)-1]
	return true, nil
}

// ToMap converts the PropertySlice to a map.
// Values are deep-copied so mutations to the map never affect internal state.
func (ps PropertySlice) ToMap() map[string]any {
	m := make(map[string]any, len(ps))
	for _, p := range ps {
		m[p.Key] = deepCopyValue(p.Value, 0)
	}
	return m
}

// Len returns the number of properties.
func (ps PropertySlice) Len() int {
	return len(ps)
}

// canonicalPropertySlice validates, de-duplicates, sorts, and deep-copies a
// caller-provided PropertySlice before it is installed on an entity.
func canonicalPropertySlice(ps PropertySlice) (PropertySlice, error) {
	if len(ps) == 0 {
		return nil, nil
	}

	sortedUnique := true
	for i, p := range ps {
		if p.Key == "" {
			return nil, ErrEmptyPropertyKey
		}
		if IsShadowKey(p.Key) {
			return nil, fmt.Errorf("%w: %q", ErrReservedPrefix, p.Key)
		}
		if i > 0 && p.Key <= ps[i-1].Key {
			sortedUnique = false
		}
	}
	if sortedUnique {
		out := make(PropertySlice, len(ps))
		for i, p := range ps {
			copied, err := canonicalPropertyValue(p.Key, p.Value)
			if err != nil {
				return nil, err
			}
			out[i] = Property{Key: p.Key, Value: copied}
		}
		return out, nil
	}

	latest := make(map[string]any, len(ps))
	for _, p := range ps {
		latest[p.Key] = p.Value
	}
	keys := make([]string, 0, len(latest))
	for key := range latest {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	out := make(PropertySlice, len(keys))
	for i, key := range keys {
		copied, err := canonicalPropertyValue(key, latest[key])
		if err != nil {
			return nil, err
		}
		out[i] = Property{Key: key, Value: copied}
	}
	return out, nil
}

func canonicalPropertyValue(key string, value any) (any, error) {
	// time.Time sugar, as in Set, so SetProperties hashes like Set (BACKLOG 6a).
	value, _ = canonicalizeTemporalValue(value)
	if err := ValidatePropertyValue(value); err != nil {
		return nil, fmt.Errorf("%w: %q (got %T)", err, key, value)
	}
	return copyValidatedPropertyValue(key, value)
}

// NewPropertySlice creates a PropertySlice from a map in O(N log N) (allocate
// once, validate, deep-copy, sort once). Returns nil, nil for nil or empty maps,
// ErrReservedPrefix for a "tkg_" key, ErrUnsupportedValueType for a bad value.
func NewPropertySlice(m map[string]any) (PropertySlice, error) {
	if len(m) == 0 {
		return nil, nil
	}
	ps := make(PropertySlice, 0, len(m))
	for k, v := range m {
		if k == "" {
			return nil, ErrEmptyPropertyKey
		}
		if IsShadowKey(k) {
			return nil, fmt.Errorf("%w: %q", ErrReservedPrefix, k)
		}
		v, _ = canonicalizeTemporalValue(v)
		if err := ValidatePropertyValue(v); err != nil {
			return nil, fmt.Errorf("%w: %q (got %T)", err, k, v)
		}
		copied, err := copyValidatedPropertyValue(k, v)
		if err != nil {
			return nil, err
		}
		ps = append(ps, Property{Key: k, Value: copied})
	}
	slices.SortFunc(ps, func(a, b Property) int {
		if a.Key < b.Key {
			return -1
		}
		if a.Key > b.Key {
			return 1
		}
		return 0
	})
	return ps, nil
}

// NewOwnedPropertySlice creates a validated, canonical property slice for
// ownership transfer into a newly constructed entity.
func NewOwnedPropertySlice(m map[string]any) (OwnedPropertySlice, error) {
	ps, err := NewPropertySlice(m)
	if err != nil {
		return OwnedPropertySlice{}, err
	}
	return OwnedPropertySlice{ps: &ps}, nil
}
