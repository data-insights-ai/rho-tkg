package types

import (
	"math"
	"slices"
	"strconv"
	"strings"
)

// NumericFloat64 widens a stored number of any width to float64 (a uint64 or
// int64 beyond 2^53 rounds); ok=false for a non-number. The one numeric
// widening of the ordered views and the statistics; equality never goes
// through it (EqualIndexValues, NumericEqual and CompareNumber are exact).
func NumericFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int8:
		return float64(x), true
	case int16:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint:
		return float64(x), true
	case uint8:
		return float64(x), true
	case uint16:
		return float64(x), true
	case uint32:
		return float64(x), true
	case uint64:
		return float64(x), true
	case float32:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}

// EqualIndexValues returns, one per index key (IndexablePropertyValueKey),
// every stored value that equals v under the property equality of the graph's
// equality doors (ByLabelAndProperty / ByTypeAndProperty, current and temporal,
// the composite lookup, the paged effective doors' filter, the equality
// counts, compare-and-set and the unique constraints): a number equals every
// stored number of the same exact value in any width (3 = int8(3) = uint(3) =
// 3.0 = float32(3); a float32 as its float64 widening; -0.0 = 0; +Inf = +Inf;
// no rounding: int64(2^53+1) is not float64(2^53)); NaN and nil equal nothing
// (nil result); a string, bool or temporal value equals itself only; a list,
// map or other non-indexable value has no index key (nil result).
func EqualIndexValues(v any) []any {
	switch x := v.(type) {
	case nil:
		return nil
	case int:
		return equalInt(int64(x))
	case int8:
		return equalInt(int64(x))
	case int16:
		return equalInt(int64(x))
	case int32:
		return equalInt(int64(x))
	case int64:
		return equalInt(x)
	case uint:
		return equalUint(uint64(x))
	case uint8:
		return equalUint(uint64(x))
	case uint16:
		return equalUint(uint64(x))
	case uint32:
		return equalUint(uint64(x))
	case uint64:
		return equalUint(x)
	case float32:
		return equalFloat(float64(x))
	case float64:
		return equalFloat(x)
	}
	if IndexablePropertyValueKey(v) == "" {
		return nil
	}
	return []any{v}
}

// EqualIndexKeys are the index keys of EqualIndexValues(v): the keys a value
// equal to v can be stored under (at most 12).
func EqualIndexKeys(v any) []string {
	forms := EqualIndexValues(v)
	if len(forms) == 0 {
		return nil
	}
	keys := make([]string, len(forms))
	for i, f := range forms {
		keys[i] = IndexablePropertyValueKey(f)
	}
	return keys
}

// maxEqualForms is the most forms a number has (EqualIndexValues): five
// signed and five unsigned integer widths and two float widths.
const maxEqualForms = 12

// twoTo63 and twoTo64 are 2^63 and 2^64 as float64 (exact).
const (
	twoTo63 = float64(1 << 63)
	twoTo64 = 2 * twoTo63
)

// equalInt returns the forms of the integer i: its integer widths, and the
// float widths that hold it exactly.
func equalInt(i int64) []any {
	if i >= 0 {
		return equalUint(uint64(i))
	}
	out := appendSignedWidths(make([]any, 0, maxEqualForms), i)
	if f := float64(i); f >= -twoTo63 && int64(f) == i {
		out = appendFloatWidths(out, f)
	}
	return out
}

// equalUint is equalInt for a non-negative integer.
func equalUint(u uint64) []any {
	out := make([]any, 0, maxEqualForms)
	if u <= math.MaxInt64 {
		out = appendSignedWidths(out, int64(u))
	}
	out = appendUnsignedWidths(out, u)
	if f := float64(u); f < twoTo64 && uint64(f) == u {
		out = appendFloatWidths(out, f)
	}
	return out
}

// equalFloat returns the forms of f: its float widths, and the integer widths
// that hold it exactly. NaN has none.
func equalFloat(f float64) []any {
	if math.IsNaN(f) {
		return nil
	}
	out := appendFloatWidths(make([]any, 0, maxEqualForms), f)
	if math.IsInf(f, 0) || f != math.Trunc(f) {
		return out
	}
	switch {
	case f < 0 && f >= -twoTo63:
		out = appendSignedWidths(out, int64(f))
	case f >= 0 && f < twoTo64:
		u := uint64(f)
		if u <= math.MaxInt64 {
			out = appendSignedWidths(out, int64(u))
		}
		out = appendUnsignedWidths(out, u)
	}
	return out
}

// appendFloatWidths appends f as float64 and, when float32 holds it exactly,
// as float32.
func appendFloatWidths(out []any, f float64) []any {
	out = append(out, f)
	if f32 := float32(f); float64(f32) == f {
		out = append(out, f32)
	}
	return out
}

// appendSignedWidths appends i in every signed width that holds it.
func appendSignedWidths(out []any, i int64) []any {
	out = append(out, i)
	if i >= math.MinInt && i <= math.MaxInt {
		out = append(out, int(i))
	}
	if i >= math.MinInt32 && i <= math.MaxInt32 {
		out = append(out, int32(i))
	}
	if i >= math.MinInt16 && i <= math.MaxInt16 {
		out = append(out, int16(i))
	}
	if i >= math.MinInt8 && i <= math.MaxInt8 {
		out = append(out, int8(i))
	}
	return out
}

// appendUnsignedWidths appends u in every unsigned width that holds it.
func appendUnsignedWidths(out []any, u uint64) []any {
	out = append(out, u)
	if u <= math.MaxUint {
		out = append(out, uint(u))
	}
	if u <= math.MaxUint32 {
		out = append(out, uint32(u))
	}
	if u <= math.MaxUint16 {
		out = append(out, uint16(u))
	}
	if u <= math.MaxUint8 {
		out = append(out, uint8(u))
	}
	return out
}

// NumericEqual reports whether a and b are numbers of the same exact value
// under the property equality (EqualIndexValues); false when either is not a
// number or is NaN.
func NumericEqual(a, b any) bool {
	if _, ok := NumericFloat64(b); !ok {
		return false
	}
	return slices.Contains(EqualIndexKeys(a), IndexablePropertyValueKey(b))
}

// CompareNumber compares the stored number v with f exactly (an integer past
// 2^53 is not rounded): -1, 0 or +1; ok=false when v is not a number or v or
// f is NaN.
func CompareNumber(v any, f float64) (int, bool) {
	if math.IsNaN(f) {
		return 0, false
	}
	switch x := v.(type) {
	case float32:
		return compareFloat(float64(x), f)
	case float64:
		return compareFloat(x, f)
	case uint:
		return compareUint(uint64(x), f), true
	case uint8:
		return compareUint(uint64(x), f), true
	case uint16:
		return compareUint(uint64(x), f), true
	case uint32:
		return compareUint(uint64(x), f), true
	case uint64:
		return compareUint(x, f), true
	case int:
		return compareInt(int64(x), f), true
	case int8:
		return compareInt(int64(x), f), true
	case int16:
		return compareInt(int64(x), f), true
	case int32:
		return compareInt(int64(x), f), true
	case int64:
		return compareInt(x, f), true
	}
	return 0, false
}

func compareFloat(a, f float64) (int, bool) {
	switch {
	case math.IsNaN(a):
		return 0, false
	case a < f:
		return -1, true
	case a > f:
		return 1, true
	}
	return 0, true
}

// compareInt compares i with f exactly.
func compareInt(i int64, f float64) int {
	switch {
	case f >= twoTo63:
		return -1
	case f < -twoTo63:
		return 1
	}
	t := math.Trunc(f)
	ti := int64(t)
	switch {
	case i < ti:
		return -1
	case i > ti:
		return 1
	case f > t:
		return -1
	case f < t:
		return 1
	}
	return 0
}

// compareUint compares u with f exactly.
func compareUint(u uint64, f float64) int {
	switch {
	case f < 0:
		return 1
	case f >= twoTo64:
		return -1
	}
	t := math.Trunc(f)
	tu := uint64(t)
	switch {
	case u < tu:
		return -1
	case u > tu:
		return 1
	case f > t:
		return -1
	}
	return 0
}

// CanonicalIndexKey maps the index key of an integer of any width to the one
// key every equal integer shares (the int64 key; the uint64 key past
// MaxInt64), so a unique constraint holds int(3) and int64(3) as one value.
// Every other key is returned unchanged (the unique constraints refuse
// floats).
func CanonicalIndexKey(vk string) string {
	prefix, digits, ok := strings.Cut(vk, ":")
	if !ok {
		return vk
	}
	switch prefix {
	case "i", "i8", "i16", "i32":
		if i, err := strconv.ParseInt(digits, 10, 64); err == nil {
			return appendIntKey("i64:", i)
		}
	case "u", "u8", "u16", "u32", "u64":
		if u, err := strconv.ParseUint(digits, 10, 64); err == nil {
			if u <= math.MaxInt64 {
				return appendIntKey("i64:", int64(u))
			}
			return appendUintKey("u64:", u)
		}
	}
	return vk
}

// PropertyEqual is the property equality of compare-and-set's expected value:
// a number by NumericEqual (every width of the same exact value; NaN equals
// nothing), anything else by PropertyValueEqual. The no-op check of an update
// (is the new value the stored one?) stays PropertyValueEqual, so writing
// 3.0 over int64(3) still stores the float64.
func PropertyEqual(stored, expected any) bool {
	if _, num := NumericFloat64(expected); num {
		return NumericEqual(expected, stored)
	}
	if _, num := NumericFloat64(stored); num {
		return false
	}
	return PropertyValueEqual(stored, expected)
}
