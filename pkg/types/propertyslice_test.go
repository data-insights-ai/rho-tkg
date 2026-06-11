package types

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"
)

// ─── Sorted invariant tests ─────────────────────────────────────────────────

func TestPropertySliceSetMaintainsSortedOrder(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	// Insert out of order.
	for _, key := range []string{"z", "a", "m", "b"} {
		if err := ps.Set(key, key); err != nil {
			t.Fatal(err)
		}
	}

	for i := 1; i < ps.Len(); i++ {
		if ps[i-1].Key >= ps[i].Key {
			t.Fatalf("sorted invariant broken: ps[%d].Key=%q >= ps[%d].Key=%q",
				i-1, ps[i-1].Key, i, ps[i].Key)
		}
	}
}

func TestPropertySliceSetOverwritesExisting(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	if err := ps.Set("k", "v1"); err != nil {
		t.Fatal(err)
	}
	if err := ps.Set("k", "v2"); err != nil {
		t.Fatal(err)
	}

	if ps.Len() != 1 {
		t.Fatalf("Len() = %d after overwrite, want 1", ps.Len())
	}
	val, ok := ps.Get("k")
	if !ok || val != "v2" {
		t.Errorf("Get(\"k\") = (%v, %v), want (\"v2\", true)", val, ok)
	}
}

func TestPropertySliceSetNilReceiverReturnsSentinel(t *testing.T) {
	t.Parallel()

	var ps *PropertySlice
	if err := ps.Set("x", int64(1)); !errors.Is(err, ErrNilPropertySlice) {
		t.Fatalf("Set on nil *PropertySlice = %v, want ErrNilPropertySlice", err)
	}
}

func TestPropertySliceDeleteNilReceiverReturnsSentinel(t *testing.T) {
	t.Parallel()

	var ps *PropertySlice
	deleted, err := ps.Delete("x")
	if !errors.Is(err, ErrNilPropertySlice) || deleted {
		t.Fatalf("Delete on nil *PropertySlice = (%v, %v), want (false, ErrNilPropertySlice)", deleted, err)
	}
}

func TestPropertySliceSetCopiesCallerValue(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	tags := []string{"alpha", "beta"}
	meta := map[string]any{"nested": []any{"one"}}
	if err := ps.Set("tags", tags); err != nil {
		t.Fatalf("Set tags: %v", err)
	}
	if err := ps.Set("meta", meta); err != nil {
		t.Fatalf("Set meta: %v", err)
	}

	tags[0] = "mutated"
	meta["nested"].([]any)[0] = "mutated"

	gotTags, ok := ps.Get("tags")
	if !ok {
		t.Fatal("Get tags missing")
	}
	if gotTags.([]string)[0] != "alpha" {
		t.Fatalf("Set retained caller slice alias: %q", gotTags.([]string)[0])
	}
	gotMeta, ok := ps.Get("meta")
	if !ok {
		t.Fatal("Get meta missing")
	}
	if gotMeta.(map[string]any)["nested"].([]any)[0] != "one" {
		t.Fatalf("Set retained caller nested map alias: %v", gotMeta)
	}
}

func TestPropertySliceGetReturnsIndependentValue(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	if err := ps.Set("meta", map[string]any{
		"tags": []any{"alpha", "beta"},
	}); err != nil {
		t.Fatalf("Set meta: %v", err)
	}

	got, ok := ps.Get("meta")
	if !ok {
		t.Fatal("Get meta missing")
	}
	got.(map[string]any)["tags"].([]any)[0] = "mutated"

	again, ok := ps.Get("meta")
	if !ok {
		t.Fatal("Get meta missing after returned-value mutation")
	}
	if again.(map[string]any)["tags"].([]any)[0] != "alpha" {
		t.Fatalf("Get returned internal mutable state: %v", again)
	}
}

// ─── Get edge cases ─────────────────────────────────────────────────────────

func TestPropertySliceGetMissReturnsNilFalse(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	_ = ps.Set("exists", 1)

	val, ok := ps.Get("missing")
	if ok || val != nil {
		t.Errorf("Get(\"missing\") = (%v, %v), want (nil, false)", val, ok)
	}
}

func TestPropertySliceGetBinarySearchEdgeCases(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	keys := []string{"alpha", "beta", "gamma", "zeta"}
	for _, k := range keys {
		_ = ps.Set(k, k)
	}

	cases := []struct {
		key  string
		want bool
	}{
		{"alpha", true},  // first
		{"gamma", true},  // middle
		{"zeta", true},   // last
		{"aaa", false},   // before first
		{"zzz", false},   // after last
		{"delta", false}, // between existing
	}

	for _, tc := range cases {
		val, ok := ps.Get(tc.key)
		if ok != tc.want {
			t.Errorf("Get(%q) found=%v, want %v", tc.key, ok, tc.want)
		}
		if tc.want && val != tc.key {
			t.Errorf("Get(%q) value=%v, want %q", tc.key, val, tc.key)
		}
	}
}

func TestIndexablePropertyValueKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
		want  string
	}{
		{"string", "Alice", "s:Alice"},
		{"int", int(-1), "i:-1"},
		{"int8", int8(-8), "i8:-8"},
		{"int16", int16(-16), "i16:-16"},
		{"int32", int32(-32), "i32:-32"},
		{"int64", int64(-64), "i64:-64"},
		{"uint", uint(1), "u:1"},
		{"uint8", uint8(8), "u8:8"},
		{"uint16", uint16(16), "u16:16"},
		{"uint32", uint32(32), "u32:32"},
		{"uint64", uint64(64), "u64:64"},
		{"float32", float32(1.25), "f32:1.25"},
		{"float64", float64(1.25), "f64:1.25"},
		{"float32 positive zero", float32(0), "f32:0"},
		{"float32 negative zero", float32(math.Copysign(0, -1)), "f32:0"},
		{"float32 nan payload", math.Float32frombits(0x7fc00001), "f32:nan"},
		{"float32 nan payload alt", math.Float32frombits(0x7fc00002), "f32:nan"},
		{"float64 positive zero", float64(0), "f64:0"},
		{"float64 negative zero", math.Copysign(0, -1), "f64:0"},
		{"float64 nan payload", math.Float64frombits(0x7ff8000000000001), "f64:nan"},
		{"float64 nan payload alt", math.Float64frombits(0x7ff8000000000002), "f64:nan"},
		{"bool true", true, "b:true"},
		{"bool false", false, "b:false"},
		{"nil", nil, ""},
		{"slice", []string{"a"}, ""},
		{"map", map[string]any{"a": 1}, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IndexablePropertyValueKey(tc.value); got != tc.want {
				t.Fatalf("IndexablePropertyValueKey(%T) = %q, want %q", tc.value, got, tc.want)
			}
		})
	}
}

// BenchmarkIndexablePropertyValueKey pins the single-allocation behaviour of
// the numeric/float key encoders. The prior `prefix + strconv.FormatX` form
// allocated twice (formatted number + concatenation); the append-into-buffer
// form must stay at one alloc/op (the result string).
func BenchmarkIndexablePropertyValueKey(b *testing.B) {
	cases := []any{int64(-9223372036854775807), uint64(18446744073709551615), float64(1.2345678901234567)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = IndexablePropertyValueKey(cases[i%len(cases)])
	}
}

func TestPropertyValueEqual(t *testing.T) {
	t.Parallel()

	if !PropertyValueEqual([]any{map[string]any{"score": math.NaN()}}, []any{map[string]any{"score": math.NaN()}}) {
		t.Fatal("nested NaN values should compare equal")
	}
	if PropertyValueEqual([]any{float32(math.NaN())}, []any{float64(math.NaN())}) {
		t.Fatal("nested NaN values must still require exact types")
	}
	if !PropertyValueEqual(map[string]float64{"x": math.NaN()}, map[string]float64{"x": math.NaN()}) {
		t.Fatal("map values containing NaN should compare equal")
	}
	if PropertyValueEqual(map[string]float64{"x": 1}, map[string]float64{"y": 1}) {
		t.Fatal("different map keys should not compare equal")
	}
	if PropertyValueEqual(int64(42), int(42)) {
		t.Fatal("different concrete numeric types should not compare equal")
	}
	var nilSlice []float64
	if !PropertyValueEqual(nilSlice, []float64(nil)) {
		t.Fatal("nil slices of the same concrete type should compare equal")
	}
	if PropertyValueEqual(nilSlice, []float64{}) {
		t.Fatal("nil and empty slices should not compare equal")
	}
	if PropertyValueEqual(make(chan int), make(chan int)) {
		t.Fatal("unsupported property equality fallback kinds should not compare equal")
	}
	ch := make(chan int)
	if PropertyValueEqual(ch, ch) {
		t.Fatal("same unsupported property equality fallback kind should not compare equal")
	}
}

type propertyValueEqualCycle struct {
	Score float64
	Next  *propertyValueEqualCycle
}

func TestPropertyValueEqualHandlesCycles(t *testing.T) {
	t.Parallel()

	leftSlice := []any{math.NaN(), nil}
	leftSlice[1] = leftSlice
	rightSlice := []any{math.NaN(), nil}
	rightSlice[1] = rightSlice
	if !PropertyValueEqual(leftSlice, rightSlice) {
		t.Fatal("cyclic []any values with matching NaN payload should compare equal")
	}
	rightSlice[0] = 1.0
	if PropertyValueEqual(leftSlice, rightSlice) {
		t.Fatal("cyclic []any values with different payloads should not compare equal")
	}

	leftMap := map[string]any{"score": math.NaN()}
	leftMap["self"] = leftMap
	rightMap := map[string]any{"score": math.NaN()}
	rightMap["self"] = rightMap
	if !PropertyValueEqual(leftMap, rightMap) {
		t.Fatal("cyclic map[string]any values with matching NaN payload should compare equal")
	}
	rightMap["score"] = 1.0
	if PropertyValueEqual(leftMap, rightMap) {
		t.Fatal("cyclic map[string]any values with different payloads should not compare equal")
	}

	leftStruct := &propertyValueEqualCycle{Score: math.NaN()}
	leftStruct.Next = leftStruct
	rightStruct := &propertyValueEqualCycle{Score: math.NaN()}
	rightStruct.Next = rightStruct
	if !PropertyValueEqual(leftStruct, rightStruct) {
		t.Fatal("cyclic struct pointer values with matching NaN payload should compare equal")
	}
	rightStruct.Score = 1.0
	if PropertyValueEqual(leftStruct, rightStruct) {
		t.Fatal("cyclic struct pointer values with different payloads should not compare equal")
	}
}

func TestPropertySlicePreservesTypedNilContainers(t *testing.T) {
	t.Parallel()

	var nilStrings []string
	var nilInts []int
	var nilInt64s []int64
	var nilFloat32s []float32
	var nilFloat64s []float64
	var nilBytes []byte
	var nilBools []bool
	var nilAny []any
	var nilMapAny map[string]any
	var nilMapString map[string]string

	cases := []struct {
		name  string
		value any
		empty any
	}{
		{name: "[]string", value: nilStrings, empty: []string{}},
		{name: "[]int", value: nilInts, empty: []int{}},
		{name: "[]int64", value: nilInt64s, empty: []int64{}},
		{name: "[]float32", value: nilFloat32s, empty: []float32{}},
		{name: "[]float64", value: nilFloat64s, empty: []float64{}},
		{name: "[]byte", value: nilBytes, empty: []byte{}},
		{name: "[]bool", value: nilBools, empty: []bool{}},
		{name: "[]any", value: nilAny, empty: []any{}},
		{name: "map[string]any", value: nilMapAny, empty: map[string]any{}},
		{name: "map[string]string", value: nilMapString, empty: map[string]string{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ps PropertySlice
			if err := ps.Set("k", tc.value); err != nil {
				t.Fatalf("Set typed nil: %v", err)
			}
			assertTypedNilContainer(t, "Set/Get", ps.mustGetForTest("k"), tc.value, tc.empty)

			cp := ps.DeepCopy()
			assertTypedNilContainer(t, "DeepCopy/Get", cp.mustGetForTest("k"), tc.value, tc.empty)
			assertTypedNilContainer(t, "ToMap", ps.ToMap()["k"], tc.value, tc.empty)

			fromMap, err := NewPropertySlice(map[string]any{"k": tc.value})
			if err != nil {
				t.Fatalf("NewPropertySlice typed nil: %v", err)
			}
			assertTypedNilContainer(t, "NewPropertySlice/Get", fromMap.mustGetForTest("k"), tc.value, tc.empty)
		})
	}
}

func (ps PropertySlice) mustGetForTest(key string) any {
	v, ok := ps.Get(key)
	if !ok {
		panic("missing test property " + key)
	}
	return v
}

func assertTypedNilContainer(t *testing.T, label string, got, wantNil, wantEmpty any) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s returned untyped nil for %T", label, wantNil)
	}
	gotValue := reflect.ValueOf(got)
	wantType := reflect.TypeOf(wantNil)
	if gotValue.Type() != wantType {
		t.Fatalf("%s type = %T, want %v", label, got, wantType)
	}
	if !gotValue.IsNil() {
		t.Fatalf("%s = %#v, want typed nil %v", label, got, wantType)
	}
	if !PropertyValueEqual(got, wantNil) {
		t.Fatalf("%s does not compare equal to typed nil %v", label, wantType)
	}
	if PropertyValueEqual(got, wantEmpty) {
		t.Fatalf("%s typed nil compares equal to empty %T", label, wantEmpty)
	}
}

func TestPropertySliceReflectCopyInternalBranches(t *testing.T) {
	t.Parallel()

	type aliasInt int
	type aliasSlice []aliasInt
	type aliasMap map[string]aliasSlice

	originalSlice := aliasSlice{1, 2}
	copiedSlice := reflectCopyValue(originalSlice, 0).(aliasSlice)
	copiedSlice[0] = 99
	if originalSlice[0] != 1 {
		t.Fatalf("reflectCopyValue retained alias slice backing array: %v", originalSlice)
	}

	originalMap := aliasMap{"k": {3, 4}}
	copiedMap := reflectCopyValue(originalMap, 0).(aliasMap)
	copiedMap["k"][0] = 99
	if originalMap["k"][0] != 3 {
		t.Fatalf("reflectCopyValue retained alias map element backing array: %v", originalMap)
	}

	var nilSlice aliasSlice
	if got := reflectCopyValue(nilSlice, 0).(aliasSlice); got != nil {
		t.Fatalf("nil alias slice copy = %#v, want nil", got)
	}
	var nilMap aliasMap
	if got := reflectCopyValue(nilMap, 0).(aliasMap); got != nil {
		t.Fatalf("nil alias map copy = %#v, want nil", got)
	}

	if got := reflectCopyValue(aliasInt(7), 0); got != aliasInt(7) {
		t.Fatalf("scalar reflect fallback = %v, want 7", got)
	}
}

func TestReflectValueForTypeBranches(t *testing.T) {
	t.Parallel()

	if got, ok := reflectValueForType(nil, reflect.TypeOf("")); !ok || got.String() != "" {
		t.Fatalf("nil value conversion = (%v, %v), want zero string true", got, ok)
	}
	if got, ok := reflectValueForType("x", reflect.TypeOf("")); !ok || got.String() != "x" {
		t.Fatalf("assignable conversion = (%v, %v), want x true", got, ok)
	}
	if got, ok := reflectValueForType(int32(7), reflect.TypeOf(int64(0))); !ok || got.Int() != 7 {
		t.Fatalf("convertible conversion = (%v, %v), want 7 true", got, ok)
	}
	if got, ok := reflectValueForType("x", reflect.TypeOf(int64(0))); ok || got.IsValid() {
		t.Fatalf("incompatible conversion = (%v, %v), want invalid false", got, ok)
	}
}

func TestIsRegisteredPropertyValueNil(t *testing.T) {
	t.Parallel()

	if isRegisteredPropertyValue(nil) {
		t.Fatal("nil must not report as a registered property value")
	}
}

// ─── ToMap ──────────────────────────────────────────────────────────────────

func TestPropertySliceToMap(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	_ = ps.Set("a", 1)
	_ = ps.Set("b", "two")
	_ = ps.Set("c", 3.0)

	m := ps.ToMap()
	if len(m) != 3 {
		t.Fatalf("ToMap() len = %d, want 3", len(m))
	}
	if m["a"] != 1 || m["b"] != "two" || m["c"] != 3.0 {
		t.Errorf("ToMap() = %v, unexpected values", m)
	}
}

func TestToMapValuesAreIndependent(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	_ = ps.Set("tags", []string{"a", "b", "c"})

	m := ps.ToMap()
	// Mutate the map's slice value.
	m["tags"].([]string)[0] = "MUTATED"

	// Original must be unchanged.
	origVal, _ := ps.Get("tags")
	origSlice := origVal.([]string)
	if origSlice[0] == "MUTATED" {
		t.Fatal("ToMap: mutating returned map's slice value affected the original PropertySlice")
	}
}

// ─── Len ────────────────────────────────────────────────────────────────────

func TestPropertySliceLen(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	if ps.Len() != 0 {
		t.Errorf("empty Len() = %d, want 0", ps.Len())
	}

	_ = ps.Set("one", 1)
	if ps.Len() != 1 {
		t.Errorf("after 1 insert Len() = %d, want 1", ps.Len())
	}

	_ = ps.Set("two", 2)
	_ = ps.Set("three", 3)
	if ps.Len() != 3 {
		t.Errorf("after 3 inserts Len() = %d, want 3", ps.Len())
	}
}

// ─── Delete ──────────────────────────────────────────────────────────────────

func TestPropertySliceDelete(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	_ = ps.Set("a", 1)
	_ = ps.Set("b", 2)
	_ = ps.Set("c", 3)

	found, err := ps.Delete("b")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("Delete(\"b\") should return true for existing key")
	}
	if ps.Len() != 2 {
		t.Fatalf("Len() = %d after delete, want 2", ps.Len())
	}
	if _, ok := ps.Get("b"); ok {
		t.Fatal("Get(\"b\") should return false after delete")
	}
	// Sorted order still maintained.
	if ps[0].Key != "a" || ps[1].Key != "c" {
		t.Errorf("sorted order broken: %v", ps)
	}
}

func TestPropertySliceDeleteClearsReleasedEntry(t *testing.T) {
	t.Parallel()

	ps := make(PropertySlice, 0, 3)
	if err := ps.Set("a", []byte("kept")); err != nil {
		t.Fatalf("Set a: %v", err)
	}
	if err := ps.Set("b", []byte("deleted")); err != nil {
		t.Fatalf("Set b: %v", err)
	}
	if err := ps.Set("c", []byte("also-kept")); err != nil {
		t.Fatalf("Set c: %v", err)
	}

	found, err := ps.Delete("b")
	if err != nil || !found {
		t.Fatalf("Delete b = (%v, %v), want true, nil", found, err)
	}
	if cap(ps) <= len(ps) {
		t.Fatalf("test setup lost spare capacity: len=%d cap=%d", len(ps), cap(ps))
	}
	released := ps[:cap(ps)][len(ps)]
	if released.Key != "" || released.Value != nil {
		t.Fatalf("released property entry = %+v, want zero value", released)
	}
}

func TestPropertySliceDeleteMissReturnsFalse(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	_ = ps.Set("a", 1)

	found, err := ps.Delete("missing")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("Delete(\"missing\") should return false")
	}
	if ps.Len() != 1 {
		t.Fatalf("Len() = %d after miss delete, want 1", ps.Len())
	}
}

func TestPropertySliceDeleteRejectsTKGPrefix(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	_, err := ps.Delete("tkg_labels")
	if err == nil {
		t.Fatal("Delete(\"tkg_labels\") should return error")
	}
	if !errors.Is(err, ErrReservedPrefix) {
		t.Errorf("errors.Is(err, ErrReservedPrefix) = false; err = %v", err)
	}
}

// ─── Stress tests ───────────────────────────────────────────────────────────

func TestPropertySliceStressLargeMap(t *testing.T) {
	t.Parallel()

	m := make(map[string]any, 1000)
	for i := range 1000 {
		m[fmt.Sprintf("key_%04d", i)] = i
	}

	var ps PropertySlice
	if err := ps.Set("big_map", m); err != nil {
		t.Fatalf("Set should accept 1000-entry map, got: %v", err)
	}

	val, ok := ps.Get("big_map")
	if !ok {
		t.Fatal("Get(\"big_map\") should return true")
	}
	got := val.(map[string]any)
	if len(got) != 1000 {
		t.Fatalf("map len = %d, want 1000", len(got))
	}
}

func TestPropertySliceStressLargeSlice(t *testing.T) {
	t.Parallel()

	s := make([]any, 1000)
	for i := range 1000 {
		s[i] = fmt.Sprintf("elem_%d", i)
	}

	var ps PropertySlice
	if err := ps.Set("big_slice", s); err != nil {
		t.Fatalf("Set should accept 1000-element slice, got: %v", err)
	}

	val, ok := ps.Get("big_slice")
	if !ok {
		t.Fatal("Get(\"big_slice\") should return true")
	}
	got := val.([]any)
	if len(got) != 1000 {
		t.Fatalf("slice len = %d, want 1000", len(got))
	}
}

func TestPropertySliceStressWideAndDeep(t *testing.T) {
	t.Parallel()

	// Build 5 levels × 5 keys of nesting.
	var build func(depth int) map[string]any
	build = func(depth int) map[string]any {
		m := make(map[string]any, 5)
		for i := range 5 {
			key := fmt.Sprintf("L%d_K%d", depth, i)
			if depth < 5 {
				m[key] = build(depth + 1)
			} else {
				m[key] = fmt.Sprintf("leaf_%d", i)
			}
		}
		return m
	}

	tree := build(1)
	var ps PropertySlice
	if err := ps.Set("tree", tree); err != nil {
		t.Fatalf("Set should accept wide-and-deep tree, got: %v", err)
	}

	// DeepCopy independence check.
	cp := ps.DeepCopy()
	copiedVal, _ := cp.Get("tree")
	copiedMap := copiedVal.(map[string]any)
	// Mutate a leaf in the copy.
	inner := copiedMap["L1_K0"].(map[string]any)
	inner["L2_K0"].(map[string]any)["L3_K0"].(map[string]any)["L4_K0"].(map[string]any)["L5_K0"] = "MUTATED"

	// Original must be unchanged.
	origVal, _ := ps.Get("tree")
	origMap := origVal.(map[string]any)
	leaf := origMap["L1_K0"].(map[string]any)["L2_K0"].(map[string]any)["L3_K0"].(map[string]any)["L4_K0"].(map[string]any)["L5_K0"]
	if leaf == "MUTATED" {
		t.Fatal("DeepCopy: mutating deep leaf in copy affected the original")
	}

	// Sorted invariant must hold across all properties.
	_ = ps.Set("aaa", 1)
	_ = ps.Set("zzz", 2)
	for i := 1; i < ps.Len(); i++ {
		if ps[i-1].Key >= ps[i].Key {
			t.Fatalf("sorted invariant broken at index %d: %q >= %q", i-1, ps[i-1].Key, ps[i].Key)
		}
	}
}

// ─── Stress: many properties sorted invariant ───────────────────────────────

func TestPropertySliceStressManyPropertiesSorted(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	for i := range 1000 {
		key := fmt.Sprintf("prop_%04d", i)
		if err := ps.Set(key, i); err != nil {
			t.Fatalf("Set(%q) failed: %v", key, err)
		}
	}

	// All 1000 retrievable.
	for i := range 1000 {
		key := fmt.Sprintf("prop_%04d", i)
		val, ok := ps.Get(key)
		if !ok || val != i {
			t.Fatalf("Get(%q) = (%v, %v), want (%d, true)", key, val, ok, i)
		}
	}

	// Sorted invariant.
	if !sort.SliceIsSorted(ps, func(i, j int) bool { return ps[i].Key < ps[j].Key }) {
		t.Fatal("PropertySlice is not sorted after 1000 insertions")
	}
}
