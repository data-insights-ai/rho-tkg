package types

import (
	"errors"
	"fmt"
	"sort"
	"testing"
)

func TestPropertySliceRejectsTKGPrefix(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	err := ps.Set("tkg_labels", "x")
	if err == nil {
		t.Fatal("PropertySlice.Set(\"tkg_labels\", ...) should return error")
	}
	// Must be programmatically distinguishable via errors.Is.
	if !errors.Is(err, ErrReservedPrefix) {
		t.Errorf("errors.Is(err, ErrReservedPrefix) = false; err = %v", err)
	}
	// Verify nothing was stored.
	if ps.Len() != 0 {
		t.Fatalf("PropertySlice should be empty after rejected Set, got %d", ps.Len())
	}
}

func TestPropertySliceSetReturnsErrReservedPrefix(t *testing.T) {
	t.Parallel()

	keys := []string{"tkg_labels", "tkg_type", "tkg_version", "tkg_", "tkg_anything"}
	for _, key := range keys {
		var ps PropertySlice
		err := ps.Set(key, "x")
		if err == nil {
			t.Errorf("Set(%q) should return error", key)
			continue
		}
		if !errors.Is(err, ErrReservedPrefix) {
			t.Errorf("Set(%q): errors.Is(err, ErrReservedPrefix) = false; err = %v", key, err)
		}
	}
}

// ─── Pointer/Struct rejection tests ─────────────────────────────────────────

func TestPropertySliceRejectsPointer(t *testing.T) {
	t.Parallel()

	type myStruct struct{ X int }
	s := &myStruct{X: 1}
	var ps PropertySlice
	err := ps.Set("p", s)
	if err == nil {
		t.Fatal("Set(\"p\", *myStruct) should return error for pointer value")
	}
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Errorf("errors.Is(err, ErrUnsupportedValueType) = false; err = %v", err)
	}
}

func TestPropertySliceRejectsStruct(t *testing.T) {
	t.Parallel()

	type myStruct struct{ X int }
	s := myStruct{X: 1}
	var ps PropertySlice
	err := ps.Set("s", s)
	if err == nil {
		t.Fatal("Set(\"s\", myStruct{}) should return error for struct value")
	}
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Errorf("errors.Is(err, ErrUnsupportedValueType) = false; err = %v", err)
	}
}

func TestPropertySliceAcceptsPrimitives(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	cases := []struct {
		key string
		val any
	}{
		{"str", "hello"},
		{"integer", 42},
		{"float", 3.14},
		{"boolean", true},
		{"nilval", nil},
	}
	for _, tc := range cases {
		if err := ps.Set(tc.key, tc.val); err != nil {
			t.Errorf("Set(%q, %v) returned unexpected error: %v", tc.key, tc.val, err)
		}
	}
}

func TestPropertySliceAcceptsSlicesAndMaps(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	if err := ps.Set("strs", []string{"a", "b"}); err != nil {
		t.Errorf("Set(\"strs\", []string) returned unexpected error: %v", err)
	}
	if err := ps.Set("meta", map[string]any{"x": 1}); err != nil {
		t.Errorf("Set(\"meta\", map[string]any) returned unexpected error: %v", err)
	}
}

// ─── Recursive validation tests (nested prohibited types) ──────────────────

func TestPropertySliceRejectsNestedPointerInSlice(t *testing.T) {
	t.Parallel()

	type myStruct struct{ X int }
	var ps PropertySlice
	err := ps.Set("bad", []any{&myStruct{X: 1}})
	if err == nil {
		t.Fatal("Set should reject []any containing a pointer")
	}
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Errorf("errors.Is(err, ErrUnsupportedValueType) = false; err = %v", err)
	}
}

func TestPropertySliceRejectsNestedStructInSlice(t *testing.T) {
	t.Parallel()

	type myStruct struct{ X int }
	var ps PropertySlice
	err := ps.Set("bad", []any{myStruct{X: 1}})
	if err == nil {
		t.Fatal("Set should reject []any containing a struct")
	}
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Errorf("errors.Is(err, ErrUnsupportedValueType) = false; err = %v", err)
	}
}

func TestPropertySliceRejectsNestedPointerInMap(t *testing.T) {
	t.Parallel()

	type myStruct struct{ X int }
	var ps PropertySlice
	err := ps.Set("bad", map[string]any{"k": &myStruct{X: 1}})
	if err == nil {
		t.Fatal("Set should reject map[string]any containing a pointer value")
	}
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Errorf("errors.Is(err, ErrUnsupportedValueType) = false; err = %v", err)
	}
}

func TestPropertySliceRejectsDeeplyNestedPointer(t *testing.T) {
	t.Parallel()

	type myStruct struct{ X int }
	var ps PropertySlice
	// 3 levels: slice → map → pointer
	err := ps.Set("bad", []any{map[string]any{"k": &myStruct{X: 1}}})
	if err == nil {
		t.Fatal("Set should reject deeply nested pointer (3 levels)")
	}
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Errorf("errors.Is(err, ErrUnsupportedValueType) = false; err = %v", err)
	}
}

func TestPropertySliceAcceptsNestedSafeValues(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	// All of these should be accepted — no pointers or structs at any depth.
	cases := []struct {
		key string
		val any
	}{
		{"nested_slice", []any{1, "two", 3.0, true, nil}},
		{"nested_map", map[string]any{"a": 1, "b": "two"}},
		{"deep_nested", []any{map[string]any{"k": []any{1, 2}}}},
		{"map_of_slices", map[string]any{"tags": []string{"a", "b"}}},
	}
	for _, tc := range cases {
		if err := ps.Set(tc.key, tc.val); err != nil {
			t.Errorf("Set(%q, ...) returned unexpected error: %v", tc.key, err)
		}
	}
}

// ─── Allowlist validation tests (arrays, funcs, chans) ──────────────────────

func TestPropertySliceRejectsArray(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	err := ps.Set("arr", [1]int{42})
	if err == nil {
		t.Fatal("Set should reject array values")
	}
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Errorf("errors.Is(err, ErrUnsupportedValueType) = false; err = %v", err)
	}
}

func TestPropertySliceRejectsArrayWithPointer(t *testing.T) {
	t.Parallel()

	type myStruct struct{ X int }
	var ps PropertySlice
	err := ps.Set("arr", [1]*myStruct{{X: 1}})
	if err == nil {
		t.Fatal("Set should reject array containing pointer")
	}
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Errorf("errors.Is(err, ErrUnsupportedValueType) = false; err = %v", err)
	}
}

func TestPropertySliceRejectsFunc(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	err := ps.Set("fn", func() {})
	if err == nil {
		t.Fatal("Set should reject func values")
	}
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Errorf("errors.Is(err, ErrUnsupportedValueType) = false; err = %v", err)
	}
}

func TestPropertySliceRejectsChan(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	err := ps.Set("ch", make(chan int))
	if err == nil {
		t.Fatal("Set should reject chan values")
	}
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Errorf("errors.Is(err, ErrUnsupportedValueType) = false; err = %v", err)
	}
}

// ─── Recursion depth limit tests ────────────────────────────────────────────

func TestValidateRejectsExcessiveDepth(t *testing.T) {
	t.Parallel()

	// Build a structure deeper than maxPropertyDepth (32).
	var v any = "leaf"
	for range 33 {
		v = []any{v}
	}

	var ps PropertySlice
	err := ps.Set("deep", v)
	if err == nil {
		t.Fatal("Set should reject values nested deeper than maxPropertyDepth")
	}
	if !errors.Is(err, ErrMaxDepthExceeded) {
		t.Errorf("errors.Is(err, ErrMaxDepthExceeded) = false; err = %v", err)
	}
}

func TestValidateAcceptsMaxDepth(t *testing.T) {
	t.Parallel()

	// Build a structure exactly at maxPropertyDepth (32 levels).
	var v any = "leaf"
	for range 32 {
		v = []any{v}
	}

	var ps PropertySlice
	if err := ps.Set("deep_ok", v); err != nil {
		t.Fatalf("Set should accept values at exactly maxPropertyDepth, got: %v", err)
	}
}

// ─── Depth boundary edge cases ──────────────────────────────────────────────

func TestValidateAcceptsOneBelowMaxDepth(t *testing.T) {
	t.Parallel()

	// 31 levels of nesting (one below maxPropertyDepth=32).
	var v any = "leaf"
	for range 31 {
		v = []any{v}
	}

	var ps PropertySlice
	if err := ps.Set("deep31", v); err != nil {
		t.Fatalf("Set should accept 31 levels of nesting, got: %v", err)
	}
}

func TestValidateRejectsMapAtExcessiveDepth(t *testing.T) {
	t.Parallel()

	// 33-level map nesting → ErrMaxDepthExceeded (exercises the map-recursion branch).
	var v any = "leaf"
	for range 33 {
		v = map[string]any{"k": v}
	}

	var ps PropertySlice
	err := ps.Set("deep_map", v)
	if err == nil {
		t.Fatal("Set should reject map nesting deeper than maxPropertyDepth")
	}
	if !errors.Is(err, ErrMaxDepthExceeded) {
		t.Errorf("errors.Is(err, ErrMaxDepthExceeded) = false; err = %v", err)
	}
}

// ─── Empty container edge cases ─────────────────────────────────────────────

func TestValidateAcceptsEmptyContainers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		val  any
	}{
		{"empty slice", []any{}},
		{"empty map", map[string]any{}},
		{"nested empty slices", []any{[]any{[]any{}}}},
	}

	for _, tc := range cases {
		var ps PropertySlice
		if err := ps.Set(tc.name, tc.val); err != nil {
			t.Errorf("Set(%q, ...) should accept empty container, got: %v", tc.name, err)
		}
	}
}

func TestValidateAcceptsMapWithNilValue(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	m := map[string]any{"key": nil}
	if err := ps.Set("nilval_map", m); err != nil {
		t.Fatalf("Set should accept map with nil value, got: %v", err)
	}

	val, ok := ps.Get("nilval_map")
	if !ok {
		t.Fatal("Get(\"nilval_map\") should return true")
	}
	got := val.(map[string]any)
	if _, exists := got["key"]; !exists {
		t.Fatal("nil-value key must be preserved")
	}
}

// ─── Mixed valid/invalid content ────────────────────────────────────────────

func TestValidateRejectsMixedValidInvalidSlice(t *testing.T) {
	t.Parallel()

	type myStruct struct{ X int }
	var ps PropertySlice
	err := ps.Set("mixed", []any{"ok", &myStruct{}})
	if err == nil {
		t.Fatal("Set should reject slice with mixed valid and invalid elements")
	}
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Errorf("errors.Is(err, ErrUnsupportedValueType) = false; err = %v", err)
	}
}

func TestValidateRejectsAnyWrappedInvalidType(t *testing.T) {
	t.Parallel()

	type myStruct struct{ X int }
	var v any = myStruct{X: 1}
	var ps PropertySlice
	err := ps.Set("wrapped", v)
	if err == nil {
		t.Fatal("Set should reject struct wrapped in any interface")
	}
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Errorf("errors.Is(err, ErrUnsupportedValueType) = false; err = %v", err)
	}
}

// ─── Map key type validation ────────────────────────────────────────────────

// TestValidateRejectsNonStringMapKeys covers F2: maps whose key type is not
// `string` cannot be hashed by appendPropertyValue (which only handles
// map[string]any and map[string]string), so Set must reject them up front
// instead of admitting a value that later panics during entity hashing.
func TestValidateRejectsNonStringMapKeys(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	err := ps.Set("intkeys", map[int]string{1: "one", 2: "two"})
	if err == nil {
		t.Fatal("Set should reject map[int]string (F2)")
	}
	if !errors.Is(err, ErrUnsupportedMapType) {
		t.Errorf("errors.Is(err, ErrUnsupportedMapType) = false; err = %v", err)
	}
}

// TestValidateRejectsMapWithInvalidKeyType keeps the original assertion that
// struct-keyed maps are rejected; the error class is now ErrUnsupportedMapType
// because the key-type check fires before the recursive value walk.
func TestValidateRejectsMapWithInvalidKeyType(t *testing.T) {
	t.Parallel()

	type myKey struct{ Name string }
	var ps PropertySlice
	err := ps.Set("structkeys", map[myKey]string{{Name: "a"}: "val"})
	if err == nil {
		t.Fatal("Set should reject map with struct key type")
	}
	if !errors.Is(err, ErrUnsupportedMapType) {
		t.Errorf("errors.Is(err, ErrUnsupportedMapType) = false; err = %v", err)
	}
}

// TestValidateRejectsConcreteValueMaps covers map[string]X for X != any,string.
// Hash and wire only know map[string]any and map[string]string; everything
// else is rejected up front so callers can't later trip the hash panic path.
func TestValidateRejectsConcreteValueMaps(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	err := ps.Set("concrete", map[string]int{"a": 1})
	if err == nil {
		t.Fatal("Set should reject map[string]int (F2)")
	}
	if !errors.Is(err, ErrUnsupportedMapType) {
		t.Errorf("errors.Is(err, ErrUnsupportedMapType) = false; err = %v", err)
	}
}

// ─── NewPropertySlice bulk loader tests ──────────────────────────────────────

func TestNewPropertySliceBasic(t *testing.T) {
	t.Parallel()

	m := map[string]any{
		"zulu":  5,
		"alpha": 1,
		"mike":  3,
		"bravo": 2,
		"echo":  4,
	}
	ps, err := NewPropertySlice(m)
	if err != nil {
		t.Fatalf("NewPropertySlice() returned error: %v", err)
	}
	if ps.Len() != 5 {
		t.Fatalf("Len() = %d, want 5", ps.Len())
	}
	// Verify sorted order.
	if !sort.SliceIsSorted(ps, func(i, j int) bool { return ps[i].Key < ps[j].Key }) {
		t.Fatal("NewPropertySlice result is not sorted")
	}
	// Verify all values are correct.
	for k, v := range m {
		got, ok := ps.Get(k)
		if !ok {
			t.Fatalf("Get(%q) not found", k)
		}
		if got != v {
			t.Fatalf("Get(%q) = %v, want %v", k, got, v)
		}
	}
}

func TestNewPropertySliceEmpty(t *testing.T) {
	t.Parallel()

	ps, err := NewPropertySlice(nil)
	if err != nil {
		t.Fatalf("NewPropertySlice(nil) returned error: %v", err)
	}
	if ps != nil {
		t.Fatalf("NewPropertySlice(nil) = %v, want nil", ps)
	}

	ps2, err := NewPropertySlice(map[string]any{})
	if err != nil {
		t.Fatalf("NewPropertySlice({}) returned error: %v", err)
	}
	if ps2 != nil {
		t.Fatalf("NewPropertySlice({}) = %v, want nil", ps2)
	}
}

func TestNewPropertySliceReservedKey(t *testing.T) {
	t.Parallel()

	_, err := NewPropertySlice(map[string]any{"tkg_labels": "hack", "name": "ok"})
	if err == nil {
		t.Fatal("NewPropertySlice should reject tkg_ prefix key")
	}
	if !errors.Is(err, ErrReservedPrefix) {
		t.Errorf("errors.Is(err, ErrReservedPrefix) = false; err = %v", err)
	}
}

func TestNewPropertySliceInvalidValue(t *testing.T) {
	t.Parallel()

	type myStruct struct{ X int }
	_, err := NewPropertySlice(map[string]any{"bad": &myStruct{X: 1}})
	if err == nil {
		t.Fatal("NewPropertySlice should reject pointer value")
	}
	if !errors.Is(err, ErrUnsupportedValueType) {
		t.Errorf("errors.Is(err, ErrUnsupportedValueType) = false; err = %v", err)
	}
}

func TestNewPropertySliceLargeMap(t *testing.T) {
	t.Parallel()

	m := make(map[string]any, 1000)
	for i := range 1000 {
		m[fmt.Sprintf("key_%04d", i)] = i
	}
	ps, err := NewPropertySlice(m)
	if err != nil {
		t.Fatalf("NewPropertySlice(1000 keys) returned error: %v", err)
	}
	if ps.Len() != 1000 {
		t.Fatalf("Len() = %d, want 1000", ps.Len())
	}
	if !sort.SliceIsSorted(ps, func(i, j int) bool { return ps[i].Key < ps[j].Key }) {
		t.Fatal("NewPropertySlice result is not sorted for 1000 keys")
	}
	// Spot-check a few values.
	for _, k := range []string{"key_0000", "key_0500", "key_0999"} {
		if _, ok := ps.Get(k); !ok {
			t.Fatalf("Get(%q) not found in 1000-key result", k)
		}
	}
}
