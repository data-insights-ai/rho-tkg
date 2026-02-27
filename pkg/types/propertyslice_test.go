package types

import (
	"errors"
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

func TestDeepCopyTruncatesExcessiveDepth(t *testing.T) {
	t.Parallel()

	// Build a structure deeper than maxPropertyDepth.
	// Bypass validation by directly constructing the PropertySlice.
	var v any = "leaf"
	for range 40 {
		v = []any{v}
	}

	ps := PropertySlice{{Key: "deep", Value: v}}

	// DeepCopy must not panic — it should stop recursing at depth limit.
	cp := ps.DeepCopy()
	if cp == nil {
		t.Fatal("DeepCopy should return non-nil even for excessively deep values")
	}
}

// ─── DeepCopy isolation tests ───────────────────────────────────────────────

func TestDeepCopySliceValueIsIndependent(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	original := []string{"a", "b", "c"}
	if err := ps.Set("tags", original); err != nil {
		t.Fatal(err)
	}

	cp := ps.DeepCopy()

	// Mutate the copied slice value.
	copiedVal, _ := cp.Get("tags")
	copiedSlice := copiedVal.([]string)
	copiedSlice[0] = "MUTATED"

	// Original must be unchanged.
	origVal, _ := ps.Get("tags")
	origSlice := origVal.([]string)
	if origSlice[0] == "MUTATED" {
		t.Fatal("DeepCopy: mutating copied []string value affected the original")
	}
}

func TestDeepCopyMapValueIsIndependent(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	original := map[string]any{"x": 1, "y": 2}
	if err := ps.Set("meta", original); err != nil {
		t.Fatal(err)
	}

	cp := ps.DeepCopy()

	// Mutate the copied map value.
	copiedVal, _ := cp.Get("meta")
	copiedMap := copiedVal.(map[string]any)
	copiedMap["x"] = 999

	// Original must be unchanged.
	origVal, _ := ps.Get("meta")
	origMap := origVal.(map[string]any)
	if origMap["x"] == 999 {
		t.Fatal("DeepCopy: mutating copied map[string]any value affected the original")
	}
}

func TestDeepCopyIntSliceIsIndependent(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	_ = ps.Set("nums", []int{1, 2, 3})

	cp := ps.DeepCopy()
	copiedVal, _ := cp.Get("nums")
	copiedSlice := copiedVal.([]int)
	copiedSlice[0] = 999

	origVal, _ := ps.Get("nums")
	origSlice := origVal.([]int)
	if origSlice[0] == 999 {
		t.Fatal("DeepCopy: mutating copied []int value affected the original")
	}
}

func TestDeepCopyFloat64SliceIsIndependent(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	_ = ps.Set("vals", []float64{1.1, 2.2})

	cp := ps.DeepCopy()
	copiedVal, _ := cp.Get("vals")
	copiedSlice := copiedVal.([]float64)
	copiedSlice[0] = 999.9

	origVal, _ := ps.Get("vals")
	origSlice := origVal.([]float64)
	if origSlice[0] == 999.9 {
		t.Fatal("DeepCopy: mutating copied []float64 value affected the original")
	}
}

func TestDeepCopyInt64SliceIsIndependent(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	_ = ps.Set("ids", []int64{100, 200, 300})

	cp := ps.DeepCopy()
	copiedVal, _ := cp.Get("ids")
	copiedSlice := copiedVal.([]int64)
	copiedSlice[0] = 999

	origVal, _ := ps.Get("ids")
	origSlice := origVal.([]int64)
	if origSlice[0] == 999 {
		t.Fatal("DeepCopy: mutating copied []int64 value affected the original")
	}
}

func TestDeepCopyBoolSliceIsIndependent(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	_ = ps.Set("flags", []bool{true, false, true})

	cp := ps.DeepCopy()
	copiedVal, _ := cp.Get("flags")
	copiedSlice := copiedVal.([]bool)
	copiedSlice[0] = false

	origVal, _ := ps.Get("flags")
	origSlice := origVal.([]bool)
	if !origSlice[0] {
		t.Fatal("DeepCopy: mutating copied []bool value affected the original")
	}
}

func TestDeepCopyByteSliceIsIndependent(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	_ = ps.Set("data", []byte{0x01, 0x02, 0x03})

	cp := ps.DeepCopy()
	copiedVal, _ := cp.Get("data")
	copiedSlice := copiedVal.([]byte)
	copiedSlice[0] = 0xFF

	origVal, _ := ps.Get("data")
	origSlice := origVal.([]byte)
	if origSlice[0] == 0xFF {
		t.Fatal("DeepCopy: mutating copied []byte value affected the original")
	}
}

func TestDeepCopyMapStringStringIsIndependent(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	_ = ps.Set("headers", map[string]string{"a": "1", "b": "2"})

	cp := ps.DeepCopy()
	copiedVal, _ := cp.Get("headers")
	copiedMap := copiedVal.(map[string]string)
	copiedMap["a"] = "MUTATED"

	origVal, _ := ps.Get("headers")
	origMap := origVal.(map[string]string)
	if origMap["a"] == "MUTATED" {
		t.Fatal("DeepCopy: mutating copied map[string]string value affected the original")
	}
}

func TestDeepCopyReflectFallbackIsIndependent(t *testing.T) {
	t.Parallel()

	t.Run("exotic_slice_int32", func(t *testing.T) {
		t.Parallel()

		var ps PropertySlice
		_ = ps.Set("scores", []int32{1, 2, 3})

		cp := ps.DeepCopy()
		copiedVal, _ := cp.Get("scores")
		copiedSlice := copiedVal.([]int32)
		copiedSlice[0] = 999

		origVal, _ := ps.Get("scores")
		origSlice := origVal.([]int32)
		if origSlice[0] == 999 {
			t.Fatal("DeepCopy: mutating copied []int32 value affected the original")
		}
	})

	t.Run("nested_map_with_slice_values", func(t *testing.T) {
		t.Parallel()

		var ps PropertySlice
		_ = ps.Set("nested", map[int][]string{1: {"a", "b"}})

		cp := ps.DeepCopy()
		copiedVal, _ := cp.Get("nested")
		copiedMap := copiedVal.(map[int][]string)
		copiedMap[1][0] = "MUTATED"

		origVal, _ := ps.Get("nested")
		origMap := origVal.(map[int][]string)
		if origMap[1][0] == "MUTATED" {
			t.Fatal("DeepCopy: mutating nested slice in copied map[int][]string affected the original")
		}
	})
}

// ─── DeepCopy nil-value preservation tests ──────────────────────────────────

func TestDeepCopyMapWithNilValuePreservesKey(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	_ = ps.Set("m", map[string]any{"present": "yes", "absent": nil})

	cp := ps.DeepCopy()
	copiedVal, ok := cp.Get("m")
	if !ok {
		t.Fatal("Get(\"m\") should return true")
	}
	copiedMap := copiedVal.(map[string]any)
	if len(copiedMap) != 2 {
		t.Fatalf("DeepCopy map len = %d, want 2 (nil value key must survive)", len(copiedMap))
	}
	if _, exists := copiedMap["absent"]; !exists {
		t.Fatal("DeepCopy: nil-value key \"absent\" was deleted")
	}
	if copiedMap["absent"] != nil {
		t.Errorf("DeepCopy: nil-value key should still be nil, got %v", copiedMap["absent"])
	}
}

func TestDeepCopyReflectMapWithNilValuePreservesKey(t *testing.T) {
	t.Parallel()

	// map[int]any hits the reflect path (not the explicit map[string]any case).
	var ps PropertySlice
	_ = ps.Set("m", map[int]any{1: "one", 2: nil})

	cp := ps.DeepCopy()
	copiedVal, ok := cp.Get("m")
	if !ok {
		t.Fatal("Get(\"m\") should return true")
	}
	copiedMap := copiedVal.(map[int]any)
	if len(copiedMap) != 2 {
		t.Fatalf("DeepCopy reflect map len = %d, want 2 (nil value key must survive)", len(copiedMap))
	}
	if _, exists := copiedMap[2]; !exists {
		t.Fatal("DeepCopy: nil-value key 2 was deleted (reflect.SetMapIndex zero Value bug)")
	}
	if copiedMap[2] != nil {
		t.Errorf("DeepCopy: nil-value key should still be nil, got %v", copiedMap[2])
	}
}

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

// ─── DeepCopy nil / empty / independence ────────────────────────────────────

func TestPropertySliceDeepCopyNil(t *testing.T) {
	t.Parallel()

	var ps PropertySlice // nil
	cp := ps.DeepCopy()
	if cp != nil {
		t.Fatalf("DeepCopy of nil should return nil, got %v", cp)
	}
}

func TestPropertySliceDeepCopyEmpty(t *testing.T) {
	t.Parallel()

	ps := PropertySlice{} // non-nil but empty
	cp := ps.DeepCopy()
	if cp == nil {
		t.Fatal("DeepCopy of empty (non-nil) should return non-nil")
	}
	if len(cp) != 0 {
		t.Fatalf("DeepCopy of empty should have len 0, got %d", len(cp))
	}
}

func TestPropertySliceDeepCopyIsIndependent(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	_ = ps.Set("key", "original")

	cp := ps.DeepCopy()
	_ = cp.Set("key", "modified")
	_ = cp.Set("new_key", "new_val")

	// Original unchanged.
	val, _ := ps.Get("key")
	if val != "original" {
		t.Errorf("original value changed to %v after modifying copy", val)
	}
	if ps.Len() != 1 {
		t.Errorf("original Len() = %d after inserting into copy, want 1", ps.Len())
	}
}
