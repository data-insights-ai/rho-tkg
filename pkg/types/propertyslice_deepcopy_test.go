package types

import (
	"testing"
)

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

		// PropertySlice.Set rejects []int32 because hash/wire only support
		// the documented slice shapes. Construct directly to exercise the
		// reflect fallback used for legacy/bypassed PropertySlice values.
		ps := PropertySlice{{Key: "scores", Value: []int32{1, 2, 3}}}

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

	t.Run("nested_map_with_slice_values_via_reflect_fallback", func(t *testing.T) {
		t.Parallel()

		// PropertySlice.Set now rejects non-string-keyed maps (F2), but
		// DeepCopy still uses reflect for any map shape because legacy data
		// or future-extended types might surface a Property whose value
		// bypassed Set. Construct the slice directly to exercise that path.
		ps := PropertySlice{{Key: "nested", Value: map[int][]string{1: {"a", "b"}}}}

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

	t.Run("named_any_slice_with_nil_value", func(t *testing.T) {
		t.Parallel()

		type namedAnySlice []any

		ps := PropertySlice{{Key: "items", Value: namedAnySlice{nil, []string{"a"}}}}

		cp := ps.DeepCopy()
		copiedVal, _ := cp.Get("items")
		copiedSlice := copiedVal.(namedAnySlice)
		if len(copiedSlice) != 2 {
			t.Fatalf("DeepCopy namedAnySlice len = %d, want 2", len(copiedSlice))
		}
		if copiedSlice[0] != nil {
			t.Fatalf("DeepCopy namedAnySlice[0] = %v, want nil", copiedSlice[0])
		}
		copiedSlice[1].([]string)[0] = "MUTATED"

		origSlice := ps[0].Value.(namedAnySlice)
		if origSlice[1].([]string)[0] == "MUTATED" {
			t.Fatal("DeepCopy: mutating copied namedAnySlice value affected the original")
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
	// Set now rejects non-string-keyed maps (F2), so construct the slice
	// directly to keep exercising the reflect deep-copy fallback for legacy
	// or future-extended values that bypass Set.
	ps := PropertySlice{{Key: "m", Value: map[int]any{1: "one", 2: nil}}}

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

// ─── DeepCopy scalar fast-path tests ─────────────────────────────────────────
//
// Scalars are immutable in Go so deep copy just returns the value.
// These tests verify each scalar fast-path branch is exercised and correct.

func TestDeepCopyScalarFastPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		val  any
	}{
		{"bool_true", true},
		{"bool_false", false},
		{"int", int(42)},
		{"int8", int8(-1)},
		{"int16", int16(1000)},
		{"int32", int32(100000)},
		{"int64", int64(9999999999)},
		{"uint", uint(42)},
		{"uint8", uint8(255)},
		{"uint16", uint16(65535)},
		{"uint32", uint32(4294967295)},
		{"uint64", uint64(18446744073709551615)},
		{"float32", float32(3.14)},
		{"float64", float64(2.718281828)},
		{"string", "hello world"},
		{"string_empty", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var ps PropertySlice
			if err := ps.Set("k", tt.val); err != nil {
				t.Fatalf("Set(%q, %v) failed: %v", "k", tt.val, err)
			}

			cp := ps.DeepCopy()
			got, ok := cp.Get("k")
			if !ok {
				t.Fatal("Get(\"k\") should return true")
			}
			if got != tt.val {
				t.Fatalf("DeepCopy scalar: got %v (%T), want %v (%T)", got, got, tt.val, tt.val)
			}
		})
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
