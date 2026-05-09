package types

import (
	"errors"
	"fmt"
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
