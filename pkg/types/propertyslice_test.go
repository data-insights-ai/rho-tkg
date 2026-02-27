package types

import "testing"

func TestPropertySliceRejectsTKGPrefix(t *testing.T) {
	t.Parallel()

	var ps PropertySlice
	if err := ps.Set("tkg_labels", "x"); err == nil {
		t.Fatal("PropertySlice.Set(\"tkg_labels\", ...) should return error")
	}
	// Verify nothing was stored.
	if ps.Len() != 0 {
		t.Fatalf("PropertySlice should be empty after rejected Set, got %d", ps.Len())
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
