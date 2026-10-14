package types

import (
	"fmt"
	"math"
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

func elementKeys(v any) []string {
	var out []string
	ForEachIndexableElementKey(v, func(vk string) bool {
		out = append(out, vk)
		return true
	})
	return out
}

// Backlog 63: the keys an Elements index takes from a value, and the match.
func TestForEachIndexableElementKey(t *testing.T) {
	t.Parallel()
	long := make([]int64, 0, 40)
	for i := range 20 {
		long = append(long, int64(i), int64(i)) // duplicates past the linear dedup bound
	}
	wantLong := make([]string, 0, 20)
	for i := range 20 {
		wantLong = append(wantLong, fmt.Sprintf("i64:%d", i))
	}
	for _, c := range []struct {
		name string
		v    any
		want []string
	}{
		{"scalar is a one-element list", int64(7), []string{"i64:7"}},
		{"duplicates once, first-occurrence order", []int64{3, 1, 3}, []string{"i64:3", "i64:1"}},
		{"empty list", []int64{}, nil},
		{"nil", nil, nil},
		{"mixed list: scalars only", []any{"a", nil, []any{"b"}, map[string]any{"c": 1}, int64(1), "a"}, []string{"s:a", "i64:1"}},
		{"strings", []string{"x", "y"}, []string{"s:x", "s:y"}},
		{"ints keep their type", []int{1}, []string{"i:1"}},
		{"bools", []bool{true, false, true}, []string{"b:true", "b:false"}},
		{"float zero folds", []float64{0, math.Copysign(0, -1)}, []string{"f64:0"}},
		{"float32", []float32{1.5}, []string{"f32:1.5"}},
		{"bytes are a blob", []byte{1, 2}, nil},
		{"map", map[string]any{"a": 1}, nil},
		{"long list dedups through the map", long, wantLong},
	} {
		if got := elementKeys(c.v); !slices.Equal(got, c.want) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
	n := 0
	if ForEachIndexableElementKey([]int64{1, 2, 3}, func(string) bool { n++; return false }) || n != 1 {
		t.Errorf("stop: ran %d, want 1 and false", n)
	}
	if !ForEachIndexableElementKey([]int64{1}, nil) {
		t.Error("nil fn: want true")
	}
	for _, c := range []struct {
		v        any
		elements bool
		target   string
		want     bool
	}{
		{[]int64{1, 2}, true, "i64:2", true},
		{[]int64{1, 2}, false, "i64:2", false},
		{[]int64{1, 2}, true, "i64:3", false},
		{int64(2), true, "i64:2", true},
		{int64(2), false, "i64:2", true},
		{[]int64{1}, true, "", false},
		{[]any{[]any{int64(1)}}, true, "i64:1", false},
	} {
		if got := IndexableValueHasKey(c.v, c.elements, c.target); got != c.want {
			t.Errorf("IndexableValueHasKey(%v, %v, %q) = %v, want %v", c.v, c.elements, c.target, got, c.want)
		}
	}
}

// Node and Relationship carry the same three doors (Testing Rule 2).
func TestNodeRelIndexKeyDoors(t *testing.T) {
	t.Parallel()
	n := NewNode(NodeID(snowflake.ID(1)), 10, nil)
	r := NewRelationship(RelID(snowflake.ID(2)), 3, NodeID(snowflake.ID(1)), NodeID(snowflake.ID(1)))
	type doors struct {
		set     func(string, any) error
		forEach func(func(string) bool, func(string, string) bool)
		perKey  func(string, bool, func(string) bool)
		has     func(string, bool, string) bool
	}
	var nilNode *Node
	var nilRel *Relationship
	for name, d := range map[string]doors{
		"node": {n.SetProperty, n.ForEachIndexKey, n.ForEachPropertyIndexKey, n.HasPropertyIndexKey},
		"rel":  {r.SetProperty, r.ForEachIndexKey, r.ForEachPropertyIndexKey, r.HasPropertyIndexKey},
	} {
		for k, v := range map[string]any{"ov": []int64{5, 6, 5}, "name": "a", "vec": []float32{1, 2}} {
			if err := d.set(k, v); err != nil {
				t.Fatal(err)
			}
		}
		var got []string
		d.forEach(func(k string) bool { return k == "ov" }, func(k, vk string) bool {
			got = append(got, k+"="+vk)
			return true
		})
		slices.Sort(got)
		if want := []string{"name=s:a", "ov=i64:5", "ov=i64:6"}; !slices.Equal(got, want) {
			t.Errorf("%s ForEachIndexKey: %v, want %v", name, got, want)
		}
		got = nil
		d.forEach(nil, func(k, vk string) bool {
			got = append(got, k+"="+vk)
			return true
		})
		if want := []string{"name=s:a"}; !slices.Equal(got, want) {
			t.Errorf("%s ForEachIndexKey(nil): %v, want %v", name, got, want)
		}
		got = nil
		d.perKey("ov", true, func(vk string) bool { got = append(got, vk); return true })
		d.perKey("ov", false, func(vk string) bool { got = append(got, vk); return true })
		d.perKey("missing", true, func(vk string) bool { got = append(got, vk); return true })
		if want := []string{"i64:5", "i64:6"}; !slices.Equal(got, want) {
			t.Errorf("%s ForEachPropertyIndexKey: %v, want %v", name, got, want)
		}
		if !d.has("ov", true, "i64:6") || d.has("ov", false, "i64:6") || d.has("missing", true, "i64:6") || !d.has("name", false, "s:a") {
			t.Errorf("%s HasPropertyIndexKey mismatch", name)
		}
	}
	nilNode.ForEachIndexKey(nil, func(string, string) bool { t.Error("nil node yielded"); return true })
	nilRel.ForEachIndexKey(nil, func(string, string) bool { t.Error("nil rel yielded"); return true })
	nilNode.ForEachPropertyIndexKey("ov", true, func(string) bool { t.Error("nil node yielded"); return true })
	nilRel.ForEachPropertyIndexKey("ov", true, func(string) bool { t.Error("nil rel yielded"); return true })
	if nilNode.HasPropertyIndexKey("ov", true, "i64:5") || nilRel.HasPropertyIndexKey("ov", true, "i64:5") {
		t.Error("nil entity matched")
	}
}
