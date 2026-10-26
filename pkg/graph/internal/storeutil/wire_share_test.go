package storeutil

import (
	"testing"
	"unsafe"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestUnmarshalNodeWireSharing pins R37's share: a version decoded beside a
// frozen node takes that node's equal string values (the same bytes in
// memory) and, when every property equals, its whole property slice, which a
// write to the version copies first. A changed value, an added key and a
// node that is not frozen decode as before. Catches a share of unequal
// bytes, a slice shared over a difference, and a write through the alias.
func TestUnmarshalNodeWireSharing(t *testing.T) {
	t.Parallel()
	node := func(props map[string]any) *types.Node {
		n := types.NewNode(types.NodeID(7), 1, nil)
		for k, v := range props {
			if err := n.SetProperty(k, v); err != nil {
				t.Fatal(err)
			}
		}
		return n
	}
	decode := func(version, share *types.Node) *types.Node {
		t.Helper()
		raw, err := MarshalNodeWire(version)
		if err != nil {
			t.Fatal(err)
		}
		var w NodeWire
		if err := UnmarshalNodeWireSharing(raw, &w, share); err != nil {
			t.Fatal(err)
		}
		n, err := WireToNodeChecked(w)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	sharesSlice := func(v, cur *types.Node) bool { // v's property slice is cur's
		v.Freeze()
		a, _ := v.FrozenPropertiesAlias()
		c, _ := cur.FrozenPropertiesAlias()
		return len(a) > 0 && len(c) > 0 && &a[0] == &c[0]
	}
	same := func(a, b any) bool { // the same string bytes in memory
		as, bs := a.(string), b.(string)
		return unsafe.StringData(as) == unsafe.StringData(bs)
	}
	props := map[string]any{"a": "alpha-alpha-alpha", "b": "beta-beta-beta-beta", "n": int64(3)}
	current := node(props)
	current.Freeze()

	// Every property equal but n (not a string): values shared, slice not.
	v := decode(node(props), current)
	for _, k := range []string{"a", "b"} {
		got, _ := v.GetProperty(k)
		want, _ := current.GetProperty(k)
		if got != want || !same(got, want) {
			t.Fatalf("equal value %q = %v, not current's bytes", k, got)
		}
	}
	if got, _ := v.GetProperty("n"); got != int64(3) {
		t.Fatalf("n = %v, want 3", got)
	}

	// Only strings, all equal: the slice is current's; a write copies first.
	strs := map[string]any{"a": "alpha-alpha-alpha", "b": "beta-beta-beta-beta"}
	cur2 := node(strs)
	cur2.Freeze()
	if !sharesSlice(decode(node(strs), cur2), cur2) {
		t.Fatal("all properties equal: the version does not share current's property slice")
	}
	v = decode(node(strs), cur2)
	if err := v.SetProperty("a", "changed"); err != nil {
		t.Fatal(err)
	}
	if got, _ := cur2.GetProperty("a"); got != "alpha-alpha-alpha" {
		t.Fatalf("a write to the version reached the current row: a = %v", got)
	}

	// A changed value: decoded as stored, never current's bytes.
	changed := map[string]any{"a": "alpha-alpha-alphA", "b": "beta-beta-beta-beta"}
	v = decode(node(changed), cur2)
	if got, _ := v.GetProperty("a"); got != "alpha-alpha-alphA" {
		t.Fatalf("changed a = %v", got)
	}
	gotB, _ := v.GetProperty("b")
	curB, _ := cur2.GetProperty("b")
	if !same(gotB, curB) {
		t.Fatalf("unchanged b beside a changed a is not current's bytes")
	}
	if sharesSlice(v, cur2) {
		t.Fatal("a changed value: the version shares current's property slice")
	}

	// An added key: not the slice.
	v = decode(node(map[string]any{"a": "alpha-alpha-alpha", "b": "beta-beta-beta-beta", "c": "x"}), cur2)
	if v.PropertyCount() != 3 {
		t.Fatalf("added key: %d properties, want 3", v.PropertyCount())
	}

	// A node that is not frozen lends values, never its slice.
	open := node(strs)
	v = decode(node(strs), open)
	open.Freeze() // only now, to compare
	if sharesSlice(v, open) {
		t.Fatal("an unfrozen node's property slice was shared")
	}
}
