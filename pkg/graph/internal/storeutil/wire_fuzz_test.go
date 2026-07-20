package storeutil

// Fuzz harness for the on-disk / import wire trust boundary.
//
// Untrusted msgpack bytes decode into NodeWire/RelWire, then the CHECKED
// reconstructors (WireToNodeChecked / WireToRelChecked) build live entities.
// The contract under attack:
//
//   1. A checked decode of ANY decoded wire must NEVER panic. It returns either
//      a usable entity or an error. (A panic here is a store/import crash on a
//      single corrupt or hostile row — a denial of service on the trust
//      boundary.)
//   2. ValidateNodeWire / ValidateRelWire is a COMPLETE gate for the unchecked
//      reconstructor: whenever the checked path accepts a wire, the unchecked
//      MustWireToNode / MustWireToRel (which panic on invalid prevalidated input) must
//      also not panic. This pins that validation covers everything
//      reconstruction asserts.
//   3. Anything the checked decoder accepts must be re-encodable
//      (MarshalNodeWire / MarshalRelWire must not error) — no
//      read-but-cannot-persist asymmetry.
//
// The seed corpus (run as ordinary tests under `go test`) doubles as a
// permanent regression battery; active fuzzing is `go test -fuzz`.

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

// fuzzMustProps builds a sorted PropertySlice from a map, failing the test on
// any disallowed value (seeds are author-controlled, so an error is a test bug).
func fuzzMustProps(tb testing.TB, m map[string]any) types.PropertySlice {
	tb.Helper()
	var ps types.PropertySlice
	for k, v := range m {
		if err := ps.Set(k, v); err != nil {
			tb.Fatalf("seed property %q: %v", k, err)
		}
	}
	return ps
}

// seedNodes returns realistic Nodes covering the property type-tag space,
// temporal metadata, and integrity fields.
func seedNodes(tb testing.TB) []*types.Node {
	tb.Helper()
	var out []*types.Node

	n1 := types.NewNode(types.NodeID(snowflake.ID(1001)), 1, []uint16{2, 3})
	n1.SetVersion(5)
	n1.SetProperties(fuzzMustProps(tb, map[string]any{
		"name":      "Alice",
		"age":       int64(30),
		"score":     float64(9.5),
		"f32":       float32(1.25),
		"flag":      true,
		"tags":      []string{"a", "b"},
		"ints":      []int{1, 2, 3},
		"i64s":      []int64{4, 5},
		"f64s":      []float64{1.5, 2.5},
		"f32s":      []float32{0.5, 0.25},
		"bools":     []bool{true, false},
		"blob":      []byte{0x00, 0x01, 0x02},
		"anyslice":  []any{int64(1), "x", true},
		"mapstrany": map[string]any{"k": int64(7)},
		"mapstrstr": map[string]string{"k": "v"},
		"when":      types.TemporalValue{Kind: types.TemporalDate, Value: "2024-01-01"},
		"u":         uint(8),
		"u8":        uint8(255),
		"i8":        int8(-12),
	}))
	n1.SetTemporal(&types.TemporalMetadata{
		ValidFrom: 100, ValidTo: 200, TxFrom: 300, TxTo: 400,
		CreatedAt: 500, UpdatedAt: 600, DeletedAt: 700,
		CreatedBy: "admin", UpdatedBy: "system",
	})
	n1.Temporal().SetBaseEntityID(types.EntityID(999))
	n1.SetIntegrity(&types.NodeIntegrity{
		Hash: "abc123", PrevHash: "def456",
		AuthorID: "auth", Signature: []byte{0xaa, 0xbb},
		AuthorizedBy: "boss", AuthorizationLevel: 7,
	})
	out = append(out, n1)

	// Minimal node: no properties, no temporal, no integrity.
	out = append(out, types.NewNode(types.NodeID(snowflake.ID(42)), 1, nil))

	// Typed-nil slice/map properties.
	n3 := types.NewNode(types.NodeID(snowflake.ID(7)), 2, nil)
	n3.SetProperties(fuzzMustProps(tb, map[string]any{
		"nilstr": []string(nil),
		"nilmap": map[string]any(nil),
	}))
	out = append(out, n3)

	return out
}

func seedRels(tb testing.TB) []*types.Relationship {
	tb.Helper()
	var out []*types.Relationship

	r1 := types.NewRelationship(types.RelID(snowflake.ID(500)), 3, types.NodeID(snowflake.ID(100)), types.NodeID(snowflake.ID(200)))
	r1.SetVersion(2)
	r1.SetProperties(fuzzMustProps(tb, map[string]any{
		"weight": float64(1.5),
		"label":  "knows",
		"since":  int64(2020),
	}))
	r1.SetTemporal(&types.TemporalMetadata{ValidFrom: 10, ValidTo: 20, TxFrom: 30})
	r1.SetIntegrity(&types.RelIntegrity{
		Hash: "h", PrevHash: "p", FromNodeHash: "fnh", ToNodeHash: "tnh",
	})
	out = append(out, r1)

	out = append(out, types.NewRelationship(types.RelID(snowflake.ID(1)), 1, types.NodeID(snowflake.ID(2)), types.NodeID(snowflake.ID(3))))
	return out
}

// craftedNodeWires returns wires whose fields exercise the validator's
// rejection paths. Each must be REJECTED by WireToNodeChecked without panic.
func craftedNodeWires() []NodeWire {
	base := func() NodeWire { return NodeWire{ID: 100, PrimaryLabel: 1, Version: 1} }
	mk := func(mut func(*NodeWire)) NodeWire { w := base(); mut(&w); return w }
	return []NodeWire{
		{},                                  // zero -> ID 0 rejected
		mk(func(w *NodeWire) { w.ID = -1 }), // negative id
		mk(func(w *NodeWire) { w.PrimaryLabel = 0 }),               // reserved primary token
		mk(func(w *NodeWire) { w.PrimaryLabel = 1 << 20 }),         // token > uint16
		mk(func(w *NodeWire) { w.Version = -1 }),                   // negative version
		mk(func(w *NodeWire) { w.ExtraLabels = []int{0} }),         // reserved extra token
		mk(func(w *NodeWire) { w.ExtraLabels = []int{1} }),         // dup of primary
		mk(func(w *NodeWire) { w.ExtraLabels = []int{2, 2} }),      // dup extra
		mk(func(w *NodeWire) { w.ExtraLabels = []int{1 << 20} }),   // extra > uint16
		mk(func(w *NodeWire) { w.BaseEntityID = -1 }),              // negative base id
		mk(func(w *NodeWire) { w.ValidFrom = 50; w.ValidTo = 40 }), // vf>=vt while HasTemporal=false
		mk(func(w *NodeWire) { w.FormatVersion = 250 }),            // unsupported future format
		// temporal payload set but HasTemporal false
		mk(func(w *NodeWire) { w.CreatedBy = "x" }),
		// property: type/value mismatch (tag int, value string)
		mk(func(w *NodeWire) { w.Properties = []PropertyWire{{Key: "k", Type: ptInt, Value: "not-an-int"}} }),
		// property: unknown high type tag
		mk(func(w *NodeWire) { w.Properties = []PropertyWire{{Key: "k", Type: 200, Value: int64(1)}} }),
		// property: custom tag, unregistered
		mk(func(w *NodeWire) {
			w.Properties = []PropertyWire{{Key: "k", Type: ptCustom, Value: []byte("x"), CustomType: "nope"}}
		}),
		// property: shadow key
		mk(func(w *NodeWire) { w.Properties = []PropertyWire{{Key: "tkg_x", Type: ptString, Value: "v"}} }),
		// property: out of sorted order
		mk(func(w *NodeWire) {
			w.Properties = []PropertyWire{
				{Key: "b", Type: ptString, Value: "1"},
				{Key: "a", Type: ptString, Value: "2"},
			}
		}),
		// property: typed-nil marker on a non-nillable type tag
		mk(func(w *NodeWire) { w.Properties = []PropertyWire{{Key: "k", Type: ptInt, Nil: true}} }),
		// property: typed-nil but carries a value
		mk(func(w *NodeWire) {
			w.Properties = []PropertyWire{{Key: "k", Type: ptSliceStr, Nil: true, Value: []any{"x"}}}
		}),
		// property: custom metadata on non-custom tag
		mk(func(w *NodeWire) {
			w.Properties = []PropertyWire{{Key: "k", Type: ptString, Value: "v", CustomType: "T"}}
		}),
		// property: temporal malformed (wrong shape)
		mk(func(w *NodeWire) { w.Properties = []PropertyWire{{Key: "k", Type: ptTemporal, Value: "not-a-pair"}} }),
		// property: temporal kind out of range
		mk(func(w *NodeWire) {
			w.Properties = []PropertyWire{{Key: "k", Type: ptTemporal, Value: []any{int64(99), "2024-01-01"}}}
		}),
	}
}

func craftedRelWires() []RelWire {
	base := func() RelWire { return RelWire{ID: 100, RelType: 1, StartID: 2, EndID: 3, Version: 1} }
	mk := func(mut func(*RelWire)) RelWire { w := base(); mut(&w); return w }
	return []RelWire{
		{},
		mk(func(w *RelWire) { w.ID = -1 }),
		mk(func(w *RelWire) { w.StartID = 0 }),
		mk(func(w *RelWire) { w.EndID = -5 }),
		mk(func(w *RelWire) { w.RelType = 0 }),
		mk(func(w *RelWire) { w.RelType = 1 << 20 }),
		mk(func(w *RelWire) { w.Version = -1 }),
		mk(func(w *RelWire) { w.BaseEntityID = -1 }),
		mk(func(w *RelWire) { w.ValidFrom = 50; w.ValidTo = 40 }),
		mk(func(w *RelWire) { w.FormatVersion = 250 }),
		mk(func(w *RelWire) { w.UpdatedBy = "x" }), // temporal payload, HasTemporal false
		mk(func(w *RelWire) { w.Properties = []PropertyWire{{Key: "k", Type: ptBool, Value: int64(1)}} }),
	}
}

func mustMarshal(tb testing.TB, v any) []byte {
	tb.Helper()
	b, err := msgpack.Marshal(v)
	if err != nil {
		tb.Fatalf("seed marshal: %v", err)
	}
	return b
}

// exerciseNode touches every reconstructed-entity accessor that could panic if
// reconstruction produced a malformed internal state.
func exerciseNode(t *testing.T, n *types.Node) {
	t.Helper()
	_ = n.DeepCopy()
	_ = n.Properties()
	_ = n.PropertiesMap()
	_ = n.AllLabelTokens()
	_ = n.Temporal()
	_ = n.Integrity()
}

func exerciseRel(t *testing.T, r *types.Relationship) {
	t.Helper()
	_ = r.DeepCopy()
	_ = r.Properties()
	_ = r.PropertiesMap()
	_ = r.Temporal()
	_ = r.Integrity()
}

func FuzzWireToNodeChecked(f *testing.F) {
	for _, n := range seedNodes(f) {
		f.Add(mustMarshal(f, NodeToWire(n)))
	}
	for _, w := range craftedNodeWires() {
		f.Add(mustMarshal(f, w))
	}
	// Raw adversarial byte seeds.
	for _, b := range [][]byte{{}, {0x00}, {0xc0}, {0x80}, {0x90}, {0xff, 0xff}, {0x81, 0xa2, 'i', 'd', 0xff}} {
		f.Add(b)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		var w NodeWire
		// SafeUnmarshal is the trust-boundary decode: it must NEVER panic and
		// must NEVER let the msgpack decoder fatal-overflow the stack — it
		// returns an error on hostile/corrupt bytes instead.
		if err := SafeUnmarshal(data, &w); err != nil {
			return
		}
		n, err := WireToNodeChecked(w) // invariant 1: must not panic
		if err != nil {
			return
		}
		if n == nil {
			t.Fatalf("WireToNodeChecked returned (nil, nil) for wire %+v", w)
		}
		exerciseNode(t, n)
		// invariant 2: validation is a complete gate for the unchecked path.
		if verr := ValidateNodeWire(w); verr != nil {
			t.Fatalf("WireToNodeChecked accepted a wire ValidateNodeWire rejects: %v\nwire=%+v", verr, w)
		}
		_ = MustWireToNode(w) // must not panic once validation has passed
		// invariant 3: anything readable must be re-encodable.
		if _, merr := MarshalNodeWire(n); merr != nil {
			t.Fatalf("decoded node cannot be re-encoded (read-but-cannot-persist): %v\nwire=%+v", merr, w)
		}
	})
}

func FuzzWireToRelChecked(f *testing.F) {
	for _, r := range seedRels(f) {
		f.Add(mustMarshal(f, RelToWire(r)))
	}
	for _, w := range craftedRelWires() {
		f.Add(mustMarshal(f, w))
	}
	for _, b := range [][]byte{{}, {0x00}, {0xc0}, {0x80}, {0x90}, {0xff}} {
		f.Add(b)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		var w RelWire
		if err := SafeUnmarshal(data, &w); err != nil {
			return
		}
		r, err := WireToRelChecked(w)
		if err != nil {
			return
		}
		if r == nil {
			t.Fatalf("WireToRelChecked returned (nil, nil) for wire %+v", w)
		}
		exerciseRel(t, r)
		if verr := ValidateRelWire(w); verr != nil {
			t.Fatalf("WireToRelChecked accepted a wire ValidateRelWire rejects: %v\nwire=%+v", verr, w)
		}
		_ = MustWireToRel(w)
		if _, merr := MarshalRelWire(r); merr != nil {
			t.Fatalf("decoded relationship cannot be re-encoded (read-but-cannot-persist): %v\nwire=%+v", merr, w)
		}
	})
}

// FuzzUnmarshalNodeWireWithKeys attacks the production decode path (the one
// badger uses): raw bytes -> NodeWire -> property-key token resolution -> checked.
func FuzzUnmarshalNodeWireWithKeys(f *testing.F) {
	reg := registrypkg.NewPropertyKeyRegistry()
	// Pre-populate a few tokens so token resolution has live entries to hit.
	for _, k := range []string{"name", "age", "weight"} {
		_, _ = reg.GetOrCreate(k)
	}
	for _, n := range seedNodes(f) {
		f.Add(mustMarshal(f, NodeToWire(n)))
	}
	// A wire carrying a tokenized key (KeyToken set, Key empty).
	f.Add(mustMarshal(f, NodeWire{ID: 5, PrimaryLabel: 1, Version: 1,
		Properties: []PropertyWire{{KeyToken: 1, Type: ptString, Value: "v"}}}))
	// A wire referencing a token the registry does not know.
	f.Add(mustMarshal(f, NodeWire{ID: 5, PrimaryLabel: 1, Version: 1,
		Properties: []PropertyWire{{KeyToken: 60000, Type: ptString, Value: "v"}}}))

	f.Fuzz(func(t *testing.T, data []byte) {
		w, err := UnmarshalNodeWireWithKeys(data, reg) // must not panic
		if err != nil {
			return
		}
		n, err := WireToNodeChecked(w)
		if err != nil {
			return
		}
		if n == nil {
			t.Fatalf("WireToNodeChecked returned (nil, nil) after key resolution for wire %+v", w)
		}
		exerciseNode(t, n)
	})
}
