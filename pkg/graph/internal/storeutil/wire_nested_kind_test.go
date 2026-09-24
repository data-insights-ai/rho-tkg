package storeutil

import (
	"bytes"
	"encoding/hex"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

// Backlog item 3: a value NESTED inside an []any / map[string]any has no
// property type tag of its own, and the msgpack hop plus the historical
// integer normalization erased its Go kind (int16 -> int64, uint8 -> uint64,
// int -> int64, uint -> int64, []string -> []any, map[string]string ->
// map[string]any, typed nil -> nil, registered struct -> map). The content
// hash is taken over the exact kinds, so such an entity failed its hash chain
// after a persisted round trip.

type nestedKindCase struct {
	name  string
	value any
}

func nestedKindCases() []nestedKindCase {
	return []nestedKindCase{
		// integers, each at zero and at both range ends
		{"int", int(-5)}, {"int/max", int(math.MaxInt)}, {"int/min", int(math.MinInt)},
		{"int8", int8(2)}, {"int8/max", int8(math.MaxInt8)}, {"int8/min", int8(math.MinInt8)},
		{"int16", int16(2)}, {"int16/max", int16(math.MaxInt16)}, {"int16/min", int16(math.MinInt16)},
		{"int32", int32(2)}, {"int32/max", int32(math.MaxInt32)}, {"int32/min", int32(math.MinInt32)},
		{"int64", int64(2)}, {"int64/max", int64(math.MaxInt64)}, {"int64/min", int64(math.MinInt64)},
		{"uint", uint(2)}, {"uint/max", uint(math.MaxUint)}, {"uint/0", uint(0)},
		{"uint8", uint8(2)}, {"uint8/max", uint8(math.MaxUint8)},
		{"uint16", uint16(2)}, {"uint16/max", uint16(math.MaxUint16)},
		{"uint32", uint32(2)}, {"uint32/max", uint32(math.MaxUint32)},
		{"uint64", uint64(2)}, {"uint64/max", uint64(math.MaxUint64)},
		// floats, incl. the values a lossy narrowing or a DeepEqual would miss
		{"float32", float32(1.5)}, {"float32/max", float32(math.MaxFloat32)},
		{"float32/nan", float32(math.NaN())}, {"float32/-0", float32(math.Copysign(0, -1))},
		{"float32/inf", float32(math.Inf(-1))},
		{"float64", 1.5}, {"float64/integral", float64(2)}, {"float64/nan", math.NaN()},
		{"float64/-0", math.Copysign(0, -1)},
		// other scalars
		{"bool", true}, {"string", "x"}, {"nil", nil},
		{"temporal", types.TemporalValue{Kind: types.TemporalDate, Value: "2024-01-02"}},
		// typed containers: non-empty, empty and typed nil
		{"[]string", []string{"a", "b"}}, {"[]string/empty", []string{}}, {"[]string/nil", []string(nil)},
		{"[]int", []int{1, -2, math.MaxInt}}, {"[]int/empty", []int{}}, {"[]int/nil", []int(nil)},
		{"[]int64", []int64{1, math.MinInt64}}, {"[]int64/empty", []int64{}}, {"[]int64/nil", []int64(nil)},
		{"[]float32", []float32{1.5, float32(math.NaN())}}, {"[]float32/empty", []float32{}}, {"[]float32/nil", []float32(nil)},
		{"[]float64", []float64{1.5, 2}}, {"[]float64/empty", []float64{}}, {"[]float64/nil", []float64(nil)},
		{"[]byte", []byte{1, 2}}, {"[]byte/empty", []byte{}}, {"[]byte/nil", []byte(nil)},
		{"[]bool", []bool{true, false}}, {"[]bool/empty", []bool{}}, {"[]bool/nil", []bool(nil)},
		{"map[string]string", map[string]string{"a": "b"}}, {"map[string]string/empty", map[string]string{}},
		{"map[string]string/nil", map[string]string(nil)},
		{"[]any/empty", []any{}}, {"[]any/nil", []any(nil)},
		{"map[string]any/empty", map[string]any{}}, {"map[string]any/nil", map[string]any(nil)},
		// a registered struct, by value and by pointer
		{"custom", wireValueDirectCustom{X: 7}}, {"*custom", &wireValueDirectCustom{X: 7}},
		// a list that mixes kinds, and one whose first element is a small int
		{"mixed", []any{int8(1), uint16(2), int(3), "s", []string{"t"}}},
	}
}

// nestedKindWrappers places a value at depth 1 and depth 2 inside both
// container kinds.
func nestedKindWrappers() []struct {
	name string
	wrap func(any) any
} {
	return []struct {
		name string
		wrap func(any) any
	}{
		{"slice", func(v any) any { return []any{v} }},
		{"map", func(v any) any { return map[string]any{"k": v} }},
		{"slice/slice", func(v any) any { return []any{"x", []any{v}} }},
		{"slice/map", func(v any) any { return []any{map[string]any{"k": v}} }},
		{"map/slice", func(v any) any { return map[string]any{"k": []any{int64(0), v}} }},
		{"map/map", func(v any) any { return map[string]any{"k": map[string]any{"k": v}} }},
	}
}

func registerNestedKindCustom(t *testing.T) {
	t.Helper()
	if err := types.RegisterPropertyStructType(wireValueDirectCustom{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}
}

// assertSameValue compares value AND Go kind at every depth: DeepEqual
// compares the dynamic types of interface elements, and the hash bytes cover
// NaN and -0, which DeepEqual cannot.
func assertSameValue(t *testing.T, want, got any) {
	t.Helper()
	wh := types.AppendPropertyValueHashBytes(nil, want)
	gh := types.AppendPropertyValueHashBytes(nil, got)
	if !bytes.Equal(wh, gh) {
		t.Fatalf("hash bytes differ:\n got %#v\nwant %#v", got, want)
	}
	if !containsNaN(want) && !reflect.DeepEqual(want, got) {
		t.Fatalf("value differs:\n got %#v\nwant %#v", got, want)
	}
}

func containsNaN(v any) bool {
	switch x := v.(type) {
	case float32:
		return math.IsNaN(float64(x))
	case float64:
		return math.IsNaN(x)
	case []float32:
		for _, f := range x {
			if math.IsNaN(float64(f)) {
				return true
			}
		}
	case []any:
		for _, e := range x {
			if containsNaN(e) {
				return true
			}
		}
	case map[string]any:
		for _, e := range x {
			if containsNaN(e) {
				return true
			}
		}
	}
	return false
}

// TestWire_NestedKindsRoundTripExact drives every nested kind through the two
// byte paths that persist it: the relationship and node rows the badger store
// writes (marshal, checked unmarshal, checked reconstruction) and the
// standalone property-slice codec of the segment fallback column.
func TestWire_NestedKindsRoundTripExact(t *testing.T) {
	registerNestedKindCustom(t)
	for _, c := range nestedKindCases() {
		for _, w := range nestedKindWrappers() {
			t.Run(c.name+"/"+w.name, func(t *testing.T) {
				want := w.wrap(c.value)
				ps, err := types.NewPropertySlice(map[string]any{"p": want})
				if err != nil {
					t.Fatalf("NewPropertySlice: %v", err)
				}

				r := types.NewRelationship(1, 1, 1, 2)
				if err := r.SetProperties(ps); err != nil {
					t.Fatalf("SetProperties: %v", err)
				}
				blob, err := MarshalRelWire(r)
				if err != nil {
					t.Fatalf("MarshalRelWire: %v", err)
				}
				rw, err := UnmarshalRelWireWithKeys(blob, nil)
				if err != nil {
					t.Fatalf("UnmarshalRelWireWithKeys: %v", err)
				}
				got, err := WireToRelChecked(rw)
				if err != nil {
					t.Fatalf("WireToRelChecked: %v", err)
				}
				gv, _ := got.GetProperty("p")
				assertSameValue(t, want, gv)

				n := types.NewNode(1, 1, nil)
				if err := n.SetProperties(ps); err != nil {
					t.Fatalf("node SetProperties: %v", err)
				}
				nblob, err := MarshalNodeWire(n)
				if err != nil {
					t.Fatalf("MarshalNodeWire: %v", err)
				}
				nw, err := UnmarshalNodeWireWithKeys(nblob, nil)
				if err != nil {
					t.Fatalf("UnmarshalNodeWireWithKeys: %v", err)
				}
				gotN, err := WireToNodeChecked(nw)
				if err != nil {
					t.Fatalf("WireToNodeChecked: %v", err)
				}
				nv, _ := gotN.GetProperty("p")
				assertSameValue(t, want, nv)

				sb, err := MarshalPropertySlice(ps)
				if err != nil {
					t.Fatalf("MarshalPropertySlice: %v", err)
				}
				gps, err := UnmarshalPropertySlice(sb)
				if err != nil {
					t.Fatalf("UnmarshalPropertySlice: %v", err)
				}
				sv, _ := gps.Get("p")
				assertSameValue(t, want, sv)
			})
		}
	}
}

// Rows written before the fix carry nested integers raw. They must still
// decode — to the same (widened) values the previous release returned, since
// the original kind is not recoverable from those bytes. The fixtures are the
// bytes the unfixed encoder produced for
//
//	{"n": []any{int16(2), uint8(3), int(4), []string{"a"},
//	            map[string]any{"m": int32(5)}, []any(nil), uint(6)}}
//
// as a relationship row (id 1, type 1, 1 -> 2) and as a property slice.
const (
	legacyNestedRelHex = "89a26676cc02a26964d30000000000000001a2727401a173d30000000000000001a165d30000000000000002a1709183a16ba16ea17697d10002cc030491a16181a16dd200000005c006a174cc15a17600a27466d30000000000000000a27474d30000000000000000"
	legacyNestedPSHex  = "9183a16ba16ea17697d10002cc030491a16181a16dd200000005c006a174cc15"
)

func legacyNestedDecoded() []any {
	return []any{int64(2), uint64(3), int64(4), []any{"a"}, map[string]any{"m": int64(5)}, nil, int64(6)}
}

func TestWire_LegacyNestedIntegersStillDecode(t *testing.T) {
	relBlob, _ := hex.DecodeString(legacyNestedRelHex)
	rw, err := UnmarshalRelWireWithKeys(relBlob, nil)
	if err != nil {
		t.Fatalf("UnmarshalRelWireWithKeys: %v", err)
	}
	r, err := WireToRelChecked(rw)
	if err != nil {
		t.Fatalf("WireToRelChecked on a pre-fix row: %v", err)
	}
	v, _ := r.GetProperty("n")
	if !reflect.DeepEqual(v, legacyNestedDecoded()) {
		t.Fatalf("pre-fix row decoded as %#v, want %#v", v, legacyNestedDecoded())
	}

	psBlob, _ := hex.DecodeString(legacyNestedPSHex)
	ps, err := UnmarshalPropertySlice(psBlob)
	if err != nil {
		t.Fatalf("UnmarshalPropertySlice on pre-fix bytes: %v", err)
	}
	v, _ = ps.Get("n")
	if !reflect.DeepEqual(v, legacyNestedDecoded()) {
		t.Fatalf("pre-fix slice decoded as %#v, want %#v", v, legacyNestedDecoded())
	}
}

// The kind envelope is only written where the kind would otherwise be lost:
// values whose kind msgpack already preserves keep their exact previous
// bytes (int64, uint64, float32, float64, bool, string, []byte, nil and
// untyped containers).
func TestWire_NativeNestedKindsAreNotRewritten(t *testing.T) {
	value := []any{"a", int64(-1), uint64(math.MaxUint64), float32(1.5), 2.5, true, nil, []byte{1},
		[]any{int64(1)}, map[string]any{"c": uint64(1)}}
	pw, err := propertyToWire(types.Property{Key: "k", Value: value})
	if err != nil {
		t.Fatalf("propertyToWire: %v", err)
	}
	if !reflect.DeepEqual(pw.Value, value) {
		t.Fatalf("wire value was rewritten:\n got %#v\nwant %#v", pw.Value, value)
	}
}

// decodeWireProperty runs a hand-built property wire through msgpack and the
// checked validator, as a stored row would.
func decodeWireProperty(t *testing.T, value any) error {
	t.Helper()
	blob, err := msgpack.Marshal([]PropertyWire{{Key: "k", Type: ptSliceAny, Value: value}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	_, err = UnmarshalPropertySlice(blob)
	return err
}

// Checked reconstruction keeps rejecting lossy, non-canonical and malformed
// kind envelopes: a corrupt or hostile row must fail, never decode into a
// silently different value.
func TestWire_MalformedNestedKindEnvelopeRejected(t *testing.T) {
	registerNestedKindCustom(t)
	customData, err := msgpack.Marshal(wireValueDirectCustom{X: 7})
	if err != nil {
		t.Fatal(err)
	}
	const customName = "storeutil.wireValueDirectCustom"
	cases := []struct {
		name  string
		value any
	}{
		{"kind: no tag", []any{[]any{nestedWireKindMarker}}},
		{"kind: too many elements", []any{[]any{nestedWireKindMarker, ptInt16, 2, 3}}},
		{"kind: tag not an integer", []any{[]any{nestedWireKindMarker, "4", 2}}},
		{"kind: unknown tag", []any{[]any{nestedWireKindMarker, 99, 2}}},
		{"kind: tag the encoder never wraps (int64)", []any{[]any{nestedWireKindMarker, ptInt64, 2}}},
		{"kind: tag the encoder never wraps (string)", []any{[]any{nestedWireKindMarker, ptString, "s"}}},
		{"kind: tag the encoder never wraps (float64)", []any{[]any{nestedWireKindMarker, ptFloat64, 1.5}}},
		{"kind: tag the encoder never wraps ([]any)", []any{[]any{nestedWireKindMarker, ptSliceAny, []any{}}}},
		{"kind: temporal tag", []any{[]any{nestedWireKindMarker, ptTemporal, []any{0, "2024-01-01"}}}},
		{"kind: custom tag", []any{[]any{nestedWireKindMarker, ptCustom, customData}}},
		{"kind: int8 out of range", []any{[]any{nestedWireKindMarker, ptInt8, 300}}},
		{"kind: uint8 negative", []any{[]any{nestedWireKindMarker, ptUint8, -1}}},
		{"kind: int16 carries a string", []any{[]any{nestedWireKindMarker, ptInt16, "x"}}},
		{"kind: nil payload", []any{[]any{nestedWireKindMarker, ptInt16, nil}}},
		{"kind: []float32 lossy element", []any{[]any{nestedWireKindMarker, ptSliceF32, []any{1e300}}}},
		{"kind: []string with an int", []any{[]any{nestedWireKindMarker, ptSliceStr, []any{"a", 1}}}},
		{"kind: typed nil of a scalar tag", []any{[]any{nestedWireKindMarker, ptInt16}}},
		{"kind: typed nil of an unknown tag", []any{[]any{nestedWireKindMarker, 99}}},
		{"custom: too few elements", []any{[]any{nestedWireCustomMarker, customName, false}}},
		{"custom: type name not a string", []any{[]any{nestedWireCustomMarker, 1, false, customData}}},
		{"custom: pointer flag not a bool", []any{[]any{nestedWireCustomMarker, customName, 1, customData}}},
		{"custom: data not bytes", []any{[]any{nestedWireCustomMarker, customName, false, "x"}}},
		{"custom: unregistered type", []any{[]any{nestedWireCustomMarker, "no/such", false, customData}}},
		{"custom: corrupt payload", []any{[]any{nestedWireCustomMarker, customName, false, []byte{0xc1}}}},
		{"kind: inside a map", map[string]any{"m": []any{nestedWireKindMarker, ptInt8, 300}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := decodeWireProperty(t, c.value); err == nil {
				t.Fatalf("accepted malformed envelope %#v", c.value)
			}
		})
	}
}

// A registered struct nested in a list gets the same per-value proof as a
// top-level one: a value whose msgpack round trip changes its hash bytes is
// refused at write time rather than stored lossy.
func TestWire_NestedCustomRoundTripHashMismatchRejected(t *testing.T) {
	if err := types.RegisterPropertyStructType(wireValueDirectHiddenFieldCustom{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}
	v := []any{wireValueDirectHiddenFieldCustom{Visible: 1, hidden: 9}}
	_, err := propertyToWire(types.Property{Key: "k", Value: v})
	if err == nil || !strings.Contains(err.Error(), "round-trip") {
		t.Fatalf("propertyToWire(nested lossy custom) = %v, want round-trip error", err)
	}
}

// The envelopes stay inside the reserved marker namespace, so a caller list
// that starts with the marker string is escaped and round-trips unchanged.
func TestWire_KindMarkerShapedUserListRoundTrips(t *testing.T) {
	for _, first := range []string{nestedWireKindMarker, nestedWireCustomMarker} {
		want := []any{[]any{first, int64(4), int64(2)}}
		got := wireRoundTrip(t, "k", want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("marker-shaped list %q:\n got %#v\nwant %#v", first, got, want)
		}
	}
	if !strings.HasPrefix(nestedWireKindMarker, nestedWireMarkerPrefix) ||
		!strings.HasPrefix(nestedWireCustomMarker, nestedWireMarkerPrefix) {
		t.Fatalf("markers %q / %q outside the reserved prefix", nestedWireKindMarker, nestedWireCustomMarker)
	}
}
