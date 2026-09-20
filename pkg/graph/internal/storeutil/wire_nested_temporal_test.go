package storeutil

import (
	"reflect"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

// wireRoundTrip pushes a property through the full persistence path — encode,
// msgpack marshal, msgpack unmarshal, reconstruct — so the test observes what
// a Badger read or an import actually produces, not an in-memory shortcut.
// The msgpack hop is the load-bearing part: it is what erases Go types.
func wireRoundTrip(t *testing.T, key string, v any) any {
	t.Helper()
	pw, err := propertyToWire(types.Property{Key: key, Value: v})
	if err != nil {
		t.Fatalf("propertyToWire(%#v): %v", v, err)
	}
	blob, err := msgpack.Marshal(pw)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded PropertyWire
	if err := SafeUnmarshal(blob, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := validatePropertyWireValue(decoded.Value, decoded.Type); err != nil {
		t.Fatalf("validatePropertyWireValue: %v", err)
	}
	got, err := reconstructPropertyWireValue(decoded)
	if err != nil {
		t.Fatalf("reconstructPropertyWireValue: %v", err)
	}
	return got
}

// Catches the erasure bug itself: an implementation that writes nested
// temporals as their ISO string (or as a bare msgpack struct) returns a string
// / map here, not a TemporalValue with its kind. The mixed kinds and the two
// renderings of the SAME instant in different zones make a "normalize to one
// canonical instant" implementation fail too — identity is the rendering.
func TestWire_NestedTemporalsRoundTripTyped(t *testing.T) {
	value := []any{
		types.TemporalValue{Kind: types.TemporalDate, Value: "2024-01-01"},
		types.TemporalValue{Kind: types.TemporalDuration, Value: "P1DT2H"},
		types.TemporalValue{Kind: types.TemporalDateTime, Value: "2024-01-01T12:00:00+02:00"},
		types.TemporalValue{Kind: types.TemporalDateTime, Value: "2024-01-01T10:00:00Z"},
		[]any{
			types.TemporalValue{Kind: types.TemporalLocalTime, Value: "12:30:00"},
			map[string]any{"at": types.TemporalValue{Kind: types.TemporalTime, Value: "12:30:00Z"}},
		},
	}
	got := wireRoundTrip(t, "list", value)
	if !reflect.DeepEqual(got, value) {
		t.Fatalf("nested temporal list round-trip:\n got %#v\nwant %#v", got, value)
	}
}

// Catches an implementation that infers temporal-ness from the string shape:
// a genuine string property that looks like a date, and one that is byte-equal
// to a temporal's rendering, must both come back as strings.
func TestWire_NestedISOLookingStringsStayStrings(t *testing.T) {
	value := []any{
		"2024-01-01",
		"P1DT2H",
		types.TemporalValue{Kind: types.TemporalDate, Value: "2024-01-01"},
	}
	got := wireRoundTrip(t, "mixed", value).([]any)
	if _, ok := got[0].(string); !ok {
		t.Fatalf("element 0 = %T, want string", got[0])
	}
	if _, ok := got[1].(string); !ok {
		t.Fatalf("element 1 = %T, want string", got[1])
	}
	tv, ok := got[2].(types.TemporalValue)
	if !ok {
		t.Fatalf("element 2 = %T, want TemporalValue", got[2])
	}
	if tv.Kind != types.TemporalDate {
		t.Fatalf("element 2 kind = %d, want TemporalDate", tv.Kind)
	}
}

// Catches an envelope that is not injective: a caller-supplied list whose FIRST
// element happens to be the reserved marker string must round-trip as that
// list, not decode as a temporal and not fail. Also covers the marker-shaped
// list that has exactly the temporal envelope arity, which an unescaped
// implementation would silently turn into a TemporalValue.
func TestWire_MarkerShapedUserListsRoundTripUnchanged(t *testing.T) {
	cases := map[string][]any{
		"bare marker":           {nestedWireTemporalMarker},
		"temporal-shaped":       {nestedWireTemporalMarker, int64(0), "2024-01-01"},
		"escape marker":         {nestedWireEscapeMarker, "x"},
		"unknown marker":        {nestedWireMarkerPrefix + "zzz", int64(1)},
		"marker not at front":   {"ok", nestedWireTemporalMarker},
		"marker beside a value": {nestedWireTemporalMarker, types.TemporalValue{Kind: types.TemporalDate, Value: "2024-01-01"}},
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			got := wireRoundTrip(t, "k", []any{value})
			want := []any{value}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("round-trip:\n got %#v\nwant %#v", got, want)
			}
		})
	}
}

// Catches a decoder that best-effort guesses at a malformed envelope. Corrupt
// or hostile rows must be rejected, never decoded into a different value.
func TestWire_MalformedNestedEnvelopeRejected(t *testing.T) {
	cases := map[string]any{
		"wrong arity":     []any{nestedWireTemporalMarker, 0},
		"kind not int":    []any{nestedWireTemporalMarker, "0", "2024-01-01"},
		"kind out of set": []any{nestedWireTemporalMarker, 99, "2024-01-01"},
		"iso not string":  []any{nestedWireTemporalMarker, 0, 7},
		"empty iso":       []any{nestedWireTemporalMarker, 0, ""},
		"unknown marker":  []any{nestedWireMarkerPrefix + "nope", 0, "2024-01-01"},
	}
	for name, envelope := range cases {
		t.Run(name, func(t *testing.T) {
			pw := PropertyWire{Key: "k", Type: ptSliceAny, Value: []any{envelope}}
			if _, err := reconstructPropertyWireValue(pw); err == nil {
				t.Fatalf("reconstruct accepted malformed envelope %#v", envelope)
			}
		})
	}
}

// Catches an implementation that rewrites every nested container: a value with
// no temporal and no marker must keep its original wire bytes, so existing
// rows are unchanged and the encoding stays backward compatible.
func TestWire_NonTemporalNestedValuesAreNotRewritten(t *testing.T) {
	value := []any{"a", int64(1), []any{"b"}, map[string]any{"c": "d"}}
	pw, err := propertyToWire(types.Property{Key: "k", Value: value})
	if err != nil {
		t.Fatalf("propertyToWire: %v", err)
	}
	if !reflect.DeepEqual(pw.Value, value) {
		t.Fatalf("wire value was rewritten:\n got %#v\nwant %#v", pw.Value, value)
	}
}

// Catches an envelope decoder that recurses without a bound on hostile input.
func TestWire_NestedEnvelopeDepthBounded(t *testing.T) {
	var deep any = []any{nestedWireTemporalMarker, 0, "2024-01-01"}
	for range maxWireDecodeDepth + 2 {
		deep = []any{deep}
	}
	if _, err := decodeNestedWireValue(deep, 0); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("decodeNestedWireValue on over-deep input = %v, want depth error", err)
	}
}
