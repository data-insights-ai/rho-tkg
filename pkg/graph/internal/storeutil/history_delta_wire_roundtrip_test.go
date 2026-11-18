package storeutil

import (
	"reflect"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// TestHistoryDeltaRoundTrip proves every hand-written NodeHistoryDelta/
// RelHistoryDelta/propKeyRef EncodeMsgpack/DecodeMsgpack pair
// reproduces the original struct exactly, for both the all-omitted-optional-
// fields case and the every-optional-field-present case. The node and rel
// deltas also round-trip through the on-disk door (EncodeNodeHistoryDelta /
// DecodeNodeHistoryDelta and the Rel pair), whose value starts with
// historyDeltaTag before the msgpack body.
func TestHistoryDeltaRoundTrip(t *testing.T) {
	t.Parallel()
	nw := NodeWire{FormatVersion: 2, ID: 100, PrimaryLabel: 1, Version: 2}
	rw := RelWire{FormatVersion: 2, ID: 200, RelType: 2, StartID: 10, EndID: 20, Version: 2}
	ps := []PropertyWire{{KeyToken: 3, Value: int64(9), Type: 2}}
	pr := []propKeyRef{{Token: 5}, {Key: "old"}}

	node := func(d any) ([]byte, func([]byte) (any, error), error) {
		raw, err := EncodeNodeHistoryDelta(d.(NodeHistoryDelta))
		return raw, func(b []byte) (any, error) { return DecodeNodeHistoryDelta(b) }, err
	}
	rel := func(d any) ([]byte, func([]byte) (any, error), error) {
		raw, err := EncodeRelHistoryDelta(d.(RelHistoryDelta))
		return raw, func(b []byte) (any, error) { return DecodeRelHistoryDelta(b) }, err
	}

	cases := []struct {
		name string
		in   any
		out  any
		// disk encodes through the on-disk door and returns its decoder; nil
		// for a type that is only ever a field of a delta.
		disk func(any) ([]byte, func([]byte) (any, error), error)
	}{
		{"NodeHistoryDelta_min", NodeHistoryDelta{Meta: nw}, &NodeHistoryDelta{}, node},
		{"NodeHistoryDelta_full", NodeHistoryDelta{Meta: nw, PS: ps, PR: pr}, &NodeHistoryDelta{}, node},
		{"RelHistoryDelta_min", RelHistoryDelta{Meta: rw}, &RelHistoryDelta{}, rel},
		{"RelHistoryDelta_full", RelHistoryDelta{Meta: rw, PS: ps, PR: pr}, &RelHistoryDelta{}, rel},
		{"propKeyRef_token", propKeyRef{Token: 5}, &propKeyRef{}, nil},
		{"propKeyRef_key", propKeyRef{Key: "old"}, &propKeyRef{}, nil},
		{"propKeyRef_both_zero", propKeyRef{}, &propKeyRef{}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, err := msgpack.Marshal(c.in)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if err := msgpack.Unmarshal(b, c.out); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			got := reflect.ValueOf(c.out).Elem().Interface()
			if !reflect.DeepEqual(got, c.in) {
				t.Fatalf("round-trip mismatch:\n got=%+v\nwant=%+v", got, c.in)
			}
			if c.disk == nil {
				return
			}
			raw, decode, err := c.disk(c.in)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if len(raw) != 1+len(b) || raw[0] != historyDeltaTag {
				t.Fatalf("on-disk value % x, want historyDeltaTag + the %d-byte msgpack body", raw, len(b))
			}
			got, err = decode(raw)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !reflect.DeepEqual(got, c.in) {
				t.Fatalf("on-disk round-trip mismatch:\n got=%+v\nwant=%+v", got, c.in)
			}
		})
	}
}
