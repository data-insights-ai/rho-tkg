package storeutil

import (
	"reflect"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// TestHistoryDeltaRoundTrip proves every hand-written NodeHistoryDelta/
// RelHistoryDelta/propKeyRef EncodeMsgpack/DecodeMsgpack pair (BACKLOG 15t)
// reproduces the original struct exactly, for both the all-omitted-optional-
// fields case and the every-optional-field-present case.
func TestHistoryDeltaRoundTrip(t *testing.T) {
	nw := NodeWire{FormatVersion: 2, ID: 100, PrimaryLabel: 1, Version: 2}
	rw := RelWire{FormatVersion: 2, ID: 200, RelType: 2, StartID: 10, EndID: 20, Version: 2}
	ps := []PropertyWire{{KeyToken: 3, Value: int64(9), Type: 2}}
	pr := []propKeyRef{{Token: 5}, {Key: "old"}}

	cases := []struct {
		name string
		in   any
		out  any
	}{
		{"NodeHistoryDelta_min", NodeHistoryDelta{Meta: nw}, &NodeHistoryDelta{}},
		{"NodeHistoryDelta_full", NodeHistoryDelta{Meta: nw, PS: ps, PR: pr}, &NodeHistoryDelta{}},
		{"RelHistoryDelta_min", RelHistoryDelta{Meta: rw}, &RelHistoryDelta{}},
		{"RelHistoryDelta_full", RelHistoryDelta{Meta: rw, PS: ps, PR: pr}, &RelHistoryDelta{}},
		{"propKeyRef_token", propKeyRef{Token: 5}, &propKeyRef{}},
		{"propKeyRef_key", propKeyRef{Key: "old"}, &propKeyRef{}},
		{"propKeyRef_both_zero", propKeyRef{}, &propKeyRef{}},
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
		})
	}
}

// TestEncodeDecodeNodeRelHistoryDelta_RoundTrip exercises the public
// EncodeNodeHistoryDelta/DecodeNodeHistoryDelta and Rel counterparts (the
// actual on-disk door, tag byte + msgpack body) end to end, confirming the
// hand-written codecs work through the full historyDeltaTag framing too.
func TestEncodeDecodeNodeRelHistoryDelta_RoundTrip(t *testing.T) {
	nw := NodeWire{FormatVersion: 2, ID: 100, PrimaryLabel: 1, Version: 2}
	d := NodeHistoryDelta{
		Meta: nw,
		PS:   []PropertyWire{{KeyToken: 3, Value: int64(9), Type: 2}},
		PR:   []propKeyRef{{Token: 5}, {Key: "old"}},
	}
	raw, err := EncodeNodeHistoryDelta(d)
	if err != nil {
		t.Fatalf("EncodeNodeHistoryDelta: %v", err)
	}
	got, err := DecodeNodeHistoryDelta(raw)
	if err != nil {
		t.Fatalf("DecodeNodeHistoryDelta: %v", err)
	}
	if !reflect.DeepEqual(got, d) {
		t.Fatalf("node history delta round-trip mismatch:\n got=%+v\nwant=%+v", got, d)
	}

	rw := RelWire{FormatVersion: 2, ID: 200, RelType: 2, StartID: 10, EndID: 20, Version: 2}
	rd := RelHistoryDelta{
		Meta: rw,
		PS:   []PropertyWire{{KeyToken: 1, Value: "x", Type: 3}},
		PR:   []propKeyRef{{Token: 9}},
	}
	rraw, err := EncodeRelHistoryDelta(rd)
	if err != nil {
		t.Fatalf("EncodeRelHistoryDelta: %v", err)
	}
	rgot, err := DecodeRelHistoryDelta(rraw)
	if err != nil {
		t.Fatalf("DecodeRelHistoryDelta: %v", err)
	}
	if !reflect.DeepEqual(rgot, rd) {
		t.Fatalf("rel history delta round-trip mismatch:\n got=%+v\nwant=%+v", rgot, rd)
	}
}
