package storeutil

import (
	"reflect"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// TestChangeBodyRoundTrip proves every hand-written change-log body
// EncodeMsgpack/DecodeMsgpack pair (BACKLOG 15s) reproduces the original
// struct exactly, for both the all-omitted-optional-fields case and the
// every-optional-field-present case.
func TestChangeBodyRoundTrip(t *testing.T) {
	nw := NodeWire{FormatVersion: 2, ID: 100, PrimaryLabel: 1, Version: 1}
	rw := RelWire{FormatVersion: 2, ID: 200, RelType: 2, StartID: 10, EndID: 20, Version: 1}

	cases := []struct {
		name string
		in   any
		out  any
	}{
		{"NodePutBody_min", NodePutBody{Wire: nw}, &NodePutBody{}},
		{"NodePutBody_wh", NodePutBody{Wire: nw, WithHistory: true}, &NodePutBody{}},
		{"RelPutBody_min", RelPutBody{Wire: rw}, &RelPutBody{}},
		{"RelPutBody_wh", RelPutBody{Wire: rw, WithHistory: true}, &RelPutBody{}},
		{"NodeDeleteBody_min", NodeDeleteBody{ID: 5}, &NodeDeleteBody{}},
		{"NodeDeleteBody_full", NodeDeleteBody{ID: 5, WithHistory: true, Tombstone: &nw, RelTombstones: []RelWire{rw}, CascadedRelIDs: []int64{200, 201}}, &NodeDeleteBody{}},
		{"RelDeleteBody_min", RelDeleteBody{ID: 7}, &RelDeleteBody{}},
		{"RelDeleteBody_full", RelDeleteBody{ID: 7, WithHistory: true, Tombstone: &rw}, &RelDeleteBody{}},
		{"ForeignIncomingDeleteBody", ForeignIncomingDeleteBody{RelID: 9, EndID: 11}, &ForeignIncomingDeleteBody{}},
		{"RangePurgeBody_min", RangePurgeBody{LabelToken: 3, Before: 1000}, &RangePurgeBody{}},
		{"RangePurgeBody_mode", RangePurgeBody{LabelToken: 3, Before: 1000, Mode: 1}, &RangePurgeBody{}},
		{"HistoryVersionNodeBody", HistoryVersionNodeBody{Version: 4, Wire: nw}, &HistoryVersionNodeBody{}},
		{"HistoryVersionRelBody", HistoryVersionRelBody{Version: 4, Wire: rw}, &HistoryVersionRelBody{}},
		{"HistoryTruncateBody_min", HistoryTruncateBody{ID: 13, Bound: 3}, &HistoryTruncateBody{}},
		{"HistoryTruncateBody_trim", HistoryTruncateBody{ID: 13, IsTrim: true, Bound: 3}, &HistoryTruncateBody{}},
		{"MetaBody_min", MetaBody{Key: "k"}, &MetaBody{}},
		{"MetaBody_val", MetaBody{Key: "k", Value: []byte("v")}, &MetaBody{}},
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

// TestChangeBodyDecodeUnknownKeyForwardCompat proves an unknown map key is
// skipped rather than erroring, matching reflection's ignore-unknown-struct-
// fields behavior (forward compatibility for a future field addition).
func TestChangeBodyDecodeUnknownKeyForwardCompat(t *testing.T) {
	// Build a MetaBody payload with an extra unknown "zz" key manually via a
	// generic map, then decode it as MetaBody — the unknown key must be
	// skipped, not error.
	raw, err := msgpack.Marshal(map[string]any{"k": "hello", "v": []byte("world"), "zz": 12345})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var b MetaBody
	if err := msgpack.Unmarshal(raw, &b); err != nil {
		t.Fatalf("Unmarshal with unknown key: %v", err)
	}
	if b.Key != "hello" || string(b.Value) != "world" {
		t.Fatalf("decoded = %+v, want Key=hello Value=world", b)
	}
}
