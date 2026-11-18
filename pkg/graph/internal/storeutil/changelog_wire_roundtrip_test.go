package storeutil

import (
	"reflect"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// TestChangeBodyRoundTrip proves every change-log body round-trips exactly
// through both doors: the hand-written EncodeMsgpack/DecodeMsgpack pair via
// msgpack.Marshal/Unmarshal, and the replica-apply door MarshalChangeBody plus
// its typed Decode* function (SafeUnmarshal), for the all-omitted-optional-
// fields case and the every-optional-field-present case.
func TestChangeBodyRoundTrip(t *testing.T) {
	t.Parallel()
	nw := NodeWire{FormatVersion: CurrentWireFormatVersion, ID: 100, PrimaryLabel: 1, Version: 1}
	rw := RelWire{FormatVersion: CurrentWireFormatVersion, ID: 200, RelType: 2, StartID: 10, EndID: 20, Version: 1}

	nodePut := func(p []byte) (any, error) { return DecodeNodePut(p) }
	relPut := func(p []byte) (any, error) { return DecodeRelPut(p) }
	nodeDelete := func(p []byte) (any, error) { return DecodeNodeDelete(p) }
	relDelete := func(p []byte) (any, error) { return DecodeRelDelete(p) }
	foreign := func(p []byte) (any, error) { return DecodeForeignIncomingDelete(p) }
	rangePurge := func(p []byte) (any, error) { return DecodeRangePurge(p) }
	relRangePurge := func(p []byte) (any, error) { return DecodeRelRangePurge(p) }
	histNode := func(p []byte) (any, error) { return DecodeHistoryVersionNode(p) }
	histRel := func(p []byte) (any, error) { return DecodeHistoryVersionRel(p) }
	truncate := func(p []byte) (any, error) { return DecodeHistoryTruncate(p) }
	meta := func(p []byte) (any, error) { return DecodeMeta(p) }

	cases := []struct {
		name   string
		in     any
		out    any // pointer to a zero body for msgpack.Unmarshal
		decode func([]byte) (any, error)
	}{
		{"NodePutBody_min", NodePutBody{Wire: nw}, &NodePutBody{}, nodePut},
		{"NodePutBody_wh", NodePutBody{Wire: nw, WithHistory: true}, &NodePutBody{}, nodePut},
		{"RelPutBody_min", RelPutBody{Wire: rw}, &RelPutBody{}, relPut},
		{"RelPutBody_wh", RelPutBody{Wire: rw, WithHistory: true}, &RelPutBody{}, relPut},
		{"NodeDeleteBody_min", NodeDeleteBody{ID: 5}, &NodeDeleteBody{}, nodeDelete},
		{"NodeDeleteBody_cascade", NodeDeleteBody{ID: 5, CascadedRelIDs: []int64{200, 201}}, &NodeDeleteBody{}, nodeDelete},
		{"NodeDeleteBody_full", NodeDeleteBody{ID: 5, WithHistory: true, Tombstone: &nw, RelTombstones: []RelWire{rw}, CascadedRelIDs: []int64{200, 201}}, &NodeDeleteBody{}, nodeDelete},
		{"RelDeleteBody_min", RelDeleteBody{ID: 7}, &RelDeleteBody{}, relDelete},
		{"RelDeleteBody_full", RelDeleteBody{ID: 7, WithHistory: true, Tombstone: &rw}, &RelDeleteBody{}, relDelete},
		{"ForeignIncomingDeleteBody", ForeignIncomingDeleteBody{RelID: 9, EndID: 11}, &ForeignIncomingDeleteBody{}, foreign},
		{"RangePurgeBody_min", RangePurgeBody{LabelToken: 3, Before: 1000}, &RangePurgeBody{}, rangePurge},
		{"RangePurgeBody_mode", RangePurgeBody{LabelToken: 3, Before: 1000, Mode: 1}, &RangePurgeBody{}, rangePurge},
		{"RelRangePurgeBody_min", RelRangePurgeBody{TypeToken: 3, Before: 1000}, &RelRangePurgeBody{}, relRangePurge},
		{"RelRangePurgeBody_mode", RelRangePurgeBody{TypeToken: 3, Before: 1000, Mode: 1}, &RelRangePurgeBody{}, relRangePurge},
		{"HistoryVersionNodeBody", HistoryVersionNodeBody{Version: 4, Wire: nw}, &HistoryVersionNodeBody{}, histNode},
		{"HistoryVersionRelBody", HistoryVersionRelBody{Version: 4, Wire: rw}, &HistoryVersionRelBody{}, histRel},
		{"HistoryTruncateBody_min", HistoryTruncateBody{ID: 13, Bound: 3}, &HistoryTruncateBody{}, truncate},
		{"HistoryTruncateBody_trim", HistoryTruncateBody{ID: 13, IsTrim: true, Bound: 3}, &HistoryTruncateBody{}, truncate},
		{"MetaBody_min", MetaBody{Key: "k"}, &MetaBody{}, meta},
		{"MetaBody_val", MetaBody{Key: "k", Value: []byte("v")}, &MetaBody{}, meta},
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
			if got := reflect.ValueOf(c.out).Elem().Interface(); !reflect.DeepEqual(got, c.in) {
				t.Fatalf("msgpack round-trip mismatch:\n got=%+v\nwant=%+v", got, c.in)
			}

			p, err := MarshalChangeBody(c.in)
			if err != nil {
				t.Fatalf("MarshalChangeBody: %v", err)
			}
			got, err := c.decode(p)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !reflect.DeepEqual(got, c.in) {
				t.Fatalf("Decode round-trip mismatch:\n got=%+v\nwant=%+v", got, c.in)
			}
		})
	}

	// A label-purge body is not a relationship-purge body: its "l" key is
	// skipped, so the type token stays 0 (no node token leaks in).
	t.Run("RangePurgeBody_as_RelRangePurge", func(t *testing.T) {
		lp, err := MarshalChangeBody(RangePurgeBody{LabelToken: 7, Before: 123456})
		if err != nil {
			t.Fatal(err)
		}
		if got, err := DecodeRelRangePurge(lp); err != nil || got != (RelRangePurgeBody{Before: 123456}) {
			t.Fatalf("label body decoded as %+v, %v; want {Before:123456}", got, err)
		}
	})
}

// TestChangeBodyDecodeUnknownKeyForwardCompat proves an unknown map key is
// skipped rather than erroring, matching reflection's ignore-unknown-struct-
// fields behavior (forward compatibility for a future field addition).
func TestChangeBodyDecodeUnknownKeyForwardCompat(t *testing.T) {
	t.Parallel()
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
