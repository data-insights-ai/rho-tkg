package sharded

import (
	"reflect"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/vmihailenco/msgpack/v5"
)

// TestStoreWireRoundTrip proves the hand-written vectorDefBlob/slotCatalog
// EncodeMsgpack/DecodeMsgpack pairs (BACKLOG 15v) reproduce the original
// value exactly. The non-empty-map slotCatalog case is verified here via
// round-trip (map content via reflect.DeepEqual, which is order-independent)
// rather than a byte-golden comparison — see store_wire_golden_test.go's doc
// comment for why a fixed hex golden isn't meaningful for that case.
func TestStoreWireRoundTrip(t *testing.T) {
	t.Run("vectorDefBlob", func(t *testing.T) {
		in := vectorDefBlob{LabelToken: 3, PropertyKey: "vec", Dims: 128, Metric: storecontract.DistanceCosine}
		b, err := msgpack.Marshal(in)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		var got vectorDefBlob
		if err := msgpack.Unmarshal(b, &got); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if got != in {
			t.Fatalf("round-trip mismatch: got=%+v want=%+v", got, in)
		}
	})

	t.Run("vectorDefBlob_slice", func(t *testing.T) {
		in := []vectorDefBlob{
			{LabelToken: 1, PropertyKey: "a", Dims: 2, Metric: storecontract.DistanceCosine},
			{LabelToken: 2, PropertyKey: "b", Dims: 4, Metric: storecontract.DistanceEuclidean},
		}
		b, err := msgpack.Marshal(in)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		var got []vectorDefBlob
		if err := msgpack.Unmarshal(b, &got); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if !reflect.DeepEqual(got, in) {
			t.Fatalf("round-trip mismatch: got=%+v want=%+v", got, in)
		}
	})

	t.Run("slotCatalog_nilmap", func(t *testing.T) {
		in := &slotCatalog{FormatVersion: 1, BaseSlot: 0, SlotCount: 0, Discipline: disciplineUnified}
		b, err := msgpack.Marshal(in)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		var got slotCatalog
		if err := msgpack.Unmarshal(b, &got); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if !reflect.DeepEqual(&got, in) {
			t.Fatalf("round-trip mismatch: got=%+v want=%+v", got, *in)
		}
	})

	t.Run("slotCatalog_populated_map", func(t *testing.T) {
		// A larger map (>10 entries) makes a would-be non-deterministic
		// encoding order overwhelmingly likely to surface across repeated runs
		// if map-iteration order ever leaked into correctness.
		in := newIdentityCatalog(0, 20)
		b, err := msgpack.Marshal(in)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		var got slotCatalog
		if err := msgpack.Unmarshal(b, &got); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if !reflect.DeepEqual(&got, in) {
			t.Fatalf("round-trip mismatch: got=%+v want=%+v", got, *in)
		}
	})

	t.Run("slotCatalog_empty_nonnil_map", func(t *testing.T) {
		// newIdentityCatalog(base, 0) allocates a non-nil, empty map (make with
		// cap 0) — distinct from the nil-map zero-value case, and must encode
		// as an empty map (0x80), not msgpack nil, exactly as reflection would
		// for a non-nil empty map.
		in := newIdentityCatalog(0, 0)
		if in.SlotShard == nil {
			t.Fatal("test invariant broken: newIdentityCatalog(0,0).SlotShard must be non-nil (allocated via make)")
		}
		b, err := msgpack.Marshal(in)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		var got slotCatalog
		if err := msgpack.Unmarshal(b, &got); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if !reflect.DeepEqual(&got, in) {
			t.Fatalf("round-trip mismatch: got=%+v want=%+v", got, *in)
		}
	})
}

// TestStoreWireEncodeByValue locks in a real regression BACKLOG 15v's
// implementation hit elsewhere (tiered.RegistryFileData): msgpack.Marshal(v)
// on a NON-ADDRESSABLE value (a plain struct literal boxed into an `any`)
// requires EncodeMsgpack to have a VALUE receiver — a pointer-receiver-only
// EncodeMsgpack fails with "Encode(non-addressable ...)" for this call
// shape. Every EncodeMsgpack in this file must use a value receiver
// (DecodeMsgpack correctly stays pointer-receiver, since decode always
// targets an addressable &out).
func TestStoreWireEncodeByValue(t *testing.T) {
	if _, err := msgpack.Marshal(vectorDefBlob{LabelToken: 1, PropertyKey: "a", Dims: 2}); err != nil {
		t.Fatalf("msgpack.Marshal(vectorDefBlob{...}) (by value) = %v, want nil (EncodeMsgpack must use a value receiver)", err)
	}
	if _, err := msgpack.Marshal(slotCatalog{FormatVersion: 1, BaseSlot: 0, SlotCount: 1, Discipline: disciplineUnified}); err != nil {
		t.Fatalf("msgpack.Marshal(slotCatalog{...}) (by value) = %v, want nil (EncodeMsgpack must use a value receiver)", err)
	}
}
