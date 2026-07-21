package storeutil

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The payload crown property: for a CREATE (WithHistory false, omitempty —
// the nested wire is the LAST content of the body), patching the pre-encoded
// payload's terminal temporal tail with the stamped TxFrom yields EXACTLY the
// bytes NodePutPayload builds from the finalized node. This is what lets the
// ingest producer thread pre-encode the change-log body and the applier patch
// it — with zero risk of a diverged feed record.
func TestPreEncodeNodePutPayloadV2CrownEquivalence(t *testing.T) {
	mkNode := func(i int, props map[string]any) *types.Node {
		n := types.NewNode(types.NodeID(1000+i), uint16(1+i%3), []uint16{uint16(10 + i%2)})
		ps, err := types.NewOwnedPropertySlice(props)
		if err != nil {
			t.Fatalf("props: %v", err)
		}
		if err := n.SetOwnedProperties(ps); err != nil {
			t.Fatalf("SetOwnedProperties: %v", err)
		}
		n.SetIntegrity(&types.NodeIntegrity{Hash: fmt.Sprintf("h%d", i)})
		// Domain valid-time claims present in SOME cases (part of the tail's
		// sibling fields — must survive both paths identically).
		tm := &types.TemporalMetadata{}
		if i%2 == 0 {
			tm.ValidFrom = types.Instant(1000 + i)
			tm.ValidTo = types.Instant(9000 + i)
		}
		if i%3 == 0 {
			tm.CreatedAt = types.Instant(500 + i)
		}
		n.SetTemporal(tm)
		return n
	}

	propCases := []map[string]any{
		{},
		{"k": int64(7)},
		{"s": "уникод-строка", "n": 1.5, "b": true},
		{"big": string(make([]byte, 4096)), "sl": []int64{1, 2, 3}},
		{"nan": math.NaN(), "inf": math.Inf(-1)},
		{"m": map[string]any{"nested": "v"}, "f32": []float32{1, 2}},
	}
	stamps := []types.Instant{1, 42, types.Instant(1<<40 + 7), types.Instant(math.MaxInt64)}

	for i := 0; i < 60; i++ {
		n := mkNode(i, propCases[i%len(propCases)])
		pre, err := PreEncodeNodePutPayloadV2(n)
		if err != nil {
			t.Fatalf("case %d: PreEncode: %v", i, err)
		}
		for _, txFrom := range stamps {
			patched := make([]byte, len(pre))
			copy(patched, pre)
			if err := PatchWireTemporalTail(patched, int64(txFrom), 0); err != nil {
				t.Fatalf("case %d stamp %d: Patch: %v", i, txFrom, err)
			}

			// The reference: finalize the node with the stamp and build the
			// payload the door way.
			tm := n.Temporal()
			savedFrom, savedTo := tm.TxFrom, tm.TxTo
			tm.TxFrom = txFrom
			tm.TxTo = 0
			n.SetTemporal(tm)
			want, err := NodePutPayload(n, false)
			if err != nil {
				t.Fatalf("case %d stamp %d: NodePutPayload: %v", i, txFrom, err)
			}
			tm.TxFrom, tm.TxTo = savedFrom, savedTo
			n.SetTemporal(tm)

			if !bytes.Equal(patched, want) {
				t.Fatalf("case %d stamp %d: patched payload diverges from door-built payload\n got %x\nwant %x", i, txFrom, patched, want)
			}
			// And the patched payload decodes to the stamped state.
			var body NodePutBody
			if err := SafeUnmarshal(patched, &body); err != nil {
				t.Fatalf("case %d stamp %d: decode: %v", i, txFrom, err)
			}
			if body.Wire.TxFrom != int64(txFrom) || body.WithHistory {
				t.Fatalf("case %d stamp %d: decoded TxFrom=%d WithHistory=%v", i, txFrom, body.Wire.TxFrom, body.WithHistory)
			}
		}
	}

	// Fail-closed: a truncated pre-encoded payload must never be patchable.
	n := mkNode(99, map[string]any{"k": int64(1)})
	pre, err := PreEncodeNodePutPayloadV2(n)
	if err != nil {
		t.Fatalf("PreEncode: %v", err)
	}
	for cut := 1; cut <= 24 && cut < len(pre); cut++ {
		trunc := pre[:len(pre)-cut]
		if err := PatchWireTemporalTail(trunc, 5, 0); err == nil {
			t.Fatalf("truncated payload (cut %d) accepted a patch", cut)
		}
	}
}

// TestPreEncodeRelPutPayloadV2CrownEquivalence mirrors
// TestPreEncodeNodePutPayloadV2CrownEquivalence for the relationship side
// (BACKLOG 21f/15p): Patch(PreEncodeRelPutPayloadV2(R,0),T) ==
// RelPutPayload(R,T) byte-for-byte.
func TestPreEncodeRelPutPayloadV2CrownEquivalence(t *testing.T) {
	mkRel := func(i int, props map[string]any) *types.Relationship {
		start := types.NodeID(2000 + i)
		end := types.NodeID(3000 + i)
		r := types.NewRelationship(types.RelID(1000+i), uint16(1+i%3), start, end)
		ps, err := types.NewOwnedPropertySlice(props)
		if err != nil {
			t.Fatalf("props: %v", err)
		}
		if err := r.SetOwnedProperties(ps); err != nil {
			t.Fatalf("SetOwnedProperties: %v", err)
		}
		r.SetIntegrity(&types.RelIntegrity{Hash: fmt.Sprintf("h%d", i), FromNodeHash: fmt.Sprintf("fh%d", i), ToNodeHash: fmt.Sprintf("th%d", i)})
		tm := &types.TemporalMetadata{}
		if i%2 == 0 {
			tm.ValidFrom = types.Instant(1000 + i)
			tm.ValidTo = types.Instant(9000 + i)
		}
		if i%3 == 0 {
			tm.CreatedAt = types.Instant(500 + i)
		}
		r.SetTemporal(tm)
		return r
	}

	propCases := []map[string]any{
		{},
		{"k": int64(7)},
		{"s": "уникод-строка", "n": 1.5, "b": true},
		{"big": string(make([]byte, 4096)), "sl": []int64{1, 2, 3}},
		{"nan": math.NaN(), "inf": math.Inf(-1)},
		{"m": map[string]any{"nested": "v"}, "f32": []float32{1, 2}},
	}
	stamps := []types.Instant{1, 42, types.Instant(1<<40 + 7), types.Instant(math.MaxInt64)}

	for i := 0; i < 60; i++ {
		r := mkRel(i, propCases[i%len(propCases)])
		pre, err := PreEncodeRelPutPayloadV2(r)
		if err != nil {
			t.Fatalf("case %d: PreEncode: %v", i, err)
		}
		for _, txFrom := range stamps {
			patched := make([]byte, len(pre))
			copy(patched, pre)
			if err := PatchWireTemporalTail(patched, int64(txFrom), 0); err != nil {
				t.Fatalf("case %d stamp %d: Patch: %v", i, txFrom, err)
			}

			tm := r.Temporal()
			savedFrom, savedTo := tm.TxFrom, tm.TxTo
			tm.TxFrom = txFrom
			tm.TxTo = 0
			r.SetTemporal(tm)
			want, err := RelPutPayload(r, false)
			if err != nil {
				t.Fatalf("case %d stamp %d: RelPutPayload: %v", i, txFrom, err)
			}
			tm.TxFrom, tm.TxTo = savedFrom, savedTo
			r.SetTemporal(tm)

			if !bytes.Equal(patched, want) {
				t.Fatalf("case %d stamp %d: patched payload diverges from door-built payload\n got %x\nwant %x", i, txFrom, patched, want)
			}
			var body RelPutBody
			if err := SafeUnmarshal(patched, &body); err != nil {
				t.Fatalf("case %d stamp %d: decode: %v", i, txFrom, err)
			}
			if body.Wire.TxFrom != int64(txFrom) || body.WithHistory {
				t.Fatalf("case %d stamp %d: decoded TxFrom=%d WithHistory=%v", i, txFrom, body.Wire.TxFrom, body.WithHistory)
			}
		}
	}

	// Fail-closed: a truncated pre-encoded payload must never be patchable.
	r := mkRel(99, map[string]any{"k": int64(1)})
	pre, err := PreEncodeRelPutPayloadV2(r)
	if err != nil {
		t.Fatalf("PreEncode: %v", err)
	}
	for cut := 1; cut <= 24 && cut < len(pre); cut++ {
		trunc := pre[:len(pre)-cut]
		if err := PatchWireTemporalTail(trunc, 5, 0); err == nil {
			t.Fatalf("truncated payload (cut %d) accepted a patch", cut)
		}
	}
}
