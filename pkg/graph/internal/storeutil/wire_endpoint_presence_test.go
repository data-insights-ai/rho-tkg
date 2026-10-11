package storeutil

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	store "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

func endpointShapes() []types.TemporalRange {
	return []types.TemporalRange{
		{From: types.TemporalEndpoint{Finite: true}, To: types.TemporalEndpoint{Value: 10, Finite: true}},
		{From: types.TemporalEndpoint{Value: -10, Finite: true}, To: types.TemporalEndpoint{Finite: true}},
		{From: types.TemporalEndpoint{Finite: true}},
		{To: types.TemporalEndpoint{Finite: true}},
		{},
		{From: types.TemporalEndpoint{Value: -100, Finite: true}, To: types.TemporalEndpoint{Value: -10, Finite: true}},
	}
}
func endpointWires(t *testing.T, r types.TemporalRange) (NodeWire, RelWire) {
	t.Helper()
	tm := &types.TemporalMetadata{TxFrom: 20, TxTo: 30}
	if err := tm.SetValidBounds(r); err != nil {
		t.Fatal(err)
	}
	n := types.NewNode(1001, 1, nil)
	n.SetTemporal(tm)
	n.SetVersion(3)
	if err := n.SetProperty("number", int64(7)); err != nil {
		t.Fatal(err)
	}
	rel := types.NewRelationship(1002, 1, 1001, 1003)
	rel.SetTemporal(tm)
	rel.SetVersion(3)
	if err := rel.SetProperty("number", int64(7)); err != nil {
		t.Fatal(err)
	}
	nw, err := NodeToWireChecked(n)
	if err != nil {
		t.Fatal(err)
	}
	rw, err := RelToWireChecked(rel)
	if err != nil {
		t.Fatal(err)
	}
	return nw, rw
}
func assertEndpoints(t *testing.T, tm *types.TemporalMetadata, want types.TemporalRange) {
	t.Helper()
	got, explicit := tm.ValidBounds()
	if !explicit || got != want {
		t.Fatalf("endpoint meaning lost: got%+v explicit%v want%+v metadata%+v", got, explicit, want, tm)
	}
}
func TestEndpointPresenceFullPartialDeltaAndTail(t *testing.T) {
	for _, shape := range endpointShapes() {
		nw, rw := endpointWires(t, shape)
		for _, node := range []bool{true, false} {
			var raw []byte
			var err error
			if node {
				raw, err = msgpack.Marshal(nw)
			} else {
				raw, err = msgpack.Marshal(rw)
			}
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err := msgpack.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			ep, present := wireMapInt(t, fields, "ep")
			if !present || ep != int64(nw.ValidEndpoints) {
				t.Fatal("custom encoder omitted endpoint presence", fields)
			}
			_, tm, err := DecodeWireTemporalMeta(raw)
			if err != nil {
				t.Fatal(err)
			}
			assertEndpoints(t, tm, shape)
			scanned, ok := scanWireTemporalMeta(raw)
			if !ok || scanned.ValidEndpoints != nw.ValidEndpoints {
				t.Fatal("scanner lost flags", scanned, ok)
			}
			slow, err := decodeWireTemporalMetaSlow(raw)
			if err != nil || slow.ValidEndpoints != scanned.ValidEndpoints {
				t.Fatal("partial slow differs", slow, err)
			}
			if node {
				var w NodeWire
				if err := SafeUnmarshal(raw, &w); err != nil {
					t.Fatal(err)
				}
				n, err := WireToNodeChecked(w)
				if err != nil {
					t.Fatal(err)
				}
				assertEndpoints(t, n.Temporal(), shape)
				anchor := nw
				anchor.FormatVersion = 2
				anchor.ValidEndpoints = 0
				anchor.ValidFrom = -100
				anchor.ValidTo = 100
				delta := DiffNodeHistory(anchor, nw)
				raw, err = EncodeNodeHistoryDelta(delta)
				if err != nil {
					t.Fatal(err)
				}
				d, err := DecodeNodeHistoryDelta(raw)
				if err != nil {
					t.Fatal(err)
				}
				n, err = WireToNodeChecked(ApplyNodeHistory(anchor, d))
				if err != nil {
					t.Fatal(err)
				}
				assertEndpoints(t, n.Temporal(), shape)
				assertEndpoints(t, SelectionTemporalMetaOfNodeWire(d.Meta), shape)
				prepared, err := PreEncodeNodeWireV2(nw)
				if err != nil {
					t.Fatal(err)
				}
				if err := PatchWireTemporalTail(prepared, 20, 30); err != nil {
					t.Fatal(err)
				}
				expected, err := msgpack.Marshal(nw)
				if err != nil || !bytes.Equal(prepared, expected) {
					t.Fatal("finite endpoint preencode patch differs", err)
				}
			} else {
				var w RelWire
				if err := SafeUnmarshal(raw, &w); err != nil {
					t.Fatal(err)
				}
				r, err := WireToRelChecked(w)
				if err != nil {
					t.Fatal(err)
				}
				assertEndpoints(t, r.Temporal(), shape)
				anchor := rw
				anchor.FormatVersion = 2
				anchor.ValidEndpoints = 0
				anchor.ValidFrom = -100
				anchor.ValidTo = 100
				delta := DiffRelHistory(anchor, rw)
				raw, err = EncodeRelHistoryDelta(delta)
				if err != nil {
					t.Fatal(err)
				}
				d, err := DecodeRelHistoryDelta(raw)
				if err != nil {
					t.Fatal(err)
				}
				r, err = WireToRelChecked(ApplyRelHistory(anchor, d))
				if err != nil {
					t.Fatal(err)
				}
				assertEndpoints(t, r.Temporal(), shape)
				assertEndpoints(t, SelectionTemporalMetaOfRelWire(d.Meta), shape)
				prepared, err := PreEncodeRelWireV2(rw)
				if err != nil {
					t.Fatal(err)
				}
				if err := PatchWireTemporalTail(prepared, 20, 30); err != nil {
					t.Fatal(err)
				}
				expected, err := msgpack.Marshal(rw)
				if err != nil || !bytes.Equal(prepared, expected) {
					t.Fatal("finite endpoint preencode patch differs", err)
				}
			}
			from, to, _, err := HistoryValueTxStamps(raw, node)
			if err != nil || from != 20 || to != 30 {
				t.Fatal("delta header wrong", from, to, err)
			}
		}
	}
}
func TestEndpointPresenceMalformedRowsFailAllSelectionPaths(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fv, ep uint8
		ht     bool
		vf, vt int64
	}{
		{"unknown bits", 4, 129, true, 0, 0}, {"no explicit bit", 4, 2, true, 0, 0}, {"noncanonical infinity", 4, 1, true, 1, 0}, {"inverted finitezero", 4, 7, true, 0, -1}, {"old schema", 3, 7, true, 0, 1}, {"no temporal block", 4, 7, false, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nw := NodeWire{FormatVersion: tc.fv, ID: 1001, PrimaryLabel: 1, HasTemporal: tc.ht, ValidEndpoints: tc.ep, ValidFrom: tc.vf, ValidTo: tc.vt}
			rw := RelWire{FormatVersion: tc.fv, ID: 1002, RelType: 1, StartID: 1001, EndID: 1003, HasTemporal: tc.ht, ValidEndpoints: tc.ep, ValidFrom: tc.vf, ValidTo: tc.vt}
			if _, err := WireToNodeChecked(nw); err == nil {
				t.Fatal("node accepted malformed flags")
			}
			if _, err := WireToRelChecked(rw); err == nil {
				t.Fatal("rel accepted malformed flags")
			}
			// Raw maps bypass the writer's fv4 upgrade so malformed old fv+ep is real.
			raw, err := msgpack.Marshal(map[string]any{"fv": tc.fv, "v": 1, "id": 1001, "pl": 1, "ht": tc.ht, "ep": tc.ep, "vf": tc.vf, "vt": tc.vt})
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := DecodeWireTemporalMeta(raw); !errors.Is(err, store.ErrCorruptWire) {
				t.Fatal("partial accepted malformed presence", err)
			}
			if _, _, _, err := DecodeWireTxStamps(raw); !errors.Is(err, store.ErrCorruptWire) {
				t.Fatal("header accepted malformed presence", err)
			}
			// Tagged delta framing must validate before its Meta becomes a selection skeleton.
			body, err := msgpack.Marshal(map[string]any{"m": map[string]any{"fv": tc.fv, "v": 1, "ht": tc.ht, "ep": tc.ep, "vf": tc.vf, "vt": tc.vt}})
			if err != nil {
				t.Fatal(err)
			}
			body = append([]byte{'D'}, body...)
			if _, err := DecodeNodeHistoryDelta(body); !errors.Is(err, store.ErrCorruptWire) {
				t.Fatal("node delta accepted malformed", err)
			}
			if _, err := DecodeRelHistoryDelta(body); !errors.Is(err, store.ErrCorruptWire) {
				t.Fatal("rel delta accepted malformed", err)
			}
		})
	}
}
func TestEndpointPresenceOldReaderFailsClosed(t *testing.T) {
	original := maxReadableWireFormatVersion
	defer func() { maxReadableWireFormatVersion = original }()
	nw, rw := endpointWires(t, endpointShapes()[1])
	nraw, _ := msgpack.Marshal(nw)
	rraw, _ := msgpack.Marshal(rw)
	ndelta, _ := EncodeNodeHistoryDelta(DiffNodeHistory(NodeWire{}, nw))
	rdelta, _ := EncodeRelHistoryDelta(DiffRelHistory(RelWire{}, rw))
	maxReadableWireFormatVersion = 3
	for _, raw := range [][]byte{nraw, rraw} {
		if _, _, err := DecodeWireTemporalMeta(raw); !errors.Is(err, store.ErrWireFormatVersionUnsupported) {
			t.Fatal("older partial reader", err)
		}
		if _, _, _, err := DecodeWireTxStamps(raw); !errors.Is(err, store.ErrWireFormatVersionUnsupported) {
			t.Fatal("older header reader", err)
		}
	}
	if _, err := WireToNodeChecked(nw); !errors.Is(err, store.ErrWireFormatVersionUnsupported) {
		t.Fatal(err)
	}
	if _, err := WireToRelChecked(rw); !errors.Is(err, store.ErrWireFormatVersionUnsupported) {
		t.Fatal(err)
	}
	if _, err := DecodeNodeHistoryDelta(ndelta); !errors.Is(err, store.ErrWireFormatVersionUnsupported) {
		t.Fatal(err)
	}
	if _, err := DecodeRelHistoryDelta(rdelta); !errors.Is(err, store.ErrWireFormatVersionUnsupported) {
		t.Fatal(err)
	}
}
func TestEndpointPresenceContentHashPolicyAndCompactFallback(t *testing.T) {
	nw, rw := endpointWires(t, endpointShapes()[1])
	n, err := WireToNodeChecked(nw)
	if err != nil {
		t.Fatal(err)
	}
	r, err := WireToRelChecked(rw)
	if err != nil {
		t.Fatal(err)
	}
	n.SetIntegrity(&types.NodeIntegrity{Hash: integrity.ComputeNodeHash(n, []string{"Fact"})})
	r.SetIntegrity(&types.RelIntegrity{Hash: integrity.ComputeRelHash(r, "FACT")})
	for _, shape := range endpointShapes() {
		nc := n.DeepCopy()
		rc := r.DeepCopy()
		if err := nc.Temporal().SetValidBounds(shape); err != nil {
			t.Fatal(err)
		}
		if err := rc.Temporal().SetValidBounds(shape); err != nil {
			t.Fatal(err)
		}
		if integrity.ComputeNodeHash(nc, []string{"Fact"}) != n.Integrity().Hash || integrity.ComputeRelHash(rc, "FACT") != r.Integrity().Hash {
			t.Fatal("existing content-only integrity contract changed")
		}
		nbytes, err := MarshalNodeWire(nc)
		if err != nil {
			t.Fatal(err)
		}
		cbytes, err := MarshalNodeWire(nc.CompactFrozenCopy())
		if err != nil || !bytes.Equal(nbytes, cbytes) {
			t.Fatal("compact fallback loses node endpoints", err)
		}
		rbytes, err := MarshalRelWire(rc)
		if err != nil {
			t.Fatal(err)
		}
		cbytes, err = MarshalRelWire(rc.CompactFrozenCopy())
		if err != nil || !bytes.Equal(rbytes, cbytes) {
			t.Fatal("compact fallback loses rel endpoints", err)
		}
		if !reflect.DeepEqual(nc.Temporal(), nc.CompactFrozenCopy().Temporal()) || !reflect.DeepEqual(rc.Temporal(), rc.CompactFrozenCopy().Temporal()) {
			t.Fatal("compact metadata differs")
		}
	}
}

func TestEndpointPresenceNarrowingCannotEraseGuard(t *testing.T) {
	for _, field := range []string{"ep", "fv"} {
		for _, value := range []any{int64(-1), uint64(256), uint64(1 << 63), uint64(^uint64(0)), nil, false, "7"} {
			m := map[string]any{"id": int64(1001), "pl": 1, "s": int64(1001), "e": int64(1003), "rt": 1, "v": 1, "ht": true, "fv": uint8(4), "ep": uint8(7), "vf": int64(-1), "vt": int64(0)}
			m[field] = value
			raw, err := msgpack.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			var nw NodeWire
			var rw RelWire
			if err := SafeUnmarshal(raw, &nw); !errors.Is(err, store.ErrCorruptWire) {
				t.Fatalf("node %s=%v silently narrowed: %+v %v", field, value, nw, err)
			}
			if err := SafeUnmarshal(raw, &rw); !errors.Is(err, store.ErrCorruptWire) {
				t.Fatalf("rel %s=%v silently narrowed: %+v %v", field, value, rw, err)
			}
			if _, _, err := DecodeWireTemporalMeta(raw); !errors.Is(err, store.ErrCorruptWire) {
				t.Fatal("partial narrowing", field, value, err)
			}
			if _, _, _, err := DecodeWireTxStamps(raw); !errors.Is(err, store.ErrCorruptWire) {
				t.Fatal("header narrowing", field, value, err)
			}
		}
	}
}

func TestEndpointPresenceNestedTemporalAndTokenizedTail(t *testing.T) {
	for _, shape := range endpointShapes() {
		want := map[string]any{"date": []any{types.TemporalValue{Kind: types.TemporalDate, Value: "1970-01-01"}, int64(0)}}
		n := types.NewNode(1001, 1, nil)
		if err := n.SetProperty("nested_bounds", want); err != nil {
			t.Fatal(err)
		}
		tm := &types.TemporalMetadata{TxFrom: 20, TxTo: 30}
		if err := tm.SetValidBounds(shape); err != nil {
			t.Fatal(err)
		}
		n.SetTemporal(tm)
		raw, err := MarshalNodeWire(n)
		if err != nil {
			t.Fatal(err)
		}
		var w NodeWire
		if err := SafeUnmarshal(raw, &w); err != nil {
			t.Fatal(err)
		}
		back, err := WireToNodeChecked(w)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := back.GetProperty("nested_bounds")
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("nested endpoint type/meaning erased: %T %v want%v", got, got, want)
		}
		registry := registrypkg.NewPropertyKeyRegistry()
		prepared, err := PreEncodeNodeWireV2WithKeys(n, registry)
		if err != nil {
			t.Fatal(err)
		}
		if err := PatchWireTemporalTail(prepared, 20, 30); err != nil {
			t.Fatal(err)
		}
		normal, err := MarshalNodeWireWithKeys(n, registry)
		if err != nil || !bytes.Equal(prepared, normal) {
			t.Fatal("tokenized endpoint preparation differs", err)
		}
		r := types.NewRelationship(1002, 1, 1001, 1003)
		r.SetTemporal(tm)
		if err := r.SetProperty("nested_bounds", want); err != nil {
			t.Fatal(err)
		}
		prepared, err = PreEncodeRelWireV2WithKeys(r, registry)
		if err != nil {
			t.Fatal(err)
		}
		if err := PatchWireTemporalTail(prepared, 20, 30); err != nil {
			t.Fatal(err)
		}
		normal, err = MarshalRelWireWithKeys(r, registry)
		if err != nil || !bytes.Equal(prepared, normal) {
			t.Fatal("tokenized rel endpoint preparation differs", err)
		}
	}
}
