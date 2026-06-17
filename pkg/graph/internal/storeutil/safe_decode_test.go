package storeutil

import (
	"bytes"
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

// decoderPanicInputs are msgpack byte sequences that make vmihailenco/msgpack
// v5.4.1 PANIC via reflect (rather than return an error) when decoded into a
// NodeWire — a map that repeats a key bound to the interface-typed
// PropertyWire.Value field, so the second decode targets an unaddressable
// reflect.Value. Found by FuzzWireToNodeChecked and independently by the
// wire-decode audit. SafeUnmarshal must convert these to ErrCorruptWire.
var decoderPanicInputs = [][]byte{
	[]byte("\xde00\xa1p\xdc00\x83\xa1v\xa10\xa1v\xa10"),                        // SetString panic
	[]byte("\x84\xa200\xd300000000\xa2000\xa1p\x92\x83\xa10\xa10\xa1v0\xa1v0"), // SetInt panic
}

func TestSafeUnmarshalRecoversDecoderPanic(t *testing.T) {
	t.Parallel()
	for i, data := range decoderPanicInputs {
		// Sanity: confirm raw msgpack.Unmarshal really does panic on this input
		// (recovered here only to keep the test process alive). If a future
		// msgpack upgrade fixes the panic this still passes — the point is that
		// SafeUnmarshal must not panic regardless.
		func() {
			defer func() { _ = recover() }()
			var w NodeWire
			_ = msgpack.Unmarshal(data, &w)
		}()

		var w NodeWire
		err := SafeUnmarshal(data, &w) // must NOT panic
		if err == nil {
			t.Fatalf("input %d: SafeUnmarshal accepted panic-inducing bytes, want error", i)
		}
		if !errors.Is(err, storepkg.ErrCorruptWire) {
			t.Fatalf("input %d: SafeUnmarshal err = %v, want errors.Is ErrCorruptWire", i, err)
		}
	}
}

func TestUnmarshalWithKeysRecoversDecoderPanic(t *testing.T) {
	t.Parallel()
	reg := registrypkg.NewPropertyKeyRegistry()
	for i, data := range decoderPanicInputs {
		if _, err := UnmarshalNodeWireWithKeys(data, reg); err == nil {
			t.Fatalf("node input %d: production decode accepted panic bytes, want error", i)
		} else if !errors.Is(err, storepkg.ErrCorruptWire) {
			t.Fatalf("node input %d: err = %v, want ErrCorruptWire", i, err)
		}
		if _, err := UnmarshalRelWireWithKeys(data, reg); err == nil {
			t.Fatalf("rel input %d: production decode accepted panic bytes, want error", i)
		} else if !errors.Is(err, storepkg.ErrCorruptWire) {
			t.Fatalf("rel input %d: err = %v, want ErrCorruptWire", i, err)
		}
	}
}

// deepNestedWire builds a NodeWire-shaped msgpack blob whose single property's
// `v` (interface{}) field is a chain of `depth` nested arrays. Decoding this
// into the interface field recurses once per level; at large depth the
// vmihailenco decoder aborts the process with a FATAL stack overflow that
// recover() cannot catch — so SafeUnmarshal must reject it via the
// non-recursive depth guard BEFORE handing it to the decoder.
func deepNestedWire(depth int) []byte {
	// fixmap{ "id":1, "pl":1, "v":1, "p": [ fixmap{ "k":"x", "t":0, "v": <deep> } ] }
	buf := []byte{
		0x84, // fixmap, 4 entries
		0xa2, 'i', 'd', 0x01,
		0xa2, 'p', 'l', 0x01,
		0xa1, 'v', 0x01,
		0xa1, 'p', // "p" ->
		0x91, // array, 1 element
		0x83, // fixmap, 3 entries (a PropertyWire)
		0xa1, 'k', 0xa1, 'x',
		0xa1, 't', 0x00,
		0xa1, 'v', // "v" ->
	}
	for i := 0; i < depth; i++ {
		buf = append(buf, 0x91) // nested array, 1 element
	}
	buf = append(buf, 0x90) // innermost: empty array
	return buf
}

func TestSafeUnmarshalRejectsDeepNesting(t *testing.T) {
	t.Parallel()
	// A depth this large would fatal-overflow the goroutine stack if it reached
	// the recursive decoder. The guard must stop it first — fast, no crash.
	data := deepNestedWire(200000)

	var w NodeWire
	err := SafeUnmarshal(data, &w)
	if err == nil {
		t.Fatal("SafeUnmarshal accepted a 200000-deep blob, want ErrCorruptWire")
	}
	if !errors.Is(err, storepkg.ErrCorruptWire) {
		t.Fatalf("SafeUnmarshal err = %v, want errors.Is ErrCorruptWire", err)
	}

	// Production path must reject it too, not crash.
	reg := registrypkg.NewPropertyKeyRegistry()
	if _, err := UnmarshalNodeWireWithKeys(data, reg); !errors.Is(err, storepkg.ErrCorruptWire) {
		t.Fatalf("UnmarshalNodeWireWithKeys err = %v, want ErrCorruptWire", err)
	}

	// And a plain deeply-nested top-level array (no envelope) is rejected too.
	if err := SafeUnmarshal(bytes.Repeat([]byte{0x91}, 100000), new(NodeWire)); !errors.Is(err, storepkg.ErrCorruptWire) {
		t.Fatalf("deep top-level array err = %v, want ErrCorruptWire", err)
	}
}

// TestGuardMsgpackDepthAcceptsLegitimateWires pins that the depth cap is not so
// low it rejects real data: every realistic seed entity, plus a property value
// nested right up against the property allowlist's 32-level limit, must pass
// the guard. (Mutation check: lowering maxWireDecodeDepth below ~36 fails this.)
func TestGuardMsgpackDepthAcceptsLegitimateWires(t *testing.T) {
	t.Parallel()
	for i, n := range seedNodes(t) {
		if err := guardMsgpackDepth(mustMarshal(t, NodeToWire(n))); err != nil {
			t.Fatalf("seed node %d rejected by depth guard: %v", i, err)
		}
	}
	for i, r := range seedRels(t) {
		if err := guardMsgpackDepth(mustMarshal(t, RelToWire(r))); err != nil {
			t.Fatalf("seed rel %d rejected by depth guard: %v", i, err)
		}
	}

	// Property value nested to depth 30 — under the 32-level allowlist limit,
	// so it is legitimate persisted data and must round-trip cleanly.
	var nested any = "leaf"
	for i := 0; i < 30; i++ {
		nested = []any{nested}
	}
	n := types.NewNode(types.NodeID(snowflake.ID(99)), 1, nil)
	if err := n.SetProperty("deep", nested); err != nil {
		t.Fatalf("set deep property: %v", err)
	}
	data, err := MarshalNodeWire(n)
	if err != nil {
		t.Fatalf("marshal deep-but-legal node: %v", err)
	}
	if err := guardMsgpackDepth(data); err != nil {
		t.Fatalf("legitimate depth-30 property rejected by guard: %v", err)
	}
	reg := registrypkg.NewPropertyKeyRegistry()
	w, err := UnmarshalNodeWireWithKeys(data, reg)
	if err != nil {
		t.Fatalf("decode deep-but-legal node: %v", err)
	}
	if _, err := WireToNodeChecked(w); err != nil {
		t.Fatalf("checked decode of deep-but-legal node: %v", err)
	}
}
