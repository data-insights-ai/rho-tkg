package storeutil

// Fuzz harness for the anchor+delta history decode trust boundary
// (ADR-0009/B6, BACKLOG 15d) — the same untrusted-bytes trust class lesson 47's
// fuzzing found the original SafeUnmarshal-panic bug on, but DecodeNodeHistoryDelta/
// DecodeRelHistoryDelta had zero fuzz coverage of their own, unlike the sibling
// WireToNodeChecked/WireToRelChecked (wire_fuzz_test.go).
//
// The contract under attack:
//  1. DecodeNodeHistoryDelta/DecodeRelHistoryDelta must NEVER panic on ANY byte
//     string, tagged or not (a panic here is a store/replica crash reading a
//     single corrupt or hostile history row — the exact denial-of-service class
//     lesson 47 exists to close). They already route through SafeUnmarshal; this
//     proves that holds for the delta types specifically, not just NodeWire/RelWire.
//  2. Given a successfully decoded delta, ApplyNodeHistory/ApplyRelHistory
//     (the reconstruction step every decode ultimately feeds) must not panic
//     when merged against a realistic anchor — a fuzzed PS/PR (arbitrary
//     PropertyWire entries, arbitrary key/token identities) exercises the
//     map-merge logic in applyProperties with adversarial shapes no seed
//     corpus built from real Diff output would produce.
//  3. If the reconstructed wire happens to pass WireToNodeChecked/WireToRelChecked
//     (a properly-shaped delta merged with a valid anchor CAN decode to a live
//     entity), every entity accessor must survive touching it, mirroring
//     wire_fuzz_test.go's exerciseNode/exerciseRel.

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// fuzzHistoryAnchorNode / fuzzHistoryAnchorRel are the fixed, realistic anchor
// wires every fuzzed delta is merged against — ApplyNodeHistory/ApplyRelHistory
// always operate on (anchor, delta) pairs, never a delta alone.
func fuzzHistoryAnchorNode(tb testing.TB) NodeWire {
	tb.Helper()
	n := types.NewNode(types.NodeID(snowflake.ID(9001)), 1, []uint16{2})
	n.SetVersion(16) // an anchor version under the default interval
	n.SetProperties(fuzzMustProps(tb, map[string]any{
		"name":  "anchor",
		"count": int64(1),
		"score": float64(2.5),
	}))
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, TxFrom: 100})
	return NodeToWire(n)
}

func fuzzHistoryAnchorRel(tb testing.TB) RelWire {
	tb.Helper()
	r := types.NewRelationship(types.RelID(snowflake.ID(9002)), 3, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	r.SetVersion(16)
	r.SetProperties(fuzzMustProps(tb, map[string]any{
		"weight": float64(1.0),
		"label":  "anchor",
	}))
	r.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, TxFrom: 100})
	return RelToWire(r)
}

// seedNodeHistoryDeltas returns realistic deltas built via DiffNodeHistory
// against fuzzHistoryAnchorNode, covering: an unchanged target (empty
// PS/PR), a property added, a property changed, and a property removed.
func seedNodeHistoryDeltas(tb testing.TB, anchor NodeWire) []NodeHistoryDelta {
	tb.Helper()
	mk := func(mut func(*types.Node)) NodeHistoryDelta {
		n := types.NewNode(types.NodeID(snowflake.ID(9001)), 1, []uint16{2})
		n.SetVersion(20)
		n.SetProperties(fuzzMustProps(tb, map[string]any{
			"name":  "anchor",
			"count": int64(1),
			"score": float64(2.5),
		}))
		n.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, TxFrom: 100})
		mut(n)
		return DiffNodeHistory(anchor, NodeToWire(n))
	}
	return []NodeHistoryDelta{
		mk(func(*types.Node) {}), // unchanged
		mk(func(n *types.Node) { _ = n.SetProperty("added", "new") }),
		mk(func(n *types.Node) { _ = n.SetProperty("count", int64(99)) }),
		mk(func(n *types.Node) {
			ps := n.Properties()
			_, _ = ps.Delete("score")
			n.SetProperties(ps)
		}),
	}
}

func seedRelHistoryDeltas(tb testing.TB, anchor RelWire) []RelHistoryDelta {
	tb.Helper()
	mk := func(mut func(*types.Relationship)) RelHistoryDelta {
		r := types.NewRelationship(types.RelID(snowflake.ID(9002)), 3, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
		r.SetVersion(20)
		r.SetProperties(fuzzMustProps(tb, map[string]any{
			"weight": float64(1.0),
			"label":  "anchor",
		}))
		r.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, TxFrom: 100})
		mut(r)
		return DiffRelHistory(anchor, RelToWire(r))
	}
	return []RelHistoryDelta{
		mk(func(*types.Relationship) {}),
		mk(func(r *types.Relationship) { _ = r.SetProperty("added", "new") }),
		mk(func(r *types.Relationship) { _ = r.SetProperty("weight", float64(9.9)) }),
		mk(func(r *types.Relationship) {
			ps := r.Properties()
			_, _ = ps.Delete("label")
			r.SetProperties(ps)
		}),
	}
}

func FuzzDecodeNodeHistoryDelta(f *testing.F) {
	anchor := fuzzHistoryAnchorNode(f)
	for _, d := range seedNodeHistoryDeltas(f, anchor) {
		raw, err := EncodeNodeHistoryDelta(d)
		if err != nil {
			f.Fatalf("seed EncodeNodeHistoryDelta: %v", err)
		}
		f.Add(raw)
	}
	// Raw adversarial byte seeds: empty, wrong/missing tag, truncated-after-tag,
	// tag with garbage msgpack.
	for _, b := range [][]byte{
		{},
		{'D'},
		{'D', 0xff},
		{'D', 0xc0},
		{'D', 0x80},
		{0x80}, // untagged (looks like an anchor, not a delta) — must be rejected, not panic
		{0x00},
	} {
		f.Add(b)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		d, err := DecodeNodeHistoryDelta(data) // invariant 1: must not panic
		if err != nil {
			return
		}
		applied := ApplyNodeHistory(anchor, d) // invariant 2: must not panic
		n, verr := WireToNodeChecked(applied)
		if verr != nil {
			return
		}
		if n == nil {
			t.Fatalf("WireToNodeChecked returned (nil, nil) for reconstructed wire %+v", applied)
		}
		exerciseNode(t, n) // invariant 3
	})
}

func FuzzDecodeRelHistoryDelta(f *testing.F) {
	anchor := fuzzHistoryAnchorRel(f)
	for _, d := range seedRelHistoryDeltas(f, anchor) {
		raw, err := EncodeRelHistoryDelta(d)
		if err != nil {
			f.Fatalf("seed EncodeRelHistoryDelta: %v", err)
		}
		f.Add(raw)
	}
	for _, b := range [][]byte{
		{},
		{'D'},
		{'D', 0xff},
		{'D', 0xc0},
		{'D', 0x80},
		{0x80},
		{0x00},
	} {
		f.Add(b)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		d, err := DecodeRelHistoryDelta(data) // invariant 1: must not panic
		if err != nil {
			return
		}
		applied := ApplyRelHistory(anchor, d) // invariant 2: must not panic
		r, verr := WireToRelChecked(applied)
		if verr != nil {
			return
		}
		if r == nil {
			t.Fatalf("WireToRelChecked returned (nil, nil) for reconstructed wire %+v", applied)
		}
		exerciseRel(t, r) // invariant 3
	})
}
