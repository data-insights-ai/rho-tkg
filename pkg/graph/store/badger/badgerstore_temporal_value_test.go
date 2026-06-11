package badger

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTemporalValue_BadgerRoundTrip pins the ptTemporal wire arm through a
// REAL store write+flush+evict+decode cycle — the msgpack encode side
// produces []any{int, string}, the decode side must reconstruct
// types.TemporalValue exactly (kind AND rendering), and a plain string
// property with the same rendering must come back a plain string.
func TestTemporalValue_BadgerRoundTrip(t *testing.T) {
	t.Parallel()
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { bs.Close() })

	n := types.NewNode(types.NodeID(1), 1, nil)
	wantTV := types.TemporalValue{Kind: types.TemporalDateTime, Value: "2024-01-01T12:30:00+02:00"}
	if err := n.SetProperty("d", wantTV); err != nil {
		t.Fatalf("set temporal: %v", err)
	}
	if err := n.SetProperty("s", "2024-01-01T12:30:00+02:00"); err != nil {
		t.Fatalf("set string: %v", err)
	}
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	bs.nodeCache.ResetForTest() // force the badger decode path

	got, err := bs.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	dv, ok := got.GetProperty("d")
	if !ok {
		t.Fatal("temporal property missing after decode")
	}
	tv, ok := dv.(types.TemporalValue)
	if !ok || tv != wantTV {
		t.Fatalf("decoded temporal = %#v (%T), want %#v", dv, dv, wantTV)
	}
	sv, ok := got.GetProperty("s")
	if !ok {
		t.Fatal("string property missing after decode")
	}
	if s, isStr := sv.(string); !isStr || s != "2024-01-01T12:30:00+02:00" {
		t.Fatalf("decoded string = %#v (%T), want plain string", sv, sv)
	}
}
