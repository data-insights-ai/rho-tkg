package storeutil

import (
	"bytes"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// P7: a store's compact frozen rows (types.CompactFrozenCopy) must put the
// same bytes on the wire as the ordinary frozen form, and recompute the same
// content hash, for first versions (compact) and for every other version
// (ordinary form kept). The wire and the hashes are on disk and replicated.
func TestCompactFrozenRowsKeepWireBytesAndHashes(t *testing.T) {
	t.Parallel()

	start := types.NewNode(types.NodeID(snowflake.ID(2001)), 1, nil)
	start.SetProperties(mustPropertySlice(t, map[string]any{"name": "10.0.0.1"}))
	start.SetTemporal(&types.TemporalMetadata{ValidFrom: 1_787_443_200_000, TxFrom: 1_787_443_200_001})
	startHash := integrity.ComputeNodeHash(start, []string{"Asset"})
	start.SetIntegrity(&types.NodeIntegrity{Hash: startHash})

	shapes := []struct {
		name string
		tm   types.TemporalMetadata
		prev bool
	}{
		{name: "first version", tm: types.TemporalMetadata{ValidFrom: 1_787_443_200_000, ValidTo: 1_787_443_200_500, TxFrom: 1_787_443_230_000}},
		{name: "updated version", tm: types.TemporalMetadata{ValidFrom: 1_787_443_200_000, TxFrom: 1_787_443_240_000, UpdatedAt: 1_787_443_240_000}, prev: true},
		{name: "history version", tm: types.TemporalMetadata{ValidFrom: 1_787_443_200_000, TxFrom: 1_787_443_230_000, TxTo: 1_787_443_240_000}},
	}
	for _, s := range shapes {
		r := types.NewRelationship(types.RelID(snowflake.ID(3001)), 4, start.ID(), start.ID())
		r.SetProperties(mustPropertySlice(t, map[string]any{"actor": `corp\u1`, "obs": int64(7), "tags": []string{"a"}}))
		tm := s.tm
		r.SetTemporal(&tm)
		ig := &types.RelIntegrity{Hash: integrity.ComputeRelHash(r, "HOP"), FromNodeHash: startHash, ToNodeHash: startHash}
		if s.prev {
			ig.PrevHash = startHash
		}
		r.SetIntegrity(ig)

		frozen := r.DeepCopy()
		frozen.Freeze()
		compact := r.CompactFrozenCopy()
		want, err := MarshalRelWire(frozen)
		if err != nil {
			t.Fatal(err)
		}
		got, err := MarshalRelWire(compact)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s: relationship wire bytes differ", s.name)
		}
		if h := integrity.ComputeRelHash(compact, "HOP"); h != ig.Hash || compact.Integrity().Hash != ig.Hash {
			t.Fatalf("%s: hash %s / stored %s, want %s", s.name, h, compact.Integrity().Hash, ig.Hash)
		}
	}

	frozenNode := start.DeepCopy()
	frozenNode.Freeze()
	compactNode := start.CompactFrozenCopy()
	want, err := MarshalNodeWire(frozenNode)
	if err != nil {
		t.Fatal(err)
	}
	got, err := MarshalNodeWire(compactNode)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("node wire bytes differ")
	}
	if h := integrity.ComputeNodeHash(compactNode, []string{"Asset"}); h != startHash || compactNode.Integrity().Hash != startHash {
		t.Fatalf("node hash %s / stored %s, want %s", h, compactNode.Integrity().Hash, startHash)
	}
}
