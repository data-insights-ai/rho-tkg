package sharded

import (
	"testing"

	badger "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestChunkSnapshotOverNeedsOrdinals (round 10 review): the ordinal snapshot
// doors' fold declines a concatenation whose slot snapshot carries no
// ordinals (ok=false), instead of handing out chunks with empty Ordinals;
// the same fold over the slots' ordinal snapshots serves them.
func TestChunkSnapshotOverNeedsOrdinals(t *testing.T) {
	t.Parallel()
	s, err := New(Config{InMemory: true, SlotCount: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	n := types.NewNode(types.NodeID(1<<40+2), 3, nil)
	if err := n.SetProperty("k", int64(7)); err != nil {
		t.Fatal(err)
	}
	if err := s.PutNode(n); err != nil {
		t.Fatal(err)
	}
	keys := []string{"k"}
	plain := func(shard *badger.Store) (types.NodeColumnReader, bool, error) {
		r, _, ok, err := shard.DocValuesSnapshot(3, keys)
		return r, ok, err
	}
	if _, _, ok, err := s.chunkSnapshotOver([]uint16{3}, false, plain); err != nil || ok {
		t.Fatalf("plain slot snapshots: ok=%v err=%v, want a decline", ok, err)
	}
	snap, _, ok, err := s.DocValuesSnapshotOrdinal(3, keys)
	if err != nil || !ok {
		t.Fatalf("ordinal door: ok=%v err=%v", ok, err)
	}
	var chunk types.ColumnChunk
	snap.ReadChunk(0, snap.Len(), &chunk)
	if len(chunk.IDs) != 1 || len(chunk.Ordinals) != 1 || chunk.Ordinals[0] == 0 {
		t.Fatalf("ordinal door chunk: ids %v ordinals %v", chunk.IDs, chunk.Ordinals)
	}
}
