package badger

import (
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 10: the tiered store writes a cross-shard relationship's entity
// through PutRelEntityAndOut (its incoming leg lives on the end node's shard)
// and removes it through DeleteRelEntityAndOut. The relationship row lives
// entirely on this shard, so its rel-type temporal envelope must be
// maintained exactly as PutRelationship / deleteRelByInfo maintain it — else a
// tiered shard keeps every cross-shard row it could prune (lost recall) and a
// deleted row stays covered by a stale envelope.
func TestRelTypeTemporalIndex_PartialDoorsMaintainEnvelope(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)
	const relType, label = uint16(6), uint16(1)
	if err := bs.PutNode(types.NewNode(types.NodeID(1), label, nil)); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateRelTemporalIndex(relType); err != nil {
		t.Fatalf("CreateRelTemporalIndex: %v", err)
	}
	put := func(id types.RelID, from, to types.Instant) {
		t.Helper()
		r := types.NewRelationship(id, relType, types.NodeID(1), types.NodeID(99)) // end node lives on another shard
		r.SetTemporal(&types.TemporalMetadata{ValidFrom: from, ValidTo: to})
		if err := bs.PutRelEntityAndOut(r); err != nil {
			t.Fatalf("PutRelEntityAndOut(%d): %v", id, err)
		}
	}
	put(101, 1000, 2000)
	put(102, 3000, 0)
	ids := []types.RelID{101, 102}

	kept, ok := bs.PruneRelTypeTemporalCandidates(relType, ids, QueryOpts{ValidAt: 2500})
	if !ok || len(kept) != 0 {
		t.Errorf("at 2500 kept %v (ok=%v), want none: both rows are covered and cannot overlap", kept, ok)
	}
	kept, _ = bs.PruneRelTypeTemporalCandidates(relType, ids, QueryOpts{ValidAt: 1500})
	if !slices.Equal(kept, []types.RelID{101}) {
		t.Errorf("at 1500 kept %v, want [101]", kept)
	}
	kept, _ = bs.PruneRelTypeTemporalCandidates(relType, ids, QueryOpts{ValidStart: 1900, ValidEnd: 3100})
	if !slices.Equal(kept, ids) {
		t.Errorf("during 1900-3100 kept %v, want %v", kept, ids)
	}

	// Delete: the row is gone from this shard, so no envelope may vouch for it.
	if _, err := bs.DeleteRelEntityAndOut(101); err != nil {
		t.Fatalf("DeleteRelEntityAndOut: %v", err)
	}
	bs.idxMu.RLock()
	_, _, covered := bs.relTypeTemporalIndexes[relType].EnvelopeOf(101)
	bs.idxMu.RUnlock()
	if covered {
		t.Error("deleted cross-shard row still covered by the envelope")
	}
	kept, _ = bs.PruneRelTypeTemporalCandidates(relType, ids, QueryOpts{ValidAt: 2500})
	if !slices.Equal(kept, []types.RelID{101}) {
		t.Errorf("after delete at 2500 kept %v, want [101] (uncovered ids are always kept)", kept)
	}
}
