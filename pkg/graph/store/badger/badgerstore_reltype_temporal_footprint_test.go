package badger

import (
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRelTypeTemporalIndexHoldsOnlyEnvelopes (RELTEMPID): badger reads a
// relationship type's temporal index only through EnvelopeOf, so through
// writes, a clean reopen from the open image and pruning it never builds the
// sorted stabbing entries (~120 -> one map slot a rel), and prunes as before.
func TestRelTypeTemporalIndexHoldsOnlyEnvelopes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	const relType, label, n = uint16(6), uint16(1), 200
	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []types.NodeID{1, 2} {
		if err := bs.PutNode(types.NewNode(id, label, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.CreateRelTemporalIndex(relType); err != nil {
		t.Fatal(err)
	}
	all := make([]types.RelID, 0, n)
	for i := range n {
		id := types.RelID(101 + i)
		r := types.NewRelationship(id, relType, 1, 2)
		r.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(1000 * (i + 1)), ValidTo: types.Instant(1000*(i+1) + 500)})
		if err := bs.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
		all = append(all, id)
	}
	check := func(stage string) {
		t.Helper()
		kept, ok := bs.PruneRelTypeTemporalCandidates(relType, all, QueryOpts{ValidAt: 5200})
		if !ok || !slices.Equal(kept, []types.RelID{105}) {
			t.Fatalf("%s: kept %v (ok %v), want [105]", stage, kept, ok)
		}
		bs.idxMu.RLock()
		defer bs.idxMu.RUnlock()
		ti := bs.relTypeTemporalIndexes[relType]
		if ti.Len() != n || ti.Entries != nil {
			t.Fatalf("%s: %d envelopes, %d sorted entries; want %d and none", stage, ti.Len(), len(ti.Entries), n)
		}
	}
	check("written")
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	if bs, err = New(Config{Dir: dir}); err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	check("reopened")
}
