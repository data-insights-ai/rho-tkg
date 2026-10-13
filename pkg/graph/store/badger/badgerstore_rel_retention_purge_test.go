package badger

import (
	"errors"
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestPurgeRelLegResidue (backlog 21): the tiered leg-residue primitive drops
// the in-leg, the out-leg and the ex-adjacency legs a purged relationship left
// on a shard that never held its row, leaves a shard that still holds the row
// alone (its own purge removes it whole), is idempotent, and rejects a
// malformed descriptor and a closed store. Both adjacency modes (RAM mirror
// and on-disk keys).
func TestPurgeRelLegResidue(t *testing.T) {
	for _, onDisk := range []bool{false, true} {
		bs, err := New(Config{InMemory: true, AdjacencyIndexOnDisk: onDisk})
		if err != nil {
			t.Fatal(err)
		}
		start, end := snowflake.ID(1<<32+1), snowflake.ID(1<<32+2)
		orphan := snowflake.ID(1<<32 + 10)
		if err := bs.PutRelIncoming(end, start, 5, orphan); err != nil {
			t.Fatal(err)
		}
		if err := bs.NoteExAdjacentRel(types.RelID(orphan), types.NodeID(start), types.NodeID(end)); err != nil {
			t.Fatal(err)
		}
		desc := storecontract.PurgedRel{ID: types.RelID(orphan), TypeToken: 5, StartID: types.NodeID(start), EndID: types.NodeID(end)}
		for i := 0; i < 2; i++ { // idempotent
			if err := bs.PurgeRelLegResidue(desc); err != nil {
				t.Fatal(err)
			}
		}
		if got := bs.IncomingRelIDs(end, 0); len(got) != 0 {
			t.Fatalf("onDisk=%v: in-legs after the residue purge = %v", onDisk, got)
		}
		for _, leg := range []struct {
			n        snowflake.ID
			incoming bool
		}{{start, false}, {end, true}} {
			var ids []types.RelID
			if err := bs.ForEachExAdjacentRelID(types.NodeID(leg.n), leg.incoming, func(id types.RelID) bool { ids = append(ids, id); return true }); err != nil {
				t.Fatal(err)
			}
			if slices.Contains(ids, types.RelID(orphan)) {
				t.Fatalf("onDisk=%v: ex-adjacency still names the purged relationship", onDisk)
			}
		}

		// A store holding the row: untouched.
		for _, n := range []snowflake.ID{start, end} {
			if err := bs.PutNode(types.NewNode(types.NodeID(n), 1, nil)); err != nil {
				t.Fatal(err)
			}
		}
		liveID := snowflake.ID(1<<32 + 11)
		if err := bs.PutRelationship(types.NewRelationship(types.RelID(liveID), 5, types.NodeID(start), types.NodeID(end))); err != nil {
			t.Fatal(err)
		}
		live := storecontract.PurgedRel{ID: types.RelID(liveID), TypeToken: 5, StartID: types.NodeID(start), EndID: types.NodeID(end)}
		if err := bs.PurgeRelLegResidue(live); err != nil {
			t.Fatal(err)
		}
		if got := bs.IncomingRelIDs(end, 0); !slices.Equal(got, []snowflake.ID{liveID}) {
			t.Fatalf("onDisk=%v: a live row's in-leg was dropped: %v", onDisk, got)
		}

		bad := desc
		bad.TypeToken = 0
		if err := bs.PurgeRelLegResidue(bad); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
			t.Fatalf("type token 0: %v, want ErrInvalidStoreMutation", err)
		}
		_ = bs.Close()
		if err := bs.PurgeRelLegResidue(desc); !errors.Is(err, storecontract.ErrStoreClosed) {
			t.Fatalf("closed store: %v, want ErrStoreClosed", err)
		}
		if _, err := bs.PurgeRelsByTypeBefore(5, 1, 0); !errors.Is(err, storecontract.ErrStoreClosed) {
			t.Fatalf("closed store purge: %v, want ErrStoreClosed", err)
		}
		if err := bs.LogRelRangePurge(5, 1, 0); !errors.Is(err, storecontract.ErrStoreClosed) {
			t.Fatalf("closed store log: %v, want ErrStoreClosed", err)
		}
	}
}
