package badger

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlogs 4, 7, 25: a store raises its format marker to
// KeptValidToStoreFormatVersion in the batch that writes its first delete
// tombstone marked DeleteKeptValidTo (a node delete with its cascaded
// relationship, or a relationship delete), and only then: a legacy-shaped
// tombstone (the delete wrote ValidTo) keeps the marker. A binary that reads
// markers one below refuses the store; this binary reads the tombstone back
// marked. Not parallel: it moves maxReadableStoreFormatVersion.
func TestKeptValidToTombstoneRaisesStoreFormatMarker(t *testing.T) {
	for _, kind := range []string{"node", "rel"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			open := func() *Store {
				t.Helper()
				bs, err := New(Config{Dir: dir})
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				return bs
			}
			closeStore := func(bs *Store) {
				t.Helper()
				if err := bs.Close(); err != nil {
					t.Fatal(err)
				}
			}
			marker := func(want uint16) {
				t.Helper()
				if v, ok := readRawMarker(t, dir); !ok || v != want {
					t.Fatalf("marker = (%d, %v); want %d", v, ok, want)
				}
			}
			live := func() *types.TemporalMetadata {
				return &types.TemporalMetadata{ValidFrom: 200, ValidTo: 900, TxFrom: 500}
			}
			tomb := func(kept bool) *types.TemporalMetadata {
				if kept {
					return &types.TemporalMetadata{ValidFrom: 200, ValidTo: 900, TxFrom: 500, TxTo: 600, DeletedAt: 600, DeleteKeptValidTo: true}
				}
				return &types.TemporalMetadata{ValidFrom: 200, ValidTo: 600, TxFrom: 500, TxTo: 600, DeletedAt: 600}
			}
			// del creates node id (and a relationship id+1 from it) and deletes
			// kind with a tombstone of the given shape; returns the history key.
			del := func(bs *Store, id int64, kept bool) {
				t.Helper()
				n := types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)
				n.SetTemporal(live())
				if err := bs.PutNode(n); err != nil {
					t.Fatal(err)
				}
				r := types.NewRelationship(types.RelID(snowflake.ID(id+1)), 1, n.ID(), n.ID())
				r.SetTemporal(live())
				if err := bs.PutRelationship(r); err != nil {
					t.Fatal(err)
				}
				rt := r.DeepCopy()
				rt.SetTemporal(tomb(kept))
				if kind == "rel" {
					if err := bs.DeleteRelWithHistory(r.ID(), r.Version(), rt); err != nil {
						t.Fatalf("DeleteRelWithHistory: %v", err)
					}
					return
				}
				nt := n.DeepCopy()
				nt.SetTemporal(tomb(false)) // the relationship's tombstone alone raises
				if err := bs.DeleteNodeWithHistory(n.ID(), n.Version(), nt, []RelTombstone{{ID: r.ID(), PrevVersion: r.Version(), Tombstone: rt}}); err != nil {
					t.Fatalf("DeleteNodeWithHistory: %v", err)
				}
			}

			bs := open()
			del(bs, 400, false)
			closeStore(bs)
			marker(openStampVersion()) // a legacy-shaped tombstone raises nothing

			bs = open()
			del(bs, 410, true)
			closeStore(bs)
			marker(storepkg.KeptValidToStoreFormatVersion)

			prev := maxReadableStoreFormatVersion
			maxReadableStoreFormatVersion = storepkg.KeptValidToStoreFormatVersion - 1
			old, err := New(Config{Dir: dir})
			maxReadableStoreFormatVersion = prev
			if err == nil {
				_ = old.Close()
				t.Fatal("an older reader opened a store holding a kept-ValidTo tombstone")
			}
			if !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
				t.Fatalf("older reader: %v; want ErrWireFormatVersionUnsupported", err)
			}

			bs = open()
			defer func() { _ = bs.Close() }()
			got, err := bs.GetRelVersion(types.RelID(snowflake.ID(411)), 0)
			if err != nil {
				t.Fatal(err)
			}
			if tm := got.Temporal(); !tm.DeleteKeptValidTo || tm.ValidTo != 900 || tm.DeletedAt != 600 {
				t.Fatalf("kept tombstone read back as %+v", tm)
			}
			legacy, err := bs.GetRelVersion(types.RelID(snowflake.ID(401)), 0)
			if err != nil {
				t.Fatal(err)
			}
			if tm := legacy.Temporal(); tm.DeleteKeptValidTo || !tm.DeleteWroteValidTo() {
				t.Fatalf("legacy tombstone read back as %+v", tm)
			}
		})
	}
}
