package core

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// F1 fix round: undoing a create (a rolled-back transaction, scoped or not, or
// a failed import) removes a relationship that never committed, so it leaves
// no ex-adjacency; a real delete still records both legs.
func TestRelCreateUndoLeavesNoExAdjacency(t *testing.T) {
	ctx := context.Background()
	for _, b := range []struct {
		name  string
		store func(t *testing.T) storepkg.MandatoryStore
	}{
		{"memory", func(*testing.T) storepkg.MandatoryStore { return memory.New() }},
		{"memory scoped tx", func(*testing.T) storepkg.MandatoryStore { return memory.New(memory.WithChangeLog()) }},
		{"badger", func(t *testing.T) storepkg.MandatoryStore {
			bs, err := badger.New(badger.Config{InMemory: true})
			if err != nil {
				t.Fatal(err)
			}
			return bs
		}},
		{"badger scoped tx", func(t *testing.T) storepkg.MandatoryStore {
			bs, err := badger.New(badger.Config{InMemory: true, ChangeLog: true})
			if err != nil {
				t.Fatal(err)
			}
			return bs
		}},
	} {
		g, err := New(Config{UntimedValidStart: UntimedFromTxFrom, Store: b.store(t)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = g.Close() })
		a, err := g.Nodes.Add(ctx, []string{"A"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		z, err := g.Nodes.Add(ctx, []string{"A"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		legs := func() (out, in []types.RelID) {
			ex := g.store.(storepkg.ExAdjacencyCapability)
			for _, l := range []struct {
				n        types.NodeID
				incoming bool
				ids      *[]types.RelID
			}{{a.ID(), false, &out}, {z.ID(), true, &in}} {
				if err := ex.ForEachExAdjacentRelID(l.n, l.incoming, func(id types.RelID) bool { *l.ids = append(*l.ids, id); return true }); err != nil {
					t.Fatal(err)
				}
			}
			return out, in
		}

		tx, err := g.BeginTx()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.AddRelationship("R", a, z, nil); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if out, in := legs(); len(out)+len(in) != 0 {
			t.Errorf("%s: rolled-back create left ex-adjacency out=%v in=%v", b.name, out, in)
		}

		// A failed import undoes relationship 300 (its second relationship
		// names a missing end node).
		var stream bytes.Buffer
		writeImportMsgpackRecord(t, &stream, exportTagHeader, exportHeader{Version: exportFormatVersion})
		writeImportMsgpackRecord(t, &stream, exportTagRegistry, tiered.RegistryFileData{Labels: []string{"", "A"}, RelTypes: []string{"", "R"}})
		for _, id := range []int64{100, 101} {
			writeImportMsgpackRecord(t, &stream, exportTagNode, mustHashedNodeWire(t, storeutil.NodeWire{ID: id, PrimaryLabel: 1}, []string{"A"}))
		}
		for _, w := range []storeutil.RelWire{{ID: 300, RelType: 1, StartID: 100, EndID: 101}, {ID: 301, RelType: 1, StartID: 100, EndID: 200}} {
			writeImportMsgpackRecord(t, &stream, exportTagRel, mustHashedRelWire(t, w, "R"))
		}
		if err := g.IO.Import(bytes.NewReader(stream.Bytes()), tkgio.ImportOptions{}); !errors.Is(err, storepkg.ErrNodeNotFound) {
			t.Fatalf("%s: Import = %v, want ErrNodeNotFound", b.name, err)
		}
		ex := g.store.(storepkg.ExAdjacencyCapability)
		for _, l := range []struct {
			n        types.NodeID
			incoming bool
		}{{100, false}, {101, true}} {
			if err := ex.ForEachExAdjacentRelID(l.n, l.incoming, func(id types.RelID) bool {
				t.Errorf("%s: failed import left ex-adjacency(%d, incoming=%v) %d", b.name, l.n, l.incoming, id)
				return true
			}); err != nil {
				t.Fatal(err)
			}
		}

		r, err := g.Rels.Add(ctx, "R", a, z, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := g.Rels.Delete(ctx, r.ID()); err != nil {
			t.Fatal(err)
		}
		if out, in := legs(); !slices.Contains(out, r.ID()) || !slices.Contains(in, r.ID()) {
			t.Errorf("%s: real delete legs out=%v in=%v, want both to hold %d", b.name, out, in, r.ID())
		}
	}
}
