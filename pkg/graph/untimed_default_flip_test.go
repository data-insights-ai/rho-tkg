package graph_test

// v4.57.0: the default of Config.UntimedValidStart (the zero value,
// UntimedDefault) is UntimedUnbounded for a NEW store, announced as
// Deprecated in v4.56.0 (docs/api.md § Stability and deprecation, default-change rule). An existing
// store keeps the mode it was written under: its persisted key (v4.56.x
// writes byte 0 for UntimedFromTxFrom at the first open of an empty store)
// or, with rows and no key, UntimedFromTxFrom. The explicit values still
// select either mode and still fail on a store of the other mode.

import (
	"bytes"
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// uvdKey reads the persisted mode key of the closed store in dir.
func uvdKey(t *testing.T, b uvsBackend, dir string) []byte {
	t.Helper()
	st := b.open(t, dir)
	defer func() { _ = st.Close() }()
	v, err := st.(storepkg.MetaKVCapability).MetaGet("untimed_valid_start")
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func uvdAdd(t *testing.T, g *graphpkg.Graph) types.NodeID {
	t.Helper()
	n, err := g.Nodes().Add(context.Background(), []string{"U"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return n.ID()
}

func TestUntimedDefaultFlip(t *testing.T) {
	t.Parallel()
	if graphpkg.UntimedDefault != 0 {
		t.Fatalf("UntimedDefault = %d, want the zero value", graphpkg.UntimedDefault)
	}
	def := graphpkg.UntimedDefault
	for _, b := range uvsDurable {
		t.Run(b.name, func(t *testing.T) {
			t.Parallel()

			// A store written under v4.56.x defaults (UntimedFromTxFrom,
			// persisted as byte 0) keeps the TxFrom rule under the new
			// default, for its old rows and for new creates.
			dir := t.TempDir()
			g, _ := uvsMustOpen(t, b, dir, graphpkg.UntimedFromTxFrom)
			if err := g.Index().CreateTemporal("U"); err != nil {
				t.Fatal(err)
			}
			old := uvdAdd(t, g)
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			if k := uvdKey(t, b, dir); !bytes.Equal(k, []byte{0}) {
				t.Fatalf("TxFrom store key = %v, want [0] (the v4.56.x encoding)", k)
			}
			for i := range 2 {
				g, _ = uvsMustOpen(t, b, dir, def)
				uvsCheckFirst(t, g, old, false, "v4.56 store, old row, default open")
				uvsCheckFirst(t, g, uvdAdd(t, g), false, "v4.56 store, new create, default open")
				if err := g.Close(); err != nil {
					t.Fatal(err)
				}
				if k := uvdKey(t, b, dir); !bytes.Equal(k, []byte{0}) {
					t.Fatalf("default open %d rewrote the TxFrom key to %v", i, k)
				}
			}
			if _, _, err := uvsOpen(t, b, dir, graphpkg.UntimedUnbounded); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
				t.Fatalf("v4.56 store opened explicitly unbounded: err = %v, want ErrUntimedModeMismatch", err)
			}

			// A store with rows and no key (written before v4.56.0).
			legacy := t.TempDir()
			g, st := uvsMustOpen(t, b, legacy, graphpkg.UntimedFromTxFrom)
			if err := g.Index().CreateTemporal("U"); err != nil {
				t.Fatal(err)
			}
			lid := uvdAdd(t, g)
			if err := st.(storepkg.MetaKVCapability).MetaSet("untimed_valid_start", nil); err != nil {
				t.Fatal(err)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			g, _ = uvsMustOpen(t, b, legacy, def)
			uvsCheckFirst(t, g, lid, false, "legacy store, default open")
			uvsCheckFirst(t, g, uvdAdd(t, g), false, "legacy store, new create")
			_ = g.Close()

			// A new store under the default is an UntimedUnbounded store.
			fresh := t.TempDir()
			g, _ = uvsMustOpen(t, b, fresh, def)
			if err := g.Index().CreateTemporal("U"); err != nil {
				t.Fatal(err)
			}
			nid := uvdAdd(t, g)
			uvsCheckFirst(t, g, nid, true, "new store, default")
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			if k := uvdKey(t, b, fresh); !bytes.Equal(k, []byte{1}) {
				t.Fatalf("new default store key = %v, want [1]", k)
			}
			g, _ = uvsMustOpen(t, b, fresh, def)
			uvsCheckFirst(t, g, nid, true, "new store, default reopen")
			_ = g.Close()
			g, _ = uvsMustOpen(t, b, fresh, graphpkg.UntimedUnbounded)
			_ = g.Close()
			if _, _, err := uvsOpen(t, b, fresh, graphpkg.UntimedFromTxFrom); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
				t.Fatalf("new default store opened explicitly TxFrom: err = %v, want ErrUntimedModeMismatch", err)
			}

			// The explicit option still selects TxFrom for a new store.
			explicit := t.TempDir()
			g, _ = uvsMustOpen(t, b, explicit, graphpkg.UntimedFromTxFrom)
			if err := g.Index().CreateTemporal("U"); err != nil {
				t.Fatal(err)
			}
			uvsCheckFirst(t, g, uvdAdd(t, g), false, "new store, explicit TxFrom")
			_ = g.Close()

			// An unfinished conversion is finished by a default open (the
			// store's persisted intent is UntimedUnbounded).
			conv := t.TempDir()
			g, st = uvsMustOpen(t, b, conv, graphpkg.UntimedFromTxFrom)
			if err := g.Index().CreateTemporal("U"); err != nil {
				t.Fatal(err)
			}
			cid := uvdAdd(t, g)
			if err := st.(storepkg.MetaKVCapability).MetaSet("untimed_valid_start", []byte{2}); err != nil {
				t.Fatal(err)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			g, _ = uvsMustOpen(t, b, conv, def)
			uvsCheckFirst(t, g, cid, true, "interrupted conversion, default open")
			_ = g.Close()
			if k := uvdKey(t, b, conv); !bytes.Equal(k, []byte{1}) {
				t.Fatalf("finished conversion key = %v, want [1]", k)
			}

			// A v4.56 store converted under the default, then reopened.
			g, _ = uvsMustOpen(t, b, dir, def)
			if err := g.Admin().SetUntimedValidStart(context.Background(), graphpkg.UntimedUnbounded); err != nil {
				t.Fatal(err)
			}
			uvsCheckFirst(t, g, uvdAdd(t, g), true, "converted under default, new create")
			_ = g.Close()
			g, _ = uvsMustOpen(t, b, dir, def)
			uvsCheckFirst(t, g, old, true, "converted under default, reopened")
			_ = g.Close()
		})
	}
	t.Run("memory", func(t *testing.T) {
		t.Parallel()
		g, err := graphpkg.New(graphpkg.Config{})
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()
		if err := g.Index().CreateTemporal("U"); err != nil {
			t.Fatal(err)
		}
		uvsCheckFirst(t, g, uvdAdd(t, g), true, "memory, default")
		if err := g.Admin().SetUntimedValidStart(context.Background(), def); !errors.Is(err, graphpkg.ErrInvalidUntimedValidStart) {
			t.Fatalf("SetUntimedValidStart(UntimedDefault): err = %v, want ErrInvalidUntimedValidStart", err)
		}
		tx, err := graphpkg.New(graphpkg.Config{UntimedValidStart: graphpkg.UntimedFromTxFrom})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Close()
		if err := tx.Index().CreateTemporal("U"); err != nil {
			t.Fatal(err)
		}
		uvsCheckFirst(t, tx, uvdAdd(t, tx), false, "memory, explicit TxFrom")
	})
}

// noMetaStore hides the wrapped store's MetaKV (an external store that cannot
// persist the mode).
type noMetaStore struct{ storepkg.MandatoryStore }

// TestUntimedDefaultWithoutMetaKV (review of the flip): a store without
// MetaKV cannot persist the mode, so a later session cannot tell it was new;
// the default stays UntimedFromTxFrom in every session, never a mix.
func TestUntimedDefaultWithoutMetaKV(t *testing.T) {
	t.Parallel()
	st := memory.New()
	for i := range 2 {
		g, err := graphpkg.New(graphpkg.Config{Store: noMetaStore{st}})
		if err != nil {
			t.Fatal(err)
		}
		id := uvdAdd(t, g)
		if _, err := g.Temporal().NodeAt(id, uvsFar); err == nil {
			t.Errorf("session %d: a default create on a store without MetaKV is valid at far (unbounded), want the TxFrom start", i)
		}
	}
}

// TestUntimedDefaultAdoptLogsTxFrom (review of the flip): a fresh default
// store logs ChangeMeta(unbounded) at its first open; an import that adopts a
// TxFrom export logs the switch too, so a replica following the feed ends in
// the store's mode.
func TestUntimedDefaultAdoptLogsTxFrom(t *testing.T) {
	t.Parallel()
	src, err := graphpkg.New(graphpkg.Config{UntimedValidStart: graphpkg.UntimedFromTxFrom})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	uvdAdd(t, src)
	var buf bytes.Buffer
	if err := src.IO().Export(&buf); err != nil {
		t.Fatal(err)
	}
	dst, err := graphpkg.New(graphpkg.Config{Store: memory.New(memory.WithChangeLog())})
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	if err := dst.IO().Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := dst.Index().CreateTemporal("U"); err != nil {
		t.Fatal(err)
	}
	uvsCheckFirst(t, dst, uvdAdd(t, dst), false, "adopted TxFrom export")
	metas := 0
	if err := dst.Replication().ForEachChange(0, func(rec storepkg.ChangeRecord) bool {
		if rec.Tag == storepkg.ChangeMeta {
			metas++
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if metas != 2 {
		t.Fatalf("ChangeMeta records = %d, want 2 (unbounded at the first open, TxFrom at the adopt)", metas)
	}
}
