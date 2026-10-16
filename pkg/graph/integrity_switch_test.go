package graph_test

// Backlog 61: Config.DisableIntegrityHashes (default false = hashes on) and
// one binary hash encoding on disk. TestIntegritySwitch is the red test.

import (
	"bytes"
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
)

var hex64 = regexp.MustCompile(`^[0-9a-f]{64}$`)

type isBackend struct {
	name       string
	persistent bool
	open       func(t *testing.T, dir string) storepkg.MandatoryStore
}

var isBackends = []isBackend{
	{"memory", false, func(t *testing.T, _ string) storepkg.MandatoryStore { return memory.New() }},
	{"badger", true, func(t *testing.T, dir string) storepkg.MandatoryStore {
		bs, err := badger.New(badger.Config{Dir: dir})
		if err != nil {
			t.Fatal(err)
		}
		return bs
	}},
	{"tiered", true, func(t *testing.T, dir string) storepkg.MandatoryStore {
		ts, err := tiered.New(tiered.Config{DataDir: dir, ShardWindow: 7 * 24 * time.Hour, RefLabels: []string{"Ref"}})
		if err != nil {
			t.Fatal(err)
		}
		return ts
	}},
}

func isOpen(t *testing.T, b isBackend, dir string, off bool) (*graphpkg.Graph, storepkg.MandatoryStore, error) {
	t.Helper()
	st := b.open(t, dir)
	g, err := graphpkg.New(graphpkg.Config{Store: st, DisableIntegrityHashes: off, UntimedValidStart: graphpkg.UntimedFromTxFrom}) // markers 3/4 under test; an unbounded store raises 7
	if err != nil {
		_ = st.Close()
		return nil, nil, err
	}
	return g, st, nil
}

type isRows struct {
	a, b   *types.Node
	r, r2  types.RelID
	nodeID types.NodeID
}

// isWrite writes two nodes, a relationship updated once (a history row and a
// PrevHash link) and a second, first-version relationship.
func isWrite(t *testing.T, g *graphpkg.Graph) isRows {
	t.Helper()
	ctx := context.Background()
	a, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"k": "a"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"k": "b"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels().Add(ctx, "E", a, b, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"w": int64(2)}); err != nil {
		t.Fatal(err)
	}
	r2, err := g.Rels().Add(ctx, "E", a, b, map[string]any{"w": int64(7)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"k": "a2"}); err != nil {
		t.Fatal(err)
	}
	return isRows{a: a, b: b, r: r.ID(), r2: r2.ID(), nodeID: a.ID()}
}

func isCheckOn(t *testing.T, g *graphpkg.Graph, rows isRows) {
	t.Helper()
	ctx := context.Background()
	n, err := g.Nodes().Get(ctx, rows.nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if ig := n.Integrity(); ig == nil || !hex64.MatchString(ig.Hash) || !hex64.MatchString(ig.PrevHash) {
		t.Fatalf("on: node integrity %+v, want 64-hex Hash and PrevHash", ig)
	}
	r, err := g.Rels().Get(ctx, rows.r)
	if err != nil {
		t.Fatal(err)
	}
	ig := r.Integrity()
	if ig == nil || !hex64.MatchString(ig.Hash) || !hex64.MatchString(ig.PrevHash) ||
		!hex64.MatchString(ig.FromNodeHash) || !hex64.MatchString(ig.ToNodeHash) {
		t.Fatalf("on: rel integrity %+v, want four 64-hex hashes", ig)
	}
	if ok, err := g.Hash().VerifyRelChain(rows.r); err != nil || !ok {
		t.Fatalf("on: VerifyRelChain = %v, %v; want true", ok, err)
	}
	if ok, err := g.Hash().VerifyNodeChain(rows.nodeID); err != nil || !ok {
		t.Fatalf("on: VerifyNodeChain = %v, %v; want true", ok, err)
	}
}

func isCheckOff(t *testing.T, g *graphpkg.Graph, st storepkg.MandatoryStore, rows isRows) {
	t.Helper()
	ctx := context.Background()
	n, err := g.Nodes().Get(ctx, rows.nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if ig := n.Integrity(); ig != nil && (ig.Hash != "" || ig.PrevHash != "") {
		t.Fatalf("off: node integrity %+v, want no hash", ig)
	}
	for _, id := range []types.RelID{rows.r, rows.r2} {
		r, err := g.Rels().Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if ig := r.Integrity(); ig != nil && (ig.Hash != "" || ig.PrevHash != "" || ig.FromNodeHash != "" || ig.ToNodeHash != "") {
			t.Fatalf("off: rel integrity %+v, want no hash", ig)
		}
		// The store's own row (resident or decoded from disk) holds none either.
		sr, err := st.GetRelationship(id)
		if err != nil {
			t.Fatal(err)
		}
		if ig := sr.Integrity(); ig != nil && (ig.Hash != "" || ig.FromNodeHash != "" || ig.ToNodeHash != "") {
			t.Fatalf("off: stored rel integrity %+v, want no hash", ig)
		}
	}
	if v, ok := g.Resolve().RelProperty(mustRel(t, g, rows.r), types.ShadowHash); ok && v != "" {
		t.Fatalf("off: tkg_hash shadow = %v, want empty", v)
	}
	if _, err := g.Hash().VerifyRelChain(rows.r); !errors.Is(err, graphpkg.ErrIntegrityDisabled) {
		t.Fatalf("off: VerifyRelChain err = %v, want ErrIntegrityDisabled", err)
	}
	if _, err := g.Hash().VerifyNodeChain(rows.nodeID); !errors.Is(err, graphpkg.ErrIntegrityDisabled) {
		t.Fatalf("off: VerifyNodeChain err = %v, want ErrIntegrityDisabled", err)
	}
}

func mustRel(t *testing.T, g *graphpkg.Graph, id types.RelID) *types.Relationship {
	t.Helper()
	r, err := g.Rels().Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// isTamper rewrites r2's content in the store behind the graph, keeping its
// stored hash: VerifyRelChain must report it.
func isTamper(t *testing.T, g *graphpkg.Graph, st storepkg.MandatoryStore, id types.RelID) {
	t.Helper()
	cur, err := st.GetRelationship(id)
	if err != nil {
		t.Fatal(err)
	}
	cp := cur.DeepCopy()
	if err := cp.SetProperty("w", int64(99)); err != nil {
		t.Fatal(err)
	}
	if err := st.ReplaceRelationship(cp); err != nil {
		t.Fatal(err)
	}
	if ok, err := g.Hash().VerifyRelChain(id); err != nil || ok {
		t.Fatalf("on: VerifyRelChain of a tampered row = %v, %v; want false, nil", ok, err)
	}
}

// rawRelRow returns the bytes of a relationship's current row in a badger DB.
func rawRelRow(t *testing.T, db *badgerv4.DB, id types.RelID) []byte {
	t.Helper()
	var out []byte
	if err := db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storeutil.RelKey(id.SnowflakeID()))
		if err != nil {
			return err
		}
		out, err = item.ValueCopy(nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestIntegritySwitch(t *testing.T) {
	for _, b := range isBackends {
		t.Run(b.name+"/on", func(t *testing.T) {
			g, st, err := isOpen(t, b, t.TempDir(), false)
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			rows := isWrite(t, g)
			isCheckOn(t, g, rows)
			isTamper(t, g, st, rows.r2)
		})
		t.Run(b.name+"/off", func(t *testing.T) {
			dir := t.TempDir()
			g, st, err := isOpen(t, b, dir, true)
			if err != nil {
				t.Fatal(err)
			}
			rows := isWrite(t, g)
			isCheckOff(t, g, st, rows)
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			if !b.persistent {
				return
			}
			// Rows decoded from disk after a reopen carry no hash either.
			g, st, err = isOpen(t, b, dir, true)
			if err != nil {
				t.Fatalf("reopen off-store with off: %v", err)
			}
			defer g.Close()
			isCheckOff(t, g, st, rows)
		})
		if !b.persistent {
			continue
		}
		t.Run(b.name+"/reopen", func(t *testing.T) {
			for _, written := range []bool{false, true} { // false = written with hashes on
				dir := t.TempDir()
				g, _, err := isOpen(t, b, dir, written)
				if err != nil {
					t.Fatal(err)
				}
				rows := isWrite(t, g)
				if err := g.Close(); err != nil {
					t.Fatal(err)
				}
				if _, _, err := isOpen(t, b, dir, !written); !errors.Is(err, graphpkg.ErrIntegrityModeMismatch) {
					t.Fatalf("store written off=%v reopened off=%v: err = %v, want ErrIntegrityModeMismatch", written, !written, err)
				}
				g, st, err := isOpen(t, b, dir, written)
				if err != nil {
					t.Fatalf("reopen with the written setting: %v", err)
				}
				if written {
					isCheckOff(t, g, st, rows)
				} else {
					isCheckOn(t, g, rows)
				}
				if err := g.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}

	// A store written before the switch existed has no persisted setting but
	// holds hashes: it is an on-store. An empty one takes the configured mode.
	t.Run("badger/legacy", func(t *testing.T) {
		dir := t.TempDir()
		g, st, err := isOpen(t, isBackends[1], dir, false)
		if err != nil {
			t.Fatal(err)
		}
		isWrite(t, g)
		if err := st.(storepkg.MetaKVCapability).MetaSet("integrity_hashes", nil); err != nil {
			t.Fatal(err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := isOpen(t, isBackends[1], dir, true); !errors.Is(err, graphpkg.ErrIntegrityModeMismatch) {
			t.Fatalf("legacy store with rows opened off: err = %v, want ErrIntegrityModeMismatch", err)
		}
		g, _, err = isOpen(t, isBackends[1], dir, false)
		if err != nil {
			t.Fatalf("legacy store opened on: %v", err)
		}
		_ = g.Close()

		empty := t.TempDir()
		g, _, err = isOpen(t, isBackends[1], empty, true)
		if err != nil {
			t.Fatalf("empty store opened off: %v", err)
		}
		_ = g.Close()
		// An empty store takes the configured setting even when a key exists.
		g, _, err = isOpen(t, isBackends[1], empty, false)
		if err != nil {
			t.Fatalf("empty store stamped off, reopened on: %v", err)
		}
		rows := isWrite(t, g)
		isCheckOn(t, g, rows)
		_ = g.Close()
	})

	// The tiered store writes the setting durably to its reference shard (its
	// rows land on event shards).
	t.Run("tiered/setting_written_durably", func(t *testing.T) {
		ts, err := tiered.New(tiered.Config{DataDir: t.TempDir(), ShardWindow: 7 * 24 * time.Hour, RefLabels: []string{"Ref"}})
		if err != nil {
			t.Fatal(err)
		}
		syncs := ts.RefShardForTest().MetaSyncCountForTest()
		g, err := graphpkg.New(graphpkg.Config{UntimedValidStart: graphpkg.UntimedFromTxFrom, Store: ts, DisableIntegrityHashes: true})
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()
		if ts.RefShardForTest().MetaSyncCountForTest() <= syncs {
			t.Fatal("tiered: the integrity-hash setting was not written durably")
		}
	})

	// Review finding 1: a New that fails on a config conflict persists nothing.
	t.Run("badger/failed_open_persists_nothing", func(t *testing.T) {
		dir := t.TempDir()
		bs, err := badger.New(badger.Config{Dir: dir})
		if err != nil {
			t.Fatal(err)
		}
		_, err = graphpkg.New(graphpkg.Config{UntimedValidStart: graphpkg.UntimedFromTxFrom, Store: bs, DisableIntegrityHashes: true,
			RelSegments: []storepkg.RelSegmentSpec{{Type: "HOP"}}})
		if !errors.Is(err, graphpkg.ErrIntegrityDisabled) {
			t.Fatalf("off + RelSegments: err = %v, want ErrIntegrityDisabled", err)
		}
		v, err := bs.MetaGet("integrity_hashes")
		_ = bs.Close()
		if err != nil || v != nil {
			t.Fatalf("failed New persisted the setting: %v, %v", v, err)
		}
		g, _, err := isOpen(t, isBackends[1], dir, false)
		if err != nil {
			t.Fatalf("on-open after the failed off-open: %v", err)
		}
		_ = g.Close()
	})

	// Review finding 2: the setting is written durably, and a store whose key
	// was lost (a power cut after its rows survived) is judged by its rows.
	t.Run("badger/lost_key_judged_by_rows", func(t *testing.T) {
		dir := t.TempDir()
		bs, err := badger.New(badger.Config{Dir: dir})
		if err != nil {
			t.Fatal(err)
		}
		syncs := bs.MetaSyncCountForTest()
		g, err := graphpkg.New(graphpkg.Config{UntimedValidStart: graphpkg.UntimedFromTxFrom, Store: bs, DisableIntegrityHashes: true})
		if err != nil {
			t.Fatal(err)
		}
		if bs.MetaSyncCountForTest() <= syncs {
			t.Fatal("the integrity-hash setting was not written durably (no meta fsync)")
		}
		rows := isWrite(t, g)
		if err := bs.MetaSet("integrity_hashes", nil); err != nil {
			t.Fatal(err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := isOpen(t, isBackends[1], dir, false); !errors.Is(err, graphpkg.ErrIntegrityModeMismatch) {
			t.Fatalf("unhashed rows without the key opened on: err = %v, want ErrIntegrityModeMismatch", err)
		}
		g, st, err := isOpen(t, isBackends[1], dir, true)
		if err != nil {
			t.Fatalf("unhashed rows without the key opened off: %v", err)
		}
		isCheckOff(t, g, st, rows)
		_ = g.Close()
	})

	// Admin().Reset wipes the meta keyspace; the setting survives it, so an
	// off-store written after a Reset still reopens as an off-store.
	t.Run("badger/reset_keeps_setting", func(t *testing.T) {
		dir := t.TempDir()
		bs, err := badger.New(badger.Config{Dir: dir})
		if err != nil {
			t.Fatal(err)
		}
		g, err := graphpkg.New(graphpkg.Config{UntimedValidStart: graphpkg.UntimedFromTxFrom, Store: bs, DisableIntegrityHashes: true, AllowReset: true})
		if err != nil {
			t.Fatal(err)
		}
		isWrite(t, g)
		if err := g.Admin().Reset(); err != nil {
			t.Fatal(err)
		}
		rows := isWrite(t, g)
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := isOpen(t, isBackends[1], dir, false); !errors.Is(err, graphpkg.ErrIntegrityModeMismatch) {
			t.Fatalf("off-store after Reset reopened on: err = %v, want ErrIntegrityModeMismatch", err)
		}
		g, st, err := isOpen(t, isBackends[1], dir, true)
		if err != nil {
			t.Fatalf("off-store after Reset reopened off: %v", err)
		}
		defer g.Close()
		isCheckOff(t, g, st, rows)
	})

	// Rollout (backlog 61): this release reads a store the binary-hash writer
	// wrote (marker 4, binary rows), keeps writing into it with its own writer,
	// never lowers the marker, and every chain still verifies.
	t.Run("badger/reads_binary_hash_store", func(t *testing.T) {
		dir := t.TempDir()
		restore := storeutil.SetBinaryHashWriterForTest(true)
		g, _, err := isOpen(t, isBackends[1], dir, false)
		if err != nil {
			restore()
			t.Fatal(err)
		}
		rows := isWrite(t, g)
		err = g.Close()
		restore()
		if err != nil {
			t.Fatal(err)
		}
		g, _, err = isOpen(t, isBackends[1], dir, false)
		if err != nil {
			t.Fatalf("open a binary-hash store with this release's writer: %v", err)
		}
		isCheckOn(t, g, rows)
		more := isWrite(t, g)
		isCheckOn(t, g, more)
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		var marker []byte
		if err := db.View(func(txn *badgerv4.Txn) error {
			item, err := txn.Get(storeutil.WireFormatVersionKey)
			if err != nil {
				return err
			}
			marker, err = item.ValueCopy(nil)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if len(marker) != 2 || marker[1] != storeutil.BinaryHashStoreFormatVersion {
			t.Fatalf("marker after reopen = %v, want %d kept", marker, storeutil.BinaryHashStoreFormatVersion)
		}
	})

	// One marker mechanism with backlog 63: a store at the Elements marker (5)
	// that the binary-hash writer opens keeps 5 (never lowered to 4), and its
	// binary rows read back.
	t.Run("badger/elements_marker_kept_by_binary_writer", func(t *testing.T) {
		defer storeutil.SetBinaryHashWriterForTest(true)()
		dir := t.TempDir()
		g, _, err := isOpen(t, isBackends[1], dir, false)
		if err != nil {
			t.Fatal(err)
		}
		rows := isWrite(t, g)
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		marker := func(set []byte) []byte {
			db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var out []byte
			if err := db.Update(func(txn *badgerv4.Txn) error {
				if set != nil {
					return txn.Set(storeutil.WireFormatVersionKey, set)
				}
				item, err := txn.Get(storeutil.WireFormatVersionKey)
				if err != nil {
					return err
				}
				out, err = item.ValueCopy(nil)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			return out
		}
		if m := marker(nil); len(m) != 2 || m[1] != storeutil.BinaryHashStoreFormatVersion {
			t.Fatalf("binary writer's marker = %v, want %d", m, storeutil.BinaryHashStoreFormatVersion)
		}
		marker([]byte{0, storeutil.ElementsStoreFormatVersion})
		g, _, err = isOpen(t, isBackends[1], dir, false)
		if err != nil {
			t.Fatalf("open at marker 5: %v", err)
		}
		isCheckOn(t, g, rows)
		isWrite(t, g)
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if m := marker(nil); len(m) != 2 || m[1] != storeutil.ElementsStoreFormatVersion {
			t.Fatalf("marker after the binary writer reopened an Elements store = %v, want %d kept", m, storeutil.ElementsStoreFormatVersion)
		}
	})

	// And the other way: a store the hex-hash writer wrote (step 1 and every
	// older release, marker 3) opens, reads and verifies with this writer.
	t.Run("badger/reads_hex_hash_store", func(t *testing.T) {
		dir := t.TempDir()
		restore := storeutil.SetBinaryHashWriterForTest(false)
		g, _, err := isOpen(t, isBackends[1], dir, false)
		if err != nil {
			restore()
			t.Fatal(err)
		}
		rows := isWrite(t, g)
		err = g.Close()
		restore()
		if err != nil {
			t.Fatal(err)
		}
		g, _, err = isOpen(t, isBackends[1], dir, false)
		if err != nil {
			t.Fatalf("open a hex-hash store with this release's writer: %v", err)
		}
		defer g.Close()
		isCheckOn(t, g, rows)
		more := isWrite(t, g)
		isCheckOn(t, g, more)
	})

	// On disk: off writes no hash bytes; the binary-hash writer writes each
	// hash as 32 raw bytes (the hex strings cost 64 + framing each).
	t.Run("badger/bytes", func(t *testing.T) {
		defer storeutil.SetBinaryHashWriterForTest(true)()
		var size [2]int
		for i, off := range []bool{false, true} {
			bs, err := badger.New(badger.Config{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			g, err := graphpkg.New(graphpkg.Config{UntimedValidStart: graphpkg.UntimedFromTxFrom, Store: bs, DisableIntegrityHashes: off})
			if err != nil {
				t.Fatal(err)
			}
			rows := isWrite(t, g)
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			raw := rawRelRow(t, bs.DBForTest(), rows.r2)
			size[i] = len(raw)
			var w storeutil.RelWire
			if err := msgpack.Unmarshal(raw, &w); err != nil {
				t.Fatal(err)
			}
			if off && (w.Hash != "" || w.FromNodeHash != "" || w.ToNodeHash != "" || w.PrevHash != "") {
				t.Fatalf("off: rel row on disk carries hashes: %+v", w)
			}
			if !off && (!hex64.MatchString(w.Hash) || !hex64.MatchString(w.FromNodeHash)) {
				t.Fatalf("on: rel row on disk decodes to %+v, want 64-hex hashes", w)
			}
			if !off {
				hexHash := []byte(w.Hash)
				if bytes.Contains(raw, hexHash) {
					t.Fatalf("on: rel row stores its hash as 64 hex characters (%d B row)", len(raw))
				}
			}
			_ = g.Close()
		}
		// Three hashes (h, fnh, tnh): 3 x (32 B + 2 B bin header) + 10 B of keys.
		if d := size[0] - size[1]; d > 3*34+10 {
			t.Fatalf("hash bytes per first-version rel row = %d (on %d B, off %d B), want <= %d", d, size[0], size[1], 3*34+10)
		}
	})

	t.Run("segments_off_refused", func(t *testing.T) {
		_, err := graphpkg.New(graphpkg.Config{UntimedValidStart: graphpkg.UntimedFromTxFrom, DisableIntegrityHashes: true,
			RelSegments: []storepkg.RelSegmentSpec{{Type: "HOP"}}})
		if !errors.Is(err, graphpkg.ErrIntegrityDisabled) {
			t.Fatalf("RelSegments with hashes off: err = %v, want ErrIntegrityDisabled", err)
		}
	})

	// Export of an on-graph imports into an off-graph without its hashes; an
	// export of an off-graph is refused by an on-graph (no hash to verify).
	t.Run("import", func(t *testing.T) {
		ctx := context.Background()
		for _, srcOff := range []bool{false, true} {
			src, err := graphpkg.New(graphpkg.Config{UntimedValidStart: graphpkg.UntimedFromTxFrom, DisableIntegrityHashes: srcOff})
			if err != nil {
				t.Fatal(err)
			}
			rows := isWrite(t, src)
			var buf bytes.Buffer
			if err := src.IO().Export(&buf); err != nil {
				t.Fatal(err)
			}
			_ = src.Close()
			dst, err := graphpkg.New(graphpkg.Config{UntimedValidStart: graphpkg.UntimedFromTxFrom, DisableIntegrityHashes: !srcOff})
			if err != nil {
				t.Fatal(err)
			}
			err = dst.IO().Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{})
			if srcOff {
				if !errors.Is(err, graphpkg.ErrCorruptExport) {
					t.Fatalf("off-export into on-graph: err = %v, want ErrCorruptExport", err)
				}
			} else {
				if err != nil {
					t.Fatalf("on-export into off-graph: %v", err)
				}
				r, err := dst.Rels().Get(ctx, rows.r)
				if err != nil {
					t.Fatal(err)
				}
				if ig := r.Integrity(); ig != nil && (ig.Hash != "" || ig.PrevHash != "" || ig.FromNodeHash != "") {
					t.Fatalf("on-export imported into off-graph keeps hashes: %+v", ig)
				}
			}
			_ = dst.Close()
		}
	})
}
