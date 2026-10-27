package badger

import (
	"encoding/binary"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// OPENIMAGE: the delta open (an image older than the store) against the full
// rebuild, for the sessions the graph-level oracle (TestCleanReopenMatchesFullRebuild)
// cannot write: one that writes history alone (the touched entity is named
// by its history key only) or a row alone (no history), an entity whose keys left the store without a
// newer version (a bottom-level compaction dropped its tombstones: it is not
// touched, only no longer live) and indexes redefined after the image (the
// image's entries are the rows' values; an index it lacks builds from its
// rows, so only that one decodes).
func TestDeltaOpenMatchesFullRebuild(t *testing.T) {
	const lbl, hfLbl, typ = uint16(1), uint16(2), uint16(7)
	node := func(id int64, label uint16, ver uint32, tf, tt types.Instant) *types.Node {
		n := types.NewNode(types.NodeID(snowflake.ID(id)), label, nil)
		_ = n.SetProperty("k", id%4)
		_ = n.SetProperty("emb", []float32{float32(id), 1})
		n.SetVersion(ver)
		n.SetTemporal(stampTM(tf, tt, 0))
		return n
	}
	rel := func(id int64, ver uint32, tf, tt types.Instant) *types.Relationship {
		r := types.NewRelationship(types.RelID(snowflake.ID(id)), typ, types.NodeID(snowflake.ID(id-100)), types.NodeID(snowflake.ID(id-99)))
		_ = r.SetProperty("w", id%3)
		r.SetVersion(ver)
		r.SetTemporal(stampTM(tf, tt, 0))
		return r
	}
	must := func(t *testing.T, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	// base writes the store and closes it cleanly: the image holds it all.
	base := func(t *testing.T, dir string) {
		bs, err := New(Config{Dir: dir})
		must(t, err)
		must(t, bs.CreatePropertyIndex(lbl, "k"))
		must(t, bs.CreateCompositePropertyIndex(lbl, []string{"k", "emb"}))
		must(t, bs.CreateTemporalIndex(lbl))
		must(t, bs.CreateVectorIndex(lbl, "emb", 2, DistanceCosine))
		must(t, bs.CreateHighFrequencyIndex(hfLbl, time.Hour))
		must(t, bs.CreateRelPropertyIndex(typ, "w"))
		must(t, bs.CreateRelTemporalIndex(typ))
		for id := int64(1); id <= 10; id++ {
			must(t, bs.PutNode(node(id, lbl, 1, types.Instant(1000+id), 0)))
			must(t, bs.PutNode(node(100+id, hfLbl, 1, types.Instant(1000+id), 0)))
		}
		for id := int64(101); id <= 108; id++ {
			must(t, bs.PutRelationship(rel(id, 1, types.Instant(2000+id), 0)))
		}
		must(t, bs.Close())
	}
	// crashed runs a session on dir that ends in a crash: no Close, no image.
	crashed := func(t *testing.T, dir string, fn func(bs *Store)) {
		bs, err := New(Config{Dir: dir})
		must(t, err)
		fn(bs)
		must(t, bs.Flush())
		bs.SetDBClosedForTest(true)
		must(t, bs.DBForTest().Close())
	}
	// raw commits fn with plain badger and one key that names no entity, so
	// the image header is no longer the newest version.
	raw := func(t *testing.T, dir string, fn func(db *badgerv4.DB) error) {
		db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
		must(t, err)
		must(t, fn(db))
		must(t, db.Update(func(txn *badgerv4.Txn) error { return txn.Set([]byte("\xfeforeign"), nil) }))
		must(t, db.Close())
	}
	// dropEntity leaves the store as a purge of the entity whose tombstones a
	// bottom-level compaction dropped: its keys gone without a newer version
	// (DropPrefix rewrites the tables; it skips a prefix with no visible key,
	// db.go:1824-1833, so it runs before any delete), its counter lowered.
	dropEntity := func(isNode bool, id int64) func(db *badgerv4.DB) error {
		names := func(k []byte) bool { // the entity's row, history, label / type and adjacency keys
			switch {
			case isNode && (k[0] == storepkg.KeyNode || k[0] == storepkg.KeyHistNode) && len(k) >= 9,
				!isNode && (k[0] == storepkg.KeyRel || k[0] == storepkg.KeyHistRel) && len(k) >= 9:
				return int64(storepkg.ParseIDFromKey(k, 1)) == id
			case isNode && k[0] == storepkg.KeyLabel && len(k) == storepkg.SizeLabelIdx,
				!isNode && k[0] == storepkg.KeyRelType && len(k) == storepkg.SizeRelTypeIdx:
				return int64(storepkg.ParseIDFromKey(k, 3)) == id
			case !isNode && (k[0] == storepkg.KeyOut || k[0] == storepkg.KeyIn) && len(k) == storepkg.SizeAdjacency:
				return int64(storepkg.ParseRelIDFromAdjKey(k)) == id
			}
			return false
		}
		return func(db *badgerv4.DB) error {
			var keys [][]byte
			err := db.View(func(txn *badgerv4.Txn) error {
				opts := badgerv4.DefaultIteratorOptions
				opts.PrefetchValues = false
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					if k := it.Item().Key(); len(k) > 0 && names(k) {
						keys = append(keys, it.Item().KeyCopy(nil))
					}
				}
				return nil
			})
			if err != nil || len(keys) == 0 {
				t.Fatalf("keys of %d: %d, %v", id, len(keys), err)
			}
			if err := db.DropPrefix(keys...); err != nil {
				return err
			}
			counter := counterRelCountKey
			if isNode {
				counter = counterNodeCountKey
			}
			return db.Update(func(txn *badgerv4.Txn) error {
				item, err := txn.Get(counter)
				if err != nil {
					return err
				}
				v, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				binary.BigEndian.PutUint64(v, binary.BigEndian.Uint64(v)-1)
				return txn.Set(counter, v)
			})
		}
	}
	const none, some = "none", "some" // rows the open decodes
	cases := []struct {
		name   string
		after  func(t *testing.T, dir string)
		decode string
	}{
		{"node history alone", func(t *testing.T, dir string) {
			crashed(t, dir, func(bs *Store) {
				must(t, bs.PutNodeVersion(types.NodeID(snowflake.ID(3)), 0, node(3, lbl, 0, 500, 1003)))
			})
		}, some},
		{"rel history alone", func(t *testing.T, dir string) {
			crashed(t, dir, func(bs *Store) { must(t, bs.PutRelVersion(types.RelID(snowflake.ID(103)), 0, rel(103, 0, 600, 2103))) })
		}, some},
		{"node row replaced without history", func(t *testing.T, dir string) {
			n := node(2, lbl, 1, 1002, 0)
			_ = n.SetProperty("k", int64(99))
			crashed(t, dir, func(bs *Store) { must(t, bs.ReplaceNode(n)) })
		}, some},
		{"rel row replaced without history", func(t *testing.T, dir string) {
			r := rel(102, 1, 2102, 0)
			_ = r.SetProperty("w", int64(99))
			crashed(t, dir, func(bs *Store) { must(t, bs.ReplaceRelationship(r)) })
		}, some},
		{"node gone without a newer version", func(t *testing.T, dir string) { raw(t, dir, dropEntity(true, 10)) }, none},
		{"rel gone without a newer version", func(t *testing.T, dir string) { raw(t, dir, dropEntity(false, 105)) }, none},
		{"vector index redefined", func(t *testing.T, dir string) {
			crashed(t, dir, func(bs *Store) {
				must(t, bs.DropVectorIndex(lbl, "emb"))
				must(t, bs.CreateVectorIndex(lbl, "emb", 2, DistanceEuclidean))
			})
		}, none},
		{"temporal index turned high-frequency", func(t *testing.T, dir string) {
			crashed(t, dir, func(bs *Store) {
				must(t, bs.DropTemporalIndex(lbl))
				must(t, bs.CreateHighFrequencyIndex(lbl, time.Hour))
			})
		}, some},
		{"property indexes dropped", func(t *testing.T, dir string) {
			crashed(t, dir, func(bs *Store) {
				must(t, bs.DropPropertyIndex(lbl, "k"))
				must(t, bs.DropRelPropertyIndex(typ, "w"))
				must(t, bs.DropCompositePropertyIndex(lbl, []string{"k", "emb"}))
				must(t, bs.CreateCompositePropertyIndex(lbl, []string{"emb", "k"}))
			})
		}, some},
		{"high-frequency index redefined", func(t *testing.T, dir string) {
			crashed(t, dir, func(bs *Store) {
				must(t, bs.DropHighFrequencyIndex(hfLbl))
				must(t, bs.CreateHighFrequencyIndex(hfLbl, time.Minute))
			})
		}, none},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			base(t, dir)
			tc.after(t, dir)
			oracle := t.TempDir()
			copyDirFiles(t, dir, oracle)
			db, err := badgerv4.Open(badgerv4.DefaultOptions(oracle).WithLogger(nil))
			must(t, err)
			must(t, db.Update(func(txn *badgerv4.Txn) error { return DeleteOpenStateForTest(txn) }))
			must(t, db.Close())
			digest := func(d string) (string, int64) {
				var s *Store
				n := CountRowDecodesForTest(func() { s, err = New(Config{Dir: d, ReadOnly: true}) })
				must(t, err)
				defer s.Close()
				return s.IndexStateDigestForTest(), n
			}
			got, decoded := digest(dir)
			want, rows := digest(oracle)
			if got != want {
				t.Fatalf("open after the image differs from the full rebuild:\n got: %.1500s\nwant: %.1500s", got, want)
			}
			kind := some
			if decoded == 0 {
				kind = none
			}
			if kind != tc.decode || decoded >= rows {
				t.Fatalf("open after the image decoded %d rows (the rebuild %d): %s, want %s", decoded, rows, kind, tc.decode)
			}
		})
	}
}
