package graph_test

import (
	"context"
	"encoding/binary"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// A crash inside a multi-transaction flush can leave index keys that disagree
// with the rows (a stale key without its row change, or a row without its
// key). A full rebuild reads the rows and ignores such keys; a clean reopen
// trusts every key. So the open that rebuilds must repair the keys it skips
// or misses, or the next clean Close would freeze them into trusted state
// (Fable review of v4.51.3, probe B). Labels, types, outgoing and incoming
// adjacency; a read-only rebuild repairs nothing and writes no image.
func TestCleanReopenAfterStaleIndexKeys(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	ctx := context.Background()
	type fixture struct {
		n1, n2, rel            int64 // raw IDs: n1 (label A) -[T]-> n2 (label B)
		tokA, tokB, tokT, tokU uint16
	}
	build := func(t *testing.T, dir string) fixture {
		g, _ := openReopenGraph(t, dir, reopenCfg{})
		n1, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"k": "n1"})
		if err != nil {
			t.Fatal(err)
		}
		n2, err := g.Nodes().Add(ctx, []string{"B"}, map[string]any{"k": "n2"})
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels().Add(ctx, "T", n1, n2, map[string]any{"w": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels().Add(ctx, "U", n2, n1, nil); err != nil { // a second type token
			t.Fatal(err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		f := fixture{n1: int64(n1.ID().SnowflakeID()), n2: int64(n2.ID().SnowflakeID()), rel: int64(r.ID().SnowflakeID())}
		rawWrite(t, dir, func(txn *badgerv4.Txn) error { // read the tokens off the keys
			it := txn.NewIterator(badgerv4.IteratorOptions{})
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				k := it.Item().Key()
				switch {
				case len(k) == storeutil.SizeLabelIdx && k[0] == storeutil.KeyLabel:
					tok, id := binary.BigEndian.Uint16(k[1:3]), int64(storeutil.ParseIDFromKey(k, 3))
					switch id {
					case f.n1:
						f.tokA = tok
					case f.n2:
						f.tokB = tok
					}
				case len(k) == storeutil.SizeRelTypeIdx && k[0] == storeutil.KeyRelType:
					tok, id := binary.BigEndian.Uint16(k[1:3]), int64(storeutil.ParseIDFromKey(k, 3))
					if id == f.rel {
						f.tokT = tok
					} else {
						f.tokU = tok
					}
				}
			}
			return nil
		})
		if f.tokA == 0 || f.tokB == 0 || f.tokT == 0 || f.tokU == 0 {
			t.Fatalf("tokens not found: %+v", f)
		}
		return f
	}
	const ghost = int64(1) << 40 // an ID with no row
	set := func(k []byte) func(*badgerv4.Txn) error {
		return func(txn *badgerv4.Txn) error { return txn.Set(k, nil) }
	}
	del := func(k []byte) func(*badgerv4.Txn) error {
		return func(txn *badgerv4.Txn) error { return txn.Delete(k) }
	}
	sid := storeutilID
	cases := []struct {
		name string
		edit func(f fixture) func(*badgerv4.Txn) error
	}{
		{"label key the row lacks", func(f fixture) func(*badgerv4.Txn) error { return set(storeutil.LabelIndexKey(f.tokB, sid(f.n1))) }},
		{"label key without a row", func(f fixture) func(*badgerv4.Txn) error { return set(storeutil.LabelIndexKey(f.tokB, sid(ghost))) }},
		{"label key missing", func(f fixture) func(*badgerv4.Txn) error { return del(storeutil.LabelIndexKey(f.tokB, sid(f.n2))) }},
		{"type key of another type", func(f fixture) func(*badgerv4.Txn) error {
			return set(storeutil.RelTypeIndexKey(f.tokU, sid(f.rel)))
		}},
		{"type key without a row", func(f fixture) func(*badgerv4.Txn) error { return set(storeutil.RelTypeIndexKey(f.tokT, sid(ghost))) }},
		{"type key missing", func(f fixture) func(*badgerv4.Txn) error { return del(storeutil.RelTypeIndexKey(f.tokT, sid(f.rel))) }},
		{"out key with other endpoints", func(f fixture) func(*badgerv4.Txn) error {
			return set(storeutil.OutKey(sid(f.n1), f.tokT, sid(f.n1), sid(f.rel)))
		}},
		{"out key without a row", func(f fixture) func(*badgerv4.Txn) error {
			return set(storeutil.OutKey(sid(f.n1), f.tokT, sid(f.n2), sid(ghost)))
		}},
		{"out key missing", func(f fixture) func(*badgerv4.Txn) error {
			return del(storeutil.OutKey(sid(f.n1), f.tokT, sid(f.n2), sid(f.rel)))
		}},
		{"in key with other endpoints", func(f fixture) func(*badgerv4.Txn) error {
			return set(storeutil.InKey(sid(f.n2), f.tokT, sid(f.n2), sid(f.rel)))
		}},
		{"in key missing", func(f fixture) func(*badgerv4.Txn) error {
			return del(storeutil.InKey(sid(f.n2), f.tokT, sid(f.n1), sid(f.rel)))
		}},
	}
	for _, tc := range cases {
		for _, readOnlyFirst := range []bool{false, true} {
			name := tc.name
			if readOnlyFirst {
				name += ", read-only rebuild first"
			}
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				f := build(t, dir)
				rawWrite(t, dir, tc.edit(f)) // the crash leftover; also a foreign commit: the next open rebuilds
				if readOnlyFirst {
					// Rebuilds in RAM, may not repair, writes no image.
					_, ro := openReopenGraph(t, dir, reopenCfg{readOnly: true})
					if err := ro.Close(); err != nil {
						t.Fatal(err)
					}
				}
				g, _ := openReopenGraph(t, dir, reopenCfg{}) // the rebuilding open
				if err := g.Close(); err != nil {            // a clean Close: image written
					t.Fatal(err)
				}
				want := withoutNDV(fullRebuildDigest(t, dir, reopenCfg{}))
				got, _ := reopenDigest(t, dir, reopenCfg{}, func(g *graphpkg.Graph) {
					if n, err := g.Nodes().CountByLabel("B"); err != nil || n != 1 {
						t.Errorf("CountByLabel(B) = %d, %v; want 1", n, err)
					}
					if rels, err := g.Rels().ByType("T", storepkg.QueryOpts{}); err != nil || len(rels) != 1 {
						t.Errorf("ByType(T) = %d, %v; want 1", len(rels), err)
					}
				})
				if got = withoutNDV(got); got != want {
					t.Fatalf("clean reopen after the repairing open differs from the full rebuild:\n%s", firstDiff(got, want))
				}
			})
		}
	}
}

func storeutilID(v int64) snowflake.ID { return snowflake.ID(v) }
