package badger

import (
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// rawHasKey reports whether the closed store directory dir holds key.
func rawHasKey(t *testing.T, dir string, key []byte) bool {
	t.Helper()
	db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatalf("raw badger open: %v", err)
	}
	defer db.Close()
	found := false
	if err := db.View(func(txn *badgerv4.Txn) error {
		_, err := txn.Get(key)
		if err == nil {
			found = true
			return nil
		}
		if err == badgerv4.ErrKeyNotFound {
			return nil
		}
		return err
	}); err != nil {
		t.Fatalf("raw badger view: %v", err)
	}
	return found
}

// TestLoadIndexes_MintInKeyRepairedAfterSplitFlush (row 97): a crash that
// splits a flush can land a rel row without its in-key. For a co-located rel
// whose both endpoints live on another shard (a tiered mint shard,
// RelsByMint) the dirty open re-creates the in-key and the leg, in map and
// disk mode, as it does for the out-key. Break cases: the out-key missing,
// both keys missing, plain badger (local endpoints) unchanged, and a legacy
// split row (local start, remote end: its in-key lives on the end's shard)
// gets no in-key invented.
func TestLoadIndexes_MintInKeyRepairedAfterSplitFlush(t *testing.T) {
	const relType = 7
	cases := []struct {
		name       string
		local      bool // a and b stored here (plain badger)
		split      bool // entity + out only: start local, end remote
		dropIn     bool
		dropOut    bool
		wantInLeg  bool
		wantOutLeg bool
	}{
		{name: "mint in-key missing", dropIn: true, wantInLeg: true, wantOutLeg: true},
		{name: "mint out-key missing", dropOut: true, wantInLeg: true, wantOutLeg: true},
		{name: "mint both missing", dropIn: true, dropOut: true, wantInLeg: true, wantOutLeg: true},
		{name: "plain in-key missing", local: true, dropIn: true, wantInLeg: true, wantOutLeg: true},
		{name: "legacy split no in-key", split: true, wantInLeg: false, wantOutLeg: true},
	}
	for _, disk := range []bool{false, true} {
		mode := "map"
		if disk {
			mode = "disk"
		}
		for _, c := range cases {
			t.Run(mode+"/"+c.name, func(t *testing.T) {
				dir := t.TempDir()
				cfg := Config{Dir: dir, AdjacencyIndexOnDisk: disk}
				bs, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				gen, relGen := newTestGen(t, 0), newTestGen(t, 1)
				a, b, rid := gen.Generate(), gen.Generate(), relGen.Generate()
				r := types.NewRelationship(types.RelID(rid), relType, types.NodeID(a), types.NodeID(b))
				switch {
				case c.local:
					putTestNode(t, bs, int64(a), 1, nil)
					putTestNode(t, bs, int64(b), 1, nil)
					err = bs.PutRelationship(r)
				case c.split:
					putTestNode(t, bs, int64(a), 1, nil)
					err = bs.PutRelEntityAndOut(r)
				default:
					err = bs.PutRelationshipCoLocated(r)
				}
				if err != nil {
					t.Fatalf("put: %v", err)
				}
				if err := bs.Close(); err != nil {
					t.Fatal(err)
				}
				inKey, outKey := storepkg.InKey(b, relType, a, rid), storepkg.OutKey(a, relType, b, rid)
				updateRawBadgerDir(t, dir, func(txn *badgerv4.Txn) error {
					if c.dropIn {
						if err := txn.Delete(inKey); err != nil {
							return err
						}
					}
					if c.dropOut {
						if err := txn.Delete(outKey); err != nil {
							return err
						}
					}
					return DeleteOpenStateForTest(txn)
				})

				check := func(phase string, s *Store) {
					t.Helper()
					var wantIn, wantOut []snowflake.ID
					if c.wantInLeg {
						wantIn = []snowflake.ID{rid}
					}
					if c.wantOutLeg {
						wantOut = []snowflake.ID{rid}
					}
					if got := s.IncomingRelIDs(b, 0); !slices.Equal(got, wantIn) {
						t.Errorf("%s: IncomingRelIDs(b) = %v, want %v", phase, got, wantIn)
					}
					if got := s.OutgoingRelIDsOfType(a, 0); !slices.Equal(got, wantOut) {
						t.Errorf("%s: OutgoingRelIDsOfType(a) = %v, want %v", phase, got, wantOut)
					}
				}
				var s *Store
				decoded := CountRowDecodesForTest(func() {
					if s, err = New(cfg); err != nil {
						t.Fatalf("dirty open: %v", err)
					}
				})
				if decoded == 0 {
					t.Fatal("open after a raw edit trusted the image; the rebuild was not exercised")
				}
				check("dirty open", s)
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				if got := rawHasKey(t, dir, inKey); got != c.wantInLeg {
					t.Errorf("in-key on disk after the dirty open = %v, want %v", got, c.wantInLeg)
				}
				if !rawHasKey(t, dir, outKey) {
					t.Error("out-key missing on disk after the dirty open")
				}
				if s, err = New(cfg); err != nil {
					t.Fatalf("clean reopen: %v", err)
				}
				defer s.Close()
				check("clean reopen", s)
			})
		}
	}
}
