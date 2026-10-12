package sharded

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Backlog 54 on sharded: every slot is a badger store that reopens from its
// keys and its clean-close image. With relationships whose endpoints live on
// other slots, each slot's RAM state after a clean reopen must equal a full
// rebuild of the same files.
func TestShardedCleanReopenMatchesFullRebuild(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Dir: dir, BaseSlot: 0, SlotCount: 4}
	st, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	reg := registrypkg.NewPropertyKeyRegistry() // as the graph wires and saves it
	st.SetPropertyKeyRegistry(reg)
	const label, typ = 10, 20
	for slot := uint8(0); slot < 4; slot++ {
		for n := int64(1); n <= 8; n++ {
			node := types.NewNode(mkNodeID(slot, n), label, nil)
			_ = node.SetProperty("k", n)
			if err := st.PutNode(node); err != nil {
				t.Fatal(err)
			}
		}
	}
	var rels []types.RelID
	for slot := uint8(0); slot < 4; slot++ {
		for n := int64(1); n <= 8; n++ {
			// The relationship's own slot, its start's and its end's all differ.
			id := mkRelID((slot+1)%4, n)
			r := types.NewRelationship(id, typ, mkNodeID(slot, n), mkNodeID((slot+2)%4, n))
			_ = r.SetProperty("w", n)
			if err := st.PutRelationship(r); err != nil {
				t.Fatal(err)
			}
			rels = append(rels, id)
		}
	}
	for i := 0; i < len(rels); i += 3 {
		if err := st.DeleteRelationship(rels[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.SavePropertyKeyRegistry(reg); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	oracle := t.TempDir()
	if err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(oracle, rel), 0o750)
		}
		in, err := os.Open(path) // #nosec G304 -- test temp dir
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(filepath.Join(oracle, rel)) // #nosec G304 -- test temp dir
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			return err
		}
		return out.Close()
	}); err != nil {
		t.Fatal(err)
	}
	if err := filepath.WalkDir(oracle, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Name() != "MANIFEST" {
			return err
		}
		db, err := badgerv4.Open(badgerv4.DefaultOptions(filepath.Dir(path)).WithLogger(nil))
		if err != nil {
			return err
		}
		if err := db.Update(func(txn *badgerv4.Txn) error { return txn.Set([]byte("\xfeforeign"), nil) }); err != nil {
			_ = db.Close()
			return err
		}
		return db.Close()
	}); err != nil {
		t.Fatal(err)
	}

	// NDV never decreases (a reopen keeps the sketch, a rebuild restarts it);
	// ordinals come from one allocator the slots share while they open in
	// parallel, so their numbering differs between any two opens.
	ndv := regexp.MustCompile(`NDV:\d+| ord=\d+`)
	digests := func(d string) ([]string, int64) {
		c := cfg
		c.Dir = d
		var s *Store
		var err error
		decoded := badger.CountRowDecodesForTest(func() { s, err = New(c) })
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		var out []string
		for _, shard := range s.shards {
			out = append(out, ndv.ReplaceAllString(shard.IndexStateDigestForTest(), "~"))
		}
		return out, decoded
	}
	got, clean := digests(dir)
	want, rebuild := digests(oracle)
	if clean != 0 || rebuild == 0 {
		t.Errorf("rows decoded by the open: clean %d (want 0), rebuild %d (want > 0)", clean, rebuild)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("slot %d: clean reopen differs from the full rebuild:\n got: %.2000s\nwant: %.2000s", i, got[i], want[i])
		}
	}
}
