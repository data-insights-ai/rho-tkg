package badger

import (
	"strings"
	"testing"

	badgerv4 "github.com/dgraph-io/badger/v4"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestExistMapCleanOpenMatchesFullRebuild pins step 5 of EXISTMAP: a clean
// open that takes the node and rel ID sets from the open image and, with the
// label index on disk, only counts the label keys, ends in the same index
// state as the full rebuild: ID sets, counters, label counts and epochs, a
// property index the image lacks (rebuilt from the label's keys read on
// demand) and a temporal index restored from the image (filtered by them). Catches a lost or extra ID,
// a wrong label count and a rebuild that misses members.
func TestExistMapCleanOpenMatchesFullRebuild(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := New(Config{Dir: dir, LabelIndexOnDisk: true})
	if err != nil {
		t.Fatal(err)
	}
	// The image leaves the property index out, so the open rebuilds it from
	// the label's members; the temporal index is restored from the image and
	// filtered by them.
	s.imageSectionFilterForTest = func(kind byte) bool { return kind != imgSecNodeProp }
	if err := s.CreatePropertyIndex(1, "p"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTemporalIndex(1); err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 40; i++ {
		var extra []uint16
		if i%3 == 0 {
			extra = []uint16{2}
		}
		n := types.NewNode(types.NodeID(i), 1, extra)
		if err := n.SetProperty("p", i%5); err != nil {
			t.Fatal(err)
		}
		n.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(1000 + i)})
		if err := s.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	for i := int64(1); i < 30; i += 2 { // nodes 31-40 stay unconnected
		if err := s.PutRelationship(types.NewRelationship(types.RelID(100+i), 7, types.NodeID(i), types.NodeID(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.DeleteNode(types.NodeID(40)); err != nil { // a hole in the ID set
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	oracle := t.TempDir()
	copyDirFiles(t, dir, oracle)
	db, err := badgerv4.Open(badgerv4.DefaultOptions(oracle).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(txn *badgerv4.Txn) error { return DeleteOpenStateForTest(txn) }); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	digest := func(d string) string {
		s, err := New(Config{Dir: d, ReadOnly: true, LabelIndexOnDisk: true})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		return s.IndexStateDigestForTest()
	}
	got, want := digest(dir), digest(oracle)
	if got != want {
		t.Fatalf("clean open differs from the full rebuild:\n got: %.900s\nwant: %.900s", got, want)
	}
	if !strings.Contains(got, "propidx 1 p") || !strings.Contains(got, "labelcount") || !strings.Contains(got, "temporal") {
		t.Fatalf("the digest lacks the rebuilt property index or the label counts:\n%.900s", got)
	}
}
