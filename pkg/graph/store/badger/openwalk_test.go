package badger

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	badgerv4 "github.com/dgraph-io/badger/v4"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// OPENWALK (SCALE, tasks/evidence/SCALE/): a clean open of a store whose
// indexes are on disk walked every adjacency key twice (KeyOut, KeyIn) and
// kept none of them (the disk mode answers from the keys, and a trusted image
// already vouches for them), and every label key only to count it. With the
// label and adjacency indexes on disk the clean open walks none of these
// keyspaces, and still ends in the full rebuild's index state.
//
// The label counts are trusted with the image's node ID set only: an image
// without one (the node keys are walked) walks the label keys too.
func TestOpenWalkCleanOpenSkipsDiskIndexes(t *testing.T) {
	t.Run("trusted", func(t *testing.T) { testOpenWalk(t, nil, nil) })
	t.Run("without node IDs", func(t *testing.T) {
		testOpenWalk(t, func(kind byte) bool { return kind != imgSecNodeIDs }, []byte{storepkg.KeyLabel})
	})
}

// testOpenWalk writes a store through an image filter, then checks its clean
// open against the full rebuild and that it walks none of the label and
// adjacency keyspaces but mustWalk.
func testOpenWalk(t *testing.T, filter func(kind byte) bool, mustWalk []byte) {
	cfg := func(dir string, readOnly bool) Config {
		return Config{Dir: dir, ReadOnly: readOnly, LabelIndexOnDisk: true, AdjacencyIndexOnDisk: true}
	}
	dir := t.TempDir()
	s, err := New(cfg(dir, false))
	if err != nil {
		t.Fatal(err)
	}
	s.imageSectionFilterForTest = filter
	for i := int64(1); i <= 40; i++ {
		var extra []uint16
		if i%3 == 0 {
			extra = []uint16{2}
		}
		if err := s.PutNode(types.NewNode(types.NodeID(i), 1, extra)); err != nil {
			t.Fatal(err)
		}
	}
	for i := int64(1); i < 30; i++ {
		if err := s.PutRelationship(types.NewRelationship(types.RelID(100+i), uint16(7+i%2), types.NodeID(i), types.NodeID(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.PutNode(types.NewNode(types.NodeID(41), 5, nil)); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{40, 41} { // a label count falls, one to zero
		if err := s.DeleteNode(types.NodeID(id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// The oracle: the same files with the image invalidated, so its open
	// rebuilds every index from the keys.
	oracle := t.TempDir()
	copyDirFiles(t, dir, oracle)
	db, err := badgerv4.Open(badgerv4.DefaultOptions(oracle).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(txn *badgerv4.Txn) error { return txn.Set([]byte("\xfeforeign"), nil) }); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	open := func(d string) (digest string, walked []byte) {
		openWalkTestHook = func(prefix byte) { walked = append(walked, prefix) }
		defer func() { openWalkTestHook = nil }()
		s, err := New(cfg(d, true))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		// The counts as the store reports them, a label without members
		// included (the digest leaves those out).
		_, _, byLabel, byType, err := s.CountSnapshot()
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprint(s.IndexStateDigestForTest(), byLabel, byType), walked
	}
	got, walked := open(dir)
	want, rebuildWalked := open(oracle)
	if got != want {
		t.Fatalf("clean open differs from the full rebuild:\n got: %.900s\nwant: %.900s", got, want)
	}
	if !strings.Contains(got, "labelcount") {
		t.Fatalf("the digest lacks the label counts:\n%.900s", got)
	}
	for _, p := range []byte{storepkg.KeyLabel, storepkg.KeyOut, storepkg.KeyIn} {
		if !slices.Contains(rebuildWalked, p) {
			t.Fatalf("the rebuild walked %x, not keyspace %#x: the hook misses it", rebuildWalked, p)
		}
		if must := slices.Contains(mustWalk, p); slices.Contains(walked, p) != must {
			t.Fatalf("the clean open walked %x: keyspace %#x walked = %v, want %v", walked, p, !must, must)
		}
	}
}
