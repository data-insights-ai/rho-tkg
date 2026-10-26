package badger

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
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
// already vouches for them), every label key only to count it, and every
// rel-type key to rebuild the RAM rel-type index. With the label and
// adjacency indexes on disk the clean open walks none of these keyspaces, and
// still ends in the full rebuild's index state.
//
// The rel-type index (an ID set per type) comes from the image too. Both are
// trusted with the image's ID sets only: an image without one (the entity
// keys are walked) walks the label and rel-type keys too.
func TestOpenWalkCleanOpenSkipsDiskIndexes(t *testing.T) {
	t.Run("trusted", func(t *testing.T) { testOpenWalk(t, nil, nil) })
	t.Run("without node IDs", func(t *testing.T) {
		testOpenWalk(t, func(kind byte) bool { return kind != imgSecNodeIDs }, []byte{storepkg.KeyLabel, storepkg.KeyRelType})
	})
	t.Run("without rel IDs", func(t *testing.T) {
		testOpenWalk(t, func(kind byte) bool { return kind != imgSecRelIDs }, []byte{storepkg.KeyLabel, storepkg.KeyRelType})
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
	// The oracle: the same files with the open state deleted, so its open
	// rebuilds every index from the keys.
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
	for _, p := range []byte{storepkg.KeyLabel, storepkg.KeyRelType, storepkg.KeyOut, storepkg.KeyIn} {
		if !slices.Contains(rebuildWalked, p) {
			t.Fatalf("the rebuild walked %x, not keyspace %#x: the hook misses it", rebuildWalked, p)
		}
		if must := slices.Contains(mustWalk, p); slices.Contains(walked, p) != must {
			t.Fatalf("the clean open walked %x: keyspace %#x walked = %v, want %v", walked, p, !must, must)
		}
	}
}

// openWalkHeapAfterReopen builds a disk store of 1,000 nodes and n rels of two
// types between them, closes it cleanly and returns the live heap the
// reopened store holds (as existMapHeapAfterReopen, ai-soc's shard config).
func openWalkHeapAfterReopen(t *testing.T, n int) uint64 {
	t.Helper()
	cfg := Config{Dir: t.TempDir(), BlockCacheSize: 1 << 20, IndexCacheSize: 1 << 20, MemTableSize: 8 << 20,
		LabelIndexOnDisk: true, AdjacencyIndexOnDisk: true, DisableOrdinals: true}
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	const nodes = 1_000
	batch := make([]*types.Node, 0, nodes)
	for i := range nodes {
		batch = append(batch, types.NewNode(types.NodeID(existMapBase+int64(i)), 1, nil))
	}
	if err := bs.PutNodesBatch(batch); err != nil {
		t.Fatal(err)
	}
	rels := make([]*types.Relationship, 0, 1000)
	for i := range n {
		start, end := existMapBase+int64(i%nodes), existMapBase+int64((i+1)%nodes)
		rels = append(rels, types.NewRelationship(types.RelID(existMapBase+nodes+int64(i)), uint16(7+i%2), types.NodeID(start), types.NodeID(end)))
		if len(rels) == cap(rels) || i == n-1 {
			if err := bs.PutRelationshipsBatch(rels); err != nil {
				t.Fatal(err)
			}
			rels = rels[:0]
		}
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)
	before := m.HeapAlloc
	bs, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	runtime.GC()
	runtime.ReadMemStats(&m)
	if m.HeapAlloc < before {
		return 0
	}
	return m.HeapAlloc - before
}

const openWalkChildEnv = "RHO_OPENWALK_CHILD"

// TestOpenWalkResidentBytesPerRel pins OPENWALK's RAM budget (SCALE, lesson
// 89): an open shard keeps at most 24 B of live heap per stored rel (the rel
// ID set and the rel-type index as ID sets; v4.86.1: ~45 B, the type index a
// map[RelID]struct{} per type). Measured as TestExistMapResidentBytesPerNode:
// the slope between two store sizes after a clean reopen, in a fresh process.
func TestOpenWalkResidentBytesPerRel(t *testing.T) {
	if os.Getenv(openWalkChildEnv) == "" {
		if testing.Short() && !raceEnabled {
			t.Skip("builds and reopens two disk stores of 50 k and 250 k rels")
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestOpenWalkResidentBytesPerRel$", "-test.count=1", "-test.v") // #nosec G204 -- the test binary itself
		cmd.Env = append(os.Environ(), openWalkChildEnv+"=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("measuring child: %v\n%s", err, out)
		}
		t.Logf("%s", out)
		return
	}
	const small, large = 50_000, 250_000
	hs := openWalkHeapAfterReopen(t, small)
	hl := openWalkHeapAfterReopen(t, large)
	perRel := (float64(hl) - float64(hs)) / (large - small)
	t.Logf("reopened store: %d B at %d rels, %d B at %d rels: %.1f B per rel", hs, small, hl, large, perRel)
	budget := 24.0
	if raceEnabled {
		budget = 60
	}
	if perRel > budget {
		t.Fatalf("an open store keeps %.1f B of heap per rel, budget %.0f B", perRel, budget)
	}
}
