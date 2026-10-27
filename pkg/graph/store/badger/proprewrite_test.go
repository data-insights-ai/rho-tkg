package badger

import (
	"slices"
	"testing"

	badgerv4 "github.com/dgraph-io/badger/v4"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// PROPREWRITE (SCALE, sigma's write-back: a state unit's new version keeps
// its indexed ov_k and ov_h values). An update deleted and set every on-disk
// property entry of the node again, so each update added a version of an
// unchanged entry, which every lookup of the value visits until compaction
// drops it. An update now writes only the entries whose value changed.

const prwID = existMapBase

// prwNode is node prwID with labels, k = "v" and a changing n.
func prwNode(n int64, version uint32, labels ...uint16) *types.Node {
	nd := types.NewNode(types.NodeID(prwID), labels[0], nil)
	for _, l := range labels[1:] {
		nd.AddLabelTokenRaw(l)
	}
	_ = nd.SetProperty("k", "v")
	_ = nd.SetProperty("n", n)
	nd.SetVersion(version)
	return nd
}

// prwVersions returns how many versions badger holds of the node's entry
// for k = "v".
func prwVersions(t *testing.T, bs *Store) int {
	t.Helper()
	tok, ok := bs.propKeyTokenFor("k")
	if !ok {
		t.Fatal("no token for k")
	}
	payload, _ := storepkg.PropertyIndexValueBytes(types.IndexablePropertyValueKey("v"))
	key := storepkg.PropertyIndexEntryKey(tok, payload, types.NodeID(prwID).SnowflakeID())
	var n int
	if err := bs.DBForTest().View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.AllVersions = true
		opts.Prefix = key
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if string(it.Item().Key()) == string(key) {
				n++
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

// prwCandidates returns the on-disk candidates for k = v.
func prwCandidates(t *testing.T, bs *Store, v string) []int64 {
	t.Helper()
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	nids, err := bs.propertyIndexDiskKeysCandidatesLocked("k", []string{types.IndexablePropertyValueKey(v)})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, 0, len(nids))
	for _, id := range nids {
		ids = append(ids, int64(id))
	}
	slices.Sort(ids)
	return ids
}

// TestPropRewriteUnchangedValueWritesNoEntry: 20 updates through each update
// door, each flushed on its own, keep k's value; badger then holds one version
// of its entry, the one the create wrote, and the lookup finds the node.
func TestPropRewriteUnchangedValueWritesNoEntry(t *testing.T) {
	doors := map[string]func(bs *Store, i int64) error{
		"ReplaceNode": func(bs *Store, i int64) error { return bs.ReplaceNode(prwNode(i, 0, 1)) },
		"ReplaceNodeWithHistory": func(bs *Store, i int64) error {
			return bs.ReplaceNodeWithHistory(prwNode(i, uint32(i), 1), uint32(i-1), prwNode(i-1, uint32(i-1), 1))
		},
		// label 2 also indexes k, so a label change deletes and sets the entry
		"Add/RemoveNodeLabelToken": func(bs *Store, i int64) error {
			if i%2 == 1 {
				return bs.AddNodeLabelToken(types.NodeID(prwID), 2, prwNode(0, 0, 1, 2))
			}
			return bs.RemoveNodeLabelToken(types.NodeID(prwID), 2, prwNode(0, 0, 1))
		},
		"Add/RemoveNodeLabelTokenWithHistory": func(bs *Store, i int64) error {
			if i%2 == 1 {
				return bs.AddNodeLabelTokenWithHistory(types.NodeID(prwID), 2, prwNode(0, uint32(i), 1, 2), uint32(i-1), prwNode(0, uint32(i-1), 1))
			}
			return bs.RemoveNodeLabelTokenWithHistory(types.NodeID(prwID), 2, prwNode(0, uint32(i), 1), uint32(i-1), prwNode(0, uint32(i-1), 1, 2))
		},
	}
	for name, door := range doors {
		t.Run(name, func(t *testing.T) {
			bs := propSeekStore(t, 0)
			if err := bs.CreatePropertyIndex(2, "k"); err != nil {
				t.Fatal(err)
			}
			if err := bs.PutNode(prwNode(0, 0, 1)); err != nil {
				t.Fatal(err)
			}
			for i := int64(1); i <= 20; i++ {
				if err := door(bs, i); err != nil {
					t.Fatalf("update %d: %v", i, err)
				}
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			if got := prwVersions(t, bs); got != 1 {
				t.Fatalf("badger holds %d versions of the unchanged entry after 20 updates, want 1", got)
			}
			if got := prwCandidates(t, bs, "v"); !slices.Equal(got, []int64{prwID}) {
				t.Fatalf("candidates %v, want [%d]", got, prwID)
			}
		})
	}
}

// TestPropRewriteDuringIndexBuildKeepsEntry: an update with an unchanged
// value while the index is being created still writes the entry, since the
// build's backfill skips the node the write marked.
func TestPropRewriteDuringIndexBuildKeepsEntry(t *testing.T) {
	bs := propSeekStore(t, 0)
	if err := bs.PutNode(prwNode(0, 0, 3)); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	propertyCreatePhase2Hook = func() {
		if err := bs.ReplaceNode(prwNode(1, 0, 3)); err != nil {
			t.Error(err)
		}
	}
	defer func() { propertyCreatePhase2Hook = nil }()
	if err := bs.CreatePropertyIndex(3, "k"); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := prwCandidates(t, bs, "v"); !slices.Equal(got, []int64{prwID}) {
		t.Fatalf("candidates %v after an update during the build, want [%d]", got, prwID)
	}
}

// TestPropRewriteChangedValueMovesEntry: a changed value still deletes the old
// entry and writes the new one.
func TestPropRewriteChangedValueMovesEntry(t *testing.T) {
	bs := propSeekStore(t, 0)
	if err := bs.PutNode(prwNode(0, 0, 1)); err != nil {
		t.Fatal(err)
	}
	nd := prwNode(1, 0, 1)
	_ = nd.SetProperty("k", "w")
	if err := bs.ReplaceNode(nd); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := prwCandidates(t, bs, "v"); len(got) != 0 {
		t.Fatalf("old value still has candidates %v", got)
	}
	if got := prwCandidates(t, bs, "w"); !slices.Equal(got, []int64{prwID}) {
		t.Fatalf("new value: candidates %v, want [%d]", got, prwID)
	}
}
