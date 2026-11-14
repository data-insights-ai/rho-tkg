package tiered

import (
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestColdOpen_TransientHandleBuildsNoIDSets: the open builds the live node set
// (and the rel and rel-type sets) from the open image's sorted runs with one
// insert per ID, which was half the CPU of an open at ai-soc's configuration
// and a fifth of its allocation at 50,000 rows (tasks/evidence/R3/coldread.txt).
// A handle that closes when its read ends (MaxOpenColdShards 0) keeps them in
// the image's encoded form and answers a membership probe from it. Sets built
// by the open, by the shard's handle: none for a transient read-only handle,
// before or after the reads (a by-ID read, a label read, a listing); a handle
// a cap keeps and a writable one build them at the open.
func TestColdOpen_TransientHandleBuildsNoIDSets(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name        string
		maxOpen     int
		wantEncoded bool
	}{
		{"cap 0", 0, true},
		{"cap 2 retains: the sets are built at the open", 2, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			ts, name, n, _ := coldOpenFixture(t, func(cfg *Config) { cfg.MaxOpenColdShards = c.maxOpen })
			store, release := readOnlyColdHandle(t, ts, name)
			defer release()
			if nodes, _, _ := store.IDSetsEncodedForTest(); nodes != c.wantEncoded {
				t.Fatalf("right after the open: the node set is encoded = %v, want %v", nodes, c.wantEncoded)
			}
			if got, err := store.GetNode(n); err != nil || got.ID() != n {
				t.Fatalf("GetNode = %v, %v", got, err)
			}
			if _, err := store.GetNode(n + 1); err == nil {
				t.Fatalf("GetNode of an unknown ID %d found it", n+1)
			}
			if ids, err := store.NodeIDsByLabel(mintSignalTok); err != nil || !slices.Equal(ids, []types.NodeID{n}) {
				t.Fatalf("NodeIDsByLabel = %v, %v, want [%d]", ids, err, n)
			}
			var all []types.NodeID
			if err := store.ForEachNodeID(func(id types.NodeID) bool { all = append(all, id); return true }); err != nil || !slices.Equal(all, []types.NodeID{n}) {
				t.Fatalf("ForEachNodeID = %v, %v, want [%d]", all, err, n)
			}
			if nodes, _, _ := store.IDSetsEncodedForTest(); nodes != c.wantEncoded {
				t.Fatalf("after the reads: the node set is encoded = %v, want %v: a read built it", nodes, c.wantEncoded)
			}
		})
	}
}

// TestColdOpen_WritableHandleBuildsItsIDSets: the writable open a late write
// makes builds the node set (a write adds to it): the encoded form is for a
// handle that only reads and closes.
func TestColdOpen_WritableHandleBuildsItsIDSets(t *testing.T) {
	t.Parallel()
	ts, name, _, late := coldOpenFixture(t, nil)
	if err := ts.PutNode(types.NewNode(late, mintSignalTok, nil)); err != nil {
		t.Fatalf("late write: %v", err)
	}
	store, ro := ts.EventShardsForTest()[name].StoreStateForTest()
	if store == nil || ro {
		t.Fatalf("the late write left store %v read-only %v, want a writable handle", store != nil, ro)
	}
	if nodes, _, _ := store.IDSetsEncodedForTest(); nodes {
		t.Fatal("the writable cold handle holds its node set encoded")
	}
}
