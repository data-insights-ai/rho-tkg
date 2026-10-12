package store_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 50: store.ExAdjacencyCapability on every backend. A delete moves
// both legs of the live row into the ex-adjacency; a history row joins it when
// its endpoints are not the live row's (another life, or no live row), not
// when they are; NoteExAdjacentRel adds both legs; Clear empties it; a nil
// callback, an invalid node or relationship and a closed store fail with their
// sentinels; on badger it survives a reopen.

func exAdjOf(t *testing.T, ex storecontract.ExAdjacencyCapability, n types.NodeID, incoming bool) []types.RelID {
	t.Helper()
	var ids []types.RelID
	if err := ex.ForEachExAdjacentRelID(n, incoming, func(id types.RelID) bool { ids = append(ids, id); return true }); err != nil {
		t.Fatalf("ForEachExAdjacentRelID: %v", err)
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

func exRel(id types.RelID, ver uint32, s, e types.NodeID) *types.Relationship {
	r := types.NewRelationship(id, 5, s, e)
	r.SetVersion(ver)
	return r
}

func TestExAdjacency(t *testing.T) {
	a, b, c := currentNodeID(1), currentNodeID(2), currentNodeID(3)
	deleted, live, absent, noted := currentRelID(10), currentRelID(11), currentRelID(12), currentRelID(13)
	for _, be := range currentBackends() {
		t.Run(be.name, func(t *testing.T) {
			st := be.open(t)
			ex, ok := st.(storecontract.ExAdjacencyCapability)
			if !ok {
				t.Fatalf("%T lacks ExAdjacencyCapability", st)
			}
			for _, n := range []types.NodeID{a, b, c} {
				if err := st.PutNode(types.NewNode(n, 1, nil)); err != nil {
					t.Fatal(err)
				}
			}
			for _, r := range []*types.Relationship{exRel(deleted, 0, a, b), exRel(live, 1, a, b)} {
				if err := st.PutRelationship(r); err != nil {
					t.Fatal(err)
				}
			}
			if err := st.DeleteRelationship(deleted); err != nil {
				t.Fatal(err)
			}
			for _, r := range []*types.Relationship{exRel(live, 0, a, b), exRel(live, 2, c, b), exRel(absent, 0, b, c)} {
				if err := st.PutRelVersion(r.ID(), r.Version(), r); err != nil {
					t.Fatal(err)
				}
			}
			if err := ex.NoteExAdjacentRel(noted, c, a); err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				n        types.NodeID
				incoming bool
				want     []types.RelID
			}{
				{a, false, []types.RelID{deleted}},      // not live (same endpoints)
				{b, true, []types.RelID{deleted, live}}, // live's other life C->B
				{c, false, []types.RelID{live, noted}},  // other life; note
				{b, false, []types.RelID{absent}},       // a history row without a live row
				{c, true, []types.RelID{absent}},        //
				{a, true, []types.RelID{noted}},         //
			} {
				if got := exAdjOf(t, ex, tc.n, tc.incoming); !slices.Equal(got, tc.want) {
					t.Errorf("ex-adjacency(%d, incoming=%v) = %v, want %v", tc.n, tc.incoming, got, tc.want)
				}
			}
			calls := 0
			if err := ex.ForEachExAdjacentRelID(b, true, func(types.RelID) bool { calls++; return false }); err != nil || calls != 1 {
				t.Fatalf("fn false: %d calls, %v; want 1", calls, err)
			}
			for name, err := range map[string]error{
				"nil fn":       ex.ForEachExAdjacentRelID(a, false, nil),
				"invalid node": ex.ForEachExAdjacentRelID(0, false, func(types.RelID) bool { return true }),
				"invalid rel":  ex.NoteExAdjacentRel(-1, a, b),
				"invalid end":  ex.NoteExAdjacentRel(noted, a, 0),
			} {
				if !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
					t.Errorf("%s = %v, want ErrInvalidStoreMutation", name, err)
				}
			}
			if err := st.Clear(); err != nil {
				t.Fatal(err)
			}
			if got := exAdjOf(t, ex, b, true); len(got) != 0 {
				t.Fatalf("after Clear = %v, want empty", got)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			if err := ex.ForEachExAdjacentRelID(a, false, func(types.RelID) bool { return true }); !errors.Is(err, storecontract.ErrStoreClosed) {
				t.Fatalf("closed ForEachExAdjacentRelID = %v, want ErrStoreClosed", err)
			}
			if err := ex.NoteExAdjacentRel(noted, a, b); !errors.Is(err, storecontract.ErrStoreClosed) {
				t.Fatalf("closed NoteExAdjacentRel = %v, want ErrStoreClosed", err)
			}
		})
	}
}

// TestExAdjacencySurvivesReopen: badger keeps the ex-adjacency on disk.
func TestExAdjacencySurvivesReopen(t *testing.T) {
	a, b, rid := currentNodeID(1), currentNodeID(2), currentRelID(10)
	dir := t.TempDir()
	open := func() *badger.Store {
		bs, err := badger.New(badger.Config{Dir: dir, FlushInterval: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		return bs
	}
	bs := open()
	for _, n := range []types.NodeID{a, b} {
		if err := bs.PutNode(types.NewNode(n, 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.PutRelationship(exRel(rid, 0, a, b)); err != nil {
		t.Fatal(err)
	}
	if err := bs.DeleteRelationship(rid); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs = open()
	defer bs.Close()
	if got := exAdjOf(t, bs, a, false); !slices.Equal(got, []types.RelID{rid}) {
		t.Fatalf("after reopen = %v, want [%d]", got, rid)
	}
}

// TestExAdjacencyPurgeAndErasure: a retention purge of node a removes a's
// ex-adjacency and the legs of the relationships purged with it (they were
// purged, not deleted) but keeps a deleted relationship's leg at a survivor; an
// exact erasure of a and its relationships leaves no ex-adjacency entry naming
// them anywhere.
func TestExAdjacencyPurgeAndErasure(t *testing.T) {
	a, b, c := currentNodeID(1), currentNodeID(2), currentNodeID(3)
	gone, purged := currentRelID(10), currentRelID(11)
	setup := func(t *testing.T, st storecontract.Store) storecontract.ExAdjacencyCapability {
		t.Helper()
		for _, n := range []*types.Node{types.NewNode(a, 7, nil), types.NewNode(b, 1, nil), types.NewNode(c, 1, nil)} {
			if err := st.PutNode(n); err != nil {
				t.Fatal(err)
			}
		}
		for _, r := range []*types.Relationship{exRel(gone, 0, a, b), exRel(purged, 0, c, a)} {
			if err := st.PutRelationship(r); err != nil {
				t.Fatal(err)
			}
		}
		if err := st.DeleteRelationship(gone); err != nil {
			t.Fatal(err)
		}
		return st.(storecontract.ExAdjacencyCapability)
	}
	for _, be := range currentBackends() {
		t.Run(be.name+"/purge", func(t *testing.T) {
			st := be.open(t)
			p, ok := st.(storecontract.RetentionPurgeCapability)
			if !ok {
				t.Skipf("%T has no retention purge", st)
			}
			ex := setup(t, st)
			if _, err := p.PurgeNodesByLabelBefore(7, types.Instant(1<<62), 0); err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				n        types.NodeID
				incoming bool
				want     []types.RelID
			}{{a, false, nil}, {a, true, nil}, {c, false, nil}, {b, true, []types.RelID{gone}}} {
				if got := exAdjOf(t, ex, tc.n, tc.incoming); !slices.Equal(got, tc.want) {
					t.Errorf("after purge ex-adjacency(%d, %v) = %v, want %v", tc.n, tc.incoming, got, tc.want)
				}
			}
		})
		t.Run(be.name+"/erasure", func(t *testing.T) {
			st := be.open(t)
			e, ok := st.(storecontract.ExactErasureCapability)
			if !ok {
				t.Skipf("%T has no exact erasure", st)
			}
			ex := setup(t, st)
			if _, err := e.ExactErase(storecontract.ExactErasureRequest{
				NodeIDs: []types.NodeID{a},
				RelIDs:  []types.RelID{gone, purged},
				Bounds:  storecontract.ExactErasureBounds{MaxRelationshipIdentities: 10, MaxRelationshipVersions: 10, MaxEndpointNodeIdentities: 10},
			}); err != nil {
				t.Fatal(err)
			}
			for _, n := range []types.NodeID{a, b, c} {
				for _, incoming := range []bool{false, true} {
					if got := exAdjOf(t, ex, n, incoming); len(got) != 0 {
						t.Errorf("after erasure ex-adjacency(%d, %v) = %v, want empty", n, incoming, got)
					}
				}
			}
		})
	}
}
