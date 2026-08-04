package memory

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Memory-side relationship columns. The oracle is the ROW path, exactly as on
// badger: the snapshot must expose what reading each *types.Relationship exposes.

const (
	mrcType  uint16 = 51
	mrcOther uint16 = 52
)

func mrcStore(t *testing.T) *Store {
	t.Helper()
	ms := New()
	t.Cleanup(func() { ms.Close() })
	return ms
}

func mrcRel(t *testing.T, ms *Store, id, start, end int64, tok uint16, weight any, from, to int64) {
	t.Helper()
	for _, nid := range []int64{start, end} {
		if _, err := ms.GetNode(types.NodeID(nid)); err != nil {
			if err := ms.PutNode(types.NewNode(types.NodeID(nid), 50, nil)); err != nil {
				t.Fatalf("PutNode %d: %v", nid, err)
			}
		}
	}
	r := types.NewRelationship(types.RelID(id), tok, types.NodeID(start), types.NodeID(end))
	if weight != nil {
		if err := r.SetProperty("weight", weight); err != nil {
			t.Fatalf("SetProperty: %v", err)
		}
	}
	r.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(from), ValidTo: types.Instant(to)})
	if err := ms.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship %d: %v", id, err)
	}
}

// TestMemRelColumns_MatchTheRowPath is the equivalence oracle.
func TestMemRelColumns_MatchTheRowPath(t *testing.T) {
	ms := mrcStore(t)
	mrcRel(t, ms, 100, 1, 2, mrcType, int64(5), 1000, 0)
	mrcRel(t, ms, 200, 2, 3, mrcType, int64(9), 2000, 3000)
	mrcRel(t, ms, 300, 3, 1, mrcType, nil, 1500, 0) // no weight
	mrcRel(t, ms, 400, 1, 3, mrcOther, int64(7), 500, 0)

	snap, _, ok, err := ms.RelColumnSnapshot(mrcType, []string{"weight"})
	if err != nil {
		t.Fatalf("RelColumnSnapshot: %v", err)
	}
	if !ok {
		t.Fatal("declined for a buildable type")
	}
	start, _ := snap.View(RelStartColumn)
	end, _ := snap.View(RelEndColumn)
	w, _ := snap.View("weight")
	vf := snap.ValidFrom()

	got := map[int64][4]int64{}
	for ord, id := range snap.IDs() {
		weight := int64(-1)
		if w.Present(ord) {
			weight = w.Ints[ord]
		}
		got[int64(id)] = [4]int64{start.Ints[ord], end.Ints[ord], weight, vf[ord]}
	}

	rels, err := ms.RelationshipsByType(mrcType, QueryOpts{})
	if err != nil {
		t.Fatalf("RelationshipsByType: %v", err)
	}
	if len(got) != len(rels) {
		t.Fatalf("row counts differ: columnar=%d row=%d", len(got), len(rels))
	}
	for _, r := range rels {
		weight := int64(-1)
		if v, has := r.GetProperty("weight"); has {
			if iv, isInt := v.(int64); isInt {
				weight = iv
			}
		}
		f, _, _ := r.ValidRange()
		want := [4]int64{int64(r.StartNodeID()), int64(r.EndNodeID()), weight, int64(f)}
		if got[int64(r.ID())] != want {
			t.Errorf("rel %d: columnar %v, row path %v", r.ID(), got[int64(r.ID())], want)
		}
	}
}

// TestMemRelColumns_ExcludeOtherTypes pins membership even though the epoch is
// store-wide.
func TestMemRelColumns_ExcludeOtherTypes(t *testing.T) {
	ms := mrcStore(t)
	mrcRel(t, ms, 100, 1, 2, mrcType, int64(5), 1000, 0)
	mrcRel(t, ms, 400, 1, 3, mrcOther, int64(7), 500, 0)

	snap, _, ok, _ := ms.RelColumnSnapshot(mrcType, nil)
	if !ok {
		t.Fatal("declined")
	}
	for _, id := range snap.IDs() {
		if int64(id) == 400 {
			t.Fatal("a relationship of a DIFFERENT type appeared in the snapshot")
		}
	}
	if n := len(snap.IDs()); n != 1 {
		t.Errorf("snapshot holds %d rels, want 1", n)
	}
}

// TestMemRelColumns_EndpointsAlwaysBuilt pins that endpoints appear even when no
// properties were requested — they are structure, not an optional column.
func TestMemRelColumns_EndpointsAlwaysBuilt(t *testing.T) {
	ms := mrcStore(t)
	mrcRel(t, ms, 100, 1, 2, mrcType, nil, 1000, 0)

	snap, _, ok, _ := ms.RelColumnSnapshot(mrcType, nil)
	if !ok {
		t.Fatal("declined")
	}
	s, hasS := snap.View(RelStartColumn)
	e, hasE := snap.View(RelEndColumn)
	if !hasS || !hasE {
		t.Fatal("endpoint columns missing when no properties were requested")
	}
	if s.Ints[0] != 1 || e.Ints[0] != 2 {
		t.Errorf("endpoints = (%d,%d), want (1,2)", s.Ints[0], e.Ints[0])
	}
}

// TestMemRelColumns_UnsetValidFromResolvesToMintTime is the Pattern-38 probe.
func TestMemRelColumns_UnsetValidFromResolvesToMintTime(t *testing.T) {
	ms := mrcStore(t)
	mrcRel(t, ms, 100, 1, 2, mrcType, int64(5), 0, 0) // ValidFrom unset

	snap, _, ok, _ := ms.RelColumnSnapshot(mrcType, nil)
	if !ok {
		t.Fatal("declined")
	}
	if got := snap.ValidFrom()[0]; got == 0 {
		t.Fatal("unset ValidFrom stored as 0; it must resolve to mint time, or a " +
			"columnar valid-time filter answers differently from the row path")
	}
}

// TestMemRelColumns_StaleSnapshotRebuilds pins the epoch gate.
func TestMemRelColumns_StaleSnapshotRebuilds(t *testing.T) {
	ms := mrcStore(t)
	mrcRel(t, ms, 100, 1, 2, mrcType, int64(5), 1000, 0)
	first, gen1, _, _ := ms.RelColumnSnapshot(mrcType, []string{"weight"})
	if n := len(first.IDs()); n != 1 {
		t.Fatalf("first snapshot holds %d, want 1", n)
	}
	mrcRel(t, ms, 200, 2, 3, mrcType, int64(9), 2000, 0)

	second, gen2, ok, _ := ms.RelColumnSnapshot(mrcType, []string{"weight"})
	if !ok {
		t.Fatal("declined after a write")
	}
	if gen2 == gen1 {
		t.Error("epoch did not advance after an edge write")
	}
	if n := len(second.IDs()); n != 2 {
		t.Errorf("second snapshot holds %d rels, want 2 — stale columns were served", n)
	}
}

// TestMemRelColumns_EmptyAndUnknownTypeDecline pins clean declines.
func TestMemRelColumns_EmptyAndUnknownTypeDecline(t *testing.T) {
	ms := mrcStore(t)
	for _, tok := range []uint16{mrcType, 99} {
		_, _, ok, err := ms.RelColumnSnapshot(tok, nil)
		if err != nil {
			t.Errorf("tok %d errored instead of declining: %v", tok, err)
		}
		if ok {
			t.Errorf("tok %d accepted an empty type", tok)
		}
	}
}
