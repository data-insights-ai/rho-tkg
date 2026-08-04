package badger

import (
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Relationship columns. The oracle is the ROW path: a columnar snapshot must expose
// exactly what reading each *types.Relationship exposes — endpoints, properties,
// presence and validity. The two read entirely different storage, so only a
// value-level comparison can catch a disagreement.

const (
	rcType  uint16 = 31
	rcOther uint16 = 32
)

func rcStore(t *testing.T) *Store {
	t.Helper()
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { bs.Close() })
	return bs
}

// rcRel inserts a relationship of typeTok from start->end with an optional weight.
func rcRel(t *testing.T, bs *Store, id, start, end int64, typeTok uint16, weight any, from, to int64) {
	t.Helper()
	// Endpoints must exist for the store to accept the edge.
	for _, nid := range []int64{start, end} {
		if _, err := bs.GetNode(types.NodeID(nid)); err != nil {
			n := types.NewNode(types.NodeID(nid), 30, nil)
			if err := bs.PutNode(n); err != nil {
				t.Fatalf("PutNode %d: %v", nid, err)
			}
		}
	}
	r := types.NewRelationship(types.RelID(id), typeTok, types.NodeID(start), types.NodeID(end))
	if weight != nil {
		if err := r.SetProperty("weight", weight); err != nil {
			t.Fatalf("SetProperty: %v", err)
		}
	}
	r.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(from), ValidTo: types.Instant(to)})
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship %d: %v", id, err)
	}
}

// rcRowTruth reads the same facts the columnar snapshot should expose, via the row
// path — the oracle.
func rcRowTruth(t *testing.T, bs *Store, typeTok uint16) map[int64][4]int64 {
	t.Helper()
	out := map[int64][4]int64{}
	rels, err := bs.RelationshipsByType(typeTok, QueryOpts{})
	if err != nil {
		t.Fatalf("RelationshipsByType: %v", err)
	}
	for _, r := range rels {
		var w int64 = -1
		if v, ok := r.GetProperty("weight"); ok {
			if iv, isInt := v.(int64); isInt {
				w = iv
			}
		}
		f, _, _ := r.ValidRange()
		out[int64(r.ID())] = [4]int64{int64(r.StartNodeID()), int64(r.EndNodeID()), w, int64(f)}
	}
	return out
}

// TestRelColumns_MatchTheRowPath is the equivalence oracle across endpoints,
// properties, absence and validity.
func TestRelColumns_MatchTheRowPath(t *testing.T) {
	bs := rcStore(t)
	rcRel(t, bs, 100, 1, 2, rcType, int64(5), 1000, 0)
	rcRel(t, bs, 200, 2, 3, rcType, int64(9), 2000, 3000)
	rcRel(t, bs, 300, 3, 1, rcType, nil, 1500, 0) // no weight
	rcRel(t, bs, 400, 1, 3, rcOther, int64(7), 500, 0)

	snap, _, ok, err := bs.RelColumnSnapshot(rcType, []string{"weight"})
	if err != nil {
		t.Fatalf("RelColumnSnapshot: %v", err)
	}
	if !ok {
		t.Fatal("RelColumnSnapshot declined for a buildable type")
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

	want := rcRowTruth(t, bs, rcType)
	if len(got) != len(want) {
		t.Fatalf("row counts differ: columnar=%d row=%d (got=%v)", len(got), len(want), got)
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("rel %d: columnar %v, row path %v", id, got[id], w)
		}
	}
}

// TestRelColumns_ExcludeOtherTypes pins membership: a snapshot for one rel type must
// contain only that type's edges, even though relEpoch is global.
func TestRelColumns_ExcludeOtherTypes(t *testing.T) {
	bs := rcStore(t)
	rcRel(t, bs, 100, 1, 2, rcType, int64(5), 1000, 0)
	rcRel(t, bs, 400, 1, 3, rcOther, int64(7), 500, 0)

	snap, _, ok, _ := bs.RelColumnSnapshot(rcType, nil)
	if !ok {
		t.Fatal("declined")
	}
	for _, id := range snap.IDs() {
		if int64(id) == 400 {
			t.Fatal("a relationship of a DIFFERENT type appeared in the snapshot; " +
				"membership must come from typeIdx, not from the global epoch")
		}
	}
	if len(snap.IDs()) != 1 {
		t.Errorf("snapshot holds %d rels, want 1", len(snap.IDs()))
	}
}

// TestRelColumns_EndpointColumnsAlwaysBuilt pins that endpoints are present even
// when the caller requested no properties at all — they are structure, not an
// optional column, and a traversal consumer always needs them.
func TestRelColumns_EndpointColumnsAlwaysBuilt(t *testing.T) {
	bs := rcStore(t)
	rcRel(t, bs, 100, 1, 2, rcType, nil, 1000, 0)

	snap, _, ok, _ := bs.RelColumnSnapshot(rcType, nil) // no properties requested
	if !ok {
		t.Fatal("declined")
	}
	for _, key := range []string{RelStartColumn, RelEndColumn} {
		v, has := snap.View(key)
		if !has {
			t.Fatalf("%s column missing when no properties were requested", key)
		}
		if !v.Present(0) {
			t.Errorf("%s absent for ordinal 0", key)
		}
	}
	s, _ := snap.View(RelStartColumn)
	e, _ := snap.View(RelEndColumn)
	if s.Ints[0] != 1 || e.Ints[0] != 2 {
		t.Errorf("endpoints = (%d,%d), want (1,2)", s.Ints[0], e.Ints[0])
	}
}

// TestRelColumns_UnsetValidFromResolvesToMintTime is the Pattern-38 probe on the rel
// side. A relationship with no explicit ValidFrom is valid from its MINT time, not
// from the epoch; storing the raw 0 would make a columnar reader disagree with every
// row-path valid-time filter on exactly those edges.
func TestRelColumns_UnsetValidFromResolvesToMintTime(t *testing.T) {
	bs := rcStore(t)
	rcRel(t, bs, 100, 1, 2, rcType, int64(5), 0, 0) // ValidFrom deliberately unset

	snap, _, ok, _ := bs.RelColumnSnapshot(rcType, nil)
	if !ok {
		t.Fatal("declined")
	}
	if got := snap.ValidFrom()[0]; got == 0 {
		t.Fatal("unset ValidFrom stored as 0; it must resolve to the relationship's " +
			"mint time, or a columnar valid-time filter answers differently from the row path")
	}
}

// TestRelColumns_EmptyAndUnknownTypeDecline pins that an empty or unknown type is a
// clean decline, not an error and not an empty-but-usable snapshot.
func TestRelColumns_EmptyAndUnknownTypeDecline(t *testing.T) {
	bs := rcStore(t)
	for _, tok := range []uint16{rcType, 99} {
		t.Run(fmt.Sprintf("tok%d", tok), func(t *testing.T) {
			_, _, ok, err := bs.RelColumnSnapshot(tok, nil)
			if err != nil {
				t.Errorf("errored instead of declining: %v", err)
			}
			if ok {
				t.Error("accepted an empty type; the caller must fall back to the row path")
			}
		})
	}
}

// TestRelColumns_StaleSnapshotRebuilds pins the epoch gate: an edge write must make
// the next snapshot reflect it.
func TestRelColumns_StaleSnapshotRebuilds(t *testing.T) {
	bs := rcStore(t)
	rcRel(t, bs, 100, 1, 2, rcType, int64(5), 1000, 0)
	first, gen1, _, _ := bs.RelColumnSnapshot(rcType, []string{"weight"})
	if n := len(first.IDs()); n != 1 {
		t.Fatalf("first snapshot holds %d, want 1", n)
	}

	rcRel(t, bs, 200, 2, 3, rcType, int64(9), 2000, 0)

	second, gen2, ok, _ := bs.RelColumnSnapshot(rcType, []string{"weight"})
	if !ok {
		t.Fatal("declined after a write")
	}
	if gen2 == gen1 {
		t.Error("epoch did not advance after an edge write")
	}
	if n := len(second.IDs()); n != 2 {
		t.Errorf("second snapshot holds %d rels, want 2 — a stale snapshot was served", n)
	}
}

// --- per-type invalidation ---

// TestRelTypeEpoch_UnrelatedTypeWriteDoesNotInvalidate is the whole point of the
// striping: inserting an edge of type B must leave type A's cached columns valid.
// Before striping, relEpoch was global and this rebuilt every type on every write.
func TestRelTypeEpoch_UnrelatedTypeWriteDoesNotInvalidate(t *testing.T) {
	bs := rcStore(t)
	rcRel(t, bs, 100, 1, 2, rcType, int64(5), 1000, 0)

	_, genA, ok, _ := bs.RelColumnSnapshot(rcType, []string{"weight"})
	if !ok {
		t.Fatal("declined")
	}
	rebuilds := bs.ColumnRebuildCount()

	// A write to a DIFFERENT type.
	rcRel(t, bs, 400, 1, 3, rcOther, int64(7), 500, 0)

	_, genA2, ok, _ := bs.RelColumnSnapshot(rcType, []string{"weight"})
	if !ok {
		t.Fatal("declined after unrelated write")
	}
	if genA2 != genA {
		t.Errorf("type A's epoch moved (%d -> %d) on a write to type B; the stripe "+
			"did not isolate it", genA, genA2)
	}
	if got := bs.ColumnRebuildCount(); got != rebuilds {
		t.Errorf("type A rebuilt (%d -> %d) after a write to type B", rebuilds, got)
	}
}

// TestRelTypeEpoch_SameTypeWriteDoesInvalidate is the other half: striping must not
// make a type blind to its OWN writes. A stripe that never invalidates would pass
// the probe above and serve stale columns forever.
func TestRelTypeEpoch_SameTypeWriteDoesInvalidate(t *testing.T) {
	bs := rcStore(t)
	rcRel(t, bs, 100, 1, 2, rcType, int64(5), 1000, 0)
	_, gen1, _, _ := bs.RelColumnSnapshot(rcType, []string{"weight"})

	rcRel(t, bs, 200, 2, 3, rcType, int64(9), 2000, 0) // SAME type

	snap, gen2, ok, _ := bs.RelColumnSnapshot(rcType, []string{"weight"})
	if !ok {
		t.Fatal("declined")
	}
	if gen2 == gen1 {
		t.Fatal("a write to the type's OWN stripe did not advance its epoch")
	}
	if n := len(snap.IDs()); n != 2 {
		t.Errorf("snapshot holds %d rels, want 2 — stale columns were served", n)
	}
}

// TestRelTypeEpoch_CoarseSiteInvalidatesEverything pins the safety inversion: a
// mutation path that does NOT know its type (here a delete, which holds only a
// RelID) must invalidate every type. Getting this wrong is the failure mode the
// whole design is arranged to avoid — an unconverted site must cost speed, never
// correctness.
func TestRelTypeEpoch_CoarseSiteInvalidatesEverything(t *testing.T) {
	bs := rcStore(t)
	rcRel(t, bs, 100, 1, 2, rcType, int64(5), 1000, 0)
	rcRel(t, bs, 400, 1, 3, rcOther, int64(7), 500, 0)

	_, genA, _, _ := bs.RelColumnSnapshot(rcType, nil)
	_, genB, _, _ := bs.RelColumnSnapshot(rcOther, nil)

	// A DELETE — the coarse path, which cannot name the type.
	if err := bs.DeleteRelationship(types.RelID(400)); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	_, genA2, _, _ := bs.RelColumnSnapshot(rcType, nil)
	if genA2 == genA {
		t.Error("an untyped mutation left type A's epoch unchanged; a site that " +
			"cannot name its type MUST invalidate everything, or it serves stale columns")
	}
	_, genB2, _, _ := bs.RelColumnSnapshot(rcOther, nil)
	if genB2 == genB {
		t.Error("an untyped mutation left type B's epoch unchanged")
	}
}
