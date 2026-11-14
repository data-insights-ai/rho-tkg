package badger

import (
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// idDoors renders what the doors that read the live ID sets answer: by-ID reads of
// live, deleted and unknown nodes and relationships, existence-gated reads
// (hash, stamps, history presence, adjacency, degrees), the type index, the
// listings in order and the counts.
func idDoors(t *testing.T, bs *Store) string {
	t.Helper()
	var out []string
	add := func(format string, args ...any) { out = append(out, fmt.Sprintf(format, args...)) }
	relIDs := func(rs []*types.Relationship) []types.RelID {
		ids := make([]types.RelID, 0, len(rs))
		for _, r := range rs {
			ids = append(ids, r.ID())
		}
		slices.Sort(ids)
		return ids
	}
	probe := []types.NodeID{9998, 9999}
	for i := types.NodeID(0); i <= 70; i++ {
		probe = append(probe, i)
	}
	for _, id := range probe {
		n, err := bs.GetNode(id)
		if err != nil {
			add("GetNode %d: %v", id, err)
		} else {
			add("GetNode %d: labels=%d props=%v", id, n.LabelTokenCount(), n.Properties())
		}
		h, err := bs.NodeIntegrityHash(id)
		add("NodeIntegrityHash %d: %s %v", id, h, err)
		st, err := bs.NodeCurrentStamps(id)
		add("NodeCurrentStamps %d: %+v %v", id, st, err)
		has, err := bs.HasNodeHistory(id)
		add("HasNodeHistory %d: %v %v", id, has, err)
		in, ierr := bs.IncomingRelationships(id, 0)
		outs, oerr := bs.OutgoingRelationships(id, 0)
		add("rels of %d: in=%v %v out=%v %v", id, relIDs(in), ierr, relIDs(outs), oerr)
		od, _ := bs.OutgoingDegree(id, 7)
		idg, _ := bs.IncomingDegree(id, 8)
		add("degrees of %d: out7=%d in8=%d", id, od, idg)
	}
	for rid := types.RelID(99); rid <= 245; rid++ {
		r, err := bs.GetRelationship(rid)
		if err != nil {
			add("GetRelationship %d: %v", rid, err)
		} else {
			add("GetRelationship %d: type=%d %d->%d", rid, r.TypeToken().Value(), r.StartNodeID(), r.EndNodeID())
		}
		st, err := bs.RelCurrentStamps(rid)
		add("RelCurrentStamps %d: %+v %v", rid, st, err)
		has, err := bs.HasRelHistory(rid)
		add("HasRelHistory %d: %v %v", rid, has, err)
	}
	for _, typ := range []uint16{7, 8, 9} {
		ids, err := bs.RelIDsByType(typ)
		add("RelIDsByType %d: %v %v", typ, ids, err)
		var scan []types.RelID
		_ = bs.ForEachRelByType(typ, storecontract.QueryOpts{}, func(r *types.Relationship) bool {
			scan = append(scan, r.ID())
			return true
		})
		add("ForEachRelByType %d: %v", typ, scan)
		rs, err := bs.RelationshipsByType(typ, storecontract.QueryOpts{Limit: 5})
		add("RelationshipsByType %d limit 5: %v %v", typ, relIDs(rs), err)
	}
	allN, err := bs.AllNodeIDs(storecontract.QueryOpts{})
	add("AllNodeIDs: %v %v", allN, err)
	allR, err := bs.AllRelIDs(storecontract.QueryOpts{})
	add("AllRelIDs: %v %v", allR, err)
	var eachN []types.NodeID
	_ = bs.ForEachNodeID(func(id types.NodeID) bool { eachN = append(eachN, id); return true })
	var eachR []types.RelID
	_ = bs.ForEachRelID(func(id types.RelID) bool { eachR = append(eachR, id); return true })
	add("ForEachNodeID %v ForEachRelID %v", eachN, eachR)
	nc, _ := bs.NodeCount()
	rc, _ := bs.RelationshipCount()
	add("counts %d %d", nc, rc)
	// What a close seals into the tiered catalog (sealCountsLocked).
	lo, hi, anyRel, err := bs.RelIDRange()
	add("RelIDRange %d %d %v %v", lo, hi, anyRel, err)
	sn, sr, byLabel, byType, err := bs.CountSnapshot()
	add("CountSnapshot %d %d %v %v %v", sn, sr, byLabel, byType, err)
	none, err := bs.HoldsNoRows()
	add("HoldsNoRows %v %v", none, err)
	former, ok := bs.FormerLabelTokens()
	add("FormerLabelTokens %v %v", former, ok)
	return strings.Join(out, "\n") + "\n" + bs.IndexStateDigestForTest()
}

// TestEncodedIDSetsMatchEagerOnEveryOpenShape: a read-only open with
// Config.TransientRead keeps the live node set, the live rel set and the
// rel-type sets of a trusted image in the image's encoded form (R3 cold open:
// building them is half the CPU of an open at ai-soc's configuration and a
// fifth of its allocation), and every door that asks them answers what the
// open that built them answers; no read builds a set afterwards. An image the
// open cannot trust (none, an older one) walks the keys and builds them as
// before. Catches a set built at the open (the count the test asserts), a
// door that does not read the encoded form, and a set built from an image it
// should not have trusted.
func TestEncodedIDSetsMatchEagerOnEveryOpenShape(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		shape    string
		encoded  bool
		revsMade bool // the transient open has the revision tables made: the walk of an untrusted open bumps a stripe per entity
	}{
		{"clean", true, false},
		{"nocounts", true, false},
		{"noproperty", true, false},
		{"crashed", false, true},
		{"delta", false, true},
	} {
		t.Run(c.shape, func(t *testing.T) {
			t.Parallel()
			dir := lazyLabelFixture(t, c.shape)
			open := func(transient bool) *Store {
				s, err := New(Config{Dir: dir, ReadOnly: true, TransientRead: transient})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.Close() })
				return s
			}
			eager, transient := open(false), open(true)
			if n, r, ty := eager.IDSetsEncodedForTest(); n || r || ty != 0 {
				t.Fatalf("the eager open holds encoded sets: nodes %v rels %v types %d", n, r, ty)
			}
			wantTypes := 0
			if c.encoded {
				wantTypes = 2 // the fixture has relationships of types 7 and 8
			}
			if n, r, ty := transient.IDSetsEncodedForTest(); n != c.encoded || r != c.encoded || ty != wantTypes {
				t.Fatalf("right after the open: encoded sets nodes %v rels %v types %d, want %v %v %d", n, r, ty, c.encoded, c.encoded, wantTypes)
			}
			// The revision tables follow the same rule: unmade on a transient read-only
			// open (they answer 1, an untouched stripe), made on any other.
			if n, r := eager.RevTablesMadeForTest(); !n || !r {
				t.Fatalf("the eager open has revision tables made: nodes %v rels %v, want both", n, r)
			}
			if n, r := transient.RevTablesMadeForTest(); n != c.revsMade || r != c.revsMade {
				t.Fatalf("the transient open has revision tables made: nodes %v rels %v, want %v", n, r, c.revsMade)
			}
			want, got := idDoors(t, eager), idDoors(t, transient)
			if got != want {
				gl, wl := strings.Split(got, "\n"), strings.Split(want, "\n")
				for i := 0; i < len(gl) && i < len(wl); i++ {
					if gl[i] != wl[i] {
						t.Fatalf("the encoded-set open answers differ at line %d:\n  encoded: %s\n    eager: %s", i+1, gl[i], wl[i])
					}
				}
				t.Fatalf("the encoded-set open answers differ in length: %d lines, eager %d", len(gl), len(wl))
			}
			if n, r, ty := transient.IDSetsEncodedForTest(); n != c.encoded || r != c.encoded || ty != wantTypes {
				t.Fatalf("after the doors: encoded sets nodes %v rels %v types %d, want %v %v %d: a read built a set", n, r, ty, c.encoded, c.encoded, wantTypes)
			}
		})
	}
}

// TestOpenImageIDSetSectionEncoded: the encoded decode of the image's ID set
// sections accepts what the eager decode accepts, to the same IDs, and refuses
// what it refuses (a zero delta, a count beyond the bytes, a delta that
// overflows, a truncated varint), so an image the eager open would reject is
// rejected by the transient open too and the open walks the keys.
func TestOpenImageIDSetSectionEncoded(t *testing.T) {
	t.Parallel()
	section := func(kind byte, payload ...uint64) []byte {
		var p []byte
		for _, v := range payload {
			p = binary.AppendUvarint(p, v)
		}
		b := []byte{kind}
		b = binary.AppendUvarint(b, uint64(len(p)))
		return append(b, p...)
	}
	var long []uint64 // 200 IDs with gaps of 1 to 300: several anchors
	long = append(long, 200)
	for i := uint64(0); i < 200; i++ {
		long = append(long, 1+(i*37)%300)
	}
	valid := map[string][]byte{
		"empty":        section(imgSecNodeIDs, 0),
		"three":        section(imgSecNodeIDs, 3, 5, 3, 100),
		"long":         section(imgSecNodeIDs, long...),
		"nodes + rels": append(section(imgSecNodeIDs, 2, 1, 1), section(imgSecRelIDs, 2, 7, 1)...),
	}
	for name, b := range valid {
		eager, err := decodeOpenImage(b)
		if err != nil {
			t.Fatalf("%s: eager decode: %v", name, err)
		}
		enc, err := decodeOpenImageWith(b, true)
		if err != nil {
			t.Fatalf("%s: encoded decode: %v", name, err)
		}
		if got, want := digestNodeIDSet(enc.nodeIDs), digestNodeIDSet(eager.nodeIDs); !slices.Equal(got, want) {
			t.Fatalf("%s: encoded node IDs %v, want %v", name, got, want)
		}
		if eager.relIDs != nil {
			if got, want := digestRelIDSet(enc.relIDs), digestRelIDSet(eager.relIDs); !slices.Equal(got, want) {
				t.Fatalf("%s: encoded rel IDs %v, want %v", name, got, want)
			}
		}
		if enc.nodeIDs.Len() != eager.nodeIDs.Len() {
			t.Fatalf("%s: encoded Len %d, want %d", name, enc.nodeIDs.Len(), eager.nodeIDs.Len())
		}
		if wantEnc := eager.nodeIDs.Len() > 0; enc.nodeIDs.Encoded() != wantEnc {
			t.Fatalf("%s: encoded = %v for %d IDs", name, enc.nodeIDs.Encoded(), enc.nodeIDs.Len())
		}
	}
	bad := map[string][]byte{
		"zero delta":        section(imgSecNodeIDs, 2, 5, 0),
		"short":             section(imgSecNodeIDs, 3, 5, 3),
		"overflow":          section(imgSecNodeIDs, 2, 1<<62, 1<<62+(1<<62-1)),
		"count beyond data": section(imgSecNodeIDs, 1000, 5),
		"truncated varint":  {imgSecNodeIDs, 2, 1, 0x80},
		"trailing garbage":  section(imgSecNodeIDs, 1, 5, 9),
	}
	for name, b := range bad {
		_, eagerErr := decodeOpenImage(b)
		_, encErr := decodeOpenImageWith(b, true)
		if (eagerErr == nil) != (encErr == nil) {
			t.Errorf("%s: eager decode error %v, encoded decode error %v: they must agree", name, eagerErr, encErr)
		}
		if encErr == nil {
			t.Errorf("%s: encoded decode accepted it", name)
		}
	}
}

// TestTransientReadIsIgnoredOnAWritableOpen: Config.TransientRead means nothing
// without ReadOnly: a writable open builds its label maps, ID sets and revision
// tables at the open as before.
func TestTransientReadIsIgnoredOnAWritableOpen(t *testing.T) {
	t.Parallel()
	dir := lazyLabelFixture(t, "clean")
	s, err := New(Config{Dir: dir, TransientRead: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if lazy, maps := s.LabelMapStateForTest(); lazy || maps == 0 {
		t.Fatalf("a writable open with TransientRead: lazy label maps %v with %d maps", lazy, maps)
	}
	if n, r, ty := s.IDSetsEncodedForTest(); n || r || ty != 0 {
		t.Fatalf("a writable open with TransientRead holds encoded sets: %v %v %d", n, r, ty)
	}
	if n, r := s.RevTablesMadeForTest(); !n || !r {
		t.Fatalf("a writable open with TransientRead has revision tables made: %v %v, want both", n, r)
	}
}
