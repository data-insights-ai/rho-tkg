package memory

import (
	"math/rand"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// TestSegments_ValidFromZeroRowsStayInMemtable (round 11 item 2): the segment
// codec has no column for a row valid from the explicit instant 0 (its
// valid-from 0 reads "unset"), so sealing leaves such a row in the memtable,
// answered as before, and the type's segment scans mark it
// (RelSegmentBatch.ValidFromZero, ColumnBatch via the source).
func TestSegments_ValidFromZeroRowsStayInMemtable(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	tw := newSegTwin(t, 0, 6)
	for i := 0; i < 20; i++ {
		tw.put(r, segTestHOP)
	}
	zero := tw.newRel(r, segTestHOP)
	tm := zero.Temporal()
	tm.ValidFrom, tm.ValidTo, tm.ValidFromZero = 0, 0, true
	tw.both("PutRelationship(valid from the explicit 0)", func(s *Store) error { return s.PutRelationship(zero) })
	tw.rels = append(tw.rels, zero.ID())
	if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
		t.Fatalf("seal with a row valid from the explicit 0: %v", err)
	}
	if st := tw.stats(); st.LiveSealedRows != 20 || st.UnsealedRows != 1 {
		t.Fatalf("the 20 plain rows seal, the explicit-0 row stays: %+v", st)
	}
	tw.compare("explicit-0 row")
	got, err := tw.declared.GetRelationship(zero.ID())
	if err != nil {
		t.Fatal(err)
	}
	if vf, ok := got.ValidFromClaim(); !ok || vf != 0 {
		t.Fatalf("explicit-0 row after the seal: ValidFromClaim = %d, %v", vf, ok)
	}
	marked := 0
	if _, err := tw.declared.ScanRelSegments(segTestHOP, nil, func(b *storecontract.RelSegmentBatch) bool {
		for i, id := range b.IDs {
			z := i < len(b.ValidFromZero) && b.ValidFromZero[i]
			if z != (id == zero.ID()) {
				t.Errorf("segment batch row %d (id %d): ValidFromZero %v", i, id, z)
			}
			if z {
				marked++
			}
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if marked != 1 {
		t.Errorf("segment scan marked %d rows valid from the explicit 0, want 1", marked)
	}
}
