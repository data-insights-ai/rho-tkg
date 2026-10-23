package memory

import (
	"errors"
	"math/rand"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// Backlog 42: a relationship of a sealed segment (ADR-0011) is no longer in
// the rels map; its current-row stamps come from the sealed row. A memory
// implementation that reads only the map reports a sealed relationship as
// not found (and LatestStamps would call it deleted).
func TestCurrentStamps_SealedRelationships(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewSource(42)) // #nosec G404 -- deterministic test data
	tw := newSegTwin(t, 0, 6)
	for i := 0; i < 40; i++ {
		tw.put(r, segTestHOP)
	}
	tw.put(r, segTestOther) // unsealed beside the sealed ones
	if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
		t.Fatal(err)
	}
	if st := tw.stats(); st.LiveSealedRows != 40 {
		t.Fatalf("setup: %+v", st)
	}
	for _, id := range tw.rels {
		lent, err := tw.declared.LendRelationship(id)
		if err != nil {
			t.Fatal(err)
		}
		want := storecontract.RelRowStamps(lent)
		if want.TxFrom == 0 {
			t.Fatalf("rel %d: fixture has no TxFrom", id)
		}
		got, err := tw.declared.RelCurrentStamps(id)
		if err != nil || got != want {
			t.Fatalf("rel %d: RelCurrentStamps = (%+v, %v), the sealed row is %+v", id, got, err, want)
		}
	}
	tw.both("DeleteRelationship", func(s *Store) error { return s.DeleteRelationship(tw.rels[3]) })
	if _, err := tw.declared.RelCurrentStamps(tw.rels[3]); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("deleted sealed rel = %v, want ErrRelNotFound", err)
	}
}
