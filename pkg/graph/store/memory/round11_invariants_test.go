package memory

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestValidFromZeroMarkerInvariant (round 11 item 2): a row carries
// ValidFromZero only with ValidFrom 0 (one representation per row); a store
// refuses the marker with any other valid-from, node and relationship, and
// keeps the marker of a well-formed row.
func TestValidFromZeroMarkerInvariant(t *testing.T) {
	s := New()
	n := types.NewNode(types.NodeID(1<<20), 1, nil)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: 5, ValidFromZero: true, TxFrom: 9})
	if err := s.PutNode(n); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Errorf("node with the marker and valid-from 5: %v, want ErrInvalidStoreMutation", err)
	}
	ok := types.NewNode(types.NodeID(1<<20+2), 1, nil)
	ok.SetTemporal(&types.TemporalMetadata{ValidFromZero: true, TxFrom: 9})
	if err := s.PutNode(ok); err != nil {
		t.Fatalf("node valid from the explicit 0: %v", err)
	}
	got, err := s.GetNode(ok.ID())
	if err != nil {
		t.Fatal(err)
	}
	if vf, claimed := got.ValidFromClaim(); !claimed || vf != 0 {
		t.Errorf("stored node: ValidFromClaim %d %v", vf, claimed)
	}
	r := types.NewRelationship(types.RelID(1<<20+1), 1, ok.ID(), ok.ID())
	r.SetTemporal(&types.TemporalMetadata{ValidFrom: -3, ValidFromZero: true, TxFrom: 9})
	if err := s.PutRelationship(r); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Errorf("relationship with the marker and valid-from -3: %v, want ErrInvalidStoreMutation", err)
	}
}
