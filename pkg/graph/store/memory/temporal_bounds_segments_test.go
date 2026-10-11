package memory

import (
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestDeclaredSegmentsRefuseTypedBoundsBeforeMutation(t *testing.T) {
	s := New()
	t.Cleanup(func() { s.Close() })
	for _, id := range []types.NodeID{1, 2} {
		if err := s.PutNode(types.NewNode(id, 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.DeclareRelSegment(segTestDecl(0)); err != nil {
		t.Fatal(err)
	}
	r := types.NewRelationship(3, segTestHOP, 1, 2)
	tm := &types.TemporalMetadata{}
	if err := tm.SetValidBounds(types.TemporalRange{From: types.TemporalEndpoint{Value: -100, Finite: true}, To: types.TemporalEndpoint{Value: 0, Finite: true}}); err != nil {
		t.Fatal(err)
	}
	r.SetTemporal(tm)
	if err := s.PutRelationship(r); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatal(err)
	}
	if _, err := s.GetRelationship(r.ID()); !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatal("refusal wrote a row", err)
	}
	plain := types.NewRelationship(4, segTestHOP, 1, 2)
	if err := s.PutRelationship(plain); err != nil {
		t.Fatal(err)
	}
	changed := plain.DeepCopy()
	changed.SetTemporal(tm)
	if err := s.ReplaceRelationship(changed); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatal(err)
	}
	if got, err := s.GetRelationship(plain.ID()); err != nil || got.Temporal() != nil {
		t.Fatal("replace refusal changed existing row", got, err)
	}
	if err := s.PutRelationshipsBatch([]*types.Relationship{types.NewRelationship(5, segTestHOP, 1, 2), r}); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatal(err)
	}
	if _, err := s.GetRelationship(5); !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatal("batch refusal partially wrote", err)
	}
}

func TestLateSegmentDeclarationRefusesExistingTypedRows(t *testing.T) {
	s := New()
	t.Cleanup(func() { s.Close() })
	for _, id := range []types.NodeID{1, 2} {
		if err := s.PutNode(types.NewNode(id, 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	r := types.NewRelationship(3, segTestHOP, 1, 2)
	tm := &types.TemporalMetadata{}
	if err := tm.SetValidBounds(types.TemporalRange{From: types.TemporalEndpoint{Value: -100, Finite: true}, To: types.TemporalEndpoint{Value: 0, Finite: true}}); err != nil {
		t.Fatal(err)
	}
	r.SetTemporal(tm)
	if err := s.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	if err := s.DeclareRelSegment(segTestDecl(0)); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatal(err)
	}
	if _, err := s.RelSegmentStats(segTestHOP); !errors.Is(err, storepkg.ErrRelSegmentNotDeclared) {
		t.Fatal("failed declaration installed format", err)
	}
	if got, err := s.GetRelationship(r.ID()); err != nil || got.Temporal().ValidEndpoints != tm.ValidEndpoints {
		t.Fatal("failed declaration lost bounds", got, err)
	}
}
