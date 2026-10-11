package badger

import (
	"testing"

	store "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestAdjacentStampPreservesFiniteZeroAndInfinity(t *testing.T) {
	s, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, id := range []types.NodeID{1001, 1002} {
		if err := s.PutNode(types.NewNode(id, 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	shapes := []struct {
		range_ types.TemporalRange
		at     types.Instant
		want   bool
	}{
		{types.TemporalRange{From: types.TemporalEndpoint{Value: -10, Finite: true}, To: types.TemporalEndpoint{Finite: true}}, 0, false},
		{types.TemporalRange{From: types.TemporalEndpoint{Value: -10, Finite: true}, To: types.TemporalEndpoint{Finite: true}}, -1, true},
		{types.TemporalRange{From: types.TemporalEndpoint{Finite: true}, To: types.TemporalEndpoint{Value: 10, Finite: true}}, 0, true},
		{types.TemporalRange{To: types.TemporalEndpoint{Finite: true}}, -100, true},
		{types.TemporalRange{}, 0, true},
	}
	for i, tc := range shapes {
		r := types.NewRelationship(types.RelID(2000+i), 1, 1001, 1002)
		tm := &types.TemporalMetadata{}
		if err := tm.SetValidBounds(tc.range_); err != nil {
			t.Fatal(err)
		}
		r.SetTemporal(tm)
		if err := s.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}
	// First query builds cached stamps; every subsequent query uses that cache.
	for _, incoming := range []bool{false, true} {
		nid := types.NodeID(1001)
		if incoming {
			nid = 1002
		}
		for i, tc := range shapes {
			opts := store.QueryOpts{ValidAt: tc.at, ValidAtSet: true}
			var endpointFound, relFound bool
			if err := s.ForEachAdjacentEndpointAt(nid, 1, incoming, opts, func(id types.RelID, _ types.NodeID) bool {
				if id == types.RelID(2000+i) {
					endpointFound = true
				}
				return true
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.ForEachAdjacentRelAt(nid, 1, incoming, opts, func(r *types.Relationship) bool {
				if r.ID() == types.RelID(2000+i) {
					relFound = true
				}
				return true
			}); err != nil {
				t.Fatal(err)
			}
			if endpointFound != tc.want || relFound != tc.want {
				t.Fatalf("shape%d incoming%v at%d: endpoint%v row%v want%v", i, incoming, tc.at, endpointFound, relFound, tc.want)
			}
		}
	}
	// Adding an explicit epoch-start row after lazy build must update its stamp.
	r := types.NewRelationship(3000, 1, 1001, 1002)
	tm := &types.TemporalMetadata{}
	if err := tm.SetValidBounds(types.TemporalRange{From: types.TemporalEndpoint{Finite: true}}); err != nil {
		t.Fatal(err)
	}
	r.SetTemporal(tm)
	if err := s.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	found := false
	if err := s.ForEachAdjacentEndpointAt(1001, 1, false, store.QueryOpts{ValidAtSet: true}, func(id types.RelID, _ types.NodeID) bool { found = found || id == 3000; return true }); err != nil || !found {
		t.Fatal("stamp added after cache build lost finite zero", found, err)
	}
}
