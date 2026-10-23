package rels

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func (s *relOpsSpy) ValidHullByType(typeName string, pin types.Instant, opts storepkg.CountTimelineOpts) (storepkg.ValidHull, bool, error) {
	s.record("ValidHullByType:" + typeName)
	return storepkg.ValidHull{OpenEnd: true, From: types.UnboundedValidFrom}, true, s.err
}

func (s *relOpsSpy) ForEachByIDsAnyVersion(ids []types.RelID, pin types.Instant, fn func(*types.Relationship) bool) error {
	s.record("ForEachByIDsAnyVersion")
	if len(ids) == 2 && pin == 11 {
		fn(types.NewRelationship(5, 0, 0, 0))
	}
	return s.err
}

// TestRound10DoorsForward: ValidHullByType forwards; a nil API reports the
// nil graph.
func TestRound10DoorsForward(t *testing.T) {
	wantErr := errors.New("ops failed")
	spy := &relOpsSpy{err: wantErr}
	h, ok, err := New(spy).ValidHullByType("T", 5, storepkg.CountTimelineOpts{CurrentRows: true})
	if !errors.Is(err, wantErr) || !ok || !h.OpenEnd || h.From != types.UnboundedValidFrom || len(spy.calls) != 1 || spy.calls[0] != "ValidHullByType:T" {
		t.Fatalf("ValidHullByType = %+v %v %v, calls %v", h, ok, err, spy.calls)
	}
	var seen int
	if err := New(spy).ForEachByIDsAnyVersion([]types.RelID{3, 4}, 11, func(*types.Relationship) bool { seen++; return true }); !errors.Is(err, wantErr) || seen != 1 {
		t.Fatalf("ForEachByIDsAnyVersion = %v, %d rows", err, seen)
	}
	var nilAPI *API
	if _, _, err := nilAPI.ValidHullByType("T", 5, storepkg.CountTimelineOpts{}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil ValidHullByType: %v", err)
	}
	if err := nilAPI.ForEachByIDsAnyVersion([]types.RelID{1}, 1, func(*types.Relationship) bool { return true }); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil ForEachByIDsAnyVersion: %v", err)
	}
}
