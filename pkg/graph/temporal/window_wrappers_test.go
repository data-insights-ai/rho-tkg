package temporal

import (
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func (s *temporalOpsSpy) ForEachNodeEffectiveByLabelsWindow(labels []string, pin types.Instant, window Window, fn func(NodeSegment) bool) error {
	s.record("ForEachNodeEffectiveByLabelsWindow")
	s.lastLabels, s.lastPin, s.lastValidFrom = labels, pin, window[0].From
	return s.spyNodeSegs(fn)
}

func (s *temporalOpsSpy) ForEachRelEffectiveByTypeWindow(typeName string, pin types.Instant, window Window, fn func(RelSegment) bool) error {
	s.record("ForEachRelEffectiveByTypeWindow")
	s.lastRelType, s.lastPin, s.lastValidFrom = typeName, pin, window[0].From
	return s.spyRelSegs(fn)
}

func (s *temporalOpsSpy) ForEachRelEffectiveAtNodesWindow(nodeIDs []types.NodeID, dir Direction, relTypes []string, pin types.Instant, window Window, fn func(RelSegment) bool) error {
	s.record("ForEachRelEffectiveAtNodesWindow")
	s.lastNodeIDs, s.lastDir, s.lastRelTypes, s.lastPin, s.lastValidFrom = nodeIDs, dir, relTypes, pin, window[0].From
	return s.spyRelSegs(fn)
}

func (s *temporalOpsSpy) ScanRelEffectiveColumnsAtNodes(nodeIDs []types.NodeID, dir Direction, relTypes []string, pin types.Instant, window Window, props []string, fn func(*storepkg.RelColumnBatch) bool) error {
	s.record("ScanRelEffectiveColumnsAtNodes")
	s.lastNodeIDs, s.lastDir, s.lastRelTypes, s.lastPin, s.lastValidFrom = nodeIDs, dir, relTypes, pin, window[0].From
	fn(&storepkg.RelColumnBatch{RelType: props[0]})
	return s.err
}

// TestRound10WindowWrappers — rule 1 for the windowed doors: a nil API and
// a typed-nil Ops return ErrNilGraph; a ready API forwards every argument
// (distinct values, so a swap is caught) and returns the Ops' error.
func TestRound10WindowWrappers(t *testing.T) {
	t.Parallel()
	const pin = types.Instant(91)
	w := Window{{From: -17, To: 4}}
	ids, types2 := []types.NodeID{3, 4}, []string{"T"}
	wantErr := errors.New("ops failed")
	n1 := types.NewNode(10, 0, nil)
	r1 := types.NewRelationship(20, 0, 0, 0)
	calls := map[string]func(a *API) error{
		"ForEachNodeEffectiveByLabelsWindow": func(a *API) error {
			return a.ForEachNodeEffectiveByLabelsWindow([]string{"L"}, pin, w, func(NodeSegment) bool { return true })
		},
		"ForEachRelEffectiveByTypeWindow": func(a *API) error {
			return a.ForEachRelEffectiveByTypeWindow("T", pin, w, func(RelSegment) bool { return true })
		},
		"ForEachRelEffectiveAtNodesWindow": func(a *API) error {
			return a.ForEachRelEffectiveAtNodesWindow(ids, Incoming, types2, pin, w, func(RelSegment) bool { return true })
		},
		"ScanRelEffectiveColumnsAtNodes": func(a *API) error {
			return a.ScanRelEffectiveColumnsAtNodes(ids, Incoming, types2, pin, w, []string{"p"}, func(b *storepkg.RelColumnBatch) bool { return b.RelType == "p" })
		},
	}
	for name, call := range calls {
		for label, api := range map[string]*API{"nil API": nil, "typed-nil ops": New((*temporalOpsSpy)(nil))} {
			if err := call(api); !errors.Is(err, grapherr.ErrNilGraph) {
				t.Fatalf("%s %s = %v, want ErrNilGraph", name, label, err)
			}
		}
		spy := &temporalOpsSpy{err: wantErr, nodeSegs: []NodeSegment{{ValidFrom: 1, Node: n1}}, relSegs: []RelSegment{{ValidFrom: 1, Rel: r1}}}
		if err := call(New(spy)); !errors.Is(err, wantErr) || !slices.Equal(spy.calls, []string{name}) || spy.lastPin != pin || spy.lastValidFrom != -17 {
			t.Fatalf("%s: err %v, calls %v, pin %v, window start %v", name, err, spy.calls, spy.lastPin, spy.lastValidFrom)
		}
		if name != "ForEachNodeEffectiveByLabelsWindow" && name != "ForEachRelEffectiveByTypeWindow" &&
			(!slices.Equal(spy.lastNodeIDs, ids) || spy.lastDir != Incoming || !slices.Equal(spy.lastRelTypes, types2)) {
			t.Fatalf("%s: ids %v dir %v types %v", name, spy.lastNodeIDs, spy.lastDir, spy.lastRelTypes)
		}
	}
}

// TestWindowValidateAndMeets: the window's rule against a brute-force
// overlap over every interval, at the far ends, 0 and negative instants.
func TestWindowValidateAndMeets(t *testing.T) {
	t.Parallel()
	bad := []Window{
		{{From: 5, To: 5}},
		{{From: 6, To: 5}},
		{{From: 0, To: 10}, {From: 9, To: 12}},
		{{From: 10, To: 12}, {From: 0, To: 5}},
		{{From: math.MinInt64, To: math.MinInt64}},
	}
	for _, w := range bad {
		if err := w.Validate(); !errors.Is(err, storepkg.ErrInvalidTimeRange) {
			t.Fatalf("Validate(%v) = %v, want ErrInvalidTimeRange", w, err)
		}
	}
	good := []Window{
		nil,
		{{From: types.UnboundedValidFrom, To: math.MaxInt64}},
		{{From: -10, To: 0}, {From: 0, To: 3}},
		{{From: -5, To: -1}, {From: 1, To: 2}, {From: 7, To: 8}},
		{{From: 0, To: 1}},
	}
	edges := []types.Instant{math.MinInt64, -11, -10, -6, -5, -2, -1, 0, 1, 2, 3, 6, 7, 8, 9, math.MaxInt64 - 1}
	for _, w := range good {
		if err := w.Validate(); err != nil {
			t.Fatalf("Validate(%v) = %v", w, err)
		}
		for _, from := range edges {
			for _, to := range append(slices.Clone(edges), 0) {
				if to != 0 && to <= from {
					continue
				}
				want := false
				for _, iv := range w {
					if from < iv.To && (to == 0 || to > iv.From) {
						want = true
					}
				}
				if got := w.Meets(from, to); got != want {
					t.Fatalf("%v.Meets(%d, %d) = %v, want %v", w, from, to, got, want)
				}
			}
		}
	}
}
