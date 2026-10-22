package temporal

import (
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The many-pin effective timelines (row 98) of temporalOpsSpy: one canned
// answer per pin.
func (s *temporalOpsSpy) NodeEffectiveTimelines(id types.NodeID, pins []types.Instant) ([][]NodeSegment, error) {
	s.record("NodeEffectiveTimelines")
	s.lastNodeID, s.lastPins = id, pins
	out := make([][]NodeSegment, len(pins))
	for i := range out {
		out[i] = s.nodeSegs
	}
	return out, s.err
}

func (s *temporalOpsSpy) RelEffectiveTimelines(id types.RelID, pins []types.Instant) ([][]RelSegment, error) {
	s.record("RelEffectiveTimelines")
	s.lastRelID, s.lastPins = id, pins
	out := make([][]RelSegment, len(pins))
	for i := range out {
		out[i] = s.relSegs
	}
	return out, s.err
}

// The lowest answered pins (row 104) of temporalOpsSpy: lastPin per id.
func (s *temporalOpsSpy) NodeLowestAnsweredPin(id types.NodeID) (types.Instant, error) {
	s.record("NodeLowestAnsweredPin")
	s.lastNodeID = id
	return s.lastPin, s.err
}

func (s *temporalOpsSpy) NodesLowestAnsweredPins(ids []types.NodeID) ([]types.Instant, error) {
	s.record("NodesLowestAnsweredPins")
	s.lastNodeIDs = ids
	out := make([]types.Instant, len(ids))
	for i := range out {
		out[i] = s.lastPin
	}
	return out, s.err
}

func (s *temporalOpsSpy) RelLowestAnsweredPin(id types.RelID) (types.Instant, error) {
	s.record("RelLowestAnsweredPin")
	s.lastRelID = id
	return s.lastPin, s.err
}

func (s *temporalOpsSpy) RelsLowestAnsweredPins(ids []types.RelID) ([]types.Instant, error) {
	s.record("RelsLowestAnsweredPins")
	s.lastRelIDs = ids
	out := make([]types.Instant, len(ids))
	for i := range out {
		out[i] = s.lastPin
	}
	return out, s.err
}

// TestLowestAnsweredPinWrappers — rule 1 for the row-104 wrappers: a nil API
// and a typed-nil Ops return ErrNilGraph and no result; a ready API forwards
// the ids verbatim and returns the Ops' answers and error unchanged.
func TestLowestAnsweredPinWrappers(t *testing.T) {
	t.Parallel()
	for name, api := range map[string]*API{"nil API": nil, "typed-nil ops": New((*temporalOpsSpy)(nil))} {
		if got, err := api.NodeLowestAnsweredPin(1); !errors.Is(err, grapherr.ErrNilGraph) || got != 0 {
			t.Fatalf("%s NodeLowestAnsweredPin = %v, %v", name, got, err)
		}
		if got, err := api.NodesLowestAnsweredPins([]types.NodeID{1}); !errors.Is(err, grapherr.ErrNilGraph) || got != nil {
			t.Fatalf("%s NodesLowestAnsweredPins = %v, %v", name, got, err)
		}
		if got, err := api.RelLowestAnsweredPin(1); !errors.Is(err, grapherr.ErrNilGraph) || got != 0 {
			t.Fatalf("%s RelLowestAnsweredPin = %v, %v", name, got, err)
		}
		if got, err := api.RelsLowestAnsweredPins([]types.RelID{1}); !errors.Is(err, grapherr.ErrNilGraph) || got != nil {
			t.Fatalf("%s RelsLowestAnsweredPins = %v, %v", name, got, err)
		}
	}
	wantErr := errors.New("ops failed")
	spy := &temporalOpsSpy{err: wantErr, lastPin: 77}
	api := New(spy)
	if got, err := api.NodeLowestAnsweredPin(10); !errors.Is(err, wantErr) || got != 77 || spy.lastNodeID != 10 {
		t.Fatalf("NodeLowestAnsweredPin = %v, %v; id %v", got, err, spy.lastNodeID)
	}
	nids := []types.NodeID{3, 1, 3}
	if got, err := api.NodesLowestAnsweredPins(nids); !errors.Is(err, wantErr) || len(got) != 3 || !slices.Equal(spy.lastNodeIDs, nids) {
		t.Fatalf("NodesLowestAnsweredPins = %v, %v; ids %v", got, err, spy.lastNodeIDs)
	}
	if got, err := api.RelLowestAnsweredPin(20); !errors.Is(err, wantErr) || got != 77 || spy.lastRelID != 20 {
		t.Fatalf("RelLowestAnsweredPin = %v, %v; id %v", got, err, spy.lastRelID)
	}
	rids := []types.RelID{5, 2}
	if got, err := api.RelsLowestAnsweredPins(rids); !errors.Is(err, wantErr) || len(got) != 2 || !slices.Equal(spy.lastRelIDs, rids) {
		t.Fatalf("RelsLowestAnsweredPins = %v, %v; ids %v", got, err, spy.lastRelIDs)
	}
	if want := []string{"NodeLowestAnsweredPin", "NodesLowestAnsweredPins", "RelLowestAnsweredPin", "RelsLowestAnsweredPins"}; !slices.Equal(spy.calls, want) {
		t.Fatalf("calls %v, want %v", spy.calls, want)
	}
}

// TestEffectiveTimelinesWrappers — rule 1 for the many-pin wrappers: a nil API
// and a typed-nil Ops return ErrNilGraph and no result without calling
// anything; a ready API forwards id and pins verbatim (same order, duplicates
// kept) and returns the Ops' answers and error unchanged.
func TestEffectiveTimelinesWrappers(t *testing.T) {
	t.Parallel()
	for name, api := range map[string]*API{"nil API": nil, "typed-nil ops": New((*temporalOpsSpy)(nil))} {
		if got, err := api.NodeEffectiveTimelines(1, []types.Instant{2}); !errors.Is(err, grapherr.ErrNilGraph) || got != nil {
			t.Fatalf("%s NodeEffectiveTimelines = %v, %v", name, got, err)
		}
		if got, err := api.RelEffectiveTimelines(1, []types.Instant{2}); !errors.Is(err, grapherr.ErrNilGraph) || got != nil {
			t.Fatalf("%s RelEffectiveTimelines = %v, %v", name, got, err)
		}
	}
	wantErr := errors.New("ops failed")
	n1 := types.NewNode(10, 0, nil)
	r1 := types.NewRelationship(20, 0, 0, 0)
	spy := &temporalOpsSpy{
		err:      wantErr,
		nodeSegs: []NodeSegment{{ValidFrom: 5, Node: n1}},
		relSegs:  []RelSegment{{ValidFrom: 3, Rel: r1}},
	}
	api := New(spy)
	pins := []types.Instant{9, 3, 9}
	ns, err := api.NodeEffectiveTimelines(10, pins)
	if !errors.Is(err, wantErr) || len(ns) != 3 || ns[2][0].Node != n1 || spy.lastNodeID != 10 || !slices.Equal(spy.lastPins, pins) {
		t.Fatalf("NodeEffectiveTimelines = %v, %v; forwarded id %v pins %v", ns, err, spy.lastNodeID, spy.lastPins)
	}
	rs, err := api.RelEffectiveTimelines(20, pins[:2])
	if !errors.Is(err, wantErr) || len(rs) != 2 || rs[1][0].Rel != r1 || spy.lastRelID != 20 || !slices.Equal(spy.lastPins, pins[:2]) {
		t.Fatalf("RelEffectiveTimelines = %v, %v; forwarded id %v pins %v", rs, err, spy.lastRelID, spy.lastPins)
	}
	if want := []string{"NodeEffectiveTimelines", "RelEffectiveTimelines"}; !slices.Equal(spy.calls, want) {
		t.Fatalf("calls %v, want %v", spy.calls, want)
	}
}
