package temporal

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The effective-timeline Ops of temporalOpsSpy: record the call and its
// arguments, hand back the spy's canned segments (each call once per segment
// for the scan forms) and error.
func (s *temporalOpsSpy) NodeEffectiveTimeline(id types.NodeID, pin types.Instant) ([]NodeSegment, error) {
	s.record("NodeEffectiveTimeline")
	s.lastNodeID, s.lastPin = id, pin
	return s.nodeSegs, s.err
}

func (s *temporalOpsSpy) RelEffectiveTimeline(id types.RelID, pin types.Instant) ([]RelSegment, error) {
	s.record("RelEffectiveTimeline")
	s.lastRelID, s.lastPin = id, pin
	return s.relSegs, s.err
}

func (s *temporalOpsSpy) ForEachNodeEffectiveByLabel(label string, pin types.Instant, fn func(NodeSegment) bool) error {
	s.record("ForEachNodeEffectiveByLabel")
	s.lastLabel, s.lastPin = label, pin
	for _, seg := range s.nodeSegs {
		if !fn(seg) {
			break
		}
	}
	return s.err
}

func (s *temporalOpsSpy) ForEachRelEffectiveByType(typeName string, pin types.Instant, fn func(RelSegment) bool) error {
	s.record("ForEachRelEffectiveByType")
	s.lastRelType, s.lastPin = typeName, pin
	for _, seg := range s.relSegs {
		if !fn(seg) {
			break
		}
	}
	return s.err
}

// TestEffectiveTimelineWrappers — rule 1 for the four wrappers: a nil API and
// a typed-nil Ops return ErrNilGraph without calling anything; a ready API
// forwards the arguments verbatim, returns the Ops' segments and error
// unchanged (errors.Is), and the scan forms hand every segment to fn until it
// returns false. Catches a wrapper that swaps id / pin, drops the error,
// copies or reorders segments, or ignores fn's stop.
func TestEffectiveTimelineWrappers(t *testing.T) {
	t.Parallel()
	for name, api := range map[string]*API{"nil API": nil, "typed-nil ops": New((*temporalOpsSpy)(nil))} {
		if _, err := api.NodeEffectiveTimeline(1, 2); !errors.Is(err, grapherr.ErrNilGraph) {
			t.Fatalf("%s NodeEffectiveTimeline = %v", name, err)
		}
		if _, err := api.RelEffectiveTimeline(1, 2); !errors.Is(err, grapherr.ErrNilGraph) {
			t.Fatalf("%s RelEffectiveTimeline = %v", name, err)
		}
		if err := api.ForEachNodeEffectiveByLabel("L", 2, func(NodeSegment) bool { return true }); !errors.Is(err, grapherr.ErrNilGraph) {
			t.Fatalf("%s ForEachNodeEffectiveByLabel = %v", name, err)
		}
		if err := api.ForEachRelEffectiveByType("T", 2, func(RelSegment) bool { return true }); !errors.Is(err, grapherr.ErrNilGraph) {
			t.Fatalf("%s ForEachRelEffectiveByType = %v", name, err)
		}
	}

	wantErr := errors.New("ops failed")
	n1, n2 := types.NewNode(10, 0, nil), types.NewNode(10, 0, nil)
	r1, r2 := types.NewRelationship(20, 0, 0, 0), types.NewRelationship(20, 0, 0, 0)
	spy := &temporalOpsSpy{
		err:      wantErr,
		nodeSegs: []NodeSegment{{ValidFrom: 5, ValidTo: 7, Node: n1}, {ValidFrom: 7, Node: n2}},
		relSegs:  []RelSegment{{ValidFrom: 3, ValidTo: 4, Rel: r1}, {ValidFrom: 9, Rel: r2}},
	}
	api := New(spy)

	ns, err := api.NodeEffectiveTimeline(10, 99)
	if !errors.Is(err, wantErr) || len(ns) != 2 || ns[0].Node != n1 || ns[1].ValidFrom != 7 || spy.lastNodeID != 10 || spy.lastPin != 99 {
		t.Fatalf("NodeEffectiveTimeline = %v, %v; forwarded id %v pin %v", ns, err, spy.lastNodeID, spy.lastPin)
	}
	rs, err := api.RelEffectiveTimeline(20, 98)
	if !errors.Is(err, wantErr) || len(rs) != 2 || rs[0].Rel != r1 || rs[1].ValidFrom != 9 || spy.lastRelID != 20 || spy.lastPin != 98 {
		t.Fatalf("RelEffectiveTimeline = %v, %v; forwarded id %v pin %v", rs, err, spy.lastRelID, spy.lastPin)
	}

	var gotN []NodeSegment
	err = api.ForEachNodeEffectiveByLabel("L", 97, func(s NodeSegment) bool { gotN = append(gotN, s); return true })
	if !errors.Is(err, wantErr) || len(gotN) != 2 || gotN[1].Node != n2 || spy.lastLabel != "L" || spy.lastPin != 97 {
		t.Fatalf("ForEachNodeEffectiveByLabel: %v, %d segments, label %q pin %v", err, len(gotN), spy.lastLabel, spy.lastPin)
	}
	var gotR []RelSegment
	err = api.ForEachRelEffectiveByType("T", 96, func(s RelSegment) bool { gotR = append(gotR, s); return false })
	if !errors.Is(err, wantErr) || len(gotR) != 1 || gotR[0].Rel != r1 || spy.lastRelType != "T" || spy.lastPin != 96 {
		t.Fatalf("ForEachRelEffectiveByType: %v, %d segments (want 1: fn stopped), type %q pin %v", err, len(gotR), spy.lastRelType, spy.lastPin)
	}
	want := []string{"NodeEffectiveTimeline", "RelEffectiveTimeline", "ForEachNodeEffectiveByLabel", "ForEachRelEffectiveByType"}
	if len(spy.calls) != len(want) {
		t.Fatalf("calls %v, want %v", spy.calls, want)
	}
	for i := range want {
		if spy.calls[i] != want[i] {
			t.Fatalf("calls %v, want %v", spy.calls, want)
		}
	}
}
