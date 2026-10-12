package temporal

import (
	"errors"
	"slices"
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

// The broader effective scans (backlog 35) of temporalOpsSpy.
func (s *temporalOpsSpy) spyNodeSegs(fn func(NodeSegment) bool) error {
	for _, seg := range s.nodeSegs {
		if !fn(seg) {
			break
		}
	}
	return s.err
}

func (s *temporalOpsSpy) spyRelSegs(fn func(RelSegment) bool) error {
	for _, seg := range s.relSegs {
		if !fn(seg) {
			break
		}
	}
	return s.err
}

func (s *temporalOpsSpy) ForEachNodeEffective(pin types.Instant, fn func(NodeSegment) bool) error {
	s.record("ForEachNodeEffective")
	s.lastPin = pin
	return s.spyNodeSegs(fn)
}

func (s *temporalOpsSpy) ForEachRelEffective(pin types.Instant, fn func(RelSegment) bool) error {
	s.record("ForEachRelEffective")
	s.lastPin = pin
	return s.spyRelSegs(fn)
}

func (s *temporalOpsSpy) NodesEffectiveByIDs(ids []types.NodeID, pin types.Instant, fn func(NodeSegment) bool) error {
	s.record("NodesEffectiveByIDs")
	s.lastNodeIDs, s.lastPin = ids, pin
	return s.spyNodeSegs(fn)
}

func (s *temporalOpsSpy) RelsEffectiveByIDs(ids []types.RelID, pin types.Instant, fn func(RelSegment) bool) error {
	s.record("RelsEffectiveByIDs")
	s.lastRelIDs, s.lastPin = ids, pin
	return s.spyRelSegs(fn)
}

func (s *temporalOpsSpy) ForEachRelEffectiveAtNodes(nodeIDs []types.NodeID, dir Direction, relTypes []string, pin types.Instant, fn func(RelSegment) bool) error {
	s.record("ForEachRelEffectiveAtNodes")
	s.lastNodeIDs, s.lastDir, s.lastRelTypes, s.lastPin = nodeIDs, dir, relTypes, pin
	return s.spyRelSegs(fn)
}

func (s *temporalOpsSpy) ForEachNodeEffectiveByLabels(labels []string, pin types.Instant, fn func(NodeSegment) bool) error {
	s.record("ForEachNodeEffectiveByLabels")
	s.lastLabels, s.lastPin = labels, pin
	return s.spyNodeSegs(fn)
}

// TestEffectiveScanWrappers — rule 1 for the six backlog-35 wrappers: a nil
// API and a typed-nil Ops return ErrNilGraph without calling anything; a ready
// API forwards every argument verbatim (pin, IDs, direction, types, labels),
// returns the Ops' error unchanged (errors.Is) and hands fn the segments until
// it returns false. Catches a wrapper that swaps or drops an argument, loses
// the error, or ignores fn's stop.
func TestEffectiveScanWrappers(t *testing.T) {
	t.Parallel()
	nodeIDs, relIDs := []types.NodeID{3, 1, 3}, []types.RelID{7, 5}
	relTypes, labels := []string{"T", "U"}, []string{"L", "M"}
	type wrapper struct {
		name    string
		rel     bool
		call    func(a *API, pin types.Instant, fnN func(NodeSegment) bool, fnR func(RelSegment) bool) error
		checked func(s *temporalOpsSpy) bool
	}
	noArgs := func(*temporalOpsSpy) bool { return true }
	ws := []wrapper{
		{"ForEachNodeEffective", false, func(a *API, p types.Instant, fn func(NodeSegment) bool, _ func(RelSegment) bool) error {
			return a.ForEachNodeEffective(p, fn)
		}, noArgs},
		{"ForEachRelEffective", true, func(a *API, p types.Instant, _ func(NodeSegment) bool, fn func(RelSegment) bool) error {
			return a.ForEachRelEffective(p, fn)
		}, noArgs},
		{"NodesEffectiveByIDs", false, func(a *API, p types.Instant, fn func(NodeSegment) bool, _ func(RelSegment) bool) error {
			return a.NodesEffectiveByIDs(nodeIDs, p, fn)
		}, func(s *temporalOpsSpy) bool { return slices.Equal(s.lastNodeIDs, nodeIDs) }},
		{"RelsEffectiveByIDs", true, func(a *API, p types.Instant, _ func(NodeSegment) bool, fn func(RelSegment) bool) error {
			return a.RelsEffectiveByIDs(relIDs, p, fn)
		}, func(s *temporalOpsSpy) bool { return slices.Equal(s.lastRelIDs, relIDs) }},
		{"ForEachRelEffectiveAtNodes", true, func(a *API, p types.Instant, _ func(NodeSegment) bool, fn func(RelSegment) bool) error {
			return a.ForEachRelEffectiveAtNodes(nodeIDs, Incoming, relTypes, p, fn)
		}, func(s *temporalOpsSpy) bool {
			return slices.Equal(s.lastNodeIDs, nodeIDs) && s.lastDir == Incoming && slices.Equal(s.lastRelTypes, relTypes)
		}},
		{"ForEachNodeEffectiveByLabels", false, func(a *API, p types.Instant, fn func(NodeSegment) bool, _ func(RelSegment) bool) error {
			return a.ForEachNodeEffectiveByLabels(labels, p, fn)
		}, func(s *temporalOpsSpy) bool { return slices.Equal(s.lastLabels, labels) }},
	}
	never := func(NodeSegment) bool { t.Fatal("fn called"); return false }
	neverR := func(RelSegment) bool { t.Fatal("fn called"); return false }
	wantErr := errors.New("ops failed")
	n1, n2 := types.NewNode(10, 0, nil), types.NewNode(11, 0, nil)
	r1, r2 := types.NewRelationship(20, 0, 0, 0), types.NewRelationship(21, 0, 0, 0)
	for i, w := range ws {
		for name, api := range map[string]*API{"nil API": nil, "typed-nil ops": New((*temporalOpsSpy)(nil))} {
			if err := w.call(api, 2, never, neverR); !errors.Is(err, grapherr.ErrNilGraph) {
				t.Fatalf("%s %s = %v, want ErrNilGraph", name, w.name, err)
			}
		}
		spy := &temporalOpsSpy{
			err:      wantErr,
			nodeSegs: []NodeSegment{{ValidFrom: 5, ValidTo: 7, Node: n1}, {ValidFrom: 7, Node: n2}},
			relSegs:  []RelSegment{{ValidFrom: 3, ValidTo: 4, Rel: r1}, {ValidFrom: 9, Rel: r2}},
		}
		pin := types.Instant(90 + i)
		var got []any
		fnN := func(s NodeSegment) bool { got = append(got, s.Node); return false }
		fnR := func(s RelSegment) bool { got = append(got, s.Rel); return false }
		err := w.call(New(spy), pin, fnN, fnR)
		first := any(n1)
		if w.rel {
			first = r1
		}
		if !errors.Is(err, wantErr) || len(spy.calls) != 1 || spy.calls[0] != w.name || spy.lastPin != pin || !w.checked(spy) ||
			len(got) != 1 || got[0] != first {
			t.Fatalf("%s: err %v, calls %v, pin %v, args forwarded %v, segments %v (want 1: fn stopped)",
				w.name, err, spy.calls, spy.lastPin, w.checked(spy), got)
		}
	}
}
