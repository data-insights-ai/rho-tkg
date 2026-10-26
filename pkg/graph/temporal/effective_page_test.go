package temporal

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The paged effective doors (backlog 69) of temporalOpsSpy: the arguments land
// in pageArgs, the answer is the spy's segments with next = 77.
type pageArgs struct {
	ctx   context.Context
	pin   Pin
	f     Filter
	after int64
	limit int
}

var spyPageArgs pageArgs

func (s *temporalOpsSpy) RelsEffectivePage(ctx context.Context, pin Pin, f Filter, after types.RelID, limit int) ([]RelSegment, types.RelID, error) {
	s.record("RelsEffectivePage")
	spyPageArgs = pageArgs{ctx, pin, f, int64(after), limit}
	return s.relSegs, 77, s.err
}

func (s *temporalOpsSpy) NodesEffectivePage(ctx context.Context, pin Pin, f Filter, after types.NodeID, limit int) ([]NodeSegment, types.NodeID, error) {
	s.record("NodesEffectivePage")
	spyPageArgs = pageArgs{ctx, pin, f, int64(after), limit}
	return s.nodeSegs, 77, s.err
}

// TestEffectivePageWrappers — rule 1 for the two paged doors: a nil API and a
// typed-nil Ops return ErrNilGraph; a ready API forwards ctx, pin, filter,
// cursor and limit verbatim and returns the Ops' segments, cursor and error.
// Not parallel: the spy records into one package variable.
func TestEffectivePageWrappers(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, 1)
	pin := Pin{Tx: 91, ValidStart: 3, ValidEnd: 9}
	f := Filter{Prop: "estate", Eq: "e1"}
	wantErr := errors.New("ops failed")
	n1 := types.NewNode(10, 0, nil)
	r1 := types.NewRelationship(20, 0, 0, 0)
	for _, rel := range []bool{true, false} {
		call := func(a *API) (int, int64, error) {
			if rel {
				segs, next, err := a.RelsEffectivePage(ctx, pin, f, 5, 7)
				return len(segs), int64(next), err
			}
			segs, next, err := a.NodesEffectivePage(ctx, pin, f, 5, 7)
			return len(segs), int64(next), err
		}
		for name, api := range map[string]*API{"nil API": nil, "typed-nil ops": New((*temporalOpsSpy)(nil))} {
			if _, _, err := call(api); !errors.Is(err, grapherr.ErrNilGraph) {
				t.Fatalf("rel=%v %s = %v, want ErrNilGraph", rel, name, err)
			}
		}
		spy := &temporalOpsSpy{err: wantErr, nodeSegs: []NodeSegment{{ValidFrom: 1, Node: n1}}, relSegs: []RelSegment{{ValidFrom: 2, Rel: r1}}}
		n, next, err := call(New(spy))
		want := "NodesEffectivePage"
		if rel {
			want = "RelsEffectivePage"
		}
		a := spyPageArgs
		switch {
		case len(spy.calls) != 1 || spy.calls[0] != want:
			t.Fatalf("rel=%v calls %v, want [%s]", rel, spy.calls, want)
		case a.ctx != ctx || a.pin != pin || a.f != f || a.after != 5 || a.limit != 7:
			t.Fatalf("rel=%v forwarded %+v", rel, a)
		case n != 1 || next != 77 || !errors.Is(err, wantErr):
			t.Fatalf("rel=%v returned %d segments, next %d, err %v", rel, n, next, err)
		}
	}
}

func (s *temporalOpsSpy) ForEachNodeEffectiveByLabelAndProperties(ctx context.Context, label string, props map[string]any, pin types.Instant, opts EffectivePageOpts, fn func(types.NodeID, []NodeSegment) bool) (types.NodeID, error) {
	s.record("ForEachNodeEffectiveByLabelAndProperties")
	spyLabelPropsArgs = labelPropsArgs{ctx, label, props, pin, opts}
	fn(10, s.nodeSegs)
	return 78, s.err
}

type labelPropsArgs struct {
	ctx   context.Context
	label string
	props map[string]any
	pin   types.Instant
	opts  EffectivePageOpts
}

var spyLabelPropsArgs labelPropsArgs

// TestForEachNodeEffectiveByLabelAndPropertiesWrapper — rule 1 for the
// listing page door (row 121): nil API and typed-nil Ops are ErrNilGraph; a
// ready API forwards every argument verbatim and returns the Ops' cursor and
// error, fn seeing the Ops' segments. Not parallel: the spy records into one
// package variable.
func TestForEachNodeEffectiveByLabelAndPropertiesWrapper(t *testing.T) {
	ctx := context.Background()
	props := map[string]any{"ns": "a", "rel": "r"}
	opts := EffectivePageOpts{After: 5, Limit: 7, Depth: 1}
	none := func(types.NodeID, []NodeSegment) bool { return true }
	for name, api := range map[string]*API{"nil API": nil, "typed-nil ops": New((*temporalOpsSpy)(nil))} {
		if _, err := api.ForEachNodeEffectiveByLabelAndProperties(ctx, "L", props, 9, opts, none); !errors.Is(err, grapherr.ErrNilGraph) {
			t.Fatalf("%s = %v, want ErrNilGraph", name, err)
		}
	}
	wantErr := errors.New("ops failed")
	spy := &temporalOpsSpy{err: wantErr, nodeSegs: []NodeSegment{{ValidFrom: 1, Node: types.NewNode(10, 0, nil)}}}
	var got int
	next, err := New(spy).ForEachNodeEffectiveByLabelAndProperties(ctx, "L", props, 9, opts, func(id types.NodeID, segs []NodeSegment) bool {
		got += int(id) + len(segs)
		return true
	})
	a := spyLabelPropsArgs
	switch {
	case len(spy.calls) != 1 || spy.calls[0] != "ForEachNodeEffectiveByLabelAndProperties":
		t.Fatalf("calls %v", spy.calls)
	case a.ctx != ctx || a.label != "L" || a.props["ns"] != "a" || a.pin != 9 || a.opts != opts:
		t.Fatalf("forwarded %+v", a)
	case next != 78 || !errors.Is(err, wantErr) || got != 11:
		t.Fatalf("returned next %d, err %v, fn saw %d", next, err, got)
	}
}

func (s *temporalOpsSpy) ForEachNodeEffectiveByLabelAndPropertyValues(ctx context.Context, label string, props map[string]any, key string, values []any, pin types.Instant, opts EffectivePageOpts, fn func(types.NodeID, []NodeSegment) bool) (types.NodeID, error) {
	s.record("ForEachNodeEffectiveByLabelAndPropertyValues")
	spyLabelPropsArgs = labelPropsArgs{ctx, label, props, pin, opts}
	spyValuesArgs = valuesArgs{key, values}
	fn(10, s.nodeSegs)
	return 79, s.err
}

type valuesArgs struct {
	key    string
	values []any
}

var spyValuesArgs valuesArgs

// TestForEachNodeEffectiveByLabelAndPropertyValuesWrapper — rule 1 for the
// value-list page door (ROWPROBE): nil API and typed-nil Ops are ErrNilGraph;
// a ready API forwards every argument verbatim and returns the Ops' cursor
// and error, fn seeing the Ops' segments. Not parallel: the spy records into
// package variables.
func TestForEachNodeEffectiveByLabelAndPropertyValuesWrapper(t *testing.T) {
	ctx := context.Background()
	props := map[string]any{"ns": "a", "rel": "r"}
	values := []any{int64(3), "x"}
	opts := EffectivePageOpts{After: 5, Limit: 7, Depth: 1}
	none := func(types.NodeID, []NodeSegment) bool { return true }
	for name, api := range map[string]*API{"nil API": nil, "typed-nil ops": New((*temporalOpsSpy)(nil))} {
		if _, err := api.ForEachNodeEffectiveByLabelAndPropertyValues(ctx, "L", props, "h", values, 9, opts, none); !errors.Is(err, grapherr.ErrNilGraph) {
			t.Fatalf("%s = %v, want ErrNilGraph", name, err)
		}
	}
	wantErr := errors.New("ops failed")
	spy := &temporalOpsSpy{err: wantErr, nodeSegs: []NodeSegment{{ValidFrom: 1, Node: types.NewNode(10, 0, nil)}}}
	var got int
	next, err := New(spy).ForEachNodeEffectiveByLabelAndPropertyValues(ctx, "L", props, "h", values, 9, opts, func(id types.NodeID, segs []NodeSegment) bool {
		got += int(id) + len(segs)
		return true
	})
	a, v := spyLabelPropsArgs, spyValuesArgs
	switch {
	case len(spy.calls) != 1 || spy.calls[0] != "ForEachNodeEffectiveByLabelAndPropertyValues":
		t.Fatalf("calls %v", spy.calls)
	case a.ctx != ctx || a.label != "L" || a.props["ns"] != "a" || a.pin != 9 || a.opts != opts:
		t.Fatalf("forwarded %+v", a)
	case v.key != "h" || len(v.values) != 2 || v.values[0] != int64(3) || v.values[1] != "x":
		t.Fatalf("forwarded key %q values %v", v.key, v.values)
	case next != 79 || !errors.Is(err, wantErr) || got != 11:
		t.Fatalf("returned next %d, err %v, fn saw %d", next, err, got)
	}
}
