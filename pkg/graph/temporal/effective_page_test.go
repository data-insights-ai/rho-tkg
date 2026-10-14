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
