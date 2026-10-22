package temporal

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// byPropsArgs records the arguments of the row-105 door on temporalOpsSpy.
type byPropsArgs struct {
	ctx      context.Context
	typeName string
	keys     []string
	tuples   [][]any
	pin      Pin
}

var spyByPropsArgs byPropsArgs

func (s *temporalOpsSpy) RelEffectiveTimelinesByProperties(ctx context.Context, typeName string, keys []string, tuples [][]any, pin Pin) ([][]RelSegment, error) {
	s.record("RelEffectiveTimelinesByProperties")
	spyByPropsArgs = byPropsArgs{ctx, typeName, keys, tuples, pin}
	out := make([][]RelSegment, len(tuples))
	for i := range out {
		out[i] = s.relSegs
	}
	return out, s.err
}

// TestRelEffectiveTimelinesByPropertiesWrapper — rule 1 for the row-105 door:
// a nil API and a typed-nil Ops return ErrNilGraph and no result; a ready API
// forwards ctx, type, keys, tuples (order and repeats kept) and pin verbatim
// and returns the Ops' aligned answer and error unchanged. Not parallel: the
// spy records into one package variable.
func TestRelEffectiveTimelinesByPropertiesWrapper(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, 1)
	keys := []string{"estate", "device"}
	tuples := [][]any{{"e1", "d1"}, {"e2", "d1"}, {"e1", "d1"}}
	pin := Pin{Tx: 91, ValidStart: 3, ValidEnd: 9}
	for name, api := range map[string]*API{"nil API": nil, "typed-nil ops": New((*temporalOpsSpy)(nil))} {
		if got, err := api.RelEffectiveTimelinesByProperties(ctx, "NET", keys, tuples, pin); !errors.Is(err, grapherr.ErrNilGraph) || got != nil {
			t.Fatalf("%s = %v, %v; want nil, ErrNilGraph", name, got, err)
		}
	}
	wantErr := errors.New("ops failed")
	r1 := types.NewRelationship(20, 0, 0, 0)
	spy := &temporalOpsSpy{err: wantErr, relSegs: []RelSegment{{ValidFrom: 3, Rel: r1}}}
	got, err := New(spy).RelEffectiveTimelinesByProperties(ctx, "NET", keys, tuples, pin)
	if !errors.Is(err, wantErr) || len(got) != 3 || got[2][0].Rel != r1 {
		t.Fatalf("answer = %v, %v", got, err)
	}
	a := spyByPropsArgs
	if a.ctx != ctx || a.typeName != "NET" || !slices.Equal(a.keys, keys) || !reflect.DeepEqual(a.tuples, tuples) || a.pin != pin {
		t.Fatalf("forwarded %+v", a)
	}
	if want := []string{"RelEffectiveTimelinesByProperties"}; !slices.Equal(spy.calls, want) {
		t.Fatalf("calls %v, want %v", spy.calls, want)
	}
}
