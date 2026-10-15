package temporal

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
)

// The excise doors on a nil or zero API return ErrNilGraph without touching
// ops, and on a live API forward id, range and instant verbatim and return the
// ops error unchanged. Catches a facade that swaps a and b, drops the instant,
// routes the WithTx door to the plain one, or swallows the sentinel.
func TestAPIExciseForwardsVerbatim(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, api := range map[string]*API{"nil": nil, "zero": {}, "typed-nil ops": New((*temporalOpsSpy)(nil))} {
		for door, err := range map[string]error{
			"node":       api.ExciseNodeValidity(ctx, 10, 1, 2),
			"rel":        api.ExciseRelValidity(ctx, 20, 1, 2),
			"nodeWithTx": api.ExciseNodeValidityWithTx(ctx, 10, 1, 2, 5),
			"relWithTx":  api.ExciseRelValidityWithTx(ctx, 20, 1, 2, 5),
		} {
			if !errors.Is(err, grapherr.ErrNilGraph) {
				t.Fatalf("%s %s = %v; want ErrNilGraph", name, door, err)
			}
		}
	}
	wantErr := errors.New("ops error")
	spy := &temporalOpsSpy{err: wantErr}
	api := New(spy)
	steps := []struct {
		call string
		run  func() error
		want ivCall
	}{
		{"ExciseNodeValidity", func() error { return api.ExciseNodeValidity(ctx, 42, 1000, 2000) }, ivCall{id: 42, vf: 1000, vt: 2000}},
		{"ExciseRelValidity", func() error { return api.ExciseRelValidity(ctx, 44, 3000, 4000) }, ivCall{id: 44, vf: 3000, vt: 4000}},
		{"ExciseNodeValidityWithTx", func() error { return api.ExciseNodeValidityWithTx(ctx, 46, 5000, 6000, 77) }, ivCall{id: 46, vf: 5000, vt: 6000, tx: 77}},
		{"ExciseRelValidityWithTx", func() error { return api.ExciseRelValidityWithTx(ctx, 48, 7000, 8000, 78) }, ivCall{id: 48, vf: 7000, vt: 8000, tx: 78}},
	}
	for i, st := range steps {
		if err := st.run(); !errors.Is(err, wantErr) {
			t.Fatalf("%s err = %v; want the ops error", st.call, err)
		}
		if len(spy.calls) != i+1 || spy.calls[i] != st.call || !reflect.DeepEqual(spy.lastIv, st.want) {
			t.Fatalf("%s forwarded calls=%v %+v; want %+v", st.call, spy.calls, spy.lastIv, st.want)
		}
	}
}
