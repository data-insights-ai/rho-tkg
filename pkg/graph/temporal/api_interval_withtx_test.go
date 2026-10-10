package temporal

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ivCall is what the spy saw of one SetXVersionIntervalWithTx call.
type ivCall struct {
	id     int64
	vf, vt types.Instant
	props  map[string]any
	tx     types.Instant
}

// The caller-instant interval doors on a nil or zero API return ErrNilGraph
// and never reach an ops value. Catches a wrapper that dereferences nil ops.
func TestAPIIntervalWithTxNilReceivers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, api := range map[string]*API{"nil": nil, "zero": {}, "typed-nil ops": New((*temporalOpsSpy)(nil))} {
		if _, err := api.SetNodeVersionIntervalWithTx(ctx, 10, 1, 2, nil, 5); !errors.Is(err, grapherr.ErrNilGraph) {
			t.Fatalf("%s SetNodeVersionIntervalWithTx = %v; want ErrNilGraph", name, err)
		}
		if _, err := api.SetRelVersionIntervalWithTx(ctx, 20, 1, 2, nil, 5); !errors.Is(err, grapherr.ErrNilGraph) {
			t.Fatalf("%s SetRelVersionIntervalWithTx = %v; want ErrNilGraph", name, err)
		}
	}
}

// The doors hand id, interval, patch and the instant to the ops verbatim and
// return the ops error unchanged. Catches a facade that forwards to the plain
// SetXVersionInterval (the instant is lost and the core stamps the clock), one
// that drops or replaces the instant (0, the clock), swaps validFrom/validTo,
// rewrites the patch (e.g. injecting a reserved tkg_tx_from property), or
// swallows the core's sentinel.
func TestAPIIntervalWithTxForwardsInstantVerbatim(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	wantErr := errors.New("ops error")
	for _, tc := range []struct {
		name string
		at   types.Instant
	}{{"min", 1}, {"typical", 1767268800000}, {"negative passes through to core validation", -7}} {
		spy := &temporalOpsSpy{err: wantErr}
		api := New(spy)
		props := map[string]any{"w": int64(2), "gone": nil}
		if _, err := api.SetNodeVersionIntervalWithTx(ctx, 42, 1000, 2000, props, tc.at); !errors.Is(err, wantErr) {
			t.Fatalf("%s: node err = %v; want the ops error", tc.name, err)
		}
		want := ivCall{id: 42, vf: 1000, vt: 2000, props: props, tx: tc.at}
		if len(spy.calls) != 1 || spy.calls[0] != "SetNodeVersionIntervalWithTx" || !reflect.DeepEqual(spy.lastIv, want) {
			t.Fatalf("%s: node forwarded calls=%v %+v; want [SetNodeVersionIntervalWithTx] %+v", tc.name, spy.calls, spy.lastIv, want)
		}
		if _, err := api.SetRelVersionIntervalWithTx(ctx, 44, 3000, 0, props, tc.at); !errors.Is(err, wantErr) {
			t.Fatalf("%s: rel err = %v; want the ops error", tc.name, err)
		}
		want = ivCall{id: 44, vf: 3000, vt: 0, props: props, tx: tc.at}
		if len(spy.calls) != 2 || spy.calls[1] != "SetRelVersionIntervalWithTx" || !reflect.DeepEqual(spy.lastIv, want) {
			t.Fatalf("%s: rel forwarded calls=%v %+v; want [.. SetRelVersionIntervalWithTx] %+v", tc.name, spy.calls, spy.lastIv, want)
		}
	}
}
