package temporal

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestCtxWrappersNilContext: every Ctx wrapper over an Ops without the Ctx
// doors answers a nil ctx with ErrNilContext, not a panic (asOfWithoutBudget
// called ctx.Err()). Not parallel: the spies record into package variables.
func TestCtxWrappersNilContext(t *testing.T) {
	api := New(&temporalOpsSpy{})
	var nilCtx context.Context
	for door, r := range byIDsCtxCalls(api, nilCtx) {
		if !errors.Is(r.err, grapherr.ErrNilContext) || r.got {
			t.Fatalf("%s(nil ctx) = %v, result %v; want ErrNilContext and none", door, r.err, r.got)
		}
	}
	for door, r := range pointCtxCalls(api, nilCtx) {
		if !errors.Is(r.err, grapherr.ErrNilContext) || r.got {
			t.Fatalf("%s(nil ctx) = %v, result %v; want ErrNilContext and none", door, r.err, r.got)
		}
	}
	if n, err := api.NodeAsOfCtx(nilCtx, 1, 2); !errors.Is(err, grapherr.ErrNilContext) || n != nil {
		t.Fatalf("NodeAsOfCtx(nil ctx) = %v, %v; want ErrNilContext and no row", n, err)
	}
	if r, err := api.RelAsOfCtx(nilCtx, types.RelID(1), 2); !errors.Is(err, grapherr.ErrNilContext) || r != nil {
		t.Fatalf("RelAsOfCtx(nil ctx) = %v, %v; want ErrNilContext and no row", r, err)
	}
}
