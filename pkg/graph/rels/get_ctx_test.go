package rels

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// getCtxSpy is an Ops that also offers the budgeted point read GetCtx.
type getCtxSpy struct {
	*relOpsSpy
	gotCtx context.Context
	row    *types.Relationship
}

func (s *getCtxSpy) GetCtx(ctx context.Context, id types.RelID) (*types.Relationship, error) {
	s.record("GetCtx")
	s.gotCtx, s.lastRelID = ctx, id
	return s.row, s.err
}

// TestAPIGetCtx_Forwarding: GetCtx hands the context and ID to an Ops that has
// the budgeted door and returns its row and error unchanged; over an Ops without
// it, it is Get (the unmetered answer); a nil API is ErrNilGraph.
func TestAPIGetCtx_Forwarding(t *testing.T) {
	t.Parallel()
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, 1)

	if r, err := (*API)(nil).GetCtx(ctx, 3); !errors.Is(err, grapherr.ErrNilGraph) || r != nil {
		t.Fatalf("GetCtx on a nil API = %v, %v; want ErrNilGraph and no row", r, err)
	}

	row := types.NewRelationship(3, 0, 1, 2)
	spy := &getCtxSpy{relOpsSpy: &relOpsSpy{}, row: row}
	if r, err := New(spy).GetCtx(ctx, 3); err != nil || r != row || spy.gotCtx != ctx || spy.lastRelID != 3 || len(spy.calls) != 1 || spy.calls[0] != "GetCtx" {
		t.Fatalf("GetCtx = %v, %v, forwarded ctx kept %v, id %d, calls %v; want the Ops' row from GetCtx alone", r, err, spy.gotCtx == ctx, spy.lastRelID, spy.calls)
	}
	boom := errors.New("boom")
	spy = &getCtxSpy{relOpsSpy: &relOpsSpy{err: boom}}
	if r, err := New(spy).GetCtx(ctx, 3); !errors.Is(err, boom) || r != nil {
		t.Fatalf("GetCtx = %v, %v; want the Ops' error", r, err)
	}

	plain := &relOpsSpy{}
	if _, err := New(plain).GetCtx(ctx, 4); err != nil || len(plain.calls) != 1 || plain.calls[0] != "Get" || plain.lastRelID != 4 {
		t.Fatalf("GetCtx over an Ops without it = %v, calls %v, id %d; want Get", err, plain.calls, plain.lastRelID)
	}
}
