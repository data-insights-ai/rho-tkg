package rels

import (
	"context"
	"errors"
	"iter"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestAPIIter_NilContextYieldsErrNilContext: a nil ctx is the repo's
// ErrNilContext (what every Ctx door answers), yielded once with the scan
// never started; Iter, OutgoingIter and IncomingIter used to panic on
// ctx.Err().
func TestAPIIter_NilContextYieldsErrNilContext(t *testing.T) {
	t.Parallel()
	spy := &iterRelRowsSpy{relOpsSpy: &relOpsSpy{}, rows: buildTestRels(3)}
	api := New(spy)
	var nilCtx context.Context
	for name, seq := range map[string]iter.Seq2[*types.Relationship, error]{
		"Iter":         api.Iter(nilCtx, storepkg.QueryOpts{}),
		"OutgoingIter": api.OutgoingIter(nilCtx, 1, "T"),
		"IncomingIter": api.IncomingIter(nilCtx, 1, "T"),
	} {
		var errs []error
		rows := 0
		for r, err := range seq {
			if r != nil {
				rows++
			}
			errs = append(errs, err)
		}
		if len(errs) != 1 || !errors.Is(errs[0], grapherr.ErrNilContext) || rows != 0 {
			t.Fatalf("%s(nil ctx) yielded errors %v and %d rows; want one ErrNilContext and no row", name, errs, rows)
		}
	}
	if spy.visited != 0 {
		t.Fatalf("the scan visited %d rows under a nil ctx; want it never started", spy.visited)
	}
}

// TestCtxWrappersNilContext: the Ctx wrappers over an Ops without the budgeted
// doors answer a nil ctx with ErrNilContext, not a panic.
func TestCtxWrappersNilContext(t *testing.T) {
	t.Parallel()
	api := New(&relOpsSpy{})
	var nilCtx context.Context
	calls := map[string]func() error{
		"ByTypeAndPropertyCtx": func() error {
			_, err := api.ByTypeAndPropertyCtx(nilCtx, "T", "k", 1, storepkg.QueryOpts{})
			return err
		},
		"OutgoingCtx": func() error {
			_, err := api.OutgoingCtx(nilCtx, 1, "T")
			return err
		},
		"IncomingCtx": func() error {
			_, err := api.IncomingCtx(nilCtx, 1, "T")
			return err
		},
		"GetByIDsCtx": func() error {
			_, err := api.GetByIDsCtx(nilCtx, []types.RelID{1})
			return err
		},
		"HasHistoryCtx": func() error {
			_, err := api.HasHistoryCtx(nilCtx, 1)
			return err
		},
		"LatestStampsCtx": func() error {
			_, _, _, err := api.LatestStampsCtx(nilCtx, 1)
			return err
		},
		"HistoryCtx": func() error {
			_, err := api.HistoryCtx(nilCtx, 1)
			return err
		},
	}
	for name, call := range calls {
		if err := call(); !errors.Is(err, grapherr.ErrNilContext) {
			t.Fatalf("%s(nil ctx) = %v; want ErrNilContext", name, err)
		}
	}
}
