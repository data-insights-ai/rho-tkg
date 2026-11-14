package nodes

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
// never started; Iter and IterByLabel used to panic on ctx.Err().
func TestAPIIter_NilContextYieldsErrNilContext(t *testing.T) {
	t.Parallel()
	spy := &iterRowsSpy{nodeOpsSpy: &nodeOpsSpy{}, rows: buildTestNodes(3)}
	api := New(spy)
	var nilCtx context.Context
	for name, seq := range map[string]iter.Seq2[*types.Node, error]{
		"Iter":        api.Iter(nilCtx, storepkg.QueryOpts{}),
		"IterByLabel": api.IterByLabel(nilCtx, "L", storepkg.QueryOpts{}),
	} {
		var errs []error
		rows := 0
		for n, err := range seq {
			if n != nil {
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
	api := New(&nodeOpsSpy{})
	var nilCtx context.Context
	opts := storepkg.QueryOpts{}
	calls := map[string]func() error{
		"ByLabelAndPropertyCtx": func() error {
			_, err := api.ByLabelAndPropertyCtx(nilCtx, "L", "k", 1, opts)
			return err
		},
		"ByLabelAndPropertyValuesCtx": func() error {
			_, err := api.ByLabelAndPropertyValuesCtx(nilCtx, "L", "k", []any{1}, opts)
			return err
		},
		"ByLabelAndPropertiesCtx": func() error {
			_, err := api.ByLabelAndPropertiesCtx(nilCtx, "L", map[string]any{"k": 1}, opts)
			return err
		},
		"ByLabelCtx": func() error {
			_, err := api.ByLabelCtx(nilCtx, "L", opts)
			return err
		},
		"IDsByLabelAndPropertyValuesCtx": func() error {
			_, err := api.IDsByLabelAndPropertyValuesCtx(nilCtx, "L", "k", []any{1}, opts)
			return err
		},
		"GetByIDsCtx": func() error {
			_, err := api.GetByIDsCtx(nilCtx, []types.NodeID{1})
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
