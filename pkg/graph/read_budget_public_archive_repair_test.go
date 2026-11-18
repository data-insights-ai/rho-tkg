package graph_test

import (
	"errors"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerstore "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestPublicReadBudget_ArchiveRepairRefusesBeforeScan(t *testing.T) {
	testtier.Slow(t)
	// Serial: the native row decoder counter is process-wide.
	cfg := tiered.Config{DataDir: t.TempDir(), RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1}
	open := func() (*graphpkg.Graph, *tiered.Store) {
		ts, err := tiered.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		g, err := graphpkg.New(graphpkg.Config{Store: ts})
		if err != nil {
			t.Fatal(err)
		}
		return g, ts
	}
	g, ts := open()
	var want []types.NodeID
	var tok uint16
	for range 3 {
		n, err := g.Nodes().Add(t.Context(), []string{"Case"}, map[string]any{"late": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		tok = n.PrimaryLabelToken().Value()
		want = append(want, n.ID())
		if err := ts.ArchiveNode(n.ID()); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g, ts = open()
	t.Cleanup(func() { _ = g.Close() })
	if err := ts.RefShardForTest().CreateCompositePropertyIndex(tok, []string{"late", "other"}); err != nil {
		t.Fatal(err)
	}
	var native, writable atomic.Int64
	ts.SetShardOpenHookForTest(func(cfg tiered.BadgerStoreConfig) {
		if filepath.Base(cfg.Dir) == "archive" {
			native.Add(1)
			if !cfg.ReadOnly {
				writable.Add(1)
			}
		}
	})
	ctx, outer := storepkg.WithReadBudget(t.Context(), storepkg.ReadBudget{DecodedBytes: 1})
	ctx, inner := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
	var got []*types.Node
	var err error
	decoded := badgerstore.CountRowDecodesForTest(func() { got, err = g.Nodes().GetByIDsCtx(ctx, want) })
	var rb *storepkg.ReadBudgetError
	if len(got) != 0 || !errors.As(err, &rb) || rb.Limit != "unsupported" || decoded != 0 || writable.Load() != 0 || ts.RefArchiveForTest().Load() != nil || outer.Exceeded() == nil || inner.Exceeded() == nil {
		t.Fatalf("pending archive repair = %d rows, %v, %d native decodes/%d writable opens; want shared refusal before maintenance", len(got), err, decoded, writable.Load())
	}
	if inner.Work().ShardOpens != uint64(native.Load()) {
		t.Fatalf("repair refusal = %d native opens, work %+v", native.Load(), inner.Work())
	}
	if rows, err := g.Nodes().GetByIDs(want); err != nil || len(rows) != len(want) {
		t.Fatalf("plain repair = %d rows, %v", len(rows), err)
	}
	for _, budget := range []storepkg.ReadBudget{{}, {ShardOpens: 2}} {
		// Closing and reopening the graph keeps the archive lazy, so each
		// healthy retry proves the native opens belong to this read.
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		g, ts = open()
		native.Store(0)
		ts.SetShardOpenHookForTest(func(cfg tiered.BadgerStoreConfig) {
			if filepath.Base(cfg.Dir) == "archive" {
				native.Add(1)
			}
		})
		ctx, meter := storepkg.WithReadBudget(t.Context(), budget)
		rows, err := g.Nodes().GetByIDsCtx(ctx, want)
		ids := make([]types.NodeID, len(rows))
		for i, row := range rows {
			ids[i] = row.ID()
		}
		slices.Sort(ids)
		if err != nil || !slices.Equal(ids, want) || native.Load() != 2 || meter.Work().ShardOpens != 2 {
			t.Fatalf("healthy retry at %+v = %v, %v, %d native opens, work %+v; want %v with both opens charged", budget, ids, err, native.Load(), meter.Work(), want)
		}
	}
}
