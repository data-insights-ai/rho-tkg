package graph_test

import (
	"errors"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerstore "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

func damagePublicOpenImage(t *testing.T, dir, damage string, id types.NodeID) {
	t.Helper()
	db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(txn *badgerv4.Txn) error {
		if damage == "absent" {
			return badgerstore.DeleteOpenStateForTest(txn)
		}
		// Rewriting the same row advances its version. The old delta open
		// scans and decodes that row although its answer stays unchanged.
		key := storeutil.NodeKey(id.SnowflakeID())
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return txn.Set(key, val)
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPublicReadBudget_OpenImageRefusesRebuild(t *testing.T) {
	// Serial: the native row decoder hook is process-wide.
	for _, damage := range []string{"absent", "dirty"} {
		for _, archive := range []bool{false, true} {
			name := "cold/" + damage
			if archive {
				name = "archive/" + damage
			}
			t.Run(name, func(t *testing.T) {
				var g *graphpkg.Graph
				var ts *tiered.Store
				var want []types.NodeID
				var dir string
				var closeLazy func()
				if archive {
					cfg := tiered.Config{DataDir: t.TempDir(), RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1}
					open := func() {
						var err error
						ts, err = tiered.New(cfg)
						if err != nil {
							t.Fatal(err)
						}
						g, err = graphpkg.New(graphpkg.Config{Store: ts})
						if err != nil {
							t.Fatal(err)
						}
					}
					open()
					for range 3 {
						n, err := g.Nodes().Add(t.Context(), []string{"Case"}, nil)
						if err != nil {
							t.Fatal(err)
						}
						want = append(want, n.ID())
						if err := ts.ArchiveNode(n.ID()); err != nil {
							t.Fatal(err)
						}
					}
					closeLazy = func() {
						if err := g.Close(); err != nil {
							t.Fatal(err)
						}
						open()
					}
					t.Cleanup(func() { _ = g.Close() })
					dir = filepath.Join(cfg.DataDir, "archive")
				} else {
					w := pmcBuild(t, 0)
					g, ts = w.g, w.ts
					want = []types.NodeID{types.NodeID(w.nodes[0])}
					closeLazy = func() { w.closeCold(t) }
					// Match the live row to its shard while all fixtures are open.
					for _, es := range w.cold {
						if n, err := es.Store().GetNode(want[0]); err == nil && n != nil {
							dir = ts.ShardConfigForTest(es.Path(), true).Dir
							break
						}
					}
					if dir == "" {
						t.Fatal("node missing from cold fixtures")
					}
				}
				closeLazy()
				damagePublicOpenImage(t, dir, damage, want[0])
				var native atomic.Int64
				ts.SetShardOpenHookForTest(func(tiered.BadgerStoreConfig) { native.Add(1) })
				ctx, outer := storepkg.WithReadBudget(t.Context(), storepkg.ReadBudget{DecodedBytes: 1})
				ctx, inner := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
				var rows []*types.Node
				var err error
				decoded := badgerstore.CountRowDecodesForTest(func() { rows, err = g.Nodes().GetByIDsCtx(ctx, want) })
				var rb *storepkg.ReadBudgetError
				if len(rows) != 0 || !errors.As(err, &rb) || rb.Limit != "unsupported" || decoded != 0 || outer.Exceeded() == nil || inner.Exceeded() == nil || inner.Work().ShardOpens != uint64(native.Load()) {
					t.Fatalf("damaged lazy image = %d rows, %v, %d native decodes/%d opens, work %+v; want shared refusal before rebuild", len(rows), err, decoded, native.Load(), inner.Work())
				}
				if rows, err := g.Nodes().GetByIDs(want); err != nil || len(rows) != len(want) {
					t.Fatalf("plain repair = %d rows, %v", len(rows), err)
				}
				// A plain cold RO read can rebuild in RAM without persisting an
				// image. Reopen the closed directory writable to finish repair.
				closeLazy()
				healCfg := ts.ShardConfigForTest(filepath.Base(dir), false)
				healCfg.Dir = dir
				healed, err := badgerstore.New(healCfg)
				if err != nil {
					t.Fatal(err)
				}
				if err := healed.Close(); err != nil {
					t.Fatal(err)
				}
				opens := uint64(1)
				if archive {
					opens = 2
				}
				for _, budget := range []storepkg.ReadBudget{{}, {ShardOpens: opens}} {
					closeLazy()
					native.Store(0)
					ts.SetShardOpenHookForTest(func(tiered.BadgerStoreConfig) { native.Add(1) })
					ctx, meter := storepkg.WithReadBudget(t.Context(), budget)
					rows, err := g.Nodes().GetByIDsCtx(ctx, want)
					ids := make([]types.NodeID, len(rows))
					for i, row := range rows {
						ids[i] = row.ID()
					}
					slices.Sort(ids)
					if err != nil || !slices.Equal(ids, want) || native.Load() != int64(opens) || meter.Work().ShardOpens != opens {
						t.Fatalf("healthy retry %+v = %v, %v, %d native opens, work %+v", budget, ids, err, native.Load(), meter.Work())
					}
				}
			})
		}
	}
}
