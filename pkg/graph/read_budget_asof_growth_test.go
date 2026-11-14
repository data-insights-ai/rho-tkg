package graph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// A selective acceptance read revisits the same head at its genesis, first
// publication and current pins. Adding later publications must not load their
// histories to recover those same old rows. The allowance comes from a real
// nonempty two-publication control; native counters observe the eight-publication
// read independently of the meter. Every write is flushed before observation.
func TestReadBudgetPublicAsOfHistoryGrowth(t *testing.T) {
	for _, backend := range []string{"badger", "sharded", "tiered"} {
		for _, kind := range []string{"node", "rel"} {
			t.Run(backend+"/"+kind, func(t *testing.T) {
				type result struct {
					work                     store.ReadWork
					decodes, visits, pending int64
					rows                     int
					err                      error
				}
				run := func(cuts int, budget store.ReadBudget) result {
					t.Helper()
					var st store.MandatoryStore
					var err error
					switch backend {
					case "badger":
						st, err = badger.New(badger.Config{InMemory: true, MaxPendingWrites: -1})
					case "sharded":
						st, err = sharded.New(sharded.Config{Dir: t.TempDir(), BaseSlot: 0, SlotCount: 2})
					case "tiered":
						st, err = tiered.New(tiered.Config{DataDir: t.TempDir(), RefLabels: []string{"Case"}, ShardWindow: time.Hour, FlushInterval: time.Hour})
					}
					if err != nil {
						t.Fatal(err)
					}
					defer st.Close()
					g, err := graph.New(graph.Config{Store: st})
					if err != nil {
						t.Fatal(err)
					}
					defer g.Close()
					a, err := g.Nodes().Add(t.Context(), []string{"Case"}, map[string]any{"revision": int64(0)})
					if err != nil {
						t.Fatal(err)
					}
					z, err := g.Nodes().Add(t.Context(), []string{"Case"}, nil)
					if err != nil {
						t.Fatal(err)
					}
					r, err := g.Rels().Add(t.Context(), "R", a, z, map[string]any{"revision": int64(0)})
					if err != nil {
						t.Fatal(err)
					}
					pins := make([]types.Instant, cuts+1)
					pins[0], err = g.Temporal().NowTx()
					if err != nil {
						t.Fatal(err)
					}
					for v := 1; v <= cuts; v++ {
						if kind == "node" {
							_, err = g.Nodes().Update(t.Context(), a.ID(), map[string]any{"revision": int64(v)})
						} else {
							_, err = g.Rels().Update(t.Context(), r.ID(), map[string]any{"revision": int64(v)})
						}
						if err != nil {
							t.Fatal(err)
						}
						pins[v], err = g.Temporal().NowTx()
						if err != nil {
							t.Fatal(err)
						}
					}
					if err := st.(interface{ Flush() error }).Flush(); err != nil {
						t.Fatal(err)
					}
					read := func(ctx context.Context, v int) error {
						if kind == "node" {
							n, err := g.Temporal().NodeAsOfCtx(ctx, a.ID(), pins[v])
							if err != nil {
								if n != nil {
									t.Fatal("failed node point read returned a row")
								}
								return err
							}
							if n == nil || n.ID() != a.ID() || n.Version() != uint32(v) || n.PropertiesMap()["revision"] != int64(v) {
								return fmt.Errorf("wrong node at revision %d: %v", v, n)
							}
						} else {
							x, err := g.Temporal().RelAsOfCtx(ctx, r.ID(), pins[v])
							if err != nil {
								if x != nil {
									t.Fatal("failed relationship point read returned a row")
								}
								return err
							}
							if x == nil || x.ID() != r.ID() || x.Version() != uint32(v) || x.PropertiesMap()["revision"] != int64(v) {
								return fmt.Errorf("wrong relationship at revision %d: %v", v, x)
							}
						}
						return nil
					}
					ctx, meter := store.WithReadBudget(t.Context(), budget)
					visits, stopVisits := badger.CountIndexEntriesVisitedForTest()
					defer stopVisits()
					pending, stopPending := badger.CountPendingKeysVisitedForTest()
					defer stopPending()
					var got result
					got.decodes = badger.CountRowDecodesForTest(func() {
						for _, v := range []int{0, cuts, 1, cuts, 1} {
							if got.err = read(ctx, v); got.err != nil {
								return
							}
							got.rows++
						}
					})
					got.work, got.visits, got.pending = meter.Work(), visits(), pending()
					return got
				}
				control := run(2, store.ReadBudget{})
				if control.err != nil || control.rows != 5 || control.decodes == 0 {
					t.Fatalf("nonempty control: %+v", control)
				}
				wide := run(8, store.ReadBudget{})
				if wide.err != nil || wide.rows != 5 || wide.decodes == 0 {
					t.Fatalf("eight-cut unlimited control: %+v; error %v", wide, wide.err)
				}
				w := control.work
				budget := store.ReadBudget{Candidates: max(w.Candidates, 1), Segments: max(w.Segments, 1), ShardOpens: max(w.ShardOpens, 1), DecodedBytes: max(w.DecodedBytes, 1), HistoryExamined: max(w.HistoryExamined, 1), BufferBytes: max(w.BufferBytes, 1)}
				later := run(8, budget)
				var refusal *store.ReadBudgetError
				t.Logf("two cuts %+v; eight-cut unlimited %+v; eight-cut limited %+v; typed refusal %t error %v", control, wide, later, errors.As(later.err, &refusal), later.err)
				if later.err != nil || later.rows != 5 || later.decodes > control.decodes || later.visits > control.visits || later.pending != 0 {
					t.Fatalf("later revisions changed selective read cost: control %+v; later %+v; error %v", control, later, later.err)
				}
				// The consumer route must still refuse one unit before completing
				// these same reads; an acceleration may not bypass the meter.
				if backend == "tiered" {
					for _, dim := range []struct {
						name   string
						amount uint64
						budget store.ReadBudget
					}{
						{"candidates", w.Candidates, store.ReadBudget{Candidates: w.Candidates - 1}},
						{"history examined", w.HistoryExamined, store.ReadBudget{HistoryExamined: w.HistoryExamined - 1}},
						{"decoded bytes", w.DecodedBytes, store.ReadBudget{DecodedBytes: w.DecodedBytes - 1}},
						{"buffer bytes", w.BufferBytes, store.ReadBudget{BufferBytes: w.BufferBytes - 1}},
					} {
						if dim.amount <= 1 {
							continue
						}
						limited := run(2, dim.budget)
						var rb *store.ReadBudgetError
						if !errors.As(limited.err, &rb) || rb.Limit != dim.name || limited.rows >= 5 || limited.work != rb.Work || limited.decodes > control.decodes || limited.visits > control.visits {
							t.Fatalf("one under %s: %+v; error %v", dim.name, limited, limited.err)
						}
					}
				}
			})
		}
	}
}

// An unsynced staged apply can retain a newer history row while losing its
// proof update and format marker. Reopen must invalidate the surviving stale
// proof before either public point door can use it.
func TestReadBudgetAsOfReopenInvalidatesPartialIndex(t *testing.T) {
	cfg := badger.Config{Dir: t.TempDir(), MemTableSize: 8 << 20, FlushInterval: time.Hour}
	bs, err := badger.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g, err := graph.New(graph.Config{Store: bs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	n, err := g.Nodes().Add(t.Context(), []string{"Case"}, map[string]any{"revision": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	z, err := g.Nodes().Add(t.Context(), []string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels().Add(t.Context(), "R", n, z, map[string]any{"revision": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	for v := int64(1); v <= 2; v++ {
		n, err = g.Nodes().Update(t.Context(), n.ID(), map[string]any{"revision": v})
		if err != nil {
			t.Fatal(err)
		}
		r, err = g.Rels().Update(t.Context(), r.ID(), map[string]any{"revision": v})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	nodePin, relPin := n.Temporal().TxFrom, r.Temporal().TxFrom
	entries := []struct{ key, seek, proof []byte }{
		{key: storeutil.HistoryAsOfProofKey(storeutil.KeyHistNode, n.ID().SnowflakeID()), seek: storeutil.HistoryAsOfSeekKey(storeutil.KeyHistNode, n.ID().SnowflakeID(), uint64(nodePin), 2)},
		{key: storeutil.HistoryAsOfProofKey(storeutil.KeyHistRel, r.ID().SnowflakeID()), seek: storeutil.HistoryAsOfSeekKey(storeutil.KeyHistRel, r.ID().SnowflakeID(), uint64(relPin), 2)},
	}
	if err := bs.DBForTest().View(func(txn *badgerv4.Txn) error {
		for i := range entries {
			item, err := txn.Get(entries[i].key)
			if err != nil {
				return err
			}
			entries[i].proof, err = item.ValueCopy(nil)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes().Update(t.Context(), n.ID(), map[string]any{"revision": int64(3)}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels().Update(t.Context(), r.ID(), map[string]any{"revision": int64(3)}); err != nil {
		t.Fatal(err)
	}
	check := func() {
		t.Helper()
		ctx, _ := store.WithReadBudget(t.Context(), store.ReadBudget{})
		gotNode, err := g.Temporal().NodeAsOfCtx(ctx, n.ID(), nodePin)
		if err != nil || gotNode == nil || gotNode.Version() != 2 || gotNode.PropertiesMap()["revision"] != int64(2) {
			t.Fatalf("old node pin after third update: %v, %v", gotNode, err)
		}
		gotRel, err := g.Temporal().RelAsOfCtx(ctx, r.ID(), relPin)
		if err != nil || gotRel == nil || gotRel.Version() != 2 || gotRel.PropertiesMap()["revision"] != int64(2) {
			t.Fatalf("old relationship pin after third update: %v, %v", gotRel, err)
		}
	}
	check()
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := badgerv4.Open(badgerv4.DefaultOptions(cfg.Dir).WithMemTableSize(cfg.MemTableSize).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(txn *badgerv4.Txn) error {
		for _, e := range entries {
			if err := txn.Set(e.key, e.proof); err != nil {
				return err
			}
			if err := txn.Delete(e.seek); err != nil {
				return err
			}
		}
		return txn.Set(storeutil.WireFormatVersionKey, []byte{0, storeutil.HistoryAsOfStoreFormatVersion - 1})
	})
	if err := errors.Join(err, db.Close()); err != nil {
		t.Fatal(err)
	}
	bs, err = badger.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g, err = graph.New(graph.Config{Store: bs})
	if err != nil {
		_ = bs.Close()
		t.Fatal(err)
	}
	check()
}
