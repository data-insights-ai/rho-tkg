package core

import (
	"context"
	"errors"
	"math"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestCASComparisonReadBudget(t *testing.T) {
	// The native decoder counter is process-wide; do not run in parallel.
	for _, be := range rbdBackendsWith(func(c *Config) { c.UntimedValidStart = UntimedWholeAxis }) {
		t.Run(be.name, func(t *testing.T) {
			g, shut := be.open(t, t.TempDir())
			defer shut()
			ctx := t.Context()
			a, err := g.Nodes.Add(ctx, []string{"Ref"}, map[string]any{"epoch": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			b, err := g.Nodes.Add(ctx, []string{"Event"}, map[string]any{"epoch": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels.Add(ctx, "LINK", a, b, map[string]any{"epoch": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes.Update(ctx, a.ID(), map[string]any{"epoch": int64(2)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"epoch": int64(2)}); err != nil {
				t.Fatal(err)
			}
			if err := g.store.(interface{ Flush() error }).Flush(); err != nil {
				t.Fatal(err)
			}
			for _, kind := range []string{"node", "relationship"} {
				t.Run(kind, func(t *testing.T) {
					run := func(budget storepkg.ReadBudget) (bool, int64, storepkg.ReadWork, error) {
						rbdEvict(t, g, []types.NodeID{a.ID(), b.ID()}, []types.RelID{r.ID()})
						ctx, meter := storepkg.WithReadBudget(ctx, budget)
						var matched bool
						var err error
						decoded := badger.CountRowDecodesForTest(func() {
							if kind == "node" {
								matched, err = g.Nodes.CompareAndSetProperty(ctx, a.ID(), "epoch", int64(0), int64(3))
							} else {
								matched, err = g.Rels.CompareAndSetProperty(ctx, r.ID(), "epoch", int64(0), int64(3))
							}
						})
						return matched, decoded, meter.Work(), err
					}
					matched, decoded, unlimited, err := run(storepkg.ReadBudget{})
					if matched || err != nil || decoded == 0 {
						t.Fatalf("non-empty mismatch control: match=%v err=%v native decodes=%d work=%+v", matched, err, decoded, unlimited)
					}
					matched, limitedDecodes, work, err := run(storepkg.ReadBudget{DecodedBytes: 1})
					var refusal *storepkg.ReadBudgetError
					if matched || !errors.As(err, &refusal) || refusal.Limit != "decoded bytes" || limitedDecodes != 0 || work != refusal.Work {
						t.Fatalf("comparison bypassed admission: match=%v err=%v native decodes=%d work=%+v; unlimited decodes=%d work=%+v", matched, err, limitedDecodes, work, decoded, unlimited)
					}
				})
			}
		})
	}
}

func TestCASComparisonHistoryAdmission(t *testing.T) {
	t.Parallel()
	st := memory.New()
	g, err := New(Config{Store: st, UntimedValidStart: UntimedWholeAxis})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	a, err := g.Nodes.Add(t.Context(), []string{"Head"}, map[string]any{"epoch": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(t.Context(), []string{"Other"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rel, err := g.Rels.Add(t.Context(), "LINK", a, b, map[string]any{"epoch": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 2; i++ {
		if _, err := g.Nodes.Update(t.Context(), a.ID(), map[string]any{"epoch": i}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels.Update(t.Context(), rel.ID(), map[string]any{"epoch": i}); err != nil {
			t.Fatal(err)
		}
	}
	before := g.lastInstant.Load()
	for _, kind := range []string{"node", "relationship"} {
		t.Run(kind, func(t *testing.T) {
			run := func(budget storepkg.ReadBudget) (storepkg.ReadWork, int64, error) {
				ctx, meter := storepkg.WithReadBudget(t.Context(), budget)
				start := st.HistoryMetasListedForTest()
				var matched bool
				var err error
				if kind == "node" {
					matched, err = g.Nodes.CompareAndSetProperty(ctx, a.ID(), "epoch", int64(-1), int64(3))
				} else {
					matched, err = g.Rels.CompareAndSetProperty(ctx, rel.ID(), "epoch", int64(-1), int64(3))
				}
				if matched {
					t.Fatal("mismatching CAS wrote payload")
				}
				return meter.Work(), st.HistoryMetasListedForTest() - start, err
			}
			control, copies, err := run(storepkg.ReadBudget{})
			if err != nil || copies != 2 || control.HistoryExamined != 2 || control.BufferBytes == 0 {
				t.Fatalf("history control: err=%v work=%+v native copies=%d", err, control, copies)
			}
			exact, copies, err := run(storepkg.ReadBudget{HistoryExamined: 2, BufferBytes: control.BufferBytes})
			if err != nil || exact != control || copies != 2 {
				t.Fatalf("exact history budget: err=%v work=%+v native copies=%d", err, exact, copies)
			}
			_, copies, err = run(storepkg.ReadBudget{HistoryExamined: 1})
			if !errors.Is(err, storepkg.ErrReadBudget) || copies != 0 {
				t.Fatalf("history refusal read before admission: err=%v native copies=%d", err, copies)
			}
			_, copies, err = run(storepkg.ReadBudget{BufferBytes: control.BufferBytes - 1})
			if !errors.Is(err, storepkg.ErrReadBudget) || copies != 1 {
				t.Fatalf("buffer refusal copied second skeleton: err=%v native copies=%d", err, copies)
			}
		})
	}
	if after := g.lastInstant.Load(); after != before {
		t.Fatalf("comparison/refusal changed clock: %d -> %d", before, after)
	}
}

func TestCASComparisonEndpointRetrySharesAllowance(t *testing.T) {
	t.Parallel()
	for _, limit := range []uint64{0, 4, 3} {
		g, probe, id := newReusedRelEndpointProbeFixture(t)
		ctx, meter := storepkg.WithReadBudget(t.Context(), storepkg.ReadBudget{Candidates: limit})
		matched, err := g.Rels.CompareAndSetProperty(ctx, id, "weight", nil, int64(7))
		if limit == 3 {
			if matched || !errors.Is(err, storepkg.ErrReadBudget) || probe.gets.Load() != 3 || meter.Work().Candidates != 4 {
				t.Fatalf("retry limit: matched=%v err=%v native reads=%d work=%+v", matched, err, probe.gets.Load(), meter.Work())
			}
			// A fresh call uses the real current endpoints and proves the
			// refused locked re-read released all three entity locks.
			matched, err = g.Rels.CompareAndSetProperty(t.Context(), id, "weight", nil, int64(7))
		} else if probe.gets.Load() != 4 || meter.Work().Candidates != 4 {
			t.Fatalf("retry control lost a native read: reads=%d work=%+v", probe.gets.Load(), meter.Work())
		}
		if !matched || err != nil {
			t.Fatalf("CAS after endpoint change: matched=%v err=%v", matched, err)
		}
		assertReusedRelEndpointProbe(t, probe)
	}
}

func TestCASComparisonUnsupportedAndInputGates(t *testing.T) {
	t.Parallel()
	st := &cancelOnGetNodeStore{Store: memory.New()}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	node, err := g.Nodes.Add(t.Context(), []string{"Head"}, map[string]any{"epoch": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	st.cancel = cancel // called only if the underlying comparison read happens
	ctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{DecodedBytes: 1})
	matched, err := g.Nodes.CompareAndSetProperty(ctx, node.ID(), "epoch", int64(0), int64(2))
	var refusal *storepkg.ReadBudgetError
	if matched || !errors.As(err, &refusal) || refusal.Limit != "unsupported" || st.canceled || meter.Work() != (storepkg.ReadWork{}) {
		t.Fatalf("unsupported wrapper read before admission: matched=%v err=%v read=%v work=%+v", matched, err, st.canceled, meter.Work())
	}
	_, err = g.Nodes.CompareAndSetProperty(ctx, node.ID(), "epoch", make(chan int), int64(2))
	if err == nil || errors.Is(err, storepkg.ErrReadBudget) || st.canceled {
		t.Fatalf("invalid input lost validation precedence: %v, read=%v", err, st.canceled)
	}
	cancel()
	_, err = g.Nodes.CompareAndSetProperty(ctx, node.ID(), "epoch", int64(1), int64(2))
	if !errors.Is(err, context.Canceled) || st.canceled {
		t.Fatalf("canceled comparison read: err=%v read=%v", err, st.canceled)
	}
}

func TestCASComparisonShardAdmission(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"node", "relationship"} {
		t.Run(kind, func(t *testing.T) {
			var w *sbTieredWorld
			if kind == "node" {
				w = sbBuildTiered(t)
			} else {
				w = r2BuildTiered(t)
			}
			for _, limit := range []uint64{0, 3, 2} {
				w.closeCold(t)
				start := w.opens()
				ctx, meter := storepkg.WithReadBudget(t.Context(), storepkg.ReadBudget{ShardOpens: limit})
				completed := 0
				var err error
				for _, i := range []int{0, 2, 4} {
					var matched bool
					if kind == "node" {
						matched, err = w.g.Nodes.CompareAndSetProperty(ctx, w.nodes[i], "i", int64(-1), int64(9))
					} else {
						matched, err = w.g.Rels.CompareAndSetProperty(ctx, w.rels[i], "i", int64(-1), int64(9))
					}
					if matched {
						t.Fatal("mismatch unexpectedly wrote a row")
					}
					if err != nil {
						break
					}
					completed++
				}
				opened := w.opens() - start
				if meter.Work().ShardOpens != 3 {
					t.Fatalf("three-key allowance restarted: %+v", meter.Work())
				}
				if limit == 2 {
					if !errors.Is(err, storepkg.ErrReadBudget) || completed != 2 || opened != 2 {
						t.Fatalf("refusal: completed=%d native opens=%d error=%v", completed, opened, err)
					}
				} else if err != nil || completed != 3 || opened != 3 {
					t.Fatalf("control: completed=%d native opens=%d error=%v", completed, opened, err)
				}
			}
		})
	}
}

func TestCASComparisonVersionCeilingParity(t *testing.T) {
	t.Parallel()
	g, err := New(Config{Store: memory.New(), UntimedValidStart: UntimedWholeAxis})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	node, err := g.Nodes.Add(t.Context(), []string{"Head"}, map[string]any{"epoch": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	end, err := g.Nodes.Add(t.Context(), []string{"End"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rel, err := g.Rels.Add(t.Context(), "LINK", node, end, map[string]any{"epoch": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	forceStoredNodeVersion(t, g, node.ID(), math.MaxUint32-1)
	forceStoredRelVersion(t, g, rel.ID(), math.MaxUint32-1)
	// Imported history can reach the terminal version while its belief stamp
	// is no newer than the current row. Preserve the plain door's ceiling rule.
	node.SetVersion(math.MaxUint32)
	rel.SetVersion(math.MaxUint32)
	if err := g.store.PutNodeVersion(node.ID(), math.MaxUint32, node); err != nil {
		t.Fatal(err)
	}
	if err := g.store.PutRelVersion(rel.ID(), math.MaxUint32, rel); err != nil {
		t.Fatal(err)
	}
	before := g.lastInstant.Load()
	for _, tc := range []struct {
		name string
		cas  func(context.Context, any) (bool, error)
	}{
		{"node", func(ctx context.Context, expected any) (bool, error) {
			return g.Nodes.CompareAndSetProperty(ctx, node.ID(), "epoch", expected, int64(1))
		}},
		{"relationship", func(ctx context.Context, expected any) (bool, error) {
			return g.Rels.CompareAndSetProperty(ctx, rel.ID(), "epoch", expected, int64(1))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, expected := range []int64{-1, 1} {
				for _, budgeted := range []bool{false, true} {
					ctx := t.Context()
					if budgeted {
						ctx, _ = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{HistoryExamined: 1})
					}
					matched, err := tc.cas(ctx, expected)
					if err != nil || matched != (expected == 1) {
						t.Fatalf("expected=%d budgeted=%v: matched=%v err=%v", expected, budgeted, matched, err)
					}
				}
			}
		})
	}
	if after := g.lastInstant.Load(); after != before {
		t.Fatalf("mismatch/no-op changed clock: %d -> %d", before, after)
	}
}

func TestCASComparisonNegativeBeliefParity(t *testing.T) {
	t.Parallel()
	g, err := New(Config{Store: memory.New(), UntimedValidStart: UntimedWholeAxis})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	node, err := g.Nodes.Add(t.Context(), []string{"Head"}, map[string]any{"epoch": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	end, err := g.Nodes.Add(t.Context(), []string{"End"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rel, err := g.Rels.Add(t.Context(), "LINK", node, end, map[string]any{"epoch": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	// A supplied store may contain negative transaction stamps. The plain
	// whole-axis proof starts its newest-belief fold at zero, even with no history.
	node.Temporal().TxFrom = -1
	rel.Temporal().TxFrom = -1
	if err := g.store.ReplaceNode(node); err != nil {
		t.Fatal(err)
	}
	if err := g.store.ReplaceRelationship(rel); err != nil {
		t.Fatal(err)
	}
	for _, metered := range []bool{false, true} {
		ctx := t.Context()
		if metered {
			ctx, _ = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
		}
		matched, err := g.Nodes.CompareAndSetProperty(ctx, node.ID(), "epoch", int64(-1), int64(1))
		if matched || !errors.Is(err, ErrUntimedModeMismatch) {
			t.Fatalf("node metered=%v: matched=%v err=%v", metered, matched, err)
		}
		matched, err = g.Rels.CompareAndSetProperty(ctx, rel.ID(), "epoch", int64(-1), int64(1))
		if matched || !errors.Is(err, ErrUntimedModeMismatch) {
			t.Fatalf("relationship metered=%v: matched=%v err=%v", metered, matched, err)
		}
	}
}
