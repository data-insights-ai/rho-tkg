package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudget_ShardOpens_TieredDoors (READBUDGET step 6): on a tiered
// store whose cold shards are closed before the read, the budgeted doors
// count each closed shard they open (ShardOpens) before they open it: the
// current index fan-out of ByLabelAndPropertyCtx, its membership fan-out
// (TxPin and ValidAt), the whole
// chain history search of NodeAsOfCtx for a node live nowhere,
// RelEffectiveTimelinesByProperties, and the relationship by-ID doors
// (Rels().GetByIDsCtx over rows in three shards, HasHistoryCtx and
// LatestStampsCtx of a relationship live nowhere), and the property lookups
// Rels().ByTypeAndPropertyCtx and Nodes().IDsByLabelAndPropertyValuesCtx over
// three shards. A budget of 1 refuses with no rows and
// opens at most one shard (the store's ColdOpens counter); unlimited, the
// answer is the unbudgeted one and the meter counts exactly the opens.
func TestReadBudget_ShardOpens_TieredDoors(t *testing.T) {
	t.Parallel()
	ts, err := tiered.New(tiered.Config{
		DataDir:           t.TempDir(),
		RefLabels:         []string{"Ref"},
		ShardWindow:       7 * 24 * time.Hour,
		FlushInterval:     1<<63 - 1,
		MaxOpenColdShards: 8,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := New(Config{Store: ts})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close(); _ = ts.Close() })
	ctx := context.Background()
	if err := g.Index.CreateProperty("Ev", "k"); err != nil {
		t.Fatal(err)
	}
	w := &rtbWorld{g: g, ctx: ctx, names: map[types.RelID]string{}, byN: map[string]types.RelID{}, typ: map[types.RelID]string{}}
	w.index(t, rtbType, rtbKeys...)

	const shards = 3
	var gone types.NodeID // deleted in the first shard: live nowhere
	var goneRel types.RelID
	var liveRels []types.RelID // one per shard
	var beforeDelete types.Instant
	for i := range shards {
		n, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"k": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"v": int64(i)}); err != nil {
			t.Fatal(err)
		}
		s, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		e, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		w.s, w.e = s.ID(), e.ID()
		r := w.add(t, fmt.Sprintf("r%d", i), rtbType, map[string]any{"estate": "e1", "device": "d1"})
		if _, err := g.Rels.Update(ctx, r, map[string]any{"v": int64(i)}); err != nil {
			t.Fatal(err)
		}
		liveRels = append(liveRels, r)
		if i == 0 {
			goneRel = w.add(t, "gone", rtbType, map[string]any{"estate": "gone", "device": "gone"})
			if err := g.Rels.Delete(ctx, goneRel); err != nil {
				t.Fatal(err)
			}
			d, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"k": int64(2)})
			if err != nil {
				t.Fatal(err)
			}
			gone = d.ID()
			if beforeDelete, err = g.Temporal.NowTx(); err != nil {
				t.Fatal(err)
			}
			if err := g.Nodes.Delete(ctx, gone); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(2 * time.Millisecond)
		if err := ts.RotateHotShard(); err != nil {
			t.Fatal(err)
		}
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}

	// Every shard but the hot one goes cold; closeCold closes them all again
	// before each read.
	ts.MuForTest().RLock()
	hot := ts.HotShardForTest()
	var cold []*tiered.EventShard
	for _, es := range ts.EventShardsForTest() {
		if es != hot {
			cold = append(cold, es)
		}
	}
	ts.MuForTest().RUnlock()
	if len(cold) < shards {
		t.Fatalf("%d cold shards, want %d", len(cold), shards)
	}
	for _, es := range cold {
		demoteToCold(ts, es.Name())
	}
	closeCold := func() {
		for _, es := range cold {
			closeEventShardStore(t, es)
		}
	}

	nodeIDs := func(ns []*types.Node) string {
		if len(ns) == 0 {
			return ""
		}
		var ids []int64
		for _, n := range ns {
			ids = append(ids, int64(n.ID()))
		}
		slices.Sort(ids)
		return fmt.Sprint(ids)
	}
	doors := []struct {
		name, door string
		run        func(context.Context) (string, error)
	}{
		{"ByLabelAndProperty current", "ByLabelAndProperty", func(c context.Context) (string, error) {
			r, err := g.Nodes.ByLabelAndPropertyCtx(c, "Ev", "k", int64(1), storepkg.QueryOpts{})
			return nodeIDs(r), err
		}},
		{"ByLabelAndProperty TxPin", "ByLabelAndProperty", func(c context.Context) (string, error) {
			r, err := g.Nodes.ByLabelAndPropertyCtx(c, "Ev", "k", int64(1), storepkg.QueryOpts{TxPin: pin})
			if len(r) == 0 && err == nil {
				return "", errors.New("no rows")
			}
			return nodeIDs(r), err
		}},
		{"ByLabelAndProperty ValidAt", "ByLabelAndProperty", func(c context.Context) (string, error) {
			r, err := g.Nodes.ByLabelAndPropertyCtx(c, "Ev", "k", int64(1), storepkg.QueryOpts{ValidAt: pin})
			if len(r) == 0 && err == nil {
				return "", errors.New("no rows")
			}
			return nodeIDs(r), err
		}},
		{"NodeAsOf", "NodeAsOf", func(c context.Context) (string, error) {
			n, err := g.Temporal.NodeAsOfCtx(c, gone, beforeDelete)
			if n == nil {
				return "", err
			}
			return fmt.Sprintf("%d v%d", n.ID(), n.Version()), err
		}},
		{"LatestStamps", "LatestStamps", func(c context.Context) (string, error) {
			f, to, del, err := g.Nodes.LatestStampsCtx(c, gone)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%d %d %t", f, to, del), nil
		}},
		{"HasHistory", "HasHistory", func(c context.Context) (string, error) {
			has, err := g.Nodes.HasHistoryCtx(c, gone)
			if err != nil || !has {
				return "", err
			}
			return "has", nil
		}},
		{"Rels ByTypeAndProperty current", "ByTypeAndProperty", func(c context.Context) (string, error) {
			r, err := g.Rels.ByTypeAndPropertyCtx(c, rtbType, "estate", "e1", storepkg.QueryOpts{})
			if err != nil {
				return "", err
			}
			if len(r) == 0 {
				return "", errors.New("no rows")
			}
			var ids []int64
			for _, row := range r {
				ids = append(ids, int64(row.ID()))
			}
			slices.Sort(ids)
			return fmt.Sprint(ids), nil
		}},
		{"Rels ByTypeAndProperty TxPin", "ByTypeAndProperty", func(c context.Context) (string, error) {
			return relIDsString(g.Rels.ByTypeAndPropertyCtx(c, rtbType, "estate", "e1", storepkg.QueryOpts{TxPin: pin}))
		}},
		{"Rels ByTypeAndProperty ValidAt", "ByTypeAndProperty", func(c context.Context) (string, error) {
			return relIDsString(g.Rels.ByTypeAndPropertyCtx(c, rtbType, "estate", "e1", storepkg.QueryOpts{ValidAt: pin, ValidAtSet: true}))
		}},
		{"IDsByLabelAndPropertyValues current", "IDsByLabelAndPropertyValues", func(c context.Context) (string, error) {
			ids, err := g.Nodes.IDsByLabelAndPropertyValuesCtx(c, "Ev", "k", []any{int64(1)}, storepkg.QueryOpts{})
			if err != nil {
				return "", err
			}
			if len(ids) == 0 {
				return "", errors.New("no ids")
			}
			return fmt.Sprint(ids), nil
		}},
		{"Rels LatestStamps", "LatestStamps", func(c context.Context) (string, error) {
			f, to, del, err := g.Rels.LatestStampsCtx(c, goneRel)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%d %d %t", f, to, del), nil
		}},
		{"Rels HasHistory", "HasHistory", func(c context.Context) (string, error) {
			has, err := g.Rels.HasHistoryCtx(c, goneRel)
			if err != nil || !has {
				return "", err
			}
			return "has", nil
		}},
		{"Rels GetByIDs", "GetByIDs", func(c context.Context) (string, error) {
			r, err := g.Rels.GetByIDsCtx(c, liveRels)
			if err != nil {
				return "", err
			}
			var ids []int64
			for _, row := range r {
				ids = append(ids, int64(row.ID()))
			}
			return fmt.Sprint(ids), nil
		}},
		{"RelEffectiveTimelinesByProperties", "RelEffectiveTimelinesByProperties", func(c context.Context) (string, error) {
			got, err := g.Temporal.RelEffectiveTimelinesByProperties(c, rtbType, rtbKeys, [][]any{{"e1", "d1"}}, temporal.Pin{Tx: pin})
			if got == nil {
				return "", err
			}
			return rtbRender(w, got), err
		}},
	}
	for _, d := range doors {
		t.Run(d.name, func(t *testing.T) {
			closeCold()
			want, err := d.run(ctx)
			if err != nil || want == "" {
				t.Fatalf("%s without budget = %q, %v", d.name, want, err)
			}

			closeCold()
			opens := ts.ShardAccessStats().ColdOpens
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
			got, err := d.run(bctx)
			opened := ts.ShardAccessStats().ColdOpens - opens
			var rb *storepkg.ReadBudgetError
			if !errors.Is(err, storepkg.ErrReadBudget) || !errors.As(err, &rb) || rb.Limit != "shard opens" || got != "" {
				t.Fatalf("%s under ShardOpens 1 = %q, %v (work %+v, %d closed shards opened); want a shard-opens refusal and no rows", d.name, got, err, meter.Work(), opened)
			}
			if rb.Door != d.door {
				t.Errorf("%s refused by door %q, want %q", d.name, rb.Door, d.door)
			}
			if opened > 1 {
				t.Fatalf("%s under ShardOpens 1 opened %d closed shards", d.name, opened)
			}

			closeCold()
			opens = ts.ShardAccessStats().ColdOpens
			bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			got, err = d.run(bctx)
			opened = ts.ShardAccessStats().ColdOpens - opens
			if err != nil || got != want {
				t.Fatalf("%s unlimited = %q, %v; want %q", d.name, got, err, want)
			}
			if opened < 2 || meter.Work().ShardOpens != opened {
				t.Fatalf("%s unlimited opened %d closed shards, counted %d; want >= 2, all counted", d.name, opened, meter.Work().ShardOpens)
			}
		})
	}
}

// relIDsString renders a relationship door's answer as its sorted IDs; an
// empty answer is an error, so the doors above never compare two empty reads.
func relIDsString(r []*types.Relationship, err error) (string, error) {
	if err != nil {
		return "", err
	}
	if len(r) == 0 {
		return "", errors.New("no rows")
	}
	var ids []int64
	for _, row := range r {
		ids = append(ids, int64(row.ID()))
	}
	slices.Sort(ids)
	return fmt.Sprint(ids), nil
}

// TestReadBudget_ShardOpens_TieredFoldRefused: a temporal property read with
// no usable index folds the label's history over every shard, opening closed
// tiered shards it does not count; under a ShardOpens budget it refuses before
// the walk (Limit "unsupported") instead of opening them unbounded, and
// without one it answers; so does the relationship lookup of a type with no
// index.
func TestReadBudget_ShardOpens_TieredFoldRefused(t *testing.T) {
	t.Parallel()
	ts, err := tiered.New(tiered.Config{DataDir: t.TempDir(), ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1, MaxOpenColdShards: 4})
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(Config{Store: ts})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close(); _ = ts.Close() })
	ctx := context.Background()
	if _, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"u": int64(1)}); err != nil {
		t.Fatal(err)
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	opts := storepkg.QueryOpts{TxPin: pin}
	bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 100})
	got, err := g.Nodes.ByLabelAndPropertyCtx(bctx, "Ev", "u", int64(1), opts)
	var rb *storepkg.ReadBudgetError
	if !errors.As(err, &rb) || rb.Limit != "unsupported" || got != nil {
		t.Fatalf("fold under ShardOpens = %d rows, %v (%+v); want an unsupported refusal", len(got), err, meter.Work())
	}
	bctx, _ = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 100})
	if got, err := g.Nodes.ByLabelAndPropertyCtx(bctx, "Ev", "u", int64(1), opts); err != nil || len(got) != 1 {
		t.Fatalf("fold under Candidates = %d rows, %v; want 1", len(got), err)
	}

	// The relationship twin (R2, Sigma doors group D): a type with no index
	// folds every shard's history, so it refuses a ShardOpens budget the same
	// way and answers under a Candidates one.
	s, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	e, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels.AddByID(ctx, "FOLD", s.ID(), e.ID(), map[string]any{"u": int64(1)}); err != nil {
		t.Fatal(err)
	}
	if pin, err = g.Temporal.NowTx(); err != nil {
		t.Fatal(err)
	}
	opts = storepkg.QueryOpts{TxPin: pin}
	bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 100})
	rels, err := g.Rels.ByTypeAndPropertyCtx(bctx, "FOLD", "u", int64(1), opts)
	if !errors.As(err, &rb) || rb.Limit != "unsupported" || rb.Door != "ByTypeAndProperty" || rels != nil {
		t.Fatalf("rel fold under ShardOpens = %d rels, %v (%+v); want an unsupported refusal", len(rels), err, meter.Work())
	}
	bctx, _ = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 100})
	if got, err := g.Rels.ByTypeAndPropertyCtx(bctx, "FOLD", "u", int64(1), opts); err != nil || len(got) != 1 {
		t.Fatalf("rel fold under Candidates = %d rows, %v; want 1", len(got), err)
	}
}
