package core

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	shardedpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RH list R2, DecodedBytes: the stored bytes a metered door hands to a row
// decoder, counted by the store before the decode. The tests run on the disk
// backends that decode (badger, sharded, tiered): they reopen the store once,
// so its entity cache starts empty, and evict rows between runs to read them
// cold again; a warm read decodes nothing and counts 0.

// rbdBackend opens one disk backend on dir; shut closes it.
type rbdBackend struct {
	name string
	open func(t testing.TB, dir string) (g *Core, shut func())
}

func rbdBackends() []rbdBackend {
	newCore := func(t testing.TB, cfg Config) *Core {
		t.Helper()
		cfg.UntimedValidStart = UntimedFromTxFrom
		g, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return g
	}
	return []rbdBackend{
		{"badger", func(t testing.TB, dir string) (*Core, func()) {
			g := newCore(t, Config{BadgerDir: dir})
			return g, func() { _ = g.Close() }
		}},
		{"sharded", func(t testing.TB, dir string) (*Core, func()) {
			st, err := shardedpkg.New(shardedpkg.Config{Dir: dir, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				t.Fatalf("sharded.New: %v", err)
			}
			g := newCore(t, Config{Store: st})
			return g, func() { _ = g.Close(); _ = st.Close() }
		}},
		{"tiered", func(t testing.TB, dir string) (*Core, func()) {
			ts, err := tiered.New(tiered.Config{
				DataDir:           dir,
				RefLabels:         []string{"Ref"},
				ShardWindow:       7 * 24 * time.Hour,
				FlushInterval:     1<<63 - 1,
				MaxOpenColdShards: 4,
			})
			if err != nil {
				t.Fatalf("tiered.New: %v", err)
			}
			g := newCore(t, Config{Store: ts})
			return g, func() { _ = g.Close(); _ = ts.Close() }
		}},
	}
}

// rbdEvict drops the entity-cache entries of the given nodes and
// relationships (clean after a reopen), so the next read decodes them again.
func rbdEvict(t testing.TB, g *Core, nodes []types.NodeID, rels []types.RelID) {
	t.Helper()
	evict := func(bs *badger.Store, err error, node bool, id int64) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		if node {
			bs.NodeCacheForTest().EvictForTest(types.NodeID(id).SnowflakeID())
		} else {
			bs.RelCacheForTest().EvictForTest(types.RelID(id).SnowflakeID())
		}
	}
	for _, id := range nodes {
		switch st := g.store.(type) {
		case *badger.Store:
			evict(st, nil, true, int64(id))
		case *shardedpkg.Store:
			bs, err := st.ShardForIDForTest(id.SnowflakeID())
			evict(bs, err, true, int64(id))
		case *tiered.Store:
			bs, err := st.ShardForNodeIDForTest(id)
			evict(bs, err, true, int64(id))
		}
	}
	for _, id := range rels {
		switch st := g.store.(type) {
		case *badger.Store:
			evict(st, nil, false, int64(id))
		case *shardedpkg.Store:
			bs, err := st.ShardForIDForTest(id.SnowflakeID())
			evict(bs, err, false, int64(id))
		case *tiered.Store:
			bs, err := st.ShardForRelIDForTest(id)
			evict(bs, err, false, int64(id))
		}
	}
}

// rbdBudgetErr unwraps a read budget error and checks the field it stopped at.
func rbdBudgetErr(t *testing.T, err error, limit string) *storepkg.ReadBudgetError {
	t.Helper()
	var be *storepkg.ReadBudgetError
	if !errors.Is(err, storepkg.ErrReadBudget) || !errors.As(err, &be) || be.Limit != limit {
		t.Fatalf("error %v, want ErrReadBudget at %q", err, limit)
	}
	return be
}

// rbdExact runs one read cold under three budgets and checks the contract:
// unlimited, it counts (the same on every cold run, at least min bytes);
// limited to exactly that count it answers as before; limited to one byte less
// it fails with ErrReadBudget "decoded bytes" from door, returning no rows
// (ok false), after counting past the limit and not at all after the refusal.
// cold evicts the rows; run reads under ctx and reports whether it answered.
func rbdExact(t *testing.T, what, door string, min uint64, cold func(), run func(ctx context.Context) (ok bool, err error)) uint64 {
	t.Helper()
	cold()
	ctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
	if ok, err := run(ctx); err != nil || !ok {
		t.Fatalf("%s: cold = %v, %v", what, ok, err)
	}
	first := meter.Work().DecodedBytes
	if first < min {
		t.Fatalf("%s: cold counted %d decoded bytes, want at least %d", what, first, min)
	}
	cold()
	ctx, meter = storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{DecodedBytes: first})
	if ok, err := run(ctx); err != nil || !ok || meter.Work().DecodedBytes != first {
		t.Fatalf("%s: budget %d = %v, %v, %+v; want the answer and the same count", what, first, ok, err, meter.Work())
	}
	cold()
	ctx, meter = storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{DecodedBytes: first - 1})
	ok, err := run(ctx)
	be := rbdBudgetErr(t, err, "decoded bytes")
	if ok || be.Door != door || be.Work.DecodedBytes <= first-1 || be.Work.DecodedBytes > first || meter.Work() != be.Work {
		t.Fatalf("%s: budget %d: answered %v, %+v, meter %+v; want no rows from %s, work in (%d, %d] and none after the refusal", what, first-1, ok, be, meter.Work(), door, first-1, first)
	}
	return first
}

// TestReadBudget_DecodedBytes_RelTimelines: RelEffectiveTimelinesByProperties
// counts the stored bytes of the current rows and history versions it
// decodes. Cold, the read counts them; within a budget of exactly that count
// it answers as without one; one byte under it returns ErrReadBudget "decoded
// bytes" and no rows, and nothing is decoded after the refusal. Warm, the
// current rows come from the entity cache and decode nothing: 0 bytes for
// relationships without history.
func TestReadBudget_DecodedBytes_RelTimelines(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			w := newRTBWorld(t, g)
			w.index(t, rtbType, rtbKeys...)
			const rows = 8
			var ids []types.RelID
			for i := range rows {
				ids = append(ids, w.add(t, fmt.Sprintf("r%02d", i), rtbType, map[string]any{"estate": "e1", "device": "d1", "pad": fmt.Sprintf("%0*d", 40+i, i)}))
			}
			pin := temporal.Pin{Tx: w.pin(t)}
			tuples := [][]any{{"e1", "d1"}}
			want := rtbRender(w, rtbCheck(t, w, "no budget", rtbKeys, tuples, pin))
			shut()

			g, shut = be.open(t, dir)
			defer shut()
			w.g = g
			rbdExact(t, "rel timelines", "RelEffectiveTimelinesByProperties", rows*40,
				func() { rbdEvict(t, g, nil, ids) },
				func(ctx context.Context) (bool, error) {
					got, err := g.Temporal.RelEffectiveTimelinesByProperties(ctx, rtbType, rtbKeys, tuples, pin)
					if err != nil {
						return false, err
					}
					return rtbRender(w, got) == want, nil
				})
			// Warm: a full read caches the rows; the next decodes nothing, so a
			// one-byte budget holds.
			if got, err := g.Temporal.RelEffectiveTimelinesByProperties(context.Background(), rtbType, rtbKeys, tuples, pin); err != nil || rtbRender(w, got) != want {
				t.Fatalf("warming read = %s, %v", rtbRender(w, got), err)
			}
			ctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{DecodedBytes: 1})
			got, err := g.Temporal.RelEffectiveTimelinesByProperties(ctx, rtbType, rtbKeys, tuples, pin)
			if err != nil || rtbRender(w, got) != want || meter.Work().DecodedBytes != 0 {
				t.Fatalf("warm run = %s, %v, %+v; want the same answer and 0 decoded bytes", rtbRender(w, got), err, meter.Work())
			}
		})
	}
}

// TestReadBudget_DecodedBytes_RelTimelinesHistory: a relationship with history
// decodes its history skeleton and winning versions on top of its current row,
// and the budget counts them: with the current rows cached, a read still
// decodes the history, and a limit under it refuses.
func TestReadBudget_DecodedBytes_RelTimelinesHistory(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			w := newRTBWorld(t, g)
			w.index(t, rtbType, rtbKeys...)
			for i := range 3 {
				id := w.add(t, fmt.Sprintf("h%d", i), rtbType, map[string]any{"estate": "e1", "device": "d1", "pad": fmt.Sprintf("%0*d", 200, i)})
				for v := range 3 {
					if _, err := g.Rels.Update(w.ctx, id, map[string]any{"v": int64(v)}); err != nil {
						t.Fatal(err)
					}
				}
			}
			pin := temporal.Pin{Tx: w.pin(t)}
			tuples := [][]any{{"e1", "d1"}}
			want := rtbRender(w, rtbCheck(t, w, "no budget", rtbKeys, tuples, pin))
			shut()

			g, shut = be.open(t, dir)
			defer shut()
			w.g = g
			run := func(ctx context.Context) (bool, error) {
				got, err := g.Temporal.RelEffectiveTimelinesByProperties(ctx, rtbType, rtbKeys, tuples, pin)
				if err != nil {
					return false, err
				}
				return rtbRender(w, got) == want, nil
			}
			// A first read caches the current rows; later reads decode only the
			// history rows, and that is counted (and refused one byte under).
			if ok, err := run(context.Background()); err != nil || !ok {
				t.Fatalf("no budget = %v, %v", ok, err)
			}
			rbdExact(t, "rel history", "RelEffectiveTimelinesByProperties", 1, func() {}, run)
		})
	}
}

// TestReadBudget_DecodedBytes_NoBudgetUnchanged: a budget without a DecodedBytes
// limit never refuses on bytes, whatever the rows weigh, and counts them.
func TestReadBudget_DecodedBytes_NoBudgetUnchanged(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			w := newRTBWorld(t, g)
			w.index(t, rtbType, rtbKeys...)
			w.add(t, "a", rtbType, map[string]any{"estate": "e1", "device": "d1", "pad": fmt.Sprintf("%0*d", 5000, 1)})
			pin := temporal.Pin{Tx: w.pin(t)}
			shut()
			g, shut = be.open(t, dir)
			defer shut()
			ctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: 1, Segments: 1})
			got, err := g.Temporal.RelEffectiveTimelinesByProperties(ctx, rtbType, rtbKeys, [][]any{{"e1", "d1"}}, pin)
			if err != nil || len(got) != 1 || len(got[0]) != 1 || meter.Work().DecodedBytes < 5000 {
				t.Fatalf("unlimited bytes = %v, %v, %+v", got, err, meter.Work())
			}
		})
	}
}
