package graph_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// pmcWorld is a public graph over a tiered store with three cold event shards,
// each holding two nodes of label D and two relationships of type NET, the
// second of each deleted (an ever-member only), each with two updates.
type pmcWorld struct {
	g           *graphpkg.Graph
	ts          *tiered.Store
	pin         types.Instant
	cold        []*tiered.EventShard
	nodes, rels []int64
}

func pmcBuild(t *testing.T, maxOpenCold int) *pmcWorld {
	t.Helper()
	ts, err := tiered.New(tiered.Config{
		DataDir:           t.TempDir(),
		RefLabels:         []string{"Ref"},
		ShardWindow:       7 * 24 * time.Hour,
		FlushInterval:     1<<63 - 1,
		MaxOpenColdShards: maxOpenCold,
	})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: ts, UntimedValidStart: graphpkg.UntimedFromTxFrom})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close(); _ = ts.Close() })
	ctx := context.Background()
	s, err := g.Nodes().Add(ctx, []string{"Ref"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	w := &pmcWorld{g: g, ts: ts}
	var relToken uint16
	for i := range 3 {
		// The relationship entity routes by its start endpoint, so each
		// shard needs its own. Keep it separate from the nodes deleted below.
		e, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		for j := range 2 {
			n, err := g.Nodes().Add(ctx, []string{"D"}, map[string]any{"i": int64(i*2 + j)})
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().AddByID(ctx, "NET", e.ID(), s.ID(), map[string]any{"i": int64(i*2 + j)})
			if err != nil {
				t.Fatal(err)
			}
			relToken = r.TypeToken().Value()
			w.nodes = append(w.nodes, int64(n.ID()))
			w.rels = append(w.rels, int64(r.ID()))
			for v := range 2 {
				if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"v": int64(v)}); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"v": int64(v)}); err != nil {
					t.Fatal(err)
				}
			}
			if j == 1 {
				if err := g.Nodes().Delete(ctx, n.ID()); err != nil {
					t.Fatal(err)
				}
				if err := g.Rels().Delete(ctx, r.ID()); err != nil {
					t.Fatal(err)
				}
			}
		}
		time.Sleep(2 * time.Millisecond)
		if err := ts.RotateHotShard(); err != nil {
			t.Fatal(err)
		}
	}
	if w.pin, err = g.Temporal().NowTx(); err != nil {
		t.Fatal(err)
	}
	ts.MuForTest().Lock()
	hot := ts.HotShardForTest()
	for _, es := range ts.EventShardsForTest() {
		if es != hot {
			w.cold = append(w.cold, es)
		}
	}
	ts.MuForTest().Unlock()
	if len(w.cold) != 3 {
		t.Fatalf("fixture: %d cold shards, want 3", len(w.cold))
	}
	for _, es := range w.cold {
		// Validate while still warm, holding only the handle lock: after
		// cold demotion the zero-cap sweeper may immediately close it.
		es.LockShardMuForTest()
		st := es.Store()
		if st == nil {
			es.UnlockShardMuForTest()
			t.Fatalf("fixture: warm shard %s has no handle", es.Name())
		}
		n, err := st.RelCountByType(relToken)
		es.UnlockShardMuForTest()
		if err != nil || n != 1 {
			t.Fatalf("fixture: shard %s has %d current NET relationships, %v; want 1", es.Name(), n, err)
		}
	}
	ts.MuForTest().Lock()
	for _, es := range w.cold {
		es.SetTierForTest(tiered.TierCold)
		ts.CatalogForTest().UpdateShardTier(es.Name(), tiered.TierCold)
	}
	ts.MuForTest().Unlock()
	return w
}

// closeCold closes every cold shard without sealing its counts: the next
// count of it opens it.
func (w *pmcWorld) closeCold(t *testing.T) {
	t.Helper()
	for _, es := range w.cold {
		es.LockShardMuForTest()
		if st := es.Store(); st != nil {
			if err := st.Close(); err != nil {
				es.UnlockShardMuForTest()
				t.Fatal(err)
			}
			es.SetStoreForTest(nil)
		}
		es.UnlockShardMuForTest()
	}
}

func (w *pmcWorld) opens() uint64 { return w.ts.ShardAccessStats().ColdOpens }

type pmcDoor struct {
	name, door string
	ids        []int64
	plain      func(fn func(pmcSegment) bool) error
	run        func(ctx context.Context, fn func(pmcSegment) bool) error
}

type pmcSegment struct {
	id       int64
	from, to types.Instant
	version  uint32
}

func (w *pmcWorld) doors() []pmcDoor {
	nf := func(fn func(pmcSegment) bool) func(temporal.NodeSegment) bool {
		return func(s temporal.NodeSegment) bool {
			return fn(pmcSegment{int64(s.Node.ID()), s.ValidFrom, s.ValidTo, s.Node.Version()})
		}
	}
	rf := func(fn func(pmcSegment) bool) func(temporal.RelSegment) bool {
		return func(s temporal.RelSegment) bool {
			return fn(pmcSegment{int64(s.Rel.ID()), s.ValidFrom, s.ValidTo, s.Rel.Version()})
		}
	}
	return []pmcDoor{
		{"node by label", "ForEachNodeEffectiveByLabel", w.nodes,
			func(fn func(pmcSegment) bool) error {
				return w.g.Temporal().ForEachNodeEffectiveByLabel("D", w.pin, nf(fn))
			},
			func(ctx context.Context, fn func(pmcSegment) bool) error {
				return w.g.Temporal().ForEachNodeEffectiveByLabelCtx(ctx, "D", w.pin, nf(fn))
			}},
		{"rel by type", "ForEachRelEffectiveByType", w.rels,
			func(fn func(pmcSegment) bool) error {
				return w.g.Temporal().ForEachRelEffectiveByType("NET", w.pin, rf(fn))
			},
			func(ctx context.Context, fn func(pmcSegment) bool) error {
				return w.g.Temporal().ForEachRelEffectiveByTypeCtx(ctx, "NET", w.pin, rf(fn))
			}},
	}
}

type pmcRun struct {
	segments []pmcSegment
	err      error
	work     storepkg.ReadWork
	opened   uint64
}

func (w *pmcWorld) read(t *testing.T, d pmcDoor, b storepkg.ReadBudget) pmcRun {
	t.Helper()
	w.closeCold(t)
	o := w.opens()
	ctx, meter := storepkg.WithReadBudget(context.Background(), b)
	var segments []pmcSegment
	err := d.run(ctx, func(s pmcSegment) bool { segments = append(segments, s); return true })
	return pmcRun{segments, err, meter.Work(), w.opens() - o}
}

func pmcPlain(t *testing.T, d pmcDoor) []pmcSegment {
	t.Helper()
	var segments []pmcSegment
	if err := d.plain(func(s pmcSegment) bool { segments = append(segments, s); return true }); err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for _, s := range segments {
		ids = append(ids, s.id)
	}
	slices.Sort(ids)
	if ids = slices.Compact(ids); !slices.Equal(ids, d.ids) {
		t.Fatalf("plain IDs = %v, want exactly %v", ids, d.ids)
	}
	return segments
}

func pmcRefusal(t *testing.T, err error, limit string) *storepkg.ReadBudgetError {
	t.Helper()
	var rb *storepkg.ReadBudgetError
	if !errors.As(err, &rb) || !errors.Is(err, storepkg.ErrReadBudget) || rb.Limit != limit {
		t.Fatalf("error %v, want the read budget refusal at %q", err, limit)
	}
	return rb
}

// TestReadBudgetPublicMemberCount: through the public effective scans on a
// tiered store whose cold shards were closed without sealed counts, the
// store's member count (asked before the members are listed) charges the
// shards it opens, under every meter: the unlimited control counts exactly the
// native opens, ShardOpens at that figure answers with the same work, one less
// refuses with no callback and no candidate, and a Candidates bound refuses
// before anything is listed (no buffer bytes) even under a ShardOpens bound.
func TestReadBudgetPublicMemberCount(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	w := pmcBuild(t, 8)
	for _, d := range w.doors() {
		t.Run(d.name, func(t *testing.T) {
			w.closeCold(t)
			plain := pmcPlain(t, d)

			// Three opens and six gathered members, plus each freshly opened
			// shard's membership build: 24 node or 21 relationship key visits.
			c := w.read(t, d, storepkg.ReadBudget{})
			// Three open-header probes; each timeline's native routing is extra.
			wantCandidates := uint64(27 + 3 + 3*4*2 + 3*4*9)
			if d.name == "node by label" {
				wantCandidates = 30 + 3 + 6*5
			}
			wantCandidates += 6 // each gathered timeline revalidates its metadata owner's liveness
			if c.err != nil || !slices.Equal(c.segments, plain) || c.opened != 3 || c.work.ShardOpens != 3 || c.work.Candidates != wantCandidates {
				t.Fatalf("control = %v, %v, %d opened, work %+v; want %v, 3 opens counted, %d candidates", c.segments, c.err, c.opened, c.work, plain, wantCandidates)
			}

			r := w.read(t, d, storepkg.ReadBudget{ShardOpens: 3})
			if r.err != nil || !slices.Equal(r.segments, plain) || r.opened != 3 || r.work != c.work {
				t.Fatalf("ShardOpens 3 = %v, %v, %d opened, work %+v; want the control's %v, %+v", r.segments, r.err, r.opened, r.work, plain, c.work)
			}

			r = w.read(t, d, storepkg.ReadBudget{ShardOpens: 2})
			rb := pmcRefusal(t, r.err, "shard opens")
			if rb.Door != d.door || len(r.segments) != 0 || r.opened != 2 || r.work.Candidates != 2 || r.work.BufferBytes != 0 || r.work != rb.Work {
				t.Fatalf("ShardOpens 2: door %q, %d callbacks, %d opened, work %+v; want %q, none, 2 opens and their two fixed-header probes", rb.Door, len(r.segments), r.opened, r.work, d.door)
			}

			r = w.read(t, d, storepkg.ReadBudget{Candidates: 1, ShardOpens: 3})
			rb = pmcRefusal(t, r.err, "candidates")
			if len(r.segments) != 0 || r.work.BufferBytes != 0 || r.opened != 1 || r.work.ShardOpens != 2 || r.work.Candidates != 2 || r.work != rb.Work {
				t.Fatalf("Candidates 1, ShardOpens 3: %d callbacks, %d opened, work %+v; want none, nothing listed, one installed handle, refusal before the second open-header probe", len(r.segments), r.opened, r.work)
			}
		})
	}

	// Sealed counts: the count opens nothing, so a Candidates refusal under a
	// ShardOpens bound comes before the listing would open a shard.
	w.closeCold(t)
	if err := w.doors()[0].run(context.Background(), func(pmcSegment) bool { return true }); err != nil {
		t.Fatal(err) // opens the three shards again (kept: room under the cap)
	}
	w.ts.SetIdleTimeoutForTest(time.Millisecond)
	for _, es := range w.cold {
		es.SetLastAccessForTest(0)
	}
	w.ts.CloseIdleShardsForTest() // closes and seals each
	w.ts.SetIdleTimeoutForTest(time.Hour)
	for _, d := range w.doors() {
		r := w.read(t, d, storepkg.ReadBudget{Candidates: 1, ShardOpens: 3})
		rb := pmcRefusal(t, r.err, "candidates")
		if len(r.segments) != 0 || r.opened != 0 || r.work.ShardOpens != 0 || r.work.BufferBytes != 0 || r.work != rb.Work {
			t.Fatalf("%s sealed, Candidates 1, ShardOpens 3: %d callbacks, %d opened, work %+v; want none, no shard opened", d.name, len(r.segments), r.opened, r.work)
		}
	}
}

// TestReadBudgetPublicMemberCount_Transient: with no room under the handle cap
// every cold read handle is transient, so the count's open of each of the 3
// unsealed shards closes again at its check-in, sealing its counts, and the
// listing and the entity reads open again the shards that hold what they read
// (a shard sealed with no member of the label or type is skipped), so the read
// opens more than the count's 3. Every native open of the read is counted
// once: no open the meter misses, none counted twice.
func TestReadBudgetPublicMemberCount_Transient(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	for _, name := range []string{"node by label", "rel by type"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			w := pmcBuild(t, 0)
			i := slices.IndexFunc(w.doors(), func(d pmcDoor) bool { return d.name == name })
			d := w.doors()[i]
			r := w.read(t, d, storepkg.ReadBudget{})
			plain := pmcPlain(t, d) // after the measured read: do not seal its counts early
			if r.err != nil || !slices.Equal(r.segments, plain) || r.opened < 6 || r.work.ShardOpens != r.opened {
				t.Fatalf("transient = %v, %v, %d opened, work %+v; want %v, the count's 3 opens and the reopens after them, each counted", r.segments, r.err, r.opened, r.work, plain)
			}
			t.Logf("%s: %d native opens, %d counted", name, r.opened, r.work.ShardOpens)
		})
	}
}
