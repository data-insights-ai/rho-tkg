package core

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The version-aware endpoint door under a read budget
// (Rels().ForEachAdjacentEndpointAtCtx): sigma's endpoint door reads a probe's
// node's edges at a pin and a valid window through it, under the cut's one
// meter.

// aeOpts is sigma's read at a pin: every valid instant, the rows the pin had.
func aeOpts(pin types.Instant) storepkg.QueryOpts {
	return storepkg.QueryOpts{TxAt: pin, TxAtSet: true, ValidStart: types.Instant(math.MinInt64), ValidEnd: types.Instant(math.MaxInt64), ValidWindowSet: true}
}

// aeCollect runs one endpoint read and lists its pairs.
func aeCollect(read func(fn func(types.RelID, types.NodeID) bool) error) ([]string, error) {
	var out []string
	err := read(func(rel types.RelID, other types.NodeID) bool {
		out = append(out, fmt.Sprintf("%d>%d", rel, other))
		return true
	})
	return out, err
}

// TestReadBudget_AdjacentEndpointAtCtx: on every backend a node with edges of
// two types (each edge with one update, so a history row, and one deleted
// edge, so an ex-adjacent entry), read by start and by end node, typed and
// untyped, at a pin before the delete and at one after it. Without a budget the
// door is the plain one. Unlimited, it answers the plain stream and counts
// two candidates per version resolution (chainLoadTestHook, which no meter
// keeps: the adjacency or ex-adjacency entry and the admission before it),
// history examined and buffer bytes. A budget at the unlimited count of each
// answers; one less refuses from "ForEachAdjacentEndpointAt" with a prefix of
// the stream (decoded bytes too on the disk stores); half the candidates stops
// before the later resolutions. A read without a temporal filter and a Depth
// other than DepthAll are refused before any read; the plain door's errors
// stay.
func TestReadBudget_AdjacentEndpointAtCtx(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			s, e := txbEndpoints(t, g)
			var gone types.RelID
			for i := range 6 {
				typ := "R"
				if i%3 == 2 {
					typ = "S"
				}
				r, err := g.Rels.AddByID(ctx, typ, s, e, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"v": int64(i)}); err != nil {
					t.Fatal(err)
				}
				gone = r.ID()
			}
			before, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Rels.Delete(ctx, gone); err != nil {
				t.Fatal(err)
			}
			after, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			for _, pin := range []types.Instant{before, after} {
				for _, d := range []struct {
					node     types.NodeID
					typeName string
					incoming bool
				}{{s, "R", false}, {s, "", false}, {e, "R", true}, {e, "", true}} {
					name := fmt.Sprintf("pin %d node %d type %q incoming %v", pin, d.node, d.typeName, d.incoming)
					opts := aeOpts(pin)
					plain := func(fn func(types.RelID, types.NodeID) bool) error {
						return g.Rels.ForEachAdjacentEndpointAt(d.node, d.typeName, d.incoming, opts, fn)
					}
					metered := func(c context.Context) func(fn func(types.RelID, types.NodeID) bool) error {
						return func(fn func(types.RelID, types.NodeID) bool) error {
							return g.Rels.ForEachAdjacentEndpointAtCtx(c, d.node, d.typeName, d.incoming, opts, fn)
						}
					}
					want, err := aeCollect(plain)
					if err != nil || len(want) == 0 {
						t.Fatalf("%s: plain = %v, %v; want pairs", name, want, err)
					}
					if pin == before && d.typeName == "" && len(want) != 6 {
						t.Fatalf("%s: plain = %d pairs before the delete, want 6 (the deleted edge from the ex-adjacency)", name, len(want))
					}
					if got, err := aeCollect(metered(ctx)); err != nil || !slices.Equal(got, want) {
						t.Fatalf("%s: no budget = %v, %v; want %v", name, got, err, want)
					}
					resolved := 0
					g.chainLoadTestHook = func() { resolved++ }
					fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
					got, err := aeCollect(metered(fctx))
					work, fullResolved := full.Work(), resolved
					if err != nil || !slices.Equal(got, want) || work.Candidates < 2*uint64(fullResolved) || work.HistoryExamined == 0 || work.BufferBytes == 0 || fullResolved == 0 {
						t.Fatalf("%s: unlimited = %v, %v, work %+v, %d resolutions; want the plain stream, two candidates per resolution (its adjacency entry and its admission), history and buffer bytes", name, got, err, work, fullResolved)
					}
					type limit struct {
						limit  string
						budget func(n uint64) storepkg.ReadBudget
						n      uint64
					}
					limits := []limit{
						{"candidates", func(n uint64) storepkg.ReadBudget { return storepkg.ReadBudget{Candidates: n} }, work.Candidates},
						{"history examined", func(n uint64) storepkg.ReadBudget { return storepkg.ReadBudget{HistoryExamined: n} }, work.HistoryExamined},
						{"buffer bytes", func(n uint64) storepkg.ReadBudget { return storepkg.ReadBudget{BufferBytes: n} }, work.BufferBytes},
					}
					if be.name != "memory" { // the disk stores decode the rows they read
						if work.DecodedBytes == 0 {
							t.Fatalf("%s: unlimited work %+v; want the decoded rows counted", name, work)
						}
						limits = append(limits, limit{"decoded bytes", func(n uint64) storepkg.ReadBudget { return storepkg.ReadBudget{DecodedBytes: n} }, work.DecodedBytes})
					}
					for _, lim := range limits {
						bctx, _ := storepkg.WithReadBudget(ctx, lim.budget(lim.n))
						if got, err := aeCollect(metered(bctx)); err != nil || !slices.Equal(got, want) {
							t.Fatalf("%s: %s %d = %v, %v; want the plain stream", name, lim.limit, lim.n, got, err)
						}
						bctx, meter := storepkg.WithReadBudget(ctx, lim.budget(lim.n-1))
						got, err := aeCollect(metered(bctx))
						rb := rbdBudgetErr(t, err, lim.limit)
						if rb.Door != "ForEachAdjacentEndpointAt" || !sbPrefix(got, want) {
							t.Fatalf("%s: %s %d: door %q, %d of %d pairs (work %+v); want rho's door and a prefix", name, lim.limit, lim.n-1, rb.Door, len(got), len(want), meter.Work())
						}
					}
					// Half the candidates: the read stops before the later relationships are resolved.
					resolved = 0
					bctx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: work.Candidates / 2})
					got, err = aeCollect(metered(bctx))
					if rbdBudgetErr(t, err, "candidates"); !sbPrefix(got, want) || len(got) == len(want) || resolved >= fullResolved {
						t.Fatalf("%s: candidates %d: %d of %d pairs, %d of %d resolutions; want a short prefix and fewer resolutions", name, work.Candidates/2, len(got), len(want), resolved, fullResolved)
					}
					g.chainLoadTestHook = nil
				}
			}

			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			deep := aeOpts(after)
			deep.Depth = storepkg.DepthHot
			if err := g.Rels.ForEachAdjacentEndpointAtCtx(bctx, s, "R", false, deep, func(types.RelID, types.NodeID) bool { return true }); err == nil {
				t.Fatal("a depth-scoped read under a budget answered")
			} else if rb := rbdBudgetErr(t, err, "unsupported"); rb.Door != "ForEachAdjacentEndpointAt" || meter.Work() != (storepkg.ReadWork{}) {
				t.Fatalf("depth-scoped: door %q, work %+v; want rho's door and nothing read", rb.Door, meter.Work())
			}
			// A type no edge has: one candidate, the node's existence read, before it.
			uctx, umeter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			if err := g.Rels.ForEachAdjacentEndpointAtCtx(uctx, s, "NOPE", false, aeOpts(after), func(types.RelID, types.NodeID) bool { return true }); err != nil || umeter.Work().Candidates == 0 {
				t.Fatalf("unregistered type = %v, work %+v; want no edge and the node's candidate counted", err, umeter.Work())
			}
			if err := g.Rels.ForEachAdjacentEndpointAtCtx(bctx, s, "R", false, storepkg.QueryOpts{}, func(types.RelID, types.NodeID) bool { return true }); err == nil {
				t.Fatal("a read without a temporal filter under a budget answered")
			} else if rb := rbdBudgetErr(t, err, "unsupported"); rb.Door != "ForEachAdjacentEndpointAt" || meter.Work() != (storepkg.ReadWork{}) {
				t.Fatalf("no temporal filter: door %q, work %+v; want rho's door and nothing read", rb.Door, meter.Work())
			}
			if err := g.Rels.ForEachAdjacentEndpointAtCtx(bctx, s, "R", false, aeOpts(after), nil); err == nil || errors.Is(err, storepkg.ErrReadBudget) {
				t.Fatalf("nil callback = %v, want the plain door's error", err)
			}
			if err := g.Rels.ForEachAdjacentEndpointAtCtx(bctx, types.NodeID(1), "", false, aeOpts(after), func(types.RelID, types.NodeID) bool { return true }); err == nil || errors.Is(err, storepkg.ErrReadBudget) {
				t.Fatalf("unknown node = %v, want the plain door's error", err)
			}
			cctx, cancel := context.WithCancel(ctx)
			cancel()
			bctx, meter = storepkg.WithReadBudget(cctx, storepkg.ReadBudget{})
			if err := g.Rels.ForEachAdjacentEndpointAtCtx(bctx, s, "R", false, aeOpts(after), func(types.RelID, types.NodeID) bool { return true }); !errors.Is(err, context.Canceled) || meter.Work() != (storepkg.ReadWork{}) {
				t.Fatalf("cancelled context: %v, work %+v", err, meter.Work())
			}
		})
	}
}

// TestReadBudget_AdjacentEndpointAtShardOpens: which day shards a tiered
// endpoint read opens for a node. Three day shards each hold an event node with
// an edge to one reference node (the edge lies where its start node does), an
// update of it and a second edge deleted. With every cold shard closed and
// sealed before the read (the idle sweep's close): a start-node read opens its
// node's day shard and no other, its deleted edge's history included (a
// Logon's outgoing edge lies on its day); an end-node read of the reference
// node opens every day that holds one of its edges. Each open is counted on the
// meter as it happens; under ShardOpens 1 the end-node read refuses having
// opened at most one; with LimitShardOpens and the start node's day allowed
// (sigma's cut meter) the start-node read answers with no shard open drawn on
// the limit. Both at the pin and by the valid window alone, with one cold
// shard open at a time, so a version or presence read reopens a shard the
// adjacency read opened: every reopen is counted too.
func TestReadBudget_AdjacentEndpointAtShardOpens(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ts, err := tiered.New(tiered.Config{
		DataDir:           t.TempDir(),
		RefLabels:         []string{"Ref"},
		ShardWindow:       7 * 24 * time.Hour,
		FlushInterval:     1<<63 - 1,
		MaxOpenColdShards: 1, // a shard the adjacency opened may close before the version reads, which open it again
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	// No shard is sealed before the unsealed case: neither the idle close, nor the
	// handle sweep's settle step, nor its swap of a demoted handle runs on its own.
	ts.SetIdleTimeoutForTest(time.Hour)
	ts.SetSweepFaultForTest(func(step string) error {
		if step == "settle" || step == "gated" {
			return errors.New("held by the test")
		}
		return nil
	})
	g, err := New(Config{Store: ts, UntimedValidStart: UntimedFromTxFrom})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close(); _ = ts.Close() })
	ctx := context.Background()
	user, err := g.Nodes.Add(ctx, []string{"Ref"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	const days = 3
	var events []types.NodeID
	var mints []types.Instant // each event node's mint instant: the time its shard is placed by
	for range days {
		ev, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, ev.ID())
		mints = append(mints, ev.ID().MintInstant())
		r, err := g.Rels.AddByID(ctx, "BY", ev.ID(), user.ID(), map[string]any{"pad": "x"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"v": int64(1)}); err != nil {
			t.Fatal(err)
		}
		d, err := g.Rels.AddByID(ctx, "BY", ev.ID(), user.ID(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := g.Rels.Delete(ctx, d.ID()); err != nil {
			t.Fatal(err)
		}
		if err := ts.RotateHotShard(); err != nil {
			t.Fatal(err)
		}
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	ts.MuForTest().RLock()
	hot := ts.HotShardForTest()
	var cold []*tiered.EventShard
	for _, es := range ts.EventShardsForTest() {
		if es != hot {
			cold = append(cold, es)
		}
	}
	ts.MuForTest().RUnlock()
	slices.SortFunc(cold, func(a, b *tiered.EventShard) int { return strings.Compare(a.Name(), b.Name()) }) // day order
	if len(cold) != days {
		t.Fatalf("%d cold shards, want %d", len(cold), days)
	}
	for _, es := range cold {
		demoteToCold(ts, es.Name())
	}
	openSet := func() []int {
		var open []int
		for i, es := range cold {
			es.LockShardMuForTest()
			if es.Store() != nil {
				open = append(open, i)
			}
			es.UnlockShardMuForTest()
		}
		return open
	}
	// closeCold closes every cold shard as the idle sweep does, sealing what it
	// holds: a deleted edge's history is then searched only on the shards whose
	// sealed relationship IDs can hold it.
	closeCold := func() {
		for _, es := range cold {
			es.SetLastAccessForTest(0)
		}
		ts.CloseIdleShardsForTest()
		if open := openSet(); len(open) != 0 {
			t.Fatalf("cold shards %v still open after the idle close", open)
		}
	}
	// Unsealed cold shards (closed with no sealed counts, as after a kill before
	// the sweep's step): a deleted edge's history is searched on every day, so
	// with only its day allowed a start-node read is refused having opened at
	// most that one, and unlimited it opens every day shard, each counted.
	unsealed := func() {
		for _, es := range cold {
			closeEventShardStore(t, es)
		}
	}
	unsealed()
	visit := func(types.RelID, types.NodeID) bool { return true }
	opens := ts.ShardAccessStats().ColdOpens
	bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
	meter.LimitShardOpens()
	meter.AllowShardWindows([2]types.Instant{mints[0], mints[0]})
	if err := g.Rels.ForEachAdjacentEndpointAtCtx(bctx, events[0], "BY", false, aeOpts(pin), visit); ts.ShardAccessStats().ColdOpens-opens > 1 {
		t.Fatalf("unsealed under its day alone: %v, %d opened; want at most its day", err, ts.ShardAccessStats().ColdOpens-opens)
	} else if rb := rbdBudgetErr(t, err, "shard opens"); rb.Door != "ForEachAdjacentEndpointAt" {
		t.Fatalf("unsealed under its day alone: door %q, want rho's", rb.Door)
	}
	unsealed()
	opens = ts.ShardAccessStats().ColdOpens
	bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
	if err := g.Rels.ForEachAdjacentEndpointAtCtx(bctx, events[0], "BY", false, aeOpts(pin), visit); err != nil || ts.ShardAccessStats().ColdOpens-opens != days || meter.Work().ShardOpens != days {
		t.Fatalf("unsealed: %v, %d opened, work %+v; want every day opened and counted", err, ts.ShardAccessStats().ColdOpens-opens, meter.Work())
	}
	ts.SetSweepFaultForTest(nil)

	// At the pin (sigma's read: every relationship resolves its version) and by the
	// valid window alone (a relationship without history answers from its live
	// row after the presence probe, which is charged as well).
	validOnly := aeOpts(pin)
	validOnly.TxAt, validOnly.TxAtSet = 0, false
	for _, c := range []struct {
		name string
		opts storepkg.QueryOpts
	}{{"pin", aeOpts(pin)}, {"valid window", validOnly}} {
		t.Run(c.name, func(t *testing.T) {
			opts := c.opts
			read := func(c context.Context, node types.NodeID, incoming bool) ([]string, error) {
				return aeCollect(func(fn func(types.RelID, types.NodeID) bool) error {
					return g.Rels.ForEachAdjacentEndpointAtCtx(c, node, "BY", incoming, opts, fn)
				})
			}

			for day, ev := range events {
				closeCold()
				want, err := read(ctx, ev, false)
				if err != nil || len(want) != 2 {
					t.Fatalf("day %d start node: plain = %v, %v; want its two edges (the deleted one inside its window)", day, want, err)
				}
				closeCold()
				opens := ts.ShardAccessStats().ColdOpens
				bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
				got, err := read(bctx, ev, false)
				opened := ts.ShardAccessStats().ColdOpens - opens
				if err != nil || !slices.Equal(got, want) || !slices.Equal(openSet(), []int{day}) || opened != 1 || meter.Work().ShardOpens != 1 {
					t.Fatalf("day %d start node: %v, %v, shards open %v, %d opened, %d counted; want the plain answer, only shard %d, one open counted", day, got, err, openSet(), opened, meter.Work().ShardOpens, day)
				}
				closeCold()
				bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
				meter.LimitShardOpens()
				meter.AllowShardWindows([2]types.Instant{mints[day], mints[day]})
				if got, err := read(bctx, ev, false); err != nil || !slices.Equal(got, want) {
					t.Fatalf("day %d start node under a limit of 0 with its day allowed = %v, %v (work %+v); want the plain answer", day, got, err, meter.Work())
				}
				// Another day allowed: refused before any open.
				closeCold()
				opens = ts.ShardAccessStats().ColdOpens
				bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
				meter.LimitShardOpens()
				meter.AllowShardWindows([2]types.Instant{mints[(day+1)%days], mints[(day+1)%days]})
				_, err = read(bctx, ev, false)
				if rb := rbdBudgetErr(t, err, "shard opens"); rb.Door != "ForEachAdjacentEndpointAt" || ts.ShardAccessStats().ColdOpens != opens {
					t.Fatalf("day %d start node with another day allowed: door %q, %d opened; want rho's door and no open", day, rb.Door, ts.ShardAccessStats().ColdOpens-opens)
				}
				// A type no edge has: the node's existence read is on the meter too.
				visit := func(types.RelID, types.NodeID) bool { return true }
				closeCold()
				opens = ts.ShardAccessStats().ColdOpens
				bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
				meter.LimitShardOpens()
				err = g.Rels.ForEachAdjacentEndpointAtCtx(bctx, ev, "NOPE", false, opts, visit)
				if rb := rbdBudgetErr(t, err, "shard opens"); rb.Door != "ForEachAdjacentEndpointAt" || ts.ShardAccessStats().ColdOpens != opens {
					t.Fatalf("day %d unregistered type under a limit of 0: door %q, %d opened; want rho's door and no open", day, rb.Door, ts.ShardAccessStats().ColdOpens-opens)
				}
				closeCold()
				bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
				meter.LimitShardOpens()
				meter.AllowShardWindows([2]types.Instant{mints[day], mints[day]})
				if err := g.Rels.ForEachAdjacentEndpointAtCtx(bctx, ev, "NOPE", false, opts, visit); err != nil || meter.Work().ShardOpens != 1 {
					t.Fatalf("day %d unregistered type with its day allowed = %v (work %+v); want no edge and the one open counted", day, err, meter.Work())
				}
			}

			closeCold()
			want, err := read(ctx, user.ID(), true)
			if err != nil || len(want) != 2*days {
				t.Fatalf("end node: plain = %v, %v; want %d edges", want, err, 2*days)
			}
			closeCold()
			opens := ts.ShardAccessStats().ColdOpens
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			got, err := read(bctx, user.ID(), true)
			opened := ts.ShardAccessStats().ColdOpens - opens
			if err != nil || !slices.Equal(got, want) || opened < days || meter.Work().ShardOpens != opened {
				t.Fatalf("end node: %v, %v, shards open %v, %d opened, %d counted; want the plain answer, at least every day opened, every open counted", got, err, openSet(), opened, meter.Work().ShardOpens)
			}
			closeCold()
			opens = ts.ShardAccessStats().ColdOpens
			bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
			got, err = read(bctx, user.ID(), true)
			opened = ts.ShardAccessStats().ColdOpens - opens
			if rb := rbdBudgetErr(t, err, "shard opens"); rb.Door != "ForEachAdjacentEndpointAt" || opened > 1 || !sbPrefix(got, want) {
				t.Fatalf("end node under ShardOpens 1: door %q, %d opened, %v (work %+v); want rho's door, at most one open, a prefix", rb.Door, opened, got, meter.Work())
			}
		})
	}
}

// TestReadBudget_AdjacencyCandidateListsHeld: the endpoint door's candidate
// lists are held on the meter before they grow: 8 buffer bytes per ex-adjacent
// ID as it joins its list, then 8 per ID of the united list, beyond what the
// store's ex-adjacency walk charges itself (the same walk read alone).
func TestReadBudget_AdjacencyCandidateListsHeld(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			s, e := txbEndpoints(t, g)
			var current []types.RelID
			const gone = 3
			for i := range 5 + gone {
				r, err := g.Rels.AddByID(ctx, "R", s, e, nil)
				if err != nil {
					t.Fatal(err)
				}
				if i < gone {
					if err := g.Rels.Delete(ctx, r.ID()); err != nil {
						t.Fatal(err)
					}
					continue
				}
				current = append(current, r.ID())
			}
			bufferOf := func(read func(run *scanRun) error) uint64 {
				t.Helper()
				bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
				run, err := g.scanRunFor(bctx, "ForEachAdjacentEndpointAt")
				if err != nil {
					t.Fatal(err)
				}
				if err := g.readUnderRLock(func() error { return read(run) }); err != nil {
					t.Fatal(err)
				}
				return meter.Work().BufferBytes
			}
			var ids []types.RelID
			lists := bufferOf(func(run *scanRun) error {
				var err error
				ids, err = g.relAdjacencyCandidateIDsSorted(run, s, adjOut, current, storepkg.DepthAll)
				return err
			})
			walk := bufferOf(func(run *scanRun) error {
				return g.forEachRelExAdjacentIDRun(run, []types.NodeID{s}, adjOut, storepkg.DepthAll, func(types.RelID) bool { return true })
			})
			if len(ids) != len(current)+gone || lists-walk != uint64(8*gone+8*len(ids)) {
				t.Fatalf("%d IDs, lists held %d bytes beyond the walk's %d; want %d IDs and %d bytes", len(ids), lists-walk, walk, len(current)+gone, 8*gone+8*(len(current)+gone))
			}
		})
	}
}

// TestReadBudget_AdjacentEndpointAtDeletedNode: a node deleted after the pin
// (its edges first) is read through its version at the pin, on the meter: on
// every backend the unlimited read answers the plain stream with its history
// examined, and a history budget the node's own version check passes is a
// refusal from "ForEachAdjacentEndpointAt", never ErrNodeNotFound (which a
// caller would take for a node with no edges).
func TestReadBudget_AdjacentEndpointAtDeletedNode(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			_, e := txbEndpoints(t, g)
			n, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"k": "gone"})
			if err != nil {
				t.Fatal(err)
			}
			for v := range 3 {
				if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"v": int64(v)}); err != nil {
					t.Fatal(err)
				}
			}
			var rels []types.RelID
			for range 2 {
				r, err := g.Rels.AddByID(ctx, "R", n.ID(), e, nil)
				if err != nil {
					t.Fatal(err)
				}
				rels = append(rels, r.ID())
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range rels {
				if err := g.Rels.Delete(ctx, r); err != nil {
					t.Fatal(err)
				}
			}
			if err := g.Nodes.Delete(ctx, n.ID()); err != nil {
				t.Fatal(err)
			}
			opts := aeOpts(pin)
			want, err := aeCollect(func(fn func(types.RelID, types.NodeID) bool) error {
				return g.Rels.ForEachAdjacentEndpointAt(n.ID(), "R", false, opts, fn)
			})
			if err != nil || len(want) != 2 {
				t.Fatalf("plain = %v, %v; want the deleted node's 2 edges", want, err)
			}
			read := func(c context.Context) ([]string, error) {
				return aeCollect(func(fn func(types.RelID, types.NodeID) bool) error {
					return g.Rels.ForEachAdjacentEndpointAtCtx(c, n.ID(), "R", false, opts, fn)
				})
			}
			bctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			if got, err := read(bctx); err != nil || !slices.Equal(got, want) || full.Work().HistoryExamined == 0 {
				t.Fatalf("unlimited = %v, %v, work %+v; want the plain stream and its history examined", got, err, full.Work())
			}
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{HistoryExamined: 1})
			got, err := read(bctx)
			if errors.Is(err, storepkg.ErrNodeNotFound) || len(got) != 0 {
				t.Fatalf("history budget 1 = %v, %v (work %+v); want a refusal before any edge, not ErrNodeNotFound", got, err, meter.Work())
			}
			if rb := rbdBudgetErr(t, err, "history examined"); rb.Door != "ForEachAdjacentEndpointAt" {
				t.Fatalf("history budget 1: door %q, want rho's", rb.Door)
			}
		})
	}
}
