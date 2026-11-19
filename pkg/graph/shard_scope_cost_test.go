package graph_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Shard scope (CUTCOST). A read that holds a key reads the shards that key
// names and no other: a reference label or type the reference shard, an event
// the shard of its instant. The fixture is ai-soc's v6 store: day shards minted
// at valid time, cold after two days, two cold days open at once, rels by mint,
// a reference label with a reference type, unique keys on the event's record
// and the user's name.

var scopeStart = time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

// scopeOp is what the reads of one operation cost: the work of their meter and
// the store's lazy opens of closed cold shards around them.
type scopeOp struct {
	Work      storepkg.ReadWork
	ColdOpens uint64
}

type scopeWorld struct {
	g     *graphpkg.Graph
	ts    *tieredpkg.Store
	clock *atomic.Int64
	fact  types.NodeID
	of    types.RelID
	n     int
}

// scopeOpen opens the fixture with one user and one derived fact about it,
// linked by the reference type.
func scopeOpen(t *testing.T) *scopeWorld {
	t.Helper()
	w := &scopeWorld{clock: &atomic.Int64{}}
	w.clock.Store(scopeStart.UnixMilli())
	ts, err := tieredpkg.New(tieredpkg.Config{DataDir: t.TempDir(), RefLabels: []string{"User", "Fact"}, RefTypes: []string{"OF"},
		ShardWindow: 24 * time.Hour, ShardWindowLocation: time.UTC, ColdAfter: 48 * time.Hour, MaxOpenColdShards: 2, MintAtValidFrom: true, RelsByMint: true,
		Clock: func() time.Time { return time.UnixMilli(w.clock.Load()) }})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: ts, SnowflakeNodeID: 1, UntimedValidStart: graphpkg.UntimedUnbounded})
	if err != nil {
		t.Fatal(errors.Join(err, ts.Close()))
	}
	t.Cleanup(func() {
		if err := g.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx := context.Background()
	for _, k := range [][2]string{{"User", "name"}, {"Fact", "key"}, {"Event", "record"}} {
		if err := g.Constraints().CreateUnique(ctx, k[0], k[1]); err != nil {
			t.Fatal(err)
		}
	}
	err = g.Tx().RunContext(ctx, func(tx *graphpkg.GraphTx) error {
		user, _, err := tx.GetOrCreateByKey("User", "name", "u", nil)
		if err != nil {
			return err
		}
		fact, _, err := tx.GetOrCreateByKey("Fact", "key", "count:u", map[string]any{"n": int64(0), "tkg_valid_from": scopeStart.UnixMilli()})
		if err != nil {
			return err
		}
		w.fact = fact.ID()
		of, err := tx.AddRelationshipByID("OF", fact.ID(), user.ID(), map[string]any{"tkg_valid_from": int64(1)})
		if err == nil {
			w.of = of.ID()
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	w.g, w.ts = g, ts
	return w
}

// step makes one operation at the next instant, step after the last: a new
// event of the user, the fact's next version from that instant, then under one
// meter that allows no closed shard the four keyed reads: the event's record
// (new, so no holder), the fact's links of the reference type, the fact's
// timeline past the previous instant (its last two versions) and the link's
// timeline (one segment, a version an operation).
func (w *scopeWorld) step(t *testing.T, step time.Duration) scopeOp {
	t.Helper()
	at := w.write(t, step)
	return w.reads(t, at, step)
}

// write is step's write; it returns its instant.
func (w *scopeWorld) write(t *testing.T, step time.Duration) int64 {
	t.Helper()
	ctx := context.Background()
	w.n++
	at := scopeStart.Add(time.Duration(w.n) * step).UnixMilli()
	w.clock.Store(at)
	err := w.g.Tx().RunContext(ctx, func(tx *graphpkg.GraphTx) error {
		event, err := tx.AddNode([]string{"Event"}, map[string]any{"record": "r" + strconv.Itoa(w.n), "tkg_valid_from": at})
		if err != nil {
			return err
		}
		user, _, err := tx.GetOrCreateByKey("User", "name", "u", nil)
		if err != nil {
			return err
		}
		if _, err := tx.AddRelationshipByID("BY", event.ID(), user.ID(), map[string]any{"tkg_valid_from": at}); err != nil {
			return err
		}
		if _, err := tx.SetNodeVersionInterval(w.fact, types.Instant(at), 0, map[string]any{"n": int64(w.n)}); err != nil {
			return err
		}
		// the fact's link over its whole validity once more, as a writer that patches its cover does
		_, err = tx.SetRelVersionInterval(w.of, 1, 0, map[string]any{"n": int64(w.n)})
		return err
	})
	if err != nil {
		t.Fatalf("operation %d: write: %v", w.n, err)
	}
	return at
}

// reads is step's four keyed reads after the write at instant at.
func (w *scopeWorld) reads(t *testing.T, at int64, step time.Duration) scopeOp {
	t.Helper()
	ctx := context.Background()
	before := scopeColdOpens(t, w.g)
	mctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
	meter.LimitShardOpens()
	if nodes, err := w.g.Nodes().ByLabelAndPropertyCtx(mctx, "Event", "record", "r"+strconv.Itoa(w.n+1), storepkg.QueryOpts{}); err != nil || len(nodes) != 0 {
		t.Fatalf("operation %d: lookup of a new record: %d nodes, %v", w.n, len(nodes), err)
	}
	if rels, err := w.g.Rels().OutgoingCtx(mctx, w.fact, "OF"); err != nil || len(rels) != 1 {
		t.Fatalf("operation %d: the fact's reference links: %d, %v", w.n, len(rels), err)
	}
	pin, err := w.g.Temporal().CommittedTx()
	if err != nil {
		t.Fatal(err)
	}
	segs, err := w.g.Temporal().NodeEffectiveTimelineFromCtx(mctx, w.fact, types.Instant(at-step.Milliseconds()+1), pin)
	if err != nil || len(segs) != 2 {
		t.Fatalf("operation %d: the fact's timeline past the previous instant: %d segments, %v; want the last two", w.n, len(segs), err)
	}
	link, err := w.g.Temporal().RelEffectiveTimelineCtx(mctx, w.of, pin)
	if err != nil || len(link) != 1 {
		t.Fatalf("operation %d: the link's timeline: %d segments, %v; want the one it covers", w.n, len(link), err)
	}
	return scopeOp{Work: meter.Work(), ColdOpens: scopeColdOpens(t, w.g) - before}
}

func scopeColdOpens(t *testing.T, g *graphpkg.Graph) uint64 {
	t.Helper()
	s, err := g.Tier().ShardAccessStats()
	if err != nil {
		t.Fatal(err)
	}
	return s.ColdOpens
}

// scopeFlat checks every operation after the reference one (ref, from 1): no
// closed shard opened, by the meter and by the store's own counter, and the
// decoded bytes, history versions and candidates within 1.5x the reference's.
func scopeFlat(t *testing.T, ops []scopeOp, ref int) {
	t.Helper()
	base := ops[ref-1].Work
	for i, op := range ops {
		if op.Work.ShardOpens != 0 || op.ColdOpens != 0 {
			t.Fatalf("operation %d: its keyed reads opened closed shards (meter %d, store %d), want none: the keys name the reference shard and the hot day", i+1, op.Work.ShardOpens, op.ColdOpens)
		}
		if i < ref {
			continue
		}
		for _, f := range []struct {
			name      string
			got, base uint64
		}{{"candidates", op.Work.Candidates, base.Candidates}, {"decoded bytes", op.Work.DecodedBytes, base.DecodedBytes}, {"history examined", op.Work.HistoryExamined, base.HistoryExamined}} {
			if float64(f.got) > 1.5*float64(f.base) {
				t.Fatalf("operation %d: %s %d, operation %d had %d: it grows with the history (work %+v)", i+1, f.name, f.got, ref, f.base, op.Work)
			}
		}
	}
}

// TestShardScope_KeyedReadsOpenNoOtherShard is the guard (rho's make dev):
// over five days, two of them cold, the keyed reads of every operation open
// no closed shard and cost what they cost on day two; a rotation does not make
// the fact's next timeline read fold its chain. Then a record of the first,
// cold day is looked up: refused before the open under a meter that allows
// none, and found with exactly one open once the meter allows one. About 1 s.
func TestShardScope_KeyedReadsOpenNoOtherShard(t *testing.T) {
	w := scopeOpen(t)
	ops := make([]scopeOp, 9)
	for i := range ops {
		ops[i] = w.step(t, 12*time.Hour)
	}
	scopeFlat(t, ops, 3)
	opened := scopeColdOpens(t, w.g)

	ctx := context.Background()
	mctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
	meter.LimitShardOpens()
	nodes, err := w.g.Nodes().ByLabelAndPropertyCtx(mctx, "Event", "record", "r1", storepkg.QueryOpts{})
	var refusal *storepkg.ReadBudgetError
	if len(nodes) != 0 || !errors.As(err, &refusal) || refusal.Limit != "shard opens" || refusal.Door != "ByLabelAndProperty" {
		t.Fatalf("lookup of a cold day's record without an allowed open: %d nodes, %v; want a refusal at shard opens (is day one cold?)", len(nodes), err)
	}
	if after := scopeColdOpens(t, w.g); after != opened {
		t.Fatalf("the refused lookup opened %d cold shards", after-opened)
	}
	// From here each caller holds what it opens and releases it: the day is closed again for the next.
	mctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
	meter.LimitShardOpens()
	meter.HoldShardOpens(storepkg.ShardHoldStrict)
	nodes, err = w.g.Nodes().ByLabelAndPropertyCtx(mctx, "Event", "record", "r1", storepkg.QueryOpts{})
	meter.Release()
	if err != nil || len(nodes) != 1 || meter.Work().ShardOpens != 1 {
		t.Fatalf("lookup of a cold day's record with one open allowed: %d nodes, %v, work %+v; want the record from its day's shard alone", len(nodes), err, meter.Work())
	}
	// The window form: the caller names the instant of its key, and only that day's shard opens.
	dayOne := types.Instant(scopeStart.Add(12 * time.Hour).UnixMilli()) // r1's instant
	for _, c := range []struct {
		name     string
		from, to types.Instant
		found    int
	}{{"the record's own day", dayOne, dayOne, 1}, {"another cold day", dayOne + types.Instant(24*time.Hour.Milliseconds()), dayOne + types.Instant(30*time.Hour.Milliseconds()), 0}, {"no day", 1, 0, 0}} {
		mctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
		meter.LimitShardOpens()
		meter.AllowShardWindows([2]types.Instant{c.from, c.to})
		meter.HoldShardOpens(storepkg.ShardHoldStrict)
		nodes, err = w.g.Nodes().ByLabelAndPropertyCtx(mctx, "Event", "record", "r1", storepkg.QueryOpts{})
		meter.Release()
		if c.found == 1 && (err != nil || len(nodes) != 1 || meter.Work().ShardOpens != 1) {
			t.Fatalf("lookup under a window naming %s: %d nodes, %v, work %+v; want the record with its day's one open", c.name, len(nodes), err, meter.Work())
		}
		if c.found == 0 && (len(nodes) != 0 || !errors.As(err, &refusal) || refusal.Limit != "shard opens") {
			t.Fatalf("lookup under a window naming %s: %d nodes, %v; want a refusal at shard opens", c.name, len(nodes), err)
		}
	}
}

// TestShardScope_CostFlatOverHistory is the same operations hourly over
// sixteen days, thirteen of them cold (the gate): no closed shard opened and
// flat candidates, decoded bytes and history versions per operation.
func TestShardScope_CostFlatOverHistory(t *testing.T) {
	testtier.Slow(t)
	w := scopeOpen(t)
	ops := make([]scopeOp, 16*24)
	for i := range ops {
		ops[i] = w.step(t, time.Hour)
	}
	scopeFlat(t, ops, 8)
	last := ops[len(ops)-1].Work
	t.Logf("operation 8: %+v; operation %d: %+v", ops[7].Work, len(ops), last)
}

// TestReadMeter_LimitShardOpens: a meter refuses its first shard open once
// limited, counts up to its ShardOpens otherwise, and leaves the budget a
// refusal carries unchanged; an unlimited zero stays unbounded; opens inside
// an allowed window count without drawing on the limit.
func TestReadMeter_LimitShardOpens(t *testing.T) {
	_, free := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
	if err := free.AddShardOpens("Door", 3); err != nil || free.BoundsShardOpens() {
		t.Fatalf("a zero ShardOpens without the limit: %v, bounds %v; want unbounded", err, free.BoundsShardOpens())
	}
	budget := storepkg.ReadBudget{ShardOpens: 1}
	_, m := storepkg.WithReadBudget(context.Background(), budget)
	m.LimitShardOpens()
	if err := m.AddShardOpens("Door", 1); err != nil {
		t.Fatalf("the one allowed open: %v", err)
	}
	var refusal *storepkg.ReadBudgetError
	if err := m.AddShardOpens("Door", 1); !errors.As(err, &refusal) || refusal.Limit != "shard opens" || refusal.Budget != budget {
		t.Fatalf("the second open: %v; want a refusal carrying the caller's budget", err)
	}
	// A window: opens of shards meeting it count and draw nothing from the limit; others draw on it.
	_, win := storepkg.WithReadBudget(context.Background(), budget)
	win.LimitShardOpens()
	win.AllowShardWindows([2]types.Instant{100, 199}, [2]types.Instant{500, 500}, [2]types.Instant{9, 8})
	for _, shard := range [][2]types.Instant{{0, 101}, {199, 300}, {150, 160}, {500, 501}, {400, 501}} {
		if err := win.AddShardOpensIn("Door", 1, shard[0], shard[1]); err != nil {
			t.Fatalf("an open of shard %v inside the windows [100, 199] and [500, 500]: %v", shard, err)
		}
	}
	if err := win.AddShardOpensIn("Door", 1, 200, 300); err != nil || win.Work().ShardOpens != 6 {
		t.Fatalf("the one allowed open outside the windows after five inside: %v, work %+v; want it counted as the first of the limit", err, win.Work())
	}
	// A shard between two windows is in neither: the windows are a set, not their hull.
	for _, shard := range [][2]types.Instant{{0, 100}, {200, 300}, {300, 500}, {501, 600}, {8, 10}} {
		_, out := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
		out.LimitShardOpens()
		out.AllowShardWindows([2]types.Instant{100, 199}, [2]types.Instant{500, 500}, [2]types.Instant{9, 8})
		err := out.AddShardOpensIn("Door", 1, shard[0], shard[1])
		var be *storepkg.ReadBudgetError
		if !errors.As(err, &be) || !be.ShardOpensLimited || be.ShardWindows != 2 || !strings.Contains(err.Error(), "limited to the budget's 0 beyond the shards of 2 windows") {
			t.Fatalf("an open of shard %v outside the windows: %v; want a refusal naming the limit and its 2 windows", shard, err)
		}
	}
	// Without a hold every open counts, as before the limit.
	_, each := storepkg.WithReadBudget(context.Background(), budget)
	each.LimitShardOpens()
	if err := errors.Join(each.AddShardOpensIn("Door", 1, 200, 300), each.AddShardOpensIn("Door", 1, 200, 300)); !errors.Is(err, storepkg.ErrReadBudget) {
		t.Fatalf("a shard opened twice past a limit of one, not held: %v; want the second open refused", err)
	}
	// Held, the limit counts a shard once: opened again, it counts in Work only; another shard is one more.
	_, once := storepkg.WithReadBudget(context.Background(), budget)
	once.LimitShardOpens()
	once.HoldShardOpens(storepkg.ShardHoldLRU)
	for range 3 {
		if err := once.AddShardOpensIn("Door", 1, 200, 300); err != nil {
			t.Fatalf("the one allowed shard opened again: %v; want it counted once against the limit", err)
		}
	}
	if err := once.AddShardOpensIn("Door", 1, 300, 400); !errors.Is(err, storepkg.ErrReadBudget) || once.Work().ShardOpens != 4 {
		t.Fatalf("a second shard past a limit of one: %v, work %+v; want a refusal and 4 opens counted", err, once.Work())
	}
	// A repair's meter takes the refused meter's scope, not only its budget.
	_, repair := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
	repair.ScopeLike(win)
	if err := repair.AddShardOpensIn("Door", 1, 150, 160); err != nil {
		t.Fatalf("a meter scoped like the windowed one, an open inside: %v", err)
	}
	if err := repair.AddShardOpensIn("Door", 1, 200, 300); !errors.Is(err, storepkg.ErrReadBudget) {
		t.Fatalf("a meter scoped like the limited one, an open outside with ShardOpens 0: %v; want a refusal", err)
	}
	if err := win.AddShardOpens("Door", 1); !errors.Is(err, storepkg.ErrReadBudget) {
		t.Fatalf("an open of a shard without a window (the archive) past the limit: %v; want a refusal", err)
	}
	_, none := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
	none.LimitShardOpens()
	if err := none.AddShardOpens("Door", 1); !errors.Is(err, storepkg.ErrReadBudget) || !none.BoundsShardOpens() || !none.BoundsStoreWork() {
		t.Fatalf("a limited meter with ShardOpens 0: %v; want the first open refused", err)
	}
}

// TestShardScope_RollbackOpensNoShardItDidNotWrite (CUTCOST H1): over five
// days, two of them cold, a transaction creates a reference node with its link
// and a version, then fails. Its rollback deletes what it created; a created
// node has no leg and no history on a shard closed since before the
// transaction began, so the store's cold-open counter does not move, and the
// node, its key and its link are gone. A transaction that creates an event of
// a cold day and fails opens that one day, which it wrote, and no other.
func TestShardScope_RollbackOpensNoShardItDidNotWrite(t *testing.T) {
	w := scopeOpen(t)
	for range 9 {
		w.step(t, 12*time.Hour)
	}
	ctx := context.Background()
	failed := errors.New("the write fails")
	before := scopeColdOpens(t, w.g)
	var made types.NodeID
	err := w.g.Tx().RunContext(ctx, func(tx *graphpkg.GraphTx) error {
		fact, _, err := tx.GetOrCreateByKey("Fact", "key", "count:other", map[string]any{"n": int64(0), "tkg_valid_from": scopeStart.UnixMilli()})
		if err != nil {
			return err
		}
		made = fact.ID()
		user, _, err := tx.GetOrCreateByKey("User", "name", "u", nil)
		if err != nil {
			return err
		}
		if _, err := tx.AddRelationshipByID("OF", fact.ID(), user.ID(), map[string]any{"tkg_valid_from": int64(1)}); err != nil {
			return err
		}
		if _, err := tx.SetNodeVersionInterval(fact.ID(), types.Instant(scopeStart.Add(time.Hour).UnixMilli()), 0, map[string]any{"n": int64(1)}); err != nil {
			return err
		}
		return failed
	})
	if !errors.Is(err, failed) {
		t.Fatalf("the failing transaction: %v", err)
	}
	if opened := scopeColdOpens(t, w.g) - before; opened != 0 {
		t.Fatalf("the rollback of a created reference node opened %d cold shards, want none: it wrote none of them", opened)
	}
	if nodes, err := w.g.Nodes().ByLabelAndPropertyCtx(ctx, "Fact", "key", "count:other", storepkg.QueryOpts{}); err != nil || len(nodes) != 0 {
		t.Fatalf("after the rollback the key finds %d nodes, %v; want none", len(nodes), err)
	}
	if pin, err := w.g.Temporal().CommittedTx(); err != nil {
		t.Fatal(err)
	} else if segs, err := w.g.Temporal().NodeEffectiveTimeline(made, pin); !errors.Is(err, graphpkg.ErrNodeNotFound) {
		t.Fatalf("after the rollback the created node's timeline: %d segments, %v; want it unknown", len(segs), err)
	}
	if rels, err := w.g.Rels().OutgoingCtx(ctx, w.fact, "OF"); err != nil || len(rels) != 1 {
		t.Fatalf("the earlier fact's link after the rollback: %d, %v; want its 1", len(rels), err)
	}

	// An event of the first, cold day: the transaction opens that day to write it, the rollback removes it there.
	before = scopeColdOpens(t, w.g)
	late := scopeStart.Add(13 * time.Hour).UnixMilli()
	err = w.g.Tx().RunContext(ctx, func(tx *graphpkg.GraphTx) error {
		if _, err := tx.AddNode([]string{"Event"}, map[string]any{"record": "late", "tkg_valid_from": late}); err != nil {
			return err
		}
		return failed
	})
	if !errors.Is(err, failed) {
		t.Fatalf("the failing late transaction: %v", err)
	}
	if opened := scopeColdOpens(t, w.g) - before; opened > 1 {
		t.Fatalf("the late event's write and rollback opened %d cold shards, want its own day at most once", opened)
	}
	mctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
	meter.LimitShardOpens()
	meter.AllowShardWindows([2]types.Instant{types.Instant(late), types.Instant(late)})
	if nodes, err := w.g.Nodes().ByLabelAndPropertyCtx(mctx, "Event", "record", "late", storepkg.QueryOpts{}); err != nil || len(nodes) != 0 {
		t.Fatalf("after the rollback the late record finds %d nodes, %v; want none", len(nodes), err)
	}
}

// TestShardScope_HoldCountsDaysWithoutAShard (CUTCOST review 3 P1): a caller
// about to write records on cold days holds those days first, also the days
// that have no shard yet (days before the store's first), which its write
// would make. Three such days pass the bound of two: refused by name, nothing
// opened. Two are held as places: no shard is made, and a caller that gives up
// (Release without a write) leaves the store as it was. When it writes, the
// shards its write makes are the meter's, with no open beside the meter, and
// closed at Release; one day that exists and one that does not, likewise.
func TestShardScope_HoldCountsDaysWithoutAShard(t *testing.T) {
	w := scopeOpen(t)
	for range 9 {
		w.step(t, 12*time.Hour)
	}
	ctx := context.Background()
	day := func(d int) types.Instant {
		return types.Instant(scopeStart.Add(time.Duration(d)*24*time.Hour + time.Hour).UnixMilli())
	}
	state := func() (shards, cold int, opens uint64) {
		t.Helper()
		list, err := w.g.Tier().ListShards()
		if err != nil {
			t.Fatal(err)
		}
		stats, err := w.g.Tier().ShardAccessStats()
		if err != nil {
			t.Fatal(err)
		}
		return len(list), stats.ColdHandles.Writable + stats.ColdHandles.ReadOnly, stats.ColdOpens
	}
	hold := func(days ...types.Instant) (*storepkg.ReadMeter, error) {
		mctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
		meter.LimitShardOpens()
		meter.HoldShardOpens(storepkg.ShardHoldStrict)
		windows := make([][2]types.Instant, len(days))
		for i, d := range days {
			windows[i] = [2]types.Instant{d, d}
		}
		meter.AllowShardWindows(windows...)
		return meter, w.g.Tier().HoldShardsAtCtx(mctx, days...)
	}
	shards, cold, opens := state()
	if cold != 0 {
		t.Fatalf("fixture: %d cold shards open", cold)
	}

	meter, err := hold(day(-1), day(-2), day(-3))
	var refusal *storepkg.ReadBudgetError
	if !errors.As(err, &refusal) || refusal.Limit != "shards held" {
		t.Fatalf("three days without a shard for a strict meter: %v; want a refusal at shards held", err)
	}
	meter.Release()
	if s, c, o := state(); s != shards || c != 0 || o != opens {
		t.Fatalf("after the refusal: %d shards (were %d), %d cold open, %d opened; want nothing made or opened", s, shards, c, o-opens)
	}
	// Two days held and given up: the hold makes no shard, so nothing is left to remove.
	if meter, err = hold(day(-4), day(-5)); err != nil {
		t.Fatalf("two days without a shard: %v", err)
	}
	meter.Release()
	if s, c, o := state(); s != shards || c != 0 || o != opens {
		t.Fatalf("after a hold given up without a write: %d shards (were %d), %d cold open, %d opened; want the store as it was", s, shards, c, o-opens)
	}
	// And the places are free again: the same two days and a third are one too many, two of them are not.
	if meter, err = hold(day(-4), day(-5), day(-6)); !errors.As(err, &refusal) {
		t.Fatalf("three days after the released hold: %v; want a refusal", err)
	}
	meter.Release()

	for _, days := range [][]types.Instant{{day(-1), day(-2)}, {day(0), day(-3)}} {
		shards, _, opens = state()
		existing := uint64(0)
		if days[0] == day(0) {
			existing = 1 // day one exists, cold and closed: its open is the meter's
		}
		meter, err = hold(days...)
		if err != nil {
			t.Fatalf("two cold days, %d of them without a shard: %v", 2-existing, err)
		}
		if s, c, o := state(); s != shards || c != int(existing) || o-opens != existing || meter.Work().ShardOpens != existing {
			t.Fatalf("held: %d shards (were %d), %d cold open, %d opened by the store, %d on the meter; want no shard made and the %d existing opened", s, shards, c, o-opens, meter.Work().ShardOpens, existing)
		}
		err = w.g.Tx().RunContext(ctx, func(tx *graphpkg.GraphTx) error {
			for i, d := range days {
				if _, err := tx.AddNode([]string{"Event"}, map[string]any{"record": "held" + strconv.Itoa(int(d)) + strconv.Itoa(i), "tkg_valid_from": int64(d)}); err != nil {
					return err
				}
			}
			return nil
		})
		if s, c, o := state(); err != nil || s != shards+2-int(existing) || c != 2 || o-opens != existing {
			t.Fatalf("the write on the held days: %v, %d shards (were %d), %d cold open, %d opened by the store; want the missing made, both days open, no open beside the meter's %d", err, s, shards, c, o-opens, existing)
		}
		meter.Release()
		if _, c, _ := state(); c != 0 {
			t.Fatalf("after Release %d cold shards are open, want none", c)
		}
	}
}
