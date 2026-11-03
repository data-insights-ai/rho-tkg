package graph_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// REVIEW 3 Nov, WHOLEROW gaps: where a replaced node lives on a tiered store
// (event, cold, archive, MintAtValidFrom), a ValidAt index read after an
// earlier start, and a replace beside concurrent writers and readers.

// rvShardsHolding names every shard of ts whose store holds id's row.
func rvShardsHolding(t *testing.T, ts *tiered.Store, id types.NodeID) []string {
	t.Helper()
	infos, err := ts.ListShards()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, info := range infos {
		st, checkin, err := ts.ResolveShardStoreForTest(info.Name)
		if err != nil {
			t.Fatalf("resolve shard %q: %v", info.Name, err)
		}
		if st.HasNodeID(id.SnowflakeID()) {
			names = append(names, info.Name)
		}
		checkin()
	}
	slices.Sort(names)
	return names
}

func rvShardInfo(t *testing.T, ts *tiered.Store, name string) tiered.ShardInfo {
	t.Helper()
	infos, err := ts.ListShards()
	if err != nil {
		t.Fatal(err)
	}
	for _, info := range infos {
		if info.Name == name {
			return info
		}
	}
	t.Fatalf("no shard %q", name)
	return tiered.ShardInfo{}
}

// rvShardOfTime names the event shard whose window holds at.
func rvShardOfTime(t *testing.T, ts *tiered.Store, at time.Time) string {
	t.Helper()
	infos, err := ts.ListShards()
	if err != nil {
		t.Fatal(err)
	}
	for _, info := range infos {
		if info.Kind == tiered.ShardEvent && !at.Before(info.TimeStart) && at.Before(info.TimeEnd) {
			return info.Name
		}
	}
	t.Fatalf("no event shard holds %v", at)
	return ""
}

// rvWantReplaced asserts the replaced node reads as the new row by ID, by
// NodeAsOf at the replace, and that its history (the superseded rows) is the
// old row alone, closed at the replace.
func rvWantReplaced(t *testing.T, g *graphpkg.Graph, id types.NodeID, old, rep *types.Node, vf, vt types.Instant, props map[string]any) {
	t.Helper()
	got, err := g.Nodes().Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get after replace: %v", err)
	}
	wantRow(t, "Get after replace", got, rep.Version(), vf, vt, props)
	asOf, err := g.Temporal().NodeAsOf(id, rep.Temporal().TxFrom)
	if err != nil {
		t.Fatalf("NodeAsOf at the replace: %v", err)
	}
	wantRow(t, "NodeAsOf at the replace", asOf, rep.Version(), vf, vt, props)
	before, err := g.Temporal().NodeAsOf(id, rep.Temporal().TxFrom-1)
	if err != nil {
		t.Fatalf("NodeAsOf before the replace: %v", err)
	}
	wantRow(t, "NodeAsOf before the replace", before, old.Version(), old.Temporal().ValidFrom, old.Temporal().ValidTo, old.PropertiesMap())
	hist, err := g.Nodes().History(id)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(hist) != 1 || hist[0].Version() != old.Version() {
		t.Fatalf("History: %d rows, want the old version %d alone", len(hist), old.Version())
	}
	if hist[0].Temporal().TxTo != rep.Temporal().TxFrom {
		t.Fatalf("superseded row TxTo %d, want the replace's TxFrom %d", hist[0].Temporal().TxTo, rep.Temporal().TxFrom)
	}
}

// Gap 1a: an event node on a warm shard (rotated out of hot), replaced with a
// start inside the new hot shard's window, stays on its own shard.
func TestReplaceVersionEventShardNodeStaysOnItsShard(t *testing.T) {
	ctx := context.Background()
	ts, err := tiered.New(tiered.Config{DataDir: t.TempDir(), ShardWindow: time.Hour, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: ts, ReplaceVersionLabels: []string{"Row"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })

	n, err := g.Nodes().Add(ctx, []string{"Row"}, map[string]any{"k": "old", "tkg_valid_from": pageOldVF})
	if err != nil {
		t.Fatal(err)
	}
	home := rvShardsHolding(t, ts, n.ID())
	if len(home) != 1 || home[0] == "reference" {
		t.Fatalf("event node on %v, want one event shard", home)
	}
	if err := g.Tier().ForceRotate(); err != nil {
		t.Fatal(err)
	}
	if info := rvShardInfo(t, ts, home[0]); info.Tier != tiered.TierWarm {
		t.Fatalf("node's shard %q is %s after rotation, want warm", home[0], info.Tier)
	}
	hot := ts.HotShardForTest().Name()
	if hot == home[0] {
		t.Fatalf("rotation left %q hot", hot)
	}
	// The new start lies in the hot shard's window: a replace that placed by
	// the new start would land there.
	vf := types.Instant(time.Now().UnixMilli())
	if got := rvShardOfTime(t, ts, time.UnixMilli(int64(vf))); got != hot {
		t.Fatalf("new start %d falls on %q, want the hot shard %q", vf, got, hot)
	}
	props := map[string]any{"k": "new"}
	rep, err := g.Nodes().ReplaceVersion(ctx, n.ID(), props, vf, 0)
	if err != nil {
		t.Fatalf("ReplaceVersion of a warm event node: %v", err)
	}
	if got := rvShardsHolding(t, ts, n.ID()); !slices.Equal(got, home) {
		t.Fatalf("node on %v after replace, want only its shard %v", got, home)
	}
	rvWantReplaced(t, g, n.ID(), n, rep, vf, 0, props)
}

// Gap 1b: on a MintAtValidFrom store a node on a cold day, replaced with a
// start on another day that has a shard, an earlier start on a day with none,
// stays on the shard of its ID (its retention day): the cold shard is opened
// for the write, nothing is made or written elsewhere, and a reopen reads it.
func TestReplaceVersionMintAtValidFromColdNodeStaysOnItsIDsDay(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	open := func() (*tiered.Store, *graphpkg.Graph) {
		ts, err := tiered.New(tiered.Config{DataDir: dir, ShardWindow: 24 * time.Hour, ShardWindowLocation: time.UTC,
			ColdAfter: 48 * time.Hour, FlushInterval: 1<<63 - 1, MintAtValidFrom: true,
			Clock: func() time.Time { return tierClockDay(25, 9) }})
		if err != nil {
			t.Fatal(err)
		}
		g, err := graphpkg.New(graphpkg.Config{Store: ts, ReplaceVersionLabels: []string{"Row"}})
		if err != nil {
			t.Fatal(err)
		}
		return ts, g
	}
	ts, g := open()
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = g.Close()
		}
	})

	n, err := g.Nodes().Add(ctx, []string{"Row"}, map[string]any{"k": "old", "tkg_valid_from": types.Instant(tierClockDay(22, 12).UnixMilli())})
	if err != nil {
		t.Fatal(err)
	}
	// Another node makes the 24th's shard, the day the first replace moves
	// the start to.
	if _, err := g.Nodes().Add(ctx, []string{"Row"}, map[string]any{"k": "other", "tkg_valid_from": types.Instant(tierClockDay(24, 1).UnixMilli())}); err != nil {
		t.Fatal(err)
	}
	day22 := rvShardOfTime(t, ts, tierClockDay(22, 0))
	day24 := rvShardOfTime(t, ts, tierClockDay(24, 0))
	if day22 == day24 {
		t.Fatalf("the 22nd and the 24th share shard %q", day22)
	}
	if got := rvShardsHolding(t, ts, n.ID()); !slices.Equal(got, []string{day22}) {
		t.Fatalf("node on %v, want %q", got, day22)
	}
	ts.SetIdleTimeoutForTest(time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	ts.CloseIdleShardsForTest()
	info := rvShardInfo(t, ts, day22)
	if info.Tier != tiered.TierCold || info.Open {
		t.Fatalf("shard %q tier %s open %v, want cold and closed", day22, info.Tier, info.Open)
	}
	shardsBefore, err := ts.ListShards()
	if err != nil {
		t.Fatal(err)
	}

	vf := types.Instant(tierClockDay(24, 12).UnixMilli())
	props := map[string]any{"k": "later"}
	rep, err := g.Nodes().ReplaceVersion(ctx, n.ID(), props, vf, 0)
	if err != nil {
		t.Fatalf("ReplaceVersion of a cold node with a start on another day: %v", err)
	}
	if after := rvShardInfo(t, ts, day22); after.ColdOpens <= info.ColdOpens {
		t.Fatalf("cold opens of %q %d after replace, want above %d: the write did not reach the cold shard", day22, after.ColdOpens, info.ColdOpens)
	}
	if got := rvShardsHolding(t, ts, n.ID()); !slices.Equal(got, []string{day22}) {
		t.Fatalf("node on %v after replace, want only %q (its ID's day)", got, day22)
	}
	rvWantReplaced(t, g, n.ID(), n, rep, vf, 0, props)

	// An earlier start on a day with no shard: no shard is made for it.
	vf2 := types.Instant(tierClockDay(18, 6).UnixMilli())
	props2 := map[string]any{"k": "earlier"}
	rep2, err := g.Nodes().ReplaceVersion(ctx, n.ID(), props2, vf2, 0)
	if err != nil {
		t.Fatalf("ReplaceVersion with a start on a day without a shard: %v", err)
	}
	shardsAfter, err := ts.ListShards()
	if err != nil {
		t.Fatal(err)
	}
	if len(shardsAfter) != len(shardsBefore) {
		t.Fatalf("%d shards after the replaces, want the %d before: a replace made a shard", len(shardsAfter), len(shardsBefore))
	}
	if got := rvShardsHolding(t, ts, n.ID()); !slices.Equal(got, []string{day22}) {
		t.Fatalf("node on %v after the earlier start, want only %q", got, day22)
	}

	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	ts, g = open()
	t.Cleanup(func() { _ = g.Close() })
	got, err := g.Nodes().Get(ctx, n.ID())
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	wantRow(t, "Get after reopen", got, rep2.Version(), vf2, 0, props2)
	if got := rvShardsHolding(t, ts, n.ID()); !slices.Equal(got, []string{day22}) {
		t.Fatalf("node on %v after reopen, want only %q", got, day22)
	}
	hist, err := g.Nodes().History(n.ID())
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 2 || hist[0].Version() != n.Version() || hist[1].Version() != rep.Version() {
		t.Fatalf("History after reopen: %d rows, want versions %d and %d superseded", len(hist), n.Version(), rep.Version())
	}
}

// Gap 1c: an archived reference node is replaced in the archive: it stays
// there, the reference shard does not take it, and every read answers the
// new row.
func TestReplaceVersionArchivedNodeStaysInTheArchive(t *testing.T) {
	ctx := context.Background()
	ts, err := tiered.New(tiered.Config{DataDir: t.TempDir(), RefLabels: []string{pageLabel}, ShardWindow: time.Hour, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: ts, ReplaceVersionLabels: []string{pageLabel}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	n, err := g.Nodes().Add(ctx, []string{pageLabel}, map[string]any{"ov_k": "k1", "tkg_valid_from": pageOldVF})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Tier().Archive(n.ID()); err != nil {
		t.Fatal(err)
	}
	if got := rvShardsHolding(t, ts, n.ID()); !slices.Equal(got, []string{"archive"}) {
		t.Fatalf("archived node on %v, want only the archive", got)
	}
	rep, err := g.Nodes().ReplaceVersion(ctx, n.ID(), pageNewProps(), pageNewVF, pageNewVT)
	if err != nil {
		t.Fatalf("ReplaceVersion of an archived node: %v", err)
	}
	if got := rvShardsHolding(t, ts, n.ID()); !slices.Equal(got, []string{"archive"}) {
		t.Fatalf("archived node on %v after replace, want only the archive", got)
	}
	rvWantReplaced(t, g, n.ID(), n, rep, pageNewVF, pageNewVT, pageNewProps())
	ns, err := g.Nodes().ByLabel(pageLabel, storepkg.QueryOpts{})
	wantRow(t, "ByLabel after replace", onlyNode(t, "ByLabel", ns, err, n.ID()), rep.Version(), pageNewVF, pageNewVT, pageNewProps())
}

// Gap 2: a ValidAt read after a replace that set an earlier start answers at
// the new start, and at the old start with the new row, on the label scan
// with a temporal (envelope) index, the named NodesByLabelAt, a property
// index read and NodeAt; before the replace those instants answered nothing
// or the old row.
func TestReplaceVersionValidAtReadAfterAnEarlierStart(t *testing.T) {
	forReplaceVersionBackends(t, func(t *testing.T, g *graphpkg.Graph) {
		if err := g.Index().CreateTemporal(pageLabel); err != nil {
			t.Fatal(err)
		}
		f := writePage(t, g)
		const atNew, atOld, inside, beforeNew, afterOld, atEnd = types.Instant(500), pageOldVF, types.Instant(1500), types.Instant(499), types.Instant(2500), pageNewVT
		type read struct {
			name string
			run  func(at types.Instant) ([]*types.Node, error)
		}
		reads := []read{
			{"ByLabel ValidAt", func(at types.Instant) ([]*types.Node, error) {
				return g.Nodes().ByLabel(pageLabel, storepkg.QueryOpts{ValidAt: at})
			}},
			{"NodesByLabelAt", func(at types.Instant) ([]*types.Node, error) { return g.Temporal().NodesByLabelAt(pageLabel, at) }},
			{"ByLabelAndProperty ov_k ValidAt", func(at types.Instant) ([]*types.Node, error) {
				return g.Nodes().ByLabelAndProperty(pageLabel, "ov_k", "k1", storepkg.QueryOpts{ValidAt: at})
			}},
			{"NodeAt", func(at types.Instant) ([]*types.Node, error) {
				n, err := g.Temporal().NodeAt(f.id, at)
				if errors.Is(err, graphpkg.ErrNoVersionValidAt) {
					return nil, nil // no version valid there
				}
				if err != nil {
					return nil, err
				}
				return []*types.Node{n}, nil
			}},
		}
		// Before: nothing at the new start, the old row at the old start.
		for _, r := range reads {
			ns, err := r.run(atNew)
			if err != nil || len(ns) != 0 {
				t.Fatalf("before replace, %s at %d: %d rows, %v; want none", r.name, atNew, len(ns), err)
			}
			ns, err = r.run(atOld)
			wantRow(t, "before replace, "+r.name+" at the old start", onlyNode(t, r.name, ns, err, f.id), f.old.Version(), pageOldVF, pageOldVT, f.old.PropertiesMap())
		}
		rep := replacePage(t, g, f)
		for _, r := range reads {
			for _, at := range []types.Instant{atNew, atOld, inside, afterOld} {
				ns, err := r.run(at)
				wantRow(t, fmt.Sprintf("%s at %d", r.name, at), onlyNode(t, r.name, ns, err, f.id), rep.Version(), pageNewVF, pageNewVT, pageNewProps())
			}
			for _, at := range []types.Instant{beforeNew, atEnd} {
				if ns, err := r.run(at); err != nil || len(ns) != 0 {
					t.Fatalf("%s at %d, outside [%d, %d): %d rows, %v; want none", r.name, at, pageNewVF, pageNewVT, len(ns), err)
				}
			}
		}
	})
}

// Gap 3: replaces from two goroutines and patches from a third on one node,
// a writer on another node, and readers of both: every call succeeds, a
// reader never sees a version fall or a replaced row with another row's
// validity, and the final chain is every write once, versions contiguous,
// TxFrom rising, each superseded row closed at its successor's TxFrom.
func TestReplaceVersionBesideConcurrentWriters(t *testing.T) {
	forReplaceVersionBackends(t, func(t *testing.T, g *graphpkg.Graph) {
		ctx := context.Background()
		mk := func(key string) types.NodeID {
			n, err := g.Nodes().Add(ctx, []string{pageLabel}, map[string]any{"ov_k": key, "tkg_valid_from": pageOldVF})
			if err != nil {
				t.Fatal(err)
			}
			return n.ID()
		}
		a, b := mk("a"), mk("b")
		const rounds = 40
		errs := make(chan error, 16)
		var writers, readers sync.WaitGroup
		stop := make(chan struct{})
		replacer := func(id types.NodeID, key string, base int) {
			defer writers.Done()
			for i := range rounds {
				r := int64(base + i)
				vf := types.Instant(1000 - r%500)
				props := map[string]any{"ov_k": key, "r": r, "vf": int64(vf)}
				if _, err := g.Nodes().ReplaceVersion(ctx, id, props, vf, 0); err != nil {
					errs <- fmt.Errorf("replace %d of %d: %w", r, id, err)
					return
				}
			}
		}
		patcher := func(id types.NodeID) {
			defer writers.Done()
			for j := range rounds {
				if _, err := g.Nodes().Update(ctx, id, map[string]any{"u": int64(j)}); err != nil {
					errs <- fmt.Errorf("update %d of %d: %w", j, id, err)
					return
				}
			}
		}
		reader := func(id types.NodeID) {
			defer readers.Done()
			var last uint32
			for {
				select {
				case <-stop:
					return
				default:
				}
				n, err := g.Nodes().Get(ctx, id)
				if err != nil {
					errs <- fmt.Errorf("read %d: %w", id, err)
					return
				}
				if n.Version() < last {
					errs <- fmt.Errorf("read %d: version %d after %d", id, n.Version(), last)
					return
				}
				last = n.Version()
				// A row a replace wrote carries its own start; a later patch
				// adds u and takes its own start.
				p := n.PropertiesMap()
				if vf, ok := p["vf"]; ok {
					if _, patched := p["u"]; !patched && n.Temporal().ValidFrom != types.Instant(vf.(int64)) {
						errs <- fmt.Errorf("read %d: replaced row r=%v with start %d, want %d", id, p["r"], n.Temporal().ValidFrom, vf)
						return
					}
				}
			}
		}
		writers.Add(5)
		readers.Add(2)
		go replacer(a, "a", 0)
		go replacer(a, "a", 1000)
		go patcher(a)
		go replacer(b, "b", 2000)
		go patcher(b)
		go reader(a)
		go reader(b)
		writers.Wait()
		close(stop)
		readers.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}

		for _, c := range []struct {
			id     types.NodeID
			writes int
		}{{a, 3 * rounds}, {b, 2 * rounds}} {
			hist, err := g.Nodes().History(c.id)
			if err != nil {
				t.Fatal(err)
			}
			cur, err := g.Nodes().Get(ctx, c.id)
			if err != nil {
				t.Fatal(err)
			}
			hist = append(hist, cur)
			if len(hist) != 1+c.writes {
				t.Fatalf("node %d: %d versions, want %d (the create and every write once)", c.id, len(hist), 1+c.writes)
			}
			seen := map[int64]bool{}
			for i, row := range hist {
				if row.Version() != hist[0].Version()+uint32(i) {
					t.Fatalf("node %d: version %d at %d, want contiguous from %d", c.id, row.Version(), i, hist[0].Version())
				}
				if i > 0 {
					prev := hist[i-1].Temporal()
					if row.Temporal().TxFrom < prev.TxFrom || prev.TxTo != row.Temporal().TxFrom {
						t.Fatalf("node %d version %d: TxFrom %d, previous [%d, %d): not closed at its successor", c.id, row.Version(), row.Temporal().TxFrom, prev.TxFrom, prev.TxTo)
					}
				}
				if r, ok := row.PropertiesMap()["r"]; ok {
					if _, patched := row.PropertiesMap()["u"]; !patched {
						if seen[r.(int64)] {
							t.Fatalf("node %d: replace r=%d written twice", c.id, r)
						}
						seen[r.(int64)] = true
					}
				}
			}
			if ok, err := g.Hash().VerifyNodeChain(c.id); err != nil || !ok {
				t.Fatalf("node %d: hash chain verifies %v, %v", c.id, ok, err)
			}
		}
	})
}
