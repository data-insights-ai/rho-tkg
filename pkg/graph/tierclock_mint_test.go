package graph_test

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	temporalpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// tierClockStore opens a tiered store that mints at valid time (ADR-0013),
// its clock fixed at 2026-07-25 09:00 UTC, day windows, Case a reference label.
func tierClockStore(t *testing.T) *tieredpkg.Store {
	t.Helper()
	ts, err := tieredpkg.New(tieredpkg.Config{DataDir: t.TempDir(), RefLabels: []string{"Case"}, ShardWindow: 24 * time.Hour,
		ShardWindowLocation: time.UTC, ColdAfter: 48 * time.Hour, FlushInterval: 1<<63 - 1, MintAtValidFrom: true,
		Clock: func() time.Time { return tierClockDay(25, 9) }})
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

func tierClockDay(day, hour int) time.Time { return time.Date(2026, 7, day, hour, 0, 0, 0, time.UTC) }

func tierClockAt(day, hour int) map[string]any {
	return map[string]any{"tkg_valid_from": types.Instant(tierClockDay(day, hour).UnixMilli())}
}

// TestTierClockGraph_GeneratedIDsCarryValidTime (TIERCLOCK step 2): on a
// tiered store with MintAtValidFrom every generated-ID create door takes its
// event IDs from the store: a node's at its ValidFrom (untimed: the store's
// clock), a relationship's inside its start node's day (a reference start
// node: at its ValidFrom), all on the reserved node field 31; a reference
// node keeps the generator; a ValidFrom outside the ID range is refused on a
// node and minted at the clock on a reference start; and graph.New refuses
// generators that would draw field 31.
func TestTierClockGraph_GeneratedIDsCarryValidTime(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{Store: tierClockStore(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	field := func(id int64) int64 { return id >> 10 & 31 }
	when := func(id interface{ MintInstant() types.Instant }) time.Time {
		return time.UnixMilli(int64(id.MintInstant())).UTC()
	}
	inDay := func(what string, id int64, at time.Time, day int) {
		t.Helper()
		if field(id) != 31 || at.Before(tierClockDay(day, 0)) || !at.Before(tierClockDay(day+1, 0)) {
			t.Fatalf("%s: ID %d (field %d, time %v), want field 31 on 2026-07-%d", what, id, field(id), at, day)
		}
	}
	exact := func(what string, id int64, at, want time.Time) {
		t.Helper()
		if field(id) != 31 || !at.Equal(want) {
			t.Fatalf("%s: ID %d (field %d, time %v), want field 31 at %v", what, id, field(id), at, want)
		}
	}

	sig, err := g.Nodes().Add(ctx, []string{"Signal"}, tierClockAt(22, 12))
	if err != nil {
		t.Fatal(err)
	}
	exact("Nodes().Add", int64(sig.ID()), when(sig.ID()), tierClockDay(22, 12))
	untimed, err := g.Nodes().Add(ctx, []string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	exact("untimed node", int64(untimed.ID()), when(untimed.ID()), tierClockDay(25, 9))
	kase, err := g.Nodes().Add(ctx, []string{"Case"}, tierClockAt(22, 12))
	if err != nil {
		t.Fatal(err)
	}
	if field(int64(kase.ID())) == 31 {
		t.Fatalf("reference node %d minted by the store", kase.ID())
	}
	r, err := g.Rels().Add(ctx, "SEEN", sig, kase, tierClockAt(23, 5))
	if err != nil {
		t.Fatal(err)
	}
	inDay("Rels().Add, event start", int64(r.ID()), when(r.ID()), 22)
	r, err = g.Rels().Add(ctx, "OF", kase, sig, tierClockAt(24, 7))
	if err != nil {
		t.Fatal(err)
	}
	exact("Rels().Add, reference start", int64(r.ID()), when(r.ID()), tierClockDay(24, 7))
	before := map[string]any{"tkg_valid_from": types.Instant(time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC).UnixMilli())}
	if _, err := g.Nodes().Add(ctx, []string{"Signal"}, before); !errors.Is(err, graphpkg.ErrValidFromOutsideIDRange) {
		t.Fatalf("node before the epoch: %v, want ErrValidFromOutsideIDRange", err)
	}
	r, err = g.Rels().Add(ctx, "OF", kase, sig, before)
	if err != nil {
		t.Fatalf("reference start before the epoch: %v", err)
	}
	inDay("Rels().Add, reference start before the epoch", int64(r.ID()), when(r.ID()), 25)

	var txNodes []*types.Node
	var txRels []*types.Relationship
	err = g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		var err error
		if txNodes, err = tx.AddNodes([]string{"Signal"}, []map[string]any{tierClockAt(20, 1), tierClockAt(21, 2)}); err != nil {
			return err
		}
		txRels, err = tx.AddRelationships("SEEN", []graphpkg.RelCreate{
			{StartID: txNodes[0].ID(), EndID: kase.ID(), Props: tierClockAt(20, 3)},
			{StartID: kase.ID(), EndID: txNodes[1].ID(), Props: tierClockAt(21, 4)},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	exact("tx.AddNodes", int64(txNodes[0].ID()), when(txNodes[0].ID()), tierClockDay(20, 1))
	exact("tx.AddNodes", int64(txNodes[1].ID()), when(txNodes[1].ID()), tierClockDay(21, 2))
	exact("tx.AddRelationships, event start", int64(txRels[0].ID()), when(txRels[0].ID()), tierClockDay(20, 3))
	exact("tx.AddRelationships, reference start", int64(txRels[1].ID()), when(txRels[1].ID()), tierClockDay(21, 4))
	err = g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		txRels, err = tx.AddRelationships("OF", []graphpkg.RelCreate{{StartID: kase.ID(), EndID: sig.ID(), Props: before}})
		return err
	})
	if err != nil {
		t.Fatalf("tx reference start before the epoch: %v", err)
	}
	inDay("tx.AddRelationships, reference start before the epoch", int64(txRels[0].ID()), when(txRels[0].ID()), 25)
	err = g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		_, err := tx.AddNodes([]string{"Signal"}, []map[string]any{before})
		return err
	})
	if !errors.Is(err, graphpkg.ErrValidFromOutsideIDRange) {
		t.Fatalf("tx node before the epoch: %v, want ErrValidFromOutsideIDRange", err)
	}

	var bNode *types.Node
	var bRel *types.Relationship
	if _, err := g.Batch().Run(func(b *graphpkg.BatchBuilder) error {
		var err error
		if bNode, err = b.AddNode([]string{"Signal"}, tierClockAt(19, 6)); err != nil {
			return err
		}
		if err := b.AddNodes([]string{"Signal"}, tierClockAt(18, 6), 2); err != nil {
			return err
		}
		bRel, err = b.AddRelationship("SEEN", bNode, kase, tierClockAt(19, 8))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	exact("batch AddNode", int64(bNode.ID()), when(bNode.ID()), tierClockDay(19, 6))
	exact("batch AddRelationship", int64(bRel.ID()), when(bRel.ID()), tierClockDay(19, 8))
	n18 := 0
	if err := g.Nodes().ForEachByLabel("Signal", storepkg.QueryOpts{}, func(n *types.Node) bool {
		if when(n.ID()).Equal(tierClockDay(18, 6)) && field(int64(n.ID())) == 31 {
			n18++
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if n18 != 2 {
		t.Fatalf("batch AddNodes: %d nodes minted at their ValidFrom, want 2", n18)
	}
	if _, err := g.Batch().Run(func(b *graphpkg.BatchBuilder) error {
		_, err := b.AddNode([]string{"Signal"}, before)
		return err
	}); !errors.Is(err, graphpkg.ErrValidFromOutsideIDRange) {
		t.Fatalf("batch node before the epoch: %v, want ErrValidFromOutsideIDRange", err)
	}
	if _, err := g.Batch().Run(func(b *graphpkg.BatchBuilder) error {
		var err error
		bRel, err = b.AddRelationship("OF", kase, sig, before)
		return err
	}); err != nil {
		t.Fatalf("batch reference start before the epoch: %v", err)
	}
	inDay("batch AddRelationship, reference start before the epoch", int64(bRel.ID()), when(bRel.ID()), 25)

	// The constraint branch of the relationship kernel mints too.
	if err := g.Constraints().Add(temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints}); err != nil {
		t.Fatal(err)
	}
	r, err = g.Rels().Add(ctx, "SEEN", sig, kase, tierClockAt(25, 10))
	if err != nil {
		t.Fatal(err)
	}
	inDay("Rels().Add under a temporal constraint", int64(r.ID()), when(r.ID()), 22)
}

// TestTierClockGraph_ForeverEdgeBetweenReferenceNodes (PREEPOCHREL): an edge
// between reference nodes stamped valid from 1 ms, visible at any valid time,
// is written with an ID at the clock and keeps its stamp: every valid-time read
// answers from the stamp, not the ID's time. With RelsByMint it lands on the
// clock's day. Past the ID range likewise.
func TestTierClockGraph_ForeverEdgeBetweenReferenceNodes(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{Store: tierClockStore(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	a, err := g.Nodes().Add(ctx, []string{"Case"}, map[string]any{"tkg_valid_from": types.Instant(1)})
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes().Add(ctx, []string{"Case"}, map[string]any{"tkg_valid_from": types.Instant(1)})
	if err != nil {
		t.Fatal(err)
	}
	far := types.Instant(time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
	for _, stamp := range []types.Instant{1, far} {
		r, err := g.Rels().Add(ctx, "ABOUT", a, b, map[string]any{"tkg_valid_from": stamp})
		if err != nil {
			t.Fatalf("ValidFrom %d: %v", stamp, err)
		}
		at := time.UnixMilli(int64(r.ID().MintInstant())).UTC()
		if at.Before(tierClockDay(25, 0)) || !at.Before(tierClockDay(26, 0)) {
			t.Fatalf("ValidFrom %d: ID time %v, want the clock's day 2026-07-25", stamp, at)
		}
		got, err := g.Temporal().RelAt(r.ID(), stamp)
		if err != nil {
			t.Fatalf("ValidFrom %d: RelAt its own start: %v", stamp, err)
		}
		if tm := got.Temporal(); tm == nil || tm.ValidFrom != stamp {
			t.Fatalf("ValidFrom %d: stored %+v", stamp, tm)
		}
		if stamp == far {
			if _, err := g.Temporal().RelAt(r.ID(), types.Instant(tierClockDay(25, 9).UnixMilli())); err == nil {
				t.Fatal("an edge valid from 2040 answers at the clock: the read used its ID's time")
			}
		}
	}
	// An edge valid from 1 ms answers before the epoch and on any later day.
	out, err := g.Rels().ByType("ABOUT", storepkg.QueryOpts{ValidAt: types.Instant(time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC).UnixMilli()), ValidAtSet: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("ByType at 2025-06-01: %d edges, want the one valid from 1 ms", len(out))
	}
}

// TestTierClockGraph_NewRefusesField31: graph.New on a minting store refuses
// a generator on node field 31 and accepts the largest that stay clear of it;
// a tiered store without the option keeps every generator.
func TestTierClockGraph_NewRefusesField31(t *testing.T) {
	plain, err := tieredpkg.New(tieredpkg.Config{DataDir: t.TempDir(), ShardWindow: 24 * time.Hour, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: plain, SnowflakeNodeID: 15, IngestLanes: 30})
	if err != nil {
		t.Fatalf("SnowflakeNodeID 15, IngestLanes 30 without MintAtValidFrom: %v", err)
	}
	_ = g.Close()

	for _, c := range []struct {
		node  int64
		lanes uint8
		ok    bool
	}{{15, 0, false}, {14, 0, true}, {0, 30, false}, {0, 29, true}} {
		ts := tierClockStore(t)
		g, err := graphpkg.New(graphpkg.Config{Store: ts, SnowflakeNodeID: c.node, IngestLanes: c.lanes})
		if (err == nil) != c.ok {
			t.Fatalf("SnowflakeNodeID %d, IngestLanes %d: %v, want ok %v", c.node, c.lanes, err, c.ok)
		}
		if g != nil {
			_ = g.Close()
		} else {
			_ = ts.Close()
		}
	}
}

// TestTierClockGraph_Replay33Days (TIERCLOCK step 4): ai-soc's shape replayed
// in valid time, 33 days (22 Jul - 23 Aug 2026) in seconds. Reference
// entities, edges between them with tkg_valid_from, RelsByMint, the host
// clock at each replayed day, a tenth of the edges a day late. Each day gets
// its own shard, at most the hot day and the two warm ones hold a handle after
// an idle sweep (ColdAfter 48 h), so every per-shard resident set is bounded
// by the open days, not by the archive; every edge reads back and none falls
// back to the reference shard.
func TestTierClockGraph_Replay33Days(t *testing.T) {
	if testing.Short() {
		t.Skip("replays 33 days")
	}
	ctx := context.Background()
	first := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	var clock atomic.Int64
	clock.Store(first.Add(12 * time.Hour).UnixMilli())
	ts, err := tieredpkg.New(tieredpkg.Config{DataDir: t.TempDir(), RefLabels: []string{"Device"}, ShardWindow: 24 * time.Hour,
		ShardWindowLocation: time.UTC, ColdAfter: 48 * time.Hour, IdleTimeout: time.Millisecond, FlushInterval: 1<<63 - 1,
		MintAtValidFrom: true, RelsByMint: true, Clock: func() time.Time { return time.UnixMilli(clock.Load()).UTC() }})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: ts})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	const entities, edgesPerDay, days = 50, 300, 33
	devices := make([]types.NodeID, entities)
	for i := range devices {
		n, err := g.Nodes().Add(ctx, []string{"Device"}, map[string]any{"i": int64(i)})
		if err != nil {
			t.Fatal(err)
		}
		devices[i] = n.ID()
	}
	var edges []types.RelID
	var ms runtime.MemStats
	var heapDay9 uint64
	for d := range days {
		day := first.AddDate(0, 0, d)
		clock.Store(day.Add(12 * time.Hour).UnixMilli())
		for e := range edgesPerDay {
			at := day.Add(time.Duration(e) * 4 * time.Minute)
			if d > 0 && e%10 == 0 {
				at = at.Add(-20 * time.Hour) // late: the previous day
			}
			r, err := g.Rels().AddByID(ctx, "SEEN", devices[e%entities], devices[(e%entities+1+(e+d)%(entities-1))%entities],
				map[string]any{"tkg_valid_from": types.Instant(at.UnixMilli())})
			if err != nil {
				t.Fatalf("day %d edge %d: %v", d, e, err)
			}
			edges = append(edges, r.ID())
		}
		time.Sleep(2 * time.Millisecond) // past IdleTimeout on the wall clock idle close keeps
		ts.CloseIdleShardsForTest()
		shards, err := ts.ListShards()
		if err != nil {
			t.Fatal(err)
		}
		open, events := 0, 0
		for _, s := range shards {
			if s.Kind != tieredpkg.ShardEvent {
				continue
			}
			events++
			if s.Open {
				open++
			}
		}
		if events != d+1 || open > 3 {
			t.Fatalf("day %d: %d event shards (%d open), want %d shards, at most 3 open", d+1, events, open, d+1)
		}
		runtime.GC()
		runtime.ReadMemStats(&ms)
		if d%8 == 0 || d == days-1 {
			t.Logf("day %2d: %d event shards, %d open, heap %d KB", d+1, events, open, ms.HeapAlloc>>10)
		}
		if d == 8 {
			heapDay9 = ms.HeapAlloc
		}
	}
	if ms.HeapAlloc > heapDay9+heapDay9/10 { // flat once the open days are full: the archive does not stay resident
		t.Fatalf("heap %d KB on day %d, %d KB on day 9: it grows with the replayed days", ms.HeapAlloc>>10, days, heapDay9>>10)
	}
	for i, id := range edges {
		if i%7 != 0 {
			continue
		}
		if _, err := g.Rels().Get(ctx, id); err != nil {
			t.Fatalf("edge %d (%d) does not read back: %v", i, id, err)
		}
	}
	if n := ts.RelsByMintFallbacks(); n != 0 {
		t.Fatalf("RelsByMintFallbacks = %d, want 0", n)
	}
}
