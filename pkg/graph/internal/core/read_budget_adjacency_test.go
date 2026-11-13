package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RH list R2, the adjacency doors of one node (Rels().OutgoingCtx and
// IncomingCtx): the adjacency entries the read examines count as candidates
// before they are collected, the stored bytes of each row decoded count before
// the decode, on tiered each closed shard (a reference node's mint-placed rels
// included) counts before it opens; past a limit *store.ReadBudgetError and no
// rows; without a budget exactly Outgoing and Incoming.

// TestReadBudget_AdjacencyDoors: on every backend a node with 30 relationships
// of one type and 10 of another counts its adjacency before the read: the
// untyped and the typed doors, outgoing and incoming. Within a budget as wide as
// the unlimited count the answer is the plain door's; one less refuses with no
// rows. The type filter is not free (a RAM index examines the node's whole set,
// a disk index only the type's entries): the count is at least the matches and
// at most the node's whole adjacency. An unknown node, an unregistered type and
// an invalid type name answer as the plain doors; a cancelled context is
// refused before the budget counts.
func TestReadBudget_AdjacencyDoors(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			s, e := txbEndpoints(t, g)
			const typed, other = 30, 10
			for i := range typed {
				if _, err := g.Rels.AddByID(ctx, "R", s, e, map[string]any{"i": int64(i)}); err != nil {
					t.Fatal(err)
				}
			}
			for range other {
				if _, err := g.Rels.AddByID(ctx, "S", s, e, nil); err != nil {
					t.Fatal(err)
				}
			}
			doors := []struct {
				name     string
				door     string
				node     types.NodeID
				typeName string
				incoming bool
				want     int
			}{
				{"outgoing typed", "Outgoing", s, "R", false, typed},
				{"outgoing all", "Outgoing", s, "", false, typed + other},
				{"incoming typed", "Incoming", e, "R", true, typed},
				{"incoming all", "Incoming", e, "", true, typed + other},
			}
			for _, d := range doors {
				plain := func() ([]*types.Relationship, error) {
					if d.incoming {
						return g.Rels.Incoming(d.node, d.typeName)
					}
					return g.Rels.Outgoing(d.node, d.typeName)
				}
				call := func(c context.Context) ([]*types.Relationship, error) {
					if d.incoming {
						return g.Rels.IncomingCtx(c, d.node, d.typeName)
					}
					return g.Rels.OutgoingCtx(c, d.node, d.typeName)
				}
				want, err := plain()
				if err != nil || len(want) != d.want {
					t.Fatalf("%s: plain door = %d rels, %v; want %d", d.name, len(want), err, d.want)
				}
				if got, err := call(ctx); err != nil || !slices.Equal(relIDsOf(got), relIDsOf(want)) {
					t.Fatalf("%s: no budget = %d rels, %v; want the plain door's", d.name, len(got), err)
				}
				fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
				got, err := call(fctx)
				cand := full.Work().Candidates
				if err != nil || !slices.Equal(relIDsOf(got), relIDsOf(want)) || cand < uint64(d.want) || cand > typed+other {
					t.Fatalf("%s: unlimited = %d rels, %v, %d candidates; want the plain answer and %d to %d candidates", d.name, len(got), err, cand, d.want, typed+other)
				}
				bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: cand})
				if got, err := call(bctx); err != nil || !slices.Equal(relIDsOf(got), relIDsOf(want)) || meter.Work().Candidates != cand {
					t.Fatalf("%s: budget %d = %d rels, %v, %+v; want the plain answer and %d candidates", d.name, cand, len(got), err, meter.Work(), cand)
				}
				bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: cand - 1})
				got, err = call(bctx)
				var rb *storepkg.ReadBudgetError
				if !errors.As(err, &rb) || rb.Door != d.door || rb.Limit != "candidates" || len(got) != 0 || meter.Work().Candidates <= cand-1 {
					t.Fatalf("%s: budget %d = %d rels, %v (meter %+v); want ErrReadBudget from %s at candidates and no rows", d.name, cand-1, len(got), err, meter.Work(), d.door)
				}
			}

			// Edges: the plain doors' answers and errors.
			longName := strings.Repeat("x", 1<<20)
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1000})
			if _, err := g.Rels.OutgoingCtx(bctx, types.NodeID(1), ""); err == nil || errors.Is(err, storepkg.ErrReadBudget) {
				t.Fatalf("OutgoingCtx of an unknown node = %v, want the plain door's error", err)
			}
			if _, err := g.Rels.Outgoing(types.NodeID(1), ""); err == nil {
				t.Fatal("plain Outgoing of an unknown node answered")
			}
			if got, err := g.Rels.OutgoingCtx(bctx, s, "NOSUCH"); err != nil || got != nil {
				t.Fatalf("OutgoingCtx of an unregistered type = %v, %v; want nil, nil", got, err)
			}
			if _, err := g.Rels.Outgoing(s, longName); err == nil {
				t.Fatal("plain Outgoing of an invalid type name answered")
			}
			if _, err := g.Rels.OutgoingCtx(bctx, s, longName); err == nil || errors.Is(err, storepkg.ErrReadBudget) {
				t.Fatalf("OutgoingCtx of an invalid type name = %v, want the plain door's error", err)
			}
			_ = meter

			cctx, cancel := context.WithCancel(ctx)
			cancel()
			bctx, meter = storepkg.WithReadBudget(cctx, storepkg.ReadBudget{Candidates: 100})
			if _, err := g.Rels.OutgoingCtx(bctx, s, "R"); !errors.Is(err, context.Canceled) || meter.Work() != (storepkg.ReadWork{}) {
				t.Fatalf("cancelled context: %v, work %+v", err, meter.Work())
			}
		})
	}
}

// TestReadBudget_DecodedBytes_Adjacency: the rows an adjacency read decodes
// count their stored bytes before the decode, outgoing and incoming. Cold, the
// count covers every row's wire bytes and is the same on every cold run; within
// a budget of exactly that count the door answers; one byte under it returns
// ErrReadBudget "decoded bytes" and no rows. Warm, the entity cache serves the
// rows and the read counts 0.
func TestReadBudget_DecodedBytes_Adjacency(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			ctx := context.Background()
			s, e := txbEndpoints(t, g)
			const rows = 6
			var ids []types.RelID
			for i := range rows {
				r, err := g.Rels.AddByID(ctx, "R", s, e, map[string]any{"pad": fmt.Sprintf("%0*d", 100+i, i)})
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, r.ID())
			}
			shut()
			g, shut = be.open(t, dir)
			defer shut()
			for _, d := range []struct {
				name, door string
				call       func(ctx context.Context) ([]*types.Relationship, error)
			}{
				{"outgoing", "Outgoing", func(ctx context.Context) ([]*types.Relationship, error) { return g.Rels.OutgoingCtx(ctx, s, "R") }},
				{"incoming", "Incoming", func(ctx context.Context) ([]*types.Relationship, error) { return g.Rels.IncomingCtx(ctx, e, "R") }},
			} {
				rbdExact(t, d.name, d.door, rows*100,
					func() { rbdEvict(t, g, nil, ids) },
					func(ctx context.Context) (bool, error) {
						got, err := d.call(ctx)
						return len(got) == rows, err
					})
				// Warm: the entity cache serves the rows; nothing is decoded.
				if _, err := d.call(context.Background()); err != nil {
					t.Fatal(err)
				}
				bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{DecodedBytes: 1})
				got, err := d.call(bctx)
				if err != nil || len(got) != rows || meter.Work().DecodedBytes != 0 {
					t.Fatalf("warm %s = %d rows, %v, %+v; want the rows and 0 decoded bytes", d.name, len(got), err, meter.Work())
				}
			}
		})
	}
}

// TestReadBudget_AdjacencyProbeStopsEarly (the probe criterion for the
// adjacency doors): over a hub of 8, 32 and 128 relationships, flushed, a
// budget of 1 on candidates or on decoded bytes stops the read with the typed
// error and no rows, having decoded about one row, whatever the hub's degree:
// on badger the store's own relationship decode counter moves by at most 2, and
// the bytes counted stay about one row's.
func TestReadBudget_AdjacencyProbeStopsEarly(t *testing.T) {
	t.Parallel()
	sizes := []int{8, 32, 128}
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			ctx := context.Background()
			hubs := map[int][2]types.NodeID{}
			var all []types.RelID
			for _, size := range sizes {
				s, err := g.Nodes.Add(ctx, []string{"Ref"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				e, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				hubs[size] = [2]types.NodeID{s.ID(), e.ID()}
				for i := range size {
					r, err := g.Rels.AddByID(ctx, "R", s.ID(), e.ID(), map[string]any{"pad": fmt.Sprintf("%0*d", 120, i)})
					if err != nil {
						t.Fatal(err)
					}
					all = append(all, r.ID())
				}
			}
			shut()
			g, shut = be.open(t, dir)
			defer shut()
			for _, size := range sizes {
				for _, d := range []struct {
					name, door string
					call       func(ctx context.Context) (int, error)
				}{
					{"outgoing", "Outgoing", func(ctx context.Context) (int, error) {
						r, err := g.Rels.OutgoingCtx(ctx, hubs[size][0], "R")
						return len(r), err
					}},
					{"incoming", "Incoming", func(ctx context.Context) (int, error) {
						r, err := g.Rels.IncomingCtx(ctx, hubs[size][1], "R")
						return len(r), err
					}},
				} {
					what := fmt.Sprintf("%s over %d relationships", d.name, size)
					rbdEvict(t, g, nil, all)
					fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
					if n, err := d.call(fctx); err != nil || n != size {
						t.Fatalf("%s: unlimited = %d, %v", what, n, err)
					}
					total := full.Work()
					if total.Candidates < uint64(size) || total.DecodedBytes < uint64(size)*100 {
						t.Fatalf("%s: unlimited counted %+v, want at least %d candidates and %d decoded bytes", what, total, size, size*100)
					}
					for _, c := range []struct {
						limit string
						set   func(*storepkg.ReadBudget)
					}{
						{"candidates", func(b *storepkg.ReadBudget) { b.Candidates = 1 }},
						{"decoded bytes", func(b *storepkg.ReadBudget) { b.DecodedBytes = 1 }},
					} {
						rbdEvict(t, g, nil, all)
						var b storepkg.ReadBudget
						c.set(&b)
						var before int64
						if bs, ok := g.store.(*badger.Store); ok {
							before = bs.RelRowDecodesForTest()
						}
						pctx, meter := storepkg.WithReadBudget(ctx, b)
						n, err := d.call(pctx)
						rb := rbdBudgetErr(t, err, c.limit)
						got := meter.Work()
						if n != 0 || rb.Door != d.door || got.Candidates < 2 && c.limit == "candidates" {
							t.Fatalf("%s, %s 1: %d rows, %+v, meter %+v; want no rows from %s", what, c.limit, n, rb, got, d.door)
						}
						if bs, ok := g.store.(*badger.Store); ok {
							if decoded := bs.RelRowDecodesForTest() - before; decoded > 2 {
								t.Fatalf("%s, %s 1: %d rows decoded before the refusal", what, c.limit, decoded)
							}
						}
						if c.limit == "decoded bytes" && got.DecodedBytes > 3*total.DecodedBytes/uint64(size) {
							t.Fatalf("%s, %s 1: decoded %d bytes before the refusal (unlimited %d over %d rows)", what, c.limit, got.DecodedBytes, total.DecodedBytes, size)
						}
					}
				}
			}
		})
	}
}

// TestReadBudget_DiskAdjacencyAdmitsAsProduced: with the adjacency index on
// disk (Config.AdjacencyIndexOnDisk) a budgeted adjacency read admits each
// entry as the iterator produces it and refuses at the one that passes the
// limit, before it collects the node's whole adjacency. The gate is the count
// of on-disk index entries the process visited
// (badger.CountIndexEntriesVisitedForTest): unbudgeted the read visits at
// least every entry of the hub (96); under a Candidates budget of 1 a handful.
func TestReadBudget_DiskAdjacencyAdmitsAsProduced(t *testing.T) {
	const matches = 96
	dir := t.TempDir()
	open := func() *Core {
		g, err := New(Config{BadgerDir: dir, AdjacencyIndexOnDisk: true, UntimedValidStart: UntimedFromTxFrom})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return g
	}
	g := open()
	ctx := context.Background()
	s, e := txbEndpoints(t, g)
	for range matches {
		if _, err := g.Rels.AddByID(ctx, "R", s, e, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g = open() // every entry is on disk, the write buffer empty
	defer func() { _ = g.Close() }()
	visited, stop := badger.CountIndexEntriesVisitedForTest()
	defer stop()
	for _, d := range []struct {
		name string
		call func(ctx context.Context) (int, error)
	}{
		{"outgoing", func(ctx context.Context) (int, error) {
			r, err := g.Rels.OutgoingCtx(ctx, s, "R")
			return len(r), err
		}},
		{"incoming", func(ctx context.Context) (int, error) {
			r, err := g.Rels.IncomingCtx(ctx, e, "R")
			return len(r), err
		}},
	} {
		t.Run(d.name, func(t *testing.T) {
			before := visited()
			fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			if n, err := d.call(fctx); err != nil || n != matches {
				t.Fatalf("unlimited = %d, %v; want %d", n, err, matches)
			}
			all := visited() - before
			if all < matches || full.Work().Candidates != matches {
				t.Fatalf("an unbudgeted read visited %d adjacency entries and counted %d candidates over %d: the counter does not see this walk", all, full.Work().Candidates, matches)
			}
			before = visited()
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1})
			n, err := d.call(bctx)
			steps := visited() - before
			var rb *storepkg.ReadBudgetError
			if !errors.As(err, &rb) || rb.Limit != "candidates" || n != 0 || rb.Work.Candidates != 2 || meter.Work() != rb.Work {
				t.Fatalf("budget 1 = %d rows, %v (meter %+v); want ErrReadBudget at candidates, no rows, work 2", n, err, meter.Work())
			}
			t.Logf("budget 1 visited %d adjacency entries (an unbudgeted read %d, %d entries)", steps, all, matches)
			if steps > 6 {
				t.Fatalf("budget 1 visited %d adjacency entries (an unbudgeted read %d, %d entries); want at most 6: the read collected the whole adjacency before it admitted one", steps, all, matches)
			}
		})
	}
}

// TestReadBudget_ShardOpens_TieredAdjacency: a reference node's mint-placed
// relationships (Config.RelsByMint) lie on the event shards, so its adjacency
// read opens every closed one (backlog 82). Under a budget each open counts
// before it happens: a ShardOpens budget of 1 refuses with no rows having opened
// at most one closed shard (the store's ColdOpens counter); unlimited, the
// answer is the plain door's and the meter counts exactly the opens.
func TestReadBudget_ShardOpens_TieredAdjacency(t *testing.T) {
	t.Parallel()
	ts, err := tiered.New(tiered.Config{
		DataDir:           t.TempDir(),
		RefLabels:         []string{"Ref"},
		ShardWindow:       7 * 24 * time.Hour,
		FlushInterval:     1<<63 - 1,
		MaxOpenColdShards: 8,
		RelsByMint:        true,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := New(Config{Store: ts, UntimedValidStart: UntimedFromTxFrom})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close(); _ = ts.Close() })
	ctx := context.Background()
	a, err := g.Nodes.Add(ctx, []string{"Ref"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(ctx, []string{"Ref"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	const shards = 3
	for range shards {
		if _, err := g.Rels.AddByID(ctx, "LINK", a.ID(), b.ID(), map[string]any{"pad": "x"}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
		if err := ts.RotateHotShard(); err != nil {
			t.Fatal(err)
		}
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
	for _, d := range []struct {
		name, door string
		call       func(ctx context.Context) (int, error)
	}{
		{"outgoing", "Outgoing", func(ctx context.Context) (int, error) {
			r, err := g.Rels.OutgoingCtx(ctx, a.ID(), "LINK")
			return len(r), err
		}},
		{"incoming", "Incoming", func(ctx context.Context) (int, error) {
			r, err := g.Rels.IncomingCtx(ctx, b.ID(), "LINK")
			return len(r), err
		}},
	} {
		t.Run(d.name, func(t *testing.T) {
			closeCold()
			want, err := d.call(ctx)
			if err != nil || want != shards {
				t.Fatalf("no budget = %d, %v; want %d", want, err, shards)
			}

			closeCold()
			opens := ts.ShardAccessStats().ColdOpens
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
			got, err := d.call(bctx)
			opened := ts.ShardAccessStats().ColdOpens - opens
			var rb *storepkg.ReadBudgetError
			if !errors.Is(err, storepkg.ErrReadBudget) || !errors.As(err, &rb) || rb.Limit != "shard opens" || got != 0 {
				t.Fatalf("%s under ShardOpens 1 = %d rows, %v (work %+v, %d closed shards opened); want a shard-opens refusal and no rows", d.name, got, err, meter.Work(), opened)
			}
			if rb.Door != d.door || opened > 1 {
				t.Fatalf("%s refused by door %q having opened %d closed shards, want %q and at most 1", d.name, rb.Door, opened, d.door)
			}

			closeCold()
			opens = ts.ShardAccessStats().ColdOpens
			bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			got, err = d.call(bctx)
			opened = ts.ShardAccessStats().ColdOpens - opens
			if err != nil || got != want {
				t.Fatalf("%s unlimited = %d, %v; want %d", d.name, got, err, want)
			}
			if opened < shards || meter.Work().ShardOpens != opened {
				t.Fatalf("%s unlimited opened %d closed shards, counted %d; want >= %d, all counted", d.name, opened, meter.Work().ShardOpens, shards)
			}
		})
	}
}
