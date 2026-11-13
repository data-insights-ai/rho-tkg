package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RH list R2, the property lookup doors Sigma reads: Rels().ByTypeAndPropertyCtx
// and Nodes().IDsByLabelAndPropertyValuesCtx. Each counts the entries it
// examines as candidates inside the store, before it collects, filters or
// decodes them, and returns *store.ReadBudgetError and no answer past a limit;
// without a budget it is the plain door.

// TestReadBudget_RelByTypeAndPropertyCtx: an indexed lookup counts its matches;
// a key without an index falls back to the type's every relationship, counted
// before the scan reads a row; within a budget as wide as that count the
// answer is the plain door's; one less refuses with no rows; a read at a time
// coordinate, which this door does not budget, is refused under a budget and
// answers without one; a cancelled context is refused before the budget
// counts.
func TestReadBudget_RelByTypeAndPropertyCtx(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			if err := g.Index.CreateRelProperty("R", "k"); err != nil {
				t.Fatal(err)
			}
			s, e := txbEndpoints(t, g)
			const rels = 30
			for i := range rels {
				r, err := g.Rels.AddByID(ctx, "R", s, e, map[string]any{"k": int64(i % 15), "u": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"w": int64(1)}); err != nil { // history
					t.Fatal(err)
				}
			}
			tx, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			doors := []struct {
				name  string
				key   string
				value int64
				want  int    // matches
				cand  uint64 // unbudgeted candidates
			}{
				{"indexed", "k", 1, 2, 2},
				{"no index", "u", 7, 1, rels}, // the type scan counts every relationship of the type
			}
			for _, d := range doors {
				call := func(c context.Context) ([]types.RelID, error) {
					r, err := g.Rels.ByTypeAndPropertyCtx(c, "R", d.key, d.value, storepkg.QueryOpts{})
					return relIDsOf(r), err
				}
				plain, err := g.Rels.ByTypeAndProperty("R", d.key, d.value, storepkg.QueryOpts{})
				if err != nil || len(plain) != d.want {
					t.Fatalf("%s: plain door = %d rows, %v; want %d", d.name, len(plain), err, d.want)
				}
				if got, err := call(ctx); err != nil || !slices.Equal(got, relIDsOf(plain)) {
					t.Fatalf("%s: no budget = %v, %v; want the plain door's %v", d.name, got, err, relIDsOf(plain))
				}
				bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: d.cand})
				got, err := call(bctx)
				if err != nil || !slices.Equal(got, relIDsOf(plain)) || meter.Work().Candidates != d.cand {
					t.Fatalf("%s: within budget = %v, %v, work %+v; want the plain answer and %d candidates", d.name, got, err, meter.Work(), d.cand)
				}
				bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: d.cand - 1})
				got, err = call(bctx)
				var rb *storepkg.ReadBudgetError
				if !errors.As(err, &rb) || rb.Door != "ByTypeAndProperty" || rb.Limit != "candidates" || len(got) != 0 || meter.Work() != rb.Work {
					t.Fatalf("%s: over budget = %v, %v (meter %+v); want ErrReadBudget from ByTypeAndProperty at candidates and no rows", d.name, got, err, meter.Work())
				}
			}

			// A time coordinate: the plain answer without a budget, a typed
			// refusal (no rows, nothing counted) under one.
			for _, opts := range []storepkg.QueryOpts{{TxAt: tx}, {TxPin: tx}, {ValidAt: tx, ValidAtSet: true}} {
				want, err := g.Rels.ByTypeAndProperty("R", "k", int64(1), opts)
				if err != nil || len(want) != 2 {
					t.Fatalf("plain at %+v = %d rows, %v; want 2", opts, len(want), err)
				}
				if got, err := g.Rels.ByTypeAndPropertyCtx(ctx, "R", "k", int64(1), opts); err != nil || !slices.Equal(relIDsOf(got), relIDsOf(want)) {
					t.Fatalf("no budget at %+v = %d rows, %v; want the plain door's", opts, len(got), err)
				}
				bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1000})
				got, err := g.Rels.ByTypeAndPropertyCtx(bctx, "R", "k", int64(1), opts)
				berr := rbdBudgetErr(t, err, "unsupported")
				if len(got) != 0 || berr.Door != "ByTypeAndProperty" || meter.Work() != (storepkg.ReadWork{}) {
					t.Fatalf("at %+v under a budget = %d rows, %+v, work %+v; want a refusal before any work", opts, len(got), berr, meter.Work())
				}
			}

			cctx, cancel := context.WithCancel(ctx)
			cancel()
			bctx, meter := storepkg.WithReadBudget(cctx, storepkg.ReadBudget{Candidates: 100})
			if _, err := g.Rels.ByTypeAndPropertyCtx(bctx, "R", "k", int64(1), storepkg.QueryOpts{}); !errors.Is(err, context.Canceled) || meter.Work() != (storepkg.ReadWork{}) {
				t.Fatalf("cancelled context: %v, work %+v", err, meter.Work())
			}
		})
	}
}

// TestReadBudget_IDsByLabelAndPropertyValuesCtx: the ID-only lookup counts the
// index entries it examines (every posting of the looked-up values, before the
// label filter), refuses past a limit with no IDs, and without a budget (or
// within one) answers what IDsByLabelAndPropertyValues answers, paged the same.
// A key without an index, and a lookup at a time coordinate, read as
// ByLabelAndPropertyValuesCtx reads.
func TestReadBudget_IDsByLabelAndPropertyValuesCtx(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			if err := g.Index.CreateProperty("D", "k"); err != nil {
				t.Fatal(err)
			}
			const nodes = 30
			for i := range nodes {
				n, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{"k": int64(i % 15), "u": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"w": int64(1)}); err != nil {
					t.Fatal(err)
				}
			}
			tx, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			doors := []struct {
				name   string
				key    string
				values []any
				opts   storepkg.QueryOpts
				want   int
				cand   uint64
			}{
				{"one value", "k", []any{int64(1)}, storepkg.QueryOpts{}, 2, 2},
				{"two values", "k", []any{int64(1), int64(2)}, storepkg.QueryOpts{}, 4, 4},
				{"no index", "u", []any{int64(7)}, storepkg.QueryOpts{}, 1, nodes},
				{"TxAt", "k", []any{int64(1)}, storepkg.QueryOpts{TxAt: tx}, 2, 0}, // counted by the temporal read: not pinned here
			}
			for _, d := range doors {
				plain, err := g.Nodes.IDsByLabelAndPropertyValues("D", d.key, d.values, d.opts)
				if err != nil || len(plain) != d.want {
					t.Fatalf("%s: plain door = %d ids, %v; want %d", d.name, len(plain), err, d.want)
				}
				call := func(c context.Context) ([]types.NodeID, error) {
					return g.Nodes.IDsByLabelAndPropertyValuesCtx(c, "D", d.key, d.values, d.opts)
				}
				if got, err := call(ctx); err != nil || !slices.Equal(got, plain) {
					t.Fatalf("%s: no budget = %v, %v; want %v", d.name, got, err, plain)
				}
				if d.cand == 0 {
					continue
				}
				bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: d.cand})
				got, err := call(bctx)
				if err != nil || !slices.Equal(got, plain) || meter.Work().Candidates != d.cand {
					t.Fatalf("%s: within budget = %v, %v, work %+v; want %v and %d candidates", d.name, got, err, meter.Work(), plain, d.cand)
				}
				bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: d.cand - 1})
				got, err = call(bctx)
				var rb *storepkg.ReadBudgetError
				if !errors.As(err, &rb) || rb.Door != "IDsByLabelAndPropertyValues" || rb.Limit != "candidates" || got != nil || meter.Work() != rb.Work {
					t.Fatalf("%s: over budget = %v, %v (meter %+v); want ErrReadBudget from IDsByLabelAndPropertyValues and no ids", d.name, got, err, meter.Work())
				}
			}
			// Paged: the window of the answer, counted as the lookup reads it.
			page := storepkg.QueryOpts{Limit: 1}
			wantPage, err := g.Nodes.IDsByLabelAndPropertyValues("D", "k", []any{int64(1), int64(2)}, page)
			if err != nil || len(wantPage) != 1 {
				t.Fatalf("plain page = %v, %v", wantPage, err)
			}
			bctx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 100})
			if got, err := g.Nodes.IDsByLabelAndPropertyValuesCtx(bctx, "D", "k", []any{int64(1), int64(2)}, page); err != nil || !slices.Equal(got, wantPage) {
				t.Fatalf("budgeted page = %v, %v; want %v", got, err, wantPage)
			}

			cctx, cancel := context.WithCancel(ctx)
			cancel()
			bctx, meter := storepkg.WithReadBudget(cctx, storepkg.ReadBudget{Candidates: 100})
			if _, err := g.Nodes.IDsByLabelAndPropertyValuesCtx(bctx, "D", "k", []any{int64(1)}, storepkg.QueryOpts{}); !errors.Is(err, context.Canceled) || meter.Work() != (storepkg.ReadWork{}) {
				t.Fatalf("cancelled context: %v, work %+v", err, meter.Work())
			}
		})
	}
}

// TestReadBudget_DecodedBytes_RelByTypeAndProperty: the rows an indexed
// relationship lookup decodes count their stored bytes before the decode.
// Cold, the count covers every matched row's wire bytes and is the same on
// every cold run; within a budget of exactly that count the door answers; one
// byte under it returns ErrReadBudget "decoded bytes" and no rows. Warm, the
// entity cache serves the rows and the read counts 0.
func TestReadBudget_DecodedBytes_RelByTypeAndProperty(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			ctx := context.Background()
			if err := g.Index.CreateRelProperty("R", "k"); err != nil {
				t.Fatal(err)
			}
			s, e := txbEndpoints(t, g)
			const rows = 6
			var ids []types.RelID
			for i := range rows {
				r, err := g.Rels.AddByID(ctx, "R", s, e, map[string]any{"k": "x", "pad": fmt.Sprintf("%0*d", 100+i, i)})
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, r.ID())
			}
			shut()
			g, shut = be.open(t, dir)
			defer shut()
			rbdExact(t, "rel property", "ByTypeAndProperty", rows*100,
				func() { rbdEvict(t, g, nil, ids) },
				func(ctx context.Context) (bool, error) {
					got, err := g.Rels.ByTypeAndPropertyCtx(ctx, "R", "k", "x", storepkg.QueryOpts{})
					return len(got) == rows, err
				})
			// Warm: the entity cache serves the rows; nothing is decoded.
			if _, err := g.Rels.GetByIDs(ids); err != nil {
				t.Fatal(err)
			}
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{DecodedBytes: 1})
			got, err := g.Rels.ByTypeAndPropertyCtx(bctx, "R", "k", "x", storepkg.QueryOpts{})
			if err != nil || len(got) != rows || meter.Work().DecodedBytes != 0 {
				t.Fatalf("warm rel property = %d rows, %v, %+v; want the rows and 0 decoded bytes", len(got), err, meter.Work())
			}
		})
	}
}

// TestReadBudget_DiskLookupDoorsAdmitAsProduced (R2, the lookup doors): on a
// store that keeps its property index on disk, a budgeted lookup admits the
// candidates as the index produces them and refuses at the one that passes the
// limit, before it collects, sorts or reads the rest of the key: the ID-only
// node lookup (badger, and tiered over three event shards) and the
// relationship lookup (RelPropertyIndexOnDiskTypes). The gate is the count of
// on-disk index entries the process visited
// (badger.CountIndexEntriesVisitedForTest), never time or MemStats: unbudgeted
// the read visits at least every match; under a Candidates budget of 1 a
// handful per shard, whatever the key holds. The refusal is typed, no answer,
// the meter equal to the error's work; the answer without a budget is
// unchanged.
func TestReadBudget_DiskLookupDoorsAdmitAsProduced(t *testing.T) {
	const matches = 96
	type backend struct {
		name      string
		rotations int
		open      func(t testing.TB, dir string) (*Core, func())
	}
	openBadger := func(t testing.TB, dir string) (*Core, func()) {
		g, err := New(Config{BadgerDir: dir, PropertyIndexOnDisk: true, RelPropertyIndexOnDiskTypes: []string{rtbType}, UntimedValidStart: UntimedFromTxFrom})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return g, func() { _ = g.Close() }
	}
	openTiered := func(t testing.TB, dir string) (*Core, func()) {
		ts, err := tiered.New(tiered.Config{
			DataDir:                     dir,
			RefLabels:                   []string{"Ref"},
			ShardWindow:                 7 * 24 * time.Hour,
			FlushInterval:               1<<63 - 1,
			MaxOpenColdShards:           8,
			PropertyIndexOnDisk:         true,
			RelPropertyIndexOnDiskTypes: []string{rtbType},
		})
		if err != nil {
			t.Fatalf("tiered.New: %v", err)
		}
		g, err := New(Config{Store: ts, UntimedValidStart: UntimedFromTxFrom})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return g, func() { _ = g.Close(); _ = ts.Close() }
	}
	for _, be := range []backend{{"badger", 1, openBadger}, {"tiered", 3, openTiered}} {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			ctx := context.Background()
			if err := g.Index.CreateProperty("D", "k"); err != nil {
				t.Fatal(err)
			}
			w := newRTBWorld(t, g)
			w.index(t, rtbType, rtbKeys...)
			for r := range be.rotations {
				for i := range matches / be.rotations {
					if _, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{"k": int64(7), "pad": fmt.Sprintf("%0*d", 60, i)}); err != nil {
						t.Fatal(err)
					}
					w.add(t, fmt.Sprintf("r%d-%d", r, i), rtbType, map[string]any{"estate": "e1", "device": "d1"})
				}
				if ts, ok := g.store.(*tiered.Store); ok && r < be.rotations-1 {
					time.Sleep(2 * time.Millisecond)
					if err := ts.RotateHotShard(); err != nil {
						t.Fatal(err)
					}
				}
			}
			shut()
			g, shut = be.open(t, dir) // every entry is on disk, the write buffer empty
			defer shut()

			doors := []struct {
				name string
				call func(ctx context.Context) (int, error)
			}{
				{"IDs current", func(ctx context.Context) (int, error) {
					r, err := g.Nodes.IDsByLabelAndPropertyValuesCtx(ctx, "D", "k", []any{int64(7)}, storepkg.QueryOpts{})
					return len(r), err
				}},
				{"rel property current", func(ctx context.Context) (int, error) {
					r, err := g.Rels.ByTypeAndPropertyCtx(ctx, rtbType, "device", "d1", storepkg.QueryOpts{})
					return len(r), err
				}},
			}
			visited, stop := badger.CountIndexEntriesVisitedForTest()
			defer stop()
			for _, d := range doors {
				t.Run(d.name, func(t *testing.T) {
					plain, err := d.call(ctx)
					if err != nil || plain != matches {
						t.Fatalf("no budget = %d, %v; want %d", plain, err, matches)
					}
					before := visited()
					fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
					if n, err := d.call(fctx); err != nil || n != plain {
						t.Fatalf("unlimited = %d, %v; want %d", n, err, plain)
					}
					all := visited() - before
					if all < matches {
						t.Fatalf("an unbudgeted read visited %d index entries over %d matches: the counter does not see this walk", all, matches)
					}
					if full.Work().Candidates < matches {
						t.Fatalf("unlimited counted %d candidates, want at least %d", full.Work().Candidates, matches)
					}

					before = visited()
					bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1})
					n, err := d.call(bctx)
					steps := visited() - before
					var rb *storepkg.ReadBudgetError
					if !errors.As(err, &rb) || rb.Limit != "candidates" || n != 0 || rb.Work.Candidates < 2 || rb.Work.Candidates > meter.Work().Candidates {
						t.Fatalf("budget 1 = %d rows, %v (meter %+v); want ErrReadBudget at candidates, no rows, work past the limit (parallel shards may add after the first refusal)", n, err, meter.Work())
					}
					t.Logf("budget 1 visited %d index entries (an unbudgeted read %d, %d matches)", steps, all, matches)
					if limit := int64(6 * be.rotations); steps > limit {
						t.Fatalf("budget 1 visited %d index entries (an unbudgeted read %d, %d matches); want at most %d: the read built the whole candidate list before it admitted one", steps, all, matches, limit)
					}
				})
			}
		})
	}
}
