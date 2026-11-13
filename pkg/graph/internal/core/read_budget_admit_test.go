package core

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// admitBackends are the stores that keep a property index on disk: badger with
// PropertyIndexOnDisk, and tiered, whose every shard does. rotations splits
// the writes over that many event shards (tiered), so a lookup fans out.
func admitBackends() []struct {
	name      string
	rotations int
	open      func(t testing.TB, dir string) (*Core, func())
} {
	return []struct {
		name      string
		rotations int
		open      func(t testing.TB, dir string) (*Core, func())
	}{
		{"badger", 1, func(t testing.TB, dir string) (*Core, func()) {
			g, err := New(Config{BadgerDir: dir, PropertyIndexOnDisk: true, UntimedValidStart: UntimedFromTxFrom})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			return g, func() { _ = g.Close() }
		}},
		{"tiered", 3, func(t testing.TB, dir string) (*Core, func()) {
			ts, err := tiered.New(tiered.Config{
				DataDir:             dir,
				RefLabels:           []string{"Ref"},
				ShardWindow:         7 * 24 * time.Hour,
				FlushInterval:       1<<63 - 1,
				MaxOpenColdShards:   8,
				PropertyIndexOnDisk: true,
			})
			if err != nil {
				t.Fatalf("tiered.New: %v", err)
			}
			g, err := New(Config{Store: ts, UntimedValidStart: UntimedFromTxFrom})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			return g, func() { _ = g.Close(); _ = ts.Close() }
		}},
	}
}

// TestReadBudget_DiskReadsAdmitAsProduced (R2 gaps A and B, 13 Nov): on a
// store that keeps its property index and its member entries on disk, a
// budgeted node property read admits the candidates as the index produces
// them and refuses at the one that passes the limit, before it reads, folds
// or sorts the rest of the key: the current lookup, the exact historical
// membership (TxPin) and the window membership (ValidAt), on badger and on
// tiered (three event shards). The gate is the count of on-disk index entries
// the process visited (badger.CountIndexEntriesVisitedForTest), never time or
// MemStats: unbudgeted (and under a budget wide enough) the read visits at
// least every match; under a Candidates budget of 1 it visits a handful per
// shard, whatever the key holds. The refusal is the typed ReadBudgetError, no
// rows, the meter equal to the error's work; the answers without a budget are
// unchanged.
func TestReadBudget_DiskReadsAdmitAsProduced(t *testing.T) {
	const matches = 96
	for _, be := range admitBackends() {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			ctx := context.Background()
			if err := g.Index.CreateProperty("D", "k"); err != nil {
				t.Fatal(err)
			}
			for r := range be.rotations {
				for i := range matches / be.rotations {
					n, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{"k": int64(7), "pad": fmt.Sprintf("%0*d", 60, i)})
					if err != nil {
						t.Fatal(err)
					}
					if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"v": int64(r)}); err != nil {
						t.Fatal(err)
					}
				}
				if ts, ok := g.store.(*tiered.Store); ok && r < be.rotations-1 {
					time.Sleep(2 * time.Millisecond)
					if err := ts.RotateHotShard(); err != nil {
						t.Fatal(err)
					}
				}
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			shut()
			g, shut = be.open(t, dir) // every entry is on disk, the write buffer empty
			defer shut()

			doors := []struct {
				name string
				call func(ctx context.Context) (int, error)
			}{
				{"current", func(ctx context.Context) (int, error) {
					r, err := g.Nodes.ByLabelAndPropertyCtx(ctx, "D", "k", int64(7), storepkg.QueryOpts{})
					return len(r), err
				}},
				{"TxPin membership", func(ctx context.Context) (int, error) {
					r, err := g.Nodes.ByLabelAndPropertyCtx(ctx, "D", "k", int64(7), storepkg.QueryOpts{TxPin: pin})
					return len(r), err
				}},
				{"ValidAt membership", func(ctx context.Context) (int, error) {
					r, err := g.Nodes.ByLabelAndPropertyCtx(ctx, "D", "k", int64(7), storepkg.QueryOpts{ValidAt: pin, ValidAtSet: true})
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

// TestReadBudget_DiskRelMembershipAdmitsAsProduced is the relationship twin
// (R2 gap B): Temporal().RelEffectiveTimelinesByProperties over a type kept on
// disk (RelPropertyIndexOnDiskTypes), 96 rows carrying the probed tuple, all
// meeting the window. Exact (a transaction pin only) and window (RELDISKWIN's
// time-ordered seek) reads under a Candidates budget of 1 refuse at the member
// that passes it, with the typed error and no rows, having visited a handful of
// member entries per shard, not the whole value; without a budget, and under a
// budget as wide as the unlimited count, the answer is the unbudgeted one; one
// less than that count refuses.
func TestReadBudget_DiskRelMembershipAdmitsAsProduced(t *testing.T) {
	const rows = 96
	backends := []struct {
		name string
		open func(t testing.TB, dir string) (*Core, func())
	}{
		{"badger", func(t testing.TB, dir string) (*Core, func()) {
			g, err := New(Config{BadgerDir: dir, RelPropertyIndexOnDiskTypes: []string{rtbType}, UntimedValidStart: UntimedFromTxFrom})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			return g, func() { _ = g.Close() }
		}},
		{"tiered", func(t testing.TB, dir string) (*Core, func()) {
			ts, err := tiered.New(tiered.Config{
				DataDir:                     dir,
				RefLabels:                   []string{"Ref"},
				ShardWindow:                 7 * 24 * time.Hour,
				FlushInterval:               1<<63 - 1,
				MaxOpenColdShards:           8,
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
		}},
	}
	for _, be := range backends {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			w := newRTBWorld(t, g)
			w.index(t, rtbType, rtbKeys...)
			for i := range rows {
				w.add(t, fmt.Sprintf("r%03d", i), rtbType, map[string]any{"estate": "e1", "device": "d1", "tkg_valid_from": types.Instant(1000), "tkg_valid_to": types.Instant(9000)})
			}
			txPin := w.pin(t)
			shut()
			g, shut = be.open(t, dir)
			defer shut()
			w.g = g
			tuples := [][]any{{"e1", "d1"}}
			ctx := context.Background()
			visited, stop := badger.CountIndexEntriesVisitedForTest()
			defer stop()
			for _, d := range []struct {
				name string
				pin  temporal.Pin
			}{
				{"exact", temporal.Pin{Tx: txPin}},
				{"window", temporal.Pin{Tx: txPin, ValidStart: 2000, ValidEnd: 3000}},
			} {
				t.Run(d.name, func(t *testing.T) {
					call := func(ctx context.Context) (int, error) {
						r, err := g.Temporal.RelEffectiveTimelinesByProperties(ctx, rtbType, rtbKeys, tuples, d.pin)
						n := 0
						for _, segs := range r {
							n += len(segs)
						}
						return n, err
					}
					before := visited()
					plain, err := call(ctx)
					if err != nil || plain < rows {
						t.Fatalf("no budget = %d segments, %v; want at least %d", plain, err, rows)
					}
					all := visited() - before
					if all < rows {
						t.Fatalf("an unbudgeted read visited %d member entries over %d rows: the counter does not see this walk", all, rows)
					}
					fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
					if n, err := call(fctx); err != nil || n != plain {
						t.Fatalf("unlimited = %d, %v; want %d", n, err, plain)
					}
					count := full.Work().Candidates
					if count < rows {
						t.Fatalf("unlimited counted %d candidates, want at least %d", count, rows)
					}
					// Exactly as wide as the unlimited count answers; one less refuses.
					wctx, wide := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: count})
					if n, err := call(wctx); err != nil || n != plain || wide.Work().Candidates != count {
						t.Fatalf("budget %d = %d, %v (work %+v); want the unbudgeted %d", count, n, err, wide.Work(), plain)
					}
					nctx, narrow := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: count - 1})
					if n, err := call(nctx); !errors.Is(err, storepkg.ErrReadBudget) || n != 0 {
						t.Fatalf("budget %d = %d, %v (work %+v); want ErrReadBudget", count-1, n, err, narrow.Work())
					}

					before = visited()
					bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1})
					n, err := call(bctx)
					steps := visited() - before
					var rb *storepkg.ReadBudgetError
					if !errors.As(err, &rb) || rb.Limit != "candidates" || n != 0 || rb.Work.Candidates < 2 || rb.Work.Candidates > meter.Work().Candidates {
						t.Fatalf("budget 1 = %d segments, %v (meter %+v); want ErrReadBudget at candidates, no rows, work past the limit (parallel shards may add after the first refusal)", n, err, meter.Work())
					}
					t.Logf("budget 1 visited %d member entries (an unbudgeted read %d, %d rows)", steps, all, rows)
					if steps > 12 {
						t.Fatalf("budget 1 visited %d member entries (an unbudgeted read %d, %d rows); want at most 12: the walk folded the whole value before it admitted one", steps, all, rows)
					}
				})
			}
		})
	}
}
