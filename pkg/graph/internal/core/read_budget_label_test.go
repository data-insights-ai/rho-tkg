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
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RH list R2, Sigma doors group D, door 2: Nodes().ByLabelCtx, the current
// members of a label under a read budget. Each member of the label counts as a
// candidate before its row is read, counted where the store lists it (a label
// index in RAM counts its whole set before the list is built, one kept on disk
// each entry as the walk produces it), then the stored bytes of each row
// decoded and, on tiered, each closed shard opened, counted before the open; a
// budget of 1 over many members stops at the listing, not after it. Past a
// limit *store.ReadBudgetError and no rows; without a budget exactly ByLabel.
// A read at a time coordinate is the label's history; since R2-SCANS it is
// counted too (it was refused under a budget, Limit "unsupported").

// TestReadBudget_ByLabelCtx: on every backend (memory, badger, sharded,
// tiered) the door answers without a budget as ByLabel does, paged the same;
// counts exactly the label's live members under one (not the other label's, not
// the deleted nodes with history); is exact (a limit at the count answers, one
// under refuses with no rows and the meter equal to the error's work); refuses a
// budget of 1 with no rows; answers a time coordinate with and without a budget
// and refuses it under a limit below its count; checks ctx first.
func TestReadBudget_ByLabelCtx(t *testing.T) {
	t.Parallel()
	const live, deleted, other = 30, 5, 10
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			var ids []types.NodeID
			for i := range live + deleted {
				n, err := g.Nodes.Add(ctx, []string{"L"}, map[string]any{"i": int64(i), "pad": fmt.Sprintf("%0*d", 40, i)})
				if err != nil {
					t.Fatal(err)
				}
				if i%2 == 0 { // history
					if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"v": int64(1)}); err != nil {
						t.Fatal(err)
					}
				}
				ids = append(ids, n.ID())
			}
			for range other {
				if _, err := g.Nodes.Add(ctx, []string{"M"}, nil); err != nil {
					t.Fatal(err)
				}
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range ids[live:] { // live nowhere now, members at the pin
				if err := g.Nodes.Delete(ctx, id); err != nil {
					t.Fatal(err)
				}
			}

			plain, err := g.Nodes.ByLabel("L", storepkg.QueryOpts{})
			if err != nil || len(plain) != live {
				t.Fatalf("plain door = %d nodes, %v; want %d", len(plain), err, live)
			}
			want := fmt.Sprint(nodeIDsOf(plain))
			call := func(c context.Context, opts storepkg.QueryOpts) ([]*types.Node, error) {
				return g.Nodes.ByLabelCtx(c, "L", opts)
			}
			if got, err := call(ctx, storepkg.QueryOpts{}); err != nil || fmt.Sprint(nodeIDsOf(got)) != want {
				t.Fatalf("no budget = %v, %v; want the plain door's %s", nodeIDsOf(got), err, want)
			}

			uctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			got, err := call(uctx, storepkg.QueryOpts{})
			total := full.Work()
			if err != nil || fmt.Sprint(nodeIDsOf(got)) != want || total.Candidates != live {
				t.Fatalf("unlimited = %v, %v, work %+v; want the plain answer and exactly the %d live members counted", nodeIDsOf(got), err, total, live)
			}

			// Exact: the count answers, one under it refuses with no rows.
			ectx, exact := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: live})
			if got, err := call(ectx, storepkg.QueryOpts{}); err != nil || fmt.Sprint(nodeIDsOf(got)) != want || exact.Work().Candidates != live {
				t.Fatalf("candidates %d (its own count) = %v, %v, %+v", live, nodeIDsOf(got), err, exact.Work())
			}
			for _, limit := range []uint64{live - 1, 1} {
				bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: limit})
				got, err := call(bctx, storepkg.QueryOpts{})
				rb := rbdBudgetErr(t, err, "candidates")
				if got != nil || meter.Work() != rb.Work || rb.Door != "ByLabel" {
					t.Fatalf("candidates %d = %d nodes, %+v, meter %+v; want no rows from ByLabel and the meter's work", limit, len(got), rb, meter.Work())
				}
			}

			// Paged: the same window as the plain door; the listing is what counts.
			page := storepkg.QueryOpts{Limit: 7, After: types.EntityID(plain[3].ID())}
			wantPage, err := g.Nodes.ByLabel("L", page)
			if err != nil || len(wantPage) != 7 {
				t.Fatalf("plain page = %d, %v", len(wantPage), err)
			}
			pctx, pmeter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			if got, err := call(pctx, page); err != nil || fmt.Sprint(nodeIDsOf(got)) != fmt.Sprint(nodeIDsOf(wantPage)) || pmeter.Work().Candidates != live {
				t.Fatalf("budgeted page = %v, %v, %+v; want %v and %d candidates", nodeIDsOf(got), err, pmeter.Work(), nodeIDsOf(wantPage), live)
			}

			// A time coordinate: the label's history is the plain door's answer
			// without a budget and under one, counted (R2-SCANS; it was refused
			// "unsupported" before any work): the members deleted since are the
			// history-only candidates, counted besides the live ones the store
			// lists, and a limit under the count refuses with no rows.
			// TestR2Scans_ByLabelCtxAtATime holds the counts on the disk backends.
			for _, opts := range []storepkg.QueryOpts{{TxPin: pin}, {TxAt: pin}, {ValidAt: pin, ValidAtSet: true}} {
				wantAt, err := g.Nodes.ByLabel("L", opts)
				if err != nil || len(wantAt) != live+deleted {
					t.Fatalf("plain at %+v = %d nodes, %v; want the %d members at the pin", opts, len(wantAt), err, live+deleted)
				}
				if got, err := call(ctx, opts); err != nil || fmt.Sprint(nodeIDsOf(got)) != fmt.Sprint(nodeIDsOf(wantAt)) {
					t.Fatalf("no budget at %+v = %v, %v; want the plain door's", opts, nodeIDsOf(got), err)
				}
				bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
				if got, err := call(bctx, opts); err != nil || fmt.Sprint(nodeIDsOf(got)) != fmt.Sprint(nodeIDsOf(wantAt)) || meter.Work().Candidates != live+deleted {
					t.Fatalf("at %+v under an unlimited budget = %v, %v, work %+v; want the plain door's answer and %d candidates", opts, nodeIDsOf(got), err, meter.Work(), live+deleted)
				}
				bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: live + deleted - 1})
				got, err := call(bctx, opts)
				rb := rbdBudgetErr(t, err, "candidates")
				if got != nil || rb.Door != "ByLabel" || meter.Work().Candidates != live+deleted {
					t.Fatalf("at %+v one under the count = %d nodes, %+v, work %+v; want no rows and a refusal at candidate %d", opts, len(got), rb, meter.Work(), live+deleted)
				}
			}

			// An unknown label has no members and counts nothing.
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1})
			if got, err := g.Nodes.ByLabelCtx(bctx, "NoSuchLabel", storepkg.QueryOpts{}); err != nil || got != nil || meter.Work() != (storepkg.ReadWork{}) {
				t.Fatalf("unknown label = %v, %v, work %+v", got, err, meter.Work())
			}

			cctx, cancel := context.WithCancel(ctx)
			cancel()
			bctx, meter = storepkg.WithReadBudget(cctx, storepkg.ReadBudget{Candidates: 100})
			if _, err := call(bctx, storepkg.QueryOpts{}); !errors.Is(err, context.Canceled) || meter.Work() != (storepkg.ReadWork{}) {
				t.Fatalf("cancelled context: %v, work %+v", err, meter.Work())
			}
		})
	}
}

// TestReadBudget_ByLabelCtxStopsAtTheListing: over a label of 8, 32 and 128
// members (and 2,100, where badger decodes its rows across cores) on badger,
// sharded and tiered, with the rows pending (as written) and flushed (after a
// reopen, the cache cold), a budget of 1 candidate refuses at the listing: no
// row is decoded by the store's own counter, however many members; a budget of
// 1 decoded byte stops at the first row's decode (within one row's bytes of
// the unlimited count over the members); a decoded-bytes limit at the cold
// count answers and one byte under refuses (rbdExact).
func TestReadBudget_ByLabelCtxStopsAtTheListing(t *testing.T) {
	t.Parallel()
	sizes := []int{8, 32, 128, 2100}
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			sizes := sizes
			if be.name != "badger" {
				sizes = sizes[:3] // the parallel decode of 2,048 or more rows is badger's own
			}
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			ctx := context.Background()
			groups := map[int][]types.NodeID{}
			for _, size := range sizes {
				for i := range size {
					n, err := g.Nodes.Add(ctx, []string{fmt.Sprintf("G%d", size)}, map[string]any{"pad": fmt.Sprintf("%0*d", 100, i)})
					if err != nil {
						t.Fatal(err)
					}
					groups[size] = append(groups[size], n.ID())
				}
			}
			probe := func(t *testing.T, g *Core, flushed bool) {
				for _, size := range sizes {
					label := fmt.Sprintf("G%d", size)
					call := func(c context.Context) ([]*types.Node, error) {
						return g.Nodes.ByLabelCtx(c, label, storepkg.QueryOpts{})
					}
					cold := func() {
						if flushed {
							rbdEvict(t, g, groups[size], nil)
						}
					}
					what := fmt.Sprintf("%s over %d members", label, size)
					plain, err := g.Nodes.ByLabel(label, storepkg.QueryOpts{})
					if err != nil || len(plain) != size {
						t.Fatalf("%s: plain door = %d, %v", what, len(plain), err)
					}
					want := fmt.Sprint(nodeIDsOf(plain))
					cold()
					fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
					got, err := call(fctx)
					total := full.Work()
					if err != nil || fmt.Sprint(nodeIDsOf(got)) != want || total.Candidates != uint64(size) {
						t.Fatalf("%s: unlimited = %d nodes, %v, work %+v; want the plain answer and %d candidates", what, len(got), err, total, size)
					}
					if flushed && total.DecodedBytes < uint64(size)*100 {
						t.Fatalf("%s: unlimited counted %d decoded bytes over cold rows, want at least %d", what, total.DecodedBytes, size*100)
					}

					// A budget of 1 candidate refuses at the listing: no row decoded.
					cold()
					var before int64
					bs, _ := g.store.(*badger.Store)
					if bs != nil {
						before = bs.NodeRowDecodesForTest()
					}
					cctx, cand := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1})
					got, err = call(cctx)
					rb := rbdBudgetErr(t, err, "candidates")
					if got != nil || cand.Work() != rb.Work || rb.Work.DecodedBytes != 0 {
						t.Fatalf("%s: candidates 1 = %d nodes, %+v, meter %+v; want no rows and nothing decoded", what, len(got), rb, cand.Work())
					}
					if bs != nil {
						if decoded := bs.NodeRowDecodesForTest() - before; decoded != 0 {
							t.Fatalf("%s: candidates 1 decoded %d rows, want 0: the read listed and read the label before it refused", what, decoded)
						}
					}
					t.Logf("%s: unlimited %+v; candidates 1 refused after %+v", what, total, rb.Work)

					// A budget of 1 decoded byte stops at the first decode.
					if flushed {
						cold()
						dctx, dec := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{DecodedBytes: 1})
						got, err = call(dctx)
						rb = rbdBudgetErr(t, err, "decoded bytes")
						if got != nil || dec.Work() != rb.Work || rb.Work.DecodedBytes > 3*total.DecodedBytes/uint64(size) {
							t.Fatalf("%s: decoded bytes 1 = %d nodes, %+v (unlimited %+v); want no rows and about one row's bytes", what, len(got), rb, total)
						}
						t.Logf("%s: decoded bytes 1 refused after %+v", what, rb.Work)
						rbdExact(t, what, "ByLabel", uint64(size)*100, cold, func(ctx context.Context) (bool, error) {
							r, err := call(ctx)
							return err == nil && fmt.Sprint(nodeIDsOf(r)) == want, err
						})
					}
				}
			}
			t.Run("pending", func(t *testing.T) { probe(t, g, false) })
			shut()
			g, shut = be.open(t, dir)
			defer shut()
			t.Run("flushed", func(t *testing.T) { probe(t, g, true) })
		})
	}
}

// TestReadBudget_ByLabelCtxDiskIndexAdmitsAsProduced: on a store that keeps
// its label membership on disk (LabelIndexOnDisk: badger, and tiered whose
// every shard does) a budgeted read admits the members as the label walk
// produces them and refuses at the one that passes the limit, before it
// collects, sorts or reads the rest of the label. The gate is the count of
// on-disk index entries the process visited
// (badger.CountIndexEntriesVisitedForTest), never time: unbudgeted the read
// visits every member; under a Candidates budget of 1 a handful per shard,
// whatever the label holds. Flushed (every entry on disk) and pending (the
// members still in the write buffer) both refuse typed, with no rows and the
// meter equal to the error's work.
func TestReadBudget_ByLabelCtxDiskIndexAdmitsAsProduced(t *testing.T) {
	const members = 96
	type backend struct {
		name      string
		rotations int
		open      func(t testing.TB, dir string) (*Core, func())
	}
	backends := []backend{
		{"badger", 1, func(t testing.TB, dir string) (*Core, func()) {
			g, err := New(Config{BadgerDir: dir, LabelIndexOnDisk: true, UntimedValidStart: UntimedFromTxFrom})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			return g, func() { _ = g.Close() }
		}},
		{"tiered", 3, func(t testing.TB, dir string) (*Core, func()) {
			ts, err := tiered.New(tiered.Config{
				DataDir:           dir,
				RefLabels:         []string{"Ref"},
				ShardWindow:       7 * 24 * time.Hour,
				FlushInterval:     1<<63 - 1,
				MaxOpenColdShards: 8,
				LabelIndexOnDisk:  true,
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
			ctx := context.Background()
			for r := range be.rotations {
				for i := range members / be.rotations {
					if _, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{"pad": fmt.Sprintf("%0*d", 60, i)}); err != nil {
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
			visited, stop := badger.CountIndexEntriesVisitedForTest()
			defer stop()
			check := func(t *testing.T, g *Core, flushed bool) {
				call := func(c context.Context) (int, error) {
					r, err := g.Nodes.ByLabelCtx(c, "D", storepkg.QueryOpts{})
					return len(r), err
				}
				before := visited()
				plain, err := call(ctx)
				if err != nil || plain != members {
					t.Fatalf("no budget = %d, %v; want %d", plain, err, members)
				}
				all := visited() - before
				if flushed && all < members {
					t.Fatalf("an unbudgeted read visited %d index entries over %d members: the counter does not see this walk", all, members)
				}
				fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
				if n, err := call(fctx); err != nil || n != members || full.Work().Candidates != members {
					t.Fatalf("unlimited = %d, %v, work %+v; want %d rows and %d candidates", n, err, full.Work(), members, members)
				}
				before = visited()
				bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1})
				n, err := call(bctx)
				steps := visited() - before
				var rb *storepkg.ReadBudgetError
				if !errors.As(err, &rb) || rb.Limit != "candidates" || n != 0 || rb.Work.Candidates < 2 || rb.Work.Candidates > meter.Work().Candidates {
					t.Fatalf("budget 1 = %d rows, %v (meter %+v); want ErrReadBudget at candidates, no rows, work past the limit (parallel shards may add after the first refusal)", n, err, meter.Work())
				}
				t.Logf("budget 1 visited %d index entries (an unbudgeted read %d, %d members), counted %+v", steps, all, members, rb.Work)
				if flushed {
					if limit := int64(6 * be.rotations); steps > limit {
						t.Fatalf("budget 1 visited %d index entries (an unbudgeted read %d, %d members); want at most %d: the read listed the whole label before it admitted one", steps, all, members, limit)
					}
				}
			}
			t.Run("pending", func(t *testing.T) { check(t, g, false) })
			shut()
			g, shut = be.open(t, dir) // every entry is on disk, the write buffer empty
			defer shut()
			t.Run("flushed", func(t *testing.T) { check(t, g, true) })
		})
	}
}
