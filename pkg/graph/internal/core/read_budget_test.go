package core

import (
	"context"
	"errors"
	"fmt"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudget_SelectiveDoorRefusesBroadRead (RH list R2, ai-soc's
// native_test case): 40 rows carry the probed value, 2 of them meet the
// window. The RAM sidecar and the on-disk index (RelPropertyIndexOnDiskTypes,
// whose member entries carry each row's valid-time envelope since RELDISKWIN)
// both narrow the candidates to the window: 2 for 40, so a budget of 10
// candidates answers in RAM. The on-disk window walk is ordered by end and
// visits every row ending after the window's start (40 here), each admitted
// under the budget (R2, the window walk's rejects): there a budget of 10 refuses
// and one of 40 answers, counting the 2 candidates. Under a read budget the door counts its
// candidates and output segments; past the candidate limit (here 1) it returns
// ErrReadBudget with the counters and no rows, before it decodes a timeline.
// Without a budget every answer is unchanged.
func TestReadBudget_SelectiveDoorRefusesBroadRead(t *testing.T) {
	t.Parallel()
	const rows, limit = 40, 10
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk=%t", disk), func(t *testing.T) {
			cfg := Config{BadgerDir: t.TempDir(), UntimedValidStart: UntimedFromTxFrom}
			if disk {
				cfg.RelPropertyIndexOnDiskTypes = []string{rtbType}
			}
			g, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			w := newRTBWorld(t, g)
			w.index(t, rtbType, rtbKeys...)
			for i := range rows {
				from := types.Instant(1000 + i*1000)
				w.add(t, fmt.Sprintf("r%02d", i), rtbType, map[string]any{
					"estate": "e1", "device": "d1", "tkg_valid_from": from, "tkg_valid_to": from + 10,
				})
			}
			pin := temporal.Pin{Tx: w.pin(t), ValidStart: 1000, ValidEnd: 2010} // r00 and r01
			tuples := [][]any{{"e1", "d1"}}
			want := rtbNames(w, rtbCheck(t, w, "no budget", rtbKeys, tuples, pin)[0])
			if want != "r00,r01" {
				t.Fatalf("no budget = %q, want r00,r01", want)
			}

			// Under the limit: the same answer, counted, on disk as in RAM. On disk
			// the walk visits every row of the value, so the limit must cover them.
			lim := uint64(limit)
			if disk {
				ctx, _ := storepkg.WithReadBudget(w.ctx, storepkg.ReadBudget{Candidates: limit})
				if _, err := g.Temporal.RelEffectiveTimelinesByProperties(ctx, rtbType, rtbKeys, tuples, pin); !errors.Is(err, storepkg.ErrReadBudget) {
					t.Fatalf("disk, budget %d over a walk of %d entries = %v; want ErrReadBudget", limit, rows, err)
				}
				lim = rows
			}
			ctx, meter := storepkg.WithReadBudget(w.ctx, storepkg.ReadBudget{Candidates: lim})
			got, err := g.Temporal.RelEffectiveTimelinesByProperties(ctx, rtbType, rtbKeys, tuples, pin)
			if err != nil || rtbNames(w, got[0]) != want {
				t.Fatalf("under budget %d = %s, %v; want %s", lim, rtbRender(w, got), err, want)
			}
			if work := meter.Work(); work.Candidates != 2 || work.Segments != 2 {
				t.Fatalf("work = %+v, want 2 candidates, 2 segments", work)
			}

			// Past the limit: refused, counted, nothing read.
			ctx, meter = storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: 1})
			got, err = g.Temporal.RelEffectiveTimelinesByProperties(ctx, rtbType, rtbKeys, tuples, pin)
			var be *storepkg.ReadBudgetError
			if !errors.Is(err, storepkg.ErrReadBudget) || !errors.As(err, &be) || got != nil {
				t.Fatalf("budget 1 = %s, %v; want ErrReadBudget", rtbRender(w, got), err)
			}
			if be.Door != "RelEffectiveTimelinesByProperties" || be.Limit != "candidates" || be.Work.Candidates != 2 || be.Work.Segments != 0 || meter.Work() != be.Work {
				t.Fatalf("budget error %+v (meter %+v), want the candidates limit with 0 segments", be, meter.Work())
			}
		})
	}
}

// TestReadBudget_LendByIDsCountsCandidates: LendByIDs under a read budget
// counts one candidate per ID before it reads any row and refuses past the
// limit without calling fn; within it, it lends as before and counts.
func TestReadBudget_LendByIDsCountsCandidates(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			var ids []types.NodeID
			for range 3 {
				n, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, n.ID())
			}
			called := 0
			fn := func(int, *types.Node) bool { called++; return true }
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 2})
			err := g.Nodes.LendByIDs(bctx, ids, fn)
			var rb *storepkg.ReadBudgetError
			if !errors.As(err, &rb) || rb.Door != "LendByIDs" || called != 0 || meter.Work().Candidates != 3 {
				t.Fatalf("over budget: %v, %d calls, work %+v", err, called, meter.Work())
			}
			bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 3})
			if err := g.Nodes.LendByIDs(bctx, ids, fn); err != nil || called != 3 || meter.Work().Candidates != 3 {
				t.Fatalf("within budget: %v, %d calls, work %+v", err, called, meter.Work())
			}
		})
	}
}

// TestReadBudget_NodePropertyDoors: the Ctx forms of ByLabelAndProperty,
// ByLabelAndPropertyValues and ByLabelAndProperties count the entities they
// examine. An indexed lookup counts its matches; a key without an index falls
// back to the label's every node, and under a budget that fallback is counted
// (and refused) before the scan reads a row; so is a temporal read's fold
// over every history ID. Without a budget the answers are unchanged.
func TestReadBudget_NodePropertyDoors(t *testing.T) {
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
				n, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{"k": int64(i % 15), "u": int64(i), "v": int64(i % 3)})
				if err != nil {
					t.Fatal(err)
				}
				// A second version: every node has history, so a temporal read
				// without an index folds over every history ID of the label.
				if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"w": int64(1)}); err != nil {
					t.Fatal(err)
				}
			}
			tx, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			type door struct {
				name string
				call func(context.Context) (int, error)
				want int // matches
				cand uint64
			}
			doors := []door{
				{"ByLabelAndProperty", func(c context.Context) (int, error) {
					r, err := g.Nodes.ByLabelAndPropertyCtx(c, "D", "k", int64(1), storepkg.QueryOpts{})
					return len(r), err
				}, 2, 2},
				{"ByLabelAndPropertyValues", func(c context.Context) (int, error) {
					r, err := g.Nodes.ByLabelAndPropertyValuesCtx(c, "D", "k", []any{int64(1), int64(2)}, storepkg.QueryOpts{})
					return len(r), err
				}, 4, 4},
				{"ByLabelAndProperty", func(c context.Context) (int, error) { // no index on u: the label scan
					r, err := g.Nodes.ByLabelAndPropertyCtx(c, "D", "u", int64(7), storepkg.QueryOpts{})
					return len(r), err
				}, 1, nodes},
				{"ByLabelAndProperties", func(c context.Context) (int, error) { // no composite index: the label scan
					r, err := g.Nodes.ByLabelAndPropertiesCtx(c, "D", map[string]any{"u": int64(7), "v": int64(1)}, storepkg.QueryOpts{})
					return len(r), err
				}, 1, nodes},
				// Temporal, no index on u: the current lookup scans the label (30),
				// then the fold resolves every history ID but the current match (29).
				{"ByLabelAndProperty", func(c context.Context) (int, error) {
					r, err := g.Nodes.ByLabelAndPropertyCtx(c, "D", "u", int64(7), storepkg.QueryOpts{TxAt: tx})
					return len(r), err
				}, 1, 2*nodes - 1},
			}
			for i, d := range doors {
				if n, err := d.call(ctx); err != nil || n != d.want {
					t.Fatalf("door %d %s without budget = %d, %v; want %d", i, d.name, n, err, d.want)
				}
				bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: d.cand})
				if n, err := d.call(bctx); err != nil || n != d.want || meter.Work().Candidates != d.cand {
					t.Fatalf("door %d %s within budget = %d, %v, work %+v; want %d, %d candidates", i, d.name, n, err, meter.Work(), d.want, d.cand)
				}
				bctx, _ = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: d.cand - 1})
				n, err := d.call(bctx)
				var rb *storepkg.ReadBudgetError
				if !errors.As(err, &rb) || rb.Door != d.name || n != 0 {
					t.Fatalf("door %d %s over budget = %d, %v; want ErrReadBudget from %s", i, d.name, n, err, d.name)
				}
			}
		})
	}
}

// TestReadBudget_ByIDDoors: GetByIDsCtx, HasHistoryCtx and LatestStampsCtx
// count one candidate per ID before reading, and Rels LendByIDs as the node
// door; the meter is cumulative, so a per-ID probe repeated past the budget
// is refused.
func TestReadBudget_ByIDDoors(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			var ids []types.NodeID
			var ns []*types.Node
			for range 3 {
				n, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				ids, ns = append(ids, n.ID()), append(ns, n)
			}
			r, err := g.Rels.Add(ctx, "R", ns[0], ns[1], nil)
			if err != nil {
				t.Fatal(err)
			}
			refused := func(what, door string, err error) {
				t.Helper()
				var rb *storepkg.ReadBudgetError
				if !errors.As(err, &rb) || rb.Door != door {
					t.Fatalf("%s: %v, want ErrReadBudget from %s", what, err, door)
				}
			}
			bctx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 2})
			got, err := g.Nodes.GetByIDsCtx(bctx, ids)
			refused("GetByIDs of 3", "GetByIDs", err)
			if got != nil {
				t.Fatal("refused GetByIDs returned rows")
			}
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 3})
			if got, err := g.Nodes.GetByIDsCtx(bctx, ids); err != nil || len(got) != 3 || meter.Work().Candidates != 3 {
				t.Fatalf("GetByIDs within = %d, %v, %+v", len(got), err, meter.Work())
			}
			bctx, _ = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1})
			if _, err := g.Nodes.HasHistoryCtx(bctx, ids[0]); err != nil {
				t.Fatal(err)
			}
			_, err = g.Nodes.HasHistoryCtx(bctx, ids[1])
			refused("second HasHistory", "HasHistory", err)
			bctx, _ = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1})
			if _, _, _, err := g.Nodes.LatestStampsCtx(bctx, ids[0]); err != nil {
				t.Fatal(err)
			}
			_, _, _, err = g.Nodes.LatestStampsCtx(bctx, ids[1])
			refused("second LatestStamps", "LatestStamps", err)
			bctx, _ = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 1})
			calls := 0
			err = g.Rels.LendByIDs(bctx, []types.RelID{r.ID(), r.ID()}, func(int, *types.Relationship) bool { calls++; return true })
			refused("Rels LendByIDs of 2", "LendByIDs", err)
			if calls != 0 {
				t.Fatal("refused rel lend called fn")
			}
		})
	}
}
