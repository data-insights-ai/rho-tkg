package core

import (
	"context"
	"slices"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// R2-SCANS (RH list R2): the From and Window scans under a read budget use the
// store's valid-end index, its first call per token after an open metered.
// Counts only (candidates, rows decoded, history looked at, shard opens,
// callbacks): never MemStats or time. Notes and red output:
// tasks/evidence/R2SCANS/red.txt.

// TestR2Scans_ValidEndFirstCallMetered: on a store just opened (the index holds
// no token yet), a budgeted From or Window scan
//   - counts the first call: one candidate for every ever-member of the label
//     or type (every entity the index enlists), counted before the entity's
//     temporal fields are read, then one more for each entity the index lists
//     and the scan reads (the unlimited count is the ever-members plus those),
//     and the history versions and stored bytes of what the first call reads;
//   - refuses inside the first call, at the candidate that passes a limit, with
//     no callback made and no entity timeline read, and leaves the index so
//     that the next call is still exact (the same stream, the first call again);
//   - once the index is resolved, reads only the entities it lists: the count is
//     the listed entities, not the label's every entity;
//   - is exact: a Candidates limit at the unlimited count answers, one under it
//     refuses.
func TestR2Scans_ValidEndFirstCallMetered(t *testing.T) {
	t.Parallel()
	const n = 12
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			w := sbBuild(t, g, n)
			shut()
			// fresh reopens the store: no valid-end index is tracked, rows are cold.
			fresh := func(t *testing.T, run func(g *Core)) {
				t.Helper()
				g, shut := be.open(t, dir)
				defer shut()
				w.g = g
				run(g)
			}
			ctx := context.Background()
			for _, d := range []string{"node by labels from", "rel by type from", "node by labels window", "rel by type window"} {
				t.Run(d, func(t *testing.T) {
					door := func(g *Core) sbScan {
						for _, s := range sbScanDoors(g, w.pin) {
							if s.name == d {
								return s
							}
						}
						t.Fatalf("no door %q", d)
						return sbScan{}
					}
					var (
						want     []string
						listed   uint64
						indexed  = true
						unlimitd storepkg.ReadWork
					)
					fresh(t, func(g *Core) {
						s := door(g)
						if (s.nodes && g.nodeValidEnds == nil) || (!s.nodes && g.relValidEnds == nil) {
							indexed = false // sharded: no valid-end index, the unbounded gather
							return
						}
						var err error
						if want, err = sbCollect(s.plain); err != nil || len(want) == 0 {
							t.Fatalf("plain scan = %d segments, %v", len(want), err)
						}
						listed = uint64(len(w.entities(s)))
					})
					if !indexed {
						t.Skip("no valid-end index on this store")
					}

					// The unlimited first call: the stream, the ever-members counted, then
					// the listed entities.
					fresh(t, func(g *Core) {
						s := door(g)
						uctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
						got, err := sbCollect(func(fn func(string) bool) error { return s.ctx(uctx, fn) })
						if err != nil || !slices.Equal(got, want) {
							t.Fatalf("unlimited first call = %v, %v; want the plain stream\n%v", got, err, want)
						}
						unlimitd = meter.Work()
						if unlimitd.Candidates < n+listed || unlimitd.HistoryExamined == 0 || unlimitd.DecodedBytes == 0 {
							t.Fatalf("unlimited first call counted %+v; want at least %d candidates (the %d ever-members and the %d listed), history examined and decoded bytes", unlimitd, n+listed, n, listed)
						}
						// The index is resolved now: the second call reads what it lists.
						sctx, second := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
						if got, err := sbCollect(func(fn func(string) bool) error { return s.ctx(sctx, fn) }); err != nil || !slices.Equal(got, want) {
							t.Fatalf("second call = %v, %v; want the plain stream", got, err)
						}
						if c := second.Work().Candidates; c != listed || listed >= n {
							t.Fatalf("second call counted %d candidates; want the %d the index lists (of the label's %d)", c, listed, n)
						}
					})

					// A refusal inside the first call: before any callback and any
					// timeline read, and the index is not half-built: the next call is
					// the first call again, with the same stream.
					fresh(t, func(g *Core) {
						s := door(g)
						pctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 5})
						got, err := sbCollect(func(fn func(string) bool) error { return s.ctx(pctx, fn) })
						rb := rbdBudgetErr(t, err, "candidates")
						work := meter.Work()
						if rb.Door != s.door || len(got) != 0 || work.Candidates != 6 || work.Segments != 0 || work.HistoryExamined > 6*4 {
							t.Fatalf("Candidates 5 on the first call: door %q, %d callbacks, work %+v; want %q, none, 6 candidates (the sixth ever-member refused), no segment and at most 6 entities' history", rb.Door, len(got), work, s.door)
						}
						bctx, bm := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{DecodedBytes: 1})
						got, err = sbCollect(func(fn func(string) bool) error { return s.ctx(bctx, fn) })
						rb = rbdBudgetErr(t, err, "decoded bytes")
						if rb.Door != s.door || len(got) != 0 || bm.Work().Candidates > 2 {
							t.Fatalf("DecodedBytes 1 on the first call: door %q, %d callbacks, work %+v; want %q, none and at most 2 entities", rb.Door, len(got), bm.Work(), s.door)
						}
						uctx, um := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
						if got, err := sbCollect(func(fn func(string) bool) error { return s.ctx(uctx, fn) }); err != nil || !slices.Equal(got, want) || um.Work().Candidates != unlimitd.Candidates {
							t.Fatalf("after two refusals: %v, %v, work %+v; want the plain stream and the first call's %d candidates again", got, err, um.Work(), unlimitd.Candidates)
						}
					})

					// Exact at the unlimited count.
					fresh(t, func(g *Core) {
						s := door(g)
						ectx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: unlimitd.Candidates})
						if got, err := sbCollect(func(fn func(string) bool) error { return s.ctx(ectx, fn) }); err != nil || !slices.Equal(got, want) {
							t.Fatalf("Candidates %d (the unlimited count) = %v, %v; want the plain stream", unlimitd.Candidates, got, err)
						}
					})
					fresh(t, func(g *Core) {
						s := door(g)
						ectx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: unlimitd.Candidates - 1})
						got, err := sbCollect(func(fn func(string) bool) error { return s.ctx(ectx, fn) })
						rbdBudgetErr(t, err, "candidates")
						if meter.Work().Candidates != unlimitd.Candidates || len(got) >= len(want) || !sbPrefix(got, want) {
							t.Fatalf("Candidates %d (one under) = %d callbacks, work %+v; want a refusal at the last candidate and a prefix", unlimitd.Candidates-1, len(got), meter.Work())
						}
					})
				})
			}
		})
	}
}
