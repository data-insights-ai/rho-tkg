package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R2-SCANS: Nodes().ByLabelCtx at a time coordinate (TxAt, TxPin, ValidAt, a
// valid interval) under a read budget. It was refused ("unsupported") before any
// work; now it is counted. Counts only. Notes and red output:
// tasks/evidence/R2SCANS/red.txt.

// r2lOpts are the four time coordinates a label read takes, at pin.
func r2lOpts(pin types.Instant) map[string]storepkg.QueryOpts {
	return map[string]storepkg.QueryOpts{
		"TxPin":    {TxPin: pin},
		"TxAt":     {TxAt: pin},
		"ValidAt":  {ValidAt: pin, ValidAtSet: true},
		"interval": {ValidStart: 1, ValidEnd: pin, ValidWindowSet: true},
	}
}

func nodeVersionsOf(nodes []*types.Node) string {
	var out []string
	for _, n := range nodes {
		out = append(out, fmt.Sprintf("%d@v%d", int64(n.ID()), n.Version()))
	}
	return fmt.Sprint(out)
}

// TestR2Scans_ByLabelCtxAtATime: on badger, sharded and tiered, rows pending
// and flushed, ByLabelCtx at each time coordinate
//   - answers as ByLabel does without a budget and under an unlimited one, which
//     counts one candidate for each live member of the label (counted by the
//     store where it lists them, before their rows are read) and one for each
//     other node that ever carried the label (counted before its history is
//     read), 8 buffer bytes for each listed candidate and each answered row, and
//     the history, decoded bytes and shard opens of the reads;
//   - refuses a Candidates limit inside the listing before any history is read, a
//     limit past the listing at the first history-only candidate, and every other
//     limit with no rows; the count is exact (a limit at it answers, one under it
//     refuses);
//   - is the plain door's answer at a page (After and Limit).
func TestR2Scans_ByLabelCtxAtATime(t *testing.T) {
	t.Parallel()
	const n = 12
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			w := sbBuild(t, g, n)
			live := uint64(n - len(w.deletedNodes))
			gone := uint64(len(w.deletedNodes))
			probe := func(t *testing.T, g *Core, flushed bool) {
				w.g = g
				ctx := context.Background()
				cold := func() {
					if flushed {
						w.evict(t)
					}
				}
				for name, opts := range r2lOpts(w.pin) {
					t.Run(name, func(t *testing.T) {
						cold()
						want, err := g.Nodes.ByLabel(sbLabel, opts)
						if err != nil || len(want) == 0 {
							t.Fatalf("plain ByLabel at %s = %d nodes, %v", name, len(want), err)
						}
						cold()
						if got, err := g.Nodes.ByLabelCtx(ctx, sbLabel, opts); err != nil || nodeVersionsOf(got) != nodeVersionsOf(want) {
							t.Fatalf("no budget = %v, %v; want the plain answer %v", nodeVersionsOf(got), err, nodeVersionsOf(want))
						}
						cold()
						fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
						if got, err := g.Nodes.ByLabelCtx(fctx, sbLabel, opts); err != nil || nodeVersionsOf(got) != nodeVersionsOf(want) {
							t.Fatalf("unlimited = %v, %v; want the plain answer %v", nodeVersionsOf(got), err, nodeVersionsOf(want))
						}
						total := full.Work()
						wantBuf := 8*(live+gone) + 8*uint64(len(want)) // #nosec G115 -- a count
						if total.Candidates != live+gone || total.BufferBytes != wantBuf {
							t.Fatalf("unlimited work %+v; want %d candidates (%d live, %d that ended) and %d buffer bytes", total, live+gone, live, gone, wantBuf)
						}
						if flushed && (total.DecodedBytes == 0 || total.HistoryExamined == 0) {
							t.Fatalf("flushed read counted %+v; want decoded bytes and history examined", total)
						}

						refused := func(b storepkg.ReadBudget, limit string) storepkg.ReadWork {
							t.Helper()
							cold()
							pctx, meter := storepkg.WithReadBudget(ctx, b)
							got, err := g.Nodes.ByLabelCtx(pctx, sbLabel, opts)
							rb := rbdBudgetErr(t, err, limit)
							if got != nil || rb.Door != "ByLabel" || meter.Work() != rb.Work {
								t.Fatalf("%s: %d nodes, %+v, meter %+v; want no rows, door ByLabel and the meter equal to the error's work", limit, len(got), rb, meter.Work())
							}
							return meter.Work()
						}
						// Inside the listing: no history read. Past it: the first history-only
						// candidate counts and is refused after the live ones before it were read.
						if work := refused(storepkg.ReadBudget{Candidates: 5}, "candidates"); work.HistoryExamined != 0 || work.Candidates > live {
							t.Fatalf("Candidates 5: work %+v; want a refusal inside the listing of the %d live members, no history read", work, live)
						}
						// (The live members before it were resolved: their answered rows are
						// counted in the buffer past the candidate list.)
						if work := refused(storepkg.ReadBudget{Candidates: live}, "candidates"); work.Candidates != live+1 || work.BufferBytes <= 8*(live+gone) {
							t.Fatalf("Candidates %d: work %+v; want the first member that ended refused (%d candidates) after the live members before it were resolved", live, work, live+1)
						}
						cold()
						ectx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: total.Candidates})
						if got, err := g.Nodes.ByLabelCtx(ectx, sbLabel, opts); err != nil || nodeVersionsOf(got) != nodeVersionsOf(want) {
							t.Fatalf("Candidates %d (the count) = %v, %v; want the plain answer", total.Candidates, nodeVersionsOf(got), err)
						}
						if work := refused(storepkg.ReadBudget{Candidates: total.Candidates - 1}, "candidates"); work.Candidates != total.Candidates {
							t.Fatalf("Candidates %d (one under): work %+v; want the last candidate refused", total.Candidates-1, work)
						}
						if work := refused(storepkg.ReadBudget{HistoryExamined: 1}, "history examined"); work.Candidates > live+gone {
							t.Fatalf("HistoryExamined 1: work %+v", work)
						}
						if work := refused(storepkg.ReadBudget{BufferBytes: 1}, "buffer bytes"); work.HistoryExamined != 0 {
							t.Fatalf("BufferBytes 1: work %+v; want a refusal before any history was read", work)
						}
						if flushed {
							if work := refused(storepkg.ReadBudget{DecodedBytes: 1}, "decoded bytes"); work.Candidates > live+gone {
								t.Fatalf("DecodedBytes 1: work %+v", work)
							}
						}

						// A page: the plain door's window of the answer.
						page := opts
						page.Limit, page.After = 3, types.EntityID(want[1].ID())
						wantPage, err := g.Nodes.ByLabel(sbLabel, page)
						if err != nil || len(wantPage) == 0 {
							t.Fatalf("plain page = %d, %v", len(wantPage), err)
						}
						cold()
						pctx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
						if got, err := g.Nodes.ByLabelCtx(pctx, sbLabel, page); err != nil || nodeVersionsOf(got) != nodeVersionsOf(wantPage) {
							t.Fatalf("budgeted page = %v, %v; want %v", nodeVersionsOf(got), err, nodeVersionsOf(wantPage))
						}
					})
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

// TestR2Scans_ByLabelCtxAtATimeShardOpens: on a tiered store whose cold shards
// are closed before each read, ByLabelCtx at a time coordinate opens the cold
// shards its listing, its membership gather and its reads reach; under
// ShardOpens 1 it opens at most one and refuses with "shard opens" and no rows;
// unlimited it answers as ByLabel and the meter counts exactly the shards opened.
func TestR2Scans_ByLabelCtxAtATimeShardOpens(t *testing.T) {
	t.Parallel()
	w := sbBuildTiered(t)
	ctx := context.Background()
	for name, opts := range r2lOpts(w.pin) {
		t.Run(name, func(t *testing.T) {
			w.closeCold(t)
			opens := w.opens()
			want, err := w.g.Nodes.ByLabel(sbLabel, opts)
			plainOpens := w.opens() - opens
			if err != nil || len(want) == 0 || plainOpens < 3 {
				t.Fatalf("plain = %d nodes, %v, %d cold shards opened; want an answer and at least 3 opens", len(want), err, plainOpens)
			}

			w.closeCold(t)
			opens = w.opens()
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
			got, err := w.g.Nodes.ByLabelCtx(bctx, sbLabel, opts)
			boundedOpened := w.opens() - opens
			rb := rbdBudgetErr(t, err, "shard opens")
			if got != nil || rb.Door != "ByLabel" || meter.Work() != rb.Work || boundedOpened > 1 {
				t.Fatalf("ShardOpens 1: %d nodes, %+v, meter %+v, %d shards opened; want no rows, the meter's work and at most 1 open", len(got), rb, meter.Work(), boundedOpened)
			}

			w.closeCold(t)
			opens = w.opens()
			bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			got, err = w.g.Nodes.ByLabelCtx(bctx, sbLabel, opts)
			opened := w.opens() - opens
			if err != nil || nodeVersionsOf(got) != nodeVersionsOf(want) || opened != plainOpens || meter.Work().ShardOpens != opened {
				t.Fatalf("unlimited = %v, %v, %d shards opened, %d counted; want the plain answer, %d opens, all counted", nodeVersionsOf(got), err, opened, meter.Work().ShardOpens, plainOpens)
			}
			t.Logf("%s: the plain read opened %d cold shards; under ShardOpens 1 it opened %d and refused; unlimited it opened %d and counted %d", name, plainOpens, boundedOpened, opened, meter.Work().ShardOpens)
		})
	}
}

// TestR2Scans_ByLabelCtxAtATimeWrapper: a store that cannot count its own reads
// (a wrapper embedding a native one) refuses a DecodedBytes or ShardOpens budget
// before any work at a time coordinate as it does without one, and answers under
// a Candidates budget through its own reads.
func TestR2Scans_ByLabelCtxAtATimeWrapper(t *testing.T) {
	t.Parallel()
	bs, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(Config{Store: rbWrapStore{bs}, UntimedValidStart: UntimedFromTxFrom})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close(); _ = bs.Close() })
	ctx := context.Background()
	const members = 20
	for range members {
		if _, err := g.Nodes.Add(ctx, []string{"W"}, nil); err != nil {
			t.Fatal(err)
		}
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	opts := storepkg.QueryOpts{TxPin: pin}
	for _, b := range []storepkg.ReadBudget{{DecodedBytes: 1 << 30}, {ShardOpens: 100}} {
		bctx, meter := storepkg.WithReadBudget(ctx, b)
		got, err := g.Nodes.ByLabelCtx(bctx, "W", opts)
		rb := rbdBudgetErr(t, err, "unsupported")
		if got != nil || rb.Door != "ByLabel" || meter.Work() != (storepkg.ReadWork{}) {
			t.Fatalf("under %+v: %d nodes, %+v, work %+v; want a refusal before any work", b, len(got), rb, meter.Work())
		}
	}
	want, err := g.Nodes.ByLabel("W", opts)
	if err != nil || len(want) != members {
		t.Fatalf("plain = %d, %v", len(want), err)
	}
	bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: members})
	if got, err := g.Nodes.ByLabelCtx(bctx, "W", opts); err != nil || !slices.EqualFunc(got, want, func(a, b *types.Node) bool { return a.ID() == b.ID() }) || meter.Work().Candidates != members {
		t.Fatalf("Candidates %d = %d nodes, %v, %+v; want the plain answer and the label counted once", members, len(got), err, meter.Work())
	}
	bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: members - 1})
	got, err := g.Nodes.ByLabelCtx(bctx, "W", opts)
	if rbdBudgetErr(t, err, "candidates"); got != nil || meter.Work().HistoryExamined != 0 {
		t.Fatalf("Candidates %d = %d nodes, work %+v; want a refusal before any history was read", members-1, len(got), meter.Work())
	}
	var rb *storepkg.ReadBudgetError
	if _, err := g.Nodes.ByLabelCtx(ctx, "W", opts); errors.As(err, &rb) {
		t.Fatalf("no budget: %v", err)
	}
}
