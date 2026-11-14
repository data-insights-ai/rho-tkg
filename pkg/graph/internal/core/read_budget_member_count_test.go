package core

import (
	"context"
	"slices"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// TestReadBudget_ScanMemberCount: the label and type scans that gather the
// current members from the store (node by label, node by labels, rel by type)
// ask the store for its member count under the read's tally before it lists
// them (storepkg.MemberCountMeteredCapability), on every meter: the shards the
// count opens are counted where they open, and the count is held on the read's
// room before the listing, under a ShardOpens bound too. On sbBuildTiered with
// its three cold shards closed unsealed before each read (the count must open
// them; the listing and the entity reads find them open), the native observers
// are the store's ColdOpens and the core's listing hook (candidatesListed).
func TestReadBudget_ScanMemberCount(t *testing.T) {
	t.Parallel()
	w := sbBuildTiered(t)
	ctx := context.Background()
	var listed int
	w.g.candidatesListed = func(n int) { listed += n }
	t.Cleanup(func() { w.g.candidatesListed = nil })
	doors := map[string]bool{"node by label": true, "node by labels": true, "rel by type": true}
	var scans []sbScan
	for _, d := range sbScanDoors(w.g, w.pin) {
		if doors[d.name] {
			scans = append(scans, d)
		}
	}
	if len(scans) != len(doors) {
		t.Fatalf("fixture: %d doors, want %d", len(scans), len(doors))
	}
	// run reads d under budget b (inside parent's context when parent is
	// set), each cold shard closed first; it returns the segments, the error,
	// the meter, the native opens and the listed candidates.
	type res struct {
		got    []string
		err    error
		meter  *storepkg.ReadMeter
		opened uint64
		listed int
	}
	run := func(d sbScan, pctx context.Context, b storepkg.ReadBudget) res {
		w.closeCold(t)
		o := w.opens()
		listed = 0
		bctx, meter := storepkg.WithReadBudget(pctx, b)
		got, err := sbCollect(func(fn func(string) bool) error { return d.ctx(bctx, fn) })
		return res{got, err, meter, w.opens() - o, listed}
	}
	for _, d := range scans {
		t.Run(d.name, func(t *testing.T) {
			w.closeCold(t)
			want, err := sbCollect(d.plain)
			if err != nil || len(want) == 0 {
				t.Fatalf("plain = %d segments, %v; want a stream", len(want), err)
			}

			// 1. Unlimited control: every native open counted.
			c := run(d, ctx, storepkg.ReadBudget{})
			work := c.meter.Work()
			if c.err != nil || !slices.Equal(c.got, want) || c.opened != 3 || work.ShardOpens != 3 {
				t.Fatalf("control = %d segments, %v, %d opened, work %+v; want the plain stream and exactly 3 opens counted", len(c.got), c.err, c.opened, work)
			}
			// Six gathered members. Node sidecars rebuild on the cold shards;
			// each timeline then routes current, presence, metadata and versions.
			// A deleted relationship probes ref/mint plus four event entries
			// and three other shards (nine probes), for metadata + three versions.
			c0, l0 := work.Candidates, c.listed
			wantCandidates := uint64(6 + 3) // members and one recovery-header probe per cold open
			if d.name != "rel by type" {
				wantCandidates += 22 + 6*5
			} else {
				wantCandidates += 3*4 + 3*4*9
			}
			if c0 != wantCandidates || l0 != 6 {
				t.Fatalf("control: %d candidates, %d listed; want %d and 6", c0, l0, wantCandidates)
			}

			// 2. ShardOpens at the control's count: the same read, no duplicate charge.
			r := run(d, ctx, storepkg.ReadBudget{ShardOpens: 3})
			if rw := r.meter.Work(); r.err != nil || !slices.Equal(r.got, want) || r.opened != 3 || rw.ShardOpens != 3 || rw.Candidates != c0 || r.listed != l0 {
				t.Fatalf("ShardOpens 3 = %d segments, %v, %d opened, %d listed, work %+v; want the control's (%d candidates, %d listed)", len(r.got), r.err, r.opened, r.listed, rw, c0, l0)
			}

			// 3. One open under: the count refuses before the listing.
			r = run(d, ctx, storepkg.ReadBudget{ShardOpens: 2})
			rb := rbdBudgetErr(t, r.err, "shard opens")
			if rb.Door != d.door || len(r.got) != 0 || r.opened != 2 || r.listed != 0 || rb.Work.Candidates != 2 || r.meter.Work() != rb.Work {
				t.Fatalf("ShardOpens 2: door %q, %d callbacks, %d opened, %d listed, work %+v; want %q, none, 2 opens and their header probes, nothing listed", rb.Door, len(r.got), r.opened, r.listed, rb.Work, d.door)
			}

			// 4. Candidates at the control's count: the opens are counted.
			r = run(d, ctx, storepkg.ReadBudget{Candidates: c0})
			if rw := r.meter.Work(); r.err != nil || !slices.Equal(r.got, want) || r.opened != 3 || rw.ShardOpens != 3 || rw.Candidates != c0 {
				t.Fatalf("Candidates %d = %d segments, %v, %d opened, work %+v; want the plain stream with 3 opens counted", c0, len(r.got), r.err, r.opened, rw)
			}

			// 5. Candidates 1: the second open's header is refused, nothing listed.
			r = run(d, ctx, storepkg.ReadBudget{Candidates: 1})
			rb = rbdBudgetErr(t, r.err, "candidates")
			if len(r.got) != 0 || r.listed != 0 || r.opened != 1 || rb.Work.ShardOpens != 2 || rb.Work.Candidates != 2 || r.meter.Work() != rb.Work {
				t.Fatalf("Candidates 1: %d callbacks, %d listed, %d opened, work %+v; want none, nothing listed, one installed handle and two attempted opens", len(r.got), r.listed, r.opened, rb.Work)
			}

			// 6. Both bounds: the count's hold still precedes the listing.
			r = run(d, ctx, storepkg.ReadBudget{Candidates: 1, ShardOpens: 3})
			rb = rbdBudgetErr(t, r.err, "candidates")
			if len(r.got) != 0 || r.listed != 0 || r.opened != 1 || rb.Work.ShardOpens != 2 || rb.Work.Candidates != 2 {
				t.Fatalf("Candidates 1, ShardOpens 3: %d callbacks, %d listed, %d opened, work %+v; want none, nothing listed", len(r.got), r.listed, r.opened, rb.Work)
			}

			// 7. Parent meters: an outer ShardOpens bound refuses the inner
			// unlimited read; unbounded, both count every open.
			octx, outer := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 2})
			r = run(d, octx, storepkg.ReadBudget{})
			rb = rbdBudgetErr(t, r.err, "shard opens")
			if rb.Budget.ShardOpens != 2 || r.opened != 2 || r.listed != 0 || len(r.got) != 0 || r.meter.Work().ShardOpens != outer.Work().ShardOpens || outer.Work().ShardOpens != 3 {
				t.Fatalf("outer ShardOpens 2: budget %+v, %d opened, %d listed, inner %+v, outer %+v; want the outer refusal at 3 with 2 opens", rb.Budget, r.opened, r.listed, r.meter.Work(), outer.Work())
			}
			octx, outer = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			r = run(d, octx, storepkg.ReadBudget{})
			if r.err != nil || !slices.Equal(r.got, want) || r.opened != 3 || r.meter.Work().ShardOpens != 3 || outer.Work() != r.meter.Work() {
				t.Fatalf("outer unlimited: %v, %d opened, inner %+v, outer %+v; want both at the 3 opens", r.err, r.opened, r.meter.Work(), outer.Work())
			}
		})
	}

	// 8. Several keys on one context: each read alone fits ShardOpens 5, the
	// two together do not; the second is refused after exactly 5 native opens.
	var byLabel, byType sbScan
	for _, d := range scans {
		switch d.name {
		case "node by label":
			byLabel = d
		case "rel by type":
			byType = d
		}
	}
	w.closeCold(t)
	o := w.opens()
	bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 5})
	if _, err := sbCollect(func(fn func(string) bool) error { return byLabel.ctx(bctx, fn) }); err != nil {
		t.Fatalf("first key: %v", err)
	}
	w.closeCold(t)
	got, err := sbCollect(func(fn func(string) bool) error { return byType.ctx(bctx, fn) })
	rb := rbdBudgetErr(t, err, "shard opens")
	if len(got) != 0 || w.opens()-o != 5 || meter.Work().ShardOpens != 6 || rb.Door != byType.door {
		t.Fatalf("second key: %d callbacks, %d opened, work %+v; want the refusal at 6 after 5 native opens", len(got), w.opens()-o, meter.Work())
	}
}
