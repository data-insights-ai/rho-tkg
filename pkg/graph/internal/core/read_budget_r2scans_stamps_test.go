package core

import (
	"context"
	"fmt"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R2-SCANS: HasHistoryCtx and LatestStampsCtx on a store that hides its
// capabilities (a wrapper declaring only the mandatory store: no history
// presence, no history stamps, no metered doors) answer from the entity's whole
// history; that read counts its versions as HistoryExamined, as the other doors
// count a read they cannot admit, instead of dropping the meter and reading the
// history uncounted. Counts only. Notes and red output:
// tasks/evidence/R2SCANS/red.txt.

// rbBareStore wraps a native badger store behind the mandatory interface only,
// so the core finds none of its optional capabilities.
type rbBareStore struct{ storepkg.MandatoryStore }

// TestR2Scans_StampsOnABareStore: on such a store HasHistoryCtx and
// LatestStampsCtx (nodes and relationships) count one candidate and the history
// versions their fold reads (the versions of the entity: 6), answer as the
// plain doors do, refuse a HistoryExamined limit under that count, and answer at
// it. A DecodedBytes or ShardOpens budget is refused before any read, as every
// door refuses what the store cannot count.
func TestR2Scans_StampsOnABareStore(t *testing.T) {
	t.Parallel()
	bs, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(Config{Store: rbBareStore{bs}, UntimedValidStart: UntimedFromTxFrom})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close(); _ = bs.Close() })
	ctx := context.Background()
	s, e := txbEndpoints(t, g)
	n, err := g.Nodes.Add(ctx, []string{"B"}, map[string]any{"k": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.AddByID(ctx, "BR", s, e, map[string]any{"k": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	const versions = 6
	for v := 1; v <= versions; v++ {
		if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"k": int64(v)}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"k": int64(v)}); err != nil {
			t.Fatal(err)
		}
	}
	nodeHist, err := g.Nodes.History(n.ID())
	if err != nil || len(nodeHist) != versions {
		t.Fatalf("fixture: node history = %d versions, %v; want %d", len(nodeHist), err, versions)
	}
	type door struct {
		name string
		run  func(ctx context.Context) (string, error)
	}
	doors := []door{
		{"node HasHistory", func(ctx context.Context) (string, error) {
			has, err := g.Nodes.HasHistoryCtx(ctx, n.ID())
			return answered(boolString(has), err)
		}},
		{"node LatestStamps", func(ctx context.Context) (string, error) {
			from, to, del, err := g.Nodes.LatestStampsCtx(ctx, n.ID())
			return answered(stampsString(from, to, del), err)
		}},
		{"rel HasHistory", func(ctx context.Context) (string, error) {
			has, err := g.Rels.HasHistoryCtx(ctx, r.ID())
			return answered(boolString(has), err)
		}},
		{"rel LatestStamps", func(ctx context.Context) (string, error) {
			from, to, del, err := g.Rels.LatestStampsCtx(ctx, r.ID())
			return answered(stampsString(from, to, del), err)
		}},
	}
	for _, d := range doors {
		t.Run(d.name, func(t *testing.T) {
			want, err := d.run(ctx) // no budget: the plain door
			if err != nil {
				t.Fatal(err)
			}
			fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			if got, err := d.run(fctx); err != nil || got != want {
				t.Fatalf("unlimited = %q, %v; want %q", got, err, want)
			}
			if w := full.Work(); w.Candidates != 1 || w.HistoryExamined != versions {
				t.Fatalf("unlimited work %+v; want 1 candidate and the entity's %d history versions examined", w, versions)
			}
			hctx, one := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{HistoryExamined: versions - 1})
			got, err := d.run(hctx)
			rbdBudgetErr(t, err, "history examined")
			if got != "" || one.Work().HistoryExamined != versions {
				t.Fatalf("HistoryExamined %d = %q, work %+v; want a refusal and no answer", versions-1, got, one.Work())
			}
			ectx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{HistoryExamined: versions})
			if got, err := d.run(ectx); err != nil || got != want {
				t.Fatalf("HistoryExamined %d (the count) = %q, %v; want %q", versions, got, err, want)
			}
			for _, b := range []storepkg.ReadBudget{{DecodedBytes: 1 << 30}, {ShardOpens: 100}} {
				bctx, meter := storepkg.WithReadBudget(ctx, b)
				rbdBudgetErr(t, func() error { _, err := d.run(bctx); return err }(), "unsupported")
				if meter.Work() != (storepkg.ReadWork{}) {
					t.Fatalf("under %+v: work %+v; want a refusal before any work", b, meter.Work())
				}
			}
		})
	}
}

func boolString(b bool) string { return map[bool]string{true: "true", false: "false"}[b] }

// answered is s, or "" when the door failed (no answer).
func answered(s string, err error) (string, error) {
	if err != nil {
		return "", err
	}
	return s, nil
}

func stampsString(from, to types.Instant, deleted bool) string {
	return fmt.Sprintf("%v:%d:%d", deleted, int64(from), int64(to))
}
