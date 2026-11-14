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
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The label and type gathers under a read budget (R2 meter, lane A: R2-1
// GATHERHOLD with R2-3 PRUNELIN; design-fixround.txt 3.4 and design-meter.txt
// 7). Every entry a gather takes into its hands is one candidate of the read:
// held ahead of the work that produces it (ReadTally.Hold, against the room net
// of the read's holds), counted by whoever drops it (ReadTally.Drop) or by the
// door that reads it. So for a read that runs to its end, the unlimited
// control's Work.Candidates is the smallest Candidates budget that answers.
// Native counters, which no meter keeps: the stores' sidecar copies
// (badger.CountTxMembersCopiedForTest, memory.CountTxMembersCopiedForTest) and
// the index entries the disk walks visit. These tests use the process-wide
// counters: none runs in parallel.

// gatherBackends are rbdBackends and the memory store.
func gatherBackends() []rbdBackend {
	return append(rbdBackends(), rbdBackend{"memory", func(t testing.TB, _ string) (*Core, func()) {
		g, err := New(Config{UntimedValidStart: UntimedFromTxFrom})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return g, func() { _ = g.Close() }
	}})
}

// copiedCounter counts the ever-members the badger and memory sidecar copies
// make (tiered's shards are badger stores).
func copiedCounter(t *testing.T) func() int64 {
	t.Helper()
	b, stopB := badger.CountTxMembersCopiedForTest()
	m, stopM := memory.CountTxMembersCopiedForTest()
	t.Cleanup(func() { stopB(); stopM() })
	return func() int64 { return b() + m() }
}

// addDeleted adds n nodes with labels, deleting the first del of them.
func addDeleted(t *testing.T, g *Core, labels []string, n, del int) []types.NodeID {
	t.Helper()
	ctx := context.Background()
	var ids []types.NodeID
	for i := range n {
		nd, err := g.Nodes.Add(ctx, labels, map[string]any{"i": int64(i)})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, nd.ID())
		if i < del {
			if err := g.Nodes.Delete(ctx, nd.ID()); err != nil {
				t.Fatal(err)
			}
		}
	}
	return ids
}

// TestGather_HeldTight (T2): a label of 5 current and 6 former members. The
// effective scan holds the 5 current members before the store lists them and
// the store holds the 6 former ones before it copies them; the temporal ByLabel
// read counts the 5 at its listing and the store holds the 6 against what is
// left. The hold was the room plus the current members of the listed read, and
// the room before the current members were held for the scan, so a set of up
// to that many former members was copied and then refused. Under Candidates
// 10 both doors are refused with no callback and nothing copied; 11 (the
// control's count) answers.
func TestGather_HeldTight(t *testing.T) {
	const current, former = 5, 6
	for _, be := range gatherBackends() {
		t.Run(be.name, func(t *testing.T) {
			g, shut := be.open(t, t.TempDir())
			defer shut()
			addDeleted(t, g, []string{"G"}, current+former, former)
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			copied := copiedCounter(t)
			doors := map[string]func(ctx context.Context) ([]string, error){
				"scan": func(ctx context.Context) ([]string, error) {
					return sbCollect(func(fn func(string) bool) error {
						return g.Temporal.ForEachNodeEffectiveByLabelCtx(ctx, "G", pin, func(s temporal.NodeSegment) bool { return fn(sbNodeSeg(s)) })
					})
				},
				"ByLabel": func(ctx context.Context) ([]string, error) {
					nodes, err := g.Nodes.ByLabelCtx(ctx, "G", storepkg.QueryOpts{TxPin: pin})
					return []string{nodeVersionsOf(nodes)}[:min(len(nodes), 1)], err
				},
			}
			for name, run := range doors {
				t.Run(name, func(t *testing.T) {
					before := copied()
					uctx, um := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
					want, err := run(uctx)
					if err != nil || len(want) == 0 || um.Work().Candidates != current+former {
						t.Fatalf("unlimited: %v, %v, work %+v; want the answer and %d candidates", want, err, um.Work(), current+former)
					}
					if be.name != "sharded" && copied()-before < former {
						t.Fatalf("unlimited: %d copied; want the %d former members copied", copied()-before, former)
					}
					before = copied()
					lctx, lm := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: current + former - 1})
					got, err := run(lctx)
					rbdBudgetErr(t, err, "candidates")
					if len(got) != 0 || copied() != before {
						t.Fatalf("Candidates %d: %d answered, %d copied, work %+v; want the refusal with no callback and nothing copied", current+former-1, len(got), copied()-before, lm.Work())
					}
					ectx, _ := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: current + former})
					if got, err := run(ectx); err != nil || fmt.Sprint(got) != fmt.Sprint(want) {
						t.Fatalf("Candidates %d: %v, %v; want the control's answer", current+former, got, err)
					}
				})
			}
		})
	}
}

// TestGather_OneRoomOverLabels (T3): three labels of 2 current and 6 former
// members each, scanned together. Each label had its own allowance, so the
// stores copied every label's set (18 on badger and tiered, 24 on memory,
// whose set repeats the current members) before the union was refused. The
// holds of one read now share one room: under Candidates 10 the read is
// refused at the second label, with only the first label's set copied.
func TestGather_OneRoomOverLabels(t *testing.T) {
	const current, former, room = 2, 6, 10
	labels := []string{"A", "B", "C"}
	for _, be := range gatherBackends() {
		if be.name == "sharded" {
			continue // no sidecar: the history fold, covered by T6
		}
		t.Run(be.name, func(t *testing.T) {
			g, shut := be.open(t, t.TempDir())
			defer shut()
			for _, l := range labels {
				addDeleted(t, g, []string{l}, current+former, former)
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			perLabel := int64(former)
			if be.name == "memory" {
				perLabel = current + former
			}
			copied := copiedCounter(t)
			run := func(ctx context.Context) ([]string, error) {
				return sbCollect(func(fn func(string) bool) error {
					return g.Temporal.ForEachNodeEffectiveByLabelsCtx(ctx, labels, pin, func(s temporal.NodeSegment) bool { return fn(sbNodeSeg(s)) })
				})
			}
			before := copied()
			uctx, um := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
			if got, err := run(uctx); err != nil || len(got) == 0 || copied()-before != 3*perLabel || um.Work().Candidates != 3*(current+former) {
				t.Fatalf("unlimited: %d segments, %v, %d copied, work %+v; want the stream, %d copied, %d candidates", len(got), err, copied()-before, um.Work(), 3*perLabel, 3*(current+former))
			}
			before = copied()
			lctx, lm := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: room})
			got, err := run(lctx)
			rbdBudgetErr(t, err, "candidates")
			if len(got) != 0 || copied()-before != perLabel {
				t.Fatalf("Candidates %d: %d segments, %d copied, work %+v; want the refusal at the second label with %d copied", room, len(got), copied()-before, lm.Work(), perLabel)
			}
		})
	}
}

// gatherTiered opens a tiered store whose next batch can go to a new event
// shard (rotate).
func gatherTiered(t *testing.T) (*Core, *tiered.Store, func()) {
	t.Helper()
	ts, err := tiered.New(tiered.Config{
		DataDir:       t.TempDir(),
		RefLabels:     []string{"Ref"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(Config{Store: ts, UntimedValidStart: UntimedFromTxFrom})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close(); _ = ts.Close() })
	rotate := func() {
		time.Sleep(2 * time.Millisecond)
		if err := ts.RotateHotShard(); err != nil {
			t.Fatal(err)
		}
	}
	return g, ts, rotate
}

// TestGather_ShardsExceedTogether (T4, T5's twin): the former members of a
// label (of a type) on three event shards, 4 on each, under a room of 10: each
// shard's set fits it, together they pass it. The shards hold their sets on one
// room: the third hold is refused, so the read is refused with no callback, two
// sets copied (8) and the 12 counted (the 8 held and the 4 of the hold that
// passed). The control copies the 12 and counts 12. Second case: one entity
// that is a former member on two shards (formerOnTwoShards) is held by both
// and dropped once by the union, so the control counts it twice and that count
// answers as a budget (it was refused: the control counted it once); a budget
// of 1 is refused at the second hold, one set copied and 2 counted.
func TestGather_ShardsExceedTogether(t *testing.T) {
	const per, shards, room = 4, 3, 10
	for _, kind := range []string{"label", "type"} {
		t.Run(kind, func(t *testing.T) {
			g, ts, rotate := gatherTiered(t)
			ctx := context.Background()
			for s := range shards {
				if s > 0 {
					rotate()
				}
				if kind == "label" {
					addDeleted(t, g, []string{"G"}, per, per)
					continue
				}
				src, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				dst, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				for range per {
					r, err := g.Rels.AddByID(ctx, "GT", src.ID(), dst.ID(), nil)
					if err != nil {
						t.Fatal(err)
					}
					if err := g.Rels.Delete(ctx, r.ID()); err != nil {
						t.Fatal(err)
					}
				}
			}
			if n := len(ts.EventShardsForTest()); n != shards {
				t.Fatalf("fixture: %d event shards, want %d", n, shards)
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			run := func(ctx context.Context) ([]string, error) {
				return sbCollect(func(fn func(string) bool) error {
					if kind == "label" {
						return g.Temporal.ForEachNodeEffectiveByLabelCtx(ctx, "G", pin, func(s temporal.NodeSegment) bool { return fn(sbNodeSeg(s)) })
					}
					return g.Temporal.ForEachRelEffectiveByTypeCtx(ctx, "GT", pin, func(s temporal.RelSegment) bool { return fn(sbRelSeg(s)) })
				})
			}
			copied := copiedCounter(t)
			uctx, um := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			if got, err := run(uctx); err != nil || len(got) == 0 || copied() != per*shards || um.Work().Candidates != per*shards {
				t.Fatalf("unlimited: %d segments, %v, %d copied, work %+v; want the stream, %d copied and counted", len(got), err, copied(), um.Work(), per*shards)
			}
			before := copied()
			lctx, lm := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: room})
			got, err := run(lctx)
			rbdBudgetErr(t, err, "candidates")
			if len(got) != 0 || copied()-before != 2*per || lm.Work().Candidates != per*shards {
				t.Fatalf("Candidates %d: %d segments, %d copied, work %+v; want the refusal with no callback, %d copied, %d counted", room, len(got), copied()-before, lm.Work(), 2*per, per*shards)
			}
		})
	}

	t.Run("former on two shards", func(t *testing.T) {
		g, ts, _ := gatherTiered(t)
		formerOnTwoShards(t, g, ts)
		pin, err := g.Temporal.NowTx()
		if err != nil {
			t.Fatal(err)
		}
		run := func(ctx context.Context) ([]string, error) {
			return sbCollect(func(fn func(string) bool) error {
				return g.Temporal.ForEachNodeEffectiveByLabelCtx(ctx, "G", pin, func(s temporal.NodeSegment) bool { return fn(sbNodeSeg(s)) })
			})
		}
		uctx, um := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
		want, err := run(uctx)
		if err != nil || len(want) == 0 || um.Work().Candidates != 2 {
			t.Fatalf("unlimited: %v, %v, work %+v; want the stream and 2 candidates (held on two shards, one dropped by the union, one read)", want, err, um.Work())
		}
		ectx, _ := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: 2})
		if got, err := run(ectx); err != nil || fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("Candidates 2 (the control's count): %v, %v; want the control's answer", got, err)
		}
		copied := copiedCounter(t)
		lctx, lm := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: 1})
		got, err := run(lctx)
		rbdBudgetErr(t, err, "candidates")
		if len(got) != 0 || copied() != 1 || lm.Work().Candidates != 2 {
			t.Fatalf("Candidates 1: %v, %d copied, work %+v; want no callback, 1 copied, 2 counted", got, copied(), lm.Work())
		}
	})
}

// formerOnTwoShards adds a reference entity carrying Ref and G (property i 0)
// with versions on the reference shard, archives it, adds versions on the
// archive, restores and deletes it: both shards keep G history rows and no
// current row, a former member of G (and Ref) on each.
func formerOnTwoShards(t *testing.T, g *Core, ts *tiered.Store) types.NodeID {
	t.Helper()
	ctx := context.Background()
	id := addDeleted(t, g, []string{"Ref", "G"}, 1, 0)[0]
	update := func(vs ...int64) {
		for _, v := range vs {
			if _, err := g.Nodes.Update(ctx, id, map[string]any{"i": v}); err != nil {
				t.Fatal(err)
			}
		}
	}
	update(1, 2)
	if err := ts.ArchiveNode(id); err != nil {
		t.Fatal(err)
	}
	update(3, 4)
	if err := ts.RestoreNode(id); err != nil {
		t.Fatal(err)
	}
	if err := g.Nodes.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	return id
}

// TestGather_PageCountsNoUnionDrop (fix round, review defect 1): the paged
// doors gather without holding (gatherFree) and count their own entities, so
// the tiered union, which counts an entity a second shard hands on only for a
// gather that held it, counts nothing for them. formerOnTwoShards' entity,
// former of Ref and G on the reference shard and the archive, through
// NodesEffectivePage (i = 0, limit 100): the control answers its one segment
// and counts 1 candidate (the entity read), as at 48d6c5a4; that budget
// answers. Before the fix the two labels' unions each counted the second
// shard's copy: 3, refused at 1 and 2.
func TestGather_PageCountsNoUnionDrop(t *testing.T) {
	g, ts, _ := gatherTiered(t)
	formerOnTwoShards(t, g, ts)
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	page := func(ctx context.Context) ([]string, error) {
		segs, _, err := g.Temporal.NodesEffectivePage(ctx, temporal.Pin{Tx: pin}, temporal.Filter{Prop: "i", Eq: int64(0)}, 0, 100)
		var out []string
		for _, s := range segs {
			out = append(out, sbNodeSeg(s))
		}
		return out, err
	}
	uctx, um := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
	want, err := page(uctx)
	if err != nil || len(want) != 1 || um.Work().Candidates != 1 {
		t.Fatalf("unlimited: %v, %v, work %+v; want one segment and 1 candidate", want, err, um.Work())
	}
	ectx, em := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: 1})
	if got, err := page(ectx); err != nil || fmt.Sprint(got) != fmt.Sprint(want) || em.Work().Candidates != 1 {
		t.Fatalf("Candidates 1 (the control's count): %v, %v, work %+v; want the control's page", got, err, em.Work())
	}
}

// TestGather_ReacquiredLabel (fix round, review defect 2): a node that lost a
// label and took it again is a current member and, on badger and tiered, a
// stale entry of the ever-member sidecar (high, never low: the sidecar is not
// pruned). The scan holds it twice, as a current member and in the store's
// copy, and counts both when it settles; memory's set repeats the current
// members, which its copy does not hold again, and sharded folds the history.
// Label G on 4 nodes (with Ev), the sidecar built by a first scan, then 2 of
// them lose G and take it again. ForEachNodeEffectiveByLabelCtx at the pin:
// the control answers the 4 nodes with 6 candidates on badger and tiered and 4
// on memory and sharded; that count answers as a budget, one less refuses with
// no callback, on every backend. Before, badger and tiered counted 4 and
// refused 4 and 5 with 6 counted.
func TestGather_ReacquiredLabel(t *testing.T) {
	want := map[string]uint64{"badger": 6, "tiered": 6, "memory": 4, "sharded": 4}
	for _, be := range gatherBackends() {
		t.Run(be.name, func(t *testing.T) {
			g, shut := be.open(t, t.TempDir())
			defer shut()
			ctx := context.Background()
			ids := addDeleted(t, g, []string{"Ev", "G"}, 4, 0)
			run := func(ctx context.Context, pin types.Instant) ([]string, error) {
				return sbCollect(func(fn func(string) bool) error {
					return g.Temporal.ForEachNodeEffectiveByLabelCtx(ctx, "G", pin, func(s temporal.NodeSegment) bool { return fn(sbNodeSeg(s)) })
				})
			}
			first, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := run(ctx, first); err != nil { // builds the sidecars
				t.Fatal(err)
			}
			for _, id := range ids[:2] {
				if err := g.Nodes.RemoveLabel(ctx, id, "G"); err != nil {
					t.Fatal(err)
				}
				if err := g.Nodes.AddLabel(ctx, id, "G"); err != nil {
					t.Fatal(err)
				}
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			uctx, um := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			answer, err := run(uctx, pin)
			count := um.Work().Candidates
			if err != nil || len(answer) == 0 || count != want[be.name] {
				t.Fatalf("unlimited: %v, %v, work %+v; want the stream and %d candidates", answer, err, um.Work(), want[be.name])
			}
			ectx, em := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: count})
			if got, err := run(ectx, pin); err != nil || fmt.Sprint(got) != fmt.Sprint(answer) || em.Work().Candidates != count {
				t.Fatalf("Candidates %d (the control's count): %v, %v, work %+v; want the control's answer and count", count, got, err, em.Work())
			}
			lctx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: count - 1})
			if got, err := run(lctx, pin); len(got) != 0 || !errors.Is(err, storepkg.ErrReadBudget) {
				t.Fatalf("Candidates %d: %v, %v; want the refusal with no callback", count-1, got, err)
			}
		})
	}
}

// TestGather_TypeHeldTight (T5, T2's twin for a type): a type of 5 current and
// 6 former relationships through the effective type scan. Under Candidates 10
// the scan is refused with no callback and nothing copied; 11 answers.
func TestGather_TypeHeldTight(t *testing.T) {
	const current, former = 5, 6
	for _, be := range gatherBackends() {
		t.Run(be.name, func(t *testing.T) {
			g, shut := be.open(t, t.TempDir())
			defer shut()
			ctx := context.Background()
			a, err := g.Nodes.Add(ctx, []string{"Ref"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			z, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			for i := range current + former {
				r, err := g.Rels.AddByID(ctx, "GT", a.ID(), z.ID(), nil)
				if err != nil {
					t.Fatal(err)
				}
				if i < former {
					if err := g.Rels.Delete(ctx, r.ID()); err != nil {
						t.Fatal(err)
					}
				}
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			copied := copiedCounter(t)
			run := func(ctx context.Context) ([]string, error) {
				return sbCollect(func(fn func(string) bool) error {
					return g.Temporal.ForEachRelEffectiveByTypeCtx(ctx, "GT", pin, func(s temporal.RelSegment) bool { return fn(sbRelSeg(s)) })
				})
			}
			uctx, um := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			want, err := run(uctx)
			if err != nil || len(want) == 0 || um.Work().Candidates != current+former {
				t.Fatalf("unlimited: %d segments, %v, work %+v; want the stream and %d candidates", len(want), err, um.Work(), current+former)
			}
			before := copied()
			lctx, lm := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: current + former - 1})
			got, err := run(lctx)
			rbdBudgetErr(t, err, "candidates")
			if len(got) != 0 || copied() != before {
				t.Fatalf("Candidates %d: %d segments, %d copied, work %+v; want the refusal with no callback and nothing copied", current+former-1, len(got), copied()-before, lm.Work())
			}
			ectx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: current + former})
			if got, err := run(ectx); err != nil || fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("Candidates %d: %d segments, %v; want the control's", current+former, len(got), err)
			}
		})
	}
}

// TestGather_BudgetIsTheControlCount (T6, review finding M1): the hold was the
// whole set before the pin filter and After, which dropped members uncounted,
// so the control's count could be refused. Label G: a1..a5 before the pin; after
// it a4 is deleted (a former member the pin keeps), b1..b6 gain G and are
// deleted (former members acquired after the pin: dropped) and c1 gains G (a
// current member that did not exist at the pin). The effective scan, the
// temporal ByLabel read and its page after a4 (which drops a4) each count the
// 12 entries the gather examined (5 current members, 7 former), answer that
// budget with the control's answer and refuse one less, on every backend.
func TestGather_BudgetIsTheControlCount(t *testing.T) {
	for _, be := range gatherBackends() {
		t.Run(be.name, func(t *testing.T) {
			g, shut := be.open(t, t.TempDir())
			defer shut()
			ctx := context.Background()
			a := addDeleted(t, g, []string{"G"}, 5, 0)
			time.Sleep(2 * time.Millisecond)
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(2 * time.Millisecond)
			if err := g.Nodes.Delete(ctx, a[3]); err != nil {
				t.Fatal(err)
			}
			addDeleted(t, g, []string{"G"}, 6, 6)
			addDeleted(t, g, []string{"G"}, 1, 0)
			doors := map[string]func(ctx context.Context) ([]string, error){
				"scan": func(ctx context.Context) ([]string, error) {
					return sbCollect(func(fn func(string) bool) error {
						return g.Temporal.ForEachNodeEffectiveByLabelCtx(ctx, "G", pin, func(s temporal.NodeSegment) bool { return fn(sbNodeSeg(s)) })
					})
				},
				"ByLabel": func(ctx context.Context) ([]string, error) {
					nodes, err := g.Nodes.ByLabelCtx(ctx, "G", storepkg.QueryOpts{TxPin: pin})
					return []string{nodeVersionsOf(nodes)}[:min(len(nodes), 1)], err
				},
				"page": func(ctx context.Context) ([]string, error) {
					nodes, err := g.Nodes.ByLabelCtx(ctx, "G", storepkg.QueryOpts{TxPin: pin, After: types.EntityID(a[3]), Limit: 10})
					return []string{nodeVersionsOf(nodes)}[:min(len(nodes), 1)], err
				},
			}
			for _, name := range slices.Sorted(func(yield func(string) bool) {
				for k := range doors {
					if !yield(k) {
						return
					}
				}
			}) {
				run := doors[name]
				t.Run(name, func(t *testing.T) {
					uctx, um := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
					want, err := run(uctx)
					count := um.Work().Candidates
					if err != nil || len(want) == 0 || count != 12 {
						t.Fatalf("unlimited: %v, %v, work %+v; want a non-empty answer and 12 candidates", want, err, um.Work())
					}
					ectx, em := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: count})
					if got, err := run(ectx); err != nil || fmt.Sprint(got) != fmt.Sprint(want) || em.Work().Candidates != count {
						t.Fatalf("Candidates %d (the control's count): %v, %v, work %+v; want the control's answer and count", count, got, err, em.Work())
					}
					lctx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: count - 1})
					got, err := run(lctx)
					if len(got) != 0 || !errors.Is(err, storepkg.ErrReadBudget) {
						t.Fatalf("Candidates %d: %v, %v; want the refusal", count-1, got, err)
					}
				})
			}
		})
	}
}

// TestTieredPropertyDeclineThroughTheDoor (D3): a tiered property membership
// read whose answer an open shard declines asks every open shard before any
// walk (O8), so the decline costs nothing and the graph's fallback is the
// read's whole count. Three event shards each hold 5
// nodes of Sig with k=1 and 5 relationships of ST with w=1; the hot shard has
// lost both indexes. Nodes().ByLabelAndPropertyCtx and
// Rels().ByTypeAndPropertyCtx at the pin: the control answers the 15 rows and
// counts the fallback alone (dtFallback: 15), with no member entry walked:
// the node read visits 10 index entries on disk, the fallback's equality
// lookups on the two shards that keep the index
// (badger.CountIndexEntriesVisitedForTest, which the meter does not keep; the
// relationship index is in RAM, so the rels door is the count's guard only).
// That count as the budget answers, one less refuses with no row. Before, the
// two older shards walked their 10 node member entries before the hot shard's
// decline was seen (20 visited), and gave that work back.
func TestTieredPropertyDeclineThroughTheDoor(t *testing.T) {
	const per, shards = 5, 3
	ts, err := tiered.New(tiered.Config{
		DataDir:             t.TempDir(),
		RefLabels:           []string{"Ref"},
		ShardWindow:         7 * 24 * time.Hour,
		FlushInterval:       1<<63 - 1,
		PropertyIndexOnDisk: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(Config{Store: ts, UntimedValidStart: UntimedFromTxFrom})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close(); _ = ts.Close() }()
	ctx := context.Background()
	if err := g.Index.CreateProperty("Sig", "k"); err != nil {
		t.Fatal(err)
	}
	if err := g.Index.CreateRelProperty("ST", "w"); err != nil {
		t.Fatal(err)
	}
	for s := range shards {
		if s > 0 {
			time.Sleep(2 * time.Millisecond)
			if err := ts.RotateHotShard(); err != nil {
				t.Fatal(err)
			}
		}
		var first types.NodeID
		for i := range per {
			n, err := g.Nodes.Add(ctx, []string{"Sig"}, map[string]any{"k": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			if i == 0 {
				first = n.ID()
			}
		}
		src, err := g.Nodes.Add(ctx, []string{"Src"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		for range per {
			if _, err := g.Rels.AddByID(ctx, "ST", src.ID(), first, map[string]any{"w": int64(1)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := ts.Flush(); err != nil {
		t.Fatal(err)
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	// The hot shard (open, walked last) loses both indexes.
	ltok, _ := g.labels.Lookup("Sig")
	rtok, _ := g.relTypes.Lookup("ST")
	if n := len(ts.EventShardsForTest()); n != shards {
		t.Fatalf("fixture: %d event shards, want %d", n, shards)
	}
	hot := ts.HotShardForTest().Store()
	if err := hot.DropPropertyIndex(ltok, "k"); err != nil {
		t.Fatal(err)
	}
	if err := hot.DropRelPropertyIndex(rtok, "w"); err != nil {
		t.Fatal(err)
	}
	visited, stop := badger.CountIndexEntriesVisitedForTest()
	defer stop()

	doors := map[string]func(ctx context.Context) (int, error){
		"nodes": func(ctx context.Context) (int, error) {
			rows, err := g.Nodes.ByLabelAndPropertyCtx(ctx, "Sig", "k", int64(1), storepkg.QueryOpts{TxPin: pin})
			return len(rows), err
		},
		"rels": func(ctx context.Context) (int, error) {
			rows, err := g.Rels.ByTypeAndPropertyCtx(ctx, "ST", "w", int64(1), storepkg.QueryOpts{TxPin: pin})
			return len(rows), err
		},
	}
	for _, name := range []string{"nodes", "rels"} {
		run := doors[name]
		t.Run(name, func(t *testing.T) {
			before := visited()
			uctx, um := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			rows, err := run(uctx)
			examined := visited() - before
			count := um.Work().Candidates
			t.Logf("%s: control %d rows, work %+v, %d index entries visited", name, rows, um.Work(), examined)
			if err != nil || rows != per*shards || examined != dtVisited[name] || count != dtFallback[name] {
				t.Fatalf("%s control: %d rows, %v, %d visited, work %+v; want %d rows, %d entries visited (no member walk), %d candidates (the fallback alone)", name, rows, err, examined, um.Work(), per*shards, dtVisited[name], dtFallback[name])
			}
			ectx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: count})
			if rows, err := run(ectx); err != nil || rows != per*shards {
				t.Fatalf("%s Candidates %d: %d rows, %v; want the control's", name, count, rows, err)
			}
			lctx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: count - 1})
			if rows, err := run(lctx); rows != 0 || !errors.Is(err, storepkg.ErrReadBudget) {
				t.Fatalf("%s Candidates %d: %d rows, %v; want the refusal with no row", name, count-1, rows, err)
			}
		})
	}
}

// dtFallback is the count of TestTieredPropertyDeclineThroughTheDoor's reads
// (the fallback's), and dtVisited the disk index entries each visits (the
// fallback's equality lookups; no member walk).
var (
	dtFallback = map[string]uint64{"nodes": 15, "rels": 15}
	dtVisited  = map[string]int64{"nodes": 10, "rels": 0}
)
