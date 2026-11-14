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
// counted before the work that produces it (ReadTally.Hold). Drop and Settle
// check for a shared refusal without charging again. For a read that runs to its end, the unlimited
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

// warmGatherMembership isolates list admission from first-build admission,
// which has its own native and public cold-read regressions. No valid-end
// index or entity read cache is warmed here.
func warmGatherMembership(t *testing.T, g *Core) {
	t.Helper()
	if g.labelTxMembers != nil {
		for _, name := range g.labels.ExportNames() {
			tok, ok := g.labels.Lookup(name)
			if ok {
				if err := g.labelTxMembers.ForEachLabelTxMember(tok, func(types.NodeID, types.Instant) bool { return true }); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if g.relTypeTxMembers != nil {
		for _, name := range g.relTypes.ExportNames() {
			tok, ok := g.relTypes.Lookup(name)
			if ok {
				if err := g.relTypeTxMembers.ForEachRelTypeTxMember(tok, func(types.RelID, types.Instant) bool { return true }); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
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
// 10 both doors are refused with no callback and nothing copied. The complete
// read also admits any native as-of probes after gathering the snapshots.
func TestGather_HeldTight(t *testing.T) {
	const current, former = 5, 6
	for _, be := range gatherBackends() {
		t.Run(be.name, func(t *testing.T) {
			gatherCandidates := uint64(current + former)
			if be.name == "memory" {
				gatherCandidates += current
			}
			g, shut := be.open(t, t.TempDir())
			defer shut()
			addDeleted(t, g, []string{"G"}, current+former, former)
			warmGatherMembership(t, g)
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
					wantCandidates := gatherCandidates
					if be.name == "badger" && name == "ByLabel" {
						// One current-row probe per member. Presence is still cold:
						// each former member captures and probes its pending key.
						wantCandidates += current + former + 2*former
					}
					before := copied()
					uctx, um := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
					want, err := run(uctx)
					if err != nil || len(want) == 0 || um.Work().Candidates != wantCandidates {
						t.Fatalf("unlimited: %v, %v, work %+v; want the answer and %d candidates", want, err, um.Work(), wantCandidates)
					}
					wantCopied := int64(former)
					if be.name == "memory" {
						wantCopied += current
					}
					if copied()-before != wantCopied {
						t.Fatalf("unlimited: %d copied; want exactly %d native membership copies", copied()-before, wantCopied)
					}
					before = copied()
					lctx, lm := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: gatherCandidates - 1})
					got, err := run(lctx)
					rbdBudgetErr(t, err, "candidates")
					if len(got) != 0 || copied() != before {
						t.Fatalf("Candidates %d: %d answered, %d copied, work %+v; want the refusal with no callback and nothing copied", gatherCandidates-1, len(got), copied()-before, lm.Work())
					}
					ectx, _ := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: wantCandidates})
					if got, err := run(ectx); err != nil || fmt.Sprint(got) != fmt.Sprint(want) {
						t.Fatalf("Candidates %d: %v, %v; want the control's answer", wantCandidates, got, err)
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
			warmGatherMembership(t, g)
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
			if got, err := run(uctx); err != nil || len(got) == 0 || copied()-before != 3*perLabel || um.Work().Candidates != uint64(3*(current+perLabel)) {
				t.Fatalf("unlimited: %d segments, %v, %d copied, work %+v; want the stream, %d copied, %d candidates", len(got), err, copied()-before, um.Work(), 3*perLabel, uint64(3*(current+perLabel)))
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

// gatherProbe installs the native counters of a gather test on g, which no
// meter keeps: the IDs a gather lists (Core.candidatesListed) and the entity
// reads a scan begins (Core.scanReadTestHook). reset zeroes both.
type gatherProbe struct{ listed, reads int }

func newGatherProbe(t *testing.T, g *Core) *gatherProbe {
	t.Helper()
	p := &gatherProbe{}
	g.candidatesListed = func(n int) { p.listed += n }
	g.scanReadTestHook = func() { p.reads++ }
	t.Cleanup(func() { g.candidatesListed, g.scanReadTestHook = nil, nil })
	return p
}

func (p *gatherProbe) reset() { p.listed, p.reads = 0, 0 }

// TestGather_FoldOverLabels preserves the exact plain multi-label fold:
// x1 carries A, x2 A and B, x3 B; x4 (A) and x5 (C) were deleted.
// A meter refuses before the native history union, which cannot admit its
// copies. No current listing, history enumeration or entity read begins.
func TestGather_FoldOverLabels(t *testing.T) {
	var sharded rbdBackend
	for _, be := range rbdBackends() {
		if be.name == "sharded" {
			sharded = be
		}
	}
	g, shut := sharded.open(t, t.TempDir())
	defer shut()
	// Hide native membership admission so this exercises a real history
	// union fallback even when the underlying store has its own sidecars.
	counter := &historyEnumCounter{MandatoryStore: g.store}
	g.store = counter
	g.labelTxMembers = labelTxMembershipCapability(counter)
	g.relTypeTxMembers = relTypeTxMembershipCapability(counter)
	ctx := context.Background()
	add := func(labels ...string) types.NodeID {
		n, err := g.Nodes.Add(ctx, labels, nil)
		if err != nil {
			t.Fatal(err)
		}
		return n.ID()
	}
	x1, x2, x3, x4, x5 := add("A"), add("A", "B"), add("B"), add("A"), add("C")
	for _, id := range []types.NodeID{x4, x5} {
		if err := g.Nodes.Delete(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	run := func(ctx context.Context) ([]string, error) {
		return sbCollect(func(fn func(string) bool) error {
			return g.Temporal.ForEachNodeEffectiveByLabelsCtx(ctx, []string{"A", "B"}, pin, func(s temporal.NodeSegment) bool { return fn(sbNodeSeg(s)) })
		})
	}
	want, err := sbCollect(func(fn func(string) bool) error {
		return g.Temporal.ForEachNodeEffectiveByLabels([]string{"A", "B"}, pin, func(s temporal.NodeSegment) bool { return fn(sbNodeSeg(s)) })
	})
	streamed := map[string]bool{}
	for _, s := range want {
		streamed[sbEntity(s)] = true
	}
	if err != nil || len(streamed) != 4 || streamed[fmt.Sprintf("n%d", int64(x5))] || !streamed[fmt.Sprintf("n%d", int64(x1))] || !streamed[fmt.Sprintf("n%d", int64(x2))] || !streamed[fmt.Sprintf("n%d", int64(x3))] || !streamed[fmt.Sprintf("n%d", int64(x4))] {
		t.Fatalf("plain scan: %v, %v; want the segments of x1..x4 and none of x5", want, err)
	}
	p := newGatherProbe(t, g)
	counter.calls, counter.ids = 0, 0
	for _, budget := range []storepkg.ReadBudget{{}, {Candidates: 6}, {Candidates: 5}, {Candidates: 1}} {
		bctx, meter := storepkg.WithReadBudget(ctx, budget)
		got, err := run(bctx)
		rbdBudgetErr(t, err, "unsupported")
		if len(got) != 0 || meter.Exceeded() == nil || meter.Work() != (storepkg.ReadWork{}) || p.listed != 0 || p.reads != 0 || counter.calls != 0 {
			t.Fatalf("budget %+v: %d callbacks, work %+v, %d listed, %d reads, %d native enum calls; want refusal before the union", budget, len(got), meter.Work(), p.listed, p.reads, counter.calls)
		}
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
			warmGatherMembership(t, g)
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
		warmGatherMembership(t, g)
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

// TestGather_PageCountsNativeCopies charges each shard snapshot even when a
// page filters to one entity. Its four sidecar copies and one entity read are
// separate native costs; compaction of duplicate IDs adds no Candidate charge.
func TestGather_PageCountsNativeCopies(t *testing.T) {
	g, ts, _ := gatherTiered(t)
	formerOnTwoShards(t, g, ts)
	warmGatherMembership(t, g)
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
	// Native counts: the entity reads the page began (one, the entity) and the
	// ever-members the shards' sidecars copied, the same in both runs: the
	// entity once for each of its two labels on each of its two shards (4).
	const wantCopied = 4
	copied := copiedCounter(t)
	reads := 0
	g.scanReadTestHook = func() { reads++ }
	defer func() { g.scanReadTestHook = nil }()
	before := copied()
	uctx, um := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
	want, err := page(uctx)
	n := copied() - before
	if err != nil || len(want) != 1 || um.Work().Candidates != wantCopied+1 || reads != 1 || n != wantCopied {
		t.Fatalf("unlimited: %v, %v, work %+v, %d read, %d copied; want one segment, 5 candidates, 1 read, %d copied", want, err, um.Work(), reads, n, wantCopied)
	}
	reads, before = 0, copied()
	ectx, em := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: wantCopied + 1})
	if got, err := page(ectx); err != nil || fmt.Sprint(got) != fmt.Sprint(want) || em.Work().Candidates != wantCopied+1 || reads != 1 || copied()-before != n {
		t.Fatalf("Candidates 5 (the control's count): %v, %v, work %+v, %d read, %d copied; want the control's page, 1 read and %d copied", got, err, em.Work(), reads, copied()-before, n)
	}
}

// TestGather_ReacquiredLabel counts native snapshot entries independently:
// a reacquired label may be repeated by both current and former-member lists.
// Memory repeats all four current IDs, while Badger/tiered/sharded repeat the two
// reacquired IDs. One less than that control refuses before the sidecar copy.
func TestGather_ReacquiredLabel(t *testing.T) {
	want := map[string]uint64{"badger": 6, "tiered": 6, "memory": 8, "sharded": 6}
	wantCopied := map[string]int64{"badger": 2, "tiered": 2, "memory": 4, "sharded": 2}
	for _, be := range gatherBackends() {
		t.Run(be.name, func(t *testing.T) {
			g, shut := be.open(t, t.TempDir())
			defer shut()
			ctx := context.Background()
			copied := copiedCounter(t)
			p := newGatherProbe(t, g)
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
			p.reset()
			before := copied()
			uctx, um := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			answer, err := run(uctx, pin)
			count, n := um.Work().Candidates, copied()-before
			if err != nil || len(answer) == 0 || count != want[be.name] || p.listed != 4 || n != wantCopied[be.name] || int64(count) != int64(p.listed)+n { // #nosec G115 -- a count
				t.Fatalf("unlimited: %v, %v, work %+v, %d listed, %d copied; want the stream, %d candidates, 4 listed, %d copied (candidates = listed + copied)", answer, err, um.Work(), p.listed, n, want[be.name], wantCopied[be.name])
			}
			ectx, em := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: count})
			if got, err := run(ectx, pin); err != nil || fmt.Sprint(got) != fmt.Sprint(answer) || em.Work().Candidates != count {
				t.Fatalf("Candidates %d (the control's count): %v, %v, work %+v; want the control's answer and count", count, got, err, em.Work())
			}
			p.reset()
			before = copied()
			lctx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: count - 1})
			if got, err := run(lctx, pin); len(got) != 0 || !errors.Is(err, storepkg.ErrReadBudget) || p.listed != 4 || copied() != before || p.reads != 0 {
				t.Fatalf("Candidates %d: %v, %v, %d listed, %d copied, %d read; want the refusal with no callback, 4 listed, nothing copied or read", count-1, got, err, p.listed, copied()-before, p.reads)
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
			wantCandidates := uint64(current + former)
			if be.name == "memory" {
				wantCandidates += current
			}
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
			warmGatherMembership(t, g)
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
			if err != nil || len(want) == 0 || um.Work().Candidates != wantCandidates {
				t.Fatalf("unlimited: %d segments, %v, work %+v; want the stream and %d candidates", len(want), err, um.Work(), wantCandidates)
			}
			before := copied()
			lctx, lm := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: wantCandidates - 1})
			got, err := run(lctx)
			rbdBudgetErr(t, err, "candidates")
			if len(got) != 0 || copied() != before {
				t.Fatalf("Candidates %d: %d segments, %d copied, work %+v; want the refusal with no callback and nothing copied", wantCandidates-1, len(got), copied()-before, lm.Work())
			}
			ectx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: wantCandidates})
			if got, err := run(ectx); err != nil || fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("Candidates %d: %d segments, %v; want the control's", wantCandidates, len(got), err)
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
// 12 entries the gather examined (5 current members, 7 former), plus the
// native as-of probes a row read needs. The total control count answers; a
// separate budget one below the gather count refuses before snapshot copies.
// Native counts beside the meter: the IDs listed, the ever-members the stores
// copied and, for the scan, the entities read. A store with the sidecar lists
// the 5 current members and a4 (b1..b6 are dropped past the pin, counted, not
// listed; the page drops a4 at After): 6, the page 5. Badger, tiered and
// sharded copy the 7 former members, memory its whole set (the 5 current
// members again: 12). Under one less the native hold refuses before its copy:
// nothing copied and the 5 current members listed. All sharded fixture IDs
// belong to one slot; the separate fanout test covers multiple owning slots.
func TestGather_BudgetIsTheControlCount(t *testing.T) {
	for _, be := range gatherBackends() {
		t.Run(be.name, func(t *testing.T) {
			g, shut := be.open(t, t.TempDir())
			defer shut()
			ctx := context.Background()
			copied := copiedCounter(t)
			p := newGatherProbe(t, g)
			gatherCandidates := uint64(12)
			if be.name == "memory" {
				gatherCandidates += 5
			}
			wantCopied := map[string]int64{"badger": 7, "tiered": 7, "memory": 12, "sharded": 7}[be.name]
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
			warmGatherMembership(t, g)
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
					wantListed := 6
					wantCandidates := gatherCandidates
					if name == "page" {
						wantListed--
					}
					if be.name == "badger" {
						switch name {
						case "ByLabel":
							// Six current probes, plus capture/probe of a4's
							// pending history key while presence is cold.
							wantCandidates += 6 + 2
						case "page":
							// Only a5 and c1 lie past After; neither has history.
							wantCandidates += 2
						}
					}
					p.reset()
					before := copied()
					uctx, um := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
					want, err := run(uctx)
					count := um.Work().Candidates
					if err != nil || len(want) == 0 || count != wantCandidates || p.listed != wantListed || copied()-before != wantCopied {
						t.Fatalf("unlimited: %v, %v, work %+v, %d listed, %d copied; want a non-empty answer, %d candidates, %d listed, %d copied", want, err, um.Work(), p.listed, copied()-before, wantCandidates, wantListed, wantCopied)
					}
					if name == "scan" && p.reads != p.listed {
						t.Fatalf("unlimited: %d entities read, %d listed; want each listed entity read once", p.reads, p.listed)
					}
					p.reset()
					before = copied()
					ectx, em := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: count})
					if got, err := run(ectx); err != nil || fmt.Sprint(got) != fmt.Sprint(want) || em.Work().Candidates != count || p.listed != wantListed || copied()-before != wantCopied {
						t.Fatalf("Candidates %d (the control's count): %v, %v, work %+v, %d listed, %d copied; want the control's answer, count, listing and copies", count, got, err, em.Work(), p.listed, copied()-before)
					}
					p.reset()
					before = copied()
					lctx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: gatherCandidates - 1})
					got, err := run(lctx)
					if len(got) != 0 || !errors.Is(err, storepkg.ErrReadBudget) || copied() != before || p.listed != 5 || p.reads != 0 {
						t.Fatalf("Candidates %d: %v, %v, %d listed, %d copied, %d read; want the refusal, nothing copied or read, the listing as the fixture says", gatherCandidates-1, got, err, p.listed, copied()-before, p.reads)
					}
					if count > gatherCandidates {
						p.reset()
						before = copied()
						lctx, lm := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: count - 1})
						got, err := run(lctx)
						if len(got) != 0 || !errors.Is(err, storepkg.ErrReadBudget) || lm.Work().Candidates != count || copied()-before != wantCopied || p.listed != wantListed {
							t.Fatalf("Candidates %d: %v, %v, work %+v, %d listed, %d copied; want refusal in the last as-of probe after the complete gather", count-1, got, err, lm.Work(), p.listed, copied()-before)
						}
					}
				})
			}
		})
	}
}

// A decline by an open shard happens before the property member walk. The
// fallback uses the admitted label/type gather: current members and copied
// ever-members count, and no property index entries are visited. The exact
// candidate allowance answers; one less refuses without a partial slice.
func TestTieredPropertyDeclineThroughTheDoor(t *testing.T) {
	g, ts, pin, ltok, rtok := dtWorld(t)
	// The hot shard (open, walked last) loses both indexes.
	hot := ts.HotShardForTest().Store()
	if err := hot.DropPropertyIndex(ltok, "k"); err != nil {
		t.Fatal(err)
	}
	if err := hot.DropRelPropertyIndex(rtok, "w"); err != nil {
		t.Fatal(err)
	}
	warmGatherMembership(t, g)
	visited, stop := badger.CountIndexEntriesVisitedForTest()
	defer stop()
	doors := dtDoors(g, pin)
	for _, name := range []string{"nodes", "rels"} {
		run := doors[name]
		t.Run(name, func(t *testing.T) {
			before := visited()
			uctx, um := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
			rows, err := run(uctx)
			examined := visited() - before
			count := um.Work().Candidates
			t.Logf("%s: control %d rows, work %+v, %d index entries visited", name, rows, um.Work(), examined)
			if err != nil || rows != dtRows || examined != dtVisited[name] || count != dtFallback[name] {
				t.Fatalf("%s control: %d rows, %v, %d visited, work %+v; want %d rows, %d entries visited (no member walk), %d candidates (the fallback alone)", name, rows, err, examined, um.Work(), dtRows, dtVisited[name], dtFallback[name])
			}
			dtBudgetAnswers(t, name, run, count)
		})
	}
}

// A closed cold shard reopens for the admitted fallback and its row reads.
// Another cold shard stays open without the property indexes, causing the
// decline. Every native cold open, including no-history probes, is charged.
func TestTieredPropertyDeclineOpenColdShard(t *testing.T) {
	g, ts, pin, ltok, rtok := dtWorld(t)
	var older []*tiered.EventShard
	ts.MuForTest().RLock()
	hot := ts.HotShardForTest()
	for _, es := range ts.EventShardsForTest() {
		if es != hot {
			older = append(older, es)
		}
	}
	ts.MuForTest().RUnlock()
	if len(older) != 2 {
		t.Fatalf("fixture: %d older event shards, want 2", len(older))
	}
	closed, open := older[0], older[1]
	for _, es := range older {
		demoteToCold(ts, es.Name())
	}
	open.LockShardMuForTest()
	openStore := open.Store()
	open.UnlockShardMuForTest()
	if openStore == nil {
		t.Fatal("fixture: the open cold shard has no store")
	}
	if err := openStore.DropPropertyIndex(ltok, "k"); err != nil {
		t.Fatal(err)
	}
	if err := openStore.DropRelPropertyIndex(rtok, "w"); err != nil {
		t.Fatal(err)
	}
	visited, stop := badger.CountIndexEntriesVisitedForTest()
	defer stop()
	doors := dtDoors(g, pin)
	for _, name := range []string{"nodes", "rels"} {
		run := doors[name]
		t.Run(name, func(t *testing.T) {
			// Warm the sidecars of retained handles before measuring a stable
			// candidate allowance. The closed shard is reopened on every read.
			warm, _ := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
			if rows, err := run(warm); err != nil || rows != dtRows {
				t.Fatalf("warm control: %d rows, %v; want %d", rows, err, dtRows)
			}
			closeEventShardStore(t, closed)
			before, opened := visited(), ts.ShardAccessStats().ColdOpens
			uctx, um := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
			rows, err := run(uctx)
			examined, opens := visited()-before, ts.ShardAccessStats().ColdOpens-opened
			count := um.Work().Candidates
			t.Logf("%s: control %d rows, work %+v, %d index entries visited, %d cold opens", name, rows, um.Work(), examined, opens)
			if err != nil || rows != dtRows || opens == 0 || um.Work().ShardOpens != opens {
				t.Fatalf("%s control: %d rows, %v, %d visited, %d cold opens, work %+v; want %d rows and every native cold open charged", name, rows, err, examined, opens, um.Work(), dtRows)
			}
			dtBudgetAnswers(t, name, run, count)
		})
	}
}

// dtRows is the rows each door of dtWorld answers.
const dtRows = 15

// dtBudgetAnswers checks that count, a control's Candidates, answers as the
// budget with dtRows rows and that one less refuses with no row.
func dtBudgetAnswers(t *testing.T, name string, run func(context.Context) (int, error), count uint64) {
	t.Helper()
	ectx, _ := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: count})
	if rows, err := run(ectx); err != nil || rows != dtRows {
		t.Fatalf("%s Candidates %d: %d rows, %v; want the control's", name, count, rows, err)
	}
	lctx, _ := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{Candidates: count - 1})
	if rows, err := run(lctx); rows != 0 || !errors.Is(err, storepkg.ErrReadBudget) {
		t.Fatalf("%s Candidates %d: %d rows, %v; want the refusal with no row", name, count-1, rows, err)
	}
}

// dtDoors are the property membership doors of dtWorld at pin.
func dtDoors(g *Core, pin types.Instant) map[string]func(ctx context.Context) (int, error) {
	return map[string]func(ctx context.Context) (int, error){
		"nodes": func(ctx context.Context) (int, error) {
			rows, err := g.Nodes.ByLabelAndPropertyCtx(ctx, "Sig", "k", int64(1), storepkg.QueryOpts{TxPin: pin})
			return len(rows), err
		},
		"rels": func(ctx context.Context) (int, error) {
			rows, err := g.Rels.ByTypeAndPropertyCtx(ctx, "ST", "w", int64(1), storepkg.QueryOpts{TxPin: pin})
			return len(rows), err
		},
	}
}

// dtWorld is a tiered graph on disk (node property index on disk) with three
// event shards, each holding 5 nodes of Sig with k=1 and 5 relationships of ST
// with w=1, both indexes on every shard, flushed; it returns the pin after the
// writes and the tokens of Sig and ST.
func dtWorld(t *testing.T) (*Core, *tiered.Store, types.Instant, uint16, uint16) {
	t.Helper()
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
	t.Cleanup(func() { _ = g.Close(); _ = ts.Close() })
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
	ltok, _ := g.labels.Lookup("Sig")
	rtok, _ := g.relTypes.Lookup("ST")
	if n := len(ts.EventShardsForTest()); n != shards {
		t.Fatalf("fixture: %d event shards, want %d", n, shards)
	}
	return g, ts, pin, ltok, rtok
}

// dtFallback is the count of the dtWorld decline reads (the fallback's), and
// dtVisited the disk index entries each visits (the fallback's equality
// lookups on the two shards that keep the index; no member walk).
var (
	dtFallback = map[string]uint64{"nodes": 15, "rels": 15}
	dtVisited  = map[string]int64{"nodes": 0, "rels": 0}
)
