package core

import (
	"context"
	"errors"
	"slices"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Sigma's group C: Temporal().RelsLowestAnsweredPinsCtx and its twin
// NodesLowestAnsweredPinsCtx under a read budget. The lowest answered pin reads
// the retention gate's rows: with a retention watermark set it reads each
// entity's current row for its labels or type and, for a deleted one, its
// history, so a budget bounds them (decoded bytes, history versions, tiered
// shard opens) as well as the candidates and the result buffer. Notes and red
// output: tasks/evidence/READBUDGET/sigma-doors-c.txt.

// scPinDoor is one lowest-answered-pin door over a fixed list of IDs, plain and
// under a context.
type scPinDoor struct {
	name, door string
	n          int
	plain      func() ([]types.Instant, error)
	ctx        func(ctx context.Context) ([]types.Instant, error)
}

// scPinDoors lists both doors over the nodes and relationships given: every ID,
// the first one again, and one the store does not hold.
func scPinDoors(g *Core, nodes []types.NodeID, rels []types.RelID) []scPinDoor {
	nodeIDs := append(slices.Clone(nodes), nodes[0], nodes[len(nodes)-1]+1<<30)
	relIDs := append(slices.Clone(rels), rels[0], rels[len(rels)-1]+1<<30)
	return []scPinDoor{
		{"nodes", "NodesLowestAnsweredPins", len(nodeIDs),
			func() ([]types.Instant, error) { return g.Temporal.NodesLowestAnsweredPins(nodeIDs) },
			func(ctx context.Context) ([]types.Instant, error) {
				return g.Temporal.NodesLowestAnsweredPinsCtx(ctx, nodeIDs)
			}},
		{"rels", "RelsLowestAnsweredPins", len(relIDs),
			func() ([]types.Instant, error) { return g.Temporal.RelsLowestAnsweredPins(relIDs) },
			func(ctx context.Context) ([]types.Instant, error) {
				return g.Temporal.RelsLowestAnsweredPinsCtx(ctx, relIDs)
			}},
	}
}

// scRetain sets a retention watermark on the scenario's label and relationship
// type, above every row written, so the lowest-answered-pin doors read each
// entity's labels or type; it returns the pins the doors answer.
func scRetain(t testing.TB, g *Core) (nodePin, relPin types.Instant) {
	t.Helper()
	pin := func() types.Instant {
		p, err := g.Temporal.NowTx()
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	nodePin = pin()
	tok, err := g.getOrCreateLabelPersisted(sbLabel)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.advanceRetentionWatermark(tok, nodePin); err != nil {
		t.Fatal(err)
	}
	relPin = pin()
	rtok, ok := g.relTypes.Lookup(sbType)
	if !ok {
		t.Fatalf("relationship type %q not registered", sbType)
	}
	if err := g.advanceRelRetentionWatermark(rtok, relPin); err != nil {
		t.Fatal(err)
	}
	return nodePin, relPin
}

// TestReadBudget_SigmaC_LowestAnsweredPins: both doors answer the plain door's
// aligned pins under a budget, or refuse with none, on badger, sharded and
// tiered, rows pending and flushed, with retention watermarks on the nodes'
// label and the relationships' type (twelve entities with three versions, every
// fourth deleted: a deleted entity's labels or type come from its history):
//   - the plain door reads each entity's row and each deleted one's history
//     (Core.historyRead) and no budget can stop it; without a budget and under
//     an unlimited one the answer is the plain door's, and the meter counts one
//     candidate and 8 buffer bytes per ID, the history versions of the deleted
//     entities and, cold, the decoded bytes of the rows read;
//   - a limit one under the ID count (candidates) or under 8 bytes per ID
//     (buffer) refuses with nothing read; a limit of 1 on history examined or
//     decoded bytes refuses within the first read; exactly the unlimited count
//     answers, one under refuses.
func TestReadBudget_SigmaC_LowestAnsweredPins(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			w := sbBuild(t, g, 12)
			wantNode, wantRel := scRetain(t, g)
			probe := func(t *testing.T, g *Core, flushed bool) {
				for _, d := range scPinDoors(g, w.nodes, w.rels) {
					t.Run(d.name, func(t *testing.T) {
						cold := func() {
							if flushed {
								rbdEvict(t, g, w.nodes, w.rels)
							}
						}
						ctx := context.Background()
						cold()
						historyRows := 0
						g.historyRead = func(n int) { historyRows += n }
						want, err := d.plain()
						g.historyRead = nil
						floor := wantNode
						if d.name == "rels" {
							floor = wantRel
						}
						if err != nil || len(want) != d.n || want[0] != floor || historyRows == 0 {
							t.Fatalf("plain = %v, %v, %d history rows read; want %d pins of %d and the deleted entities' history read", want, err, historyRows, d.n, floor)
						}
						cold()
						if got, err := d.ctx(ctx); err != nil || !slices.Equal(got, want) {
							t.Fatalf("no budget = %v, %v; want %v", got, err, want)
						}
						cold()
						fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
						if got, err := d.ctx(fctx); err != nil || !slices.Equal(got, want) {
							t.Fatalf("unlimited = %v, %v; want %v", got, err, want)
						}
						total := full.Work()
						if total.Candidates != uint64(d.n) || total.BufferBytes != uint64(8*d.n) || total.HistoryExamined == 0 || flushed && total.DecodedBytes == 0 || total.ShardOpens != 0 || total.Segments != 0 { // #nosec G115 -- counts
							t.Fatalf("unlimited work %+v; want %d candidates, %d buffer bytes, the history of the deleted entities and no shard opens", total, d.n, 8*d.n)
						}
						for _, c := range []struct {
							limit string
							n     uint64
						}{{"candidates", uint64(d.n) - 1}, {"buffer bytes", uint64(8*d.n) - 1}} { // #nosec G115 -- counts
							cold()
							pctx, meter := storepkg.WithReadBudget(ctx, sbBudget(c.limit, c.n))
							got, err := d.ctx(pctx)
							rb := rbdBudgetErr(t, err, c.limit)
							if got != nil || rb.Door != d.door || meter.Work() != rb.Work || meter.Work().HistoryExamined != 0 || meter.Work().DecodedBytes != 0 {
								t.Fatalf("%s %d: %v, door %q, work %+v; want no answer from %s and nothing read", c.limit, c.n, got, rb.Door, meter.Work(), d.door)
							}
						}
						for _, limit := range []string{"history examined", "decoded bytes"} {
							if sbWorkOf(total, limit) <= 1 {
								continue
							}
							t.Run(limit, func(t *testing.T) {
								cold()
								pctx, meter := storepkg.WithReadBudget(ctx, sbBudget(limit, 1))
								got, err := d.ctx(pctx)
								rb := rbdBudgetErr(t, err, limit)
								work := meter.Work()
								if got != nil || rb.Door != d.door || work != rb.Work {
									t.Fatalf("%s 1: %v, door %q, work %+v, meter %+v; want no answer from %s and the meter's work", limit, got, rb.Door, rb.Work, work, d.door)
								}
								if limit == "history examined" && work.HistoryExamined > 2 || limit == "decoded bytes" && work.DecodedBytes > total.DecodedBytes/2 {
									t.Fatalf("%s 1: refused after %+v (unlimited %+v); want the read stopped near the limit", limit, work, total)
								}
								t.Logf("%s 1: refused after %+v (unlimited %+v, the plain door read %d history rows)", limit, work, total, historyRows)
								cold()
								xctx, _ := storepkg.WithReadBudget(ctx, sbBudget(limit, sbWorkOf(total, limit)))
								if got, err := d.ctx(xctx); err != nil || !slices.Equal(got, want) {
									t.Fatalf("%s %d (the unlimited count) = %v, %v; want %v", limit, sbWorkOf(total, limit), got, err, want)
								}
								cold()
								xctx, _ = storepkg.WithReadBudget(ctx, sbBudget(limit, sbWorkOf(total, limit)-1))
								if got, err := d.ctx(xctx); got != nil || !isBudget(err, limit) {
									t.Fatalf("%s %d (one under) = %v, %v; want a refusal and no answer", limit, sbWorkOf(total, limit)-1, got, err)
								}
							})
						}
					})
				}
			}
			t.Run("pending", func(t *testing.T) { probe(t, g, false) })
			shut()
			g, shut = be.open(t, dir)
			defer shut()
			w.g = g
			t.Run("flushed", func(t *testing.T) { probe(t, g, true) })
		})
	}
}

// TestReadBudget_SigmaC_LowestAnsweredPinsGates: the call's own checks run
// first, a cancelled context before all of them; without retention watermarks
// the doors read no row and count only the candidates and the result buffer; an
// empty ID list answers an empty result.
func TestReadBudget_SigmaC_LowestAnsweredPinsGates(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, shut := be.open(t, t.TempDir())
			w := sbBuild(t, g, 8)
			ctx := context.Background()
			cctx, cancel := context.WithCancel(ctx)
			cancel()
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 100})
			for _, d := range scPinDoors(g, w.nodes, w.rels) {
				if _, err := d.ctx(cctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("%s: cancelled ctx = %v", d.name, err)
				}
			}
			if _, err := g.Temporal.RelsLowestAnsweredPinsCtx(bctx, []types.RelID{w.rels[0], 0}); err == nil || isBudget(err, "candidates") || meter.Work().Candidates != 0 {
				t.Fatalf("an invalid ID = %v, work %+v; want ErrInvalidStoreMutation and nothing counted", err, meter.Work())
			}
			if got, err := g.Temporal.RelsLowestAnsweredPinsCtx(bctx, nil); err != nil || got == nil || len(got) != 0 {
				t.Fatalf("no IDs = %v, %v; want an empty answer", got, err)
			}
			// No retention watermark: nothing to read, so the doors count only what they always did.
			fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			for _, d := range scPinDoors(g, w.nodes, w.rels) {
				want, err := d.plain()
				if err != nil {
					t.Fatal(err)
				}
				if got, err := d.ctx(fctx); err != nil || !slices.Equal(got, want) {
					t.Fatalf("%s: unlimited = %v, %v; want %v", d.name, got, err, want)
				}
			}
			ids := uint64(len(w.nodes) + 2 + len(w.rels) + 2) // #nosec G115 -- counts
			if total := full.Work(); total != (storepkg.ReadWork{Candidates: ids, BufferBytes: 8 * ids}) {
				t.Fatalf("unlimited work %+v; want %d candidates and %d buffer bytes only (no watermark, no row read)", total, ids, 8*ids)
			}
			shut()
			if _, err := g.Temporal.RelsLowestAnsweredPinsCtx(bctx, []types.RelID{w.rels[0]}); !errors.Is(err, ErrGraphClosed) {
				t.Fatalf("closed graph = %v; want ErrGraphClosed", err)
			}
		})
	}
}

// TestReadBudget_SigmaC_LowestAnsweredPinsShardOpens: on a tiered store whose
// cold shards are closed before each read, with retention watermarks set, the
// plain doors open the cold shards of the entities they read (a live entity's
// own shard; a deleted one's history on every shard); under ShardOpens 1 each
// Ctx door opens at most one and refuses with no answer; unlimited the answer is
// the plain door's and the meter counts exactly the opens.
func TestReadBudget_SigmaC_LowestAnsweredPinsShardOpens(t *testing.T) {
	t.Parallel()
	w := sbBuildTiered(t)
	scRetain(t, w.g)
	ctx := context.Background()
	// Nodes and relationships: the live one of each shard, then one deleted in the first.
	nodes := []types.NodeID{w.nodes[0], w.nodes[2], w.nodes[4], w.nodes[1]}
	rels := []types.RelID{w.rels[0], w.rels[2], w.rels[4], w.rels[1]}
	for _, d := range scPinDoors(w.g, nodes, rels) {
		t.Run(d.name, func(t *testing.T) {
			w.closeCold(t)
			opens := w.opens()
			want, err := d.plain()
			plainOpens := w.opens() - opens
			if err != nil || len(want) != d.n || plainOpens < 3 {
				t.Fatalf("plain = %v, %v, %d shards opened; want the pins and at least 3 opens", want, err, plainOpens)
			}

			w.closeCold(t)
			opens = w.opens()
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{ShardOpens: 1})
			got, err := d.ctx(bctx)
			boundedOpened := w.opens() - opens
			rb := rbdBudgetErr(t, err, "shard opens")
			if got != nil || rb.Door != d.door || meter.Work() != rb.Work || boundedOpened > 1 {
				t.Fatalf("ShardOpens 1 = %v, door %q, work %+v, %d shards opened; want no answer, door %q and at most 1 open", got, rb.Door, meter.Work(), boundedOpened, d.door)
			}

			w.closeCold(t)
			opens = w.opens()
			bctx, meter = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			got, err = d.ctx(bctx)
			opened := w.opens() - opens
			if err != nil || !slices.Equal(got, want) || meter.Work().ShardOpens != opened {
				t.Fatalf("unlimited = %v, %v, %d shards opened, %d counted; want the plain pins and every open counted", got, err, opened, meter.Work().ShardOpens)
			}
			t.Logf("%s: the plain read opened %d cold shards; under ShardOpens 1 the read opened %d and refused; unlimited it opened %d and counted %d", d.name, plainOpens, boundedOpened, opened, meter.Work().ShardOpens)
		})
	}
}

// TestReadBudget_SigmaC_LowestAnsweredPinsMemory: the memory store has no read
// snapshot and decodes nothing: with retention watermarks the doors count the
// versions of each deleted entity's chain when they read it (all at once) and no
// decoded bytes; a history limit refuses within the first chain read, exactly
// the unlimited count answers.
func TestReadBudget_SigmaC_LowestAnsweredPinsMemory(t *testing.T) {
	t.Parallel()
	g := txbBackends()[0].open(t, false)
	w := sbBuild(t, g, 12)
	scRetain(t, g)
	ctx := context.Background()
	for _, d := range scPinDoors(g, w.nodes, w.rels) {
		t.Run(d.name, func(t *testing.T) {
			want, err := d.plain()
			if err != nil || len(want) != d.n || want[0] == 0 {
				t.Fatalf("plain = %v, %v; want %d non-zero pins", want, err, d.n)
			}
			fctx, full := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			if got, err := d.ctx(fctx); err != nil || !slices.Equal(got, want) {
				t.Fatalf("unlimited = %v, %v; want %v", got, err, want)
			}
			total := full.Work()
			if total.Candidates != uint64(d.n) || total.BufferBytes != uint64(8*d.n) || total.HistoryExamined == 0 || total.DecodedBytes != 0 || total.ShardOpens != 0 { // #nosec G115 -- counts
				t.Fatalf("unlimited work %+v; want %d candidates, %d buffer bytes, the deleted entities' versions and no decoded bytes", total, d.n, 8*d.n)
			}
			pctx, meter := storepkg.WithReadBudget(ctx, sbBudget("history examined", 1))
			got, err := d.ctx(pctx)
			rb := rbdBudgetErr(t, err, "history examined")
			if got != nil || rb.Door != d.door || meter.Work() != rb.Work || meter.Work().HistoryExamined > total.HistoryExamined/2 {
				t.Fatalf("history 1: %v, door %q, work %+v (unlimited %+v); want no answer from %s within the first chain", got, rb.Door, meter.Work(), total, d.door)
			}
			xctx, _ := storepkg.WithReadBudget(ctx, sbBudget("history examined", total.HistoryExamined))
			if got, err := d.ctx(xctx); err != nil || !slices.Equal(got, want) {
				t.Fatalf("history %d (the unlimited count) = %v, %v; want %v", total.HistoryExamined, got, err, want)
			}
			xctx, _ = storepkg.WithReadBudget(ctx, sbBudget("history examined", total.HistoryExamined-1))
			if got, err := d.ctx(xctx); got != nil || !isBudget(err, "history examined") {
				t.Fatalf("history %d (one under) = %v, %v; want a refusal and no answer", total.HistoryExamined-1, got, err)
			}
		})
	}
}
