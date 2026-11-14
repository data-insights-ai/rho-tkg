package core

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	shardedpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R2-5 HEADREAD, fix round 2 (design tasks/evidence/R2CUMUL/design-headread2.txt
// § 2): a read budget's refusal ends the read at the site that sees it (M1),
// memory's counts pinned against badger's (M2), and the native counters kept
// by the stores (C1).

// hr3Native is the history work the stores holding the nodes and rels did,
// kept by the stores and not by the meter: skeletons decoded (badger family)
// or listed (memory), and rows decoded or copied out of Badger (badger
// family) or handed out (memory).
type hr3Native struct{ metas, rows int64 }

func (a hr3Native) sub(b hr3Native) hr3Native { return hr3Native{a.metas - b.metas, a.rows - b.rows} }

func hr3Count(t testing.TB, g *Core, nodes []types.NodeID, rels []types.RelID) hr3Native {
	t.Helper()
	if ms, ok := g.store.(*memory.Store); ok {
		return hr3Native{ms.HistoryMetasListedForTest(), ms.HistoryRowsReadForTest()}
	}
	seen := map[*badger.Store]bool{}
	var out hr3Native
	add := func(bs *badger.Store, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		if bs == nil || seen[bs] {
			return
		}
		seen[bs] = true
		out.metas += bs.HistoryMetaDecodesForTest()
		out.rows += bs.HistoryRowDecodesForTest() + bs.HistoryRowCopiesForTest()
	}
	switch st := g.store.(type) {
	case *badger.Store:
		add(st, nil)
	case *shardedpkg.Store:
		for _, id := range nodes {
			add(st.ShardForIDForTest(id.SnowflakeID()))
		}
		for _, id := range rels {
			add(st.ShardForIDForTest(id.SnowflakeID()))
		}
	case *tiered.Store:
		for _, id := range nodes {
			add(st.ShardForNodeIDForTest(id))
		}
		for _, id := range rels {
			add(st.ShardForRelIDForTest(id))
		}
	default:
		t.Fatalf("no native counter for %T", g.store)
	}
	return out
}

// hr3At is one point at-door, plain and under ctx, rendered.
type hr3At struct {
	name  string
	node  bool
	plain func() (string, error)
	ctx   func(context.Context) (string, error)
}

func hr3Row(n *types.Node, err error) (string, error) {
	if err != nil || n == nil {
		return "", err
	}
	return fmt.Sprintf("n%d v%d %v", n.ID(), n.Version(), n.PropertiesMap()), nil
}

func hr3RelRow(r *types.Relationship, err error) (string, error) {
	if err != nil || r == nil {
		return "", err
	}
	return fmt.Sprintf("r%d v%d %v", r.ID(), r.Version(), r.PropertiesMap()), nil
}

// hr3AtDoors are NodeAtCtx and RelAtCtx at valid instant at.
func hr3AtDoors(g *Core, node types.NodeID, rel types.RelID, at types.Instant) []hr3At {
	tp := g.Temporal
	return []hr3At{
		{"NodeAt", true,
			func() (string, error) { return hr3Row(tp.NodeAt(node, at)) },
			func(c context.Context) (string, error) { return hr3Row(tp.NodeAtCtx(c, node, at)) }},
		{"RelAt", false,
			func() (string, error) { return hr3RelRow(tp.RelAt(rel, at)) },
			func(c context.Context) (string, error) { return hr3RelRow(tp.RelAtCtx(c, rel, at)) }},
	}
}

// TestBudgetedAt_RefusalEndsTheRead (design M1): on the stores whose fold
// lists skeletons (memory, badger, tiered; sharded has no skeleton list and
// reads the chain whole), rows pending and flushed, an 80-version node and
// relationship (N = 79 history rows) read at a valid instant whose winner is
// a history row:
//   - unlimited: the plain door's row; HistoryExamined N + 1 = 80; natively
//     79 skeletons and 1 row;
//   - HistoryExamined 79: refused at the winner, no row; 80 counted (the
//     refused winner's charge); natively the 79 skeletons and no row: no
//     full-chain fallback runs past the refusal (it copied the 79 rows, and on
//     memory counted them again: 159);
//   - two such nodes through NodesAtTxByIDsCtx under 81 (each fits at 80, the
//     two do not): refused, nothing returned; the second node is refused in its
//     skeleton list (badger family: at its second skeleton, 82 counted, 80
//     decoded; memory: its list counted whole after it was listed, 159 and
//     158: the chain copy before the charge, "Not yet bounded").
func TestBudgetedAt_RefusalEndsTheRead(t *testing.T) {
	t.Parallel()
	type world struct {
		nodes []types.NodeID
		rels  []types.RelID
	}
	hr2Phases(t, func(t *testing.T, g *Core) any {
		w := &world{}
		for range 2 {
			w.nodes = append(w.nodes, hrNode(t, g, 80))
			w.rels = append(w.rels, hrRel(t, g, 80))
		}
		return w
	}, func(t *testing.T, be hr2Backend, g *Core, wa any, flushed bool) {
		if !hr2Skeletons(be) {
			t.Skip("no skeleton list: the at-doors read the chain whole")
		}
		w := wa.(*world)
		evict := func() {
			if flushed {
				rbdEvict(t, g, w.nodes, w.rels)
			}
		}
		native := func() hr3Native { return hr3Count(t, g, w.nodes, w.rels) }
		for _, d := range hr3AtDoors(g, w.nodes[0], w.rels[0], 3500) {
			t.Run(d.name, func(t *testing.T) {
				want, err := d.plain()
				if err != nil || want == "" {
					t.Fatalf("plain = %q, %v", want, err)
				}
				run := func(b storepkg.ReadBudget) (string, error, storepkg.ReadWork, hr3Native) {
					evict()
					ctx, meter := storepkg.WithReadBudget(context.Background(), b)
					n0 := native()
					got, err := d.ctx(ctx)
					return got, err, meter.Work(), native().sub(n0)
				}
				got, err, work, nat := run(storepkg.ReadBudget{})
				t.Logf("unlimited: HistoryExamined %d, DecodedBytes %d, native %+v", work.HistoryExamined, work.DecodedBytes, nat)
				if err != nil || got != want || work.HistoryExamined != 80 || nat != (hr3Native{79, 1}) {
					t.Fatalf("unlimited = %q, %v, HistoryExamined %d, native %+v; want the plain row %q, 80, {79 1}", got, err, work.HistoryExamined, nat, want)
				}
				got, err, work, nat = run(storepkg.ReadBudget{HistoryExamined: 79})
				t.Logf("limit 79: %v; HistoryExamined %d, native %+v", err, work.HistoryExamined, nat)
				if !isBudget(err, "history examined") || got != "" || work.HistoryExamined != 80 || nat != (hr3Native{79, 0}) {
					t.Fatalf("limit 79 = %q, %v, HistoryExamined %d, native %+v; want a refusal at the winner: 80 counted, {79 0} read", got, err, work.HistoryExamined, nat)
				}
			})
		}
		t.Run("NodesAtTxByIDs", func(t *testing.T) {
			pin := g.peekNow()
			evict()
			ctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{HistoryExamined: 81})
			n0 := native()
			got, err := g.Temporal.NodesAtTxByIDsCtx(ctx, w.nodes, 3500, pin)
			work, nat := meter.Work(), native().sub(n0)
			t.Logf("two nodes under 81: %v; HistoryExamined %d, native %+v", err, work.HistoryExamined, nat)
			wantWork, wantNat := uint64(82), hr3Native{80, 1}
			if be.name == "memory" {
				wantWork, wantNat = 159, hr3Native{158, 1}
			}
			if !isBudget(err, "history examined") || got != nil || work.HistoryExamined != wantWork || nat != wantNat {
				t.Fatalf("two nodes under 81 = %v, %v, HistoryExamined %d, native %+v; want a refusal, nothing returned, %d and %+v",
					got, err, work.HistoryExamined, nat, wantWork, wantNat)
			}
		})
	})
}

// TestBudgetedAt_HelperReturnsRefusal (design M1, at the helper that owns
// it): nodeAtViaTemporalMeta and relAtViaTemporalMeta, called with a metered
// read whose HistoryExamined budget refuses the winner, return (nil, handled,
// the refusal), so their callers never fall back to the full chain.
func TestBudgetedAt_HelperReturnsRefusal(t *testing.T) {
	t.Parallel()
	for _, be := range hr2Backends() {
		if !hr2Skeletons(be) {
			continue
		}
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, shut := be.open(t, t.TempDir())
			defer shut()
			node, rel := hrNode(t, g, 10), hrRel(t, g, 10)
			_ = g.readUnderRLock(func() error {
				_, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{HistoryExamined: 9})
				rd, release := g.meteredEntityReads(meter.Tally("helper"))
				defer release()
				cur, err := rd.currentNode(node)
				if err != nil {
					t.Fatal(err)
				}
				n, handled, err := g.nodeAtViaTemporalMeta(rd, node, cur, 3500, 0)
				if n != nil || !handled || !isBudget(err, "history examined") {
					t.Errorf("nodeAtViaTemporalMeta = %v, handled %v, %v; want nil, true and the refusal", n, handled, err)
				}
				_, meter2 := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{HistoryExamined: 9})
				rd2, release2 := g.meteredEntityReads(meter2.Tally("helper"))
				defer release2()
				rcur, err := rd2.currentRel(rel)
				if err != nil {
					t.Fatal(err)
				}
				r, handled, err := g.relAtViaTemporalMeta(rd2, rel, rcur, 3500, 0)
				if r != nil || !handled || !isBudget(err, "history examined") {
					t.Errorf("relAtViaTemporalMeta = %v, handled %v, %v; want nil, true and the refusal", r, handled, err)
				}
				return nil
			})
		})
	}
}

// hr3TieredAtWorld is a tiered graph (MaxOpenColdShards maxCold; 0: every
// read reopens a cold shard) holding a node and a relationship of versions
// rows shaped as hrNode / hrRel (the relationship between two event nodes) on
// one event shard, demoted to cold.
type hr3TieredAtWorld struct {
	g    *Core
	ts   *tiered.Store
	node types.NodeID
	rel  types.RelID
	cold []*tiered.EventShard
}

func hr3BuildTieredAt(t *testing.T, versions, maxCold int) *hr3TieredAtWorld {
	t.Helper()
	ts, err := tiered.New(tiered.Config{
		DataDir:           t.TempDir(),
		RefLabels:         []string{"Ref"},
		ShardWindow:       7 * 24 * time.Hour,
		FlushInterval:     1<<63 - 1,
		MaxOpenColdShards: maxCold,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := New(Config{Store: ts, UntimedValidStart: UntimedFromTxFrom})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close(); _ = ts.Close() })
	ctx := context.Background()
	w := &hr3TieredAtWorld{g: g, ts: ts, node: hrNode(t, g, versions)}
	a, err := g.Nodes.Add(ctx, []string{sbLabel}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(ctx, []string{sbLabel}, nil)
	if err != nil {
		t.Fatal(err)
	}
	p := r10Props(0)
	p["tkg_valid_from"], p["tkg_valid_to"] = int64(1000), int64(2000)
	r, err := g.Rels.AddByID(ctx, "LINK", a.ID(), b.ID(), p)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < versions; i++ {
		from, to := types.Instant(3000), types.Instant(4000)
		if i == 1 {
			from, to = 2000, 3000
		}
		if _, err := g.Temporal.SetRelVersionInterval(ctx, r.ID(), from, to, r10Props(i)); err != nil {
			t.Fatal(err)
		}
	}
	w.rel = r.ID()
	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatal(err)
	}
	ts.MuForTest().RLock()
	hot := ts.HotShardForTest()
	for _, es := range ts.EventShardsForTest() {
		if es != hot {
			w.cold = append(w.cold, es)
		}
	}
	ts.MuForTest().RUnlock()
	for _, es := range w.cold {
		demoteToCold(ts, es.Name())
	}
	return w
}

// TestBudgetedAt_TieredRefusalOpensNoMore (design M1, tiered): the at-doors
// over an entity on a closed cold shard (MaxOpenColdShards 0: every read
// reopens it), a DecodedBytes limit one below the unlimited read's: refused,
// and the shards opened equal both the unlimited read's opens and the
// ShardOpens counted: no full-chain fallback opens the shard again.
func TestBudgetedAt_TieredRefusalOpensNoMore(t *testing.T) {
	t.Parallel()
	w := hr3BuildTieredAt(t, 10, 0)
	closeCold := func() {
		for _, es := range w.cold {
			closeEventShardStore(t, es)
		}
	}
	opens := func() uint64 { return w.ts.ShardAccessStats().ColdOpens }
	for _, d := range hr3AtDoors(w.g, w.node, w.rel, 3500) {
		t.Run(d.name, func(t *testing.T) {
			closeCold()
			want, err := d.plain()
			if err != nil || want == "" {
				t.Fatalf("plain = %q, %v", want, err)
			}
			closeCold()
			o := opens()
			ctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
			got, err := d.ctx(ctx)
			opened, work := opens()-o, meter.Work()
			if err != nil || got != want || opened == 0 || work.ShardOpens != opened || work.DecodedBytes == 0 {
				t.Fatalf("unlimited = %q, %v, %d opened, work %+v; want the plain row %q, every open counted", got, err, opened, work, want)
			}
			closeCold()
			o = opens()
			ctx, meter = storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{DecodedBytes: work.DecodedBytes - 1})
			got, err = d.ctx(ctx)
			bounded := opens() - o
			t.Logf("unlimited: %d opened, %d decoded bytes; limit %d: %v, %d opened, %d counted", opened, work.DecodedBytes, work.DecodedBytes-1, err, bounded, meter.Work().ShardOpens)
			if !isBudget(err, "decoded bytes") || got != "" || bounded != opened || meter.Work().ShardOpens != bounded {
				t.Fatalf("limit %d = %q, %v, %d opened, %d counted; want a refusal with the unlimited read's %d opens, each counted",
					work.DecodedBytes-1, got, err, bounded, meter.Work().ShardOpens, opened)
			}
		})
	}
}

// hr3Door is one door of the M2 table: plain and under ctx, rendered, with
// the HistoryExamined and native figures memory and badger both give.
type hr3Door struct {
	name   string
	plain  func() (string, error)
	ctx    func(context.Context) (string, error)
	work   uint64
	native hr3Native
}

func hr3Nodes(ns []*types.Node, err error) (string, error) {
	s := ""
	for _, n := range ns {
		r, _ := hr3Row(n, nil)
		s += r + " | "
	}
	return s, err
}

func hr3Rels(rs []*types.Relationship, err error) (string, error) {
	s := ""
	for _, r := range rs {
		x, _ := hr3RelRow(r, nil)
		s += x + " | "
	}
	return s, err
}

// hr3M2Doors are the doors design M2 pins for nodes and rels of versions rows
// (hrNode / hrRel: N = versions-1 history rows, W = 2 history winners; the
// winner at 3500 is history version N, the current row is version 0 at
// [1000, 2000)): a point at-door reads the N skeletons and its one winner
// (N + 1), or the N skeletons alone when the current row wins (N); a by-IDs
// at-door the sum over its IDs; a by-IDs timeline scan and a label or type
// scan (the paged scans' walk) the N skeletons and the W winners of each
// entity (N + W).
func hr3M2Doors(g *Core, nodes []types.NodeID, rels []types.RelID, versions int, pin types.Instant) []hr3Door {
	tp := g.Temporal
	n := uint64(versions - 1) // #nosec G115 -- a test size
	nn := int64(versions - 1) // #nosec G115 -- a test size
	at := func(k uint64) uint64 { return k * (n + 1) }
	scan := func(k uint64) uint64 { return k * (n + 2) }
	atN := func(k int64) hr3Native { return hr3Native{k * nn, k} }
	scanN := func(k int64) hr3Native { return hr3Native{k * nn, 2 * k} }
	nodeSegs := func(read func(func(temporal.NodeSegment) bool) error) (string, error) {
		var segs []temporal.NodeSegment
		err := read(func(s temporal.NodeSegment) bool { segs = append(segs, s); return true })
		return sbSegs(segs), err
	}
	relSegs := func(read func(func(temporal.RelSegment) bool) error) (string, error) {
		var segs []temporal.RelSegment
		err := read(func(s temporal.RelSegment) bool { segs = append(segs, s); return true })
		return sbRelSegs(segs), err
	}
	opts := storepkg.QueryOpts{ValidAt: 3500, TxAt: pin, TxAtSet: true}
	return []hr3Door{
		{"NodeAt", func() (string, error) { return hr3Row(tp.NodeAt(nodes[0], 3500)) },
			func(c context.Context) (string, error) { return hr3Row(tp.NodeAtCtx(c, nodes[0], 3500)) }, at(1), atN(1)},
		{"NodeAtTx", func() (string, error) { return hr3Row(tp.NodeAtTx(nodes[0], 3500, pin)) },
			func(c context.Context) (string, error) { return hr3Row(tp.NodeAtTxCtx(c, nodes[0], 3500, pin)) }, at(1), atN(1)},
		{"NodeAt current", func() (string, error) { return hr3Row(tp.NodeAt(nodes[0], 1500)) },
			func(c context.Context) (string, error) { return hr3Row(tp.NodeAtCtx(c, nodes[0], 1500)) }, n, hr3Native{nn, 0}},
		{"RelAt", func() (string, error) { return hr3RelRow(tp.RelAt(rels[0], 3500)) },
			func(c context.Context) (string, error) { return hr3RelRow(tp.RelAtCtx(c, rels[0], 3500)) }, at(1), atN(1)},
		{"RelAtTx", func() (string, error) { return hr3RelRow(tp.RelAtTx(rels[0], 3500, pin)) },
			func(c context.Context) (string, error) { return hr3RelRow(tp.RelAtTxCtx(c, rels[0], 3500, pin)) }, at(1), atN(1)},
		{"NodesAtTxByIDs", func() (string, error) { return hr3Nodes(tp.NodesAtTxByIDs(nodes, 3500, pin)) },
			func(c context.Context) (string, error) { return hr3Nodes(tp.NodesAtTxByIDsCtx(c, nodes, 3500, pin)) }, at(2), atN(2)},
		{"NodesAtOptsByIDs", func() (string, error) { return hr3Nodes(tp.NodesAtOptsByIDs(nodes, opts)) },
			func(c context.Context) (string, error) { return hr3Nodes(tp.NodesAtOptsByIDsCtx(c, nodes, opts)) }, at(2), atN(2)},
		{"RelsAtTxByIDs", func() (string, error) { return hr3Rels(tp.RelsAtTxByIDs(rels, 3500, pin)) },
			func(c context.Context) (string, error) { return hr3Rels(tp.RelsAtTxByIDsCtx(c, rels, 3500, pin)) }, at(2), atN(2)},
		{"NodesEffectiveByIDs", func() (string, error) {
			return nodeSegs(func(fn func(temporal.NodeSegment) bool) error { return tp.NodesEffectiveByIDs(nodes, pin, fn) })
		}, func(c context.Context) (string, error) {
			return nodeSegs(func(fn func(temporal.NodeSegment) bool) error { return tp.NodesEffectiveByIDsCtx(c, nodes, pin, fn) })
		}, scan(2), scanN(2)},
		{"RelsEffectiveByIDs", func() (string, error) {
			return relSegs(func(fn func(temporal.RelSegment) bool) error { return tp.RelsEffectiveByIDs(rels, pin, fn) })
		}, func(c context.Context) (string, error) {
			return relSegs(func(fn func(temporal.RelSegment) bool) error { return tp.RelsEffectiveByIDsCtx(c, rels, pin, fn) })
		}, scan(2), scanN(2)},
		{"NodeEffectiveByLabel", func() (string, error) {
			return nodeSegs(func(fn func(temporal.NodeSegment) bool) error {
				return tp.ForEachNodeEffectiveByLabel("Derived", pin, fn)
			})
		}, func(c context.Context) (string, error) {
			return nodeSegs(func(fn func(temporal.NodeSegment) bool) error {
				return tp.ForEachNodeEffectiveByLabelCtx(c, "Derived", pin, fn)
			})
		}, scan(2), scanN(2)},
		{"RelEffectiveByType", func() (string, error) {
			return relSegs(func(fn func(temporal.RelSegment) bool) error { return tp.ForEachRelEffectiveByType("LINK", pin, fn) })
		}, func(c context.Context) (string, error) {
			return relSegs(func(fn func(temporal.RelSegment) bool) error {
				return tp.ForEachRelEffectiveByTypeCtx(c, "LINK", pin, fn)
			})
		}, scan(2), scanN(2)},
	}
}

// TestBudgetedMemory_CountsEachVersion (design M2): memory and badger (rows
// pending and flushed), the same data, two nodes and two relationships of 10
// and of 80 versions each in their own graph: every door of hr3M2Doors gives
// the plain door's answer unlimited, counts its formula's HistoryExamined on
// both stores (memory counts each version it reads on its own, as the disk
// stores do: N + 1 for a point at-door whose winner is history, was N; N + W
// a scanned entity, was N), and natively reads the skeletons and rows the
// formula names; one below the count, it refuses.
func TestBudgetedMemory_CountsEachVersion(t *testing.T) {
	t.Parallel()
	for _, versions := range []int{10, 80} {
		t.Run(fmt.Sprint(versions), func(t *testing.T) {
			t.Parallel()
			type world struct {
				nodes []types.NodeID
				rels  []types.RelID
				pin   types.Instant
			}
			hr2Phases(t, func(t *testing.T, g *Core) any {
				w := &world{}
				for range 2 {
					w.nodes = append(w.nodes, hrNode(t, g, versions))
					w.rels = append(w.rels, hrRel(t, g, versions))
				}
				w.pin = g.peekNow()
				return w
			}, func(t *testing.T, be hr2Backend, g *Core, wa any, flushed bool) {
				if be.name != "memory" && be.name != "badger" {
					t.Skip("memory and badger side by side")
				}
				w := wa.(*world)
				for _, d := range hr3M2Doors(g, w.nodes, w.rels, versions, w.pin) {
					t.Run(d.name, func(t *testing.T) {
						want, err := d.plain()
						if err != nil || want == "" {
							t.Fatalf("plain = %q, %v", want, err)
						}
						run := func(b storepkg.ReadBudget) (string, error, uint64, hr3Native) {
							if flushed {
								rbdEvict(t, g, w.nodes, w.rels)
							}
							ctx, meter := storepkg.WithReadBudget(context.Background(), b)
							n0 := hr3Count(t, g, w.nodes, w.rels)
							got, err := d.ctx(ctx)
							return got, err, meter.Work().HistoryExamined, hr3Count(t, g, w.nodes, w.rels).sub(n0)
						}
						got, err, he, nat := run(storepkg.ReadBudget{})
						t.Logf("unlimited: HistoryExamined %d, native %+v", he, nat)
						if err != nil || got != want || he != d.work || nat != d.native {
							t.Fatalf("unlimited = %q, %v, HistoryExamined %d, native %+v; want the plain answer %q, %d, %+v", got, err, he, nat, want, d.work, d.native)
						}
						if d.work == 0 {
							return
						}
						got, err, he, nat = run(storepkg.ReadBudget{HistoryExamined: d.work - 1})
						t.Logf("limit %d: %v; HistoryExamined %d, native %+v", d.work-1, err, he, nat)
						if !isBudget(err, "history examined") {
							t.Fatalf("limit %d = %q, %v; want the refusal", d.work-1, got, err)
						}
					})
				}
			})
		})
	}
}

// TestBudgetedTimeline_ProbeOnlyHeadHit (design, the probe fix, at the
// public caller): a badger store opened with HistoryPresenceProbeOnly (as
// tiered opens its cold shards), rows pending and flushed, a node and a
// relationship of 10 and of 80 versions: after the plain door filled the head
// entry, each head door under an unlimited budget gives the plain answer from
// the entry and counts the two winners only (HistoryExamined 2, 2 rows read,
// no skeleton): the presence read before it decodes nothing. It counted
// N + W (11 and 81): the snapshot's probe decoded every skeleton.
func TestBudgetedTimeline_ProbeOnlyHeadHit(t *testing.T) {
	t.Parallel()
	bs, err := badger.New(badger.Config{InMemory: true, HistoryPresenceProbeOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(Config{Store: bs, UntimedValidStart: UntimedFromTxFrom})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close(); _ = bs.Close() })
	sizes := []int{10, 80}
	nodes, rels := map[int]types.NodeID{}, map[int]types.RelID{}
	for _, n := range sizes {
		nodes[n], rels[n] = hrNode(t, g, n), hrRel(t, g, n)
	}
	for _, phase := range []string{"pending", "flushed"} {
		if phase == "flushed" {
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		pin := g.peekNow()
		for _, n := range sizes {
			ids, rids := []types.NodeID{nodes[n]}, []types.RelID{rels[n]}
			for _, d := range hrDoors(g, nodes[n], rels[n], pin) {
				if !d.head {
					continue
				}
				t.Run(fmt.Sprintf("%s/%d/%s", phase, n, d.name), func(t *testing.T) {
					if phase == "flushed" {
						rbdEvict(t, g, ids, rids)
					}
					want, err := d.plain() // fills the head entry
					if err != nil || want == "" {
						t.Fatalf("plain = %q, %v", want, err)
					}
					if phase == "flushed" {
						rbdEvict(t, g, ids, rids)
					}
					hits, n0 := g.headHits.Load(), hr3Count(t, g, ids, rids)
					ctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
					got, err := d.ctx(ctx)
					he, nat := meter.Work().HistoryExamined, hr3Count(t, g, ids, rids).sub(n0)
					t.Logf("HistoryExamined %d, native %+v, head hits %d", he, nat, g.headHits.Load()-hits)
					if err != nil || got != want || he != 2 || nat != (hr3Native{0, 2}) || g.headHits.Load()-hits != 1 {
						t.Fatalf("unlimited = %q, %v, HistoryExamined %d, native %+v, %d head hits; want the plain answer %q from the entry: 2, {0 2}, 1",
							got, err, he, nat, g.headHits.Load()-hits, want)
					}
				})
			}
		}
	}
}

// TestTieredSnapshotPresence_ReadsKeysOnly (design, "tiered meteredShard has
// no HasNodeHistory: keep", pinned): a node and a relationship of 10 and of
// 80 versions on a closed cold shard (MaxOpenColdShards 1: the shard the read
// opens stays open to be counted): a metered tiered snapshot's HasNodeHistory
// and HasRelHistory answer true, charge no history (HistoryExamined 0,
// DecodedBytes 0), count each shard open (ShardOpens equals the ColdOpens
// delta, 1) and decode nothing on the shard they opened (its skeleton and row
// decode counters 0): the cold shard answers by its key probe.
func TestTieredSnapshotPresence_ReadsKeysOnly(t *testing.T) {
	t.Parallel()
	for _, versions := range []int{10, 80} {
		t.Run(fmt.Sprint(versions), func(t *testing.T) {
			t.Parallel()
			w := hr3BuildTieredAt(t, versions, 1)
			if len(w.cold) != 1 {
				t.Fatalf("fixture: %d cold shards; want 1", len(w.cold))
			}
			es := w.cold[0]
			for _, rel := range []bool{false, true} {
				t.Run(fmt.Sprintf("rel=%v", rel), func(t *testing.T) {
					closeEventShardStore(t, es)
					snap := w.ts.NewReadSnapshot()
					defer snap.Close()
					_, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
					snap.(storepkg.ReadSnapshotMeterCapability).MeterReads(meter.Tally("presence"))
					o := w.ts.ShardAccessStats().ColdOpens
					var has bool
					var err error
					if rel {
						has, err = snap.(interface {
							HasRelHistory(types.RelID) (bool, error)
						}).HasRelHistory(w.rel)
					} else {
						has, err = snap.HasNodeHistory(w.node)
					}
					opened, work := w.ts.ShardAccessStats().ColdOpens-o, meter.Work()
					es.LockShardMuForTest()
					st := es.Store()
					var metas, rows int64 = -1, -1
					if st != nil {
						metas, rows = st.HistoryMetaDecodesForTest(), st.HistoryRowDecodesForTest()
					}
					es.UnlockShardMuForTest()
					t.Logf("has %v, %v; %d opened, work %+v; the opened shard decoded %d skeletons, %d rows", has, err, opened, work, metas, rows)
					if err != nil || !has || opened == 0 || work.ShardOpens != opened || work.HistoryExamined != 0 || work.DecodedBytes != 0 {
						t.Fatalf("has = %v, %v, %d opened, work %+v; want true, every open counted, no history charged", has, err, opened, work)
					}
					if st == nil || metas != 0 || rows != 0 {
						t.Fatalf("the shard the read opened (open %v) decoded %d skeletons and %d rows; want it open, keys only", st != nil, metas, rows)
					}
				})
			}
		})
	}
}

// hr3StoreOf is the badger store holding door d's entity (badger, a sharded
// slot, a tiered shard).
func hr3StoreOf(t *testing.T, g *Core, d hrDoor, node types.NodeID, rel types.RelID) *badger.Store {
	t.Helper()
	isRel := hr2Key(d, node, rel).rel
	var bs *badger.Store
	var err error
	switch st := g.store.(type) {
	case *badger.Store:
		bs = st
	case *shardedpkg.Store:
		if isRel {
			bs, err = st.ShardForIDForTest(rel.SnowflakeID())
		} else {
			bs, err = st.ShardForIDForTest(node.SnowflakeID())
		}
	case *tiered.Store:
		if isRel {
			bs, err = st.ShardForRelIDForTest(rel)
		} else {
			bs, err = st.ShardForNodeIDForTest(node)
		}
	default:
		t.Fatalf("no badger store under %T", g.store)
	}
	if err != nil || bs == nil {
		t.Fatalf("the entity's store: %v, %v", bs, err)
	}
	return bs
}

// TestBudgetedTimeline_FillInCommitWindow (design L2, guard): a budgeted miss
// fills the head entry exactly while a flush commits the entity's pending
// history rows inside the read: between the snapshot's overlay capture and its
// Badger read (historyScanTestHook), and with the read run inside the flush
// right after the rows left the overlay (flushClearedTestHook). On badger, a
// sharded slot and a tiered shard, node and rel head doors, rows pending:
// the answer equals the head-cache-off oracle's, the entry kept equals the
// one a plain fold keeps (generations aside; the live token included), the
// next plain read is a head hit with the oracle's answer, and the native
// count equals HistoryExamined (the skeletons and the 2 winners). The fill is
// exact because store.ReadSnapshot answers each read as the store does at
// that moment and the entity lock holds for the whole read
// (entityReads.readsHead).
func TestBudgetedTimeline_FillInCommitWindow(t *testing.T) {
	t.Parallel()
	for _, be := range hr2Backends() {
		if !be.persistent {
			continue
		}
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, shut := be.open(t, t.TempDir())
			defer shut()
			for i, d0 := range hrDoors(g, 0, 0, 0)[:5] {
				t.Run(d0.name, func(t *testing.T) {
					node, rel := hrNode(t, g, 10), hrRel(t, g, 10)
					for _, window := range []string{"overlay then read", "after the overlay cleared"} {
						t.Run(window, func(t *testing.T) {
							ctx := context.Background()
							if _, err := g.Temporal.SetNodeVersionInterval(ctx, node, 3000, 4000, r10Props(50)); err != nil {
								t.Fatal(err)
							}
							if _, err := g.Temporal.SetRelVersionInterval(ctx, rel, 3000, 4000, r10Props(50)); err != nil {
								t.Fatal(err)
							}
							d := hrDoors(g, node, rel, g.peekNow())[i]
							k := hr2Key(d, node, rel)
							bs := hr3StoreOf(t, g, d, node, rel)
							g.headCacheOff = true
							want, err := d.plain()
							g.headCacheOff = false
							if err != nil || want == "" {
								t.Fatalf("oracle = %q, %v", want, err)
							}
							g.heads.put(k, nil)
							var (
								r     hr2Read
								once  sync.Once
								fired bool
							)
							if window == "overlay then read" {
								bs.SetHistoryScanTestHookForTest(func() {
									once.Do(func() {
										fired = true
										if err := bs.Flush(); err != nil {
											t.Error(err)
										}
									})
								})
								r = hr2Budget(t, g, node, rel, d, storepkg.ReadBudget{})
								bs.SetHistoryScanTestHookForTest(nil)
							} else {
								bs.SetFlushClearedTestHookForTest(func() {
									once.Do(func() { fired = true; r = hr2Budget(t, g, node, rel, d, storepkg.ReadBudget{}) })
								})
								if err := bs.Flush(); err != nil {
									t.Fatal(err)
								}
								bs.SetFlushClearedTestHookForTest(nil)
							}
							t.Logf("fired %v: HistoryExamined %d, native %d, hits %d", fired, r.work.HistoryExamined, r.native, r.hits)
							if !fired {
								t.Fatal("the window never opened: no history read between the overlay capture and the Badger read")
							}
							if r.err != nil || r.answer != want || r.hits != 0 || r.native != int64(r.work.HistoryExamined) { // #nosec G115 -- a test size
								t.Fatalf("budgeted miss = %q, %v, %d hits, native %d, HistoryExamined %d; want the oracle %q, a miss, native = HistoryExamined",
									r.answer, r.err, r.hits, r.native, r.work.HistoryExamined, want)
							}
							kept := g.heads.get(k)
							if kept == nil {
								t.Fatal("the budgeted miss kept no entry")
							}
							g.heads.put(k, nil)
							if got, err := d.plain(); err != nil || got != want {
								t.Fatalf("plain fold = %q, %v; want %q", got, err, want)
							}
							fold := g.heads.get(k)
							if fold == nil {
								t.Fatal("the plain fold kept no entry")
							}
							a, b := *kept, *fold
							a.gens, b.gens = nil, nil
							if !reflect.DeepEqual(a, b) {
								t.Fatalf("the entry the budgeted miss kept differs from the plain fold's\n kept %+v\n fold %+v", a, b)
							}
							g.heads.put(k, kept)
							hits := g.headHits.Load()
							if got, err := d.plain(); err != nil || got != want || g.headHits.Load()-hits != 1 {
								t.Fatalf("next plain read = %q, %v, %d hits; want the oracle %q from the kept entry", got, err, g.headHits.Load()-hits, want)
							}
						})
					}
				})
			}
		})
	}
}
