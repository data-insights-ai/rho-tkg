package core

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 68b on twin graphs: graph A plans corrections and GraphTx
// rollback snapshots from the head entry (effective_head_write.go), graph B
// reads the whole history (headWriteOff, the code before 68b). Both run the
// same operations — corrections bounded and open (often of one validity), at
// the clock and at caller instants (accepted and refused), excises, Updates,
// in-place validity rewrites, CloseVersion, label changes, deletes and
// re-imports of nodes and relationships, GraphTx commits and rollbacks with
// several mutations of one entity (a correction that moves the current row
// into history below the rows it keeps among them), rollback-then-rewrite,
// history compaction, Clear, retention purge and, on tiered, Archive /
// Restore, rotation and the idle close of cold shards — with the same IDs
// and a clock that only the writes advance. After every step the two stores
// must hold byte-identical rows (current row and every history row, wire
// encoded: versions, stamps, hashes, content), every error must match, a
// rollback must leave A byte-identical to before its tx, and A's head-pin
// and earlier timelines must equal B's fold.
//
// Catches: a planned piece or base that differs from the history's, a wrong
// resumption end, a current-slot choice that differs, a version or stamp
// off by one, a trim point that keeps a tx row or drops a kept one, a demoted
// current row left in history by a rollback.

func TestEffectiveHeadWrite_TwinDifferential(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	seeds, steps := 2, 60
	if !testing.Short() {
		seeds, steps = 6, 150
	}
	if isRaceEnabled() {
		seeds, steps = 1, 40
	}
	if n, err := strconv.Atoi(os.Getenv("HEAD_TWIN_SEEDS")); err == nil && n > 0 {
		seeds = n
	}
	var planned, rollbacks, demotes, gaps uint64
	for _, be := range append(r10AllBackends(t), r10WholeAxisBackends(t)...) {
		for s := range seeds {
			seed := uint64(0x68b0 + s)
			t.Run(fmt.Sprintf("%s/seed=%d", be.name, seed), func(t *testing.T) {
				tw := newHeadTwin(t, be, seed)
				if s%2 == 1 {
					// Trimmed entries (row 85): A plans most corrections from a
					// suffix of the head answer and reads fold.
					tw.a.headPieceCap, tw.a.headBoundCap = 3, 4
				}
				for step := range steps {
					tw.step(step)
					if os.Getenv("HEAD_TWIN_DEBUG") != "" {
						t.Logf("step %d: %s", step, tw.last)
					}
					tw.check(fmt.Sprintf("step %d (%s)", step, tw.last))
				}
				t.Logf("ops %v; %d corrections planned from the entry", tw.ops, tw.a.headWrites.Load())
				planned += tw.a.headWrites.Load()
				rollbacks += uint64(tw.ops["rollback"])
				demotes += uint64(tw.ops["tx demote"])
				gaps += uint64(tw.ops["gap"])
				if tw.b.headWrites.Load() != 0 {
					t.Fatalf("graph B planned %d corrections from the entry", tw.b.headWrites.Load())
				}
			})
		}
	}
	t.Logf("%d corrections planned from the entry, %d rollbacks, %d tx demotes", planned, rollbacks, demotes)
	if planned == 0 || rollbacks == 0 || demotes == 0 || gaps == 0 {
		t.Fatalf("the generator never exercised the write side: %d planned, %d rollbacks, %d demotes, %d compaction gaps", planned, rollbacks, demotes, gaps)
	}
}

type headTwin struct {
	t                *testing.T
	a, b             *Core
	rng              *rand.Rand
	nodes            []types.NodeID
	rels             []types.RelID
	pins             []types.Instant
	last             string
	ops              map[string]int
	tieredA, tieredB *tiered.Store
}

func newHeadTwin(t *testing.T, be r10Backend, seed uint64) *headTwin {
	tw := &headTwin{t: t, rng: rand.New(rand.NewPCG(seed, 0x68b)), ops: map[string]int{}}
	// One instant for both clocks: every write steps its graph's clock
	// floor by one, so the twins stamp alike while their writes do.
	fixed := time.Now().Add(time.Hour)
	open := func() *Core {
		cfg := be.cfg()
		cfg.AllowTxBackfill, cfg.AllowReset, cfg.AllowRetentionPurge = true, true, true
		g, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = g.Close() })
		g.SetClockForTest(t, func() time.Time { return fixed })
		return g
	}
	tw.a, tw.b = open(), open()
	tw.b.headWriteOff = true
	tw.tieredA, _ = tw.a.store.(*tiered.Store)
	tw.tieredB, _ = tw.b.store.(*tiered.Store)
	tw.setup()
	return tw
}

func (tw *headTwin) props() map[string]any {
	return map[string]any{"c": int64(tw.rng.IntN(3)), "w": int64(tw.rng.IntN(1000))}
}

// both runs fn on A and on B; the errors must match.
func (tw *headTwin) both(fn func(g *Core) error) error {
	ea, eb := fn(tw.a), fn(tw.b)
	if fmt.Sprint(ea) != fmt.Sprint(eb) {
		tw.t.Fatalf("%s: A %v, B %v", tw.last, ea, eb)
	}
	return ea
}

func (tw *headTwin) setup() {
	ctx := context.Background()
	first := len(tw.nodes)
	for i := range 3 {
		p := tw.props()
		if i > 0 {
			p["tkg_valid_from"] = int64(100 * i)
		}
		if i == 2 {
			p["tkg_valid_to"] = int64(900)
		}
		labels := []string{"H"}
		if i%2 == 0 {
			labels = []string{"Ref", "H"}
		}
		id := tw.a.nextNodeID()
		tw.last = "setup node"
		if err := tw.both(func(g *Core) error { _, err := g.Nodes.Import(ctx, id, labels, p); return err }); err != nil {
			tw.t.Fatal(err)
		}
		tw.nodes = append(tw.nodes, id)
	}
	for i := range 2 {
		p := tw.props()
		p["tkg_valid_from"] = int64(50 + 100*i)
		id := tw.a.nextRelID()
		tw.last = "setup rel"
		if err := tw.both(func(g *Core) error { return tw.importRel(g, id, first, p) }); err != nil {
			tw.t.Fatal(err)
		}
		tw.rels = append(tw.rels, id)
	}
}

func (tw *headTwin) importRel(g *Core, id types.RelID, first int, p map[string]any) error {
	ctx := context.Background()
	s, err := g.Nodes.Get(ctx, tw.nodes[first])
	if err != nil {
		return err
	}
	e, err := g.Nodes.Get(ctx, tw.nodes[first+1])
	if err != nil {
		return err
	}
	_, err = g.Rels.Import(ctx, id, "HR", s, e, p)
	return err
}

func (tw *headTwin) interval() (types.Instant, types.Instant) {
	if tw.a.axis().Literal() && tw.rng.IntN(5) == 0 {
		// The literal axis (round 11): spans that start or end at the
		// instant 0 and cross it.
		k := types.Instant(50 * (1 + tw.rng.IntN(4)))
		switch tw.rng.IntN(3) {
		case 0:
			return -k, 0
		case 1:
			return 0, k
		default:
			return -k, k
		}
	}
	a := types.Instant(50 * (1 + tw.rng.IntN(20)))
	if tw.rng.IntN(4) == 0 {
		return a, tw.a.axis().OpenEnd() // open (0 on the legacy axis)
	}
	return a, a + types.Instant(50*(1+tw.rng.IntN(6)))
}

// sigmaInterval is the R10 shape half of the time.
func (tw *headTwin) sigmaInterval() (types.Instant, types.Instant) {
	if tw.rng.IntN(2) == 0 {
		return 1000, 2000
	}
	return tw.interval()
}

// callerInstant is just above the newest stamp, or (one in four) at it: refused.
func (tw *headTwin) callerInstant(g *Core, rel bool, nid types.NodeID, rid types.RelID) types.Instant {
	var f, to types.Instant
	if rel {
		f, to, _, _ = g.Rels.LatestStamps(rid)
	} else {
		f, to, _, _ = g.Nodes.LatestStamps(nid)
	}
	if tw.rng.IntN(4) == 0 {
		return max(f, to)
	}
	return max(f, to) + 1
}

func (tw *headTwin) step(step int) {
	ctx := context.Background()
	rel := tw.rng.IntN(3) == 0
	nid := tw.nodes[tw.rng.IntN(len(tw.nodes))]
	rid := tw.rels[tw.rng.IntN(len(tw.rels))]
	a, b := tw.sigmaInterval()
	p := tw.props()
	op := tw.rng.IntN(100)
	switch {
	case op < 30:
		tw.last = fmt.Sprintf("SetVersionInterval [%d,%d) rel=%v", a, b, rel)
		_ = tw.both(func(g *Core) error {
			if rel {
				_, err := g.Temporal.SetRelVersionInterval(ctx, rid, a, b, p)
				return err
			}
			_, err := g.Temporal.SetNodeVersionInterval(ctx, nid, a, b, p)
			return err
		})
	case op < 38:
		at := tw.callerInstant(tw.a, rel, nid, rid)
		tw.last = fmt.Sprintf("SetVersionIntervalWithTx [%d,%d) at %d rel=%v", a, b, at, rel)
		_ = tw.both(func(g *Core) error {
			if rel {
				_, err := g.Temporal.SetRelVersionIntervalWithTx(ctx, rid, a, b, p, at)
				return err
			}
			_, err := g.Temporal.SetNodeVersionIntervalWithTx(ctx, nid, a, b, p, at)
			return err
		})
	case op < 48:
		tw.txStep(rel, nid, rid, false)
	case op < 58:
		tw.gapStep(rel, nid, rid)
	case op < 66:
		if tw.a.axis().Open(b) {
			b = a + 100
		}
		tw.last = fmt.Sprintf("Excise [%d,%d) rel=%v", a, b, rel)
		_ = tw.both(func(g *Core) error {
			if rel {
				return g.Temporal.ExciseRelValidity(ctx, rid, a, b)
			}
			return g.Temporal.ExciseNodeValidity(ctx, nid, a, b)
		})
	case op < 72:
		tw.last = fmt.Sprintf("Update rel=%v", rel)
		_ = tw.both(func(g *Core) error {
			if rel {
				_, err := g.Rels.Update(ctx, rid, p)
				return err
			}
			_, err := g.Nodes.Update(ctx, nid, p)
			return err
		})
	case op < 75:
		p["tkg_valid_to"] = int64(a + 2000)
		tw.last = fmt.Sprintf("UpdateInPlace valid_to %d rel=%v", a+2000, rel)
		_ = tw.both(func(g *Core) error {
			if rel {
				_, err := g.Rels.UpdateInPlace(ctx, rid, p)
				return err
			}
			_, err := g.Nodes.UpdateInPlace(ctx, nid, p)
			return err
		})
	case op < 80:
		tw.last = fmt.Sprintf("CloseVersion %d rel=%v", a+1000, rel)
		_ = tw.both(func(g *Core) error {
			if rel {
				return g.Rels.CloseVersion(ctx, rid, a+1000)
			}
			return g.Nodes.CloseVersion(ctx, nid, a+1000)
		})
	case op < 84 && step > 10:
		tw.deleteOrReimport(rel, nid, rid, a)
	case op < 88:
		keep := 1 + tw.rng.IntN(4)
		tw.last = fmt.Sprintf("Compact KeepVersions %d", keep)
		_ = tw.both(func(g *Core) error {
			if _, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: keep}); err != nil {
				return err
			}
			_, err := g.Admin.CompactHistoryRels(ctx, RetentionPolicy{KeepVersions: keep})
			return err
		})
	case op == 88 && step > 30 && tw.rng.IntN(3) == 0:
		tw.last = "Admin.Reset, then new entities"
		if tw.both(func(g *Core) error { return g.Admin.Reset() }) == nil {
			tw.setup()
		}
	case op == 89:
		before := types.Instant(50 * (1 + tw.rng.IntN(40)))
		tw.last = fmt.Sprintf("PurgeExpiredNodes ByValidTo before %d", before)
		_ = tw.both(func(g *Core) error {
			_, err := g.Admin.PurgeExpiredNodes(ctx, PurgePolicy{Label: "H", Mode: PurgeByValidTo, Before: before})
			return err
		})
	case op >= 97 && tw.tieredA != nil:
		tw.tieredStep()
	default:
		tw.last = "read only"
	}
	if f := strings.Fields(tw.last); len(f) > 0 {
		tw.ops[f[0]]++
	}
	tw.pins = append(tw.pins, tw.a.peekNow())
}

// txStep runs one GraphTx of one to four mutations on one entity (and, one
// time in four, a second entity), then commits or rolls back; a rollback
// must leave A's rows byte-identical to before the tx, and a rewrite follows.
// appendsOnly: two appending writes (backlog 70's shape after gapStep).
func (tw *headTwin) txStep(rel bool, nid types.NodeID, rid types.RelID, appendsOnly bool) {
	ctx := context.Background()
	commit := tw.rng.IntN(2) == 0
	type txOp struct {
		name string
		fn   func(tx *GraphTx) error
	}
	var ops []txOp
	n := 1 + tw.rng.IntN(4)
	if appendsOnly {
		n, commit = 2, tw.rng.IntN(4) == 0 // the rollback is the case at stake
	}
	for range n {
		a, b := tw.sigmaInterval()
		p := tw.props()
		k := tw.rng.IntN(13)
		if appendsOnly {
			k = 5 + 6*tw.rng.IntN(2) // Update, or a close by tkg_valid_to
		}
		switch {
		case k < 5:
			// Open-ended after bounded corrections: the current row moves
			// into history below the rows the tx keeps.
			ops = append(ops, txOp{fmt.Sprintf("SetVersionInterval [%d,%d)", a, b), func(tx *GraphTx) error {
				if rel {
					_, err := tx.SetRelVersionInterval(rid, a, b, p)
					return err
				}
				_, err := tx.SetNodeVersionInterval(nid, a, b, p)
				return err
			}})
		case k < 7:
			ops = append(ops, txOp{"Update", func(tx *GraphTx) error {
				if rel {
					_, err := tx.UpdateRelationship(rid, p)
					return err
				}
				_, err := tx.UpdateNode(nid, p)
				return err
			}})
		case k < 8:
			if tw.a.axis().Open(b) {
				b = a + 100
			}
			ops = append(ops, txOp{fmt.Sprintf("Excise [%d,%d)", a, b), func(tx *GraphTx) error {
				if rel {
					return tx.ExciseRelValidity(rid, a, b)
				}
				return tx.ExciseNodeValidity(nid, a, b)
			}})
		case k < 9:
			lbl := fmt.Sprintf("L%d", tw.rng.IntN(2))
			ops = append(ops, txOp{"AddNodeLabel " + lbl, func(tx *GraphTx) error { return tx.AddNodeLabel(nid, lbl) }})
		case k < 10:
			ops = append(ops, txOp{"Delete", func(tx *GraphTx) error {
				if rel {
					return tx.DeleteRelationship(rid)
				}
				return tx.DeleteNode(nid)
			}})
		case k < 12:
			// A close: Update with only tkg_valid_to ends the current version.
			end := map[string]any{"tkg_valid_to": int64(a + 3000)}
			ops = append(ops, txOp{fmt.Sprintf("close at %d", a+3000), func(tx *GraphTx) error {
				if rel {
					_, err := tx.UpdateRelationship(rid, end)
					return err
				}
				_, err := tx.UpdateNode(nid, end)
				return err
			}})
		default:
			ops = append(ops, txOp{fmt.Sprintf("other entity SetVersionInterval [%d,%d)", a, b), func(tx *GraphTx) error {
				if rel {
					_, err := tx.SetNodeVersionInterval(nid, a, b, p)
					return err
				}
				_, err := tx.SetRelVersionInterval(rid, a, b, p)
				return err
			}})
		}
	}
	names := make([]string, len(ops))
	for i, o := range ops {
		names[i] = o.name
	}
	tw.last = fmt.Sprintf("GraphTx rel=%v [%s] commit=%v", rel, strings.Join(names, "; "), commit)
	var before string
	if !commit {
		before = tw.dump(tw.a)
	}
	demoted := false
	_ = tw.both(func(g *Core) error {
		tx, err := g.BeginTx()
		if err != nil {
			return err
		}
		var curBefore uint32
		if g == tw.a {
			curBefore = tw.currentVersion(g, rel, nid, rid)
		}
		var errs []string
		for _, o := range ops {
			if err := o.fn(tx); err != nil {
				errs = append(errs, err.Error())
			}
		}
		if g == tw.a && curBefore != 0 && tw.historyHas(g, rel, nid, rid, curBefore) {
			demoted = true
		}
		if g == tw.a && os.Getenv("HEAD_TWIN_DEBUG") != "" {
			tw.t.Logf("%s: A snapshots nodes %+v rels %+v", tw.last, tx.updatedNodes, tx.updatedRels)
		}
		if commit {
			if err := tx.Commit(); err != nil {
				return err
			}
		} else if err := tx.Rollback(); err != nil {
			return err
		}
		if len(errs) > 0 {
			return errors.New(strings.Join(errs, " | "))
		}
		return nil
	})
	if demoted {
		tw.ops["tx demote"]++
	}
	if commit {
		tw.ops["commit"]++
		return
	}
	tw.ops["rollback"]++
	if after := tw.dump(tw.a); after != before {
		tw.t.Fatalf("%s: the rollback did not restore A's rows:%s", tw.last, dumpDiff(before, after))
	}
	// Rollback, then rewrite: the next write reuses the rolled-back versions.
	a, b := tw.sigmaInterval()
	p := tw.props()
	tw.last += fmt.Sprintf(", then SetVersionInterval [%d,%d)", a, b)
	_ = tw.both(func(g *Core) error {
		if rel {
			_, err := g.Temporal.SetRelVersionInterval(ctx, rid, a, b, p)
			return err
		}
		_, err := g.Temporal.SetNodeVersionInterval(ctx, nid, a, b, p)
		return err
	})
}

func (tw *headTwin) currentVersion(g *Core, rel bool, nid types.NodeID, rid types.RelID) uint32 {
	if rel {
		if r, err := g.store.GetRelationship(rid); err == nil {
			return r.Version()
		}
		return 0
	}
	if n, err := g.store.GetNode(nid); err == nil {
		return n.Version()
	}
	return 0
}

func (tw *headTwin) historyHas(g *Core, rel bool, nid types.NodeID, rid types.RelID, v uint32) bool {
	if rel {
		_, err := g.store.GetRelVersion(rid, v)
		return err == nil
	}
	_, err := g.store.GetNodeVersion(nid, v)
	return err == nil
}

func (tw *headTwin) deleteOrReimport(rel bool, nid types.NodeID, rid types.RelID, a types.Instant) {
	ctx := context.Background()
	if rel {
		if _, err := tw.a.Rels.Get(ctx, rid); errors.Is(err, storepkg.ErrRelNotFound) {
			tw.last = "Rels.Import (second life)"
			first := len(tw.nodes) - 3
			_ = tw.both(func(g *Core) error {
				return tw.importRel(g, rid, first, map[string]any{"tkg_valid_from": int64(a), "life": int64(2)})
			})
			return
		}
		tw.last = fmt.Sprintf("Delete rel %v", rid)
		_ = tw.both(func(g *Core) error { return g.Rels.Delete(ctx, rid) })
		return
	}
	if _, err := tw.a.Nodes.Get(ctx, nid); errors.Is(err, storepkg.ErrNodeNotFound) {
		tw.last = "Nodes.Import (second life)"
		_ = tw.both(func(g *Core) error {
			_, err := g.Nodes.Import(ctx, nid, []string{"H"}, map[string]any{"tkg_valid_from": int64(a), "life": int64(2)})
			return err
		})
		return
	}
	tw.last = fmt.Sprintf("Delete node %v", nid)
	_ = tw.both(func(g *Core) error { return g.Nodes.Delete(ctx, nid) })
}

func (tw *headTwin) tieredStep() {
	switch tw.rng.IntN(3) {
	case 0:
		ref := tw.nodes[len(tw.nodes)-1-2*tw.rng.IntN(2)]
		tw.last = fmt.Sprintf("Admin.Archive / Restore %v", ref)
		if tw.both(func(g *Core) error { return g.Admin.Archive(ref) }) == nil {
			tw.check("after Archive")
			_ = tw.both(func(g *Core) error { return g.Admin.Restore(ref) })
			tw.ops["archive"]++
		}
	case 1:
		tw.last = "ForceRotate"
		_ = tw.both(func(g *Core) error { return g.Admin.ForceRotate() })
		tw.ops["rotate"]++
	default:
		tw.last = "demote event shards, close idle cold shards"
		for _, g := range []*Core{tw.a, tw.b} {
			ts := tw.tieredA
			if g == tw.b {
				ts = tw.tieredB
			}
			shards, err := g.Admin.ListShards()
			if err != nil {
				continue
			}
			for i, sh := range shards {
				if sh.Kind == tiered.ShardEvent && i < len(shards)-1 {
					demoteToCold(ts, sh.Name)
				}
			}
			ts.SetIdleTimeoutForTest(0)
			ts.CloseIdleShardsForTest()
		}
		tw.ops["idle-close"]++
	}
}

// dump is every tracked entity's stored rows, wire encoded: the current row
// and each history row by version.
func (tw *headTwin) dump(g *Core) string {
	var sb strings.Builder
	enc := func(b []byte, err error) string {
		if err != nil {
			return "encode error " + err.Error()
		}
		return hex.EncodeToString(b)
	}
	// tms leads each row with its temporal stamps, so a differing field
	// shows before dumpDiff truncates the wire bytes.
	tms := func(tm *types.TemporalMetadata) string {
		if tm == nil {
			return "tm=nil"
		}
		return fmt.Sprintf("vf=%d vt=%d tf=%d tt=%d del=%d void=%v retr=%v", tm.ValidFrom, tm.ValidTo, tm.TxFrom, tm.TxTo, tm.DeletedAt, tm.Void, tm.Retracted)
	}
	for _, id := range tw.nodes {
		fmt.Fprintf(&sb, "\nnode %v:", id)
		if n, err := g.store.GetNode(id); err != nil {
			fmt.Fprintf(&sb, " current %v", err)
		} else {
			fmt.Fprintf(&sb, " current v%d %s %s", n.Version(), tms(n.Temporal()), enc(storeutil.MarshalNodeWire(n)))
		}
		hist, err := g.store.GetNodeHistory(id)
		if err != nil {
			fmt.Fprintf(&sb, " history %v", err)
		}
		for _, h := range hist {
			fmt.Fprintf(&sb, "\n  v%d %s %s", h.Version(), tms(h.Temporal()), enc(storeutil.MarshalNodeWire(h)))
		}
	}
	for _, id := range tw.rels {
		fmt.Fprintf(&sb, "\nrel %v:", id)
		if r, err := g.store.GetRelationship(id); err != nil {
			fmt.Fprintf(&sb, " current %v", err)
		} else {
			fmt.Fprintf(&sb, " current v%d %s %s", r.Version(), tms(r.Temporal()), enc(storeutil.MarshalRelWire(r)))
		}
		hist, err := g.store.GetRelHistory(id)
		if err != nil {
			fmt.Fprintf(&sb, " history %v", err)
		}
		for _, h := range hist {
			fmt.Fprintf(&sb, "\n  v%d %s %s", h.Version(), tms(h.Temporal()), enc(storeutil.MarshalRelWire(h)))
		}
	}
	return sb.String()
}

// check compares the twins' rows and A's timelines (head entry on) against
// B's fold (head entry off) at the head pin and at earlier pins.
func (tw *headTwin) check(when string) {
	if da, db := tw.dump(tw.a), tw.dump(tw.b); da != db {
		tw.t.Fatalf("%s: the twins' rows differ (A, B):%s", when, dumpDiff(da, db))
	}
	pins := []types.Instant{tw.a.peekNow()}
	for range 2 {
		if len(tw.pins) > 0 {
			pins = append(pins, tw.pins[tw.rng.IntN(len(tw.pins))])
		}
	}
	for _, pin := range pins {
		for _, id := range tw.nodes {
			got, gerr := tw.a.Temporal.NodeEffectiveTimeline(id, pin)
			tw.b.headCacheOff = true
			want, werr := tw.b.Temporal.NodeEffectiveTimeline(id, pin)
			tw.b.headCacheOff = false
			if fmt.Sprint(gerr) != fmt.Sprint(werr) || etSegsString(etNodeSegs(got)) != etSegsString(etNodeSegs(want)) {
				tw.t.Fatalf("%s: node %v pin %d:\n A %v%s\n B %v%s", when, id, pin, gerr, etSegsString(etNodeSegs(got)), werr, etSegsString(etNodeSegs(want)))
			}
		}
		for _, id := range tw.rels {
			got, gerr := tw.a.Temporal.RelEffectiveTimeline(id, pin)
			tw.b.headCacheOff = true
			want, werr := tw.b.Temporal.RelEffectiveTimeline(id, pin)
			tw.b.headCacheOff = false
			if fmt.Sprint(gerr) != fmt.Sprint(werr) || etSegsString(etRelSegs(got)) != etSegsString(etRelSegs(want)) {
				tw.t.Fatalf("%s: rel %v pin %d:\n A %v%s\n B %v%s", when, id, pin, gerr, etSegsString(etRelSegs(got)), werr, etSegsString(etRelSegs(want)))
			}
		}
	}
}

// dumpDiff lists the dump lines only one side has (entity headers kept).
func dumpDiff(a, b string) string {
	la, lb := strings.Split(a, "\n"), strings.Split(b, "\n")
	in := func(ls []string) map[string]bool {
		m := make(map[string]bool, len(ls))
		for _, l := range ls {
			m[l] = true
		}
		return m
	}
	ma, mb := in(la), in(lb)
	var sb strings.Builder
	short := func(l string) string {
		if len(l) > 160 {
			return l[:160] + "..."
		}
		return l
	}
	for _, l := range la {
		if !mb[l] {
			sb.WriteString("\n- " + short(l))
		}
	}
	for _, l := range lb {
		if !ma[l] {
			sb.WriteString("\n+ " + short(l))
		}
	}
	return sb.String()
}

// gapStep is backlog 70's shape: three bounded corrections of one validity,
// history compaction to one row (a version gap directly above the current
// row), a head read, then a GraphTx of two appending writes, rolled back
// three times in four.
func (tw *headTwin) gapStep(rel bool, nid types.NodeID, rid types.RelID) {
	ctx := context.Background()
	for i := range 3 {
		p := tw.props()
		tw.last = fmt.Sprintf("gap: SetVersionInterval [1000,2000) #%d rel=%v", i, rel)
		_ = tw.both(func(g *Core) error {
			if rel {
				_, err := g.Temporal.SetRelVersionInterval(ctx, rid, 1000, 2000, p)
				return err
			}
			_, err := g.Temporal.SetNodeVersionInterval(ctx, nid, 1000, 2000, p)
			return err
		})
	}
	tw.last = "gap: Compact KeepVersions 1"
	_ = tw.both(func(g *Core) error {
		if _, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 1}); err != nil {
			return err
		}
		_, err := g.Admin.CompactHistoryRels(ctx, RetentionPolicy{KeepVersions: 1})
		return err
	})
	tw.check("gap: after compaction")
	if cur := tw.currentVersion(tw.a, rel, nid, rid); cur != 0 || !tw.historyHas(tw.a, rel, nid, rid, 0) {
		if !tw.historyHas(tw.a, rel, nid, rid, cur+1) {
			if above, err := tw.hasAbove(rel, nid, rid, cur+1); err == nil && above {
				tw.ops["gap"]++ // a row above a missing current+1
				if os.Getenv("HEAD_TWIN_DEBUG") != "" {
					var top uint32
					var ok bool
					if rel {
						r, _ := tw.a.store.GetRelationship(rid)
						top, ok = tw.a.relHeadTop(rid, r)
					} else {
						n, _ := tw.a.store.GetNode(nid)
						top, ok = tw.a.nodeHeadTop(nid, n)
					}
					tw.t.Logf("gap: rel=%v cur v%d head top %d %v", rel, cur, top, ok)
				}
			}
		}
	}
	tw.txStep(rel, nid, rid, true)
}

func (tw *headTwin) hasAbove(rel bool, nid types.NodeID, rid types.RelID, v uint32) (bool, error) {
	if rel {
		return tw.a.relHasHistoryFrom(rid, v)
	}
	return tw.a.nodeHasHistoryFrom(nid, v)
}
