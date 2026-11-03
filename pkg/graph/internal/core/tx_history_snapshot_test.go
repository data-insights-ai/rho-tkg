package core

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RELHIST P1: a GraphTx's rollback snapshot of a deleted or updated entity
// keeps the rows at or above the entity's current version (one page), never
// the whole chain. On tiered every tx delete copied the whole history plus a
// DeepCopy per row (no rollback trim), and every tx update of an entity with
// rows above its current version (a bounded correction left them, the head
// entry missing) copied it on every store: sigma's cut 10 profile, 16.1 GB of
// 160 GB in copyRelHistory under snapshotCurrentRelLocked.

type relhistFixture struct {
	g     *Core
	ts    *tiered.Store
	label string
	a, b  types.NodeID
	rel   types.RelID
	cold  *tiered.EventShard // the cold placement's shard: closed before each tx
}

// relhistBuild makes nodes a, b of label (property k indexed) and a rel a->b,
// each with versions rows (versions-1 updates). above adds a bounded
// correction to each, which leaves two history rows above the current
// version and no head entry.
func relhistBuild(t *testing.T, g *Core, ts *tiered.Store, label string, versions int, above bool) *relhistFixture {
	return relhistBuildPadded(t, g, ts, label, versions, above, "")
}

// relhistBuildPadded is relhistBuild with a constant pad property on every
// entity: with history delta encoding a row then stores as a delta (smaller
// than the full row).
func relhistBuildPadded(t *testing.T, g *Core, ts *tiered.Store, label string, versions int, above bool, pad string) *relhistFixture {
	t.Helper()
	ctx := context.Background()
	f := &relhistFixture{g: g, ts: ts, label: label}
	props := func(k string, v int64) map[string]any {
		m := map[string]any{k: v}
		if pad != "" {
			m["pad"] = pad
		}
		return m
	}
	a, err := g.Nodes.Add(ctx, []string{label}, props("k", 0))
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(ctx, []string{label}, props("k", -1))
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add(ctx, "OBSERVES", a, b, props("w", 0))
	if err != nil {
		t.Fatal(err)
	}
	f.a, f.b, f.rel = a.ID(), b.ID(), r.ID()
	for i := 1; i < versions; i++ {
		if _, err := g.Nodes.Update(ctx, f.a, map[string]any{"k": int64(i)}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels.Update(ctx, f.rel, map[string]any{"w": int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if above {
		f.correct(t)
	}
	return f
}

// correct appends a bounded correction before each entity's current row:
// two history rows above the current version, no head entry.
func (f *relhistFixture) correct(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	n, err := f.g.store.GetNode(f.a)
	if err != nil {
		t.Fatal(err)
	}
	vf := n.Temporal().ValidFrom
	if _, err := f.g.Temporal.SetNodeVersionInterval(ctx, f.a, vf-1000, vf-500, map[string]any{"k": int64(-7)}); err != nil {
		t.Fatal(err)
	}
	r, err := f.g.store.GetRelationship(f.rel)
	if err != nil {
		t.Fatal(err)
	}
	vf = r.Temporal().ValidFrom
	if _, err := f.g.Temporal.SetRelVersionInterval(ctx, f.rel, vf-1000, vf-500, map[string]any{"w": int64(-7)}); err != nil {
		t.Fatal(err)
	}
}

// dump renders everything a rollback must restore: each entity's current
// row and full history (wire bytes), both nodes' adjacency, the label, type
// and property index answers.
func (f *relhistFixture) dump(t *testing.T) string {
	t.Helper()
	var sb strings.Builder
	enc := func(b []byte, err error) string {
		if err != nil {
			return "encode error " + err.Error()
		}
		return hex.EncodeToString(b)
	}
	for _, id := range []types.NodeID{f.a, f.b} {
		fmt.Fprintf(&sb, "\nnode %v:", id)
		if n, err := f.g.store.GetNode(id); err != nil {
			fmt.Fprintf(&sb, " current %v", err)
		} else {
			fmt.Fprintf(&sb, " current v%d %s", n.Version(), enc(storeutil.MarshalNodeWire(n)))
		}
		hist, err := f.g.store.GetNodeHistory(id)
		if err != nil {
			fmt.Fprintf(&sb, " history %v", err)
		}
		for _, h := range hist {
			fmt.Fprintf(&sb, "\n  v%d %s", h.Version(), enc(storeutil.MarshalNodeWire(h)))
		}
		out, err := f.g.store.OutgoingRelationships(id, 0)
		fmt.Fprintf(&sb, "\n  out %s %v", relhistRelIDs(out), err)
		in, err := f.g.store.IncomingRelationships(id, 0)
		fmt.Fprintf(&sb, "\n  in %s %v", relhistRelIDs(in), err)
	}
	fmt.Fprintf(&sb, "\nrel %v:", f.rel)
	if r, err := f.g.store.GetRelationship(f.rel); err != nil {
		fmt.Fprintf(&sb, " current %v", err)
	} else {
		fmt.Fprintf(&sb, " current v%d %s", r.Version(), enc(storeutil.MarshalRelWire(r)))
	}
	hist, err := f.g.store.GetRelHistory(f.rel)
	if err != nil {
		fmt.Fprintf(&sb, " history %v", err)
	}
	for _, h := range hist {
		fmt.Fprintf(&sb, "\n  v%d %s", h.Version(), enc(storeutil.MarshalRelWire(h)))
	}
	byLabel, err := f.g.Nodes.ByLabel(f.label, storepkg.QueryOpts{})
	fmt.Fprintf(&sb, "\nlabel %s %v", relhistNodeIDs(byLabel), err)
	byType, err := f.g.Rels.ByType("OBSERVES", storepkg.QueryOpts{})
	fmt.Fprintf(&sb, "\ntype %s %v", relhistRelIDs(byType), err)
	n, err := f.g.store.GetNode(f.a)
	if err == nil {
		k, _ := n.GetProperty("k")
		byProp, err := f.g.Nodes.ByLabelAndProperty(f.label, "k", k, storepkg.QueryOpts{})
		fmt.Fprintf(&sb, "\nprop k=%v %s %v", k, relhistNodeIDs(byProp), err)
	}
	return sb.String()
}

func relhistRelIDs(rs []*types.Relationship) string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, fmt.Sprintf("%v@v%d", r.ID(), r.Version()))
	}
	slices.Sort(out)
	return strings.Join(out, ",")
}

func relhistNodeIDs(ns []*types.Node) string {
	out := make([]string, 0, len(ns))
	for _, n := range ns {
		out = append(out, fmt.Sprintf("%v@v%d", n.ID(), n.Version()))
	}
	slices.Sort(out)
	return strings.Join(out, ",")
}

type relhistOp struct {
	name string
	fn   func(tx *GraphTx, f *relhistFixture) error
}

func relhistOps() []relhistOp {
	return []relhistOp{
		{"delete rel", func(tx *GraphTx, f *relhistFixture) error { return tx.DeleteRelationship(f.rel) }},
		{"delete start node", func(tx *GraphTx, f *relhistFixture) error { return tx.DeleteNode(f.a) }},
		{"delete end node", func(tx *GraphTx, f *relhistFixture) error { return tx.DeleteNode(f.b) }},
		{"update rel and node", func(tx *GraphTx, f *relhistFixture) error {
			if _, err := tx.UpdateRelationship(f.rel, map[string]any{"w": int64(1000)}); err != nil {
				return err
			}
			_, err := tx.UpdateNode(f.a, map[string]any{"k": int64(1000)})
			return err
		}},
		{"update then delete", func(tx *GraphTx, f *relhistFixture) error {
			if _, err := tx.UpdateRelationship(f.rel, map[string]any{"w": int64(1001)}); err != nil {
				return err
			}
			if _, err := tx.UpdateNode(f.a, map[string]any{"k": int64(1001)}); err != nil {
				return err
			}
			return tx.DeleteNode(f.a)
		}},
		{"correction then delete", func(tx *GraphTx, f *relhistFixture) error {
			r, err := tx.g.getCurrentRelationship(f.rel)
			if err != nil {
				return err
			}
			vf := r.Temporal().ValidFrom
			if _, err := tx.SetRelVersionInterval(f.rel, vf-3000, vf-2000, map[string]any{"w": int64(-9)}); err != nil {
				return err
			}
			return tx.DeleteRelationship(f.rel)
		}},
		{"delete then re-import rel", func(tx *GraphTx, f *relhistFixture) error {
			if err := tx.DeleteRelationship(f.rel); err != nil {
				return err
			}
			a, err := tx.g.getCurrentNode(f.a)
			if err != nil {
				return err
			}
			b, err := tx.g.getCurrentNode(f.b)
			if err != nil {
				return err
			}
			_, err = tx.ImportRelationshipWithID(context.Background(), f.rel, "OBSERVES", a, b, map[string]any{"w": int64(5000)})
			return err
		}},
		{"delete then re-import node", func(tx *GraphTx, f *relhistFixture) error {
			if err := tx.DeleteNode(f.a); err != nil {
				return err
			}
			_, err := tx.ImportNodeWithID(context.Background(), f.a, []string{f.label}, map[string]any{"k": int64(5000)})
			return err
		}},
	}
}

func relhistRollback(t *testing.T, f *relhistFixture, op relhistOp) {
	t.Helper()
	before := f.dump(t)
	tx, err := f.g.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	if err := op.fn(tx, f); err != nil {
		_ = tx.Rollback()
		t.Fatalf("%s: %v", op.name, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("%s: rollback: %v", op.name, err)
	}
	if after := f.dump(t); after != before {
		t.Fatalf("%s: rollback did not restore the state (before, after):%s", op.name, dumpDiff(before, after))
	}
}

// relhistColdFixture builds the fixture on the hot shard of a disk store,
// rotates it out, demotes it and lets the idle sweep close it: the tx opens
// it as the write path does.
func relhistColdFixture(t *testing.T, above bool) *relhistFixture {
	g, ts := newDiskTieredGraph(t)
	f := relhistBuild(t, g, ts, "Signal", 4, above)
	ts.MuForTest().RLock()
	origin := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()
	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatal(err)
	}
	demoteToCold(ts, origin)
	f.cold = eventShardByName(t, ts, origin)
	return f
}

// closeCold closes the cold placement's shard through the idle sweep, so the
// next tx opens it as the write path does.
func (f *relhistFixture) closeCold(t *testing.T) {
	t.Helper()
	if f.cold == nil {
		return
	}
	f.ts.SetIdleTimeoutForTest(0)
	time.Sleep(2 * time.Millisecond) // past the last access
	f.ts.CloseIdleShardsForTest()
	f.ts.SetIdleTimeoutForTest(time.Hour) // the tx's checkouts keep it open
	f.cold.LockShardMuForTest()
	open := f.cold.Store() != nil
	f.cold.UnlockShardMuForTest()
	if open {
		t.Fatal("setup: the idle sweep left the cold shard open")
	}
}

// relhistArchiveFixture archives both reference nodes (the rel moves with
// them) after their first versions and updates them after: the history spans
// the reference shard and the archive.
func relhistArchiveFixture(t *testing.T, above bool) *relhistFixture {
	g, ts := newTestTieredGraph(t)
	f := relhistBuild(t, g, ts, "Case", 3, false)
	if err := ts.ArchiveNode(f.a); err != nil {
		t.Fatalf("ArchiveNode a: %v", err)
	}
	if err := ts.ArchiveNode(f.b); err != nil {
		t.Fatalf("ArchiveNode b: %v", err)
	}
	ctx := context.Background()
	for i := 10; i < 12; i++ {
		if _, err := g.Nodes.Update(ctx, f.a, map[string]any{"k": int64(i)}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels.Update(ctx, f.rel, map[string]any{"w": int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if above {
		f.correct(t)
	}
	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("setup: no archive")
	}
	if !archive.HasNodeID(f.a.SnowflakeID()) {
		t.Fatal("setup: the node is not on the archive")
	}
	for name, s := range map[string]*tiered.BadgerStore{"reference": ts.RefShardForTest(), "archive": archive} {
		nh, err := s.GetNodeHistory(f.a)
		if err != nil {
			t.Fatal(err)
		}
		rh, err := s.GetRelHistory(f.rel)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s holds %d node and %d rel history rows", name, len(nh), len(rh))
	}
	return f
}

// TestRELHISTTxRollbackRestoresTiered: a rolled-back GraphTx leaves every
// row, history row, adjacency entry and index answer as before the tx, on
// every tiered placement (rewrite at rollback) and on memory and badger (the
// rollback trim), with and without history rows above the current version.
// Guards the snapshot change; passes before it.
func TestRELHISTTxRollbackRestoresTiered(t *testing.T) {
	t.Parallel()
	placements := []struct {
		name  string
		cold  bool
		build func(t *testing.T, above bool) *relhistFixture
	}{
		{"reference", false, func(t *testing.T, above bool) *relhistFixture {
			g, ts := newTestTieredGraph(t)
			return relhistBuild(t, g, ts, "Case", 4, above)
		}},
		{"event", false, func(t *testing.T, above bool) *relhistFixture {
			g, ts := newTestTieredGraph(t)
			return relhistBuild(t, g, ts, "Signal", 4, above)
		}},
		{"cold shard", true, relhistColdFixture},
		{"archive", false, relhistArchiveFixture},
		// The rollback-trim stores: one trim plus the copied rows. Without a
		// head entry (evicted, or a chain it refuses) every update snapshot
		// takes that path; with one, an update keeps the 68b shape.
		{"memory", false, func(t *testing.T, above bool) *relhistFixture {
			return relhistBuild(t, newTxTestGraph(t), nil, "Signal", 4, above)
		}},
		{"memory without head entries", false, func(t *testing.T, above bool) *relhistFixture {
			g := newTxTestGraph(t)
			g.headCacheOff = true
			return relhistBuild(t, g, nil, "Signal", 4, above)
		}},
		{"badger without head entries", false, func(t *testing.T, above bool) *relhistFixture {
			g, _ := newBadgerCoreForIndexes(t)
			g.headCacheOff = true
			return relhistBuild(t, g, nil, "Signal", 4, above)
		}},
		// Sharded pages history but has no rollback trim: the rewrite branch.
		{"sharded", false, func(t *testing.T, above bool) *relhistFixture {
			return relhistBuild(t, relhistShardedGraph(t), nil, "Signal", 4, above)
		}},
	}
	for _, p := range placements {
		for _, above := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/above=%v", p.name, above), func(t *testing.T) {
				if p.cold && testing.Short() && !isRaceEnabled() {
					t.Skip("long tier: a disk cold shard reopened per tx (about 10 s)")
				}
				t.Parallel()
				f := p.build(t, above)
				if err := f.g.Index.CreateProperty(f.label, "k"); err != nil {
					t.Fatal(err)
				}
				// Each rollback restores the state, so the ops share one fixture.
				for _, op := range relhistOps() {
					f.closeCold(t)
					relhistRollback(t, f, op)
				}
			})
		}
	}
}

// TestRELHISTTxSnapshotCopyBounded: the rows a tx snapshot copies do not grow
// with the entity's versions. A delete copies none (no row at or above the
// current version); an update of an entity with rows above its current
// version copies those rows only.
func TestRELHISTTxSnapshotCopyBounded(t *testing.T) {
	t.Parallel()
	shapes := []struct {
		name  string
		above bool
		max   int
		op    func(tx *GraphTx, f *relhistFixture) error
	}{
		{"delete rel", false, 0, func(tx *GraphTx, f *relhistFixture) error { return tx.DeleteRelationship(f.rel) }},
		{"delete node with rel", false, 0, func(tx *GraphTx, f *relhistFixture) error { return tx.DeleteNode(f.a) }},
		{"update rel above current", true, 2, func(tx *GraphTx, f *relhistFixture) error {
			_, err := tx.UpdateRelationship(f.rel, map[string]any{"w": int64(1000)})
			return err
		}},
		{"update node above current", true, 2, func(tx *GraphTx, f *relhistFixture) error {
			_, err := tx.UpdateNode(f.a, map[string]any{"k": int64(1000)})
			return err
		}},
	}
	for _, s := range shapes {
		for _, n := range []int{1, 10, 100} {
			t.Run(fmt.Sprintf("%s/N=%d", s.name, n), func(t *testing.T) {
				g, ts := newTestTieredGraph(t)
				f := relhistBuild(t, g, ts, "Signal", n, s.above)
				before := f.dump(t)
				copied := 0
				g.historyCopy = func(rows int) { copied += rows }
				tx, err := g.BeginTx()
				if err != nil {
					t.Fatal(err)
				}
				if err := s.op(tx, f); err != nil {
					t.Fatal(err)
				}
				g.historyCopy = nil
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
				if after := f.dump(t); after != before {
					t.Fatalf("rollback did not restore the state:%s", dumpDiff(before, after))
				}
				t.Logf("RELHIST copy %s N=%d: %d rows", s.name, n, copied)
				if copied > s.max {
					t.Fatalf("the tx snapshot copied %d history rows at %d versions, want at most %d (the rows at or above the current version)", copied, n, s.max)
				}
			})
		}
	}
}

func relhistShardedGraph(t *testing.T) *Core {
	t.Helper()
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	g, err := New(Config{SnowflakeNodeID: 0, Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// relhistDeltaBadgerGraph is badger with history delta encoding at anchor
// interval 4: a history row whose version is not a multiple of 4 is stored
// as a delta against the row at the multiple below it.
func relhistDeltaBadgerGraph(t *testing.T) *Core {
	t.Helper()
	bs, err := badger.New(badger.Config{InMemory: true, HistoryDeltaEncoding: true, HistoryAnchorInterval: 4})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	g, err := New(Config{Store: bs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// excise voids a bounded interval before each entity's current row: void
// rows above the current version.
func (f *relhistFixture) excise(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	n, err := f.g.store.GetNode(f.a)
	if err != nil {
		t.Fatal(err)
	}
	vf := n.Temporal().ValidFrom
	if err := f.g.Temporal.ExciseNodeValidity(ctx, f.a, vf-1000, vf-500); err != nil {
		t.Fatal(err)
	}
	r, err := f.g.store.GetRelationship(f.rel)
	if err != nil {
		t.Fatal(err)
	}
	vf = r.Temporal().ValidFrom
	if err := f.g.Temporal.ExciseRelValidity(ctx, f.rel, vf-1000, vf-500); err != nil {
		t.Fatal(err)
	}
}

// reimport deletes the rel and imports it again under its ID, then updates
// it twice: a tombstone in the chain (no head entry ever qualifies).
func (f *relhistFixture) reimport(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	if err := f.g.Rels.Delete(ctx, f.rel); err != nil {
		t.Fatal(err)
	}
	a, err := f.g.store.GetNode(f.a)
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.g.store.GetNode(f.b)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.Rels.Import(ctx, f.rel, "OBSERVES", a, b, map[string]any{"w": int64(100)}); err != nil {
		t.Fatal(err)
	}
	for i := 101; i < 103; i++ {
		if _, err := f.g.Rels.Update(ctx, f.rel, map[string]any{"w": int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
}

// relhistVoidAbove fails unless the rel holds a void history row above its
// current version (the excise shape's premise).
func (f *relhistFixture) relhistVoidAbove(t *testing.T) {
	t.Helper()
	cur, err := f.g.store.GetRelationship(f.rel)
	if err != nil {
		t.Fatal(err)
	}
	hist, err := f.g.store.GetRelHistory(f.rel)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range hist {
		if h.Version() > cur.Version() && h.Temporal() != nil && h.Temporal().Void {
			return
		}
	}
	t.Fatal("setup: no void history row above the current version")
}

// TestRELHISTTxRollbackRestoresShapes: the rollback equality of
// TestRELHISTTxRollbackRestoresTiered (decoded history rows compared as wire
// bytes) on chains the copied page must carry as they are: void rows above
// the current version (an excise), a tombstone in the chain (a delete and
// re-import before the tx, then a correction), and on delta-encoded badger
// above-rows that are deltas anchored below the trim point (current version
// 6: rows 7 and 8, 7 a delta on 4) or a trim point at an anchor version
// (current version 8: the tx writes the anchor row 8 that rollback must
// remove). Every entity carries a 512-byte pad so a history row stores as a
// delta (badger keeps the full row when the delta is not smaller).
func TestRELHISTTxRollbackRestoresShapes(t *testing.T) {
	t.Parallel()
	placements := []struct {
		name  string
		graph func(t *testing.T) (*Core, *tiered.Store)
	}{
		{"memory without head entries", func(t *testing.T) (*Core, *tiered.Store) {
			g := newTxTestGraph(t)
			g.headCacheOff = true
			return g, nil
		}},
		{"badger deltas without head entries", func(t *testing.T) (*Core, *tiered.Store) {
			g := relhistDeltaBadgerGraph(t)
			g.headCacheOff = true
			return g, nil
		}},
		{"badger deltas", func(t *testing.T) (*Core, *tiered.Store) { return relhistDeltaBadgerGraph(t), nil }},
		{"tiered event", func(t *testing.T) (*Core, *tiered.Store) { return newTestTieredGraph(t) }},
		{"sharded", func(t *testing.T) (*Core, *tiered.Store) { return relhistShardedGraph(t), nil }},
	}
	shapes := []struct {
		name     string
		versions int
		prep     func(t *testing.T, f *relhistFixture)
	}{
		{"void rows above current", 4, func(t *testing.T, f *relhistFixture) { f.excise(t); f.relhistVoidAbove(t) }},
		{"tombstone in the chain", 4, func(t *testing.T, f *relhistFixture) { f.reimport(t); f.correct(t) }},
		{"deltas anchored below the trim point", 7, func(t *testing.T, f *relhistFixture) { f.correct(t) }},
		{"trim point at an anchor version", 9, func(t *testing.T, f *relhistFixture) { f.correct(t) }},
	}
	for _, p := range placements {
		for _, s := range shapes {
			t.Run(p.name+"/"+s.name, func(t *testing.T) {
				t.Parallel()
				g, ts := p.graph(t)
				f := relhistBuildPadded(t, g, ts, "Signal", s.versions, false, strings.Repeat("p", 512))
				s.prep(t, f)
				if err := f.g.Index.CreateProperty(f.label, "k"); err != nil {
					t.Fatal(err)
				}
				for _, op := range relhistOps() {
					relhistRollback(t, f, op)
				}
			})
		}
	}
}
