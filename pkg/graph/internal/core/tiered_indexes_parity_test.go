package core

// Backlog 10 (ai-soc request 3): the tiered store builds relationship-type
// temporal indexes and composite node indexes per shard. Parity oracle: the
// same scenario written to a badger graph and to a disk tiered graph whose
// rows span two event shards, the reference shard and the cross-shard
// split-write doors. The store-level candidate prune must drop exactly the
// rows badger's drops (a literal expected set, so both stores cannot be wrong
// together), both query doors (generic ByType with QueryOpts, named
// Temporal.RelsByTypeAt / CountByTypeAt) must answer as badger answers, and
// both must still hold after a hot->warm rotation, a cold demotion with the
// shard closed and lazily reopened, RunRepair and VerifyShard. Composite:
// exact sets across shards, after update and delete of a member, after a cold
// checkout.

import (
	"context"
	"errors"
	"slices"
	"sort"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func newDiskTieredCoreForIndexes(t *testing.T) (*Core, *tiered.Store) {
	t.Helper()
	ts, err := tiered.New(tiered.Config{
		DataDir:           t.TempDir(),
		RefLabels:         []string{"Case", "User"},
		ShardWindow:       7 * 24 * time.Hour,
		FlushInterval:     1<<63 - 1,
		MaxOpenColdShards: 4, // a cold shard a read reopens stays open
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	c, err := New(Config{SnowflakeNodeID: 0, Store: ts})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, ts
}

func newBadgerCoreForIndexes(t *testing.T) (*Core, *badger.Store) {
	t.Helper()
	bs, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	c, err := New(Config{Store: bs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, bs
}

// relTemporalWorld records one graph's rel IDs by scenario name.
type relTemporalWorld struct {
	c     *Core
	store storepkg.MandatoryStore
	ids   map[string]types.RelID
	names map[types.RelID]string
	nodes map[string]*types.Node
}

func newRelTemporalWorld(c *Core, s storepkg.MandatoryStore) *relTemporalWorld {
	return &relTemporalWorld{c: c, store: s, ids: map[string]types.RelID{}, names: map[types.RelID]string{}, nodes: map[string]*types.Node{}}
}

func (w *relTemporalWorld) node(t *testing.T, name, label string) {
	t.Helper()
	n, err := w.c.Nodes.Add(context.Background(), []string{label}, map[string]any{"name": name})
	if err != nil {
		t.Fatalf("add node %s: %v", name, err)
	}
	w.nodes[name] = n
}

func (w *relTemporalWorld) rel(t *testing.T, name, from, to string, validFrom, validTo types.Instant) {
	t.Helper()
	props := map[string]any{"name": name, types.ShadowValidFrom: validFrom}
	if validTo != 0 {
		props[types.ShadowValidTo] = validTo
	}
	r, err := w.c.Rels.Add(context.Background(), "HOP", w.nodes[from], w.nodes[to], props)
	if err != nil {
		t.Fatalf("add rel %s: %v", name, err)
	}
	w.ids[name] = r.ID()
	w.names[r.ID()] = name
}

// phase1 writes rows on the first event shard and the reference shard:
// a closed row, an open row, a row whose interval later moves (two-phase),
// a row later deleted, and the two cross-shard split-write shapes (E->R keeps
// its entity on the event shard, R->E on the reference shard).
func (w *relTemporalWorld) phase1(t *testing.T) {
	t.Helper()
	w.node(t, "s1", "Signal")
	w.node(t, "s2", "Signal")
	w.node(t, "case", "Case")
	w.rel(t, "closed", "s1", "s2", 1000, 2000)
	w.rel(t, "open", "s1", "s2", 3000, 0)
	w.rel(t, "moved", "s1", "s2", 1000, 0)
	w.rel(t, "deleted", "s1", "s2", 1000, 2000)
	w.rel(t, "crossER", "s1", "case", 1000, 2000)
	w.rel(t, "crossRE", "case", "s2", 1000, 2000)
}

// mutate moves "moved" off its first interval and deletes "deleted" — after
// the index exists, so maintenance (not the build) must keep both answers.
func (w *relTemporalWorld) mutate(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	if _, err := w.c.Temporal.SetRelVersionInterval(ctx, w.ids["moved"], 5000, 0, nil); err != nil {
		t.Fatalf("move interval: %v", err)
	}
	if err := w.c.Rels.Delete(ctx, w.ids["deleted"]); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

// phase2 writes rows after the rotation: a same-shard row on the new hot
// shard and a cross-event-shard row (entity on the new shard).
func (w *relTemporalWorld) phase2(t *testing.T) {
	t.Helper()
	w.node(t, "s3", "Signal")
	w.node(t, "s4", "Signal")
	w.rel(t, "late", "s3", "s4", 6000, 7000)
	w.rel(t, "lateCross", "s3", "s1", 6000, 7000)
}

func (w *relTemporalWorld) allIDs() []types.RelID {
	out := make([]types.RelID, 0, len(w.ids))
	for _, id := range w.ids {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

func (w *relTemporalWorld) relNames(rels []*types.Relationship) []string {
	out := make([]string, 0, len(rels))
	for _, r := range rels {
		out = append(out, w.names[r.ID()])
	}
	sort.Strings(out)
	return out
}

func (w *relTemporalWorld) prune(t *testing.T, opts storepkg.QueryOpts) ([]string, bool) {
	t.Helper()
	cap, ok := w.store.(storepkg.RelTypeTemporalCandidateCapability)
	if !ok {
		t.Fatalf("%T does not implement RelTypeTemporalCandidateCapability", w.store)
	}
	tok, found := w.c.relTypes.Lookup("HOP")
	if !found {
		t.Fatal("HOP not registered")
	}
	kept, pruned := cap.PruneRelTypeTemporalCandidates(tok, w.allIDs(), opts)
	out := make([]string, 0, len(kept))
	for _, id := range kept {
		out = append(out, w.names[id])
	}
	sort.Strings(out)
	return out, pruned
}

type pruneCase struct {
	name string
	opts storepkg.QueryOpts
	want []string // kept, sorted
}

// pruneCases lists the literal kept sets once both phases are written: an
// envelope keeps every row whose union of intervals can overlap the probe;
// the deleted row is no longer covered by any index and is always kept.
func pruneCases() []pruneCase {
	return []pruneCase{
		{"at 1500", storepkg.QueryOpts{ValidAt: 1500}, []string{"closed", "crossER", "crossRE", "deleted", "moved"}},
		{"at 2500", storepkg.QueryOpts{ValidAt: 2500}, []string{"deleted", "moved"}},
		{"at 3500", storepkg.QueryOpts{ValidAt: 3500}, []string{"deleted", "moved", "open"}},
		{"at 6500", storepkg.QueryOpts{ValidAt: 6500}, []string{"deleted", "late", "lateCross", "moved", "open"}},
		{"during 2100-2900", storepkg.QueryOpts{ValidStart: 2100, ValidEnd: 2900}, []string{"deleted", "moved"}},
		{"during 6100-6200", storepkg.QueryOpts{ValidStart: 6100, ValidEnd: 6200}, []string{"deleted", "late", "lateCross", "moved", "open"}},
	}
}

// assertRelTemporalParity checks the prune (literal set on both stores) and
// every query door (tiered == badger) for each case. coldRows are the rows
// whose shard is cold: tiered keeps no relationship temporal index there (the
// hot + warm bound), so its prune keeps them whatever the filter.
func assertRelTemporalParity(t *testing.T, stage string, bw, tw *relTemporalWorld, coldRows ...string) {
	t.Helper()
	for _, pc := range pruneCases() {
		bKept, bOK := bw.prune(t, pc.opts)
		tKept, tOK := tw.prune(t, pc.opts)
		if !bOK || !slices.Equal(bKept, pc.want) {
			t.Errorf("%s %s: badger prune = %v (ok=%v), want %v", stage, pc.name, bKept, bOK, pc.want)
		}
		tWant := slices.Clone(pc.want)
		for _, r := range coldRows {
			if !slices.Contains(tWant, r) {
				tWant = append(tWant, r)
			}
		}
		sort.Strings(tWant)
		if !tOK || !slices.Equal(tKept, tWant) {
			t.Errorf("%s %s: tiered prune = %v (ok=%v), want %v", stage, pc.name, tKept, tOK, tWant)
		}

		bRels, err := bw.c.Rels.ByType("HOP", pc.opts)
		if err != nil {
			t.Fatalf("%s %s: badger ByType: %v", stage, pc.name, err)
		}
		tRels, err := tw.c.Rels.ByType("HOP", pc.opts)
		if err != nil {
			t.Fatalf("%s %s: tiered ByType: %v", stage, pc.name, err)
		}
		if b, tt := bw.relNames(bRels), tw.relNames(tRels); !slices.Equal(b, tt) {
			t.Errorf("%s %s: ByType tiered %v, badger %v", stage, pc.name, tt, b)
		}
		bCount, err := bw.c.Rels.CountByTypeAt("HOP", pc.opts)
		if err != nil {
			t.Fatalf("%s %s: badger CountByTypeAt: %v", stage, pc.name, err)
		}
		tCount, err := tw.c.Rels.CountByTypeAt("HOP", pc.opts)
		if err != nil {
			t.Fatalf("%s %s: tiered CountByTypeAt: %v", stage, pc.name, err)
		}
		if bCount != tCount || bCount != len(bRels) {
			t.Errorf("%s %s: CountByTypeAt tiered %d, badger %d, badger ByType %d", stage, pc.name, tCount, bCount, len(bRels))
		}
		if pc.opts.ValidAt == 0 {
			continue
		}
		bAt, err := bw.c.Temporal.RelsByTypeAt("HOP", pc.opts.ValidAt)
		if err != nil {
			t.Fatalf("%s %s: badger RelsByTypeAt: %v", stage, pc.name, err)
		}
		tAt, err := tw.c.Temporal.RelsByTypeAt("HOP", pc.opts.ValidAt)
		if err != nil {
			t.Fatalf("%s %s: tiered RelsByTypeAt: %v", stage, pc.name, err)
		}
		if b, tt := bw.relNames(bAt), tw.relNames(tAt); !slices.Equal(b, tt) {
			t.Errorf("%s %s: RelsByTypeAt tiered %v, badger %v", stage, pc.name, tt, b)
		}
	}
}

// The two-phase answer the rule-15 test pins: "moved" held [1000, 5000) at
// t=1500 and must still be returned there after its interval moved.
func assertMovedRemembered(t *testing.T, stage string, w *relTemporalWorld) {
	t.Helper()
	rels, err := w.c.Temporal.RelsByTypeAt("HOP", 1500)
	if err != nil {
		t.Fatalf("%s: RelsByTypeAt: %v", stage, err)
	}
	if !slices.Contains(w.relNames(rels), "moved") {
		t.Errorf("%s: RelsByTypeAt(1500) = %v, must contain the moved row's past version", stage, w.relNames(rels))
	}
	rels, err = w.c.Temporal.RelsByTypeAt("HOP", 2500)
	if err != nil {
		t.Fatalf("%s: RelsByTypeAt: %v", stage, err)
	}
	if slices.Contains(w.relNames(rels), "closed") || slices.Contains(w.relNames(rels), "crossER") {
		t.Errorf("%s: RelsByTypeAt(2500) = %v, closed rows must not appear", stage, w.relNames(rels))
	}
}

func hotShardName(ts *tiered.Store) string {
	ts.MuForTest().RLock()
	defer ts.MuForTest().RUnlock()
	return ts.HotShardForTest().Name()
}

func TestTieredRelTemporalPrune_ParityWithBadger_RotationColdRepair(t *testing.T) {
	bc, bs := newBadgerCoreForIndexes(t)
	tc, ts := newDiskTieredCoreForIndexes(t)
	bw, tw := newRelTemporalWorld(bc, bs), newRelTemporalWorld(tc, ts)

	bw.phase1(t)
	tw.phase1(t)
	for _, w := range []*relTemporalWorld{bw, tw} {
		if err := w.c.Index.CreateRelTemporal("HOP"); err != nil {
			t.Fatalf("CreateRelTemporal on %T: %v", w.store, err)
		}
		w.mutate(t)
	}
	first := hotShardName(ts)
	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	bw.phase2(t)
	tw.phase2(t)
	if hotShardName(ts) == first {
		t.Fatal("rotation did not open a new hot shard")
	}
	assertRelTemporalParity(t, "after rotation", bw, tw)
	assertMovedRemembered(t, "after rotation", tw)

	// Cold checkout: the first shard is demoted and closed as an idle cold
	// shard would be. The doors reopen it — as a cold shard, without its
	// relationship temporal index (hot + warm bound) — so its rows are kept by
	// the prune and every door still answers as badger.
	demoteToCold(ts, first)
	closeEventShardStore(t, eventShardByName(t, ts, first))
	if _, err := tc.Rels.ByType("HOP", storepkg.QueryOpts{ValidAt: 1500}); err != nil {
		t.Fatalf("read reopening the cold shard: %v", err)
	}
	if eventShardByName(t, ts, first).Store() == nil {
		t.Fatal("cold shard not reopened by the read")
	}
	coldRows := []string{"closed", "crossER", "deleted", "moved", "open"}
	assertRelTemporalParity(t, "after cold checkout", bw, tw, coldRows...)
	assertMovedRemembered(t, "after cold checkout", tw)

	if _, err := ts.RunRepair(); err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	for _, name := range []string{first, hotShardName(ts)} {
		res, err := tc.Admin.VerifyShard(name)
		if err != nil {
			t.Fatalf("VerifyShard(%s): %v", name, err)
		}
		if res == nil || res.NodesFailed != 0 || res.RelsFailed != 0 {
			t.Fatalf("VerifyShard(%s) = %+v, want no failures", name, res)
		}
	}
	assertRelTemporalParity(t, "after repair and verify", bw, tw, coldRows...)
	if got, err := tc.Index.ListRelTemporal(); err != nil || !slices.Equal(got, []string{"HOP"}) {
		t.Errorf("ListRelTemporal = %v, %v; want [HOP]", got, err)
	}
}

func TestTieredRelTemporalIndex_DropRemovesEverywhere(t *testing.T) {
	tc, ts := newDiskTieredCoreForIndexes(t)
	tw := newRelTemporalWorld(tc, ts)
	tw.phase1(t)
	if err := tc.Index.CreateRelTemporal("HOP"); err != nil {
		t.Fatalf("CreateRelTemporal: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if err := tc.Index.CreateRelTemporal("HOP"); !errors.Is(err, storepkg.ErrTemporalIndexExists) {
		t.Errorf("duplicate CreateRelTemporal: err = %v, want ErrTemporalIndexExists", err)
	}
	if err := tc.Index.DeleteRelTemporal("Nope"); !errors.Is(err, storepkg.ErrTemporalIndexNotFound) {
		t.Errorf("DeleteRelTemporal unknown type: err = %v, want ErrTemporalIndexNotFound", err)
	}
	if err := tc.Index.DeleteRelTemporal("HOP"); err != nil {
		t.Fatalf("DeleteRelTemporal: %v", err)
	}
	if err := tc.Index.DeleteRelTemporal("HOP"); !errors.Is(err, storepkg.ErrTemporalIndexNotFound) {
		t.Errorf("DeleteRelTemporal twice: err = %v, want ErrTemporalIndexNotFound", err)
	}
	if kept, ok := tw.prune(t, storepkg.QueryOpts{ValidAt: 2500}); ok || len(kept) != len(tw.ids) {
		t.Errorf("prune after drop = %v (ok=%v), want every id kept, ok=false", kept, ok)
	}
	if has, err := tc.Index.HasRelTemporal("HOP"); err != nil || has {
		t.Errorf("HasRelTemporal after drop = %v, %v; want false", has, err)
	}
	stores, release, err := ts.AllShardStoresWithLazyOpenForTest()
	if err != nil {
		t.Fatalf("shard stores: %v", err)
	}
	defer release()
	if len(stores) < 3 {
		t.Fatalf("shard stores = %d, want reference + two event shards", len(stores))
	}
	for _, ns := range stores {
		if got, err := ns.StoreForTest().RelTemporalIndexTypes(); err != nil || len(got) != 0 {
			t.Errorf("a shard still lists rel temporal types %v (err %v) after drop", got, err)
		}
	}
}

// --- composite ---

type compositeWorld struct {
	c     *Core
	names map[types.NodeID]string
	nodes map[string]*types.Node
}

func (w *compositeWorld) add(t *testing.T, name, device string, pid int64, ctime int64) {
	t.Helper()
	n, err := w.c.Nodes.Add(context.Background(), []string{"Signal"}, map[string]any{
		"name": name, "device": device, "pid": pid, "ctime": ctime,
	})
	if err != nil {
		t.Fatalf("add %s: %v", name, err)
	}
	w.names[n.ID()] = name
	w.nodes[name] = n
}

func (w *compositeWorld) lookup(t *testing.T, device string, pid, ctime int64) []string {
	t.Helper()
	nodes, err := w.c.Nodes.ByLabelAndProperties("Signal", map[string]any{"device": device, "pid": pid, "ctime": ctime}, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperties: %v", err)
	}
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, w.names[n.ID()])
	}
	sort.Strings(out)
	return out
}

type compositeCase struct {
	device     string
	pid, ctime int64
	want       []string
}

func assertCompositeParity(t *testing.T, stage string, bw, tw *compositeWorld, cases []compositeCase) {
	t.Helper()
	for _, cc := range cases {
		b, tt := bw.lookup(t, cc.device, cc.pid, cc.ctime), tw.lookup(t, cc.device, cc.pid, cc.ctime)
		if !slices.Equal(b, cc.want) || !slices.Equal(tt, cc.want) {
			t.Errorf("%s (%s,%d,%d): tiered %v, badger %v, want %v", stage, cc.device, cc.pid, cc.ctime, tt, b, cc.want)
		}
	}
}

func TestTieredComposite_ExactSetsAcrossShardsUpdateDeleteCold(t *testing.T) {
	bc, _ := newBadgerCoreForIndexes(t)
	tc, ts := newDiskTieredCoreForIndexes(t)
	bw := &compositeWorld{c: bc, names: map[types.NodeID]string{}, nodes: map[string]*types.Node{}}
	tw := &compositeWorld{c: tc, names: map[types.NodeID]string{}, nodes: map[string]*types.Node{}}
	keys := []string{"device", "pid", "ctime"}

	for _, w := range []*compositeWorld{bw, tw} {
		w.add(t, "a1", "dev-1", 10, 100)
		w.add(t, "a2", "dev-1", 11, 100)
		w.add(t, "a3", "dev-2", 10, 100)
		w.add(t, "dupA", "dev-9", 99, 900)
		if err := w.c.Index.CreateComposite("Signal", keys); err != nil {
			t.Fatalf("CreateComposite: %v", err)
		}
	}
	first := hotShardName(ts)
	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	ctx := context.Background()
	for _, w := range []*compositeWorld{bw, tw} {
		w.add(t, "b1", "dev-1", 10, 200)
		w.add(t, "dupB", "dev-9", 99, 900) // same tuple as dupA, on the other shard
		// Update a member on the first shard: its old tuple must stop matching.
		if _, err := w.c.Nodes.Update(ctx, w.nodes["a2"].ID(), map[string]any{"pid": int64(12)}); err != nil {
			t.Fatalf("update a2: %v", err)
		}
		if err := w.c.Nodes.Delete(ctx, w.nodes["a3"].ID()); err != nil {
			t.Fatalf("delete a3: %v", err)
		}
	}
	cases := []compositeCase{
		{"dev-1", 10, 100, []string{"a1"}},
		{"dev-1", 11, 100, []string{}},
		{"dev-1", 12, 100, []string{"a2"}},
		{"dev-2", 10, 100, []string{}},
		{"dev-1", 10, 200, []string{"b1"}},
		{"dev-9", 99, 900, []string{"dupA", "dupB"}},
		{"dev-1", 10, 999, []string{}},
	}
	assertCompositeParity(t, "after rotation", bw, tw, cases)

	demoteToCold(ts, first)
	closeEventShardStore(t, eventShardByName(t, ts, first))
	assertCompositeParity(t, "after cold checkout", bw, tw, cases)

	if got, err := tc.Index.ListComposites("Signal"); err != nil || len(got) != 1 || !slices.Equal(got[0], keys) {
		t.Errorf("ListComposites = %v, %v; want [%v]", got, err, keys)
	}
	if has, err := tc.Index.HasComposite("Signal", keys); err != nil || !has {
		t.Errorf("HasComposite = %v, %v; want true", has, err)
	}
	// Every shard maintains its own entries (the lookup is the index, not a
	// fallback label scan on some shards).
	stores, release, err := ts.AllShardStoresWithLazyOpenForTest()
	if err != nil {
		t.Fatalf("shard stores: %v", err)
	}
	tok, _ := tc.labels.Lookup("Signal")
	for _, ns := range stores {
		if got, err := ns.StoreForTest().ListCompositePropertyIndexes(tok); err != nil || len(got) != 1 {
			t.Errorf("a shard lists composites %v (err %v), want one definition", got, err)
		}
	}
	release()

	if err := tc.Index.DeleteComposite("Nope", keys); !errors.Is(err, storepkg.ErrIndexNotFound) {
		t.Errorf("DeleteComposite unknown label: err = %v, want ErrIndexNotFound", err)
	}
	if err := tc.Index.DeleteComposite("Signal", keys); err != nil {
		t.Fatalf("DeleteComposite: %v", err)
	}
	if err := tc.Index.DeleteComposite("Signal", keys); !errors.Is(err, storepkg.ErrIndexNotFound) {
		t.Errorf("DeleteComposite twice: err = %v, want ErrIndexNotFound", err)
	}
	// Answers stay correct without the index (fallback scan on every shard).
	if got := tw.lookup(t, "dev-9", 99, 900); !slices.Equal(got, []string{"dupA", "dupB"}) {
		t.Errorf("after drop lookup = %v, want [dupA dupB]", got)
	}
}

// ArchiveNode moves a reference relationship's row to the archive and leaves
// its history on the reference shard; RestoreNode moves the row back. After
// that round trip the reference shard's envelope covers only the restored
// current row, so it must not prune: the moved row's past version still
// answers at t=1500 (two-phase), exactly as on badger.
func TestTieredRelTemporalPrune_ArchiveRestoreKeepsHistory(t *testing.T) {
	bc, bs := newBadgerCoreForIndexes(t)
	tc, ts := newDiskTieredCoreForIndexes(t)
	bw, tw := newRelTemporalWorld(bc, bs), newRelTemporalWorld(tc, ts)
	for _, w := range []*relTemporalWorld{bw, tw} {
		w.node(t, "c1", "Case")
		w.node(t, "c2", "Case")
		w.rel(t, "refMoved", "c1", "c2", 1000, 0)
		w.rel(t, "refClosed", "c1", "c2", 1000, 2000)
		if err := w.c.Index.CreateRelTemporal("HOP"); err != nil {
			t.Fatalf("CreateRelTemporal: %v", err)
		}
		if _, err := w.c.Temporal.SetRelVersionInterval(context.Background(), w.ids["refMoved"], 5000, 0, nil); err != nil {
			t.Fatalf("move interval: %v", err)
		}
	}
	if err := ts.ArchiveNode(tw.nodes["c1"].ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	if err := ts.RestoreNode(tw.nodes["c1"].ID()); err != nil {
		t.Fatalf("RestoreNode: %v", err)
	}
	if b, _ := bw.c.Temporal.RelsByTypeAt("HOP", 1500); !slices.Contains(bw.relNames(b), "refMoved") {
		t.Fatalf("oracle: badger RelsByTypeAt(1500) = %v, want refMoved's past version", bw.relNames(b))
	}
	for _, at := range []types.Instant{1500, 2500, 6000} {
		b, err := bw.c.Temporal.RelsByTypeAt("HOP", at)
		if err != nil {
			t.Fatal(err)
		}
		tt, err := tw.c.Temporal.RelsByTypeAt("HOP", at)
		if err != nil {
			t.Fatal(err)
		}
		if bn, tn := bw.relNames(b), tw.relNames(tt); !slices.Equal(bn, tn) {
			t.Errorf("RelsByTypeAt(%d): tiered %v, badger %v", at, tn, bn)
		}
		kept, ok := tw.prune(t, storepkg.QueryOpts{ValidAt: at})
		if !ok || !slices.Contains(kept, "refMoved") {
			t.Errorf("prune at %d = %v (ok=%v), must keep refMoved", at, kept, ok)
		}
	}
}

// Composite lookups follow the shard walk of the single-key door: the
// archive answers at DepthAll only, warm event shards drop out at DepthHot,
// and Limit/After page the merged ID order.
func TestTieredComposite_ArchiveDepthAndPaging(t *testing.T) {
	tc, ts := newDiskTieredCoreForIndexes(t)
	ctx := context.Background()
	names := map[types.NodeID]string{}
	add := func(name, label string) *types.Node {
		t.Helper()
		n, err := tc.Nodes.Add(ctx, []string{label}, map[string]any{"name": name, "device": "d", "pid": int64(1)})
		if err != nil {
			t.Fatalf("add %s: %v", name, err)
		}
		names[n.ID()] = name
		return n
	}
	keys := []string{"device", "pid"}
	for _, label := range []string{"Case", "Signal"} {
		if err := tc.Index.CreateComposite(label, keys); err != nil {
			t.Fatalf("CreateComposite %s: %v", label, err)
		}
	}
	add("caseLive", "Case")
	archived := add("caseArchived", "Case")
	add("sigWarm", "Signal")
	if err := ts.ArchiveNode(archived.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	add("sigHot", "Signal")

	lookup := func(label string, opts storepkg.QueryOpts) []string {
		t.Helper()
		nodes, err := tc.Nodes.ByLabelAndProperties(label, map[string]any{"device": "d", "pid": int64(1)}, opts)
		if err != nil {
			t.Fatalf("ByLabelAndProperties(%s, %+v): %v", label, opts, err)
		}
		out := make([]string, 0, len(nodes))
		for _, n := range nodes {
			out = append(out, names[n.ID()])
		}
		return out
	}
	if got := lookup("Case", storepkg.QueryOpts{}); !slices.Equal(got, []string{"caseLive", "caseArchived"}) {
		t.Errorf("Case DepthAll = %v, want [caseLive caseArchived]", got)
	}
	if got := lookup("Case", storepkg.QueryOpts{Depth: storepkg.DepthHot}); !slices.Equal(got, []string{"caseLive"}) {
		t.Errorf("Case DepthHot = %v, want [caseLive] (archive answers at DepthAll only)", got)
	}
	if got := lookup("Signal", storepkg.QueryOpts{}); !slices.Equal(got, []string{"sigWarm", "sigHot"}) {
		t.Errorf("Signal DepthAll = %v, want [sigWarm sigHot]", got)
	}
	if got := lookup("Signal", storepkg.QueryOpts{Depth: storepkg.DepthHot}); !slices.Equal(got, []string{"sigHot"}) {
		t.Errorf("Signal DepthHot = %v, want [sigHot]", got)
	}
	first := lookup("Signal", storepkg.QueryOpts{Limit: 1})
	if !slices.Equal(first, []string{"sigWarm"}) {
		t.Fatalf("Signal Limit 1 = %v, want [sigWarm]", first)
	}
	var warmID types.NodeID
	for id, n := range names {
		if n == "sigWarm" {
			warmID = id
		}
	}
	if got := lookup("Signal", storepkg.QueryOpts{Limit: 1, After: types.EntityID(warmID)}); !slices.Equal(got, []string{"sigHot"}) {
		t.Errorf("Signal page 2 = %v, want [sigHot]", got)
	}

	// Store-level break cases on the tiered door itself.
	tok, _ := tc.labels.Lookup("Signal")
	values := map[string]any{"device": "d", "pid": int64(1)}
	if _, err := ts.NodesByLabelAndProperties(0, values, storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Errorf("label 0: err = %v, want ErrInvalidStoreMutation", err)
	}
	if _, err := ts.NodesByLabelAndProperties(tok, values, storepkg.QueryOpts{Limit: -1}); !errors.Is(err, storepkg.ErrInvalidQueryLimit) {
		t.Errorf("negative limit: err = %v, want ErrInvalidQueryLimit", err)
	}
	if _, err := ts.NodesByLabelAndProperties(tok, values, storepkg.QueryOpts{Depth: 99}); !errors.Is(err, storepkg.ErrInvalidShardDepth) {
		t.Errorf("unknown depth: err = %v, want ErrInvalidShardDepth", err)
	}
	if _, err := ts.NodesByLabelAndProperties(tok, map[string]any{"device": "d"}, storepkg.QueryOpts{}); err == nil {
		t.Error("one-key values: want an error, got nil")
	}
}
