package badger

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RELDISKWIN: the on-disk membership of a definition (RelPropertyIndexOnDiskTypes,
// PropertyIndexOnDiskLabels) keeps each row's valid-time envelope in its member
// entry, so its windowed door answers what the RAM sidecar answers. These tests
// run one RAM store and one disk store on identical writes and compare them.

// spanWorld holds the registries one RAM store and one disk store share.
type spanWorld struct {
	t        *testing.T
	labels   *registrypkg.LabelRegistry
	relTypes *registrypkg.RelTypeRegistry
	r, d     uint16
	vk       string
}

func newSpanWorld(t *testing.T) *spanWorld {
	t.Helper()
	w := &spanWorld{t: t, labels: registrypkg.NewLabelRegistry(), relTypes: registrypkg.NewRelTypeRegistry(), vk: types.IndexablePropertyValueKey(int64(1))}
	w.d, _ = w.labels.GetOrCreate("D")
	w.r, _ = w.relTypes.GetOrCreate("R")
	return w
}

// open opens a store with the seat index on both an R relationship and a D
// node: in RAM, or kept on disk (dir "" : in memory). A directory that has the
// definitions keeps them.
func (w *spanWorld) open(dir string, disk, readOnly bool) *Store {
	w.t.Helper()
	cfg := Config{PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), LabelNames: w.labels, RelTypeNames: w.relTypes, ReadOnly: readOnly}
	if dir == "" {
		cfg.InMemory = true
	} else {
		cfg.Dir = dir
	}
	if disk {
		cfg.PropertyIndexOnDiskLabels, cfg.RelPropertyIndexOnDiskTypes = []string{"D"}, []string{"R"}
	}
	bs, err := New(cfg)
	if err != nil {
		w.t.Fatalf("open %q disk=%t readOnly=%t: %v", dir, disk, readOnly, err)
	}
	w.t.Cleanup(func() { _ = bs.Close() })
	return bs
}

// create makes the indexes and the endpoint nodes (writable stores).
func (w *spanWorld) create(bs *Store) {
	w.t.Helper()
	if err := bs.CreatePropertyIndex(w.d, "seat"); err != nil {
		w.t.Fatal(err)
	}
	if err := bs.CreateRelPropertyIndex(w.r, "seat"); err != nil {
		w.t.Fatal(err)
	}
	putTestNode(w.t, bs, 1, 9, nil)
	putTestNode(w.t, bs, 2, 9, nil)
}

func (w *spanWorld) rel(id, seat int64, version uint32, txFrom, txTo, vf, vt types.Instant) *types.Relationship {
	x := types.NewRelationship(types.RelID(snowflake.ID(id)), w.r, 1, 2)
	_ = x.SetProperty("seat", seat)
	x.SetVersion(version)
	x.SetTemporal(&types.TemporalMetadata{TxFrom: txFrom, TxTo: txTo, ValidFrom: vf, ValidTo: vt})
	return x
}

func (w *spanWorld) node(id, seat int64, version uint32, txFrom, txTo, vf, vt types.Instant) *types.Node {
	n := types.NewNode(types.NodeID(snowflake.ID(id)), w.d, nil)
	_ = n.SetProperty("seat", seat)
	n.SetVersion(version)
	n.SetTemporal(&types.TemporalMetadata{TxFrom: txFrom, TxTo: txTo, ValidFrom: vf, ValidTo: vt})
	return n
}

// nodeBase is added to a relationship's ID for the node twin of its scenario.
const nodeBase = 1000

// How a tombstone's delete treats the row's ValidTo.
const (
	tombWrite = iota // the delete wrote it (ValidTo = DeletedAt)
	tombKeep         // the row's believed close at or after the delete stays
	tombPlain        // a close before the delete stays
)

func stampTomb(tm *types.TemporalMetadata, deletedAt types.Instant, mode int) {
	tm.DeletedAt, tm.TxTo = deletedAt, deletedAt
	switch mode {
	case tombWrite:
		tm.ValidTo = deletedAt
	case tombKeep:
		tm.DeleteKeptValidTo = true
	}
}

func tomb(r *types.Relationship, deletedAt types.Instant, mode int) *types.Relationship {
	stampTomb(r.Temporal(), deletedAt, mode)
	return r
}

func tombNode(n *types.Node, deletedAt types.Instant, mode int) *types.Node {
	stampTomb(n.Temporal(), deletedAt, mode)
	return n
}

// both runs one write on each of the stores.
func must(t *testing.T, what string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

// scenarioA writes the rows of the first phase on bs, relationships 11 to 19
// and their node twins (id + nodeBase). Every transaction time is on the
// valid-time clock, as the sidecar's own tests do.
//
//	11  open since 100, replaced at 200 by a row valid [210, 260)
//	12  open since 100, deleted at 300 (the delete writes the ValidTo)
//	13  valid [100, 120)
//	14  open since 100, current
//	15  open since 100, replaced at 400 by a row of another seat
//	16  valid [100, 400), deleted at 350 keeping that ValidTo
//	17  created at 500 and replaced in the same millisecond by a row valid [510, 520)
//	18  open since 100, replaced at 200 by [150, 300), at 300 by [300, 400)
//	19  a history-only version valid [50, 80) (PutRelVersion) of a row open since 100
func (w *spanWorld) scenarioA(bs *Store) {
	t := w.t
	t.Helper()
	for _, off := range []int64{0, nodeBase} {
		rel := off == 0
		put := func(id, seat int64, version uint32, txFrom, txTo, vf, vt types.Instant) {
			t.Helper()
			if rel {
				must(t, "put", bs.PutRelationship(w.rel(id, seat, version, txFrom, txTo, vf, vt)))
			} else {
				must(t, "put", bs.PutNode(w.node(id+off, seat, version, txFrom, txTo, vf, vt)))
			}
		}
		replace := func(id, seat int64, cur [4]types.Instant, curVersion uint32, prev [4]types.Instant) {
			t.Helper() // current: txFrom, 0, vf, vt; prev: txFrom, txTo, vf, vt
			if rel {
				must(t, "replace", bs.ReplaceRelWithHistory(w.rel(id, seat, curVersion, cur[0], 0, cur[2], cur[3]), curVersion-1, w.rel(id, 1, curVersion-1, prev[0], prev[1], prev[2], prev[3])))
			} else {
				must(t, "replace", bs.ReplaceNodeWithHistory(w.node(id+off, seat, curVersion, cur[0], 0, cur[2], cur[3]), curVersion-1, w.node(id+off, 1, curVersion-1, prev[0], prev[1], prev[2], prev[3])))
			}
		}
		put(11, 1, 0, 100, 0, 100, 0)
		replace(11, 1, [4]types.Instant{200, 0, 210, 260}, 1, [4]types.Instant{100, 200, 100, 0})
		put(12, 1, 0, 100, 0, 100, 0)
		put(13, 1, 0, 100, 0, 100, 120)
		put(14, 1, 0, 100, 0, 100, 0)
		put(15, 1, 0, 100, 0, 100, 0)
		replace(15, 2, [4]types.Instant{400, 0, 410, 0}, 1, [4]types.Instant{100, 400, 100, 0})
		put(16, 1, 0, 100, 0, 100, 400)
		put(17, 1, 0, 500, 0, 500, 0)
		replace(17, 1, [4]types.Instant{500, 0, 510, 520}, 1, [4]types.Instant{500, 500, 500, 0})
		put(18, 1, 0, 100, 0, 100, 0)
		replace(18, 1, [4]types.Instant{200, 0, 150, 300}, 1, [4]types.Instant{100, 200, 100, 0})
		replace(18, 1, [4]types.Instant{300, 0, 300, 400}, 2, [4]types.Instant{200, 300, 150, 300})
		put(19, 1, 1, 100, 0, 100, 0)
		if rel {
			must(t, "version", bs.PutRelVersion(types.RelID(snowflake.ID(19)), 0, w.rel(19, 1, 0, 20, 100, 50, 80)))
		} else {
			must(t, "version", bs.PutNodeVersion(types.NodeID(snowflake.ID(19+off)), 0, w.node(19+off, 1, 0, 20, 100, 50, 80)))
		}
	}
}

// scenarioB deletes 12 (writing the ValidTo) and 16 (keeping it), and 14 at a
// later time, after the first phase.
func (w *spanWorld) scenarioB(bs *Store) {
	t := w.t
	t.Helper()
	must(t, "delete 12", bs.DeleteRelWithHistory(types.RelID(snowflake.ID(12)), 0, tomb(w.rel(12, 1, 0, 100, 0, 100, 0), 300, tombWrite)))
	must(t, "delete 16", bs.DeleteRelWithHistory(types.RelID(snowflake.ID(16)), 0, tomb(w.rel(16, 1, 0, 100, 0, 100, 400), 350, tombKeep)))
	must(t, "delete node 12", bs.DeleteNodeWithHistory(types.NodeID(snowflake.ID(12+nodeBase)), 0, tombNode(w.node(12+nodeBase, 1, 0, 100, 0, 100, 0), 300, tombWrite), nil))
	must(t, "delete node 16", bs.DeleteNodeWithHistory(types.NodeID(snowflake.ID(16+nodeBase)), 0, tombNode(w.node(16+nodeBase, 1, 0, 100, 0, 100, 400), 350, tombKeep), nil))
}

func (w *spanWorld) scenarioC(bs *Store) {
	t := w.t
	t.Helper()
	must(t, "delete 14", bs.DeleteRelWithHistory(types.RelID(snowflake.ID(14)), 0, tomb(w.rel(14, 1, 0, 100, 0, 100, 0), 600, tombWrite)))
	must(t, "delete node 14", bs.DeleteNodeWithHistory(types.NodeID(snowflake.ID(14+nodeBase)), 0, tombNode(w.node(14+nodeBase, 1, 0, 100, 0, 100, 0), 600, tombWrite), nil))
}

// windowOf returns the members (id -> first transaction time) of seat=1 the
// windowed door answers, relationships and nodes (its ids minus nodeBase).
func (w *spanWorld) windowOf(bs *Store, start, end, pin types.Instant) (rels, nodes map[int64]types.Instant) {
	w.t.Helper()
	rels, nodes = map[int64]types.Instant{}, map[int64]types.Instant{}
	must(w.t, "rel window", bs.ForEachRelPropertyTxMemberDuring(w.r, "seat", w.vk, start, end, pin, func(id types.RelID, tx types.Instant) bool {
		rels[int64(id)] = tx
		return true
	}))
	must(w.t, "node window", bs.ForEachNodePropertyTxMemberDuring(w.d, "seat", w.vk, start, end, pin, func(id types.NodeID, tx types.Instant) bool {
		nodes[int64(id)-nodeBase] = tx
		return true
	}))
	return rels, nodes
}

var (
	spanWindows = [][2]types.Instant{{types.UnboundedValidFrom, math.MaxInt64}, {50, 60}, {100, 101}, {130, 140}, {250, 270}, {510, 515}, {1000, math.MaxInt64}, {120, 120}, {200, 100}}
	spanPins    = []types.Instant{0, 10, 25, 99, 100, 150, 200, 250, 300, 350, 400, 450, 500, 501, 550, 1 << 40}
)

// agree fails for every window and pin on which the disk store's windowed
// door answers other than the per-row oracle (rowOracle, from the RAM store's
// rows), and for every member it answers that the RAM sidecar does not: the
// time-ordered read (RELDISKWIN option B) judges a member's rows one at a time,
// a subset of the sidecar's envelope, with the lowest transaction time of the
// rows that meet.
func (w *spanWorld) agree(ram, disk *Store, phase string) {
	w.t.Helper()
	for _, win := range spanWindows {
		for _, pin := range spanPins {
			w.agreeAt(ram, disk, phase, win[0], win[1], pin)
		}
	}
}

// agreeAt is agree at one window and pin.
func (w *spanWorld) agreeAt(ram, disk *Store, phase string, start, end, pin types.Instant) {
	w.t.Helper()
	wr, wn := w.windowOf(ram, start, end, pin)
	gr, gn := w.windowOf(disk, start, end, pin)
	or, on := w.rowOracle(ram, start, end, pin)
	if !maps.Equal(or, gr) {
		w.t.Errorf("%s: rel window [%d,%d)@%d: disk %v, per-row oracle %v (%v / %v)", phase, start, end, pin, memberIDs(gr), memberIDs(or), gr, or)
	}
	if !maps.Equal(on, gn) {
		w.t.Errorf("%s: node window [%d,%d)@%d: disk %v, per-row oracle %v (%v / %v)", phase, start, end, pin, memberIDs(gn), memberIDs(on), gn, on)
	}
	for id := range gr {
		if _, ok := wr[id]; !ok {
			w.t.Errorf("%s: rel window [%d,%d)@%d: disk answers %d, RAM does not", phase, start, end, pin, id)
		}
	}
	for id := range gn {
		if _, ok := wn[id]; !ok {
			w.t.Errorf("%s: node window [%d,%d)@%d: disk answers %d, RAM does not", phase, start, end, pin, id)
		}
	}
}

// rowOracle answers a window from bs's stored rows, not its index entries:
// for every relationship and node that ever carried seat=1, each row that
// carries it, with its span as the sidecar derives it (RowInfo.LateSpan over
// the entity's later rows, AnyPinSpan, Last), is judged alone
// (indexpkg.RowMeets); a member meets with its first meeting row's
// transaction time, the lowest.
func (w *spanWorld) rowOracle(bs *Store, start, end, pin types.Instant) (rels, nodes map[int64]types.Instant) {
	w.t.Helper()
	rels, nodes = map[int64]types.Instant{}, map[int64]types.Instant{}
	if start >= end {
		return rels, nodes
	}
	axis := bs.validAxis.Get()
	judge := func(out map[int64]types.Instant, key int64, infos []indexpkg.RowInfo, carries []bool) {
		for i, ri := range infos {
			if !carries[i] {
				continue
			}
			late := ri.LateSpan(infos[i+1:])
			if !indexpkg.RowMeets(ri.Span.From, ri.AnyPinSpan().To, late.To, ri.Last(), start, end, pin) {
				continue
			}
			if prev, ok := out[key]; !ok || ri.TxFrom < prev {
				out[key] = ri.TxFrom
			}
		}
	}
	var relIDs, nodeIDs []int64
	must(w.t, "members", bs.ForEachRelPropertyTxMember(w.r, "seat", w.vk, func(id types.RelID, _ types.Instant) bool { relIDs = append(relIDs, int64(id)); return true }))
	must(w.t, "members", bs.ForEachNodePropertyTxMember(w.d, "seat", w.vk, func(id types.NodeID, _ types.Instant) bool { nodeIDs = append(nodeIDs, int64(id)); return true }))
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	for _, id := range relIDs {
		rows, err := bs.getRelHistoryByPrefix(storepkg.HistRelPrefix(snowflake.ID(id)))
		must(w.t, "history", err)
		if r, err := bs.getRelLocked(types.RelID(snowflake.ID(id))); err == nil {
			rows = append(rows, r)
		}
		infos, carries := make([]indexpkg.RowInfo, len(rows)), make([]bool, len(rows))
		for i, r := range rows {
			infos[i] = indexpkg.RelRowInfo(axis, r)
			vk, _ := r.IndexablePropertyValueKey("seat")
			carries[i] = vk == w.vk
		}
		judge(rels, id, infos, carries)
	}
	for _, id := range nodeIDs {
		rows, err := bs.getNodeHistoryByPrefix(storepkg.HistNodePrefix(snowflake.ID(id)))
		must(w.t, "history", err)
		if n, err := bs.getNodeLocked(types.NodeID(snowflake.ID(id))); err == nil {
			rows = append(rows, n)
		}
		infos, carries := make([]indexpkg.RowInfo, len(rows)), make([]bool, len(rows))
		for i, n := range rows {
			infos[i] = indexpkg.NodeRowInfo(axis, n)
			vk, _ := n.IndexablePropertyValueKey("seat")
			carries[i] = vk == w.vk && n.HasLabelTokenRaw(w.d)
		}
		judge(nodes, id-nodeBase, infos, carries)
	}
	return rels, nodes
}

func memberIDs(m map[int64]types.Instant) []int64 {
	ids := slices.Sorted(maps.Keys(m))
	return ids
}

// TestMemberSpan_DiskMatchesSidecar: the same writes on a RAM store and a disk
// store, a create, an update that replaces an open row, two in a row, an update
// in the same millisecond as its create, a history-only version, a delete that
// writes the ValidTo and one that keeps it: the windowed door answers alike at
// every window and every pin (before and after each row's last stamp), for
// relationships and nodes, with the writes still buffered, flushed, and after a
// clean reopen.
func TestMemberSpan_DiskMatchesSidecar(t *testing.T) {
	t.Parallel()
	w := newSpanWorld(t)
	dir := t.TempDir()
	ram, disk := w.open("", false, false), w.open(dir, true, false)
	w.create(ram)
	w.create(disk)
	for _, phase := range []struct {
		name string
		do   func(*Store)
	}{{"A", w.scenarioA}, {"B", w.scenarioB}, {"C", w.scenarioC}} {
		phase.do(ram)
		phase.do(disk)
		w.agree(ram, disk, phase.name+" buffered")
		must(t, "flush", disk.Flush())
		w.agree(ram, disk, phase.name+" flushed")
	}
	must(t, "close", disk.Close())
	reopened := w.open(dir, true, false)
	w.agree(ram, reopened, "reopened")
	if got, ok := readRawMarker2(t, reopened); !ok || got != storepkg.MemberSpanStoreFormatVersion {
		t.Errorf("format marker %d (present %t), want %d", got, ok, storepkg.MemberSpanStoreFormatVersion)
	}
}

// TestMemberSpan_HandCheckedWindows: the rows of the sidecar's own test
// (TestPropTxWindow_WritePathImageScanAgree: 11 open since 100 and replaced at
// 200 by a row valid [210, 260); 12 open and deleted at 300; 13 valid [100,
// 120); 14 open; 15 open and replaced at 400 by a row of another seat) on a
// definition kept on disk answer its table: the late window [1000, inf) holds
// 14 only at the newest pin, and before each replacement or delete the open row
// answers too.
func TestMemberSpan_HandCheckedWindows(t *testing.T) {
	t.Parallel()
	w := newSpanWorld(t)
	bs := w.open("", true, false)
	w.create(bs)
	must(t, "put 11", bs.PutRelationship(w.rel(11, 1, 0, 100, 0, 100, 0)))
	must(t, "replace 11", bs.ReplaceRelWithHistory(w.rel(11, 1, 1, 200, 0, 210, 260), 0, w.rel(11, 1, 0, 100, 200, 100, 0)))
	must(t, "put 12", bs.PutRelationship(w.rel(12, 1, 0, 100, 0, 100, 0)))
	must(t, "delete 12", bs.DeleteRelWithHistory(types.RelID(12), 0, tomb(w.rel(12, 1, 0, 100, 0, 100, 0), 300, tombWrite)))
	must(t, "put 13", bs.PutRelationship(w.rel(13, 1, 0, 100, 0, 100, 120)))
	must(t, "put 14", bs.PutRelationship(w.rel(14, 1, 0, 100, 0, 100, 0)))
	must(t, "put 15", bs.PutRelationship(w.rel(15, 1, 0, 100, 0, 100, 0)))
	must(t, "replace 15", bs.ReplaceRelWithHistory(w.rel(15, 2, 1, 400, 0, 410, 0), 0, w.rel(15, 1, 0, 100, 400, 100, 0)))
	for _, c := range []struct {
		start, end, pin types.Instant
		want            string
	}{
		{1000, math.MaxInt64, 0, "[14]"},
		{1000, math.MaxInt64, 1 << 40, "[14]"},
		{1000, math.MaxInt64, 450, "[14]"},
		{1000, math.MaxInt64, 350, "[14 15]"},
		{1000, math.MaxInt64, 250, "[12 14 15]"},
		{1000, math.MaxInt64, 150, "[11 12 14 15]"},
		{100, 101, 0, "[11 12 13 14 15]"},
		{250, 270, 0, "[11 12 14 15]"},
	} {
		got, _ := w.windowOf(bs, c.start, c.end, c.pin)
		if g := fmt.Sprint(memberIDs(got)); g != c.want {
			t.Errorf("window [%d,%d)@%d = %s, want %s", c.start, c.end, c.pin, g, c.want)
		}
	}
}

// readRawMarker2 reads the store-level format marker of an open store.
func readRawMarker2(t *testing.T, bs *Store) (uint16, bool) {
	t.Helper()
	var v uint16
	found := false
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.WireFormatVersionKey)
		if errors.Is(err, badgerv4.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			v, found = uint16(val[0])<<8|uint16(val[1]), true
			return nil
		})
	})
	must(t, "marker", err)
	return v, found
}

// spanEntries returns the span keys of the definition's member head under
// kind, with their values.
func spanEntries(t *testing.T, bs *Store, kind byte, scope uint16) map[string][]byte {
	t.Helper()
	tok, ok := bs.propKeyTokenFor("seat")
	if !ok {
		t.Fatal("no token for seat")
	}
	out := map[string][]byte{}
	must(t, "flush", bs.Flush())
	must(t, "view", bs.db.View(func(txn *badgerv4.Txn) error {
		it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: storepkg.AppendEntityIndexHead(nil, kind, scope, tok), PrefetchValues: true})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			out[string(it.Item().KeyCopy(nil))] = v
		}
		return nil
	}))
	return out
}

// TestMemberSpan_SameMillisecondKeepsBothEntries: a create and an update of one
// entity in one millisecond share value, ID and TxFrom; the version in the key
// keeps the update's narrow span from overwriting the create's open one, so the
// entity still answers a window only the open row meets.
func TestMemberSpan_SameMillisecondKeepsBothEntries(t *testing.T) {
	t.Parallel()
	w := newSpanWorld(t)
	disk := w.open("", true, false)
	w.create(disk)
	must(t, "put", disk.PutRelationship(w.rel(17, 1, 0, 500, 0, 500, 0)))
	must(t, "update", disk.ReplaceRelWithHistory(w.rel(17, 1, 1, 500, 0, 510, 520), 0, w.rel(17, 1, 0, 500, 500, 500, 0)))
	entries := spanEntries(t, disk, storepkg.EntityIndexRelSpan, w.r)
	if len(entries) != 2 {
		t.Fatalf("%d span entries for one entity updated in the millisecond of its create, want 2 (one per version)", len(entries))
	}
	var spans []string
	for _, v := range entries {
		from, allTo, lateTo, last, ok := storepkg.ParseEntityIndexSpanValue(v)
		if !ok {
			t.Fatalf("entry value of %d bytes", len(v))
		}
		spans = append(spans, fmt.Sprint(from, allTo, lateTo, last))
	}
	slices.Sort(spans)
	// v0: from 500, open, open at the same millisecond (not replaced later), last 500;
	// v1: from 510, to 520, last 500.
	if want := []string{fmt.Sprint(500, int64(math.MaxInt64), int64(math.MaxInt64), 500), fmt.Sprint(510, 520, 520, 500)}; !slices.Equal(spans, want) {
		t.Fatalf("spans %v, want %v", spans, want)
	}
	if late, recent := w.timeEntries(disk); late != 2 || recent != 0 {
		t.Fatalf("%d late-axis and %d recent-axis entries, want 2 and 0 (one per version; neither row widened)", late, recent)
	}
	got, _ := w.windowOf(disk, 1000, math.MaxInt64, 0)
	if _, ok := got[17]; !ok {
		t.Fatal("17 does not meet [1000, inf): its open first row was overwritten by the narrow second")
	}
}

// downgradeMembersToLegacy rewrites the closed store's span entries as the
// member entries before RELDISKWIN (no version in the key, no value), the
// built markers as value 1 and the format marker as 16.
func downgradeMembersToLegacy(t *testing.T, dir string) (entries int) {
	t.Helper()
	db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("raw close: %v", err)
		}
	}()
	var del, set [][]byte
	var markers [][]byte
	must(t, "scan", db.View(func(txn *badgerv4.Txn) error {
		it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: []byte{storepkg.KeyEntityIndex}})
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().KeyCopy(nil)
			if storepkg.EntityIndexIsTime(k[1]) { // the legacy layout had no time-ordered entries
				del = append(del, k)
				continue
			}
			if !storepkg.EntityIndexIsSpan(k[1]) {
				continue
			}
			del = append(del, k)
			legacy := slices.Clone(k[:len(k)-4])
			legacy[1] = map[byte]byte{storepkg.EntityIndexRelSpan: storepkg.EntityIndexRelMember, storepkg.EntityIndexNodeSpan: storepkg.EntityIndexNodeMember}[k[1]]
			set = append(set, legacy)
		}
		it.Close()
		mit := txn.NewIterator(badgerv4.IteratorOptions{Prefix: storepkg.EntityIndexBuiltPrefix})
		defer mit.Close()
		for mit.Rewind(); mit.Valid(); mit.Next() {
			markers = append(markers, mit.Item().KeyCopy(nil))
		}
		return nil
	}))
	wb := db.NewWriteBatch()
	defer wb.Cancel()
	for _, k := range del {
		must(t, "delete", wb.Delete(k))
	}
	for _, k := range set {
		must(t, "set", wb.Set(k, []byte{}))
	}
	for _, k := range markers {
		must(t, "marker", wb.Set(k, []byte{storepkg.EntityIndexBuiltLegacy}))
	}
	must(t, "format marker", wb.Set(storepkg.WireFormatVersionKey, formatMarkerValue(storepkg.UniqueOwnerStoreFormatVersion)))
	must(t, "flush", wb.Flush())
	return len(set)
}

// membersOfValue returns every relationship and node that ever carried seat=1
// (the legacy answer of every window: the value's members).
func (w *spanWorld) membersOfValue(bs *Store) (rels, nodes map[int64]types.Instant) {
	w.t.Helper()
	return w.windowOf(bs, types.UnboundedValidFrom, math.MaxInt64, 0)
}

// TestMemberSpan_LegacyDefinitionBroadThenRebuilt: a store whose built markers
// say value 1 (the member entries before RELDISKWIN, here made from a store
// the new code wrote) answers a windowed lookup with every member of the value
// when opened read-only, the broad answer it always gave; its non-windowed
// doors answer from the same entries. A writable open builds the definitions
// again: the legacy entries are gone, the markers say 2, the format marker 17,
// and the windows narrow to what a RAM store answers. A copy of the legacy
// store opened read-only after that stays broad (the copy was not rebuilt).
func TestMemberSpan_LegacyDefinitionBroadThenRebuilt(t *testing.T) {
	t.Parallel()
	w := newSpanWorld(t)
	ram := w.open("", false, false)
	w.create(ram)
	w.scenarioA(ram)
	w.scenarioB(ram)
	dir := t.TempDir()
	disk := w.open(dir, true, false)
	w.create(disk)
	w.scenarioA(disk)
	w.scenarioB(disk)
	must(t, "close", disk.Close())
	copyDir := copyStoreDir(t, dir) // a copy of the new-layout store, to downgrade apart
	if n := downgradeMembersToLegacy(t, dir); n == 0 {
		t.Fatal("no span entry to downgrade")
	}
	legacyCopy := copyStoreDir(t, dir) // the downgraded store, kept as it is

	// Read-only: broad.
	ro := w.open(dir, true, true)
	wantR, wantN := w.windowOf(ram, types.UnboundedValidFrom, math.MaxInt64, 0)
	for _, win := range [][2]types.Instant{{1000, math.MaxInt64}, {50, 60}, {250, 270}} {
		for _, pin := range []types.Instant{0, 150, 1 << 40} {
			gr, gn := w.windowOf(ro, win[0], win[1], pin)
			if !maps.Equal(gr, wantR) || !maps.Equal(gn, wantN) {
				t.Errorf("read-only legacy window [%d,%d)@%d: rels %v nodes %v, want every member %v %v", win[0], win[1], pin, memberIDs(gr), memberIDs(gn), memberIDs(wantR), memberIDs(wantN))
			}
		}
	}
	if gr, gn := w.windowOf(ro, 100, 100, 0); len(gr)+len(gn) != 0 {
		t.Errorf("read-only legacy empty window answered %v %v", memberIDs(gr), memberIDs(gn))
	}
	var plain []int64
	must(t, "members", ro.ForEachRelPropertyTxMember(w.r, "seat", w.vk, func(id types.RelID, _ types.Instant) bool { plain = append(plain, int64(id)); return true }))
	slices.Sort(plain)
	if !slices.Equal(plain, memberIDs(wantR)) {
		t.Errorf("legacy non-windowed members %v, want %v", plain, memberIDs(wantR))
	}
	must(t, "close", ro.Close())

	// Writable: rebuilt.
	rw := w.open(dir, true, false)
	w.agree(ram, rw, "rebuilt")
	if n := len(spanEntries(t, rw, storepkg.EntityIndexRelMember, w.r)) + len(spanEntries(t, rw, storepkg.EntityIndexNodeMember, w.d)); n != 0 {
		t.Errorf("%d legacy entries left after the rebuild", n)
	}
	for _, c := range []struct {
		kind  byte
		scope uint16
	}{{storepkg.EntityIndexRelMember, w.r}, {storepkg.EntityIndexNodeMember, w.d}} {
		if v := markerValue(t, rw, c.kind, c.scope); v != storepkg.EntityIndexBuiltSpan {
			t.Errorf("built marker of kind %d is %d, want %d", c.kind, v, storepkg.EntityIndexBuiltSpan)
		}
	}
	if got, _ := readRawMarker2(t, rw); got != storepkg.MemberSpanStoreFormatVersion {
		t.Errorf("format marker %d after the rebuild, want %d", got, storepkg.MemberSpanStoreFormatVersion)
	}
	must(t, "close", rw.Close())
	// The rebuilt store trusts its entries at the next open, read-only too.
	w.agree(ram, w.open(dir, true, true), "rebuilt, read-only")

	// The write path of the new layout and the rebuild's scan wrote alike.
	fresh, rebuilt := w.open(copyDir, true, false), w.open(copyStoreDir(t, dir), true, true)
	w.agree(ram, fresh, "the new layout as written")
	fl, fr := w.timeEntries(fresh)
	if rl, rr := w.timeEntries(rebuilt); fl != rl || fr != rr || fl == 0 || fr == 0 {
		t.Errorf("time-ordered entries: written %d late-axis %d recent-axis, rebuilt %d and %d", fl, fr, rl, rr)
	}
	for _, win := range spanWindows {
		for _, pin := range spanPins {
			ar, an := w.windowOf(fresh, win[0], win[1], pin)
			br, bn := w.windowOf(rebuilt, win[0], win[1], pin)
			if !maps.Equal(ar, br) || !maps.Equal(an, bn) {
				t.Errorf("rebuilt vs written [%d,%d)@%d differ", win[0], win[1], pin)
			}
		}
	}

	// A legacy store copy never opened writable stays broad.
	still := w.open(legacyCopy, true, true)
	if gr, _ := w.windowOf(still, 1000, math.MaxInt64, 0); !maps.Equal(gr, wantR) {
		t.Errorf("an unrebuilt legacy copy answered %v, want every member", memberIDs(gr))
	}
}

// markerValue returns the value of the built marker of the seat definition of
// kind (a member kind) and scope.
func markerValue(t *testing.T, bs *Store, kind byte, scope uint16) byte {
	t.Helper()
	var v byte
	must(t, "marker", bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.EntityIndexBuiltKey(kind, scope, "seat"))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error { v = val[0]; return nil })
	}))
	return v
}

// copyStoreDir copies a closed store's directory and returns the copy's path.
func copyStoreDir(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	must(t, "copy", filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o750)
		}
		in, err := os.Open(path) // #nosec G304 -- a test's own directory
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY, 0o600) // #nosec G304 -- a test's own directory
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		return err
	}))
	return dst
}

// TestMemberSpan_RebuildCrashLeavesNoTrustedMarker (docs/badger.md rule 1): the
// rebuild of a legacy definition is one WriteBatch that sets the built marker
// last. With a batch of several transactions, a failure after the entries went
// in leaves the definition without the new marker (the marker of the earlier
// layout is gone with the first part, so the next open builds again), and that
// next open answers as a RAM store does.
func TestMemberSpan_RebuildCrashLeavesNoTrustedMarker(t *testing.T) {
	w := newSpanWorld(t)
	dir := t.TempDir()
	const rows = 9000 // two entries each: above one transaction at 8 MB
	ram := w.open("", false, false)
	cfgOpen := func() *Store {
		bs, err := New(Config{Dir: dir, MemTableSize: 8 << 20, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), LabelNames: w.labels, RelTypeNames: w.relTypes,
			RelPropertyIndexOnDiskTypes: []string{"R"}})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		return bs
	}
	bs := cfgOpen()
	if err := bs.CreateRelPropertyIndex(w.r, "seat"); err != nil {
		t.Fatal(err)
	}
	if err := ram.CreateRelPropertyIndex(w.r, "seat"); err != nil {
		t.Fatal(err)
	}
	putTestNode(t, bs, 1, 9, nil)
	putTestNode(t, bs, 2, 9, nil)
	putTestNode(t, ram, 1, 9, nil)
	putTestNode(t, ram, 2, 9, nil)
	for i := int64(0); i < rows; i++ {
		from := types.Instant(1000 + 10*i)
		must(t, "put", bs.PutRelationship(w.rel(100+i, 1, 0, 100, 0, from, from+5)))
		must(t, "put ram", ram.PutRelationship(w.rel(100+i, 1, 0, 100, 0, from, from+5)))
	}
	must(t, "close", bs.Close())
	downgradeMembersToLegacy(t, dir)

	failAt := errors.New("injected failure before the marker")
	entityIndexOpenBeforeMarkHook = func() error { return failAt }
	_, err := New(Config{Dir: dir, MemTableSize: 8 << 20, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), LabelNames: w.labels, RelTypeNames: w.relTypes,
		RelPropertyIndexOnDiskTypes: []string{"R"}})
	entityIndexOpenBeforeMarkHook = nil
	if !errors.Is(err, failAt) {
		t.Fatalf("open with the injected failure: %v, want it", err)
	}
	// What the failed open left, read raw: no span marker; some entries of the batch
	// may be there (a prefix of its transactions).
	db, rerr := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
	if rerr != nil {
		t.Fatalf("raw open: %v", rerr)
	}
	var marker []byte
	var spans int
	must(t, "view", db.View(func(txn *badgerv4.Txn) error {
		if item, err := txn.Get(storepkg.EntityIndexBuiltKey(storepkg.EntityIndexRelMember, w.r, "seat")); err == nil {
			marker, _ = item.ValueCopy(nil)
		}
		it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: []byte{storepkg.KeyEntityIndex, storepkg.EntityIndexRelSpan}})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			spans++
		}
		return nil
	}))
	must(t, "raw close", db.Close())
	if len(marker) == 1 && marker[0] == storepkg.EntityIndexBuiltSpan {
		t.Fatalf("the failed rebuild left the span marker with %d span entries", spans)
	}
	t.Logf("after the failed rebuild: marker %v, %d span entries of the batch's committed parts", marker, spans)

	bs = cfgOpen()
	defer bs.Close()
	for _, win := range [][2]types.Instant{{1000, 1100}, {5000, 5003}, {types.UnboundedValidFrom, math.MaxInt64}} {
		got := map[int64]types.Instant{}
		want := map[int64]types.Instant{}
		must(t, "disk", bs.ForEachRelPropertyTxMemberDuring(w.r, "seat", w.vk, win[0], win[1], 0, func(id types.RelID, tx types.Instant) bool { got[int64(id)] = tx; return true }))
		must(t, "ram", ram.ForEachRelPropertyTxMemberDuring(w.r, "seat", w.vk, win[0], win[1], 0, func(id types.RelID, tx types.Instant) bool { want[int64(id)] = tx; return true }))
		if !maps.Equal(got, want) {
			t.Errorf("window [%d,%d) after the next open: %d members, RAM %d", win[0], win[1], len(got), len(want))
		}
	}
	if v := markerValue(t, bs, storepkg.EntityIndexRelMember, w.r); v != storepkg.EntityIndexBuiltSpan {
		t.Errorf("marker %d after the next open, want %d", v, storepkg.EntityIndexBuiltSpan)
	}
}

// unrelated is the number of unrelated rows a TestMemberSpan_UnrelatedHistory
// store gives the value.
var unrelatedHistory = []int{8, 32, 128}

// TestMemberSpan_UnrelatedHistoryDoesNotGrowCandidates: two rows meet the fixed
// window [1000, 1010) of seat=1; n others (8, 32, 128), each with a corrected,
// deleted or plain history of rows far from it, carry the same value. The door
// hands on the same two members whatever n is, and holds one member at a time;
// its walk visits the value's entries (it still reads each one: work counts
// them), which grow with n. Old-pin and new-pin answers equal the RAM store's
// through a correction of a target and the deletion of the other, with the
// writes buffered, flushed, after a copied reopen (writable and read-only), for
// relationships and nodes.
func TestMemberSpan_UnrelatedHistoryDoesNotGrowCandidates(t *testing.T) {
	t.Parallel()
	for _, n := range unrelatedHistory {
		t.Run(fmt.Sprint("n=", n), func(t *testing.T) {
			t.Parallel()
			w := newSpanWorld(t)
			dir := t.TempDir()
			ram, disk := w.open("", false, false), w.open(dir, true, false)
			w.create(ram)
			w.create(disk)
			both := func(f func(bs *Store, rel bool, id func(int64) int64)) {
				for _, bs := range []*Store{ram, disk} {
					f(bs, true, func(i int64) int64 { return i })
					f(bs, false, func(i int64) int64 { return i + nodeBase })
				}
			}
			put := func(bs *Store, rel bool, id int64, seat int64, version uint32, txFrom, txTo, vf, vt types.Instant) {
				if rel {
					must(t, "put", bs.PutRelationship(w.rel(id, seat, version, txFrom, txTo, vf, vt)))
				} else {
					must(t, "put", bs.PutNode(w.node(id, seat, version, txFrom, txTo, vf, vt)))
				}
			}
			replace := func(bs *Store, rel bool, id int64, seat int64, version uint32, txFrom, vf, vt types.Instant, prevFrom, prevTo, prevVf, prevVt types.Instant) {
				if rel {
					must(t, "replace", bs.ReplaceRelWithHistory(w.rel(id, seat, version, txFrom, 0, vf, vt), version-1, w.rel(id, 1, version-1, prevFrom, prevTo, prevVf, prevVt)))
				} else {
					must(t, "replace", bs.ReplaceNodeWithHistory(w.node(id, seat, version, txFrom, 0, vf, vt), version-1, w.node(id, 1, version-1, prevFrom, prevTo, prevVf, prevVt)))
				}
			}
			// Targets 1 and 2: valid [1000, 1010) and [1005, 1006), created at 100.
			both(func(bs *Store, rel bool, id func(int64) int64) {
				put(bs, rel, id(1), 1, 0, 100, 0, 1000, 1010)
				put(bs, rel, id(2), 1, 0, 100, 0, 1005, 1006)
			})
			// The others: valid far from the window; every third is corrected at 200 (its
			// replacement far too), every third more deleted at 300, the rest plain.
			for i := int64(0); i < int64(n); i++ {
				idv := 100 + i
				from := types.Instant(5 + i)
				both(func(bs *Store, rel bool, id func(int64) int64) {
					put(bs, rel, id(idv), 1, 0, 100, 0, from, from+10)
					switch i % 3 {
					case 0:
						replace(bs, rel, id(idv), 1, 1, 200, from+20, from+30, 100, 200, from, from+10)
					case 1:
						if rel {
							must(t, "delete", bs.DeleteRelWithHistory(types.RelID(snowflake.ID(id(idv))), 0, tomb(w.rel(id(idv), 1, 0, 100, 0, from, from+10), 300, tombPlain)))
						} else {
							must(t, "delete", bs.DeleteNodeWithHistory(types.NodeID(snowflake.ID(id(idv))), 0, tombNode(w.node(id(idv), 1, 0, 100, 0, from, from+10), 300, tombPlain), nil))
						}
					}
				})
			}
			check := func(phase string, stores ...*Store) {
				t.Helper()
				for _, win := range [][2]types.Instant{{1000, 1010}, {1005, 1006}, {2000, 2010}, {0, 500}} {
					for _, pin := range []types.Instant{0, 150, 250, 350, 1 << 40} {
						for _, bs := range stores {
							w.agreeAt(ram, bs, phase, win[0], win[1], pin)
						}
					}
				}
			}
			check("buffered", disk)
			// The meter: the fixed window hands on 2 members, holds 1, walks the value's entries.
			vk := w.vk
			var gotRel, gotNode = 0, 0
			workR, err := disk.DiagnoseRelPropertyTxMembersDuring(w.r, "seat", vk, 1000, 1010, 0, func(types.RelID, types.Instant) bool { gotRel++; return true })
			must(t, "diagnose rel", err)
			workN, err := disk.DiagnoseNodePropertyTxMembersDuring(w.d, "seat", vk, 1000, 1010, 0, func(types.NodeID, types.Instant) bool { gotNode++; return true })
			must(t, "diagnose node", err)
			if gotRel != 2 || gotNode != 2 {
				t.Fatalf("n=%d: the window hands on %d relationships, %d nodes, want 2 each (the unrelated history grew the candidates)", n, gotRel, gotNode)
			}
			for what, work := range map[string][5]uint64{
				"rel":  {workR.MembersEmitted, workR.FoldMembersPeak, workR.IteratorSteps, workR.DiskEntriesEmitted, workR.OverlayOpsMatched},
				"node": {workN.MembersEmitted, workN.FoldMembersPeak, workN.IteratorSteps, workN.DiskEntriesEmitted, workN.OverlayOpsMatched},
			} {
				if work[0] != 2 || work[1] > 1+uint64(2*n) { // the buffer's entities are held until the walk meets them
					t.Errorf("n=%d %s: emitted %d, held at most %d members", n, what, work[0], work[1])
				}
				t.Logf("n=%d %s window [1000,1010): emitted %d, held peak %d, iterator steps %d, disk entries %d, buffered ops matched %d", n, what, work[0], work[1], work[2], work[3], work[4])
			}
			must(t, "flush", disk.Flush())
			check("flushed", disk)
			// Flushed, the walk seeks to the window (RELDISKWIN option B): the late axis
			// from the window's start, the recent axis down to the pin; the unrelated
			// history, all of it ending before the window, is never visited.
			for _, c := range []struct {
				pin      types.Instant
				maxSteps uint64
			}{{0, 4}, {350, 6}} {
				workR, err = disk.DiagnoseRelPropertyTxMembersDuring(w.r, "seat", vk, 1000, 1010, c.pin, func(types.RelID, types.Instant) bool { return true })
				must(t, "diagnose flushed", err)
				workN, err = disk.DiagnoseNodePropertyTxMembersDuring(w.d, "seat", vk, 1000, 1010, c.pin, func(types.NodeID, types.Instant) bool { return true })
				must(t, "diagnose flushed node", err)
				for what, work := range map[string][3]uint64{
					"rel":  {workR.MembersEmitted, workR.FoldMembersPeak, workR.IteratorSteps},
					"node": {workN.MembersEmitted, workN.FoldMembersPeak, workN.IteratorSteps},
				} {
					if work[0] != 2 || work[1] > 2 {
						t.Errorf("n=%d flushed %s @%d: emitted %d, held %d, want 2 and at most 2", n, what, c.pin, work[0], work[1])
					}
					if work[2] > c.maxSteps {
						t.Errorf("n=%d flushed %s window [1000,1010)@%d: %d iterator steps, want at most %d (the unrelated history is walked)", n, what, c.pin, work[2], c.maxSteps)
					}
					t.Logf("n=%d flushed %s window [1000,1010)@%d: iterator steps %d, held peak %d, emitted %d", n, what, c.pin, work[2], work[1], work[0])
				}
			}

			// A correction of target 1 at 400 (it moves out of the window) and the deletion
			// of target 2 at 450, written after the flush: buffered over a flushed store.
			both(func(bs *Store, rel bool, id func(int64) int64) {
				replace(bs, rel, id(1), 1, 1, 400, 2000, 2010, 100, 400, 1000, 1010)
				if rel {
					must(t, "delete target", bs.DeleteRelWithHistory(types.RelID(snowflake.ID(id(2))), 0, tomb(w.rel(id(2), 1, 0, 100, 0, 1005, 1006), 450, tombPlain)))
				} else {
					must(t, "delete target", bs.DeleteNodeWithHistory(types.NodeID(snowflake.ID(id(2))), 0, tombNode(w.node(id(2), 1, 0, 100, 0, 1005, 1006), 450, tombPlain), nil))
				}
			})
			check("target corrected and deleted, buffered", disk)
			// The old pin still sees the targets in the window; the new pin sees what the
			// envelope keeps (the sidecar's answer): the same on disk.
			gr, _ := w.windowOf(disk, 1000, 1010, 150)
			if _, ok := gr[1]; !ok {
				t.Errorf("n=%d: at pin 150 relationship 1 is not a member of [1000,1010)", n)
			}
			if _, ok := gr[2]; !ok {
				t.Errorf("n=%d: at pin 150 relationship 2 is not a member of [1000,1010)", n)
			}
			gr, _ = w.windowOf(disk, 2000, 2010, 500)
			if _, ok := gr[1]; !ok {
				t.Errorf("n=%d: at pin 500 the corrected relationship 1 is not a member of [2000,2010)", n)
			}
			if _, ok := gr[2]; ok {
				t.Errorf("n=%d: at pin 500 the deleted relationship 2 (valid [1005,1006)) is a member of [2000,2010)", n)
			}
			must(t, "flush", disk.Flush())
			check("target corrected and deleted, flushed", disk)

			// A copied reopen, writable and read-only.
			must(t, "close", disk.Close())
			copiedA, copiedB := copyStoreDir(t, dir), copyStoreDir(t, dir)
			check("copied reopen", w.open(copiedA, true, false))
			check("copied reopen, read-only", w.open(copiedB, true, true))
		})
	}
}

// TestMemberSpan_WriteOverhead measures what the span costs a write and the
// store (logged, not asserted: time moves with load): the bytes of an entry's
// key and value, and the time of n creates and n updates (one history row each)
// with the definition on disk against the RAM index.
func TestMemberSpan_WriteOverhead(t *testing.T) {
	if testing.Short() {
		t.Skip("measures 20,000 writes")
	}
	w := newSpanWorld(t)
	const n = 5000
	var lines []string
	for _, disk := range []bool{false, true} {
		bs := w.open("", disk, false)
		w.create(bs)
		start := nowNano()
		for i := int64(0); i < n; i++ {
			must(t, "put", bs.PutRelationship(w.rel(100+i, 1, 0, 100, 0, 1000+types.Instant(i), 0)))
		}
		create := nowNano() - start
		start = nowNano()
		for i := int64(0); i < n; i++ {
			must(t, "replace", bs.ReplaceRelWithHistory(w.rel(100+i, 1, 1, 200, 0, 2000+types.Instant(i), 0), 0, w.rel(100+i, 1, 0, 100, 200, 1000+types.Instant(i), 0)))
		}
		update := nowNano() - start
		must(t, "flush", bs.Flush())
		var keyBytes, valBytes, entries int
		if disk {
			for k, v := range spanEntries(t, bs, storepkg.EntityIndexRelSpan, w.r) {
				keyBytes += len(k)
				valBytes += len(v)
				entries++
			}
		}
		lines = append(lines, fmt.Sprintf("disk=%t: %d creates %.2f us each, %d updates %.2f us each; %d span entries, %d key bytes + %d value bytes (%.1f B each)",
			disk, n, float64(create)/n/1000, n, float64(update)/n/1000, entries, keyBytes, valBytes, float64(keyBytes+valBytes)/float64(max(entries, 1))))
		if disk {
			late, recent := w.timeEntries(bs)
			lines = append(lines, fmt.Sprintf("disk=true after the updates: %d late-axis and %d recent-axis entries (option B)", late, recent))
		}
	}
	// What one door writes to 0x0C (option B against the span entries alone,
	// which is what the door wrote before it): the buffered member ops of each
	// door on a flushed store, by kind, with their key and value bytes.
	bs := w.open("", true, false)
	w.create(bs)
	const doors = 100
	type tally struct{ sets, dels, bytes int }
	count := func(do func(i int64)) (span, timed tally) {
		for i := int64(0); i < doors; i++ {
			must(t, "flush", bs.Flush())
			do(i)
			bs.wbMu.Lock()
			for k, op := range bs.pending {
				if k[0] != storepkg.KeyEntityIndex || len(k) < 2 || k[1] == storepkg.EntityIndexRelCurrent {
					continue
				}
				c := &span
				if storepkg.EntityIndexIsTime(k[1]) {
					c = &timed
				}
				if op.opType == writeOpSet {
					c.sets++
				} else {
					c.dels++
				}
				c.bytes += len(k) + len(op.value)
			}
			bs.wbMu.Unlock()
		}
		return span, timed
	}
	for _, d := range []struct {
		name string
		do   func(i int64)
	}{
		{"create", func(i int64) {
			must(t, "put", bs.PutRelationship(w.rel(100+i, 1, 0, 100, 0, 1000+types.Instant(i), 0)))
		}},
		{"update of an open row", func(i int64) {
			must(t, "replace", bs.ReplaceRelWithHistory(w.rel(100+i, 1, 1, 200, 0, 2000+types.Instant(i), 0), 0, w.rel(100+i, 1, 0, 100, 200, 1000+types.Instant(i), 0)))
		}},
		{"delete writing ValidTo", func(i int64) {
			must(t, "delete", bs.DeleteRelWithHistory(types.RelID(snowflake.ID(100+i)), 1, tomb(w.rel(100+i, 1, 1, 200, 0, 2000+types.Instant(i), 0), 9000, tombWrite)))
		}},
	} {
		span, timed := count(d.do)
		lines = append(lines, fmt.Sprintf("%s: before (span entries) %.1f sets %.1f deletes %.1f B; option B adds %.1f sets %.1f deletes %.1f B (per door, %d doors)",
			d.name, float64(span.sets)/doors, float64(span.dels)/doors, float64(span.bytes)/doors, float64(timed.sets)/doors, float64(timed.dels)/doors, float64(timed.bytes)/doors, doors))
	}
	t.Log("\n" + strings.Join(lines, "\n"))
}

func nowNano() int64 { return time.Now().UnixNano() }
