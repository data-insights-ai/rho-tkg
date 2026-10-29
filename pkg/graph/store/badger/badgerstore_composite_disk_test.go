package badger

import (
	"fmt"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// compositeNode is a node of label with the given string properties.
func compositeNode(t *testing.T, id int64, label uint16, props map[string]string) *types.Node {
	t.Helper()
	n := types.NewNode(types.NodeID(snowflake.ID(id)), label, nil)
	for k, v := range props {
		if err := n.SetProperty(k, v); err != nil {
			t.Fatal(err)
		}
	}
	return n
}

// compositeIDs is the IDs NodesByLabelAndProperties answers.
func compositeIDs(t *testing.T, bs *Store, label uint16, values map[string]any, opts QueryOpts) string {
	t.Helper()
	nodes, err := bs.NodesByLabelAndProperties(label, values, opts)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for _, n := range nodes {
		ids = append(ids, int64(n.ID()))
	}
	return fmt.Sprint(ids)
}

// compositeHead is the head of label's composite on keys.
func compositeHead(t *testing.T, bs *Store, label uint16, keys ...string) []byte {
	t.Helper()
	head, ok := bs.compositeDiskHead(label, keys)
	if !ok {
		t.Fatal("no head")
	}
	return head
}

// TestCompositeDisk_Lifecycle (COMPDISK): a composite of a label kept on disk
// holds one entry per node and tuple, per definition (two composites of a
// label, one over a prefix of the other's keys, never share an entry); a
// lookup and the ID page answer from them, paged from a cursor, with the write
// buffer, the buffer being flushed and the disk merged per key; an update that
// keeps the tuple writes no entry, one that changes it moves the entry; a drop
// deletes its entries and marker only; Clear every one.
func TestCompositeDisk_Lifecycle(t *testing.T) {
	t.Parallel()
	labels := registrypkg.NewLabelRegistry()
	d, _ := labels.GetOrCreate("D")
	bs := newFlushParkStore(t, func(cfg *Config) {
		cfg.FlushInterval = time.Hour
		cfg.PropertyKeyRegistry = registrypkg.NewPropertyKeyRegistry()
		cfg.LabelNames = labels
		cfg.PropertyIndexOnDiskLabels = []string{"D"}
	})
	for _, keys := range [][]string{{"a", "b"}, {"a", "b", "c"}} {
		if err := bs.CreateCompositePropertyIndex(d, keys); err != nil {
			t.Fatal(err)
		}
	}
	ab, abc := compositeHead(t, bs, d, "a", "b"), compositeHead(t, bs, d, "a", "b", "c")
	for id := int64(1); id <= 6; id++ {
		if err := bs.PutNode(compositeNode(t, id, d, map[string]string{"a": "x", "b": fmt.Sprint("y", id%2), "c": "z", "o": "0"})); err != nil {
			t.Fatal(err)
		}
	}
	if got := entityIDs(t, bs, storepkg.EntityIndexNodeComposite, ab); got != "[1 2 3 4 5 6]" {
		t.Fatalf("(a, b) entries %s, want one per node", got)
	}
	if got := entityIDs(t, bs, storepkg.EntityIndexNodeComposite, abc); got != "[1 2 3 4 5 6]" {
		t.Fatalf("(a, b, c) entries %s, want one per node", got)
	}
	y1 := map[string]any{"a": "x", "b": "y1"}
	if got := compositeIDs(t, bs, d, y1, QueryOpts{}); got != "[1 3 5]" {
		t.Errorf("(x, y1): %s, want [1 3 5]", got)
	}
	if got := compositeIDs(t, bs, d, map[string]any{"a": "x", "b": "y0", "c": "z"}, QueryOpts{}); got != "[2 4 6]" {
		t.Errorf("(x, y0, z): %s, want [2 4 6]", got)
	}
	if got := compositeIDs(t, bs, d, y1, QueryOpts{After: 1, Limit: 1}); got != "[3]" {
		t.Errorf("(x, y1) after 1, limit 1: %s, want [3]", got)
	}
	page := func(after types.NodeID, n int) string {
		t.Helper()
		ids, more, ok, err := bs.NodeIDsByLabelAndPropertiesPage(d, y1, after, n, 0)
		if err != nil || !ok {
			t.Fatalf("ID page: %v, ok %v", err, ok)
		}
		return fmt.Sprint(ids, more)
	}
	if got := page(1, 1); got != "[3] true" {
		t.Errorf("ID page after 1, 1: %s, want [3] true", got)
	}
	if got := page(3, 2); got != "[5] false" {
		t.Errorf("ID page after 3, 2: %s, want [5] false", got)
	}

	// The write buffer and the buffer being flushed over the disk.
	if err := bs.PutNode(compositeNode(t, 7, d, map[string]string{"a": "x", "b": "y1"})); err != nil {
		t.Fatal(err)
	}
	parkPendingIntoFlushing(t, bs) // node 7's entry being flushed
	if err := bs.DeleteNode(3); err != nil {
		t.Fatal(err)
	}
	if got := compositeIDs(t, bs, d, y1, QueryOpts{}); got != "[1 5 7]" {
		t.Errorf("node 7 being flushed, node 3 deleted in the buffer: %s, want [1 5 7]", got)
	}
	if got := page(1, 1); got != "[5] true" {
		t.Errorf("ID page after 1, 1, node 3 deleted in the buffer: %s, want [5] true", got)
	}
	commitFlushingToBadger(t, bs)
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}

	// An update that keeps the tuple writes no entry; one that changes it
	// moves the entry.
	before := entityVersions(t, bs)
	if err := bs.ReplaceNode(compositeNode(t, 1, d, map[string]string{"a": "x", "b": "y1", "c": "z", "o": "1"})); err != nil {
		t.Fatal(err)
	}
	if after := entityVersions(t, bs); after != before {
		t.Errorf("an update keeping the tuple wrote %d entry versions, want none", after-before)
	}
	if err := bs.ReplaceNode(compositeNode(t, 5, d, map[string]string{"a": "x", "b": "y0", "c": "z"})); err != nil {
		t.Fatal(err)
	}
	if got := compositeIDs(t, bs, d, y1, QueryOpts{}) + compositeIDs(t, bs, d, map[string]any{"a": "x", "b": "y0"}, QueryOpts{}); got != "[1 7][2 4 5 6]" {
		t.Errorf("node 5 moved to y0: %s, want [1 7][2 4 5 6]", got)
	}

	// A drop deletes its entries and marker, not the other composite's.
	if err := bs.DropCompositePropertyIndex(d, []string{"a", "b", "c"}); err != nil {
		t.Fatal(err)
	}
	if got := entityIDs(t, bs, storepkg.EntityIndexNodeComposite, abc); got != "[]" {
		t.Errorf("dropped (a, b, c): entries %s, want none", got)
	}
	if got := entityIDs(t, bs, storepkg.EntityIndexNodeComposite, ab); got != "[1 2 4 5 6 7]" {
		t.Errorf("(a, b) after the other's drop: entries %s", got)
	}
	if keys := entityKeys(t, bs, storepkg.EntityIndexBuiltKey(storepkg.EntityIndexNodeComposite, d, "")); len(keys) != 1 {
		t.Errorf("%d composite markers after the drop, want 1", len(keys))
	}
	if err := bs.Clear(); err != nil {
		t.Fatal(err)
	}
	if keys := entityKeys(t, bs, []byte{storepkg.KeyEntityIndex}); len(keys) != 0 {
		t.Errorf("%d entries after Clear, want none", len(keys))
	}
	if len(bs.compositeDiskDefs) != 0 {
		t.Errorf("%d composites kept on disk after Clear, want none", len(bs.compositeDiskDefs))
	}
}

// TestCompositeDisk_CreateMerge (COMPDISK): a create reads the rows without a
// lock; a node written or deleted meanwhile keeps the entry its door wrote, or
// none.
func TestCompositeDisk_CreateMerge(t *testing.T) {
	t.Parallel()
	labels := registrypkg.NewLabelRegistry()
	d, _ := labels.GetOrCreate("D")
	bs, err := New(Config{InMemory: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), LabelNames: labels,
		PropertyIndexOnDiskLabels: []string{"D"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bs.Close() }()
	for id := int64(1); id <= 4; id++ {
		if err := bs.PutNode(compositeNode(t, id, d, map[string]string{"a": "x", "b": "y"})); err != nil {
			t.Fatal(err)
		}
	}
	bs.compositeDiskCreateTestHook = func() {
		if err := bs.ReplaceNode(compositeNode(t, 1, d, map[string]string{"a": "x", "b": "moved"})); err != nil {
			t.Error(err)
		}
		if err := bs.DeleteNode(2); err != nil {
			t.Error(err)
		}
		if err := bs.PutNode(compositeNode(t, 5, d, map[string]string{"a": "x", "b": "y"})); err != nil {
			t.Error(err)
		}
	}
	if err := bs.CreateCompositePropertyIndex(d, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	bs.compositeDiskCreateTestHook = nil
	head := compositeHead(t, bs, d, "a", "b")
	if got := entityIDs(t, bs, storepkg.EntityIndexNodeComposite, head); got != "[1 3 4 5]" {
		t.Errorf("entries %s, want [1 3 4 5] (1 moved, 2 deleted, 5 added during the scan)", got)
	}
	if got := compositeIDs(t, bs, d, map[string]any{"a": "x", "b": "y"}, QueryOpts{}) + compositeIDs(t, bs, d, map[string]any{"a": "x", "b": "moved"}, QueryOpts{}); got != "[3 4 5][1]" {
		t.Errorf("lookups %s, want [3 4 5][1]", got)
	}
}

// TestCompositeDisk_Removal (COMPDISK): a retention purge, a purge of a node
// whose row cannot be read and an exact erasure delete the node's composite
// entries.
func TestCompositeDisk_Removal(t *testing.T) {
	t.Parallel()
	labels := registrypkg.NewLabelRegistry()
	d, _ := labels.GetOrCreate("D")
	bs, err := New(Config{InMemory: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), LabelNames: labels,
		PropertyIndexOnDiskLabels: []string{"D"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bs.Close() }()
	if err := bs.CreateCompositePropertyIndex(d, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	for id := int64(1); id <= 4; id++ {
		n := compositeNode(t, id, d, map[string]string{"a": "x", "b": "y"})
		end := types.Instant(1000)
		if id == 1 {
			end = 100
		}
		n.SetTemporal(&types.TemporalMetadata{ValidFrom: 10, ValidTo: end})
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	head := compositeHead(t, bs, d, "a", "b")
	if _, err := bs.PurgeNodesByLabelValidToBefore(d, 150, 10); err != nil { // node 1, valid to 100
		t.Fatal(err)
	}
	if got := entityIDs(t, bs, storepkg.EntityIndexNodeComposite, head); got != "[2 3 4]" {
		t.Fatalf("after the retention purge: entries %s, want [2 3 4]", got)
	}
	// The purge of a node whose row cannot be read (cascadeDeleteApply).
	bs.idxMu.Lock()
	ops := bs.maintainPropertyIndexesPurge(2)
	bs.idxMu.Unlock()
	bs.appendOps(ops...)
	if got := entityIDs(t, bs, storepkg.EntityIndexNodeComposite, head); got != "[3 4]" {
		t.Fatalf("after the brute-force purge of node 2: entries %s, want [3 4]", got)
	}
	// A residue entry of node 3 (under no definition's tuple) goes with it.
	residue := compositeDiskEntry(head, "stale", 3, writeOpSet)
	bs.appendOps(residue)
	if _, err := bs.ExactErase(storecontract.ExactErasureRequest{NodeIDs: []types.NodeID{3}, Bounds: badgerExactErasureBounds}); err != nil {
		t.Fatal(err)
	}
	if got := entityIDs(t, bs, storepkg.EntityIndexNodeComposite, head); got != "[4]" {
		t.Errorf("after erasing node 3: entries %s, want [4]", got)
	}
}

// TestCompositeDisk_Opens (COMPDISK): a composite on disk survives a reopen
// (marked, not rebuilt); a writable open that keeps the label in RAM deletes
// its marker; a read-only open over an unmarked composite answers from RAM; a
// create after entries were left behind deletes them first.
func TestCompositeDisk_Opens(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	labels := registrypkg.NewLabelRegistry()
	d, _ := labels.GetOrCreate("D")
	keys := registrypkg.NewPropertyKeyRegistry()
	open := func(listed, readOnly bool) *Store {
		t.Helper()
		cfg := Config{Dir: dir, ReadOnly: readOnly, PropertyKeyRegistry: keys, LabelNames: labels}
		if listed {
			cfg.PropertyIndexOnDiskLabels = []string{"D"}
		}
		bs, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		return bs
	}
	closeStore := func(bs *Store) {
		t.Helper()
		if err := bs.Close(); err != nil {
			t.Fatal(err)
		}
	}
	onDisk := func(bs *Store) bool {
		t.Helper()
		stats, err := bs.PropertyIndexStats()
		if err != nil {
			t.Fatal(err)
		}
		for _, st := range stats {
			if st.Composite {
				return st.OnDisk
			}
		}
		t.Fatal("no composite")
		return false
	}
	y := map[string]any{"a": "x", "b": "y"}
	bs := open(true, false)
	if err := bs.CreateCompositePropertyIndex(d, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	for id := int64(1); id <= 3; id++ {
		if err := bs.PutNode(compositeNode(t, id, d, map[string]string{"a": "x", "b": "y"})); err != nil {
			t.Fatal(err)
		}
	}
	closeStore(bs)

	bs = open(true, true)
	if !onDisk(bs) || compositeIDs(t, bs, d, y, QueryOpts{}) != "[1 2 3]" {
		t.Errorf("read-only, marked: on disk %v, %s", onDisk(bs), compositeIDs(t, bs, d, y, QueryOpts{}))
	}
	_ = bs.Close()

	bs = open(false, false) // in RAM: the marker goes, node 3 moves in RAM only
	if onDisk(bs) {
		t.Error("unlisted: on disk")
	}
	if err := bs.ReplaceNode(compositeNode(t, 3, d, map[string]string{"a": "x", "b": "moved"})); err != nil {
		t.Fatal(err)
	}
	closeStore(bs)

	bs = open(true, true) // read-only over an unmarked composite: RAM
	if onDisk(bs) || compositeIDs(t, bs, d, y, QueryOpts{}) != "[1 2]" {
		t.Errorf("read-only, unmarked: on disk %v, %s, want RAM, [1 2]", onDisk(bs), compositeIDs(t, bs, d, y, QueryOpts{}))
	}
	_ = bs.Close()

	bs = open(false, false) // dropped in RAM: node 3's stale entry stays on disk
	if err := bs.DropCompositePropertyIndex(d, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	closeStore(bs)
	bs = open(true, false)
	defer closeStore(bs)
	if err := bs.CreateCompositePropertyIndex(d, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if got := entityIDs(t, bs, storepkg.EntityIndexNodeComposite, compositeHead(t, bs, d, "a", "b")); got != "[1 2 3]" {
		t.Errorf("created again: entries %s, want [1 2 3] (node 3 under its new tuple only)", got)
	}
	if got := compositeIDs(t, bs, d, y, QueryOpts{}); got != "[1 2]" {
		t.Errorf("created again: %s, want [1 2]", got)
	}
	var n int
	for _, k := range entityKeys(t, bs, compositeHead(t, bs, d, "a", "b")) {
		if _, id, _, _ := storepkg.EntityIndexEntryOf(k, storepkg.EntityIndexNodeComposite); id == 3 {
			n++
		}
	}
	if n != 1 {
		t.Errorf("node 3 has %d entries, want 1", n)
	}
}
