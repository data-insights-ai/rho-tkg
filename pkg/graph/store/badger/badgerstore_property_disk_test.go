package badger

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// badgerHasKey reports whether key is present in the RAW persisted Badger
// keyspace (bypassing the pending-write overlay entirely) — used to prove
// same-WriteBatch atomicity by direct inspection.
func badgerHasKey(bs *Store, key []byte) bool {
	found := false
	_ = bs.DBForTest().View(func(txn *badgerv4.Txn) error {
		_, err := txn.Get(key)
		found = err == nil
		return nil
	})
	return found
}

// Disk-resident property-index probes (parity discipline: the disk arm and
// the in-memory Entries/numBuckets arm are parallel implementations of one
// contract — every probe compares them on identical op sequences, mirroring
// badgerstore_label_disk_test.go).

// newTestBadgerStorePropIdxOnDisk creates an in-memory Store with
// PropertyIndexOnDisk enabled and a wired property-key registry (required —
// CreatePropertyIndex fails closed without one in disk mode).
func newTestBadgerStorePropIdxOnDisk(t *testing.T) *Store {
	t.Helper()
	bs, err := New(Config{
		InMemory:            true,
		PropertyIndexOnDisk: true,
		PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(),
	})
	if err != nil {
		t.Fatalf("New disk-mode: %v", err)
	}
	t.Cleanup(func() { bs.Close() })
	return bs
}

func nodeIDsVia(t *testing.T, bs *Store, label uint16, propKey string, value any) []int64 {
	t.Helper()
	nodes, err := bs.NodesByLabelAndProperty(label, propKey, value, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty: %v", err)
	}
	ids := make([]int64, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, int64(n.ID()))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// --- (e) equivalence: flag ON vs OFF, equality ---

// TestPropertyIndexOnDisk_EqualityParityWithMemoryMode drives both modes
// through the SAME mutation sequence — puts (numeric + string values),
// property updates via ReplaceNode, label removal, deletes — comparing
// NodesByLabelAndProperty after EVERY mutation, both BEFORE flush (pending
// overlay visibility) and after (persisted keyspace visibility).
func TestPropertyIndexOnDisk_EqualityParityWithMemoryMode(t *testing.T) {
	t.Parallel()
	mem, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("New mem-mode: %v", err)
	}
	t.Cleanup(func() { mem.Close() })
	disk := newTestBadgerStorePropIdxOnDisk(t)

	const labelA = uint16(1)
	for _, bs := range []*Store{mem, disk} {
		if err := bs.CreatePropertyIndex(labelA, "age"); err != nil {
			t.Fatalf("CreatePropertyIndex age: %v", err)
		}
		if err := bs.CreatePropertyIndex(labelA, "city"); err != nil {
			t.Fatalf("CreatePropertyIndex city: %v", err)
		}
	}

	compare := func(step string) {
		t.Helper()
		for _, v := range []int64{20, 21, 22, 23} {
			a, b := nodeIDsVia(t, mem, labelA, "age", v), nodeIDsVia(t, disk, labelA, "age", v)
			if fmt.Sprint(a) != fmt.Sprint(b) {
				t.Fatalf("%s: age=%d mem=%v disk=%v", step, v, a, b)
			}
		}
		for _, v := range []string{"nyc", "sf"} {
			a, b := nodeIDsVia(t, mem, labelA, "city", v), nodeIDsVia(t, disk, labelA, "city", v)
			if fmt.Sprint(a) != fmt.Sprint(b) {
				t.Fatalf("%s: city=%q mem=%v disk=%v", step, v, a, b)
			}
		}
	}

	for i := 1; i <= 8; i++ {
		age := int64(20 + i%4)
		city := "nyc"
		if i%2 == 0 {
			city = "sf"
		}
		for _, bs := range []*Store{mem, disk} {
			n := types.NewNode(types.NodeID(i), labelA, nil)
			if err := n.SetProperty("age", age); err != nil {
				t.Fatalf("prop: %v", err)
			}
			if err := n.SetProperty("city", city); err != nil {
				t.Fatalf("prop: %v", err)
			}
			if err := bs.PutNode(n); err != nil {
				t.Fatalf("PutNode(%d): %v", i, err)
			}
		}
		compare(fmt.Sprintf("after put %d (pre-flush)", i))
	}
	for _, bs := range []*Store{mem, disk} {
		if err := bs.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	compare("after flush")

	// ReplaceNode: mutate age+city on node 3.
	for _, bs := range []*Store{mem, disk} {
		n, err := bs.GetNode(types.NodeID(3))
		if err != nil {
			t.Fatalf("GetNode(3): %v", err)
		}
		if err := n.SetProperty("age", int64(99)); err != nil {
			t.Fatalf("prop: %v", err)
		}
		if err := n.SetProperty("city", "sf"); err != nil {
			t.Fatalf("prop: %v", err)
		}
		if err := bs.ReplaceNode(n); err != nil {
			t.Fatalf("ReplaceNode(3): %v", err)
		}
	}
	a99, b99 := nodeIDsVia(t, mem, labelA, "age", int64(99)), nodeIDsVia(t, disk, labelA, "age", int64(99))
	if fmt.Sprint(a99) != fmt.Sprint(b99) || len(a99) != 1 {
		t.Fatalf("after replace: age=99 mem=%v disk=%v", a99, b99)
	}
	compare("after replace (pre-flush)")
	for _, bs := range []*Store{mem, disk} {
		if err := bs.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	compare("after replace (flushed)")

	// DeleteNode: remove node 5.
	for _, bs := range []*Store{mem, disk} {
		if err := bs.DeleteNode(types.NodeID(5)); err != nil {
			t.Fatalf("DeleteNode(5): %v", err)
		}
	}
	compare("after delete (pre-flush)")
	for _, bs := range []*Store{mem, disk} {
		if err := bs.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	compare("after delete (flushed)")
}

// TestPropertyIndexOnDisk_MultiLabelSharedPropertyKey pins the on-disk key
// design's central invariant: rows are physically shared across labels that
// index the SAME property key, and each reader's HasLabelTokenRaw recheck is
// what keeps them label-scoped. Also exercises DropPropertyIndex's
// reference-counted physical purge — dropping ONE (label,key) definition
// must not corrupt a SIBLING definition still using the same key.
func TestPropertyIndexOnDisk_MultiLabelSharedPropertyKey(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStorePropIdxOnDisk(t)
	const labelA, labelB = uint16(1), uint16(2)

	n1 := types.NewNode(types.NodeID(1), labelA, []uint16{labelB}) // both labels
	n2 := types.NewNode(types.NodeID(2), labelA, nil)              // A only
	n3 := types.NewNode(types.NodeID(3), labelB, nil)              // B only
	if err := n1.SetProperty("age", int64(5)); err != nil {
		t.Fatal(err)
	}
	if err := n2.SetProperty("age", int64(7)); err != nil {
		t.Fatal(err)
	}
	if err := n3.SetProperty("age", int64(5)); err != nil {
		t.Fatal(err)
	}
	for _, n := range []*types.Node{n1, n2, n3} {
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	if err := bs.CreatePropertyIndex(labelA, "age"); err != nil {
		t.Fatalf("CreatePropertyIndex A: %v", err)
	}
	if err := bs.CreatePropertyIndex(labelB, "age"); err != nil {
		t.Fatalf("CreatePropertyIndex B: %v", err)
	}

	assertSet := func(step string, label uint16, val int64, want []int64) {
		t.Helper()
		got := nodeIDsVia(t, bs, label, "age", val)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s: label=%d age=%d got=%v want=%v", step, label, val, got, want)
		}
	}
	assertSet("initial", labelA, 5, []int64{1})    // n3 excluded (no label A)
	assertSet("initial", labelB, 5, []int64{1, 3}) // shares row, label-filtered read
	assertSet("initial", labelA, 7, []int64{2})

	// Drop A's definition — the equality door falls back to a label scan
	// (NodesByLabelAndProperty never errors on a missing index), but the
	// range door gates on index existence and must now decline.
	if err := bs.DropPropertyIndex(labelA, "age"); err != nil {
		t.Fatalf("DropPropertyIndex A: %v", err)
	}
	assertSet("after drop A (fallback scan)", labelA, 5, []int64{1})
	rangeErr := bs.ForEachNodeByLabelPropertyRange(labelA, "age", 0, 100, true, true, QueryOpts{}, func(*types.Node) bool { return true })
	if !errors.Is(rangeErr, ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound for dropped A range index, got %v", rangeErr)
	}
	assertSet("after drop A", labelB, 5, []int64{1, 3})

	// Mutate n1's age — B's definition must track the new value and the
	// row must persist (created via A originally, but B's def keeps it).
	n1b, err := bs.GetNode(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNode(1): %v", err)
	}
	if err := n1b.SetProperty("age", int64(6)); err != nil {
		t.Fatal(err)
	}
	if err := bs.ReplaceNode(n1b); err != nil {
		t.Fatalf("ReplaceNode(1): %v", err)
	}
	assertSet("after mutate", labelB, 6, []int64{1})
	assertSet("after mutate", labelB, 5, []int64{3})

	// Drop B's definition too — now the LAST reference to "age" is gone, so
	// the physical rows are purged. Recreating must reflect ONLY current
	// state (no resurrected stale rows).
	if err := bs.DropPropertyIndex(labelB, "age"); err != nil {
		t.Fatalf("DropPropertyIndex B: %v", err)
	}
	if err := bs.CreatePropertyIndex(labelB, "age"); err != nil {
		t.Fatalf("recreate CreatePropertyIndex B: %v", err)
	}
	assertSet("after purge+recreate", labelB, 6, []int64{1})
	assertSet("after purge+recreate", labelB, 5, []int64{3})
	// Negative: the OLD age=5 value for node 1 must not have resurfaced.
	got := nodeIDsVia(t, bs, labelB, "age", int64(5))
	for _, id := range got {
		if id == 1 {
			t.Fatal("stale age=5 entry for node 1 resurfaced after drop+recreate")
		}
	}
}

// --- (e) equivalence: flag ON vs OFF, numeric range ---

// TestPropertyIndexOnDisk_RangeParityWithMemoryMode compares
// ForEachNodeByLabelPropertyRange candidate sets AFTER an exact recheck
// (the contract both modes share: candidates are over-selected, callers
// recheck) across both modes, over randomized numeric values including
// updates and deletes, and the "predicate held during part of an update
// history but not the current version" shape via ReplaceNode.
func TestPropertyIndexOnDisk_RangeParityWithMemoryMode(t *testing.T) {
	t.Parallel()
	mem, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("New mem-mode: %v", err)
	}
	t.Cleanup(func() { mem.Close() })
	disk := newTestBadgerStorePropIdxOnDisk(t)

	const label = uint16(1)
	for _, bs := range []*Store{mem, disk} {
		if err := bs.CreatePropertyIndex(label, "score"); err != nil {
			t.Fatalf("CreatePropertyIndex: %v", err)
		}
	}

	exactRangeIDs := func(bs *Store, min, max float64, inclMin, inclMax bool) []int64 {
		t.Helper()
		var ids []int64
		err := bs.ForEachNodeByLabelPropertyRange(label, "score", min, max, inclMin, inclMax, QueryOpts{}, func(n *types.Node) bool {
			v, ok := n.GetProperty("score")
			if !ok {
				return true
			}
			f, ok := v.(int64)
			if !ok {
				return true
			}
			fv := float64(f)
			if fv < min || fv > max {
				return true
			}
			if !inclMin && fv == min {
				return true
			}
			if !inclMax && fv == max {
				return true
			}
			ids = append(ids, int64(n.ID()))
			return true
		})
		if err != nil {
			t.Fatalf("ForEachNodeByLabelPropertyRange: %v", err)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		return ids
	}

	compare := func(step string) {
		t.Helper()
		bounds := [][2]float64{{0, 50}, {10, 30}, {25, 25}, {-10, 100}}
		for _, b := range bounds {
			a, d := exactRangeIDs(mem, b[0], b[1], true, true), exactRangeIDs(disk, b[0], b[1], true, true)
			if fmt.Sprint(a) != fmt.Sprint(d) {
				t.Fatalf("%s: range[%v,%v] mem=%v disk=%v", step, b[0], b[1], a, d)
			}
			a2, d2 := exactRangeIDs(mem, b[0], b[1], false, false), exactRangeIDs(disk, b[0], b[1], false, false)
			if fmt.Sprint(a2) != fmt.Sprint(d2) {
				t.Fatalf("%s: exclusive range[%v,%v] mem=%v disk=%v", step, b[0], b[1], a2, d2)
			}
		}
	}

	scores := []int64{5, 15, 25, 25, 35, 45, 0, 50}
	for i, sc := range scores {
		for _, bs := range []*Store{mem, disk} {
			n := types.NewNode(types.NodeID(i+1), label, nil)
			if err := n.SetProperty("score", sc); err != nil {
				t.Fatal(err)
			}
			if err := bs.PutNode(n); err != nil {
				t.Fatalf("PutNode: %v", err)
			}
		}
	}
	compare("after puts (pre-flush)")
	for _, bs := range []*Store{mem, disk} {
		if err := bs.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	compare("after flush")

	// Two-phase: mutate node 3's score (was 25 at t0) to a value outside the
	// probe range, verifying the CURRENT state (not history) governs.
	for _, bs := range []*Store{mem, disk} {
		n, err := bs.GetNode(types.NodeID(3))
		if err != nil {
			t.Fatalf("GetNode(3): %v", err)
		}
		if err := n.SetProperty("score", int64(999)); err != nil {
			t.Fatal(err)
		}
		if err := bs.ReplaceNode(n); err != nil {
			t.Fatalf("ReplaceNode(3): %v", err)
		}
	}
	compare("after replace (pre-flush)")
	for _, bs := range []*Store{mem, disk} {
		if err := bs.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	compare("after replace (flushed)")

	// Node 3 (score 999) must NOT appear in [0,50] anymore — negative assertion.
	for _, bs := range []*Store{mem, disk} {
		ids := exactRangeIDs(bs, 0, 50, true, true)
		for _, id := range ids {
			if id == 3 {
				t.Fatalf("node 3 (score=999) must not match range [0,50]")
			}
		}
	}
}

// TestPropertyIndexOnDisk_RangeCardinalityDeclines pins the documented
// disk-mode trade-off: NodeRangeCardinality always declines (exact=false)
// in disk mode, never returning a wrong count, while RAM mode returns an
// exact O(1) count for the identical data.
func TestPropertyIndexOnDisk_RangeCardinalityDeclines(t *testing.T) {
	t.Parallel()
	mem, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { mem.Close() })
	disk := newTestBadgerStorePropIdxOnDisk(t)

	const label = uint16(1)
	for _, bs := range []*Store{mem, disk} {
		if err := bs.CreatePropertyIndex(label, "score"); err != nil {
			t.Fatalf("CreatePropertyIndex: %v", err)
		}
		for i := 1; i <= 5; i++ {
			n := types.NewNode(types.NodeID(i), label, nil)
			if err := n.SetProperty("score", int64(i*10)); err != nil {
				t.Fatal(err)
			}
			if err := bs.PutNode(n); err != nil {
				t.Fatalf("PutNode: %v", err)
			}
		}
	}

	count, exact, err := mem.NodeRangeCardinality(label, "score", 10, 30, true, true)
	if err != nil {
		t.Fatalf("mem NodeRangeCardinality: %v", err)
	}
	if !exact || count != 3 {
		t.Fatalf("mem: exact=%v count=%d, want exact=true count=3", exact, count)
	}

	dcount, dexact, err := disk.NodeRangeCardinality(label, "score", 10, 30, true, true)
	if err != nil {
		t.Fatalf("disk NodeRangeCardinality: %v", err)
	}
	if dexact {
		t.Fatal("disk mode must decline (exact=false), never claim an unmaintained O(1) count")
	}
	if dcount != 0 {
		t.Fatalf("disk decline must report count=0, got %d", dcount)
	}
}

// --- (d) enable-on-existing-dir rebuild ---

// TestPropertyIndexOnDisk_EnableOnExistingDir pins the rebuild-on-enable
// contract: a directory with property-index DEFINITIONS but no prior 0x0A
// rows (because PropertyIndexOnDisk was off) is backfilled from current
// node state the first time the flag is turned on, and a SECOND reopen
// (marker already set) still answers correctly without needing the
// registry to be re-supplied (auto-loaded from persisted meta).
func TestPropertyIndexOnDisk_EnableOnExistingDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	const label = uint16(1)

	// Session 1: RAM-mode property index on a real directory.
	reg := registrypkg.NewPropertyKeyRegistry()
	bs, err := New(Config{Dir: dir, PropertyKeyRegistry: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for i := 1; i <= 6; i++ {
		n := types.NewNode(types.NodeID(i), label, nil)
		if err := n.SetProperty("age", int64(20+i%3)); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}
	if err := bs.CreatePropertyIndex(label, "age"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Session 2: reopen with PropertyIndexOnDisk turned ON — no explicit
	// registry (auto-loaded from persisted meta), triggering the one-time
	// backfill scan.
	bs2, err := New(Config{Dir: dir, PropertyIndexOnDisk: true})
	if err != nil {
		t.Fatalf("reopen with disk mode: %v", err)
	}
	want := map[int64][]int64{20: nil, 21: nil, 22: nil}
	for i := int64(1); i <= 6; i++ {
		age := 20 + i%3
		want[age] = append(want[age], i)
	}
	for age, ids := range want {
		got := nodeIDsVia(t, bs2, label, "age", age)
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		if fmt.Sprint(got) != fmt.Sprint(ids) {
			t.Fatalf("session2: age=%d got=%v want=%v", age, got, ids)
		}
	}

	// Add one more node in session 2, flush, close.
	n7 := types.NewNode(types.NodeID(7), label, nil)
	if err := n7.SetProperty("age", int64(20)); err != nil {
		t.Fatal(err)
	}
	if err := bs2.PutNode(n7); err != nil {
		t.Fatalf("PutNode(7): %v", err)
	}
	if err := bs2.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := bs2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Session 3: reopen again with the flag still on — the marker must
	// already be set, so this is a plain persisted-keyspace read (no
	// rescan needed); functionally verify BOTH sessions' data survive.
	bs3, err := New(Config{Dir: dir, PropertyIndexOnDisk: true})
	if err != nil {
		t.Fatalf("session3 reopen: %v", err)
	}
	t.Cleanup(func() { bs3.Close() })
	got20 := nodeIDsVia(t, bs3, label, "age", int64(20))
	wantIDs := append(append([]int64{}, want[20]...), 7)
	sort.Slice(wantIDs, func(i, j int) bool { return wantIDs[i] < wantIDs[j] })
	if fmt.Sprint(got20) != fmt.Sprint(wantIDs) {
		t.Fatalf("session3: age=20 got=%v want=%v", got20, wantIDs)
	}
}

// --- overlay set-then-delete-then-set (lesson 57) ---

// TestPropertyIndexOnDisk_OverlaySetThenDeleteThenSetPending drives a
// set→delete→set sequence entirely within the unflushed pending buffer
// (recreating the same node ID before any flush), asserting the overlay
// resolves to the FINAL op per key (lesson 57 — per-key resolution, not a
// running aggregate) both before and after the eventual flush.
func TestPropertyIndexOnDisk_OverlaySetThenDeleteThenSetPending(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStorePropIdxOnDisk(t)
	const label = uint16(1)
	if err := bs.CreatePropertyIndex(label, "age"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}

	put := func(id int64, age int64) {
		t.Helper()
		n := types.NewNode(types.NodeID(id), label, nil)
		if err := n.SetProperty("age", age); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", id, err)
		}
	}

	// set (pending) -> delete (pending) -> set (pending), never flushed in
	// between.
	put(1, 5)
	if err := bs.DeleteNode(types.NodeID(1)); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	put(1, 5)
	got := nodeIDsVia(t, bs, label, "age", int64(5))
	if fmt.Sprint(got) != "[1]" {
		t.Fatalf("set-delete-set (all pending): got=%v want=[1]", got)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	got = nodeIDsVia(t, bs, label, "age", int64(5))
	if fmt.Sprint(got) != "[1]" {
		t.Fatalf("set-delete-set (flushed): got=%v want=[1]", got)
	}
}

// TestPropertyIndexOnDisk_OverlayDeleteAfterFlushThenSetPending exercises
// the persisted-then-overlaid case: a value is durably flushed, then
// deleted (pending only) — the overlay must mask the persisted row — then
// re-added with the SAME value (pending SET overwrites the pending
// DELETE), all still observable before the second flush lands it.
func TestPropertyIndexOnDisk_OverlayDeleteAfterFlushThenSetPending(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStorePropIdxOnDisk(t)
	const label = uint16(1)
	if err := bs.CreatePropertyIndex(label, "age"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}

	n := types.NewNode(types.NodeID(1), label, nil)
	if err := n.SetProperty("age", int64(5)); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := nodeIDsVia(t, bs, label, "age", int64(5)); fmt.Sprint(got) != "[1]" {
		t.Fatalf("after flush: got=%v want=[1]", got)
	}

	// Delete (pending only) — persisted row must be masked immediately.
	if err := bs.DeleteNode(types.NodeID(1)); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if got := nodeIDsVia(t, bs, label, "age", int64(5)); len(got) != 0 {
		t.Fatalf("after pending delete: got=%v want=[]", got)
	}

	// Re-add with the SAME value (pending SET overwrites the pending
	// DELETE for the same physical key) — must be visible again pre-flush.
	n2 := types.NewNode(types.NodeID(1), label, nil)
	if err := n2.SetProperty("age", int64(5)); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n2); err != nil {
		t.Fatalf("PutNode re-add: %v", err)
	}
	if got := nodeIDsVia(t, bs, label, "age", int64(5)); fmt.Sprint(got) != "[1]" {
		t.Fatalf("after pending re-add: got=%v want=[1]", got)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := nodeIDsVia(t, bs, label, "age", int64(5)); fmt.Sprint(got) != "[1]" {
		t.Fatalf("after final flush: got=%v want=[1]", got)
	}
}

// --- reopen persistence ---

// TestPropertyIndexOnDisk_ReopenPersistence pins that a store opened with
// PropertyIndexOnDisk from the START (no rebuild needed — the marker is set
// by the initial CreatePropertyIndex-less path... actually still routed
// through the write-path maintenance on every PutNode) persists its 0x0A
// rows durably across a Close/reopen without re-scanning.
func TestPropertyIndexOnDisk_ReopenPersistence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	const label = uint16(1)
	reg := registrypkg.NewPropertyKeyRegistry()

	bs, err := New(Config{Dir: dir, PropertyIndexOnDisk: true, PropertyKeyRegistry: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := bs.CreatePropertyIndex(label, "age"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}
	for i := 1; i <= 5; i++ {
		n := types.NewNode(types.NodeID(i), label, nil)
		if err := n.SetProperty("age", int64(30+i)); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	bs2, err := New(Config{Dir: dir, PropertyIndexOnDisk: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { bs2.Close() })
	for i := 1; i <= 5; i++ {
		got := nodeIDsVia(t, bs2, label, "age", int64(30+i))
		if fmt.Sprint(got) != fmt.Sprintf("[%d]", i) {
			t.Fatalf("reopen: age=%d got=%v want=[%d]", 30+i, got, i)
		}
	}
}

// --- crash / same-WriteBatch consistency ---

// TestPropertyIndexOnDisk_SameWriteBatchAsEntityRow proves the crash-
// consistency requirement (b): a property-index disk row and the node row
// it describes commit TOGETHER in one flush. Rather than fault-injecting a
// mid-flush crash, this directly inspects the raw Badger keyspace before
// and after an explicit Flush(): pre-flush, NEITHER row is durable (a crash
// now loses both, symmetrically); post-flush, BOTH are durable together —
// proving they travel in the SAME writeOp batch (see maintainPropertyIndexesAdd
// call sites, which always merge into the caller's single appendOps call).
func TestPropertyIndexOnDisk_SameWriteBatchAsEntityRow(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	reg := registrypkg.NewPropertyKeyRegistry()
	bs, err := New(Config{Dir: dir, PropertyIndexOnDisk: true, PropertyKeyRegistry: reg, FlushInterval: 0})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { bs.Close() })
	const label = uint16(1)
	if err := bs.CreatePropertyIndex(label, "age"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}
	if err := bs.Flush(); err != nil { // flush the index-creation metadata first
		t.Fatalf("flush defs: %v", err)
	}

	n := types.NewNode(types.NodeID(42), label, nil)
	if err := n.SetProperty("age", int64(7)); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	tok, ok := bs.propKeyTokenFor("age")
	if !ok {
		t.Fatal("expected a resolvable property-key token")
	}
	payload, ok := storepkg.PropertyIndexValueBytes("i64:7")
	if !ok {
		t.Fatal("expected a valid payload encoding")
	}
	propKey := storepkg.PropertyIndexEntryKey(tok, payload, snowflake.ID(42))
	nodeKey := storepkg.NodeKey(snowflake.ID(42))

	if badgerHasKey(bs, nodeKey) {
		t.Fatal("pre-flush: node row must NOT be durable yet")
	}
	if badgerHasKey(bs, propKey) {
		t.Fatal("pre-flush: property-index row must NOT be durable yet")
	}

	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if !badgerHasKey(bs, nodeKey) {
		t.Fatal("post-flush: node row must be durable")
	}
	if !badgerHasKey(bs, propKey) {
		t.Fatal("post-flush: property-index row must be durable")
	}
}

// TestPropertyIndexOnDisk_CrashRecoveryAgreesWithRowData is the crash-fault
// counterpart of TestPropertyIndexOnDisk_SameWriteBatchAsEntityRow (which
// proves same-WriteBatch atomicity by direct inspection of a single live
// instance) and mirrors TestBadgerStoreRecoveryAfterAbruptShutdown's
// "discard the pending buffer, Close, reopen" crash simulation — a flush
// that stalls/fails leaves its writes buffered-but-undurable, exactly as if
// the process had crashed before the next flush cycle landed them.
//
// Two disjoint node sets are written: 1-5 are flushed durably (their row AND
// property-index entry travel in the same WriteBatch — must both survive the
// crash); 6-10 are left pending when the crash is simulated (their row AND
// property-index entry must both be lost together — never partially).
//
// After reopen, the property index (flag ON) is checked against a full-scan
// ground truth built by decoding every SURVIVING node's actual property
// value directly (bypassing the index) — the query-level check pins "no
// missing entries for rows that did commit". A second, raw check scans the
// persisted 0x0A keyspace directly (bypassing NodesByLabelAndProperty's
// value-recheck fallback, which would otherwise silently absorb a phantom
// row pointing at a now-nonexistent node) — this pins "no phantom index
// entries for rows that never committed".
func TestPropertyIndexOnDisk_CrashRecoveryAgreesWithRowData(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	reg := registrypkg.NewPropertyKeyRegistry()

	// Large intervals so no background flush fires during the test — same
	// discipline as TestBadgerStoreRecoveryAfterAbruptShutdown.
	bs1, err := New(Config{
		Dir:                 dir,
		PropertyIndexOnDisk: true,
		PropertyKeyRegistry: reg,
		FlushInterval:       10 * time.Minute,
		GCInterval:          10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	const label = uint16(1)
	if err := bs1.CreatePropertyIndex(label, "age"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}
	if err := bs1.Flush(); err != nil {
		t.Fatalf("flush defs: %v", err)
	}

	put := func(id int64, age int64) {
		t.Helper()
		n := types.NewNode(types.NodeID(id), label, nil)
		if err := n.SetProperty("age", age); err != nil {
			t.Fatal(err)
		}
		if err := bs1.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", id, err)
		}
	}

	// Committed set: nodes 1-5, ages 21-25 — flushed durably before the crash.
	for i := int64(1); i <= 5; i++ {
		put(i, 20+i)
	}
	if err := bs1.Flush(); err != nil {
		t.Fatalf("flush committed set: %v", err)
	}

	// Lost set: nodes 6-10, ages 26-30 — never flushed; the crash below must
	// lose both the row AND the index entry for every one of them.
	for i := int64(6); i <= 10; i++ {
		put(i, 20+i)
	}

	// Simulate the crash: the pending flush stalls/fails and the process
	// goes down before it retries — discard the buffer so Close()'s shutdown
	// flush finds nothing to write (bs1.pending is the SAME buffer flush()
	// would have drained; clearing it here is equivalent to that flush never
	// having landed).
	bs1.wbMu.Lock()
	bs1.pending = make(map[string]writeOp)
	bs1.wbMu.Unlock()

	if err := bs1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: a fresh Store instance reading only what actually committed.
	bs2, err := New(Config{Dir: dir, PropertyIndexOnDisk: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { bs2.Close() })

	// Ground truth: decode every surviving node's actual "age" value
	// directly — entirely independent of the property index.
	survivors, err := bs2.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatalf("AllNodes: %v", err)
	}
	if len(survivors) != 5 {
		t.Fatalf("expected 5 surviving nodes (1-5 committed, 6-10 lost), got %d", len(survivors))
	}
	groundTruth := make(map[int64][]int64) // age -> surviving node IDs
	for _, n := range survivors {
		v, ok := n.GetProperty("age")
		if !ok {
			t.Fatalf("surviving node %d missing its age property", n.ID())
		}
		age, ok := v.(int64)
		if !ok {
			t.Fatalf("node %d: age property not int64: %T", n.ID(), v)
		}
		groundTruth[age] = append(groundTruth[age], int64(n.ID()))
	}

	// (1) No missing entries: the flag-ON query door must reproduce the
	// ground truth for every value a committed row actually carries.
	for age, ids := range groundTruth {
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		got := nodeIDsVia(t, bs2, label, "age", age)
		if fmt.Sprint(got) != fmt.Sprint(ids) {
			t.Fatalf("age=%d: flag-ON query got=%v want=%v (a committed row's index entry did not survive)", age, got, ids)
		}
	}

	// (2) The lost set's values must return nothing through the query door —
	// this alone doesn't rule out a phantom on-disk row (NodesByLabelAndProperty
	// rechecks the fetched node's actual value/existence before returning, so
	// it would silently absorb one), but a regression here would still be a
	// visible behavioral break.
	for i := int64(6); i <= 10; i++ {
		age := 20 + i
		got := nodeIDsVia(t, bs2, label, "age", age)
		if len(got) != 0 {
			t.Fatalf("age=%d: expected no matches after crash (node %d's write was lost), got %v", age, i, got)
		}
	}

	// (3) No phantom entries: scan the persisted 0x0A keyspace directly
	// (bypassing the query door's recheck fallback) and assert the RAW set
	// of indexed node IDs is EXACTLY the committed set {1..5} — a phantom row
	// surviving for a lost node (6-10) would show up here even though the
	// query-level check above cannot see it.
	tok, ok := bs2.propKeyTokenFor("age")
	if !ok {
		t.Fatal("expected a resolvable property-key token after reopen")
	}
	rawIDs := make(map[int64]struct{})
	if err := bs2.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		prefix := storepkg.PropertyIndexTokenPrefix(tok)
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			rawIDs[int64(storepkg.PropertyIndexNodeIDFromKey(key))] = struct{}{}
		}
		return nil
	}); err != nil {
		t.Fatalf("raw index scan: %v", err)
	}
	for i := int64(6); i <= 10; i++ {
		if _, found := rawIDs[i]; found {
			t.Fatalf("phantom on-disk index row for node %d (write never committed) survived the crash", i)
		}
	}
	for i := int64(1); i <= 5; i++ {
		if _, found := rawIDs[i]; !found {
			t.Fatalf("missing on-disk index row for committed node %d after reopen", i)
		}
	}
	if len(rawIDs) != 5 {
		t.Fatalf("expected exactly 5 raw index rows after reopen, got %d: %v", len(rawIDs), rawIDs)
	}
}

// --- corruption-path purge (white-box, direct) ---

// TestMaintainPropertyIndexesPurge_DiskMode exercises the corruption-path
// brute-force purge directly (the real trigger — a decode failure during
// DeleteNodeCascade — is exercised for the RAM arm nowhere in the existing
// suite either; this pins the NEW disk-mode logic in isolation).
func TestMaintainPropertyIndexesPurge_DiskMode(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStorePropIdxOnDisk(t)
	const label = uint16(1)
	if err := bs.CreatePropertyIndex(label, "age"); err != nil {
		t.Fatalf("CreatePropertyIndex age: %v", err)
	}
	if err := bs.CreatePropertyIndex(label, "city"); err != nil {
		t.Fatalf("CreatePropertyIndex city: %v", err)
	}
	n := types.NewNode(types.NodeID(1), label, nil)
	if err := n.SetProperty("age", int64(5)); err != nil {
		t.Fatal(err)
	}
	if err := n.SetProperty("city", "nyc"); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := nodeIDsVia(t, bs, label, "age", int64(5)); fmt.Sprint(got) != "[1]" {
		t.Fatalf("pre-purge: age got=%v want=[1]", got)
	}

	bs.idxMu.Lock()
	ops := bs.maintainPropertyIndexesPurge(snowflake.ID(1))
	bs.idxMu.Unlock()
	if len(ops) != 2 {
		t.Fatalf("expected 2 purge ops (age+city rows), got %d", len(ops))
	}
	for _, op := range ops {
		if op.opType != writeOpDelete {
			t.Fatalf("purge op must be a delete, got %v", op.opType)
		}
	}
	bs.appendOps(ops...)
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush purge: %v", err)
	}
	if got := nodeIDsVia(t, bs, label, "age", int64(5)); len(got) != 0 {
		t.Fatalf("post-purge: age got=%v want=[]", got)
	}
	if got := nodeIDsVia(t, bs, label, "city", "nyc"); len(got) != 0 {
		t.Fatalf("post-purge: city got=%v want=[]", got)
	}
}

// TestMaintainPropertyIndexesPurge_DiskMode_PendingOverlay exercises the
// unflushed-SET branch of the purge scan: a node's property-index rows are
// still sitting in the write buffer (never flushed to Badger) when the
// corruption-path purge runs, and must still be found and deleted.
func TestMaintainPropertyIndexesPurge_DiskMode_PendingOverlay(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStorePropIdxOnDisk(t)
	const label = uint16(1)
	if err := bs.CreatePropertyIndex(label, "age"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}
	n := types.NewNode(types.NodeID(1), label, nil)
	if err := n.SetProperty("age", int64(5)); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	// No Flush() — the age=5 row is still pending only.
	if got := nodeIDsVia(t, bs, label, "age", int64(5)); fmt.Sprint(got) != "[1]" {
		t.Fatalf("pre-purge (pending): age got=%v want=[1]", got)
	}

	bs.idxMu.Lock()
	ops := bs.maintainPropertyIndexesPurge(snowflake.ID(1))
	bs.idxMu.Unlock()
	if len(ops) != 1 {
		t.Fatalf("expected 1 purge op for the pending row, got %d", len(ops))
	}
	if ops[0].opType != writeOpDelete {
		t.Fatalf("purge op must be a delete, got %v", ops[0].opType)
	}
	bs.appendOps(ops...)
	if got := nodeIDsVia(t, bs, label, "age", int64(5)); len(got) != 0 {
		t.Fatalf("post-purge (still pending): age got=%v want=[]", got)
	}
}

// --- config validation ---

// TestPropertyIndexOnDisk_RequiresPropertyKeyRegistry pins the fail-closed
// guard: CreatePropertyIndex in disk mode without a wired property-key
// registry returns a sentinel-wrapped error, checked with errors.Is at the
// store-level call layer.
func TestPropertyIndexOnDisk_RequiresPropertyKeyRegistry(t *testing.T) {
	t.Parallel()
	bs, err := New(Config{InMemory: true, PropertyIndexOnDisk: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { bs.Close() })

	err = bs.CreatePropertyIndex(1, "age")
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("expected ErrInvalidStoreMutation, got %v", err)
	}
}

// --- race ---

// TestPropertyIndexOnDisk_ConcurrentMutationAndRead races PutNode / ReplaceNode
// / DeleteNode against NodesByLabelAndProperty reads under -race.
func TestPropertyIndexOnDisk_ConcurrentMutationAndRead(t *testing.T) {
	bs := newTestBadgerStorePropIdxOnDisk(t)
	const label = uint16(1)
	if err := bs.CreatePropertyIndex(label, "age"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}

	const n = 40
	for i := 1; i <= n; i++ {
		node := types.NewNode(types.NodeID(i), label, nil)
		if err := node.SetProperty("age", int64(i%5)); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(node); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}

	var writers sync.WaitGroup
	var readers sync.WaitGroup
	stop := make(chan struct{})

	// Writers: cycle ReplaceNode across a subset of ages.
	for w := 0; w < 4; w++ {
		writers.Add(1)
		go func(w int) {
			defer writers.Done()
			for iter := 0; iter < 200; iter++ {
				id := types.NodeID(1 + (iter+w)%n)
				node, err := bs.GetNode(id)
				if err != nil {
					continue
				}
				if err := node.SetProperty("age", int64((iter+w)%5)); err != nil {
					continue
				}
				_ = bs.ReplaceNode(node)
			}
		}(w)
	}

	// Readers: concurrent equality lookups, running until writers finish.
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for age := int64(0); age < 5; age++ {
					_, _ = bs.NodesByLabelAndProperty(label, "age", age, QueryOpts{})
				}
			}
		}()
	}

	writers.Wait()
	close(stop)
	readers.Wait()
}
