package badger

import (
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The append fast path returns data IDENTICAL to a rebuild by construction, so no
// correctness assertion can tell the two apart. These probes therefore assert on
// BOTH: the rows are right, AND the path taken was the intended one. A fast path
// that silently never fires passes every correctness test ever written.

const adLabel uint16 = 11

func adStore(t *testing.T) *Store {
	t.Helper()
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { bs.Close() })
	return bs
}

func adNode(id, qty int64) *types.Node {
	n := types.NewNode(types.NodeID(id), adLabel, nil)
	_ = n.SetProperty("qty", qty)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(id * 10)})
	return n
}

// adPut INSERTS a new node — the append shape.
func adPut(t *testing.T, bs *Store, id int64, qty int64) *types.Node {
	t.Helper()
	n := adNode(id, qty)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode %d: %v", id, err)
	}
	return n
}

// adUpdate REPLACES an existing node — remove-then-add, so it must poison.
func adUpdate(t *testing.T, bs *Store, id int64, qty int64) {
	t.Helper()
	if err := bs.ReplaceNode(adNode(id, qty)); err != nil {
		t.Fatalf("ReplaceNode %d: %v", id, err)
	}
}

// adRead forces a snapshot refresh and returns the (id -> qty) it exposes.
func adRead(t *testing.T, bs *Store) map[int64]int64 {
	t.Helper()
	got := map[int64]int64{}
	_, ok, err := bs.ForEachDocValues(adLabel, []string{"qty"},
		func(id types.NodeID, vals []any, present []bool) bool {
			if present[0] {
				got[int64(id)] = vals[0].(int64)
			}
			return true
		})
	if err != nil {
		t.Fatalf("ForEachDocValues: %v", err)
	}
	if !ok {
		t.Fatal("ForEachDocValues declined")
	}
	return got
}

func adWant(t *testing.T, got map[int64]int64, want map[int64]int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("row count %d, want %d (got=%v)", len(got), len(want), got)
	}
	for id, q := range want {
		if got[id] != q {
			t.Errorf("id %d: qty %d, want %d", id, got[id], q)
		}
	}
}

// TestAppendDelta_PureInsertExtendsInsteadOfRebuilding is the point of R3: after a
// snapshot exists, inserting new nodes must EXTEND it, not rebuild it.
func TestAppendDelta_PureInsertExtendsInsteadOfRebuilding(t *testing.T) {
	bs := adStore(t)
	for i := int64(1); i <= 5; i++ {
		adPut(t, bs, i, i*100)
	}
	adWant(t, adRead(t, bs), map[int64]int64{1: 100, 2: 200, 3: 300, 4: 400, 5: 500})

	rebuilds, extends := bs.ColumnRebuildCount(), bs.ColumnExtendCount()

	// Pure inserts — no update, no delete.
	adPut(t, bs, 6, 600)
	adPut(t, bs, 7, 700)

	adWant(t, adRead(t, bs), map[int64]int64{
		1: 100, 2: 200, 3: 300, 4: 400, 5: 500, 6: 600, 7: 700,
	})

	if got := bs.ColumnExtendCount(); got != extends+1 {
		t.Errorf("extend count %d, want %d — the append fast path did NOT fire, so "+
			"every read after a write still rebuilds the whole label", got, extends+1)
	}
	if got := bs.ColumnRebuildCount(); got != rebuilds {
		t.Errorf("rebuild count went %d -> %d; a pure insert must not rebuild",
			rebuilds, got)
	}
}

// TestAppendDelta_UpdateForcesRebuild pins the poison. An UPDATE is remove-then-add,
// so it passes the removal seam; extending across it would carry the node's OLD
// value forward, which is a silently wrong answer.
func TestAppendDelta_UpdateForcesRebuild(t *testing.T) {
	bs := adStore(t)
	for i := int64(1); i <= 3; i++ {
		adPut(t, bs, i, i*100)
	}
	adRead(t, bs)
	extends := bs.ColumnExtendCount()

	adUpdate(t, bs, 2, 999) // UPDATE of an existing node

	adWant(t, adRead(t, bs), map[int64]int64{1: 100, 2: 999, 3: 300})
	if got := bs.ColumnExtendCount(); got != extends {
		t.Error("an UPDATE was served by the append fast path — the refreshed " +
			"snapshot would carry the node's stale value")
	}
}

// TestAppendDelta_DeleteForcesRebuild pins the other poison. A deleted node is still
// in the old snapshot's ordinals; extending would keep serving its row.
func TestAppendDelta_DeleteForcesRebuild(t *testing.T) {
	bs := adStore(t)
	for i := int64(1); i <= 3; i++ {
		adPut(t, bs, i, i*100)
	}
	adRead(t, bs)
	extends := bs.ColumnExtendCount()

	if err := bs.DeleteNode(types.NodeID(2)); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	adWant(t, adRead(t, bs), map[int64]int64{1: 100, 3: 300})
	if got := bs.ColumnExtendCount(); got != extends {
		t.Error("a DELETE was served by the append fast path — the refreshed " +
			"snapshot would still contain the deleted node's row")
	}
}

// TestAppendDelta_MixedWriteBatchIsCorrect is the end-to-end oracle: an interleaving
// of inserts, updates and deletes must produce exactly what the store holds,
// whichever path each refresh took.
func TestAppendDelta_MixedWriteBatchIsCorrect(t *testing.T) {
	bs := adStore(t)
	want := map[int64]int64{}

	do := func(f func()) {
		f()
		adWant(t, adRead(t, bs), want)
	}

	for i := int64(1); i <= 4; i++ {
		id, q := i, i*100
		do(func() { adPut(t, bs, id, q); want[id] = q })
	}
	do(func() { adPut(t, bs, 5, 500); want[5] = 500 })      // insert
	do(func() { adUpdate(t, bs, 3, 3333); want[3] = 3333 }) // update
	do(func() { adPut(t, bs, 6, 600); want[6] = 600 })      // insert after update
	do(func() { _ = bs.DeleteNode(types.NodeID(1)); delete(want, 1) })
	do(func() { adPut(t, bs, 7, 700); want[7] = 700 })    // insert after delete
	do(func() { adUpdate(t, bs, 7, 777); want[7] = 777 }) // update the newest

	if bs.ColumnExtendCount() == 0 {
		t.Error("no refresh in this sequence used the append path; the probes above " +
			"would then all be testing the rebuild path against itself")
	}
}

// TestAppendDelta_DeleteThenInsertWithoutAnInterveningRead is the case NEITHER of
// the other guards catches, and the only one that proves poisonNodeLabels earns its
// place. Found by mutation-testing: deleting the poison broke no other probe.
//
//	delete X, insert Y, THEN read — with no read between the two writes.
//
// The epoch stamp matches (Y's own append recorded it), and Y is a perfectly clean
// append, so takeAppendDelta says yes and Extend says yes. But the cached snapshot
// still holds X's row, and X's absence is INVISIBLE to Extend — it only ever looks
// at what is being added. Only the removal-seam poison can catch this, and without
// it the read returns a deleted node.
//
// Every other probe reads after every single write, which rebuilds and masks it.
func TestAppendDelta_DeleteThenInsertWithoutAnInterveningRead(t *testing.T) {
	bs := adStore(t)
	for i := int64(1); i <= 4; i++ {
		adPut(t, bs, i, i*100)
	}
	adRead(t, bs) // establish the snapshot containing 1..4

	// Two writes, NO read between them.
	if err := bs.DeleteNode(types.NodeID(2)); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	adPut(t, bs, 5, 500)

	got := adRead(t, bs)
	if _, stillThere := got[2]; stillThere {
		t.Fatal("read returned node 2 AFTER it was deleted — the snapshot was " +
			"extended across a removal. Extend cannot see a deletion; only the " +
			"removal-seam poison can force the rebuild that drops the row.")
	}
	adWant(t, got, map[int64]int64{1: 100, 3: 300, 4: 400, 5: 500})
}

// TestAppendDelta_UpdateThenInsertWithoutAnInterveningRead is the sibling case: the
// stale VALUE rather than the stale row.
func TestAppendDelta_UpdateThenInsertWithoutAnInterveningRead(t *testing.T) {
	bs := adStore(t)
	for i := int64(1); i <= 4; i++ {
		adPut(t, bs, i, i*100)
	}
	adRead(t, bs)

	adUpdate(t, bs, 2, 999) // changes a value the snapshot already captured
	adPut(t, bs, 5, 500)    // a clean append riding along behind it

	got := adRead(t, bs)
	if got[2] != 999 {
		t.Fatalf("node 2 reads as %d, want 999 — the snapshot was extended across an "+
			"UPDATE and kept the stale value", got[2])
	}
	adWant(t, got, map[int64]int64{1: 100, 2: 999, 3: 300, 4: 400, 5: 500})
}

// TestAppendDelta_ExtendMatchesRebuildExactly is the differential oracle. It runs the
// SAME insert sequence twice: once letting the append path work, once defeating it
// by poisoning before every read. Both must expose identical rows.
func TestAppendDelta_ExtendMatchesRebuildExactly(t *testing.T) {
	build := func(poison bool) map[int64]int64 {
		bs := adStore(t)
		for i := int64(1); i <= 20; i++ {
			adPut(t, bs, i, i*7)
			if i%5 == 0 {
				if poison {
					// Force the rebuild path without changing the DATA.
					bs.poisonAllLabels()
				}
				adRead(t, bs)
			}
		}
		if poison {
			bs.poisonAllLabels()
		}
		return adRead(t, bs)
	}
	extended, rebuilt := build(false), build(true)
	if len(extended) != len(rebuilt) {
		t.Fatalf("row counts differ: extend=%d rebuild=%d", len(extended), len(rebuilt))
	}
	for id, q := range rebuilt {
		if extended[id] != q {
			t.Errorf("id %d: extend path says %d, rebuild path says %d", id, extended[id], q)
		}
	}
}

// TestAppendDelta_ColumnScanAgreesAfterExtend closes the loop to the public
// capability: the columnar ScanNodeColumns must see appended rows too, including
// their validity, after an extend.
func TestAppendDelta_ColumnScanAgreesAfterExtend(t *testing.T) {
	bs := adStore(t)
	for i := int64(1); i <= 4; i++ {
		adPut(t, bs, i, i*100)
	}
	adRead(t, bs) // establish the snapshot
	adPut(t, bs, 5, 500)
	adRead(t, bs) // extend

	seen := map[int64]int64{}
	err := bs.ScanNodeColumns(adLabel, []string{"qty"}, storecontract.QueryOpts{},
		func(b *storecontract.ColumnBatch) bool {
			j := 0
			for r := range b.IDs {
				if !b.Null[0][r] {
					seen[int64(b.IDs[r])] = b.Ints[0][j]
					j++
				}
				if b.ValidFrom[r] != int64(b.IDs[r])*10 {
					t.Errorf("id %d: validFrom %d, want %d — the extended rows lost "+
						"their validity columns", b.IDs[r], b.ValidFrom[r], int64(b.IDs[r])*10)
				}
			}
			return true
		})
	if err != nil {
		t.Fatalf("ScanNodeColumns: %v", err)
	}
	adWant(t, seen, map[int64]int64{1: 100, 2: 200, 3: 300, 4: 400, 5: 500})
}

// TestAppendDelta_ConcurrentInsertsAndReads is the race probe. The append record is
// written under a mutex from the write path and read from the refresh path; a torn
// record must never surface as a wrong row.
func TestAppendDelta_ConcurrentInsertsAndReads(t *testing.T) {
	bs := adStore(t)
	for i := int64(1); i <= 10; i++ {
		adPut(t, bs, i, i*100)
	}
	adRead(t, bs)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := int64(11); i <= 60; i++ {
			adPut(t, bs, i, i*100)
		}
	}()
	for i := 0; i < 50; i++ {
		for id, q := range adRead(t, bs) {
			if q != id*100 {
				t.Errorf("id %d exposed qty %d, want %d — a concurrent append "+
					"corrupted the snapshot", id, q, id*100)
				return
			}
		}
	}
	<-done
	got := adRead(t, bs)
	if len(got) != 60 {
		t.Errorf("final row count %d, want 60", len(got))
	}
	for id, q := range got {
		if q != id*100 {
			t.Errorf("id %d: qty %d, want %d", id, q, id*100)
		}
	}
}
