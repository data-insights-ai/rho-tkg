package memory

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Memory-store append-delta. Same contract as badger's, but the safety argument is
// different: POISON IS THE DEFAULT here, because this store has no single write
// seam. These probes therefore check both that the fast path fires at all and that
// it does NOT fire for anything but a clean insert.

const mdLabel uint16 = 21

func mdStore(t *testing.T) *Store {
	t.Helper()
	ms := New()
	t.Cleanup(func() { ms.Close() })
	return ms
}

func mdNode(id, qty int64) *types.Node {
	n := types.NewNode(types.NodeID(id), mdLabel, nil)
	_ = n.SetProperty("qty", qty)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(id * 10)})
	return n
}

func mdPut(t *testing.T, ms *Store, id, qty int64) {
	t.Helper()
	if err := ms.PutNode(mdNode(id, qty)); err != nil {
		t.Fatalf("PutNode %d: %v", id, err)
	}
}

func mdUpdate(t *testing.T, ms *Store, id, qty int64) {
	t.Helper()
	if err := ms.ReplaceNode(mdNode(id, qty)); err != nil {
		t.Fatalf("ReplaceNode %d: %v", id, err)
	}
}

func mdRead(t *testing.T, ms *Store) map[int64]int64 {
	t.Helper()
	got := map[int64]int64{}
	_, ok, err := ms.ForEachDocValues(mdLabel, []string{"qty"},
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

func mdWant(t *testing.T, got, want map[int64]int64) {
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

// TestMemAppendDelta_PureInsertExtends proves the fast path fires at all. Without
// this, the default-poison design could silently never opt in and every correctness
// probe below would be testing the rebuild path against itself.
func TestMemAppendDelta_PureInsertExtends(t *testing.T) {
	ms := mdStore(t)
	for i := int64(1); i <= 5; i++ {
		mdPut(t, ms, i, i*100)
	}
	mdWant(t, mdRead(t, ms), map[int64]int64{1: 100, 2: 200, 3: 300, 4: 400, 5: 500})
	rebuilds, extends := ms.ColumnRebuildCount(), ms.ColumnExtendCount()

	mdPut(t, ms, 6, 600)
	mdPut(t, ms, 7, 700)
	mdWant(t, mdRead(t, ms), map[int64]int64{
		1: 100, 2: 200, 3: 300, 4: 400, 5: 500, 6: 600, 7: 700,
	})

	if got := ms.ColumnExtendCount(); got != extends+1 {
		t.Errorf("extend count %d, want %d — the append path never fired", got, extends+1)
	}
	if got := ms.ColumnRebuildCount(); got != rebuilds {
		t.Errorf("rebuild count %d -> %d; a pure insert must not rebuild", rebuilds, got)
	}
}

// TestMemAppendDelta_DeleteThenInsertWithoutAnInterveningRead is the case that only
// the poison catches: the epoch stamp matches (the insert recorded it), the insert
// is a clean append, and the deleted row's absence is INVISIBLE to Extend.
func TestMemAppendDelta_DeleteThenInsertWithoutAnInterveningRead(t *testing.T) {
	ms := mdStore(t)
	for i := int64(1); i <= 4; i++ {
		mdPut(t, ms, i, i*100)
	}
	mdRead(t, ms)

	if err := ms.DeleteNode(types.NodeID(2)); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	mdPut(t, ms, 5, 500) // no read between the two writes

	got := mdRead(t, ms)
	if _, stillThere := got[2]; stillThere {
		t.Fatal("read returned node 2 AFTER it was deleted — the snapshot was " +
			"extended across a removal")
	}
	mdWant(t, got, map[int64]int64{1: 100, 3: 300, 4: 400, 5: 500})
}

// TestMemAppendDelta_UpdateThenInsertWithoutAnInterveningRead is the stale-VALUE
// sibling.
func TestMemAppendDelta_UpdateThenInsertWithoutAnInterveningRead(t *testing.T) {
	ms := mdStore(t)
	for i := int64(1); i <= 4; i++ {
		mdPut(t, ms, i, i*100)
	}
	mdRead(t, ms)

	mdUpdate(t, ms, 2, 999)
	mdPut(t, ms, 5, 500)

	got := mdRead(t, ms)
	if got[2] != 999 {
		t.Fatalf("node 2 reads as %d, want 999 — extended across an UPDATE", got[2])
	}
	mdWant(t, got, map[int64]int64{1: 100, 2: 999, 3: 300, 4: 400, 5: 500})
}

// TestMemAppendDelta_FailedInsertLeavesNoPhantomAppend pins the nil-poisons rule. An
// insert that ERRORS must not record an append: `inserted` stays nil, and a nil
// poisons. Otherwise the next read would extend using an ID that was never written.
func TestMemAppendDelta_FailedInsertLeavesNoPhantomAppend(t *testing.T) {
	ms := mdStore(t)
	for i := int64(1); i <= 3; i++ {
		mdPut(t, ms, i, i*100)
	}
	mdRead(t, ms)

	// A duplicate insert fails with ErrNodeExists after the epoch bump.
	if err := ms.PutNode(mdNode(2, 555)); err == nil {
		t.Fatal("expected a duplicate insert to fail")
	}
	mdPut(t, ms, 4, 400)

	mdWant(t, mdRead(t, ms), map[int64]int64{1: 100, 2: 200, 3: 300, 4: 400})
}

// TestMemAppendDelta_ExtendMatchesRebuildExactly runs one insert sequence twice —
// once with the append path live, once with it defeated — and requires identical
// rows. Both paths return the same data by construction, so this is the only oracle
// that can catch a divergence between them.
func TestMemAppendDelta_ExtendMatchesRebuildExactly(t *testing.T) {
	build := func(defeat bool) map[int64]int64 {
		ms := mdStore(t)
		for i := int64(1); i <= 20; i++ {
			mdPut(t, ms, i, i*7)
			if i%5 == 0 {
				if defeat {
					ms.mu.Lock()
					ms.appendDelta.poisoned, ms.appendDelta.byLabel = true, nil
					ms.mu.Unlock()
				}
				mdRead(t, ms)
			}
		}
		if defeat {
			ms.mu.Lock()
			ms.appendDelta.poisoned, ms.appendDelta.byLabel = true, nil
			ms.mu.Unlock()
		}
		return mdRead(t, ms)
	}
	extended, rebuilt := build(false), build(true)
	if len(extended) != len(rebuilt) {
		t.Fatalf("row counts differ: extend=%d rebuild=%d", len(extended), len(rebuilt))
	}
	for id, q := range rebuilt {
		if extended[id] != q {
			t.Errorf("id %d: extend=%d rebuild=%d", id, extended[id], q)
		}
	}
}
