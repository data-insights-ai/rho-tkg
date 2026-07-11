package badger

import (
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// These tests pin the SCAN->MERGE commit-window race in the full-history prefix
// readers (GetNodeHistory / GetRelHistory via getNodeHistoryByPrefix /
// getRelHistoryByPrefix). Those readers snapshot Badger FIRST, then merge the
// `flushing`/`pending` overlay. A concurrent flush() that commits a parked row
// into Badger and clears `flushing` BETWEEN the reader's Badger snapshot and its
// overlay read leaves the row in NEITHER view — dropped. This is distinct from
// the plain "consult flushing" property (badgerstore_flushing_break_test.go):
// there the flush never completes; here it completes IN the window.
//
// The bug is load-dependent in the wild (a descheduled flush goroutine widens the
// window); these tests make it deterministic with historyScanTestHook, which
// fires exactly in the window and lands the concurrent-flush completion.

// commitFlushingToBadger reproduces the tail of flush(): it commits whatever is
// parked in `flushing` to Badger via a WriteBatch and clears `flushing` — exactly
// the transition (parked -> durable, snapshot dropped) that a scan-first reader
// must not lose a row across.
func commitFlushingToBadger(t *testing.T, bs *Store) {
	t.Helper()
	bs.wbMu.Lock()
	flushing := bs.flushing
	bs.wbMu.Unlock()
	if len(flushing) == 0 {
		return
	}
	wb := bs.db.NewWriteBatch()
	for _, op := range flushing {
		switch op.opType {
		case writeOpSet:
			if err := wb.SetEntry(badgerv4.NewEntry(op.key, op.value)); err != nil {
				t.Fatalf("commit flushing set: %v", err)
			}
		case writeOpDelete:
			if err := wb.Delete(op.key); err != nil {
				t.Fatalf("commit flushing delete: %v", err)
			}
		}
	}
	if err := wb.Flush(); err != nil {
		t.Fatalf("commit flushing flush: %v", err)
	}
	bs.wbMu.Lock()
	bs.flushing = nil
	bs.wbMu.Unlock()
}

// SYSTEM PROPERTY (node): GetNodeHistory must return the full version chain even
// when a concurrent flush() commits the parked history rows and clears `flushing`
// in the reader's scan->merge window. A scan-first reader observes the row in
// neither its (older) Badger snapshot nor the (now-cleared) overlay and drops it.
func TestFlushingCommitWindow_GetNodeHistory_NoDropAcrossFlush(t *testing.T) {
	bs := newFlushParkStore(t, nil)

	// Current row v1 (open) + one superseded history version v0. PutNodeVersion
	// writes the 0x07 history key into `pending` (no cache mirror), so it is the
	// row that lives ONLY in flushing during the window.
	cur := types.NewNode(types.NodeID(1), 10, nil)
	cur.SetVersion(1)
	if err := bs.PutNode(cur); err != nil {
		t.Fatalf("PutNode(current): %v", err)
	}
	v0 := types.NewNode(types.NodeID(1), 10, nil)
	v0.SetVersion(0)
	if err := bs.PutNodeVersion(types.NodeID(1), 0, v0); err != nil {
		t.Fatalf("PutNodeVersion(v0): %v", err)
	}

	parkPendingIntoFlushing(t, bs)

	var once sync.Once
	bs.historyScanTestHook = func() {
		once.Do(func() { commitFlushingToBadger(t, bs) })
	}
	defer func() { bs.historyScanTestHook = nil }()

	hist, err := bs.GetNodeHistory(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(hist) != 1 {
		t.Fatalf("GetNodeHistory dropped a version across the commit window: got %d versions, want 1 (v0). "+
			"The reader scanned Badger before the concurrent flush committed v0 and read the overlay after it was cleared.", len(hist))
	}
	if hist[0].Version() != 0 {
		t.Fatalf("GetNodeHistory returned version %d, want 0", hist[0].Version())
	}
}

// SYSTEM PROPERTY (rel mirror of the node case above).
func TestFlushingCommitWindow_GetRelHistory_NoDropAcrossFlush(t *testing.T) {
	bs := newFlushParkStore(t, nil)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)

	cur := types.NewRelationship(types.RelID(100), 5, types.NodeID(1), types.NodeID(2))
	cur.SetVersion(1)
	if err := bs.PutRelationship(cur); err != nil {
		t.Fatalf("PutRelationship(current): %v", err)
	}
	v0 := types.NewRelationship(types.RelID(100), 5, types.NodeID(1), types.NodeID(2))
	v0.SetVersion(0)
	if err := bs.PutRelVersion(types.RelID(100), 0, v0); err != nil {
		t.Fatalf("PutRelVersion(v0): %v", err)
	}

	parkPendingIntoFlushing(t, bs)

	var once sync.Once
	bs.historyScanTestHook = func() {
		once.Do(func() { commitFlushingToBadger(t, bs) })
	}
	defer func() { bs.historyScanTestHook = nil }()

	hist, err := bs.GetRelHistory(types.RelID(100))
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(hist) != 1 {
		t.Fatalf("GetRelHistory dropped a version across the commit window: got %d versions, want 1 (v0).", len(hist))
	}
	if hist[0].Version() != 0 {
		t.Fatalf("GetRelHistory returned version %d, want 0", hist[0].Version())
	}
}

// SYSTEM PROPERTY (node, DELETE direction): a version durable in Badger whose
// history-key delete is parked in `flushing` and committed mid-window must NOT
// resurface as a phantom stale row. This is the mirror-image race of
// TestFlushingCommitWindow_GetNodeHistory_NoDropAcrossFlush (which pins the
// SET-parking direction): there a concurrently-committed SET must not be lost;
// here a concurrently-committed DELETE must not be masked BACK IN by a
// scan-first reader that captured Badger before the delete landed and the
// (already-cleared) overlay after.
//
// v0 is flushed to Badger first (durable). TrimNodeHistoryFrom(0) then computes
// a delete for v0's history key and parks it in `pending` (not yet a Badger
// write). parkPendingIntoFlushing moves it into `flushing`. The test hook fires
// exactly between the reader's Badger scan and its overlay merge and commits
// that parked delete to Badger. Under the old scan-first order the reader's
// Badger snapshot (taken before the hook committed the delete) still holds the
// stale v0 row, and reading the overlay AFTER the hook sees `flushing` already
// cleared — so nothing masks the stale row and it leaks through. Under the fix
// (overlay captured BEFORE the scan) the delete is already recorded in
// overlayDeletes and masks the row regardless of when the commit lands.
func TestFlushingCommitWindow_GetNodeHistory_DeleteMaskedAcrossFlush(t *testing.T) {
	bs := newFlushParkStore(t, nil)

	cur := types.NewNode(types.NodeID(1), 10, nil)
	cur.SetVersion(1)
	if err := bs.PutNode(cur); err != nil {
		t.Fatalf("PutNode(current): %v", err)
	}
	v0 := types.NewNode(types.NodeID(1), 10, nil)
	v0.SetVersion(0)
	if err := bs.PutNodeVersion(types.NodeID(1), 0, v0); err != nil {
		t.Fatalf("PutNodeVersion(v0): %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush(v0): %v", err)
	}

	// Sanity: v0 is durable and visible before the trim.
	before, err := bs.GetNodeHistory(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNodeHistory(before trim): %v", err)
	}
	if len(before) != 1 || before[0].Version() != 0 {
		t.Fatalf("GetNodeHistory(before trim) = %v, want [v0]", before)
	}

	// Compute the delete for v0's history key; it lands in `pending` only
	// (FlushInterval=1h, no SyncWrites, buffer well under maxPending).
	if err := bs.TrimNodeHistoryFrom(types.NodeID(1), 0); err != nil {
		t.Fatalf("TrimNodeHistoryFrom: %v", err)
	}
	if pendingLen(bs) == 0 {
		t.Fatal("TrimNodeHistoryFrom did not park a delete in pending")
	}
	parkPendingIntoFlushing(t, bs)

	var once sync.Once
	bs.historyScanTestHook = func() {
		once.Do(func() { commitFlushingToBadger(t, bs) })
	}
	defer func() { bs.historyScanTestHook = nil }()

	hist, err := bs.GetNodeHistory(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(hist) != 0 {
		t.Fatalf("GetNodeHistory resurrected a phantom stale row across the commit window: got %v, want none. "+
			"The reader's Badger snapshot predates the concurrent flush's delete and the overlay was read after "+
			"`flushing` was already cleared, so nothing masked the stale row.", hist)
	}
}

// SYSTEM PROPERTY (rel mirror of the node DELETE-direction case above).
func TestFlushingCommitWindow_GetRelHistory_DeleteMaskedAcrossFlush(t *testing.T) {
	bs := newFlushParkStore(t, nil)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)

	cur := types.NewRelationship(types.RelID(100), 5, types.NodeID(1), types.NodeID(2))
	cur.SetVersion(1)
	if err := bs.PutRelationship(cur); err != nil {
		t.Fatalf("PutRelationship(current): %v", err)
	}
	v0 := types.NewRelationship(types.RelID(100), 5, types.NodeID(1), types.NodeID(2))
	v0.SetVersion(0)
	if err := bs.PutRelVersion(types.RelID(100), 0, v0); err != nil {
		t.Fatalf("PutRelVersion(v0): %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush(v0): %v", err)
	}

	before, err := bs.GetRelHistory(types.RelID(100))
	if err != nil {
		t.Fatalf("GetRelHistory(before trim): %v", err)
	}
	if len(before) != 1 || before[0].Version() != 0 {
		t.Fatalf("GetRelHistory(before trim) = %v, want [v0]", before)
	}

	if err := bs.TrimRelHistoryFrom(types.RelID(100), 0); err != nil {
		t.Fatalf("TrimRelHistoryFrom: %v", err)
	}
	if pendingLen(bs) == 0 {
		t.Fatal("TrimRelHistoryFrom did not park a delete in pending")
	}
	parkPendingIntoFlushing(t, bs)

	var once sync.Once
	bs.historyScanTestHook = func() {
		once.Do(func() { commitFlushingToBadger(t, bs) })
	}
	defer func() { bs.historyScanTestHook = nil }()

	hist, err := bs.GetRelHistory(types.RelID(100))
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(hist) != 0 {
		t.Fatalf("GetRelHistory resurrected a phantom stale row across the commit window: got %v, want none.", hist)
	}
}

// SYSTEM PROPERTY: a mix of persisted-in-Badger and parked-in-flushing versions
// must all survive the window. v0 is durable in Badger before the read; v1 and v2
// are parked in flushing and committed mid-window. None may be dropped.
func TestFlushingCommitWindow_GetNodeHistory_MixedPersistedAndParked(t *testing.T) {
	bs := newFlushParkStore(t, nil)

	cur := types.NewNode(types.NodeID(9), 10, nil)
	cur.SetVersion(3)
	if err := bs.PutNode(cur); err != nil {
		t.Fatalf("PutNode(current): %v", err)
	}
	// v0 persisted to Badger up front.
	v0 := types.NewNode(types.NodeID(9), 10, nil)
	v0.SetVersion(0)
	if err := bs.PutNodeVersion(types.NodeID(9), 0, v0); err != nil {
		t.Fatalf("PutNodeVersion(v0): %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush(v0): %v", err)
	}
	// v1, v2 stay in pending, then parked into flushing.
	for _, v := range []uint32{1, 2} {
		n := types.NewNode(types.NodeID(9), 10, nil)
		n.SetVersion(v)
		if err := bs.PutNodeVersion(types.NodeID(9), v, n); err != nil {
			t.Fatalf("PutNodeVersion(v%d): %v", v, err)
		}
	}
	parkPendingIntoFlushing(t, bs)

	var once sync.Once
	bs.historyScanTestHook = func() {
		once.Do(func() { commitFlushingToBadger(t, bs) })
	}
	defer func() { bs.historyScanTestHook = nil }()

	hist, err := bs.GetNodeHistory(types.NodeID(9))
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(hist) != 3 {
		t.Fatalf("GetNodeHistory dropped versions across the commit window: got %d, want 3 (v0,v1,v2)", len(hist))
	}
	for i, want := range []uint32{0, 1, 2} {
		if hist[i].Version() != want {
			t.Fatalf("GetNodeHistory[%d] = v%d, want v%d", i, hist[i].Version(), want)
		}
	}
}

// SYSTEM PROPERTY (concurrency, no hook): a real background flush loop racing
// GetNodeHistory reads must never transiently drop a persisted-or-parked version.
// This exercises the same window with genuine goroutine scheduling under -race.
func TestFlushingCommitWindow_ConcurrentFlushNoDrop(t *testing.T) {
	bs, err := New(Config{InMemory: true, FlushInterval: 200 * time.Microsecond})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { bs.Close() })

	const nNodes = 40
	const versionsPer = 6
	for id := int64(1); id <= nNodes; id++ {
		cur := types.NewNode(types.NodeID(id), 10, nil)
		cur.SetVersion(versionsPer)
		if err := bs.PutNode(cur); err != nil {
			t.Fatalf("PutNode(%d): %v", id, err)
		}
		for v := uint32(0); v < versionsPer; v++ {
			n := types.NewNode(types.NodeID(id), 10, nil)
			n.SetVersion(v)
			if err := bs.PutNodeVersion(types.NodeID(id), v, n); err != nil {
				t.Fatalf("PutNodeVersion(%d,v%d): %v", id, v, err)
			}
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// Writer that keeps re-touching versions so the flush loop always has work,
	// widening the commit windows the readers race against.
	wg.Add(1)
	go func() {
		defer wg.Done()
		v := uint32(versionsPer)
		for {
			select {
			case <-stop:
				return
			default:
			}
			for id := int64(1); id <= nNodes; id++ {
				n := types.NewNode(types.NodeID(id), 10, nil)
				n.SetVersion(v)
				_ = bs.PutNodeVersion(types.NodeID(id), v, n)
			}
			v++
		}
	}()

	var readErr error
	var readErrOnce sync.Once
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 400; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := int64((i % nNodes) + 1)
				hist, err := bs.GetNodeHistory(types.NodeID(id))
				if err != nil {
					readErrOnce.Do(func() { readErr = err })
					return
				}
				// The first versionsPer versions (v0..v5) are always durable/parked;
				// a correct reader never reports fewer than versionsPer of them.
				if len(hist) < versionsPer {
					readErrOnce.Do(func() {
						readErr = errDroppedVersion(id, len(hist), versionsPer)
					})
					return
				}
			}
		}()
	}

	time.Sleep(60 * time.Millisecond)
	close(stop)
	wg.Wait()
	if readErr != nil {
		t.Fatalf("concurrent GetNodeHistory observed a dropped version: %v", readErr)
	}
}

type droppedVersionErr struct {
	id       int64
	got, min int
}

func (e droppedVersionErr) Error() string {
	return "node " + itoa(e.id) + ": GetNodeHistory returned " + itoa(int64(e.got)) +
		" versions, expected at least " + itoa(int64(e.min))
}

func errDroppedVersion(id int64, got, min int) error {
	return droppedVersionErr{id: id, got: got, min: min}
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
