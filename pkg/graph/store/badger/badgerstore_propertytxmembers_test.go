package badger

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 8, badger arm of the property membership sidecars: the lazy build
// runs without idxMu, so these tests drive the windows that protocol must
// close (lessons 63, 64, 74). The door matrix lives in
// pkg/graph/store/property_tx_members_test.go.

const (
	ptxT     uint16 = 31
	ptxLabel uint16 = 41
)

func ptxVK(v int64) string { return types.IndexablePropertyValueKey(v) }

func ptxRel(id int64, seat int64, version uint32, tx types.Instant) *types.Relationship {
	r := types.NewRelationship(types.RelID(id), ptxT, types.NodeID(1), types.NodeID(2))
	_ = r.SetProperty("seat", seat)
	r.SetVersion(version)
	r.SetTemporal(&types.TemporalMetadata{TxFrom: tx})
	return r
}

func ptxNodeRow(id int64, seat int64, version uint32, tx types.Instant) *types.Node {
	n := types.NewNode(types.NodeID(id), ptxLabel, nil)
	_ = n.SetProperty("seat", seat)
	n.SetVersion(version)
	n.SetTemporal(&types.TemporalMetadata{TxFrom: tx})
	return n
}

func ptxSetup(t *testing.T, bs *Store) {
	t.Helper()
	putTestNode(t, bs, 1, 9, nil)
	putTestNode(t, bs, 2, 9, nil)
	if err := bs.CreateRelPropertyIndex(ptxT, "seat"); err != nil {
		t.Fatalf("CreateRelPropertyIndex: %v", err)
	}
	if err := bs.CreatePropertyIndex(ptxLabel, "seat"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}
}

func ptxRelMembers(t *testing.T, bs *Store, value int64) string {
	t.Helper()
	got := map[int64]types.Instant{}
	if err := bs.ForEachRelPropertyTxMember(ptxT, "seat", ptxVK(value), func(id types.RelID, tx types.Instant) bool {
		got[int64(id.SnowflakeID())] = tx
		return true
	}); err != nil {
		t.Fatalf("ForEachRelPropertyTxMember: %v", err)
	}
	return fmtPtx(got)
}

func ptxNodeMembers(t *testing.T, bs *Store, value int64) string {
	t.Helper()
	got := map[int64]types.Instant{}
	if err := bs.ForEachNodePropertyTxMember(ptxLabel, "seat", ptxVK(value), func(id types.NodeID, tx types.Instant) bool {
		got[int64(id.SnowflakeID())] = tx
		return true
	}); err != nil {
		t.Fatalf("ForEachNodePropertyTxMember: %v", err)
	}
	return fmtPtx(got)
}

func fmtPtx(m map[int64]types.Instant) string {
	ids := make([]int64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	s := "{"
	for i, id := range ids {
		if i > 0 {
			s += " "
		}
		s += fmt.Sprintf("%d:%d", id, m[id])
	}
	return s + "}"
}

// writeChains writes, for rels and nodes, a value carried only by history
// rows (an anchor and the delta after it under HistoryDeltaEncoding with
// anchor interval 2) and a value carried by the current row.
func writeChains(t *testing.T, bs *Store) {
	t.Helper()
	for _, step := range []struct {
		version uint32
		seat    int64
		tx      types.Instant
	}{{0, 1, 100}, {1, 1, 110}, {2, 1, 120}, {3, 2, 130}} {
		r, n := ptxRel(100, step.seat, step.version, step.tx), ptxNodeRow(200, step.seat, step.version, step.tx)
		if step.version == 0 {
			if err := bs.PutRelationship(r); err != nil {
				t.Fatalf("PutRelationship: %v", err)
			}
			if err := bs.PutNode(n); err != nil {
				t.Fatalf("PutNode: %v", err)
			}
			continue
		}
		prevR, prevN := ptxRel(100, 1, step.version-1, step.tx-10), ptxNodeRow(200, 1, step.version-1, step.tx-10)
		if err := bs.ReplaceRelWithHistory(r, step.version-1, prevR); err != nil {
			t.Fatalf("ReplaceRelWithHistory v%d: %v", step.version, err)
		}
		if err := bs.ReplaceNodeWithHistory(n, step.version-1, prevN); err != nil {
			t.Fatalf("ReplaceNodeWithHistory v%d: %v", step.version, err)
		}
	}
}

// TestPropTxBuild_CommittedDeltaAndPendingRows: the build reads rows from all
// three places a row can be — committed (with delta history rows needing
// their anchor), parked in `flushing`, and in `pending`. Faulty builds caught:
// skipping the write buffer (the pending/flushing rels 101/102), skipping
// delta rows or failing their anchor lookup (rel 100 v1 and v3 under delta
// encoding), and scanning current rows only (value 1 lives only in history).
func TestPropTxBuild_CommittedDeltaAndPendingRows(t *testing.T) {
	for _, delta := range []bool{false, true} {
		t.Run(fmt.Sprintf("delta=%v", delta), func(t *testing.T) {
			bs := newFlushParkStore(t, func(c *Config) {
				c.HistoryDeltaEncoding = delta
				c.HistoryAnchorInterval = 2
			})
			ptxSetup(t, bs)
			writeChains(t, bs)
			if err := bs.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if err := bs.PutRelationship(ptxRel(101, 1, 0, 140)); err != nil {
				t.Fatalf("put 101: %v", err)
			}
			parkPendingIntoFlushing(t, bs)
			if err := bs.PutRelationship(ptxRel(102, 1, 0, 150)); err != nil {
				t.Fatalf("put 102: %v", err)
			}
			if got, want := ptxRelMembers(t, bs, 1), "{100:100 101:140 102:150}"; got != want {
				t.Fatalf("rel members(1) = %s, want %s", got, want)
			}
			if got, want := ptxRelMembers(t, bs, 2), "{100:130}"; got != want {
				t.Fatalf("rel members(2) = %s, want %s", got, want)
			}
			if got, want := ptxNodeMembers(t, bs, 1), "{200:100}"; got != want {
				t.Fatalf("node members(1) = %s, want %s", got, want)
			}
		})
	}
}

// TestPropTxBuild_NoDropAcrossFlushCommit: a flush that was parked before the
// build commits while the build runs (between its overlay capture and its
// badger view). Faulty build caught: reading the badger view before the
// overlay — the parked row would be in neither.
func TestPropTxBuild_NoDropAcrossFlushCommit(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	ptxSetup(t, bs)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Value 1 lives only in rel 100's history row v0, parked in `flushing`.
	if err := bs.PutRelationship(ptxRel(100, 2, 1, 200)); err != nil {
		t.Fatalf("put current: %v", err)
	}
	if err := bs.PutRelVersion(types.RelID(100), 0, ptxRel(100, 1, 0, 100)); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}
	parkPendingIntoFlushing(t, bs)
	var once sync.Once
	bs.propTxScanTestHook = func() { once.Do(func() { commitFlushingToBadger(t, bs) }) }
	defer func() { bs.propTxScanTestHook = nil }()
	if got, want := ptxRelMembers(t, bs, 1), "{100:100}"; got != want {
		t.Fatalf("members(1) = %s, want %s: the build lost a row a flush committed during it", got, want)
	}
	if flushingLen(bs) != 0 {
		t.Fatal("hook did not fire")
	}
}

// TestPropTxBuild_WriteDuringBuildIsRecorded: a write that lands while the
// build scans (after its overlay capture, before its view: neither sees it)
// must be recorded by its door, because tracking is on before the capture.
// Faulty build caught: installing the sidecar (turning tracking on) only when
// the scan is merged. Covers a current-row door and the history door that
// enqueues without idxMu (PutRelVersion), and the node twin.
func TestPropTxBuild_WriteDuringBuildIsRecorded(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	ptxSetup(t, bs)
	var once sync.Once
	bs.propTxScanTestHook = func() {
		once.Do(func() {
			if err := bs.PutRelationship(ptxRel(300, 5, 1, 500)); err != nil {
				t.Errorf("concurrent put: %v", err)
			}
			if err := bs.PutRelVersion(types.RelID(300), 0, ptxRel(300, 6, 0, 490)); err != nil {
				t.Errorf("concurrent PutRelVersion: %v", err)
			}
		})
	}
	// The first lookup builds; the rows land in `pending` after the overlay
	// capture and are not committed, so only the doors' records can supply
	// them to this very build.
	if got, want := ptxRelMembers(t, bs, 5), "{300:500}"; got != want {
		t.Fatalf("members(5) = %s, want %s: a write during the build was lost", got, want)
	}
	if got, want := ptxRelMembers(t, bs, 6), "{300:490}"; got != want {
		t.Fatalf("members(6) = %s, want %s: a PutRelVersion during the build was lost", got, want)
	}
	bs.propTxScanTestHook = nil

	once = sync.Once{}
	bs.propTxScanTestHook = func() {
		once.Do(func() {
			if err := bs.PutNode(ptxNodeRow(400, 7, 1, 600)); err != nil {
				t.Errorf("concurrent PutNode: %v", err)
			}
			if err := bs.PutNodeVersion(types.NodeID(400), 0, ptxNodeRow(400, 8, 0, 590)); err != nil {
				t.Errorf("concurrent PutNodeVersion: %v", err)
			}
		})
	}
	if got, want := ptxNodeMembers(t, bs, 7), "{400:600}"; got != want {
		t.Fatalf("node members(7) = %s, want %s", got, want)
	}
	if got, want := ptxNodeMembers(t, bs, 8), "{400:590}"; got != want {
		t.Fatalf("node members(8) = %s, want %s", got, want)
	}
	bs.propTxScanTestHook = nil
}

// TestPropTxBuild_GenerationGuardDiscardsStaleScan: Clear lands between the
// build's scan and its install. Faulty build caught: installing the scan
// without re-checking the generation — the cleared rows would be served (and
// an erased or purged value would stay resident). The build must rescan and
// serve the post-Clear state; Builds counts only the installed build.
func TestPropTxBuild_GenerationGuardDiscardsStaleScan(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	ptxSetup(t, bs)
	if err := bs.PutRelationship(ptxRel(100, 1, 0, 100)); err != nil {
		t.Fatalf("put: %v", err)
	}
	var once sync.Once
	bs.propTxBuildTestHook = func() {
		once.Do(func() {
			if err := bs.Clear(); err != nil {
				t.Errorf("Clear: %v", err)
			}
			// Clear keeps no index definition in RAM; declare it again so the
			// lookup has an index to serve.
			putTestNode(t, bs, 1, 9, nil)
			putTestNode(t, bs, 2, 9, nil)
			if err := bs.CreateRelPropertyIndex(ptxT, "seat"); err != nil {
				t.Errorf("re-create index: %v", err)
			}
		})
	}
	defer func() { bs.propTxBuildTestHook = nil }()
	err := bs.ForEachRelPropertyTxMember(ptxT, "seat", ptxVK(1), func(id types.RelID, tx types.Instant) bool {
		t.Errorf("stale member %d:%d served after Clear", id.SnowflakeID(), tx)
		return true
	})
	if err != nil && !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("lookup: %v", err)
	}
	st, err := bs.PropertyTxMembershipStats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.RelPostings != 0 || st.Builds != 1 {
		t.Fatalf("stats after a raced build = %+v, want 0 postings and 1 installed build", st)
	}
}

// TestPropTxBuild_GenerationGuardExhaustion: an invalidation in every build
// attempt makes the lookup give up with ErrIndexNotFound (the caller folds)
// instead of looping or serving a stale scan.
func TestPropTxBuild_GenerationGuardExhaustion(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	ptxSetup(t, bs)
	if err := bs.PutRelationship(ptxRel(100, 1, 0, 100)); err != nil {
		t.Fatalf("put: %v", err)
	}
	bs.propTxBuildTestHook = func() {
		bs.idxMu.Lock()
		bs.dropPropertyTxMembersLocked()
		bs.idxMu.Unlock()
	}
	defer func() { bs.propTxBuildTestHook = nil }()
	err := bs.ForEachRelPropertyTxMember(ptxT, "seat", ptxVK(1), func(types.RelID, types.Instant) bool { return true })
	if !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("exhausted build: %v, want ErrIndexNotFound", err)
	}
	if err := bs.ForEachNodePropertyTxMember(ptxLabel, "seat", ptxVK(1), func(types.NodeID, types.Instant) bool { return true }); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("exhausted node build: %v, want ErrIndexNotFound", err)
	}
	bs.propTxBuildTestHook = nil
	if got, want := ptxRelMembers(t, bs, 1), "{100:100}"; got != want {
		t.Fatalf("members(1) after the races stop = %s, want %s", got, want)
	}
}

// TestPropTxBuild_RebuildAfterReopen: the sidecars are RAM-only; a disk store
// reopened rebuilds them from the committed keyspaces, delta rows included.
func TestPropTxBuild_RebuildAfterReopen(t *testing.T) {
	dir := t.TempDir()
	open := func() *Store {
		bs, err := New(Config{Dir: dir, HistoryDeltaEncoding: true, HistoryAnchorInterval: 2})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return bs
	}
	bs := open()
	ptxSetup(t, bs)
	writeChains(t, bs)
	if got, want := ptxRelMembers(t, bs, 1), "{100:100}"; got != want {
		t.Fatalf("before reopen members(1) = %s, want %s", got, want)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	bs = open()
	defer bs.Close()
	st, err := bs.PropertyTxMembershipStats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.RelSidecars != 0 || st.Builds != 0 {
		t.Fatalf("a reopened store holds built sidecars: %+v", st)
	}
	if got, want := ptxRelMembers(t, bs, 1), "{100:100}"; got != want {
		t.Fatalf("after reopen members(1) = %s, want %s", got, want)
	}
	if got, want := ptxNodeMembers(t, bs, 2), "{200:130}"; got != want {
		t.Fatalf("after reopen node members(2) = %s, want %s", got, want)
	}
	if st, _ := bs.PropertyTxMembershipStats(); st.RelSidecars != 1 || st.NodeSidecars != 1 || st.Builds != 2 || st.BuildDuration <= 0 {
		t.Fatalf("stats after the lazy rebuilds = %+v", st)
	}
}
