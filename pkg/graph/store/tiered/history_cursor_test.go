package tiered

import (
	"errors"
	"reflect"
	"sort"
	"testing"

	badgerv4 "github.com/dgraph-io/badger/v4"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// seedTieredHistory writes one history version per node across both ref and
// event shards. Returns the sorted slice of all written IDs.
func seedTieredHistory(t *testing.T, ts *Store, refCount, evtCount int) []types.NodeID {
	t.Helper()
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	all := make([]types.NodeID, 0, refCount+evtCount)
	for i := 0; i < refCount; i++ {
		n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode ref: %v", err)
		}
		if err := ts.PutNodeVersion(n.ID(), 0, n); err != nil {
			t.Fatalf("PutNodeVersion ref: %v", err)
		}
		all = append(all, n.ID())
	}
	for i := 0; i < evtCount; i++ {
		n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode evt: %v", err)
		}
		if err := ts.PutNodeVersion(n.ID(), 0, n); err != nil {
			t.Fatalf("PutNodeVersion evt: %v", err)
		}
		all = append(all, n.ID())
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	return all
}

func TestTieredStore_AllNodeHistoryIDsFrom_Empty(t *testing.T) {
	ts := newTestTieredStore(t)

	got, err := ts.AllNodeHistoryIDsFrom(types.NodeID(0), 64)
	if err != nil {
		t.Fatalf("AllNodeHistoryIDsFrom(empty): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("AllNodeHistoryIDsFrom(empty) = %v; want empty", got)
	}
}

func TestTieredStore_AllHistoryIDsFromRejectsInvalidPagination(t *testing.T) {
	ts := newTestTieredStore(t)

	checks := []struct {
		name string
		run  func() error
		want error
	}{
		{name: "node negative limit", run: func() error {
			_, err := ts.AllNodeHistoryIDsFrom(types.NodeID(0), -1)
			return err
		}, want: storecontract.ErrInvalidQueryLimit},
		{name: "node negative cursor", run: func() error {
			_, err := ts.AllNodeHistoryIDsFrom(types.NodeID(-1), 0)
			return err
		}, want: storecontract.ErrInvalidQueryCursor},
		{name: "rel negative limit", run: func() error {
			_, err := ts.AllRelHistoryIDsFrom(types.RelID(0), -1)
			return err
		}, want: storecontract.ErrInvalidQueryLimit},
		{name: "rel negative cursor", run: func() error {
			_, err := ts.AllRelHistoryIDsFrom(types.RelID(-1), 0)
			return err
		}, want: storecontract.ErrInvalidQueryCursor},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.run(); !errors.Is(err, check.want) {
				t.Fatalf("err = %v, want %v", err, check.want)
			}
		})
	}
}

func TestTieredStore_AllNodeHistoryIDsFrom_LimitZeroAllRemaining(t *testing.T) {
	ts := newTestTieredStore(t)
	want := seedTieredHistory(t, ts, 50, 50)

	got, err := ts.AllNodeHistoryIDsFrom(types.NodeID(0), 0)
	if err != nil {
		t.Fatalf("AllNodeHistoryIDsFrom(0,0): %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("limit=0 must return all; got %d, want %d", len(got), len(want))
	}

	all, err := ts.AllNodeHistoryIDs()
	if err != nil {
		t.Fatalf("AllNodeHistoryIDs: %v", err)
	}
	if !reflect.DeepEqual(got, all) {
		t.Fatal("limit=0 result must equal AllNodeHistoryIDs result")
	}
}

func TestTieredStore_AllNodeHistoryIDsFrom_CursorMatchesUnpaginated(t *testing.T) {
	ts := newTestTieredStore(t)
	want := seedTieredHistory(t, ts, 200, 300)

	var all []types.NodeID
	var cursor types.NodeID
	const pageSize = 64
	for {
		page, err := ts.AllNodeHistoryIDsFrom(cursor, pageSize)
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		if len(page) == 0 {
			break
		}
		// Each page is strictly ascending and strictly > cursor.
		for i, id := range page {
			if id.SnowflakeID() <= cursor.SnowflakeID() {
				t.Fatalf("page[%d]=%d not strictly > cursor=%d", i, id, cursor)
			}
			if i > 0 && id <= page[i-1] {
				t.Fatalf("page[%d]=%d not strictly > page[%d]=%d", i, id, i-1, page[i-1])
			}
		}
		all = append(all, page...)
		if len(page) < pageSize {
			break
		}
		cursor = page[len(page)-1]
	}
	if !reflect.DeepEqual(all, want) {
		t.Fatalf("paginated walk diverges: paged=%d, want=%d", len(all), len(want))
	}
}

// TestTieredStore_AllNodeHistoryIDsFrom_CrossShardDedup covers the case where
// a node has history records on more than one shard (archive + ref, archive +
// event after archive-then-update). The cursor must report each ID exactly
// once.
func TestTieredStore_AllNodeHistoryIDsFrom_CrossShardDedup(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	// Write a history version that lands on the reference shard.
	if err := ts.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatalf("PutNodeVersion v0: %v", err)
	}

	// Plant the SAME ID on a second shard to simulate cross-shard duplication.
	// We do this by writing directly to the hot event shard's BadgerStore.
	// This mirrors what an ArchiveNode/RestoreNode interleave could leave
	// behind in pathological cases.
	hot := ts.HotShardForTest().Store()
	nV1 := n.DeepCopy()
	nV1.SetVersion(1)
	if err := hot.PutNodeVersion(n.ID(), 1, nV1); err != nil {
		t.Fatalf("hot.PutNodeVersion: %v", err)
	}

	// Single-page scan must collapse to one entry, not two.
	got, err := ts.AllNodeHistoryIDsFrom(types.NodeID(0), 0)
	if err != nil {
		t.Fatalf("AllNodeHistoryIDsFrom: %v", err)
	}
	count := 0
	for _, id := range got {
		if id == n.ID() {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("cross-shard ID reported %d times, want 1: %v", count, got)
	}

	// Walk paged — the same dedup must hold.
	var paged []types.NodeID
	var cursor types.NodeID
	for {
		page, err := ts.AllNodeHistoryIDsFrom(cursor, 8)
		if err != nil {
			t.Fatalf("paged: %v", err)
		}
		if len(page) == 0 {
			break
		}
		paged = append(paged, page...)
		if len(page) < 8 {
			break
		}
		cursor = page[len(page)-1]
	}
	count = 0
	for _, id := range paged {
		if id == n.ID() {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("paged cross-shard ID reported %d times, want 1: %v", count, paged)
	}
}

func TestTieredStore_GetNodeHistory_MergesReferenceAndArchiveVersions(t *testing.T) {
	ts, caseTok, _ := newArchiveWriteTestStore(t)

	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatalf("PutNodeVersion ref: %v", err)
	}
	if err := ts.ArchiveNode(n.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	currentV1 := n.DeepCopy()
	currentV1.SetVersion(1)
	if err := ts.ReplaceNodeWithHistory(currentV1, n.Version(), n); err != nil {
		t.Fatalf("ReplaceNodeWithHistory archive v1: %v", err)
	}
	currentV2 := currentV1.DeepCopy()
	currentV2.SetVersion(2)
	if err := ts.ReplaceNodeWithHistory(currentV2, currentV1.Version(), currentV1); err != nil {
		t.Fatalf("ReplaceNodeWithHistory archive v2: %v", err)
	}

	history, err := ts.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if versions := tieredNodeHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{0, 1}) {
		t.Fatalf("GetNodeHistory versions = %v, want [0 1]", versions)
	}

	if err := ts.TruncateNodeHistory(n.ID(), 1); err != nil {
		t.Fatalf("TruncateNodeHistory: %v", err)
	}
	history, err = ts.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatalf("GetNodeHistory after truncate: %v", err)
	}
	if versions := tieredNodeHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{1}) {
		t.Fatalf("GetNodeHistory after truncate versions = %v, want [1]", versions)
	}
}

func TestTieredStore_NodeHistoryVersionsFrom_MergesReferenceAndArchiveVersions(t *testing.T) {
	ts, caseTok, _ := newArchiveWriteTestStore(t)

	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatalf("PutNodeVersion ref: %v", err)
	}
	if err := ts.ArchiveNode(n.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	currentV1 := n.DeepCopy()
	currentV1.SetVersion(1)
	if err := ts.ReplaceNodeWithHistory(currentV1, n.Version(), n); err != nil {
		t.Fatalf("ReplaceNodeWithHistory archive v1: %v", err)
	}
	currentV2 := currentV1.DeepCopy()
	currentV2.SetVersion(2)
	if err := ts.ReplaceNodeWithHistory(currentV2, currentV1.Version(), currentV1); err != nil {
		t.Fatalf("ReplaceNodeWithHistory archive v2: %v", err)
	}

	history, err := ts.NodeHistoryVersionsFrom(n.ID(), 0, 2)
	if err != nil {
		t.Fatalf("NodeHistoryVersionsFrom start 0 limit 2: %v", err)
	}
	if versions := tieredNodeHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{0, 1}) {
		t.Fatalf("NodeHistoryVersionsFrom versions = %v, want [0 1]", versions)
	}
	history, err = ts.NodeHistoryVersionsFrom(n.ID(), 1, 1)
	if err != nil {
		t.Fatalf("NodeHistoryVersionsFrom start 1 limit 1: %v", err)
	}
	if versions := tieredNodeHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{1}) {
		t.Fatalf("NodeHistoryVersionsFrom paged versions = %v, want [1]", versions)
	}
}

func TestTieredStore_NodeHistoryReadersPropagateCurrentRowCorruption(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}
	corruptTieredNodeRowAfterFlush(t, ts.RefShardForTest(), n.ID())

	checks := []struct {
		name string
		run  func() error
	}{
		{
			name: "GetNodeVersion",
			run: func() error {
				_, err := ts.GetNodeVersion(n.ID(), 99)
				return err
			},
		},
		{
			name: "GetNodeHistory",
			run: func() error {
				_, err := ts.GetNodeHistory(n.ID())
				return err
			},
		},
		{
			name: "NodeHistoryVersionsFrom",
			run: func() error {
				_, err := ts.NodeHistoryVersionsFrom(n.ID(), 0, 1)
				return err
			},
		},
		{
			name: "TruncateNodeHistory",
			run: func() error {
				return ts.TruncateNodeHistory(n.ID(), 1)
			},
		},
	}

	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			err := check.run()
			if err == nil {
				t.Fatal("expected corruption error, got nil")
			}
			if errors.Is(err, ErrNodeNotFound) || errors.Is(err, ErrVersionNotFound) {
				t.Fatalf("got typed not-found error for corrupt current row: %v", err)
			}
		})
	}
}

func corruptTieredNodeRowAfterFlush(t *testing.T, shard *BadgerStore, nid types.NodeID) {
	t.Helper()
	if err := shard.Flush(); err != nil {
		t.Fatalf("Flush node shard: %v", err)
	}
	shard.NodeCacheForTest().ResetForTest()
	if err := shard.DBForTest().Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storeutil.NodeKey(nid.SnowflakeID()), []byte("corrupt-node-wire"))
	}); err != nil {
		t.Fatalf("corrupt node row: %v", err)
	}
}

func TestTieredStore_AllRelHistoryIDsFrom_Empty(t *testing.T) {
	ts := newTestTieredStore(t)

	got, err := ts.AllRelHistoryIDsFrom(types.RelID(0), 64)
	if err != nil {
		t.Fatalf("AllRelHistoryIDsFrom(empty): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("AllRelHistoryIDsFrom(empty) = %v; want empty", got)
	}
}

func TestTieredStore_AllRelHistoryIDsFrom_LimitZeroAllRemaining(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")

	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	// Real ref nodes as endpoints.
	a := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	b := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(a); err != nil {
		t.Fatalf("PutNode a: %v", err)
	}
	if err := ts.PutNode(b); err != nil {
		t.Fatalf("PutNode b: %v", err)
	}

	want := make([]types.RelID, 0, 50)
	for i := 0; i < 50; i++ {
		r := types.NewRelationship(types.RelID(relGen.Generate()), 1, a.ID(), b.ID())
		if err := ts.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship: %v", err)
		}
		if err := ts.PutRelVersion(r.ID(), 0, r); err != nil {
			t.Fatalf("PutRelVersion: %v", err)
		}
		want = append(want, r.ID())
	}
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })

	got, err := ts.AllRelHistoryIDsFrom(types.RelID(0), 0)
	if err != nil {
		t.Fatalf("AllRelHistoryIDsFrom(0,0): %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("limit=0 must return all; got %d, want %d", len(got), len(want))
	}

	all, err := ts.AllRelHistoryIDs()
	if err != nil {
		t.Fatalf("AllRelHistoryIDs: %v", err)
	}
	if !reflect.DeepEqual(got, all) {
		t.Fatal("limit=0 result must equal AllRelHistoryIDs result")
	}
}

func TestTieredStore_GetRelHistory_MergesReferenceAndArchiveVersions(t *testing.T) {
	ts, caseTok, _ := newArchiveWriteTestStore(t)

	gen := tieredNodeGen(t)
	a := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	b := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(a); err != nil {
		t.Fatalf("PutNode a: %v", err)
	}
	if err := ts.PutNode(b); err != nil {
		t.Fatalf("PutNode b: %v", err)
	}

	r := newArchiveWriteRel(t, a.ID(), b.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := ts.PutRelVersion(r.ID(), 0, r); err != nil {
		t.Fatalf("PutRelVersion ref: %v", err)
	}
	if err := ts.ArchiveNode(a.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	currentV1 := r.DeepCopy()
	currentV1.SetVersion(1)
	if err := ts.ReplaceRelWithHistory(currentV1, r.Version(), r); err != nil {
		t.Fatalf("ReplaceRelWithHistory archive v1: %v", err)
	}
	currentV2 := currentV1.DeepCopy()
	currentV2.SetVersion(2)
	if err := ts.ReplaceRelWithHistory(currentV2, currentV1.Version(), currentV1); err != nil {
		t.Fatalf("ReplaceRelWithHistory archive v2: %v", err)
	}

	history, err := ts.GetRelHistory(r.ID())
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if versions := tieredRelHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{0, 1}) {
		t.Fatalf("GetRelHistory versions = %v, want [0 1]", versions)
	}

	if err := ts.TruncateRelHistory(r.ID(), 1); err != nil {
		t.Fatalf("TruncateRelHistory: %v", err)
	}
	history, err = ts.GetRelHistory(r.ID())
	if err != nil {
		t.Fatalf("GetRelHistory after truncate: %v", err)
	}
	if versions := tieredRelHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{1}) {
		t.Fatalf("GetRelHistory after truncate versions = %v, want [1]", versions)
	}
}

func TestTieredStore_RelHistoryVersionsFrom_MergesReferenceAndArchiveVersions(t *testing.T) {
	ts, caseTok, _ := newArchiveWriteTestStore(t)

	gen := tieredNodeGen(t)
	a := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	b := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(a); err != nil {
		t.Fatalf("PutNode a: %v", err)
	}
	if err := ts.PutNode(b); err != nil {
		t.Fatalf("PutNode b: %v", err)
	}

	r := newArchiveWriteRel(t, a.ID(), b.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := ts.PutRelVersion(r.ID(), 0, r); err != nil {
		t.Fatalf("PutRelVersion ref: %v", err)
	}
	if err := ts.ArchiveNode(a.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	currentV1 := r.DeepCopy()
	currentV1.SetVersion(1)
	if err := ts.ReplaceRelWithHistory(currentV1, r.Version(), r); err != nil {
		t.Fatalf("ReplaceRelWithHistory archive v1: %v", err)
	}
	currentV2 := currentV1.DeepCopy()
	currentV2.SetVersion(2)
	if err := ts.ReplaceRelWithHistory(currentV2, currentV1.Version(), currentV1); err != nil {
		t.Fatalf("ReplaceRelWithHistory archive v2: %v", err)
	}

	history, err := ts.RelHistoryVersionsFrom(r.ID(), 0, 2)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom start 0 limit 2: %v", err)
	}
	if versions := tieredRelHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{0, 1}) {
		t.Fatalf("RelHistoryVersionsFrom versions = %v, want [0 1]", versions)
	}
	history, err = ts.RelHistoryVersionsFrom(r.ID(), 1, 1)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom start 1 limit 1: %v", err)
	}
	if versions := tieredRelHistoryVersions(history); !reflect.DeepEqual(versions, []uint32{1}) {
		t.Fatalf("RelHistoryVersionsFrom paged versions = %v, want [1]", versions)
	}
}

func TestTieredStore_RelHistoryReadersPropagateCurrentRowCorruption(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	a := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	b := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(a); err != nil {
		t.Fatalf("PutNode a: %v", err)
	}
	if err := ts.PutNode(b); err != nil {
		t.Fatalf("PutNode b: %v", err)
	}

	r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, a.ID(), b.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := ts.PutRelVersion(r.ID(), 0, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}
	corruptTieredRelRowAfterFlush(t, ts.RefShardForTest(), r.ID())

	checks := []struct {
		name string
		run  func() error
	}{
		{
			name: "GetRelVersion",
			run: func() error {
				_, err := ts.GetRelVersion(r.ID(), 99)
				return err
			},
		},
		{
			name: "GetRelHistory",
			run: func() error {
				_, err := ts.GetRelHistory(r.ID())
				return err
			},
		},
		{
			name: "RelHistoryVersionsFrom",
			run: func() error {
				_, err := ts.RelHistoryVersionsFrom(r.ID(), 0, 1)
				return err
			},
		},
		{
			name: "TruncateRelHistory",
			run: func() error {
				return ts.TruncateRelHistory(r.ID(), 1)
			},
		},
	}

	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			err := check.run()
			if err == nil {
				t.Fatal("expected corruption error, got nil")
			}
			if errors.Is(err, ErrRelNotFound) || errors.Is(err, ErrVersionNotFound) {
				t.Fatalf("got typed not-found error for corrupt current row: %v", err)
			}
		})
	}
}

func corruptTieredRelRowAfterFlush(t *testing.T, shard *BadgerStore, rid types.RelID) {
	t.Helper()
	if err := shard.Flush(); err != nil {
		t.Fatalf("Flush relationship shard: %v", err)
	}
	shard.RelCacheForTest().ResetForTest()
	if err := shard.DBForTest().Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storeutil.RelKey(rid.SnowflakeID()), []byte("corrupt-rel-wire"))
	}); err != nil {
		t.Fatalf("corrupt relationship row: %v", err)
	}
}

func TestTieredStore_GetVersionFindsArchiveHistoryAfterRestore(t *testing.T) {
	ts, caseTok, _ := newArchiveWriteTestStore(t)

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.ArchiveNode(n.ID()); err != nil {
		t.Fatalf("ArchiveNode node: %v", err)
	}
	nodeCurrent := n.DeepCopy()
	nodeCurrent.SetVersion(1)
	if err := ts.ReplaceNodeWithHistory(nodeCurrent, n.Version(), n); err != nil {
		t.Fatalf("ReplaceNodeWithHistory archive: %v", err)
	}
	if err := ts.RestoreNode(n.ID()); err != nil {
		t.Fatalf("RestoreNode node: %v", err)
	}
	nodeVersion, err := ts.GetNodeVersion(n.ID(), n.Version())
	if err != nil {
		t.Fatalf("GetNodeVersion restored archive history: %v", err)
	}
	if nodeVersion.Version() != n.Version() {
		t.Fatalf("GetNodeVersion restored archive history version = %d, want %d", nodeVersion.Version(), n.Version())
	}

	a := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	b := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(a); err != nil {
		t.Fatalf("PutNode a: %v", err)
	}
	if err := ts.PutNode(b); err != nil {
		t.Fatalf("PutNode b: %v", err)
	}
	r := newArchiveWriteRel(t, a.ID(), b.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := ts.ArchiveNode(a.ID()); err != nil {
		t.Fatalf("ArchiveNode relationship: %v", err)
	}
	relCurrent := r.DeepCopy()
	relCurrent.SetVersion(1)
	if err := ts.ReplaceRelWithHistory(relCurrent, r.Version(), r); err != nil {
		t.Fatalf("ReplaceRelWithHistory archive: %v", err)
	}
	if err := ts.RestoreNode(a.ID()); err != nil {
		t.Fatalf("RestoreNode relationship: %v", err)
	}
	relVersion, err := ts.GetRelVersion(r.ID(), r.Version())
	if err != nil {
		t.Fatalf("GetRelVersion restored archive history: %v", err)
	}
	if relVersion.Version() != r.Version() {
		t.Fatalf("GetRelVersion restored archive history version = %d, want %d", relVersion.Version(), r.Version())
	}
}

func tieredNodeHistoryVersions(history []*types.Node) []uint32 {
	versions := make([]uint32, len(history))
	for i, n := range history {
		versions[i] = n.Version()
	}
	return versions
}
