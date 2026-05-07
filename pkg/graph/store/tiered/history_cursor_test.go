package tiered

import (
	"reflect"
	"sort"
	"testing"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
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
	if err := hot.PutNodeVersion(n.ID(), 1, n); err != nil {
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
