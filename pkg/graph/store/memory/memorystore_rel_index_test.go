package memory

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func putMemRelWeight(t *testing.T, ms *Store, relID, startID, endID int64, typeToken uint16, weight int64) *types.Relationship {
	t.Helper()
	r := types.NewRelationship(types.RelID(snowflake.ID(relID)), typeToken, types.NodeID(snowflake.ID(startID)), types.NodeID(snowflake.ID(endID)))
	if err := r.SetProperty("weight", weight); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := ms.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship(%d): %v", relID, err)
	}
	return r
}

func putMemNodes(t *testing.T, ms *Store, ids ...int64) {
	t.Helper()
	for _, id := range ids {
		n := types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", id, err)
		}
	}
}

func TestMemStoreCreateRelPropertyIndex_BackfillAndDuplicate(t *testing.T) {
	t.Parallel()
	ms := New()
	putMemNodes(t, ms, 1, 2)
	putMemRelWeight(t, ms, 10, 1, 2, 1, 5)
	putMemRelWeight(t, ms, 20, 1, 2, 1, 9)

	if err := ms.CreateRelPropertyIndex(1, "weight"); err != nil {
		t.Fatalf("CreateRelPropertyIndex: %v", err)
	}
	if err := ms.CreateRelPropertyIndex(1, "weight"); !errors.Is(err, ErrIndexExists) {
		t.Fatalf("duplicate = %v, want ErrIndexExists", err)
	}
	got, err := ms.RelationshipsByTypeAndProperty(1, "weight", int64(5), storecontract.QueryOpts{})
	if err != nil {
		t.Fatalf("RelationshipsByTypeAndProperty: %v", err)
	}
	if len(got) != 1 || got[0].ID() != types.RelID(snowflake.ID(10)) {
		t.Fatalf("backfill weight=5: got %d rels, want rel 10", len(got))
	}
}

func TestMemStoreDropRelPropertyIndex_NotFound(t *testing.T) {
	t.Parallel()
	ms := New()
	if err := ms.DropRelPropertyIndex(1, "weight"); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("drop absent = %v, want ErrIndexNotFound", err)
	}
}

func TestMemStoreRelationshipsByTypeAndProperty_FallbackScan(t *testing.T) {
	t.Parallel()
	ms := New()
	putMemNodes(t, ms, 1, 2)
	putMemRelWeight(t, ms, 10, 1, 2, 1, 5)
	putMemRelWeight(t, ms, 20, 1, 2, 1, 9)
	// No index — fallback type scan.
	got, err := ms.RelationshipsByTypeAndProperty(1, "weight", int64(9), storecontract.QueryOpts{})
	if err != nil {
		t.Fatalf("fallback: %v", err)
	}
	if len(got) != 1 || got[0].ID() != types.RelID(snowflake.ID(20)) {
		t.Fatalf("fallback weight=9: got %d rels, want rel 20", len(got))
	}
}

func TestMemStoreForEachRelByTypePropertyRange(t *testing.T) {
	t.Parallel()
	ms := New()
	putMemNodes(t, ms, 1, 2)
	putMemRelWeight(t, ms, 10, 1, 2, 1, 1)
	putMemRelWeight(t, ms, 20, 1, 2, 1, 5)
	putMemRelWeight(t, ms, 30, 1, 2, 1, 9)
	putMemRelWeight(t, ms, 40, 1, 2, 1, 15)
	if err := ms.CreateRelPropertyIndex(1, "weight"); err != nil {
		t.Fatalf("CreateRelPropertyIndex: %v", err)
	}

	got := map[types.RelID]struct{}{}
	err := ms.ForEachRelByTypePropertyRange(1, "weight", 5, 12, true, true, storecontract.QueryOpts{}, func(r *types.Relationship) bool {
		w, _ := r.GetProperty("weight")
		if wv, ok := w.(int64); ok && wv >= 5 && wv <= 12 {
			got[r.ID()] = struct{}{}
		}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachRelByTypePropertyRange: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("range [5,12] exact recheck: got %d, want 2", len(got))
	}

	// No index for a different key → ErrIndexNotFound.
	err = ms.ForEachRelByTypePropertyRange(1, "missing", 0, 100, true, true, storecontract.QueryOpts{}, func(*types.Relationship) bool { return true })
	if !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("range no index = %v, want ErrIndexNotFound", err)
	}
}

// TestForEachRelByTypePropertyRange_CallbackCanReenterStoreWithoutDeadlock is
// the BACKLOG 17b regression: ForEachRelByTypePropertyRange used to hold
// ms.mu.RLock() across the ENTIRE callback loop, unlike every sibling
// streaming method (which snapshot IDs, release the lock, then call fn with
// no lock held). Go's sync.RWMutex documents that once a Lock() call is
// waiting, no NEW RLock() is granted until that writer proceeds — so a
// callback that re-enters the store via a read door, WHILE a concurrent
// writer is queued on Lock() behind this scan's outstanding RLock, would
// deadlock forever: the writer waits for this scan's RLock to release, and
// this scan's nested RLock (from inside fn) waits behind the now-queued
// writer. The fix must let fn call back into the store safely even under
// exactly this interleaving.
func TestForEachRelByTypePropertyRange_CallbackCanReenterStoreWithoutDeadlock(t *testing.T) {
	t.Parallel()
	ms := New()
	putMemNodes(t, ms, 1, 2)
	putMemRelWeight(t, ms, 10, 1, 2, 1, 5)
	if err := ms.CreateRelPropertyIndex(1, "weight"); err != nil {
		t.Fatalf("CreateRelPropertyIndex: %v", err)
	}

	inCallback := make(chan struct{})
	writerQueued := make(chan struct{})
	scanDone := make(chan error, 1)

	go func() {
		scanDone <- ms.ForEachRelByTypePropertyRange(1, "weight", 0, 100, true, true, storecontract.QueryOpts{}, func(r *types.Relationship) bool {
			close(inCallback)
			<-writerQueued // wait until a concurrent writer is queued on ms.mu.Lock()
			// Re-enter the store from inside the callback — this is exactly
			// what would deadlock if the scan still held its RLock here.
			if _, err := ms.GetRelationship(r.ID()); err != nil {
				t.Errorf("GetRelationship from inside callback: %v", err)
			}
			return true
		})
	}()

	<-inCallback

	writerDone := make(chan struct{})
	go func() {
		n := types.NewNode(types.NodeID(snowflake.ID(999)), 1, nil)
		_ = ms.PutNode(n)
		close(writerDone)
	}()
	// Give the writer goroutine time to actually reach and block on
	// ms.mu.Lock() (queued behind the scan's still-open outer RLock) before
	// letting the callback proceed — this ordering is what makes the
	// deadlock deterministic in the unfixed code.
	time.Sleep(50 * time.Millisecond)
	close(writerQueued)

	select {
	case err := <-scanDone:
		if err != nil {
			t.Fatalf("ForEachRelByTypePropertyRange: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ForEachRelByTypePropertyRange deadlocked when its callback re-entered the store — BACKLOG 17b regression")
	}
	<-writerDone
}

func TestMemStoreRelPropertyIndex_NilStoreGuards(t *testing.T) {
	t.Parallel()
	var ms *Store
	if err := ms.CreateRelPropertyIndex(1, "weight"); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil CreateRelPropertyIndex = %v, want ErrNilStore", err)
	}
	if err := ms.DropRelPropertyIndex(1, "weight"); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil DropRelPropertyIndex = %v, want ErrNilStore", err)
	}
	if _, err := ms.RelationshipsByTypeAndProperty(1, "weight", int64(5), storecontract.QueryOpts{}); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil RelationshipsByTypeAndProperty = %v, want ErrNilStore", err)
	}
	if err := ms.ForEachRelByTypePropertyRange(1, "weight", 0, 1, true, true, storecontract.QueryOpts{}, func(*types.Relationship) bool { return true }); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil ForEachRelByTypePropertyRange = %v, want ErrNilStore", err)
	}
}
