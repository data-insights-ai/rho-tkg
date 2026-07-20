package memory

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 17h (remainder): CreateCompositePropertyIndex, CreateTemporalIndex,
// CreateHighFrequencyIndex, and CreateVectorIndexWithOptions were ALSO
// rewritten from "hold ms.mu.Lock() across the whole scan-and-build" to the
// same 3-phase pattern already proven out for CreatePropertyIndex (see
// memorystore_index_3phase_test.go's doc comment for the general shape and
// TestCreatePropertyIndex_ReleasesLockDuringScan/_ConcurrentMutationDuringScanIsReconciled
// for the template these tests mirror).

// --- CreateCompositePropertyIndex ---

func TestCreateCompositePropertyIndex_ReleasesLockDuringScan(t *testing.T) {
	ms := New()
	const n = 20_000
	for i := 1; i <= n; i++ {
		nd := memNode(int64(i), 10)
		if err := nd.SetProperty("a", int64(1)); err != nil {
			t.Fatalf("SetProperty(a, %d): %v", i, err)
		}
		if err := nd.SetProperty("b", int64(2)); err != nil {
			t.Fatalf("SetProperty(b, %d): %v", i, err)
		}
		if err := ms.PutNode(nd); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}

	var tryLockSuccesses atomic.Int64
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			if ms.mu.TryLock() {
				tryLockSuccesses.Add(1)
				ms.mu.Unlock()
			}
		}
	}()

	if err := ms.CreateCompositePropertyIndex(10, []string{"a", "b"}); err != nil {
		t.Fatalf("CreateCompositePropertyIndex: %v", err)
	}
	close(done)
	wg.Wait()

	// Threshold rationale: see TestCreatePropertyIndex_ReleasesLockDuringScan.
	if got := tryLockSuccesses.Load(); got < 100 {
		t.Fatalf("TryLock succeeded only %d times while CreateCompositePropertyIndex scanned %d nodes — "+
			"the exclusive lock was held for the whole scan (BACKLOG 17h regression)", got, n)
	}
}

func TestCreateCompositePropertyIndex_ConcurrentMutationDuringScanIsReconciled(t *testing.T) {
	ms := New()
	const n = 20_000
	for i := 1; i <= n; i++ {
		nd := memNode(int64(i), 10)
		if err := nd.SetProperty("a", int64(1)); err != nil {
			t.Fatalf("SetProperty(a, %d): %v", i, err)
		}
		if err := nd.SetProperty("b", int64(2)); err != nil {
			t.Fatalf("SetProperty(b, %d): %v", i, err)
		}
		if err := ms.PutNode(nd); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(1); i <= 2_000; i++ {
			if err := ms.DeleteNode(types.NodeID(i)); err != nil {
				t.Errorf("DeleteNode(%d): %v", i, err)
				return
			}
		}
		for i := int64(2_001); i <= 4_000; i++ {
			updated := memNode(i, 10)
			if err := updated.SetProperty("a", int64(1)); err != nil {
				t.Errorf("SetProperty(a, %d): %v", i, err)
				return
			}
			if err := updated.SetProperty("b", int64(999)); err != nil {
				t.Errorf("SetProperty(b, %d): %v", i, err)
				return
			}
			if err := ms.ReplaceNode(updated); err != nil {
				t.Errorf("ReplaceNode(%d): %v", i, err)
				return
			}
		}
	}()

	if err := ms.CreateCompositePropertyIndex(10, []string{"a", "b"}); err != nil {
		t.Fatalf("CreateCompositePropertyIndex: %v", err)
	}
	wg.Wait()

	ms.mu.RLock()
	key := indexpkg.CompositeIndexKey{LabelToken: 10, Keys: indexpkg.EncodeCompositeKeyTuple([]string{"a", "b"})}
	idx := ms.compositeIndexes[key]
	ms.mu.RUnlock()
	if idx == nil {
		t.Fatal("index missing after CreateCompositePropertyIndex")
	}

	vkOriginal, ok := indexpkg.QueryCompositeValueKey(idx.Keys, map[string]any{"a": int64(1), "b": int64(2)})
	if !ok {
		t.Fatal("QueryCompositeValueKey(original) = false")
	}
	vkUpdated, ok := indexpkg.QueryCompositeValueKey(idx.Keys, map[string]any{"a": int64(1), "b": int64(999)})
	if !ok {
		t.Fatal("QueryCompositeValueKey(updated) = false")
	}

	inSet := func(ids []types.NodeID, id int64) bool {
		for _, v := range ids {
			if v == types.NodeID(id) {
				return true
			}
		}
		return false
	}

	for i := int64(1); i <= 2_000; i++ {
		if inSet(idx.NodeIDs(vkOriginal), i) {
			t.Fatalf("deleted node %d still present in index under its original value", i)
		}
	}
	for i := int64(2_001); i <= 4_000; i++ {
		if !inSet(idx.NodeIDs(vkUpdated), i) {
			t.Fatalf("updated node %d missing from index under its new value", i)
		}
		if inSet(idx.NodeIDs(vkOriginal), i) {
			t.Fatalf("updated node %d still present in index under its stale pre-update value", i)
		}
	}
	if !inSet(idx.NodeIDs(vkOriginal), n) {
		t.Fatalf("untouched node %d missing from index under its original value", n)
	}
}

// --- CreateTemporalIndex ---

func TestCreateTemporalIndex_ReleasesLockDuringScan(t *testing.T) {
	ms := New()
	const n = 20_000
	for i := 1; i <= n; i++ {
		nd := memNode(int64(i), 10)
		nd.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(1000)})
		if err := ms.PutNode(nd); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}

	var tryLockSuccesses atomic.Int64
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			if ms.mu.TryLock() {
				tryLockSuccesses.Add(1)
				ms.mu.Unlock()
			}
		}
	}()

	if err := ms.CreateTemporalIndex(10); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	close(done)
	wg.Wait()

	if got := tryLockSuccesses.Load(); got < 100 {
		t.Fatalf("TryLock succeeded only %d times while CreateTemporalIndex scanned %d nodes — "+
			"the exclusive lock was held for the whole scan (BACKLOG 17h regression)", got, n)
	}
}

func TestCreateTemporalIndex_ConcurrentMutationDuringScanIsReconciled(t *testing.T) {
	ms := New()
	const n = 20_000
	for i := 1; i <= n; i++ {
		nd := memNode(int64(i), 10)
		nd.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(1000)})
		if err := ms.PutNode(nd); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(1); i <= 2_000; i++ {
			if err := ms.DeleteNode(types.NodeID(i)); err != nil {
				t.Errorf("DeleteNode(%d): %v", i, err)
				return
			}
		}
		for i := int64(2_001); i <= 4_000; i++ {
			updated := memNode(i, 10)
			updated.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(5000)})
			if err := ms.ReplaceNode(updated); err != nil {
				t.Errorf("ReplaceNode(%d): %v", i, err)
				return
			}
		}
	}()

	if err := ms.CreateTemporalIndex(10); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	wg.Wait()

	ms.mu.RLock()
	ti := ms.temporalIndexes[10]
	ms.mu.RUnlock()
	if ti == nil {
		t.Fatal("index missing after CreateTemporalIndex")
	}

	for i := int64(1); i <= 2_000; i++ {
		if _, _, ok := ti.EnvelopeOf(snowflake.ID(i)); ok {
			t.Fatalf("deleted node %d still present in temporal index envelope", i)
		}
	}
	for i := int64(2_001); i <= 4_000; i++ {
		from, _, ok := ti.EnvelopeOf(snowflake.ID(i))
		if !ok {
			t.Fatalf("updated node %d missing from temporal index envelope", i)
		}
		if from != types.Instant(5000) {
			t.Fatalf("updated node %d envelope from = %d, want 5000 (its new ValidFrom, not the stale 1000)", i, from)
		}
	}
	from, _, ok := ti.EnvelopeOf(snowflake.ID(n))
	if !ok || from != types.Instant(1000) {
		t.Fatalf("untouched node %d envelope = (from=%d, ok=%v), want (1000, true)", n, from, ok)
	}
}

// --- CreateHighFrequencyIndex ---

func TestCreateHighFrequencyIndex_ReleasesLockDuringScan(t *testing.T) {
	ms := New()
	const n = 20_000
	for i := 1; i <= n; i++ {
		nd := memNode(int64(i), 10)
		nd.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(1000)})
		if err := ms.PutNode(nd); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}

	var tryLockSuccesses atomic.Int64
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			if ms.mu.TryLock() {
				tryLockSuccesses.Add(1)
				ms.mu.Unlock()
			}
		}
	}()

	if err := ms.CreateHighFrequencyIndex(10, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	close(done)
	wg.Wait()

	if got := tryLockSuccesses.Load(); got < 100 {
		t.Fatalf("TryLock succeeded only %d times while CreateHighFrequencyIndex scanned %d nodes — "+
			"the exclusive lock was held for the whole scan (BACKLOG 17h regression)", got, n)
	}
}

func TestCreateHighFrequencyIndex_ConcurrentMutationDuringScanIsReconciled(t *testing.T) {
	ms := New()
	const n = 20_000
	for i := 1; i <= n; i++ {
		nd := memNode(int64(i), 10)
		nd.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(1000)})
		if err := ms.PutNode(nd); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(1); i <= 2_000; i++ {
			if err := ms.DeleteNode(types.NodeID(i)); err != nil {
				t.Errorf("DeleteNode(%d): %v", i, err)
				return
			}
		}
		// 4 hours later — with a 1-hour bucketSize this lands in a DIFFERENT
		// bucket than ValidFrom=1000 (both bucket to 0 with a naive nearby
		// value, which would make this test vacuously pass regardless of
		// correctness).
		const updatedFrom = types.Instant(4 * int64(time.Hour/time.Millisecond))
		for i := int64(2_001); i <= 4_000; i++ {
			updated := memNode(i, 10)
			updated.SetTemporal(&types.TemporalMetadata{ValidFrom: updatedFrom})
			if err := ms.ReplaceNode(updated); err != nil {
				t.Errorf("ReplaceNode(%d): %v", i, err)
				return
			}
		}
	}()

	if err := ms.CreateHighFrequencyIndex(10, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	wg.Wait()

	ms.mu.RLock()
	hfi := ms.hfIndexes[10]
	ms.mu.RUnlock()
	if hfi == nil {
		t.Fatal("index missing after CreateHighFrequencyIndex")
	}

	inSet := func(ids []types.NodeID, id int64) bool {
		for _, v := range ids {
			if v == types.NodeID(id) {
				return true
			}
		}
		return false
	}

	origBucket := hfi.PointQuery(types.Instant(1000))
	updBucket := hfi.PointQuery(4 * types.Instant(time.Hour/time.Millisecond))

	for i := int64(1); i <= 2_000; i++ {
		if inSet(origBucket, i) {
			t.Fatalf("deleted node %d still present in original bucket", i)
		}
	}
	for i := int64(2_001); i <= 4_000; i++ {
		if !inSet(updBucket, i) {
			t.Fatalf("updated node %d missing from its new bucket", i)
		}
		if inSet(origBucket, i) {
			t.Fatalf("updated node %d still present in its stale original bucket", i)
		}
	}
	if !inSet(origBucket, n) {
		t.Fatalf("untouched node %d missing from its original bucket", n)
	}
}

// --- CreateVectorIndexWithOptions ---

func memVec(seed int64, dims int) []float32 {
	v := make([]float32, dims)
	for i := range v {
		v[i] = float32((seed+int64(i))%997) / 997.0
	}
	return v
}

func TestCreateVectorIndexWithOptions_ReleasesLockDuringScan(t *testing.T) {
	ms := New()
	const n = 5_000
	const dims = 4
	for i := 1; i <= n; i++ {
		nd := memNode(int64(i), 10)
		if err := nd.SetProperty("vec", memVec(int64(i), dims)); err != nil {
			t.Fatalf("SetProperty(vec, %d): %v", i, err)
		}
		if err := ms.PutNode(nd); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}

	var tryLockSuccesses atomic.Int64
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			if ms.mu.TryLock() {
				tryLockSuccesses.Add(1)
				ms.mu.Unlock()
			}
		}
	}()

	if err := ms.CreateVectorIndex(10, "vec", dims, storecontract.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	close(done)
	wg.Wait()

	// Lower threshold than the 20,000-node property-index test: HNSW insert
	// cost per node is much higher than a property-map insert, so even 5,000
	// nodes takes comparable or longer wall-clock time and clears a lower
	// TryLock-success bar comfortably under the 3-phase fix.
	if got := tryLockSuccesses.Load(); got < 20 {
		t.Fatalf("TryLock succeeded only %d times while CreateVectorIndex scanned %d nodes — "+
			"the exclusive lock was held for the whole scan (BACKLOG 17h regression)", got, n)
	}
}

func TestCreateVectorIndexWithOptions_ConcurrentMutationDuringScanIsReconciled(t *testing.T) {
	ms := New()
	const n = 5_000
	const dims = 4
	for i := 1; i <= n; i++ {
		nd := memNode(int64(i), 10)
		if err := nd.SetProperty("vec", memVec(int64(i), dims)); err != nil {
			t.Fatalf("SetProperty(vec, %d): %v", i, err)
		}
		if err := ms.PutNode(nd); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(1); i <= 500; i++ {
			if err := ms.DeleteNode(types.NodeID(i)); err != nil {
				t.Errorf("DeleteNode(%d): %v", i, err)
				return
			}
		}
	}()

	if err := ms.CreateVectorIndex(10, "vec", dims, storecontract.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	wg.Wait()

	ms.mu.RLock()
	key := indexpkg.VectorIndexKey{LabelToken: 10, PropertyKey: "vec"}
	vi := ms.vectorIndexes[key]
	ms.mu.RUnlock()
	if vi == nil {
		t.Fatal("index missing after CreateVectorIndex")
	}
	if vi.IsBuilding() {
		t.Fatal("index still reports IsBuilding after CreateVectorIndex returned")
	}

	ids := vi.IDs()
	present := make(map[snowflake.ID]bool, len(ids))
	for _, id := range ids {
		present[id] = true
	}
	for i := int64(1); i <= 500; i++ {
		if present[snowflake.ID(i)] {
			t.Fatalf("deleted node %d still present in vector index", i)
		}
	}
	if !present[snowflake.ID(n)] {
		t.Fatalf("untouched node %d missing from vector index", n)
	}
}
