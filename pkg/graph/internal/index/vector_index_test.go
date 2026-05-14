package index

import (
	"errors"
	"math"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

func TestVectorIndexMutationTracking(t *testing.T) {
	vi := &VectorIndex{Dims: 2, Metric: storepkg.DistanceCosine, Mutated: make(map[snowflake.ID]struct{})}
	id := snowflake.ID(42)

	if vi.WasMutated(id) {
		t.Fatal("fresh index reports mutation")
	}
	if err := vi.Add(id, []float32{1, 0}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if !vi.WasMutated(id) {
		t.Fatal("Add did not mark ID as mutated")
	}

	vi.ClearMutationTracking()
	if vi.WasMutated(id) {
		t.Fatal("ClearMutationTracking left mutation marker behind")
	}

	vi.Mutated = make(map[snowflake.ID]struct{})
	wrongDim := snowflake.ID(77)
	if err := vi.Add(wrongDim, []float32{1, 0, 0}); err != ErrDimensionMismatch {
		t.Fatalf("Add wrong dimension error = %v, want ErrDimensionMismatch", err)
	}
	if !vi.WasMutated(wrongDim) {
		t.Fatal("Add with wrong dimensions did not mark ID as mutated")
	}

	missing := snowflake.ID(99)
	vi.Remove(missing)
	if !vi.WasMutated(missing) {
		t.Fatal("Remove of absent ID did not mark ID as mutated")
	}
}

func TestVectorIndexPositionsReplaceAndRemove(t *testing.T) {
	t.Parallel()

	vi := &VectorIndex{Dims: 2, Metric: storepkg.DistanceEuclidean}
	if err := vi.Add(1, []float32{1, 0}); err != nil {
		t.Fatalf("Add 1: %v", err)
	}
	if err := vi.Add(2, []float32{0, 1}); err != nil {
		t.Fatalf("Add 2: %v", err)
	}
	if err := vi.Add(1, []float32{2, 0}); err != nil {
		t.Fatalf("replace 1: %v", err)
	}
	if len(vi.entries) != 2 {
		t.Fatalf("replace grew entries to %d, want 2", len(vi.entries))
	}
	if got, ok := vi.positions[1]; !ok || got != 0 {
		t.Fatalf("position for 1 = %d, %v; want 0, true", got, ok)
	}

	vi.Remove(1)
	if _, ok := vi.positions[1]; ok {
		t.Fatal("Remove left deleted ID in positions")
	}
	if len(vi.entries) != 1 || vi.entries[0].id != 2 {
		t.Fatalf("entries after remove = %+v, want only id 2", vi.entries)
	}
	if cap(vi.entries) > len(vi.entries) {
		released := vi.entries[:cap(vi.entries)][len(vi.entries)]
		if released.id != 0 || released.vec != nil {
			t.Fatalf("released entry after remove = %+v, want zero value", released)
		}
	}
	if got, ok := vi.positions[2]; !ok || got != 0 {
		t.Fatalf("swapped position for 2 = %d, %v; want 0, true", got, ok)
	}
}

func TestVectorIndexAddOwned(t *testing.T) {
	t.Parallel()

	id := snowflake.ID(42)
	var nilIndex *VectorIndex
	if err := nilIndex.AddOwned(id, []float32{1, 0}); !errors.Is(err, ErrInvalidVectorIndexConfig) {
		t.Fatalf("nil AddOwned = %v, want ErrInvalidVectorIndexConfig", err)
	}

	vi := &VectorIndex{Dims: 2, Metric: storepkg.DistanceEuclidean, Mutated: make(map[snowflake.ID]struct{})}
	if err := vi.AddOwned(id, []float32{1, 0}); err != nil {
		t.Fatalf("AddOwned: %v", err)
	}
	if !vi.WasMutated(id) {
		t.Fatal("AddOwned did not mark ID as mutated")
	}
	if got := vi.IDs(); len(got) != 1 || got[0] != id {
		t.Fatalf("IDs after AddOwned = %v, want [%d]", got, id)
	}
	if err := vi.AddOwned(id, []float32{0, 1}); err != nil {
		t.Fatalf("AddOwned replace: %v", err)
	}
	if len(vi.entries) != 1 || vi.entries[0].vec[0] != 0 || vi.entries[0].vec[1] != 1 {
		t.Fatalf("entries after AddOwned replace = %+v, want one [0 1] vector", vi.entries)
	}
	if err := vi.AddOwned(99, []float32{1, 0, 0}); !errors.Is(err, ErrDimensionMismatch) {
		t.Fatalf("AddOwned dimension mismatch = %v, want ErrDimensionMismatch", err)
	}
}

func TestVectorIndexSearchNearestInvokesFilterOutsideIndexLock(t *testing.T) {
	t.Parallel()

	vi := &VectorIndex{Dims: 2, Metric: storepkg.DistanceEuclidean}
	if err := vi.Add(1, []float32{1, 0}); err != nil {
		t.Fatalf("Add 1: %v", err)
	}
	if err := vi.Add(2, []float32{0, 1}); err != nil {
		t.Fatalf("Add 2: %v", err)
	}

	filterEntered := make(chan struct{}, 1)
	releaseFilter := make(chan struct{})
	searchDone := make(chan error, 1)
	go func() {
		_, err := vi.SearchNearest([]float32{1, 0}, 1, func(snowflake.ID) bool {
			select {
			case filterEntered <- struct{}{}:
			default:
			}
			<-releaseFilter
			return true
		})
		searchDone <- err
	}()

	select {
	case <-filterEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("SearchNearest did not enter filter")
	}

	addDone := make(chan error, 1)
	go func() {
		addDone <- vi.Add(3, []float32{0.5, 0.5})
	}()

	var addErr error
	select {
	case addErr = <-addDone:
	case <-time.After(2 * time.Second):
		close(releaseFilter)
		addErr = <-addDone
		if searchErr := <-searchDone; searchErr != nil {
			t.Fatalf("SearchNearest after releasing blocked filter: %v", searchErr)
		}
		t.Fatalf("Add blocked while SearchNearest filter was paused; filter ran under vector index lock (Add after release: %v)", addErr)
	}
	if addErr != nil {
		close(releaseFilter)
		<-searchDone
		t.Fatalf("Add during paused filter: %v", addErr)
	}

	close(releaseFilter)
	if err := <-searchDone; err != nil {
		t.Fatalf("SearchNearest: %v", err)
	}
}

func TestVectorIndexPositionsRebuildFromExistingEntries(t *testing.T) {
	t.Parallel()

	vi := &VectorIndex{
		Dims:   2,
		Metric: storepkg.DistanceEuclidean,
		entries: []vectorEntry{
			{id: 10, vec: []float32{1, 0}},
			{id: 20, vec: []float32{0, 1}},
		},
	}

	vi.Remove(10)
	if len(vi.entries) != 1 || vi.entries[0].id != 20 {
		t.Fatalf("entries after remove = %+v, want only id 20", vi.entries)
	}
	if got, ok := vi.positions[20]; !ok || got != 0 {
		t.Fatalf("rebuilt position for 20 = %d, %v; want 0, true", got, ok)
	}

	if err := vi.Add(20, []float32{3, 4}); err != nil {
		t.Fatalf("replace existing rebuilt entry: %v", err)
	}
	if len(vi.entries) != 1 || vi.entries[0].vec[0] != 3 || vi.entries[0].vec[1] != 4 {
		t.Fatalf("replace after rebuild entries = %+v, want updated single entry", vi.entries)
	}
}

func TestVectorIndexPositionsRebuildCompactsDuplicateIDs(t *testing.T) {
	t.Parallel()

	vi := &VectorIndex{
		Dims:   2,
		Metric: storepkg.DistanceEuclidean,
		entries: []vectorEntry{
			{id: 10, vec: []float32{1, 0}},
			{id: 20, vec: []float32{0, 1}},
			{id: 10, vec: []float32{2, 0}},
		},
	}

	if err := vi.Add(10, []float32{3, 0}); err != nil {
		t.Fatalf("Add duplicate ID after rebuild: %v", err)
	}
	if len(vi.entries) != 2 {
		t.Fatalf("entries after duplicate compaction = %+v, want 2 unique IDs", vi.entries)
	}
	if got, ok := vi.positions[10]; !ok || got != 0 {
		t.Fatalf("position for 10 = %d, %v; want 0, true", got, ok)
	}
	if got, ok := vi.positions[20]; !ok || got != 1 {
		t.Fatalf("position for 20 = %d, %v; want 1, true", got, ok)
	}
	if vi.entries[0].id != 10 || vi.entries[0].vec[0] != 3 {
		t.Fatalf("duplicate ID replacement entry = %+v, want id 10 vector [3 0]", vi.entries[0])
	}
	if cap(vi.entries) > len(vi.entries) {
		released := vi.entries[:cap(vi.entries)][len(vi.entries)]
		if released.id != 0 || released.vec != nil {
			t.Fatalf("released entry after duplicate compaction = %+v, want zero value", released)
		}
	}

	vi.Remove(10)
	if len(vi.entries) != 1 || vi.entries[0].id != 20 {
		t.Fatalf("entries after removing compacted duplicate ID = %+v, want only id 20", vi.entries)
	}
	if _, ok := vi.positions[10]; ok {
		t.Fatal("Remove left compacted duplicate ID in positions")
	}
}

func TestVectorIndexCreatePlaceholderGuards(t *testing.T) {
	key := VectorIndexKey{LabelToken: 7, PropertyKey: "vec"}
	original := &VectorIndex{Dims: 3, Metric: storepkg.DistanceCosine}
	replacement := &VectorIndex{Dims: 3, Metric: storepkg.DistanceCosine}
	idxs := map[VectorIndexKey]*VectorIndex{key: original}

	if err := RequireVectorIndexCurrentForCreate(idxs, key, original); err != nil {
		t.Fatalf("RequireVectorIndexCurrentForCreate original: %v", err)
	}

	idxs[key] = replacement
	DeleteVectorIndexIfCurrent(idxs, key, original)
	if idxs[key] != replacement {
		t.Fatal("DeleteVectorIndexIfCurrent removed replacement index")
	}
	if err := RequireVectorIndexCurrentForCreate(idxs, key, original); !errors.Is(err, ErrVectorIndexExists) {
		t.Fatalf("RequireVectorIndexCurrentForCreate replacement = %v, want ErrVectorIndexExists", err)
	}

	delete(idxs, key)
	if err := RequireVectorIndexCurrentForCreate(idxs, key, original); !errors.Is(err, ErrVectorIndexNotFound) {
		t.Fatalf("RequireVectorIndexCurrentForCreate dropped = %v, want ErrVectorIndexNotFound", err)
	}
}

func TestValidateVectorIndexConfigRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		dims   int
		metric storepkg.DistanceMetric
	}{
		{name: "zero dims", dims: 0, metric: storepkg.DistanceCosine},
		{name: "negative dims", dims: -1, metric: storepkg.DistanceCosine},
		{name: "unsupported metric", dims: 2, metric: storepkg.DistanceMetric(99)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateVectorIndexConfig(tc.dims, tc.metric); !errors.Is(err, ErrInvalidVectorIndexConfig) {
				t.Fatalf("ValidateVectorIndexConfig(%d, %d) = %v, want ErrInvalidVectorIndexConfig",
					tc.dims, tc.metric, err)
			}
			if err := ValidateVectorIndexConfig(tc.dims, tc.metric); !errors.Is(err, storepkg.ErrInvalidVectorIndexConfig) {
				t.Fatalf("ValidateVectorIndexConfig(%d, %d) = %v, want public store ErrInvalidVectorIndexConfig",
					tc.dims, tc.metric, err)
			}
		})
	}
}

func TestVectorIndexRejectsNonFiniteVectors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		vec  []float32
	}{
		{name: "nan", vec: []float32{float32(math.NaN())}},
		{name: "positive infinity", vec: []float32{float32(math.Inf(1))}},
		{name: "negative infinity", vec: []float32{float32(math.Inf(-1))}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vi := &VectorIndex{Dims: 1, Metric: storepkg.DistanceEuclidean}
			if err := vi.Add(1, tc.vec); !errors.Is(err, ErrInvalidVectorValue) {
				t.Fatalf("Add non-finite vector = %v, want ErrInvalidVectorValue", err)
			}
			if err := vi.AddOwned(2, tc.vec); !errors.Is(err, ErrInvalidVectorValue) {
				t.Fatalf("AddOwned non-finite vector = %v, want ErrInvalidVectorValue", err)
			}
			if got := vi.IDs(); got != nil {
				t.Fatalf("IDs after rejected vector = %v, want nil", got)
			}
		})
	}

	vi := &VectorIndex{Dims: 1, Metric: storepkg.DistanceEuclidean}
	if err := vi.Add(1, []float32{0}); err != nil {
		t.Fatalf("Add finite vector: %v", err)
	}
	got, err := vi.SearchNearest([]float32{float32(math.NaN())}, 1, nil)
	if !errors.Is(err, ErrInvalidVectorValue) || got != nil {
		t.Fatalf("SearchNearest non-finite query = (%v, %v), want nil, ErrInvalidVectorValue", got, err)
	}
	got, err = vi.SearchNearest([]float32{float32(math.NaN())}, 0, nil)
	if !errors.Is(err, ErrInvalidVectorValue) || got != nil {
		t.Fatalf("SearchNearest non-finite query with non-positive k = (%v, %v), want nil, ErrInvalidVectorValue", got, err)
	}
}

func TestVectorIndexInvalidConfigReturnsErrorBeforeDistanceMath(t *testing.T) {
	t.Parallel()

	id := snowflake.ID(42)
	vi := &VectorIndex{
		Dims:    0,
		Metric:  storepkg.DistanceCosine,
		entries: []vectorEntry{{id: id, vec: []float32{1}}},
	}

	if err := vi.Add(id, []float32{1}); !errors.Is(err, ErrInvalidVectorIndexConfig) {
		t.Fatalf("Add invalid config = %v, want ErrInvalidVectorIndexConfig", err)
	}
	if _, err := vi.SearchNearest([]float32{1, 2}, 1, nil); !errors.Is(err, ErrInvalidVectorIndexConfig) {
		t.Fatalf("SearchNearest invalid config = %v, want ErrInvalidVectorIndexConfig", err)
	}
}

func TestVectorIndexNilReceiverFailClosed(t *testing.T) {
	t.Parallel()

	id := snowflake.ID(42)
	var vi *VectorIndex
	if err := vi.Add(id, []float32{1}); !errors.Is(err, ErrInvalidVectorIndexConfig) {
		t.Fatalf("nil Add = %v, want ErrInvalidVectorIndexConfig", err)
	}
	vi.Remove(id)
	if vi.WasMutated(id) {
		t.Fatal("nil WasMutated = true, want false")
	}
	if vi.IsBuilding() {
		t.Fatal("nil IsBuilding = true, want false")
	}
	vi.ClearMutationTracking()
	if got := vi.IDs(); got != nil {
		t.Fatalf("nil IDs = %v, want nil", got)
	}
	if got, err := vi.SearchNearest([]float32{1}, 1, nil); !errors.Is(err, ErrInvalidVectorIndexConfig) || got != nil {
		t.Fatalf("nil SearchNearest = %v, %v; want nil, ErrInvalidVectorIndexConfig", got, err)
	}
}

func TestVectorIndexNilMapEntriesFailClosed(t *testing.T) {
	t.Parallel()

	id := snowflake.ID(99)
	node := types.NewNode(types.NodeID(id), 1, nil)
	if err := node.SetProperty("vec", []float32{1, 0}); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}

	relevant := map[VectorIndexKey]*VectorIndex{
		{LabelToken: 1, PropertyKey: "vec"}: nil,
	}
	if updates, err := PrepareNodeVectorIndexUpdates(relevant, node, id); !errors.Is(err, ErrInvalidVectorIndexConfig) || updates != nil {
		t.Fatalf("PrepareNodeVectorIndexUpdates nil relevant index = (%v, %v), want nil, ErrInvalidVectorIndexConfig", updates, err)
	}
	RemoveNodeFromVectorIndexes(relevant, node, id)
	PurgeNodeFromAllVectorIndexes(relevant, id)

	unrelated := map[VectorIndexKey]*VectorIndex{
		{LabelToken: 2, PropertyKey: "vec"}: nil,
	}
	if updates, err := PrepareNodeVectorIndexUpdates(unrelated, node, id); err != nil || updates != nil {
		t.Fatalf("PrepareNodeVectorIndexUpdates nil unrelated index = (%v, %v), want nil, nil", updates, err)
	}
	if err := AddPreparedNodeToVectorIndexes([]NodeVectorIndexUpdate{
		{key: VectorIndexKey{LabelToken: 1, PropertyKey: "vec"}},
	}, id); !errors.Is(err, ErrInvalidVectorIndexConfig) {
		t.Fatalf("AddPreparedNodeToVectorIndexes nil index = %v, want ErrInvalidVectorIndexConfig", err)
	}
}

func TestRemoveNodeFromVectorIndexesPurgesStaleMembershipAcrossLabels(t *testing.T) {
	t.Parallel()

	id := snowflake.ID(42)
	node := types.NewNode(types.NodeID(id), 1, nil)
	if err := node.SetProperty("vec", []float32{1, 0}); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	matching := &VectorIndex{Dims: 2, Metric: storepkg.DistanceCosine}
	stale := &VectorIndex{Dims: 2, Metric: storepkg.DistanceCosine}
	if err := matching.Add(id, []float32{1, 0}); err != nil {
		t.Fatalf("matching Add: %v", err)
	}
	if err := stale.Add(id, []float32{0, 1}); err != nil {
		t.Fatalf("stale Add: %v", err)
	}

	RemoveNodeFromVectorIndexes(map[VectorIndexKey]*VectorIndex{
		{LabelToken: 1, PropertyKey: "vec"}: matching,
		{LabelToken: 2, PropertyKey: "vec"}: stale,
	}, node, id)

	if got, err := matching.SearchNearest([]float32{1, 0}, 1, nil); err != nil || len(got) != 0 {
		t.Fatalf("matching index after remove = (%v, %v), want no entries", got, err)
	}
	if got, err := stale.SearchNearest([]float32{0, 1}, 1, nil); err != nil || len(got) != 0 {
		t.Fatalf("stale index after remove = (%v, %v), want no entries", got, err)
	}
}

func TestNodeMatchesVectorIndex(t *testing.T) {
	t.Parallel()

	node := types.NewNode(types.NodeID(snowflake.ID(50)), 1, nil)
	if err := node.SetProperty("vec", []float32{1, 0}); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	key := VectorIndexKey{LabelToken: 1, PropertyKey: "vec"}
	if !NodeMatchesVectorIndex(node, key, 2) {
		t.Fatal("NodeMatchesVectorIndex returned false for matching vector row")
	}
	if NodeMatchesVectorIndex(node, VectorIndexKey{LabelToken: 2, PropertyKey: "vec"}, 2) {
		t.Fatal("NodeMatchesVectorIndex returned true for wrong label")
	}
	if NodeMatchesVectorIndex(node, key, 3) {
		t.Fatal("NodeMatchesVectorIndex returned true for wrong dimensions")
	}
	if NodeMatchesVectorIndex(node, VectorIndexKey{LabelToken: 1, PropertyKey: "missing"}, 2) {
		t.Fatal("NodeMatchesVectorIndex returned true for missing property")
	}
}

func TestPrepareNodeVectorIndexUpdates(t *testing.T) {
	t.Parallel()

	id := snowflake.ID(101)
	node := types.NewNode(types.NodeID(id), 1, []uint16{2})
	if err := node.SetProperty("vec", []float32{1, 0}); err != nil {
		t.Fatalf("SetProperty vec: %v", err)
	}
	if err := node.SetProperty("other", []float32{0, 1}); err != nil {
		t.Fatalf("SetProperty other: %v", err)
	}

	vecIdx := &VectorIndex{Dims: 2, Metric: storepkg.DistanceCosine}
	otherIdx := &VectorIndex{Dims: 2, Metric: storepkg.DistanceEuclidean}
	unmatchedIdx := &VectorIndex{Dims: 2, Metric: storepkg.DistanceCosine}
	idxs := map[VectorIndexKey]*VectorIndex{
		{LabelToken: 1, PropertyKey: "vec"}:     vecIdx,
		{LabelToken: 2, PropertyKey: "other"}:   otherIdx,
		{LabelToken: 3, PropertyKey: "vec"}:     unmatchedIdx,
		{LabelToken: 1, PropertyKey: "missing"}: {Dims: 2, Metric: storepkg.DistanceCosine},
	}

	updates, err := PrepareNodeVectorIndexUpdates(idxs, node, id)
	if err != nil {
		t.Fatalf("PrepareNodeVectorIndexUpdates: %v", err)
	}
	if len(updates) != 2 {
		t.Fatalf("prepared updates len = %d, want 2", len(updates))
	}

	if err := node.SetProperty("vec", []float32{0, 99}); err != nil {
		t.Fatalf("mutate node after prepare: %v", err)
	}
	if err := AddPreparedNodeToVectorIndexes(updates, id); err != nil {
		t.Fatalf("AddPreparedNodeToVectorIndexes: %v", err)
	}
	if got := vecIdx.IDs(); len(got) != 1 || got[0] != id {
		t.Fatalf("vecIdx IDs = %v, want [%d]", got, id)
	}
	if got := otherIdx.IDs(); len(got) != 1 || got[0] != id {
		t.Fatalf("otherIdx IDs = %v, want [%d]", got, id)
	}
	if got := unmatchedIdx.IDs(); got != nil {
		t.Fatalf("unmatchedIdx IDs = %v, want nil", got)
	}

	nodeVec, ok := node.GetProperty("vec")
	if !ok {
		t.Fatal("node vec missing")
	}
	nodeVec.([]float32)[0] = 99
	got, err := vecIdx.SearchNearest([]float32{1, 0}, 1, nil)
	if err != nil {
		t.Fatalf("SearchNearest after caller mutation: %v", err)
	}
	if len(got) != 1 || got[0] != id {
		t.Fatalf("SearchNearest after caller mutation = %v, want [%d]", got, id)
	}
}

func TestPrepareNodeVectorIndexUpdatesRejectsInvalidBeforeApply(t *testing.T) {
	t.Parallel()

	id := snowflake.ID(102)
	node := types.NewNode(types.NodeID(id), 1, nil)
	if err := node.SetProperty("vec", []float32{1, 0, 0}); err != nil {
		t.Fatalf("SetProperty vec: %v", err)
	}
	good := &VectorIndex{Dims: 3, Metric: storepkg.DistanceCosine}
	bad := &VectorIndex{Dims: 2, Metric: storepkg.DistanceCosine}
	idxs := map[VectorIndexKey]*VectorIndex{
		{LabelToken: 1, PropertyKey: "vec"}:  good,
		{LabelToken: 1, PropertyKey: "vec2"}: bad,
	}
	if err := node.SetProperty("vec2", []float32{1, 0, 0}); err != nil {
		t.Fatalf("SetProperty vec2: %v", err)
	}

	updates, err := PrepareNodeVectorIndexUpdates(idxs, node, id)
	if !errors.Is(err, ErrDimensionMismatch) {
		t.Fatalf("PrepareNodeVectorIndexUpdates = (%v, %v), want ErrDimensionMismatch", updates, err)
	}
	if err := AddPreparedNodeToVectorIndexes(updates, id); err != nil {
		t.Fatalf("AddPreparedNodeToVectorIndexes on rejected updates: %v", err)
	}
	if got := good.IDs(); got != nil {
		t.Fatalf("good index mutated after rejected prepare: %v", got)
	}
	if got := bad.IDs(); got != nil {
		t.Fatalf("bad index mutated after rejected prepare: %v", got)
	}
}

func TestPrepareNodeVectorIndexUpdatesRejectsNonFiniteVector(t *testing.T) {
	t.Parallel()

	id := snowflake.ID(103)
	node := types.NewNode(types.NodeID(id), 1, nil)
	if err := node.SetProperty("vec", []float32{float32(math.NaN()), 0}); err != nil {
		t.Fatalf("SetProperty vec: %v", err)
	}
	idx := &VectorIndex{Dims: 2, Metric: storepkg.DistanceEuclidean}
	updates, err := PrepareNodeVectorIndexUpdates(map[VectorIndexKey]*VectorIndex{
		{LabelToken: 1, PropertyKey: "vec"}: idx,
	}, node, id)
	if !errors.Is(err, ErrInvalidVectorValue) || updates != nil {
		t.Fatalf("PrepareNodeVectorIndexUpdates = (%v, %v), want nil, ErrInvalidVectorValue", updates, err)
	}
	if got := idx.IDs(); got != nil {
		t.Fatalf("index mutated after rejected prepare: %v", got)
	}
}

func TestVectorIndexSearchNearestHugeKDoesNotPreallocateK(t *testing.T) {
	t.Parallel()

	vi := &VectorIndex{Dims: 2, Metric: storepkg.DistanceEuclidean}
	if err := vi.Add(snowflake.ID(1), []float32{1, 0}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	got, err := vi.SearchNearest([]float32{0, 0}, math.MaxInt, nil)
	if err != nil {
		t.Fatalf("SearchNearest huge k: %v", err)
	}
	if len(got) != 1 || got[0] != snowflake.ID(1) {
		t.Fatalf("SearchNearest huge k = %v, want [1]", got)
	}
}

func TestVectorIndexSearchNearestTieBreaksByID(t *testing.T) {
	t.Parallel()

	vi := &VectorIndex{Dims: 2, Metric: storepkg.DistanceEuclidean}
	for _, id := range []snowflake.ID{30, 10, 20} {
		if err := vi.Add(id, []float32{1, 0}); err != nil {
			t.Fatalf("Add %d: %v", id, err)
		}
	}

	got, err := vi.SearchNearest([]float32{0, 0}, 2, nil)
	if err != nil {
		t.Fatalf("SearchNearest: %v", err)
	}
	want := []snowflake.ID{10, 20}
	if len(got) != len(want) {
		t.Fatalf("SearchNearest len = %d, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("SearchNearest tie order = %v, want %v", got, want)
		}
	}
}

func BenchmarkVectorIndexReplaceExisting(b *testing.B) {
	const size = 8192
	vi := &VectorIndex{Dims: 3, Metric: storepkg.DistanceEuclidean}
	ids := make([]snowflake.ID, size)
	for i := range ids {
		id := snowflake.ID(i + 1)
		ids[i] = id
		if err := vi.Add(id, []float32{float32(i), 1, 2}); err != nil {
			b.Fatalf("setup Add: %v", err)
		}
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := vi.Add(ids[i%len(ids)], []float32{float32(i), 3, 4}); err != nil {
			b.Fatalf("Add: %v", err)
		}
	}
}

func BenchmarkVectorIndexRemoveAndReadd(b *testing.B) {
	const size = 8192
	vi := &VectorIndex{Dims: 3, Metric: storepkg.DistanceEuclidean}
	ids := make([]snowflake.ID, size)
	for i := range ids {
		id := snowflake.ID(i + 1)
		ids[i] = id
		if err := vi.Add(id, []float32{float32(i), 1, 2}); err != nil {
			b.Fatalf("setup Add: %v", err)
		}
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		id := ids[i%len(ids)]
		vi.Remove(id)
		if err := vi.Add(id, []float32{float32(i), 3, 4}); err != nil {
			b.Fatalf("Add: %v", err)
		}
	}
}
