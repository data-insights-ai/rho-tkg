package index

import (
	"errors"
	"math"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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
	if err := ValidateNodeVectorIndexes(relevant, node, id); !errors.Is(err, ErrInvalidVectorIndexConfig) {
		t.Fatalf("ValidateNodeVectorIndexes nil relevant index = %v, want ErrInvalidVectorIndexConfig", err)
	}
	if err := AddNodeToVectorIndexes(relevant, node, id); !errors.Is(err, ErrInvalidVectorIndexConfig) {
		t.Fatalf("AddNodeToVectorIndexes nil relevant index = %v, want ErrInvalidVectorIndexConfig", err)
	}
	RemoveNodeFromVectorIndexes(relevant, node, id)
	PurgeNodeFromAllVectorIndexes(relevant, id)

	unrelated := map[VectorIndexKey]*VectorIndex{
		{LabelToken: 2, PropertyKey: "vec"}: nil,
	}
	if err := ValidateNodeVectorIndexes(unrelated, node, id); err != nil {
		t.Fatalf("ValidateNodeVectorIndexes nil unrelated index = %v, want nil", err)
	}
	if err := AddNodeToVectorIndexes(unrelated, node, id); err != nil {
		t.Fatalf("AddNodeToVectorIndexes nil unrelated index = %v, want nil", err)
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
