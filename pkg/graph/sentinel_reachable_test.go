// Tests in this file pin that every IO/bitemporal/tx-completion
// sentinel that docs/api.md tells external callers to errors.Is-check is
// reachable from the public pkg/graph package (or from pkg/graph/io for
// the IO sentinels) without dipping into internal/core.

package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	typespkg "github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestSentinels_ReachableFromPublicPackages: each re-exported sentinel is a
// non-nil error with the SAME identity as the package that defines it, so a
// caller can errors.Is against either qualifier. The sentinels pkg/graph defines
// itself (no second home) are checked non-nil only.
func TestSentinels_ReachableFromPublicPackages(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		public, home error // home == nil: defined in pkg/graph itself
	}{
		{"ErrNilReader", graphpkg.ErrNilReader, tkgio.ErrNilReader},
		{"ErrNilWriter", graphpkg.ErrNilWriter, tkgio.ErrNilWriter},
		{"ErrIncompatibleExport", graphpkg.ErrIncompatibleExport, tkgio.ErrIncompatibleExport},
		{"ErrIncompatibleRegistry", graphpkg.ErrIncompatibleRegistry, tkgio.ErrIncompatibleRegistry},
		{"ErrCorruptExport", graphpkg.ErrCorruptExport, tkgio.ErrCorruptExport},
		{"ErrImportSizeLimit", graphpkg.ErrImportSizeLimit, tkgio.ErrImportSizeLimit},
		// graph.ErrTxDone is store.ErrTxDone, so callers that already import
		// store can keep using that qualifier.
		{"ErrTxDone", graphpkg.ErrTxDone, storepkg.ErrTxDone},
		{"ErrNilNode", graphpkg.ErrNilNode, typespkg.ErrNilNode},
		{"ErrNilRelationship", graphpkg.ErrNilRelationship, typespkg.ErrNilRelationship},
		{"ErrNilGraph", graphpkg.ErrNilGraph, nil},
		{"ErrNilStore", graphpkg.ErrNilStore, nil},
		{"ErrNoVersionAsOf", graphpkg.ErrNoVersionAsOf, nil},
		{"ErrConflictingTemporalOpts", graphpkg.ErrConflictingTemporalOpts, nil},
	} {
		if tc.public == nil {
			t.Errorf("graph.%s is nil; a sentinel must be a non-nil error value", tc.name)
			continue
		}
		if tc.home != nil && !errors.Is(tc.public, tc.home) {
			t.Errorf("graph.%s must alias its defining package's %s (errors.Is must match)", tc.name, tc.name)
		}
	}
}

func TestNew_TypedNilStoreReturnsPublicSentinel(t *testing.T) {
	t.Parallel()

	var typedNilStore *memory.Store
	g, err := graphpkg.New(graphpkg.Config{Store: typedNilStore})
	if !errors.Is(err, graphpkg.ErrNilStore) {
		t.Fatalf("graph.New(typed nil store) = (%v, %v), want ErrNilStore", g, err)
	}
	if g != nil {
		t.Fatalf("graph.New(typed nil store) returned graph %v", g)
	}
}

func TestGraphFacade_NilReceiversReturnErrNilGraph(t *testing.T) {
	t.Parallel()

	var nilGraph *graphpkg.Graph
	if err := nilGraph.Close(); !errors.Is(err, graphpkg.ErrNilGraph) {
		t.Fatalf("nilGraph.Close() = %v, want ErrNilGraph", err)
	}
	if b, err := graphpkg.NewBatchBuilder(nilGraph); !errors.Is(err, graphpkg.ErrNilGraph) || b != nil {
		t.Fatalf("NewBatchBuilder(nil graph) = (%v, %v), want (nil, ErrNilGraph)", b, err)
	}

	var zeroGraph graphpkg.Graph
	if err := zeroGraph.Close(); !errors.Is(err, graphpkg.ErrNilGraph) {
		t.Fatalf("zeroGraph.Close() = %v, want ErrNilGraph", err)
	}
	if b, err := graphpkg.NewBatchBuilder(&zeroGraph); !errors.Is(err, graphpkg.ErrNilGraph) || b != nil {
		t.Fatalf("NewBatchBuilder(zero graph) = (%v, %v), want (nil, ErrNilGraph)", b, err)
	}
}

func TestTxAndBatchAPIs_ZeroValuesReturnErrNilGraph(t *testing.T) {
	t.Parallel()

	var tx graphpkg.TxAPI
	if got, err := tx.Begin(); !errors.Is(err, graphpkg.ErrNilGraph) || got != nil {
		t.Fatalf("zero TxAPI.Begin() = (%v, %v), want (nil, ErrNilGraph)", got, err)
	}
	if err := tx.Run(func(*graphpkg.GraphTx) error { return nil }); !errors.Is(err, graphpkg.ErrNilGraph) {
		t.Fatalf("zero TxAPI.Run() = %v, want ErrNilGraph", err)
	}
	if err := tx.RunContext(context.Background(), func(*graphpkg.GraphTx) error { return nil }); !errors.Is(err, graphpkg.ErrNilGraph) {
		t.Fatalf("zero TxAPI.RunContext() = %v, want ErrNilGraph", err)
	}

	var batch graphpkg.BatchAPI
	if got, err := batch.New(); !errors.Is(err, graphpkg.ErrNilGraph) || got != nil {
		t.Fatalf("zero BatchAPI.New() = (%v, %v), want (nil, ErrNilGraph)", got, err)
	}
}
