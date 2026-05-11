// Tests in this file pin the R4-F8 fix: every IO/bitemporal/tx-completion
// sentinel that docs/api.md tells external callers to errors.Is-check is
// reachable from the public pkg/graph package (or from pkg/graph/io for
// the IO sentinels) without dipping into internal/core.

package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
	tkgio "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/io"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
	typespkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestSentinels_ReachableFromPublicPackages(t *testing.T) {
	t.Parallel()
	// IO sentinels via pkg/graph re-exports.
	for name, s := range map[string]error{
		"graph.ErrNilReader":            graphpkg.ErrNilReader,
		"graph.ErrNilWriter":            graphpkg.ErrNilWriter,
		"graph.ErrNilGraph":             graphpkg.ErrNilGraph,
		"graph.ErrNilStore":             graphpkg.ErrNilStore,
		"graph.ErrIncompatibleExport":   graphpkg.ErrIncompatibleExport,
		"graph.ErrIncompatibleRegistry": graphpkg.ErrIncompatibleRegistry,
		"graph.ErrCorruptExport":        graphpkg.ErrCorruptExport,
		"graph.ErrImportSizeLimit":      graphpkg.ErrImportSizeLimit,
		"graph.ErrNoVersionAsOf":        graphpkg.ErrNoVersionAsOf,
		"graph.ErrTxDone":               graphpkg.ErrTxDone,
	} {
		if s == nil {
			t.Errorf("%s is nil — sentinel must be a non-nil error value", name)
		}
	}
	// Same identity reachable from pkg/graph/io for IO sentinels.
	if !errors.Is(graphpkg.ErrNilReader, tkgio.ErrNilReader) {
		t.Errorf("graph.ErrNilReader must alias io.ErrNilReader")
	}
	if !errors.Is(graphpkg.ErrNilWriter, tkgio.ErrNilWriter) {
		t.Errorf("graph.ErrNilWriter must alias io.ErrNilWriter")
	}
	if !errors.Is(graphpkg.ErrIncompatibleExport, tkgio.ErrIncompatibleExport) {
		t.Errorf("graph.ErrIncompatibleExport must alias io.ErrIncompatibleExport (errors.Is should match)")
	}
	if !errors.Is(graphpkg.ErrIncompatibleRegistry, tkgio.ErrIncompatibleRegistry) {
		t.Errorf("graph.ErrIncompatibleRegistry must alias io.ErrIncompatibleRegistry")
	}
	if !errors.Is(graphpkg.ErrCorruptExport, tkgio.ErrCorruptExport) {
		t.Errorf("graph.ErrCorruptExport must alias io.ErrCorruptExport")
	}
	if !errors.Is(graphpkg.ErrImportSizeLimit, tkgio.ErrImportSizeLimit) {
		t.Errorf("graph.ErrImportSizeLimit must alias io.ErrImportSizeLimit")
	}
	// graph.ErrTxDone must be the SAME sentinel as store.ErrTxDone so
	// callers that already import store can keep using the qualifier.
	if !errors.Is(graphpkg.ErrTxDone, storepkg.ErrTxDone) {
		t.Errorf("graph.ErrTxDone must alias store.ErrTxDone")
	}
	if !errors.Is(graphpkg.ErrNilNode, typespkg.ErrNilNode) {
		t.Errorf("graph.ErrNilNode must alias types.ErrNilNode")
	}
	if !errors.Is(graphpkg.ErrNilRelationship, typespkg.ErrNilRelationship) {
		t.Errorf("graph.ErrNilRelationship must alias types.ErrNilRelationship")
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
