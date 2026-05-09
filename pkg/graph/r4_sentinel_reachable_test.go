// Tests in this file pin the R4-F8 fix: every IO/bitemporal/tx-completion
// sentinel that docs/api.md tells external callers to errors.Is-check is
// reachable from the public pkg/graph package (or from pkg/graph/io for
// the IO sentinels) without dipping into internal/core.

package graph_test

import (
	"errors"
	"testing"

	graphpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
	tkgio "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/io"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
)

func TestSentinels_ReachableFromPublicPackages(t *testing.T) {
	t.Parallel()
	// IO sentinels via pkg/graph re-exports.
	for name, s := range map[string]error{
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
}
