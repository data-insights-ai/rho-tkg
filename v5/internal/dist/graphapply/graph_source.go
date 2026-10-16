package graphapply

import (
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// capturedGraphSource is created only by a checked native Host capture or a
// matching completed reply on a registered authenticated source endpoint.
// Its data interface grants no control, readiness or write authority.
type capturedGraphSource struct {
	observation allocationObservation
	root        raftlog.ApplicationRoot
	floor       uint64
	data        graphstore.ApplicationReadSource
}
