package graphstore

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Range import computes fresh optional point records after exact native undo.
// A retained native tombstone means that optional record is absent at this
// coordinate. Keep its owned backing so the shared reader charges physical
// source work and output before interpreting absence. Required index/axis/member
// consumers still reject a missing record; this adapter grants no readiness,
// namespace, installation, recovery, or handoff authority.
//
// Catalog.view retains the original native view for guards/initialization.
// Only Catalog.data uses this adapter, exclusively inside the range importer.
// Root, binding, limits, Scan, and lifetime remain the native borrowed view's.
type rangeMoveMaintenanceSource struct {
	*raftlog.ApplicationView
}

func (s rangeMoveMaintenanceSource) Get(ctx context.Context, key []byte, maximum int) (raftlog.KV, bool, error) {
	row, found, err := s.ApplicationView.Get(ctx, key, maximum)
	if err == nil && found && row.Deleted {
		found = false
	}
	return row, found, err
}

var _ ApplicationReadSource = rangeMoveMaintenanceSource{}
