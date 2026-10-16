// Package admin mirrors the v4 import path pkg/graph/admin. DecomposeNodeID,
// IDSlotLease, foreign relationships and tier admin are declined: v5 IDs carry no
// clock or slot.
package admin

import (
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/types"
)

// ErrNotSupportedInV5 is the class of every declined door of this package.
var ErrNotSupportedInV5 = store.ErrNotSupportedInV5

// DecomposeNodeID is declined.
func DecomposeNodeID(types.NodeID) (slot uint16, at types.Instant, seq uint64, err error) {
	return 0, 0, 0, fmt.Errorf("%w: admin.DecomposeNodeID", ErrNotSupportedInV5)
}
