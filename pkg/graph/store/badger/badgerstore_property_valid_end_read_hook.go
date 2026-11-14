package badger

import (
	"sync/atomic"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var propertyValidEndDecodeCounter atomic.Pointer[atomic.Int64]

func decodePropertyValidEndBound(v []byte) (types.Instant, bool) {
	if c := propertyValidEndDecodeCounter.Load(); c != nil {
		c.Add(1)
	}
	return storepkg.DecodeValidEndBound(v)
}

// CountPropertyValidEndDecodesForTest observes the actual value-decoder calls
// of property-index pruning. It is independent of ReadTally. Tests using it
// must not run alongside another pruning read.
func CountPropertyValidEndDecodesForTest() (decoded func() int64, stop func()) {
	c := new(atomic.Int64)
	previous := propertyValidEndDecodeCounter.Swap(c)
	return c.Load, func() { propertyValidEndDecodeCounter.Store(previous) }
}
