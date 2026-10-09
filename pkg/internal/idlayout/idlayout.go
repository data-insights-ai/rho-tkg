// Package idlayout is the single home of the snowflake epoch and bit layout
// shared by every ID generator and decoder in this module: pkg/types (the
// public NodeID/RelID.MintInstant doors) and pkg/graph/internal/** (the
// resolver, the stores, the lock sharding). It lives in pkg/internal so both
// sides can import it without growing the public API and without a cycle.
//
// All graphs in a process share this epoch and layout; neither depends on a
// graph's Config.
package idlayout

import (
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// Epoch is the custom epoch for all snowflake ID generation (2026-01-01 UTC).
var Epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// Layout is the snowflake.Layout matching the graph's generators:
// 1 zero bit | 48 bits microseconds since Epoch | 5 bits node | 10 bits step.
var Layout = func() snowflake.Layout {
	l, err := snowflake.NewLayout(
		snowflake.WithEpoch(Epoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		panic("idlayout: Layout: " + err.Error())
	}
	return l
}()

// MintInstantMillis returns the IMMUTABLE mint instant of a snowflake ID in
// Unix MILLISECONDS (the unit of types.Instant), floored from the layout's
// microsecond time field: the derived valid-from of a row that carries no
// explicit one. Node and relationship IDs share the time bits (nodes use the
// even node field, relationships the odd one), so one function serves both.
//
// A non-positive ID is never minted by a generator (the sign bit is always
// zero), so it returns 0, the temporal "unset" sentinel, instead of a
// pre-epoch or epoch instant. It never panics and does not allocate.
func MintInstantMillis(id snowflake.ID) int64 {
	if id <= 0 {
		return 0
	}
	return Layout.CreatedAt(id).UnixMilli()
}
