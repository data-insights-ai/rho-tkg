package store

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// UntimedValidStartMetaKey is the MetaKV key persisting the untimed
// valid-start mode a store's rows are written under (graph
// Config.UntimedValidStart, backlog 73): one byte. The graph owns the key;
// a store reads it once at open to fix its valid axis (ValidAxisOfModeByte).
const UntimedValidStartMetaKey = "untimed_valid_start"

// UntimedWholeAxisModeByte is the persisted mode byte of an UntimedWholeAxis
// store (round 11), the only mode whose rows are on types.LiteralValidAxis.
// The bytes 0 (UntimedFromTxFrom), 1 (UntimedUnbounded) and 2 (a conversion
// to UntimedUnbounded in progress) are legacy-axis modes.
const UntimedWholeAxisModeByte byte = 3

// ValidAxisOfModeByte is the valid axis of a store whose persisted mode key
// holds v (nil or empty: no key, the legacy axis). It is decided once, when
// the store opens; no row carries it.
func ValidAxisOfModeByte(v []byte) types.ValidAxis {
	if len(v) == 1 && v[0] == UntimedWholeAxisModeByte {
		return types.LiteralValidAxis
	}
	return types.LegacyValidAxis
}

// ValidAxisCapability is implemented by a store whose own reads interpret
// valid time (push-down filters, temporal index envelopes, column scans,
// the valid-time stamps it persists): it reads its rows on one
// types.ValidAxis, fixed for the store (round 11). A store that persists the
// mode key reads it at open; the graph sets the axis of a new store with
// SetValidAxis before it writes the first row, and on every mode change of an
// empty store (an import adopting its source's mode, a replica following its
// primary). Memory, badger, tiered and sharded implement it. A store without
// it reads every row on the legacy axis; the graph refuses to open an
// UntimedWholeAxis graph on it.
type ValidAxisCapability interface {
	ValidAxis() types.ValidAxis
	SetValidAxis(a types.ValidAxis)
}
