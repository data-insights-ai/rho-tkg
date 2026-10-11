package store

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// OrderedHistoricalCandidatesCapability streams complete current/history
// candidates in ascending ID order, after an exclusive cursor. Empty property
// selects label/type membership. A property requires its declared index.
// FirstTx is a conservative lower bound (zero means unknown); callers still
// resolve the exact historical row and recheck every predicate before counting
// a match. ErrIndexNotFound declines an unavailable/incomplete accelerator.
type OrderedHistoricalCandidatesCapability interface {
	ForEachNodeHistoricalCandidate(token uint16, property, value string, after types.NodeID, fn func(types.NodeID, types.Instant) bool) error
	ForEachRelHistoricalCandidate(token uint16, property, value string, after types.RelID, fn func(types.RelID, types.Instant) bool) error
}
