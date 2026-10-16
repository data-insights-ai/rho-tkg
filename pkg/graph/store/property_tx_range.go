package store

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// NodePropertyTxMemberRangeCapability is OPTIONAL (round 8 request 2): the
// members NodePropertyTxMembershipCapability enumerates, for every value of
// (label, key) whose numeric sort key lies in [lo, hi] (inclusive; a float32
// by its shortest decimal rendering) and for every NaN value, each member once
// with the lowest bound among those values. Same contract otherwise
// (append-only sound superset of the entities whose rows ever carried such a
// value, firstTxFrom 0 = unknown, store.ErrIndexNotFound before fn when no
// index is declared or it is being created); fn runs outside store locks.
// The store keeps a numeric view of the sidecar's values from the first such
// call on (lazy: no cost for a sidecar no range read asked for). memory and
// badger implement it.
type NodePropertyTxMemberRangeCapability interface {
	ForEachNodePropertyTxMemberInRange(labelToken uint16, propertyKey string, lo, hi float64, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error
}

// RelPropertyTxMemberRangeCapability is the relationship mirror of
// NodePropertyTxMemberRangeCapability, keyed (type, key).
type RelPropertyTxMemberRangeCapability interface {
	ForEachRelPropertyTxMemberInRange(relTypeToken uint16, propertyKey string, lo, hi float64, fn func(id types.RelID, firstTxFrom types.Instant) bool) error
}

// NodePropertyTxMemberAnyCapability is OPTIONAL (sigma-tkgd round 9): the
// members NodePropertyTxMembershipCapability enumerates for every value of
// (label, key), each once with the merged lower bound on its first
// transaction time over the values (MergeFirstTx's rule), in no order;
// ErrIndexNotFound without a declared index (or while it is built).
type NodePropertyTxMemberAnyCapability interface {
	ForEachNodePropertyTxMemberAny(labelToken uint16, propertyKey string, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error
}

// RelPropertyTxMemberAnyCapability is the relationship mirror of
// NodePropertyTxMemberAnyCapability, keyed (type, key).
type RelPropertyTxMemberAnyCapability interface {
	ForEachRelPropertyTxMemberAny(relTypeToken uint16, propertyKey string, fn func(id types.RelID, firstTxFrom types.Instant) bool) error
}
