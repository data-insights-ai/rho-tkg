package store

// PropertyStats holds NDV / min / max / count planner statistics for one
// (label, property key) pair — the richer sibling of
// NodePropertyKeyStatsCapability's presence-only count.
//
// This type lives in pkg/graph/store rather than pkg/types: pkg/types must
// stay a dependency-free data-model package (Node/Relationship never
// reference store-layer concepts), while PropertyStats is a pure
// store-capability return shape returned BY store.NodePropertyStatsCapability
// and consumed by pkg/graph/stats (which already imports this package for
// QueryOpts — see RangeCardinality). Declaring it here avoids a cycle in
// either direction: pkg/graph/store already imports pkg/types, and nothing
// pkg/types-side needs to import it back.
type PropertyStats struct {
	// NDV is an ESTIMATED count of distinct values, from a HyperLogLog
	// sketch (see pkg/graph/internal/index.HyperLogLog). It is
	// non-decreasing across deletes: HyperLogLog has no removal operation,
	// so a deleted value's contribution to NDV persists until the sketch is
	// rebuilt from scratch (index load / restart). See
	// docs/query-planners.md "Deletion semantics".
	NDV int64
	// Min is the EXACT minimum value currently held for the pair, or nil if
	// no scalar-ORDERED value has been observed (see the Min/Max value
	// families note below).
	Min any
	// Max is the EXACT maximum value currently held for the pair, or nil
	// under the same condition as Min.
	Max any
	// Count is the same presence count NodeCountByLabelAndPropertyKey
	// returns — the number of current nodes carrying the label with an
	// indexable scalar value for the property key.
	Count int64
}

// NodePropertyStatsCapability is an OPTIONAL statistics surface for stores
// that maintain, per (label, property key) pair, a HyperLogLog NDV sketch
// plus an exact min/max over the pair's currently-live values. It augments
// NodePropertyKeyStatsCapability's presence-only count with a richer
// PropertyStats — a query planner uses it for selectivity/range estimates
// once NodeCountByLabelAndPropertyKey has already confirmed the label can
// satisfy a scalar predicate on the key at all.
//
// Min/Max value families: only numeric (any allowlisted int/uint/float type)
// and string values participate in the exact min/max — see
// docs/query-planners.md "Min/Max value families" for the full rationale and
// the mixed-family tie-break rule. A key populated only with unordered
// value types (bool, TemporalValue, …) reports Count>0, NDV>0, Min/Max nil.
//
// memory, badger, AND tiered all implement this capability (tiered folds
// NDV via a register-max HyperLogLog merge across shards — see
// docs/adr/0005-tiered-parity.md §3.1 and docs/query-planners.md "Tiered NDV
// fold"). A backend satisfying only MandatoryStore returns
// ErrCapabilityNotSupported; check with errors.Is, never a string compare.
type NodePropertyStatsCapability interface {
	NodePropertyStats(labelToken uint16, propertyKey string) (PropertyStats, error)
}

// RelPropertyStatsCapability is the RELATIONSHIP mirror of
// NodePropertyStatsCapability (BACKLOG 21a) — the same NDV/min/max/count
// PropertyStats shape, keyed by (relationship type token, property key)
// instead of (label token, property key). It gives a query planner a
// selectivity estimate for a relationship-property predicate, complementing
// the existing rel-side ordering-soundness primitives RelRangeCardinality
// and RelPropertyTypeClassCounts.
//
// memory AND badger implement this capability. tiered does NOT — mirroring
// the precedent already set by its two rel-side sibling capabilities,
// RelRangeCardinality and RelPropertyTypeClassCounts, neither of which tiered
// implements either (rel property indexes are RAM-only per-shard with no
// cross-shard fold defined for them, unlike the node-side property/NDV
// indexes which are scoped to reference-label shards). sharded does not
// implement PropertyStats on the node side either, so it is not expected to
// implement this capability. A backend satisfying only MandatoryStore
// returns ErrCapabilityNotSupported; check with errors.Is, never a string
// compare.
type RelPropertyStatsCapability interface {
	RelPropertyStats(relTypeToken uint16, propertyKey string) (PropertyStats, error)
}
