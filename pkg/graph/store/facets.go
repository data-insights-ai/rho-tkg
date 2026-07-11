package store

// Capability facets — thematic groupings of the optional capability interfaces.
//
// This file is PURELY ADDITIVE (ADR-0003 STAGE 1). It does not rename, move, or
// remove any existing interface: each facet below is a plain Go composition
// (interface embedding) of interfaces that already exist in capabilities.go,
// changefeed.go, and property_stats.go. Because Go interfaces are structural, a
// store that already implements every member of a facet automatically satisfies
// the facet type too — there is nothing for any existing implementer (in-tree or
// external) to change, and no source-compatibility risk.
//
// The mandatory facet is the pre-existing MandatoryStore (capabilities.go): the
// CRUD / adjacency / bulk-read / batch / history / stats / iteration core every
// backend must satisfy. Store composes it (plus the four Store-embedded index
// capabilities). The facets here group the OPTIONAL capabilities that
// internal/core type-asserts for at runtime.
//
// A composed optional facet is deliberately NOT asserted against any in-tree
// backend with a static `var _ Facet = (*X.Store)(nil)`: post-parity almost
// every optional capability is universally implemented, but a few are declined
// somewhere (tiered declines TransactionTimeQuery/HistoryRollbackTrim; the two
// depth-iteration capabilities are tiered-only), so no single backend satisfies
// a whole acceleration facet. The facets exist as documentation-grade groupings
// and as the organizing shape for CapabilityReport (below), which probes each
// member individually and keeps full per-capability fidelity. Callers that need
// a decision must still assert the individual narrow capability, exactly as
// today — the facets add a name for the group, not a coarser assertion.

// IntegrityAccelerationFacet groups the fast-path integrity-hash reads that let
// relationship create/update capture endpoint hashes without a full node deep
// copy.
type IntegrityAccelerationFacet interface {
	NodeIntegrityHashCapability
	EndpointIntegrityHashCapability
}

// HistoryAccelerationFacet groups the optional capabilities that bypass the
// mandatory History/Iteration fallback with a cheaper native read on the
// bitemporal / version-history / maintenance paths.
type HistoryAccelerationFacet interface {
	TransactionTimeQueryCapability
	HistoryRollbackTrimCapability
	HistoryVersionPageCapability
	HistoryCompactionCapability
	DegreeCapability
	DepthHistoryIterationCapability
	DeletedIterationCapability
	DepthDeletedIterationCapability
}

// IndexAccelerationFacet groups the secondary-index-backed query and planner
// paths (property / temporal / vector / high-frequency indexes plus the
// property-statistics surfaces).
type IndexAccelerationFacet interface {
	PropertyIndexCapability
	TemporalIndexCapability
	VectorIndexCapability
	VectorIndexOptionsCapability
	FilteredVectorSearchCapability
	HighFrequencyIndexCapability
	NodePropertyKeyStatsCapability
	NodePropertyStatsCapability
}

// ChangeLogFacet groups the op-log / replication change-feed capabilities.
type ChangeLogFacet interface {
	ChangeFeedCapability
	ChangeLogStatusCapability
	TxChangeLogScope
}

// MetadataFacet groups the durable arbitrary-KV metadata surface used by as-of
// tags, unique constraints, the graph epoch, the bitemporal migration marker,
// the replication watermark/lease, and the compaction stub. Its members are the
// single-member MetaKVCapability plus the atomic HistoryCompactionCapability
// (compaction trims history AND stamps its metadata stub in one write).
type MetadataFacet interface {
	MetaKVCapability
	HistoryCompactionCapability
}

// CapabilityReport is a full-fidelity, per-capability snapshot of which optional
// capabilities a store exposes. It is grouped into the five facets above for
// readability while keeping one named boolean per underlying capability — a
// coarse per-facet bool would silently over-claim when a backend implements some
// but not all members of a facet.
//
// It is produced by CapabilitiesOf for DIAGNOSTIC / introspection use (tests,
// tooling, a future admin surface). It is NOT the decision mechanism the graph
// layer uses at runtime: internal/core keeps its own cached capability handles,
// which additionally apply the wrapper-visibility guard that CapabilitiesOf
// deliberately does not (see the doc on CapabilitiesOf).
type CapabilityReport struct {
	IntegrityAcceleration struct {
		NodeHash     bool
		EndpointHash bool
	}
	HistoryAcceleration struct {
		TxTimeQuery      bool
		RollbackTrim     bool
		VersionPaging    bool
		Compaction       bool
		Degree           bool
		DepthHistoryIter bool
		DeletedIter      bool
		DepthDeletedIter bool
	}
	IndexAcceleration struct {
		PropertyIndex        bool
		TemporalIndex        bool
		VectorIndex          bool
		VectorIndexOptions   bool
		FilteredVectorSearch bool
		HighFrequencyIndex   bool
		PropertyKeyStats     bool
		PropertyStats        bool
	}
	ChangeLog struct {
		Feed        bool
		StatusQuery bool
		TxScope     bool
	}
	MetaKV bool
}

// CapabilitiesOf reports which optional capabilities a store exposes, by probing
// each optional capability interface once. It is a pure function with no side
// effects, intended for diagnostics, tests, and tooling that want a single
// consolidated snapshot instead of scattering ad-hoc `_, ok := s.(store.X)`
// checks.
//
// The parameter is MandatoryStore — the narrowest type the graph layer holds
// (Core.store) and the same surface these ad-hoc asserts run against — so the
// four index capabilities that Store embeds (PropertyIndex / TemporalIndex /
// VectorIndex / HighFrequencyIndex) are reported as the genuine optionals they
// are for a MandatoryStore-only backend. A full Store satisfies MandatoryStore,
// so any Store may be passed too.
//
// IMPORTANT — this is a STRUCTURAL probe only. It reports whether the concrete
// store's method set satisfies each capability; it does NOT reproduce the
// wrapper-visibility guard that internal/core applies to its own cached handles
// (the guard that suppresses an acceleration path a test/fault-injection/policy
// wrapper has overridden a method of). A wrapper embedding an in-tree backend
// will therefore report the embedded backend's accelerators as present here even
// where the graph layer would route around them. Use this for observation, not
// to drive a routing decision that must honour wrapper overrides — for that, the
// graph layer's cached handles remain authoritative.
func CapabilitiesOf(s MandatoryStore) CapabilityReport {
	var r CapabilityReport
	if s == nil {
		return r
	}

	_, r.IntegrityAcceleration.NodeHash = s.(NodeIntegrityHashCapability)
	_, r.IntegrityAcceleration.EndpointHash = s.(EndpointIntegrityHashCapability)

	_, r.HistoryAcceleration.TxTimeQuery = s.(TransactionTimeQueryCapability)
	_, r.HistoryAcceleration.RollbackTrim = s.(HistoryRollbackTrimCapability)
	_, r.HistoryAcceleration.VersionPaging = s.(HistoryVersionPageCapability)
	_, r.HistoryAcceleration.Compaction = s.(HistoryCompactionCapability)
	_, r.HistoryAcceleration.Degree = s.(DegreeCapability)
	_, r.HistoryAcceleration.DepthHistoryIter = s.(DepthHistoryIterationCapability)
	_, r.HistoryAcceleration.DeletedIter = s.(DeletedIterationCapability)
	_, r.HistoryAcceleration.DepthDeletedIter = s.(DepthDeletedIterationCapability)

	_, r.IndexAcceleration.PropertyIndex = s.(PropertyIndexCapability)
	_, r.IndexAcceleration.TemporalIndex = s.(TemporalIndexCapability)
	_, r.IndexAcceleration.VectorIndex = s.(VectorIndexCapability)
	_, r.IndexAcceleration.VectorIndexOptions = s.(VectorIndexOptionsCapability)
	_, r.IndexAcceleration.FilteredVectorSearch = s.(FilteredVectorSearchCapability)
	_, r.IndexAcceleration.HighFrequencyIndex = s.(HighFrequencyIndexCapability)
	_, r.IndexAcceleration.PropertyKeyStats = s.(NodePropertyKeyStatsCapability)
	_, r.IndexAcceleration.PropertyStats = s.(NodePropertyStatsCapability)

	_, r.ChangeLog.Feed = s.(ChangeFeedCapability)
	_, r.ChangeLog.StatusQuery = s.(ChangeLogStatusCapability)
	_, r.ChangeLog.TxScope = s.(TxChangeLogScope)

	_, r.MetaKV = s.(MetaKVCapability)

	return r
}
