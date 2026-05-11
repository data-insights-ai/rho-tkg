package memory

import (
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
)

// Compile-time assertions: memory.Store satisfies every capability interface
// defined in the store contract. If any capability cluster is later renamed
// or split, the affected line fails to compile here, surfacing the
// adaptation cost at the implementation boundary instead of at call sites.
var (
	_ storecontract.Store                           = (*Store)(nil)
	_ storecontract.Lifecycle                       = (*Store)(nil)
	_ storecontract.NodeCRUDCapability              = (*Store)(nil)
	_ storecontract.RelationshipCRUDCapability      = (*Store)(nil)
	_ storecontract.AdjacencyCapability             = (*Store)(nil)
	_ storecontract.BulkReadCapability              = (*Store)(nil)
	_ storecontract.BatchCapability                 = (*Store)(nil)
	_ storecontract.HistoryCapability               = (*Store)(nil)
	_ storecontract.StatsCapability                 = (*Store)(nil)
	_ storecontract.IterationCapability             = (*Store)(nil)
	_ storecontract.NodeIntegrityHashCapability     = (*Store)(nil)
	_ storecontract.EndpointIntegrityHashCapability = (*Store)(nil)
	_ storecontract.PropertyIndexCapability         = (*Store)(nil)
	_ storecontract.TemporalIndexCapability         = (*Store)(nil)
	_ storecontract.VectorIndexCapability           = (*Store)(nil)
	_ storecontract.FilteredVectorSearchCapability  = (*Store)(nil)
	_ storecontract.HighFrequencyIndexCapability    = (*Store)(nil)
)
