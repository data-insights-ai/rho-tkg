package tiered

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/generatedcreate"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
)

// Compile-time assertions: tiered.Store satisfies every capability interface
// defined in the store contract. See pkg/graph/store/memory/capabilities.go
// for the rationale.
var (
	_ storecontract.Store                                = (*Store)(nil)
	_ storecontract.Lifecycle                            = (*Store)(nil)
	_ storecontract.NodeCRUDCapability                   = (*Store)(nil)
	_ storecontract.NodeIntegrityHashCapability          = (*Store)(nil)
	_ storecontract.EndpointIntegrityHashCapability      = (*Store)(nil)
	_ storecontract.RelationshipCRUDCapability           = (*Store)(nil)
	_ generatedcreate.Capability                         = (*Store)(nil)
	_ generatedcreate.RelationshipEndpointHashCapability = (*Store)(nil)
	_ storecontract.AdjacencyCapability                  = (*Store)(nil)
	_ storecontract.BulkReadCapability                   = (*Store)(nil)
	_ storecontract.BatchCapability                      = (*Store)(nil)
	_ storecontract.HistoryCapability                    = (*Store)(nil)
	_ storecontract.StatsCapability                      = (*Store)(nil)
	_ storecontract.IterationCapability                  = (*Store)(nil)
	_ storecontract.DepthHistoryIterationCapability      = (*Store)(nil)
	_ storecontract.PropertyIndexCapability              = (*Store)(nil)
	_ storecontract.TemporalIndexCapability              = (*Store)(nil)
	_ storecontract.VectorIndexCapability                = (*Store)(nil)
	_ storecontract.FilteredVectorSearchCapability       = (*Store)(nil)
	_ storecontract.HighFrequencyIndexCapability         = (*Store)(nil)
)
