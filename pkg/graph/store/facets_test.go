package store

import (
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// mandatoryCoreStore declares ONLY the method sets of MandatoryStore's nine
// mandatory narrow interfaces — the minimum an external backend can implement.
// It implements NONE of the optional capabilities, including the four index
// capabilities that Store embeds. The methods are stubs: this type is never
// executed, only type-checked, so the facet refactor cannot silently add a
// method to any mandatory interface (that would break this compile).
type mandatoryCoreStore struct{}

// --- Lifecycle ---
func (mandatoryCoreStore) Clear() error { return nil }
func (mandatoryCoreStore) Close() error { return nil }

// --- NodeCRUDCapability ---
func (mandatoryCoreStore) PutNode(*types.Node) error                                    { return nil }
func (mandatoryCoreStore) GetNode(types.NodeID) (*types.Node, error)                    { return nil, nil }
func (mandatoryCoreStore) ReplaceNode(*types.Node) error                                { return nil }
func (mandatoryCoreStore) DeleteNode(types.NodeID) error                                { return nil }
func (mandatoryCoreStore) DeleteNodeCascade(types.NodeID) error                         { return nil }
func (mandatoryCoreStore) RemoveNodeLabelToken(types.NodeID, uint16, *types.Node) error { return nil }
func (mandatoryCoreStore) AddNodeLabelToken(types.NodeID, uint16, *types.Node) error    { return nil }

// --- RelationshipCRUDCapability ---
func (mandatoryCoreStore) PutRelationship(*types.Relationship) error                { return nil }
func (mandatoryCoreStore) GetRelationship(types.RelID) (*types.Relationship, error) { return nil, nil }
func (mandatoryCoreStore) ReplaceRelationship(*types.Relationship) error            { return nil }
func (mandatoryCoreStore) DeleteRelationship(types.RelID) error                     { return nil }

// --- AdjacencyCapability ---
func (mandatoryCoreStore) OutgoingRelationships(types.NodeID, uint16) ([]*types.Relationship, error) {
	return nil, nil
}
func (mandatoryCoreStore) IncomingRelationships(types.NodeID, uint16) ([]*types.Relationship, error) {
	return nil, nil
}
func (mandatoryCoreStore) OutgoingRelationshipsForNodes([]types.NodeID, uint16) (map[types.NodeID][]*types.Relationship, error) {
	return nil, nil
}
func (mandatoryCoreStore) IncomingRelationshipsForNodes([]types.NodeID, uint16) (map[types.NodeID][]*types.Relationship, error) {
	return nil, nil
}

// --- BulkReadCapability ---
func (mandatoryCoreStore) NodesByLabel(uint16, QueryOpts) ([]*types.Node, error) { return nil, nil }
func (mandatoryCoreStore) RelationshipsByType(uint16, QueryOpts) ([]*types.Relationship, error) {
	return nil, nil
}
func (mandatoryCoreStore) AllNodes(QueryOpts) ([]*types.Node, error) { return nil, nil }
func (mandatoryCoreStore) AllRelationships(QueryOpts) ([]*types.Relationship, error) {
	return nil, nil
}
func (mandatoryCoreStore) GetNodesByIDs([]types.NodeID) ([]*types.Node, error) { return nil, nil }
func (mandatoryCoreStore) GetRelationshipsByIDs([]types.RelID) ([]*types.Relationship, error) {
	return nil, nil
}

// --- BatchCapability ---
func (mandatoryCoreStore) PutNodesBatch([]*types.Node) error                 { return nil }
func (mandatoryCoreStore) PutRelationshipsBatch([]*types.Relationship) error { return nil }
func (mandatoryCoreStore) DeleteNodesBatch([]types.NodeID) error             { return nil }
func (mandatoryCoreStore) DeleteRelationshipsBatch([]types.RelID) error      { return nil }

// --- HistoryCapability ---
func (mandatoryCoreStore) ReplaceNodeWithHistory(*types.Node, uint32, *types.Node) error {
	return nil
}
func (mandatoryCoreStore) ReplaceRelWithHistory(*types.Relationship, uint32, *types.Relationship) error {
	return nil
}
func (mandatoryCoreStore) PutNodeVersion(types.NodeID, uint32, *types.Node) error   { return nil }
func (mandatoryCoreStore) GetNodeVersion(types.NodeID, uint32) (*types.Node, error) { return nil, nil }
func (mandatoryCoreStore) GetNodeHistory(types.NodeID) ([]*types.Node, error)       { return nil, nil }
func (mandatoryCoreStore) TruncateNodeHistory(types.NodeID, int) error              { return nil }
func (mandatoryCoreStore) PutRelVersion(types.RelID, uint32, *types.Relationship) error {
	return nil
}
func (mandatoryCoreStore) GetRelVersion(types.RelID, uint32) (*types.Relationship, error) {
	return nil, nil
}
func (mandatoryCoreStore) GetRelHistory(types.RelID) ([]*types.Relationship, error) {
	return nil, nil
}
func (mandatoryCoreStore) TruncateRelHistory(types.RelID, int) error { return nil }
func (mandatoryCoreStore) RemoveNodeLabelTokenWithHistory(types.NodeID, uint16, *types.Node, uint32, *types.Node) error {
	return nil
}
func (mandatoryCoreStore) AddNodeLabelTokenWithHistory(types.NodeID, uint16, *types.Node, uint32, *types.Node) error {
	return nil
}
func (mandatoryCoreStore) DeleteNodeWithHistory(types.NodeID, uint32, *types.Node, []RelTombstone) error {
	return nil
}
func (mandatoryCoreStore) DeleteRelWithHistory(types.RelID, uint32, *types.Relationship) error {
	return nil
}

// --- StatsCapability ---
func (mandatoryCoreStore) NodeCount() (int, error)              { return 0, nil }
func (mandatoryCoreStore) RelationshipCount() (int, error)      { return 0, nil }
func (mandatoryCoreStore) NodeCountByLabel(uint16) (int, error) { return 0, nil }
func (mandatoryCoreStore) RelCountByType(uint16) (int, error)   { return 0, nil }

// --- IterationCapability ---
func (mandatoryCoreStore) AllNodeIDs(QueryOpts) ([]types.NodeID, error) { return nil, nil }
func (mandatoryCoreStore) AllRelIDs(QueryOpts) ([]types.RelID, error)   { return nil, nil }
func (mandatoryCoreStore) AllNodeHistoryIDs() ([]types.NodeID, error)   { return nil, nil }
func (mandatoryCoreStore) AllRelHistoryIDs() ([]types.RelID, error)     { return nil, nil }
func (mandatoryCoreStore) AllNodeHistoryIDsFrom(types.NodeID, int) ([]types.NodeID, error) {
	return nil, nil
}
func (mandatoryCoreStore) AllRelHistoryIDsFrom(types.RelID, int) ([]types.RelID, error) {
	return nil, nil
}
func (mandatoryCoreStore) ForEachNodeID(func(types.NodeID) bool) error        { return nil }
func (mandatoryCoreStore) ForEachRelID(func(types.RelID) bool) error          { return nil }
func (mandatoryCoreStore) ForEachNodeHistoryID(func(types.NodeID) bool) error { return nil }
func (mandatoryCoreStore) ForEachRelHistoryID(func(types.RelID) bool) error   { return nil }

// A mandatory-only external backend satisfies MandatoryStore but NOT Store.
var _ MandatoryStore = mandatoryCoreStore{}

// oldStyleStore is an EXTERNAL-shaped FULL store that declares ONLY the method
// sets of the pre-facet narrow interfaces (mandatory core + the four
// Store-embedded index capabilities) — exactly what a third-party backend wrote
// against the interface names that existed before ADR-0003. It embeds no facet
// type. The compile-time assertions below prove the hard back-compat constraint:
// introducing the composed facets did not change any method set, so a store
// implementing only the old-style methods still satisfies Store automatically
// (Go structural typing).
type oldStyleStore struct{ mandatoryCoreStore }

// --- PropertyIndexCapability (embedded in Store) ---
func (oldStyleStore) CreatePropertyIndex(uint16, string) error { return nil }
func (oldStyleStore) DropPropertyIndex(uint16, string) error   { return nil }
func (oldStyleStore) NodesByLabelAndProperty(uint16, string, any, QueryOpts) ([]*types.Node, error) {
	return nil, nil
}

// --- TemporalIndexCapability (embedded in Store) ---
func (oldStyleStore) CreateTemporalIndex(uint16) error { return nil }
func (oldStyleStore) DropTemporalIndex(uint16) error   { return nil }

// --- VectorIndexCapability (embedded in Store) ---
func (oldStyleStore) CreateVectorIndex(uint16, string, int, DistanceMetric) error { return nil }
func (oldStyleStore) DropVectorIndex(uint16, string) error                        { return nil }
func (oldStyleStore) SearchNearestNodes(uint16, string, []float32, int, QueryOpts) ([]*types.Node, error) {
	return nil, nil
}

// --- HighFrequencyIndexCapability (embedded in Store) ---
func (oldStyleStore) CreateHighFrequencyIndex(uint16, time.Duration) error { return nil }
func (oldStyleStore) DropHighFrequencyIndex(uint16) error                  { return nil }

// Store back-compat: an old-style store still satisfies the full Store contract.
var (
	_ Store          = oldStyleStore{}
	_ MandatoryStore = oldStyleStore{}
)

// fullOptionalStore additionally implements every optional facet's members, to
// prove the composed facet types are satisfiable by structural typing alone —
// no facet embeds a method that does not already exist on some narrow interface.
type fullOptionalStore struct{ oldStyleStore }

func (fullOptionalStore) NodeIntegrityHash(types.NodeID) (string, error) { return "", nil }
func (fullOptionalStore) EndpointIntegrityHashes(types.NodeID, types.NodeID) (string, string, error) {
	return "", "", nil
}
func (fullOptionalStore) NodeAsOf(types.NodeID, types.Instant) (*types.Node, error) { return nil, nil }
func (fullOptionalStore) RelAsOf(types.RelID, types.Instant) (*types.Relationship, error) {
	return nil, nil
}
func (fullOptionalStore) NodesAsOf(types.Instant) ([]*types.Node, error)        { return nil, nil }
func (fullOptionalStore) RelsAsOf(types.Instant) ([]*types.Relationship, error) { return nil, nil }
func (fullOptionalStore) TrimNodeHistoryFrom(types.NodeID, uint32) error        { return nil }
func (fullOptionalStore) TrimRelHistoryFrom(types.RelID, uint32) error          { return nil }
func (fullOptionalStore) NodeHistoryVersionsFrom(types.NodeID, uint32, int) ([]*types.Node, error) {
	return nil, nil
}
func (fullOptionalStore) RelHistoryVersionsFrom(types.RelID, uint32, int) ([]*types.Relationship, error) {
	return nil, nil
}

// --- RelPropertyIndexCapability (K3b) ---
func (fullOptionalStore) CreateRelPropertyIndex(uint16, string) error { return nil }
func (fullOptionalStore) DropRelPropertyIndex(uint16, string) error   { return nil }
func (fullOptionalStore) RelationshipsByTypeAndProperty(uint16, string, any, QueryOpts) ([]*types.Relationship, error) {
	return nil, nil
}

func (fullOptionalStore) CompactNodeHistory(types.NodeID, int, []MetaWrite) error { return nil }
func (fullOptionalStore) CompactRelHistory(types.RelID, int, []MetaWrite) error   { return nil }
func (fullOptionalStore) OutgoingDegree(types.NodeID, uint16) (int, error)        { return 0, nil }
func (fullOptionalStore) IncomingDegree(types.NodeID, uint16) (int, error)        { return 0, nil }
func (fullOptionalStore) ForEachNodeHistoryIDByDepth(ShardDepth, func(types.NodeID) bool) error {
	return nil
}
func (fullOptionalStore) ForEachRelHistoryIDByDepth(ShardDepth, func(types.RelID) bool) error {
	return nil
}
func (fullOptionalStore) ForEachDeletedNodeID(func(types.NodeID) bool) error { return nil }
func (fullOptionalStore) ForEachDeletedRelID(func(types.RelID) bool) error   { return nil }
func (fullOptionalStore) ForEachDeletedNodeIDByDepth(ShardDepth, func(types.NodeID) bool) error {
	return nil
}
func (fullOptionalStore) ForEachDeletedRelIDByDepth(ShardDepth, func(types.RelID) bool) error {
	return nil
}
func (fullOptionalStore) CreateVectorIndexWithOptions(uint16, string, int, DistanceMetric, VectorIndexOptions) error {
	return nil
}
func (fullOptionalStore) SearchNearestFiltered(uint16, string, []float32, int, func(snowflake.ID) bool) ([]snowflake.ID, error) {
	return nil, nil
}
func (fullOptionalStore) NodeCountByLabelAndPropertyKey(uint16, string) (int, error) { return 0, nil }
func (fullOptionalStore) NodePropertyStats(uint16, string) (PropertyStats, error) {
	return PropertyStats{}, nil
}
func (fullOptionalStore) ChangeFeed(uint64, int) ([]ChangeRecord, error)      { return nil, nil }
func (fullOptionalStore) ForEachChange(uint64, func(ChangeRecord) bool) error { return nil }
func (fullOptionalStore) LastCommittedLSN() (uint64, error)                   { return 0, nil }
func (fullOptionalStore) ChangeLogEnabled() bool                              { return false }
func (fullOptionalStore) BeginLogScope() error                                { return nil }
func (fullOptionalStore) SetLogDivert(bool)                                   {}
func (fullOptionalStore) CommitLogScope() (uint64, error)                     { return 0, nil }
func (fullOptionalStore) DiscardLogScope() error                              { return nil }
func (fullOptionalStore) MetaGet(string) ([]byte, error)                      { return nil, nil }
func (fullOptionalStore) MetaSet(string, []byte) error                        { return nil }
func (fullOptionalStore) ExactErasureRelationshipClosure(ExactErasureClosureRequest) (ExactErasureClosure, error) {
	return ExactErasureClosure{}, nil
}
func (fullOptionalStore) ExactErase(ExactErasureRequest) (ExactErasureResult, error) {
	return ExactErasureResult{}, nil
}
func (fullOptionalStore) CreateCompositePropertyIndex(uint16, []string) error { return nil }
func (fullOptionalStore) DropCompositePropertyIndex(uint16, []string) error   { return nil }
func (fullOptionalStore) NodesByLabelAndProperties(uint16, map[string]any, QueryOpts) ([]*types.Node, error) {
	return nil, nil
}

// Compile-time proof that every composed facet is satisfiable by structural
// typing over the existing narrow method sets.
var (
	_ Store                      = fullOptionalStore{}
	_ IntegrityAccelerationFacet = fullOptionalStore{}
	_ HistoryAccelerationFacet   = fullOptionalStore{}
	_ IndexAccelerationFacet     = fullOptionalStore{}
	_ ChangeLogFacet             = fullOptionalStore{}
	_ MetadataFacet              = fullOptionalStore{}
	_ DestructiveAdminFacet      = fullOptionalStore{}
)

func TestCapabilitiesOfMandatoryOnlyReportsAllAbsent(t *testing.T) {
	t.Parallel()
	r := CapabilitiesOf(mandatoryCoreStore{})
	want := CapabilityReport{} // zero value: every optional bool false
	if r != want {
		t.Fatalf("CapabilitiesOf(mandatoryCore) = %+v, want all-absent zero value", r)
	}
}

func TestCapabilitiesOfFullOptionalReportsAllPresent(t *testing.T) {
	t.Parallel()
	r := CapabilitiesOf(fullOptionalStore{})
	present := []struct {
		name string
		got  bool
	}{
		{"IntegrityAcceleration.NodeHash", r.IntegrityAcceleration.NodeHash},
		{"IntegrityAcceleration.EndpointHash", r.IntegrityAcceleration.EndpointHash},
		{"HistoryAcceleration.TxTimeQuery", r.HistoryAcceleration.TxTimeQuery},
		{"HistoryAcceleration.RollbackTrim", r.HistoryAcceleration.RollbackTrim},
		{"HistoryAcceleration.VersionPaging", r.HistoryAcceleration.VersionPaging},
		{"HistoryAcceleration.Compaction", r.HistoryAcceleration.Compaction},
		{"HistoryAcceleration.Degree", r.HistoryAcceleration.Degree},
		{"HistoryAcceleration.DepthHistoryIter", r.HistoryAcceleration.DepthHistoryIter},
		{"HistoryAcceleration.DeletedIter", r.HistoryAcceleration.DeletedIter},
		{"HistoryAcceleration.DepthDeletedIter", r.HistoryAcceleration.DepthDeletedIter},
		{"IndexAcceleration.PropertyIndex", r.IndexAcceleration.PropertyIndex},
		{"IndexAcceleration.RelPropertyIndex", r.IndexAcceleration.RelPropertyIndex},
		{"IndexAcceleration.CompositePropertyIndex", r.IndexAcceleration.CompositePropertyIndex},
		{"IndexAcceleration.TemporalIndex", r.IndexAcceleration.TemporalIndex},
		{"IndexAcceleration.VectorIndex", r.IndexAcceleration.VectorIndex},
		{"IndexAcceleration.VectorIndexOptions", r.IndexAcceleration.VectorIndexOptions},
		{"IndexAcceleration.FilteredVectorSearch", r.IndexAcceleration.FilteredVectorSearch},
		{"IndexAcceleration.HighFrequencyIndex", r.IndexAcceleration.HighFrequencyIndex},
		{"IndexAcceleration.PropertyKeyStats", r.IndexAcceleration.PropertyKeyStats},
		{"IndexAcceleration.PropertyStats", r.IndexAcceleration.PropertyStats},
		{"ChangeLog.Feed", r.ChangeLog.Feed},
		{"ChangeLog.StatusQuery", r.ChangeLog.StatusQuery},
		{"ChangeLog.TxScope", r.ChangeLog.TxScope},
		{"DestructiveAdmin.ExactErasure", r.DestructiveAdmin.ExactErasure},
		{"MetaKV", r.MetaKV},
	}
	for _, c := range present {
		if !c.got {
			t.Errorf("CapabilitiesOf(fullOptional).%s = false, want true", c.name)
		}
	}
}

// oldStyleStore is a full Store implementing NONE of the direct optional
// capabilities but DOES embed the four Store index capabilities — so the report
// marks exactly those four present and everything else absent.
func TestCapabilitiesOfOldStyleStoreReportsIndexQuartetOnly(t *testing.T) {
	t.Parallel()
	r := CapabilitiesOf(oldStyleStore{})
	if !r.IndexAcceleration.PropertyIndex || !r.IndexAcceleration.TemporalIndex ||
		!r.IndexAcceleration.VectorIndex || !r.IndexAcceleration.HighFrequencyIndex {
		t.Fatalf("old-style Store must report the four embedded index capabilities present, got %+v", r.IndexAcceleration)
	}
	if r.IndexAcceleration.VectorIndexOptions || r.IndexAcceleration.FilteredVectorSearch ||
		r.IndexAcceleration.PropertyKeyStats || r.IndexAcceleration.PropertyStats ||
		r.IndexAcceleration.RelPropertyIndex || r.IndexAcceleration.CompositePropertyIndex {
		t.Fatalf("old-style Store must NOT report the direct-optional index accelerators, got %+v", r.IndexAcceleration)
	}
	if r.MetaKV || r.ChangeLog.Feed || r.IntegrityAcceleration.NodeHash {
		t.Fatalf("old-style Store must not report non-index optionals present, got %+v", r)
	}
}

func TestCapabilitiesOfNilReportsAllAbsent(t *testing.T) {
	t.Parallel()
	if r := CapabilitiesOf(nil); r != (CapabilityReport{}) {
		t.Fatalf("CapabilitiesOf(nil) = %+v, want zero value", r)
	}
}
