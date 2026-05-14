package core

import (
	"context"
	"bytes"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/badger"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

type concreteTxTimeFaultStore struct {
	*memory.Store
	nodeTarget types.NodeID
	relTarget  types.RelID
	err        error
	failNode   atomic.Bool
	failRel    atomic.Bool
}

func (s *concreteTxTimeFaultStore) GetNode(id types.NodeID) (*types.Node, error) {
	if s.failNode.Load() && id == s.nodeTarget {
		return nil, s.err
	}
	return s.Store.GetNode(id)
}

func (s *concreteTxTimeFaultStore) GetRelationship(id types.RelID) (*types.Relationship, error) {
	if s.failRel.Load() && id == s.relTarget {
		return nil, s.err
	}
	return s.Store.GetRelationship(id)
}

type concreteRollbackHistoryFaultStore struct {
	*memory.Store
	err          error
	failNodeTrim atomic.Bool
	failRelTrim  atomic.Bool
}

type directTrimEmbeddedHistoryFaultStore struct {
	*memory.Store
	err             error
	failNodeHistory atomic.Bool
	failRelHistory  atomic.Bool
}

func (s *directTrimEmbeddedHistoryFaultStore) GetNodeHistory(id types.NodeID) ([]*types.Node, error) {
	if s.failNodeHistory.Load() {
		return nil, s.err
	}
	return s.Store.GetNodeHistory(id)
}

func (s *directTrimEmbeddedHistoryFaultStore) GetRelHistory(id types.RelID) ([]*types.Relationship, error) {
	if s.failRelHistory.Load() {
		return nil, s.err
	}
	return s.Store.GetRelHistory(id)
}

func (s *directTrimEmbeddedHistoryFaultStore) TrimNodeHistoryFrom(id types.NodeID, minVersion uint32) error {
	return s.Store.TrimNodeHistoryFrom(id, minVersion)
}

func (s *directTrimEmbeddedHistoryFaultStore) TrimRelHistoryFrom(id types.RelID, minVersion uint32) error {
	return s.Store.TrimRelHistoryFrom(id, minVersion)
}

type tieredDepthHistoryIterationFaultStore struct {
	*tiered.Store
	err             error
	failNodeHistory atomic.Bool
	failRelHistory  atomic.Bool
}

func (s *tieredDepthHistoryIterationFaultStore) ForEachNodeHistoryID(fn func(types.NodeID) bool) error {
	if s.failNodeHistory.Load() {
		return s.err
	}
	return s.Store.ForEachNodeHistoryID(fn)
}

func (s *tieredDepthHistoryIterationFaultStore) ForEachRelHistoryID(fn func(types.RelID) bool) error {
	if s.failRelHistory.Load() {
		return s.err
	}
	return s.Store.ForEachRelHistoryID(fn)
}

type promotedHistoryPageStore struct {
	*memory.Store
}

type nestedPromotedHistoryPageStore struct {
	promotedHistoryPageStore
}

type directDepthHistoryIterationStore struct {
	storepkg.MandatoryStore
	nodeErr error
	relErr  error
}

func (s *directDepthHistoryIterationStore) ForEachNodeHistoryIDByDepth(storepkg.ShardDepth, func(types.NodeID) bool) error {
	return s.nodeErr
}

func (s *directDepthHistoryIterationStore) ForEachRelHistoryIDByDepth(storepkg.ShardDepth, func(types.RelID) bool) error {
	return s.relErr
}

type concreteFilteredVectorFaultStore struct {
	*memory.Store
	err  error
	fail atomic.Bool
}

func (s *concreteFilteredVectorFaultStore) SearchNearestNodes(labelToken uint16, propertyKey string, query []float32, k int, opts storepkg.QueryOpts) ([]*types.Node, error) {
	if s.fail.Load() {
		return nil, s.err
	}
	return s.Store.SearchNearestNodes(labelToken, propertyKey, query, k, opts)
}

type concretePropertyQueryFaultStore struct {
	*memory.Store
	err  error
	fail atomic.Bool
}

func (s *concretePropertyQueryFaultStore) NodesByLabel(token uint16, opts storepkg.QueryOpts) ([]*types.Node, error) {
	if s.fail.Load() {
		return nil, s.err
	}
	return s.Store.NodesByLabel(token, opts)
}

type concretePropertyScanTrackingStore struct {
	*memory.Store
	lastOpts storepkg.QueryOpts
}

func (s *concretePropertyScanTrackingStore) NodesByLabel(token uint16, opts storepkg.QueryOpts) ([]*types.Node, error) {
	s.lastOpts = opts
	return s.Store.NodesByLabel(token, opts)
}

type interfaceStorePropertyScanFaultStore struct {
	storepkg.Store
	err  error
	fail atomic.Bool
}

func (s *interfaceStorePropertyScanFaultStore) NodesByLabel(token uint16, opts storepkg.QueryOpts) ([]*types.Node, error) {
	if s.fail.Load() {
		return nil, s.err
	}
	return s.Store.NodesByLabel(token, opts)
}

type interfaceStoreDirectPropertyQueryFaultStore struct {
	storepkg.Store
	queryErr error
	scanErr  error
}

func (s *interfaceStoreDirectPropertyQueryFaultStore) NodesByLabel(token uint16, opts storepkg.QueryOpts) ([]*types.Node, error) {
	return nil, s.scanErr
}

func (s *interfaceStoreDirectPropertyQueryFaultStore) NodesByLabelAndProperty(uint16, string, any, storepkg.QueryOpts) ([]*types.Node, error) {
	return nil, s.queryErr
}

type concreteBulkReadRowFaultStore struct {
	*memory.Store
	nodeRows     []*types.Node
	relRows      []*types.Relationship
	outgoingRows []*types.Relationship
	incomingRows []*types.Relationship
	outgoingMap  map[types.NodeID][]*types.Relationship
	incomingMap  map[types.NodeID][]*types.Relationship
	getNodeRow   *types.Node
	getRelRow    *types.Relationship
	nodeHistory  []*types.Node
	relHistory   []*types.Relationship
	nodeVersion  *types.Node
	relVersion   *types.Relationship
	nodePage     []*types.Node
	relPage      []*types.Relationship
	nodeIDs      []types.NodeID
	relIDs       []types.RelID
	nodeHistIDs  []types.NodeID
	relHistIDs   []types.RelID
	nodeCount    int
	relCount     int
	labelCount   int
	typeCount    int
	allNodeRows  []*types.Node
	allRelRows   []*types.Relationship
	getNodeRows  []*types.Node
	getRelRows   []*types.Relationship
	failNode     atomic.Bool
	failRel      atomic.Bool
	failOutgoing atomic.Bool
	failIncoming atomic.Bool
	failOutMap   atomic.Bool
	failInMap    atomic.Bool
	failGetNode  atomic.Bool
	failGetRel   atomic.Bool
	failNodeHist atomic.Bool
	failRelHist  atomic.Bool
	failNodeVer  atomic.Bool
	failRelVer   atomic.Bool
	failNodePage atomic.Bool
	failRelPage  atomic.Bool
	failNodeIDs  atomic.Bool
	failRelIDs   atomic.Bool
	failNodeHIDs atomic.Bool
	failRelHIDs  atomic.Bool
	failEachNode atomic.Bool
	failEachRel  atomic.Bool
	failEachNH   atomic.Bool
	failEachRH   atomic.Bool
	failNodeCnt  atomic.Bool
	failRelCnt   atomic.Bool
	failLabelCnt atomic.Bool
	failTypeCnt  atomic.Bool
	failAllNodes atomic.Bool
	failAllRels  atomic.Bool
	failGetNodes atomic.Bool
	failGetRels  atomic.Bool
}

type directHistoryPageFaultStore struct {
	storepkg.MandatoryStore
	nodePage     []*types.Node
	relPage      []*types.Relationship
	failNodePage atomic.Bool
	failRelPage  atomic.Bool
}

func (s *directHistoryPageFaultStore) NodeHistoryVersionsFrom(id types.NodeID, startVersion uint32, limit int) ([]*types.Node, error) {
	if s.failNodePage.Load() {
		return s.nodePage, nil
	}
	return s.MandatoryStore.(storepkg.HistoryVersionPageCapability).NodeHistoryVersionsFrom(id, startVersion, limit)
}

func (s *directHistoryPageFaultStore) RelHistoryVersionsFrom(id types.RelID, startVersion uint32, limit int) ([]*types.Relationship, error) {
	if s.failRelPage.Load() {
		return s.relPage, nil
	}
	return s.MandatoryStore.(storepkg.HistoryVersionPageCapability).RelHistoryVersionsFrom(id, startVersion, limit)
}

func (s *concreteBulkReadRowFaultStore) NodesByLabel(token uint16, opts storepkg.QueryOpts) ([]*types.Node, error) {
	if s.failNode.Load() {
		return s.nodeRows, nil
	}
	return s.Store.NodesByLabel(token, opts)
}

func (s *concreteBulkReadRowFaultStore) RelationshipsByType(token uint16, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	if s.failRel.Load() {
		return s.relRows, nil
	}
	return s.Store.RelationshipsByType(token, opts)
}

func (s *concreteBulkReadRowFaultStore) OutgoingRelationships(nodeID types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	if s.failOutgoing.Load() {
		return s.outgoingRows, nil
	}
	return s.Store.OutgoingRelationships(nodeID, typeToken)
}

func (s *concreteBulkReadRowFaultStore) IncomingRelationships(nodeID types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	if s.failIncoming.Load() {
		return s.incomingRows, nil
	}
	return s.Store.IncomingRelationships(nodeID, typeToken)
}

func (s *concreteBulkReadRowFaultStore) OutgoingRelationshipsForNodes(nodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	if s.failOutMap.Load() {
		return s.outgoingMap, nil
	}
	return s.Store.OutgoingRelationshipsForNodes(nodeIDs, typeToken)
}

func (s *concreteBulkReadRowFaultStore) IncomingRelationshipsForNodes(nodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	if s.failInMap.Load() {
		return s.incomingMap, nil
	}
	return s.Store.IncomingRelationshipsForNodes(nodeIDs, typeToken)
}

func (s *concreteBulkReadRowFaultStore) GetNode(id types.NodeID) (*types.Node, error) {
	if s.failGetNode.Load() {
		return s.getNodeRow, nil
	}
	return s.Store.GetNode(id)
}

func (s *concreteBulkReadRowFaultStore) GetRelationship(id types.RelID) (*types.Relationship, error) {
	if s.failGetRel.Load() {
		return s.getRelRow, nil
	}
	return s.Store.GetRelationship(id)
}

func (s *concreteBulkReadRowFaultStore) GetNodeHistory(id types.NodeID) ([]*types.Node, error) {
	if s.failNodeHist.Load() {
		return s.nodeHistory, nil
	}
	return s.Store.GetNodeHistory(id)
}

func (s *concreteBulkReadRowFaultStore) GetRelHistory(id types.RelID) ([]*types.Relationship, error) {
	if s.failRelHist.Load() {
		return s.relHistory, nil
	}
	return s.Store.GetRelHistory(id)
}

func (s *concreteBulkReadRowFaultStore) GetNodeVersion(id types.NodeID, version uint32) (*types.Node, error) {
	if s.failNodeVer.Load() {
		return s.nodeVersion, nil
	}
	return s.Store.GetNodeVersion(id, version)
}

func (s *concreteBulkReadRowFaultStore) GetRelVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	if s.failRelVer.Load() {
		return s.relVersion, nil
	}
	return s.Store.GetRelVersion(id, version)
}

func (s *concreteBulkReadRowFaultStore) NodeHistoryVersionsFrom(id types.NodeID, startVersion uint32, limit int) ([]*types.Node, error) {
	if s.failNodePage.Load() {
		return s.nodePage, nil
	}
	return s.Store.NodeHistoryVersionsFrom(id, startVersion, limit)
}

func (s *concreteBulkReadRowFaultStore) RelHistoryVersionsFrom(id types.RelID, startVersion uint32, limit int) ([]*types.Relationship, error) {
	if s.failRelPage.Load() {
		return s.relPage, nil
	}
	return s.Store.RelHistoryVersionsFrom(id, startVersion, limit)
}

func (s *concreteBulkReadRowFaultStore) AllNodeIDs(opts storepkg.QueryOpts) ([]types.NodeID, error) {
	if s.failNodeIDs.Load() {
		return s.nodeIDs, nil
	}
	return s.Store.AllNodeIDs(opts)
}

func (s *concreteBulkReadRowFaultStore) AllRelIDs(opts storepkg.QueryOpts) ([]types.RelID, error) {
	if s.failRelIDs.Load() {
		return s.relIDs, nil
	}
	return s.Store.AllRelIDs(opts)
}

func (s *concreteBulkReadRowFaultStore) AllNodeHistoryIDsFrom(after types.NodeID, limit int) ([]types.NodeID, error) {
	if s.failNodeHIDs.Load() {
		return s.nodeHistIDs, nil
	}
	return s.Store.AllNodeHistoryIDsFrom(after, limit)
}

func (s *concreteBulkReadRowFaultStore) AllRelHistoryIDsFrom(after types.RelID, limit int) ([]types.RelID, error) {
	if s.failRelHIDs.Load() {
		return s.relHistIDs, nil
	}
	return s.Store.AllRelHistoryIDsFrom(after, limit)
}

func (s *concreteBulkReadRowFaultStore) ForEachNodeID(fn func(types.NodeID) bool) error {
	if s.failEachNode.Load() {
		for _, id := range s.nodeIDs {
			if !fn(id) {
				return nil
			}
		}
		return nil
	}
	return s.Store.ForEachNodeID(fn)
}

func (s *concreteBulkReadRowFaultStore) ForEachRelID(fn func(types.RelID) bool) error {
	if s.failEachRel.Load() {
		for _, id := range s.relIDs {
			if !fn(id) {
				return nil
			}
		}
		return nil
	}
	return s.Store.ForEachRelID(fn)
}

func (s *concreteBulkReadRowFaultStore) ForEachNodeHistoryID(fn func(types.NodeID) bool) error {
	if s.failEachNH.Load() {
		for _, id := range s.nodeHistIDs {
			if !fn(id) {
				return nil
			}
		}
		return nil
	}
	return s.Store.ForEachNodeHistoryID(fn)
}

func (s *concreteBulkReadRowFaultStore) ForEachRelHistoryID(fn func(types.RelID) bool) error {
	if s.failEachRH.Load() {
		for _, id := range s.relHistIDs {
			if !fn(id) {
				return nil
			}
		}
		return nil
	}
	return s.Store.ForEachRelHistoryID(fn)
}

func (s *concreteBulkReadRowFaultStore) NodeCount() (int, error) {
	if s.failNodeCnt.Load() {
		return s.nodeCount, nil
	}
	return s.Store.NodeCount()
}

func (s *concreteBulkReadRowFaultStore) RelationshipCount() (int, error) {
	if s.failRelCnt.Load() {
		return s.relCount, nil
	}
	return s.Store.RelationshipCount()
}

func (s *concreteBulkReadRowFaultStore) NodeCountByLabel(token uint16) (int, error) {
	if s.failLabelCnt.Load() {
		return s.labelCount, nil
	}
	return s.Store.NodeCountByLabel(token)
}

func (s *concreteBulkReadRowFaultStore) RelCountByType(token uint16) (int, error) {
	if s.failTypeCnt.Load() {
		return s.typeCount, nil
	}
	return s.Store.RelCountByType(token)
}

func (s *concreteBulkReadRowFaultStore) AllNodes(opts storepkg.QueryOpts) ([]*types.Node, error) {
	if s.failAllNodes.Load() {
		return s.allNodeRows, nil
	}
	return s.Store.AllNodes(opts)
}

func (s *concreteBulkReadRowFaultStore) AllRelationships(opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	if s.failAllRels.Load() {
		return s.allRelRows, nil
	}
	return s.Store.AllRelationships(opts)
}

func (s *concreteBulkReadRowFaultStore) GetNodesByIDs(ids []types.NodeID) ([]*types.Node, error) {
	if s.failGetNodes.Load() {
		return s.getNodeRows, nil
	}
	return s.Store.GetNodesByIDs(ids)
}

func (s *concreteBulkReadRowFaultStore) GetRelationshipsByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	if s.failGetRels.Load() {
		return s.getRelRows, nil
	}
	return s.Store.GetRelationshipsByIDs(ids)
}

func nativeWrapperTestNodeIDs(nodes []*types.Node) []types.NodeID {
	ids := make([]types.NodeID, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID()
	}
	return ids
}

type nestedConcretePropertyQueryFaultStore struct {
	*concretePropertyQueryFaultStore
}

type directFilteredVectorFaultStore struct {
	storepkg.MandatoryStore
	createVec       func(uint16, string, int, storepkg.DistanceMetric) error
	dropVec         func(uint16, string) error
	searchVec       func(uint16, string, []float32, int, storepkg.QueryOpts) ([]*types.Node, error)
	getNodeOverride map[types.NodeID]*types.Node
	filteredIDs     []snowflake.ID
	filteredErr     error
}

func (s *directFilteredVectorFaultStore) CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric storepkg.DistanceMetric) error {
	return s.createVec(labelToken, propertyKey, dims, metric)
}

func (s *directFilteredVectorFaultStore) DropVectorIndex(labelToken uint16, propertyKey string) error {
	return s.dropVec(labelToken, propertyKey)
}

func (s *directFilteredVectorFaultStore) SearchNearestNodes(labelToken uint16, propertyKey string, query []float32, k int, opts storepkg.QueryOpts) ([]*types.Node, error) {
	return s.searchVec(labelToken, propertyKey, query, k, opts)
}

func (s *directFilteredVectorFaultStore) GetNode(id types.NodeID) (*types.Node, error) {
	if s.getNodeOverride != nil {
		if n, ok := s.getNodeOverride[id]; ok {
			return n, nil
		}
	}
	return s.MandatoryStore.GetNode(id)
}

func (s *directFilteredVectorFaultStore) SearchNearestFiltered(uint16, string, []float32, int, func(snowflake.ID) bool) ([]snowflake.ID, error) {
	return s.filteredIDs, s.filteredErr
}

type directVectorSearchStore struct {
	storepkg.MandatoryStore
	createVec func(uint16, string, int, storepkg.DistanceMetric) error
	dropVec   func(uint16, string) error
	searchVec func(uint16, string, []float32, int, storepkg.QueryOpts) ([]*types.Node, error)
}

func (s *directVectorSearchStore) CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric storepkg.DistanceMetric) error {
	return s.createVec(labelToken, propertyKey, dims, metric)
}

func (s *directVectorSearchStore) DropVectorIndex(labelToken uint16, propertyKey string) error {
	return s.dropVec(labelToken, propertyKey)
}

func (s *directVectorSearchStore) SearchNearestNodes(labelToken uint16, propertyKey string, query []float32, k int, opts storepkg.QueryOpts) ([]*types.Node, error) {
	return s.searchVec(labelToken, propertyKey, query, k, opts)
}

func TestCapabilityReflectionHelpersDetectPromotedNativeMethods(t *testing.T) {
	t.Parallel()

	historyPageType := reflect.TypeOf((*storepkg.HistoryVersionPageCapability)(nil)).Elem()
	if typeCanPromoteCapability(nil, historyPageType) {
		t.Fatal("nil type must not promote a capability")
	}
	if !typeCanPromoteCapability(reflect.TypeOf(memory.Store{}), historyPageType) {
		t.Fatal("memory.Store value type should promote pointer-receiver history page methods")
	}

	if typeDeclaresMethod(reflect.TypeOf(&promotedHistoryPageStore{}), "NodeHistoryVersionsFrom") {
		t.Fatal("promoted memory.Store method must not be treated as wrapper-declared")
	}
	if !typeDeclaresMethod(reflect.TypeOf(&directHistoryPageFaultStore{}), "NodeHistoryVersionsFrom") {
		t.Fatal("source-backed wrapper method should be treated as wrapper-declared")
	}

	if !typeEmbedsNativeCapability(reflect.TypeOf((*nestedPromotedHistoryPageStore)(nil)), historyPageType, make(map[reflect.Type]bool)) {
		t.Fatal("nested promoted memory.Store capability should be detected from type alone")
	}
	if typeEmbedsNativeCapability(reflect.TypeOf(struct{ *bytes.Buffer }{}), historyPageType, make(map[reflect.Type]bool)) {
		t.Fatal("unrelated embedded type must not be treated as an in-tree store capability")
	}

	var nilMandatory storepkg.MandatoryStore
	if valueEmbedsNativeCapability(reflect.ValueOf(&nilMandatory).Elem(), historyPageType, make(map[reflect.Type]bool)) {
		t.Fatal("nil interface value must not be treated as embedding a native capability")
	}
	var nilNested *nestedPromotedHistoryPageStore
	if !valueEmbedsNativeCapability(reflect.ValueOf(nilNested), historyPageType, make(map[reflect.Type]bool)) {
		t.Fatal("nil wrapper pointer should still expose promoted native methods through its type")
	}
	if !embedsNativeCapability(&promotedHistoryPageStore{Store: memory.New()}, historyPageType) {
		t.Fatal("wrapper that only promotes memory.Store methods should be detected")
	}

	direct := &directHistoryPageFaultStore{MandatoryStore: memory.New()}
	if embedsNativeCapability(direct, historyPageType, "NodeHistoryVersionsFrom", "RelHistoryVersionsFrom") {
		t.Fatal("wrapper-declared methods must prevent native capability suppression")
	}
}

func TestDepthHistoryIterationCapabilityAllowsDirectExternalCapability(t *testing.T) {
	t.Parallel()

	injectedNode := errors.New("synthetic direct node depth iterator fault")
	injectedRel := errors.New("synthetic direct relationship depth iterator fault")
	fs := &directDepthHistoryIterationStore{
		MandatoryStore: memory.New(),
		nodeErr:        injectedNode,
		relErr:         injectedRel,
	}
	if _, ok := any(fs).(storepkg.DepthHistoryIterationCapability); !ok {
		t.Fatal("test store must declare DepthHistoryIterationCapability")
	}
	if depthHistoryIterationCapability(fs) == nil {
		t.Fatal("direct external depth history capability must remain enabled")
	}

	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.depthHistory == nil {
		t.Fatal("Core should cache the direct external depth history capability")
	}

	if err := g.forEachNodeHistoryIDByDepth(storepkg.DepthHot, func(types.NodeID) bool {
		return true
	}); !errors.Is(err, injectedNode) {
		t.Fatalf("forEachNodeHistoryIDByDepth error = %v, want direct capability fault", err)
	}
	if err := g.forEachRelHistoryIDByDepth(storepkg.DepthWarm, func(types.RelID) bool {
		return true
	}); !errors.Is(err, injectedRel) {
		t.Fatalf("forEachRelHistoryIDByDepth error = %v, want direct capability fault", err)
	}
}

func TestQueryCapabilityResultValidationTrustPolicy(t *testing.T) {
	t.Parallel()
	native, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New native memory: %v", err)
	}
	t.Cleanup(func() { _ = native.Close() })
	if !native.propertyQueryTrust {
		t.Fatal("native memory property query results should not be revalidated")
	}
	if !native.vectorRowsTrust {
		t.Fatal("native memory vector search rows should not be revalidated")
	}

	ms := memory.New()
	external, err := New(Config{Store: &directVectorSearchStore{
		MandatoryStore: ms,
		createVec:      ms.CreateVectorIndex,
		dropVec:        ms.DropVectorIndex,
		searchVec:      ms.SearchNearestNodes,
	}})
	if err != nil {
		t.Fatalf("New direct external vector store: %v", err)
	}
	t.Cleanup(func() { _ = external.Close() })
	if external.vectorRowsTrust {
		t.Fatal("direct external vector search rows must be validated")
	}

	propMS := memory.New()
	externalProperty, err := New(Config{Store: &directPropertyQueryFaultStore{
		MandatoryStore: propMS,
		createProp:     propMS.CreatePropertyIndex,
		dropProp:       propMS.DropPropertyIndex,
	}})
	if err != nil {
		t.Fatalf("New direct external property store: %v", err)
	}
	t.Cleanup(func() { _ = externalProperty.Close() })
	if externalProperty.propertyQueryTrust {
		t.Fatal("direct external property query rows must be validated")
	}
}

func TestPropertyQuery_IgnoresInterfaceEmbeddedNativeStore(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic interface-embedded NodesByLabel fault")
	ms := memory.New()
	fs := &interfaceStorePropertyScanFaultStore{Store: ms, err: injected}
	if _, ok := any(fs).(storepkg.PropertyIndexCapability); !ok {
		t.Fatal("test wrapper must inherit PropertyIndexCapability from store.Store")
	}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.propertyQuery != nil {
		t.Fatal("interface-embedded native store must not enable the property-query fast path")
	}

	if _, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"status": "draft"}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	fs.fail.Store(true)
	if nodes, err := g.Nodes.ByLabelAndProperty("Doc", "status", "draft", storepkg.QueryOpts{}); !errors.Is(err, injected) || nodes != nil {
		t.Fatalf("ByLabelAndProperty with interface-embedded native store = (%v, %v), want nil, injected fault", nodes, err)
	}
}

func TestPropertyQuery_AllowsInterfaceEmbeddedDirectCapability(t *testing.T) {
	t.Parallel()
	queryErr := errors.New("synthetic direct interface property query fault")
	scanErr := errors.New("synthetic fallback label scan fault")
	ms := memory.New()
	fs := &interfaceStoreDirectPropertyQueryFaultStore{
		Store:    ms,
		queryErr: queryErr,
		scanErr:  scanErr,
	}
	if _, ok := any(fs).(storepkg.PropertyIndexCapability); !ok {
		t.Fatal("test wrapper must satisfy PropertyIndexCapability")
	}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.propertyQuery == nil {
		t.Fatal("interface-embedded direct property query capability must remain enabled")
	}

	if _, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"status": "draft"}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if nodes, err := g.Nodes.ByLabelAndProperty("Doc", "status", "draft", storepkg.QueryOpts{}); !errors.Is(err, queryErr) || nodes != nil {
		t.Fatalf("ByLabelAndProperty with direct interface store = (%v, %v), want nil, direct query fault", nodes, err)
	}
}

type directPropertyQueryFaultStore struct {
	storepkg.MandatoryStore
	createProp func(uint16, string) error
	dropProp   func(uint16, string) error
	queryNodes []*types.Node
	queryErr   error
	queryCalls atomic.Int64
}

func (s *directPropertyQueryFaultStore) CreatePropertyIndex(labelToken uint16, propertyKey string) error {
	return s.createProp(labelToken, propertyKey)
}

func (s *directPropertyQueryFaultStore) DropPropertyIndex(labelToken uint16, propertyKey string) error {
	return s.dropProp(labelToken, propertyKey)
}

func (s *directPropertyQueryFaultStore) NodesByLabelAndProperty(uint16, string, any, storepkg.QueryOpts) ([]*types.Node, error) {
	s.queryCalls.Add(1)
	return s.queryNodes, s.queryErr
}

func (s *concreteRollbackHistoryFaultStore) TruncateNodeHistory(id types.NodeID, keepVersions int) error {
	if s.failNodeTrim.Load() {
		return s.err
	}
	return s.Store.TruncateNodeHistory(id, keepVersions)
}

func (s *concreteRollbackHistoryFaultStore) TruncateRelHistory(id types.RelID, keepVersions int) error {
	if s.failRelTrim.Load() {
		return s.err
	}
	return s.Store.TruncateRelHistory(id, keepVersions)
}

func TestNativeTransactionTimeQuery_IgnoresConcreteMemoryWrapper(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic transaction-time read fault")
	fs := &concreteTxTimeFaultStore{Store: memory.New(), err: injected}
	if _, ok := any(fs).(storepkg.TransactionTimeQueryCapability); !ok {
		t.Fatal("test wrapper must inherit TransactionTimeQueryCapability from memory.Store")
	}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.txTimeQuery != nil {
		t.Fatal("concrete wrapper must not enable the transaction-time query fast path")
	}

	a, err := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	fs.nodeTarget = a.ID()
	fs.failNode.Store(true)
	if _, err := g.Temporal.NodeAsOf(a.ID(), a.Temporal().TxFrom); !errors.Is(err, injected) {
		t.Fatalf("NodeAsOf err = %v, want injected read fault", err)
	}
	fs.failNode.Store(false)

	fs.relTarget = r.ID()
	fs.failRel.Store(true)
	if _, err := g.Temporal.RelAsOf(r.ID(), r.Temporal().TxFrom); !errors.Is(err, injected) {
		t.Fatalf("RelAsOf err = %v, want injected read fault", err)
	}
}

func TestNativeHistoryRollbackTrim_IgnoresConcreteMemoryWrapperForNodeRollback(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic node history restore fault")
	fs := &concreteRollbackHistoryFaultStore{Store: memory.New(), err: injected}
	if _, ok := any(fs).(storepkg.HistoryRollbackTrimCapability); !ok {
		t.Fatal("test wrapper must inherit HistoryRollbackTrimCapability from memory.Store")
	}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.historyTrim != nil {
		t.Fatal("concrete wrapper must not enable the history rollback trim fast path")
	}

	n, err := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.UpdateNode(n.ID(), map[string]any{"v": int64(1)}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	fs.failNodeTrim.Store(true)
	if err := tx.Rollback(); !errors.Is(err, injected) {
		t.Fatalf("Rollback err = %v, want injected history restore fault", err)
	}
}

func TestNativeHistoryRollbackTrim_IgnoresConcreteMemoryWrapperForRelRollback(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic relationship history restore fault")
	fs := &concreteRollbackHistoryFaultStore{Store: memory.New(), err: injected}
	if _, ok := any(fs).(storepkg.HistoryRollbackTrimCapability); !ok {
		t.Fatal("test wrapper must inherit HistoryRollbackTrimCapability from memory.Store")
	}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.historyTrim != nil {
		t.Fatal("concrete wrapper must not enable the history rollback trim fast path")
	}

	a, err := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.UpdateRelationship(r.ID(), map[string]any{"v": int64(1)}); err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}
	fs.failRelTrim.Store(true)
	if err := tx.Rollback(); !errors.Is(err, injected) {
		t.Fatalf("Rollback err = %v, want injected history restore fault", err)
	}
}

func TestNativeHistoryRollbackTrim_AllowsExactBadgerStore(t *testing.T) {
	t.Parallel()
	bs, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	g, err := New(Config{Store: bs})
	if err != nil {
		_ = bs.Close()
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.historyTrim == nil {
		t.Fatal("exact badger.Store must enable the history rollback trim fast path")
	}
}

func TestImportRollbackCopyHistoryFromIgnoresEmbeddedNativePager(t *testing.T) {
	t.Parallel()

	ms := memory.New()
	nid := types.NodeID(100)
	for _, version := range []uint32{0, 2} {
		n := types.NewNode(nid, 1, nil)
		n.SetVersion(version)
		if err := ms.PutNodeVersion(nid, version, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", version, err)
		}
	}
	rid := types.RelID(300)
	for _, version := range []uint32{0, 2} {
		r := types.NewRelationship(rid, 1, types.NodeID(10), types.NodeID(20))
		r.SetVersion(version)
		if err := ms.PutRelVersion(rid, version, r); err != nil {
			t.Fatalf("PutRelVersion(%d): %v", version, err)
		}
	}

	injected := errors.New("synthetic history read fault")
	fs := &directTrimEmbeddedHistoryFaultStore{Store: ms, err: injected}
	if _, ok := any(fs).(storepkg.HistoryRollbackTrimCapability); !ok {
		t.Fatal("test wrapper must declare HistoryRollbackTrimCapability")
	}
	if _, ok := any(fs).(storepkg.HistoryVersionPageCapability); !ok {
		t.Fatal("test wrapper must inherit HistoryVersionPageCapability from memory.Store")
	}
	if historyVersionPageCapability(fs) != nil {
		t.Fatal("embedded native history pager must not be selected for the wrapper")
	}

	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.historyTrim == nil {
		t.Fatal("direct wrapper trim implementation should enable rollback trim")
	}

	fs.failNodeHistory.Store(true)
	if _, err := g.copyNodeHistoryFrom(nid, 1); !errors.Is(err, injected) {
		t.Fatalf("copyNodeHistoryFrom error = %v, want injected read fault", err)
	}
	fs.failNodeHistory.Store(false)
	nodeSuffix, err := g.copyNodeHistoryFrom(nid, 1)
	if err != nil {
		t.Fatalf("copyNodeHistoryFrom: %v", err)
	}
	if got := importRollbackNodeHistoryVersions(nodeSuffix); len(got) != 1 || got[0] != 2 {
		t.Fatalf("node suffix versions = %v, want [2]", got)
	}

	fs.failRelHistory.Store(true)
	if _, err := g.copyRelHistoryFrom(rid, 1); !errors.Is(err, injected) {
		t.Fatalf("copyRelHistoryFrom error = %v, want injected read fault", err)
	}
	fs.failRelHistory.Store(false)
	relSuffix, err := g.copyRelHistoryFrom(rid, 1)
	if err != nil {
		t.Fatalf("copyRelHistoryFrom: %v", err)
	}
	if got := importRollbackRelHistoryVersions(relSuffix); len(got) != 1 || got[0] != 2 {
		t.Fatalf("relationship suffix versions = %v, want [2]", got)
	}
}

func TestDepthHistoryIterationCapability_IgnoresEmbeddedTieredWrapper(t *testing.T) {
	t.Parallel()

	injected := errors.New("synthetic depth history fallback fault")
	fs := &tieredDepthHistoryIterationFaultStore{Store: newTestTieredStore(t), err: injected}
	if _, ok := any(fs).(storepkg.DepthHistoryIterationCapability); !ok {
		t.Fatal("test wrapper must inherit DepthHistoryIterationCapability from tiered.Store")
	}
	if depthHistoryIterationCapability(fs) != nil {
		t.Fatal("embedded tiered depth history iterator must not be selected for the wrapper")
	}

	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.depthHistory != nil {
		t.Fatal("Core should not cache an embedded tiered depth history capability for the wrapper")
	}

	fs.failNodeHistory.Store(true)
	if err := g.forEachNodeHistoryIDByDepth(storepkg.DepthHot, func(types.NodeID) bool {
		return true
	}); !errors.Is(err, injected) {
		t.Fatalf("forEachNodeHistoryIDByDepth error = %v, want injected fallback fault", err)
	}
	fs.failNodeHistory.Store(false)
	if err := g.forEachNodeHistoryIDByDepth(storepkg.DepthHot, func(types.NodeID) bool {
		return true
	}); err != nil {
		t.Fatalf("forEachNodeHistoryIDByDepth after fault disabled: %v", err)
	}

	fs.failRelHistory.Store(true)
	if err := g.forEachRelHistoryIDByDepth(storepkg.DepthHot, func(types.RelID) bool {
		return true
	}); !errors.Is(err, injected) {
		t.Fatalf("forEachRelHistoryIDByDepth error = %v, want injected fallback fault", err)
	}
	fs.failRelHistory.Store(false)
	if err := g.forEachRelHistoryIDByDepth(storepkg.DepthHot, func(types.RelID) bool {
		return true
	}); err != nil {
		t.Fatalf("forEachRelHistoryIDByDepth after fault disabled: %v", err)
	}
}

func TestFilteredVectorSearch_IgnoresConcreteMemoryWrapper(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic vector search fault")
	fs := &concreteFilteredVectorFaultStore{Store: memory.New(), err: injected}
	if _, ok := any(fs).(storepkg.FilteredVectorSearchCapability); !ok {
		t.Fatal("test wrapper must inherit FilteredVectorSearchCapability from memory.Store")
	}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.filteredVector != nil {
		t.Fatal("concrete wrapper must not enable the filtered vector fast path")
	}

	n, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"embedding": []float32{1, 0}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := g.Index.CreateVector("Doc", "embedding", 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVector: %v", err)
	}

	fs.fail.Store(true)
	_, err = g.Index.SearchNearest("Doc", "embedding", []float32{1, 0}, 1, storepkg.QueryOpts{ValidAt: n.Temporal().TxFrom})
	if !errors.Is(err, injected) {
		t.Fatalf("SearchNearest err = %v, want injected vector search fault", err)
	}
}

func TestFilteredVectorSearch_AllowsDirectExternalCapability(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic direct filtered vector fault")
	ms := memory.New()
	fs := &directFilteredVectorFaultStore{
		MandatoryStore: ms,
		createVec:      ms.CreateVectorIndex,
		dropVec:        ms.DropVectorIndex,
		searchVec:      ms.SearchNearestNodes,
		filteredErr:    injected,
	}
	if _, ok := any(fs).(storepkg.FilteredVectorSearchCapability); !ok {
		t.Fatal("test store must satisfy FilteredVectorSearchCapability directly")
	}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.filteredVector == nil {
		t.Fatal("direct external filtered vector capability must remain enabled")
	}

	n, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"embedding": []float32{1, 0}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := g.Index.CreateVector("Doc", "embedding", 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVector: %v", err)
	}

	_, err = g.Index.SearchNearest("Doc", "embedding", []float32{1, 0}, 1, storepkg.QueryOpts{ValidAt: n.Temporal().TxFrom})
	if !errors.Is(err, injected) {
		t.Fatalf("SearchNearest err = %v, want direct filtered vector fault", err)
	}
}

func TestSearchNearestCopiesValidExternalVectorRows(t *testing.T) {
	t.Parallel()
	ms := memory.New()
	var backing *types.Node
	fs := &directVectorSearchStore{
		MandatoryStore: ms,
		createVec:      ms.CreateVectorIndex,
		dropVec:        ms.DropVectorIndex,
		searchVec: func(uint16, string, []float32, int, storepkg.QueryOpts) ([]*types.Node, error) {
			return []*types.Node{backing}, nil
		},
	}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if g.vectorRowsTrust {
		t.Fatal("direct external vector rows must be copied defensively")
	}

	backing, err = g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"embedding": []float32{1, 0}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := g.Index.CreateVector("Doc", "embedding", 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVector: %v", err)
	}

	nodes, err := g.Index.SearchNearest("Doc", "embedding", []float32{1, 0}, 1, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearest: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("SearchNearest len = %d, want 1", len(nodes))
	}
	if nodes[0] == backing {
		t.Fatal("SearchNearest returned external node pointer")
	}
	if err := nodes[0].SetProperty("probe_vector", "mutated"); err != nil {
		t.Fatalf("mutate returned vector row: %v", err)
	}
	if _, ok := backing.GetProperty("probe_vector"); ok {
		t.Fatal("SearchNearest returned node shares properties with external backing row")
	}
}

func TestSearchNearestRejectsInvalidFilteredVectorCapabilityIDs(t *testing.T) {
	t.Parallel()
	ms := memory.New()
	fs := &directFilteredVectorFaultStore{
		MandatoryStore: ms,
		createVec:      ms.CreateVectorIndex,
		dropVec:        ms.DropVectorIndex,
		searchVec:      ms.SearchNearestNodes,
	}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	n1, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"embedding": []float32{1, 0}})
	if err != nil {
		t.Fatalf("AddNode first: %v", err)
	}
	n2, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"embedding": []float32{2, 0}})
	if err != nil {
		t.Fatalf("AddNode second: %v", err)
	}
	if err := g.Index.CreateVector("Doc", "embedding", 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVector: %v", err)
	}
	notIndexed, err := g.Nodes.Add(context.Background(), []string{"Doc"}, nil)
	if err != nil {
		t.Fatalf("AddNode not indexed: %v", err)
	}

	fs.filteredIDs = []snowflake.ID{n1.ID().SnowflakeID(), n2.ID().SnowflakeID()}
	if nodes, err := g.Index.SearchNearest("Doc", "embedding", []float32{1, 0}, 1, storepkg.QueryOpts{ValidAt: n1.Temporal().TxFrom}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("SearchNearest over-k filtered IDs = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}

	fs.filteredIDs = []snowflake.ID{n1.ID().SnowflakeID(), n1.ID().SnowflakeID()}
	if nodes, err := g.Index.SearchNearest("Doc", "embedding", []float32{1, 0}, 2, storepkg.QueryOpts{ValidAt: n1.Temporal().TxFrom}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("SearchNearest duplicate filtered IDs = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}

	fs.filteredIDs = []snowflake.ID{notIndexed.ID().SnowflakeID()}
	if nodes, err := g.Index.SearchNearest("Doc", "embedding", []float32{1, 0}, 1, storepkg.QueryOpts{ValidAt: n1.Temporal().TxFrom}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("SearchNearest non-indexed filtered ID = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}

	wrongDims := types.NewNode(n2.ID()+1000, n1.PrimaryLabelToken().Value(), nil)
	if err := wrongDims.SetProperty("embedding", []float32{1, 0, 0}); err != nil {
		t.Fatalf("SetProperty wrong dims: %v", err)
	}
	fs.getNodeOverride = map[types.NodeID]*types.Node{wrongDims.ID(): wrongDims}
	fs.filteredIDs = []snowflake.ID{wrongDims.ID().SnowflakeID()}
	if nodes, err := g.Index.SearchNearest("Doc", "embedding", []float32{1, 0}, 1, storepkg.QueryOpts{ValidAt: n1.Temporal().TxFrom}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("SearchNearest wrong-dimension filtered ID = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}
}

func TestSearchNearestRejectsInvalidVectorCapabilityRows(t *testing.T) {
	t.Parallel()
	ms := memory.New()
	fs := &directVectorSearchStore{
		MandatoryStore: ms,
		createVec:      ms.CreateVectorIndex,
		dropVec:        ms.DropVectorIndex,
		searchVec: func(uint16, string, []float32, int, storepkg.QueryOpts) ([]*types.Node, error) {
			return []*types.Node{nil}, nil
		},
	}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	n, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"embedding": []float32{1, 0}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := g.Index.CreateVector("Doc", "embedding", 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVector: %v", err)
	}
	n2, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"embedding": []float32{2, 0}})
	if err != nil {
		t.Fatalf("AddNode second: %v", err)
	}
	if nodes, err := g.Index.SearchNearest("Doc", "embedding", []float32{1, 0}, 1, storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("SearchNearest invalid non-temporal row = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}

	fs.searchVec = func(uint16, string, []float32, int, storepkg.QueryOpts) ([]*types.Node, error) {
		return []*types.Node{n, n2}, nil
	}
	if nodes, err := g.Index.SearchNearest("Doc", "embedding", []float32{1, 0}, 1, storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("SearchNearest over-k non-temporal rows = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}

	fs.searchVec = func(uint16, string, []float32, int, storepkg.QueryOpts) ([]*types.Node, error) {
		return []*types.Node{n, n}, nil
	}
	if nodes, err := g.Index.SearchNearest("Doc", "embedding", []float32{1, 0}, 2, storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("SearchNearest duplicate non-temporal rows = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}

	missingVector := types.NewNode(n.ID()+2, n.PrimaryLabelToken().Value(), nil)
	fs.searchVec = func(uint16, string, []float32, int, storepkg.QueryOpts) ([]*types.Node, error) {
		return []*types.Node{missingVector}, nil
	}
	if nodes, err := g.Index.SearchNearest("Doc", "embedding", []float32{1, 0}, 1, storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("SearchNearest missing-vector row = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}

	wrongDims := types.NewNode(n.ID()+3, n.PrimaryLabelToken().Value(), nil)
	if err := wrongDims.SetProperty("embedding", []float32{1, 0, 0}); err != nil {
		t.Fatalf("SetProperty wrong dims: %v", err)
	}
	fs.searchVec = func(uint16, string, []float32, int, storepkg.QueryOpts) ([]*types.Node, error) {
		return []*types.Node{wrongDims}, nil
	}
	if nodes, err := g.Index.SearchNearest("Doc", "embedding", []float32{1, 0}, 1, storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("SearchNearest wrong-dimension row = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}

	fs.searchVec = func(uint16, string, []float32, int, storepkg.QueryOpts) ([]*types.Node, error) {
		return []*types.Node{n, n2}, nil
	}
	if nodes, err := g.Index.SearchNearest("Doc", "embedding", []float32{1, 0}, 1, storepkg.QueryOpts{ValidAt: n.Temporal().TxFrom}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("SearchNearest over-k temporal rows = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}

	wrongLabel := types.NewNode(n.ID()+1, 2, nil)
	fs.searchVec = func(uint16, string, []float32, int, storepkg.QueryOpts) ([]*types.Node, error) {
		return []*types.Node{wrongLabel}, nil
	}
	if nodes, err := g.Index.SearchNearest("Doc", "embedding", []float32{1, 0}, 1, storepkg.QueryOpts{ValidAt: n.Temporal().TxFrom}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("SearchNearest invalid temporal row = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}
}

func TestPropertyQuery_IgnoresConcreteMemoryWrapper(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic property fallback label scan fault")
	fs := &concretePropertyQueryFaultStore{Store: memory.New(), err: injected}
	if _, ok := any(fs).(storepkg.PropertyIndexCapability); !ok {
		t.Fatal("test wrapper must inherit PropertyIndexCapability from memory.Store")
	}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.propertyQuery != nil {
		t.Fatal("concrete wrapper must not enable the property query fast path")
	}

	if _, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"status": "draft"}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	fs.fail.Store(true)
	_, err = g.Nodes.ByLabelAndProperty("Doc", "status", "draft", storepkg.QueryOpts{})
	if !errors.Is(err, injected) {
		t.Fatalf("ByLabelAndProperty err = %v, want injected label scan fault", err)
	}
}

func TestPropertyQueryFallback_PushesCursorButNotLimitToLabelScan(t *testing.T) {
	t.Parallel()
	fs := &concretePropertyScanTrackingStore{Store: memory.New()}
	if _, ok := any(fs).(storepkg.PropertyIndexCapability); !ok {
		t.Fatal("test wrapper must inherit PropertyIndexCapability from memory.Store")
	}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.propertyQuery != nil {
		t.Fatal("concrete wrapper must use graph property fallback")
	}

	first, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"status": "match"})
	if err != nil {
		t.Fatalf("Add first: %v", err)
	}
	if _, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"status": "skip"}); err != nil {
		t.Fatalf("Add middle: %v", err)
	}
	want, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"status": "match"})
	if err != nil {
		t.Fatalf("Add want: %v", err)
	}

	got, err := g.Nodes.ByLabelAndProperty("Doc", "status", "match", storepkg.QueryOpts{
		After: types.EntityID(first.ID()),
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("ByLabelAndProperty: %v", err)
	}
	if len(got) != 1 || got[0].ID() != want.ID() {
		t.Fatalf("ByLabelAndProperty returned IDs %v, want [%d]", nativeWrapperTestNodeIDs(got), want.ID())
	}
	if fs.lastOpts.After != types.EntityID(first.ID()) {
		t.Fatalf("fallback NodesByLabel After = %d, want cursor %d", fs.lastOpts.After, first.ID())
	}
	if fs.lastOpts.Limit != 0 {
		t.Fatalf("fallback NodesByLabel Limit = %d, want 0 so post-filter pagination stays correct", fs.lastOpts.Limit)
	}
}

func TestPropertyQuery_IgnoresNestedConcreteMemoryWrapper(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic nested property fallback label scan fault")
	fs := &nestedConcretePropertyQueryFaultStore{
		concretePropertyQueryFaultStore: &concretePropertyQueryFaultStore{
			Store: memory.New(),
			err:   injected,
		},
	}
	if _, ok := any(fs).(storepkg.PropertyIndexCapability); !ok {
		t.Fatal("test wrapper must inherit PropertyIndexCapability from nested memory.Store")
	}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.propertyQuery != nil {
		t.Fatal("nested concrete wrapper must not enable the property query fast path")
	}

	if _, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"status": "draft"}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	fs.fail.Store(true)
	_, err = g.Nodes.ByLabelAndProperty("Doc", "status", "draft", storepkg.QueryOpts{})
	if !errors.Is(err, injected) {
		t.Fatalf("ByLabelAndProperty err = %v, want injected label scan fault", err)
	}
}

func TestPropertyQuery_AllowsDirectExternalCapability(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic direct property query fault")
	ms := memory.New()
	fs := &directPropertyQueryFaultStore{
		MandatoryStore: ms,
		createProp:     ms.CreatePropertyIndex,
		dropProp:       ms.DropPropertyIndex,
		queryErr:       injected,
	}
	if _, ok := any(fs).(storepkg.PropertyIndexCapability); !ok {
		t.Fatal("test store must satisfy PropertyIndexCapability directly")
	}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.propertyQuery == nil {
		t.Fatal("direct external property query capability must remain enabled")
	}

	if _, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"status": "draft"}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	_, err = g.Nodes.ByLabelAndProperty("Doc", "status", "draft", storepkg.QueryOpts{})
	if !errors.Is(err, injected) {
		t.Fatalf("ByLabelAndProperty err = %v, want direct property query fault", err)
	}
}

func TestPropertyQuery_UnindexableValueSkipsDirectCapability(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic direct property query fault")
	ms := memory.New()
	fs := &directPropertyQueryFaultStore{
		MandatoryStore: ms,
		createProp:     ms.CreatePropertyIndex,
		dropProp:       ms.DropPropertyIndex,
		queryErr:       injected,
	}
	if _, ok := any(fs).(storepkg.PropertyIndexCapability); !ok {
		t.Fatal("test store must satisfy PropertyIndexCapability directly")
	}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if g.propertyQuery == nil {
		t.Fatal("direct external property query capability must remain enabled")
	}

	if _, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"tags": []string{"alpha"}}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	nodes, err := g.Nodes.ByLabelAndProperty("Doc", "tags", []string{"alpha"}, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperty unindexable value: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("ByLabelAndProperty unindexable value returned %d nodes, want 0", len(nodes))
	}
	if got := fs.queryCalls.Load(); got != 0 {
		t.Fatalf("direct property query calls = %d, want 0", got)
	}
}

func TestPropertyQueryRejectsInvalidDirectCapabilityRows(t *testing.T) {
	t.Parallel()
	ms := memory.New()
	fs := &directPropertyQueryFaultStore{
		MandatoryStore: ms,
		createProp:     ms.CreatePropertyIndex,
		dropProp:       ms.DropPropertyIndex,
		queryNodes:     []*types.Node{nil},
	}
	if _, ok := any(fs).(storepkg.PropertyIndexCapability); !ok {
		t.Fatal("test store must satisfy PropertyIndexCapability directly")
	}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if g.propertyQuery == nil {
		t.Fatal("direct external property query capability must remain enabled")
	}

	doc, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	doc2, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddNode second: %v", err)
	}
	if nodes, err := g.Nodes.ByLabelAndProperty("Doc", "status", "draft", storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("ByLabelAndProperty nil direct row = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}

	fs.queryNodes = []*types.Node{types.NewNode(doc.ID()+1, 2, nil)}
	if nodes, err := g.Nodes.ByLabelAndProperty("Doc", "status", "draft", storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("ByLabelAndProperty wrong-label direct row = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}

	wrongProperty := types.NewNode(doc.ID()+2, doc.PrimaryLabelToken().Value(), nil)
	if err := wrongProperty.SetProperty("status", "archived"); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	fs.queryNodes = []*types.Node{wrongProperty}
	if nodes, err := g.Nodes.ByLabelAndProperty("Doc", "status", "draft", storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("ByLabelAndProperty wrong-property direct row = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}

	fs.queryNodes = []*types.Node{doc, doc2}
	if nodes, err := g.Nodes.ByLabelAndProperty("Doc", "status", "draft", storepkg.QueryOpts{Limit: 1}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("ByLabelAndProperty over-limit direct rows = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}
	fs.queryNodes = []*types.Node{doc}
	if nodes, err := g.Nodes.ByLabelAndProperty("Doc", "status", "draft", storepkg.QueryOpts{After: types.EntityID(doc2.ID())}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("ByLabelAndProperty before-cursor direct row = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}
}

func TestPropertyQueryCopiesValidDirectCapabilityRows(t *testing.T) {
	t.Parallel()
	ms := memory.New()
	fs := &directPropertyQueryFaultStore{
		MandatoryStore: ms,
		createProp:     ms.CreatePropertyIndex,
		dropProp:       ms.DropPropertyIndex,
	}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if g.propertyQueryTrust {
		t.Fatal("direct external property query rows must be copied defensively")
	}

	doc, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	fs.queryNodes = []*types.Node{doc}

	nodes, err := g.Nodes.ByLabelAndProperty("Doc", "status", "draft", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperty: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("ByLabelAndProperty len = %d, want 1", len(nodes))
	}
	if nodes[0] == doc {
		t.Fatal("ByLabelAndProperty returned external node pointer")
	}
	if err := nodes[0].SetProperty("probe_property_query", "mutated"); err != nil {
		t.Fatalf("mutate returned property-query row: %v", err)
	}
	if _, ok := doc.GetProperty("probe_property_query"); ok {
		t.Fatal("ByLabelAndProperty returned node shares properties with external backing row")
	}
	nodes[0] = nil
	if fs.queryNodes[0] == nil {
		t.Fatal("ByLabelAndProperty returned slice shares external backing array")
	}
}

func TestMandatoryBulkReadRowsRejectInvalidExternalRows(t *testing.T) {
	t.Parallel()
	fs := &concreteBulkReadRowFaultStore{Store: memory.New()}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if g.storeRowsTrust {
		t.Fatal("concrete external store rows must be validated")
	}

	a, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Doc"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	rel, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	a, err = g.Nodes.Update(context.Background(), a.ID(), map[string]any{"status": "published"})
	if err != nil {
		t.Fatalf("UpdateNode A: %v", err)
	}
	rel, err = g.Rels.Update(context.Background(), rel.ID(), map[string]any{"status": "published"})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}
	rel2, err := g.Rels.Add(context.Background(), "KNOWS", b, a, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddRelationship second: %v", err)
	}
	rel3, err := g.Rels.Add(context.Background(), "LIKES", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship third: %v", err)
	}
	missingNode := a.ID() + 999999

	fs.failGetNode.Store(true)
	if node, err := g.Nodes.Get(context.Background(), a.ID()); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || node != nil {
		t.Fatalf("Nodes.Get nil row = (%v, %v), want nil, ErrInvalidStoreMutation", node, err)
	}
	fs.getNodeRow = b
	if node, err := g.Nodes.Get(context.Background(), a.ID()); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || node != nil {
		t.Fatalf("Nodes.Get mismatched row = (%v, %v), want nil, ErrInvalidStoreMutation", node, err)
	}
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if node, err := tx.GetNode(a.ID()); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || node != nil {
		t.Fatalf("Tx.GetNode mismatched row = (%v, %v), want nil, ErrInvalidStoreMutation", node, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	fs.failGetNode.Store(false)
	fs.getNodeRow = nil

	fs.failGetRel.Store(true)
	if gotRel, err := g.Rels.Get(context.Background(), rel.ID()); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || gotRel != nil {
		t.Fatalf("Rels.Get nil row = (%v, %v), want nil, ErrInvalidStoreMutation", gotRel, err)
	}
	fs.getRelRow = types.NewRelationship(rel.ID()+1, rel.TypeToken().Value(), a.ID(), b.ID())
	if gotRel, err := g.Rels.Get(context.Background(), rel.ID()); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || gotRel != nil {
		t.Fatalf("Rels.Get mismatched row = (%v, %v), want nil, ErrInvalidStoreMutation", gotRel, err)
	}
	fs.failGetRel.Store(false)
	fs.getRelRow = nil

	fs.nodeHistory = []*types.Node{nil}
	fs.failNodeHist.Store(true)
	if history, err := g.Nodes.History(a.ID()); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || history != nil {
		t.Fatalf("Nodes.History nil row = (%v, %v), want nil, ErrInvalidStoreMutation", history, err)
	}
	if node, err := g.Temporal.NodeAt(a.ID(), a.Temporal().TxFrom); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || node != nil {
		t.Fatalf("NodeAt nil history row = (%v, %v), want nil, ErrInvalidStoreMutation", node, err)
	}
	if valid, err := g.Hash.VerifyNodeChain(a.ID()); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || valid {
		t.Fatalf("VerifyNodeChain nil history row = (%v, %v), want false, ErrInvalidStoreMutation", valid, err)
	}
	tx2, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx history fault: %v", err)
	}
	if node, err := tx2.UpdateNode(a.ID(), map[string]any{"tx": "blocked"}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || node != nil {
		t.Fatalf("Tx.UpdateNode nil history row = (%v, %v), want nil, ErrInvalidStoreMutation", node, err)
	}
	if err := tx2.Rollback(); err != nil {
		t.Fatalf("Rollback history fault tx: %v", err)
	}
	fs.failNodeHist.Store(false)
	fs.nodeHistory = nil

	fs.relHistory = []*types.Relationship{nil}
	fs.failRelHist.Store(true)
	if history, err := g.Rels.History(rel.ID()); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || history != nil {
		t.Fatalf("Rels.History nil row = (%v, %v), want nil, ErrInvalidStoreMutation", history, err)
	}
	if gotRel, err := g.Temporal.RelAt(rel.ID(), rel.Temporal().TxFrom); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || gotRel != nil {
		t.Fatalf("RelAt nil history row = (%v, %v), want nil, ErrInvalidStoreMutation", gotRel, err)
	}
	if valid, err := g.Hash.VerifyRelChain(rel.ID()); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || valid {
		t.Fatalf("VerifyRelChain nil history row = (%v, %v), want false, ErrInvalidStoreMutation", valid, err)
	}
	fs.failRelHist.Store(false)
	fs.relHistory = nil

	fs.failNodeVer.Store(true)
	if node, err := g.Nodes.VersionBefore(a.ID(), 1); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || node != nil {
		t.Fatalf("Nodes.VersionBefore nil row = (%v, %v), want nil, ErrInvalidStoreMutation", node, err)
	}
	fs.failNodeVer.Store(false)

	fs.failRelVer.Store(true)
	if gotRel, err := g.Rels.VersionBefore(rel.ID(), 1); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || gotRel != nil {
		t.Fatalf("Rels.VersionBefore nil row = (%v, %v), want nil, ErrInvalidStoreMutation", gotRel, err)
	}
	fs.failRelVer.Store(false)

	pageBase := memory.New()
	pageStore := &directHistoryPageFaultStore{MandatoryStore: pageBase}
	pageGraph, err := New(Config{Store: pageStore})
	if err != nil {
		t.Fatalf("New page fault graph: %v", err)
	}
	defer pageGraph.Close()
	pageNode, err := pageGraph.Nodes.Add(context.Background(), []string{"PagedDoc"}, map[string]any{"name": "a"})
	if err != nil {
		t.Fatalf("page graph AddNode: %v", err)
	}
	pageEnd, err := pageGraph.Nodes.Add(context.Background(), []string{"PagedDoc"}, nil)
	if err != nil {
		t.Fatalf("page graph AddNode end: %v", err)
	}
	pageRel, err := pageGraph.Rels.Add(context.Background(), "PAGED_REL", pageNode, pageEnd, map[string]any{"v": int64(1)})
	if err != nil {
		t.Fatalf("page graph AddRelationship: %v", err)
	}
	if _, err := pageGraph.Nodes.Update(context.Background(), pageNode.ID(), map[string]any{"name": "b"}); err != nil {
		t.Fatalf("page graph UpdateNode: %v", err)
	}
	if _, err := pageGraph.Rels.Update(context.Background(), pageRel.ID(), map[string]any{"v": int64(2)}); err != nil {
		t.Fatalf("page graph UpdateRelationship: %v", err)
	}
	pageStore.nodePage = []*types.Node{nil}
	pageStore.failNodePage.Store(true)
	if err := pageGraph.IO.Export(&bytes.Buffer{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("Export nil node history page row = %v, want ErrInvalidStoreMutation", err)
	}
	pageStore.failNodePage.Store(false)
	pageStore.nodePage = nil

	pageStore.relPage = []*types.Relationship{nil}
	pageStore.failRelPage.Store(true)
	if err := pageGraph.IO.Export(&bytes.Buffer{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("Export nil relationship history page row = %v, want ErrInvalidStoreMutation", err)
	}
	pageStore.failRelPage.Store(false)
	pageStore.relPage = nil

	fs.nodeRows = []*types.Node{a, b}
	fs.failNode.Store(true)
	if nodes, err := g.Nodes.ByLabel("Doc", storepkg.QueryOpts{Limit: 1}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("ByLabel over-limit rows = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}
	fs.nodeRows = []*types.Node{a}
	if nodes, err := g.Nodes.ByLabel("Doc", storepkg.QueryOpts{After: types.EntityID(b.ID())}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("ByLabel before-cursor row = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}
	fs.failNode.Store(false)
	fs.nodeRows = nil

	fs.relRows = []*types.Relationship{rel, rel2}
	fs.failRel.Store(true)
	if rels, err := g.Rels.ByType("KNOWS", storepkg.QueryOpts{Limit: 1}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("ByType over-limit rows = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.relRows = []*types.Relationship{rel}
	if rels, err := g.Rels.ByType("KNOWS", storepkg.QueryOpts{After: types.EntityID(rel2.ID())}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("ByType before-cursor row = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.failRel.Store(false)
	fs.relRows = nil

	fs.allNodeRows = []*types.Node{b, a}
	fs.failAllNodes.Store(true)
	if nodes, err := g.Nodes.All(storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("AllNodes non-ascending rows = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}
	fs.failAllNodes.Store(false)
	fs.allNodeRows = nil

	fs.allRelRows = []*types.Relationship{rel2, rel}
	fs.failAllRels.Store(true)
	if rels, err := g.Rels.All(storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("AllRelationships non-ascending rows = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.failAllRels.Store(false)
	fs.allRelRows = nil

	fs.nodeIDs = []types.NodeID{0}
	fs.failNodeIDs.Store(true)
	if err := g.IO.Export(&bytes.Buffer{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("Export zero AllNodeIDs row = %v, want ErrInvalidStoreMutation", err)
	}
	fs.failNodeIDs.Store(false)
	fs.nodeIDs = nil

	fs.nodeHistIDs = []types.NodeID{0}
	fs.failNodeHIDs.Store(true)
	if err := g.IO.Export(&bytes.Buffer{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("Export zero AllNodeHistoryIDsFrom row = %v, want ErrInvalidStoreMutation", err)
	}
	fs.failNodeHIDs.Store(false)
	fs.nodeHistIDs = nil

	fs.relIDs = []types.RelID{0}
	fs.failRelIDs.Store(true)
	if err := g.IO.Export(&bytes.Buffer{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("Export zero AllRelIDs row = %v, want ErrInvalidStoreMutation", err)
	}
	fs.failRelIDs.Store(false)
	fs.relIDs = nil

	fs.relHistIDs = []types.RelID{0}
	fs.failRelHIDs.Store(true)
	if err := g.IO.Export(&bytes.Buffer{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("Export zero AllRelHistoryIDsFrom row = %v, want ErrInvalidStoreMutation", err)
	}
	fs.failRelHIDs.Store(false)
	fs.relHistIDs = nil

	fs.nodeIDs = []types.NodeID{0}
	fs.failEachNode.Store(true)
	if nodes, err := g.Temporal.NodesAsOf(a.Temporal().TxFrom); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("NodesAsOf zero ForEachNodeID row = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}
	fs.failEachNode.Store(false)
	fs.nodeIDs = nil

	fs.nodeHistIDs = []types.NodeID{0}
	fs.failEachNH.Store(true)
	if nodes, err := g.Temporal.NodesAsOf(a.Temporal().TxFrom); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("NodesAsOf zero ForEachNodeHistoryID row = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}
	fs.failEachNH.Store(false)
	fs.nodeHistIDs = nil

	fs.relIDs = []types.RelID{0}
	fs.failEachRel.Store(true)
	if rels, err := g.Temporal.RelsAsOf(rel.Temporal().TxFrom); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("RelsAsOf zero ForEachRelID row = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.failEachRel.Store(false)
	fs.relIDs = nil

	fs.relHistIDs = []types.RelID{0}
	fs.failEachRH.Store(true)
	if rels, err := g.Temporal.RelsAsOf(rel.Temporal().TxFrom); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("RelsAsOf zero ForEachRelHistoryID row = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.failEachRH.Store(false)
	fs.relHistIDs = nil

	fs.nodeCount = -1
	fs.failNodeCnt.Store(true)
	if count, err := g.Nodes.Count(); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || count != 0 {
		t.Fatalf("Nodes.Count negative store count = (%d, %v), want 0, ErrInvalidStoreMutation", count, err)
	}
	if err := g.IO.Export(&bytes.Buffer{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("Export negative node count = %v, want ErrInvalidStoreMutation", err)
	}
	g.mu.Lock()
	_, err = g.importTargetEmptyLocked()
	g.mu.Unlock()
	if !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("importTargetEmptyLocked negative node count = %v, want ErrInvalidStoreMutation", err)
	}
	fs.failNodeCnt.Store(false)
	fs.nodeCount = 0

	fs.relCount = -1
	fs.failRelCnt.Store(true)
	if count, err := g.Rels.Count(); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || count != 0 {
		t.Fatalf("Rels.Count negative store count = (%d, %v), want 0, ErrInvalidStoreMutation", count, err)
	}
	fs.nodeCount = 0
	fs.failNodeCnt.Store(true)
	g.mu.Lock()
	_, err = g.importTargetEmptyLocked()
	g.mu.Unlock()
	if !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("importTargetEmptyLocked negative relationship count = %v, want ErrInvalidStoreMutation", err)
	}
	fs.failNodeCnt.Store(false)
	fs.nodeCount = 0
	fs.failRelCnt.Store(false)
	fs.relCount = 0

	fs.labelCount = -1
	fs.failLabelCnt.Store(true)
	if count, err := g.Nodes.CountByLabel("Doc"); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || count != 0 {
		t.Fatalf("Nodes.CountByLabel negative store count = (%d, %v), want 0, ErrInvalidStoreMutation", count, err)
	}
	if _, err := g.Stats.AllLabelCounts(); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("Stats.AllLabelCounts negative store count = %v, want ErrInvalidStoreMutation", err)
	}
	fs.failLabelCnt.Store(false)
	fs.labelCount = 0

	fs.typeCount = -1
	fs.failTypeCnt.Store(true)
	if count, err := g.Rels.CountByType("KNOWS"); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || count != 0 {
		t.Fatalf("Rels.CountByType negative store count = (%d, %v), want 0, ErrInvalidStoreMutation", count, err)
	}
	if _, err := g.Stats.AllRelTypeCounts(); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("Stats.AllRelTypeCounts negative store count = %v, want ErrInvalidStoreMutation", err)
	}
	fs.failTypeCnt.Store(false)
	fs.typeCount = 0

	fs.nodeRows = []*types.Node{nil}
	fs.failNode.Store(true)
	if nodes, err := g.Nodes.ByLabel("Doc", storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("ByLabel nil row = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}
	if nodes, err := g.Nodes.ByLabel("Doc", storepkg.QueryOpts{ValidAt: a.Temporal().TxFrom}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("ByLabel temporal nil row = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}
	if nodes, err := g.Nodes.ByLabelAndProperty("Doc", "status", "draft", storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("ByLabelAndProperty fallback nil row = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}
	fs.failNode.Store(false)

	fs.relRows = []*types.Relationship{nil}
	fs.failRel.Store(true)
	if rels, err := g.Rels.ByType("KNOWS", storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("ByType nil row = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	if rels, err := g.Rels.ByType("KNOWS", storepkg.QueryOpts{ValidAt: a.Temporal().TxFrom}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("ByType temporal nil row = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	if rels, err := g.Temporal.RelsByTypePropertyAt("KNOWS", "status", "draft", a.Temporal().TxFrom); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("RelsByTypePropertyAt nil seed row = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	if rels, err := g.Temporal.RelsByTypePropertyDuring("KNOWS", "status", "draft", rel.Temporal().TxFrom, rel.Temporal().TxFrom+1); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("RelsByTypePropertyDuring nil seed row = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.failRel.Store(false)

	fs.outgoingRows = []*types.Relationship{nil}
	fs.failOutgoing.Store(true)
	if rels, err := g.Rels.Outgoing(a.ID(), ""); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("Outgoing nil row = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.outgoingRows = []*types.Relationship{rel3, rel}
	if rels, err := g.Rels.Outgoing(a.ID(), ""); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("Outgoing non-ascending rows = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.outgoingRows = []*types.Relationship{rel, rel}
	if rels, err := g.Rels.Outgoing(a.ID(), ""); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("Outgoing duplicate rows = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.outgoingRows = []*types.Relationship{nil}
	if nodes, err := g.Temporal.NeighborsAt(a.ID(), a.Temporal().TxFrom); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("NeighborsAt nil outgoing row = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}
	if got, created, err := g.Rels.AddByIDIfAbsent(context.Background(), "KNOWS", a.ID(), b.ID(), nil); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || got != nil || created {
		t.Fatalf("AddByIDIfAbsent nil duplicate-probe row = (%v, %v, %v), want nil, false, ErrInvalidStoreMutation", got, created, err)
	}
	if err := g.Nodes.Delete(context.Background(), a.ID()); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNode nil outgoing row = %v, want ErrInvalidStoreMutation", err)
	}
	fs.outgoingRows = nil
	if rels, err := g.Rels.Outgoing(missingNode, ""); !errors.Is(err, storepkg.ErrNodeNotFound) || rels != nil {
		t.Fatalf("Outgoing missing node hidden by external store = (%v, %v), want nil, ErrNodeNotFound", rels, err)
	}
	fs.failOutgoing.Store(false)

	fs.incomingRows = []*types.Relationship{nil}
	fs.failIncoming.Store(true)
	if rels, err := g.Rels.Incoming(b.ID(), ""); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("Incoming nil row = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.incomingRows = []*types.Relationship{rel3, rel}
	if rels, err := g.Rels.Incoming(b.ID(), ""); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("Incoming non-ascending rows = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.incomingRows = []*types.Relationship{rel, rel}
	if rels, err := g.Rels.Incoming(b.ID(), ""); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("Incoming duplicate rows = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.incomingRows = []*types.Relationship{nil}
	if nodes, err := g.Temporal.NeighborsAt(b.ID(), rel.Temporal().TxFrom); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("NeighborsAt nil incoming row = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}
	fs.incomingRows = nil
	if rels, err := g.Rels.Incoming(missingNode, ""); !errors.Is(err, storepkg.ErrNodeNotFound) || rels != nil {
		t.Fatalf("Incoming missing node hidden by external store = (%v, %v), want nil, ErrNodeNotFound", rels, err)
	}
	fs.failIncoming.Store(false)

	fs.outgoingMap = map[types.NodeID][]*types.Relationship{
		a.ID(): {nil},
	}
	fs.failOutMap.Store(true)
	if rels, err := g.Rels.OutgoingForNodes([]types.NodeID{a.ID()}, ""); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("OutgoingForNodes nil row = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.outgoingMap = map[types.NodeID][]*types.Relationship{a.ID(): nil}
	if rels, err := g.Rels.OutgoingForNodes([]types.NodeID{a.ID()}, ""); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("OutgoingForNodes empty entry = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.outgoingMap = map[types.NodeID][]*types.Relationship{
		a.ID(): {rel3, rel},
	}
	if rels, err := g.Rels.OutgoingForNodes([]types.NodeID{a.ID()}, ""); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("OutgoingForNodes non-ascending rows = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.outgoingMap = map[types.NodeID][]*types.Relationship{a.ID() + 999: {}}
	if rels, err := g.Rels.OutgoingForNodes([]types.NodeID{a.ID()}, ""); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("OutgoingForNodes extra key = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.outgoingMap = nil
	if rels, err := g.Rels.OutgoingForNodes([]types.NodeID{a.ID(), missingNode}, ""); !errors.Is(err, storepkg.ErrNodeNotFound) || rels != nil {
		t.Fatalf("OutgoingForNodes missing node hidden by external store = (%v, %v), want nil, ErrNodeNotFound", rels, err)
	}
	fs.failOutMap.Store(false)

	fs.incomingMap = map[types.NodeID][]*types.Relationship{
		b.ID(): {nil},
	}
	fs.failInMap.Store(true)
	if rels, err := g.Rels.IncomingForNodes([]types.NodeID{b.ID()}, ""); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("IncomingForNodes nil row = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.incomingMap = map[types.NodeID][]*types.Relationship{b.ID(): nil}
	if rels, err := g.Rels.IncomingForNodes([]types.NodeID{b.ID()}, ""); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("IncomingForNodes empty entry = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.incomingMap = map[types.NodeID][]*types.Relationship{
		b.ID(): {rel3, rel},
	}
	if rels, err := g.Rels.IncomingForNodes([]types.NodeID{b.ID()}, ""); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("IncomingForNodes non-ascending rows = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.incomingMap = map[types.NodeID][]*types.Relationship{b.ID() + 999: {}}
	if rels, err := g.Rels.IncomingForNodes([]types.NodeID{b.ID()}, ""); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("IncomingForNodes extra key = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.incomingMap = nil
	if rels, err := g.Rels.IncomingForNodes([]types.NodeID{b.ID(), missingNode}, ""); !errors.Is(err, storepkg.ErrNodeNotFound) || rels != nil {
		t.Fatalf("IncomingForNodes missing node hidden by external store = (%v, %v), want nil, ErrNodeNotFound", rels, err)
	}
	fs.failInMap.Store(false)

	fs.allNodeRows = []*types.Node{nil}
	fs.failAllNodes.Store(true)
	if nodes, err := g.Nodes.All(storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("AllNodes nil row = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}
	fs.failAllNodes.Store(false)

	fs.allRelRows = []*types.Relationship{nil}
	fs.failAllRels.Store(true)
	if rels, err := g.Rels.All(storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("AllRelationships nil row = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.failAllRels.Store(false)

	fs.getNodeRows = []*types.Node{b}
	fs.failGetNodes.Store(true)
	if nodes, err := g.Nodes.GetByIDs([]types.NodeID{a.ID()}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("GetNodesByIDs mismatched row = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}
	fs.getNodeRows = []*types.Node{b, a}
	if nodes, err := g.Nodes.GetByIDs([]types.NodeID{a.ID(), b.ID()}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("GetNodesByIDs non-ascending rows = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}
	fs.getNodeRows = []*types.Node{a, a}
	if nodes, err := g.Nodes.GetByIDs([]types.NodeID{a.ID(), a.ID()}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("GetNodesByIDs aliased duplicate rows = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}
	fs.failGetNodes.Store(false)

	fs.getRelRows = []*types.Relationship{nil}
	fs.failGetRels.Store(true)
	if rels, err := g.Rels.GetByIDs([]types.RelID{rel.ID()}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("GetRelationshipsByIDs nil row = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.getRelRows = []*types.Relationship{rel2, rel}
	if rels, err := g.Rels.GetByIDs([]types.RelID{rel.ID(), rel2.ID()}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("GetRelationshipsByIDs non-ascending rows = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
	fs.getRelRows = []*types.Relationship{rel, rel}
	if rels, err := g.Rels.GetByIDs([]types.RelID{rel.ID(), rel.ID()}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("GetRelationshipsByIDs aliased duplicate rows = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
}

func TestMandatoryBulkReadRowsCopyValidExternalRows(t *testing.T) {
	t.Parallel()
	fs := &concreteBulkReadRowFaultStore{Store: memory.New()}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if g.storeRowsTrust {
		t.Fatal("concrete external store rows must be copied defensively")
	}

	a, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	rel, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	assertNodeDetached := func(name string, got, backing *types.Node) {
		t.Helper()
		if got == nil {
			t.Fatalf("%s returned nil node", name)
		}
		if got == backing {
			t.Fatalf("%s returned external node pointer", name)
		}
		if err := got.SetProperty("probe_"+name, "mutated"); err != nil {
			t.Fatalf("%s mutate returned node: %v", name, err)
		}
		if _, ok := backing.GetProperty("probe_" + name); ok {
			t.Fatalf("%s returned node shares properties with external backing row", name)
		}
	}
	assertRelDetached := func(name string, got, backing *types.Relationship) {
		t.Helper()
		if got == nil {
			t.Fatalf("%s returned nil relationship", name)
		}
		if got == backing {
			t.Fatalf("%s returned external relationship pointer", name)
		}
		if err := got.SetProperty("probe_"+name, "mutated"); err != nil {
			t.Fatalf("%s mutate returned relationship: %v", name, err)
		}
		if _, ok := backing.GetProperty("probe_" + name); ok {
			t.Fatalf("%s returned relationship shares properties with external backing row", name)
		}
	}
	assertNodeSliceDetached := func(name string, got, backing []*types.Node) {
		t.Helper()
		if len(got) != len(backing) {
			t.Fatalf("%s len = %d, want %d", name, len(got), len(backing))
		}
		assertNodeDetached(name, got[0], backing[0])
		got[0] = nil
		if backing[0] == nil {
			t.Fatalf("%s returned node slice shares external backing array", name)
		}
	}
	assertRelSliceDetached := func(name string, got, backing []*types.Relationship) {
		t.Helper()
		if len(got) != len(backing) {
			t.Fatalf("%s len = %d, want %d", name, len(got), len(backing))
		}
		assertRelDetached(name, got[0], backing[0])
		got[0] = nil
		if backing[0] == nil {
			t.Fatalf("%s returned relationship slice shares external backing array", name)
		}
	}

	fs.getNodeRow = a
	fs.failGetNode.Store(true)
	gotNode, err := g.Nodes.Get(context.Background(), a.ID())
	if err != nil {
		t.Fatalf("Nodes.Get: %v", err)
	}
	assertNodeDetached("GetNode", gotNode, a)
	fs.failGetNode.Store(false)

	fs.nodeRows = []*types.Node{a, b}
	fs.failNode.Store(true)
	nodes, err := g.Nodes.ByLabel("Doc", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabel: %v", err)
	}
	assertNodeSliceDetached("ByLabel", nodes, fs.nodeRows)
	nodes, err = g.Nodes.ByLabelAndProperty("Doc", "status", "draft", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperty fallback: %v", err)
	}
	assertNodeSliceDetached("ByLabelAndProperty", nodes, fs.nodeRows)
	fs.failNode.Store(false)

	fs.allNodeRows = []*types.Node{a, b}
	fs.failAllNodes.Store(true)
	nodes, err = g.Nodes.All(storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("AllNodes: %v", err)
	}
	assertNodeSliceDetached("AllNodes", nodes, fs.allNodeRows)
	fs.failAllNodes.Store(false)

	fs.getNodeRows = []*types.Node{a, b}
	fs.failGetNodes.Store(true)
	nodes, err = g.Nodes.GetByIDs([]types.NodeID{a.ID(), b.ID()})
	if err != nil {
		t.Fatalf("GetNodesByIDs: %v", err)
	}
	assertNodeSliceDetached("GetNodesByIDs", nodes, fs.getNodeRows)
	fs.failGetNodes.Store(false)

	fs.getRelRow = rel
	fs.failGetRel.Store(true)
	gotRel, err := g.Rels.Get(context.Background(), rel.ID())
	if err != nil {
		t.Fatalf("Rels.Get: %v", err)
	}
	assertRelDetached("GetRelationship", gotRel, rel)
	fs.failGetRel.Store(false)

	fs.relRows = []*types.Relationship{rel}
	fs.failRel.Store(true)
	rels, err := g.Rels.ByType("KNOWS", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByType: %v", err)
	}
	assertRelSliceDetached("ByType", rels, fs.relRows)
	fs.failRel.Store(false)

	fs.outgoingRows = []*types.Relationship{rel}
	fs.failOutgoing.Store(true)
	rels, err = g.Rels.Outgoing(a.ID(), "")
	if err != nil {
		t.Fatalf("Outgoing: %v", err)
	}
	assertRelSliceDetached("Outgoing", rels, fs.outgoingRows)
	fs.failOutgoing.Store(false)

	fs.incomingRows = []*types.Relationship{rel}
	fs.failIncoming.Store(true)
	rels, err = g.Rels.Incoming(b.ID(), "")
	if err != nil {
		t.Fatalf("Incoming: %v", err)
	}
	assertRelSliceDetached("Incoming", rels, fs.incomingRows)
	fs.failIncoming.Store(false)

	fs.outgoingMap = map[types.NodeID][]*types.Relationship{a.ID(): {rel}}
	fs.failOutMap.Store(true)
	outMap, err := g.Rels.OutgoingForNodes([]types.NodeID{a.ID()}, "")
	if err != nil {
		t.Fatalf("OutgoingForNodes: %v", err)
	}
	assertRelSliceDetached("OutgoingForNodes", outMap[a.ID()], fs.outgoingMap[a.ID()])
	delete(outMap, a.ID())
	if _, ok := fs.outgoingMap[a.ID()]; !ok {
		t.Fatal("OutgoingForNodes returned map shares external backing map")
	}
	fs.failOutMap.Store(false)

	fs.incomingMap = map[types.NodeID][]*types.Relationship{b.ID(): {rel}}
	fs.failInMap.Store(true)
	inMap, err := g.Rels.IncomingForNodes([]types.NodeID{b.ID()}, "")
	if err != nil {
		t.Fatalf("IncomingForNodes: %v", err)
	}
	assertRelSliceDetached("IncomingForNodes", inMap[b.ID()], fs.incomingMap[b.ID()])
	delete(inMap, b.ID())
	if _, ok := fs.incomingMap[b.ID()]; !ok {
		t.Fatal("IncomingForNodes returned map shares external backing map")
	}
	fs.failInMap.Store(false)

	fs.allRelRows = []*types.Relationship{rel}
	fs.failAllRels.Store(true)
	rels, err = g.Rels.All(storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("AllRelationships: %v", err)
	}
	assertRelSliceDetached("AllRelationships", rels, fs.allRelRows)
	fs.failAllRels.Store(false)

	fs.getRelRows = []*types.Relationship{rel}
	fs.failGetRels.Store(true)
	rels, err = g.Rels.GetByIDs([]types.RelID{rel.ID()})
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs: %v", err)
	}
	assertRelSliceDetached("GetRelationshipsByIDs", rels, fs.getRelRows)
}
