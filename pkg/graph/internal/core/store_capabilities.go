package core

import (
	"fmt"

	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Capability accessors — type-assert the store field against an optional
// capability and surface ErrCapabilityNotSupported (with a diagnostic
// message) when the underlying backend does not implement it.
//
// Core's `store` field is typed as MandatoryStore so out-of-tree backends
// that satisfy only the mandatory capabilities can still be plugged in;
// their consumers must accept ErrCapabilityNotSupported on the optional
// surfaces. In-tree backends (memory.Store, badger.Store, tiered.Store)
// satisfy every capability, so these assertions always succeed in the
// reference configuration.

func (c *Core) propertyIndexCap() (storepkg.PropertyIndexCapability, error) {
	cap, ok := c.store.(storepkg.PropertyIndexCapability)
	if !ok {
		return nil, fmt.Errorf("%w: PropertyIndexCapability", storepkg.ErrCapabilityNotSupported)
	}
	return cap, nil
}

func (c *Core) temporalIndexCap() (storepkg.TemporalIndexCapability, error) {
	cap, ok := c.store.(storepkg.TemporalIndexCapability)
	if !ok {
		return nil, fmt.Errorf("%w: TemporalIndexCapability", storepkg.ErrCapabilityNotSupported)
	}
	return cap, nil
}

func (c *Core) vectorIndexCap() (storepkg.VectorIndexCapability, error) {
	cap, ok := c.store.(storepkg.VectorIndexCapability)
	if !ok {
		return nil, fmt.Errorf("%w: VectorIndexCapability", storepkg.ErrCapabilityNotSupported)
	}
	return cap, nil
}

func (c *Core) highFrequencyIndexCap() (storepkg.HighFrequencyIndexCapability, error) {
	cap, ok := c.store.(storepkg.HighFrequencyIndexCapability)
	if !ok {
		return nil, fmt.Errorf("%w: HighFrequencyIndexCapability", storepkg.ErrCapabilityNotSupported)
	}
	return cap, nil
}

// nodesByLabelAndProperty answers the (label-token, property-key, value)
// query whether or not the underlying store implements the accelerated
// property-query capability. Exact in-tree stores and direct external
// implementations use NodesByLabelAndProperty. Concrete wrappers that merely
// inherit an in-tree method intentionally fall back to a label scan so wrapper
// NodesByLabel overrides remain visible. Every in-tree backend already applies
// the same scan-and-filter internally when no property index covers
// (label, key); replicating it here ensures out-of-tree mandatory-only
// backends still get the correct semantics — the optional capability is index
// management/acceleration, not query correctness.
//
// Empty-key contract (R4-F9): `indexpkg.PropertyValueKey` returns "" for
// values it cannot canonicalise (slices, maps, nested structs without a
// custom `HashableValue` etc.). The in-tree backends (and the property
// index itself) treat such queries as "no matches"; the graph-layer
// fallback must do the same — otherwise every candidate whose stored
// value is also unindexable canonicalises to "" and matches falsely.
func (c *Core) nodesByLabelAndProperty(tok uint16, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	wantKey := indexpkg.PropertyValueKey(value)
	if wantKey == "" {
		// The query value itself is not canonicalisable — by contract,
		// no matches. Mirrors memory.Store / badger.Store internal
		// guards before their fallback scans.
		return nil, nil
	}
	if c.propertyQuery != nil {
		nodes, err := c.propertyQuery.NodesByLabelAndProperty(tok, key, value, opts)
		if err != nil {
			return nil, err
		}
		if !c.propertyQueryTrust {
			if err := validateNodesByLabelAndProperty(tok, key, wantKey, opts, nodes); err != nil {
				return nil, err
			}
			nodes = copyNodeRows(nodes)
		}
		return nodes, nil
	}
	// Fallback: label scan + property filter. Pagination is applied
	// after filtering since the property predicate can drop arbitrary
	// elements; pre-filter Limit would over-count. The cursor can still
	// be pushed into the label scan because filtering preserves ID order.
	pageOpts := opts
	pageOpts.Limit = 0
	candidates, err := c.store.NodesByLabel(tok, pageOpts)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := c.validateNodesByLabelPage(tok, pageOpts, candidates); err != nil {
			return nil, err
		}
	}
	out := make([]*types.Node, 0, len(candidates))
	for _, n := range candidates {
		gotKey, found := n.IndexablePropertyValueKey(key)
		if !found || gotKey == "" {
			// Stored value is also unindexable; refuse to claim
			// equality through the empty sentinel.
			continue
		}
		if gotKey != wantKey {
			continue
		}
		out = append(out, n)
	}
	out = storeutil.PaginateNodes(out, opts.After, opts.Limit)
	if !c.storeRowsTrust {
		out = copyNodeRows(out)
	}
	return out, nil
}

func validateNodesByLabelAndProperty(labelToken uint16, key, wantKey string, opts storepkg.QueryOpts, nodes []*types.Node) error {
	for _, n := range nodes {
		if err := validateNodeByLabelRow(labelToken, n); err != nil {
			return err
		}
		gotKey, found := n.IndexablePropertyValueKey(key)
		if !found || gotKey != wantKey {
			return fmt.Errorf("%w: property query node %d does not match property %q", storepkg.ErrInvalidStoreMutation, n.ID(), key)
		}
	}
	return validateNodeRowPage(nodes, opts, "NodesByLabelAndProperty")
}

func validateNodeByLabelRow(labelToken uint16, n *types.Node) error {
	if err := storepkg.ValidateNodeWrite(n); err != nil {
		return err
	}
	if !n.HasLabelTokenRaw(labelToken) {
		return fmt.Errorf("%w: node %d missing label token %d", storepkg.ErrInvalidStoreMutation, n.ID(), labelToken)
	}
	return nil
}

func validateRelationshipByTypeRow(typeToken uint16, r *types.Relationship) error {
	if err := storepkg.ValidateRelationshipWrite(r); err != nil {
		return err
	}
	if !r.HasTypeTokenRaw(typeToken) {
		return fmt.Errorf("%w: relationship %d missing type token %d", storepkg.ErrInvalidStoreMutation, r.ID(), typeToken)
	}
	return nil
}

func validateOutgoingRelationshipRow(nodeID types.NodeID, typeToken uint16, r *types.Relationship) error {
	if err := storepkg.ValidateRelationshipWrite(r); err != nil {
		return err
	}
	if r.StartNodeID() != nodeID {
		return fmt.Errorf("%w: outgoing relationship %d has start node %d, want %d", storepkg.ErrInvalidStoreMutation, r.ID(), r.StartNodeID(), nodeID)
	}
	if typeToken != 0 && !r.HasTypeTokenRaw(typeToken) {
		return fmt.Errorf("%w: outgoing relationship %d missing type token %d", storepkg.ErrInvalidStoreMutation, r.ID(), typeToken)
	}
	return nil
}

func validateIncomingRelationshipRow(nodeID types.NodeID, typeToken uint16, r *types.Relationship) error {
	if err := storepkg.ValidateRelationshipWrite(r); err != nil {
		return err
	}
	if r.EndNodeID() != nodeID {
		return fmt.Errorf("%w: incoming relationship %d has end node %d, want %d", storepkg.ErrInvalidStoreMutation, r.ID(), r.EndNodeID(), nodeID)
	}
	if typeToken != 0 && !r.HasTypeTokenRaw(typeToken) {
		return fmt.Errorf("%w: incoming relationship %d missing type token %d", storepkg.ErrInvalidStoreMutation, r.ID(), typeToken)
	}
	return nil
}

func (c *Core) nodeIDsFromLabelRows(labelToken uint16, nodes []*types.Node) ([]types.NodeID, error) {
	ids := make([]types.NodeID, 0, len(nodes))
	if c.storeRowsTrust {
		for _, n := range nodes {
			ids = append(ids, n.ID())
		}
		return ids, nil
	}
	for _, n := range nodes {
		if err := validateNodeByLabelRow(labelToken, n); err != nil {
			return nil, err
		}
		ids = append(ids, n.ID())
	}
	return ids, nil
}

func (c *Core) validateNodesByLabelRows(labelToken uint16, nodes []*types.Node) error {
	for _, n := range nodes {
		if err := validateNodeByLabelRow(labelToken, n); err != nil {
			return err
		}
	}
	return nil
}

func (c *Core) validateNodesByLabelPage(labelToken uint16, opts storepkg.QueryOpts, nodes []*types.Node) error {
	if err := c.validateNodesByLabelRows(labelToken, nodes); err != nil {
		return err
	}
	return validateNodeRowPage(nodes, opts, "NodesByLabel")
}

func (c *Core) relIDsFromTypeRows(typeToken uint16, rels []*types.Relationship) ([]types.RelID, error) {
	ids := make([]types.RelID, 0, len(rels))
	if c.storeRowsTrust {
		for _, r := range rels {
			ids = append(ids, r.ID())
		}
		return ids, nil
	}
	for _, r := range rels {
		if err := validateRelationshipByTypeRow(typeToken, r); err != nil {
			return nil, err
		}
		ids = append(ids, r.ID())
	}
	return ids, nil
}

func (c *Core) validateRelationshipsByTypeRows(typeToken uint16, rels []*types.Relationship) error {
	for _, r := range rels {
		if err := validateRelationshipByTypeRow(typeToken, r); err != nil {
			return err
		}
	}
	return nil
}

func (c *Core) validateRelationshipsByTypePage(typeToken uint16, opts storepkg.QueryOpts, rels []*types.Relationship) error {
	if err := c.validateRelationshipsByTypeRows(typeToken, rels); err != nil {
		return err
	}
	return validateRelRowPage(rels, opts, "RelationshipsByType")
}

func (c *Core) outgoingRelIDsFromRows(nodeID types.NodeID, typeToken uint16, rels []*types.Relationship) ([]types.RelID, error) {
	ids := make([]types.RelID, 0, len(rels))
	if c.storeRowsTrust {
		for _, r := range rels {
			ids = append(ids, r.ID())
		}
		return ids, nil
	}
	for _, r := range rels {
		if err := validateOutgoingRelationshipRow(nodeID, typeToken, r); err != nil {
			return nil, err
		}
		ids = append(ids, r.ID())
	}
	if err := validateRelationshipRowsAscending("OutgoingRelationships", rels); err != nil {
		return nil, err
	}
	return ids, nil
}

func (c *Core) incomingRelIDsFromRows(nodeID types.NodeID, typeToken uint16, rels []*types.Relationship) ([]types.RelID, error) {
	ids := make([]types.RelID, 0, len(rels))
	if c.storeRowsTrust {
		for _, r := range rels {
			ids = append(ids, r.ID())
		}
		return ids, nil
	}
	for _, r := range rels {
		if err := validateIncomingRelationshipRow(nodeID, typeToken, r); err != nil {
			return nil, err
		}
		ids = append(ids, r.ID())
	}
	if err := validateRelationshipRowsAscending("IncomingRelationships", rels); err != nil {
		return nil, err
	}
	return ids, nil
}

func (c *Core) validateOutgoingRelationshipRows(nodeID types.NodeID, typeToken uint16, rels []*types.Relationship) error {
	for _, r := range rels {
		if err := validateOutgoingRelationshipRow(nodeID, typeToken, r); err != nil {
			return err
		}
	}
	return validateRelationshipRowsAscending("OutgoingRelationships", rels)
}

func (c *Core) validateIncomingRelationshipRows(nodeID types.NodeID, typeToken uint16, rels []*types.Relationship) error {
	for _, r := range rels {
		if err := validateIncomingRelationshipRow(nodeID, typeToken, r); err != nil {
			return err
		}
	}
	return validateRelationshipRowsAscending("IncomingRelationships", rels)
}

func validateRelationshipRowsAscending(source string, rels []*types.Relationship) error {
	var prev types.RelID
	for i, r := range rels {
		id := r.ID()
		if i > 0 && id <= prev {
			return fmt.Errorf("%w: %s returned non-ascending relationship %d after %d", storepkg.ErrInvalidStoreMutation, source, id, prev)
		}
		prev = id
	}
	return nil
}

func (c *Core) validateOutgoingRelationshipMap(nodeIDs []types.NodeID, typeToken uint16, rows map[types.NodeID][]*types.Relationship) error {
	requested := make(map[types.NodeID]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		requested[id] = struct{}{}
	}
	for nodeID, rels := range rows {
		if _, ok := requested[nodeID]; !ok {
			return fmt.Errorf("%w: outgoing relationship map contains unrequested node %d", storepkg.ErrInvalidStoreMutation, nodeID)
		}
		if len(rels) == 0 {
			return fmt.Errorf("%w: outgoing relationship map contains empty entry for node %d", storepkg.ErrInvalidStoreMutation, nodeID)
		}
		if err := c.validateOutgoingRelationshipRows(nodeID, typeToken, rels); err != nil {
			return err
		}
	}
	return nil
}

func copyNodeRows(rows []*types.Node) []*types.Node {
	if rows == nil {
		return nil
	}
	out := make([]*types.Node, len(rows))
	for i, n := range rows {
		out[i] = n.DeepCopy()
	}
	return out
}

func copyRelationshipRows(rows []*types.Relationship) []*types.Relationship {
	if rows == nil {
		return nil
	}
	out := make([]*types.Relationship, len(rows))
	for i, r := range rows {
		out[i] = r.DeepCopy()
	}
	return out
}

func copyRelationshipRowMap(rows map[types.NodeID][]*types.Relationship) map[types.NodeID][]*types.Relationship {
	if rows == nil {
		return nil
	}
	out := make(map[types.NodeID][]*types.Relationship, len(rows))
	for nodeID, rels := range rows {
		out[nodeID] = copyRelationshipRows(rels)
	}
	return out
}

func (c *Core) validateIncomingRelationshipMap(nodeIDs []types.NodeID, typeToken uint16, rows map[types.NodeID][]*types.Relationship) error {
	requested := make(map[types.NodeID]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		requested[id] = struct{}{}
	}
	for nodeID, rels := range rows {
		if _, ok := requested[nodeID]; !ok {
			return fmt.Errorf("%w: incoming relationship map contains unrequested node %d", storepkg.ErrInvalidStoreMutation, nodeID)
		}
		if len(rels) == 0 {
			return fmt.Errorf("%w: incoming relationship map contains empty entry for node %d", storepkg.ErrInvalidStoreMutation, nodeID)
		}
		if err := c.validateIncomingRelationshipRows(nodeID, typeToken, rels); err != nil {
			return err
		}
	}
	return nil
}

func (c *Core) validateRequestedNodesExist(nodeIDs []types.NodeID) error {
	seen := make(map[types.NodeID]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if _, err := c.getCurrentNode(id); err != nil {
			return err
		}
	}
	return nil
}

func validateAllNodeRows(nodes []*types.Node) error {
	for _, n := range nodes {
		if err := storepkg.ValidateNodeWrite(n); err != nil {
			return err
		}
	}
	return nil
}

func validateAllNodePage(opts storepkg.QueryOpts, nodes []*types.Node) error {
	if err := validateAllNodeRows(nodes); err != nil {
		return err
	}
	return validateNodeRowPage(nodes, opts, "AllNodes")
}

func validateCurrentNodeRow(id types.NodeID, n *types.Node) error {
	if err := storepkg.ValidateNodeSnapshotKey(id, n); err != nil {
		return err
	}
	return storepkg.ValidateNodeWrite(n)
}

func validateCurrentRelationshipRow(id types.RelID, r *types.Relationship) error {
	if err := storepkg.ValidateRelSnapshotKey(id, r); err != nil {
		return err
	}
	return storepkg.ValidateRelationshipWrite(r)
}

func validateNodeHistoryRows(id types.NodeID, history []*types.Node) error {
	var prevVersion uint32
	for i, n := range history {
		if err := storepkg.ValidateNodeHistorySnapshot(id, n); err != nil {
			return err
		}
		version := n.Version()
		if i > 0 && version <= prevVersion {
			return fmt.Errorf("%w: node history %d returned non-ascending version %d after %d", storepkg.ErrInvalidStoreMutation, id, version, prevVersion)
		}
		prevVersion = version
	}
	return nil
}

func validateRelationshipHistoryRows(id types.RelID, history []*types.Relationship) error {
	var prevVersion uint32
	for i, r := range history {
		if err := storepkg.ValidateRelationshipHistorySnapshot(id, r); err != nil {
			return err
		}
		version := r.Version()
		if i > 0 && version <= prevVersion {
			return fmt.Errorf("%w: relationship history %d returned non-ascending version %d after %d", storepkg.ErrInvalidStoreMutation, id, version, prevVersion)
		}
		prevVersion = version
	}
	return nil
}

func validateNodeHistoryPageRows(id types.NodeID, startVersion uint32, limit int, history []*types.Node) error {
	if limit < 0 {
		return storepkg.ErrInvalidQueryLimit
	}
	if limit > 0 && len(history) > limit {
		return fmt.Errorf("%w: node history %d returned %d rows for limit %d", storepkg.ErrInvalidStoreMutation, id, len(history), limit)
	}
	if err := validateNodeHistoryRows(id, history); err != nil {
		return err
	}
	for _, n := range history {
		if n.Version() < startVersion {
			return fmt.Errorf("%w: node history %d returned version %d before start %d", storepkg.ErrInvalidStoreMutation, id, n.Version(), startVersion)
		}
	}
	return nil
}

func validateRelationshipHistoryPageRows(id types.RelID, startVersion uint32, limit int, history []*types.Relationship) error {
	if limit < 0 {
		return storepkg.ErrInvalidQueryLimit
	}
	if limit > 0 && len(history) > limit {
		return fmt.Errorf("%w: relationship history %d returned %d rows for limit %d", storepkg.ErrInvalidStoreMutation, id, len(history), limit)
	}
	if err := validateRelationshipHistoryRows(id, history); err != nil {
		return err
	}
	for _, r := range history {
		if r.Version() < startVersion {
			return fmt.Errorf("%w: relationship history %d returned version %d before start %d", storepkg.ErrInvalidStoreMutation, id, r.Version(), startVersion)
		}
	}
	return nil
}

func (c *Core) getCurrentNode(id types.NodeID) (*types.Node, error) {
	n, err := c.store.GetNode(id)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := validateCurrentNodeRow(id, n); err != nil {
			return nil, err
		}
		n = n.DeepCopy()
	}
	return n, nil
}

func (c *Core) getCurrentRelationship(id types.RelID) (*types.Relationship, error) {
	r, err := c.store.GetRelationship(id)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := validateCurrentRelationshipRow(id, r); err != nil {
			return nil, err
		}
		r = r.DeepCopy()
	}
	return r, nil
}

func (c *Core) getNodeHistory(id types.NodeID) ([]*types.Node, error) {
	history, err := c.store.GetNodeHistory(id)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := validateNodeHistoryRows(id, history); err != nil {
			return nil, err
		}
		history = copyNodeRows(history)
	}
	return history, nil
}

func (c *Core) getRelHistory(id types.RelID) ([]*types.Relationship, error) {
	history, err := c.store.GetRelHistory(id)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := validateRelationshipHistoryRows(id, history); err != nil {
			return nil, err
		}
		history = copyRelationshipRows(history)
	}
	return history, nil
}

func (c *Core) getNodeVersion(id types.NodeID, version uint32) (*types.Node, error) {
	n, err := c.store.GetNodeVersion(id, version)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := storepkg.ValidateNodeHistoryVersionSnapshot(id, version, n); err != nil {
			return nil, err
		}
		n = n.DeepCopy()
	}
	return n, nil
}

func (c *Core) getRelVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	r, err := c.store.GetRelVersion(id, version)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := storepkg.ValidateRelationshipHistoryVersionSnapshot(id, version, r); err != nil {
			return nil, err
		}
		r = r.DeepCopy()
	}
	return r, nil
}

func (c *Core) nodeHistoryVersionsFrom(pager storepkg.HistoryVersionPageCapability, id types.NodeID, startVersion uint32, limit int) ([]*types.Node, error) {
	history, err := pager.NodeHistoryVersionsFrom(id, startVersion, limit)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := validateNodeHistoryPageRows(id, startVersion, limit, history); err != nil {
			return nil, err
		}
		history = copyNodeRows(history)
	}
	return history, nil
}

func (c *Core) relHistoryVersionsFrom(pager storepkg.HistoryVersionPageCapability, id types.RelID, startVersion uint32, limit int) ([]*types.Relationship, error) {
	history, err := pager.RelHistoryVersionsFrom(id, startVersion, limit)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := validateRelationshipHistoryPageRows(id, startVersion, limit, history); err != nil {
			return nil, err
		}
		history = copyRelationshipRows(history)
	}
	return history, nil
}

func validateNodeIDPage(ids []types.NodeID, after types.EntityID, limit int, source string) error {
	if limit < 0 {
		return storepkg.ErrInvalidQueryLimit
	}
	if limit > 0 && len(ids) > limit {
		return fmt.Errorf("%w: %s returned %d node IDs for limit %d", storepkg.ErrInvalidStoreMutation, source, len(ids), limit)
	}
	var prev types.NodeID
	for i, id := range ids {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return err
		}
		if types.EntityID(id.SnowflakeID()) <= after {
			return fmt.Errorf("%w: %s returned node ID %d not after cursor %d", storepkg.ErrInvalidStoreMutation, source, id, after)
		}
		if i > 0 && id <= prev {
			return fmt.Errorf("%w: %s returned non-ascending node ID %d after %d", storepkg.ErrInvalidStoreMutation, source, id, prev)
		}
		prev = id
	}
	return nil
}

func validateRelIDPage(ids []types.RelID, after types.EntityID, limit int, source string) error {
	if limit < 0 {
		return storepkg.ErrInvalidQueryLimit
	}
	if limit > 0 && len(ids) > limit {
		return fmt.Errorf("%w: %s returned %d relationship IDs for limit %d", storepkg.ErrInvalidStoreMutation, source, len(ids), limit)
	}
	var prev types.RelID
	for i, id := range ids {
		if err := storepkg.ValidateRelID(id); err != nil {
			return err
		}
		if types.EntityID(id.SnowflakeID()) <= after {
			return fmt.Errorf("%w: %s returned relationship ID %d not after cursor %d", storepkg.ErrInvalidStoreMutation, source, id, after)
		}
		if i > 0 && id <= prev {
			return fmt.Errorf("%w: %s returned non-ascending relationship ID %d after %d", storepkg.ErrInvalidStoreMutation, source, id, prev)
		}
		prev = id
	}
	return nil
}

func validateStoreCount(source string, count int) error {
	if count < 0 {
		return fmt.Errorf("%w: %s returned negative count %d", storepkg.ErrInvalidStoreMutation, source, count)
	}
	return nil
}

func (c *Core) nodeCount() (int, error) {
	count, err := c.store.NodeCount()
	if err != nil {
		return 0, err
	}
	if err := validateStoreCount("NodeCount", count); err != nil {
		return 0, err
	}
	return count, nil
}

func (c *Core) relCount() (int, error) {
	count, err := c.store.RelationshipCount()
	if err != nil {
		return 0, err
	}
	if err := validateStoreCount("RelationshipCount", count); err != nil {
		return 0, err
	}
	return count, nil
}

func (c *Core) nodeCountByLabel(tok uint16) (int, error) {
	count, err := c.store.NodeCountByLabel(tok)
	if err != nil {
		return 0, err
	}
	if err := validateStoreCount("NodeCountByLabel", count); err != nil {
		return 0, err
	}
	return count, nil
}

func (c *Core) relCountByType(tok uint16) (int, error) {
	count, err := c.store.RelCountByType(tok)
	if err != nil {
		return 0, err
	}
	if err := validateStoreCount("RelCountByType", count); err != nil {
		return 0, err
	}
	return count, nil
}

func (c *Core) allNodeIDs(opts storepkg.QueryOpts) ([]types.NodeID, error) {
	ids, err := c.store.AllNodeIDs(opts)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := validateNodeIDPage(ids, opts.After, opts.Limit, "AllNodeIDs"); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

func (c *Core) allRelIDs(opts storepkg.QueryOpts) ([]types.RelID, error) {
	ids, err := c.store.AllRelIDs(opts)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := validateRelIDPage(ids, opts.After, opts.Limit, "AllRelIDs"); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

func (c *Core) allNodeHistoryIDsFrom(after types.NodeID, limit int) ([]types.NodeID, error) {
	ids, err := c.store.AllNodeHistoryIDsFrom(after, limit)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := validateNodeIDPage(ids, types.EntityID(after.SnowflakeID()), limit, "AllNodeHistoryIDsFrom"); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

func (c *Core) allRelHistoryIDsFrom(after types.RelID, limit int) ([]types.RelID, error) {
	ids, err := c.store.AllRelHistoryIDsFrom(after, limit)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := validateRelIDPage(ids, types.EntityID(after.SnowflakeID()), limit, "AllRelHistoryIDsFrom"); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

func (c *Core) forEachNodeID(fn func(types.NodeID) bool) error {
	if c.storeRowsTrust {
		return c.store.ForEachNodeID(fn)
	}
	var invalid error
	err := c.store.ForEachNodeID(func(id types.NodeID) bool {
		if vErr := storepkg.ValidateNodeID(id); vErr != nil {
			invalid = vErr
			return false
		}
		return fn(id)
	})
	if invalid != nil {
		return invalid
	}
	return err
}

func (c *Core) forEachRelID(fn func(types.RelID) bool) error {
	if c.storeRowsTrust {
		return c.store.ForEachRelID(fn)
	}
	var invalid error
	err := c.store.ForEachRelID(func(id types.RelID) bool {
		if vErr := storepkg.ValidateRelID(id); vErr != nil {
			invalid = vErr
			return false
		}
		return fn(id)
	})
	if invalid != nil {
		return invalid
	}
	return err
}

func validateAllRelationshipRows(rels []*types.Relationship) error {
	for _, r := range rels {
		if err := storepkg.ValidateRelationshipWrite(r); err != nil {
			return err
		}
	}
	return nil
}

func validateAllRelationshipPage(opts storepkg.QueryOpts, rels []*types.Relationship) error {
	if err := validateAllRelationshipRows(rels); err != nil {
		return err
	}
	return validateRelRowPage(rels, opts, "AllRelationships")
}

func validateNodeRowPage(nodes []*types.Node, opts storepkg.QueryOpts, source string) error {
	if opts.Limit > 0 && len(nodes) > opts.Limit {
		return fmt.Errorf("%w: %s returned %d nodes for limit %d", storepkg.ErrInvalidStoreMutation, source, len(nodes), opts.Limit)
	}
	var prev types.NodeID
	for i, n := range nodes {
		id := n.ID()
		if types.EntityID(id.SnowflakeID()) <= opts.After {
			return fmt.Errorf("%w: %s returned node %d not after cursor %d", storepkg.ErrInvalidStoreMutation, source, id, opts.After)
		}
		if i > 0 && id <= prev {
			return fmt.Errorf("%w: %s returned non-ascending node %d after %d", storepkg.ErrInvalidStoreMutation, source, id, prev)
		}
		prev = id
	}
	return nil
}

func validateRelRowPage(rels []*types.Relationship, opts storepkg.QueryOpts, source string) error {
	if opts.Limit > 0 && len(rels) > opts.Limit {
		return fmt.Errorf("%w: %s returned %d relationships for limit %d", storepkg.ErrInvalidStoreMutation, source, len(rels), opts.Limit)
	}
	var prev types.RelID
	for i, r := range rels {
		id := r.ID()
		if types.EntityID(id.SnowflakeID()) <= opts.After {
			return fmt.Errorf("%w: %s returned relationship %d not after cursor %d", storepkg.ErrInvalidStoreMutation, source, id, opts.After)
		}
		if i > 0 && id <= prev {
			return fmt.Errorf("%w: %s returned non-ascending relationship %d after %d", storepkg.ErrInvalidStoreMutation, source, id, prev)
		}
		prev = id
	}
	return nil
}

func validateNodesByIDRows(ids []types.NodeID, nodes []*types.Node) error {
	remaining := make(map[types.NodeID]int, len(ids))
	for _, id := range ids {
		remaining[id]++
	}
	seenRows := make(map[*types.Node]struct{}, len(nodes))
	var prev types.NodeID
	for _, n := range nodes {
		if err := storepkg.ValidateNodeWrite(n); err != nil {
			return err
		}
		if _, ok := seenRows[n]; ok {
			return fmt.Errorf("%w: GetNodesByIDs returned aliased node pointer for %d", storepkg.ErrInvalidStoreMutation, n.ID())
		}
		seenRows[n] = struct{}{}
		id := n.ID()
		if prev != 0 && id < prev {
			return fmt.Errorf("%w: GetNodesByIDs returned non-ascending node %d after %d", storepkg.ErrInvalidStoreMutation, id, prev)
		}
		prev = id
		if remaining[id] == 0 {
			return fmt.Errorf("%w: GetNodesByIDs returned unexpected node %d", storepkg.ErrInvalidStoreMutation, id)
		}
		remaining[id]--
	}
	for id, count := range remaining {
		if count != 0 {
			return fmt.Errorf("%w: GetNodesByIDs missing node %d", storepkg.ErrInvalidStoreMutation, id)
		}
	}
	return nil
}

func validateRelationshipsByIDRows(ids []types.RelID, rels []*types.Relationship) error {
	remaining := make(map[types.RelID]int, len(ids))
	for _, id := range ids {
		remaining[id]++
	}
	seenRows := make(map[*types.Relationship]struct{}, len(rels))
	var prev types.RelID
	for _, r := range rels {
		if err := storepkg.ValidateRelationshipWrite(r); err != nil {
			return err
		}
		if _, ok := seenRows[r]; ok {
			return fmt.Errorf("%w: GetRelationshipsByIDs returned aliased relationship pointer for %d", storepkg.ErrInvalidStoreMutation, r.ID())
		}
		seenRows[r] = struct{}{}
		id := r.ID()
		if prev != 0 && id < prev {
			return fmt.Errorf("%w: GetRelationshipsByIDs returned non-ascending relationship %d after %d", storepkg.ErrInvalidStoreMutation, id, prev)
		}
		prev = id
		if remaining[id] == 0 {
			return fmt.Errorf("%w: GetRelationshipsByIDs returned unexpected relationship %d", storepkg.ErrInvalidStoreMutation, id)
		}
		remaining[id]--
	}
	for id, count := range remaining {
		if count != 0 {
			return fmt.Errorf("%w: GetRelationshipsByIDs missing relationship %d", storepkg.ErrInvalidStoreMutation, id)
		}
	}
	return nil
}
