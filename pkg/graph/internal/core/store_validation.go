package core

import (
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Row, page, and history validators for Store outputs. Every public Core
// query that bypasses storeRowsTrust funnels through one of these helpers so
// the trust boundary is uniform: validate before deep-copy.

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
