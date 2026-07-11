package core

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Trust-aware fetch helpers: every Store read used by Core flows through one
// of these methods so storeRowsTrust toggles validation+deep-copy uniformly.
// Trusted backends return Store rows directly; untrusted backends validate
// first and then deep-copy before returning.

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

// relIDsFromNodeMapRows extracts relationship IDs from a per-node adjacency
// map (OutgoingRelationshipsForNodes / IncomingRelationshipsForNodes), the
// batched counterpart of outgoingRelIDsFromRows / incomingRelIDsFromRows.
// Trust-aware: an untrusted backend is validated per-node (same map
// validators the non-temporal OutgoingForNodes/IncomingForNodes doors use)
// before IDs are extracted; the extracted IDs are seeds for a temporal
// re-resolution, so no deep copy is needed here.
func (c *Core) relIDsFromNodeMapRows(nodeIDs []types.NodeID, typeToken uint16, rows map[types.NodeID][]*types.Relationship, outgoing bool) ([]types.RelID, error) {
	if !c.storeRowsTrust {
		var err error
		if outgoing {
			err = c.validateOutgoingRelationshipMap(nodeIDs, typeToken, rows)
		} else {
			err = c.validateIncomingRelationshipMap(nodeIDs, typeToken, rows)
		}
		if err != nil {
			return nil, err
		}
	}
	ids := make([]types.RelID, 0, len(rows))
	for _, rels := range rows {
		for _, r := range rels {
			ids = append(ids, r.ID())
		}
	}
	return ids, nil
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

func (c *Core) nodeCountByLabelAndPropertyKey(tok uint16, propertyKey string) (int, error) {
	stats, ok := c.store.(storepkg.NodePropertyKeyStatsCapability)
	if !ok {
		return 0, storepkg.ErrCapabilityNotSupported
	}
	count, err := stats.NodeCountByLabelAndPropertyKey(tok, propertyKey)
	if err != nil {
		return 0, err
	}
	if err := validateStoreCount("NodeCountByLabelAndPropertyKey", count); err != nil {
		return 0, err
	}
	return count, nil
}

func (c *Core) nodePropertyStats(tok uint16, propertyKey string) (storepkg.PropertyStats, error) {
	stats, ok := c.store.(storepkg.NodePropertyStatsCapability)
	if !ok {
		return storepkg.PropertyStats{}, storepkg.ErrCapabilityNotSupported
	}
	return stats.NodePropertyStats(tok, propertyKey)
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
