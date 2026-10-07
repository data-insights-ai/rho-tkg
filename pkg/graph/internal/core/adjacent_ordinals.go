package core

import (
	"errors"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ForEachAdjacentEndpointOrdinal is ForEachAdjacentEndpoint with the dense
// ordinals (types.Relationship.Ordinal, types.Node.Ordinal) of each
// relationship and of its other endpoint, so a traversal that numbers nodes
// and relationships needs no Lend per edge. Memory reads them from the rows it
// holds, badger from its RAM ordinal maps (no row decoded), both under one lock
// per node's adjacency; other stores decode the relationships and lend the
// other endpoint. An ordinal is 0 where the store numbers none (tiered, badger
// with DisableOrdinals, a declared segment type's relationship) or the other
// endpoint has no current row any more. Order, isolation and errors as
// ForEachAdjacentEndpoint; fn returning false stops the scan.
func (r *RelOps) ForEachAdjacentEndpointOrdinal(nodeID types.NodeID, typeName string, incoming bool,
	fn func(rel types.RelID, relOrdinal uint32, other types.NodeID, otherOrdinal uint32) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return err
		}
	}
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return err
	}
	scanner, native := c.store.(storepkg.AdjacentEndpointOrdinalCapability)
	if !native || !c.storeRowsTrust {
		return r.forEachAdjacentEndpointOrdinalByRows(nodeID, typeName, incoming, fn)
	}
	var tok uint16
	if typeName != "" {
		var ok bool
		if err := c.readUnderRLock(func() error {
			tok, ok = c.lookupRelTypeQueryToken(typeName)
			return nil
		}); err != nil {
			return err
		}
		if !ok {
			return c.readUnderRLock(func() error {
				return c.validateRequestedNodesExist([]types.NodeID{nodeID})
			})
		}
	}
	// Deliberately outside c.mu — see ForEachByType's isolation note.
	return scanner.ForEachAdjacentEndpointOrdinal(nodeID, tok, incoming, fn)
}

// forEachAdjacentEndpointOrdinalByRows is the door for stores without the
// capability: the relationships decoded, the other endpoint lent.
func (r *RelOps) forEachAdjacentEndpointOrdinalByRows(nodeID types.NodeID, typeName string, incoming bool,
	fn func(rel types.RelID, relOrdinal uint32, other types.NodeID, otherOrdinal uint32) bool) error {
	c := r.c
	var (
		rels []*types.Relationship
		err  error
	)
	if incoming {
		rels, err = r.Incoming(nodeID, typeName)
	} else {
		rels, err = r.Outgoing(nodeID, typeName)
	}
	if err != nil {
		return err
	}
	lender, lends := c.store.(storepkg.EntityLendCapability)
	for _, rel := range rels {
		other := rel.EndNodeID()
		if incoming {
			other = rel.StartNodeID()
		}
		var otherOrd uint32
		var n *types.Node
		if lends && c.storeRowsTrust {
			n, err = lender.LendNode(other)
		} else {
			n, err = c.store.GetNode(other)
		}
		switch {
		case err == nil && n != nil:
			otherOrd = n.Ordinal()
		case err != nil && !errors.Is(err, storepkg.ErrNodeNotFound):
			return err
		}
		if !fn(rel.InternalID(), rel.Ordinal(), other, otherOrd) {
			return nil
		}
	}
	return nil
}
