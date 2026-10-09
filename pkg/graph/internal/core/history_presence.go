package core

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// HasHistory reports whether the node has at least one history row: true for
// a node that was updated, relabelled, cascaded, closed or deleted (and whose
// history the store still holds), false for a node with only its current row
// or an unknown ID. It equals len(History(id)) > 0 at every moment without
// reading the rows (store.HistoryPresenceCapability: memory, badger, sharded,
// tiered); a store without the capability is answered from History.
// Errors: ErrGraphClosed, an invalid ID (ErrInvalidStoreMutation), a store error.
func (n *NodeOps) HasHistory(id types.NodeID) (bool, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return false, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return false, err
	}
	var has bool
	err := c.readUnderRLock(func() error {
		if p, ok := c.store.(storepkg.HistoryPresenceCapability); ok {
			var err error
			has, err = p.HasNodeHistory(id)
			return err
		}
		history, err := c.getNodeHistory(id)
		has = len(history) > 0
		return err
	})
	if err != nil {
		return false, err
	}
	return has, nil
}

// HasHistory is NodeOps.HasHistory for relationships.
func (r *RelOps) HasHistory(id types.RelID) (bool, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return false, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return false, err
	}
	var has bool
	err := c.readUnderRLock(func() error {
		if p, ok := c.store.(storepkg.HistoryPresenceCapability); ok {
			var err error
			has, err = p.HasRelHistory(id)
			return err
		}
		history, err := c.getRelHistory(id)
		has = len(history) > 0
		return err
	})
	if err != nil {
		return false, err
	}
	return has, nil
}
