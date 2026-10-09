package core

import (
	"errors"
	"math"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// One version allocator for every row a write appends to an entity's chain
// (backlog 18). A bounded SetVersionInterval appends rows ABOVE the current
// row's version while the current row keeps the store's current slot, so
// "current.Version() + 1" is not a free version: an Update, CloseVersion,
// label change or property CAS that used it wrote a second row with the
// cascade row's version, and every later reader that orders a chain by
// version (the as-of door, the rollback trim, History) saw two rows with one
// version. Every appending door therefore allocates one above the HIGHEST
// version stored for the entity — current row and history — so versions are
// unique and strictly increasing in write order.
//
// The highest version is derived from the chain under the entity lock (no
// persisted counter, no store format change). Versions are allocated densely
// (each write takes max+1), so a row above the current version exists iff the
// version current+1 does: the common case costs one point read that misses;
// only an entity whose chain holds rows above the current row (a cascade that
// left the current row in place) reads its history to find the top.

// nodeHasHistoryAbove reports whether node id has a history row at version
// v+1, i.e. (versions being dense) any row above v.
func (c *Core) nodeHasHistoryAbove(id types.NodeID, v uint32) (bool, error) {
	if v == math.MaxUint32 {
		return false, nil
	}
	_, err := c.getNodeVersion(id, v+1)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, storepkg.ErrVersionNotFound):
		return false, nil
	default:
		return false, err
	}
}

// relHasHistoryAbove mirrors nodeHasHistoryAbove for relationships. A Model-A
// foreign-incoming stub's slot is foreign (ErrSlotNotLocal): it has no local
// history.
func (c *Core) relHasHistoryAbove(id types.RelID, v uint32) (bool, error) {
	if v == math.MaxUint32 {
		return false, nil
	}
	_, err := c.getRelVersion(id, v+1)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, storepkg.ErrVersionNotFound), errors.Is(err, storepkg.ErrSlotNotLocal):
		return false, nil
	default:
		return false, err
	}
}

// nextNodeVersion returns the version of the next row appended to node id,
// whose current row is current: one above the highest version on the chain.
// Caller holds the entity lock.
func (c *Core) nextNodeVersion(id types.NodeID, current *types.Node) (uint32, error) {
	top := current.Version()
	above, err := c.nodeHasHistoryAbove(id, top)
	if err != nil {
		return 0, err
	}
	if above {
		history, err := c.getNodeHistory(id)
		if err != nil {
			return 0, err
		}
		for _, h := range history {
			top = max(top, h.Version())
		}
	}
	return nextEntityVersion(top)
}

// nextRelVersion mirrors nextNodeVersion for relationships.
func (c *Core) nextRelVersion(id types.RelID, current *types.Relationship) (uint32, error) {
	top := current.Version()
	above, err := c.relHasHistoryAbove(id, top)
	if err != nil {
		return 0, err
	}
	if above {
		history, err := c.getRelHistory(id)
		if err != nil {
			return 0, err
		}
		for _, h := range history {
			top = max(top, h.Version())
		}
	}
	return nextEntityVersion(top)
}
