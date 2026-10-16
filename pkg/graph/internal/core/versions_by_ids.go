package core

import (
	"errors"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Every version of a set of entities recorded at a pin (sigma-tkgd round 10
// request 6): for each ID the rows the any-version equality reads
// (eachNodeChainInterval over [types.UnboundedValidFrom, MaxInt64) at the
// pin: every valid-time version, those of an entity deleted before the pin
// included, retracted lives dropped), the set read through the effective
// scans' chunked path (streamEntityTimelines: one store snapshot per chunk,
// each entity under its entity lock, fn without graph locks).

// ForEachByIDsAnyVersion streams every row recorded at pin of each node in
// ids (see nodes.API.ForEachByIDsAnyVersion).
func (n *NodeOps) ForEachByIDsAnyVersion(ids []types.NodeID, pin types.Instant, fn func(*types.Node) bool) error {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	for _, id := range ids {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return err
		}
	}
	if err := c.validateEffectivePin(pin); err != nil {
		return err
	}
	return streamEntityTimelines(c, ascending(firstOccurrences(ids)), func(rd entityReads, id types.NodeID) ([]*types.Node, error) {
		c.entityLocks.LockEntity(id.SnowflakeID())
		defer c.entityLocks.UnlockEntity(id.SnowflakeID())
		if err := c.checkNodePointCompaction(id, pin); err != nil {
			return nil, err
		}
		if err := c.checkNodePointRetention(id, pin); err != nil {
			return nil, err
		}
		current, err := rd.currentNode(id)
		if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
			return nil, err
		}
		history, err := rd.nodeHistory(id)
		if err != nil {
			return nil, err
		}
		var rows []*types.Node
		c.eachNodeChainInterval(chainOf(history, current), anyValidStart, anyValidEnd, pin, nil, func(n *types.Node) bool {
			rows = append(rows, n)
			return true
		})
		return rows, nil
	}, func(_ types.NodeID, rows []*types.Node) bool {
		for _, r := range rows {
			if !fn(r) {
				return false
			}
		}
		return true
	})
}

// ForEachByIDsAnyVersion is NodeOps.ForEachByIDsAnyVersion for
// relationships.
func (r *RelOps) ForEachByIDsAnyVersion(ids []types.RelID, pin types.Instant, fn func(*types.Relationship) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	for _, id := range ids {
		if err := storepkg.ValidateRelID(id); err != nil {
			return err
		}
	}
	if err := c.validateEffectivePin(pin); err != nil {
		return err
	}
	for _, id := range ids {
		if err := c.checkRelTypePointRetention(id, pin); err != nil {
			return err
		}
	}
	return streamEntityTimelines(c, ascending(firstOccurrences(ids)), func(rd entityReads, id types.RelID) ([]*types.Relationship, error) {
		c.entityLocks.LockEntity(id.SnowflakeID())
		defer c.entityLocks.UnlockEntity(id.SnowflakeID())
		if err := c.checkRelPointCompaction(id, pin); err != nil {
			return nil, err
		}
		if err := c.checkRelPointRetention(pin); err != nil {
			return nil, err
		}
		current, err := rd.currentRel(id)
		if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
			return nil, err
		}
		history, err := rd.relHistory(id)
		if err != nil {
			return nil, err
		}
		var rows []*types.Relationship
		c.eachRelChainInterval(chainOf(history, current), anyValidStart, anyValidEnd, pin, nil, func(rel *types.Relationship) bool {
			rows = append(rows, rel)
			return true
		})
		return rows, nil
	}, func(_ types.RelID, rows []*types.Relationship) bool {
		for _, row := range rows {
			if !fn(row) {
				return false
			}
		}
		return true
	})
}
