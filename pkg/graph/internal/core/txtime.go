package core

import (
	"errors"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ErrNoVersionAsOf is returned when no entity version was recorded at the given
// transaction time.
var ErrNoVersionAsOf = errors.New("graph: no entity version recorded at the given transaction time")

// NodeAsOf returns the node version that was current at the given transaction time.
//
// Algorithm:
//  1. Try current: if TxFrom > 0 && TxFrom <= txTime && TxTo == 0 → return it.
//  2. Scan Nodes.History(id): find version where TxFrom > 0 && TxFrom <= txTime
//     && (TxTo == 0 || TxTo > txTime) → return latest matching.
//  3. None found → ErrNoVersionAsOf.
func (t *TempOps) NodeAsOf(id types.NodeID, txTime types.Instant) (*types.Node, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	var result *types.Node
	err := c.readUnderRLock(func() error {
		n, err := c.nodeAsOfLocked(id, txTime)
		result = n
		return err
	})
	return result, err
}

func (c *Core) nodeAsOfLocked(id types.NodeID, txTime types.Instant) (*types.Node, error) {
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	if err := c.checkNodePointCompaction(id, txTime); err != nil {
		return nil, err
	}
	if c.txTimeQuery != nil {
		n, err := c.txTimeQuery.NodeAsOf(id, txTime)
		if errors.Is(err, storepkg.ErrVersionNotFound) {
			return nil, ErrNoVersionAsOf
		}
		if err != nil {
			return nil, err
		}
		if n == nil {
			return nil, ErrNoVersionAsOf
		}
		if err := storepkg.ValidateNodeHistorySnapshot(id, n); err != nil {
			return nil, err
		}
		return c.nodeCapabilityVisibleAtTxTime(n, txTime), nil
	}

	// Try current node first.
	current, err := c.getCurrentNode(id)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return nil, err
	}
	if current != nil {
		if tm := current.Temporal(); tm != nil && tm.TxFrom > 0 && tm.TxFrom <= txTime && tm.TxTo == 0 {
			return nodeVisibleAtTxTime(current, txTime), nil
		}
	}

	// Scan history through the single resolution seam. The {TxPin}-shaped probe
	// runs the newest-belief-by-version selection + retraction rule (lesson 62)
	// that resolveNodeChain concentrates. The fast path above already returned
	// the current row when it is the answer, so any current row that reaches here
	// is not a candidate at txTime (recorded later, or no TX stamp) and is
	// correctly excluded by passing history alone.
	hist, err := c.getNodeHistory(id)
	if err != nil {
		return nil, err
	}
	return c.resolveNodeChain(hist, chainProbe{kind: probeAsOf, tx: txTime}, nil)
}

// RelAsOf returns the relationship version that was current at the given
// transaction time. Mirrors GetNodeAsOf for relationships.
func (t *TempOps) RelAsOf(id types.RelID, txTime types.Instant) (*types.Relationship, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	var result *types.Relationship
	err := c.readUnderRLock(func() error {
		r, err := c.relAsOfLocked(id, txTime)
		result = r
		return err
	})
	return result, err
}

func (c *Core) relAsOfLocked(id types.RelID, txTime types.Instant) (*types.Relationship, error) {
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	if err := c.checkRelPointCompaction(id, txTime); err != nil {
		return nil, err
	}
	if c.txTimeQuery != nil {
		r, err := c.txTimeQuery.RelAsOf(id, txTime)
		if errors.Is(err, storepkg.ErrVersionNotFound) {
			return nil, ErrNoVersionAsOf
		}
		if err != nil {
			return nil, err
		}
		if r == nil {
			return nil, ErrNoVersionAsOf
		}
		if err := storepkg.ValidateRelationshipHistorySnapshot(id, r); err != nil {
			return nil, err
		}
		return c.relCapabilityVisibleAtTxTime(r, txTime), nil
	}

	current, err := c.getCurrentRelationship(id)
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return nil, err
	}
	if current != nil {
		if tm := current.Temporal(); tm != nil && tm.TxFrom > 0 && tm.TxFrom <= txTime && tm.TxTo == 0 {
			return relVisibleAtTxTime(current, txTime), nil
		}
	}

	// Scan history through the single resolution seam — see nodeAsOfLocked.
	hist, err := c.getRelHistory(id)
	if err != nil {
		return nil, err
	}
	return c.resolveRelChain(hist, chainProbe{kind: probeAsOf, tx: txTime}, nil)
}

// NodesAsOf returns all nodes that existed at the given transaction time.
// Collects all known node IDs (current + history) using ForEach iterators,
// calls GetNodeAsOf per ID, skips ErrNoVersionAsOf.
// Returns nil, nil if no nodes existed at txTime.
// NowTx returns the current transaction-time instant of the graph's commit
// clock — the pin to hand to the AS-OF reads (QueryOpts.TxAt / NodeAtTx /
// NodesAsOf, or a named TagAsOf) to snapshot "everything committed so far".
// Every entity already committed has TxFrom <= NowTx(), and every subsequent
// mutation is stamped strictly greater, so pinning at NowTx() includes all
// prior writes and excludes every later one.
//
// Reading it ADVANCES the commit clock by one tick (reserving the instant):
// that reservation is what guarantees the strict separation, and — because it
// consults the same monotonic-floor clock mutations use, not a bare wall-clock
// read — the value is correct even right after a Close/reopen, where the
// session's in-memory high-water mark (lastInstant) starts fresh yet the wall
// clock still dominates every historical stamp (lesson 61). A bare wall-clock
// pin is unsafe: a burst of mutations can outrun the wall, so a slept pin can
// land BEFORE the last write's logical stamp.
func (t *TempOps) NowTx() (types.Instant, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return 0, err
	}
	return c.now(), nil
}

func (t *TempOps) NodesAsOf(txTime types.Instant) ([]*types.Node, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}

	var result []*types.Node
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.nodesAsOfLocked(txTime)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Core) nodesAsOfLocked(txTime types.Instant) ([]*types.Node, error) {
	if err := c.checkScanCompactionAt(txTime); err != nil {
		return nil, err
	}
	var result []*types.Node
	if c.txTimeQuery != nil {
		nodes, err := c.txTimeQuery.NodesAsOf(txTime)
		if err != nil {
			return nil, err
		}
		if c.txTimeQueryCopy && len(nodes) > 0 {
			result = make([]*types.Node, 0, len(nodes))
		} else {
			result = nodes
		}
		for _, n := range nodes {
			if n == nil {
				return nil, storepkg.ValidateNodeHistorySnapshot(0, nil)
			}
			if err := storepkg.ValidateNodeHistorySnapshot(n.ID(), n); err != nil {
				return nil, err
			}
			if c.txTimeQueryCopy {
				result = append(result, c.nodeCapabilityVisibleAtTxTime(n, txTime))
			} else {
				nodeVisibleAtTxTime(n, txTime)
			}
		}
		storeutil.SortNodesByID(result)
		return result, nil
	}

	seen := make(map[snowflake.ID]struct{})
	if err := c.forEachNodeID(func(id types.NodeID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	}); err != nil {
		return nil, err
	}
	if err := c.forEachNodeHistoryIDByDepth(storepkg.DepthAll, func(id types.NodeID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	}); err != nil {
		return nil, err
	}

	ids := make([]snowflake.ID, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	storeutil.SortSnowflakeIDs(ids)

	for _, id := range ids {
		n, err := c.nodeAsOfLocked(types.NodeID(id), txTime)
		if err != nil {
			if errors.Is(err, ErrNoVersionAsOf) {
				continue
			}
			return nil, err
		}
		result = append(result, n)
	}
	return result, nil
}

// RelsAsOf returns all relationships that existed at the given transaction time.
// Mirrors GetNodesAsOf for relationships.
func (t *TempOps) RelsAsOf(txTime types.Instant) ([]*types.Relationship, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}

	var result []*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.relsAsOfLocked(txTime)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Core) relsAsOfLocked(txTime types.Instant) ([]*types.Relationship, error) {
	if err := c.checkScanCompactionAt(txTime); err != nil {
		return nil, err
	}
	var result []*types.Relationship
	if c.txTimeQuery != nil {
		rels, err := c.txTimeQuery.RelsAsOf(txTime)
		if err != nil {
			return nil, err
		}
		if c.txTimeQueryCopy && len(rels) > 0 {
			result = make([]*types.Relationship, 0, len(rels))
		} else {
			result = rels
		}
		for _, r := range rels {
			if r == nil {
				return nil, storepkg.ValidateRelationshipHistorySnapshot(0, nil)
			}
			if err := storepkg.ValidateRelationshipHistorySnapshot(r.ID(), r); err != nil {
				return nil, err
			}
			if c.txTimeQueryCopy {
				result = append(result, c.relCapabilityVisibleAtTxTime(r, txTime))
			} else {
				relVisibleAtTxTime(r, txTime)
			}
		}
		storeutil.SortRelsByID(result)
		return result, nil
	}

	seen := make(map[snowflake.ID]struct{})
	if err := c.forEachRelID(func(id types.RelID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	}); err != nil {
		return nil, err
	}
	if err := c.forEachRelHistoryIDByDepth(storepkg.DepthAll, func(id types.RelID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	}); err != nil {
		return nil, err
	}

	ids := make([]snowflake.ID, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	storeutil.SortSnowflakeIDs(ids)

	for _, id := range ids {
		r, err := c.relAsOfLocked(types.RelID(id), txTime)
		if err != nil {
			if errors.Is(err, ErrNoVersionAsOf) {
				continue
			}
			return nil, err
		}
		result = append(result, r)
	}
	return result, nil
}

func nodeVisibleAtTxTime(n *types.Node, txTime types.Instant) *types.Node {
	if n == nil {
		return nil
	}
	normalizeTemporalVisibleAtTxTime(n.Temporal(), txTime)
	return n
}

func relVisibleAtTxTime(r *types.Relationship, txTime types.Instant) *types.Relationship {
	if r == nil {
		return nil
	}
	normalizeTemporalVisibleAtTxTime(r.Temporal(), txTime)
	return r
}

func (c *Core) nodeCapabilityVisibleAtTxTime(n *types.Node, txTime types.Instant) *types.Node {
	if c.txTimeQueryCopy {
		n = n.DeepCopy()
	}
	return nodeVisibleAtTxTime(n, txTime)
}

func (c *Core) relCapabilityVisibleAtTxTime(r *types.Relationship, txTime types.Instant) *types.Relationship {
	if c.txTimeQueryCopy {
		r = r.DeepCopy()
	}
	return relVisibleAtTxTime(r, txTime)
}

func normalizeTemporalVisibleAtTxTime(tm *types.TemporalMetadata, txTime types.Instant) {
	if tm == nil {
		return
	}
	if tm.TxTo > txTime {
		tm.TxTo = 0
	}
	if tm.DeletedAt > txTime {
		if tm.ValidTo == tm.DeletedAt {
			tm.ValidTo = 0
		}
		tm.DeletedAt = 0
	}
}
