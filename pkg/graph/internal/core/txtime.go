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
// The rule is storeutil.SelectAsOfWithCurrent's: the newest row (highest
// version) recorded by txTime; the live current row answers unless a row with
// a higher version was recorded at or after it (a bounded cascade that left
// the current row in its slot); absent when that row was superseded or deleted
// by txTime, or when the row holding the current slot was deleted after it and
// by txTime. None found → ErrNoVersionAsOf.
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
	if err := c.checkNodePointRetention(id, txTime); err != nil {
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
			// Fast path: no history row above the current version (versions
			// are dense, version_alloc.go), so nothing can outrank it.
			above, err := c.nodeHasHistoryAbove(id, current.Version())
			if err != nil {
				return nil, err
			}
			if !above {
				return nodeVisibleAtTxTime(current, txTime), nil
			}
		}
	}

	// Resolve through the single resolution seam. The {TxPin}-shaped probe runs
	// the as-of rule (storeutil.SelectAsOfWithCurrent: newest row recorded by
	// the pin, retraction, tombstone life end) that resolveNodeChain
	// concentrates, over history ‖ current with the current row flagged.
	hist, err := c.getNodeHistory(id)
	if err != nil {
		return nil, err
	}
	chain := hist
	if current != nil {
		chain = append(append(make([]*types.Node, 0, len(hist)+1), hist...), current)
	}
	return c.resolveNodeChain(chain, chainProbe{kind: probeAsOf, tx: txTime, asOfCurrent: current != nil}, nil)
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
	if err := c.checkRelPointRetention(txTime); err != nil {
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
			above, err := c.relHasHistoryAbove(id, current.Version())
			if err != nil {
				return nil, err
			}
			if !above {
				return relVisibleAtTxTime(current, txTime), nil
			}
		}
	}

	// Resolve through the single resolution seam — see nodeAsOfLocked.
	hist, err := c.getRelHistory(id)
	if err != nil {
		return nil, err
	}
	chain := hist
	if current != nil {
		chain = append(append(make([]*types.Relationship, 0, len(hist)+1), hist...), current)
	}
	return c.resolveRelChain(chain, chainProbe{kind: probeAsOf, tx: txTime, asOfCurrent: current != nil}, nil)
}

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
//
// Because each call reserves an instant, NowTx is NOT a metrics/polling read: a
// loop that samples it inflates the commit clock and burns instants a mutation
// would otherwise take. For observability (dashboards, health checks) use PeekTx,
// which reads the clock without reserving.
func (t *TempOps) NowTx() (types.Instant, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return 0, err
	}
	return c.now(), nil
}

// CommittedTx returns a transaction-time pin at which every write is
// committed: the open transaction's start instant (GraphTx.StartInstant) while
// one is open, else a fresh NowTx. A read pinned there (QueryOpts.TxPin, or
// TxAt with a valid time) sees no uncommitted transaction write — every write
// of an open transaction is stamped after its start — and no write stamped
// after the pin, so repeating it gives the same answer, whatever commits,
// rolls back or starts meanwhile (a history rewrite below the pin excepted:
// compaction, purge, erasure). A batch or an ingest group in progress is waited
// for (they hold the graph's write lock). Not covered: a privileged backfill
// (AllowTxBackfill) stamps a caller's past instant, and a standalone mutation
// (outside a transaction) in flight when the pin is taken is stamped before it
// and may land after a read at it started. Never above NowTx; reserves an
// instant only when no transaction is open. Errors: ErrGraphClosed.
func (t *TempOps) CommittedTx() (types.Instant, error) {
	c := t.c
	var pin types.Instant
	err := c.readUnderRLock(func() error {
		// now() first, then the open transaction: a transaction whose start is
		// not seen yet stores it after this load, so its writes are stamped
		// after now() returned (the clock is one atomic sequence).
		pin = c.now()
		if start := types.Instant(c.openTxStart.Load()); start != 0 && start < pin {
			pin = start
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return pin, nil
}

// PeekTx returns the current transaction-clock value WITHOUT reserving an instant —
// the non-burning, observability-only sibling of NowTx. A metrics/polling loop can
// call it as often as it likes without advancing the commit clock.
//
// It is DELIBERATELY NOT a sound as-of pin. The value can equal the instant a
// concurrent mutation is about to reserve via NowTx/commit, so a read pinned at
// PeekTx() would include or exclude that write nondeterministically. For a sound
// pin use NowTx() (its reservation is what guarantees strict before/after
// separation), a value returned BY a write (e.g. Tx().RunWithLSN), or a named
// TagAsOf. PeekTx is for "roughly where is the clock now", never "snapshot exactly
// what is committed".
func (t *TempOps) PeekTx() (types.Instant, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return 0, err
	}
	return c.peekNow(), nil
}

// AdvanceClock raises the transaction-clock floor to at least `to` and returns
// the resulting floor — a no-op returning the current floor when `to` is not
// ahead of it (the clock never moves backward). It is the Hybrid-Logical-Clock
// merge seam for a distributed deployment: a coordinator observing a peer
// machine's transaction timestamp calls this before stamping local writes, so
// subsequent local TxFrom values are >= every peer timestamp seen, giving a
// causal order across machines without a central sequencer. See
// Core.advanceClockFloor. rho-tkg owns only the monotonic bump; the HLC
// bookkeeping is the coordinator's.
func (t *TempOps) AdvanceClock(to types.Instant) (types.Instant, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return 0, err
	}
	return c.advanceClockFloor(to)
}

// NodesAsOf returns all nodes that existed at the given transaction time.
// Collects all known node IDs (current + history) using ForEach iterators,
// calls GetNodeAsOf per ID, skips ErrNoVersionAsOf.
// Returns nil, nil if no nodes existed at txTime.
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
	if err := c.checkScanRetentionAt(txTime); err != nil {
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
//
// DECLARED view: the relationship rows believed at txTime, NOT masked by
// endpoint validity or endpoint belief.
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
	if err := c.checkScanRetentionAt(txTime); err != nil {
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

// normalizeTemporalVisibleAtTxTime rewinds tm to the belief state as of txTime:
// a TxTo or delete recorded after txTime is removed. "ValidTo == DeletedAt"
// marks a ValidTo the delete itself wrote (the row was open, or its scheduled
// close was clamped to the delete), so it reopens; any other ValidTo is a close
// recorded before the delete and is kept. The delete doors guarantee the marker
// is unambiguous (stampDeleteTombstone / deleteInstantClearOfCloses in core).
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
