package core

import (
	"context"
	"errors"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The by-endpoint scans under a read budget (storepkg.WithReadBudget, RH list
// R2, R2-SCANS): ForEachRelEffectiveAtNodes, its Window form and the typed
// column scan. Like the other scans they stream and have no partial answer to
// withhold (the callbacks made before a refusal stand, none follows; the column
// scan holds every row first and is exact or refused). Their gather is counted
// as it walks, before each step (relEffectiveAdjacencyCandidatesRun):
//   - Candidates: one per seed probed, each entry of the seeds' current adjacency
//     before it is collected (storepkg.AdjacencyMeteredCapability: a RAM index
//     counts a node's whole set before the list is built, the on-disk adjacency
//     index each entry as its walk produces it) and each entry of their
//     ex-adjacency as it is produced; then, as the other scans, one per
//     relationship before its timeline is read (a relationship the gather listed
//     counts twice: a budget reads high, never low);
//   - DecodedBytes and ShardOpens: the seeds' rows and the adjacency rows the
//     gather decodes, each closed shard the gather opens (a tiered seed's
//     ex-adjacency lives on the archive and on the event shard of its mint time:
//     storepkg.ExAdjacencyMeteredCapability), before they happen;
//   - BufferBytes: 8 per seed and 8 per listed relationship once each list is
//     built, and for the column scan 24 per kept segment before it is held;
//   - HistoryExamined, DecodedBytes, ShardOpens and Segments of the timeline
//     reads and of the segments handed out, as the scans of read_budget_scans.go.

// ForEachRelEffectiveAtNodesCtx is ForEachRelEffectiveAtNodes under ctx's read
// budget (temporal.API.ForEachRelEffectiveAtNodesCtx).
func (t *TempOps) ForEachRelEffectiveAtNodesCtx(ctx context.Context, nodeIDs []types.NodeID, dir temporal.Direction, relTypes []string, pin types.Instant, fn func(temporal.RelSegment) bool) error {
	run, err := t.c.scanRunFor(ctx, "ForEachRelEffectiveAtNodes")
	if err != nil {
		return err
	}
	return t.c.forEachRelEffectiveAtNodesRun(run, nodeIDs, dir, relTypes, recordedPin(pin), nil, fn)
}

// ForEachRelEffectiveAtNodesWindowCtx is ForEachRelEffectiveAtNodesWindow under
// ctx's read budget.
func (t *TempOps) ForEachRelEffectiveAtNodesWindowCtx(ctx context.Context, nodeIDs []types.NodeID, dir temporal.Direction, relTypes []string, pin types.Instant, window temporal.Window, fn func(temporal.RelSegment) bool) error {
	run, err := t.c.scanRunFor(ctx, "ForEachRelEffectiveAtNodesWindow")
	if err != nil {
		return err
	}
	return t.forEachRelEffectiveAtNodesWindowRun(run, nodeIDs, dir, relTypes, recordedPin(pin), window, fn)
}

// ScanRelEffectiveColumnsAtNodesCtx is ScanRelEffectiveColumnsAtNodes under
// ctx's read budget.
func (t *TempOps) ScanRelEffectiveColumnsAtNodesCtx(ctx context.Context, nodeIDs []types.NodeID, dir temporal.Direction, relTypes []string, pin types.Instant, window temporal.Window, props []string, fn func(*storepkg.RelColumnBatch) bool) error {
	run, err := t.c.scanRunFor(ctx, "ScanRelEffectiveColumnsAtNodes")
	if err != nil {
		return err
	}
	return t.scanRelEffectiveColumnsAtNodesRun(run, nodeIDs, dir, relTypes, recordedPin(pin), window, props, fn)
}

// relEffectiveAdjacencyCandidatesRun is relEffectiveAdjacencyCandidates (c.mu
// held) under a read budget (run nil: the plain gather, one batched adjacency
// read): the seeds are probed one at a time through a metered read snapshot,
// each counted as a candidate before its row is read; the adjacency of each
// seed that exists is read through storeAdjacentRelsTallied (the entries and
// decodes counted by the store, or the node's degree counted before the read on
// a store that cannot count its own); and the ex-adjacency is walked entry by
// entry, each counted before it is collected. The IDs and their union are the
// plain gather's.
func (c *Core) relEffectiveAdjacencyCandidatesRun(run *scanRun, seeds []types.NodeID, toks []uint16, outgoing bool) ([]types.RelID, error) {
	if run == nil {
		return c.relEffectiveAdjacencyCandidates(seeds, toks, outgoing)
	}
	tally := run.tally()
	rd, release := c.meteredEntityReads(tally)
	defer release()
	live := make([]types.NodeID, 0, len(seeds))
	for _, id := range seeds {
		if err := run.admit(); err != nil {
			return nil, err
		}
		if _, err := rd.currentNode(id); err != nil {
			if errors.Is(err, storepkg.ErrNodeNotFound) {
				continue // deleted (or never stored): its edges are history
			}
			return nil, err
		}
		live = append(live, id)
	}
	var tok uint16
	if len(toks) == 1 {
		tok = toks[0]
	}
	rows := make(map[types.NodeID][]*types.Relationship, len(live))
	for _, id := range live {
		rels, err := c.storeAdjacentRelsTallied(id, tok, !outgoing, storepkg.DepthAll, tally)
		if err != nil {
			return nil, err
		}
		if len(rels) > 0 {
			rows[id] = rels
		}
	}
	var current []types.RelID
	if len(live) > 0 {
		var err error
		if current, err = c.relIDsFromNodeMapRows(live, tok, rows, outgoing); err != nil {
			return nil, err
		}
	}
	var ids []types.RelID
	err := c.forEachRelAdjacencyCandidateIDRun(run, seeds, adjDirsOf(outgoing), current, func(id types.RelID) error {
		ids = append(ids, id)
		return nil
	})
	return ids, err
}

// forEachRelAdjacencyCandidateIDRun is forEachRelAdjacencyCandidateID under a
// read budget: each ex-adjacent entry counts one candidate as the store
// produces it, and the walk stops at the one that passes a limit.
func (c *Core) forEachRelAdjacencyCandidateIDRun(run *scanRun, seeds []types.NodeID, dirs adjDirs, currentIDs []types.RelID, fn func(types.RelID) error) error {
	seen := make(map[types.RelID]struct{}, len(currentIDs))
	for _, id := range currentIDs {
		seen[id] = struct{}{}
	}
	var refused error
	if err := c.forEachRelExAdjacentIDRun(run, seeds, dirs, storepkg.DepthAll, func(id types.RelID) bool {
		if refused = run.admit(); refused != nil {
			return false
		}
		seen[id] = struct{}{}
		return true
	}); err != nil {
		return err
	}
	if refused != nil {
		return refused
	}
	for id := range seen {
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}

// forEachRelExAdjacentIDRun is forEachRelExAdjacentID under a read budget: a
// store that opens closed shards to answer charges each open to the run's tally
// before it (storepkg.ExAdjacencyMeteredCapability); a store without an
// ex-adjacency covering every relationship folds the history IDs of every shard,
// which on a store that opens shards cannot count its opens, so a ShardOpens
// budget refuses it before the fold (Limit "unsupported", foldOpensRefused). The
// callback counts each ID it is handed.
func (c *Core) forEachRelExAdjacentIDRun(run *scanRun, seeds []types.NodeID, dirs adjDirs, depth storepkg.ShardDepth, fn func(types.RelID) bool) error {
	if run == nil {
		return c.forEachRelExAdjacentID(seeds, dirs, depth, fn)
	}
	if c.exAdj == nil {
		if err := c.foldOpensRefused(run.opts(storepkg.QueryOpts{})); err != nil {
			return err
		}
		return c.forEachRelHistoryIDByDepth(depth, fn)
	}
	metered, _ := c.exAdj.(storepkg.ExAdjacencyMeteredCapability)
	if metered == nil {
		return c.forEachRelExAdjacentID(seeds, dirs, depth, fn)
	}
	stopped := false
	visit := func(id types.RelID) bool {
		if !fn(id) {
			stopped = true
		}
		return !stopped
	}
	for _, seed := range seeds {
		for _, d := range []adjDirs{adjOut, adjIn} {
			if dirs&d == 0 {
				continue
			}
			if err := metered.ForEachExAdjacentRelIDMetered(seed, d == adjIn, run.tally(), visit); err != nil || stopped {
				return err
			}
		}
	}
	return nil
}
