package core

import (
	"errors"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Resolve before consuming Limit. The store's complete ordered superset also
// includes deleted/currently nonmatching rows, so no initial current-label or
// property materialization is needed. The existing canonical resolver owns all
// as-of, valid-time, interval and retraction semantics.
func (c *Core) orderedHistoricalNodes(token uint16, property, value string, opts storepkg.QueryOpts, pred func(*types.Node) bool) ([]*types.Node, bool, error) {
	cap, ok := c.store.(storepkg.OrderedHistoricalCandidatesCapability)
	if !ok || !c.storeRowsTrust {
		return nil, false, nil
	}
	var out []*types.Node
	var resolveErr error
	pin := effectiveTxPin(opts)
	resolveOpts := c.normalizeTxAtOnlyOpts(opts)
	err := cap.ForEachNodeHistoricalCandidate(token, property, value, types.NodeID(opts.After), func(id types.NodeID, first types.Instant) bool {
		if pin != 0 && first != 0 && first > pin {
			return true
		}
		n, err := c.findNodeVersionForOpts(id, resolveOpts, pred)
		if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
			return true
		}
		if err != nil {
			resolveErr = err
			return false
		}
		out = append(out, n)
		return opts.Limit == 0 || len(out) < opts.Limit
	})
	if errors.Is(err, storepkg.ErrIndexNotFound) {
		return nil, false, nil
	}
	return out, true, errors.Join(err, resolveErr)
}

func (c *Core) orderedHistoricalRels(token uint16, property, value string, opts storepkg.QueryOpts, pred func(*types.Relationship) bool) ([]*types.Relationship, bool, error) {
	cap, ok := c.store.(storepkg.OrderedHistoricalCandidatesCapability)
	if !ok || !c.storeRowsTrust {
		return nil, false, nil
	}
	var out []*types.Relationship
	var resolveErr error
	pin := effectiveTxPin(opts)
	resolveOpts := c.normalizeTxAtOnlyOpts(opts)
	err := cap.ForEachRelHistoricalCandidate(token, property, value, types.RelID(opts.After), func(id types.RelID, first types.Instant) bool {
		if pin != 0 && first != 0 && first > pin {
			return true
		}
		r, err := c.findRelVersionForOpts(id, resolveOpts, pred)
		if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
			return true
		}
		if err != nil {
			resolveErr = err
			return false
		}
		out = append(out, r)
		return opts.Limit == 0 || len(out) < opts.Limit
	})
	if errors.Is(err, storepkg.ErrIndexNotFound) {
		return nil, false, nil
	}
	return out, true, errors.Join(err, resolveErr)
}
