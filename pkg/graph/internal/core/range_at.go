package core

import (
	"errors"
	"math"
	"slices"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Numeric range reads at a temporal coordinate from the property index's
// membership sidecar (sigma-tkgd round 8 request 2).
//
// ForEachByLabelPropertyRange / ForEachByTypePropertyRange under a temporal
// opts fold every label (type) member: ByLabel's (ByType's) resolved version,
// kept when its value is in range (forEachNodeInRangeTemporal). A version
// whose value is in range is a row that carried the label and that value, so
// its entity is an ever-member of one of the range's values in the sidecar
// of a declared index on (label, key) — the set ByLabelAndProperty takes its
// temporal candidates from for one value. The candidates here are the members
// of every value in the range (store.NodePropertyTxMemberRangeCapability),
// pruned by the transaction-time pin as for equality, then resolved by the
// label read's own resolver in ascending ID order and filtered by the fold's
// value test: the fold's answers, from the entities that can match instead of
// every member. No declared index, a store without the capability, a
// depth-scoped read on a store with depth-aware history, an untrusted store or
// a NaN bound: the fold, as before.

// rangeCandidateWiden widens a bound for the candidate selection: the
// sidecar's sort key of a float32 is its shortest decimal rendering, the
// fold's test its float64 widening; they differ by less than 2^-24 of the
// value (denormals: below 1e-30 absolute), so the widened range selects every
// value the fold keeps.
func rangeCandidateWiden(b float64) float64 { return math.Abs(b)*1e-6 + 1e-30 }

// nodeRangeCandidatesAt returns the ascending candidate IDs of a temporal
// range read on (tok, propKey); served=false sends the caller to the fold.
// Callers hold c.mu (R or W).
func (c *Core) nodeRangeCandidatesAt(tok uint16, propKey string, min, max float64, opts storepkg.QueryOpts) (ids []types.NodeID, served bool, err error) {
	ranger, ok := c.store.(storepkg.NodePropertyTxMemberRangeCapability)
	if !ok || !c.storeRowsTrust || !c.membershipSidecarUsable(opts.Depth) || math.IsNaN(min) || math.IsNaN(max) {
		return nil, false, nil
	}
	ids, err = nodeRangeCandidates(ranger, tok, propKey, min, max, effectiveTxPin(opts))
	if err != nil {
		if errors.Is(err, storepkg.ErrIndexNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return ids, true, nil
}

// nodeRangeCandidates is the sidecar's range members (bounds widened, see
// rangeCandidateWiden) first recorded at or before pinTx (0: every one),
// ascending; the store's ErrIndexNotFound is returned as is.
func nodeRangeCandidates(ranger storepkg.NodePropertyTxMemberRangeCapability, tok uint16, propKey string, min, max float64, pinTx types.Instant) (ids []types.NodeID, err error) {
	err = ranger.ForEachNodePropertyTxMemberInRange(tok, propKey, min-rangeCandidateWiden(min), max+rangeCandidateWiden(max),
		func(id types.NodeID, firstTx types.Instant) bool {
			if pinTx == 0 || firstTx == 0 || firstTx <= pinTx {
				ids = append(ids, id)
			}
			return true
		})
	if err != nil {
		return nil, err
	}
	slices.Sort(ids)
	return ids, nil
}

// relRangeCandidatesAt is nodeRangeCandidatesAt for a relationship type.
func (c *Core) relRangeCandidatesAt(tok uint16, propKey string, min, max float64, opts storepkg.QueryOpts) (ids []types.RelID, served bool, err error) {
	ranger, ok := c.store.(storepkg.RelPropertyTxMemberRangeCapability)
	if !ok || !c.storeRowsTrust || !c.membershipSidecarUsable(opts.Depth) || math.IsNaN(min) || math.IsNaN(max) {
		return nil, false, nil
	}
	ids, err = relRangeCandidates(ranger, tok, propKey, min, max, effectiveTxPin(opts))
	if err != nil {
		if errors.Is(err, storepkg.ErrIndexNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return ids, true, nil
}

// relRangeCandidates is the sidecar's range members (bounds widened, see
// rangeCandidateWiden) first recorded at or before pinTx (0: every one),
// ascending; the store's ErrIndexNotFound is returned as is.
func relRangeCandidates(ranger storepkg.RelPropertyTxMemberRangeCapability, tok uint16, propKey string, min, max float64, pinTx types.Instant) (ids []types.RelID, err error) {
	err = ranger.ForEachRelPropertyTxMemberInRange(tok, propKey, min-rangeCandidateWiden(min), max+rangeCandidateWiden(max),
		func(id types.RelID, firstTx types.Instant) bool {
			if pinTx == 0 || firstTx == 0 || firstTx <= pinTx {
				ids = append(ids, id)
			}
			return true
		})
	if err != nil {
		return nil, err
	}
	slices.Sort(ids)
	return ids, nil
}

// nodesInRangeAtLocked is the version list forEachNodeInRangeTemporal folds:
// from the sidecar's range candidates when served, else ByLabel's resolved
// versions. Callers hold c.mu (R or W).
func (c *Core) nodesInRangeAtLocked(label, propKey string, min, max float64, opts storepkg.QueryOpts) ([]*types.Node, error) {
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	ids, served, err := c.nodeRangeCandidatesAt(tok, propKey, min, max, opts)
	if err != nil {
		return nil, err
	}
	if !served {
		return c.nodesByLabelLocked(label, opts)
	}
	scan := c.newNodeLabelScanAt(tok, nil, opts)
	var out []*types.Node
	for _, id := range skipAfter(ids, opts.After) {
		n, err := scan.resolve(id)
		if err != nil {
			return nil, err
		}
		if n != nil {
			out = append(out, n)
		}
	}
	return out, nil
}

// relsInRangeAtLocked is nodesInRangeAtLocked for a relationship type.
func (c *Core) relsInRangeAtLocked(typeName, propKey string, min, max float64, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	tok, ok := c.lookupRelTypeQueryToken(typeName)
	if !ok {
		return nil, nil
	}
	ids, served, err := c.relRangeCandidatesAt(tok, propKey, min, max, opts)
	if err != nil {
		return nil, err
	}
	if !served {
		return c.relsByTypeLocked(typeName, opts)
	}
	scan := c.newRelTypeScanAt(tok, nil, opts)
	var out []*types.Relationship
	for _, id := range skipAfter(ids, opts.After) {
		r, err := scan.resolve(id)
		if err != nil {
			return nil, err
		}
		if r != nil {
			out = append(out, r)
		}
	}
	return out, nil
}

// inRangeExact is the count's value test: the stored number within the
// bounds per the inclusivity flags, compared exactly (types.CompareNumber: a
// float32 widened, an integer past 2^53 not rounded), so [3, 3] counts what
// the equality doors find for 3. NaN is never in range.
func inRangeExact(v any, min, max float64, inclMin, inclMax bool) bool {
	lo, ok := types.CompareNumber(v, min)
	if !ok || lo < 0 || (lo == 0 && !inclMin) {
		return false
	}
	hi, ok := types.CompareNumber(v, max)
	return ok && (hi < 0 || (hi == 0 && inclMax))
}

// nodeRangeCardinalityAt is RangeCardinality under a temporal opts: the
// number of versions the temporal range door returns (After and Limit
// ignored) whose value passes inRangeExact, exact=true only when the sidecar
// served the candidates (else the caller scans, as before).
func (c *Core) nodeRangeCardinalityAt(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error) {
	if validateTemporalQueryOpts(opts) != nil || math.IsNaN(min) || math.IsNaN(max) {
		return 0, false, nil
	}
	opts.After, opts.Limit = 0, 0
	var count int64
	var served bool
	err := c.readUnderRLock(func() error {
		tok, ok := c.labels.Lookup(label)
		if !ok {
			return nil
		}
		ids, srv, err := c.nodeRangeCandidatesAt(tok, propKey, min, max, opts)
		if err != nil || !srv {
			return err
		}
		served = true
		scan := c.newNodeLabelScanAt(tok, nil, opts)
		for _, id := range ids {
			n, err := scan.resolve(id)
			if err != nil {
				return err
			}
			if n == nil {
				continue
			}
			if v, ok := n.GetProperty(propKey); ok && inRangeExact(v, min, max, inclMin, inclMax) {
				count++
			}
		}
		return nil
	})
	if err != nil || !served {
		return 0, false, err
	}
	return count, true, nil
}

// relRangeCardinalityAt is nodeRangeCardinalityAt for a relationship type.
func (c *Core) relRangeCardinalityAt(typeName, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error) {
	if validateTemporalQueryOpts(opts) != nil || math.IsNaN(min) || math.IsNaN(max) {
		return 0, false, nil
	}
	opts.After, opts.Limit = 0, 0
	var count int64
	var served bool
	err := c.readUnderRLock(func() error {
		tok, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			return nil
		}
		ids, srv, err := c.relRangeCandidatesAt(tok, propKey, min, max, opts)
		if err != nil || !srv {
			return err
		}
		served = true
		scan := c.newRelTypeScanAt(tok, nil, opts)
		for _, id := range ids {
			r, err := scan.resolve(id)
			if err != nil {
				return err
			}
			if r == nil {
				continue
			}
			if v, ok := r.GetProperty(propKey); ok && inRangeExact(v, min, max, inclMin, inclMax) {
				count++
			}
		}
		return nil
	})
	if err != nil || !served {
		return 0, false, err
	}
	return count, true, nil
}

// RangeCardinalityAt is RangeCardinality at a temporal coordinate (round 8
// request 2): the versions ForEachByLabelPropertyRange returns under opts
// whose value passes inRangeExact, counted from the membership sidecar's
// range candidates. exact=false declines: no temporal filter, invalid opts, a
// NaN bound, no declared index, a store without the range capability, a
// depth-scoped read on a store with depth-aware history, an unknown label.
func (n *NodeOps) RangeCardinalityAt(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return 0, false, err
	}
	if err := c.validateIndexLabel(label); err != nil {
		return 0, false, err
	}
	if !hasTemporalFilter(opts) {
		return 0, false, nil
	}
	return c.nodeRangeCardinalityAt(label, propKey, min, max, inclMin, inclMax, opts)
}

// RangeCardinalityAt is the relationship mirror of NodeOps.RangeCardinalityAt.
func (r *RelOps) RangeCardinalityAt(typeName, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return 0, false, err
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return 0, false, err
	}
	if !hasTemporalFilter(opts) {
		return 0, false, nil
	}
	return c.relRangeCardinalityAt(typeName, propKey, min, max, inclMin, inclMax, opts)
}
