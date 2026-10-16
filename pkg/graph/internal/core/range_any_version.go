package core

import (
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Numeric range reads over ANY version recorded at a pin (sigma-tkgd round
// 9 request R9-1, backlog 16 as a new door).
//
// ByLabelAndProperty(label, key, v, {ValidStart: types.UnboundedValidFrom,
// ValidEnd: MaxInt64, TxAt: pin}) matches an entity when any row recorded at pin (every
// valid-time version, those of an entity deleted before the pin included,
// retracted lives dropped) carries the label and v. The range doors at a
// coordinate test the ONE version the label read resolves (round 8, backlog
// 16 unchanged). These doors are the equality door's semantics over a
// range: the entities of which some row recorded at pin carries the label
// and a numeric value within the bounds (compared as RangeCardinalityAt
// compares: a float32 widened, NaN never in range). Candidates are the
// property index's membership sidecar over the range
// (store.NodePropertyTxMemberRangeCapability), pruned by the pin, each
// entity's chain then tested row by row (eachNodeChainInterval, the
// equality door's resolver as a visitor).

// anyValidStart and anyValidEnd bound "any valid time":
// [types.UnboundedValidFrom, math.MaxInt64), a real interval filter that also
// reaches rows valid only before the epoch (ledger row 60).
const (
	anyValidStart = types.UnboundedValidFrom
	anyValidEnd   = types.Instant(math.MaxInt64)
)

// anyVersionPinOpts are the opts of every row recorded at pin.
func anyVersionPinOpts(pin types.Instant) storepkg.QueryOpts {
	return storepkg.QueryOpts{ValidStart: anyValidStart, ValidEnd: anyValidEnd, TxAt: pin}
}

// anyVersionServes reports whether the sidecar range can serve the doors:
// a native range capability, trusted rows, a usable sidecar, no Elements
// index on the pair (a list's elements are no row value).
func (c *Core) anyVersionNodeServes(tok uint16, key string) (bool, error) {
	if _, ok := c.store.(storepkg.NodePropertyTxMemberRangeCapability); !ok || !c.storeRowsTrust || !c.membershipSidecarUsable(storepkg.DepthAll) {
		return false, nil
	}
	elements, err := c.nodeIndexElements(tok, key)
	return err == nil && !elements, err
}

func (c *Core) anyVersionRelServes(tok uint16, key string) (bool, error) {
	if _, ok := c.store.(storepkg.RelPropertyTxMemberRangeCapability); !ok || !c.storeRowsTrust || !c.membershipSidecarUsable(storepkg.DepthAll) {
		return false, nil
	}
	elements, err := c.relIndexElements(tok, key)
	return err == nil && !elements, err
}

// nodeRangeAnyVersionLocked calls each(id, rows) for every candidate with
// at least one row recorded at pin carrying the label and a value in range,
// rows newest first, in ascending ID order; served=false when the sidecar
// cannot serve, the store's ErrIndexNotFound without an index on the pair
// (or one being built). Callers hold c.mu (R or W).
func (c *Core) nodeRangeAnyVersionLocked(tok uint16, key string, min, max float64, inclMin, inclMax bool, pin types.Instant,
	each func(rows []*types.Node) bool) (served bool, err error) {

	if ok, err := c.anyVersionNodeServes(tok, key); !ok || err != nil {
		return false, err
	}
	ids, err := nodeRangeCandidates(c.store.(storepkg.NodePropertyTxMemberRangeCapability), tok, key, min, max, pin)
	if err != nil {
		return true, err
	}
	pred := func(n *types.Node) bool {
		v, ok := n.GetProperty(key)
		return ok && n.HasLabelTokenRaw(tok) && inRangeExact(v, min, max, inclMin, inclMax)
	}
	var rows []*types.Node
	for _, id := range ids {
		chain, err := c.nodeChainOf(id)
		if err != nil {
			return true, err
		}
		rows = rows[:0]
		c.eachNodeChainInterval(chain, anyValidStart, anyValidEnd, pin, pred, func(n *types.Node) bool {
			rows = append(rows, n)
			return true
		})
		if len(rows) > 0 && !each(rows) {
			return true, nil
		}
	}
	return true, nil
}

// relRangeAnyVersionLocked is nodeRangeAnyVersionLocked for a type.
func (c *Core) relRangeAnyVersionLocked(tok uint16, key string, min, max float64, inclMin, inclMax bool, pin types.Instant,
	each func(rows []*types.Relationship) bool) (served bool, err error) {

	if ok, err := c.anyVersionRelServes(tok, key); !ok || err != nil {
		return false, err
	}
	ids, err := relRangeCandidates(c.store.(storepkg.RelPropertyTxMemberRangeCapability), tok, key, min, max, pin)
	if err != nil {
		return true, err
	}
	pred := func(r *types.Relationship) bool {
		v, ok := r.GetProperty(key)
		return ok && r.HasTypeTokenRaw(tok) && inRangeExact(v, min, max, inclMin, inclMax)
	}
	var rows []*types.Relationship
	for _, id := range ids {
		chain, err := c.relChainOf(id)
		if err != nil {
			return true, err
		}
		rows = rows[:0]
		c.eachRelChainInterval(chain, anyValidStart, anyValidEnd, pin, pred, func(r *types.Relationship) bool {
			rows = append(rows, r)
			return true
		})
		if len(rows) > 0 && !each(rows) {
			return true, nil
		}
	}
	return true, nil
}

// nodeChainOf is an entity's stored rows (history, then the current row);
// nil for an unknown ID. Callers hold c.mu (R or W).
func (c *Core) nodeChainOf(id types.NodeID) ([]*types.Node, error) {
	current, err := c.getCurrentNode(id)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return nil, err
	}
	c.afterCurrentRead(int64(id))
	history, err := c.getNodeHistory(id)
	if err != nil {
		return nil, err
	}
	return chainOf(history, current), nil
}

// relChainOf is nodeChainOf for a relationship.
func (c *Core) relChainOf(id types.RelID) ([]*types.Relationship, error) {
	current, err := c.getCurrentRelationship(id)
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return nil, err
	}
	c.afterCurrentRead(int64(id))
	history, err := c.getRelHistory(id)
	if err != nil {
		return nil, err
	}
	return chainOf(history, current), nil
}

// anyVersionArgs validates a door's arguments; scope is the retention scope
// the pin is checked against (scopeNodes, or the relationship type's
// relTypeRetentionScope, so a pin below a purge watermark fails closed as the
// equality door does); skip reports a call that
// visits nothing (pin <= 0, a NaN bound: no value is in such a range).
func (c *Core) anyVersionArgs(validateName func() error, scope retentionScope, key string, min, max float64, pin types.Instant) (skip bool, err error) {
	if err := c.checkOpen(); err != nil {
		return false, err
	}
	if err := validateName(); err != nil {
		return false, err
	}
	if err := c.validateIndexPropertyKey(key); err != nil {
		return false, err
	}
	if pin <= 0 || math.IsNaN(min) || math.IsNaN(max) {
		return true, nil
	}
	return false, c.validateTemporalQueryOptsScan(anyVersionPinOpts(pin), scope)
}

// ForEachByLabelPropertyRangeAnyVersion visits, once each in ascending ID
// order, the label's nodes of which some row recorded at pin carries the
// label and a numeric value of key within [min, max] per the flags; fn gets
// the newest such row and false stops. Errors: ErrIndexNotFound without a
// property index on the pair, ErrCapabilityNotSupported where the sidecar
// range cannot serve (tiered, sharded, external stores, an Elements index).
func (n *NodeOps) ForEachByLabelPropertyRangeAnyVersion(label, key string, min, max float64, inclMin, inclMax bool, pin types.Instant, fn func(*types.Node) bool) error {
	c := n.c
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	skip, err := c.anyVersionArgs(func() error { return c.validateIndexLabel(label) }, scopeNodes, key, min, max, pin)
	if err != nil || skip {
		return err
	}
	var out []*types.Node
	err = c.readUnderRLock(func() error {
		tok, ok := c.labels.Lookup(label)
		if !ok {
			return nil
		}
		served, err := c.nodeRangeAnyVersionLocked(tok, key, min, max, inclMin, inclMax, pin, func(rows []*types.Node) bool {
			out = append(out, rows[0])
			return true
		})
		if err == nil && !served {
			return storepkg.ErrCapabilityNotSupported
		}
		return err
	})
	if err != nil {
		return err
	}
	for _, nd := range out {
		if !fn(nd) {
			return nil
		}
	}
	return nil
}

// RangeCardinalityAnyVersion counts the nodes ForEachByLabelPropertyRangeAnyVersion
// visits and their rows recorded at pin whose value is in range; exact=false
// declines (no index, an index being built, an Elements index, tiered,
// sharded, external stores), never walks the label.
func (n *NodeOps) RangeCardinalityAnyVersion(label, key string, min, max float64, inclMin, inclMax bool, pin types.Instant) (storepkg.RangeAnyVersionCount, bool, error) {
	c := n.c
	var count storepkg.RangeAnyVersionCount
	skip, err := c.anyVersionArgs(func() error { return c.validateIndexLabel(label) }, scopeNodes, key, min, max, pin)
	if err != nil || skip {
		return count, err == nil, err
	}
	served := false
	err = c.readUnderRLock(func() error {
		tok, ok := c.labels.Lookup(label)
		if !ok {
			served = true
			return nil
		}
		var err error
		served, err = c.nodeRangeAnyVersionLocked(tok, key, min, max, inclMin, inclMax, pin, func(rows []*types.Node) bool {
			count.Entities++
			count.Rows += int64(len(rows))
			return true
		})
		return err
	})
	if errors.Is(err, storepkg.ErrIndexNotFound) {
		err, served = nil, false
	}
	if err != nil || !served {
		return storepkg.RangeAnyVersionCount{}, false, err
	}
	return count, true, nil
}

// ForEachByTypePropertyRangeAnyVersion is the relationship mirror of
// NodeOps.ForEachByLabelPropertyRangeAnyVersion over a type.
func (r *RelOps) ForEachByTypePropertyRangeAnyVersion(typeName, key string, min, max float64, inclMin, inclMax bool, pin types.Instant, fn func(*types.Relationship) bool) error {
	c := r.c
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	skip, err := c.anyVersionArgs(func() error { return c.validateRelTypeQueryName(typeName) }, c.relTypeRetentionScope(typeName), key, min, max, pin)
	if err != nil || skip {
		return err
	}
	var out []*types.Relationship
	err = c.readUnderRLock(func() error {
		tok, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			return nil
		}
		served, err := c.relRangeAnyVersionLocked(tok, key, min, max, inclMin, inclMax, pin, func(rows []*types.Relationship) bool {
			out = append(out, rows[0])
			return true
		})
		if err == nil && !served {
			return storepkg.ErrCapabilityNotSupported
		}
		return err
	})
	if err != nil {
		return err
	}
	for _, rel := range out {
		if !fn(rel) {
			return nil
		}
	}
	return nil
}

// RangeCardinalityAnyVersion is the relationship mirror of
// NodeOps.RangeCardinalityAnyVersion.
func (r *RelOps) RangeCardinalityAnyVersion(typeName, key string, min, max float64, inclMin, inclMax bool, pin types.Instant) (storepkg.RangeAnyVersionCount, bool, error) {
	c := r.c
	var count storepkg.RangeAnyVersionCount
	skip, err := c.anyVersionArgs(func() error { return c.validateRelTypeQueryName(typeName) }, c.relTypeRetentionScope(typeName), key, min, max, pin)
	if err != nil || skip {
		return count, err == nil, err
	}
	served := false
	err = c.readUnderRLock(func() error {
		tok, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			served = true
			return nil
		}
		var err error
		served, err = c.relRangeAnyVersionLocked(tok, key, min, max, inclMin, inclMax, pin, func(rows []*types.Relationship) bool {
			count.Entities++
			count.Rows += int64(len(rows))
			return true
		})
		return err
	})
	if errors.Is(err, storepkg.ErrIndexNotFound) {
		err, served = nil, false
	}
	if err != nil || !served {
		return storepkg.RangeAnyVersionCount{}, false, err
	}
	return count, true, nil
}
