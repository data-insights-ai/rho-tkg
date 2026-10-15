package graphapply

import (
	"context"
	"crypto/sha256"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// PrepareRangeMove reads original placement from this source's actual native
// store. Request recovery precedes current placement checks; a moved range can
// still recover its original intent without interpreting a new business retry.
func (h *allocationHost) PrepareRangeMove(ctx context.Context, id requestID, rangeID, target uint64, maxPages, pageRows, pageBytes int) (intent rangeMoveIntent, err error) {
	return h.prepareRangeMove(ctx, id, rangeID, target, maxPages, pageRows, pageBytes, rangeMovePools{})
}

// Fresh hard limits are explicit and preserved in the original GMI3. They are
// not a caller certificate that a source fits: every candidate is checked.
func (h *allocationHost) PreparePooledRangeMove(ctx context.Context, id requestID, rangeID, target uint64, maxPages, pageRows, pageBytes int, pools rangeMovePools) (rangeMoveIntent, error) {
	if !pools.valid(true) {
		return rangeMoveIntent{}, errInvalid
	}
	return h.prepareRangeMove(ctx, id, rangeID, target, maxPages, pageRows, pageBytes, pools)
}
func (h *allocationHost) prepareRangeMove(ctx context.Context, id requestID, rangeID, target uint64, maxPages, pageRows, pageBytes int, pools rangeMovePools) (intent rangeMoveIntent, err error) {
	if h == nil || ctx == nil || id == (requestID{}) || rangeID == 0 || target == 0 {
		return intent, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return intent, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unavailableLocked() {
		return intent, replica.ErrStopped
	}
	if h.machine == nil || !h.machine.rangeMoves() {
		return intent, errInvalid
	}
	if !pools.valid(rangeMovePoolsEnabled(h.machine.SemanticContractID())) {
		return intent, errInvalid
	}
	to, err := h.target(target)
	if err != nil || to == h.machine.scope() {
		return intent, errors.Join(errInvalid, err)
	}
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		return intent, err
	}
	defer func() {
		err = errors.Join(err, view.Close())
		if err != nil {
			intent = rangeMoveIntent{}
		}
	}()
	base, err := view.RootBounded(ctx, 172)
	if err != nil {
		return intent, err
	}
	q := reader{ctx: ctx, view: view, ns: h.machine.ns, base: base, limits: h.machine.limits.allocation}
	move := rangeMoveID(h.machine.ns, id)
	status, present, err := readRangeMoveStatus(&q, move)
	if err != nil {
		return intent, err
	}
	if present {
		r, found, e := crossControl(&q, rangeMoveControlKey(q.ns, move, 'I'), 1024)
		if e != nil || !found || sha256.Sum256(r.Value) != status.intent {
			return intent, errors.Join(errCorrupt, e)
		}
		intent, err = decodeRangeMoveIntent(r.Value)
		if err != nil {
			return intent, err
		}
		if intent.id != id || intent.source != h.machine.scope() || intent.target != to || intent.publication.RangeID != rangeID || intent.maxPages != maxPages || intent.pageRows != pageRows || intent.pageBytes != pageBytes || intent.pools != pools {
			return rangeMoveIntent{}, errInvalid
		}
		return intent, nil
	}
	active, err := readRangeMoveActive(&q)
	if err != nil {
		return intent, err
	}
	if active != ([16]byte{}) {
		return intent, errRangeMovePending
	}
	cfg, err := h.machine.initialized(&q)
	if err != nil {
		return intent, err
	}
	configuration, err := cfg.digest()
	if err != nil {
		return intent, err
	}
	budget := graphstore.OwnershipBudget{SourceRows: q.limits.readRows - q.rows, SourceBytes: q.limits.readBytes - q.bytes, OutputBytes: h.machine.limits.outputBytes}
	publication, owner, work, err := graphstore.LookupPublishedRange(ctx, view, graphstore.Namespace{Graph: graphstate.GraphID(h.machine.ns.graph), Partition: h.machine.ns.partition}, rangeID, configuration, budget)
	if charged := chargeGraphWork(&q, work); charged != nil {
		return intent, errors.Join(charged, err)
	}
	if err != nil {
		return intent, err
	}
	if publication.RangeID != rangeID || owner.Check(publication, h.machine.ns.partition, owner.Epoch) != nil {
		return intent, graphstore.ErrStaleOwner
	}
	intent = rangeMoveIntent{id: id, source: h.machine.scope(), target: to, publication: publication, epoch: owner.Epoch, maxPages: maxPages, pageRows: pageRows, pageBytes: pageBytes}
	intent.pools = pools
	if !intent.valid() {
		return rangeMoveIntent{}, errInvalid
	}
	return intent, nil
}
