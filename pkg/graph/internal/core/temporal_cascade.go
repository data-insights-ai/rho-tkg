package core

import (
	"context"
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Cascade timeline edit — full implementation
//
// Five overlap classifications between an existing version's valid interval
// [vf, vt) and the cascade target [newVF, newVT):
//
//   - keep            no overlap, version untouched
//   - closeRight      vf < newVF < vt — close existing at newVF (vt becomes newVF)
//   - openLeft        newVF <= vf < newVT < vt — open existing from newVT
//   - eclipse         newVF <= vf < vt <= newVT — version fully contained,
//                     marked zero-length (ValidFrom == ValidTo) so the
//                     resolver never matches it for any VT query
//   - split           vf < newVF < newVT < vt — version spans the target;
//                     becomes two rows: [vf, newVF) at original version
//                     number, [newVT, vt) at a freshly-allocated version
//
// Per-row hashes are unchanged because TemporalMetadata is NOT part of the
// content hash (see integrity.computeNodeHashWithBuffer). The new version
// itself gets a fresh hash and joins the chain via PrevHash from whichever
// row it directly supersedes on the VT axis.
//
// "Current" semantics: the row that has the latest open-ended interval
// (max(effectiveValidFrom) where ValidTo == 0) is the new current. If the
// cascade leaves no open-ended row, the entity has no current — the store
// still holds history rows, queries by ID return ErrNodeNotFound, but
// temporal queries find the appropriate version.
// =============================================================================

// eclipsedNodeBounds returns whether a node's temporal metadata represents
// a cascade-eclipsed (near-zero-length, 1-instant) interval. Sentinel:
// ValidTo == ValidFrom + 1. The store rejects ValidFrom == ValidTo, so we
// use +1 as the smallest tile the store accepts. The resolver explicitly
// skips eclipsed rows during version selection so the 1-instant width does
// not cause spurious matches at t == ValidFrom.
func eclipsedNodeBounds(n *types.Node) bool {
	tm := n.Temporal()
	if tm == nil {
		return false
	}
	return tm.ValidFrom != 0 && tm.ValidTo != 0 && tm.ValidTo == tm.ValidFrom+1
}

func eclipsedRelBounds(r *types.Relationship) bool {
	tm := r.Temporal()
	if tm == nil {
		return false
	}
	return tm.ValidFrom != 0 && tm.ValidTo != 0 && tm.ValidTo == tm.ValidFrom+1
}

// =============================================================================
// Node cascade
// =============================================================================

func (c *Core) cascadeNodeVersionInterval(ctx context.Context, id types.NodeID, newVF, newVT types.Instant, props map[string]any) (*types.Node, error) {
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	if newVF == 0 {
		return nil, fmt.Errorf("%w: cascade requires non-zero validFrom", ErrInvalidTimeRange)
	}
	if newVT != 0 && newVF >= newVT {
		return nil, fmt.Errorf("%w: validFrom %d >= validTo %d", ErrInvalidTimeRange, newVF, newVT)
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if err := c.checkpointDirtyRegistriesBeforeMutation("cascade node"); err != nil {
		return nil, err
	}

	current, err := c.getCurrentNode(id)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return nil, err
	}
	history, err := c.getNodeHistory(id)
	if err != nil {
		return nil, err
	}
	if current == nil && len(history) == 0 {
		return nil, storepkg.ErrNodeNotFound
	}

	maxVersion := uint32(0)
	for _, h := range history {
		if h.Version() > maxVersion {
			maxVersion = h.Version()
		}
	}
	if current != nil && current.Version() > maxVersion {
		maxVersion = current.Version()
	}
	nextVersion := maxVersion + 1
	now := c.now()

	// Pick the most recent non-eclipsed version as the template for
	// labels/integrity carry-over for the inserted row.
	var template *types.Node
	if current != nil && !eclipsedNodeBounds(current) {
		template = current
	} else {
		for i := len(history) - 1; i >= 0; i-- {
			if !eclipsedNodeBounds(history[i]) {
				template = history[i]
				break
			}
		}
	}
	if template == nil {
		return nil, fmt.Errorf("%w: cascade requires at least one non-eclipsed version", storepkg.ErrNodeNotFound)
	}

	// APPEND-ONLY (audited correction). The cascade records, AT `now`, a new
	// belief: "[newVF, newVT) is `props`". Transaction time is append-only and
	// monotonic, so we NEVER mutate an existing row's stored valid-interval or
	// transaction stamps (doing so would make a row claim the DB believed a
	// world-boundary at a past TxFrom it actually decided now — corrupting
	// NodeAtTx at earlier txAt). Instead we append two fresh-TxFrom rows and
	// leave every existing row untouched; the resolver tiles the TxFrom-filtered
	// chain by valid-from and, on overlap, the newer belief wins
	// (resolveNodeVersionAt). Existing rows therefore reconstruct the
	// pre-correction belief exactly, and the appended rows the post-correction
	// belief — no holes, no leaks.
	preChain := make([]*types.Node, 0, len(history)+1)
	preChain = append(preChain, history...)
	if current != nil {
		preChain = append(preChain, current)
	}

	// Resumption: re-assert, from newVT onward, whatever value held AT newVT in
	// the pre-correction belief, so the part of the timeline after the
	// correction is unchanged. Open-ended (newVT == 0) corrections need none.
	var resumption *types.Node
	if newVT != 0 {
		if src, err := c.resolveNodeVersionAt(append([]*types.Node(nil), preChain...), newVT); err == nil && src != nil {
			resumption = src.DeepCopy()
			ensureNodeTemporal(resumption)
			resumption.SetVersion(nextVersion)
			nextVersion++
			resumption.Temporal().ValidFrom = newVT
			resumption.Temporal().ValidTo = 0 // open; tiles to the next existing version
			resumption.Temporal().UpdatedAt = now
			resumption.Temporal().TxFrom = now
			rLabels := c.nodeLabelsUnlocked(resumption)
			rHash, err := integrity.ComputeNodeHashChecked(resumption, rLabels)
			if err != nil {
				return nil, fmt.Errorf("graph: cascade compute resumption hash: %w", err)
			}
			rPrev := ""
			if ig := src.Integrity(); ig != nil {
				rPrev = ig.Hash
			}
			resumption.SetIntegrity(nodeIntegrityWithHash(resumption.Integrity(), rHash, rPrev))
		} else if err != nil && !errors.Is(err, storepkg.ErrNoVersionValidAt) {
			return nil, fmt.Errorf("graph: cascade resolve resumption source: %w", err)
		}
	}

	// The inserted interval [newVF, newVT) carrying the corrected state.
	newVer := template.DeepCopy()
	newVer.SetVersion(nextVersion) // last allocation; no further increment needed
	for key, val := range props {
		if val == nil {
			if _, err := newVer.DeleteProperty(key); err != nil {
				return nil, fmt.Errorf("graph: cascade prop %q: %w", key, err)
			}
		} else {
			if err := newVer.SetProperty(key, val); err != nil {
				return nil, fmt.Errorf("graph: cascade prop %q: %w", key, err)
			}
		}
	}
	if newVer.PropertyCount() > c.validation.MaxPropertiesPerEntity {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyProperties, newVer.PropertyCount(), c.validation.MaxPropertiesPerEntity)
	}
	ensureNodeTemporal(newVer)
	newVer.Temporal().ValidFrom = newVF
	newVer.Temporal().ValidTo = newVT
	newVer.Temporal().UpdatedAt = now
	newVer.Temporal().TxFrom = now

	prevHash := ""
	if ig := template.Integrity(); ig != nil {
		prevHash = ig.Hash
	}
	labels := c.nodeLabelsUnlocked(newVer)
	hash, err := integrity.ComputeNodeHashChecked(newVer, labels)
	if err != nil {
		return nil, fmt.Errorf("graph: cascade compute hash: %w", err)
	}
	newVer.SetIntegrity(nodeIntegrityWithHash(newVer.Integrity(), hash, prevHash))

	appended := []*types.Node{newVer}
	if resumption != nil {
		appended = append(appended, resumption)
	}

	// The new "current" row is the rightmost open-ended tile of the
	// post-correction belief (the value at "now"). Build the post chain, sort by
	// valid-from, and take the last row whose tiled interval is open-ended.
	postChain := make([]*types.Node, 0, len(preChain)+len(appended))
	postChain = append(postChain, preChain...)
	postChain = append(postChain, appended...)
	c.sortNodeChainForResolve(postChain)
	var newCurrent *types.Node
	for i := range postChain {
		if eclipsedNodeBounds(postChain[i]) {
			continue
		}
		if _, vEnd := c.nodeVersionBounds(postChain, i); vEnd == 0 {
			newCurrent = postChain[i] // sorted asc → last open-ended wins
		}
	}

	// Write: append the new rows, never touching existing ones. The store keeps
	// one "current" KV slot; place newCurrent there (demoting the prior current
	// to a history row — its bytes are unchanged, only its slot moves).
	curIsNew := newCurrent != nil && (newCurrent == newVer || newCurrent == resumption)
	for _, r := range appended {
		if r == newCurrent {
			continue // written via ReplaceNode below
		}
		if err := c.store.PutNodeVersion(id, r.Version(), r); err != nil {
			return nil, fmt.Errorf("graph: cascade put appended version: %w", err)
		}
	}
	if curIsNew {
		if current != nil {
			if err := c.store.PutNodeVersion(id, current.Version(), current); err != nil {
				return nil, fmt.Errorf("graph: cascade demote current to history: %w", err)
			}
		}
		if err := c.store.ReplaceNode(newCurrent); err != nil {
			return nil, fmt.Errorf("graph: cascade replace current: %w", err)
		}
	}

	c.opNodeUpdates.Add(1)
	return newVer, nil
}

func ensureNodeTemporal(n *types.Node) {
	if n.Temporal() == nil {
		n.SetTemporal(&types.TemporalMetadata{})
	}
}

func ensureRelTemporal(r *types.Relationship) {
	if r.Temporal() == nil {
		r.SetTemporal(&types.TemporalMetadata{})
	}
}

// =============================================================================
// Relationship cascade (mirror of node cascade, rule 2 parity)
// =============================================================================

func (c *Core) cascadeRelVersionInterval(ctx context.Context, id types.RelID, newVF, newVT types.Instant, props map[string]any) (*types.Relationship, error) {
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	if newVF == 0 {
		return nil, fmt.Errorf("%w: cascade requires non-zero validFrom", ErrInvalidTimeRange)
	}
	if newVT != 0 && newVF >= newVT {
		return nil, fmt.Errorf("%w: validFrom %d >= validTo %d", ErrInvalidTimeRange, newVF, newVT)
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if err := c.checkpointDirtyRegistriesBeforeMutation("cascade rel"); err != nil {
		return nil, err
	}

	current, err := c.getCurrentRelationship(id)
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return nil, err
	}
	history, err := c.getRelHistory(id)
	if err != nil {
		return nil, err
	}
	if current == nil && len(history) == 0 {
		return nil, storepkg.ErrRelNotFound
	}

	maxVersion := uint32(0)
	for _, h := range history {
		if h.Version() > maxVersion {
			maxVersion = h.Version()
		}
	}
	if current != nil && current.Version() > maxVersion {
		maxVersion = current.Version()
	}
	nextVersion := maxVersion + 1
	now := c.now()

	var template *types.Relationship
	if current != nil && !eclipsedRelBounds(current) {
		template = current
	} else {
		for i := len(history) - 1; i >= 0; i-- {
			if !eclipsedRelBounds(history[i]) {
				template = history[i]
				break
			}
		}
	}
	if template == nil {
		return nil, fmt.Errorf("%w: cascade requires at least one non-eclipsed version", storepkg.ErrRelNotFound)
	}

	// APPEND-ONLY (audited correction) — mirror of cascadeNodeVersionInterval.
	// Never mutate an existing row's stored interval or transaction stamps;
	// append fresh-TxFrom rows and let the resolver tile by valid-from with the
	// newer belief winning on overlap. See the node cascade for the rationale.
	preChain := make([]*types.Relationship, 0, len(history)+1)
	preChain = append(preChain, history...)
	if current != nil {
		preChain = append(preChain, current)
	}

	var resumption *types.Relationship
	if newVT != 0 {
		if src, err := c.resolveRelVersionAt(append([]*types.Relationship(nil), preChain...), newVT); err == nil && src != nil {
			resumption = src.DeepCopy()
			ensureRelTemporal(resumption)
			resumption.SetVersion(nextVersion)
			nextVersion++
			resumption.Temporal().ValidFrom = newVT
			resumption.Temporal().ValidTo = 0
			resumption.Temporal().UpdatedAt = now
			resumption.Temporal().TxFrom = now
			rType := c.relTypeUnlocked(resumption)
			rHash, err := integrity.ComputeRelHashChecked(resumption, rType)
			if err != nil {
				return nil, fmt.Errorf("graph: cascade compute rel resumption hash: %w", err)
			}
			rPrev := ""
			if ig := src.Integrity(); ig != nil {
				rPrev = ig.Hash
			}
			rIG := relIntegrityWithHash(resumption.Integrity(), rHash, rPrev)
			if err := c.refreshRelationshipEndpointHashes(resumption, rIG); err != nil {
				return nil, fmt.Errorf("graph: cascade refresh rel resumption endpoint hashes: %w", err)
			}
			resumption.SetIntegrity(rIG)
		} else if err != nil && !errors.Is(err, storepkg.ErrNoVersionValidAt) {
			return nil, fmt.Errorf("graph: cascade resolve rel resumption source: %w", err)
		}
	}

	newVer := template.DeepCopy()
	newVer.SetVersion(nextVersion) // last allocation; no further increment needed
	for key, val := range props {
		if val == nil {
			if _, err := newVer.DeleteProperty(key); err != nil {
				return nil, fmt.Errorf("graph: cascade prop %q: %w", key, err)
			}
		} else {
			if err := newVer.SetProperty(key, val); err != nil {
				return nil, fmt.Errorf("graph: cascade prop %q: %w", key, err)
			}
		}
	}
	if newVer.PropertyCount() > c.validation.MaxPropertiesPerEntity {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyProperties, newVer.PropertyCount(), c.validation.MaxPropertiesPerEntity)
	}
	ensureRelTemporal(newVer)
	newVer.Temporal().ValidFrom = newVF
	newVer.Temporal().ValidTo = newVT
	newVer.Temporal().UpdatedAt = now
	newVer.Temporal().TxFrom = now

	prevHash := ""
	if ig := template.Integrity(); ig != nil {
		prevHash = ig.Hash
	}
	relTypeName := c.relTypeUnlocked(newVer)
	hash, err := integrity.ComputeRelHashChecked(newVer, relTypeName)
	if err != nil {
		return nil, fmt.Errorf("graph: cascade compute rel hash: %w", err)
	}
	relIG := relIntegrityWithHash(newVer.Integrity(), hash, prevHash)
	if err := c.refreshRelationshipEndpointHashes(newVer, relIG); err != nil {
		return nil, fmt.Errorf("graph: cascade refresh endpoint hashes: %w", err)
	}
	newVer.SetIntegrity(relIG)

	appended := []*types.Relationship{newVer}
	if resumption != nil {
		appended = append(appended, resumption)
	}

	postChain := make([]*types.Relationship, 0, len(preChain)+len(appended))
	postChain = append(postChain, preChain...)
	postChain = append(postChain, appended...)
	c.sortRelChainForResolve(postChain)
	var newCurrent *types.Relationship
	for i := range postChain {
		if eclipsedRelBounds(postChain[i]) {
			continue
		}
		if _, vEnd := c.relVersionBounds(postChain, i); vEnd == 0 {
			newCurrent = postChain[i]
		}
	}

	curIsNew := newCurrent != nil && (newCurrent == newVer || newCurrent == resumption)
	for _, r := range appended {
		if r == newCurrent {
			continue
		}
		if err := c.store.PutRelVersion(id, r.Version(), r); err != nil {
			return nil, fmt.Errorf("graph: cascade put appended rel version: %w", err)
		}
	}
	if curIsNew {
		if current != nil {
			if err := c.store.PutRelVersion(id, current.Version(), current); err != nil {
				return nil, fmt.Errorf("graph: cascade demote rel current: %w", err)
			}
		}
		if err := c.store.ReplaceRelationship(newCurrent); err != nil {
			return nil, fmt.Errorf("graph: cascade replace rel current: %w", err)
		}
	}

	c.opRelUpdates.Add(1)
	return newVer, nil
}
