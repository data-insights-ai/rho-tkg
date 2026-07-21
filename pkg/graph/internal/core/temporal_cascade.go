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
// itself gets a fresh hash and its PrevHash is set to the "template" row's
// hash — the most-recent-non-eclipsed row the new row's labels/properties
// were carried over from (BACKLOG 10e) — NOT the row it "supersedes on the
// VT axis" in any positional or temporal sense; a cascade never derives a
// VT-axis predecessor (nodeVersionBounds/relVersionBounds' positional
// derivation is a read-time concern, not a write-time PrevHash-selection
// one — see BACKLOG 10b for why deriving true VT-axis lineage at write time
// is a known correctness minefield in this file, not a matter of picking a
// "smarter" template). This is safe: verifyChainLinkage (integrity.go) only
// requires a non-genesis row's PrevHash to match SOME hash present anywhere
// in the same entity's full chain, not the immediately-preceding-by-version
// or VT-axis-adjacent row specifically — template is always a member of that
// chain, so linkage verification never fails on this choice. Do not "fix"
// PrevHash to walk VT-axis lineage without re-running the full bitemporal
// oracle fuzz harness (bitemporaloracle_test.go /
// bitemporaloracle_commitwindow_test.go) — 10b's reverted fix attempts prove
// this file's positional-bounds logic breaks in non-obvious multi-cascade
// ways under exactly that kind of change.
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
	nextVersion, err := nextEntityVersion(maxVersion)
	if err != nil {
		return nil, err
	}
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
	//
	// BACKLOG 10b: the resumption row's ValidTo is stamped EXPLICITLY here,
	// via nodeResumptionEnd — the smallest own-interval boundary (any row's
	// own vStart or vEnd) in the PRE-correction chain that is strictly after
	// newVT. This is NOT "the positionally-next row after src" (that was
	// tried and is UNSAFE on an already-cascaded preChain: src can be chosen
	// by BELIEF, not position, so the row positionally after it in a
	// valid-from-sorted array can have an own vStart earlier than newVT
	// itself, producing an inverted [newVT, earlier) interval — caught by the
	// oracle harness during this fix's own verification). Own-interval bounds
	// make every row's own [vStart, vEnd) a fixed, position-independent
	// interval, so the set of rows covering any t is piecewise-constant
	// between consecutive own-boundary points — meaning the belief-winner
	// (src) provably cannot change before the NEXT such boundary across ANY
	// row, not just src's positional neighbor. A resumption left open (or
	// bounded by the wrong point) would be structurally indistinguishable,
	// via its own stored ValidFrom/ValidTo/TxFrom, from a genuine override
	// starting at the same point — the two must resolve oppositely on
	// overlap, which is exactly why the bound must be exact.
	var resumption *types.Node
	if newVT != 0 {
		if src, err := c.resolveNodeVersionAt(append([]*types.Node(nil), preChain...), newVT); err == nil && src != nil {
			resumptionEnd := nodeResumptionEnd(c, preChain, newVT)
			resumption = src.DeepCopy()
			ensureNodeTemporal(resumption)
			resumption.SetVersion(nextVersion)
			nextVersion, err = nextEntityVersion(nextVersion)
			if err != nil {
				return nil, err
			}
			resumption.Temporal().ValidFrom = newVT
			resumption.Temporal().ValidTo = resumptionEnd // explicit; 0 == open (src was the pre-correction open tail)
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

	// The new "current" row is the NEWEST BELIEF among rows whose OWN interval
	// is open-ended (the value at "now" — resolving the point query at +inf).
	// BACKLOG 10b: this is deliberately NOT "the last positionally-open row in
	// valid-from order" — a row's own-open status must never be decided by its
	// neighbors, or an untouched older open row can wrongly keep the "current"
	// slot from a newer, wider-reaching open correction. See nodeOwnBounds.
	postChain := make([]*types.Node, 0, len(preChain)+len(appended))
	postChain = append(postChain, preChain...)
	postChain = append(postChain, appended...)
	var newCurrent *types.Node
	for i := range postChain {
		entry := postChain[i]
		if eclipsedNodeBounds(entry) {
			continue
		}
		if _, vEnd := c.nodeOwnBounds(entry); vEnd != 0 {
			continue // only own-open rows can own the current slot
		}
		if newCurrent == nil || nodeBeliefNewerThan(entry, newCurrent) {
			newCurrent = entry
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

// nodeResumptionEnd returns the smallest own-interval boundary (any row's own
// vStart, or own vEnd when finite) in preChain that is STRICTLY AFTER newVT —
// 0 (open) when no such boundary exists. BACKLOG 10b: own-interval bounds
// make every row's [vStart, vEnd) a fixed, position-independent interval, so
// the SET of rows covering any instant t is piecewise-constant between
// consecutive own-boundary points across the WHOLE chain — therefore the
// belief-winner at newVT (src) provably cannot change before the next such
// boundary, from ANY row, not merely the row positionally adjacent to src.
// Scanning every row (not just src's chain neighbor) is what makes this safe
// on an already-cascaded preChain, where src can be selected by belief
// rather than position.
func nodeResumptionEnd(c *Core, preChain []*types.Node, newVT types.Instant) types.Instant {
	var end types.Instant
	consider := func(b types.Instant) {
		if b > newVT && (end == 0 || b < end) {
			end = b
		}
	}
	for _, row := range preChain {
		if eclipsedNodeBounds(row) {
			continue
		}
		vStart, vEnd := c.nodeOwnBounds(row)
		consider(vStart)
		if vEnd != 0 {
			consider(vEnd)
		}
	}
	return end
}

// relResumptionEnd mirrors nodeResumptionEnd for relationships.
func relResumptionEnd(c *Core, preChain []*types.Relationship, newVT types.Instant) types.Instant {
	var end types.Instant
	consider := func(b types.Instant) {
		if b > newVT && (end == 0 || b < end) {
			end = b
		}
	}
	for _, row := range preChain {
		if eclipsedRelBounds(row) {
			continue
		}
		vStart, vEnd := c.relOwnBounds(row)
		consider(vStart)
		if vEnd != 0 {
			consider(vEnd)
		}
	}
	return end
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
	nextVersion, err := nextEntityVersion(maxVersion)
	if err != nil {
		return nil, err
	}
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

	// BACKLOG 10b: explicit resumption ValidTo via relResumptionEnd — see the
	// node cascade above for the full rationale (own-interval boundary scan,
	// not "positionally-next row after src").
	var resumption *types.Relationship
	if newVT != 0 {
		if src, err := c.resolveRelVersionAt(append([]*types.Relationship(nil), preChain...), newVT); err == nil && src != nil {
			resumptionEnd := relResumptionEnd(c, preChain, newVT)
			resumption = src.DeepCopy()
			ensureRelTemporal(resumption)
			resumption.SetVersion(nextVersion)
			nextVersion, err = nextEntityVersion(nextVersion)
			if err != nil {
				return nil, err
			}
			resumption.Temporal().ValidFrom = newVT
			resumption.Temporal().ValidTo = resumptionEnd
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

	// BACKLOG 10b: newest-belief-among-own-open — see the node cascade above.
	postChain := make([]*types.Relationship, 0, len(preChain)+len(appended))
	postChain = append(postChain, preChain...)
	postChain = append(postChain, appended...)
	var newCurrent *types.Relationship
	for i := range postChain {
		entry := postChain[i]
		if eclipsedRelBounds(entry) {
			continue
		}
		if _, vEnd := c.relOwnBounds(entry); vEnd != 0 {
			continue
		}
		if newCurrent == nil || relBeliefNewerThan(entry, newCurrent) {
			newCurrent = entry
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
