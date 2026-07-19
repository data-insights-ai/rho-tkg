package core

import (
	"fmt"
	"slices"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
)

func (c *Core) getOrCreateLabelWithSnapshot(label string) (uint16, []string, bool, error) {
	c.registryMu.Lock()
	snapshot := c.labels.ExportNames()
	_, existed := c.labels.Lookup(label)
	token, err := c.labels.GetOrCreate(label)
	if err != nil {
		c.registryMu.Unlock()
		return 0, nil, false, err
	}
	if existed {
		if err := c.persistRegistriesIfDirtyLockedPanicSafe(); err != nil {
			c.registryMu.Unlock()
			return 0, nil, false, err
		}
	}
	return token, snapshot, !existed, nil
}

func (c *Core) getOrCreateLabelPersisted(label string) (uint16, error) {
	c.registryMu.Lock()
	defer c.registryMu.Unlock()

	if tok, ok := c.labels.Lookup(label); ok {
		if err := c.persistRegistriesIfDirtyLocked(); err != nil {
			return 0, err
		}
		return tok, nil
	}
	snapshot := c.labels.ExportNames()
	token, err := c.labels.GetOrCreate(label)
	if err != nil {
		return 0, err
	}
	if err := c.persistRegistries(); err != nil {
		allocated := newlyAllocatedNames(snapshot, c.labels.ExportNames())
		if len(allocated) > 0 {
			if ok, rbErr := c.labels.RollbackNames(snapshot, allocated...); rbErr != nil {
				err = fmt.Errorf("%w; additionally failed to restore label registry: %v", err, rbErr)
			} else if !ok {
				err = fmt.Errorf("%w; label registry changed before rollback", err)
			} else if persistErr := c.persistRegistries(); persistErr != nil {
				err = fmt.Errorf("%w; additionally failed to persist restored label registry: %v", err, persistErr)
			}
		}
		return 0, err
	}
	return token, nil
}

func (c *Core) lookupLabelLocked(label string) (uint16, bool) {
	c.registryMu.Lock()
	defer c.registryMu.Unlock()
	return c.labels.Lookup(label)
}

type nodeLabelTokens struct {
	primary uint16
	extras  []uint16
}

// existingLabelsOrNextProbeTokens resolves label names to tokens, minting a
// provisional PROBE token (re-stamped at apply) for any name not yet in the
// registry.
//
// Contract: the caller MUST hold c.mu (read or write). That is what fences the
// registry POINTER against the swap that import / tx-rollback perform — those
// swaps take c.mu.Lock, so a reader under c.mu.RLock observes a stable c.labels
// pointer WITHOUT needing c.registryMu (registryMu on the swap is only to
// synchronize with registryMu-only readers that lack c.mu). This lets the hot
// concurrent-ingest prepare fast path (a single ALREADY-registered label — the
// steady state after declare-on-prepare) resolve its token via the label
// registry's OWN internal RWMutex alone, keeping ALL lanes off the single
// exclusive c.registryMu that otherwise serialized every prepared node
// (ADR-0007 lever #1). Only the probe / miss / multi-label paths — which mint
// provisional tokens from Len() and so need cross-lane serialization — take
// c.registryMu.
func (c *Core) existingLabelsOrNextProbeTokens(labels []string) (nodeLabelTokens, []string, error) {
	// Fast path: one label already in the registry needs no probe allocation.
	// Lookup is safe under the registry's own RWMutex; the pointer is safe under
	// the caller's c.mu. No c.registryMu, no cross-lane serialization.
	if len(labels) == 1 {
		if tok, ok := c.labels.Lookup(labels[0]); ok {
			return nodeLabelTokens{primary: tok}, []string{labels[0]}, nil
		}
	}

	c.registryMu.Lock()
	defer c.registryMu.Unlock()

	if len(labels) == 1 {
		label := labels[0]
		if tok, ok := c.labels.Lookup(label); ok {
			return nodeLabelTokens{primary: tok}, []string{label}, nil
		}
		nextToken := c.labels.Len() + 1
		if nextToken > int(registrypkg.TokenCapacityMax) {
			return nodeLabelTokens{}, nil, fmt.Errorf("graph: label registry full (%d tokens)", registrypkg.TokenCapacityMax)
		}
		return nodeLabelTokens{primary: uint16(nextToken)}, []string{label}, nil // #nosec G115 -- bounded by TokenCapacityMax above.
	}

	nextToken := c.labels.Len() + 1
	probeByName := make(map[string]uint16)
	seen := make(map[string]struct{}, len(labels))
	canonicalLabels := make([]string, 0, len(labels))
	tokens := nodeLabelTokens{extras: make([]uint16, 0, len(labels)-1)}

	tokenFor := func(label string) (uint16, error) {
		if tok, ok := c.labels.Lookup(label); ok {
			return tok, nil
		}
		if tok, ok := probeByName[label]; ok {
			return tok, nil
		}
		if nextToken > int(registrypkg.TokenCapacityMax) {
			return 0, fmt.Errorf("graph: label registry full (%d tokens)", registrypkg.TokenCapacityMax)
		}
		tok := uint16(nextToken) // #nosec G115 -- bounded by TokenCapacityMax above.
		nextToken++
		probeByName[label] = tok
		return tok, nil
	}

	for i, label := range labels {
		tok, err := tokenFor(label)
		if err != nil {
			return nodeLabelTokens{}, nil, err
		}
		if _, ok := seen[label]; ok {
			continue
		}
		seen[label] = struct{}{}
		canonicalLabels = append(canonicalLabels, label)
		if i == 0 {
			tokens.primary = tok
		} else {
			tokens.extras = append(tokens.extras, tok)
		}
	}
	if tokens.primary == 0 {
		return nodeLabelTokens{}, nil, fmt.Errorf("graph: batch primary label: %w", ErrEmptyName)
	}
	return tokens, canonicalLabels, nil
}

func (c *Core) getOrCreateLabelsWithSnapshot(labels []string) (uint16, []uint16, []string, []string, bool, error) {
	c.registryMu.Lock()
	primaryToken, ok := c.labels.Lookup(labels[0])
	if ok {
		extraTokens := make([]uint16, 0, len(labels)-1)
		allExisting := true
		for _, label := range labels[1:] {
			tok, exists := c.labels.Lookup(label)
			if !exists {
				allExisting = false
				break
			}
			extraTokens = append(extraTokens, tok)
		}
		if allExisting {
			if err := c.persistRegistriesIfDirtyLockedPanicSafe(); err != nil {
				c.registryMu.Unlock()
				return 0, nil, nil, nil, false, err
			}
			c.registryMu.Unlock()
			return primaryToken, extraTokens, nil, nil, false, nil
		}
	}

	snapshot := c.labels.ExportNames()
	fail := func(err error) (uint16, []uint16, []string, []string, bool, error) {
		allocated := newlyAllocatedNames(snapshot, c.labels.ExportNames())
		if len(allocated) > 0 {
			if ok, rbErr := c.labels.RollbackNames(snapshot, allocated...); rbErr != nil {
				err = fmt.Errorf("%w; additionally failed to restore label registry: %v", err, rbErr)
			} else if !ok {
				err = fmt.Errorf("%w; label registry changed before rollback", err)
			}
		}
		c.registryMu.Unlock()
		return 0, nil, nil, nil, false, err
	}

	primaryToken, err := c.labels.GetOrCreate(labels[0])
	if err != nil {
		return fail(fmt.Errorf("graph: primary label: %w", err))
	}

	extraTokens := make([]uint16, 0, len(labels)-1)
	for _, label := range labels[1:] {
		tok, err := c.labels.GetOrCreate(label)
		if err != nil {
			return fail(fmt.Errorf("graph: extra label %q: %w", label, err))
		}
		extraTokens = append(extraTokens, tok)
	}

	allocated := newlyAllocatedNames(snapshot, c.labels.ExportNames())
	if len(allocated) == 0 {
		c.registryMu.Unlock()
		return primaryToken, extraTokens, nil, nil, false, nil
	}
	return primaryToken, extraTokens, snapshot, allocated, true, nil
}

func (c *Core) getOrCreateBatchNodeLabelsWithSnapshot(nodes []pendingNode) ([]nodeLabelTokens, []string, []string, bool, error) {
	c.registryMu.Lock()
	snapshot := c.labels.ExportNames()
	out := make([]nodeLabelTokens, len(nodes))
	var previousLabels []string
	var previousTokens nodeLabelTokens
	fail := func(err error) ([]nodeLabelTokens, []string, []string, bool, error) {
		allocated := newlyAllocatedNames(snapshot, c.labels.ExportNames())
		if len(allocated) > 0 {
			if ok, rbErr := c.labels.RollbackNames(snapshot, allocated...); rbErr != nil {
				err = fmt.Errorf("%w; additionally failed to restore label registry: %v", err, rbErr)
			} else if !ok {
				err = fmt.Errorf("%w; label registry changed before rollback", err)
			}
		}
		c.registryMu.Unlock()
		return nil, nil, nil, false, err
	}

	for i, pn := range nodes {
		if len(pn.labels) == 0 {
			return fail(ErrNoLabels)
		}
		if slices.Equal(previousLabels, pn.labels) {
			out[i] = previousTokens
			continue
		}
		primary, err := c.labels.GetOrCreate(pn.labels[0])
		if err != nil {
			return fail(fmt.Errorf("graph: batch primary label: %w", err))
		}
		out[i].primary = primary
		if len(pn.labels) > 1 {
			out[i].extras = make([]uint16, 0, len(pn.labels)-1)
		}
		for _, label := range pn.labels[1:] {
			tok, err := c.labels.GetOrCreate(label)
			if err != nil {
				return fail(fmt.Errorf("graph: batch extra label %q: %w", label, err))
			}
			out[i].extras = append(out[i].extras, tok)
		}
		previousLabels = pn.labels
		previousTokens = out[i]
	}

	allocated := newlyAllocatedNames(snapshot, c.labels.ExportNames())
	if len(allocated) == 0 {
		if err := c.persistRegistriesIfDirtyLockedPanicSafe(); err != nil {
			c.registryMu.Unlock()
			return nil, nil, nil, false, err
		}
		c.registryMu.Unlock()
		return out, nil, nil, false, nil
	}
	return out, snapshot, allocated, true, nil
}

func (c *Core) getOrCreateRelTypePersisted(typeName string) (uint16, error) {
	if !c.registryDirty.Load() {
		if tok, ok := c.cachedRelType(typeName); ok {
			return tok, nil
		}
	}
	c.registryMu.Lock()
	defer c.registryMu.Unlock()

	if tok, ok := c.relTypes.Lookup(typeName); ok {
		if err := c.persistRegistriesIfDirtyLocked(); err != nil {
			return 0, err
		}
		c.rememberRelType(typeName, tok)
		return tok, nil
	}

	snapshot := c.relTypes.ExportNames()
	token, err := c.relTypes.GetOrCreate(typeName)
	if err != nil {
		return 0, err
	}
	if err := c.persistRegistries(); err != nil {
		allocated := newlyAllocatedNames(snapshot, c.relTypes.ExportNames())
		if len(allocated) > 0 {
			if ok, rbErr := c.relTypes.RollbackNames(snapshot, allocated...); rbErr != nil {
				err = fmt.Errorf("%w; additionally failed to restore reltype registry: %v", err, rbErr)
			} else if !ok {
				err = fmt.Errorf("%w; reltype registry changed before rollback", err)
			} else if persistErr := c.persistRegistries(); persistErr != nil {
				err = fmt.Errorf("%w; additionally failed to persist restored reltype registry: %v", err, persistErr)
			}
		}
		return 0, err
	}
	c.rememberRelType(typeName, token)
	return token, nil
}

func (c *Core) lookupRelTypeLocked(typeName string) (uint16, bool) {
	c.registryMu.Lock()
	defer c.registryMu.Unlock()
	return c.relTypes.Lookup(typeName)
}

func (c *Core) existingRelTypeOrNextProbeToken(typeName string) (uint16, error) {
	c.registryMu.Lock()
	defer c.registryMu.Unlock()
	typeToken, ok := c.relTypes.Lookup(typeName)
	if ok {
		return typeToken, nil
	}
	nextTypeToken := c.relTypes.Len() + 1
	if nextTypeToken > int(registrypkg.TokenCapacityMax) {
		return 0, fmt.Errorf("graph: reltype registry full (%d tokens)", registrypkg.TokenCapacityMax)
	}
	return uint16(nextTypeToken), nil // #nosec G115 -- bounded by TokenCapacityMax above.
}

func (c *Core) getOrCreateRelTypeWithSnapshot(typeName string) (uint16, []string, bool, error) {
	if !c.registryDirty.Load() {
		if tok, ok := c.cachedRelType(typeName); ok {
			return tok, nil, false, nil
		}
	}
	c.registryMu.Lock()
	if typeToken, existed := c.relTypes.Lookup(typeName); existed {
		if err := c.persistRegistriesIfDirtyLockedPanicSafe(); err != nil {
			c.registryMu.Unlock()
			return 0, nil, false, err
		}
		c.registryMu.Unlock()
		c.rememberRelType(typeName, typeToken)
		return typeToken, nil, false, nil
	}
	snapshot := c.relTypes.ExportNames()
	typeToken, err := c.relTypes.GetOrCreate(typeName)
	if err != nil {
		c.registryMu.Unlock()
		return 0, nil, false, err
	}
	return typeToken, snapshot, true, nil
}

func (c *Core) cachedRelType(typeName string) (uint16, bool) {
	c.relTypeCacheMu.RLock()
	tok, ok := c.relTypeCache[typeName]
	c.relTypeCacheMu.RUnlock()
	return tok, ok && tok != 0
}

func (c *Core) rememberRelType(typeName string, tok uint16) {
	if tok != 0 {
		c.relTypeCacheMu.Lock()
		if c.relTypeCache == nil {
			c.relTypeCache = make(map[string]uint16)
		}
		c.relTypeCache[typeName] = tok
		c.relTypeCacheMu.Unlock()
	}
}

func (c *Core) clearRelTypeCache() {
	c.relTypeCacheMu.Lock()
	if c.relTypeCache == nil {
		c.relTypeCache = make(map[string]uint16)
	} else {
		clear(c.relTypeCache)
	}
	c.relTypeCacheMu.Unlock()
}

func (c *Core) restoreNewLabelOnError(snapshot []string, allocated bool, label string, err error) error {
	allocatedLabels := []string(nil)
	if allocated {
		allocatedLabels = []string{label}
	}
	return c.restoreNewLabelsOnError(snapshot, allocatedLabels, err)
}

func (c *Core) restoreNewLabelsOnError(snapshot, allocated []string, err error) error {
	defer c.registryMu.Unlock()
	if len(allocated) == 0 {
		return err
	}
	if err == nil {
		return c.persistRegistries()
	}
	// When the change-log is on, the token registries are APPEND-ONLY across an
	// error rollback (lesson 51/55): a label this op allocated may already be
	// referenced by a durable change-log record — most acutely a partial BATCH,
	// which emits ChangeNodePut for the nodes it created, then this path runs the
	// delete cleanup AND would de-allocate the label. De-allocating poisons the
	// feed (a replica, even with a refetch source, can never resolve the token —
	// the primary rolled it back too) and the number gets reused for a different
	// name. The token is already persisted (allocated via getOrCreate*Persisted)
	// and an allocated-but-unused token is harmless, so keep it.
	if c.changeLogEnabled {
		return err
	}
	if ok, rbErr := c.labels.RollbackNames(snapshot, allocated...); rbErr != nil {
		return fmt.Errorf("%w; additionally failed to restore label registry: %v", err, rbErr)
	} else if !ok {
		return err
	}
	if persistErr := c.persistRegistries(); persistErr != nil {
		return fmt.Errorf("%w; additionally failed to persist restored label registry: %v", err, persistErr)
	}
	return err
}

func (c *Core) restoreNewRelTypeOnError(snapshot []string, allocated bool, typeName string, err error) error {
	if snapshot == nil {
		return err
	}
	defer c.registryMu.Unlock()
	if !allocated {
		return err
	}
	if err == nil {
		return c.persistRegistries()
	}
	// Append-only across rollback when the change-log is on — a rel-type this op
	// allocated may already be referenced by a durable change-log record; see
	// restoreNewLabelsOnError. Keep the (already-persisted) token.
	if c.changeLogEnabled {
		return err
	}
	if ok, rbErr := c.relTypes.RollbackNames(snapshot, typeName); rbErr != nil {
		return fmt.Errorf("%w; additionally failed to restore reltype registry: %v", err, rbErr)
	} else if !ok {
		return err
	}
	if persistErr := c.persistRegistries(); persistErr != nil {
		return fmt.Errorf("%w; additionally failed to persist restored reltype registry: %v", err, persistErr)
	}
	return err
}

// rollbackLabelsIfUnreferenced reclaims the given newly-allocated label names
// (obtained via GetOrCreate against `snapshot`) via RollbackNames — UNLESS a
// concurrently-persisted node has already adopted one of the resulting
// tokens (BACKLOG 11b — currently wired ONLY into GraphTx.restoreRegistries;
// see the caveat below for why it is NOT a universal drop-in replacement for
// every direct RollbackNames call). Returns RollbackNames' own (ok, err):
// ok=false with err=nil means nothing was mutated (either the registry
// drifted from the expected snapshot+allocated shape, or a token turned out
// to be referenced) — the caller's existing "rollback declined, keep the
// original error" handling applies unchanged either way.
//
// Registry Lookup is visible cross-goroutine independent of registryMu — by
// design, so the ADR-0007 hot concurrent-ingest path can resolve an
// already-declared label via the registry's own internal RWMutex alone
// without serializing on the single exclusive registryMu
// (existingLabelsOrNextProbeTokens). That means a concurrent writer can
// Lookup and persist an entity referencing a token THIS call just allocated,
// before this call's own operation fails and tries to undo the allocation —
// even though registry MUTATIONS (GetOrCreate) are themselves serialized via
// registryMu (every GetOrCreate call site takes it first). De-allocating a
// referenced token would leave that concurrent entity's label dangling: the
// next distinct name allocated anywhere reuses the freed token number,
// silently reassigning the entity's label. So each newly-allocated token is
// reclaimed only when NO current node references it (checked via the O(1)
// label-count stat, under the SAME registryMu critical section the caller
// already holds — no concurrent MUTATION can be interleaving, only a prior
// Lookup+persist that already landed). A referenced token is left registered
// rather than risking corruption; the (rare) leaked registry slot is a
// strictly better failure mode than a silently mis-labeled entity. Caller
// must hold c.registryMu.
//
// CAVEAT — a "referenced" token is not proof of a legitimate concurrent
// adopter: it could instead be a PRE-EXISTING row that already carried the
// (brand-new, about-to-be-freed) token number BEFORE this call's own
// allocation ever ran — reachable only via a corrupt/untrusted store
// (TestAddNodeLabel_CorruptFutureTokenRollsBackRegistry seeds exactly this:
// a node holding a raw label bit for a token that isn't registered to any
// name yet). This function cannot distinguish that case from a genuine
// concurrent adopter without a "reference count immediately before this
// call's own allocation" baseline, which the OTHER registry_rollback.go call
// sites (getOrCreateLabelPersisted and friends, restoreNewLabelsOnError) do
// not currently capture — so those sites intentionally keep calling
// RollbackNames directly rather than through this guard. GraphTx.Rollback is
// different: by the time restoreRegistries runs, steps 1-7 have already
// deleted every entity this tx itself created, so a nonzero count found here
// can only be a genuinely concurrent writer's entity, never this tx's own —
// the corrupt-pre-existing-row ambiguity does not apply to that call site.
func (c *Core) rollbackLabelsIfUnreferenced(snapshot, allocated []string) (bool, error) {
	if len(allocated) == 0 {
		return true, nil
	}
	referenced, err := c.anyTokenReferenced(len(snapshot), len(allocated), c.store.NodeCountByLabel)
	if err != nil {
		return false, err
	}
	if referenced {
		return false, nil
	}
	return c.labels.RollbackNames(snapshot, allocated...)
}

// rollbackRelTypesIfUnreferenced is the relationship-type mirror of
// rollbackLabelsIfUnreferenced.
func (c *Core) rollbackRelTypesIfUnreferenced(snapshot, allocated []string) (bool, error) {
	if len(allocated) == 0 {
		return true, nil
	}
	referenced, err := c.anyTokenReferenced(len(snapshot), len(allocated), c.store.RelCountByType)
	if err != nil {
		return false, err
	}
	if referenced {
		return false, nil
	}
	return c.relTypes.RollbackNames(snapshot, allocated...)
}

// anyTokenReferenced reports whether any of the `count` newly-allocated token
// numbers starting at `base` (registries are contiguous and append-only: a
// name's token number is fixed at its ExportNames() index for life) currently
// has any live members, via the given counter (StatsCapability.NodeCountByLabel
// / RelCountByType — an O(1) maintained counter, not a scan). A counter error
// is treated as "referenced" (fail safe: leaves the token registered rather
// than risking a corrupt reclaim) and propagated to the caller for visibility.
func (c *Core) anyTokenReferenced(base, count int, counter func(uint16) (int, error)) (bool, error) {
	for i := 0; i < count; i++ {
		tok := uint16(base + i) // #nosec G115 — bounded by registry TokenCapacityMax
		n, err := counter(tok)
		if err != nil {
			return true, err
		}
		if n > 0 {
			return true, nil
		}
	}
	return false, nil
}

func newlyAllocatedNames(before, after []string) []string {
	if len(after) <= len(before) {
		return nil
	}
	if len(before) == 0 {
		cp := make([]string, len(after))
		copy(cp, after)
		return cp
	}
	for i := range before {
		if i >= len(after) || after[i] != before[i] {
			return nil
		}
	}
	cp := make([]string, len(after)-len(before))
	copy(cp, after[len(before):])
	return cp
}
