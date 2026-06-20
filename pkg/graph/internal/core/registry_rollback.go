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

func (c *Core) existingLabelsOrNextProbeTokens(labels []string) (nodeLabelTokens, []string, error) {
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
