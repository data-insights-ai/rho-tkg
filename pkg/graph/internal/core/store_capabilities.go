package core

import (
	"fmt"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
)

// Capability accessors — type-assert the store field against an optional
// capability and surface ErrCapabilityNotSupported (with a diagnostic
// message) when the underlying backend does not implement it.
//
// Core's `store` field is typed as MandatoryStore so out-of-tree backends
// that satisfy only the mandatory capabilities can still be plugged in;
// their consumers must accept ErrCapabilityNotSupported on the optional
// surfaces. In-tree backends (memory.Store, badger.Store, tiered.Store)
// satisfy every capability, so these assertions always succeed in the
// reference configuration.

func (c *Core) propertyIndexCap() (storepkg.PropertyIndexCapability, error) {
	cap, ok := c.store.(storepkg.PropertyIndexCapability)
	if !ok {
		return nil, fmt.Errorf("%w: PropertyIndexCapability", storepkg.ErrCapabilityNotSupported)
	}
	return cap, nil
}

func (c *Core) temporalIndexCap() (storepkg.TemporalIndexCapability, error) {
	cap, ok := c.store.(storepkg.TemporalIndexCapability)
	if !ok {
		return nil, fmt.Errorf("%w: TemporalIndexCapability", storepkg.ErrCapabilityNotSupported)
	}
	return cap, nil
}

func (c *Core) vectorIndexCap() (storepkg.VectorIndexCapability, error) {
	cap, ok := c.store.(storepkg.VectorIndexCapability)
	if !ok {
		return nil, fmt.Errorf("%w: VectorIndexCapability", storepkg.ErrCapabilityNotSupported)
	}
	return cap, nil
}

func (c *Core) highFrequencyIndexCap() (storepkg.HighFrequencyIndexCapability, error) {
	cap, ok := c.store.(storepkg.HighFrequencyIndexCapability)
	if !ok {
		return nil, fmt.Errorf("%w: HighFrequencyIndexCapability", storepkg.ErrCapabilityNotSupported)
	}
	return cap, nil
}
