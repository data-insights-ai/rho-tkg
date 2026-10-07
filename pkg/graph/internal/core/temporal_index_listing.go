package core

import (
	"fmt"
	"slices"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// ListTemporal returns the labels that carry a temporal interval index,
// sorted by name (a high-frequency index does not count, as for HasTemporal).
// Backends without the listing return store.ErrCapabilityNotSupported.
func (i *IndexOps) ListTemporal() ([]string, error) {
	c := i.c
	var out []string
	err := c.readUnderRLock(func() error {
		lister, ok := c.store.(storepkg.TemporalIndexListingCapability)
		if !ok {
			return fmt.Errorf("%w: TemporalIndexListingCapability", storepkg.ErrCapabilityNotSupported)
		}
		toks, err := lister.TemporalIndexLabels()
		if err != nil {
			return err
		}
		for _, tok := range toks {
			if name := c.labels.Resolve(tok); name != "" {
				out = append(out, name)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.Sort(out)
	return out, nil
}

// ListRelTemporal returns the relationship types that carry a temporal
// interval index (CreateRelTemporal), sorted by name; none on a store without
// relationship-type temporal indexes (tiered). Backends without the listing
// return store.ErrCapabilityNotSupported.
func (i *IndexOps) ListRelTemporal() ([]string, error) {
	c := i.c
	var out []string
	err := c.readUnderRLock(func() error {
		lister, ok := c.store.(storepkg.TemporalIndexListingCapability)
		if !ok {
			return fmt.Errorf("%w: TemporalIndexListingCapability", storepkg.ErrCapabilityNotSupported)
		}
		toks, err := lister.RelTemporalIndexTypes()
		if err != nil {
			return err
		}
		for _, tok := range toks {
			if name := c.relTypes.Resolve(tok); name != "" {
				out = append(out, name)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.Sort(out)
	return out, nil
}

// HasRelTemporal reports whether a temporal interval index exists on the
// relationship type, the mirror of HasTemporal. Unregistered types return
// false. Backends without the listing return store.ErrCapabilityNotSupported.
func (i *IndexOps) HasRelTemporal(typeName string) (bool, error) {
	c := i.c
	var out bool
	err := c.readUnderRLock(func() error {
		if err := c.validateIndexName(typeName); err != nil {
			return err
		}
		lister, ok := c.store.(storepkg.TemporalIndexListingCapability)
		if !ok {
			return fmt.Errorf("%w: TemporalIndexListingCapability", storepkg.ErrCapabilityNotSupported)
		}
		tok, found := c.relTypes.Lookup(typeName)
		if !found {
			return nil
		}
		toks, err := lister.RelTemporalIndexTypes()
		if err != nil {
			return err
		}
		_, out = slices.BinarySearch(toks, tok)
		return nil
	})
	return out, err
}
