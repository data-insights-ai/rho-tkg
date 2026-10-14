package core

import (
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// --- Relationship property indexes (K3b) ---
//
// Rel mirror of CreateProperty / DeleteProperty, keyed by rel type, backed by
// store.RelPropertyIndexCapability. The tiered store declines creation with
// store.ErrRelPropertyIndexUnsupported (rel values are scattered across
// timestamp-routed shards); RelsByTypeAndProperty still works everywhere via the
// graph-layer type scan.

// CreateRelProperty creates a relationship property index on (rel type,
// property key), resolving or creating the rel-type token. Returns
// store.ErrIndexExists, or store.ErrRelPropertyIndexUnsupported on the tiered store.
func (i *IndexOps) CreateRelProperty(typeName, propertyKey string) error {
	return i.CreateRelPropertyWithOptions(typeName, propertyKey, storepkg.PropertyIndexOptions{})
}

// CreateRelPropertyWithOptions is CreateRelProperty with
// storepkg.PropertyIndexOptions; zero options equal CreateRelProperty. Non-zero
// options without storepkg.RelPropertyIndexOptionsCapability return
// ErrCapabilityNotSupported before anything is created.
func (i *IndexOps) CreateRelPropertyWithOptions(typeName, propertyKey string, opts storepkg.PropertyIndexOptions) error {
	c := i.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	if err := storepkg.ValidatePropertyIndexOptions(opts); err != nil {
		return err
	}
	return c.readUnderRLock(func() error {
		if err := c.validateIndexName(typeName); err != nil {
			return err
		}
		if err := c.validateIndexPropertyKey(propertyKey); err != nil {
			return err
		}
		cap, err := c.relPropertyIndexCap()
		if err != nil {
			return err
		}
		create := func(tok uint16) error { return cap.CreateRelPropertyIndex(tok, propertyKey) }
		if opts != (storepkg.PropertyIndexOptions{}) {
			oc, ok := c.store.(storepkg.RelPropertyIndexOptionsCapability)
			if !ok {
				return fmt.Errorf("%w: RelPropertyIndexOptionsCapability", storepkg.ErrCapabilityNotSupported)
			}
			create = func(tok uint16) error { return oc.CreateRelPropertyIndexWithOptions(tok, propertyKey, opts) }
		}
		tok, snapshot, allocated, err := c.getOrCreateRelTypeWithSnapshot(typeName)
		if err != nil {
			return err
		}
		typeFinished := false
		defer func() {
			if !typeFinished {
				_ = c.restoreNewRelTypeIndexOnError(snapshot, allocated, typeName,
					fmt.Errorf("panic during relationship property index create"),
					func() error { return c.indexDDL(cap.DropRelPropertyIndex(tok, propertyKey)) },
					storepkg.ErrIndexNotFound,
					storepkg.ErrIndexExists,
				)
			}
		}()
		err = c.restoreNewRelTypeIndexOnError(snapshot, allocated, typeName,
			c.indexDDL(create(tok)),
			func() error { return c.indexDDL(cap.DropRelPropertyIndex(tok, propertyKey)) },
			storepkg.ErrIndexNotFound,
			storepkg.ErrIndexExists,
		)
		typeFinished = true
		return err
	})
}

// DeleteRelProperty removes a relationship property index; store.ErrIndexNotFound
// if none.
func (i *IndexOps) DeleteRelProperty(typeName, propertyKey string) error {
	c := i.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	return c.readUnderRLock(func() error {
		if err := c.validateIndexName(typeName); err != nil {
			return err
		}
		if err := c.validateIndexPropertyKey(propertyKey); err != nil {
			return err
		}
		tok, ok := c.relTypes.Lookup(typeName)
		if !ok {
			return storepkg.ErrIndexNotFound
		}
		cap, err := c.relPropertyIndexCap()
		if err != nil {
			return err
		}
		return c.indexDDL(cap.DropRelPropertyIndex(tok, propertyKey))
	})
}

// --- Relationship-type temporal indexes (BACKLOG 21c) ---
//
// Rel mirror of CreateTemporal / DeleteTemporal via
// store.RelTypeTemporalIndexCapability (memory, badger; sharded and tiered build
// it on every shard, backlog 10). Without it CreateRelTemporal returns
// storepkg.ErrCapabilityNotSupported.

// CreateRelTemporal creates a temporal interval index on relationships of the rel
// type, resolving or creating the token. Returns storepkg.ErrTemporalIndexExists.
func (i *IndexOps) CreateRelTemporal(typeName string) error {
	c := i.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	return c.readUnderRLock(func() error {
		if err := c.validateIndexName(typeName); err != nil {
			return err
		}
		cap, err := c.relTypeTemporalIndexCap()
		if err != nil {
			return err
		}
		tok, snapshot, allocated, err := c.getOrCreateRelTypeWithSnapshot(typeName)
		if err != nil {
			return err
		}
		typeFinished := false
		defer func() {
			if !typeFinished {
				_ = c.restoreNewRelTypeIndexOnError(snapshot, allocated, typeName,
					fmt.Errorf("panic during relationship temporal index create"),
					func() error { return c.indexDDL(cap.DropRelTemporalIndex(tok)) },
					storepkg.ErrTemporalIndexNotFound,
					storepkg.ErrTemporalIndexExists,
				)
			}
		}()
		err = c.restoreNewRelTypeIndexOnError(snapshot, allocated, typeName,
			c.indexDDL(cap.CreateRelTemporalIndex(tok)),
			func() error { return c.indexDDL(cap.DropRelTemporalIndex(tok)) },
			storepkg.ErrTemporalIndexNotFound,
			storepkg.ErrTemporalIndexExists,
		)
		typeFinished = true
		return err
	})
}

// DeleteRelTemporal removes a rel type's temporal index;
// storepkg.ErrTemporalIndexNotFound if none.
func (i *IndexOps) DeleteRelTemporal(typeName string) error {
	c := i.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	return c.readUnderRLock(func() error {
		if err := c.validateIndexName(typeName); err != nil {
			return err
		}
		tok, ok := c.relTypes.Lookup(typeName)
		if !ok {
			return storepkg.ErrTemporalIndexNotFound
		}
		cap, err := c.relTypeTemporalIndexCap()
		if err != nil {
			return err
		}
		return c.indexDDL(cap.DropRelTemporalIndex(tok))
	})
}

// restoreNewRelTypeIndexOnError is the rel-type mirror of
// restoreNewLabelIndexOnError: on success it persists the registry, on failure it
// best-effort-drops the partial index and rolls the token back. The registry lock
// is held on entry iff the token was freshly allocated (snapshot != nil) and is
// always released here (via restoreNewRelTypeOnError).
func (c *Core) restoreNewRelTypeIndexOnError(snapshot []string, allocated bool, typeName string, err error, cleanup func() error, notFound, exists error) error {
	if err != nil && cleanup != nil {
		if !allocated && exists != nil && errors.Is(err, exists) {
			return c.restoreNewRelTypeOnError(snapshot, allocated, typeName, err)
		}
		if cleanupErr := runRollbackCleanup(cleanup); cleanupErr != nil && (notFound == nil || !errors.Is(cleanupErr, notFound)) {
			err = fmt.Errorf("%w; additionally failed to remove partial index for rolled-back rel type %q: %v", err, typeName, cleanupErr)
			if allocated {
				if persistErr := c.persistRegistries(); persistErr != nil {
					err = fmt.Errorf("%w; additionally failed to persist retained reltype registry after partial index cleanup failure: %v", err, persistErr)
				}
				c.registryMu.Unlock()
				return err
			}
		}
	}
	if err == nil && allocated {
		if persistErr := c.persistRegistries(); persistErr != nil {
			err = persistErr
			if cleanup != nil {
				if cleanupErr := runRollbackCleanup(cleanup); cleanupErr != nil && (notFound == nil || !errors.Is(cleanupErr, notFound)) {
					err = fmt.Errorf("%w; additionally failed to remove partial index for rolled-back rel type %q: %v", err, typeName, cleanupErr)
					if persistRetainedErr := c.persistRegistries(); persistRetainedErr != nil {
						err = fmt.Errorf("%w; additionally failed to persist retained reltype registry after partial index cleanup failure: %v", err, persistRetainedErr)
					}
					c.registryMu.Unlock()
					return err
				}
			}
			return c.restoreNewRelTypeOnError(snapshot, allocated, typeName, err)
		}
		c.registryMu.Unlock()
		return nil
	}
	return c.restoreNewRelTypeOnError(snapshot, allocated, typeName, err)
}
