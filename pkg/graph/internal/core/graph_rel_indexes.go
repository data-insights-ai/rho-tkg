package core

import (
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// --- Relationship property indexes (K3b) ---
//
// The relationship mirror of graph_indexes.go's CreateProperty / DeleteProperty,
// keyed by rel type instead of label. Backed by the optional
// store.RelPropertyIndexCapability; the tiered store implements it but declines
// index creation with store.ErrRelPropertyIndexUnsupported (rel values are
// scattered across timestamp-routed event shards, so a shard-local index cannot
// answer them). RelsByTypeAndProperty still works on every backend via the
// graph-layer type-scan fallback.

// CreateRelProperty creates a relationship property index on the given rel type
// and property key. Resolves or creates the rel-type token so the index applies
// to future matching relationships. Returns store.ErrIndexExists if the index
// already exists, store.ErrRelPropertyIndexUnsupported on the tiered store.
func (i *IndexOps) CreateRelProperty(typeName, propertyKey string) error {
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
		cap, err := c.relPropertyIndexCap()
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
					fmt.Errorf("panic during relationship property index create"),
					func() error { return cap.DropRelPropertyIndex(tok, propertyKey) },
					storepkg.ErrIndexNotFound,
					storepkg.ErrIndexExists,
				)
			}
		}()
		err = c.restoreNewRelTypeIndexOnError(snapshot, allocated, typeName,
			cap.CreateRelPropertyIndex(tok, propertyKey),
			func() error { return cap.DropRelPropertyIndex(tok, propertyKey) },
			storepkg.ErrIndexNotFound,
			storepkg.ErrIndexExists,
		)
		typeFinished = true
		return err
	})
}

// DeleteRelProperty removes a relationship property index.
// Resolves the rel-type name to a token. Returns store.ErrIndexNotFound if the
// index does not exist.
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
		return cap.DropRelPropertyIndex(tok, propertyKey)
	})
}

// restoreNewRelTypeIndexOnError is the rel-type mirror of
// restoreNewLabelIndexOnError — it reconciles a freshly-allocated rel-type
// token with the outcome of the index create it was allocated for. On success
// it persists the registry; on failure it best-effort-drops the partial index
// and rolls the token back. The registry lock is HELD on entry iff the token
// was freshly allocated (snapshot != nil); this method always releases it in
// that case (via restoreNewRelTypeOnError).
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
