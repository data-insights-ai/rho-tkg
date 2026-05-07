package graph

import (
	"errors"
	"fmt"
	"strings"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/locks"
)

// registriesPersister is the optional interface implemented by Store
// backends that can persist both the label and reltype registries atomically.
// Both BadgerStore (single-txn) and TieredStore (single registry-file write)
// satisfy this interface; MemoryStore does not need to.
type registriesPersister interface {
	SaveRegistries(*indexpkg.LabelRegistry, *indexpkg.RelTypeRegistry) error
}

// New creates a new Graph with the given configuration.
// Returns an error if SnowflakeNodeID is out of range (0-15).
// The ID is mapped to an even/odd pair (ID*2 for nodes, ID*2+1 for rels)
// to guarantee value-level uniqueness across entity types.
//
// Store selection priority:
//  1. config.Store (explicit injection)
//  2. BadgerStore (if BadgerDir or BadgerInMemory is set)
//  3. MemoryStore (default)
//
// When a BadgerStore is created, registries are loaded from persisted data.
// Call Close() when done to save registries and close the store.
func New(config Config) (*Graph, error) {
	if config.SnowflakeNodeID < 0 || config.SnowflakeNodeID > 15 {
		return nil, fmt.Errorf("graph: SnowflakeNodeID must be 0-15, got %d", config.SnowflakeNodeID)
	}

	nodeGen, err := snowflake.NewNode(config.SnowflakeNodeID*2,
		snowflake.WithEpoch(snowflakeEpoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		return nil, fmt.Errorf("graph: node ID generator: %w", err)
	}
	relGen, err := snowflake.NewNode(config.SnowflakeNodeID*2+1,
		snowflake.WithEpoch(snowflakeEpoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		return nil, fmt.Errorf("graph: rel ID generator: %w", err)
	}

	// Resolve zero validation limits to defaults.
	v := config.Validation
	if v.MaxLabelsPerNode == 0 {
		v.MaxLabelsPerNode = defaultMaxLabelsPerNode
	}
	if v.MaxPropertiesPerEntity == 0 {
		v.MaxPropertiesPerEntity = defaultMaxPropertiesPerEntity
	}
	if v.MaxPropertyKeyLength == 0 {
		v.MaxPropertyKeyLength = defaultMaxPropertyKeyLength
	}
	if v.MaxPropertyValueSize == 0 {
		v.MaxPropertyValueSize = defaultMaxPropertyValueSize
	}
	if v.MaxNameLength == 0 {
		v.MaxNameLength = defaultMaxNameLength
	}

	if v.MaxLabelsPerNode < 0 || v.MaxPropertiesPerEntity < 0 ||
		v.MaxPropertyKeyLength < 0 || v.MaxPropertyValueSize < 0 ||
		v.MaxNameLength < 0 {
		return nil, fmt.Errorf("graph: validation limits must not be negative")
	}

	g := &Graph{
		labels:         indexpkg.NewLabelRegistry(),
		relTypes:       indexpkg.NewRelTypeRegistry(),
		nodeIDGen:      nodeGen,
		relIDGen:       relGen,
		entityLocks:    locks.NewManager(),
		validation:     v,
		indexProviders: make(map[string]*indexProviderEntry),
	}

	// Validate BadgerDir: reject whitespace-only strings (silent fallback hazard).
	if config.Store == nil && config.BadgerDir != "" {
		if strings.TrimSpace(config.BadgerDir) == "" {
			return nil, fmt.Errorf("graph: BadgerDir is whitespace-only; use a valid path or omit for MemoryStore")
		}
	}

	store := config.Store
	if store == nil {
		if config.BadgerDir != "" || config.BadgerInMemory {
			bs, err := NewBadgerStore(BadgerStoreConfig{
				Dir:                  config.BadgerDir,
				InMemory:             config.BadgerInMemory,
				SyncWrites:           config.SyncWrites,
				Compression:          config.Compression,
				ZSTDCompressionLevel: config.ZSTDCompressionLevel,
			})
			if err != nil {
				return nil, fmt.Errorf("graph: badger store: %w", err)
			}

			// Load persisted registries. Fail fast if the saved data is corrupt.
			if _, err := bs.LoadLabelRegistry(g.labels); err != nil {
				_ = bs.Close() // best-effort cleanup; returning primary error
				return nil, fmt.Errorf("graph: load label registry: %w", err)
			}
			if _, err := bs.LoadRelTypeRegistry(g.relTypes); err != nil {
				_ = bs.Close() // best-effort cleanup; returning primary error
				return nil, fmt.Errorf("graph: load reltype registry: %w", err)
			}

			store = bs
		} else {
			store = NewMemoryStore()
		}
	}

	g.store = store

	// Wire TieredStore to the label registry for ontology token resolution.
	if ts, ok := store.(*TieredStore); ok {
		ts.SetLabelRegistry(g.labels)
		if _, err := ts.LoadLabelRegistry(g.labels); err != nil {
			_ = ts.Close() // best-effort cleanup; returning primary error
			return nil, fmt.Errorf("graph: load label registry: %w", err)
		}
		if _, err := ts.LoadRelTypeRegistry(g.relTypes); err != nil {
			_ = ts.Close() // best-effort cleanup; returning primary error
			return nil, fmt.Errorf("graph: load reltype registry: %w", err)
		}
	}

	return g, nil
}

// Close saves registries (if Badger) and closes the underlying store.
// Safe to call concurrently and multiple times.
//
// store.Close() always runs even if registry saves fail — prevents resource leaks.
// Returns all errors joined; subsequent calls return nil.
func (g *Graph) Close() error {
	var closeErr error
	g.closeOnce.Do(func() {
		// Close index providers before the store so they can flush their
		// own state. Errors are collected; store close still runs.
		closeErr = errors.Join(closeErr, g.closeIndexProviders())

		// Save registries if the store supports atomic persistence.
		// Both BadgerStore and TieredStore satisfy registriesPersister; the
		// type-assertion lets us go through a single uniform path that writes
		// label and reltype registries atomically.
		if rp, ok := g.store.(registriesPersister); ok {
			if err := rp.SaveRegistries(g.labels, g.relTypes); err != nil {
				closeErr = errors.Join(closeErr, fmt.Errorf("graph: save registries: %w", err))
			}
		}
		// Always close the store — even if registry saves failed.
		closeErr = errors.Join(closeErr, g.store.Close())
	})
	return closeErr
}
