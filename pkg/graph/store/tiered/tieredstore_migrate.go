package tiered

import (
	"errors"
	"fmt"
	"os"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// MigrateFromBadger copies all nodes and relationships from a single BadgerStore
// into a Store. Entities are routed by the Store's ontology — reference
// labels go to refShard, event labels go to the hot event shard.
//
// Uses paginated iteration (ForEachNodeID + GetNode) instead of materializing all
// entities into memory, making it safe for large graphs.
//
// No history migration: hash chains would need re-creation. This handles the 95%
// case of migrating a single-BadgerStore deployment to tiered layout.
//
// The label registry is loaded from src and wired to dst before migration so
// ontology routing sees the same token mapping used by src. Both source
// registries are saved to dst after a successful entity copy.
func MigrateFromBadger(src *BadgerStore, dst *Store) error {
	if src == nil {
		return fmt.Errorf("%w: nil source badger store", ErrInvalidStoreMutation)
	}
	if dst == nil {
		return fmt.Errorf("%w: nil destination tiered store", ErrInvalidStoreMutation)
	}
	if err := dst.checkOpen(); err != nil {
		return err
	}
	if err := validateMigrateDestinationEmpty(dst); err != nil {
		return err
	}
	registrySnapshot, err := snapshotMigrateRegistryFile(dst)
	if err != nil {
		return err
	}

	labels := registrypkg.NewLabelRegistry()
	labelRegistryFound, err := src.LoadLabelRegistry(labels)
	if err != nil {
		return fmt.Errorf("graph: migrate: load label registry: %w", err)
	}
	relTypes := registrypkg.NewRelTypeRegistry()
	relTypeRegistryFound, err := src.LoadRelTypeRegistry(relTypes)
	if err != nil {
		return fmt.Errorf("graph: migrate: load reltype registry: %w", err)
	}
	if err := validateMigrateRegistriesPresent(src, labelRegistryFound, relTypeRegistryFound); err != nil {
		return err
	}
	if err := preflightMigrateSource(src, labels, relTypes); err != nil {
		return err
	}

	// Wire ontology for routing.
	previousLabels := dst.ontology.SetLabelRegistry(labels)
	insertedNodes := make([]types.NodeID, 0)
	insertedRels := make([]types.RelID, 0)

	// Migrate nodes one at a time via ForEachNodeID.
	var migrateErr error
	if err := src.ForEachNodeID(func(id types.NodeID) bool {
		n, err := src.GetNode(id)
		if err != nil {
			migrateErr = fmt.Errorf("graph: migrate: get node %d: %w", id, err)
			return false
		}
		if err := validateMigrateNodeTokens(n, labels); err != nil {
			migrateErr = err
			return false
		}
		if err := dst.PutNode(n); err != nil {
			migrateErr = fmt.Errorf("graph: migrate: put node %d: %w", id, err)
			return false
		}
		insertedNodes = append(insertedNodes, id)
		return true
	}); err != nil {
		return failMigrate(dst, previousLabels, registrySnapshot, false, insertedNodes, insertedRels,
			fmt.Errorf("graph: migrate: iterate nodes: %w", err))
	}
	if migrateErr != nil {
		return failMigrate(dst, previousLabels, registrySnapshot, false, insertedNodes, insertedRels, migrateErr)
	}

	// Migrate relationships one at a time via ForEachRelID.
	if err := src.ForEachRelID(func(id types.RelID) bool {
		r, err := src.GetRelationship(id)
		if err != nil {
			migrateErr = fmt.Errorf("graph: migrate: get rel %d: %w", id, err)
			return false
		}
		if err := validateMigrateRelTokens(r, relTypes); err != nil {
			migrateErr = err
			return false
		}
		if err := dst.PutRelationship(r); err != nil {
			migrateErr = fmt.Errorf("graph: migrate: put rel %d: %w", id, err)
			return false
		}
		insertedRels = append(insertedRels, id)
		return true
	}); err != nil {
		return failMigrate(dst, previousLabels, registrySnapshot, false, insertedNodes, insertedRels,
			fmt.Errorf("graph: migrate: iterate rels: %w", err))
	}
	if migrateErr != nil {
		return failMigrate(dst, previousLabels, registrySnapshot, false, insertedNodes, insertedRels, migrateErr)
	}

	if err := dst.SaveRegistries(labels, relTypes); err != nil {
		return failMigrate(dst, previousLabels, registrySnapshot, true, insertedNodes, insertedRels,
			fmt.Errorf("graph: migrate: save registries: %w", err))
	}

	return nil
}

type migrateRegistryFileSnapshot struct {
	path   string
	data   []byte
	exists bool
}

func snapshotMigrateRegistryFile(dst *Store) (migrateRegistryFileSnapshot, error) {
	if dst.inMemory || dst.regFile == "" {
		return migrateRegistryFileSnapshot{}, nil
	}
	data, err := os.ReadFile(dst.regFile) // #nosec G304 — path derived from trusted Store config
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return migrateRegistryFileSnapshot{path: dst.regFile}, nil
		}
		return migrateRegistryFileSnapshot{}, fmt.Errorf("graph: migrate: snapshot destination registries: %w", err)
	}
	return migrateRegistryFileSnapshot{path: dst.regFile, data: data, exists: true}, nil
}

func validateMigrateDestinationEmpty(dst *Store) error {
	nodeCount, err := dst.NodeCount()
	if err != nil {
		return fmt.Errorf("graph: migrate: destination node count: %w", err)
	}
	relCount, err := dst.RelationshipCount()
	if err != nil {
		return fmt.Errorf("graph: migrate: destination relationship count: %w", err)
	}
	if nodeCount != 0 || relCount != 0 {
		return fmt.Errorf("%w: destination tiered store is not empty", ErrInvalidStoreMutation)
	}
	return nil
}

func validateMigrateRegistriesPresent(src *BadgerStore, labelRegistryFound, relTypeRegistryFound bool) error {
	if labelRegistryFound && relTypeRegistryFound {
		return nil
	}

	nodeCount, err := src.NodeCount()
	if err != nil {
		return fmt.Errorf("graph: migrate: source node count: %w", err)
	}
	relCount, err := src.RelationshipCount()
	if err != nil {
		return fmt.Errorf("graph: migrate: source relationship count: %w", err)
	}
	if !labelRegistryFound && (nodeCount > 0 || relCount > 0) {
		return fmt.Errorf("%w: source label registry not found", ErrInvalidStoreMutation)
	}
	if !relTypeRegistryFound && relCount > 0 {
		return fmt.Errorf("%w: source relationship type registry not found", ErrInvalidStoreMutation)
	}
	return nil
}

func preflightMigrateSource(src *BadgerStore, labels *registrypkg.LabelRegistry, relTypes *registrypkg.RelTypeRegistry) error {
	var migrateErr error
	if err := src.ForEachNodeID(func(id types.NodeID) bool {
		n, err := src.GetNode(id)
		if err != nil {
			migrateErr = fmt.Errorf("graph: migrate: preflight get node %d: %w", id, err)
			return false
		}
		if err := validateMigrateNodeTokens(n, labels); err != nil {
			migrateErr = err
			return false
		}
		return true
	}); err != nil {
		return fmt.Errorf("graph: migrate: preflight iterate nodes: %w", err)
	}
	if migrateErr != nil {
		return migrateErr
	}

	if err := src.ForEachRelID(func(id types.RelID) bool {
		r, err := src.GetRelationship(id)
		if err != nil {
			migrateErr = fmt.Errorf("graph: migrate: preflight get rel %d: %w", id, err)
			return false
		}
		if err := validateMigrateRelTokens(r, relTypes); err != nil {
			migrateErr = err
			return false
		}
		if err := validateMigrateRelEndpoints(src, r); err != nil {
			migrateErr = err
			return false
		}
		return true
	}); err != nil {
		return fmt.Errorf("graph: migrate: preflight iterate rels: %w", err)
	}
	return migrateErr
}

func validateMigrateNodeTokens(n *types.Node, labels *registrypkg.LabelRegistry) error {
	max := labels.Len()
	for _, tok := range n.AllLabelTokens() {
		raw := tok.Value()
		if raw == 0 || int(raw) > max {
			return fmt.Errorf("%w: node %d label token %d not in source registry (size %d)",
				ErrInvalidStoreMutation, n.ID(), raw, max)
		}
	}
	return nil
}

func validateMigrateRelEndpoints(src *BadgerStore, r *types.Relationship) error {
	if _, err := src.GetNode(r.StartNodeID()); err != nil {
		if errors.Is(err, ErrNodeNotFound) {
			return fmt.Errorf("%w: relationship %d start node %d not found in source",
				ErrInvalidStoreMutation, r.ID(), r.StartNodeID())
		}
		return fmt.Errorf("graph: migrate: source relationship %d start node %d: %w",
			r.ID(), r.StartNodeID(), err)
	}
	if _, err := src.GetNode(r.EndNodeID()); err != nil {
		if errors.Is(err, ErrNodeNotFound) {
			return fmt.Errorf("%w: relationship %d end node %d not found in source",
				ErrInvalidStoreMutation, r.ID(), r.EndNodeID())
		}
		return fmt.Errorf("graph: migrate: source relationship %d end node %d: %w",
			r.ID(), r.EndNodeID(), err)
	}
	return nil
}

func validateMigrateRelTokens(r *types.Relationship, relTypes *registrypkg.RelTypeRegistry) error {
	max := relTypes.Len()
	raw := r.TypeToken().Value()
	if raw == 0 || int(raw) > max {
		return fmt.Errorf("%w: relationship %d type token %d not in source registry (size %d)",
			ErrInvalidStoreMutation, r.ID(), raw, max)
	}
	return nil
}

func failMigrate(
	dst *Store,
	previousLabels *registrypkg.LabelRegistry,
	registrySnapshot migrateRegistryFileSnapshot,
	restoreRegistryFile bool,
	insertedNodes []types.NodeID,
	insertedRels []types.RelID,
	err error,
) error {
	rollbackErr := rollbackMigrateWrites(dst, insertedNodes, insertedRels)
	dst.ontology.SetLabelRegistry(previousLabels)
	if restoreRegistryFile {
		rollbackErr = errors.Join(rollbackErr, restoreMigrateRegistryFile(registrySnapshot))
	}
	if rollbackErr != nil {
		return fmt.Errorf("%w (rollback failed: %v)", err, rollbackErr)
	}
	return err
}

func rollbackMigrateWrites(dst *Store, insertedNodes []types.NodeID, insertedRels []types.RelID) error {
	var rollbackErr error
	for i := len(insertedRels) - 1; i >= 0; i-- {
		if err := dst.DeleteRelationship(insertedRels[i]); err != nil && !errors.Is(err, ErrRelNotFound) {
			rollbackErr = errors.Join(rollbackErr, fmt.Errorf("delete relationship %d: %w", insertedRels[i], err))
		}
	}
	for i := len(insertedNodes) - 1; i >= 0; i-- {
		if err := dst.DeleteNode(insertedNodes[i]); err != nil && !errors.Is(err, ErrNodeNotFound) {
			rollbackErr = errors.Join(rollbackErr, fmt.Errorf("delete node %d: %w", insertedNodes[i], err))
		}
	}
	return rollbackErr
}

func restoreMigrateRegistryFile(snapshot migrateRegistryFileSnapshot) error {
	if snapshot.path == "" {
		return nil
	}
	if snapshot.exists {
		return atomicWriteFile(snapshot.path, snapshot.data, "migration registry rollback")
	}
	if err := os.Remove(snapshot.path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("migration registry rollback: remove: %w", err)
	}
	if err := syncParentDir(snapshot.path, "migration registry rollback"); err != nil {
		return err
	}
	return nil
}
