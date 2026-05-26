package core

import (
	"fmt"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// =============================================================================
// Bitemporal migration (one-shot, idempotent)
//
// Pre-Phase-1 Update copied prev.ValidFrom into each new version, then the
// resolver detected this with nodeVersionInheritedValidFrom() and ignored
// the inherited value. Post-Phase-1 Update stops copying, but on-disk data
// written by older versions still carries the inheritance pattern. The
// resolver heuristic remains as a back-compat shim until this migration
// rewrites the affected rows.
//
// Algorithm: for every node + rel history row (Version > 0), clear
// ValidFrom IF it equals the immediately-preceding version's ValidFrom.
// After the migration runs the heuristic becomes dead code — every non-zero
// ValidFrom on a history row is now caller-supplied.
//
// Gated by meta key "schema_version" with value bitemporalSchemaVersion.
// Backends that do not implement MetaKVCapability skip the migration (and
// keep the heuristic load-bearing for whatever data they hold).
// =============================================================================

const (
	schemaVersionKey         = "schema_version"
	bitemporalSchemaVersion  = "bitemporal_v1"
)

// runBitemporalMigrationBestEffort runs the migration if it can, sets the
// bitemporalMigrated flag on success. Backends without MetaKVCapability or
// stores whose iteration fails (e.g. test mocks that gate scans) are left
// with bitemporalMigrated=false, which keeps the legacy inheritance
// heuristic active for their data.
func (c *Core) runBitemporalMigrationBestEffort() {
	meta, ok := c.store.(storepkg.MetaKVCapability)
	if !ok {
		return
	}
	cur, err := meta.MetaGet(schemaVersionKey)
	if err != nil {
		return
	}
	if string(cur) == bitemporalSchemaVersion {
		c.bitemporalMigrated = true
		return
	}
	if err := c.runBitemporalMigration(meta); err != nil {
		// Migration failed — silently keep heuristic. Acceptable because
		// the heuristic is correct (just slow + load-bearing).
		return
	}
	c.bitemporalMigrated = true
}

func (c *Core) runBitemporalMigration(meta storepkg.MetaKVCapability) error {
	if err := c.migrateNodeHistoryClearInheritedValidFrom(); err != nil {
		return err
	}
	if err := c.migrateRelHistoryClearInheritedValidFrom(); err != nil {
		return err
	}
	if err := meta.MetaSet(schemaVersionKey, []byte(bitemporalSchemaVersion)); err != nil {
		return fmt.Errorf("write schema_version: %w", err)
	}
	return nil
}

func (c *Core) migrateNodeHistoryClearInheritedValidFrom() error {
	var ids []types.NodeID
	if err := c.store.ForEachNodeHistoryID(func(id types.NodeID) bool {
		ids = append(ids, id)
		return true
	}); err != nil {
		return fmt.Errorf("iterate node history: %w", err)
	}
	seen := make(map[types.NodeID]struct{}, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}

		hist, err := c.store.GetNodeHistory(id)
		if err != nil {
			return fmt.Errorf("get node history %d: %w", id, err)
		}
		// Build a full ordered chain: history (ascending by version) + current.
		current, err := c.getCurrentNode(id)
		if err != nil {
			// Skip — entity may have been deleted; history alone is OK.
			current = nil
		}
		chain := make([]*types.Node, 0, len(hist)+1)
		chain = append(chain, hist...)
		if current != nil {
			chain = append(chain, current)
		}
		for i := 1; i < len(chain); i++ {
			v := chain[i]
			tm := v.Temporal()
			if tm == nil || tm.ValidFrom == 0 || tm.UpdatedAt == 0 {
				continue
			}
			prevTM := chain[i-1].Temporal()
			if prevTM == nil || prevTM.ValidFrom == 0 {
				continue
			}
			if prevTM.ValidFrom != tm.ValidFrom {
				continue
			}
			// Detected inherited ValidFrom — clear it.
			modified := v.DeepCopy()
			modified.Temporal().ValidFrom = 0
			// Distinguish current from history: current is the LAST entry.
			if current != nil && i == len(chain)-1 {
				if err := c.store.ReplaceNode(modified); err != nil {
					return fmt.Errorf("clear node %d current ValidFrom: %w", id, err)
				}
			} else {
				if err := c.store.PutNodeVersion(id, v.Version(), modified); err != nil {
					return fmt.Errorf("clear node %d history v=%d ValidFrom: %w", id, v.Version(), err)
				}
			}
		}
	}
	return nil
}

func (c *Core) migrateRelHistoryClearInheritedValidFrom() error {
	var ids []types.RelID
	if err := c.store.ForEachRelHistoryID(func(id types.RelID) bool {
		ids = append(ids, id)
		return true
	}); err != nil {
		return fmt.Errorf("iterate rel history: %w", err)
	}
	seen := make(map[types.RelID]struct{}, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}

		hist, err := c.store.GetRelHistory(id)
		if err != nil {
			return fmt.Errorf("get rel history %d: %w", id, err)
		}
		current, err := c.getCurrentRelationship(id)
		if err != nil {
			current = nil
		}
		chain := make([]*types.Relationship, 0, len(hist)+1)
		chain = append(chain, hist...)
		if current != nil {
			chain = append(chain, current)
		}
		for i := 1; i < len(chain); i++ {
			v := chain[i]
			tm := v.Temporal()
			if tm == nil || tm.ValidFrom == 0 || tm.UpdatedAt == 0 {
				continue
			}
			prevTM := chain[i-1].Temporal()
			if prevTM == nil || prevTM.ValidFrom == 0 {
				continue
			}
			if prevTM.ValidFrom != tm.ValidFrom {
				continue
			}
			modified := v.DeepCopy()
			modified.Temporal().ValidFrom = 0
			if current != nil && i == len(chain)-1 {
				if err := c.store.ReplaceRelationship(modified); err != nil {
					return fmt.Errorf("clear rel %d current ValidFrom: %w", id, err)
				}
			} else {
				if err := c.store.PutRelVersion(id, v.Version(), modified); err != nil {
					return fmt.Errorf("clear rel %d history v=%d ValidFrom: %w", id, v.Version(), err)
				}
			}
		}
	}
	return nil
}
