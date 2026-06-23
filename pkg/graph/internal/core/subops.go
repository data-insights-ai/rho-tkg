// Package core sub-Core types — narrow operation groups that mirror the
// public sub-API field grouping on *graph.Graph (g.Nodes, g.Rels, etc.).
//
// Each sub-Core type holds a back-reference to *Core so methods can access
// shared state (locks, store, registries, generators). Sub-Core methods drop
// the noun prefix (NodeOps.Add was Core.Nodes.Add; RelOps.Get was
// Core.Rels.Get). The sub-API wrappers in pkg/graph/<name>/api.go
// forward to these directly with no further name translation.
//
// Sub-Core types are exported so the wrappers can name them, but the entire
// `core` package is in `internal/`, so external consumers cannot see them.
package core

// NodeOps groups node-management operations.
type NodeOps struct{ c *Core }

// RelOps groups relationship-management operations.
type RelOps struct{ c *Core }

// TempOps groups temporal-query operations (point-in-time, interval,
// bitemporal, snapshot/diff, Allen relations).
type TempOps struct{ c *Core }

// IndexOps groups property/vector/high-frequency index management plus
// IndexProvider registration.
type IndexOps struct{ c *Core }

// EventOps groups EventBus / AsyncEventBus management.
type EventOps struct{ c *Core }

// AdminOps groups tiered-store admin operations: archive, repair, shards,
// rotate, reset, decompose-id.
type AdminOps struct{ c *Core }

// ConstraintOps groups temporal-constraint set management.
type ConstraintOps struct{ c *Core }

// HashOps groups hash-chain verification.
type HashOps struct{ c *Core }

// IOOps groups Export / Import.
type IOOps struct{ c *Core }

// ResolveOps groups shadow-property and registry resolution.
type ResolveOps struct{ c *Core }

// StatOps groups counter / count-by-label statistics.
type StatOps struct{ c *Core }

// ReplOps groups the change-log / replication surface (g.Replication()). It
// forwards to the store's optional ChangeFeedCapability and returns
// ErrCapabilityNotSupported when the backend does not provide one.
type ReplOps struct{ c *Core }
