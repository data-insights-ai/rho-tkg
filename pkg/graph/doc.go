// Package graph implements the graph layer for the Temporal Knowledge Graph v3.
//
// It owns the label and relationship type registries, dual snowflake ID
// generators (one for nodes, one for relationships), and provides string
// resolution for token-based entities.
//
// # Storage
//
// The Store interface defines pure persistence operations. Two implementations
// are provided: MemoryStore (thread-safe in-memory with hash-set adjacency)
// and BadgerStore (Badger v4 with LRU caches, async batch writes, and
// in-memory indexes as the source of truth).
//
// # Concurrency
//
// An entity lock manager (256-shard mutex array) serializes operations on
// overlapping entities. AddRelationship locks both endpoints; DeleteNode
// locks the target before cascade. LockTwo acquires shards in ascending
// order to prevent deadlocks.
//
// Node and Relationship remain pure-data structs in pkg/types.
// This package is the sole owner of string resolution — entities never
// resolve tokens to strings themselves.
package graph
