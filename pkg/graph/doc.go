// Package graph implements the graph layer for the Temporal Knowledge Graph v3.
//
// It owns the label and relationship type registries, dual snowflake ID
// generators (one for nodes, one for relationships), and provides string
// resolution for token-based entities.
//
// Node and Relationship remain pure-data structs in pkg/types.
// This package is the sole owner of string resolution — entities never
// resolve tokens to strings themselves.
package graph
