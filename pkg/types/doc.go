// Package types defines the core domain types for the Temporal Knowledge Graph v3.
//
// This includes nodes, relationships, property storage, and shadow key
// definitions. All entity fields are unexported; access is through methods only.
// IDs use snowflake.ID for full encoding, serialization, and SQL support.
package types
