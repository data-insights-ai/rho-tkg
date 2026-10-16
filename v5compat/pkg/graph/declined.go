package graph

import "fmt"

// declined builds the ErrNotSupportedInV5 error of one door.
func declined(door string) error { return fmt.Errorf("%w: %s", ErrNotSupportedInV5, door) }

// The doors below keep the v4 names so a consumer compiles and fails at run time
// with a typed error naming the door (the X list of WP-11).

// Ingest is the v4 ingest sub-API (sessions, WaitApplied): declined.
func (g *Graph) Ingest() error { return declined("Ingest()") }

// Admin is the v4 admin sub-API (DecomposeNodeID, IDSlotLease, tier admin): declined.
func (g *Graph) Admin() error { return declined("Admin()") }

// Events is the v4 event sub-API (changefeed, watchers): declined.
func (g *Graph) Events() error { return declined("Events()") }

// IO is the v4 export/import format: declined (the importer maps v4 stores).
func (g *Graph) IO() error { return declined("IO() v4 export format") }

// Replication is the async LSN follower: declined.
func (g *Graph) Replication() error { return declined("Replication()") }

// Constraints are the v4 unique constraints (CreateUnique, DryRunValidate): not
// in the first batch.
func (g *Graph) Constraints() error { return declined("Constraints()") }

// Index is the v4 index DDL and the vector index: not in the first batch.
func (g *Graph) Index() error { return declined("Index()") }

// Stats are the v4 counters: not in the first batch.
func (g *Graph) Stats() error { return declined("Stats()") }
