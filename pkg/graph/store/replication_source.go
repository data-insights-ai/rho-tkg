package store

// RegistrySnapshot is a primary's token-registry state captured at a known
// change-log position. A log-shipped replica fetches it when an applied record
// references a label / rel-type token the primary allocated AFTER the replica's
// bootstrap snapshot, and append-only-extends its own registry to resolve it.
//
// Labels / RelTypes / PropKeys are the full name slices (index 0 reserved ""),
// exactly as the registries' ExportNames() returns them. CapturedAtLSN is the
// primary's last-committed change-log LSN at capture time — the replica requires
// CapturedAtLSN >= the LSN of the record that triggered the refetch, so the
// snapshot is guaranteed to already contain the missing token.
type RegistrySnapshot struct {
	Labels        []string
	RelTypes      []string
	PropKeys      []string
	CapturedAtLSN uint64
}

// ReplicationSource is the replica's handle to its primary's registry. The
// replica's apply path calls RegistrySnapshot() on an unresolved token, then
// grows its own registry from the result. In-process, a primary's
// g.Replication() satisfies this directly; across a network, sigma's transport
// implements it. It is injected via graph.Config.ReplicationSource or
// g.SetReplicationSource — nil means "fail closed on an unknown token" (the
// behaviour before this capability existed).
type ReplicationSource interface {
	RegistrySnapshot() (*RegistrySnapshot, error)
}

// IDSlotLeaseRecord is the durable snowflake-slot assignment an external
// orchestrator (e.g. sigma) records for a node, read back to drive failover.
// It is a HINT, not a consensus primitive: the backing MetaSet is
// last-writer-wins (the store runs DetectConflicts=false), so the orchestrator
// MUST serialize lease writes — rho-tkg only persists and returns it.
//
// Promotion-by-reopen is the intended use: an orchestrator reads the lease
// (g.Replication().IDSlotLease()), Close()s the replica, then New()s it with
// Config{SnowflakeNodeID: lease.Slot, ReadOnlyReplica: false}. A promoted node
// and the node it replaces MUST hold DIFFERENT slots so their minted IDs never
// collide during a split-brain window (slot difference, not lease durability,
// is what prevents collisions). The snowflake generators are built only in New,
// so promotion is always a reopen — never an in-place mutation.
type IDSlotLeaseRecord struct {
	Slot      int    `msgpack:"slot"`
	Owner     string `msgpack:"owner,omitempty"`
	Epoch     uint64 `msgpack:"epoch,omitempty"`
	ExpiresAt int64  `msgpack:"exp,omitempty"`
}
