// Package replication is a sub-API accessor for the durable, ordered change-log
// (op-log): change-data-capture, audit, point-in-time recovery, and the
// foundation for read-replica streaming. It forwards to the store's optional
// ChangeFeedCapability; backends without one (e.g. tiered) return
// ErrCapabilityNotSupported.
package replication

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// Ops is the subset of *core.ReplOps the replication sub-API forwards to.
type Ops interface {
	ChangeFeed(afterLSN uint64, limit int) ([]store.ChangeRecord, error)
	ForEachChange(afterLSN uint64, fn func(store.ChangeRecord) bool) error
	LastCommittedLSN() (uint64, error)
	ApplyChange(rec store.ChangeRecord) error
	ApplyChanges(recs []store.ChangeRecord) (uint64, error)
	AppliedLSN() (uint64, error)
	SetAppliedLSN(lsn uint64) error
	RegistrySnapshot() (*store.RegistrySnapshot, error)
	IDSlotLease() (*store.IDSlotLeaseRecord, error)
	SetIDSlotLease(rec *store.IDSlotLeaseRecord) error
}

// API is the replication sub-API accessor.
type API struct {
	ops Ops
	ok  bool
}

// New constructs a replication sub-API.
func New(ops Ops) *API { return &API{ops: ops, ok: !grapherr.IsNil(ops)} }

func (a *API) ready() (Ops, error) {
	if a == nil || !a.ok {
		return nil, grapherr.ErrNilGraph
	}
	return a.ops, nil
}

// ChangeFeed returns up to limit committed change-log records with LSN >
// afterLSN, in ascending LSN order (limit <= 0 returns all). Returns
// ErrCapabilityNotSupported when the backend has no change-log.
func (a *API) ChangeFeed(afterLSN uint64, limit int) ([]store.ChangeRecord, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.ChangeFeed(afterLSN, limit)
}

// ForEachChange streams committed change-log records with LSN > afterLSN in
// ascending order, invoking fn for each until it returns false or the log is
// exhausted. The callback runs outside store locks and may re-enter the graph.
func (a *API) ForEachChange(afterLSN uint64, fn func(store.ChangeRecord) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ForEachChange(afterLSN, fn)
}

// LastCommittedLSN returns the highest durably-committed change-log LSN (0 when
// empty). It is the read-your-writes watermark a session records after a write.
func (a *API) LastCommittedLSN() (uint64, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.LastCommittedLSN()
}

// ApplyChange applies one change-log record from a primary's feed to this
// replica, reproducing the primary's row exactly and advancing the applied-LSN
// watermark. It bypasses the read-only-replica write gate (it is the replica's
// only write path). Use it to tail a primary: read AppliedLSN, pull records with
// LSN > it from the primary's ForEachChange, and ApplyChange each in order.
func (a *API) ApplyChange(rec store.ChangeRecord) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.ApplyChange(rec)
}

// ApplyChanges applies a batch of records in ascending LSN order, returning the
// highest LSN durably applied. On the first failing record it stops and returns
// (lastApplied, err); resume from lastApplied (apply is idempotent).
func (a *API) ApplyChanges(recs []store.ChangeRecord) (uint64, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.ApplyChanges(recs)
}

// AppliedLSN returns the highest LSN this replica has durably applied (0 when
// none). On restart, a driver resumes tailing the primary from this watermark.
func (a *API) AppliedLSN() (uint64, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.AppliedLSN()
}

// SetAppliedLSN overwrites the durable applied-LSN watermark — used to record the
// snapshot LSN after a bootstrap import, before tailing begins.
func (a *API) SetAppliedLSN(lsn uint64) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.SetAppliedLSN(lsn)
}

// RegistrySnapshot captures this graph's token registries plus the change-log
// LSN they are complete as-of, for a replica to extend its own registry on an
// unresolved token. Returns ErrCapabilityNotSupported when there is no
// change-log. *API satisfies store.ReplicationSource, so a primary's
// g.Replication() can be handed directly to a replica's g.SetReplicationSource.
func (a *API) RegistrySnapshot() (*store.RegistrySnapshot, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.RegistrySnapshot()
}

// IDSlotLease returns the durable snowflake-slot lease (an orchestrator failover
// hint), or (nil, nil) when none is recorded; ErrCapabilityNotSupported when the
// backend cannot persist metadata. See store.IDSlotLeaseRecord for the
// promotion-by-reopen flow.
func (a *API) IDSlotLease() (*store.IDSlotLeaseRecord, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.IDSlotLease()
}

// SetIDSlotLease persists the snowflake-slot lease. It is a last-writer-wins
// durable hint, not a consensus primitive — the orchestrator must serialize
// writes. The slot is validated against the 0-15 range.
func (a *API) SetIDSlotLease(rec *store.IDSlotLeaseRecord) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.SetIDSlotLease(rec)
}
