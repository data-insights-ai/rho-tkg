package core

import (
	"encoding/binary"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// replicaAppliedLSNMeta is the MetaKV key holding this replica's highest applied
// change-log LSN (8-byte big-endian). It is DISTINCT from the store's own
// last_lsn watermark (which tracks a local feed, if any): a replica reads this on
// restart to resume tailing from where it left off. The advance is a separate
// MetaSet after each record's door commits; the at-most-one-record crash window
// it opens is closed by idempotent re-apply, not co-commit.
const replicaAppliedLSNMeta = "replica_applied_lsn"

// idSlotLeaseMeta is the MetaKV key holding the durable snowflake-slot lease
// (an orchestrator hint for failover). Distinct from replicaAppliedLSNMeta.
const idSlotLeaseMeta = "id_slot_lease"

// changeFeedOrErr returns the store's change-feed capability or a wrapped
// ErrCapabilityNotSupported when the backend does not provide one (e.g. tiered,
// or a store opened without the change-log enabled does provide the methods but
// returns an empty feed; only backends lacking the capability entirely error).
func (r *ReplOps) changeFeedOrErr() (storepkg.ChangeFeedCapability, error) {
	if r == nil || r.c == nil || r.c.changeFeed == nil {
		return nil, fmt.Errorf("graph: change feed: %w", storepkg.ErrCapabilityNotSupported)
	}
	return r.c.changeFeed, nil
}

// ChangeFeed returns up to limit committed change-log records with LSN >
// afterLSN, in ascending LSN order (limit <= 0 = all).
func (r *ReplOps) ChangeFeed(afterLSN uint64, limit int) ([]storepkg.ChangeRecord, error) {
	cf, err := r.changeFeedOrErr()
	if err != nil {
		return nil, err
	}
	return cf.ChangeFeed(afterLSN, limit)
}

// ForEachChange streams committed change-log records with LSN > afterLSN in
// ascending order, stopping early when fn returns false.
func (r *ReplOps) ForEachChange(afterLSN uint64, fn func(storepkg.ChangeRecord) bool) error {
	cf, err := r.changeFeedOrErr()
	if err != nil {
		return err
	}
	return cf.ForEachChange(afterLSN, fn)
}

// LastCommittedLSN returns the highest durably-committed change-log LSN, or 0
// when the log is empty. It is the watermark a session reads after a write to
// drive read-your-writes routing against a read replica.
func (r *ReplOps) LastCommittedLSN() (uint64, error) {
	cf, err := r.changeFeedOrErr()
	if err != nil {
		return 0, err
	}
	return cf.LastCommittedLSN()
}

// RegistrySnapshot captures this graph's full token registries plus the
// change-log LSN they are complete as-of, for a replica to append-only-extend
// its own registry on an unresolved token. Returns ErrCapabilityNotSupported
// when there is no change-log (without an LSN the snapshot cannot anchor the
// replica's CapturedAtLSN >= record-LSN guard).
//
// The LSN is read BEFORE the names so CapturedAtLSN can never exceed the names'
// coverage: any mutation committed at/below this LSN allocated its token before
// committing, hence before the (later) names read under registryMu — so the
// returned names already contain every token up to CapturedAtLSN.
func (r *ReplOps) RegistrySnapshot() (*storepkg.RegistrySnapshot, error) {
	if r == nil || r.c == nil {
		return nil, ErrNilGraph
	}
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if c.changeFeed == nil {
		return nil, fmt.Errorf("graph: registry snapshot: %w", storepkg.ErrCapabilityNotSupported)
	}
	lsn, err := c.changeFeed.LastCommittedLSN()
	if err != nil {
		return nil, fmt.Errorf("graph: registry snapshot: LSN: %w", err)
	}
	c.registryMu.Lock()
	snap := &storepkg.RegistrySnapshot{
		Labels:        c.labels.ExportNames(),
		RelTypes:      c.relTypes.ExportNames(),
		PropKeys:      c.propKeys.ExportNames(),
		CapturedAtLSN: lsn,
	}
	c.registryMu.Unlock()
	return snap, nil
}

// IDSlotLease returns the durable snowflake-slot lease, or (nil, nil) when none
// has been recorded. ErrCapabilityNotSupported when the backend cannot persist
// metadata (a custom store without MetaKVCapability; all in-tree backends —
// memory, badger, tiered — persist it). The persisted bytes are decoded through
// SafeUnmarshal (untrusted-bytes trust boundary) — a corrupt lease fails closed,
// never panics — and the slot is re-validated against the 0-15 range the write
// path enforces, so a foreign/out-of-range record fails closed at read time
// rather than being carried into a New() that would reject it.
func (r *ReplOps) IDSlotLease() (*storepkg.IDSlotLeaseRecord, error) {
	if r == nil || r.c == nil {
		return nil, ErrNilGraph
	}
	if err := r.c.checkOpen(); err != nil {
		return nil, err
	}
	mk, ok := r.c.store.(storepkg.MetaKVCapability)
	if !ok {
		return nil, fmt.Errorf("graph: id-slot lease: %w", storepkg.ErrCapabilityNotSupported)
	}
	v, err := mk.MetaGet(idSlotLeaseMeta)
	if err != nil {
		return nil, fmt.Errorf("graph: read id-slot lease: %w", err)
	}
	if len(v) == 0 {
		return nil, nil
	}
	var rec storepkg.IDSlotLeaseRecord
	if err := storeutil.SafeUnmarshal(v, &rec); err != nil {
		return nil, fmt.Errorf("graph: decode id-slot lease: %w", err)
	}
	if rec.Slot < 0 || rec.Slot > 15 {
		return nil, fmt.Errorf("graph: id-slot lease: persisted slot %d out of range 0-15", rec.Slot)
	}
	return &rec, nil
}

// SetIDSlotLease persists the snowflake-slot lease (an orchestrator-supplied
// failover hint). The slot is validated against the 0-15 range New accepts.
// last-writer-wins (not CAS) — the orchestrator must serialize writes.
func (r *ReplOps) SetIDSlotLease(rec *storepkg.IDSlotLeaseRecord) error {
	if r == nil || r.c == nil {
		return ErrNilGraph
	}
	if err := r.c.checkOpen(); err != nil {
		return err
	}
	if rec == nil {
		return fmt.Errorf("graph: set id-slot lease: nil record")
	}
	if rec.Slot < 0 || rec.Slot > 15 {
		return fmt.Errorf("graph: set id-slot lease: slot %d out of range 0-15", rec.Slot)
	}
	mk, ok := r.c.store.(storepkg.MetaKVCapability)
	if !ok {
		return fmt.Errorf("graph: id-slot lease: %w", storepkg.ErrCapabilityNotSupported)
	}
	b, err := msgpack.Marshal(rec)
	if err != nil {
		return fmt.Errorf("graph: encode id-slot lease: %w", err)
	}
	if err := mk.MetaSet(idSlotLeaseMeta, b); err != nil {
		return fmt.Errorf("graph: persist id-slot lease: %w", err)
	}
	return nil
}

// SetReplicationSource sets (or clears, with nil) the primary handle the apply
// path uses to refetch the registry on an unresolved token. Safe to call after
// New (e.g. once a driver has dialed the primary).
func (c *Core) SetReplicationSource(src storepkg.ReplicationSource) {
	c.replSourceMu.Lock()
	c.replSource = src
	c.replSourceMu.Unlock()
}

func (c *Core) replicationSource() storepkg.ReplicationSource {
	c.replSourceMu.RLock()
	defer c.replSourceMu.RUnlock()
	return c.replSource
}

// ApplyChange applies one change-log record received from a primary's feed,
// reproducing the primary's row exactly, then advances the applied-LSN
// watermark. It is the per-record half of log-shipped replication and is the
// ONE write path that bypasses the read-only-replica gate (it calls store doors
// directly under c.mu.Lock, checking only that the graph is open) — so it works
// on a graph opened with Config.ReadOnlyReplica.
//
// Durability ordering (the load-bearing invariant): the applied data is flushed
// to the backend BEFORE the watermark advances, so a crash between can only
// leave the watermark BEHIND the data (→ a harmless idempotent re-apply), never
// ahead of it (→ a permanently-skipped record). A record at or below the current
// watermark is a no-op (already applied / stale redelivery), which also prevents
// a buggy driver from regressing the replica with an out-of-order record.
func (r *ReplOps) ApplyChange(rec storepkg.ChangeRecord) error {
	if r == nil || r.c == nil {
		return ErrNilGraph
	}
	c := r.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkOpen(); err != nil {
		return err
	}
	applied, err := c.appliedLSNLocked()
	if err != nil {
		return err
	}
	if rec.LSN <= applied {
		return nil // already applied / stale redelivery
	}
	if err := c.applyChangeRecordLocked(rec); err != nil {
		return fmt.Errorf("graph: apply change LSN %d: %w", rec.LSN, err)
	}
	if err := c.flushStoreLocked(); err != nil {
		return fmt.Errorf("graph: apply change LSN %d: flush before watermark: %w", rec.LSN, err)
	}
	return c.setAppliedLSNLocked(rec.LSN)
}

// ApplyChanges applies a batch of records in ascending LSN order, returning the
// highest LSN durably applied. Records at or below the current watermark are
// skipped (idempotent redelivery). It applies the whole batch, then flushes the
// data ONCE and advances the watermark to the last applied LSN — so the durable
// watermark never runs ahead of durable data. On the first failing record it
// stops, still flushes + records the watermark for the successful prefix, and
// returns (lastApplied, err); the caller resumes from lastApplied.
func (r *ReplOps) ApplyChanges(recs []storepkg.ChangeRecord) (uint64, error) {
	if r == nil || r.c == nil {
		return 0, ErrNilGraph
	}
	c := r.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkOpen(); err != nil {
		return 0, err
	}
	applied, err := c.appliedLSNLocked()
	if err != nil {
		return 0, err
	}
	last := applied
	var applyErr error
	for i := range recs {
		rec := recs[i]
		if rec.LSN <= last {
			continue // already applied / stale
		}
		if err := c.applyChangeRecordLocked(rec); err != nil {
			applyErr = fmt.Errorf("graph: apply change LSN %d: %w", rec.LSN, err)
			break
		}
		last = rec.LSN
	}
	if last > applied {
		if err := c.flushStoreLocked(); err != nil {
			return applied, fmt.Errorf("graph: flush before watermark: %w", err)
		}
		if err := c.setAppliedLSNLocked(last); err != nil {
			return applied, err
		}
	}
	return last, applyErr
}

// AppliedLSN returns the highest change-log LSN this replica has durably applied
// (0 when none / when the backend cannot persist the watermark — the caller then
// resumes from 0, which re-applies idempotently).
func (r *ReplOps) AppliedLSN() (uint64, error) {
	if r == nil || r.c == nil {
		return 0, ErrNilGraph
	}
	return r.c.appliedLSNLocked()
}

// appliedLSNLocked reads the durable replica watermark (0 when unset or the
// backend cannot persist it). Caller holds c.mu (or it is otherwise safe — the
// underlying MetaGet is its own backend transaction).
func (c *Core) appliedLSNLocked() (uint64, error) {
	mk, ok := c.store.(storepkg.MetaKVCapability)
	if !ok {
		return 0, nil
	}
	v, err := mk.MetaGet(replicaAppliedLSNMeta)
	if err != nil {
		return 0, fmt.Errorf("graph: read applied LSN: %w", err)
	}
	if len(v) == 0 {
		return 0, nil
	}
	if len(v) != 8 {
		return 0, fmt.Errorf("graph: read applied LSN: corrupt watermark (%d bytes)", len(v))
	}
	return binary.BigEndian.Uint64(v), nil
}

// storeFlusher is the optional backend ability to synchronously drain its write
// buffer to durable storage. The badger backend implements it; the in-RAM memory
// backend does not (nothing to flush — it is non-durable by design).
type storeFlusher interface{ Flush() error }

// flushStoreLocked drains any buffered writes to the backend so applied data is
// durable before the replica watermark advances. No-op on a backend without a
// buffer (memory). Caller holds c.mu.
func (c *Core) flushStoreLocked() error {
	if f, ok := c.store.(storeFlusher); ok {
		return f.Flush()
	}
	return nil
}

// SetAppliedLSN overwrites the durable applied-LSN watermark. Exposed for a
// driver that bootstraps from a snapshot and wants to set the initial position
// before tailing.
func (r *ReplOps) SetAppliedLSN(lsn uint64) error {
	if r == nil || r.c == nil {
		return ErrNilGraph
	}
	r.c.mu.Lock()
	defer r.c.mu.Unlock()
	if err := r.c.checkOpen(); err != nil {
		return err
	}
	return r.c.setAppliedLSNLocked(lsn)
}

// setAppliedLSNLocked persists the watermark when the backend supports MetaKV;
// otherwise it is a no-op (the in-session driver still tracks the LSN, and a
// restart resumes from 0 → idempotent re-apply). Caller holds c.mu.
func (c *Core) setAppliedLSNLocked(lsn uint64) error {
	mk, ok := c.store.(storepkg.MetaKVCapability)
	if !ok {
		return nil
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, lsn)
	if err := mk.MetaSet(replicaAppliedLSNMeta, buf); err != nil {
		return fmt.Errorf("graph: advance applied LSN to %d: %w", lsn, err)
	}
	return nil
}
