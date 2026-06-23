package core

import (
	"encoding/binary"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// replicaAppliedLSNMeta is the MetaKV key holding this replica's highest applied
// change-log LSN (8-byte big-endian). It is DISTINCT from the store's own
// last_lsn watermark (which tracks a local feed, if any): a replica reads this on
// restart to resume tailing from where it left off. The advance is a separate
// MetaSet after each record's door commits; the at-most-one-record crash window
// it opens is closed by idempotent re-apply, not co-commit.
const replicaAppliedLSNMeta = "replica_applied_lsn"

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
