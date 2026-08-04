package memory

import (
	"fmt"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func changeLogEncodeErr(err error) error {
	return fmt.Errorf("graph: encode change-log: %w", err)
}

// Option configures a memory Store at construction.
type Option func(*Store)

// WithChangeLog enables the in-memory change-log (op-log): every committed
// mutation appends a record to an ordered in-RAM slice, exposed via
// store.ChangeFeedCapability. Off by default (zero overhead). The memory log is
// NOT durable — it is a parity/testing facility so the change-feed contract can
// be exercised without disk; the durable op-log is the badger backend.
func WithChangeLog() Option {
	return func(ms *Store) { ms.logEnabled = true }
}

// WithoutPlannerStats disables maintenance of the query-planner statistics
// (presence/NDV/min-max/type-class counters). See Config.DisablePlannerStats.
// The stat methods then fail closed with ErrCapabilityNotSupported.
func WithoutPlannerStats() Option {
	return func(ms *Store) { ms.disablePlannerStats = true }
}

// logChangeLocked appends one change-log record. The caller MUST hold ms.mu
// (every mutation door does), so the LSN assignment and the append are atomic
// and totally ordered. payload is the tag-specific msgpack body; it is copied so
// the stored record never aliases caller memory. No-op when the log is disabled.
func (ms *Store) logChangeLocked(tag storecontract.ChangeTag, payload []byte) {
	if !ms.logEnabled {
		return
	}
	cp := append([]byte(nil), payload...)
	if ms.scopeActive {
		// Per-tx scope open: buffer WITHOUT advancing logSeq (LSN minted at commit).
		// A rolled-back scope (DiscardLogScope) emits nothing.
		ms.scopeLog = append(ms.scopeLog, storecontract.ChangeRecord{Tag: tag, Payload: cp})
		return
	}
	ms.logSeq++
	ms.changeLog = append(ms.changeLog, storecontract.ChangeRecord{LSN: ms.logSeq, Tag: tag, Payload: cp})
}

// --- store.TxChangeLogScope (parallel to the badger backend) ---

// BeginLogScope opens the per-tx record buffer. No-op when the log is off.
func (ms *Store) BeginLogScope() error {
	if !ms.logEnabled {
		return nil
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.scopeLog != nil || ms.scopeActive {
		return fmt.Errorf("graph: %w: change-log scope already open", storecontract.ErrInvalidStoreMutation)
	}
	ms.scopeLog = make([]storecontract.ChangeRecord, 0, 8)
	return nil
}

// SetLogDivert toggles record diversion for one tx mutation. The core calls it
// only under its exclusive write lock, so a concurrent standalone (read lock)
// mutation can never see diversion active. See the badger sibling.
func (ms *Store) SetLogDivert(on bool) {
	if !ms.logEnabled {
		return
	}
	ms.mu.Lock()
	ms.scopeActive = on
	ms.mu.Unlock()
}

// CommitLogScope mints contiguous LSNs for the buffered records (at commit time)
// and splices them into the durable-order log under ms.mu — atomic (memory has no
// flush; the splice IS the commit).
func (ms *Store) CommitLogScope() (uint64, error) {
	if !ms.logEnabled {
		return 0, nil
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	buffered := ms.scopeLog
	ms.scopeLog = nil
	ms.scopeActive = false
	if len(buffered) == 0 {
		return 0, nil
	}
	for i := range buffered {
		ms.logSeq++
		rec := buffered[i]
		rec.LSN = ms.logSeq
		ms.changeLog = append(ms.changeLog, rec)
	}
	return ms.logSeq, nil // the last minted LSN is the max
}

// DiscardLogScope drops the buffered records; logSeq is never advanced.
func (ms *Store) DiscardLogScope() error {
	if !ms.logEnabled {
		return nil
	}
	ms.mu.Lock()
	ms.scopeLog = nil
	ms.scopeActive = false
	ms.mu.Unlock()
	return nil
}

// The log*Locked helpers build a record body via the shared storeutil builders
// and append it, all under ms.mu (held by the calling door). Each is a no-op
// when the log is disabled, so a door can unconditionally `return ms.logXLocked(...)`.

func (ms *Store) logNodePutLocked(n *types.Node, withHistory bool) error {
	if !ms.logEnabled {
		return nil
	}
	p, err := storeutil.NodePutPayload(n, withHistory)
	if err != nil {
		return changeLogEncodeErr(err)
	}
	ms.logChangeLocked(storecontract.ChangeNodePut, p)
	return nil
}

func (ms *Store) logRelPutLocked(r *types.Relationship, withHistory bool) error {
	if !ms.logEnabled {
		return nil
	}
	p, err := storeutil.RelPutPayload(r, withHistory)
	if err != nil {
		return changeLogEncodeErr(err)
	}
	ms.logChangeLocked(storecontract.ChangeRelPut, p)
	return nil
}

// logNodeHardDeleteLocked records a hard node delete (no tombstone/history).
// cascadedRelIDs (raw snowflake IDs of relationships removed by a cascade) are
// sorted so the record is deterministic and byte-identical to the badger feed.
func (ms *Store) logNodeHardDeleteLocked(id snowflake.ID, cascadedRelIDs []int64) error {
	if !ms.logEnabled {
		return nil
	}
	sort.Slice(cascadedRelIDs, func(i, j int) bool { return cascadedRelIDs[i] < cascadedRelIDs[j] })
	p, err := storeutil.MarshalChangeBody(storeutil.NodeDeleteBody{ID: int64(id), CascadedRelIDs: cascadedRelIDs})
	if err != nil {
		return changeLogEncodeErr(err)
	}
	ms.logChangeLocked(storecontract.ChangeNodeDelete, p)
	return nil
}

func (ms *Store) logRelHardDeleteLocked(id snowflake.ID) error {
	if !ms.logEnabled {
		return nil
	}
	p, err := storeutil.MarshalChangeBody(storeutil.RelDeleteBody{ID: int64(id)})
	if err != nil {
		return changeLogEncodeErr(err)
	}
	ms.logChangeLocked(storecontract.ChangeRelDelete, p)
	return nil
}

// LastCommittedLSN returns the highest change-log LSN, or 0 when the log is
// empty/disabled. The memory store has no async buffer, so every appended record
// is immediately "committed".
func (ms *Store) LastCommittedLSN() (uint64, error) {
	if ms == nil {
		return 0, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return 0, err
	}
	return ms.logSeq, nil
}

// ChangeLogEnabled reports whether this store is recording mutations to its
// change-log (store.ChangeLogStatusCapability). False when constructed without
// WithChangeLog — the feed methods still work but return empty.
func (ms *Store) ChangeLogEnabled() bool {
	if ms == nil {
		return false
	}
	return ms.logEnabled
}

// snapshotChangesLocked copies the records with LSN > afterLSN (up to limit;
// <=0 = all). The caller holds at least an RLock. Payloads are copied so the
// returned records never alias the stored log.
//
// changeLog is append-only with strictly ascending LSN (logChangeLocked and
// CommitLogScope both mint LSN via ms.logSeq++ immediately before appending),
// so the starting position is found by binary search instead of a linear
// scan from the beginning. BACKLOG 17g: the prior linear scan was O(total
// log size) per call regardless of afterLSN, making small-limit polling (the
// normal ChangeFeed/replication consumption pattern) O(n^2) to fully drain a
// log of size n.
func (ms *Store) snapshotChangesLocked(afterLSN uint64, limit int) []storecontract.ChangeRecord {
	start := sort.Search(len(ms.changeLog), func(i int) bool {
		return ms.changeLog[i].LSN > afterLSN
	})
	remaining := ms.changeLog[start:]
	if limit > 0 && len(remaining) > limit {
		remaining = remaining[:limit]
	}
	if len(remaining) == 0 {
		return nil
	}
	out := make([]storecontract.ChangeRecord, len(remaining))
	for i, rec := range remaining {
		out[i] = storecontract.ChangeRecord{LSN: rec.LSN, Tag: rec.Tag, Payload: append([]byte(nil), rec.Payload...)}
	}
	return out
}

// ChangeFeed returns up to limit records with LSN > afterLSN in ascending order.
// limit <= 0 returns all. Payloads are owned copies.
func (ms *Store) ChangeFeed(afterLSN uint64, limit int) ([]storecontract.ChangeRecord, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	return ms.snapshotChangesLocked(afterLSN, limit), nil
}

// ForEachChange streams records with LSN > afterLSN in ascending order. The
// callback is invoked OUTSIDE the store lock (a snapshot is taken first) so it
// may re-enter Store methods; returning false stops early.
func (ms *Store) ForEachChange(afterLSN uint64, fn func(storecontract.ChangeRecord) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	if fn == nil {
		return ErrInvalidStoreMutation
	}
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return err
	}
	snapshot := ms.snapshotChangesLocked(afterLSN, 0)
	ms.mu.RUnlock()

	for _, rec := range snapshot {
		if !fn(rec) {
			return nil
		}
	}
	return nil
}
