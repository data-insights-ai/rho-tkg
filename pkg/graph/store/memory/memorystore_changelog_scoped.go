package memory

import (
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// This file implements store.ScopedTxChangeLog (BACKLOG 11f Batch A —
// foundation only; see the interface's doc comment in
// pkg/graph/store/changefeed.go for the full design rationale). It is the
// multi-token sibling of the single-scope BeginLogScope/SetLogDivert/
// CommitLogScope/DiscardLogScope mechanism in memorystore_changelog.go, which
// it does not modify or replace — both mechanisms coexist.

// BeginScopedLog opens a new independently-addressed scope and returns its
// token. Returns (0, nil) when the change-log is disabled — token 0 is
// reserved and every *Scoped door treats it as "no scope" (identical to the
// door's unscoped sibling).
func (ms *Store) BeginScopedLog() (uint64, error) {
	if !ms.logEnabled {
		return 0, nil
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.scopedTokenSeq++
	token := ms.scopedTokenSeq
	if ms.scopedLogs == nil {
		ms.scopedLogs = make(map[uint64][]storecontract.ChangeRecord)
	}
	ms.scopedLogs[token] = make([]storecontract.ChangeRecord, 0, 8)
	return token, nil
}

// CommitScopedLog mints contiguous LSNs for token's buffered records (at
// commit time, mirroring CommitLogScope) and splices them into the durable-
// order log under ms.mu. The token is retired on return (success or
// failure), so it can never be reused.
func (ms *Store) CommitScopedLog(token uint64) (uint64, error) {
	if !ms.logEnabled || token == 0 {
		return 0, nil
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	buffered, ok := ms.scopedLogs[token]
	delete(ms.scopedLogs, token)
	if !ok {
		return 0, fmt.Errorf("graph: %w: unknown scoped change-log token", storecontract.ErrInvalidStoreMutation)
	}
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

// DiscardScopedLog drops token's buffered records; logSeq is never advanced.
// The token is retired on return.
func (ms *Store) DiscardScopedLog(token uint64) error {
	if !ms.logEnabled || token == 0 {
		return nil
	}
	ms.mu.Lock()
	delete(ms.scopedLogs, token)
	ms.mu.Unlock()
	return nil
}

// logChangeScopedLocked appends one record to token's open buffer. The caller
// MUST hold ms.mu. Returns ErrInvalidStoreMutation if token is unknown (never
// opened, or already committed/discarded) — by the time this is called the
// door's entity write has already landed, so this can only be reached via
// caller misuse (a stale or foreign token), never a normal runtime condition.
func (ms *Store) logChangeScopedLocked(token uint64, tag storecontract.ChangeTag, payload []byte) error {
	buf, ok := ms.scopedLogs[token]
	if !ok {
		return fmt.Errorf("graph: %w: unknown scoped change-log token", storecontract.ErrInvalidStoreMutation)
	}
	cp := append([]byte(nil), payload...)
	ms.scopedLogs[token] = append(buf, storecontract.ChangeRecord{Tag: tag, Payload: cp})
	return nil
}

// logNodePutRoutedLocked is logNodePutLocked's token-aware sibling: token ==
// 0 takes the exact eager/legacy-scope path logNodePutLocked does; token != 0
// routes the record into that scope's buffer instead. Used only by the
// *Scoped create doors.
func (ms *Store) logNodePutRoutedLocked(n *types.Node, withHistory bool, token uint64) error {
	if !ms.logEnabled {
		return nil
	}
	p, err := storeutil.NodePutPayload(n, withHistory)
	if err != nil {
		return changeLogEncodeErr(err)
	}
	if token != 0 {
		return ms.logChangeScopedLocked(token, storecontract.ChangeNodePut, p)
	}
	ms.logChangeLocked(storecontract.ChangeNodePut, p)
	return nil
}

// logRelPutRoutedLocked is logRelPutLocked's token-aware sibling — see
// logNodePutRoutedLocked.
func (ms *Store) logRelPutRoutedLocked(r *types.Relationship, withHistory bool, token uint64) error {
	if !ms.logEnabled {
		return nil
	}
	p, err := storeutil.RelPutPayload(r, withHistory)
	if err != nil {
		return changeLogEncodeErr(err)
	}
	if token != 0 {
		return ms.logChangeScopedLocked(token, storecontract.ChangeRelPut, p)
	}
	ms.logChangeLocked(storecontract.ChangeRelPut, p)
	return nil
}

// logNodeDeleteWithHistoryRoutedLocked (BACKLOG 11f Batch C) builds and
// appends the with-history node-delete change-log record: token == 0 routes
// through the eager/legacy-scope path (ms.logChangeLocked); token != 0 routes
// the record into that scope's buffer instead. The single call site for both
// DeleteNodeWithHistory (token 0) and DeleteNodeWithHistoryScoped.
func (ms *Store) logNodeDeleteWithHistoryRoutedLocked(id snowflake.ID, nodeTombstone *types.Node, relTombstones []RelTombstone, token uint64) error {
	if !ms.logEnabled {
		return nil
	}
	p, err := storeutil.NodeDeleteWithHistoryPayload(id, nodeTombstone, relTombstones)
	if err != nil {
		return changeLogEncodeErr(err)
	}
	if token != 0 {
		return ms.logChangeScopedLocked(token, storecontract.ChangeNodeDelete, p)
	}
	ms.logChangeLocked(storecontract.ChangeNodeDelete, p)
	return nil
}

// logRelDeleteWithHistoryRoutedLocked is the relationship mirror of
// logNodeDeleteWithHistoryRoutedLocked — see its doc comment.
func (ms *Store) logRelDeleteWithHistoryRoutedLocked(id snowflake.ID, tombstone *types.Relationship, token uint64) error {
	if !ms.logEnabled {
		return nil
	}
	p, err := storeutil.RelDeleteWithHistoryPayload(id, tombstone)
	if err != nil {
		return changeLogEncodeErr(err)
	}
	if token != 0 {
		return ms.logChangeScopedLocked(token, storecontract.ChangeRelDelete, p)
	}
	ms.logChangeLocked(storecontract.ChangeRelDelete, p)
	return nil
}

// logNodeHistoryVersionRoutedLocked is logNodeHistoryVersionLocked's
// token-aware sibling (BACKLOG 11f Batch E) — see logNodePutRoutedLocked's
// doc comment for the routing rule.
func (ms *Store) logNodeHistoryVersionRoutedLocked(version uint32, n *types.Node, token uint64) error {
	if !ms.logEnabled {
		return nil
	}
	p, err := storeutil.NodeHistoryVersionPayload(version, n)
	if err != nil {
		return changeLogEncodeErr(err)
	}
	if token != 0 {
		return ms.logChangeScopedLocked(token, storecontract.ChangeNodeHistoryVersion, p)
	}
	ms.logChangeLocked(storecontract.ChangeNodeHistoryVersion, p)
	return nil
}

// logRelHistoryVersionRoutedLocked is logRelHistoryVersionLocked's
// token-aware sibling — see logNodeHistoryVersionRoutedLocked's doc comment.
func (ms *Store) logRelHistoryVersionRoutedLocked(version uint32, r *types.Relationship, token uint64) error {
	if !ms.logEnabled {
		return nil
	}
	p, err := storeutil.RelHistoryVersionPayload(version, r)
	if err != nil {
		return changeLogEncodeErr(err)
	}
	if token != 0 {
		return ms.logChangeScopedLocked(token, storecontract.ChangeRelHistoryVersion, p)
	}
	ms.logChangeLocked(storecontract.ChangeRelHistoryVersion, p)
	return nil
}
