package badger

import (
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// This file implements store.ScopedTxChangeLog (BACKLOG 11f Batch A —
// foundation only; see the interface's doc comment in
// pkg/graph/store/changefeed.go for the full design rationale). It is the
// multi-token sibling of the single-scope BeginLogScope/SetLogDivert/
// CommitLogScope/DiscardLogScope mechanism in badgerstore_changelog.go, which
// it does not modify or replace — both mechanisms coexist.

// BeginScopedLog opens a new independently-addressed scope and returns its
// token. Returns (0, nil) when the change-log is disabled — token 0 is
// reserved and every *Scoped door treats it as "no scope" (identical to the
// door's unscoped sibling).
func (bs *Store) BeginScopedLog() (uint64, error) {
	if !bs.logEnabled.Load() {
		return 0, nil
	}
	bs.wbMu.Lock()
	defer bs.wbMu.Unlock()
	bs.scopedTokenSeq++
	token := bs.scopedTokenSeq
	if bs.scopedLogs == nil {
		bs.scopedLogs = make(map[uint64][][]byte)
	}
	bs.scopedLogs[token] = make([][]byte, 0, 8)
	return token, nil
}

// CommitScopedLog mints contiguous LSNs for token's buffered records (newest
// LSNs at this instant, mirroring CommitLogScope) and flushes so they
// co-commit with any of the scope's still-pending data in one WriteBatch. The
// token is retired on return (success or failure), so it can never be reused.
func (bs *Store) CommitScopedLog(token uint64) (uint64, error) {
	if !bs.logEnabled.Load() || token == 0 {
		return 0, nil
	}
	bs.wbMu.Lock()
	buffered, ok := bs.scopedLogs[token]
	delete(bs.scopedLogs, token)
	if !ok {
		bs.wbMu.Unlock()
		return 0, fmt.Errorf("graph: %w: unknown scoped change-log token", storecontract.ErrInvalidStoreMutation)
	}
	if len(buffered) == 0 {
		bs.wbMu.Unlock()
		return 0, nil
	}
	var maxLSN uint64
	for _, value := range buffered {
		lsn := bs.nextLSN()
		bs.pendingLog = append(bs.pendingLog, pendingLogRecord{lsn: lsn, value: value})
		maxLSN = lsn // contiguous + monotonic, so the last minted is the max
	}
	bs.wbMu.Unlock()
	if err := bs.flush(); err != nil {
		return 0, err
	}
	return maxLSN, nil
}

// DiscardScopedLog drops token's buffered records without minting any LSN — a
// rolled-back scope emits nothing and burns no sequence number. The token is
// retired on return.
func (bs *Store) DiscardScopedLog(token uint64) error {
	if !bs.logEnabled.Load() || token == 0 {
		return nil
	}
	bs.wbMu.Lock()
	delete(bs.scopedLogs, token)
	bs.wbMu.Unlock()
	return nil
}

// logChangeScoped appends one framed record to token's open buffer. Returns
// ErrInvalidStoreMutation if token is unknown (never opened, or already
// committed/discarded) — by the time this is called the door's entity write
// has already landed, so this can only be reached via caller misuse (a stale
// or foreign token), never a normal runtime condition.
func (bs *Store) logChangeScoped(token uint64, tag storecontract.ChangeTag, payload []byte) error {
	if !bs.logEnabled.Load() || token == 0 {
		return nil
	}
	value := storepkg.EncodeChangeValue(tag, payload)
	bs.wbMu.Lock()
	buf, ok := bs.scopedLogs[token]
	if !ok {
		bs.wbMu.Unlock()
		return fmt.Errorf("graph: %w: unknown scoped change-log token", storecontract.ErrInvalidStoreMutation)
	}
	bs.scopedLogs[token] = append(buf, value)
	bs.wbMu.Unlock()
	return nil
}

// logChangeRoutedRaw is logChangeRaw's token-aware sibling: token == 0 takes
// the exact logChangeRaw path; token != 0 routes into that scope's buffer via
// logChangeScoped instead. Used by the *Scoped create doors (PutNodeScoped /
// PutRelationshipScoped), which — like their unscoped siblings — build their
// change-log payload once outside idxMu.Lock and pass it in already-encoded,
// so this never re-encodes. Callers MUST hold idxMu.Lock, same as
// logChangeRaw.
//
// There is deliberately no badger-side "build the payload for me" sibling
// mirroring memory's logRelPutRoutedLocked: badger has no bulk relationship
// create door reachable from a Scoped call site (BACKLOG 21g's investigation
// found the same fact from the opposite direction — see its CHANGELOG entry),
// and PutRelationshipGeneratedIDWithEndpointHashes/its Scoped counterpart are
// not implemented by this backend at all (only memory and tiered implement
// generatedcreate.RelationshipEndpointHashCapability), so
// c.endpointHashWrite is nil for a badger-backed Core and the scoped
// endpoint-hash-write branch in relationship_create_kernel.go never runs
// against this backend.
func (bs *Store) logChangeRoutedRaw(tag storecontract.ChangeTag, payload []byte, token uint64) error {
	if token != 0 {
		return bs.logChangeScoped(token, tag, payload)
	}
	bs.logChangeRaw(tag, payload)
	return nil
}
