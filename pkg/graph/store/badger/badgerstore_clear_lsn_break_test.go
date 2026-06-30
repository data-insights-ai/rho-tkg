package badger

import (
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// SYSTEM PROPERTY: a Clear() issued while the change-log is DISABLED must still
// PRESERVE a durable LastLSNKey left by a prior change-log-enabled session, so a
// FUTURE change-log-enabled reopen reseeds the LSN allocator ABOVE the pre-Clear
// watermark — never reusing an LSN a tailing consumer already checkpointed past
// (the gap-20 / lesson-52 hole reached through the log-disabled Clear door).
//
// Pre-fix behavior: the log-off Clear arm called db.DropAll(), wiping LastLSNKey;
// the reopen reseeded the allocator to 0 and the post-Clear write reused LSN 1.
func TestChangeLog_LogOffClearPreservesWatermark(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Session 1: change-log ON, mint two records.
	s1, err := New(Config{Dir: dir, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("New(session1): %v", err)
	}
	putTestNode(t, s1, 1, 10, nil)
	putTestNode(t, s1, 2, 10, nil)
	before, err := s1.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN(session1): %v", err)
	}
	if before == 0 {
		t.Fatalf("pre-Clear watermark = %d, want > 0", before)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close(session1): %v", err)
	}

	// Session 2: change-log OFF, Clear(). The log-off Clear arm must preserve the
	// watermark left by session 1.
	s2, err := New(Config{Dir: dir, ChangeLog: false, SyncWrites: true})
	if err != nil {
		t.Fatalf("New(session2): %v", err)
	}
	if err := s2.Clear(); err != nil {
		t.Fatalf("Clear(log-off): %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatalf("Close(session2): %v", err)
	}

	// Session 3: change-log ON again; a fresh write must NOT reuse an LSN.
	s3, err := New(Config{Dir: dir, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("New(session3): %v", err)
	}
	t.Cleanup(func() { _ = s3.Close() })
	putTestNode(t, s3, 3, 10, nil)
	if err := s3.Flush(); err != nil {
		t.Fatalf("Flush(session3): %v", err)
	}
	after, err := s3.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN(session3): %v", err)
	}
	if after <= before {
		t.Fatalf("post-Clear write LSN watermark = %d, want > pre-Clear %d (log-off Clear reused LSNs — DropAll wiped LastLSNKey)", after, before)
	}
}

// SYSTEM PROPERTY: a store opened with the change-log DISABLED that NEVER logged
// anything has no watermark to protect — Clear() must still drop everything and
// the LSN watermark must stay 0 (the never-logged store takes the plain DropAll
// arm, not the LastLSN-preserving arm).
func TestChangeLog_NeverLoggedClearStillDropAll(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir, ChangeLog: false, SyncWrites: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if lsn, _ := bs.LastCommittedLSN(); lsn != 0 {
		t.Fatalf("pre-Clear watermark = %d, want 0 (log was never enabled)", lsn)
	}

	if err := bs.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}

	// Everything dropped, watermark still 0.
	if cnt, err := bs.NodeCount(); err != nil {
		t.Fatalf("NodeCount: %v", err)
	} else if cnt != 0 {
		t.Fatalf("NodeCount after Clear = %d, want 0", cnt)
	}
	if lsn, err := bs.LastCommittedLSN(); err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	} else if lsn != 0 {
		t.Fatalf("post-Clear watermark = %d, want 0 (never-logged Clear must not invent a watermark)", lsn)
	}
}

// SYSTEM PROPERTY: Clear() on a change-log-enabled store is strictly monotonic and
// leaves EXACTLY ONE re-anchoring ChangeClear marker. A SECOND Clear() (which
// drives the empty-metaKeys arm of clearDataPreservingLastLSN — after the first
// Clear, the only surviving meta key is LastLSNKey, so the meta-delete batch is
// skipped) must: return nil, advance the watermark strictly (lsn2 > lsn1), wipe
// the first marker, and leave the feed's only/last marker as the new ChangeClear
// at lsn2.
func TestChangeLog_DoubleClear_MonotonicSingleMarker(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)

	// First Clear → exactly one ChangeClear marker at lsn1.
	if err := bs.Clear(); err != nil {
		t.Fatalf("Clear #1: %v", err)
	}
	lsn1, err := bs.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN after Clear #1: %v", err)
	}
	recs, err := bs.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed after Clear #1: %v", err)
	}
	clearCount := 0
	for _, r := range recs {
		if r.Tag == storecontract.ChangeClear {
			clearCount++
		}
	}
	if len(recs) != 1 || recs[0].Tag != storecontract.ChangeClear || recs[0].LSN != lsn1 || clearCount != 1 {
		t.Fatalf("feed after Clear #1 = %v at LSNs %v, want exactly one ChangeClear at %d", tagSeq(recs), lsnSeq(recs), lsn1)
	}

	// Second Clear → empty-metaKeys arm; must succeed, advance strictly, and leave
	// only the NEW marker.
	if err := bs.Clear(); err != nil {
		t.Fatalf("Clear #2 (empty-metaKeys arm): %v", err)
	}
	lsn2, err := bs.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN after Clear #2: %v", err)
	}
	if lsn2 <= lsn1 {
		t.Fatalf("post-double-Clear watermark = %d, want > %d (strict monotonicity across repeated Clear)", lsn2, lsn1)
	}

	recs, err = bs.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed after Clear #2: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("feed empty after Clear #2, want the re-anchoring ChangeClear marker")
	}
	// The old marker at lsn1 must be GONE; the last (and only) marker is at lsn2.
	for _, r := range recs {
		if r.LSN == lsn1 {
			t.Fatalf("first Clear marker at LSN %d survived the second Clear (DropPrefix did not wipe the old change-log record)", lsn1)
		}
	}
	last := recs[len(recs)-1]
	if last.Tag != storecontract.ChangeClear || last.LSN != lsn2 {
		t.Fatalf("feed last record = {tag:%v lsn:%d}, want {ChangeClear %d}", last.Tag, last.LSN, lsn2)
	}
}
