package graph_test

import (
	"bytes"
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// BACKLOG 13e: PurgeExpiredNodes advances the per-label retention watermark
// BEFORE emitting the replication ChangeRangePurge record. If an operator
// retries the SAME policy after a crash landed exactly between those two
// steps, the watermark advance is a no-op on retry (max-monotonic — it was
// already durably advanced by the crashed call), but the log emission has
// no such guard and fires again unconditionally — a genuine duplicate
// ChangeRangePurge record, confirmed here by literally calling
// PurgeExpiredNodes twice with an identical policy.
//
// Investigated whether to suppress the second emission (skip logging when
// the watermark didn't advance) and rejected it: that "fix" cannot
// distinguish "already logged by an earlier successful call" from "crashed
// BEFORE logging, watermark already advanced" — the exact scenario this
// finding describes — and would silently DROP the one record a replica
// needs to learn about the purge, trading harmless log-noise for a real
// replica-divergence bug. The log's replay is a re-executed PREDICATE
// against the replica's own state (not per-entity deletes), so replaying it
// twice is a no-op the second time — this test proves that harmlessness
// directly rather than changing production behavior.
func TestRetentionPurge_RetryAfterWatermarkAdvanceDuplicatesLogButConverges(t *testing.T) {
	ctx := context.Background()

	primary, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, AllowRetentionPurge: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()

	for i := 0; i < 3; i++ {
		if _, err := primary.Nodes().Add(ctx, []string{"Event"}, map[string]any{"seq": int64(i)}); err != nil {
			t.Fatalf("add event: %v", err)
		}
	}

	// Bootstrap the replica from a snapshot taken BEFORE the purge, so its
	// registry already carries the "Event" label token the range-purge
	// predicate records reference.
	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("export: %v", err)
	}
	lsn0, _ := primary.Replication().LastCommittedLSN()

	replica, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 7, BadgerInMemory: true, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer replica.Close()
	if err := replica.IO().Import(bytes.NewReader(snap.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("replica import: %v", err)
	}
	if err := replica.Replication().SetAppliedLSN(lsn0); err != nil {
		t.Fatalf("replica SetAppliedLSN: %v", err)
	}

	policy := adminpkg.PurgePolicy{Label: "Event", Mode: adminpkg.PurgeByAge, Before: farFuture}
	res1, err := primary.Admin().PurgeExpiredNodes(ctx, policy)
	if err != nil {
		t.Fatalf("first purge: %v", err)
	}
	if res1.NodesPurged != 3 {
		t.Fatalf("first purge NodesPurged = %d, want 3", res1.NodesPurged)
	}

	// Operator retry with the IDENTICAL policy — simulates re-invoking after
	// a crash, since the caller has no way to know the first call already
	// completed. The watermark advance is a no-op (already at Before), but
	// nothing left to purge either (already gone) — the observable effect on
	// the primary itself is a no-op purge, but the log call is unconditional.
	res2, err := primary.Admin().PurgeExpiredNodes(ctx, policy)
	if err != nil {
		t.Fatalf("retry purge: %v", err)
	}
	if res2.NodesPurged != 0 {
		t.Fatalf("retry purge NodesPurged = %d, want 0 (nothing left)", res2.NodesPurged)
	}

	var recs []store.ChangeRecord
	rangePurges := 0
	if err := primary.Replication().ForEachChange(lsn0, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		if rec.Tag == store.ChangeRangePurge {
			rangePurges++
		}
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if rangePurges != 2 {
		t.Fatalf("feed carried %d ChangeRangePurge records after retry, want exactly 2 — BACKLOG 13e mechanism no longer reproduces, update this test's premise", rangePurges)
	}

	// The replica applies BOTH duplicate records and converges to the same
	// correct state as if only one had been applied — proving the duplicate
	// is harmless, not just asserting its existence. Applying the SECOND
	// (already-a-no-op) predicate against an already-purged replica state
	// must not error or resurrect/re-affect anything.
	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("replica ApplyChanges (both records): %v", err)
	}
	if ev, err := replica.Nodes().ByLabel("Event", store.QueryOpts{}); err != nil || len(ev) != 0 {
		t.Fatalf("replica Event count after applying both records = (%d, %v), want (0, nil)", len(ev), err)
	}
}
