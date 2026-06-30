package memory

import (
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// Memory-store parity for the per-tx change-log scope (mirrors the badger
// store-level proofs): a DISCARDED scope emits nothing and burns no LSN; a
// COMMITTED scope mints contiguous LSNs at commit; a non-diverted write while the
// scope is open (divert off) emits eagerly.

func TestMemoryLogScope_DiscardEmitsNothingAndBurnsNoLSN(t *testing.T) {
	ms := New(WithChangeLog())
	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	before, _ := ms.LastCommittedLSN()
	if before == 0 {
		t.Fatal("precondition: a committed record should exist")
	}

	if err := ms.BeginLogScope(); err != nil {
		t.Fatalf("BeginLogScope: %v", err)
	}
	ms.SetLogDivert(true)
	if err := ms.PutNode(memNode(2, 10)); err != nil {
		t.Fatalf("PutNode in scope: %v", err)
	}
	if err := ms.PutNode(memNode(3, 10)); err != nil {
		t.Fatalf("PutNode in scope: %v", err)
	}
	ms.SetLogDivert(false)
	if lsn, _ := ms.LastCommittedLSN(); lsn != before {
		t.Fatalf("watermark moved to %d during open scope, want %d", lsn, before)
	}

	if err := ms.DiscardLogScope(); err != nil {
		t.Fatalf("DiscardLogScope: %v", err)
	}
	if lsn, _ := ms.LastCommittedLSN(); lsn != before {
		t.Fatalf("discarded scope changed watermark to %d, want %d", lsn, before)
	}
	recs, _ := ms.ChangeFeed(before, 0)
	if len(recs) != 0 {
		t.Fatalf("discarded scope left %d records, want 0", len(recs))
	}
}

func TestMemoryLogScope_CommitMintsContiguousLSNs(t *testing.T) {
	ms := New(WithChangeLog())
	if err := ms.PutNode(memNode(1, 10)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	before, _ := ms.LastCommittedLSN()

	if err := ms.BeginLogScope(); err != nil {
		t.Fatalf("BeginLogScope: %v", err)
	}
	ms.SetLogDivert(true)
	if err := ms.PutNode(memNode(2, 10)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ms.PutNode(memNode(3, 10)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	ms.SetLogDivert(false)
	if lsn, _ := ms.LastCommittedLSN(); lsn != before {
		t.Fatalf("watermark moved during open scope")
	}

	if err := ms.CommitLogScope(); err != nil {
		t.Fatalf("CommitLogScope: %v", err)
	}
	recs, _ := ms.ChangeFeed(before, 0)
	if len(recs) != 2 {
		t.Fatalf("committed scope emitted %d records, want 2", len(recs))
	}
	if recs[0].LSN != before+1 || recs[1].LSN != before+2 {
		t.Fatalf("scope LSNs = %d,%d, want %d,%d", recs[0].LSN, recs[1].LSN, before+1, before+2)
	}
	for _, r := range recs {
		if r.Tag != storecontract.ChangeNodePut {
			t.Fatalf("tag = %d, want ChangeNodePut", r.Tag)
		}
	}
}

func TestMemoryLogScope_NonDivertedWriteEmitsEagerly(t *testing.T) {
	ms := New(WithChangeLog())
	before, _ := ms.LastCommittedLSN()
	if err := ms.BeginLogScope(); err != nil {
		t.Fatalf("BeginLogScope: %v", err)
	}
	// Divert OFF (gap between tx mutations): a standalone write emits immediately.
	if err := ms.PutNode(memNode(9, 10)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if lsn, _ := ms.LastCommittedLSN(); lsn != before+1 {
		t.Fatalf("non-diverted write watermark = %d, want %d", lsn, before+1)
	}
	_ = ms.DiscardLogScope()
}
