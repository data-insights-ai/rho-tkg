package badger

import (
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Store-level proof of the per-tx change-log scope, independent of the core
// wiring: records produced while a scope is open + diverted are buffered WITHOUT
// burning an LSN; DiscardLogScope emits nothing and advances no LSN; CommitLogScope
// mints contiguous LSNs and co-commits the records so the feed shows them.

func newScopeStore(t *testing.T) *Store {
	t.Helper()
	bs, err := New(Config{InMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	return bs
}

// A scope that is DISCARDED must emit zero records and burn zero LSNs — the
// headline property (a rolled-back tx leaves the feed untouched).
func TestLogScope_DiscardEmitsNothingAndBurnsNoLSN(t *testing.T) {
	bs := newScopeStore(t)
	putTestNode(t, bs, 1, 10, nil) // a committed pre-scope write
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	before, _ := bs.LastCommittedLSN()
	if before == 0 {
		t.Fatal("precondition: a committed record should exist")
	}

	if err := bs.BeginLogScope(); err != nil {
		t.Fatalf("BeginLogScope: %v", err)
	}
	bs.SetLogDivert(true)
	putTestNode(t, bs, 2, 10, nil) // diverted into the scope buffer
	putTestNode(t, bs, 3, 10, nil)
	bs.SetLogDivert(false)

	// Nothing emitted yet: the scope buffer holds the records; pendingLog/logSeq
	// are untouched.
	if lsn, _ := bs.LastCommittedLSN(); lsn != before {
		t.Fatalf("LastCommittedLSN moved to %d during open scope, want %d", lsn, before)
	}

	if err := bs.DiscardLogScope(); err != nil {
		t.Fatalf("DiscardLogScope: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush after discard: %v", err)
	}
	// The discarded scope emitted NOTHING and burned NO LSN.
	if lsn, _ := bs.LastCommittedLSN(); lsn != before {
		t.Fatalf("discarded scope changed the watermark to %d, want %d (no record, no LSN burned)", lsn, before)
	}
	recs, _ := bs.ChangeFeed(before, 0)
	if len(recs) != 0 {
		t.Fatalf("discarded scope left %d records in the feed, want 0", len(recs))
	}
}

// A scope that is COMMITTED emits its buffered records with contiguous LSNs minted
// at commit time, co-committed (visible in the feed).
func TestLogScope_CommitMintsContiguousLSNsAndEmits(t *testing.T) {
	bs := newScopeStore(t)
	putTestNode(t, bs, 1, 10, nil)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	before, _ := bs.LastCommittedLSN()

	if err := bs.BeginLogScope(); err != nil {
		t.Fatalf("BeginLogScope: %v", err)
	}
	bs.SetLogDivert(true)
	putTestNode(t, bs, 2, 10, nil)
	putTestNode(t, bs, 3, 10, nil)
	bs.SetLogDivert(false)
	if lsn, _ := bs.LastCommittedLSN(); lsn != before {
		t.Fatalf("watermark moved during open scope")
	}

	if _, err := bs.CommitLogScope(); err != nil {
		t.Fatalf("CommitLogScope: %v", err)
	}
	// Two records emitted at contiguous LSNs after `before`.
	recs, _ := bs.ChangeFeed(before, 0)
	if len(recs) != 2 {
		t.Fatalf("committed scope emitted %d records, want 2", len(recs))
	}
	if recs[0].LSN != before+1 || recs[1].LSN != before+2 {
		t.Fatalf("scope LSNs = %d,%d, want %d,%d (contiguous, minted at commit)", recs[0].LSN, recs[1].LSN, before+1, before+2)
	}
	for _, r := range recs {
		if r.Tag != storecontract.ChangeNodePut {
			t.Fatalf("scope record tag = %d, want ChangeNodePut", r.Tag)
		}
	}
	// The data committed too.
	if n, err := bs.GetNode(types.NodeID(2)); err != nil || n == nil {
		t.Fatalf("node 2 not committed after scope commit: %v", err)
	}
}

// A standalone (non-diverted) write while a scope is OPEN but divert is OFF must
// emit its record EAGERLY with a real LSN — proving divert is the gate, not merely
// the scope being open (the core relies on this for concurrent standalone writes
// in the gaps between a tx's mutations).
func TestLogScope_NonDivertedWriteEmitsEagerly(t *testing.T) {
	bs := newScopeStore(t)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	before, _ := bs.LastCommittedLSN()

	if err := bs.BeginLogScope(); err != nil {
		t.Fatalf("BeginLogScope: %v", err)
	}
	// Divert OFF (simulating the gap between tx mutations): a write here is a
	// concurrent standalone and must emit immediately.
	putTestNode(t, bs, 9, 10, nil)
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if lsn, _ := bs.LastCommittedLSN(); lsn != before+1 {
		t.Fatalf("non-diverted write LSN watermark = %d, want %d (eager emit while scope open, divert off)", lsn, before+1)
	}
	_ = bs.DiscardLogScope()
}
