package badger

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// testSeqSource is a store-global LSN allocator stand-in used to prove the
// badger shard draws its change-log LSNs from an injected source
// (Config.ChangeLogSeqSource) instead of its own logSeq, and folds its open-time
// watermark in via Observe. It records the watermarks observed so a reseed test
// can assert the shard reported its durable max.
type testSeqSource struct {
	mu        sync.Mutex
	seq       uint64
	observed  []uint64
	nextCalls atomic.Int64
}

func (s *testSeqSource) Next() uint64 {
	s.nextCalls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	return s.seq
}

func (s *testSeqSource) Observe(w uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observed = append(s.observed, w)
	if w > s.seq {
		s.seq = w
	}
}

// TestChangeLogSeqSourceInjection proves a badger shard opened with an injected
// ChangeLogSeqSource draws every change-log LSN from that source (not logSeq).
func TestChangeLogSeqSourceInjection(t *testing.T) {
	src := &testSeqSource{seq: 1000} // pretend other shards already reached LSN 1000
	bs, err := badgerNewSeq(t, src)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer bs.Close()

	for id := int64(1); id <= 3; id++ {
		if err := bs.PutNode(pnodeSeq(id)); err != nil {
			t.Fatalf("PutNode %d: %v", id, err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// The shard must NOT have advanced its own logSeq (source owns the sequence).
	if got := bs.logSeq.Load(); got != 0 {
		t.Errorf("shard logSeq advanced to %d; source should own the sequence", got)
	}
	// Three PutNode records → three Next() draws, all above the source's 1000 seed.
	if got := src.nextCalls.Load(); got != 3 {
		t.Errorf("Next() called %d times, want 3", got)
	}
	feed, err := bs.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(feed) != 3 {
		t.Fatalf("feed length %d, want 3", len(feed))
	}
	wantLSN := uint64(1001)
	for i, rec := range feed {
		if rec.LSN != wantLSN {
			t.Errorf("record[%d] LSN = %d, want %d (drawn from injected source)", i, rec.LSN, wantLSN)
		}
		wantLSN++
	}
}

// TestChangeLogSeqSourceObserveAtOpen proves the shard folds its durable
// watermark into the allocator via Observe when reopened.
func TestChangeLogSeqSourceObserveAtOpen(t *testing.T) {
	dir := t.TempDir()
	src1 := &testSeqSource{}
	bs, err := New(Config{Dir: dir, ChangeLog: true, SyncWrites: true, ChangeLogSeqSource: src1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for id := int64(1); id <= 4; id++ {
		if err := bs.PutNode(pnodeSeq(id)); err != nil {
			t.Fatalf("PutNode %d: %v", id, err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	last, err := bs.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}
	if last != 4 {
		t.Fatalf("LastCommittedLSN = %d, want 4", last)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen with a FRESH source seeded at 0 — Observe must fold the durable
	// watermark (4) so the allocator resumes above it, never reissuing an LSN.
	src2 := &testSeqSource{}
	bs2, err := New(Config{Dir: dir, ChangeLog: true, SyncWrites: true, ChangeLogSeqSource: src2})
	if err != nil {
		t.Fatalf("reopen New: %v", err)
	}
	defer bs2.Close()
	src2.mu.Lock()
	seededTo := src2.seq
	observed := append([]uint64(nil), src2.observed...)
	src2.mu.Unlock()
	if seededTo < 4 {
		t.Errorf("allocator seeded to %d after reopen; want >= 4 (durable watermark folded via Observe)", seededTo)
	}
	found4 := false
	for _, w := range observed {
		if w == 4 {
			found4 = true
		}
	}
	if !found4 {
		t.Errorf("Observe never saw the durable watermark 4; observed=%v", observed)
	}
	// A fresh write must draw an LSN strictly above 4.
	if err := bs2.PutNode(pnodeSeq(5)); err != nil {
		t.Fatalf("PutNode 5: %v", err)
	}
	if err := bs2.Flush(); err != nil {
		t.Fatalf("Flush 2: %v", err)
	}
	feed, err := bs2.ChangeFeed(4, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(feed) != 1 || feed[0].LSN <= 4 {
		t.Fatalf("post-reopen feed = %+v; want one record with LSN > 4", feed)
	}
}

// TestChangeLogNilSeqSourceUnchanged proves the self-owned counter path is
// byte-for-byte unchanged: with no source, logSeq advances as before.
func TestChangeLogNilSeqSourceUnchanged(t *testing.T) {
	bs, err := New(Config{InMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer bs.Close()
	for id := int64(1); id <= 3; id++ {
		if err := bs.PutNode(pnodeSeq(id)); err != nil {
			t.Fatalf("PutNode %d: %v", id, err)
		}
	}
	if got := bs.logSeq.Load(); got != 3 {
		t.Errorf("self-owned logSeq = %d, want 3", got)
	}
}

func badgerNewSeq(t *testing.T, src ChangeLogSeqSource) (*Store, error) {
	t.Helper()
	return New(Config{InMemory: true, ChangeLog: true, SyncWrites: true, ChangeLogSeqSource: src})
}

func pnodeSeq(id int64) *types.Node {
	return types.NewNode(types.NodeID(id), 10, nil)
}
