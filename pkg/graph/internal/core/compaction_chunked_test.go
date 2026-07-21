package core

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// withCompactionChunk temporarily shrinks compactionChunk so a small test
// population still spans multiple chunks, and restores it on cleanup.
func withCompactionChunk(t *testing.T, n int) {
	t.Helper()
	orig := compactionChunk
	compactionChunk = n
	t.Cleanup(func() { compactionChunk = orig })
}

// withCompactionChunkHook installs fn as the between-chunk test hook and
// clears it on cleanup.
func withCompactionChunkHook(t *testing.T, fn func()) {
	t.Helper()
	compactionChunkHook = fn
	t.Cleanup(func() { compactionChunkHook = nil })
}

// TestCompaction_ChunkedUnderConcurrentWrite is the primary correctness proof
// for BACKLOG 13c: a chunk's plan and its apply happen under ONE uninterrupted
// c.mu.Lock hold, so a concurrent write landing in a BETWEEN-chunk gap can only
// ever affect a not-yet-planned entity in a LATER chunk — which is planned
// FRESH against the CURRENT chain when its own chunk's turn comes, never a
// stale plan applied atop changed history. 6 entities, chunk size 2 (3 chunks).
// While chunk 1 ([0,1]) is being processed, entity 5 (destined for chunk 3,
// [4,5]) is updated AGAIN before its own chunk is ever planned. Entity 5's
// resulting stub/keepVersions must reflect its FINAL (post-update) chain.
func TestCompaction_ChunkedUnderConcurrentWrite(t *testing.T) {
	withCompactionChunk(t, 2)
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	const n = 6
	ids := make([]types.NodeID, n)
	for i := range ids {
		ids[i], _ = buildNodeChain(t, g, 4) // v0..v4, history v0..v3 (4 history rows)
	}

	var wg sync.WaitGroup
	chunksSeen := 0
	withCompactionChunkHook(t, func() {
		chunksSeen++
		if chunksSeen != 1 {
			return
		}
		release := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(release)
			if _, err := g.Nodes.Update(ctx, ids[n-1], map[string]any{"state": "updated-mid-run"}); err != nil {
				t.Errorf("concurrent update: %v", err)
			}
		}()
		<-release
	})

	if _, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 2}); err != nil {
		t.Fatalf("CompactHistoryNodes: %v", err)
	}
	wg.Wait()

	// Entity n-1 now has history v0..v4 (5 rows) at plan time, not the v0..v3
	// (4 rows) it had when the outer id snapshot was taken. KeepVersions:2 over
	// 5 history rows trims 3 (v0,v1,v2), not 2 (v0,v1) — the stale-plan count.
	stub, ok, err := g.loadNodeCompactionStub(ids[n-1])
	if err != nil {
		t.Fatalf("loadNodeCompactionStub: %v", err)
	}
	if !ok {
		t.Fatal("expected a stub for the concurrently-updated entity")
	}
	if stub.TrimmedThroughVersion != 2 { // boundary = history[trim-1].version; trim=3 -> boundary version index 2 (0-based v2)
		t.Fatalf("TrimmedThroughVersion = %d, want 2 (fresh post-update plan, not the stale pre-update plan)", stub.TrimmedThroughVersion)
	}
	hist, err := g.Nodes.History(ids[n-1])
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(hist) != 2 { // 5 history rows - 3 trimmed = 2 kept (v3, v4)
		t.Fatalf("history len = %d, want 2 (fresh post-update plan kept v3,v4)", len(hist))
	}
	if ok, err := g.Hash.VerifyNodeChain(ids[n-1]); err != nil || !ok {
		t.Fatalf("VerifyNodeChain = (%v,%v), want (true,nil)", ok, err)
	}

	// Every other (untouched) entity trimmed exactly 2 (its unmodified 4-row history).
	for i := 0; i < n-1; i++ {
		h, err := g.Nodes.History(ids[i])
		if err != nil {
			t.Fatalf("History(%d): %v", i, err)
		}
		if len(h) != 2 {
			t.Fatalf("entity %d history len = %d, want 2", i, len(h))
		}
	}
}

// TestCompaction_RelChunkedUnderConcurrentWrite mirrors the node test for
// CompactHistoryRels / compactRelChunk (Testing Rule 2).
func TestCompaction_RelChunkedUnderConcurrentWrite(t *testing.T) {
	withCompactionChunk(t, 2)
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	const n = 6
	ids := make([]types.RelID, n)
	for i := range ids {
		ids[i], _ = buildRelChain(t, g, 4)
	}

	var wg sync.WaitGroup
	chunksSeen := 0
	withCompactionChunkHook(t, func() {
		chunksSeen++
		if chunksSeen != 1 {
			return
		}
		release := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(release)
			if _, err := g.Rels.Update(ctx, ids[n-1], map[string]any{"state": "updated-mid-run"}); err != nil {
				t.Errorf("concurrent rel update: %v", err)
			}
		}()
		<-release
	})

	if _, err := g.Admin.CompactHistoryRels(ctx, RetentionPolicy{KeepVersions: 2}); err != nil {
		t.Fatalf("CompactHistoryRels: %v", err)
	}
	wg.Wait()

	stub, ok, err := g.loadRelCompactionStub(ids[n-1])
	if err != nil {
		t.Fatalf("loadRelCompactionStub: %v", err)
	}
	if !ok {
		t.Fatal("expected a stub for the concurrently-updated rel")
	}
	hist, err := g.Rels.History(ids[n-1])
	if err != nil {
		t.Fatalf("Rel History: %v", err)
	}
	if len(hist) != 2 {
		t.Fatalf("rel history len = %d, want 2 (fresh post-update plan)", len(hist))
	}
	_ = stub
	if ok, err := g.Hash.VerifyRelChain(ids[n-1]); err != nil || !ok {
		t.Fatalf("VerifyRelChain = (%v,%v), want (true,nil)", ok, err)
	}
}

// TestCompaction_WatermarkAdvancesPerChunkAndUntouchedEntityStaysAnswerable
// (design section 7 item 4): between chunks, the watermark reflects only the
// chunks committed SO FAR, and an entity in a not-yet-processed chunk remains
// fully answerable below the FINAL watermark (a negative assertion — it must
// NOT yet see ErrHistoryCompacted just because SOME OTHER chunk advanced the
// graph-wide watermark past its own boundary).
func TestCompaction_WatermarkAdvancesPerChunkAndUntouchedEntityStaysAnswerable(t *testing.T) {
	withCompactionChunk(t, 2)
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	const n = 4
	ids := make([]types.NodeID, n)
	txs := make([][]types.Instant, n)
	for i := range ids {
		ids[i], txs[i] = buildNodeChain(t, g, 4)
	}

	var midRunWatermark types.Instant
	var midRunAnswerErr error
	chunksSeen := 0
	withCompactionChunkHook(t, func() {
		chunksSeen++
		if chunksSeen != 1 {
			return
		}
		// After chunk 1 ([0,1]) committed, chunk 2 ([2,3]) has NOT been planned
		// yet. The watermark must reflect only chunk 1's boundary...
		midRunWatermark = types.Instant(g.compactedThroughTx.Load())
		// ...and entity 2 (not yet compacted) must still answer a low pin with
		// its own actual history, never ErrHistoryCompacted just because the
		// GRAPH watermark already advanced past entity 2's own eventual boundary.
		_, midRunAnswerErr = g.Temporal.NodeAsOf(ids[2], txs[2][1])
	})

	rep, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 2})
	if err != nil {
		t.Fatalf("CompactHistoryNodes: %v", err)
	}

	if midRunWatermark == 0 {
		t.Fatal("hook never observed a mid-run watermark")
	}
	if midRunWatermark != txs[1][2] && midRunWatermark != txs[0][2] {
		// chunk 1 covers entities 0,1; the watermark must equal ONE of their
		// boundaries (whichever is larger), not entity 2/3's (not yet planned).
		maxWant := txs[0][2]
		if txs[1][2] > maxWant {
			maxWant = txs[1][2]
		}
		if midRunWatermark != maxWant {
			t.Fatalf("mid-run watermark = %d, want %d (max boundary of chunk 1 only)", midRunWatermark, maxWant)
		}
	}
	if midRunAnswerErr != nil {
		t.Fatalf("entity 2 (not yet compacted) NodeAsOf(low) = %v, want nil (fully answerable pre-compaction)", midRunAnswerErr)
	}
	if rep.Watermark < midRunWatermark {
		t.Fatalf("final watermark %d < mid-run watermark %d — not monotonic", rep.Watermark, midRunWatermark)
	}
}

// TestCompaction_ProtectedTagPartialCommit (design section 7 item 5, D2):
// a protected as-of tag violated by an entity in a LATER chunk must NOT roll
// back chunks that already committed cleanly — every committed chunk
// independently respected the tag, so no protected knowledge is ever trimmed,
// but the whole call still returns ErrCompactionProtectedTag and the
// violating chunk itself commits nothing.
func TestCompaction_ProtectedTagPartialCommit(t *testing.T) {
	withCompactionChunk(t, 2)
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	const n = 4
	ids := make([]types.NodeID, n)
	txs := make([][]types.Instant, n)
	for i := range ids {
		ids[i], txs[i] = buildNodeChain(t, g, 4)
	}

	// Register a tag pinned strictly below chunk 2's ([2,3]) boundary but
	// at/above chunk 1's ([0,1]) boundary, so chunk 1 passes and chunk 2 (or
	// later) violates.
	maxChunk1Boundary := txs[0][2]
	if txs[1][2] > maxChunk1Boundary {
		maxChunk1Boundary = txs[1][2]
	}
	tagAt := maxChunk1Boundary
	if err := g.Temporal.TagAsOf("protect", tagAt); err != nil {
		t.Fatalf("TagAsOf: %v", err)
	}

	_, err = g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 2})
	if !errors.Is(err, ErrCompactionProtectedTag) {
		t.Fatalf("CompactHistoryNodes err = %v, want ErrCompactionProtectedTag", err)
	}

	// Chunk 1's entities (0,1) DID commit — their own boundaries are <= tagAt.
	for i := 0; i < 2; i++ {
		h, err := g.Nodes.History(ids[i])
		if err != nil {
			t.Fatalf("History(%d): %v", i, err)
		}
		if len(h) != 2 {
			t.Fatalf("entity %d (chunk 1, should have committed) history len = %d, want 2", i, len(h))
		}
		if _, ok, err := g.loadNodeCompactionStub(ids[i]); err != nil || !ok {
			t.Fatalf("entity %d expected a committed stub, got ok=%v err=%v", i, ok, err)
		}
	}
	// Chunk 2's entities (2,3) must NOT have committed anything (the violating
	// chunk commits nothing at all).
	for i := 2; i < n; i++ {
		h, err := g.Nodes.History(ids[i])
		if err != nil {
			t.Fatalf("History(%d): %v", i, err)
		}
		if len(h) != 4 {
			t.Fatalf("entity %d (violating chunk, must NOT have committed) history len = %d, want 4 (untouched)", i, len(h))
		}
		if _, ok, err := g.loadNodeCompactionStub(ids[i]); err != nil || ok {
			t.Fatalf("entity %d must have NO stub (violating chunk committed nothing), got ok=%v err=%v", i, ok, err)
		}
	}
}

// TestCompaction_CancelledBetweenChunks (design section 7 item 7): a context
// cancelled between chunks stops the run with ctx.Err(), leaving prior chunks
// committed and consistent — it must not corrupt or partially apply the chunk
// that was never started.
func TestCompaction_CancelledBetweenChunks(t *testing.T) {
	withCompactionChunk(t, 2)
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	const n = 4
	ids := make([]types.NodeID, n)
	for i := range ids {
		ids[i], _ = buildNodeChain(t, g, 4)
	}

	ctx, cancel := context.WithCancel(context.Background())
	chunksSeen := 0
	withCompactionChunkHook(t, func() {
		chunksSeen++
		if chunksSeen == 1 {
			cancel()
		}
	})

	_, err = g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 2})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CompactHistoryNodes err = %v, want context.Canceled", err)
	}

	// Chunk 1 (entities 0,1) committed before cancellation was observed.
	for i := 0; i < 2; i++ {
		h, err := g.Nodes.History(ids[i])
		if err != nil {
			t.Fatalf("History(%d): %v", i, err)
		}
		if len(h) != 2 {
			t.Fatalf("entity %d history len = %d, want 2 (chunk 1 committed before cancellation)", i, len(h))
		}
	}
	// Chunk 2 (entities 2,3) never ran.
	for i := 2; i < n; i++ {
		h, err := g.Nodes.History(ids[i])
		if err != nil {
			t.Fatalf("History(%d): %v", i, err)
		}
		if len(h) != 4 {
			t.Fatalf("entity %d history len = %d, want 4 (chunk 2 never ran)", i, len(h))
		}
	}
}

// TestCompaction_IdempotentRerunAfterInterruption (design section 7 item 2,
// simplified to a from-scratch re-run rather than an actual process-crash
// simulation): running CompactHistoryNodes a second time with the same policy
// after a first successful run compacts nothing further and leaves the stub
// self-hash, watermark, and kept history byte-identical — the property the
// "no resume cursor" decision (D3) relies on.
func TestCompaction_IdempotentRerunAfterInterruption(t *testing.T) {
	withCompactionChunk(t, 2)
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	const n = 5
	ids := make([]types.NodeID, n)
	for i := range ids {
		ids[i], _ = buildNodeChain(t, g, 4)
	}

	rep1, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 2})
	if err != nil {
		t.Fatalf("first CompactHistoryNodes: %v", err)
	}
	if rep1.EntitiesCompacted != n {
		t.Fatalf("first run compacted %d entities, want %d", rep1.EntitiesCompacted, n)
	}

	stubsBefore := make(map[types.NodeID]compactionStub, n)
	for _, id := range ids {
		s, ok, err := g.loadNodeCompactionStub(id)
		if err != nil || !ok {
			t.Fatalf("loadNodeCompactionStub(%v) = (_,%v,%v), want ok", id, ok, err)
		}
		stubsBefore[id] = s
	}
	watermarkBefore := types.Instant(g.compactedThroughTx.Load())

	rep2, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 2})
	if err != nil {
		t.Fatalf("second CompactHistoryNodes: %v", err)
	}
	if rep2.EntitiesCompacted != 0 || rep2.VersionsTrimmed != 0 {
		t.Fatalf("second run = %+v, want a no-op (0 entities, 0 versions — already compacted)", rep2)
	}
	if rep2.Watermark != watermarkBefore {
		t.Fatalf("second run watermark = %d, want unchanged %d", rep2.Watermark, watermarkBefore)
	}
	for _, id := range ids {
		s, ok, err := g.loadNodeCompactionStub(id)
		if err != nil || !ok {
			t.Fatalf("loadNodeCompactionStub(%v) after rerun = (_,%v,%v), want ok", id, ok, err)
		}
		if s != stubsBefore[id] {
			t.Fatalf("stub for %v changed after idempotent rerun: before=%+v after=%+v", id, stubsBefore[id], s)
		}
	}
}
