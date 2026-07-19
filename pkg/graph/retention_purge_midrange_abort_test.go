package graph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var errInjectedSecondPurgeChunk = errors.New("injected: second purge chunk failure")

// failSecondPurgeChunkStore lets the FIRST PurgeNodesByLabelBefore chunk
// commit for real (so it actually deletes rows and durably frees their
// UniqueForever claims' owners), then fails the SECOND call — simulating a
// mid-range abort (ctx cancel or a store error) with committed prior chunks,
// exactly the BACKLOG 13a scenario. It records the first chunk's result so
// the test can identify EXACTLY which nodes it purged — the memory store's
// purgeNodesByLabel selects victims by ranging a Go map ("Map order is
// random — fine: the purge is order-independent", see
// memorystore_retention_purge.go), so a test cannot assume creation order
// predicts which nodes land in the first chunk.
type failSecondPurgeChunkStore struct {
	*memory.Store
	calls            int
	firstChunkResult storepkg.RetentionPurgeResult
}

func (s *failSecondPurgeChunkStore) PurgeNodesByLabelBefore(labelToken uint16, before types.Instant, chunk int) (storepkg.RetentionPurgeResult, error) {
	s.calls++
	if s.calls >= 2 {
		return storepkg.RetentionPurgeResult{}, errInjectedSecondPurgeChunk
	}
	res, err := s.Store.PurgeNodesByLabelBefore(labelToken, before, chunk)
	if err == nil {
		s.firstChunkResult = res
	}
	return res, err
}

// TestPurgeRangeAllChunks_ReapsForeverOwnersOnMidRangeAbort is the BACKLOG 13a
// regression: purgeRangeAllChunks only reaped UniqueForever claims of purged
// owners AFTER the chunk loop exited normally — both early-return paths (ctx
// cancel, chunk error) skipped the reap even though PRIOR chunks in the same
// call had already committed their deletions. A purged node is gone forever,
// so a later retry can never recapture it in a fresh chunk result: skipping
// the reap on abort left a PERMANENT ghost owner barring its value forever.
//
// This test forces > one chunk (chunk size 256) of UniqueForever-owning User
// nodes, injects a failure on the SECOND store-level purge call (so the FIRST
// chunk's ~256 deletions are real and committed), and proves the values freed
// by that first chunk are reusable afterward DESPITE PurgeExpiredNodes itself
// returning an error.
func TestPurgeRangeAllChunks_ReapsForeverOwnersOnMidRangeAbort(t *testing.T) {
	ctx := context.Background()
	failStore := &failSecondPurgeChunkStore{Store: memory.New()}
	g, err := graphpkg.New(graphpkg.Config{AllowRetentionPurge: true, Store: failStore})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if err := g.Constraints().CreateUniqueForever(ctx, "User", "email"); err != nil {
		t.Fatalf("CreateUniqueForever: %v", err)
	}

	const total = 300 // > one 256-row chunk
	emailByID := make(map[types.NodeID]string, total)
	for i := 0; i < total; i++ {
		email := fmt.Sprintf("user%03d@x.com", i)
		n, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": email})
		if err != nil {
			t.Fatalf("add user %d: %v", i, err)
		}
		emailByID[n.ID()] = email
	}

	// The purge must fail (the injected second-chunk error), NOT succeed.
	_, err = g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "User", Mode: adminpkg.PurgeByAge, Before: farFuture})
	if !errors.Is(err, errInjectedSecondPurgeChunk) {
		t.Fatalf("PurgeExpiredNodes err = %v, want errInjectedSecondPurgeChunk", err)
	}
	if failStore.calls < 2 {
		t.Fatalf("purge chunk calls = %d, want >= 2 (first must have committed before the injected failure)", failStore.calls)
	}
	if len(failStore.firstChunkResult.PurgedNodeIDs) == 0 {
		t.Fatal("first (successful) purge chunk reported zero purged nodes — test setup invalid")
	}

	// Every node the FIRST (successful) chunk actually deleted — identified by
	// its recorded result, not assumed from creation order — must have its
	// email value reusable. The reap must have run for them despite the
	// overall call returning an error.
	for _, purgedID := range failStore.firstChunkResult.PurgedNodeIDs {
		email, ok := emailByID[purgedID]
		if !ok {
			t.Fatalf("first chunk purged unknown node %v", purgedID)
		}
		if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": email}); err != nil {
			t.Fatalf("re-add purged user %v's email %q after mid-range abort failed (ghost owner — BACKLOG 13a regression): %v", purgedID, email, err)
		}
	}
}
