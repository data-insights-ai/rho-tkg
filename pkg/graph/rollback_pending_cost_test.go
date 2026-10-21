package graph_test

import (
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// rollbackPendingScans opens the ai-soc Flux shape (tiered, RelsByMint,
// AdjacencyIndexOnDisk, SyncWrites, graph DurableCommit), creates n reference
// -> event rels and their endpoints in one GraphTx, rolls it back and returns
// the buffered entries the overlay readers visited during the rollback.
func rollbackPendingScans(t *testing.T, n int) uint64 {
	t.Helper()
	ts, err := tiered.New(tiered.Config{
		DataDir:              t.TempDir(),
		RefLabels:            []string{"Ref"},
		ShardWindow:          7 * 24 * time.Hour,
		RelsByMint:           true,
		AdjacencyIndexOnDisk: true,
		SyncWrites:           true,
	})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: ts, DurableCommit: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })

	tx, err := g.Tx().Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	for i := 0; i < n; i++ {
		a, err := tx.AddNode([]string{"Ref"}, nil)
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		b, err := tx.AddNode([]string{"Event"}, nil)
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		if _, err := tx.AddRelationshipByID("LINK", a.ID(), b.ID(), nil); err != nil {
			t.Fatalf("AddRelationshipByID: %v", err)
		}
	}
	before := ts.PendingScannedForTest()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	scanned := ts.PendingScannedForTest() - before
	if c, err := g.Stats().NodeCount(); err != nil || c != 0 {
		t.Fatalf("after rollback: %d nodes (%v), want 0", c, err)
	}
	return scanned
}

// TestRollbackManyCreatedRelsLinearInPending (row 102): rolling back n created
// rels visits O(n + pending) buffered entries, not n × pending. pending grows
// with n here (one held durable group), so quadratic cost shows as a 16x jump
// from n to 4n; linear cost as about 4x.
func TestRollbackManyCreatedRelsLinearInPending(t *testing.T) {
	const small, large = 100, 400
	const perRel = 64 // generous: each rel's own rows plus its endpoints'
	s, l := rollbackPendingScans(t, small), rollbackPendingScans(t, large)
	t.Logf("rollback scanned %d buffered entries at n=%d, %d at n=%d (ratio %.1f)", s, small, l, large, float64(l)/float64(max(s, 1)))
	if l > perRel*large {
		t.Errorf("rollback of %d rels scanned %d buffered entries, want <= %d (O(n + pending))", large, l, perRel*large)
	}
	if l > 6*max(s, 1) {
		t.Errorf("rollback cost grew %dx from n=%d to n=%d, want <= 6x (linear)", l/max(s, 1), small, large)
	}
}
