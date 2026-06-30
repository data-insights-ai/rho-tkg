package graph_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// The per-tx change-log scope diverts records via a store-global flag the core
// flips only under its EXCLUSIVE per-mutation write lock. This pins the property
// that makes that safe: a CONCURRENT standalone mutation (which holds only the
// shared read lock, so it can only run in the gaps where divert is off) is NEVER
// misrouted into an open tx's scope — every standalone write's record reaches the
// feed even when a concurrent tx is rolling back (and discarding ITS records).
// Run under -race: the tx's exclusive Lock vs the standalone's RLock must not race.
func TestTxScope_ConcurrentStandaloneNotMisrouted(t *testing.T) {
	ctx := context.Background()
	// SyncWrites so each committed record is durable (ChangeFeed reads durable
	// records only); also the strongest concurrency case (tx mutations flush their
	// DATA per-door while their records stay diverted in the scope).
	g, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	// Seed label "S" so standalone creates need no new token.
	mustAdd(t, g, []string{"S"}, map[string]any{"n": "seed"})

	const standaloneN = 200
	standaloneIDs := make([]string, 0, standaloneN)
	var idMu sync.Mutex

	var wg sync.WaitGroup
	// Goroutine A: repeatedly run a tx that creates nodes then ROLLS BACK, so its
	// records are always discarded — none must ever reach the feed.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			tx, err := g.Tx().Begin()
			if err != nil {
				return // graph may be closing; not under test here
			}
			_, _ = tx.AddNode([]string{"S"}, map[string]any{"r": fmt.Sprintf("rollback-%d", i)})
			_, _ = tx.AddNode([]string{"S"}, map[string]any{"r": fmt.Sprintf("rollback-%d-b", i)})
			_ = tx.Rollback()
		}
	}()

	// Goroutine B: standalone creates a known set of nodes concurrently.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < standaloneN; i++ {
			if _, err := g.Nodes().Add(ctx, []string{"S"}, map[string]any{"k": fmt.Sprintf("standalone-%d", i)}); err != nil {
				continue
			}
			idMu.Lock()
			standaloneIDs = append(standaloneIDs, fmt.Sprintf("standalone-%d", i))
			idMu.Unlock()
		}
	}()
	wg.Wait()

	// Collect every record in the feed and bucket by the "k"/"r" property: every
	// standalone node's record must be present; NO rolled-back-tx record may be.
	var recs []store.ChangeRecord
	if err := g.Replication().ForEachChange(0, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}

	// Cross-check against live state: every node in the feed must be live (a
	// rolled-back tx's node would be in the feed but NOT live → misrouted+leaked).
	// And the standalone count in live state must equal what B recorded.
	live, _ := g.Nodes().All(graph.QueryOpts{})
	liveStandalone := 0
	for _, n := range live {
		if v, _ := n.GetProperty("k"); v != nil {
			liveStandalone++
		}
		if v, _ := n.GetProperty("r"); v != nil {
			t.Fatalf("a rolled-back tx node survived in live state (k/r=%v) — misrouted/leaked", v)
		}
	}
	idMu.Lock()
	wantStandalone := len(standaloneIDs)
	idMu.Unlock()
	if liveStandalone != wantStandalone {
		t.Fatalf("live standalone nodes = %d, want %d (a standalone create was lost — its record may have been misrouted into a rolled-back tx scope)", liveStandalone, wantStandalone)
	}

	// The feed's NodePut records must be exactly the live standalone nodes — no
	// rolled-back-tx churn. (A committed standalone create = one NodePut; the seed
	// is one more.)
	puts := 0
	for _, r := range recs {
		if r.Tag == store.ChangeNodePut {
			puts++
		}
	}
	if puts != wantStandalone+1 { // +1 for the seed
		t.Fatalf("feed NodePut count = %d, want %d (standalone + seed); extra puts mean rolled-back-tx records leaked into the feed", puts, wantStandalone+1)
	}
}
