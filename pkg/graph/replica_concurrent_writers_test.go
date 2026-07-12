package graph_test

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The eager in-door change-log path is the multi-writer story (the per-tx scope
// is exclusive-lock-only — see store.TxChangeLogScope): N goroutines mutating
// concurrently through the standalone doors must produce a feed that is (a)
// LSN-gapless — records are minted only on doors that stage data, so nothing is
// burned or skipped under contention — and (b) sufficient for a tailing replica
// to converge BYTE-EXACT. This is the Task-2 (Lanes:N groundwork) proof: the
// concurrent ingest mode rides this same eager path, so its feed correctness is
// pinned here independent of the pipeline machinery. Run under -race.
func TestReplicaConvergence_ConcurrentEagerWriters(t *testing.T) {
	ctx := context.Background()

	primary, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()

	// Seed the vocabulary so concurrent writers never race a first-token
	// allocation against the snapshot (token sync is a separate, tested door).
	seedA := mustAdd(t, primary, []string{"W", "B"}, map[string]any{"n": "seedA"})
	seedB := mustAdd(t, primary, []string{"W"}, map[string]any{"n": "seedB"})
	if _, err := primary.Rels().AddByID(ctx, "KNOWS", seedA.ID(), seedB.ID(), nil); err != nil {
		t.Fatalf("seed rel: %v", err)
	}

	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	lsn0, err := primary.Replication().LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	replica, err := graph.New(graph.Config{SnowflakeNodeID: 2, BadgerInMemory: true, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer replica.Close()
	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("replica Import: %v", err)
	}
	if err := replica.Replication().SetAppliedLSN(lsn0); err != nil {
		t.Fatalf("SetAppliedLSN: %v", err)
	}

	// N concurrent writers, each owning its own entities: create a run of nodes,
	// update some (with-history), rel-link consecutive ones, delete one (cascade).
	// Per-writer ownership keeps the final state deterministic; cross-writer
	// entity contention is covered by the lock-manager suites — what is under
	// test HERE is the shared feed: eager records from overlapping doors.
	const writers = 6
	const perWriter = 20
	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var mine []types.NodeID
			for i := 0; i < perWriter; i++ {
				n, err := primary.Nodes().Add(ctx, []string{"W"}, map[string]any{"w": int64(w), "i": int64(i)})
				if err != nil {
					errCh <- err
					return
				}
				mine = append(mine, n.ID())
			}
			for i := 0; i+1 < len(mine); i += 4 {
				if _, err := primary.Rels().AddByID(ctx, "KNOWS", mine[i], mine[i+1], nil); err != nil {
					errCh <- err
					return
				}
			}
			if _, err := primary.Nodes().Update(ctx, mine[2], map[string]any{"w": int64(w), "i": int64(2), "u": true}); err != nil {
				errCh <- err
				return
			}
			if err := primary.Nodes().AddLabel(ctx, mine[3], "B"); err != nil {
				errCh <- err
				return
			}
			if err := primary.Nodes().Delete(ctx, mine[0]); err != nil { // cascades mine[0]->mine[1]
				errCh <- err
				return
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent writer: %v", err)
	}

	// (a) Gapless: every LSN in (lsn0, last] present exactly once, in order.
	var recs []store.ChangeRecord
	if err := primary.Replication().ForEachChange(lsn0, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("no change records from the concurrent writers")
	}
	for i, rec := range recs {
		if want := lsn0 + uint64(i) + 1; rec.LSN != want {
			t.Fatalf("LSN gap/misorder at index %d: got %d, want %d", i, rec.LSN, want)
		}
	}

	// (b) Byte-exact convergence from the interleaved feed.
	applied, err := replica.Replication().ApplyChanges(recs)
	if err != nil {
		t.Fatalf("ApplyChanges: %v", err)
	}
	if want := recs[len(recs)-1].LSN; applied != want {
		t.Fatalf("applied LSN = %d, want %d", applied, want)
	}
	assertConverged(t, "after concurrent-writer tail", primary, replica)
}
