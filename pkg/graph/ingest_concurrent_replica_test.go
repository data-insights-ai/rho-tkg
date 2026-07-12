package graph_test

import (
	"bytes"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The Lanes:N crown test: a primary written by N CONCURRENT ingest sessions
// (§14 concurrent mode — self-applying under the shared read lock, change-log
// records emitted eagerly per store door) must still produce a gapless feed
// that a tailing replica applies to BYTE-EXACT convergence. This is what makes
// the concurrent write door safe to use in a replicated deployment.
func TestIngestConcurrentReplicaByteExact(t *testing.T) {
	primary, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()

	// Seed vocabulary (labels + rel type) before the snapshot so no
	// post-bootstrap token refetch is needed.
	warm, err := primary.Ingest().NewSession(ingest.IngestOptions{Concurrent: true, DeclareLabels: []string{"Event", "Extra"}, DeclareRelTypes: []string{"NEXT"}})
	if err != nil {
		t.Fatalf("warm NewSession: %v", err)
	}
	a, err := warm.AddNode([]string{"Event"}, map[string]any{"n": "seedA"})
	if err != nil {
		t.Fatalf("warm AddNode: %v", err)
	}
	b, err := warm.AddNode([]string{"Event"}, map[string]any{"n": "seedB"})
	if err != nil {
		t.Fatalf("warm AddNode: %v", err)
	}
	if _, err := warm.AddRelationship("NEXT", a, b, nil); err != nil {
		t.Fatalf("warm AddRelationship: %v", err)
	}
	if _, err := warm.Submit(); err != nil {
		t.Fatalf("warm Submit: %v", err)
	}
	if err := warm.Close(); err != nil {
		t.Fatalf("warm Close: %v", err)
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

	// N concurrent sessions: creates + rels + a with-history update + a label
	// add + a cascade delete each — every record kind the concurrent door
	// emits, interleaved across sessions.
	const sessions = 5
	const perSession = 12
	var wg sync.WaitGroup
	errCh := make(chan error, sessions)
	for w := 0; w < sessions; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			sess, err := primary.Ingest().NewSession(ingest.IngestOptions{Concurrent: true})
			if err != nil {
				errCh <- err
				return
			}
			defer sess.Close()
			var mine []*types.Node
			for i := 0; i < perSession; i++ {
				n, err := sess.AddNode([]string{"Event"}, map[string]any{"w": int64(w), "i": int64(i)})
				if err != nil {
					errCh <- err
					return
				}
				mine = append(mine, n)
				if i%3 == 2 {
					if _, err := sess.AddRelationship("NEXT", mine[i-1], n, nil); err != nil {
						errCh <- err
						return
					}
				}
				if i%4 == 3 {
					if _, err := sess.Submit(); err != nil {
						errCh <- err
						return
					}
				}
			}
			if _, err := sess.Submit(); err != nil {
				errCh <- err
				return
			}
			if err := sess.UpdateNode(mine[1].ID(), map[string]any{"w": int64(w), "u": true}); err != nil {
				errCh <- err
				return
			}
			if err := sess.DeleteNode(mine[2].ID()); err != nil { // cascades the 1->2 rel
				errCh <- err
				return
			}
			if _, err := sess.Submit(); err != nil {
				errCh <- err
				return
			}
			errCh <- nil
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent session: %v", err)
		}
	}

	// Gapless feed, then byte-exact convergence.
	var recs []store.ChangeRecord
	if err := primary.Replication().ForEachChange(lsn0, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("no change records from the concurrent sessions")
	}
	for i, rec := range recs {
		if want := lsn0 + uint64(i) + 1; rec.LSN != want {
			t.Fatalf("LSN gap/misorder at index %d: got %d, want %d", i, rec.LSN, want)
		}
	}
	applied, err := replica.Replication().ApplyChanges(recs)
	if err != nil {
		t.Fatalf("ApplyChanges: %v", err)
	}
	if want := recs[len(recs)-1].LSN; applied != want {
		t.Fatalf("applied LSN = %d, want %d", applied, want)
	}
	assertConverged(t, "after concurrent-ingest tail", primary, replica)
}
