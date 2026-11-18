package memory

import (
	"sync"
	"sync/atomic"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Phase 2 of the temporal-index create once ranged a node's history map
// after RUnlock while a concurrent PutNodeVersion wrote it; the race itself
// shows only under -race. Without it the test still proves the index the
// last create publishes during those writes keeps every node at an instant
// only its history rows cover and at one only its current row covers.
func TestMemoryNodeTemporalEnvelope_ConcurrentPutNodeVersionDuringCreate(t *testing.T) {
	t.Parallel()
	ms := New()
	const n = 200
	for i := 0; i < n; i++ {
		nid := types.NodeID(snowflake.ID(9000 + i))
		if err := ms.PutNode(envNode(nid, 1000, 5000, 6000, 7)); err != nil {
			t.Fatal(err)
		}
		if err := ms.PutNodeVersion(nid, 1, envNode(nid, 1, 100, 200, 7)); err != nil {
			t.Fatal(err)
		}
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	started := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := uint32(2); !stop.Load(); v++ {
			if v == 3 {
				close(started)
			}
			for i := 0; i < n; i++ {
				nid := types.NodeID(snowflake.ID(9000 + i))
				if err := ms.PutNodeVersion(nid, v, envNode(nid, v, 100, 200, 7)); err != nil {
					t.Error(err)
					return
				}
			}
		}
	}()
	<-started
	const rounds = 10
	for round := 0; round < rounds; round++ {
		if err := ms.CreateTemporalIndex(7); err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatal(err)
		}
		if round == rounds-1 {
			break // keep the last index for the checks below
		}
		if err := ms.DropTemporalIndex(7); err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatal(err)
		}
	}
	stop.Store(true)
	wg.Wait()
	for i := 0; i < n; i++ {
		nid := types.NodeID(snowflake.ID(9000 + i))
		// The index vouches for the node (an instant past every row prunes
		// it), so keeping it at 150 and 5500 is the envelope's answer.
		if kept, ok := ms.PruneTemporalCandidates(7, []types.NodeID{nid}, QueryOpts{ValidAt: 7000}); !ok || len(kept) != 0 {
			t.Fatalf("prune %d at 7000 = %v (ok=%v), want [] from a published envelope", nid, kept, ok)
		}
		envMustKeep(t, ms, nid, 150, 5500)
	}
}
