package memory

import (
	"sync"
	"sync/atomic"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Final review of backlog 56, finding 2: Phase 2 ranged a node's history map
// after RUnlock while a concurrent PutNodeVersion wrote it (run with -race).
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
	defer wg.Wait()
	defer stop.Store(true)
	for round := 0; round < 10; round++ {
		if err := ms.CreateTemporalIndex(7); err != nil {
			t.Fatal(err)
		}
		if err := ms.DropTemporalIndex(7); err != nil {
			t.Fatal(err)
		}
	}
}
