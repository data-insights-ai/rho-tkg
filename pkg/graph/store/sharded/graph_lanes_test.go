package sharded_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestGraphLevelIngestLanesRouteAcrossShards is the S4 sharded integration gate.
// A graph opened with Config.IngestLanes wires per-lane UNIFIED generators; a
// concurrent ingest session pins its whole group to one slot -> one shard. With
// SnowflakeNodeID=0 the interactive pair is {0,1} and 4 lanes occupy slots
// {2,3,4,5}, so a store claiming BaseSlot=0/SlotCount=6 covers every slot the
// core can mint. The test asserts (1) every concurrent write succeeds (no
// ErrSlotNotLocal — the lane IDs landed on claimed shards), (2) the population
// spreads across MULTIPLE lane slots (genuine shard distribution), and (3) each
// session's nodes co-locate on a single slot.
func TestGraphLevelIngestLanesRouteAcrossShards(t *testing.T) {
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 6})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 0, IngestLanes: 4})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	defer func() { _ = g.Close() }()

	const sessions = 8
	const perSession = 20
	var wg sync.WaitGroup
	sessionSlots := make([]int64, sessions)
	errCh := make(chan error, sessions)

	for w := 0; w < sessions; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			sess, err := g.Ingest().NewSession(ingest.IngestOptions{Concurrent: true})
			if err != nil {
				errCh <- err
				return
			}
			defer func() { _ = sess.Close() }()
			slot := int64(-1)
			var prev *types.Node
			for i := 0; i < perSession; i++ {
				n, err := sess.AddNode([]string{"Event"}, map[string]any{"w": int64(w), "i": int64(i)})
				if err != nil {
					errCh <- err
					return
				}
				s := g.Admin().DecomposeNodeID(n.ID()).NodeID
				if slot == -1 {
					slot = s
				} else if s != slot {
					errCh <- fmt.Errorf("session %d: node on slot %d, expected the session's single slot %d", w, s, slot)
					return
				}
				if prev != nil {
					if _, err := sess.AddRelationship("NEXT", prev, n, nil); err != nil {
						errCh <- err
						return
					}
				}
				prev = n
			}
			if _, err := sess.Submit(); err != nil {
				errCh <- err
				return
			}
			sessionSlots[w] = slot
			errCh <- nil
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("session error: %v", err)
		}
	}

	// (2) genuine distribution: the sessions must have used more than one slot.
	distinct := map[int64]struct{}{}
	for _, s := range sessionSlots {
		distinct[s] = struct{}{}
		if s < 2 || s > 5 {
			t.Fatalf("session used slot %d, expected a lane slot in [2,5]", s)
		}
	}
	if len(distinct) < 2 {
		t.Fatalf("expected sessions to spread across >=2 lane slots, saw %d", len(distinct))
	}

	// (1)/(3): all nodes were created and are readable — proves lane IDs routed
	// to claimed shards (a wrong slot would have failed with ErrSlotNotLocal).
	n, err := g.Stats().NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if n != sessions*perSession {
		t.Fatalf("node count = %d, want %d", n, sessions*perSession)
	}
}
