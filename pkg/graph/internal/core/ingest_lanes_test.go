package core

import (
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	snowflakepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/snowflake"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// newLanedCore opens an in-memory graph with the given IngestLanes for the
// generator-level batteries. It uses the memory store so every minted ID (any
// slot) lands somewhere — the batteries assert the CORE minting invariant
// (global value uniqueness), independent of any store's slot claim.
func newLanedCore(t *testing.T, snowflakeNodeID int64, lanes uint8) *Core {
	t.Helper()
	c, err := New(Config{SnowflakeNodeID: snowflakeNodeID, IngestLanes: lanes})
	if err != nil {
		t.Fatalf("New(IngestLanes=%d): %v", lanes, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// slotOf extracts the snowflake node-field (routing slot) from an ID.
func slotOf(id snowflake.ID) uint8 {
	return uint8(snowflakepkg.DecomposeID(id).NodeID)
}

// --- HARD COMMIT GATE (ADR-0007 S4): global ID uniqueness across all lanes ---

// TestLaneGeneratorsGlobalUniqueness is the primary S4 gate. It mints a large
// node+rel population from the interactive pair AND every lane generator, then
// asserts the ENTIRE node∪rel ID set is collision-free — the silent-ID-collision
// class that dropping the even/odd value-uniqueness invariant could reintroduce.
func TestLaneGeneratorsGlobalUniqueness(t *testing.T) {
	const lanes = 6
	c := newLanedCore(t, 0, lanes)

	perSource := 500_000
	if testing.Short() {
		perSource = 50_000
	}

	// Sources: lane 0 (interactive even/odd) + lanes 1..lanes (unified).
	seen := make(map[snowflake.ID]struct{}, perSource*(lanes+1)*2)
	add := func(id snowflake.ID, what string) {
		if _, dup := seen[id]; dup {
			t.Fatalf("DUPLICATE ID %d minted (%s) — value uniqueness violated", id, what)
		}
		seen[id] = struct{}{}
	}

	for laneNum := uint16(0); laneNum <= lanes; laneNum++ {
		for i := 0; i < perSource; i++ {
			add(c.nextNodeIDForLane(laneNum).SnowflakeID(), "node")
			add(c.nextRelIDForLane(laneNum).SnowflakeID(), "rel")
		}
	}

	wantCount := perSource * (lanes + 1) * 2
	if len(seen) != wantCount {
		t.Fatalf("expected %d unique IDs, got %d", wantCount, len(seen))
	}
}

// TestLaneGeneratorsConcurrentUniqueness mints from every lane concurrently
// (the concurrent-ingest scenario) and asserts global uniqueness. Run under
// -race, it also proves the shared generators are safe under parallel Generate.
func TestLaneGeneratorsConcurrentUniqueness(t *testing.T) {
	const lanes = 8
	c := newLanedCore(t, 0, lanes)

	perLane := 100_000
	if testing.Short() {
		perLane = 20_000
	}

	var wg sync.WaitGroup
	perLaneIDs := make([][]snowflake.ID, lanes+1)
	for laneNum := 0; laneNum <= lanes; laneNum++ {
		wg.Add(1)
		go func(laneNum int) {
			defer wg.Done()
			ln := uint16(laneNum)
			ids := make([]snowflake.ID, 0, perLane*2)
			for i := 0; i < perLane; i++ {
				ids = append(ids, c.nextNodeIDForLane(ln).SnowflakeID())
				ids = append(ids, c.nextRelIDForLane(ln).SnowflakeID())
			}
			perLaneIDs[laneNum] = ids
		}(laneNum)
	}
	wg.Wait()

	seen := make(map[snowflake.ID]struct{}, perLane*(lanes+1)*2)
	for _, ids := range perLaneIDs {
		for _, id := range ids {
			if _, dup := seen[id]; dup {
				t.Fatalf("DUPLICATE ID %d across concurrent lanes", id)
			}
			seen[id] = struct{}{}
		}
	}
	if want := perLane * (lanes + 1) * 2; len(seen) != want {
		t.Fatalf("expected %d unique IDs, got %d", want, len(seen))
	}
}

// TestLaneGeneratorSlots verifies each lane generator pins to its OWN distinct
// node-field (slot), none colliding with the interactive pair, and that a
// unified generator stamps the SAME slot on both a node and a rel it mints.
func TestLaneGeneratorSlots(t *testing.T) {
	const snowflakeNodeID = 3 // interactive pair {6,7}
	const lanes = 5
	c := newLanedCore(t, snowflakeNodeID, lanes)

	interactiveNode := slotOf(c.nextNodeID().SnowflakeID())
	interactiveRel := slotOf(c.nextRelID().SnowflakeID())
	if interactiveNode != 6 || interactiveRel != 7 {
		t.Fatalf("interactive slots = {%d,%d}, want {6,7}", interactiveNode, interactiveRel)
	}

	if len(c.laneGenerators) != lanes {
		t.Fatalf("laneGenerators len = %d, want %d", len(c.laneGenerators), lanes)
	}
	slotSeen := map[uint8]bool{interactiveNode: true, interactiveRel: true}
	for laneNum := uint16(1); laneNum <= lanes; laneNum++ {
		nodeSlot := slotOf(c.nextNodeIDForLane(laneNum).SnowflakeID())
		relSlot := slotOf(c.nextRelIDForLane(laneNum).SnowflakeID())
		if nodeSlot != relSlot {
			t.Fatalf("lane %d: unified generator stamped node slot %d != rel slot %d", laneNum, nodeSlot, relSlot)
		}
		if slotSeen[nodeSlot] {
			t.Fatalf("lane %d: slot %d collides with a prior generator's slot", laneNum, nodeSlot)
		}
		slotSeen[nodeSlot] = true
		if want := c.laneSlots[laneNum-1]; nodeSlot != want {
			t.Fatalf("lane %d: minted slot %d != recorded laneSlots %d", laneNum, nodeSlot, want)
		}
	}
	// {6,7} interactive + 5 distinct lane slots = 7 distinct slots.
	if len(slotSeen) != 2+lanes {
		t.Fatalf("expected %d distinct slots, saw %d", 2+lanes, len(slotSeen))
	}
}

// TestLaneGeneratorsDisabledByDefault confirms IngestLanes==0 keeps the legacy
// dual model: no lane generators, and a nonzero lane falls back to interactive.
func TestLaneGeneratorsDisabledByDefault(t *testing.T) {
	c := newLanedCore(t, 0, 0)
	if len(c.laneGenerators) != 0 {
		t.Fatalf("laneGenerators should be empty when IngestLanes==0, got %d", len(c.laneGenerators))
	}
	// A nonzero lane must fall back to the interactive even/odd pair (slots 0,1).
	if got := slotOf(c.nextNodeIDForLane(7).SnowflakeID()); got != 0 {
		t.Fatalf("lane fallback node slot = %d, want 0 (interactive)", got)
	}
	if got := slotOf(c.nextRelIDForLane(7).SnowflakeID()); got != 1 {
		t.Fatalf("lane fallback rel slot = %d, want 1 (interactive)", got)
	}
}

// TestIngestConcurrentLanePinning is the S4 end-to-end integration gate: N
// concurrent ingest sessions on a laned graph each mint through their pinned
// unified generator. It asserts (1) read-your-writes still holds, (2) every
// created node+rel ID is globally unique, and (3) a single session's ENTIRE
// population (nodes AND rels, across multiple sub-group Submits) lands on ONE
// slot — the "group -> one slot -> one shard" property S4 exists to provide.
func TestIngestConcurrentLanePinning(t *testing.T) {
	c, err := New(Config{Store: memory.New(), IngestLanes: 4})
	if err != nil {
		t.Fatalf("New(IngestLanes=4): %v", err)
	}
	t.Cleanup(func() { c.Close() })

	const sessions = 8
	const perSession = 30
	var wg sync.WaitGroup
	sessionSlots := make([]uint8, sessions) // the one slot each session pinned to
	sessionIDs := make([][]snowflake.ID, sessions)
	errCh := make(chan error, sessions)

	for w := 0; w < sessions; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			sess, err := c.Ingest.NewSession(IngestOptions{Concurrent: true})
			if err != nil {
				errCh <- err
				return
			}
			defer sess.Close()
			var mine []snowflake.ID
			var prev *types.Node
			for i := 0; i < perSession; i++ {
				n, err := sess.AddNode([]string{"Event"}, map[string]any{"w": int64(w), "i": int64(i)})
				if err != nil {
					errCh <- err
					return
				}
				mine = append(mine, n.ID().SnowflakeID())
				if prev != nil {
					r, err := sess.AddRelationship("NEXT", prev, n, nil)
					if err != nil {
						errCh <- err
						return
					}
					mine = append(mine, r.ID().SnowflakeID())
				}
				prev = n
				if i%10 == 9 {
					token, err := sess.Submit()
					if err != nil {
						errCh <- err
						return
					}
					if err := c.Ingest.WaitApplied(token); err != nil {
						errCh <- err
						return
					}
				}
			}
			sessionIDs[w] = mine
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

	// (2) global uniqueness + (3) one-slot-per-session.
	seen := make(map[snowflake.ID]struct{})
	for w := 0; w < sessions; w++ {
		ids := sessionIDs[w]
		if len(ids) == 0 {
			t.Fatalf("session %d minted nothing", w)
		}
		slot := slotOf(ids[0])
		for _, id := range ids {
			if got := slotOf(id); got != slot {
				t.Fatalf("session %d: ID %d on slot %d, expected the session's single slot %d", w, id, got, slot)
			}
			if _, dup := seen[id]; dup {
				t.Fatalf("session %d: duplicate global ID %d", w, id)
			}
			seen[id] = struct{}{}
		}
		sessionSlots[w] = slot
	}

	// (1) read-your-writes: every created node is readable at wall-now.
	nodeCount, err := c.Stats.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if nodeCount != sessions*perSession {
		t.Fatalf("node count = %d, want %d", nodeCount, sessions*perSession)
	}
}

// TestBuildLaneGeneratorsSlotExhaustion asserts the fail-closed guard: asking
// for more lanes than the 5-bit node field can hold (2 interactive + lanes > 32)
// is rejected at New, not silently truncated into slot collisions.
func TestBuildLaneGeneratorsSlotExhaustion(t *testing.T) {
	if _, err := New(Config{SnowflakeNodeID: 0, IngestLanes: 31}); err == nil {
		t.Fatal("expected New to fail with IngestLanes=31 (needs 33 slots), got nil")
	}
	// 30 lanes + 2 interactive = 32 slots exactly — must succeed.
	c, err := New(Config{SnowflakeNodeID: 0, IngestLanes: 30})
	if err != nil {
		t.Fatalf("New(IngestLanes=30) should fit exactly 32 slots: %v", err)
	}
	_ = c.Close()
}
