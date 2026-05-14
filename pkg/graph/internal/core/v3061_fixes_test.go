package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store/tiered"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/temporal"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/events"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// --- Task 2: extractProvenance signature isolation ---

func TestExtractProvenance_SignatureIsolation(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	sig := []byte{0xAA}
	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{
		"name":          "Alice",
		"tkg_signature": sig,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Mutate caller's slice after AddNode.
	sig[0] = 0xFF

	// Stored signature must be unaffected.
	stored := n.Integrity().Signature
	if stored[0] != 0xAA {
		t.Fatalf("stored signature corrupted: got %x, want AA", stored[0])
	}
}

// --- Task 3: wire signature isolation ---

func TestWireRoundTrip_NodeSignatureIsolation(t *testing.T) {
	t.Parallel()

	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	n.SetIntegrity(&types.NodeIntegrity{
		Hash:      "h1",
		Signature: []byte{0xAA, 0xBB},
	})

	w := storeutil.NodeToWire(n)

	// Mutate wire signature — must not affect original node.
	w.Signature[0] = 0xFF
	if n.Integrity().Signature[0] != 0xAA {
		t.Fatal("nodeToWire shares Signature backing array with original node")
	}

	// Reset wire for decode test.
	w.Signature[0] = 0xAA

	decoded := storeutil.WireToNode(w)

	// Mutate wire again — must not affect decoded node.
	w.Signature[0] = 0xFF
	if decoded.Integrity().Signature[0] != 0xAA {
		t.Fatal("wireToNode shares Signature backing array with wire struct")
	}
}

func TestWireRoundTrip_RelSignatureIsolation(t *testing.T) {
	t.Parallel()

	r := types.NewRelationship(types.RelID(snowflake.ID(200)), 1, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	r.SetIntegrity(&types.RelIntegrity{
		Hash:      "rh1",
		Signature: []byte{0xCC, 0xDD},
	})

	w := storeutil.RelToWire(r)

	// Mutate wire signature — must not affect original rel.
	w.Signature[0] = 0xFF
	if r.Integrity().Signature[0] != 0xCC {
		t.Fatal("relToWire shares Signature backing array with original rel")
	}

	// Reset wire for decode test.
	w.Signature[0] = 0xCC

	decoded := storeutil.WireToRel(w)

	// Mutate wire again — must not affect decoded rel.
	w.Signature[0] = 0xFF
	if decoded.Integrity().Signature[0] != 0xCC {
		t.Fatal("wireToRel shares Signature backing array with wire struct")
	}
}

// --- Task 4: SetEventBus race ---

func TestSetEventBus_NoRace(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	bus := eventspkg.NewEventBus()
	_ = g.Events.SetSync(bus)

	var wg sync.WaitGroup
	const N = 20

	// Writers: AddNode.
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				g.Nodes.Add(context.Background(), []string{"Person"}, nil)
			}
		}()
	}

	// Toggle event bus concurrently.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 50; j++ {
			_ = g.Events.SetSync(bus)
			_ = g.Events.SetSync(nil)
		}
	}()

	wg.Wait()
}

// --- Task 5: SetTemporalConstraints race ---

func TestSetTemporalConstraints_NoRace(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	var wg sync.WaitGroup
	const N = 20

	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)

	// Writers: AddRelationship (reads constraints).
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				g.Rels.Add(context.Background(), "KNOWS", a, b, nil)
			}
		}()
	}

	// Toggle constraints concurrently.
	wg.Add(1)
	go func() {
		defer wg.Done()
		cs := temporalpkg.NewConstraintSet(temporalpkg.TemporalConstraint{
			Kind: temporalpkg.ConstraintRelWithinEndpoints,
		})
		for j := 0; j < 50; j++ {
			if err := g.Constraints.Set(cs); err != nil {
				t.Errorf("Constraints.Set(cs): %v", err)
				return
			}
			if err := g.Constraints.Set(temporalpkg.ConstraintSet{}); err != nil {
				t.Errorf("Constraints.Set(empty): %v", err)
				return
			}
		}
	}()

	wg.Wait()
}

// --- Task 6: Sync event handler no deadlock ---

func TestSyncEventHandler_GraphRead_NoDeadlock(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	bus := eventspkg.NewEventBus()
	_ = g.Events.SetSync(bus)

	// Sync handler calls g.Nodes.Get inside the callback.
	var handlerNodeID types.EntityID
	bus.Subscribe(func(e eventspkg.Event) {
		if e.Type == eventspkg.EventNodeCreate {
			// This would deadlock if publishEvent ran under g.mu.RLock
			// because GetNodeWithContext doesn't acquire g.mu.RLock,
			// but more complex handlers calling write methods would.
			_, _ = g.Nodes.Get(context.Background(), types.NodeID(e.EntityID))
			handlerNodeID = e.EntityID
		}
	})

	done := make(chan struct{})
	go func() {
		n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
		if err != nil {
			t.Errorf("AddNode failed: %v", err)
		}
		_ = n
		close(done)
	}()

	select {
	case <-done:
		// Success — no deadlock.
		if handlerNodeID == 0 {
			t.Fatal("handler was not called")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("deadlock: sync event handler blocked for 2 seconds")
	}
}

// --- Task 7: Config/contract fixes ---

func TestNew_SnowflakeNodeID_Bounds(t *testing.T) {
	t.Parallel()

	// 15 accepted.
	g, err := New(Config{SnowflakeNodeID: 15})
	if err != nil {
		t.Fatalf("SnowflakeNodeID=15 rejected: %v", err)
	}
	g.Close()

	// 16 rejected.
	_, err = New(Config{SnowflakeNodeID: 16})
	if err == nil {
		t.Fatal("SnowflakeNodeID=16 should be rejected")
	}

	// -1 rejected.
	_, err = New(Config{SnowflakeNodeID: -1})
	if err == nil {
		t.Fatal("SnowflakeNodeID=-1 should be rejected")
	}
}

func TestNewTieredStore_ShardWindow_Invalid(t *testing.T) {
	t.Parallel()

	// Negative window.
	_, err := tiered.New(tiered.Config{
		InMemory:    true,
		ShardWindow: -time.Hour,
	})
	if err == nil {
		t.Fatal("negative ShardWindow should be rejected")
	}

	// Sub-minute window.
	_, err = tiered.New(tiered.Config{
		InMemory:    true,
		ShardWindow: 30 * time.Second,
	})
	if err == nil {
		t.Fatal("sub-minute ShardWindow should be rejected")
	}

	// Exactly 1 minute — should succeed.
	ts, err := tiered.New(tiered.Config{
		InMemory:    true,
		ShardWindow: time.Minute,
	})
	if err != nil {
		t.Fatalf("1-minute ShardWindow rejected: %v", err)
	}
	ts.Close()
}

// --- Task 8: Test coverage for uncovered critical paths ---

func TestTx_ImportRelationshipWithID(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	// Create nodes outside tx.
	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)

	relID := snowflake.ID(999999)

	// Import node+rel in tx, commit, verify exists.
	tx, _ := g.BeginTx()
	nodeID := snowflake.ID(888888)
	c, err := tx.ImportNodeWithID(context.Background(), types.NodeID(nodeID), []string{"Place"}, map[string]any{"name": "Berlin"})
	if err != nil {
		t.Fatal(err)
	}

	r, err := tx.ImportRelationshipWithID(context.Background(), types.RelID(relID), "KNOWS", a, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.ID() != types.RelID(relID) {
		t.Fatal("ID mismatch")
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Verify exists.
	got, err := g.Rels.Get(context.Background(), types.RelID(relID))
	if err != nil {
		t.Fatalf("GetRelationship after commit: %v", err)
	}
	_ = got

	// Import duplicate — should fail.
	_, err = g.Rels.Import(context.Background(), types.RelID(relID), "KNOWS", a, b, nil)
	if !errors.Is(err, storepkg.ErrRelExists) {
		t.Fatalf("expected storepkg.ErrRelExists, got %v", err)
	}

	// Rollback: create new rel in tx, rollback, verify original persists.
	tx2, _ := g.BeginTx()
	newRelID := snowflake.ID(777777)
	_, err = tx2.ImportRelationshipWithID(context.Background(), types.RelID(newRelID), "LIKES", a, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx2.Rollback(); err != nil {
		t.Fatal(err)
	}

	// New rel should not exist.
	_, err = g.Rels.Get(context.Background(), types.RelID(newRelID))
	if !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("expected storepkg.ErrRelNotFound after rollback, got %v", err)
	}

	// Original rel should still exist.
	_, err = g.Rels.Get(context.Background(), types.RelID(relID))
	if err != nil {
		t.Fatalf("original rel lost after rollback: %v", err)
	}
}

func TestGetRelsAsOf(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	clk := useTestClock(t, g)

	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)

	// Create rel.
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"weight": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	rid := r.ID()
	txFrom1 := r.Temporal().TxFrom

	// Widen the gap between txFrom1 and the Update's TxFrom so
	// "as-of txFrom1" cleanly resolves to v0 (R5-F10).
	clk.Advance(2 * time.Millisecond)

	// Update rel — creates history entry.
	r2, err := g.Rels.Update(context.Background(), rid, map[string]any{"weight": int64(2)})
	if err != nil {
		t.Fatal(err)
	}
	txFrom2 := r2.Temporal().TxFrom

	// Query between txFrom1 and txFrom2 — should get v0.
	rels, err := g.Temporal.RelsAsOf(txFrom1)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, rel := range rels {
		if rel.ID() == rid {
			found = true
			v, _ := rel.GetProperty("weight")
			if v != int64(1) {
				t.Fatalf("expected weight=1 at txFrom1, got %v", v)
			}
		}
	}
	if !found {
		t.Fatal("rel not found in GetRelsAsOf(txFrom1)")
	}

	// Query at txFrom2 — should get v1.
	rels2, err := g.Temporal.RelsAsOf(txFrom2)
	if err != nil {
		t.Fatal(err)
	}
	found = false
	for _, rel := range rels2 {
		if rel.ID() == rid {
			found = true
			v, _ := rel.GetProperty("weight")
			if v != int64(2) {
				t.Fatalf("expected weight=2 at txFrom2, got %v", v)
			}
		}
	}
	if !found {
		t.Fatal("rel not found in GetRelsAsOf(txFrom2)")
	}
}

func TestCreateDropTemporalIndex(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	// Register the label.
	g.Nodes.Add(context.Background(), []string{"eventspkg.Event"}, nil)

	// Create temporal index.
	if err := g.Index.CreateTemporal("eventspkg.Event"); err != nil {
		t.Fatal(err)
	}

	// Drop.
	if err := g.Index.DeleteTemporal("eventspkg.Event"); err != nil {
		t.Fatal(err)
	}

	// Second drop should error.
	err = g.Index.DeleteTemporal("eventspkg.Event")
	if err == nil {
		t.Fatal("second DropTemporalIndex should fail")
	}
}

func TestDropHighFrequencyIndex(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	// Register the label.
	g.Nodes.Add(context.Background(), []string{"Metric"}, nil)

	// Create HF index.
	if err := g.Index.CreateHighFrequency("Metric", time.Hour); err != nil {
		t.Fatal(err)
	}

	// Drop.
	if err := g.Index.DeleteHighFrequency("Metric"); err != nil {
		t.Fatal(err)
	}

	// Second drop should error.
	err = g.Index.DeleteHighFrequency("Metric")
	if err == nil {
		t.Fatal("second DropHighFrequencyIndex should fail")
	}
}

func TestToFloat32SliceWire(t *testing.T) {
	t.Parallel()

	// []any{float32, float64} input.
	got := storeutil.ToFloat32SliceWire([]any{float32(1.5), float64(2.5)})
	if len(got) != 2 || got[0] != 1.5 || got[1] != 2.5 {
		t.Fatalf("[]any input: got %v", got)
	}

	// []float32 input (passthrough).
	got2 := storeutil.ToFloat32SliceWire([]float32{3.0, 4.0})
	if len(got2) != 2 || got2[0] != 3.0 || got2[1] != 4.0 {
		t.Fatalf("[]float32 input: got %v", got2)
	}

	// nil input.
	got3 := storeutil.ToFloat32SliceWire(nil)
	if got3 != nil {
		t.Fatalf("nil input: got %v", got3)
	}

	// Unsupported type.
	got4 := storeutil.ToFloat32SliceWire("not a slice")
	if got4 != nil {
		t.Fatalf("unsupported input: got %v", got4)
	}
}
