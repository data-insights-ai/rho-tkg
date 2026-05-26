package core

import (
	"context"
	"testing"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// Phase 1 proper — bitemporal migration tests.

func TestBitemporalMigration_StampsSchemaVersionOnFreshStore(t *testing.T) {
	st := memory.New()
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	var meta storepkg.MetaKVCapability = st
	got, err := meta.MetaGet(schemaVersionKey)
	if err != nil {
		t.Fatalf("MetaGet: %v", err)
	}
	if string(got) != bitemporalSchemaVersion {
		t.Fatalf("schema_version = %q, want %q", got, bitemporalSchemaVersion)
	}
	if !g.bitemporalMigrated {
		t.Fatal("bitemporalMigrated should be true after migration")
	}
}

func TestBitemporalMigration_IdempotentOnSecondRun(t *testing.T) {
	// Use a shared store across two Core instances without closing the
	// store between them (memory.Store does not survive Close).
	st := memory.New()
	g1, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	// Detach Core from the shared store before second New (don't actually
	// Close the store — that would mark it closed).
	g1.closed.Store(true)
	g1.store = nil

	g2, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("second New: %v", err)
	}
	t.Cleanup(func() { _ = g2.Close() })
	if !g2.bitemporalMigrated {
		t.Fatal("second New should detect existing schema_version and set bitemporalMigrated")
	}
}

func TestBitemporalMigration_ClearsInheritedValidFrom(t *testing.T) {
	// Simulate pre-Phase-1 data: history row v=0 with ValidFrom=5000,
	// current row v=1 with ValidFrom=5000 (inherited) + UpdatedAt set.
	st := memory.New()
	id := types.NodeID(1 << 30)

	histRow := types.NewNode(id, 1, nil)
	histRow.SetVersion(0)
	histRow.SetTemporal(&types.TemporalMetadata{ValidFrom: 5000, TxFrom: 5000})
	if err := st.PutNodeVersion(id, 0, histRow); err != nil {
		t.Fatalf("PutNodeVersion v=0: %v", err)
	}

	current := types.NewNode(id, 1, nil)
	current.SetVersion(1)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 5000, TxFrom: 6000, UpdatedAt: 6000})
	_ = current.SetProperty("x", int64(2))
	if err := st.PutNode(current); err != nil {
		t.Fatalf("PutNode current: %v", err)
	}

	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	// Re-read current — ValidFrom should be cleared on the inherited row.
	cur, err := st.GetNode(id)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if vf := cur.Temporal().ValidFrom; vf != 0 {
		t.Fatalf("post-migration current ValidFrom = %d, want 0 (cleared)", vf)
	}
}

func TestBitemporalMigration_PreservesExplicitValidFrom(t *testing.T) {
	// History row vf=5000, current row with DIFFERENT vf=7000 → not inherited.
	st := memory.New()
	id := types.NodeID(1 << 30 | 1)

	hist := types.NewNode(id, 1, nil)
	hist.SetVersion(0)
	hist.SetTemporal(&types.TemporalMetadata{ValidFrom: 5000, TxFrom: 5000})
	if err := st.PutNodeVersion(id, 0, hist); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}

	current := types.NewNode(id, 1, nil)
	current.SetVersion(1)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 7000, TxFrom: 8000, UpdatedAt: 8000})
	if err := st.PutNode(current); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	cur, _ := st.GetNode(id)
	if vf := cur.Temporal().ValidFrom; vf != 7000 {
		t.Fatalf("post-migration ValidFrom = %d, want 7000 (preserved)", vf)
	}
}

func TestBitemporalMigration_BackendsWithoutMetaKVStaySafe(t *testing.T) {
	// Wrap memory.Store with a type that does NOT implement MetaKVCapability.
	// Migration should be skipped, bitemporalMigrated stays false, heuristic
	// remains active.
	st := &nonMetaStore{Store: memory.New()}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if g.bitemporalMigrated {
		t.Fatal("non-MetaKV store should not flip bitemporalMigrated")
	}

	// Verify normal operations still work.
	_, err = g.Nodes.Add(context.Background(), []string{"L"}, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
}

// nonMetaStore wraps storepkg.Store via interface composition so it
// satisfies the full Store contract but does NOT implement
// MetaKVCapability (which would be promoted by embedding the concrete
// memory.Store). Used to verify migration is gracefully skipped on
// backends without MetaKV support.
type nonMetaStore struct{ storepkg.Store }
