package core

import (
	"context"
	"errors"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 13d: Admin.Reset (a whole-graph destructive wipe) had no config-gated
// safety valve, unlike PurgeExpiredNodes/AllowRetentionPurge — any caller
// holding a *Graph handle could wipe every entity/index/history row in one
// call. Config.AllowReset now gates it the same way.
func TestAdminOpsReset_DisabledByDefault(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	if _, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if err := g.Admin.Reset(); !errors.Is(err, ErrResetDisabled) {
		t.Fatalf("Reset without Config.AllowReset = %v, want ErrResetDisabled", err)
	}
	// Refused, not just erroring after a partial wipe — the graph must be untouched.
	if count, err := g.Nodes.Count(); err != nil || count != 1 {
		t.Fatalf("node count after refused Reset = (%d, %v), want (1, nil)", count, err)
	}
}

func TestAdminOpsReset_SucceedsWhenAllowed(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New(), AllowReset: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	if _, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := g.Admin.Reset(); err != nil {
		t.Fatalf("Reset with Config.AllowReset: %v", err)
	}
	if count, err := g.Nodes.Count(); err != nil || count != 0 {
		t.Fatalf("node count after allowed Reset = (%d, %v), want (0, nil)", count, err)
	}
}

func TestAdminOpsClosedGraphReturnsErrGraphClosed(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	id := types.NodeID(1)
	checks := []struct {
		name string
		err  error
	}{
		{name: "Archive", err: g.Admin.Archive(id)},
		{name: "Restore", err: g.Admin.Restore(id)},
		{name: "ForceRotate", err: g.Admin.ForceRotate()},
		{name: "RebuildCatalog", err: g.Admin.RebuildCatalog()},
		{name: "Reset", err: g.Admin.Reset()},
	}
	for _, check := range checks {
		if !errors.Is(check.err, ErrGraphClosed) {
			t.Fatalf("%s after Close = %v, want ErrGraphClosed", check.name, check.err)
		}
	}

	if _, err := g.Admin.ListShards(); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("ListShards after Close = %v, want ErrGraphClosed", err)
	}
	// Stats.Get must also fail closed — pre-fix it silently dropped the
	// error because SnapshotCounters discarded it (B1).
	if _, err := g.Stats.Get(); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("Stats.Get after Close = %v, want ErrGraphClosed", err)
	}
	if _, _, _, _, _, _, _, _, _, _, _, _, err := g.Stats.SnapshotCounters(); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("Stats.SnapshotCounters after Close err = %v, want ErrGraphClosed", err)
	}
	if _, err := g.Admin.Repair(); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("Repair after Close = %v, want ErrGraphClosed", err)
	}
	if _, err := g.Admin.VerifyShard("ref"); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("VerifyShard after Close = %v, want ErrGraphClosed", err)
	}
}

func TestAdminOpsRebuildCatalogTieredSuccess(t *testing.T) {
	t.Parallel()

	g, _ := newTestTieredGraph(t)
	if err := g.Admin.RebuildCatalog(); err != nil {
		t.Fatalf("RebuildCatalog: %v", err)
	}
}

func TestAdminOpsArchiveEventNodeReturnsNotReference(t *testing.T) {
	t.Parallel()

	g, _ := newTestTieredGraph(t)
	signal, err := g.Nodes.Add(context.Background(), []string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode Signal: %v", err)
	}

	err = g.Admin.Archive(signal.ID())
	if !errors.Is(err, tiered.ErrNotReferenceEntity) {
		t.Fatalf("Archive event node error = %v, want ErrNotReferenceEntity", err)
	}
}

func TestAdminOpsResetClearsOperationCounters(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New(), AllowReset: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	a, err := g.Nodes.Add(context.Background(), []string{"ResetStats"}, map[string]any{"v": int64(1)})
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"ResetStats"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "RESET_STATS_REL", a, b, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	if _, err := g.Nodes.Get(context.Background(), a.ID()); err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if _, err := g.Rels.Get(context.Background(), r.ID()); err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if _, err := g.Nodes.Update(context.Background(), a.ID(), map[string]any{"v": int64(2)}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	if _, err := g.Rels.Update(context.Background(), r.ID(), map[string]any{"w": int64(2)}); err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}
	if err := g.Rels.Delete(context.Background(), r.ID()); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	if err := g.Nodes.Delete(context.Background(), b.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	before, _ := g.Stats.Get()
	if before.NodesAdded == 0 || before.NodesRead == 0 || before.NodesUpdated == 0 || before.NodesDeleted == 0 ||
		before.RelsAdded == 0 || before.RelsRead == 0 || before.RelsUpdated == 0 || before.RelsDeleted == 0 {
		t.Fatalf("test did not exercise every operation counter before Reset: %+v", before)
	}

	if err := g.Admin.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	after, _ := g.Stats.Get()
	if after.NodesAdded != 0 || after.NodesRead != 0 || after.NodesUpdated != 0 || after.NodesDeleted != 0 ||
		after.RelsAdded != 0 || after.RelsRead != 0 || after.RelsUpdated != 0 || after.RelsDeleted != 0 {
		t.Fatalf("operation counters after Reset = %+v, want zero operation counters", after)
	}
	if count, err := g.Nodes.Count(); err != nil || count != 0 {
		t.Fatalf("node count after Reset = (%d, %v), want (0, nil)", count, err)
	}
	if count, err := g.Rels.Count(); err != nil || count != 0 {
		t.Fatalf("relationship count after Reset = (%d, %v), want (0, nil)", count, err)
	}
}

func TestAdminOpsResetPersistsRegistrySnapshotAfterClear(t *testing.T) {
	t.Parallel()

	store := &resetRegistryPersistStore{Store: memory.New()}
	g, err := New(Config{Store: store, AllowReset: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	a, err := g.Nodes.Add(context.Background(), []string{"ResetRegistry"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"ResetRegistry"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	if _, err := g.Rels.Add(context.Background(), "RESET_REGISTRY_REL", a, b, nil); err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	callsBefore := store.saveCalls
	if err := g.Admin.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if store.saveCalls != callsBefore+1 {
		t.Fatalf("SaveRegistries calls after Reset = %d, want %d", store.saveCalls, callsBefore+1)
	}
	if !stringSliceContains(store.savedLabels, "ResetRegistry") {
		t.Fatalf("saved labels after Reset = %v, want ResetRegistry preserved", store.savedLabels)
	}
	if !stringSliceContains(store.savedRelTypes, "RESET_REGISTRY_REL") {
		t.Fatalf("saved reltypes after Reset = %v, want RESET_REGISTRY_REL preserved", store.savedRelTypes)
	}
}

// TestAdminOpsResetPreservesIDSlotLease guards BACKLOG 13l: id_slot_lease is
// an EXTERNAL ORCHESTRATOR's failover hint, not graph data, so Reset must not
// wipe it — doing so would risk two nodes colliding on the same
// ID-generation slot after a failover. Two-phase: set a lease, Reset, confirm
// the lease value is still readable afterward (unchanged from what was set).
//
// Uses badger, not memory: memory.Store.Clear() never touches its metaKV map
// at all (MetaKV trivially "survives" Clear on memory regardless of any fix,
// making a memory-backed test non-load-bearing). Badger's Clear() genuinely
// wipes the persisted MetaKV keyspace (DropAll / the LastLSNKey-preserving
// variants), so it is the backend that actually exercises this fix.
func TestAdminOpsResetPreservesIDSlotLease(t *testing.T) {
	t.Parallel()

	bs, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	g, err := New(Config{Store: bs, AllowReset: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	want := &storepkg.IDSlotLeaseRecord{Slot: 7}
	if err := g.Repl.SetIDSlotLease(want); err != nil {
		t.Fatalf("SetIDSlotLease: %v", err)
	}

	if _, err := g.Nodes.Add(context.Background(), []string{"LeaseReset"}, nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if err := g.Admin.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	got, err := g.Repl.IDSlotLease()
	if err != nil {
		t.Fatalf("IDSlotLease after Reset: %v", err)
	}
	if got == nil {
		t.Fatal("IDSlotLease after Reset = nil, want the lease to survive Reset")
	}
	if got.Slot != want.Slot {
		t.Fatalf("IDSlotLease after Reset = %+v, want Slot=%d preserved", got, want.Slot)
	}
}

// TestAdminOpsResetWithNoIDSlotLeaseIsNoOp confirms Reset does not fail or
// fabricate a lease when none was ever set.
func TestAdminOpsResetWithNoIDSlotLeaseIsNoOp(t *testing.T) {
	t.Parallel()

	bs, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	g, err := New(Config{Store: bs, AllowReset: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := g.Admin.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	got, err := g.Repl.IDSlotLease()
	if err != nil {
		t.Fatalf("IDSlotLease after Reset: %v", err)
	}
	if got != nil {
		t.Fatalf("IDSlotLease after Reset with none set = %+v, want nil", got)
	}
}

func TestAdminOpsResetReturnsRegistryCheckpointError(t *testing.T) {
	t.Parallel()

	injected := errors.New("registry checkpoint failed")
	store := &resetRegistryPersistStore{Store: memory.New(), saveErr: injected}
	g, err := New(Config{Store: store, AllowReset: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := g.Admin.Reset(); !errors.Is(err, injected) {
		t.Fatalf("Reset error = %v, want registry checkpoint error", err)
	}
	if store.saveCalls != 1 {
		t.Fatalf("SaveRegistries calls = %d, want 1", store.saveCalls)
	}
	if !g.registryDirty.Load() {
		t.Fatal("registryDirty = false after Reset checkpoint failure, want true")
	}
}

type resetRegistryPersistStore struct {
	*memory.Store
	saveCalls     int
	savedLabels   []string
	savedRelTypes []string
	saveErr       error
}

func (s *resetRegistryPersistStore) SaveRegistries(labels *registrypkg.LabelRegistry, relTypes *registrypkg.RelTypeRegistry) error {
	s.saveCalls++
	s.savedLabels = labels.ExportNames()
	s.savedRelTypes = relTypes.ExportNames()
	return s.saveErr
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
