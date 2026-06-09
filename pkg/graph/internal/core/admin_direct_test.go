package core

import (
	"context"
	"errors"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

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

	g, err := New(Config{Store: memory.New()})
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
	g, err := New(Config{Store: store})
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

func TestAdminOpsResetReturnsRegistryCheckpointError(t *testing.T) {
	t.Parallel()

	injected := errors.New("registry checkpoint failed")
	store := &resetRegistryPersistStore{Store: memory.New(), saveErr: injected}
	g, err := New(Config{Store: store})
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
