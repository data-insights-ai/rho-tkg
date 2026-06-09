package core

import (
	"errors"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

func newCoreForRegistryRollbackTest(t *testing.T) *Core {
	t.Helper()
	c, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestGetOrCreateLabelWithSnapshotAllocatesExistingAndRollsBack(t *testing.T) {
	t.Parallel()
	c := newCoreForRegistryRollbackTest(t)

	tok, snapshot, allocated, err := c.getOrCreateLabelWithSnapshot("Case")
	if err != nil {
		t.Fatalf("getOrCreateLabelWithSnapshot allocate: %v", err)
	}
	if tok != 1 || !allocated || len(snapshot) != 1 || snapshot[0] != "" {
		t.Fatalf("allocated label result = tok:%d allocated:%v snapshot:%v, want 1 true [\"\"]", tok, allocated, snapshot)
	}
	if err := c.restoreNewLabelOnError(snapshot, allocated, "Case", nil); err != nil {
		t.Fatalf("restoreNewLabelOnError commit: %v", err)
	}

	tok, snapshot, allocated, err = c.getOrCreateLabelWithSnapshot("Case")
	if err != nil {
		t.Fatalf("getOrCreateLabelWithSnapshot existing: %v", err)
	}
	if tok != 1 || allocated {
		t.Fatalf("existing label result = tok:%d allocated:%v, want 1 false", tok, allocated)
	}
	if err := c.restoreNewLabelOnError(snapshot, allocated, "Case", nil); err != nil {
		t.Fatalf("restoreNewLabelOnError existing: %v", err)
	}

	_, snapshot, allocated, err = c.getOrCreateLabelWithSnapshot("Transient")
	if err != nil {
		t.Fatalf("getOrCreateLabelWithSnapshot transient: %v", err)
	}
	injected := errors.New("injected label failure")
	if rbErr := c.restoreNewLabelOnError(snapshot, allocated, "Transient", injected); !errors.Is(rbErr, injected) {
		t.Fatalf("restoreNewLabelOnError failure = %v, want injected error", rbErr)
	}
	if tok, ok := c.lookupLabelLocked("Transient"); ok || tok != 0 {
		t.Fatalf("transient label survived rollback = (%d, %v), want (0, false)", tok, ok)
	}

	if _, _, _, err := c.getOrCreateLabelWithSnapshot(" "); !errors.Is(err, registrypkg.ErrEmptyName) {
		t.Fatalf("getOrCreateLabelWithSnapshot blank = %v, want ErrEmptyName", err)
	}
}

func TestRelTypeSnapshotProbeAndCachePaths(t *testing.T) {
	t.Parallel()
	c := newCoreForRegistryRollbackTest(t)

	tok, snapshot, allocated, err := c.getOrCreateRelTypeWithSnapshot("LINKED_TO")
	if err != nil {
		t.Fatalf("getOrCreateRelTypeWithSnapshot allocate: %v", err)
	}
	if tok != 1 || !allocated || len(snapshot) != 1 || snapshot[0] != "" {
		t.Fatalf("allocated reltype result = tok:%d allocated:%v snapshot:%v, want 1 true [\"\"]", tok, allocated, snapshot)
	}
	if err := c.restoreNewRelTypeOnError(snapshot, allocated, "LINKED_TO", nil); err != nil {
		t.Fatalf("restoreNewRelTypeOnError commit: %v", err)
	}

	tok, snapshot, allocated, err = c.getOrCreateRelTypeWithSnapshot("LINKED_TO")
	if err != nil {
		t.Fatalf("getOrCreateRelTypeWithSnapshot existing: %v", err)
	}
	if tok != 1 || allocated || snapshot != nil {
		t.Fatalf("existing reltype result = tok:%d allocated:%v snapshot:%v, want 1 false nil", tok, allocated, snapshot)
	}

	tok, snapshot, allocated, err = c.getOrCreateRelTypeWithSnapshot("LINKED_TO")
	if err != nil {
		t.Fatalf("getOrCreateRelTypeWithSnapshot cached: %v", err)
	}
	if tok != 1 || allocated || snapshot != nil {
		t.Fatalf("cached reltype result = tok:%d allocated:%v snapshot:%v, want 1 false nil", tok, allocated, snapshot)
	}

	probeTok, err := c.existingRelTypeOrNextProbeToken("PROBE_ONLY")
	if err != nil {
		t.Fatalf("existingRelTypeOrNextProbeToken new: %v", err)
	}
	if probeTok != 2 {
		t.Fatalf("probe token = %d, want 2", probeTok)
	}
	if tok, ok := c.lookupRelTypeLocked("PROBE_ONLY"); ok || tok != 0 {
		t.Fatalf("probe reltype was allocated = (%d, %v), want (0, false)", tok, ok)
	}

	existingTok, err := c.existingRelTypeOrNextProbeToken("LINKED_TO")
	if err != nil {
		t.Fatalf("existingRelTypeOrNextProbeToken existing: %v", err)
	}
	if existingTok != 1 {
		t.Fatalf("existing probe token = %d, want 1", existingTok)
	}
}

func TestGetOrCreateRelTypeWithSnapshotRollsBackAllocationOnError(t *testing.T) {
	t.Parallel()
	c := newCoreForRegistryRollbackTest(t)

	_, snapshot, allocated, err := c.getOrCreateRelTypeWithSnapshot("TEMP_REL")
	if err != nil {
		t.Fatalf("getOrCreateRelTypeWithSnapshot: %v", err)
	}
	injected := errors.New("injected reltype failure")
	if rbErr := c.restoreNewRelTypeOnError(snapshot, allocated, "TEMP_REL", injected); !errors.Is(rbErr, injected) {
		t.Fatalf("restoreNewRelTypeOnError failure = %v, want injected error", rbErr)
	}
	if tok, ok := c.lookupRelTypeLocked("TEMP_REL"); ok || tok != 0 {
		t.Fatalf("temp reltype survived rollback = (%d, %v), want (0, false)", tok, ok)
	}

	if _, _, _, err := c.getOrCreateRelTypeWithSnapshot(" "); !errors.Is(err, registrypkg.ErrEmptyName) {
		t.Fatalf("getOrCreateRelTypeWithSnapshot blank = %v, want ErrEmptyName", err)
	}
}

func TestRegistryPersistencePanicReleasesRegistryLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(*testing.T, *Core)
	}{
		{
			name: "single label snapshot existing",
			run: func(t *testing.T, c *Core) {
				if _, err := c.labels.GetOrCreate("ExistingLabel"); err != nil {
					t.Fatalf("seed label: %v", err)
				}
				c.registryDirty.Store(true)
				_, _, _, _ = c.getOrCreateLabelWithSnapshot("ExistingLabel")
			},
		},
		{
			name: "multi label snapshot existing",
			run: func(t *testing.T, c *Core) {
				if _, err := c.labels.GetOrCreate("PrimaryLabel"); err != nil {
					t.Fatalf("seed primary label: %v", err)
				}
				if _, err := c.labels.GetOrCreate("ExtraLabel"); err != nil {
					t.Fatalf("seed extra label: %v", err)
				}
				c.registryDirty.Store(true)
				_, _, _, _, _, _ = c.getOrCreateLabelsWithSnapshot([]string{"PrimaryLabel", "ExtraLabel"})
			},
		},
		{
			name: "batch label snapshot existing",
			run: func(t *testing.T, c *Core) {
				if _, err := c.labels.GetOrCreate("BatchLabel"); err != nil {
					t.Fatalf("seed batch label: %v", err)
				}
				c.registryDirty.Store(true)
				_, _, _, _, _ = c.getOrCreateBatchNodeLabelsWithSnapshot([]pendingNode{{labels: []string{"BatchLabel"}}})
			},
		},
		{
			name: "persisted reltype existing",
			run: func(t *testing.T, c *Core) {
				if _, err := c.relTypes.GetOrCreate("EXISTING_REL"); err != nil {
					t.Fatalf("seed reltype: %v", err)
				}
				c.registryDirty.Store(true)
				_, _ = c.getOrCreateRelTypePersisted("EXISTING_REL")
			},
		},
		{
			name: "reltype snapshot existing",
			run: func(t *testing.T, c *Core) {
				if _, err := c.relTypes.GetOrCreate("SNAPSHOT_REL"); err != nil {
					t.Fatalf("seed snapshot reltype: %v", err)
				}
				c.registryDirty.Store(true)
				_, _, _, _ = c.getOrCreateRelTypeWithSnapshot("SNAPSHOT_REL")
			},
		},
		{
			name: "dirty checkpoint",
			run: func(t *testing.T, c *Core) {
				c.registryDirty.Store(true)
				_ = c.checkpointDirtyRegistriesBeforeMutation("panic test")
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, err := New(Config{Store: &registryPersistPanicStore{Store: memory.New()}})
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			didPanic := false
			func() {
				defer func() {
					if recover() != nil {
						didPanic = true
					}
				}()
				tc.run(t, c)
			}()
			if !didPanic {
				t.Fatal("expected registry persistence panic")
			}

			if !c.registryMu.TryLock() {
				t.Fatal("registry mutex remained locked after recovered persistence panic")
			}
			c.registryMu.Unlock()
		})
	}
}

func TestNewlyAllocatedNames(t *testing.T) {
	t.Parallel()

	allocated := newlyAllocatedNames([]string{"", "A"}, []string{"", "A", "B", "C"})
	if len(allocated) != 2 || allocated[0] != "B" || allocated[1] != "C" {
		t.Fatalf("newlyAllocatedNames suffix = %v, want [B C]", allocated)
	}

	fromEmpty := newlyAllocatedNames(nil, []string{"", "A"})
	if len(fromEmpty) != 2 || fromEmpty[0] != "" || fromEmpty[1] != "A" {
		t.Fatalf("newlyAllocatedNames empty before = %v, want [\"\" A]", fromEmpty)
	}
	fromEmpty[1] = "mutated"
	if again := newlyAllocatedNames(nil, []string{"", "A"}); again[1] != "A" {
		t.Fatalf("newlyAllocatedNames did not return an independent copy: %v", again)
	}

	if got := newlyAllocatedNames([]string{"", "A", "B"}, []string{"", "A"}); got != nil {
		t.Fatalf("newlyAllocatedNames shorter after = %v, want nil", got)
	}
	if got := newlyAllocatedNames([]string{"", "A"}, []string{"", "X", "B"}); got != nil {
		t.Fatalf("newlyAllocatedNames mismatched prefix = %v, want nil", got)
	}
}

type registryPersistPanicStore struct {
	*memory.Store
}

func (s *registryPersistPanicStore) SaveRegistries(*registrypkg.LabelRegistry, *registrypkg.RelTypeRegistry) error {
	panic("injected registry persistence panic")
}
