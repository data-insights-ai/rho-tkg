package tiered

import (
	"bytes"
	"errors"
	"os"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestMigrateFromBadgerRejectsNilInputs(t *testing.T) {
	src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	dst := newTestTieredStore(t)

	cases := []struct {
		name string
		src  *BadgerStore
		dst  *Store
	}{
		{name: "nil source", src: nil, dst: dst},
		{name: "nil destination", src: src, dst: nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := MigrateFromBadger(tc.src, tc.dst)
			if !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatalf("MigrateFromBadger error = %v, want ErrInvalidStoreMutation", err)
			}
		})
	}
}

func TestMigrateFromBadgerClosedDestinationPrecedesEmptySuccess(t *testing.T) {
	src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	dst := newTestTieredStore(t)
	if err := dst.Close(); err != nil {
		t.Fatalf("Close dst: %v", err)
	}

	err = MigrateFromBadger(src, dst)
	if !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("MigrateFromBadger closed dst error = %v, want ErrStoreClosed", err)
	}
}

func TestMigrateFromBadgerClosedSourceFailsClosed(t *testing.T) {
	src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("Close src: %v", err)
	}

	dst := newTestTieredStore(t)
	err = MigrateFromBadger(src, dst)
	if !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("MigrateFromBadger closed src error = %v, want ErrStoreClosed", err)
	}
}

func TestMigrateFromBadgerRejectsMissingSourceLabelRegistryForData(t *testing.T) {
	src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	gen := newTestGen(t, 0)
	n := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := src.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	dst := newTestTieredStore(t)
	err = MigrateFromBadger(src, dst)
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("MigrateFromBadger missing registry error = %v, want ErrInvalidStoreMutation", err)
	}
}

func TestMigrateFromBadgerRejectsNonEmptyDestination(t *testing.T) {
	src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	labels := registrypkg.NewLabelRegistry()
	caseTok, err := labels.GetOrCreate("Case")
	if err != nil {
		t.Fatalf("GetOrCreate Case: %v", err)
	}
	relTypes := registrypkg.NewRelTypeRegistry()
	if err := src.SaveRegistries(labels, relTypes); err != nil {
		t.Fatalf("SaveRegistries source: %v", err)
	}

	dst := newTestTieredStore(t)
	dst.SetLabelRegistry(labels)
	existing := types.NewNode(types.NodeID(newTestGen(t, 0).Generate()), caseTok, nil)
	if err := dst.PutNode(existing); err != nil {
		t.Fatalf("PutNode destination: %v", err)
	}

	err = MigrateFromBadger(src, dst)
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("MigrateFromBadger non-empty destination error = %v, want ErrInvalidStoreMutation", err)
	}
	if got, err := dst.NodeCount(); err != nil {
		t.Fatalf("NodeCount destination: %v", err)
	} else if got != 1 {
		t.Fatalf("NodeCount after rejected migration = %d, want 1", got)
	}
}

func TestMigrateFromBadgerRejectsDestinationWithHistoryOnly(t *testing.T) {
	src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	labels := registrypkg.NewLabelRegistry()
	caseTok, err := labels.GetOrCreate("Case")
	if err != nil {
		t.Fatalf("GetOrCreate Case: %v", err)
	}
	relTypes := registrypkg.NewRelTypeRegistry()
	if err := src.SaveRegistries(labels, relTypes); err != nil {
		t.Fatalf("SaveRegistries source: %v", err)
	}

	dst := newTestTieredStore(t)
	dst.SetLabelRegistry(labels)
	stale := types.NewNode(types.NodeID(newTestGen(t, 0).Generate()), caseTok, nil)
	if err := dst.PutNodeVersion(stale.ID(), stale.Version(), stale); err != nil {
		t.Fatalf("PutNodeVersion destination: %v", err)
	}
	if got, err := dst.NodeCount(); err != nil {
		t.Fatalf("NodeCount destination: %v", err)
	} else if got != 0 {
		t.Fatalf("NodeCount with history-only destination = %d, want 0", got)
	}

	err = MigrateFromBadger(src, dst)
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("MigrateFromBadger history-only destination error = %v, want ErrInvalidStoreMutation", err)
	}
	history, err := dst.GetNodeHistory(stale.ID())
	if err != nil {
		t.Fatalf("GetNodeHistory after rejected migration: %v", err)
	}
	if len(history) != 1 || history[0].ID() != stale.ID() {
		t.Fatalf("history after rejected migration = %#v, want stale snapshot preserved", history)
	}
}

func TestMigrateFromBadgerRejectsDestinationWithRelationshipHistoryOnly(t *testing.T) {
	src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	labels := registrypkg.NewLabelRegistry()
	if _, err := labels.GetOrCreate("Case"); err != nil {
		t.Fatalf("GetOrCreate Case: %v", err)
	}
	relTypes := registrypkg.NewRelTypeRegistry()
	relTok, err := relTypes.GetOrCreate("LINKS")
	if err != nil {
		t.Fatalf("GetOrCreate LINKS: %v", err)
	}
	if err := src.SaveRegistries(labels, relTypes); err != nil {
		t.Fatalf("SaveRegistries source: %v", err)
	}

	dst := newTestTieredStore(t)
	dst.SetLabelRegistry(labels)
	nodeGen := newTestGen(t, 0)
	startID := types.NodeID(nodeGen.Generate())
	endID := types.NodeID(nodeGen.Generate())
	stale := types.NewRelationship(types.RelID(newTestGen(t, 1).Generate()), relTok, startID, endID)
	if err := dst.PutRelVersion(stale.ID(), stale.Version(), stale); err != nil {
		t.Fatalf("PutRelVersion destination: %v", err)
	}
	if got, err := dst.RelationshipCount(); err != nil {
		t.Fatalf("RelationshipCount destination: %v", err)
	} else if got != 0 {
		t.Fatalf("RelationshipCount with history-only destination = %d, want 0", got)
	}

	err = MigrateFromBadger(src, dst)
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("MigrateFromBadger relationship-history-only destination error = %v, want ErrInvalidStoreMutation", err)
	}
	history, err := dst.GetRelHistory(stale.ID())
	if err != nil {
		t.Fatalf("GetRelHistory after rejected migration: %v", err)
	}
	if len(history) != 1 || history[0].ID() != stale.ID() {
		t.Fatalf("relationship history after rejected migration = %#v, want stale snapshot preserved", history)
	}
}

func TestMigrateFromBadgerRejectsNodeTokenOutsideSourceRegistry(t *testing.T) {
	src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	labels := registrypkg.NewLabelRegistry()
	if _, err := labels.GetOrCreate("Case"); err != nil {
		t.Fatalf("GetOrCreate Case: %v", err)
	}
	if err := src.SaveLabelRegistry(labels); err != nil {
		t.Fatalf("SaveLabelRegistry: %v", err)
	}

	gen := newTestGen(t, 0)
	good := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := src.PutNode(good); err != nil {
		t.Fatalf("PutNode good: %v", err)
	}
	n := types.NewNode(types.NodeID(gen.Generate()), 2, nil)
	if err := src.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	dst := newTestTieredStore(t)
	err = MigrateFromBadger(src, dst)
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("MigrateFromBadger node token error = %v, want ErrInvalidStoreMutation", err)
	}
	if got, countErr := dst.NodeCount(); countErr != nil {
		t.Fatalf("NodeCount destination: %v", countErr)
	} else if got != 0 {
		t.Fatalf("NodeCount after preflight failure = %d, want 0", got)
	}
}

func TestMigrateFromBadgerRejectsRelTypeOutsideSourceRegistry(t *testing.T) {
	src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	labels := registrypkg.NewLabelRegistry()
	caseTok, err := labels.GetOrCreate("Case")
	if err != nil {
		t.Fatalf("GetOrCreate Case: %v", err)
	}
	relTypes := registrypkg.NewRelTypeRegistry()
	if _, err := relTypes.GetOrCreate("RELATED"); err != nil {
		t.Fatalf("GetOrCreate RELATED: %v", err)
	}
	if err := src.SaveRegistries(labels, relTypes); err != nil {
		t.Fatalf("SaveRegistries: %v", err)
	}

	nodeGen := newTestGen(t, 0)
	relGen := newTestGen(t, 1)
	n1 := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	n2 := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	if err := src.PutNode(n1); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	if err := src.PutNode(n2); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}
	r := types.NewRelationship(types.RelID(relGen.Generate()), 2, n1.ID(), n2.ID())
	if err := src.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	dst := newTestTieredStore(t)
	err = MigrateFromBadger(src, dst)
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("MigrateFromBadger rel token error = %v, want ErrInvalidStoreMutation", err)
	}
}

func TestMigrateFromBadgerRejectsRelationshipMissingEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name         string
		missingStart bool
	}{
		{name: "missing start", missingStart: true},
		{name: "missing end", missingStart: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
			if err != nil {
				t.Fatalf("NewBadgerStore: %v", err)
			}
			t.Cleanup(func() { _ = src.Close() })

			labels := registrypkg.NewLabelRegistry()
			caseTok, err := labels.GetOrCreate("Case")
			if err != nil {
				t.Fatalf("GetOrCreate Case: %v", err)
			}
			relTypes := registrypkg.NewRelTypeRegistry()
			relTok, err := relTypes.GetOrCreate("RELATED")
			if err != nil {
				t.Fatalf("GetOrCreate RELATED: %v", err)
			}
			if err := src.SaveRegistries(labels, relTypes); err != nil {
				t.Fatalf("SaveRegistries: %v", err)
			}

			nodeGen := newTestGen(t, 0)
			relGen := newTestGen(t, 1)
			existing := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
			if err := src.PutNode(existing); err != nil {
				t.Fatalf("PutNode existing: %v", err)
			}
			missing := types.NodeID(nodeGen.Generate())
			start := existing.ID()
			end := existing.ID()
			if tc.missingStart {
				start = missing
			} else {
				end = missing
			}
			rel := types.NewRelationship(types.RelID(relGen.Generate()), relTok, start, end)
			if err := src.PutRelEntityAndOut(rel); err != nil {
				t.Fatalf("PutRelEntityAndOut corrupt source rel: %v", err)
			}

			dst := newTestTieredStore(t)
			err = MigrateFromBadger(src, dst)
			if !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatalf("MigrateFromBadger missing endpoint error = %v, want ErrInvalidStoreMutation", err)
			}
			if got, countErr := dst.NodeCount(); countErr != nil {
				t.Fatalf("NodeCount destination: %v", countErr)
			} else if got != 0 {
				t.Fatalf("NodeCount after preflight endpoint failure = %d, want 0", got)
			}
			if got, countErr := dst.RelationshipCount(); countErr != nil {
				t.Fatalf("RelationshipCount destination: %v", countErr)
			} else if got != 0 {
				t.Fatalf("RelationshipCount after preflight endpoint failure = %d, want 0", got)
			}
		})
	}
}

func TestValidateMigrateRelEndpointsPropagatesSourceReadErrors(t *testing.T) {
	t.Run("start read error", func(t *testing.T) {
		src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
		if err != nil {
			t.Fatalf("NewBadgerStore: %v", err)
		}
		if err := src.Close(); err != nil {
			t.Fatalf("Close source: %v", err)
		}

		rel := types.NewRelationship(types.RelID(1), 1, types.NodeID(10), types.NodeID(20))
		if err := validateMigrateRelEndpoints(src, rel); !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("validateMigrateRelEndpoints closed source = %v, want ErrStoreClosed", err)
		}
	})

	t.Run("end read error", func(t *testing.T) {
		src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
		if err != nil {
			t.Fatalf("NewBadgerStore: %v", err)
		}
		t.Cleanup(func() { _ = src.Close() })

		start := types.NewNode(types.NodeID(10), 1, nil)
		if err := src.PutNode(start); err != nil {
			t.Fatalf("PutNode start: %v", err)
		}
		rel := types.NewRelationship(types.RelID(1), 1, start.ID(), 0)
		if err := validateMigrateRelEndpoints(src, rel); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatalf("validateMigrateRelEndpoints invalid end = %v, want ErrInvalidStoreMutation", err)
		}
	})
}

func TestMigrateFromBadgerRollsBackDestinationWritesOnPutFailure(t *testing.T) {
	src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	sourceLabels := registrypkg.NewLabelRegistry()
	signalTok, err := sourceLabels.GetOrCreate("Signal")
	if err != nil {
		t.Fatalf("GetOrCreate Signal: %v", err)
	}
	sourceRelTypes := registrypkg.NewRelTypeRegistry()
	if err := src.SaveRegistries(sourceLabels, sourceRelTypes); err != nil {
		t.Fatalf("SaveRegistries source: %v", err)
	}

	gen := newTestGen(t, 0)
	first := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := src.PutNode(first); err != nil {
		t.Fatalf("PutNode first: %v", err)
	}
	second := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := second.SetProperty("vec", []float32{1}); err != nil {
		t.Fatalf("SetProperty vec: %v", err)
	}
	if err := src.PutNode(second); err != nil {
		t.Fatalf("PutNode second: %v", err)
	}

	dst := newTestTieredStore(t)
	previousLabels := registrypkg.NewLabelRegistry()
	if _, err := previousLabels.GetOrCreate("Case"); err != nil {
		t.Fatalf("GetOrCreate previous Case: %v", err)
	}
	dst.SetLabelRegistry(previousLabels)
	if err := dst.CreateVectorIndex(signalTok, "vec", 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex destination: %v", err)
	}

	err = MigrateFromBadger(src, dst)
	if !errors.Is(err, ErrDimensionMismatch) {
		t.Fatalf("MigrateFromBadger error = %v, want ErrDimensionMismatch", err)
	}
	if got, countErr := dst.NodeCount(); countErr != nil {
		t.Fatalf("NodeCount destination: %v", countErr)
	} else if got != 0 {
		t.Fatalf("NodeCount after rollback = %d, want 0", got)
	}
	if got := dst.ontology.ClassifyByToken(signalTok); got != ClassReference {
		t.Fatalf("destination ontology after rollback = %v, want ClassReference from previous registry", got)
	}
}

func TestMigrateRollbackRestoresDestinationRegistryFile(t *testing.T) {
	dst := newDiskTestTieredStore(t)
	original := []byte("previous registry bytes")
	if err := atomicWriteFile(dst.regFile, original, "test registry setup"); err != nil {
		t.Fatalf("write original registry: %v", err)
	}
	snapshot, err := snapshotMigrateRegistryFile(dst)
	if err != nil {
		t.Fatalf("snapshotMigrateRegistryFile: %v", err)
	}
	if err := atomicWriteFile(dst.regFile, []byte("source registry bytes"), "test registry overwrite"); err != nil {
		t.Fatalf("write overwritten registry: %v", err)
	}

	err = failMigrate(dst, nil, snapshot, true, nil, nil, ErrDimensionMismatch)
	if !errors.Is(err, ErrDimensionMismatch) {
		t.Fatalf("failMigrate error = %v, want ErrDimensionMismatch", err)
	}
	got, err := os.ReadFile(dst.regFile)
	if err != nil {
		t.Fatalf("read restored registry: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("restored registry bytes = %q, want %q", got, original)
	}
}

func TestMigrateRollbackRemovesNewDestinationRegistryFile(t *testing.T) {
	dst := newDiskTestTieredStore(t)
	snapshot, err := snapshotMigrateRegistryFile(dst)
	if err != nil {
		t.Fatalf("snapshotMigrateRegistryFile: %v", err)
	}
	if err := atomicWriteFile(dst.regFile, []byte("source registry bytes"), "test registry create"); err != nil {
		t.Fatalf("write new registry: %v", err)
	}

	err = failMigrate(dst, nil, snapshot, true, nil, nil, ErrDimensionMismatch)
	if !errors.Is(err, ErrDimensionMismatch) {
		t.Fatalf("failMigrate error = %v, want ErrDimensionMismatch", err)
	}
	if _, err := os.ReadFile(dst.regFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("registry file after rollback error = %v, want os.ErrNotExist", err)
	}
}

func TestMigrateFromBadgerSavesRegistriesToDestination(t *testing.T) {
	src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	labels := registrypkg.NewLabelRegistry()
	if _, err := labels.GetOrCreate("Case"); err != nil {
		t.Fatalf("GetOrCreate label: %v", err)
	}
	relTypes := registrypkg.NewRelTypeRegistry()
	if _, err := relTypes.GetOrCreate("RELATED"); err != nil {
		t.Fatalf("GetOrCreate rel type: %v", err)
	}
	if err := src.SaveRegistries(labels, relTypes); err != nil {
		t.Fatalf("SaveRegistries source: %v", err)
	}

	dst := newDiskTestTieredStore(t)
	if err := MigrateFromBadger(src, dst); err != nil {
		t.Fatalf("MigrateFromBadger: %v", err)
	}

	loadedLabels := registrypkg.NewLabelRegistry()
	if count, err := dst.LoadLabelRegistry(loadedLabels); err != nil {
		t.Fatalf("LoadLabelRegistry destination: %v", err)
	} else if count != 1 {
		t.Fatalf("LoadLabelRegistry count = %d, want 1", count)
	}
	if tok, ok := loadedLabels.Lookup("Case"); !ok || tok != 1 {
		t.Fatalf("destination label registry Case = (%d, %v), want (1, true)", tok, ok)
	}

	loadedRelTypes := registrypkg.NewRelTypeRegistry()
	if count, err := dst.LoadRelTypeRegistry(loadedRelTypes); err != nil {
		t.Fatalf("LoadRelTypeRegistry destination: %v", err)
	} else if count != 1 {
		t.Fatalf("LoadRelTypeRegistry count = %d, want 1", count)
	}
	if tok, ok := loadedRelTypes.Lookup("RELATED"); !ok || tok != 1 {
		t.Fatalf("destination reltype registry RELATED = (%d, %v), want (1, true)", tok, ok)
	}
}
