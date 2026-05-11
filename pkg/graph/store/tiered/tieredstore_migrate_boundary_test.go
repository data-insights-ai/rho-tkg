package tiered

import (
	"bytes"
	"errors"
	"os"
	"testing"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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
