package core

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

// mockRegistrySource is a trivial store.ReplicationSource returning a fixed
// snapshot (or error), so the apply-path refetch can be driven directly.
type mockRegistrySource struct {
	snap *storepkg.RegistrySnapshot
	err  error
}

func (m mockRegistrySource) RegistrySnapshot() (*storepkg.RegistrySnapshot, error) {
	return m.snap, m.err
}

// faultyPersistStore wraps a full memory.Store and adds the registry-persister
// surface so persistRegistries reaches it — with an injectable property-key
// persist failure, to exercise the refetch's persist-then-rollback path.
type faultyPersistStore struct {
	*memory.Store
	failPropKey atomic.Bool
}

func (f *faultyPersistStore) SaveRegistries(*registrypkg.LabelRegistry, *registrypkg.RelTypeRegistry) error {
	return nil
}

func (f *faultyPersistStore) SavePropertyKeyRegistry(*registrypkg.PropertyKeyRegistry) error {
	if f.failPropKey.Load() {
		return errors.New("injected property-key persist failure")
	}
	return nil
}

func (f *faultyPersistStore) LoadPropertyKeyRegistry(*registrypkg.PropertyKeyRegistry) (bool, error) {
	return false, nil
}

// A refetched snapshot whose CapturedAtLSN is below the record's LSN is the
// retryable stale condition — surfaced as errors.Is(ErrPrimaryRegistryStale).
func TestRefetch_StaleSnapshotIsRetryableSentinel(t *testing.T) {
	g := newTestGraph(t)
	if _, err := g.Nodes.Add(context.Background(), []string{"A"}, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
	src := mockRegistrySource{snap: &storepkg.RegistrySnapshot{
		Labels:        []string{"", "A", "B"},
		RelTypes:      g.relTypes.ExportNames(),
		CapturedAtLSN: 10,
	}}
	err := g.refetchRegistriesLocked(src, storepkg.ChangeRecord{LSN: 50})
	if !errors.Is(err, storepkg.ErrPrimaryRegistryStale) {
		t.Fatalf("stale refetch err = %v, want ErrPrimaryRegistryStale", err)
	}
	// The registry must be untouched (the guard fires before any grow).
	if g.labels.Len() != 1 {
		t.Fatalf("registry grew on a stale refetch: len %d", g.labels.Len())
	}
}

// A source whose names do NOT prefix-extend the replica's is a FATAL divergence
// — surfaced as errors.Is(ErrRegistryDiverged), caught even in the grow branch.
func TestRefetch_DivergedSourceIsFatalSentinel(t *testing.T) {
	g := newTestGraph(t)
	if _, err := g.Nodes.Add(context.Background(), []string{"A"}, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Local label at token 1 is "A"; the source claims "Z" there, then grows.
	src := mockRegistrySource{snap: &storepkg.RegistrySnapshot{
		Labels:        []string{"", "Z", "B"},
		RelTypes:      g.relTypes.ExportNames(),
		CapturedAtLSN: 100,
	}}
	err := g.refetchRegistriesLocked(src, storepkg.ChangeRecord{LSN: 50})
	if !errors.Is(err, storepkg.ErrRegistryDiverged) {
		t.Fatalf("diverged refetch err = %v, want ErrRegistryDiverged", err)
	}
	if g.labels.Len() != 1 {
		t.Fatalf("registry grew on a diverged refetch: len %d", g.labels.Len())
	}
}

// On a registry persist failure mid-refetch the in-memory grow is rolled back
// (in-memory never leads what was meant to be durable), and a retry once the
// failure clears converges.
func TestRefetch_PersistFailureRollsBackThenRetryConverges(t *testing.T) {
	fs := &faultyPersistStore{Store: memory.New()}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if _, err := g.Nodes.Add(context.Background(), []string{"A"}, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	src := mockRegistrySource{snap: &storepkg.RegistrySnapshot{
		Labels:        []string{"", "A", "B"},
		RelTypes:      g.relTypes.ExportNames(),
		CapturedAtLSN: 100,
	}}
	rec := storepkg.ChangeRecord{LSN: 50}

	fs.failPropKey.Store(true)
	if err := g.refetchRegistriesLocked(src, rec); err == nil {
		t.Fatal("expected refetch to fail on the injected persist error")
	}
	if _, ok := g.labels.Lookup("B"); ok {
		t.Fatal("label B was NOT rolled back after the persist failure")
	}
	if g.labels.Len() != 1 {
		t.Fatalf("registry not rolled back: len %d, want 1", g.labels.Len())
	}

	fs.failPropKey.Store(false)
	if err := g.refetchRegistriesLocked(src, rec); err != nil {
		t.Fatalf("retry after clearing the fault: %v", err)
	}
	if _, ok := g.labels.Lookup("B"); !ok {
		t.Fatal("label B missing after a successful retry")
	}
}

// The replication sub-API surfaces ErrCapabilityNotSupported (errors.Is) for
// the change-log / MetaKV-dependent methods on a mandatory-only backend.
func TestReplOps_CapabilityDeclinesAreSentinels(t *testing.T) {
	g := newMandatoryOnlyGraph(t)
	if _, err := g.Repl.RegistrySnapshot(); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("RegistrySnapshot = %v, want ErrCapabilityNotSupported", err)
	}
	if _, err := g.Repl.IDSlotLease(); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("IDSlotLease = %v, want ErrCapabilityNotSupported", err)
	}
	if err := g.Repl.SetIDSlotLease(&storepkg.IDSlotLeaseRecord{Slot: 1}); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Errorf("SetIDSlotLease = %v, want ErrCapabilityNotSupported", err)
	}
}

// Nil-receiver calls fail closed with ErrNilGraph (never panic).
func TestReplOps_NilReceiverIsErrNilGraph(t *testing.T) {
	var r *ReplOps
	if _, err := r.RegistrySnapshot(); !errors.Is(err, ErrNilGraph) {
		t.Errorf("RegistrySnapshot(nil) = %v, want ErrNilGraph", err)
	}
	if _, err := r.IDSlotLease(); !errors.Is(err, ErrNilGraph) {
		t.Errorf("IDSlotLease(nil) = %v, want ErrNilGraph", err)
	}
	if err := r.SetIDSlotLease(&storepkg.IDSlotLeaseRecord{Slot: 1}); !errors.Is(err, ErrNilGraph) {
		t.Errorf("SetIDSlotLease(nil) = %v, want ErrNilGraph", err)
	}
}
