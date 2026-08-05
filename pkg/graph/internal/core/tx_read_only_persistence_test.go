package core

import (
	"context"
	"errors"
	"sync"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

var errTxRegistryCheckpoint = errors.New("transaction registry checkpoint failure")

type txRegistryCountingStore struct {
	*memory.Store

	mu                    sync.Mutex
	registrySaves         int
	propertyRegistrySaves int
	saveErr               error
	commitScopeCalls      int
	discardScopeCalls     int
}

func (s *txRegistryCountingStore) SaveRegistries(*registrypkg.LabelRegistry, *registrypkg.RelTypeRegistry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registrySaves++
	return s.saveErr
}

func (s *txRegistryCountingStore) SavePropertyKeyRegistry(*registrypkg.PropertyKeyRegistry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.propertyRegistrySaves++
	return s.saveErr
}

func (s *txRegistryCountingStore) LoadPropertyKeyRegistry(*registrypkg.PropertyKeyRegistry) (bool, error) {
	return false, nil
}

func (s *txRegistryCountingStore) CommitScopedLog(token uint64) (uint64, error) {
	s.mu.Lock()
	s.commitScopeCalls++
	s.mu.Unlock()
	return s.Store.CommitScopedLog(token)
}

func (s *txRegistryCountingStore) DiscardScopedLog(token uint64) error {
	s.mu.Lock()
	s.discardScopeCalls++
	s.mu.Unlock()
	return s.Store.DiscardScopedLog(token)
}

func (s *txRegistryCountingStore) resetCounts() {
	s.mu.Lock()
	s.registrySaves = 0
	s.propertyRegistrySaves = 0
	s.commitScopeCalls = 0
	s.discardScopeCalls = 0
	s.mu.Unlock()
}

func (s *txRegistryCountingStore) counts() (registries, properties, commits, discards int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.registrySaves, s.propertyRegistrySaves, s.commitScopeCalls, s.discardScopeCalls
}

func (s *txRegistryCountingStore) setSaveErr(err error) {
	s.mu.Lock()
	s.saveErr = err
	s.mu.Unlock()
}

func newTxRegistryCountingGraph(t *testing.T, changeLog bool) (*Core, *txRegistryCountingStore) {
	t.Helper()
	opts := []memory.Option{}
	if changeLog {
		opts = append(opts, memory.WithChangeLog())
	}
	st := &txRegistryCountingStore{Store: memory.New(opts...)}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	st.resetCounts()
	return g, st
}

func TestGraphTxReadOnlyCommitSkipsRegistryPersistence(t *testing.T) {
	t.Parallel()
	g, st := newTxRegistryCountingGraph(t, true)

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.NodeCount(); err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	registries, properties, commits, discards := st.counts()
	if registries != 0 || properties != 0 {
		t.Fatalf("read-only Commit registry saves = (%d, %d), want (0, 0)", registries, properties)
	}
	if commits != 1 || discards != 0 {
		t.Fatalf("read-only Commit scope finalization = (commit %d, discard %d), want (1, 0)", commits, discards)
	}
	if got := tx.CommittedLSN(); got != 0 {
		t.Fatalf("CommittedLSN = %d, want 0 for read-only Commit", got)
	}
}

func TestGraphTxWritePathPreservesRegistryCheckpoint(t *testing.T) {
	t.Parallel()
	g, st := newTxRegistryCountingGraph(t, false)

	n, err := g.Nodes.Add(context.Background(), []string{"Checkpointed"}, map[string]any{"status": "before"})
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}
	st.resetCounts()

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.UpdateNode(n.ID(), map[string]any{"status": "after"}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	beforeRegistries, beforeProperties, _, _ := st.counts()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	registries, properties, _, _ := st.counts()
	if registries != beforeRegistries+1 || properties != beforeProperties+1 {
		t.Fatalf("write Commit registry saves = (%d, %d), want (%d, %d)", registries, properties, beforeRegistries+1, beforeProperties+1)
	}
}

func TestGraphTxReadOnlyCommitRetriesDirtyRegistryCheckpoint(t *testing.T) {
	t.Parallel()
	g, st := newTxRegistryCountingGraph(t, false)
	g.registryDirty.Store(true)

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.NodeCount(); err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	st.setSaveErr(errTxRegistryCheckpoint)
	if err := tx.Commit(); !errors.Is(err, errTxRegistryCheckpoint) {
		t.Fatalf("Commit with dirty checkpoint failure = %v, want injected error", err)
	}
	if !g.registryDirty.Load() {
		t.Fatal("failed dirty checkpoint cleared registryDirty")
	}

	st.setSaveErr(nil)
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit retry: %v", err)
	}
	registries, properties, _, _ := st.counts()
	if registries != 2 || properties != 1 {
		t.Fatalf("dirty retry registry saves = (%d, %d), want (2, 1)", registries, properties)
	}
	if g.registryDirty.Load() {
		t.Fatal("successful dirty checkpoint retry left registryDirty set")
	}
}

func TestGraphTxReadOnlyRollbackSkipsRegistryPersistenceAndDiscardsScope(t *testing.T) {
	t.Parallel()
	g, st := newTxRegistryCountingGraph(t, true)

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.NodeCount(); err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	epoch := g.asOfColumns.currentEpoch()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	registries, properties, commits, discards := st.counts()
	if registries != 0 || properties != 0 {
		t.Fatalf("read-only Rollback registry saves = (%d, %d), want (0, 0)", registries, properties)
	}
	if commits != 0 || discards != 1 {
		t.Fatalf("read-only Rollback scope finalization = (commit %d, discard %d), want (0, 1)", commits, discards)
	}
	if got := g.asOfColumns.currentEpoch(); got != epoch {
		t.Fatalf("read-only Rollback AS-OF epoch = %d, want unchanged %d", got, epoch)
	}

	// Rollback must release txMu as well as retire the scope.
	next, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx after read-only Rollback: %v", err)
	}
	if err := next.Rollback(); err != nil {
		t.Fatalf("second Rollback: %v", err)
	}
}
