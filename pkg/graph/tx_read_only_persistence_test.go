package graph

import (
	"context"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

type publicTxRegistryCountingStore struct {
	*memory.Store
	registrySaves         int
	propertyRegistrySaves int
}

func (s *publicTxRegistryCountingStore) SaveRegistries(*registrypkg.LabelRegistry, *registrypkg.RelTypeRegistry) error {
	s.registrySaves++
	return nil
}

func (s *publicTxRegistryCountingStore) SavePropertyKeyRegistry(*registrypkg.PropertyKeyRegistry) error {
	s.propertyRegistrySaves++
	return nil
}

func (s *publicTxRegistryCountingStore) LoadPropertyKeyRegistry(*registrypkg.PropertyKeyRegistry) (bool, error) {
	return false, nil
}

func TestTxAPIReadOnlyHelpersSkipRegistryPersistence(t *testing.T) {
	t.Parallel()
	st := &publicTxRegistryCountingStore{Store: memory.New(memory.WithChangeLog())}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	read := func(tx *GraphTx) error {
		_, err := tx.NodeCount()
		return err
	}

	if err := g.Tx().Run(read); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if st.registrySaves != 0 || st.propertyRegistrySaves != 0 {
		t.Fatalf("Run registry saves = (%d, %d), want (0, 0)", st.registrySaves, st.propertyRegistrySaves)
	}

	if err := g.Tx().RunContext(context.Background(), read); err != nil {
		t.Fatalf("RunContext: %v", err)
	}
	if st.registrySaves != 0 || st.propertyRegistrySaves != 0 {
		t.Fatalf("RunContext cumulative registry saves = (%d, %d), want (0, 0)", st.registrySaves, st.propertyRegistrySaves)
	}

	lsn, err := g.Tx().RunWithLSN(read)
	if err != nil {
		t.Fatalf("RunWithLSN: %v", err)
	}
	if lsn != 0 {
		t.Fatalf("RunWithLSN LSN = %d, want 0", lsn)
	}
	if st.registrySaves != 0 || st.propertyRegistrySaves != 0 {
		t.Fatalf("RunWithLSN cumulative registry saves = (%d, %d), want (0, 0)", st.registrySaves, st.propertyRegistrySaves)
	}
}

func TestTxAPIRunContextReadOnlyCancellationRollbackSkipsRegistryPersistence(t *testing.T) {
	t.Parallel()
	st := &publicTxRegistryCountingStore{Store: memory.New(memory.WithChangeLog())}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	err = g.Tx().RunContext(ctx, func(tx *GraphTx) error {
		if _, err := tx.NodeCount(); err != nil {
			return err
		}
		cancel()
		return nil
	})
	if err != context.Canceled {
		t.Fatalf("RunContext cancellation = %v, want context.Canceled", err)
	}
	if st.registrySaves != 0 || st.propertyRegistrySaves != 0 {
		t.Fatalf("canceled RunContext registry saves = (%d, %d), want (0, 0)", st.registrySaves, st.propertyRegistrySaves)
	}
}
