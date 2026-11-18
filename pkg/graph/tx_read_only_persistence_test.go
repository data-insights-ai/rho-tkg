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

// TestTxAPIReadOnlyHelpersSkipRegistryPersistence pins that the public
// transaction doors (Run, RunContext, RunWithLSN, and a RunContext whose
// context is canceled after a read) save no registry when the body only reads,
// and that the same counting store does see saves once a Run interns a new
// label and property key (so a counter that is never wired cannot pass).
func TestTxAPIReadOnlyHelpersSkipRegistryPersistence(t *testing.T) {
	t.Parallel()
	st := &publicTxRegistryCountingStore{Store: memory.New(memory.WithChangeLog())}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	st.registrySaves, st.propertyRegistrySaves = 0, 0

	read := func(tx *GraphTx) error {
		_, err := tx.NodeCount()
		return err
	}
	wantNoSaves := func(door string) {
		t.Helper()
		if st.registrySaves != 0 || st.propertyRegistrySaves != 0 {
			t.Fatalf("%s cumulative registry saves = (%d, %d), want (0, 0)", door, st.registrySaves, st.propertyRegistrySaves)
		}
	}

	if err := g.Tx().Run(read); err != nil {
		t.Fatalf("Run: %v", err)
	}
	wantNoSaves("Run")

	if err := g.Tx().RunContext(context.Background(), read); err != nil {
		t.Fatalf("RunContext: %v", err)
	}
	wantNoSaves("RunContext")

	lsn, err := g.Tx().RunWithLSN(read)
	if err != nil {
		t.Fatalf("RunWithLSN: %v", err)
	}
	if lsn != 0 {
		t.Fatalf("RunWithLSN LSN = %d, want 0", lsn)
	}
	wantNoSaves("RunWithLSN")

	ctx, cancel := context.WithCancel(context.Background())
	err = g.Tx().RunContext(ctx, func(tx *GraphTx) error {
		if err := read(tx); err != nil {
			return err
		}
		cancel()
		return nil
	})
	if err != context.Canceled {
		t.Fatalf("RunContext cancellation = %v, want context.Canceled", err)
	}
	wantNoSaves("canceled RunContext")

	// Positive control: a body that interns a new label and property key saves both.
	if err := g.Tx().Run(func(tx *GraphTx) error {
		_, err := tx.AddNode([]string{"NeverSeenLabel"}, map[string]any{"never_seen_key": 1})
		return err
	}); err != nil {
		t.Fatalf("writing Run: %v", err)
	}
	if st.registrySaves == 0 || st.propertyRegistrySaves == 0 {
		t.Fatalf("writing Run registry saves = (%d, %d), want both > 0", st.registrySaves, st.propertyRegistrySaves)
	}
}
