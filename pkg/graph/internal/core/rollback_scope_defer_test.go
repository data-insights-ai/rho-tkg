package core

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// scopePanicStore is a store with a created-entity rollback scope whose node
// delete panics: scopes counts the scopes begun, open those not yet ended.
type scopePanicStore struct {
	*memory.Store
	scopes, open int
}

func (s *scopePanicStore) WriteStamp() uint64 { return 7 }

func (s *scopePanicStore) ScopeCreatedSince(stamp uint64) (end func()) {
	if stamp != 7 {
		panic("the rollback scopes at another stamp than the transaction took")
	}
	s.scopes++
	s.open++
	return func() { s.open-- }
}

func (s *scopePanicStore) DeleteNodeCascade(types.NodeID) error {
	if s.open != 1 {
		panic("the created node is deleted outside the scope")
	}
	panic("the store's delete panics")
}

// TestRollbackScope_EndsWhenAStepPanics (CUTCOST review 2 N3): a rollback
// scopes its deletes of created entities at the stamp the transaction took
// (store.CreatedRollbackCapability). When a delete panics and the caller
// recovers, the scope has ended: left set, every later read of the store would
// skip its sealed shards without an error.
func TestRollbackScope_EndsWhenAStepPanics(t *testing.T) {
	t.Parallel()
	st := &scopePanicStore{Store: memory.New()}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.AddNode([]string{"A"}, nil); err != nil {
		t.Fatal(err)
	}
	panicked := func() (v any) {
		defer func() { v = recover() }()
		_ = tx.Rollback()
		return nil
	}()
	if panicked == nil || st.scopes != 1 {
		t.Fatalf("the rollback: panic %v, %d scopes begun; want the store's panic inside one scope", panicked, st.scopes)
	}
	if st.open != 0 {
		t.Fatalf("after the recovered panic %d rollback scopes are still set, want none", st.open)
	}
}
