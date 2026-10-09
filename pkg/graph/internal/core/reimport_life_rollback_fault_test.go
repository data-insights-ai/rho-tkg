package core

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// rlRollbackFaultStore wraps the memory store WITHOUT its rollback-trim
// capability (an embedded native capability is not exposed to wrappers,
// nativeHistoryRollbackTrim), so a GraphTx rollback of a re-import rewrites
// the earlier life's rows from a copy — the path sharded and tiered take. The
// history read or the version put of that rewrite can be made to fail.
type rlRollbackFaultStore struct {
	*memory.Store
	err         error
	failHistory atomic.Bool
	failPut     atomic.Bool
}

func (s *rlRollbackFaultStore) GetNodeHistory(id types.NodeID) ([]*types.Node, error) {
	if s.failHistory.Load() {
		return nil, s.err
	}
	return s.Store.GetNodeHistory(id)
}

func (s *rlRollbackFaultStore) GetRelHistory(id types.RelID) ([]*types.Relationship, error) {
	if s.failHistory.Load() {
		return nil, s.err
	}
	return s.Store.GetRelHistory(id)
}

func (s *rlRollbackFaultStore) PutNodeVersion(id types.NodeID, version uint32, n *types.Node) error {
	if s.failPut.Load() {
		return s.err
	}
	return s.Store.PutNodeVersion(id, version, n)
}

func (s *rlRollbackFaultStore) PutRelVersion(id types.RelID, version uint32, r *types.Relationship) error {
	if s.failPut.Load() {
		return s.err
	}
	return s.Store.PutRelVersion(id, version, r)
}

// TestReImportTxRollbackCopyPathReportsFaults: on a store without the
// rollback-trim capability, a failing history read or version put while the
// rollback rewrites the earlier life's rows is returned by Rollback
// (errors.Is), not swallowed; with no fault the same path restores the chain
// exactly (TestReImportTxRollbackRestoresEarlierLife covers the four
// backends). Node and relationship.
func TestReImportTxRollbackCopyPathReportsFaults(t *testing.T) {
	t.Parallel()
	for _, rel := range []bool{false, true} {
		for _, fault := range []string{"none", "history read", "version put"} {
			t.Run(map[bool]string{false: "node", true: "rel"}[rel]+"/"+fault, func(t *testing.T) {
				boom := errors.New("injected rollback fault")
				st := &rlRollbackFaultStore{Store: memory.New(), err: boom}
				g, err := New(Config{Store: st})
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				defer g.Close()
				if g.historyTrim != nil {
					t.Fatalf("fixture: the wrapper exposes the rollback-trim capability")
				}
				useTestClock(t, g)
				e := newCCEnt(t, g, rel)
				id := e.add("T", 1000, nil)
				e.mustUpdate(id, map[string]any{"x": int64(1)})
				e.mustDel(id)
				before := e.chainString(id)
				tx, err := g.BeginTx()
				if err != nil {
					t.Fatalf("BeginTx: %v", err)
				}
				props := map[string]any{"tkg_valid_from": types.Instant(5000), "x": int64(9)}
				if rel {
					s, _ := g.Nodes.Get(context.Background(), e.start)
					en, _ := g.Nodes.Get(context.Background(), e.end)
					if _, err := tx.ImportRelationshipWithID(context.Background(), types.RelID(id), ccType, s, en, props); err != nil {
						t.Fatalf("tx import: %v", err)
					}
				} else if _, err := tx.ImportNodeWithID(context.Background(), types.NodeID(id), []string{ccLabel}, props); err != nil {
					t.Fatalf("tx import: %v", err)
				}
				switch fault {
				case "history read":
					st.failHistory.Store(true)
				case "version put":
					st.failPut.Store(true)
				}
				err = tx.Rollback()
				st.failHistory.Store(false)
				st.failPut.Store(false)
				if fault == "none" {
					if err != nil {
						t.Fatalf("Rollback: %v", err)
					}
					if got := e.chainString(id); got != before {
						t.Fatalf("chain after rollback:%s\n want:%s", got, before)
					}
					return
				}
				if !errors.Is(err, boom) {
					t.Fatalf("Rollback with a failing %s = %v; want the injected fault", fault, err)
				}
			})
		}
	}
}
