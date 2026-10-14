package core

import (
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestAtTxByIDsAcrossChunks: 600 positions over 300 distinct IDs (more than
// effScanChunk; duplicates land in another chunk), half updated after the pin, plus
// unknown IDs, equal NodeAtTx / RelAtTx. Memory and badger (snapshot reopened per chunk).
func TestAtTxByIDsAcrossChunks(t *testing.T) {
	t.Parallel()
	for _, b := range txbBackends()[:2] {
		for _, rel := range []bool{false, true} {
			name := b.name + "/nodes"
			if rel {
				name = b.name + "/rels"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				g := b.open(t, false)
				k := batNodeKit(g)
				if rel {
					k = batRelKit(t, g)
				}
				distinct := make([]int64, 300)
				for i := range distinct {
					distinct[i] = k.add(t, map[string]any{"tkg_valid_from": types.Instant(1000), "i": int64(i)})
				}
				pin, err := g.Temporal.NowTx()
				if err != nil {
					t.Fatalf("NowTx: %v", err)
				}
				for _, id := range distinct[:150] {
					k.upd(t, id, map[string]any{"u": true})
				}
				distinct[299] += 1 << 22 // unknown
				ids := make([]int64, 600)
				for i := range ids {
					ids[i] = distinct[i%len(distinct)]
				}
				if effScanChunk >= len(distinct) {
					t.Fatalf("effScanChunk %d not below the %d distinct IDs", effScanChunk, len(distinct))
				}
				for _, validAt := range []types.Instant{500, pin} {
					got, err := k.batch(ids, validAt, pin)
					if err != nil || len(got) != len(ids) {
						t.Fatalf("batch(validAt %d): %d rows, %v", validAt, len(got), err)
					}
					for i, id := range ids {
						if want := batOne(t, k, id, validAt, pin); got[i] != want {
							t.Fatalf("validAt %d ids[%d]=%d: batch %q, per-ID %q", validAt, i, id, got[i], want)
						}
					}
				}
			})
		}
	}
}

// batFaultStore fails the current-row or history read of one armed ID with a non-not-found error.
type batFaultStore struct {
	storepkg.MandatoryStore
	failCurrent, failHistory int64
}

var errBatFault = errors.New("injected store fault")

func (s *batFaultStore) GetNode(id types.NodeID) (*types.Node, error) {
	if int64(id) == s.failCurrent {
		return nil, errBatFault
	}
	return s.MandatoryStore.GetNode(id)
}

func (s *batFaultStore) GetRelationship(id types.RelID) (*types.Relationship, error) {
	if int64(id) == s.failCurrent {
		return nil, errBatFault
	}
	return s.MandatoryStore.GetRelationship(id)
}

func (s *batFaultStore) GetNodeHistory(id types.NodeID) ([]*types.Node, error) {
	if int64(id) == s.failHistory {
		return nil, errBatFault
	}
	return s.MandatoryStore.GetNodeHistory(id)
}

func (s *batFaultStore) GetRelHistory(id types.RelID) ([]*types.Relationship, error) {
	if int64(id) == s.failHistory {
		return nil, errBatFault
	}
	return s.MandatoryStore.GetRelHistory(id)
}

// TestAtTxByIDsStoreFaultFailsCall: a member's non-not-found store error (current
// row or history read) fails the call, as the per-ID door does; unarmed, it answers.
func TestAtTxByIDsStoreFaultFailsCall(t *testing.T) {
	t.Parallel()
	for _, rel := range []bool{false, true} {
		st := &batFaultStore{MandatoryStore: memory.New()}
		g, err := New(Config{Store: st})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = g.Close() })
		k := batNodeKit(g)
		if rel {
			k = batRelKit(t, g)
		}
		a := k.add(t, map[string]any{"tkg_valid_from": types.Instant(1000)})
		b := k.add(t, map[string]any{"tkg_valid_from": types.Instant(1000)})
		pin, err := g.Temporal.NowTx()
		if err != nil {
			t.Fatalf("NowTx: %v", err)
		}
		k.upd(t, b, map[string]any{"u": true}) // b answers from history at pin
		ids := []int64{a, b, a}
		if got, err := k.batch(ids, pin, pin); err != nil || got[0] == batNil || got[1] == batNil {
			t.Fatalf("rel=%v unarmed: %v, %v", rel, got, err)
		}
		for _, c := range []struct {
			name             string
			current, history int64
		}{{"current-row fault", a, 0}, {"history fault", 0, b}} {
			st.failCurrent, st.failHistory = c.current, c.history
			if _, err := k.one(c.current+c.history, pin, pin); !errors.Is(err, errBatFault) {
				t.Fatalf("rel=%v %s: per-ID door = %v, want the fault (fixture)", rel, c.name, err)
			}
			got, err := k.batch(ids, pin, pin)
			if !errors.Is(err, errBatFault) || got != nil {
				t.Fatalf("rel=%v %s: batch = %v, %v; want the fault and no rows", rel, c.name, got, err)
			}
		}
		st.failCurrent, st.failHistory = 0, 0
	}
}
