package graph_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type driftingSidecarStore struct {
	*memory.Store
	listed, written chan struct{}
}

func (s *driftingSidecarStore) NodeIDsByLabel(tok uint16) ([]types.NodeID, error) {
	return s.Store.NodeIDsByLabel(tok)
}
func (s *driftingSidecarStore) RelIDsByType(tok uint16) ([]types.RelID, error) {
	return s.Store.RelIDsByType(tok)
}
func (s *driftingSidecarStore) NodeIDsByLabelMetered(tok uint16, t *store.ReadTally) ([]types.NodeID, error) {
	ids, e := s.Store.NodeIDsByLabelMetered(tok, t)
	close(s.listed)
	<-s.written
	return ids, e
}
func (s *driftingSidecarStore) RelIDsByTypeMetered(tok uint16, t *store.ReadTally) ([]types.RelID, error) {
	return s.Store.RelIDsByTypeMetered(tok, t)
}
func (s *driftingSidecarStore) ForEachLabelTxMember(tok uint16, fn func(types.NodeID, types.Instant) bool) error {
	return s.Store.ForEachLabelTxMember(tok, fn)
}
func (s *driftingSidecarStore) ForEachLabelTxMemberMetered(tok uint16, t *store.ReadTally, fn func(types.NodeID, types.Instant) bool) error {
	return s.Store.ForEachLabelTxMemberMetered(tok, t, fn)
}
func (s *driftingSidecarStore) ForEachRelTypeTxMemberMetered(tok uint16, t *store.ReadTally, fn func(types.RelID, types.Instant) bool) error {
	return s.Store.ForEachRelTypeTxMemberMetered(tok, t, fn)
}

func TestReadBudgetPublicCurrentToSidecarDrift(t *testing.T) {
	for _, limited := range []bool{false, true} {
		t.Run(map[bool]string{false: "control", true: "limited"}[limited], func(t *testing.T) {
			s := &driftingSidecarStore{Store: memory.New(), listed: make(chan struct{}), written: make(chan struct{})}
			g, err := graph.New(graph.Config{Store: s})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			var want []types.NodeID
			var tok uint16
			for range 4 {
				n, e := g.Nodes().Add(t.Context(), []string{"D"}, nil)
				if e != nil {
					t.Fatal(e)
				}
				want = append(want, n.ID())
				tok = n.PrimaryLabelToken().Value()
			}
			if err := s.Store.ForEachLabelTxMember(tok, func(types.NodeID, types.Instant) bool { return true }); err != nil {
				t.Fatal(err)
			}
			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				<-s.listed
				for i := range 4 {
					n := types.NewNode(types.NodeID(100+i), tok, nil)
					n.SetTemporal(&types.TemporalMetadata{TxFrom: 1, ValidFrom: 1})
					if e := s.PutNode(n); e != nil {
						close(s.written)
						done <- e
						return
					}
				}
				close(s.written)
				done <- nil
			}()
			copied, stop := memory.CountTxMembersCopiedForTest()
			defer stop()
			budget := store.ReadBudget{}
			if limited {
				budget.Candidates = 8
			}
			ctx, m := store.WithReadBudget(t.Context(), budget)
			var got []types.NodeID
			err = g.Temporal().ForEachNodeEffectiveByLabelCtx(ctx, "D", pin, func(seg temporal.NodeSegment) bool { got = append(got, seg.Node.ID()); return true })
			if e := <-done; e != nil {
				t.Fatal(e)
			}
			if limited {
				var rb *store.ReadBudgetError
				if !errors.As(err, &rb) || rb.Limit != "candidates" || len(got) != 0 || copied() != 0 {
					t.Fatalf("drift IDs %v, copies %d, work %+v, err %v; want refusal before sidecar copy", got, copied(), m.Work(), err)
				}
			} else {
				for i := range 4 {
					want = append(want, types.NodeID(100+i))
				}
				slices.Sort(want)
				slices.Sort(got)
				if err != nil || !slices.Equal(got, want) || copied() != 8 || m.Work().Candidates != 12 {
					t.Fatalf("control IDs %v, want %v, copies %d, work %+v, err %v", got, want, copied(), m.Work(), err)
				}
			}
		})
	}
}
