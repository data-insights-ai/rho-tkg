package graph_test

import (
	"errors"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type driftingCurrentStore struct {
	*badger.Store
	counted, written chan struct{}
	token            uint16
	copied           atomic.Int64
}

func (s *driftingCurrentStore) NodeCountByLabel(tok uint16) (int, error) {
	n, err := s.Store.NodeCountByLabel(tok)
	if tok == s.token {
		close(s.counted)
		<-s.written
	}
	return n, err
}
func (s *driftingCurrentStore) NodeIDsByLabel(tok uint16) ([]types.NodeID, error) {
	ids, err := s.Store.NodeIDsByLabel(tok)
	s.copied.Add(int64(len(ids)))
	return ids, err
}
func (s *driftingCurrentStore) RelIDsByType(tok uint16) ([]types.RelID, error) {
	return s.Store.RelIDsByType(tok)
}
func (s *driftingCurrentStore) NodeIDsByLabelMetered(tok uint16, t *store.ReadTally) ([]types.NodeID, error) {
	if native, ok := any(s.Store).(interface {
		NodeIDsByLabelMetered(uint16, *store.ReadTally) ([]types.NodeID, error)
	}); ok {
		ids, err := native.NodeIDsByLabelMetered(tok, t)
		s.copied.Add(int64(len(ids)))
		return ids, err
	}
	return s.NodeIDsByLabel(tok)
}
func (s *driftingCurrentStore) RelIDsByTypeMetered(tok uint16, t *store.ReadTally) ([]types.RelID, error) {
	if native, ok := any(s.Store).(interface {
		RelIDsByTypeMetered(uint16, *store.ReadTally) ([]types.RelID, error)
	}); ok {
		return native.RelIDsByTypeMetered(tok, t)
	}
	return s.RelIDsByType(tok)
}

func TestReadBudgetPublicCurrentCountDrift(t *testing.T) {
	for _, limited := range []bool{false, true} {
		t.Run(map[bool]string{false: "control", true: "limited"}[limited], func(t *testing.T) {
			bs, err := badger.New(badger.Config{InMemory: true})
			if err != nil {
				t.Fatal(err)
			}
			s := &driftingCurrentStore{Store: bs, counted: make(chan struct{}), written: make(chan struct{})}
			g, err := graph.New(graph.Config{Store: s})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			var want []types.NodeID
			for range 4 {
				n, e := g.Nodes().Add(t.Context(), []string{"D"}, nil)
				if e != nil {
					t.Fatal(e)
				}
				want = append(want, n.ID())
				s.token = n.PrimaryLabelToken().Value()
			}
			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				<-s.counted
				for i := range 4 {
					n := types.NewNode(types.NodeID(100+i), s.token, nil)
					n.SetTemporal(&types.TemporalMetadata{TxFrom: 1, ValidFrom: 1})
					if e := bs.PutNode(n); e != nil {
						close(s.written)
						done <- e
						return
					}
				}
				close(s.written)
				done <- nil
			}()
			budget := store.ReadBudget{}
			if limited {
				budget.Candidates = 4
			}
			ctx, m := store.WithReadBudget(t.Context(), budget)
			var got []types.NodeID
			err = g.Temporal().ForEachNodeEffectiveByLabelCtx(ctx, "D", pin, func(seg temporal.NodeSegment) bool { got = append(got, seg.Node.ID()); return true })
			if e := <-done; e != nil {
				t.Fatal(e)
			}
			if limited {
				var rb *store.ReadBudgetError
				if !errors.As(err, &rb) || rb.Limit != "candidates" || len(got) != 0 || s.copied.Load() != 0 {
					t.Fatalf("drift IDs %v, copied %d, work %+v, err %v; want refusal before native copy", got, s.copied.Load(), m.Work(), err)
				}
			} else {
				for i := range 4 {
					want = append(want, types.NodeID(100+i))
				}
				slices.Sort(want)
				slices.Sort(got)
				if err != nil || !slices.Equal(got, want) || s.copied.Load() != 8 || m.Work().Candidates != 8 {
					t.Fatalf("control IDs %v, want %v, copied %d, work %+v, err %v", got, want, s.copied.Load(), m.Work(), err)
				}
			}
		})
	}
}

type plainMemberOverride struct {
	*badger.Store
	listed int
}

func (s *plainMemberOverride) NodeIDsByLabel(tok uint16) ([]types.NodeID, error) {
	s.listed++
	return s.Store.NodeIDsByLabel(tok)
}
func (s *plainMemberOverride) RelIDsByType(tok uint16) ([]types.RelID, error) {
	s.listed++
	return s.Store.RelIDsByType(tok)
}

func TestReadBudgetPublicUnmeteredMemberOverrideRefuses(t *testing.T) {
	bs, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	s := &plainMemberOverride{Store: bs}
	g, err := graph.New(graph.Config{Store: s})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	var want []types.NodeID
	for range 3 {
		n, e := g.Nodes().Add(t.Context(), []string{"D"}, nil)
		if e != nil {
			t.Fatal(e)
		}
		want = append(want, n.ID())
	}
	pin, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	var got []types.NodeID
	if err := g.Temporal().ForEachNodeEffectiveByLabel("D", pin, func(seg temporal.NodeSegment) bool { got = append(got, seg.Node.ID()); return true }); err != nil || !slices.Equal(got, want) || s.listed != 1 {
		t.Fatalf("plain IDs %v, listed %d, err %v", got, s.listed, err)
	}
	s.listed = 0
	ctx, m := store.WithReadBudget(t.Context(), store.ReadBudget{})
	got = nil
	err = g.Temporal().ForEachNodeEffectiveByLabelCtx(ctx, "D", pin, func(seg temporal.NodeSegment) bool { got = append(got, seg.Node.ID()); return true })
	var rb *store.ReadBudgetError
	if !errors.As(err, &rb) || rb.Limit != "unsupported" || len(got) != 0 || s.listed != 0 || m.Work() != (store.ReadWork{}) {
		t.Fatalf("budget IDs %v, listed %d, work %+v, err %v; want refusal before trusting inherited native methods", got, s.listed, m.Work(), err)
	}
}

type plainSidecarOverride struct {
	*badger.Store
	listed int
}

func (s *plainSidecarOverride) NodeIDsByLabel(tok uint16) ([]types.NodeID, error) {
	return s.Store.NodeIDsByLabel(tok)
}
func (s *plainSidecarOverride) RelIDsByType(tok uint16) ([]types.RelID, error) {
	return s.Store.RelIDsByType(tok)
}
func (s *plainSidecarOverride) NodeIDsByLabelMetered(tok uint16, t *store.ReadTally) ([]types.NodeID, error) {
	return s.Store.NodeIDsByLabelMetered(tok, t)
}
func (s *plainSidecarOverride) RelIDsByTypeMetered(tok uint16, t *store.ReadTally) ([]types.RelID, error) {
	return s.Store.RelIDsByTypeMetered(tok, t)
}
func (s *plainSidecarOverride) ForEachLabelTxMember(_ uint16, _ func(types.NodeID, types.Instant) bool) error {
	s.listed++
	return nil
}

func TestReadBudgetPublicUnmeteredSidecarOverrideRefuses(t *testing.T) {
	bs, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	s := &plainSidecarOverride{Store: bs}
	g, err := graph.New(graph.Config{Store: s})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	var want []types.NodeID
	for range 3 {
		n, e := g.Nodes().Add(t.Context(), []string{"D"}, nil)
		if e != nil {
			t.Fatal(e)
		}
		want = append(want, n.ID())
	}
	pin, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	var got []types.NodeID
	if err := g.Temporal().ForEachNodeEffectiveByLabel("D", pin, func(seg temporal.NodeSegment) bool { got = append(got, seg.Node.ID()); return true }); err != nil || !slices.Equal(got, want) || s.listed != 1 {
		t.Fatalf("plain IDs %v, sidecars %d, err %v", got, s.listed, err)
	}
	s.listed = 0
	ctx, m := store.WithReadBudget(t.Context(), store.ReadBudget{})
	got = nil
	decoded, stop := badger.CountMembershipDecodesForTest()
	defer stop()
	err = g.Temporal().ForEachNodeEffectiveByLabelCtx(ctx, "D", pin, func(seg temporal.NodeSegment) bool { got = append(got, seg.Node.ID()); return true })
	var rb *store.ReadBudgetError
	if !errors.As(err, &rb) || rb.Limit != "unsupported" || len(got) != 0 || s.listed != 0 || decoded() != 0 {
		t.Fatalf("budget IDs %v, sidecars %d, decodes %d, work %+v, err %v; want refusal before trusting inherited sidecar methods", got, s.listed, decoded(), m.Work(), err)
	}
}
