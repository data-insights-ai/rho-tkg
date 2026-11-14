package graph_test

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type pausedMemberStore struct {
	*badger.Store
	first            uint16
	entered, release chan struct{}
	once             sync.Once
	listed           atomic.Int64
}

func (s *pausedMemberStore) RelIDsByType(tok uint16) ([]types.RelID, error) {
	return s.Store.RelIDsByType(tok)
}

func (s *pausedMemberStore) NodeIDsByLabel(tok uint16) ([]types.NodeID, error) {
	if tok == s.first {
		s.once.Do(func() { close(s.entered); <-s.release })
	}
	ids, err := s.Store.NodeIDsByLabel(tok)
	s.listed.Add(int64(len(ids)))
	return ids, err
}

func (s *pausedMemberStore) NodeIDsByLabelMetered(tok uint16, t *store.ReadTally) ([]types.NodeID, error) {
	ids, err := s.Store.NodeIDsByLabelMetered(tok, t)
	s.listed.Add(int64(len(ids)))
	if tok == s.first {
		s.once.Do(func() { close(s.entered); <-s.release })
	}
	return ids, err
}
func (s *pausedMemberStore) RelIDsByTypeMetered(tok uint16, t *store.ReadTally) ([]types.RelID, error) {
	return s.Store.RelIDsByTypeMetered(tok, t)
}

func TestReadBudgetPublicConcurrentGathers(t *testing.T) {
	for _, limited := range []bool{false, true} {
		t.Run(map[bool]string{false: "control", true: "limited"}[limited], func(t *testing.T) {
			bs, err := badger.New(badger.Config{InMemory: true})
			if err != nil {
				t.Fatal(err)
			}
			s := &pausedMemberStore{Store: bs, entered: make(chan struct{}), release: make(chan struct{})}
			g, err := graph.New(graph.Config{Store: s})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			want := map[string][]types.NodeID{}
			for _, label := range []string{"A", "B"} {
				for range 4 {
					n, err := g.Nodes().Add(t.Context(), []string{label}, nil)
					if err != nil {
						t.Fatal(err)
					}
					want[label] = append(want[label], n.ID())
					if label == "A" {
						s.first = n.PrimaryLabelToken().Value()
					}
				}
			}
			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			b := store.ReadBudget{}
			if limited {
				b.Candidates = 6
			}
			ctx, m := store.WithReadBudget(t.Context(), b)
			type result struct {
				ids []types.NodeID
				err error
			}
			read := func(label string) result {
				var r result
				r.err = g.Temporal().ForEachNodeEffectiveByLabelCtx(ctx, label, pin, func(seg temporal.NodeSegment) bool { r.ids = append(r.ids, seg.Node.ID()); return true })
				slices.Sort(r.ids)
				return r
			}
			done := make(chan result, 1)
			go func() { done <- read("A") }()
			select {
			case <-s.entered:
			case <-time.After(5 * time.Second):
				close(s.release)
				t.Fatal("first gather did not reach its native listing after admission")
			}
			right := read("B")
			before := s.listed.Load()
			close(s.release)
			left := <-done
			if !limited {
				if left.err != nil || right.err != nil || !slices.Equal(left.ids, want["A"]) || !slices.Equal(right.ids, want["B"]) || s.listed.Load() != 8 || m.Work().Candidates != 8 {
					t.Fatalf("control: left %+v, right %+v, listed %d, work %+v", left, right, s.listed.Load(), m.Work())
				}
				return
			}
			var refusal *store.ReadBudgetError
			if !errors.As(right.err, &refusal) || refusal.Limit != "candidates" || len(right.ids) != 0 || before != 4 || s.listed.Load() > 4 {
				t.Fatalf("second gather: %+v, native listings before first released %d, total %d; want refusal before any second listing (four IDs already admitted by the first)", right, before, s.listed.Load())
			}
		})
	}
}
