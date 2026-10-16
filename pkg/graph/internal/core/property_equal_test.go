package core

import (
	"context"
	"sync/atomic"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// lookupCountingStore counts the store's equality lookups.
type lookupCountingStore struct {
	*memory.Store
	calls atomic.Int64
}

func (s *lookupCountingStore) NodesByLabelAndProperty(tok uint16, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	s.calls.Add(1)
	return s.Store.NodesByLabelAndProperty(tok, key, value, opts)
}

func (s *lookupCountingStore) NodesByLabelAndProperties(tok uint16, values map[string]any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	s.calls.Add(1)
	return s.Store.NodesByLabelAndProperties(tok, values, opts)
}

func (s *lookupCountingStore) NodesByLabel(tok uint16, opts storepkg.QueryOpts) ([]*types.Node, error) {
	s.calls.Add(1)
	return s.Store.NodesByLabel(tok, opts)
}

// TestCompositeLookupDoesNotCombineForms: a composite lookup of three integral
// components (12 forms each) reads through the component with the fewest
// forms and filters the rest, never the 12^3 combinations: at most 12 store
// lookups, and the rows of every width.
func TestCompositeLookupDoesNotCombineForms(t *testing.T) {
	st := &lookupCountingStore{Store: memory.New()}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, k := range []string{"a", "b", "c"} {
		if err := g.Index.CreateProperty("P", k); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Index.CreateComposite("P", []string{"a", "b", "c"}); err != nil {
		t.Fatal(err)
	}
	want := map[types.NodeID]bool{}
	for i, props := range []map[string]any{
		{"a": int64(1), "b": 2.0, "c": uint8(3)},
		{"a": 1.0, "b": int32(2), "c": float32(3)},
		{"a": int64(1), "b": int64(2), "c": int64(4)},
	} {
		n, err := g.Nodes.Add(ctx, []string{"P"}, props)
		if err != nil {
			t.Fatal(err)
		}
		want[n.ID()] = i < 2
	}
	st.calls.Store(0)
	got, err := g.Nodes.ByLabelAndProperties("P", map[string]any{"a": 1, "b": 2, "c": 3}, storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if calls := st.calls.Load(); calls > 12 {
		t.Errorf("composite lookup made %d store calls, want <= 12 (no form combinations)", calls)
	}
	if len(got) != 2 || !want[got[0].ID()] || !want[got[1].ID()] {
		t.Errorf("composite lookup = %d rows, want the 2 rows of every width", len(got))
	}
}
