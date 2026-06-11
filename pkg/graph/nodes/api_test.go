package nodes

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestAPINilReceiversReturnErrNilGraphOrZero(t *testing.T) {
	t.Parallel()

	var nilAPI *API
	ctx := context.Background()
	id := types.NodeID(42)
	opts := storepkg.QueryOpts{Limit: 1}

	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{name: "Add", run: func() error { _, err := nilAPI.Add(context.Background(), []string{"Node"}, nil); return err }},
		{name: "AddWithContext", run: func() error { _, err := nilAPI.Add(ctx, []string{"Node"}, nil); return err }},
		{name: "Get", run: func() error { _, err := nilAPI.Get(context.Background(), id); return err }},
		{name: "GetWithContext", run: func() error { _, err := nilAPI.Get(ctx, id); return err }},
		{name: "GetByIDs", run: func() error { _, err := nilAPI.GetByIDs([]types.NodeID{id}); return err }},
		{name: "Update", run: func() error { _, err := nilAPI.Update(context.Background(), id, nil); return err }},
		{name: "UpdateWithContext", run: func() error { _, err := nilAPI.Update(ctx, id, nil); return err }},
		{name: "UpdateInPlace", run: func() error { _, err := nilAPI.UpdateInPlace(context.Background(), id, nil); return err }},
		{name: "UpdateInPlaceWithContext", run: func() error { _, err := nilAPI.UpdateInPlace(ctx, id, nil); return err }},
		{name: "Delete", run: func() error { return nilAPI.Delete(context.Background(), id) }},
		{name: "DeleteWithContext", run: func() error { return nilAPI.Delete(ctx, id) }},
		{name: "Import", run: func() error { _, err := nilAPI.Import(ctx, id, []string{"Node"}, nil); return err }},
		{name: "AddByIDIfAbsent", run: func() error { _, _, err := nilAPI.AddByIDIfAbsent(ctx, id, []string{"Node"}, nil); return err }},
		{name: "All", run: func() error { _, err := nilAPI.All(opts); return err }},
		{name: "ByLabel", run: func() error { _, err := nilAPI.ByLabel("Node", opts); return err }},
		{name: "ByLabelAndProperty", run: func() error { _, err := nilAPI.ByLabelAndProperty("Node", "name", "Ada", opts); return err }},
		{name: "Count", run: func() error { _, err := nilAPI.Count(); return err }},
		{name: "CountByLabel", run: func() error { _, err := nilAPI.CountByLabel("Node"); return err }},
		{name: "SetProperty", run: func() error { return nilAPI.SetProperty(ctx, id, "name", "Ada") }},
		{name: "DeleteProperty", run: func() error { return nilAPI.DeleteProperty(ctx, id, "name") }},
		{name: "CompareAndSetProperty", run: func() error {
			_, err := nilAPI.CompareAndSetProperty(context.Background(), id, "name", "old", "new")
			return err
		}},
		{name: "CompareAndSetPropertyWithContext", run: func() error {
			_, err := nilAPI.CompareAndSetProperty(ctx, id, "name", "old", "new")
			return err
		}},
		{name: "AddLabel", run: func() error { return nilAPI.AddLabel(ctx, id, "Admin") }},
		{name: "RemoveLabel", run: func() error { return nilAPI.RemoveLabel(ctx, id, "Admin") }},
		{name: "CloseVersion", run: func() error { return nilAPI.CloseVersion(ctx, id, 100) }},
		{name: "History", run: func() error { _, err := nilAPI.History(id); return err }},
		{name: "VersionAfter", run: func() error { _, err := nilAPI.VersionAfter(id, 1); return err }},
		{name: "VersionBefore", run: func() error { _, err := nilAPI.VersionBefore(id, 1); return err }},
	} {
		if err := tc.run(); !errors.Is(err, grapherr.ErrNilGraph) {
			t.Fatalf("%s = %v, want ErrNilGraph", tc.name, err)
		}
	}
	if nilAPI.HasLabel(nil, "Node") {
		t.Fatal("nil HasLabel = true, want false")
	}
	if got := nilAPI.Labels(nil); got != nil {
		t.Fatalf("nil Labels = %v, want nil", got)
	}
	if got := nilAPI.PrimaryLabel(nil); got != "" {
		t.Fatalf("nil PrimaryLabel = %q, want empty", got)
	}
	if got := nilAPI.NextID(); got != 0 {
		t.Fatalf("nil NextID = %v, want 0", got)
	}

	api := New((*nodeOpsSpy)(nil))
	if _, err := api.Get(context.Background(), id); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil Get = %v, want ErrNilGraph", err)
	}
	if api.HasLabel(nil, "Node") {
		t.Fatal("typed-nil HasLabel = true, want false")
	}
	if got := api.NextID(); got != 0 {
		t.Fatalf("typed-nil NextID = %v, want 0", got)
	}
}

func TestAPIForwardsEveryMethod(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("node op failed")
	ctx := context.Background()
	id := types.NodeID(42)
	opts := storepkg.QueryOpts{Limit: 3}
	ops := &nodeOpsSpy{
		err:          wantErr,
		casResult:    true,
		count:        7,
		hasLabel:     true,
		labels:       []string{"Node", "Admin"},
		primaryLabel: "Node",
		nextID:       types.NodeID(99),
	}
	api := New(ops)

	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{name: "Add", run: func() error {
			_, err := api.Add(context.Background(), []string{"Node"}, map[string]any{"name": "Ada"})
			return err
		}},
		{name: "AddWithContext", run: func() error { _, err := api.Add(ctx, []string{"Node"}, nil); return err }},
		{name: "Get", run: func() error { _, err := api.Get(context.Background(), id); return err }},
		{name: "GetWithContext", run: func() error { _, err := api.Get(ctx, id); return err }},
		{name: "GetByIDs", run: func() error { _, err := api.GetByIDs([]types.NodeID{id}); return err }},
		{name: "Update", run: func() error {
			_, err := api.Update(context.Background(), id, map[string]any{"name": "Grace"})
			return err
		}},
		{name: "UpdateWithContext", run: func() error { _, err := api.Update(ctx, id, nil); return err }},
		{name: "UpdateInPlace", run: func() error { _, err := api.UpdateInPlace(context.Background(), id, nil); return err }},
		{name: "UpdateInPlaceWithContext", run: func() error { _, err := api.UpdateInPlace(ctx, id, nil); return err }},
		{name: "Delete", run: func() error { return api.Delete(context.Background(), id) }},
		{name: "DeleteWithContext", run: func() error { return api.Delete(ctx, id) }},
		{name: "Import", run: func() error { _, err := api.Import(ctx, id, []string{"Node"}, nil); return err }},
		{name: "AddByIDIfAbsent", run: func() error { _, _, err := api.AddByIDIfAbsent(ctx, id, []string{"Node"}, nil); return err }},
		{name: "All", run: func() error { _, err := api.All(opts); return err }},
		{name: "ByLabel", run: func() error { _, err := api.ByLabel("Node", opts); return err }},
		{name: "ByLabelAndProperty", run: func() error { _, err := api.ByLabelAndProperty("Node", "name", "Ada", opts); return err }},
		{name: "Count", run: func() error { _, err := api.Count(); return err }},
		{name: "CountByLabel", run: func() error { _, err := api.CountByLabel("Node"); return err }},
		{name: "SetProperty", run: func() error { return api.SetProperty(ctx, id, "name", "Ada") }},
		{name: "DeleteProperty", run: func() error { return api.DeleteProperty(ctx, id, "name") }},
		{name: "CompareAndSetProperty", run: func() error {
			got, err := api.CompareAndSetProperty(context.Background(), id, "name", "old", "new")
			if !got {
				t.Fatal("CompareAndSetProperty bool = false, want true")
			}
			return err
		}},
		{name: "CompareAndSetPropertyWithContext", run: func() error {
			got, err := api.CompareAndSetProperty(ctx, id, "name", "old", "new")
			if !got {
				t.Fatal("CompareAndSetPropertyWithContext bool = false, want true")
			}
			return err
		}},
		{name: "AddLabel", run: func() error { return api.AddLabel(ctx, id, "Admin") }},
		{name: "RemoveLabel", run: func() error { return api.RemoveLabel(ctx, id, "Admin") }},
		{name: "CloseVersion", run: func() error { return api.CloseVersion(ctx, id, 100) }},
		{name: "History", run: func() error { _, err := api.History(id); return err }},
		{name: "VersionAfter", run: func() error { _, err := api.VersionAfter(id, 1); return err }},
		{name: "VersionBefore", run: func() error { _, err := api.VersionBefore(id, 1); return err }},
	} {
		if err := tc.run(); !errors.Is(err, wantErr) {
			t.Fatalf("%s = %v, want %v", tc.name, err, wantErr)
		}
	}
	if !api.HasLabel(nil, "Node") {
		t.Fatal("HasLabel = false, want true")
	}
	if got := api.Labels(nil); len(got) != 2 || got[1] != "Admin" {
		t.Fatalf("Labels = %v, want [Node Admin]", got)
	}
	labels := api.Labels(nil)
	labels[0] = "Mutated"
	if ops.labels[0] != "Node" {
		t.Fatalf("mutating Labels result changed ops labels: %v", ops.labels)
	}
	if got := api.PrimaryLabel(nil); got != "Node" {
		t.Fatalf("PrimaryLabel = %q, want Node", got)
	}
	if got := api.NextID(); got != types.NodeID(99) {
		t.Fatalf("NextID = %v, want 99", got)
	}

	wantCalls := []string{
		"Add", "Add", "Get", "Get", "GetByIDs",
		"Update", "Update", "UpdateInPlace", "UpdateInPlace",
		"Delete", "Delete", "Import", "AddByIDIfAbsent", "All", "ByLabel", "ByLabelAndProperty",
		"Count", "CountByLabel", "SetProperty", "DeleteProperty",
		"CompareAndSetProperty", "CompareAndSetProperty",
		"AddLabel", "RemoveLabel", "CloseVersion", "History", "VersionAfter", "VersionBefore",
		"HasLabel", "Labels", "Labels", "PrimaryLabel", "NextID",
	}
	if len(ops.calls) != len(wantCalls) {
		t.Fatalf("calls = %v, want %v", ops.calls, wantCalls)
	}
	for i, want := range wantCalls {
		if ops.calls[i] != want {
			t.Fatalf("call[%d] = %s, want %s; all calls %v", i, ops.calls[i], want, ops.calls)
		}
	}
	if ops.lastID != id || ops.lastLabel != "Node" || ops.lastKey != "name" {
		t.Fatalf("forwarded id/label/key = %v/%q/%q", ops.lastID, ops.lastLabel, ops.lastKey)
	}
	if ops.lastOpts != opts {
		t.Fatalf("forwarded opts = %+v, want %+v", ops.lastOpts, opts)
	}
}

type nodeOpsSpy struct {
	err          error
	casResult    bool
	count        int
	hasLabel     bool
	labels       []string
	primaryLabel string
	nextID       types.NodeID

	calls     []string
	lastID    types.NodeID
	lastLabel string
	lastKey   string
	lastOpts  storepkg.QueryOpts
}

func (s *nodeOpsSpy) record(name string) { s.calls = append(s.calls, name) }

func (s *nodeOpsSpy) Add(ctx context.Context, labels []string, props map[string]any) (*types.Node, error) {
	s.record("Add")
	if len(labels) > 0 {
		s.lastLabel = labels[0]
	}
	return nil, s.err
}

func (s *nodeOpsSpy) AddWithContext(ctx context.Context, labels []string, props map[string]any) (*types.Node, error) {
	s.record("AddWithContext")
	return nil, s.err
}

func (s *nodeOpsSpy) Get(ctx context.Context, id types.NodeID) (*types.Node, error) {
	s.record("Get")
	s.lastID = id
	return nil, s.err
}

func (s *nodeOpsSpy) GetWithContext(ctx context.Context, id types.NodeID) (*types.Node, error) {
	s.record("GetWithContext")
	s.lastID = id
	return nil, s.err
}

func (s *nodeOpsSpy) GetByIDs(ids []types.NodeID) ([]*types.Node, error) {
	s.record("GetByIDs")
	if len(ids) > 0 {
		s.lastID = ids[0]
	}
	return nil, s.err
}

func (s *nodeOpsSpy) Update(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
	s.record("Update")
	s.lastID = id
	return nil, s.err
}

func (s *nodeOpsSpy) UpdateWithContext(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
	s.record("UpdateWithContext")
	s.lastID = id
	return nil, s.err
}

func (s *nodeOpsSpy) UpdateInPlace(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
	s.record("UpdateInPlace")
	s.lastID = id
	return nil, s.err
}

func (s *nodeOpsSpy) UpdateInPlaceWithContext(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
	s.record("UpdateInPlaceWithContext")
	s.lastID = id
	return nil, s.err
}

func (s *nodeOpsSpy) Delete(ctx context.Context, id types.NodeID) error {
	s.record("Delete")
	s.lastID = id
	return s.err
}

func (s *nodeOpsSpy) DeleteWithContext(ctx context.Context, id types.NodeID) error {
	s.record("DeleteWithContext")
	s.lastID = id
	return s.err
}

func (s *nodeOpsSpy) Import(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, error) {
	s.record("Import")
	s.lastID = id
	return nil, s.err
}

func (s *nodeOpsSpy) AddByIDIfAbsent(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, bool, error) {
	s.record("AddByIDIfAbsent")
	s.lastID = id
	return nil, false, s.err
}

func (s *nodeOpsSpy) All(opts storepkg.QueryOpts) ([]*types.Node, error) {
	s.record("All")
	s.lastOpts = opts
	return nil, s.err
}

func (s *nodeOpsSpy) ByLabel(label string, opts storepkg.QueryOpts) ([]*types.Node, error) {
	s.record("ByLabel")
	s.lastLabel = label
	s.lastOpts = opts
	return nil, s.err
}

func (s *nodeOpsSpy) ForEachByLabel(label string, opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	s.record("ForEachByLabel")
	s.lastLabel = label
	s.lastOpts = opts
	return s.err
}

func (s *nodeOpsSpy) ForEachByLabelPropertyRange(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	s.record("ForEachByLabelPropertyRange")
	s.lastLabel = label
	s.lastKey = propKey
	s.lastOpts = opts
	return s.err
}

func (s *nodeOpsSpy) ByLabelAndProperty(label, key string, value any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	s.record("ByLabelAndProperty")
	s.lastLabel = label
	s.lastKey = key
	s.lastOpts = opts
	return nil, s.err
}

func (s *nodeOpsSpy) Count() (int, error) {
	s.record("Count")
	return s.count, s.err
}

func (s *nodeOpsSpy) CountByLabel(label string) (int, error) {
	s.record("CountByLabel")
	s.lastLabel = label
	return s.count, s.err
}

func (s *nodeOpsSpy) SetProperty(ctx context.Context, id types.NodeID, key string, value any) error {
	s.record("SetProperty")
	s.lastID = id
	s.lastKey = key
	return s.err
}

func (s *nodeOpsSpy) DeleteProperty(ctx context.Context, id types.NodeID, key string) error {
	s.record("DeleteProperty")
	s.lastID = id
	s.lastKey = key
	return s.err
}

func (s *nodeOpsSpy) CompareAndSetProperty(ctx context.Context, id types.NodeID, key string, expected, newVal any) (bool, error) {
	s.record("CompareAndSetProperty")
	s.lastID = id
	s.lastKey = key
	return s.casResult, s.err
}

func (s *nodeOpsSpy) CompareAndSetPropertyWithContext(ctx context.Context, id types.NodeID, key string, expected, newVal any) (bool, error) {
	s.record("CompareAndSetPropertyWithContext")
	s.lastID = id
	s.lastKey = key
	return s.casResult, s.err
}

func (s *nodeOpsSpy) AddLabel(ctx context.Context, id types.NodeID, label string) error {
	s.record("AddLabel")
	s.lastID = id
	s.lastLabel = label
	return s.err
}

func (s *nodeOpsSpy) RemoveLabel(ctx context.Context, id types.NodeID, label string) error {
	s.record("RemoveLabel")
	s.lastID = id
	s.lastLabel = label
	return s.err
}

func (s *nodeOpsSpy) HasLabel(n *types.Node, label string) bool {
	s.record("HasLabel")
	s.lastLabel = label
	return s.hasLabel
}

func (s *nodeOpsSpy) Labels(n *types.Node) []string {
	s.record("Labels")
	return s.labels
}

func (s *nodeOpsSpy) PrimaryLabel(n *types.Node) string {
	s.record("PrimaryLabel")
	return s.primaryLabel
}

func (s *nodeOpsSpy) CloseVersion(ctx context.Context, id types.NodeID, tm types.Instant) error {
	s.record("CloseVersion")
	s.lastID = id
	return s.err
}

func (s *nodeOpsSpy) History(id types.NodeID) ([]*types.Node, error) {
	s.record("History")
	s.lastID = id
	return nil, s.err
}

func (s *nodeOpsSpy) VersionAfter(id types.NodeID, version uint32) (*types.Node, error) {
	s.record("VersionAfter")
	s.lastID = id
	return nil, s.err
}

func (s *nodeOpsSpy) VersionBefore(id types.NodeID, version uint32) (*types.Node, error) {
	s.record("VersionBefore")
	s.lastID = id
	return nil, s.err
}

func (s *nodeOpsSpy) NextID() types.NodeID {
	s.record("NextID")
	return s.nextID
}
