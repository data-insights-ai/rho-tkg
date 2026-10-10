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
		{name: "AddWithTx", run: func() error { _, err := nilAPI.AddWithTx(ctx, []string{"Node"}, nil, 1000); return err }},
		{name: "Get", run: func() error { _, err := nilAPI.Get(context.Background(), id); return err }},
		{name: "Lend", run: func() error { _, err := nilAPI.Lend(context.Background(), id); return err }},
		{name: "GetWithContext", run: func() error { _, err := nilAPI.Get(ctx, id); return err }},
		{name: "GetByIDs", run: func() error { _, err := nilAPI.GetByIDs([]types.NodeID{id}); return err }},
		{name: "Update", run: func() error { _, err := nilAPI.Update(context.Background(), id, nil); return err }},
		{name: "UpdateWithContext", run: func() error { _, err := nilAPI.Update(ctx, id, nil); return err }},
		{name: "UpdateInPlace", run: func() error { _, err := nilAPI.UpdateInPlace(context.Background(), id, nil); return err }},
		{name: "UpdateInPlaceWithContext", run: func() error { _, err := nilAPI.UpdateInPlace(ctx, id, nil); return err }},
		{name: "Delete", run: func() error { return nilAPI.Delete(context.Background(), id) }},
		{name: "DeleteWithContext", run: func() error { return nilAPI.Delete(ctx, id) }},
		{name: "DeleteWithTx", run: func() error { return nilAPI.DeleteWithTx(ctx, id, 1000) }},
		{name: "Retract", run: func() error { return nilAPI.Retract(ctx, id) }},
		{name: "RetractWithTx", run: func() error { return nilAPI.RetractWithTx(ctx, id, 1000) }},
		{name: "UpdateWithTx", run: func() error { _, err := nilAPI.UpdateWithTx(ctx, id, nil, 1000); return err }},
		{name: "Import", run: func() error { _, err := nilAPI.Import(ctx, id, []string{"Node"}, nil); return err }},
		{name: "AddByIDIfAbsent", run: func() error { _, _, err := nilAPI.AddByIDIfAbsent(ctx, id, []string{"Node"}, nil); return err }},
		{name: "GetOrCreateByKey", run: func() error { _, _, err := nilAPI.GetOrCreateByKey(ctx, "Node", "name", "Ada", nil); return err }},
		{name: "All", run: func() error { _, err := nilAPI.All(opts); return err }},
		{name: "ForEach", run: func() error { return nilAPI.ForEach(opts, func(*types.Node) bool { return true }) }},
		{name: "Iter", run: func() error { return drainNodeIter(nilAPI.Iter(ctx, opts)) }},
		{name: "ByLabel", run: func() error { _, err := nilAPI.ByLabel("Node", opts); return err }},
		{name: "ByLabelAndProperty", run: func() error { _, err := nilAPI.ByLabelAndProperty("Node", "name", "Ada", opts); return err }},
		{name: "ByLabelAndProperties", run: func() error {
			_, err := nilAPI.ByLabelAndProperties("Node", map[string]any{"name": "Ada"}, opts)
			return err
		}},
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
		{name: "HasHistory", run: func() error { _, err := nilAPI.HasHistory(id); return err }},
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
		{name: "AddWithTx", run: func() error { _, err := api.AddWithTx(ctx, []string{"Node"}, nil, 1000); return err }},
		{name: "Get", run: func() error { _, err := api.Get(context.Background(), id); return err }},
		{name: "GetWithContext", run: func() error { _, err := api.Get(ctx, id); return err }},
		{name: "Lend", run: func() error { _, err := api.Lend(ctx, id); return err }},
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
		{name: "DeleteWithTx", run: func() error { return api.DeleteWithTx(ctx, id, 1000) }},
		{name: "Retract", run: func() error { return api.Retract(ctx, id) }},
		{name: "RetractWithTx", run: func() error { return api.RetractWithTx(ctx, id, 1000) }},
		{name: "UpdateWithTx", run: func() error { _, err := api.UpdateWithTx(ctx, id, nil, 1000); return err }},
		{name: "Import", run: func() error { _, err := api.Import(ctx, id, []string{"Node"}, nil); return err }},
		{name: "AddByIDIfAbsent", run: func() error { _, _, err := api.AddByIDIfAbsent(ctx, id, []string{"Node"}, nil); return err }},
		{name: "GetOrCreateByKey", run: func() error { _, _, err := api.GetOrCreateByKey(ctx, "Node", "name", "Ada", nil); return err }},
		{name: "All", run: func() error { _, err := api.All(opts); return err }},
		{name: "ForEach", run: func() error { return api.ForEach(opts, func(*types.Node) bool { return true }) }},
		{name: "Iter", run: func() error { return drainNodeIter(api.Iter(ctx, opts)) }},
		{name: "ByLabel", run: func() error { _, err := api.ByLabel("Node", opts); return err }},
		{name: "ByLabelAndProperty", run: func() error { _, err := api.ByLabelAndProperty("Node", "name", "Ada", opts); return err }},
		{name: "ByLabelAndProperties", run: func() error {
			_, err := api.ByLabelAndProperties("Node", map[string]any{"name": "Ada"}, opts)
			return err
		}},
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
		{name: "HasHistory", run: func() error { _, err := api.HasHistory(id); return err }},
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
		"Add", "Add", "AddWithTx", "Get", "Get", "Lend", "GetByIDs",
		"Update", "Update", "UpdateInPlace", "UpdateInPlace",
		"Delete", "Delete", "DeleteWithTx", "Retract", "RetractWithTx", "UpdateWithTx", "Import", "AddByIDIfAbsent", "GetOrCreateByKey", "All", "ForEach", "ForEach", "ByLabel", "ByLabelAndProperty", "ByLabelAndProperties",
		"Count", "CountByLabel", "SetProperty", "DeleteProperty",
		"CompareAndSetProperty", "CompareAndSetProperty",
		"AddLabel", "RemoveLabel", "CloseVersion", "History", "HasHistory", "VersionAfter", "VersionBefore",
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
	hasHistory   bool
	stamps       [2]types.Instant
	deleted      bool

	calls     []string
	lastID    types.NodeID
	lastLabel string
	lastKey   string
	lastOpts  storepkg.QueryOpts
	lastTx    types.Instant

	lastUpdates map[string]any
}

func (s *nodeOpsSpy) record(name string) { s.calls = append(s.calls, name) }

func (s *nodeOpsSpy) Add(ctx context.Context, labels []string, props map[string]any) (*types.Node, error) {
	s.record("Add")
	if len(labels) > 0 {
		s.lastLabel = labels[0]
	}
	return nil, s.err
}

func (s *nodeOpsSpy) AddWithTx(ctx context.Context, labels []string, props map[string]any, txFrom types.Instant) (*types.Node, error) {
	s.record("AddWithTx")
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

func (s *nodeOpsSpy) Lend(ctx context.Context, id types.NodeID) (*types.Node, error) {
	s.record("Lend")
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

func (s *nodeOpsSpy) DeleteWithTx(ctx context.Context, id types.NodeID, txTo types.Instant) error {
	s.record("DeleteWithTx")
	s.lastID = id
	s.lastTx = txTo
	return s.err
}

func (s *nodeOpsSpy) Retract(ctx context.Context, id types.NodeID) error {
	s.record("Retract")
	s.lastID = id
	return s.err
}

func (s *nodeOpsSpy) RetractWithTx(ctx context.Context, id types.NodeID, txTo types.Instant) error {
	s.record("RetractWithTx")
	s.lastID = id
	s.lastTx = txTo
	return s.err
}

func (s *nodeOpsSpy) UpdateWithTx(ctx context.Context, id types.NodeID, updates map[string]any, txFrom types.Instant) (*types.Node, error) {
	s.record("UpdateWithTx")
	s.lastID = id
	s.lastTx = txFrom
	s.lastUpdates = updates
	return nil, s.err
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

func (s *nodeOpsSpy) GetOrCreateByKey(ctx context.Context, label, propertyKey string, value any, extraProps map[string]any) (*types.Node, bool, error) {
	s.record("GetOrCreateByKey")
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

func (s *nodeOpsSpy) RangeCardinality(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error) {
	s.record("RangeCardinality")
	s.lastLabel = label
	s.lastOpts = opts
	return 0, false, s.err
}

func (s *nodeOpsSpy) ForEachDocValues(label string, propKeys []string, fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error) {
	s.record("ForEachDocValues")
	s.lastLabel = label
	return 0, false, s.err
}

func (s *nodeOpsSpy) ForEachDocValuesMulti(labels []string, propKeys []string, fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error) {
	s.record("ForEachDocValuesMulti")
	if len(labels) > 0 {
		s.lastLabel = labels[0]
	}
	return 0, false, s.err
}

func (s *nodeOpsSpy) DocValuesSnapshot(label string, propKeys []string) (types.NodeColumnReader, uint64, bool, error) {
	s.record("DocValuesSnapshot")
	s.lastLabel = label
	return nil, 0, false, s.err
}

func (s *nodeOpsSpy) DocValuesSnapshotAsOf(label string, propKeys []string, txAt types.Instant) (types.NodeColumnReader, uint64, bool, error) {
	s.record("DocValuesSnapshotAsOf")
	s.lastLabel = label
	return nil, 0, false, s.err
}

func (s *nodeOpsSpy) ForEachDocValuesAsOf(label string, propKeys []string, txAt types.Instant, fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error) {
	s.record("ForEachDocValuesAsOf")
	s.lastLabel = label
	return 0, false, s.err
}

func (s *nodeOpsSpy) NodeMutationEpoch() uint64 {
	s.record("NodeMutationEpoch")
	return 0
}

func (s *nodeOpsSpy) NodeLabelMutationEpoch(label string) uint64 {
	s.record("NodeLabelMutationEpoch")
	s.lastLabel = label
	return 0
}

func (s *nodeOpsSpy) ForEachByLabel(label string, opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	s.record("ForEachByLabel")
	s.lastLabel = label
	s.lastOpts = opts
	return s.err
}

func (s *nodeOpsSpy) ForEach(opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	s.record("ForEach")
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

func (s *nodeOpsSpy) ForEachByLabelPropertyRangeOrdered(label, propKey string, min, max float64, inclMin, inclMax, desc bool, opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	s.record("ForEachByLabelPropertyRangeOrdered")
	s.lastLabel = label
	s.lastKey = propKey
	s.lastOpts = opts
	return s.err
}

func (s *nodeOpsSpy) ForEachByLabelPropertyPrefix(label, propKey, prefix string, desc bool, opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	s.record("ForEachByLabelPropertyPrefix")
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

func (s *nodeOpsSpy) ByLabelAndProperties(label string, values map[string]any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	s.record("ByLabelAndProperties")
	s.lastLabel = label
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

func (s *nodeOpsSpy) HasHistory(id types.NodeID) (bool, error) {
	s.record("HasHistory")
	s.lastID = id
	return s.hasHistory, s.err
}

func (s *nodeOpsSpy) LatestStamps(id types.NodeID) (types.Instant, types.Instant, bool, error) {
	s.record("LatestStamps")
	s.lastID = id
	return s.stamps[0], s.stamps[1], s.deleted, s.err
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

func (s *nodeOpsSpy) MaxOrdinal() (uint32, bool, error) {
	s.record("MaxOrdinal")
	return 7, true, s.err
}

func TestMaxOrdinalForwards(t *testing.T) {
	var nilAPI *API
	if _, _, err := nilAPI.MaxOrdinal(); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil MaxOrdinal: %v", err)
	}
	if m, ok, err := New(&nodeOpsSpy{}).MaxOrdinal(); m != 7 || !ok || err != nil {
		t.Fatalf("MaxOrdinal = %d %v %v", m, ok, err)
	}
}

func (s *nodeOpsSpy) ScanKeepsOrder(string) (bool, error) {
	s.record("ScanKeepsOrder")
	return true, s.err
}

func TestScanKeepsOrderForwards(t *testing.T) {
	var nilAPI *API
	if _, err := nilAPI.ScanKeepsOrder("X"); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil ScanKeepsOrder: %v", err)
	}
	if ok, err := New(&nodeOpsSpy{}).ScanKeepsOrder("X"); !ok || err != nil {
		t.Fatalf("ScanKeepsOrder = %v %v", ok, err)
	}
}

func (s *nodeOpsSpy) DocValuesColumn(string, string) (storepkg.DocValuesColumn, bool, error) {
	s.record("DocValuesColumn")
	return storepkg.DocValuesString, true, s.err
}

func TestDocValuesColumnForwards(t *testing.T) {
	var nilAPI *API
	if _, _, err := nilAPI.DocValuesColumn("L", "k"); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil DocValuesColumn: %v", err)
	}
	if kind, ok, err := New(&nodeOpsSpy{}).DocValuesColumn("L", "k"); kind != storepkg.DocValuesString || !ok || err != nil {
		t.Fatalf("DocValuesColumn = %v %v %v", kind, ok, err)
	}
}

func (s *nodeOpsSpy) CountByLabelAt(string, storepkg.QueryOpts) (int, error) {
	s.record("CountByLabelAt")
	return s.count, s.err
}

func TestCountByLabelAtForwards(t *testing.T) {
	var nilAPI *API
	if _, err := nilAPI.CountByLabelAt("L", storepkg.QueryOpts{ValidAt: 1}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil CountByLabelAt: %v", err)
	}
	spy := &nodeOpsSpy{count: 7}
	if n, err := New(spy).CountByLabelAt("L", storepkg.QueryOpts{ValidAt: 1}); n != 7 || err != nil {
		t.Fatalf("CountByLabelAt = %d %v", n, err)
	}
	if len(spy.calls) != 1 || spy.calls[0] != "CountByLabelAt" {
		t.Fatalf("calls = %v", spy.calls)
	}
}

// The caller-instant doors must hand the instant, id and update map to the ops
// verbatim. Catches a facade that drops the instant (forwards 0, which the
// core reads as invalid), swaps it for the clock, forwards to the plain
// Delete/Update, or rewrites the update map (e.g. injecting a reserved
// tkg_tx_from/tkg_tx_to property instead of passing the argument).
func TestAPIWithTxDoorsForwardInstantVerbatim(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		at   types.Instant
	}{{"min", 1}, {"typical", 1767268800000}, {"negative passes through to core validation", -7}} {
		ops := &nodeOpsSpy{}
		api := New(ops)
		if err := api.DeleteWithTx(ctx, 42, tc.at); err != nil {
			t.Fatalf("%s: DeleteWithTx: %v", tc.name, err)
		}
		if len(ops.calls) != 1 || ops.calls[0] != "DeleteWithTx" || ops.lastID != 42 || ops.lastTx != tc.at {
			t.Fatalf("%s: DeleteWithTx forwarded calls=%v id=%v at=%d; want [DeleteWithTx] 42 %d", tc.name, ops.calls, ops.lastID, ops.lastTx, tc.at)
		}
		upd := map[string]any{"w": int64(2)}
		if _, err := api.UpdateWithTx(ctx, 44, upd, tc.at); err != nil {
			t.Fatalf("%s: UpdateWithTx: %v", tc.name, err)
		}
		if len(ops.calls) != 2 || ops.calls[1] != "UpdateWithTx" || ops.lastID != 44 || ops.lastTx != tc.at {
			t.Fatalf("%s: UpdateWithTx forwarded calls=%v id=%v at=%d; want [.. UpdateWithTx] 44 %d", tc.name, ops.calls, ops.lastID, ops.lastTx, tc.at)
		}
		if len(ops.lastUpdates) != 1 || ops.lastUpdates["w"] != int64(2) {
			t.Fatalf("%s: UpdateWithTx forwarded updates %v; want exactly {w:2}", tc.name, ops.lastUpdates)
		}
	}
}

// HasHistory forwards the id and returns the ops answer unchanged: a wrapper
// that drops the bool (always false) or swaps the id fails here.
func TestAPIHasHistoryForwardsAnswer(t *testing.T) {
	t.Parallel()
	for _, want := range []bool{true, false} {
		spy := &nodeOpsSpy{hasHistory: want}
		got, err := New(spy).HasHistory(types.NodeID(77))
		if err != nil || got != want {
			t.Fatalf("HasHistory = %v, %v; want %v, nil", got, err, want)
		}
		if spy.lastID != types.NodeID(77) || len(spy.calls) != 1 || spy.calls[0] != "HasHistory" {
			t.Fatalf("forwarded id %v calls %v", spy.lastID, spy.calls)
		}
	}
	if got, err := New((*nodeOpsSpy)(nil)).HasHistory(1); got || !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil HasHistory = %v, %v; want false, ErrNilGraph", got, err)
	}
}

// LatestStamps forwards the id and returns the ops answer unchanged (both
// stamps, the deleted flag, the error): a wrapper that swaps the stamps, drops
// deleted or swaps the id fails here; a nil or typed-nil ops is ErrNilGraph.
func TestAPILatestStampsForwardsAnswer(t *testing.T) {
	t.Parallel()
	for _, deleted := range []bool{true, false} {
		spy := &nodeOpsSpy{stamps: [2]types.Instant{11, 22}, deleted: deleted}
		from, to, gotDeleted, err := New(spy).LatestStamps(types.NodeID(77))
		if err != nil || from != 11 || to != 22 || gotDeleted != deleted {
			t.Fatalf("LatestStamps = (%d, %d, %v, %v); want (11, 22, %v, nil)", from, to, gotDeleted, err, deleted)
		}
		if spy.lastID != types.NodeID(77) || len(spy.calls) != 1 || spy.calls[0] != "LatestStamps" {
			t.Fatalf("forwarded id %v calls %v", spy.lastID, spy.calls)
		}
	}
	wantErr := errors.New("ops failed")
	if _, _, _, err := New(&nodeOpsSpy{err: wantErr}).LatestStamps(1); !errors.Is(err, wantErr) {
		t.Fatalf("ops error = %v, want %v", err, wantErr)
	}
	if _, _, _, err := New((*nodeOpsSpy)(nil)).LatestStamps(1); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil LatestStamps = %v, want ErrNilGraph", err)
	}
	var nilAPI *API
	if _, _, _, err := nilAPI.LatestStamps(1); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil API LatestStamps = %v, want ErrNilGraph", err)
	}
}

// Backlog 43: the retraction doors must reach Ops.Retract / Ops.RetractWithTx
// with the id and the instant verbatim. Catches a facade that forwards a
// retraction to Delete / DeleteWithTx (Retract as Delete: the past stays
// readable), drops the instant, or swaps the id.
func TestAPIRetractDoorsForwardVerbatim(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ops := &nodeOpsSpy{}
	api := New(ops)
	if err := api.Retract(ctx, 41); err != nil {
		t.Fatalf("Retract: %v", err)
	}
	if len(ops.calls) != 1 || ops.calls[0] != "Retract" || ops.lastID != 41 {
		t.Fatalf("Retract forwarded calls=%v id=%v; want [Retract] 41", ops.calls, ops.lastID)
	}
	for _, at := range []types.Instant{1, 1767268800000, -7} {
		ops := &nodeOpsSpy{}
		api := New(ops)
		if err := api.RetractWithTx(ctx, 42, at); err != nil {
			t.Fatalf("RetractWithTx(%d): %v", at, err)
		}
		if len(ops.calls) != 1 || ops.calls[0] != "RetractWithTx" || ops.lastID != 42 || ops.lastTx != at {
			t.Fatalf("RetractWithTx forwarded calls=%v id=%v at=%d; want [RetractWithTx] 42 %d", ops.calls, ops.lastID, ops.lastTx, at)
		}
	}
}
