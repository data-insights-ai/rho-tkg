package rels

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
	nodeID := types.NodeID(10)
	relID := types.RelID(42)
	opts := storepkg.QueryOpts{Limit: 1}

	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{name: "Add", run: func() error { _, err := nilAPI.Add(context.Background(), "KNOWS", nil, nil, nil); return err }},
		{name: "AddWithContext", run: func() error { _, err := nilAPI.Add(ctx, "KNOWS", nil, nil, nil); return err }},
		{name: "AddByID", run: func() error { _, err := nilAPI.AddByID(context.Background(), "KNOWS", nodeID, nodeID, nil); return err }},
		{name: "AddByIDWithContext", run: func() error { _, err := nilAPI.AddByID(ctx, "KNOWS", nodeID, nodeID, nil); return err }},
		{name: "AddByIDIfAbsent", run: func() error {
			_, _, err := nilAPI.AddByIDIfAbsent(context.Background(), "KNOWS", nodeID, nodeID, nil)
			return err
		}},
		{name: "AddByIDIfAbsentWithContext", run: func() error {
			_, _, err := nilAPI.AddByIDIfAbsent(ctx, "KNOWS", nodeID, nodeID, nil)
			return err
		}},
		{name: "Get", run: func() error { _, err := nilAPI.Get(context.Background(), relID); return err }},
		{name: "GetWithContext", run: func() error { _, err := nilAPI.Get(ctx, relID); return err }},
		{name: "GetByIDs", run: func() error { _, err := nilAPI.GetByIDs([]types.RelID{relID}); return err }},
		{name: "Update", run: func() error { _, err := nilAPI.Update(context.Background(), relID, nil); return err }},
		{name: "UpdateWithContext", run: func() error { _, err := nilAPI.Update(ctx, relID, nil); return err }},
		{name: "UpdateInPlace", run: func() error { _, err := nilAPI.UpdateInPlace(context.Background(), relID, nil); return err }},
		{name: "UpdateInPlaceWithContext", run: func() error { _, err := nilAPI.UpdateInPlace(ctx, relID, nil); return err }},
		{name: "Delete", run: func() error { return nilAPI.Delete(context.Background(), relID) }},
		{name: "DeleteWithContext", run: func() error { return nilAPI.Delete(ctx, relID) }},
		{name: "Import", run: func() error { _, err := nilAPI.Import(ctx, relID, "KNOWS", nil, nil, nil); return err }},
		{name: "All", run: func() error { _, err := nilAPI.All(opts); return err }},
		{name: "ByType", run: func() error { _, err := nilAPI.ByType("KNOWS", opts); return err }},
		{name: "Count", run: func() error { _, err := nilAPI.Count(); return err }},
		{name: "CountByType", run: func() error { _, err := nilAPI.CountByType("KNOWS"); return err }},
		{name: "Outgoing", run: func() error { _, err := nilAPI.Outgoing(nodeID, "KNOWS"); return err }},
		{name: "Incoming", run: func() error { _, err := nilAPI.Incoming(nodeID, "KNOWS"); return err }},
		{name: "OutgoingForNodes", run: func() error { _, err := nilAPI.OutgoingForNodes([]types.NodeID{nodeID}, "KNOWS"); return err }},
		{name: "IncomingForNodes", run: func() error { _, err := nilAPI.IncomingForNodes([]types.NodeID{nodeID}, "KNOWS"); return err }},
		{name: "SetProperty", run: func() error { return nilAPI.SetProperty(ctx, relID, "since", 2026) }},
		{name: "DeleteProperty", run: func() error { return nilAPI.DeleteProperty(ctx, relID, "since") }},
		{name: "CompareAndSetProperty", run: func() error {
			_, err := nilAPI.CompareAndSetProperty(context.Background(), relID, "since", 2025, 2026)
			return err
		}},
		{name: "CompareAndSetPropertyWithContext", run: func() error {
			_, err := nilAPI.CompareAndSetProperty(ctx, relID, "since", 2025, 2026)
			return err
		}},
		{name: "CloseVersion", run: func() error { return nilAPI.CloseVersion(ctx, relID, 100) }},
		{name: "History", run: func() error { _, err := nilAPI.History(relID); return err }},
		{name: "VersionAfter", run: func() error { _, err := nilAPI.VersionAfter(relID, 1); return err }},
		{name: "VersionBefore", run: func() error { _, err := nilAPI.VersionBefore(relID, 1); return err }},
	} {
		if err := tc.run(); !errors.Is(err, grapherr.ErrNilGraph) {
			t.Fatalf("%s = %v, want ErrNilGraph", tc.name, err)
		}
	}
	if nilAPI.HasType(nil, "KNOWS") {
		t.Fatal("nil HasType = true, want false")
	}
	if got := nilAPI.Type(nil); got != "" {
		t.Fatalf("nil Type = %q, want empty", got)
	}
	if got := nilAPI.NextID(); got != 0 {
		t.Fatalf("nil NextID = %v, want 0", got)
	}

	api := New((*relOpsSpy)(nil))
	if _, err := api.Get(context.Background(), relID); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil Get = %v, want ErrNilGraph", err)
	}
	if api.HasType(nil, "KNOWS") {
		t.Fatal("typed-nil HasType = true, want false")
	}
	if got := api.NextID(); got != 0 {
		t.Fatalf("typed-nil NextID = %v, want 0", got)
	}
}

func TestAPIForwardsEveryMethod(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("relationship op failed")
	ctx := context.Background()
	nodeID := types.NodeID(10)
	relID := types.RelID(42)
	opts := storepkg.QueryOpts{Limit: 3}
	ops := &relOpsSpy{
		err:      wantErr,
		created:  true,
		count:    7,
		hasType:  true,
		typeName: "KNOWS",
		nextID:   types.RelID(99),
	}
	api := New(ops)

	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{name: "Add", run: func() error {
			_, err := api.Add(context.Background(), "KNOWS", nil, nil, map[string]any{"since": 2026})
			return err
		}},
		{name: "AddWithContext", run: func() error { _, err := api.Add(ctx, "KNOWS", nil, nil, nil); return err }},
		{name: "AddByID", run: func() error { _, err := api.AddByID(context.Background(), "KNOWS", nodeID, nodeID, nil); return err }},
		{name: "AddByIDWithContext", run: func() error { _, err := api.AddByID(ctx, "KNOWS", nodeID, nodeID, nil); return err }},
		{name: "AddByIDIfAbsent", run: func() error {
			_, created, err := api.AddByIDIfAbsent(context.Background(), "KNOWS", nodeID, nodeID, nil)
			if !created {
				t.Fatal("AddByIDIfAbsent created = false, want true")
			}
			return err
		}},
		{name: "AddByIDIfAbsentWithContext", run: func() error {
			_, created, err := api.AddByIDIfAbsent(ctx, "KNOWS", nodeID, nodeID, nil)
			if !created {
				t.Fatal("AddByIDIfAbsentWithContext created = false, want true")
			}
			return err
		}},
		{name: "Get", run: func() error { _, err := api.Get(context.Background(), relID); return err }},
		{name: "GetWithContext", run: func() error { _, err := api.Get(ctx, relID); return err }},
		{name: "GetByIDs", run: func() error { _, err := api.GetByIDs([]types.RelID{relID}); return err }},
		{name: "Update", run: func() error {
			_, err := api.Update(context.Background(), relID, map[string]any{"since": 2027})
			return err
		}},
		{name: "UpdateWithContext", run: func() error { _, err := api.Update(ctx, relID, nil); return err }},
		{name: "UpdateInPlace", run: func() error { _, err := api.UpdateInPlace(context.Background(), relID, nil); return err }},
		{name: "UpdateInPlaceWithContext", run: func() error { _, err := api.UpdateInPlace(ctx, relID, nil); return err }},
		{name: "Delete", run: func() error { return api.Delete(context.Background(), relID) }},
		{name: "DeleteWithContext", run: func() error { return api.Delete(ctx, relID) }},
		{name: "Import", run: func() error { _, err := api.Import(ctx, relID, "KNOWS", nil, nil, nil); return err }},
		{name: "All", run: func() error { _, err := api.All(opts); return err }},
		{name: "ByType", run: func() error { _, err := api.ByType("KNOWS", opts); return err }},
		{name: "ForEachByType", run: func() error {
			return api.ForEachByType("KNOWS", opts, func(*types.Relationship) bool { return true })
		}},
		{name: "ForEachOutgoing", run: func() error {
			return api.ForEachOutgoing(nodeID, "KNOWS", func(*types.Relationship) bool { return true })
		}},
		{name: "ForEachIncoming", run: func() error {
			return api.ForEachIncoming(nodeID, "KNOWS", func(*types.Relationship) bool { return true })
		}},
		{name: "Count", run: func() error { _, err := api.Count(); return err }},
		{name: "CountByType", run: func() error { _, err := api.CountByType("KNOWS"); return err }},
		{name: "Outgoing", run: func() error { _, err := api.Outgoing(nodeID, "KNOWS"); return err }},
		{name: "Incoming", run: func() error { _, err := api.Incoming(nodeID, "KNOWS"); return err }},
		{name: "OutgoingForNodes", run: func() error { _, err := api.OutgoingForNodes([]types.NodeID{nodeID}, "KNOWS"); return err }},
		{name: "IncomingForNodes", run: func() error { _, err := api.IncomingForNodes([]types.NodeID{nodeID}, "KNOWS"); return err }},
		{name: "SetProperty", run: func() error { return api.SetProperty(ctx, relID, "since", 2026) }},
		{name: "DeleteProperty", run: func() error { return api.DeleteProperty(ctx, relID, "since") }},
		{name: "CompareAndSetProperty", run: func() error {
			ok, err := api.CompareAndSetProperty(context.Background(), relID, "since", 2025, 2026)
			if !ok {
				t.Fatal("CompareAndSetProperty bool = false, want true")
			}
			return err
		}},
		{name: "CompareAndSetPropertyWithContext", run: func() error {
			ok, err := api.CompareAndSetProperty(ctx, relID, "since", 2025, 2026)
			if !ok {
				t.Fatal("CompareAndSetPropertyWithContext bool = false, want true")
			}
			return err
		}},
		{name: "CloseVersion", run: func() error { return api.CloseVersion(ctx, relID, 100) }},
		{name: "History", run: func() error { _, err := api.History(relID); return err }},
		{name: "VersionAfter", run: func() error { _, err := api.VersionAfter(relID, 1); return err }},
		{name: "VersionBefore", run: func() error { _, err := api.VersionBefore(relID, 1); return err }},
	} {
		if err := tc.run(); !errors.Is(err, wantErr) {
			t.Fatalf("%s = %v, want %v", tc.name, err, wantErr)
		}
	}
	if !api.HasType(nil, "KNOWS") {
		t.Fatal("HasType = false, want true")
	}
	if got := api.Type(nil); got != "KNOWS" {
		t.Fatalf("Type = %q, want KNOWS", got)
	}
	if got := api.NextID(); got != types.RelID(99) {
		t.Fatalf("NextID = %v, want 99", got)
	}

	wantCalls := []string{
		"Add", "Add", "AddByID", "AddByID",
		"AddByIDIfAbsent", "AddByIDIfAbsent", "Get", "Get", "GetByIDs",
		"Update", "Update", "UpdateInPlace", "UpdateInPlace",
		"Delete", "Delete", "Import", "All", "ByType",
		"ForEachByType", "ForEachOutgoing", "ForEachIncoming", "Count", "CountByType",
		"Outgoing", "Incoming", "OutgoingForNodes", "IncomingForNodes", "SetProperty", "DeleteProperty",
		"CompareAndSetProperty", "CompareAndSetProperty",
		"CloseVersion", "History", "VersionAfter", "VersionBefore", "HasType", "Type", "NextID",
	}
	if len(ops.calls) != len(wantCalls) {
		t.Fatalf("calls = %v, want %v", ops.calls, wantCalls)
	}
	for i, want := range wantCalls {
		if ops.calls[i] != want {
			t.Fatalf("call[%d] = %s, want %s; all calls %v", i, ops.calls[i], want, ops.calls)
		}
	}
	if ops.lastRelID != relID || ops.lastNodeID != nodeID || ops.lastType != "KNOWS" || ops.lastKey != "since" {
		t.Fatalf("forwarded rel/node/type/key = %v/%v/%q/%q", ops.lastRelID, ops.lastNodeID, ops.lastType, ops.lastKey)
	}
	if ops.lastOpts != opts {
		t.Fatalf("forwarded opts = %+v, want %+v", ops.lastOpts, opts)
	}
}

type relOpsSpy struct {
	err      error
	created  bool
	count    int
	hasType  bool
	typeName string
	nextID   types.RelID

	calls      []string
	lastRelID  types.RelID
	lastNodeID types.NodeID
	lastType   string
	lastKey    string
	lastOpts   storepkg.QueryOpts
}

func (s *relOpsSpy) record(name string) { s.calls = append(s.calls, name) }

func (s *relOpsSpy) Add(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	s.record("Add")
	s.lastType = typeName
	return nil, s.err
}

func (s *relOpsSpy) AddWithContext(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	s.record("AddWithContext")
	s.lastType = typeName
	return nil, s.err
}

func (s *relOpsSpy) AddByID(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
	s.record("AddByID")
	s.lastType = typeName
	s.lastNodeID = startID
	return nil, s.err
}

func (s *relOpsSpy) AddByIDWithContext(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
	s.record("AddByIDWithContext")
	s.lastType = typeName
	s.lastNodeID = startID
	return nil, s.err
}

func (s *relOpsSpy) AddByIDIfAbsent(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error) {
	s.record("AddByIDIfAbsent")
	s.lastType = typeName
	s.lastNodeID = startID
	return nil, s.created, s.err
}

func (s *relOpsSpy) AddByIDIfAbsentWithContext(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error) {
	s.record("AddByIDIfAbsentWithContext")
	s.lastType = typeName
	s.lastNodeID = startID
	return nil, s.created, s.err
}

func (s *relOpsSpy) Get(ctx context.Context, id types.RelID) (*types.Relationship, error) {
	s.record("Get")
	s.lastRelID = id
	return nil, s.err
}

func (s *relOpsSpy) GetWithContext(ctx context.Context, id types.RelID) (*types.Relationship, error) {
	s.record("GetWithContext")
	s.lastRelID = id
	return nil, s.err
}

func (s *relOpsSpy) GetByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	s.record("GetByIDs")
	if len(ids) > 0 {
		s.lastRelID = ids[0]
	}
	return nil, s.err
}

func (s *relOpsSpy) Update(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	s.record("Update")
	s.lastRelID = id
	return nil, s.err
}

func (s *relOpsSpy) UpdateWithContext(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	s.record("UpdateWithContext")
	s.lastRelID = id
	return nil, s.err
}

func (s *relOpsSpy) UpdateInPlace(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	s.record("UpdateInPlace")
	s.lastRelID = id
	return nil, s.err
}

func (s *relOpsSpy) UpdateInPlaceWithContext(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	s.record("UpdateInPlaceWithContext")
	s.lastRelID = id
	return nil, s.err
}

func (s *relOpsSpy) Delete(ctx context.Context, id types.RelID) error {
	s.record("Delete")
	s.lastRelID = id
	return s.err
}

func (s *relOpsSpy) DeleteWithContext(ctx context.Context, id types.RelID) error {
	s.record("DeleteWithContext")
	s.lastRelID = id
	return s.err
}

func (s *relOpsSpy) Import(ctx context.Context, id types.RelID, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	s.record("Import")
	s.lastRelID = id
	s.lastType = typeName
	return nil, s.err
}

func (s *relOpsSpy) All(opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	s.record("All")
	s.lastOpts = opts
	return nil, s.err
}

func (s *relOpsSpy) ByType(typeName string, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	s.record("ByType")
	s.lastType = typeName
	s.lastOpts = opts
	return nil, s.err
}

func (s *relOpsSpy) ForEachByType(typeName string, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	s.record("ForEachByType")
	s.lastType = typeName
	s.lastOpts = opts
	return s.err
}

func (s *relOpsSpy) ForEachOutgoing(nodeID types.NodeID, typeName string, fn func(*types.Relationship) bool) error {
	s.record("ForEachOutgoing")
	s.lastNodeID = nodeID
	s.lastType = typeName
	return s.err
}

func (s *relOpsSpy) ForEachAdjacentEndpoint(nodeID types.NodeID, typeName string, incoming bool, fn func(rel types.RelID, other types.NodeID) bool) error {
	s.record("ForEachAdjacentEndpoint")
	s.lastNodeID = nodeID
	s.lastType = typeName
	return s.err
}

func (s *relOpsSpy) ForEachAdjacentEndpointAt(nodeID types.NodeID, typeName string, incoming bool, opts storepkg.QueryOpts, fn func(rel types.RelID, other types.NodeID) bool) error {
	s.record("ForEachAdjacentEndpointAt")
	s.lastNodeID = nodeID
	s.lastType = typeName
	return s.err
}

func (s *relOpsSpy) ForEachIncoming(nodeID types.NodeID, typeName string, fn func(*types.Relationship) bool) error {
	s.record("ForEachIncoming")
	s.lastNodeID = nodeID
	s.lastType = typeName
	return s.err
}

func (s *relOpsSpy) Count() (int, error) {
	s.record("Count")
	return s.count, s.err
}

func (s *relOpsSpy) CountByType(typeName string) (int, error) {
	s.record("CountByType")
	s.lastType = typeName
	return s.count, s.err
}

func (s *relOpsSpy) Outgoing(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	s.record("Outgoing")
	s.lastNodeID = nodeID
	s.lastType = typeName
	return nil, s.err
}

func (s *relOpsSpy) Incoming(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	s.record("Incoming")
	s.lastNodeID = nodeID
	s.lastType = typeName
	return nil, s.err
}

func (s *relOpsSpy) OutgoingDegree(nodeID types.NodeID, typeName string) (int, error) {
	s.record("OutgoingDegree")
	s.lastNodeID = nodeID
	s.lastType = typeName
	return 0, s.err
}

func (s *relOpsSpy) IncomingDegree(nodeID types.NodeID, typeName string) (int, error) {
	s.record("IncomingDegree")
	s.lastNodeID = nodeID
	s.lastType = typeName
	return 0, s.err
}

func (s *relOpsSpy) OutgoingForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	s.record("OutgoingForNodes")
	if len(nodeIDs) > 0 {
		s.lastNodeID = nodeIDs[0]
	}
	s.lastType = typeName
	return nil, s.err
}

func (s *relOpsSpy) IncomingForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	s.record("IncomingForNodes")
	if len(nodeIDs) > 0 {
		s.lastNodeID = nodeIDs[0]
	}
	s.lastType = typeName
	return nil, s.err
}

func (s *relOpsSpy) SetProperty(ctx context.Context, id types.RelID, key string, value any) error {
	s.record("SetProperty")
	s.lastRelID = id
	s.lastKey = key
	return s.err
}

func (s *relOpsSpy) DeleteProperty(ctx context.Context, id types.RelID, key string) error {
	s.record("DeleteProperty")
	s.lastRelID = id
	s.lastKey = key
	return s.err
}

func (s *relOpsSpy) CompareAndSetProperty(ctx context.Context, id types.RelID, key string, expected, newVal any) (bool, error) {
	s.record("CompareAndSetProperty")
	s.lastRelID = id
	s.lastKey = key
	return true, s.err
}

func (s *relOpsSpy) CompareAndSetPropertyWithContext(ctx context.Context, id types.RelID, key string, expected, newVal any) (bool, error) {
	s.record("CompareAndSetPropertyWithContext")
	s.lastRelID = id
	s.lastKey = key
	return true, s.err
}

func (s *relOpsSpy) HasType(r *types.Relationship, typ string) bool {
	s.record("HasType")
	s.lastType = typ
	return s.hasType
}

func (s *relOpsSpy) Type(r *types.Relationship) string {
	s.record("Type")
	return s.typeName
}

func (s *relOpsSpy) CloseVersion(ctx context.Context, id types.RelID, tm types.Instant) error {
	s.record("CloseVersion")
	s.lastRelID = id
	return s.err
}

func (s *relOpsSpy) History(id types.RelID) ([]*types.Relationship, error) {
	s.record("History")
	s.lastRelID = id
	return nil, s.err
}

func (s *relOpsSpy) VersionAfter(id types.RelID, version uint32) (*types.Relationship, error) {
	s.record("VersionAfter")
	s.lastRelID = id
	return nil, s.err
}

func (s *relOpsSpy) VersionBefore(id types.RelID, version uint32) (*types.Relationship, error) {
	s.record("VersionBefore")
	s.lastRelID = id
	return nil, s.err
}

func (s *relOpsSpy) NextID() types.RelID {
	s.record("NextID")
	return s.nextID
}
