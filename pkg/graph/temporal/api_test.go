package temporal

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestAPINilReceiversReturnErrNilGraph(t *testing.T) {
	t.Parallel()

	var nilAPI *API
	nodeID := types.NodeID(10)
	relID := types.RelID(20)

	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{name: "NodeAt", run: func() error { _, err := nilAPI.NodeAt(nodeID, 1); return err }},
		{name: "NodesAt", run: func() error { _, err := nilAPI.NodesAt(1); return err }},
		{name: "NodesByLabelAt", run: func() error { _, err := nilAPI.NodesByLabelAt("Node", 1); return err }},
		{name: "RelAt", run: func() error { _, err := nilAPI.RelAt(relID, 1); return err }},
		{name: "RelsAt", run: func() error { _, err := nilAPI.RelsAt(1); return err }},
		{name: "RelsByTypeAt", run: func() error { _, err := nilAPI.RelsByTypeAt("KNOWS", 1); return err }},
		{name: "NeighborsAt", run: func() error { _, err := nilAPI.NeighborsAt(nodeID, 1); return err }},
		{name: "OutgoingRelsAt", run: func() error { _, err := nilAPI.OutgoingRelsAt(nodeID, 1); return err }},
		{name: "IncomingRelsAt", run: func() error { _, err := nilAPI.IncomingRelsAt(nodeID, 1); return err }},
		{name: "NodesByLabelPropertyAt", run: func() error { _, err := nilAPI.NodesByLabelPropertyAt("Node", "name", "Ada", 1); return err }},
		{name: "RelsByTypePropertyAt", run: func() error { _, err := nilAPI.RelsByTypePropertyAt("KNOWS", "since", 2026, 1); return err }},
		{name: "NodesDuring", run: func() error { _, err := nilAPI.NodesDuring(1, 2); return err }},
		{name: "RelsDuring", run: func() error { _, err := nilAPI.RelsDuring(1, 2); return err }},
		{name: "NodesByLabelPropertyDuring", run: func() error { _, err := nilAPI.NodesByLabelPropertyDuring("Node", "name", "Ada", 1, 2); return err }},
		{name: "RelsByTypePropertyDuring", run: func() error { _, err := nilAPI.RelsByTypePropertyDuring("KNOWS", "since", 2026, 1, 2); return err }},
		{name: "NowTx", run: func() error { _, err := nilAPI.NowTx(); return err }},
		{name: "NodeAsOf", run: func() error { _, err := nilAPI.NodeAsOf(nodeID, 1); return err }},
		{name: "RelAsOf", run: func() error { _, err := nilAPI.RelAsOf(relID, 1); return err }},
		{name: "NodesAsOf", run: func() error { _, err := nilAPI.NodesAsOf(1); return err }},
		{name: "RelsAsOf", run: func() error { _, err := nilAPI.RelsAsOf(1); return err }},
		{name: "NodeAtTx", run: func() error { _, err := nilAPI.NodeAtTx(nodeID, 1, 2); return err }},
		{name: "RelAtTx", run: func() error { _, err := nilAPI.RelAtTx(relID, 1, 2); return err }},
		{name: "NodesAtTx", run: func() error { _, err := nilAPI.NodesAtTx(1, 2); return err }},
		{name: "RelsAtTx", run: func() error { _, err := nilAPI.RelsAtTx(1, 2); return err }},
		{name: "SetNodeVersionInterval", run: func() error {
			_, err := nilAPI.SetNodeVersionInterval(context.Background(), nodeID, 1, 2, nil)
			return err
		}},
		{name: "SetRelVersionInterval", run: func() error {
			_, err := nilAPI.SetRelVersionInterval(context.Background(), relID, 1, 2, nil)
			return err
		}},
		{name: "Snapshot", run: func() error { _, err := nilAPI.Snapshot(1); return err }},
		{name: "Diff", run: func() error { _, err := nilAPI.Diff(1, 2); return err }},
		{name: "DiffCallback", run: func() error { return nilAPI.DiffCallback(1, 2, DiffHandlers{}) }},
		{name: "NodeInterval", run: func() error { _, _, err := nilAPI.NodeInterval(nil); return err }},
		{name: "RelInterval", run: func() error { _, _, err := nilAPI.RelInterval(nil); return err }},
		{name: "RelateNodes", run: func() error { _, err := nilAPI.RelateNodes(nil, nil); return err }},
		{name: "RelateRels", run: func() error { _, err := nilAPI.RelateRels(nil, nil); return err }},
		{name: "TagAsOf", run: func() error { return nilAPI.TagAsOf("x", 1) }},
		{name: "ResolveAsOf", run: func() error { _, _, err := nilAPI.ResolveAsOf("x"); return err }},
		{name: "AsOfTags", run: func() error { _, err := nilAPI.AsOfTags(); return err }},
		{name: "RemoveAsOfTag", run: func() error { return nilAPI.RemoveAsOfTag("x") }},
	} {
		if err := tc.run(); !errors.Is(err, grapherr.ErrNilGraph) {
			t.Fatalf("%s = %v, want ErrNilGraph", tc.name, err)
		}
	}

	api := New((*temporalOpsSpy)(nil))
	if _, err := api.NodeAt(nodeID, 1); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil NodeAt = %v, want ErrNilGraph", err)
	}

	// RelMatchesValidTime returns bool, not error: a not-ready graph must report
	// "not valid" (false), the safe default for a post-filter consumer.
	var nilGraphAPI *API
	if nilGraphAPI.RelMatchesValidTime(nil, storepkg.QueryOpts{}) {
		t.Fatal("nil-graph RelMatchesValidTime = true, want false")
	}
}

func TestAPIForwardsEveryMethod(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("temporal op failed")
	nodeID := types.NodeID(10)
	relID := types.RelID(20)
	ops := &temporalOpsSpy{
		err:      wantErr,
		start:    11,
		end:      22,
		relation: types.Before,
	}
	api := New(ops)

	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{name: "NodeAt", run: func() error { _, err := api.NodeAt(nodeID, 1); return err }},
		{name: "NodesAt", run: func() error { _, err := api.NodesAt(1); return err }},
		{name: "NodesByLabelAt", run: func() error { _, err := api.NodesByLabelAt("Node", 1); return err }},
		{name: "RelAt", run: func() error { _, err := api.RelAt(relID, 1); return err }},
		{name: "RelsAt", run: func() error { _, err := api.RelsAt(1); return err }},
		{name: "RelsByTypeAt", run: func() error { _, err := api.RelsByTypeAt("KNOWS", 1); return err }},
		{name: "NeighborsAt", run: func() error { _, err := api.NeighborsAt(nodeID, 1); return err }},
		{name: "OutgoingRelsAt", run: func() error { _, err := api.OutgoingRelsAt(nodeID, 1); return err }},
		{name: "IncomingRelsAt", run: func() error { _, err := api.IncomingRelsAt(nodeID, 1); return err }},
		{name: "NodesByLabelPropertyAt", run: func() error { _, err := api.NodesByLabelPropertyAt("Node", "name", "Ada", 1); return err }},
		{name: "RelsByTypePropertyAt", run: func() error { _, err := api.RelsByTypePropertyAt("KNOWS", "since", 2026, 1); return err }},
		{name: "NodesDuring", run: func() error { _, err := api.NodesDuring(1, 2); return err }},
		{name: "RelsDuring", run: func() error { _, err := api.RelsDuring(1, 2); return err }},
		{name: "NodesByLabelPropertyDuring", run: func() error { _, err := api.NodesByLabelPropertyDuring("Node", "name", "Ada", 1, 2); return err }},
		{name: "RelsByTypePropertyDuring", run: func() error { _, err := api.RelsByTypePropertyDuring("KNOWS", "since", 2026, 1, 2); return err }},
		{name: "NowTx", run: func() error { _, err := api.NowTx(); return err }},
		{name: "NodeAsOf", run: func() error { _, err := api.NodeAsOf(nodeID, 1); return err }},
		{name: "RelAsOf", run: func() error { _, err := api.RelAsOf(relID, 1); return err }},
		{name: "NodesAsOf", run: func() error { _, err := api.NodesAsOf(1); return err }},
		{name: "RelsAsOf", run: func() error { _, err := api.RelsAsOf(1); return err }},
		{name: "NodeAtTx", run: func() error { _, err := api.NodeAtTx(nodeID, 1, 2); return err }},
		{name: "RelAtTx", run: func() error { _, err := api.RelAtTx(relID, 1, 2); return err }},
		{name: "NodesAtTx", run: func() error { _, err := api.NodesAtTx(1, 2); return err }},
		{name: "RelsAtTx", run: func() error { _, err := api.RelsAtTx(1, 2); return err }},
		{name: "SetNodeVersionInterval", run: func() error {
			_, err := api.SetNodeVersionInterval(context.Background(), nodeID, 1, 2, nil)
			return err
		}},
		{name: "SetRelVersionInterval", run: func() error {
			_, err := api.SetRelVersionInterval(context.Background(), relID, 1, 2, nil)
			return err
		}},
		{name: "Snapshot", run: func() error { _, err := api.Snapshot(1); return err }},
		{name: "Diff", run: func() error { _, err := api.Diff(1, 2); return err }},
		{name: "DiffCallback", run: func() error { return api.DiffCallback(1, 2, DiffHandlers{}) }},
		{name: "NodeInterval", run: func() error {
			start, end, err := api.NodeInterval(nil)
			if start != 11 || end != 22 {
				t.Fatalf("NodeInterval = %d/%d, want 11/22", start, end)
			}
			return err
		}},
		{name: "RelInterval", run: func() error {
			start, end, err := api.RelInterval(nil)
			if start != 11 || end != 22 {
				t.Fatalf("RelInterval = %d/%d, want 11/22", start, end)
			}
			return err
		}},
		{name: "RelateNodes", run: func() error {
			relation, err := api.RelateNodes(nil, nil)
			if relation != types.Before {
				t.Fatalf("RelateNodes = %v, want Before", relation)
			}
			return err
		}},
		{name: "RelateRels", run: func() error {
			relation, err := api.RelateRels(nil, nil)
			if relation != types.Before {
				t.Fatalf("RelateRels = %v, want Before", relation)
			}
			return err
		}},
		{name: "RelMatchesValidTime", run: func() error {
			if !api.RelMatchesValidTime(nil, storepkg.QueryOpts{}) {
				t.Fatal("RelMatchesValidTime forwarded result = false, want true")
			}
			return wantErr // not error-returning; satisfy the loop's errors.Is(wantErr) check
		}},
		{name: "TagAsOf", run: func() error { return api.TagAsOf("x", 1) }},
		{name: "ResolveAsOf", run: func() error { _, _, err := api.ResolveAsOf("x"); return err }},
		{name: "AsOfTags", run: func() error { _, err := api.AsOfTags(); return err }},
		{name: "RemoveAsOfTag", run: func() error { return api.RemoveAsOfTag("x") }},
	} {
		if err := tc.run(); !errors.Is(err, wantErr) {
			t.Fatalf("%s = %v, want %v", tc.name, err, wantErr)
		}
	}

	wantCalls := []string{
		"NodeAt", "NodesAt", "NodesByLabelAt", "RelAt", "RelsAt", "RelsByTypeAt",
		"NeighborsAt", "OutgoingRelsAt", "IncomingRelsAt", "NodesByLabelPropertyAt", "RelsByTypePropertyAt", "NodesDuring", "RelsDuring",
		"NodesByLabelPropertyDuring", "RelsByTypePropertyDuring", "NowTx", "NodeAsOf", "RelAsOf", "NodesAsOf", "RelsAsOf",
		"NodeAtTx", "RelAtTx", "NodesAtTx", "RelsAtTx",
		"SetNodeVersionInterval", "SetRelVersionInterval",
		"Snapshot", "Diff", "DiffCallback", "NodeInterval", "RelInterval", "RelateNodes", "RelateRels",
		"RelMatchesValidTime",
		"TagAsOf", "ResolveAsOf", "AsOfTags", "RemoveAsOfTag",
	}
	if len(ops.calls) != len(wantCalls) {
		t.Fatalf("calls = %v, want %v", ops.calls, wantCalls)
	}
	for i, want := range wantCalls {
		if ops.calls[i] != want {
			t.Fatalf("call[%d] = %s, want %s; all calls %v", i, ops.calls[i], want, ops.calls)
		}
	}
	if ops.lastNodeID != nodeID || ops.lastRelID != relID || ops.lastLabel != "Node" || ops.lastRelType != "KNOWS" || ops.lastKey != "since" {
		t.Fatalf("forwarded node/rel/label/type/key = %v/%v/%q/%q/%q", ops.lastNodeID, ops.lastRelID, ops.lastLabel, ops.lastRelType, ops.lastKey)
	}
}

type temporalOpsSpy struct {
	err      error
	start    types.Instant
	end      types.Instant
	relation types.AllenRelation

	calls       []string
	lastNodeID  types.NodeID
	lastRelID   types.RelID
	lastLabel   string
	lastRelType string
	lastKey     string
}

func (s *temporalOpsSpy) record(name string) { s.calls = append(s.calls, name) }

func (s *temporalOpsSpy) NodeAt(id types.NodeID, tm types.Instant) (*types.Node, error) {
	s.record("NodeAt")
	s.lastNodeID = id
	return nil, s.err
}

func (s *temporalOpsSpy) NodesAt(tm types.Instant) ([]*types.Node, error) {
	s.record("NodesAt")
	return nil, s.err
}

func (s *temporalOpsSpy) NodesByLabelAt(label string, tm types.Instant) ([]*types.Node, error) {
	s.record("NodesByLabelAt")
	s.lastLabel = label
	return nil, s.err
}

func (s *temporalOpsSpy) RelAt(id types.RelID, tm types.Instant) (*types.Relationship, error) {
	s.record("RelAt")
	s.lastRelID = id
	return nil, s.err
}

func (s *temporalOpsSpy) RelsAt(tm types.Instant) ([]*types.Relationship, error) {
	s.record("RelsAt")
	return nil, s.err
}

func (s *temporalOpsSpy) RelsByTypeAt(relType string, tm types.Instant) ([]*types.Relationship, error) {
	s.record("RelsByTypeAt")
	s.lastRelType = relType
	return nil, s.err
}

func (s *temporalOpsSpy) NeighborsAt(nodeID types.NodeID, tm types.Instant) ([]*types.Node, error) {
	s.record("NeighborsAt")
	s.lastNodeID = nodeID
	return nil, s.err
}

func (s *temporalOpsSpy) OutgoingRelsAt(nodeID types.NodeID, tm types.Instant) ([]*types.Relationship, error) {
	s.record("OutgoingRelsAt")
	s.lastNodeID = nodeID
	return nil, s.err
}

func (s *temporalOpsSpy) IncomingRelsAt(nodeID types.NodeID, tm types.Instant) ([]*types.Relationship, error) {
	s.record("IncomingRelsAt")
	s.lastNodeID = nodeID
	return nil, s.err
}

func (s *temporalOpsSpy) NodesByLabelPropertyAt(label, key string, value any, tm types.Instant) ([]*types.Node, error) {
	s.record("NodesByLabelPropertyAt")
	s.lastLabel = label
	s.lastKey = key
	return nil, s.err
}

func (s *temporalOpsSpy) RelsByTypePropertyAt(relType, key string, value any, tm types.Instant) ([]*types.Relationship, error) {
	s.record("RelsByTypePropertyAt")
	s.lastRelType = relType
	s.lastKey = key
	return nil, s.err
}

func (s *temporalOpsSpy) NodesDuring(start, end types.Instant) ([]*types.Node, error) {
	s.record("NodesDuring")
	return nil, s.err
}

func (s *temporalOpsSpy) RelsDuring(start, end types.Instant) ([]*types.Relationship, error) {
	s.record("RelsDuring")
	return nil, s.err
}

func (s *temporalOpsSpy) NodesByLabelPropertyDuring(label, key string, value any, start, end types.Instant) ([]*types.Node, error) {
	s.record("NodesByLabelPropertyDuring")
	s.lastLabel = label
	s.lastKey = key
	return nil, s.err
}

func (s *temporalOpsSpy) RelsByTypePropertyDuring(relType, key string, value any, start, end types.Instant) ([]*types.Relationship, error) {
	s.record("RelsByTypePropertyDuring")
	s.lastRelType = relType
	s.lastKey = key
	return nil, s.err
}

func (s *temporalOpsSpy) NodeAsOf(id types.NodeID, txTime types.Instant) (*types.Node, error) {
	s.record("NodeAsOf")
	s.lastNodeID = id
	return nil, s.err
}

func (s *temporalOpsSpy) RelAsOf(id types.RelID, txTime types.Instant) (*types.Relationship, error) {
	s.record("RelAsOf")
	s.lastRelID = id
	return nil, s.err
}

func (s *temporalOpsSpy) NowTx() (types.Instant, error) {
	s.record("NowTx")
	return 0, s.err
}

func (s *temporalOpsSpy) NodesAsOf(txTime types.Instant) ([]*types.Node, error) {
	s.record("NodesAsOf")
	return nil, s.err
}

func (s *temporalOpsSpy) RelsAsOf(txTime types.Instant) ([]*types.Relationship, error) {
	s.record("RelsAsOf")
	return nil, s.err
}

func (s *temporalOpsSpy) NodeAtTx(id types.NodeID, validAt, txAt types.Instant) (*types.Node, error) {
	s.record("NodeAtTx")
	return nil, s.err
}

func (s *temporalOpsSpy) RelAtTx(id types.RelID, validAt, txAt types.Instant) (*types.Relationship, error) {
	s.record("RelAtTx")
	return nil, s.err
}

func (s *temporalOpsSpy) NodesAtTx(validAt, txAt types.Instant) ([]*types.Node, error) {
	s.record("NodesAtTx")
	return nil, s.err
}

func (s *temporalOpsSpy) RelsAtTx(validAt, txAt types.Instant) ([]*types.Relationship, error) {
	s.record("RelsAtTx")
	return nil, s.err
}

func (s *temporalOpsSpy) SetNodeVersionInterval(ctx context.Context, id types.NodeID, validFrom, validTo types.Instant, props map[string]any) (*types.Node, error) {
	s.record("SetNodeVersionInterval")
	return nil, s.err
}

func (s *temporalOpsSpy) SetRelVersionInterval(ctx context.Context, id types.RelID, validFrom, validTo types.Instant, props map[string]any) (*types.Relationship, error) {
	s.record("SetRelVersionInterval")
	return nil, s.err
}

func (s *temporalOpsSpy) Snapshot(tm types.Instant) (*GraphSnapshot, error) {
	s.record("Snapshot")
	return nil, s.err
}

func (s *temporalOpsSpy) Diff(t1, t2 types.Instant) (*SnapshotDiff, error) {
	s.record("Diff")
	return nil, s.err
}

func (s *temporalOpsSpy) DiffCallback(t1, t2 types.Instant, h DiffHandlers) error {
	s.record("DiffCallback")
	return s.err
}

func (s *temporalOpsSpy) NodeInterval(n *types.Node) (start, end types.Instant, err error) {
	s.record("NodeInterval")
	return s.start, s.end, s.err
}

func (s *temporalOpsSpy) RelInterval(r *types.Relationship) (start, end types.Instant, err error) {
	s.record("RelInterval")
	return s.start, s.end, s.err
}

func (s *temporalOpsSpy) RelateNodes(a, b *types.Node) (types.AllenRelation, error) {
	s.record("RelateNodes")
	return s.relation, s.err
}

func (s *temporalOpsSpy) RelateRels(a, b *types.Relationship) (types.AllenRelation, error) {
	s.record("RelateRels")
	return s.relation, s.err
}

func (s *temporalOpsSpy) TagAsOf(name string, at types.Instant) error {
	s.record("TagAsOf")
	return s.err
}

func (s *temporalOpsSpy) ResolveAsOf(name string) (types.Instant, bool, error) {
	s.record("ResolveAsOf")
	return 0, false, s.err
}

func (s *temporalOpsSpy) AsOfTags() (map[string]types.Instant, error) {
	s.record("AsOfTags")
	return nil, s.err
}

func (s *temporalOpsSpy) RemoveAsOfTag(name string) error {
	s.record("RemoveAsOfTag")
	return s.err
}

func (s *temporalOpsSpy) RelMatchesValidTime(r *types.Relationship, opts storepkg.QueryOpts) bool {
	s.record("RelMatchesValidTime")
	return true
}
