package core

import (
	"context"
	"errors"
	"strings"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var errEmptyBulkReadProbeStoreTouched = errors.New("empty bulk read probe touched store")

type emptyBulkReadProbeStore struct {
	storepkg.MandatoryStore
	nodeBulkReads int
	relBulkReads  int
}

func (s *emptyBulkReadProbeStore) GetNodesByIDs([]types.NodeID) ([]*types.Node, error) {
	s.nodeBulkReads++
	return nil, errEmptyBulkReadProbeStoreTouched
}

func (s *emptyBulkReadProbeStore) GetRelationshipsByIDs([]types.RelID) ([]*types.Relationship, error) {
	s.relBulkReads++
	return nil, errEmptyBulkReadProbeStoreTouched
}

func TestGraphQueryAPIsRejectInvalidDepthBeforeEmptyShortcuts(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	badOpts := storepkg.QueryOpts{Depth: storepkg.ShardDepth(99)}
	checks := []struct {
		name string
		run  func() error
	}{
		{name: "Nodes.ByLabel unknown label", run: func() error {
			_, err := g.Nodes.ByLabel("Missing", badOpts)
			return err
		}},
		{name: "Nodes.ByLabelAndProperty unknown label", run: func() error {
			_, err := g.Nodes.ByLabelAndProperty("Missing", "color", "red", badOpts)
			return err
		}},
		{name: "Nodes.All", run: func() error {
			_, err := g.Nodes.All(badOpts)
			return err
		}},
		{name: "Rels.ByType unknown type", run: func() error {
			_, err := g.Rels.ByType("MISSING", badOpts)
			return err
		}},
		{name: "Rels.All", run: func() error {
			_, err := g.Rels.All(badOpts)
			return err
		}},
		{name: "Index.SearchNearest unknown label", run: func() error {
			_, err := g.Index.SearchNearest("Missing", "vec", []float32{0}, 1, badOpts)
			return err
		}},
	}

	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.run(); !errors.Is(err, storepkg.ErrInvalidShardDepth) {
				t.Fatalf("err = %v, want ErrInvalidShardDepth", err)
			}
		})
	}
}

func TestNodesByLabelAndPropertyRejectsInvalidQueryValueBeforeEmptyShortcuts(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	_, err = g.Nodes.ByLabelAndProperty("Missing", "color", struct{ Bad int }{Bad: 1}, storepkg.QueryOpts{})
	if !errors.Is(err, types.ErrUnsupportedValueType) {
		t.Fatalf("ByLabelAndProperty invalid value with unknown label = %v, want ErrUnsupportedValueType", err)
	}

	nodes, err := g.Nodes.ByLabelAndProperty("Missing", "color", []string{"valid", "unindexable"}, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperty valid unindexable value: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("ByLabelAndProperty valid unindexable value returned %d nodes, want 0", len(nodes))
	}
}

func TestGraphQueryAPIsRejectInvalidTemporalRangeBeforeEmptyShortcuts(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	badOpts := storepkg.QueryOpts{ValidStart: 20, ValidEnd: 10}
	checks := []struct {
		name string
		run  func() error
	}{
		{name: "Nodes.ByLabel unknown label", run: func() error {
			_, err := g.Nodes.ByLabel("Missing", badOpts)
			return err
		}},
		{name: "Nodes.ByLabelAndProperty unknown label", run: func() error {
			_, err := g.Nodes.ByLabelAndProperty("Missing", "color", "red", badOpts)
			return err
		}},
		{name: "Nodes.All", run: func() error {
			_, err := g.Nodes.All(badOpts)
			return err
		}},
		{name: "Rels.ByType unknown type", run: func() error {
			_, err := g.Rels.ByType("MISSING", badOpts)
			return err
		}},
		{name: "Rels.All", run: func() error {
			_, err := g.Rels.All(badOpts)
			return err
		}},
		{name: "Index.SearchNearest unknown label", run: func() error {
			_, err := g.Index.SearchNearest("Missing", "vec", []float32{0}, 1, badOpts)
			return err
		}},
	}

	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.run(); !errors.Is(err, ErrInvalidTimeRange) {
				t.Fatalf("err = %v, want ErrInvalidTimeRange", err)
			}
		})
	}
}

func TestGraphQueryAPIsRejectNegativeLimitBeforeEmptyShortcuts(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	badOpts := storepkg.QueryOpts{Limit: -1}
	checks := []struct {
		name string
		run  func() error
	}{
		{name: "Nodes.ByLabel unknown label", run: func() error {
			_, err := g.Nodes.ByLabel("Missing", badOpts)
			return err
		}},
		{name: "Nodes.ByLabelAndProperty unknown label", run: func() error {
			_, err := g.Nodes.ByLabelAndProperty("Missing", "color", "red", badOpts)
			return err
		}},
		{name: "Nodes.All", run: func() error {
			_, err := g.Nodes.All(badOpts)
			return err
		}},
		{name: "Rels.ByType unknown type", run: func() error {
			_, err := g.Rels.ByType("MISSING", badOpts)
			return err
		}},
		{name: "Rels.All", run: func() error {
			_, err := g.Rels.All(badOpts)
			return err
		}},
		{name: "Index.SearchNearest unknown label", run: func() error {
			_, err := g.Index.SearchNearest("Missing", "vec", []float32{0}, 1, badOpts)
			return err
		}},
	}

	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.run(); !errors.Is(err, ErrInvalidQueryLimit) {
				t.Fatalf("err = %v, want ErrInvalidQueryLimit", err)
			}
		})
	}
}

func TestGraphQueryAPIsRejectNegativeCursorBeforeEmptyShortcuts(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	badOpts := storepkg.QueryOpts{After: types.EntityID(-1)}
	checks := []struct {
		name string
		run  func() error
	}{
		{name: "Nodes.ByLabel unknown label", run: func() error {
			_, err := g.Nodes.ByLabel("Missing", badOpts)
			return err
		}},
		{name: "Nodes.ByLabelAndProperty unknown label", run: func() error {
			_, err := g.Nodes.ByLabelAndProperty("Missing", "color", "red", badOpts)
			return err
		}},
		{name: "Nodes.All", run: func() error {
			_, err := g.Nodes.All(badOpts)
			return err
		}},
		{name: "Rels.ByType unknown type", run: func() error {
			_, err := g.Rels.ByType("MISSING", badOpts)
			return err
		}},
		{name: "Rels.All", run: func() error {
			_, err := g.Rels.All(badOpts)
			return err
		}},
		{name: "Index.SearchNearest unknown label", run: func() error {
			_, err := g.Index.SearchNearest("Missing", "vec", []float32{0}, 1, badOpts)
			return err
		}},
	}

	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.run(); !errors.Is(err, ErrInvalidQueryCursor) {
				t.Fatalf("err = %v, want ErrInvalidQueryCursor", err)
			}
		})
	}
}

func TestGraphGetByIDsEmptyInputsDoNotTouchStore(t *testing.T) {
	t.Parallel()

	store := &emptyBulkReadProbeStore{MandatoryStore: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	emptyNodeIDs := []types.NodeID{}
	for _, ids := range [][]types.NodeID{nil, emptyNodeIDs} {
		got, err := g.Nodes.GetByIDs(ids)
		if err != nil {
			t.Fatalf("Nodes.GetByIDs(%v) returned error: %v", ids, err)
		}
		if got != nil {
			t.Fatalf("Nodes.GetByIDs(%v) = %v, want nil", ids, got)
		}
	}
	emptyRelIDs := []types.RelID{}
	for _, ids := range [][]types.RelID{nil, emptyRelIDs} {
		got, err := g.Rels.GetByIDs(ids)
		if err != nil {
			t.Fatalf("Rels.GetByIDs(%v) returned error: %v", ids, err)
		}
		if got != nil {
			t.Fatalf("Rels.GetByIDs(%v) = %v, want nil", ids, got)
		}
	}
	if store.nodeBulkReads != 0 {
		t.Fatalf("empty node bulk reads touched store %d time(s)", store.nodeBulkReads)
	}
	if store.relBulkReads != 0 {
		t.Fatalf("empty relationship bulk reads touched store %d time(s)", store.relBulkReads)
	}
}

func TestGraphReadAPIsRejectInvalidIDs(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	n1, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add n1: %v", err)
	}
	n2, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add n2: %v", err)
	}
	rel, err := g.Rels.Add(context.Background(), "KNOWS", n1, n2, nil)
	if err != nil {
		t.Fatalf("Add relationship: %v", err)
	}

	checks := []struct {
		name string
		run  func() error
	}{
		{name: "Nodes.Get zero", run: func() error { _, err := g.Nodes.Get(context.Background(), 0); return err }},
		{name: "Nodes.Get negative", run: func() error { _, err := g.Nodes.Get(context.Background(), types.NodeID(-1)); return err }},
		{name: "Rels.Get zero", run: func() error { _, err := g.Rels.Get(context.Background(), 0); return err }},
		{name: "Rels.Get negative", run: func() error { _, err := g.Rels.Get(context.Background(), types.RelID(-1)); return err }},
		{name: "Nodes.GetByIDs zero", run: func() error {
			_, err := g.Nodes.GetByIDs([]types.NodeID{n1.ID(), 0})
			return err
		}},
		{name: "Nodes.GetByIDs negative", run: func() error {
			_, err := g.Nodes.GetByIDs([]types.NodeID{n1.ID(), types.NodeID(-1)})
			return err
		}},
		{name: "Rels.GetByIDs zero", run: func() error {
			_, err := g.Rels.GetByIDs([]types.RelID{rel.ID(), 0})
			return err
		}},
		{name: "Rels.GetByIDs negative", run: func() error {
			_, err := g.Rels.GetByIDs([]types.RelID{rel.ID(), types.RelID(-1)})
			return err
		}},
		{name: "Rels.Outgoing zero", run: func() error { _, err := g.Rels.Outgoing(0, ""); return err }},
		{name: "Rels.Outgoing negative", run: func() error {
			_, err := g.Rels.Outgoing(types.NodeID(-1), "")
			return err
		}},
		{name: "Rels.Incoming zero", run: func() error { _, err := g.Rels.Incoming(0, ""); return err }},
		{name: "Rels.Incoming negative", run: func() error {
			_, err := g.Rels.Incoming(types.NodeID(-1), "")
			return err
		}},
		{name: "Rels.OutgoingForNodes zero", run: func() error {
			_, err := g.Rels.OutgoingForNodes([]types.NodeID{n1.ID(), 0}, "")
			return err
		}},
		{name: "Rels.OutgoingForNodes negative", run: func() error {
			_, err := g.Rels.OutgoingForNodes([]types.NodeID{n1.ID(), types.NodeID(-1)}, "")
			return err
		}},
		{name: "Rels.IncomingForNodes zero", run: func() error {
			_, err := g.Rels.IncomingForNodes([]types.NodeID{n2.ID(), 0}, "")
			return err
		}},
		{name: "Rels.IncomingForNodes negative", run: func() error {
			_, err := g.Rels.IncomingForNodes([]types.NodeID{n2.ID(), types.NodeID(-1)}, "")
			return err
		}},
		{name: "Nodes.History zero", run: func() error { _, err := g.Nodes.History(0); return err }},
		{name: "Nodes.History negative", run: func() error {
			_, err := g.Nodes.History(types.NodeID(-1))
			return err
		}},
		{name: "Rels.History zero", run: func() error { _, err := g.Rels.History(0); return err }},
		{name: "Rels.History negative", run: func() error {
			_, err := g.Rels.History(types.RelID(-1))
			return err
		}},
		{name: "Temporal.NodeAt zero", run: func() error { _, err := g.Temporal.NodeAt(0, 1); return err }},
		{name: "Temporal.NodeAt negative", run: func() error {
			_, err := g.Temporal.NodeAt(types.NodeID(-1), 1)
			return err
		}},
		{name: "Temporal.RelAt zero", run: func() error { _, err := g.Temporal.RelAt(0, 1); return err }},
		{name: "Temporal.RelAt negative", run: func() error {
			_, err := g.Temporal.RelAt(types.RelID(-1), 1)
			return err
		}},
	}

	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.run(); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("err = %v, want ErrInvalidStoreMutation", err)
			}
		})
	}
}

func TestGraphQueryNamesValidateBeforeEmptyShortcuts(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	checks := []struct {
		name string
		want error
		run  func() error
	}{
		{name: "Nodes.ByLabel empty", want: ErrEmptyName, run: func() error {
			_, err := g.Nodes.ByLabel("", storepkg.QueryOpts{})
			return err
		}},
		{name: "Nodes.ByLabel whitespace", want: ErrEmptyName, run: func() error {
			_, err := g.Nodes.ByLabel("   ", storepkg.QueryOpts{})
			return err
		}},
		{name: "Nodes.ByLabel too long", want: ErrNameTooLong, run: func() error {
			_, err := g.Nodes.ByLabel(strings.Repeat("x", defaultMaxNameLength+1), storepkg.QueryOpts{})
			return err
		}},
		{name: "Rels.ByType empty", want: ErrEmptyName, run: func() error {
			_, err := g.Rels.ByType("", storepkg.QueryOpts{})
			return err
		}},
		{name: "Rels.ByType whitespace", want: ErrEmptyName, run: func() error {
			_, err := g.Rels.ByType("\t", storepkg.QueryOpts{})
			return err
		}},
		{name: "Nodes.CountByLabel empty", want: ErrEmptyName, run: func() error {
			_, err := g.Nodes.CountByLabel("")
			return err
		}},
		{name: "Rels.CountByType empty", want: ErrEmptyName, run: func() error {
			_, err := g.Rels.CountByType("")
			return err
		}},
		{name: "Rels.Outgoing whitespace type", want: ErrEmptyName, run: func() error {
			_, err := g.Rels.Outgoing(types.NodeID(1), " ")
			return err
		}},
		{name: "Rels.Incoming whitespace type", want: ErrEmptyName, run: func() error {
			_, err := g.Rels.Incoming(types.NodeID(1), " ")
			return err
		}},
		{name: "Rels.OutgoingForNodes validates type before empty nodes", want: ErrEmptyName, run: func() error {
			_, err := g.Rels.OutgoingForNodes(nil, " ")
			return err
		}},
		{name: "Rels.IncomingForNodes validates type before empty nodes", want: ErrEmptyName, run: func() error {
			_, err := g.Rels.IncomingForNodes(nil, " ")
			return err
		}},
	}

	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.run(); !errors.Is(err, check.want) {
				t.Fatalf("err = %v, want %v", err, check.want)
			}
		})
	}
}

func TestGraphQueryOptsNonPositiveIntervalBoundsDoNotActivateTemporalFilter(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	alice, err := g.Nodes.Add(context.Background(), []string{"Case"}, map[string]any{"name": "alice"})
	if err != nil {
		t.Fatalf("add alice: %v", err)
	}
	bob, err := g.Nodes.Add(context.Background(), []string{"Case"}, map[string]any{"name": "bob"})
	if err != nil {
		t.Fatalf("add bob: %v", err)
	}
	if _, err := g.Rels.Add(context.Background(), "LINK", alice, bob, nil); err != nil {
		t.Fatalf("add rel: %v", err)
	}

	optsCases := []struct {
		name string
		opts storepkg.QueryOpts
	}{
		{name: "negative start", opts: storepkg.QueryOpts{ValidStart: types.Instant(-1), ValidEnd: types.Instant(1)}},
		{name: "negative end", opts: storepkg.QueryOpts{ValidStart: types.Instant(1), ValidEnd: types.Instant(-1)}},
	}

	for _, tc := range optsCases {
		t.Run(tc.name, func(t *testing.T) {
			byLabel, err := g.Nodes.ByLabel("Case", tc.opts)
			if err != nil {
				t.Fatalf("Nodes.ByLabel: %v", err)
			}
			if len(byLabel) != 2 {
				t.Fatalf("Nodes.ByLabel len = %d, want 2", len(byLabel))
			}

			byProperty, err := g.Nodes.ByLabelAndProperty("Case", "name", "alice", tc.opts)
			if err != nil {
				t.Fatalf("Nodes.ByLabelAndProperty: %v", err)
			}
			if len(byProperty) != 1 || byProperty[0].ID() != alice.ID() {
				t.Fatalf("Nodes.ByLabelAndProperty IDs = %v, want alice", nodeIDs(byProperty))
			}

			allNodes, err := g.Nodes.All(tc.opts)
			if err != nil {
				t.Fatalf("Nodes.All: %v", err)
			}
			if len(allNodes) != 2 {
				t.Fatalf("Nodes.All len = %d, want 2", len(allNodes))
			}

			byType, err := g.Rels.ByType("LINK", tc.opts)
			if err != nil {
				t.Fatalf("Rels.ByType: %v", err)
			}
			if len(byType) != 1 {
				t.Fatalf("Rels.ByType len = %d, want 1", len(byType))
			}

			allRels, err := g.Rels.All(tc.opts)
			if err != nil {
				t.Fatalf("Rels.All: %v", err)
			}
			if len(allRels) != 1 {
				t.Fatalf("Rels.All len = %d, want 1", len(allRels))
			}
		})
	}
}
