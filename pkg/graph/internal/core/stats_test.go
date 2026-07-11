package core

import (
	"context"
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestGraphStats_InitialState(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	s, _ := g.Stats.Get()
	if s.NodesAdded != 0 || s.NodesRead != 0 || s.NodesUpdated != 0 || s.NodesDeleted != 0 {
		t.Errorf("initial node counters non-zero: %+v", s)
	}
	if s.RelsAdded != 0 || s.RelsRead != 0 || s.RelsUpdated != 0 || s.RelsDeleted != 0 {
		t.Errorf("initial rel counters non-zero: %+v", s)
	}
	if s.NodeCacheHits != 0 || s.NodeCacheMisses != 0 || s.RelCacheHits != 0 || s.RelCacheMisses != 0 {
		t.Errorf("initial cache counters non-zero: %+v", s)
	}
}

func TestGraphStats_NodeCounters(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	n, err := g.Nodes.Add(context.Background(), []string{"X"}, map[string]any{"v": int64(1)})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	s, _ := g.Stats.Get()
	if s.NodesAdded != 1 {
		t.Errorf("NodesAdded = %d, want 1", s.NodesAdded)
	}

	if _, err := g.Nodes.Get(context.Background(), id); err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	s, _ = g.Stats.Get()
	if s.NodesRead != 1 {
		t.Errorf("NodesRead = %d, want 1", s.NodesRead)
	}

	if _, err := g.Nodes.Update(context.Background(), id, map[string]any{"v": int64(2)}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	s, _ = g.Stats.Get()
	if s.NodesUpdated != 1 {
		t.Errorf("NodesUpdated = %d, want 1", s.NodesUpdated)
	}

	if err := g.Nodes.Delete(context.Background(), id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	s, _ = g.Stats.Get()
	if s.NodesDeleted != 1 {
		t.Errorf("NodesDeleted = %d, want 1", s.NodesDeleted)
	}
}

func TestGraphStats_RelCounters(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	a, err := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	s, _ := g.Stats.Get()
	if s.RelsAdded != 1 {
		t.Errorf("RelsAdded = %d, want 1", s.RelsAdded)
	}

	if _, err := g.Rels.Get(context.Background(), rid); err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	s, _ = g.Stats.Get()
	if s.RelsRead != 1 {
		t.Errorf("RelsRead = %d, want 1", s.RelsRead)
	}

	if _, err := g.Rels.Update(context.Background(), rid, map[string]any{"w": int64(2)}); err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}
	s, _ = g.Stats.Get()
	if s.RelsUpdated != 1 {
		t.Errorf("RelsUpdated = %d, want 1", s.RelsUpdated)
	}

	if err := g.Rels.Delete(context.Background(), rid); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	s, _ = g.Stats.Get()
	if s.RelsDeleted != 1 {
		t.Errorf("RelsDeleted = %d, want 1", s.RelsDeleted)
	}
}

func TestGraphStats_NodeCountByLabelAndPropertyKey(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	ctx := context.Background()

	a, err := g.Nodes.Add(ctx, []string{"StatsPropPerson", "StatsPropUser"}, map[string]any{
		"id":   int64(1),
		"name": "Ada",
	})
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(ctx, []string{"StatsPropPerson"}, map[string]any{
		"id": int64(2),
	})
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	c, err := g.Nodes.Add(ctx, []string{"StatsPropOther"}, map[string]any{
		"tags": []string{"not", "indexable"},
	})
	if err != nil {
		t.Fatalf("AddNode c: %v", err)
	}

	if got, err := g.Stats.NodeCountByLabelAndPropertyKey("StatsPropPerson", "id"); err != nil || got != 2 {
		t.Fatalf("Person/id count = (%d, %v), want (2, nil)", got, err)
	}
	if got, err := g.Stats.NodeCountByLabelAndPropertyKey("StatsPropUser", "id"); err != nil || got != 1 {
		t.Fatalf("User/id count = (%d, %v), want (1, nil)", got, err)
	}
	if got, err := g.Stats.NodeCountByLabelAndPropertyKey("StatsPropOther", "tags"); err != nil || got != 0 {
		t.Fatalf("Other/tags count = (%d, %v), want (0, nil)", got, err)
	}
	if got, err := g.Stats.NodeCountByLabelAndPropertyKey("StatsPropMissing", "id"); err != nil || got != 0 {
		t.Fatalf("missing label count = (%d, %v), want (0, nil)", got, err)
	}

	if err := g.Nodes.DeleteProperty(ctx, b.ID(), "id"); err != nil {
		t.Fatalf("DeleteProperty b.id: %v", err)
	}
	if got, err := g.Stats.NodeCountByLabelAndPropertyKey("StatsPropPerson", "id"); err != nil || got != 1 {
		t.Fatalf("Person/id after delete property = (%d, %v), want (1, nil)", got, err)
	}

	if err := g.Nodes.SetProperty(ctx, c.ID(), "id", int64(3)); err != nil {
		t.Fatalf("SetProperty c.id: %v", err)
	}
	if got, err := g.Stats.NodeCountByLabelAndPropertyKey("StatsPropOther", "id"); err != nil || got != 1 {
		t.Fatalf("Other/id after set property = (%d, %v), want (1, nil)", got, err)
	}

	if err := g.Nodes.Delete(ctx, a.ID()); err != nil {
		t.Fatalf("DeleteNode a: %v", err)
	}
	if got, err := g.Stats.NodeCountByLabelAndPropertyKey("StatsPropPerson", "id"); err != nil || got != 0 {
		t.Fatalf("Person/id after delete node = (%d, %v), want (0, nil)", got, err)
	}
	if got, err := g.Stats.NodeCountByLabelAndPropertyKey("StatsPropUser", "id"); err != nil || got != 0 {
		t.Fatalf("User/id after delete node = (%d, %v), want (0, nil)", got, err)
	}
}

func TestGraphStats_NodeCountByLabelAndPropertyKeyErrors(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	ctx := context.Background()
	if _, err := g.Nodes.Add(ctx, []string{"StatsErrPerson"}, map[string]any{"id": int64(1)}); err != nil {
		t.Fatalf("seed Add: %v", err)
	}

	// Empty label is rejected before any store access.
	if _, err := g.Stats.NodeCountByLabelAndPropertyKey("", "id"); err == nil {
		t.Fatal("empty label should be rejected")
	}
	// Reserved shadow property key is rejected.
	if _, err := g.Stats.NodeCountByLabelAndPropertyKey("StatsErrPerson", "tkg_version"); err == nil {
		t.Fatal("shadow property key should be rejected")
	}
	// Closed graph surfaces ErrGraphClosed.
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := g.Stats.NodeCountByLabelAndPropertyKey("StatsErrPerson", "id"); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("closed graph = %v, want ErrGraphClosed", err)
	}
}

// TestGraphStats_NodeCountByLabelAndPropertyKeyCapabilityMissing pins the
// behaviour on a backend that satisfies only MandatoryStore: the optional
// NodePropertyKeyStatsCapability is absent, so the call surfaces the typed
// sentinel rather than silently returning 0.
func TestGraphStats_NodeCountByLabelAndPropertyKeyCapabilityMissing(t *testing.T) {
	t.Parallel()
	g := newMandatoryOnlyGraph(t)
	ctx := context.Background()
	if _, err := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{"id": int64(1)}); err != nil {
		t.Fatalf("seed Add: %v", err)
	}
	if _, err := g.Stats.NodeCountByLabelAndPropertyKey("Person", "id"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("capability-missing = %v, want ErrCapabilityNotSupported", err)
	}
}

// propKeyStatsFaultStore satisfies MandatoryStore (via the embedded memory
// store) AND NodePropertyKeyStatsCapability, but returns a caller-controlled
// count/error from the capability method so the core wrapper's error and
// invariant-validation branches are observable.
type propKeyStatsFaultStore struct {
	storepkg.MandatoryStore
	count int
	err   error
}

func (s *propKeyStatsFaultStore) NodeCountByLabelAndPropertyKey(uint16, string) (int, error) {
	return s.count, s.err
}

func TestGraphStats_NodeCountByLabelAndPropertyKeyStoreFaults(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// A store error propagates unchanged.
	errBoom := errors.New("prop-key stats boom")
	gErr, err := New(Config{Store: &propKeyStatsFaultStore{MandatoryStore: memory.New(), err: errBoom}})
	if err != nil {
		t.Fatalf("New (err store): %v", err)
	}
	t.Cleanup(func() { _ = gErr.Close() })
	if _, err := gErr.Nodes.Add(ctx, []string{"Person"}, nil); err != nil {
		t.Fatalf("seed Add (err store): %v", err)
	}
	if _, err := gErr.Stats.NodeCountByLabelAndPropertyKey("Person", "id"); !errors.Is(err, errBoom) {
		t.Fatalf("store error = %v, want errBoom", err)
	}

	// A negative count from the store is rejected as an invariant violation.
	gNeg, err := New(Config{Store: &propKeyStatsFaultStore{MandatoryStore: memory.New(), count: -1}})
	if err != nil {
		t.Fatalf("New (neg store): %v", err)
	}
	t.Cleanup(func() { _ = gNeg.Close() })
	if _, err := gNeg.Nodes.Add(ctx, []string{"Person"}, nil); err != nil {
		t.Fatalf("seed Add (neg store): %v", err)
	}
	if _, err := gNeg.Stats.NodeCountByLabelAndPropertyKey("Person", "id"); err == nil {
		t.Fatal("negative store count should be rejected")
	}
}

func TestGraphStats_PropertyStats(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	ctx := context.Background()

	a, err := g.Nodes.Add(ctx, []string{"PropStatsPerson"}, map[string]any{"score": int64(10)})
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(ctx, []string{"PropStatsPerson"}, map[string]any{"score": int64(30)})
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	if _, err := g.Nodes.Add(ctx, []string{"PropStatsPerson"}, map[string]any{"score": int64(20)}); err != nil {
		t.Fatalf("AddNode c: %v", err)
	}

	stats, err := g.Stats.PropertyStats("PropStatsPerson", "score")
	if err != nil {
		t.Fatalf("PropertyStats: %v", err)
	}
	if stats.Count != 3 {
		t.Fatalf("Count = %d, want 3", stats.Count)
	}
	if stats.Min != int64(10) {
		t.Fatalf("Min = %v, want int64(10)", stats.Min)
	}
	if stats.Max != int64(30) {
		t.Fatalf("Max = %v, want int64(30)", stats.Max)
	}
	if stats.NDV < 1 || stats.NDV > 10 {
		t.Fatalf("NDV = %d, want a small positive estimate (3 distinct values)", stats.NDV)
	}

	// Missing label → zero-value stats, not an error.
	missing, err := g.Stats.PropertyStats("PropStatsMissing", "score")
	if err != nil {
		t.Fatalf("PropertyStats missing label: %v", err)
	}
	if missing != (storepkg.PropertyStats{}) {
		t.Fatalf("missing-label stats = %+v, want zero value", missing)
	}

	// Delete-the-extremum: b holds the max (30); after deleting it the
	// reported max must reflect the surviving nodes, not the stale 30.
	if err := g.Nodes.Delete(ctx, b.ID()); err != nil {
		t.Fatalf("DeleteNode b: %v", err)
	}
	after, err := g.Stats.PropertyStats("PropStatsPerson", "score")
	if err != nil {
		t.Fatalf("PropertyStats after delete: %v", err)
	}
	if after.Max != int64(20) {
		t.Fatalf("Max after delete-the-extremum = %v, want int64(20)", after.Max)
	}
	if after.Count != 2 {
		t.Fatalf("Count after delete = %d, want 2", after.Count)
	}

	if err := g.Nodes.Delete(ctx, a.ID()); err != nil {
		t.Fatalf("DeleteNode a: %v", err)
	}
}

func TestGraphStats_PropertyStatsErrors(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	ctx := context.Background()
	if _, err := g.Nodes.Add(ctx, []string{"PropStatsErrPerson"}, map[string]any{"id": int64(1)}); err != nil {
		t.Fatalf("seed Add: %v", err)
	}

	if _, err := g.Stats.PropertyStats("", "id"); err == nil {
		t.Fatal("empty label should be rejected")
	}
	if _, err := g.Stats.PropertyStats("PropStatsErrPerson", "tkg_version"); err == nil {
		t.Fatal("shadow property key should be rejected")
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := g.Stats.PropertyStats("PropStatsErrPerson", "id"); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("closed graph = %v, want ErrGraphClosed", err)
	}
}

// TestGraphStats_PropertyStatsCapabilityMissing pins the behaviour on a
// backend that satisfies only MandatoryStore: the optional
// NodePropertyStatsCapability is absent, so the call surfaces the typed
// sentinel rather than silently returning zero-value stats.
func TestGraphStats_PropertyStatsCapabilityMissing(t *testing.T) {
	t.Parallel()
	g := newMandatoryOnlyGraph(t)
	ctx := context.Background()
	if _, err := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{"id": int64(1)}); err != nil {
		t.Fatalf("seed Add: %v", err)
	}
	if _, err := g.Stats.PropertyStats("Person", "id"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("capability-missing = %v, want ErrCapabilityNotSupported", err)
	}
}

// propStatsFaultStore satisfies MandatoryStore (via the embedded memory
// store) AND NodePropertyStatsCapability, but returns a caller-controlled
// PropertyStats/error from the capability method so the core wrapper's error
// branch is observable.
type propStatsFaultStore struct {
	storepkg.MandatoryStore
	stats storepkg.PropertyStats
	err   error
}

func (s *propStatsFaultStore) NodePropertyStats(uint16, string) (storepkg.PropertyStats, error) {
	return s.stats, s.err
}

// TestGraphStats_PropertyStatsTiered pins the ADR-0005 §3.1 parity fix:
// tiered.Store now implements the richer NodePropertyStatsCapability (a
// cross-shard NDV/min/max/count fold — see
// docs/query-planners.md "Tiered NDV fold"), so g.Stats.PropertyStats must
// return real statistics through the graph-layer label-string door exactly
// like the presence-only NodeCountByLabelAndPropertyKey sibling always did —
// not ErrCapabilityNotSupported. Deep cross-shard/adversarial coverage of the
// fold itself (same-value-two-shards NDV, min/max spanning shards, cold
// shards) lives at the tiered-store level
// (pkg/graph/store/tiered/tieredstore_property_stats_test.go); this test only
// pins the graph-layer wiring plus the delete-the-extremum rescan through the
// label-string door.
func TestGraphStats_PropertyStatsTiered(t *testing.T) {
	t.Parallel()
	g, _ := newTestTieredGraph(t)
	ctx := context.Background()

	if _, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{"id": int64(1)}); err != nil {
		t.Fatalf("seed Add a: %v", err)
	}
	b, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{"id": int64(3)})
	if err != nil {
		t.Fatalf("seed Add b: %v", err)
	}

	// The presence-only sibling still works on tiered.
	if _, err := g.Stats.NodeCountByLabelAndPropertyKey("Case", "id"); err != nil {
		t.Fatalf("NodeCountByLabelAndPropertyKey (sibling) on tiered: %v", err)
	}

	stats, err := g.Stats.PropertyStats("Case", "id")
	if err != nil {
		t.Fatalf("PropertyStats on tiered: %v", err)
	}
	if stats.Count != 2 {
		t.Fatalf("Count = %d, want 2", stats.Count)
	}
	if stats.Min != int64(1) {
		t.Fatalf("Min = %v, want int64(1)", stats.Min)
	}
	if stats.Max != int64(3) {
		t.Fatalf("Max = %v, want int64(3)", stats.Max)
	}
	if stats.NDV < 1 {
		t.Fatalf("NDV = %d, want a positive estimate", stats.NDV)
	}

	// Delete-the-extremum: b holds the max (3); the NEXT PropertyStats read
	// must reflect the surviving nodes, not the stale 3 (rule 15 two-phase
	// shape via a mutation-then-query check).
	if err := g.Nodes.Delete(ctx, b.ID()); err != nil {
		t.Fatalf("DeleteNode b: %v", err)
	}
	after, err := g.Stats.PropertyStats("Case", "id")
	if err != nil {
		t.Fatalf("PropertyStats after delete: %v", err)
	}
	if after.Max != int64(1) {
		t.Fatalf("Max after delete-the-extremum = %v, want int64(1)", after.Max)
	}
	if after.Count != 1 {
		t.Fatalf("Count after delete = %d, want 1", after.Count)
	}

	// Missing label → zero-value stats, not an error (matches the
	// non-tiered convention pinned in TestGraphStats_PropertyStats).
	missing, err := g.Stats.PropertyStats("NoSuchTieredLabel", "id")
	if err != nil {
		t.Fatalf("PropertyStats missing label: %v", err)
	}
	if missing != (storepkg.PropertyStats{}) {
		t.Fatalf("missing-label stats = %+v, want zero value", missing)
	}
}

func TestGraphStats_PropertyStatsStoreFaultPropagates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	errBoom := errors.New("prop stats boom")
	gErr, err := New(Config{Store: &propStatsFaultStore{MandatoryStore: memory.New(), err: errBoom}})
	if err != nil {
		t.Fatalf("New (err store): %v", err)
	}
	t.Cleanup(func() { _ = gErr.Close() })
	if _, err := gErr.Nodes.Add(ctx, []string{"Person"}, nil); err != nil {
		t.Fatalf("seed Add (err store): %v", err)
	}
	if _, err := gErr.Stats.PropertyStats("Person", "id"); !errors.Is(err, errBoom) {
		t.Fatalf("store error = %v, want errBoom", err)
	}
}

func TestGraphStats_NodeCascadeDeleteCountsRelationships(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add(context.Background(), []string{"CascadeStats"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"CascadeStats"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	c, err := g.Nodes.Add(context.Background(), []string{"CascadeStats"}, nil)
	if err != nil {
		t.Fatalf("AddNode c: %v", err)
	}
	if _, err := g.Rels.Add(context.Background(), "CASCADE_STATS_OUT", a, b, nil); err != nil {
		t.Fatalf("AddRelationship out: %v", err)
	}
	if _, err := g.Rels.Add(context.Background(), "CASCADE_STATS_IN", c, a, nil); err != nil {
		t.Fatalf("AddRelationship in: %v", err)
	}

	before, _ := g.Stats.Get()
	if err := g.Nodes.Delete(context.Background(), a.ID()); err != nil {
		t.Fatalf("DeleteNode cascade: %v", err)
	}

	after, _ := g.Stats.Get()
	if after.NodesDeleted != before.NodesDeleted+1 {
		t.Fatalf("NodesDeleted after cascade = %d, want %d", after.NodesDeleted, before.NodesDeleted+1)
	}
	if after.RelsDeleted != before.RelsDeleted+2 {
		t.Fatalf("RelsDeleted after cascade = %d, want %d", after.RelsDeleted, before.RelsDeleted+2)
	}
}

func TestGraphStats_TxReadCountersCommitAndRollback(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add(context.Background(), []string{"TxReadStats"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"TxReadStats"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "TX_READ_STATS_REL", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	before, _ := g.Stats.Get()
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.GetNode(a.ID()); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx GetNode: %v", err)
	}
	if _, err := tx.GetRelationship(r.ID()); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx GetRelationship: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	afterCommit, _ := g.Stats.Get()
	if afterCommit.NodesRead != before.NodesRead+1 {
		t.Fatalf("NodesRead after committed tx read = %d, want %d", afterCommit.NodesRead, before.NodesRead+1)
	}
	if afterCommit.RelsRead != before.RelsRead+1 {
		t.Fatalf("RelsRead after committed tx read = %d, want %d", afterCommit.RelsRead, before.RelsRead+1)
	}

	tx, err = g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx rollback: %v", err)
	}
	if _, err := tx.GetNode(b.ID()); err != nil {
		_ = tx.Rollback()
		t.Fatalf("rollback tx GetNode: %v", err)
	}
	if _, err := tx.GetRelationship(r.ID()); err != nil {
		_ = tx.Rollback()
		t.Fatalf("rollback tx GetRelationship: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	afterRollback, _ := g.Stats.Get()
	if afterRollback.NodesRead != afterCommit.NodesRead || afterRollback.RelsRead != afterCommit.RelsRead {
		t.Fatalf("read stats changed after rollback:\nafterCommit=%+v\nafterRollback=%+v", afterCommit, afterRollback)
	}
}

func TestGraphStats_BulkIDReadCounters(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add(context.Background(), []string{"BulkReadStats"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"BulkReadStats"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "BULK_READ_STATS_REL", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	before, _ := g.Stats.Get()
	nodes, err := g.Nodes.GetByIDs([]types.NodeID{b.ID(), a.ID(), a.ID()})
	if err != nil {
		t.Fatalf("GetByIDs nodes: %v", err)
	}
	if len(nodes) != 3 {
		t.Fatalf("GetByIDs nodes len = %d, want 3", len(nodes))
	}
	afterNodes, _ := g.Stats.Get()
	if afterNodes.NodesRead != before.NodesRead+int64(len(nodes)) {
		t.Fatalf("NodesRead after GetByIDs = %d, want %d", afterNodes.NodesRead, before.NodesRead+int64(len(nodes)))
	}

	rels, err := g.Rels.GetByIDs([]types.RelID{r.ID(), r.ID()})
	if err != nil {
		t.Fatalf("GetByIDs rels: %v", err)
	}
	if len(rels) != 2 {
		t.Fatalf("GetByIDs rels len = %d, want 2", len(rels))
	}
	afterRels, _ := g.Stats.Get()
	if afterRels.RelsRead != before.RelsRead+int64(len(rels)) {
		t.Fatalf("RelsRead after GetByIDs = %d, want %d", afterRels.RelsRead, before.RelsRead+int64(len(rels)))
	}
}

func TestGraphStats_BatchCreateCounters(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	b, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	a, err := b.AddNode([]string{"BatchStats"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	bn, err := b.AddNode([]string{"BatchStats"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	if _, err := b.AddRelationship("BATCH_STATS_REL", a, bn, nil); err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Created != 3 {
		t.Fatalf("Created = %d, want 3", result.Created)
	}

	s, _ := g.Stats.Get()
	if s.NodesAdded != 2 {
		t.Fatalf("NodesAdded = %d, want 2", s.NodesAdded)
	}
	if s.RelsAdded != 1 {
		t.Fatalf("RelsAdded = %d, want 1", s.RelsAdded)
	}
}

func TestGraphStats_BatchUpdateAndDeleteCounters(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add(context.Background(), []string{"BatchStatsCounters"}, map[string]any{"state": "old"})
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"BatchStatsCounters"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "BATCH_STATS_COUNTER_REL", a, b, map[string]any{"state": "old"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	beforeUpdate, _ := g.Stats.Get()
	updates, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder update: %v", err)
	}
	if err := updates.UpdateNode(a.ID(), map[string]any{"state": "new"}); err != nil {
		t.Fatalf("UpdateNode queue: %v", err)
	}
	if err := updates.UpdateRelationship(r.ID(), map[string]any{"state": "new"}); err != nil {
		t.Fatalf("UpdateRelationship queue: %v", err)
	}
	updateResult, err := updates.Execute()
	if err != nil {
		t.Fatalf("Execute updates: %v", err)
	}
	if updateResult.Updated != 2 {
		t.Fatalf("Updated = %d, want 2", updateResult.Updated)
	}
	afterUpdate, _ := g.Stats.Get()
	if afterUpdate.NodesUpdated != beforeUpdate.NodesUpdated+1 {
		t.Fatalf("NodesUpdated after batch update = %d, want %d", afterUpdate.NodesUpdated, beforeUpdate.NodesUpdated+1)
	}
	if afterUpdate.RelsUpdated != beforeUpdate.RelsUpdated+1 {
		t.Fatalf("RelsUpdated after batch update = %d, want %d", afterUpdate.RelsUpdated, beforeUpdate.RelsUpdated+1)
	}

	deletes, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder delete: %v", err)
	}
	if err := deletes.DeleteNode(a.ID()); err != nil {
		t.Fatalf("DeleteNode queue: %v", err)
	}
	deleteResult, err := deletes.Execute()
	if err != nil {
		t.Fatalf("Execute deletes: %v", err)
	}
	if deleteResult.Deleted != 2 {
		t.Fatalf("Deleted = %d, want node plus cascaded relationship", deleteResult.Deleted)
	}
	afterDelete, _ := g.Stats.Get()
	if afterDelete.NodesDeleted != afterUpdate.NodesDeleted+1 {
		t.Fatalf("NodesDeleted after batch delete = %d, want %d", afterDelete.NodesDeleted, afterUpdate.NodesDeleted+1)
	}
	if afterDelete.RelsDeleted != afterUpdate.RelsDeleted+1 {
		t.Fatalf("RelsDeleted after batch cascade delete = %d, want %d", afterDelete.RelsDeleted, afterUpdate.RelsDeleted+1)
	}
}

func TestGraphStats_CloseVersionCounters(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add(context.Background(), []string{"CloseStats"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"CloseStats"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "CLOSE_STATS_REL", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	before, _ := g.Stats.Get()
	nodeCloseTime := g.nodeValidFrom(a) + 1000
	if err := g.Nodes.CloseVersion(context.Background(), a.ID(), nodeCloseTime); err != nil {
		t.Fatalf("CloseVersion node: %v", err)
	}
	afterNode, _ := g.Stats.Get()
	if afterNode.NodesUpdated != before.NodesUpdated+1 {
		t.Fatalf("NodesUpdated after node CloseVersion = %d, want %d", afterNode.NodesUpdated, before.NodesUpdated+1)
	}

	relCloseTime := g.relValidFrom(r) + 1000
	if err := g.Rels.CloseVersion(context.Background(), r.ID(), relCloseTime); err != nil {
		t.Fatalf("CloseVersion rel: %v", err)
	}
	afterRel, _ := g.Stats.Get()
	if afterRel.RelsUpdated != before.RelsUpdated+1 {
		t.Fatalf("RelsUpdated after rel CloseVersion = %d, want %d", afterRel.RelsUpdated, before.RelsUpdated+1)
	}

	if err := g.Nodes.CloseVersion(context.Background(), a.ID(), nodeCloseTime+1000); !errors.Is(err, ErrAlreadyClosed) {
		t.Fatalf("second node CloseVersion = %v, want ErrAlreadyClosed", err)
	}
	if err := g.Rels.CloseVersion(context.Background(), r.ID(), relCloseTime+1000); !errors.Is(err, ErrAlreadyClosed) {
		t.Fatalf("second rel CloseVersion = %v, want ErrAlreadyClosed", err)
	}
	afterRejected, _ := g.Stats.Get()
	if afterRejected.NodesUpdated != afterNode.NodesUpdated {
		t.Fatalf("NodesUpdated changed after rejected CloseVersion: %d -> %d", afterNode.NodesUpdated, afterRejected.NodesUpdated)
	}
	if afterRejected.RelsUpdated != afterRel.RelsUpdated {
		t.Fatalf("RelsUpdated changed after rejected CloseVersion: %d -> %d", afterRel.RelsUpdated, afterRejected.RelsUpdated)
	}
}

// TestGraphStats_EmptyUpdate_NoUpdateIncrement verifies that an empty updates map
// causes UpdateNode to delegate to GetNode (incrementing NodesRead) without
// incrementing NodesUpdated.
func TestGraphStats_EmptyUpdate_NoUpdateIncrement(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	n, err := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	before, _ := g.Stats.Get()
	// Empty updates → returns GetNode result; increments Read but not Updated.
	if _, err := g.Nodes.Update(context.Background(), id, map[string]any{}); err != nil {
		t.Fatalf("UpdateNode (empty): %v", err)
	}
	after, _ := g.Stats.Get()

	if after.NodesUpdated != before.NodesUpdated {
		t.Errorf("NodesUpdated changed on empty update: %d -> %d", before.NodesUpdated, after.NodesUpdated)
	}
	if after.NodesRead != before.NodesRead+1 {
		t.Errorf("NodesRead should increment on empty UpdateNode (calls GetNode): %d -> %d", before.NodesRead, after.NodesRead)
	}
}

func TestGraphStats_CacheMetrics_MemoryStore_Zero(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{}) // MemoryStore — no cache instrumentation
	n, err := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()
	if _, err := g.Nodes.Get(context.Background(), id); err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if _, err := g.Nodes.Get(context.Background(), id); err != nil {
		t.Fatalf("GetNode (2nd): %v", err)
	}

	s, _ := g.Stats.Get()
	if s.NodeCacheHits != 0 || s.NodeCacheMisses != 0 {
		t.Errorf("MemoryStore node cache metrics should be 0: hits=%d misses=%d", s.NodeCacheHits, s.NodeCacheMisses)
	}
	if s.RelCacheHits != 0 || s.RelCacheMisses != 0 {
		t.Errorf("MemoryStore rel cache metrics should be 0: hits=%d misses=%d", s.RelCacheHits, s.RelCacheMisses)
	}
}

// TestGraphStats_CacheMetrics_BadgerStore verifies that badger.Store populates
// NodeCacheHits and NodeCacheMisses in Stats(). AddNode calls PutNode which
// inserts the node into the LRU cache, so subsequent GetNode calls are cache hits.
func TestGraphStats_CacheMetrics_BadgerStore(t *testing.T) {
	t.Parallel()
	g, err := New(Config{BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New(BadgerInMemory): %v", err)
	}
	defer func() {
		if err := g.Close(); err != nil {
			t.Logf("Close: %v", err)
		}
	}()

	n, err := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// PutNode inserted the node into the LRU cache, so both GetNode calls are hits.
	if _, err := g.Nodes.Get(context.Background(), id); err != nil {
		t.Fatalf("GetNode (1st): %v", err)
	}
	if _, err := g.Nodes.Get(context.Background(), id); err != nil {
		t.Fatalf("GetNode (2nd): %v", err)
	}

	s, _ := g.Stats.Get()
	if s.NodeCacheHits == 0 {
		t.Errorf("expected NodeCacheHits > 0 with badger.Store, got %d", s.NodeCacheHits)
	}
	// Total cache activity must be positive.
	if s.NodeCacheHits+s.NodeCacheMisses == 0 {
		t.Error("expected some node cache activity, got zero for both hits and misses")
	}
}

func TestGraphStats_SnapshotCountersMatchesGet(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add(context.Background(), []string{"StatsSnapshot"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"StatsSnapshot"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "STATS_SNAPSHOT_REL", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	if _, err := g.Nodes.Get(context.Background(), a.ID()); err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if _, err := g.Rels.Get(context.Background(), r.ID()); err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}

	want, _ := g.Stats.Get()
	nodesAdded, nodesRead, nodesUpdated, nodesDeleted,
		relsAdded, relsRead, relsUpdated, relsDeleted,
		nodeCacheHits, nodeCacheMisses, relCacheHits, relCacheMisses, snapErr := g.Stats.SnapshotCounters()
	if snapErr != nil {
		t.Fatalf("SnapshotCounters on open graph: %v", snapErr)
	}

	if nodesAdded != want.NodesAdded || nodesRead != want.NodesRead ||
		nodesUpdated != want.NodesUpdated || nodesDeleted != want.NodesDeleted ||
		relsAdded != want.RelsAdded || relsRead != want.RelsRead ||
		relsUpdated != want.RelsUpdated || relsDeleted != want.RelsDeleted ||
		nodeCacheHits != want.NodeCacheHits || nodeCacheMisses != want.NodeCacheMisses ||
		relCacheHits != want.RelCacheHits || relCacheMisses != want.RelCacheMisses {
		t.Fatalf("SnapshotCounters tuple does not match Get: got node=(%d,%d,%d,%d) rel=(%d,%d,%d,%d) cache=(%d,%d,%d,%d), want %+v",
			nodesAdded, nodesRead, nodesUpdated, nodesDeleted,
			relsAdded, relsRead, relsUpdated, relsDeleted,
			nodeCacheHits, nodeCacheMisses, relCacheHits, relCacheMisses,
			want)
	}
}

func TestGraphStats_ScopedCountForwardersDirect(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add(context.Background(), []string{"StatsScoped", "SharedStatsScoped"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"StatsScoped"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	c, err := g.Nodes.Add(context.Background(), []string{"OtherStatsScoped"}, nil)
	if err != nil {
		t.Fatalf("AddNode c: %v", err)
	}
	if _, err := g.Rels.Add(context.Background(), "STATS_SCOPED_REL", a, b, nil); err != nil {
		t.Fatalf("AddRelationship a-b: %v", err)
	}
	if _, err := g.Rels.Add(context.Background(), "STATS_OTHER_REL", b, c, nil); err != nil {
		t.Fatalf("AddRelationship b-c: %v", err)
	}

	if got, err := g.Stats.NodeCountByLabel("StatsScoped"); err != nil || got != 2 {
		t.Fatalf("NodeCountByLabel(StatsScoped) = (%d, %v), want (2, nil)", got, err)
	}
	if got, err := g.Stats.NodeCountByLabel("SharedStatsScoped"); err != nil || got != 1 {
		t.Fatalf("NodeCountByLabel(SharedStatsScoped) = (%d, %v), want (1, nil)", got, err)
	}
	if got, err := g.Stats.NodeCountByLabel("MissingStatsScoped"); err != nil || got != 0 {
		t.Fatalf("NodeCountByLabel(MissingStatsScoped) = (%d, %v), want (0, nil)", got, err)
	}
	if got, err := g.Stats.RelCountByType("STATS_SCOPED_REL"); err != nil || got != 1 {
		t.Fatalf("RelCountByType(STATS_SCOPED_REL) = (%d, %v), want (1, nil)", got, err)
	}
	if got, err := g.Stats.RelCountByType("MISSING_STATS_REL"); err != nil || got != 0 {
		t.Fatalf("RelCountByType(MISSING_STATS_REL) = (%d, %v), want (0, nil)", got, err)
	}
}

func TestGraphStats_AllCountsEnumerateOmitZeroAndClosed(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add(context.Background(), []string{"StatsAllCount", "StatsAllShared"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"StatsAllCount"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	zeroNode, err := g.Nodes.Add(context.Background(), []string{"StatsAllZero"}, nil)
	if err != nil {
		t.Fatalf("AddNode zero: %v", err)
	}
	if err := g.Nodes.Delete(context.Background(), zeroNode.ID()); err != nil {
		t.Fatalf("Delete zero-count node: %v", err)
	}

	rel, err := g.Rels.Add(context.Background(), "STATS_ALL_COUNT_REL", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship live: %v", err)
	}
	zeroRel, err := g.Rels.Add(context.Background(), "STATS_ALL_ZERO_REL", b, a, nil)
	if err != nil {
		t.Fatalf("AddRelationship zero: %v", err)
	}
	if err := g.Rels.Delete(context.Background(), zeroRel.ID()); err != nil {
		t.Fatalf("Delete zero-count rel: %v", err)
	}

	labelCounts, err := g.Stats.AllLabelCounts()
	if err != nil {
		t.Fatalf("AllLabelCounts: %v", err)
	}
	if labelCounts["StatsAllCount"] != 2 || labelCounts["StatsAllShared"] != 1 {
		t.Fatalf("AllLabelCounts = %v, want StatsAllCount=2 and StatsAllShared=1", labelCounts)
	}
	if _, ok := labelCounts["StatsAllZero"]; ok {
		t.Fatalf("AllLabelCounts included zero-count label: %v", labelCounts)
	}

	relCounts, err := g.Stats.AllRelTypeCounts()
	if err != nil {
		t.Fatalf("AllRelTypeCounts: %v", err)
	}
	if relCounts["STATS_ALL_COUNT_REL"] != 1 {
		t.Fatalf("AllRelTypeCounts = %v, want STATS_ALL_COUNT_REL=1", relCounts)
	}
	if _, ok := relCounts["STATS_ALL_ZERO_REL"]; ok {
		t.Fatalf("AllRelTypeCounts included zero-count type: %v", relCounts)
	}
	if _, err := g.Rels.Get(context.Background(), rel.ID()); err != nil {
		t.Fatalf("live relationship was lost while testing counts: %v", err)
	}

	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := g.Stats.AllLabelCounts(); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("AllLabelCounts after close = %v, want ErrGraphClosed", err)
	}
	if _, err := g.Stats.AllRelTypeCounts(); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("AllRelTypeCounts after close = %v, want ErrGraphClosed", err)
	}
}

func TestGraphStats_AllCountsPropagateStoreErrors(t *testing.T) {
	t.Parallel()

	nodeErr := errors.New("node count failed")
	nodeStore := &statsCountErrorStore{Store: memory.New(), nodeCountErr: nodeErr}
	g, err := New(Config{Store: nodeStore})
	if err != nil {
		t.Fatalf("New node error store: %v", err)
	}
	if _, err := g.Nodes.Add(context.Background(), []string{"StatsCountError"}, nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if _, err := g.Stats.AllLabelCounts(); !errors.Is(err, nodeErr) {
		t.Fatalf("AllLabelCounts error = %v, want %v", err, nodeErr)
	}

	relErr := errors.New("rel count failed")
	relStore := &statsCountErrorStore{Store: memory.New(), relCountErr: relErr}
	g, err = New(Config{Store: relStore})
	if err != nil {
		t.Fatalf("New rel error store: %v", err)
	}
	a, err := g.Nodes.Add(context.Background(), []string{"StatsRelCountError"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"StatsRelCountError"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	if _, err := g.Rels.Add(context.Background(), "STATS_REL_COUNT_ERROR", a, b, nil); err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	if _, err := g.Stats.AllRelTypeCounts(); !errors.Is(err, relErr) {
		t.Fatalf("AllRelTypeCounts error = %v, want %v", err, relErr)
	}
}

type statsCountErrorStore struct {
	*memory.Store
	nodeCountErr error
	relCountErr  error
}

func (s *statsCountErrorStore) NodeCountByLabel(token uint16) (int, error) {
	if s.nodeCountErr != nil {
		return 0, s.nodeCountErr
	}
	return s.Store.NodeCountByLabel(token)
}

func (s *statsCountErrorStore) RelCountByType(token uint16) (int, error) {
	if s.relCountErr != nil {
		return 0, s.relCountErr
	}
	return s.Store.RelCountByType(token)
}

func TestGraphStats_UpdateNodeInPlace_CountsAsUpdate(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	n, err := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	beforeSnap, _ := g.Stats.Get()

	before := beforeSnap.NodesUpdated
	if _, err := g.Nodes.UpdateInPlace(context.Background(), id, map[string]any{"k": "v"}); err != nil {
		t.Fatalf("UpdateNodeInPlace: %v", err)
	}
	afterSnap, _ := g.Stats.Get()
	after := afterSnap.NodesUpdated
	if after != before+1 {
		t.Errorf("NodesUpdated after UpdateNodeInPlace: got %d, want %d", after, before+1)
	}
}

func TestGraphStats_UpdateRelInPlace_CountsAsUpdate(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	a, err := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	id := r.ID()

	beforeSnap, _ := g.Stats.Get()

	before := beforeSnap.RelsUpdated
	if _, err := g.Rels.UpdateInPlace(context.Background(), id, map[string]any{"k": "v"}); err != nil {
		t.Fatalf("UpdateRelInPlace: %v", err)
	}
	afterSnap, _ := g.Stats.Get()
	after := afterSnap.RelsUpdated
	if after != before+1 {
		t.Errorf("RelsUpdated after UpdateRelInPlace: got %d, want %d", after, before+1)
	}
}

func TestGraphStats_RelCompareAndSetProperty_CountsAsUpdate(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	a, err := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"k": "old"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	beforeSnap, _ := g.Stats.Get()

	before := beforeSnap.RelsUpdated
	ok, err := g.Rels.CompareAndSetProperty(context.Background(), r.ID(), "k", "old", "new")
	if err != nil {
		t.Fatalf("CompareAndSetProperty: %v", err)
	}
	if !ok {
		t.Fatal("CompareAndSetProperty ok = false, want true")
	}
	afterSnap, _ := g.Stats.Get()
	after := afterSnap.RelsUpdated
	if after != before+1 {
		t.Errorf("RelsUpdated after CompareAndSetProperty: got %d, want %d", after, before+1)
	}
}

func TestGraphStats_NodeCompareAndSetProperty_CountsAsUpdate(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	n, err := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{"k": "old"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	beforeSnap, _ := g.Stats.Get()

	before := beforeSnap.NodesUpdated
	ok, err := g.Nodes.CompareAndSetProperty(context.Background(), n.ID(), "k", "old", "new")
	if err != nil {
		t.Fatalf("CompareAndSetProperty: %v", err)
	}
	if !ok {
		t.Fatal("CompareAndSetProperty ok = false, want true")
	}
	afterSnap, _ := g.Stats.Get()
	after := afterSnap.NodesUpdated
	if after != before+1 {
		t.Errorf("NodesUpdated after CompareAndSetProperty: got %d, want %d", after, before+1)
	}
}
