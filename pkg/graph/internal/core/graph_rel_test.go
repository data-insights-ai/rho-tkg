package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store/memory"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

type relCreateFailAfterInstallStore struct {
	*memory.Store
	err        error
	failPut    bool
	failDelete bool
	panicPut   bool
}

func (s *relCreateFailAfterInstallStore) PutRelationship(r *types.Relationship) error {
	if err := s.Store.PutRelationship(r); err != nil {
		return err
	}
	if s.panicPut {
		panic("injected post-write relationship panic")
	}
	if s.failPut {
		return s.err
	}
	return nil
}

func (s *relCreateFailAfterInstallStore) DeleteRelationship(id types.RelID) error {
	if s.failDelete {
		return s.err
	}
	return s.Store.DeleteRelationship(id)
}

func newRelCreateRollbackGraph(t *testing.T) (*Core, *relCreateFailAfterInstallStore, *types.Node, *types.Node) {
	t.Helper()

	fs := &relCreateFailAfterInstallStore{
		Store: memory.New(),
		err:   errors.New("injected post-write relationship failure"),
	}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	a, err := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	return g, fs, a, b
}

func TestGraphRelationshipType(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	knowsTok, _ := g.Resolve.GetOrCreateRelType("KNOWS")

	r := types.NewRelationship(types.RelID(snowflake.ID(1)), knowsTok, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	got := g.Rels.Type(r)
	if got != "KNOWS" {
		t.Errorf("RelationshipType = %q, want \"KNOWS\"", got)
	}
}

func TestGraphRelationshipHasType(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	knowsTok, _ := g.Resolve.GetOrCreateRelType("KNOWS")

	r := types.NewRelationship(types.RelID(snowflake.ID(1)), knowsTok, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))

	if !g.Rels.HasType(r, "KNOWS") {
		t.Error("RelationshipHasType(\"KNOWS\") = false, want true")
	}
	if g.Rels.HasType(r, "LIKES") {
		t.Error("RelationshipHasType(\"LIKES\") = true, want false (unregistered)")
	}

	// Registered but wrong type: Lookup succeeds, comparison fails.
	g.Resolve.GetOrCreateRelType("LIKES")
	if g.Rels.HasType(r, "LIKES") {
		t.Error("RelationshipHasType(\"LIKES\") = true, want false (registered but wrong type)")
	}
}

func TestGraphAddRelationship(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	nB, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})

	r, err := g.Rels.Add(context.Background(), "KNOWS", nA, nB, map[string]any{"since": 2020})
	if err != nil {
		t.Fatalf("AddRelationship() returned error: %v", err)
	}

	// Verify type.
	if g.Rels.Type(r) != "KNOWS" {
		t.Errorf("RelationshipType = %q, want \"KNOWS\"", g.Rels.Type(r))
	}

	// Verify endpoints.
	if r.StartNodeID() != nA.ID() {
		t.Error("StartNodeID does not match nA")
	}
	if r.EndNodeID() != nB.ID() {
		t.Error("EndNodeID does not match nB")
	}

	// Verify property.
	since, ok := r.GetProperty("since")
	if !ok || since != 2020 {
		t.Errorf("GetProperty(\"since\") = (%v, %v), want (2020, true)", since, ok)
	}
}

func TestGraphAddRelationshipFailureDeletesPartialRowBeforeRelTypeRollback(t *testing.T) {
	t.Parallel()

	g, fs, a, b := newRelCreateRollbackGraph(t)

	fs.failPut = true
	_, err := g.Rels.Add(context.Background(), "TRANSIENT_REL", a, b, nil)
	if !errors.Is(err, fs.err) {
		t.Fatalf("Add transient relationship error = %v, want injected", err)
	}
	if tok, ok := g.Resolve.LookupRelType("TRANSIENT_REL"); ok || tok != 0 {
		t.Fatalf("LookupRelType(TRANSIENT_REL) = %d, %v; want rollback", tok, ok)
	}

	fs.failPut = false
	if _, err := g.Rels.Add(context.Background(), "REAL_REL", a, b, nil); err != nil {
		t.Fatalf("Add real relationship: %v", err)
	}
	rels, err := g.Rels.ByType("REAL_REL", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByType(REAL_REL): %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("ByType(REAL_REL) len = %d, want 1; stale failed row inherited reused token", len(rels))
	}
}

func TestGraphAddRelationshipExistingRelTypeFailureDeletesPartialRow(t *testing.T) {
	t.Parallel()

	g, fs, a, b := newRelCreateRollbackGraph(t)
	if _, err := g.Resolve.GetOrCreateRelType("EXISTING_REL"); err != nil {
		t.Fatalf("GetOrCreateRelType: %v", err)
	}

	fs.failPut = true
	_, err := g.Rels.Add(context.Background(), "EXISTING_REL", a, b, nil)
	if !errors.Is(err, fs.err) {
		t.Fatalf("Add existing-type relationship error = %v, want injected", err)
	}

	fs.failPut = false
	if _, err := g.Rels.Add(context.Background(), "EXISTING_REL", a, b, nil); err != nil {
		t.Fatalf("retry Add existing-type relationship: %v", err)
	}
	rels, err := g.Rels.ByType("EXISTING_REL", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByType(EXISTING_REL): %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("ByType(EXISTING_REL) len = %d, want 1; failed row was not cleaned up", len(rels))
	}
}

func TestGraphAddRelationshipExistingRelTypePanicDeletesPartialRow(t *testing.T) {
	t.Parallel()

	g, fs, a, b := newRelCreateRollbackGraph(t)
	if _, err := g.Resolve.GetOrCreateRelType("EXISTING_REL"); err != nil {
		t.Fatalf("GetOrCreateRelType: %v", err)
	}

	fs.panicPut = true
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("Add existing-type relationship did not panic")
			}
		}()
		_, _ = g.Rels.Add(context.Background(), "EXISTING_REL", a, b, nil)
	}()

	fs.panicPut = false
	if _, err := g.Rels.Add(context.Background(), "EXISTING_REL", a, b, nil); err != nil {
		t.Fatalf("retry Add existing-type relationship: %v", err)
	}
	rels, err := g.Rels.ByType("EXISTING_REL", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByType(EXISTING_REL): %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("ByType(EXISTING_REL) len = %d, want 1; panic row was not cleaned up", len(rels))
	}
}

func TestGraphAddRelationshipExistingRelTypeCleanupFailureReturnsPartialRow(t *testing.T) {
	t.Parallel()

	g, fs, a, b := newRelCreateRollbackGraph(t)
	if _, err := g.Resolve.GetOrCreateRelType("EXISTING_REL"); err != nil {
		t.Fatalf("GetOrCreateRelType: %v", err)
	}

	fs.failPut = true
	fs.failDelete = true
	rel, err := g.Rels.Add(context.Background(), "EXISTING_REL", a, b, nil)
	if !errors.Is(err, fs.err) {
		t.Fatalf("Add existing-type relationship error = %v, want injected", err)
	}
	if rel == nil {
		t.Fatal("Add existing-type relationship returned nil after cleanup failure; live partial row must be caller-visible")
	}
	rels, err := g.Rels.ByType("EXISTING_REL", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByType(EXISTING_REL): %v", err)
	}
	if len(rels) != 1 || rels[0].ID() != rel.ID() {
		t.Fatalf("ByType(EXISTING_REL) = %v rows, want retained live partial row %d", len(rels), rel.ID())
	}
}

func TestGraphAddRelationshipCleanupFailureRetainsRelTypeToken(t *testing.T) {
	t.Parallel()

	g, fs, a, b := newRelCreateRollbackGraph(t)

	fs.failPut = true
	fs.failDelete = true
	rel, err := g.Rels.Add(context.Background(), "QUARANTINED_REL", a, b, nil)
	if !errors.Is(err, fs.err) {
		t.Fatalf("Add relationship error = %v, want injected", err)
	}
	if rel == nil {
		t.Fatal("Add relationship returned nil relationship after cleanup failure; live partial row must be caller-visible")
	}
	if tok, ok := g.Resolve.LookupRelType("QUARANTINED_REL"); !ok || tok == 0 {
		t.Fatalf("LookupRelType(QUARANTINED_REL) = %d, %v; want retained token", tok, ok)
	}
	quarantined, err := g.Rels.ByType("QUARANTINED_REL", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByType(QUARANTINED_REL): %v", err)
	}
	if len(quarantined) != 1 || quarantined[0].ID() != rel.ID() {
		t.Fatalf("ByType(QUARANTINED_REL) = %v rows, want retained live partial row %d", len(quarantined), rel.ID())
	}

	fs.failPut = false
	fs.failDelete = false
	if _, err := g.Rels.Add(context.Background(), "REAL_REL", a, b, nil); err != nil {
		t.Fatalf("Add real relationship: %v", err)
	}
	rels, err := g.Rels.ByType("REAL_REL", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByType(REAL_REL): %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("ByType(REAL_REL) len = %d, want 1; retained token should prevent stale-row inheritance", len(rels))
	}
}

func TestGraphAddRelationshipByIDFailureDeletesPartialRowBeforeRelTypeRollback(t *testing.T) {
	t.Parallel()

	g, fs, a, b := newRelCreateRollbackGraph(t)

	fs.failPut = true
	_, err := g.Rels.AddByID(context.Background(), "TRANSIENT_REL", a.ID(), b.ID(), nil)
	if !errors.Is(err, fs.err) {
		t.Fatalf("AddByID transient relationship error = %v, want injected", err)
	}
	if tok, ok := g.Resolve.LookupRelType("TRANSIENT_REL"); ok || tok != 0 {
		t.Fatalf("LookupRelType(TRANSIENT_REL) = %d, %v; want rollback", tok, ok)
	}

	fs.failPut = false
	if _, err := g.Rels.AddByID(context.Background(), "REAL_REL", a.ID(), b.ID(), nil); err != nil {
		t.Fatalf("AddByID real relationship: %v", err)
	}
	rels, err := g.Rels.ByType("REAL_REL", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByType(REAL_REL): %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("ByType(REAL_REL) len = %d, want 1; stale failed row inherited reused token", len(rels))
	}
}

func TestGraphAddRelationshipByIDIfAbsentFailureDeletesPartialRowBeforeRelTypeRollback(t *testing.T) {
	t.Parallel()

	g, fs, a, b := newRelCreateRollbackGraph(t)

	fs.failPut = true
	_, created, err := g.Rels.AddByIDIfAbsent(context.Background(), "TRANSIENT_REL", a.ID(), b.ID(), nil)
	if !errors.Is(err, fs.err) || created {
		t.Fatalf("AddByIDIfAbsent transient = created %v, err %v; want injected create failure", created, err)
	}
	if tok, ok := g.Resolve.LookupRelType("TRANSIENT_REL"); ok || tok != 0 {
		t.Fatalf("LookupRelType(TRANSIENT_REL) = %d, %v; want rollback", tok, ok)
	}

	fs.failPut = false
	_, created, err = g.Rels.AddByIDIfAbsent(context.Background(), "REAL_REL", a.ID(), b.ID(), nil)
	if err != nil || !created {
		t.Fatalf("AddByIDIfAbsent real = created %v, err %v; want create", created, err)
	}
	rels, err := g.Rels.ByType("REAL_REL", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByType(REAL_REL): %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("ByType(REAL_REL) len = %d, want 1; stale failed row inherited reused token", len(rels))
	}
}

func TestGraphImportRelationshipFailureDeletesPartialRowBeforeRelTypeRollback(t *testing.T) {
	t.Parallel()

	g, fs, a, b := newRelCreateRollbackGraph(t)
	relID := types.RelID(snowflake.ID(900001))

	fs.failPut = true
	_, err := g.Rels.Import(context.Background(), relID, "TRANSIENT_REL", a, b, nil)
	if !errors.Is(err, fs.err) {
		t.Fatalf("Import transient relationship error = %v, want injected", err)
	}
	if tok, ok := g.Resolve.LookupRelType("TRANSIENT_REL"); ok || tok != 0 {
		t.Fatalf("LookupRelType(TRANSIENT_REL) = %d, %v; want rollback", tok, ok)
	}

	fs.failPut = false
	if _, err := g.Rels.Import(context.Background(), relID, "REAL_REL", a, b, nil); err != nil {
		t.Fatalf("Import real relationship with same ID: %v", err)
	}
	rels, err := g.Rels.ByType("REAL_REL", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByType(REAL_REL): %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("ByType(REAL_REL) len = %d, want 1; stale failed row inherited reused token", len(rels))
	}
}

func TestBatchAddRelationshipFailureDeletesPartialRowBeforeRelTypeRollback(t *testing.T) {
	t.Parallel()

	g, fs, a, b := newRelCreateRollbackGraph(t)
	bb, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	if _, err := bb.AddRelationship("TRANSIENT_REL", a, b, nil); err != nil {
		t.Fatalf("Batch AddRelationship: %v", err)
	}

	fs.failPut = true
	res, err := bb.Execute()
	if !errors.Is(err, ErrBatchFailed) {
		t.Fatalf("Execute error = %v, want ErrBatchFailed", err)
	}
	if res == nil || res.Created != 0 || res.Failed != 1 {
		t.Fatalf("Execute result = %+v, want Created=0 Failed=1", res)
	}
	if tok, ok := g.Resolve.LookupRelType("TRANSIENT_REL"); ok || tok != 0 {
		t.Fatalf("LookupRelType(TRANSIENT_REL) = %d, %v; want rollback", tok, ok)
	}

	fs.failPut = false
	if _, err := g.Rels.Add(context.Background(), "REAL_REL", a, b, nil); err != nil {
		t.Fatalf("Add real relationship: %v", err)
	}
	rels, err := g.Rels.ByType("REAL_REL", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByType(REAL_REL): %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("ByType(REAL_REL) len = %d, want 1; stale failed row inherited reused token", len(rels))
	}
}

func TestBatchAddRelationshipCleanupFailureRetainsRelTypeToken(t *testing.T) {
	t.Parallel()

	g, fs, a, b := newRelCreateRollbackGraph(t)
	bb, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	rel, err := bb.AddRelationship("QUARANTINED_REL", a, b, nil)
	if err != nil {
		t.Fatalf("Batch AddRelationship: %v", err)
	}

	fs.failPut = true
	fs.failDelete = true
	res, err := bb.Execute()
	if !errors.Is(err, ErrBatchFailed) {
		t.Fatalf("Execute error = %v, want ErrBatchFailed", err)
	}
	if res == nil || res.Created != 1 || res.Failed != 1 {
		t.Fatalf("Execute result = %+v, want Created=1 Failed=1", res)
	}
	if stats, _ := g.Stats.Get(); stats.RelsAdded != 1 {
		t.Fatalf("RelsAdded after live failed batch relationship = %d, want 1", stats.RelsAdded)
	}
	if tok, ok := g.Resolve.LookupRelType("QUARANTINED_REL"); !ok || tok == 0 {
		t.Fatalf("LookupRelType(QUARANTINED_REL) = %d, %v; want retained token", tok, ok)
	}
	quarantined, err := g.Rels.ByType("QUARANTINED_REL", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByType(QUARANTINED_REL): %v", err)
	}
	if len(quarantined) != 1 || quarantined[0].ID() != rel.ID() {
		t.Fatalf("ByType(QUARANTINED_REL) = %v rows, want retained live partial row %d", len(quarantined), rel.ID())
	}

	fs.failPut = false
	fs.failDelete = false
	if _, err := g.Rels.Add(context.Background(), "REAL_REL", a, b, nil); err != nil {
		t.Fatalf("Add real relationship: %v", err)
	}
	rels, err := g.Rels.ByType("REAL_REL", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByType(REAL_REL): %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("ByType(REAL_REL) len = %d, want 1; retained token should prevent stale-row inheritance", len(rels))
	}
}

func TestBatchAddRelationshipExistingRelTypePanicDeletesPartialRow(t *testing.T) {
	t.Parallel()

	g, fs, a, b := newRelCreateRollbackGraph(t)
	if _, err := g.Resolve.GetOrCreateRelType("EXISTING_REL"); err != nil {
		t.Fatalf("GetOrCreateRelType: %v", err)
	}
	bb, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	if _, err := bb.AddRelationship("EXISTING_REL", a, b, nil); err != nil {
		t.Fatalf("Batch AddRelationship: %v", err)
	}

	fs.panicPut = true
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("Execute did not panic")
			}
		}()
		_, _ = bb.Execute()
	}()

	fs.panicPut = false
	if _, err := g.Rels.Add(context.Background(), "EXISTING_REL", a, b, nil); err != nil {
		t.Fatalf("Add existing-type relationship: %v", err)
	}
	rels, err := g.Rels.ByType("EXISTING_REL", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByType(EXISTING_REL): %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("ByType(EXISTING_REL) len = %d, want 1; panic row was not cleaned up", len(rels))
	}
}

func TestGraphAddRelNilStart(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)

	_, err := g.Rels.Add(context.Background(), "KNOWS", nil, nB, nil)
	if !errors.Is(err, ErrNilNode) {
		t.Errorf("AddRelationship(nil start): errors.Is(err, ErrNilNode) = false; err = %v", err)
	}
}

func TestGraphAddRelNilEnd(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)

	_, err := g.Rels.Add(context.Background(), "KNOWS", nA, nil, nil)
	if !errors.Is(err, ErrNilNode) {
		t.Errorf("AddRelationship(nil end): errors.Is(err, ErrNilNode) = false; err = %v", err)
	}
}

func TestGraphAddRelEmptyType(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)

	_, err := g.Rels.Add(context.Background(), "", nA, nB, nil)
	if err == nil {
		t.Fatal("AddRelationship(\"\") should return error")
	}
}

// ---- AddRelationshipByID ----

func TestGraphAddRelationshipByID(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	nB, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})

	aID := nA.ID()
	bID := nB.ID()

	r, err := g.Rels.AddByID(context.Background(), "KNOWS", aID, bID, map[string]any{"since": 2020})
	if err != nil {
		t.Fatalf("AddRelationshipByID() returned error: %v", err)
	}

	// Verify type.
	if g.Rels.Type(r) != "KNOWS" {
		t.Errorf("RelationshipType = %q, want \"KNOWS\"", g.Rels.Type(r))
	}

	// Verify endpoints.
	if r.StartNodeID() != aID {
		t.Error("StartNodeID does not match nA")
	}
	if r.EndNodeID() != bID {
		t.Error("EndNodeID does not match nB")
	}

	// Verify property.
	since, ok := r.GetProperty("since")
	if !ok || since != 2020 {
		t.Errorf("GetProperty(\"since\") = (%v, %v), want (2020, true)", since, ok)
	}

	// Verify relationship is retrievable from the store.
	fetched, err := g.Rels.Get(context.Background(), r.ID())
	if err != nil {
		t.Fatalf("GetRelationship() returned error: %v", err)
	}
	if g.Rels.Type(fetched) != "KNOWS" {
		t.Errorf("fetched type = %q, want \"KNOWS\"", g.Rels.Type(fetched))
	}

	// Verify integrity: hash and endpoint hashes match Add's semantics.
	ig := r.Integrity()
	if ig == nil {
		t.Fatal("Integrity() is nil")
	}
	if ig.Hash == "" {
		t.Error("integrity hash should be non-empty")
	}
	if ig.FromNodeHash == "" {
		t.Error("FromNodeHash should be non-empty for ByID path")
	}
	if ig.ToNodeHash == "" {
		t.Error("ToNodeHash should be non-empty for ByID path")
	}

	// Verify adjacency: nA should have an outgoing KNOWS relationship.
	rels, err := g.Rels.Outgoing(aID, "KNOWS")
	if err != nil {
		t.Fatalf("OutgoingRelationships() returned error: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("OutgoingRelationships: got %d, want 1", len(rels))
	}
}

func TestGraphAddRelationshipByID_SelfLoop(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nID := n.ID()

	_, err := g.Rels.AddByID(context.Background(), "SELF", nID, nID, nil)
	if !errors.Is(err, ErrSelfLoop) {
		t.Errorf("AddRelationshipByID(self): got %v, want ErrSelfLoop", err)
	}
}

func TestGraphAddRelationshipByIDIfAbsent(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	aID := nA.ID()
	bID := nB.ID()

	// First call: should create.
	r1, created1, err := g.Rels.AddByIDIfAbsent(context.Background(), "KNOWS", aID, bID, nil)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if !created1 {
		t.Error("first call: created should be true")
	}
	if ig := r1.Integrity(); ig == nil || ig.FromNodeHash == "" || ig.ToNodeHash == "" {
		t.Fatalf("created relationship endpoint hashes = %#v, want both non-empty", ig)
	}

	// Second call: should return existing.
	r2, created2, err := g.Rels.AddByIDIfAbsent(context.Background(), "KNOWS", aID, bID, nil)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if created2 {
		t.Error("second call: created should be false")
	}
	if r2.ID() != r1.ID() {
		t.Error("second call should return same relationship")
	}

	// Verify only one relationship exists.
	rels, err := g.Rels.Outgoing(aID, "KNOWS")
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("OutgoingRelationships: got %d, want 1", len(rels))
	}
}

type ifAbsentExistingRowStore struct {
	storepkg.MandatoryStore
	start        types.NodeID
	existingRows []*types.Relationship
}

func (s *ifAbsentExistingRowStore) OutgoingRelationships(id types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	if id == s.start && len(s.existingRows) != 0 {
		return s.existingRows, nil
	}
	return s.MandatoryStore.OutgoingRelationships(id, typeToken)
}

func TestGraphAddRelationshipByIDIfAbsentCopiesExistingCustomStoreRow(t *testing.T) {
	t.Parallel()

	store := &ifAbsentExistingRowStore{MandatoryStore: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	nA, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add nA: %v", err)
	}
	nB, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add nB: %v", err)
	}
	rel, err := g.Rels.AddByID(context.Background(), "KNOWS", nA.ID(), nB.ID(), nil)
	if err != nil {
		t.Fatalf("AddByID: %v", err)
	}

	storeRow := rel.DeepCopy()
	store.start = nA.ID()
	store.existingRows = []*types.Relationship{storeRow}

	got, created, err := g.Rels.AddByIDIfAbsent(context.Background(), "KNOWS", nA.ID(), nB.ID(), nil)
	if err != nil {
		t.Fatalf("AddByIDIfAbsent existing: %v", err)
	}
	if created {
		t.Fatal("AddByIDIfAbsent existing created a new relationship")
	}
	if got.ID() != rel.ID() {
		t.Fatalf("AddByIDIfAbsent existing ID = %d, want %d", got.ID(), rel.ID())
	}
	if got == storeRow {
		t.Fatal("AddByIDIfAbsent returned custom store-owned relationship row")
	}

	if err := got.SetProperty("caller_mutation", "visible only to caller"); err != nil {
		t.Fatalf("mutate returned relationship: %v", err)
	}
	if _, ok := storeRow.GetProperty("caller_mutation"); ok {
		t.Fatal("mutating returned existing relationship changed the custom store row")
	}
}

func TestGraphAddRelationshipByIDIfAbsent_Concurrent(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	aID := nA.ID()
	bID := nB.ID()

	const goroutines = 10
	const iterations = 50

	var createdCount atomic.Int64
	var errCount atomic.Int64
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range iterations {
				_, created, err := g.Rels.AddByIDIfAbsent(context.Background(), "KNOWS", aID, bID, nil)
				if err != nil {
					errCount.Add(1)
					continue
				}
				if created {
					createdCount.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if errCount.Load() != 0 {
		t.Errorf("errors: %d", errCount.Load())
	}
	if createdCount.Load() != 1 {
		t.Errorf("created count: got %d, want 1", createdCount.Load())
	}

	rels, err := g.Rels.Outgoing(aID, "KNOWS")
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("OutgoingRelationships: got %d, want 1", len(rels))
	}
}

func TestGraphRelationshipsByType(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)

	g.Rels.Add(context.Background(), "KNOWS", nA, nB, nil)
	g.Rels.Add(context.Background(), "KNOWS", nB, nA, nil)
	g.Rels.Add(context.Background(), "LIKES", nA, nB, nil)

	knows, err := g.Rels.ByType("KNOWS", storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(knows) != 2 {
		t.Fatalf("RelationshipsByType(\"KNOWS\") = %d, want 2", len(knows))
	}

	likes, err := g.Rels.ByType("LIKES", storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(likes) != 1 {
		t.Fatalf("RelationshipsByType(\"LIKES\") = %d, want 1", len(likes))
	}

	unknown, err := g.Rels.ByType("HATES", storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(unknown) != 0 {
		t.Fatalf("RelationshipsByType(\"HATES\") = %d, want 0", len(unknown))
	}
}

func TestGraphRelCount(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	g.Rels.Add(context.Background(), "R", nA, nB, nil)
	rc, err := g.Rels.Count()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 1 {
		t.Fatalf("RelationshipCount() = %d, want 1", rc)
	}
}

func TestGraphDeleteRelationship(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "R", nA, nB, nil)

	if err := g.Rels.Delete(context.Background(), r.ID()); err != nil {
		t.Fatalf("DeleteRelationship() returned error: %v", err)
	}
	rc, err := g.Rels.Count()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 0 {
		t.Errorf("RelationshipCount() = %d, want 0", rc)
	}
}

func TestGraphOutgoingRelationships(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	c, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)

	g.Rels.Add(context.Background(), "KNOWS", a, b, nil)
	g.Rels.Add(context.Background(), "WORKS_WITH", a, c, nil)

	// All outgoing from A.
	all, err := g.Rels.Outgoing(a.ID(), "")
	if err != nil {
		t.Fatalf("OutgoingRelationships(all): %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("OutgoingRelationships(all) = %d, want 2", len(all))
	}

	// Filtered by type.
	knows, err := g.Rels.Outgoing(a.ID(), "KNOWS")
	if err != nil {
		t.Fatalf("OutgoingRelationships(KNOWS): %v", err)
	}
	if len(knows) != 1 {
		t.Fatalf("OutgoingRelationships(KNOWS) = %d, want 1", len(knows))
	}

	// Unregistered type returns nil.
	none, err := g.Rels.Outgoing(a.ID(), "NONEXISTENT")
	if err != nil {
		t.Fatalf("OutgoingRelationships(NONEXISTENT): %v", err)
	}
	if none != nil {
		t.Errorf("OutgoingRelationships(NONEXISTENT) = %v, want nil", none)
	}

	// Node with no outgoing.
	empty, err := g.Rels.Outgoing(c.ID(), "")
	if err != nil {
		t.Fatalf("OutgoingRelationships(c, all): %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("OutgoingRelationships(c, all) = %d, want 0", len(empty))
	}
}

func TestGraphIncomingRelationships(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	c, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)

	g.Rels.Add(context.Background(), "KNOWS", a, b, nil)
	g.Rels.Add(context.Background(), "WORKS_WITH", c, b, nil)

	// All incoming to B.
	all, err := g.Rels.Incoming(b.ID(), "")
	if err != nil {
		t.Fatalf("IncomingRelationships(all): %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("IncomingRelationships(all) = %d, want 2", len(all))
	}

	// Filtered by type.
	knows, err := g.Rels.Incoming(b.ID(), "KNOWS")
	if err != nil {
		t.Fatalf("IncomingRelationships(KNOWS): %v", err)
	}
	if len(knows) != 1 {
		t.Fatalf("IncomingRelationships(KNOWS) = %d, want 1", len(knows))
	}

	// Unregistered type returns nil.
	none, err := g.Rels.Incoming(b.ID(), "NONEXISTENT")
	if err != nil {
		t.Fatalf("IncomingRelationships(NONEXISTENT): %v", err)
	}
	if none != nil {
		t.Errorf("IncomingRelationships(NONEXISTENT) = %v, want nil", none)
	}

	// Node with no incoming.
	empty, err := g.Rels.Incoming(a.ID(), "")
	if err != nil {
		t.Fatalf("IncomingRelationships(a, all): %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("IncomingRelationships(a, all) = %d, want 0", len(empty))
	}
}

// ─── UpdateNode tests ────────────────────────────────────────────────────────

func TestGraphUpdateRelationship(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", nA, nB, map[string]any{"since": 2020})
	id := r.ID()

	updated, err := g.Rels.Update(context.Background(), id, map[string]any{"since": 2021})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}
	v, ok := updated.GetProperty("since")
	if !ok || v != 2021 {
		t.Fatalf("since = %v, want 2021", v)
	}

	// Verify persisted.
	got, _ := g.Rels.Get(context.Background(), id)
	v, _ = got.GetProperty("since")
	if v != 2021 {
		t.Fatalf("persisted since = %v, want 2021", v)
	}
}

func TestGraphUpdateRelAddProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", nA, nB, nil)
	id := r.ID()

	_, err := g.Rels.Update(context.Background(), id, map[string]any{"weight": 0.5})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	got, _ := g.Rels.Get(context.Background(), id)
	v, ok := got.GetProperty("weight")
	if !ok || v != 0.5 {
		t.Fatalf("weight = %v, want 0.5", v)
	}
}

func TestGraphUpdateRelDeleteProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", nA, nB, map[string]any{"since": 2020, "note": "friend"})
	id := r.ID()

	_, err := g.Rels.Update(context.Background(), id, map[string]any{"note": nil})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	got, _ := g.Rels.Get(context.Background(), id)
	_, ok := got.GetProperty("note")
	if ok {
		t.Fatal("note should be deleted")
	}
	v, ok := got.GetProperty("since")
	if !ok || v != 2020 {
		t.Fatalf("since = %v, want 2020 (unchanged)", v)
	}
}

func TestGraphUpdateRelMixed(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", nA, nB, map[string]any{"since": 2020, "note": "friend"})
	id := r.ID()

	_, err := g.Rels.Update(context.Background(), id, map[string]any{
		"since":  2021,
		"note":   nil,
		"weight": 0.8,
	})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	got, _ := g.Rels.Get(context.Background(), id)
	v, _ := got.GetProperty("since")
	if v != 2021 {
		t.Fatalf("since = %v, want 2021", v)
	}
	_, ok := got.GetProperty("note")
	if ok {
		t.Fatal("note should be deleted")
	}
	v, ok = got.GetProperty("weight")
	if !ok || v != 0.8 {
		t.Fatalf("weight = %v, want 0.8", v)
	}
}

func TestGraphUpdateRelNotFound(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	_, err := g.Rels.Update(context.Background(), types.RelID(999), map[string]any{"x": 1})
	if !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("UpdateRelationship(nonexistent): errors.Is(err, storepkg.ErrRelNotFound) = false; err = %v", err)
	}
}

func TestGraphUpdateRelInvalidProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", nA, nB, nil)
	id := r.ID()

	_, err := g.Rels.Update(context.Background(), id, map[string]any{"tkg_hack": "bad"})
	if !errors.Is(err, types.ErrReservedPrefix) {
		t.Fatalf("UpdateRelationship(tkg_ key): errors.Is(err, ErrReservedPrefix) = false; err = %v", err)
	}
}

func TestGraphUpdateRelInvalidValue(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", nA, nB, nil)
	id := r.ID()

	type badStruct struct{ X int }
	_, err := g.Rels.Update(context.Background(), id, map[string]any{"bad": badStruct{42}})
	if !errors.Is(err, types.ErrUnsupportedValueType) {
		t.Fatalf("UpdateRelationship(bad value): errors.Is(err, ErrUnsupportedValueType) = false; err = %v", err)
	}
}

func TestGraphUpdateRelVersionIncrement(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", nA, nB, nil)
	id := r.ID()

	if r.Version() != 0 {
		t.Fatalf("initial version = %d, want 0", r.Version())
	}

	u1, _ := g.Rels.Update(context.Background(), id, map[string]any{"x": 1})
	if u1.Version() != 1 {
		t.Fatalf("version after first update = %d, want 1", u1.Version())
	}

	u2, _ := g.Rels.Update(context.Background(), id, map[string]any{"x": 2})
	if u2.Version() != 2 {
		t.Fatalf("version after second update = %d, want 2", u2.Version())
	}
}

func TestGraphUpdateRelEmptyUpdates(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", nA, nB, map[string]any{"x": 1})
	id := r.ID()

	got, err := g.Rels.Update(context.Background(), id, map[string]any{})
	if err != nil {
		t.Fatalf("UpdateRelationship(empty): %v", err)
	}
	if got.Version() != 0 {
		t.Fatalf("version after empty update = %d, want 0 (no bump)", got.Version())
	}
}

func TestGraphUpdateRelEndpointsUnchanged(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", nA, nB, map[string]any{"x": 1})
	id := r.ID()
	origStartID := r.StartNodeID().SnowflakeID()
	origEndID := r.EndNodeID().SnowflakeID()

	g.Rels.Update(context.Background(), id, map[string]any{"x": 2})

	got, _ := g.Rels.Get(context.Background(), id)
	if got.StartNodeID().SnowflakeID() != origStartID {
		t.Fatal("startID changed after update")
	}
	if got.EndNodeID().SnowflakeID() != origEndID {
		t.Fatal("endID changed after update")
	}
}

// ─── Convenience method tests ────────────────────────────────────────────────

func TestGraphSetRelationshipProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", nA, nB, nil)
	id := r.ID()

	if err := g.Rels.SetProperty(context.Background(), id, "weight", 0.5); err != nil {
		t.Fatalf("SetRelationshipProperty: %v", err)
	}

	got, _ := g.Rels.Get(context.Background(), id)
	v, ok := got.GetProperty("weight")
	if !ok || v != 0.5 {
		t.Fatalf("weight = %v, want 0.5", v)
	}
}

func TestGraphDeleteRelationshipProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", nA, nB, map[string]any{"weight": 0.5})
	id := r.ID()

	if err := g.Rels.DeleteProperty(context.Background(), id, "weight"); err != nil {
		t.Fatalf("DeleteRelationshipProperty: %v", err)
	}

	got, _ := g.Rels.Get(context.Background(), id)
	_, ok := got.GetProperty("weight")
	if ok {
		t.Fatal("weight should be deleted")
	}
}

// ─── MemoryStore integration: UpdateNode / UpdateRelationship ────────────────

func TestGraphUpdateRelWithMemStore(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", nA, nB, map[string]any{"since": 2020})
	id := r.ID()

	_, err = g.Rels.Update(context.Background(), id, map[string]any{"since": 2021})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	got, _ := g.Rels.Get(context.Background(), id)
	v, _ := got.GetProperty("since")
	if v != 2021 {
		t.Fatalf("since = %v, want 2021", v)
	}
}

func TestGraphConcurrentAddRelSameEndpoints(t *testing.T) {
	t.Parallel()

	const iterations = 50
	for i := range iterations {
		t.Run(fmt.Sprintf("iter_%d", i), func(t *testing.T) {
			t.Parallel()
			g, err := New(Config{Store: memory.New()})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer g.Close()

			nA, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
			nB, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)

			var wg sync.WaitGroup
			wg.Add(2)

			var err1, err2 error
			go func() {
				defer wg.Done()
				_, err1 = g.Rels.Add(context.Background(), "R1", nA, nB, nil)
			}()
			go func() {
				defer wg.Done()
				_, err2 = g.Rels.Add(context.Background(), "R2", nA, nB, nil)
			}()
			wg.Wait()

			if err1 != nil {
				t.Fatalf("AddRelationship R1: %v", err1)
			}
			if err2 != nil {
				t.Fatalf("AddRelationship R2: %v", err2)
			}

			rc, _ := g.Rels.Count()
			if rc != 2 {
				t.Fatalf("RelationshipCount = %d, want 2", rc)
			}

			out, err := g.Rels.Outgoing(nA.ID(), "")
			if err != nil {
				t.Fatalf("OutgoingRelationships: %v", err)
			}
			if len(out) != 2 {
				t.Fatalf("outgoing = %d, want 2", len(out))
			}
		})
	}
}

func TestGraphAllRels(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	nC, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	g.Rels.Add(context.Background(), "KNOWS", nA, nB, nil)
	g.Rels.Add(context.Background(), "LIKES", nB, nC, nil)

	got, err := g.Rels.All(storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("AllRelationships() returned error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("AllRelationships() = %d rels, want 2", len(got))
	}
}

func TestGraphAllRelsEmpty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	got, err := g.Rels.All(storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("AllRelationships() returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("AllRelationships() on empty graph = %v, want nil", got)
	}
}

func TestGraphGetRelsByIDs(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	r1, _ := g.Rels.Add(context.Background(), "KNOWS", nA, nB, nil)
	r2, _ := g.Rels.Add(context.Background(), "LIKES", nA, nB, nil)

	ids := []types.RelID{
		r1.ID(),
		types.RelID(999), // missing
		r2.ID(),
	}

	_, err := g.Rels.GetByIDs(ids)
	if !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("GetRelationshipsByIDs() err = %v, want ErrRelNotFound", err)
	}
}

func TestGraphGetRelsByIDsDuplicatesReturnIndependentRows(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add nA: %v", err)
	}
	nB, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add nB: %v", err)
	}
	r1, err := g.Rels.Add(context.Background(), "KNOWS", nA, nB, nil)
	if err != nil {
		t.Fatalf("Add r1: %v", err)
	}
	r2, err := g.Rels.Add(context.Background(), "LIKES", nA, nB, nil)
	if err != nil {
		t.Fatalf("Add r2: %v", err)
	}

	beforeSnap, _ := g.Stats.Get()

	before := beforeSnap.RelsRead
	got, err := g.Rels.GetByIDs([]types.RelID{r2.ID(), r1.ID(), r2.ID()})
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs duplicates: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("GetRelationshipsByIDs duplicates len = %d, want 3", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].ID() < got[i-1].ID() {
			t.Fatalf("GetRelationshipsByIDs not sorted: result[%d].ID=%d < result[%d].ID=%d", i, got[i].ID(), i-1, got[i-1].ID())
		}
	}
	var copies []*types.Relationship
	for _, r := range got {
		if r.ID() == r2.ID() {
			copies = append(copies, r)
		}
	}
	if len(copies) != 2 || copies[0] == copies[1] {
		t.Fatal("GetRelationshipsByIDs returned aliased rows for duplicate relationship IDs")
	}
	afterSnap, _ := g.Stats.Get()
	if after := afterSnap.RelsRead; after != before+int64(len(got))  {
		t.Fatalf("RelsRead after duplicate GetByIDs = %d, want %d", after, before+int64(len(got)))
	}
}

func TestGraphGetRelsByIDsEmpty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	got, err := g.Rels.GetByIDs(nil)
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs(nil) returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("GetRelationshipsByIDs(nil) = %v, want nil", got)
	}
}

// --- Per-Label / Per-Type Statistics tests ---

func TestRelCountByType_Empty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Resolve.GetOrCreateRelType("KNOWS")

	count, err := g.Rels.CountByType("KNOWS")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Fatalf("RelCountByType = %d, want 0", count)
	}
}

func TestRelCountByType_UnregisteredType(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	count, err := g.Rels.CountByType("NeverRegistered")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Fatalf("RelCountByType = %d, want 0", count)
	}
}

func TestRelCountByType_SingleRel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	g.Rels.Add(context.Background(), "KNOWS", a, b, nil)

	count, err := g.Rels.CountByType("KNOWS")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Fatalf("RelCountByType = %d, want 1", count)
	}
}

func TestRelCountByType_AfterDelete(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	r1, _ := g.Rels.Add(context.Background(), "KNOWS", a, b, nil)
	g.Rels.Add(context.Background(), "KNOWS", b, a, nil)

	g.Rels.Delete(context.Background(), r1.ID())

	count, err := g.Rels.CountByType("KNOWS")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Fatalf("RelCountByType after delete = %d, want 1", count)
	}
}

func TestMemStoreRelCountByType_AfterPut(t *testing.T) {
	t.Parallel()
	ms := memory.New()

	n1 := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	n2 := types.NewNode(types.NodeID(snowflake.ID(200)), 1, nil)
	ms.PutNode(n1)
	ms.PutNode(n2)

	r := types.NewRelationship(types.RelID(snowflake.ID(300)), 5, types.NodeID(snowflake.ID(100)), types.NodeID(snowflake.ID(200)))
	ms.PutRelationship(r)

	c, _ := ms.RelCountByType(5)
	if c != 1 {
		t.Fatalf("type 5 count = %d, want 1", c)
	}
	c0, _ := ms.RelCountByType(99) // non-existent
	if c0 != 0 {
		t.Fatalf("type 99 count = %d, want 0", c0)
	}
}

func TestMemStoreRelCountByType_AfterDelete(t *testing.T) {
	t.Parallel()
	ms := memory.New()

	n1 := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	n2 := types.NewNode(types.NodeID(snowflake.ID(200)), 1, nil)
	ms.PutNode(n1)
	ms.PutNode(n2)

	r1 := types.NewRelationship(types.RelID(snowflake.ID(300)), 5, types.NodeID(snowflake.ID(100)), types.NodeID(snowflake.ID(200)))
	r2 := types.NewRelationship(types.RelID(snowflake.ID(400)), 5, types.NodeID(snowflake.ID(200)), types.NodeID(snowflake.ID(100)))
	ms.PutRelationship(r1)
	ms.PutRelationship(r2)

	ms.DeleteRelationship(types.RelID(300))

	c, _ := ms.RelCountByType(5)
	if c != 1 {
		t.Fatalf("type 5 count after delete = %d, want 1", c)
	}
}

func TestGraphOutgoingRelationshipsForNodes(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	c, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)

	g.Rels.Add(context.Background(), "KNOWS", a, b, nil)
	g.Rels.Add(context.Background(), "WORKS_WITH", a, c, nil)
	g.Rels.Add(context.Background(), "KNOWS", b, c, nil)

	aID := a.ID()
	bID := b.ID()
	cID := c.ID()

	// All outgoing for A and B.
	got, err := g.Rels.OutgoingForNodes([]types.NodeID{aID, bID}, "")
	if err != nil {
		t.Fatalf("OutgoingRelationshipsForNodes: %v", err)
	}
	if len(got[aID]) != 2 {
		t.Fatalf("node A: got %d rels, want 2", len(got[aID]))
	}
	if len(got[bID]) != 1 {
		t.Fatalf("node B: got %d rels, want 1", len(got[bID]))
	}

	// Type-filtered.
	got, err = g.Rels.OutgoingForNodes([]types.NodeID{aID, bID}, "KNOWS")
	if err != nil {
		t.Fatalf("OutgoingRelationshipsForNodes(KNOWS): %v", err)
	}
	if len(got[aID]) != 1 {
		t.Fatalf("node A KNOWS: got %d, want 1", len(got[aID]))
	}
	if len(got[bID]) != 1 {
		t.Fatalf("node B KNOWS: got %d, want 1", len(got[bID]))
	}

	// Node with no outgoing absent from result.
	got, err = g.Rels.OutgoingForNodes([]types.NodeID{cID}, "")
	if err != nil {
		t.Fatalf("OutgoingRelationshipsForNodes(c): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("node C (no outgoing): got %d entries, want 0", len(got))
	}

	// Empty input.
	got, err = g.Rels.OutgoingForNodes(nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("nil input: got %v, want nil", got)
	}
}

func TestGraphOutgoingForNodesUnregisteredType(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	g.Rels.Add(context.Background(), "KNOWS", a, b, nil)

	got, err := g.Rels.OutgoingForNodes(
		[]types.NodeID{a.ID()}, "NONEXISTENT")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("unregistered type: got %v, want nil", got)
	}
}

// ─── IncomingRelationshipsForNodes ───────────────────────────────────────────

func TestGraphIncomingRelationshipsForNodes(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	c, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)

	g.Rels.Add(context.Background(), "KNOWS", a, b, nil)
	g.Rels.Add(context.Background(), "WORKS_WITH", a, c, nil)
	g.Rels.Add(context.Background(), "KNOWS", c, b, nil)

	aID := a.ID()
	bID := b.ID()
	cID := c.ID()

	// All incoming to B and C.
	got, err := g.Rels.IncomingForNodes([]types.NodeID{bID, cID}, "")
	if err != nil {
		t.Fatalf("IncomingRelationshipsForNodes: %v", err)
	}
	if len(got[bID]) != 2 {
		t.Fatalf("node B: got %d rels, want 2", len(got[bID]))
	}
	if len(got[cID]) != 1 {
		t.Fatalf("node C: got %d rels, want 1", len(got[cID]))
	}

	// Type-filtered.
	got, err = g.Rels.IncomingForNodes([]types.NodeID{bID, cID}, "KNOWS")
	if err != nil {
		t.Fatalf("IncomingRelationshipsForNodes(KNOWS): %v", err)
	}
	if len(got[bID]) != 2 {
		t.Fatalf("node B KNOWS: got %d, want 2", len(got[bID]))
	}
	if _, ok := got[cID]; ok {
		t.Fatal("node C should not be in KNOWS result")
	}

	// Node with no incoming absent from result.
	got, err = g.Rels.IncomingForNodes([]types.NodeID{aID}, "")
	if err != nil {
		t.Fatalf("IncomingRelationshipsForNodes(a): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("node A (no incoming): got %d entries, want 0", len(got))
	}

	// Empty input.
	got, err = g.Rels.IncomingForNodes(nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("nil input: got %v, want nil", got)
	}
}

func TestGraphIncomingForNodesUnregisteredType(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	g.Rels.Add(context.Background(), "KNOWS", a, b, nil)

	got, err := g.Rels.IncomingForNodes(
		[]types.NodeID{b.ID()}, "NONEXISTENT")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("unregistered type: got %v, want nil", got)
	}
}

func TestGraphAdjacencyMissingNodeReturnsErrNodeNotFound(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if _, err := g.Rels.Add(context.Background(), "KNOWS", a, b, nil); err != nil {
		t.Fatal(err)
	}

	missing := types.NodeID(snowflake.ID(999))
	if _, err := g.Rels.Outgoing(missing, ""); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("Outgoing missing err = %v, want ErrNodeNotFound", err)
	}
	if _, err := g.Rels.Incoming(missing, ""); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("Incoming missing err = %v, want ErrNodeNotFound", err)
	}
	if got, err := g.Rels.OutgoingForNodes([]types.NodeID{a.ID(), missing}, ""); !errors.Is(err, storepkg.ErrNodeNotFound) || got != nil {
		t.Fatalf("OutgoingForNodes mixed = %#v, %v; want nil, ErrNodeNotFound", got, err)
	}
	if got, err := g.Rels.IncomingForNodes([]types.NodeID{b.ID(), missing}, ""); !errors.Is(err, storepkg.ErrNodeNotFound) || got != nil {
		t.Fatalf("IncomingForNodes mixed = %#v, %v; want nil, ErrNodeNotFound", got, err)
	}
}

func TestGraphAdjacencyMissingNodeWithUnregisteredTypeReturnsErrNodeNotFound(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if _, err := g.Rels.Add(context.Background(), "KNOWS", a, b, nil); err != nil {
		t.Fatal(err)
	}

	missing := types.NodeID(snowflake.ID(999))
	if _, err := g.Rels.Outgoing(missing, "NONEXISTENT"); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("Outgoing missing with unregistered type err = %v, want ErrNodeNotFound", err)
	}
	if _, err := g.Rels.Incoming(missing, "NONEXISTENT"); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("Incoming missing with unregistered type err = %v, want ErrNodeNotFound", err)
	}
	if got, err := g.Rels.OutgoingForNodes([]types.NodeID{a.ID(), missing}, "NONEXISTENT"); !errors.Is(err, storepkg.ErrNodeNotFound) || got != nil {
		t.Fatalf("OutgoingForNodes mixed with unregistered type = %#v, %v; want nil, ErrNodeNotFound", got, err)
	}
	if got, err := g.Rels.IncomingForNodes([]types.NodeID{b.ID(), missing}, "NONEXISTENT"); !errors.Is(err, storepkg.ErrNodeNotFound) || got != nil {
		t.Fatalf("IncomingForNodes mixed with unregistered type = %#v, %v; want nil, ErrNodeNotFound", got, err)
	}
}
