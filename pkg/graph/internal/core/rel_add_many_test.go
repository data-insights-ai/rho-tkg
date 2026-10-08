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

// relBatchFailStore fails or panics after its relationship batch write, and
// can fail the cleanup's deletes.
type relBatchFailStore struct {
	*memory.Store
	err        error
	failBatch  bool
	panicBatch bool
	failDelete bool
}

func (s *relBatchFailStore) PutRelationshipsBatch(rels []*types.Relationship) error {
	if err := s.Store.PutRelationshipsBatch(rels); err != nil {
		return err
	}
	if s.panicBatch {
		panic("injected post-write relationship batch panic")
	}
	if s.failBatch {
		return s.err
	}
	return nil
}

func (s *relBatchFailStore) DeleteRelationship(id types.RelID) error {
	if s.failDelete {
		return s.err
	}
	return s.Store.DeleteRelationship(id)
}

func newRelBatchFailGraph(t *testing.T) (*Core, *relBatchFailStore) {
	t.Helper()
	fs := &relBatchFailStore{Store: memory.New(), err: errors.New("injected post-write relationship batch failure")}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g, fs
}

func addRelsEndpoints(t *testing.T, g *Core, n int) []types.NodeID {
	t.Helper()
	ids := make([]types.NodeID, n)
	for i := range ids {
		nd, err := g.Nodes.Add(context.Background(), []string{"P"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = nd.ID()
	}
	return ids
}

func addRelsSpecs(ids []types.NodeID, n int) []RelCreate {
	specs := make([]RelCreate, n)
	for i := range specs {
		specs[i] = RelCreate{StartID: ids[i%len(ids)], EndID: ids[(i+1)%len(ids)], Props: map[string]any{"i": int64(i)}}
	}
	return specs
}

func relCount(t *testing.T, g *Core) int {
	t.Helper()
	n, err := g.Rels.Count()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// A refused element leaves no relationship and no new type behind, and the
// error names the element.
func TestAddRelationshipsInternalRestoresType(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)
	ids := addRelsEndpoints(t, g, 3)
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	specs := addRelsSpecs(ids, 3)
	specs[2].Props = map[string]any{"bad": struct{}{}}
	if rels, err := tx.AddRelationships("FRESH", specs); err == nil || rels != nil {
		t.Fatalf("AddRelationships = %d rels, %v; want nil and an error", len(rels), err)
	} else if !strings.Contains(err.Error(), "relationship 2 of 3") {
		t.Fatalf("err %q does not name the element", err)
	}
	specs = addRelsSpecs(ids, 3)
	specs[1].EndID = g.Nodes.NextID() // never stored
	if rels, err := tx.AddRelationships("FRESH", specs); !errors.Is(err, storepkg.ErrNodeNotFound) || rels != nil {
		t.Fatalf("missing endpoint: %d rels, %v; want nil and ErrNodeNotFound", len(rels), err)
	} else if !strings.Contains(err.Error(), "relationship 1 of 3") {
		t.Fatalf("err %q does not name the element", err)
	}
	if _, ok := g.relTypes.Lookup("FRESH"); ok {
		t.Fatal("a refused call left its type")
	}
	if n := relCount(t, g); n != 0 {
		t.Fatalf("%d relationships", n)
	}
}

// A store batch that fails after writing: the written relationships are
// removed, the new type is restored, the add counter does not move. When the
// cleanup itself fails the error says so.
func TestAddRelationshipsInternalStoreFailure(t *testing.T) {
	t.Parallel()
	g, fs := newRelBatchFailGraph(t)
	ids := addRelsEndpoints(t, g, 3)
	fs.failBatch = true
	before, _ := g.Stats.Get()
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	rels, err := tx.AddRelationships("FRESH", addRelsSpecs(ids, 4))
	if !errors.Is(err, fs.err) || rels != nil {
		t.Fatalf("AddRelationships = %d rels, %v; want nil and the injected error", len(rels), err)
	}
	if n := relCount(t, g); n != 0 {
		t.Fatalf("%d relationships stayed after the failed batch", n)
	}
	if _, ok := g.relTypes.Lookup("FRESH"); ok {
		t.Fatal("the failed batch left its type")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	after, _ := g.Stats.Get()
	if after.RelsAdded != before.RelsAdded {
		t.Fatalf("RelsAdded %d -> %d", before.RelsAdded, after.RelsAdded)
	}

	fs.failDelete = true
	tx, err = g.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.AddRelationships("FRESH", addRelsSpecs(ids, 2))
	if !errors.Is(err, fs.err) || !strings.Contains(err.Error(), "failed to remove partial relationship") {
		t.Fatalf("err = %v, want the injected error and the cleanup failure", err)
	}
	fs.failDelete, fs.failBatch = false, false
	_ = tx.Rollback()
}

// A panic in the store's batch write releases the registry lock and the
// endpoint locks and restores the type; the graph keeps working.
func TestAddRelationshipsInternalPanic(t *testing.T) {
	t.Parallel()
	g, fs := newRelBatchFailGraph(t)
	ids := addRelsEndpoints(t, g, 3)
	fs.panicBatch = true
	func() {
		tx, err := g.BeginTx()
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if recover() == nil {
				t.Fatal("the injected panic did not surface")
			}
			_ = tx.Rollback()
		}()
		_, _ = tx.AddRelationships("FRESH", addRelsSpecs(ids, 2))
	}()
	if _, ok := g.relTypes.Lookup("FRESH"); ok {
		t.Fatal("the panicking batch left its type")
	}
	fs.panicBatch = false
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.AddRelationships("FRESH", addRelsSpecs(ids, 2)); err != nil {
		t.Fatalf("after the panic: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if n := relCount(t, g); n != 2 {
		t.Fatalf("%d relationships, want 2", n)
	}
	// The endpoint locks were released: a standalone write on them proceeds.
	if _, err := g.Rels.AddByID(context.Background(), "AFTER", ids[0], ids[1], nil); err != nil {
		t.Fatal(err)
	}
}

// Under a scoped write token or with temporal constraints every relationship
// takes the single create path; a cancelled context creates nothing; the
// per-element path keeps the earlier relationships when a later one fails.
func TestAddRelationshipsInternalScopedAndCancelled(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)
	ids := addRelsEndpoints(t, g, 3)
	ctx := withScopeToken(context.Background(), 7)
	if g.canBatchRelCreates(ctx) {
		t.Fatal("a scoped write must not batch")
	}
	if !g.canBatchRelCreates(context.Background()) {
		t.Fatal("a plain write without constraints batches")
	}
	var got int
	if _, err := g.runUnderRLock(func() {
		created, err := g.addRelationshipsInternal(ctx, "S", addRelsSpecs(ids, 3))
		if err != nil {
			t.Errorf("scoped AddRelationships: %v", err)
		}
		got = len(created)
	}); err != nil {
		t.Fatal(err)
	}
	if got != 3 || relCount(t, g) != 3 {
		t.Fatalf("scoped path created %d (store %d), want 3", got, relCount(t, g))
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.runUnderRLock(func() {
		created, err := g.addRelationshipsInternal(cancelled, "S", addRelsSpecs(ids, 3))
		if !errors.Is(err, context.Canceled) || created != nil {
			t.Errorf("cancelled: %d rels, %v", len(created), err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if n := relCount(t, g); n != 3 {
		t.Fatalf("%d relationships after the cancelled call, want 3", n)
	}
	if _, err := g.runUnderRLock(func() {
		specs := addRelsSpecs(ids, 2)
		specs[1].Props = map[string]any{"x": struct{}{}}
		created, err := g.addRelationshipsInternal(ctx, "S", specs)
		if err == nil || len(created) != 1 {
			t.Errorf("per-element path with a bad second element: %d rels, %v; want the first and an error", len(created), err)
		}
	}); err != nil {
		t.Fatal(err)
	}
}

// The temporal shadow keys of each element: valid time, creation time and,
// where the graph allows backfill, the transaction time; refused otherwise.
func TestAddRelationshipsInternalTemporalKeys(t *testing.T) {
	t.Parallel()
	g, err := New(Config{AllowTxBackfill: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ids := addRelsEndpoints(t, g, 2)
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	specs := []RelCreate{
		{StartID: ids[0], EndID: ids[1], Props: map[string]any{"tkg_valid_from": types.Instant(100), "tkg_valid_to": types.Instant(200), "tkg_created_at": types.Instant(50)}},
		{StartID: ids[1], EndID: ids[0], Props: map[string]any{"tkg_tx_from": types.Instant(5000)}},
		{StartID: ids[0], EndID: ids[1]},
	}
	rels, err := tx.AddRelationships("T", specs)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tm := rels[0].Temporal()
	if tm.ValidFrom != 100 || tm.ValidTo != 200 || tm.CreatedAt != 50 {
		t.Fatalf("relationship 0 temporal %+v", *tm)
	}
	if got := rels[1].Temporal().TxFrom; got != 5000 {
		t.Fatalf("relationship 1 TxFrom %d, want the backfilled 5000", got)
	}
	if got := rels[2].Temporal().TxFrom; got <= 5000 {
		t.Fatalf("relationship 2 TxFrom %d, want the clock", got)
	}

	plain := newTxTestGraph(t)
	pids := addRelsEndpoints(t, plain, 2)
	tx, err = plain.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.AddRelationships("T", []RelCreate{{StartID: pids[0], EndID: pids[1], Props: map[string]any{"tkg_tx_from": types.Instant(5000)}}}); !errors.Is(err, ErrTxBackfillDisabled) {
		t.Fatalf("backfill without the gate: %v, want ErrTxBackfillDisabled", err)
	}
	if _, err := tx.AddRelationships("T", []RelCreate{{StartID: pids[0], EndID: pids[1], Props: map[string]any{"tkg_valid_from": "x"}}}); err == nil {
		t.Fatal("a malformed valid time passed")
	}
	if _, err := tx.AddRelationships("T", []RelCreate{{StartID: pids[0], EndID: pids[1], Props: map[string]any{"tkg_author_id": int64(1)}}}); err == nil {
		t.Fatal("a malformed provenance key passed")
	}
}
