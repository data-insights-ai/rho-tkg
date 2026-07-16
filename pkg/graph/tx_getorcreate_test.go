package graph_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Ask 1 — GraphTx.GetOrCreateByKey (tx-scoped atomic get-or-create)
//
// The tx-scoped sibling of NodeOps.GetOrCreateByKey. Unlike the g.Nodes() door
// (which auto-commits its own write), the create PARTICIPATES in the caller's
// open transaction: visible to later reads on the same tx, published on Commit,
// undone by Rollback. Backends: memory + badger (uniqueBackends).
// =============================================================================

// Within one tx, a second GetOrCreateByKey with the same key is a HIT of the
// first call's create (ghost-read consistency) — created=false, same ID, and the
// node is readable via the tx's own read door before Commit.
func TestTxGetOrCreateByKey_InTxVisibility(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()

			tx, err := g.Tx().Begin()
			if err != nil {
				t.Fatalf("Begin: %v", err)
			}
			defer tx.Rollback()

			first, created, err := tx.GetOrCreateByKey("User", "email", "ghost@x.com", map[string]any{"name": "G"})
			if err != nil || !created {
				t.Fatalf("first GetOrCreateByKey = (%v, created=%v), want create", err, created)
			}
			// The uncommitted create is visible to the tx's own read door.
			if _, err := tx.GetNode(first.ID()); err != nil {
				t.Fatalf("in-tx GetNode of uncommitted create: %v", err)
			}
			second, created2, err := tx.GetOrCreateByKey("User", "email", "ghost@x.com", map[string]any{"name": "SHOULD-BE-IGNORED"})
			if err != nil {
				t.Fatalf("second GetOrCreateByKey: %v", err)
			}
			if created2 {
				t.Fatalf("second call created=true, want false (ghost-read hit)")
			}
			if second.ID() != first.ID() {
				t.Fatalf("hit id = %d, want %d", second.ID(), first.ID())
			}
			// extraProps ignored on the hit — the original name survives.
			if name, _ := second.GetProperty("name"); name != "G" {
				t.Fatalf("hit name = %v, want G (extraProps must be ignored on hit)", name)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit: %v", err)
			}
			// Exactly one holder after commit.
			holders, _ := g.Nodes().ByLabelAndProperty("User", "email", "ghost@x.com", store.QueryOpts{})
			if len(holders) != 1 {
				t.Fatalf("post-commit holders = %d, want 1", len(holders))
			}
		})
	}
}

// Two-phase (rule 15): a create made inside a tx must be UNDONE by Rollback. The
// node exists at phase 1 (in-tx read succeeds) and is GONE at phase 2 (after
// Rollback, neither the property scan nor a point read finds it) — proving the
// create truly participated in the tx rather than auto-committing.
func TestTxGetOrCreateByKey_RollbackUndoesCreate(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			ctx := context.Background()

			tx, err := g.Tx().Begin()
			if err != nil {
				t.Fatalf("Begin: %v", err)
			}
			node, created, err := tx.GetOrCreateByKey("User", "email", "rollback@x.com", nil)
			if err != nil || !created {
				t.Fatalf("GetOrCreateByKey = (%v, created=%v)", err, created)
			}
			id := node.ID()
			// Phase 1: present inside the tx.
			if _, err := tx.GetNode(id); err != nil {
				t.Fatalf("phase 1 in-tx GetNode: %v", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatalf("Rollback: %v", err)
			}
			// Phase 2: gone everywhere after rollback.
			if _, err := g.Nodes().Get(ctx, id); !errors.Is(err, graphpkg.ErrNodeNotFound) {
				t.Fatalf("post-rollback Get err = %v, want ErrNodeNotFound", err)
			}
			holders, _ := g.Nodes().ByLabelAndProperty("User", "email", "rollback@x.com", store.QueryOpts{})
			if len(holders) != 0 {
				t.Fatalf("post-rollback holders = %d, want 0 (create must be undone)", len(holders))
			}
			// And the key is reusable — a fresh create after rollback succeeds.
			n2, created2, err := g.Nodes().GetOrCreateByKey(ctx, "User", "email", "rollback@x.com", nil)
			if err != nil || !created2 {
				t.Fatalf("post-rollback re-create = (%v, created=%v), want fresh create", err, created2)
			}
			if n2.ID() == id {
				t.Fatalf("re-create reused rolled-back id %d", id)
			}
		})
	}
}

// A GetOrCreateByKey inside a tx for a key that ALREADY exists (created before
// the tx opened) is a hit — created=false, returns the pre-existing node, adds
// no new holder.
func TestTxGetOrCreateByKey_HitPreExisting(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()

			pre := mustAddUser(t, g, "pre@x.com")

			tx, err := g.Tx().Begin()
			if err != nil {
				t.Fatalf("Begin: %v", err)
			}
			defer tx.Rollback()
			got, created, err := tx.GetOrCreateByKey("User", "email", "pre@x.com", map[string]any{"name": "ignored"})
			if err != nil {
				t.Fatalf("GetOrCreateByKey: %v", err)
			}
			if created {
				t.Fatalf("created=true, want false (pre-existing hit)")
			}
			if got.ID() != pre.ID() {
				t.Fatalf("hit id = %d, want pre-existing %d", got.ID(), pre.ID())
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit: %v", err)
			}
			holders, _ := g.Nodes().ByLabelAndProperty("User", "email", "pre@x.com", store.QueryOpts{})
			if len(holders) != 1 {
				t.Fatalf("holders = %d, want 1 (no new node on hit)", len(holders))
			}
		})
	}
}

// Concurrency: N goroutines each open their OWN tx and GetOrCreateByKey the same
// key, then Commit. Because txs serialize (c.txMu) the first commits the create
// and every later tx sees it (hit) — exactly one create, all the same ID, one
// holder. Runs with and without an active unique constraint (the value stripe,
// not the constraint, is what makes it atomic).
func TestTxGetOrCreateByKey_ConcurrentTxsExactlyOneCreate(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		for _, withConstraint := range []bool{false, true} {
			name := bc.name
			if withConstraint {
				name += "/constraint"
			} else {
				name += "/no-constraint"
			}
			t.Run(name, func(t *testing.T) {
				g := bc.open(t)
				defer g.Close()
				if withConstraint {
					mustCreateUnique(t, g)
				}

				const goroutines = 32
				var creates atomic.Int32
				var wg sync.WaitGroup
				ids := make([]types.NodeID, goroutines)
				errs := make([]error, goroutines)
				start := make(chan struct{})
				wg.Add(goroutines)
				for i := 0; i < goroutines; i++ {
					go func(i int) {
						defer wg.Done()
						<-start
						tx, err := g.Tx().Begin()
						if err != nil {
							errs[i] = err
							return
						}
						n, created, err := tx.GetOrCreateByKey("User", "email", "storm@x.com", map[string]any{"seed": int64(i)})
						if err != nil {
							errs[i] = err
							_ = tx.Rollback()
							return
						}
						if err := tx.Commit(); err != nil {
							errs[i] = err
							return
						}
						ids[i] = n.ID()
						if created {
							creates.Add(1)
						}
					}(i)
				}
				close(start)
				wg.Wait()

				if creates.Load() != 1 {
					t.Fatalf("creates = %d, want exactly 1", creates.Load())
				}
				var want types.NodeID
				for i := 0; i < goroutines; i++ {
					if errs[i] != nil {
						t.Fatalf("goroutine %d err = %v", i, errs[i])
					}
					if want == 0 {
						want = ids[i]
					} else if ids[i] != want {
						t.Fatalf("goroutine %d id = %d, want all == %d", i, ids[i], want)
					}
				}
				holders, _ := g.Nodes().ByLabelAndProperty("User", "email", "storm@x.com", store.QueryOpts{})
				if len(holders) != 1 {
					t.Fatalf("storm holders = %d, want 1", len(holders))
				}
			})
		}
	}
}

// The tx door shares the value stripe with the STANDALONE door: a tx create and
// concurrent standalone GetOrCreateByKey callers on the same key still produce
// exactly one create (all committing — no dirty-read-then-rollback).
func TestTxGetOrCreateByKey_SharesStripeWithStandalone(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			ctx := context.Background()

			const standalone = 16
			var creates atomic.Int32
			ids := make([]types.NodeID, standalone+1)
			errs := make([]error, standalone+1)
			var wg sync.WaitGroup
			start := make(chan struct{})

			// One tx caller.
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				tx, err := g.Tx().Begin()
				if err != nil {
					errs[standalone] = err
					return
				}
				n, created, err := tx.GetOrCreateByKey("User", "email", "mix@x.com", nil)
				if err != nil {
					errs[standalone] = err
					_ = tx.Rollback()
					return
				}
				if err := tx.Commit(); err != nil {
					errs[standalone] = err
					return
				}
				ids[standalone] = n.ID()
				if created {
					creates.Add(1)
				}
			}()

			// Many standalone callers.
			wg.Add(standalone)
			for i := 0; i < standalone; i++ {
				go func(i int) {
					defer wg.Done()
					<-start
					n, created, err := g.Nodes().GetOrCreateByKey(ctx, "User", "email", "mix@x.com", nil)
					errs[i] = err
					if err == nil {
						ids[i] = n.ID()
						if created {
							creates.Add(1)
						}
					}
				}(i)
			}
			close(start)
			wg.Wait()

			for i, err := range errs {
				if err != nil {
					t.Fatalf("caller %d err = %v", i, err)
				}
			}
			if creates.Load() != 1 {
				t.Fatalf("creates = %d, want exactly 1 (shared stripe)", creates.Load())
			}
			// All callers converge on the same node.
			want := ids[0]
			for i, id := range ids {
				if id != want {
					t.Fatalf("caller %d id = %d, want all == %d", i, id, want)
				}
			}
			holders, _ := g.Nodes().ByLabelAndProperty("User", "email", "mix@x.com", store.QueryOpts{})
			if len(holders) != 1 {
				t.Fatalf("mix holders = %d, want 1", len(holders))
			}
		})
	}
}

// Float values are rejected with ErrUniqueUnsupportedType (bit-pattern equality
// trap) — same guard as the standalone door, tested at the tx layer (rule 4).
func TestTxGetOrCreateByKey_FloatRejected(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	tx, err := g.Tx().Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()
	_, _, err = tx.GetOrCreateByKey("Sensor", "reading", 1.5, nil)
	if !errors.Is(err, graphpkg.ErrUniqueUnsupportedType) {
		t.Fatalf("float tx GetOrCreateByKey err = %v, want ErrUniqueUnsupportedType", err)
	}
}
