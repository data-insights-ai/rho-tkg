package graph_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Stage D — batch enforcement
// =============================================================================

// Two same-value node creates inside ONE batch: the second fails at op time,
// the first commits.
func TestUnique_BatchInternalDuplicate(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUnique(t, g)

			b, err := g.Batch().New()
			if err != nil {
				t.Fatalf("Batch.New: %v", err)
			}
			if _, err := b.AddNode([]string{"User"}, map[string]any{"email": "dup@x.com"}); err != nil {
				t.Fatalf("queue first: %v", err)
			}
			if _, err := b.AddNode([]string{"User"}, map[string]any{"email": "dup@x.com"}); err != nil {
				t.Fatalf("queue second: %v", err)
			}
			// A third, distinct value must still commit.
			if _, err := b.AddNode([]string{"User"}, map[string]any{"email": "ok@x.com"}); err != nil {
				t.Fatalf("queue third: %v", err)
			}
			res, err := b.Execute()
			if !errors.Is(err, graphpkg.ErrBatchFailed) {
				t.Fatalf("Execute err = %v, want ErrBatchFailed", err)
			}
			if res.Created != 2 {
				t.Fatalf("Created = %d, want 2 (first dup + distinct)", res.Created)
			}
			if res.Failed != 1 {
				t.Fatalf("Failed = %d, want 1 (second dup)", res.Failed)
			}
			foundViolation := false
			for _, be := range res.Errors {
				if errors.Is(be.Err, graphpkg.ErrUniqueViolation) {
					foundViolation = true
				}
			}
			if !foundViolation {
				t.Fatalf("no ErrUniqueViolation among batch errors: %+v", res.Errors)
			}
			// Exactly one current node holds dup@x.com.
			got, err := g.Nodes().ByLabelAndProperty("User", "email", "dup@x.com", store.QueryOpts{})
			if err != nil {
				t.Fatalf("lookup: %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("dup@x.com holders = %d, want exactly 1", len(got))
			}
		})
	}
}

// A batch create that collides with COMMITTED state fails; a batch create of a
// fresh value commits.
func TestUnique_BatchVsCommitted(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUnique(t, g)
			mustAddUser(t, g, "taken@x.com")

			b, err := g.Batch().New()
			if err != nil {
				t.Fatalf("Batch.New: %v", err)
			}
			if _, err := b.AddNode([]string{"User"}, map[string]any{"email": "taken@x.com"}); err != nil {
				t.Fatalf("queue collide: %v", err)
			}
			if _, err := b.AddNode([]string{"User"}, map[string]any{"email": "fresh@x.com"}); err != nil {
				t.Fatalf("queue fresh: %v", err)
			}
			res, err := b.Execute()
			if !errors.Is(err, graphpkg.ErrBatchFailed) {
				t.Fatalf("Execute err = %v, want ErrBatchFailed", err)
			}
			if res.Created != 1 || res.Failed != 1 {
				t.Fatalf("Created=%d Failed=%d, want 1/1", res.Created, res.Failed)
			}
			// taken@x.com still has exactly one holder.
			got, _ := g.Nodes().ByLabelAndProperty("User", "email", "taken@x.com", store.QueryOpts{})
			if len(got) != 1 {
				t.Fatalf("taken@x.com holders = %d, want 1", len(got))
			}
		})
	}
}

// Batch UPDATE into a violation is rejected (delegates to the enforced internal
// update door).
func TestUnique_BatchUpdateIntoViolation(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUnique(t, g)
			mustAddUser(t, g, "a@x.com")
			bn := mustAddUser(t, g, "b@x.com")

			b, err := g.Batch().New()
			if err != nil {
				t.Fatalf("Batch.New: %v", err)
			}
			if err := b.UpdateNode(bn.ID(), map[string]any{"email": "a@x.com"}); err != nil {
				t.Fatalf("queue update: %v", err)
			}
			res, err := b.Execute()
			if !errors.Is(err, graphpkg.ErrBatchFailed) {
				t.Fatalf("Execute err = %v, want ErrBatchFailed", err)
			}
			if res.Failed != 1 {
				t.Fatalf("Failed = %d, want 1", res.Failed)
			}
			foundViolation := false
			for _, be := range res.Errors {
				if errors.Is(be.Err, graphpkg.ErrUniqueViolation) {
					foundViolation = true
				}
			}
			if !foundViolation {
				t.Fatalf("no ErrUniqueViolation in batch errors: %+v", res.Errors)
			}
		})
	}
}

// =============================================================================
// Stage D — tx enforcement
// =============================================================================

// Two same-value creates inside one tx: the second fails at op time (the first
// is already visible in the store).
func TestUnique_TxInternalDuplicate(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUnique(t, g)

			tx, err := g.Tx().Begin()
			if err != nil {
				t.Fatalf("Begin: %v", err)
			}
			if _, err := tx.AddNode([]string{"User"}, map[string]any{"email": "tx@x.com"}); err != nil {
				t.Fatalf("first tx create: %v", err)
			}
			_, err = tx.AddNode([]string{"User"}, map[string]any{"email": "tx@x.com"})
			if !errors.Is(err, graphpkg.ErrUniqueViolation) {
				t.Fatalf("second tx create err = %v, want ErrUniqueViolation", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatalf("Rollback: %v", err)
			}
		})
	}
}

// A UniqueCurrent value claimed inside a tx is freed on rollback (removing the
// created node removes its index entry).
func TestUnique_TxRollbackFreesValue(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUnique(t, g)

			tx, err := g.Tx().Begin()
			if err != nil {
				t.Fatalf("Begin: %v", err)
			}
			if _, err := tx.AddNode([]string{"User"}, map[string]any{"email": "rb@x.com"}); err != nil {
				t.Fatalf("tx create: %v", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatalf("Rollback: %v", err)
			}
			// Value is free — a standalone create succeeds.
			mustAddUser(t, g, "rb@x.com")
			// And exactly one holder now.
			got, _ := g.Nodes().ByLabelAndProperty("User", "email", "rb@x.com", store.QueryOpts{})
			if len(got) != 1 {
				t.Fatalf("rb@x.com holders after rollback+recreate = %d, want 1", len(got))
			}
		})
	}
}

// A committed tx create is enforced against a concurrent standalone create of
// the same value: exactly one survives (race-detector coverage).
func TestUnique_TxVsStandaloneRace(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUnique(t, g)

			var wg sync.WaitGroup
			var okCount atomic.Int32
			start := make(chan struct{})
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				tx, err := g.Tx().Begin()
				if err != nil {
					return
				}
				if _, err := tx.AddNode([]string{"User"}, map[string]any{"email": "race@x.com"}); err != nil {
					_ = tx.Rollback()
					return
				}
				if err := tx.Commit(); err == nil {
					okCount.Add(1)
				}
			}()
			go func() {
				defer wg.Done()
				<-start
				if _, err := g.Nodes().Add(context.Background(), []string{"User"}, map[string]any{"email": "race@x.com"}); err == nil {
					okCount.Add(1)
				}
			}()
			close(start)
			wg.Wait()

			got, _ := g.Nodes().ByLabelAndProperty("User", "email", "race@x.com", store.QueryOpts{})
			if len(got) != 1 {
				t.Fatalf("race@x.com holders = %d, want exactly 1", len(got))
			}
			if okCount.Load() != 1 {
				t.Fatalf("successful writers = %d, want exactly 1", okCount.Load())
			}
		})
	}
}

// =============================================================================
// Stage E — import validation + replica no-op
// =============================================================================

// A stream carrying two current nodes with the same constrained value fails the
// post-replay validation with ErrUniqueViolation and rolls the import back.
func TestUnique_ImportValidatesAndRollsBack(t *testing.T) {
	ctx := context.Background()
	// Source graph WITHOUT a constraint holds duplicate emails.
	src, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1})
	if err != nil {
		t.Fatalf("src New: %v", err)
	}
	defer src.Close()
	mustAddUser(t, src, "dup@x.com")
	mustAddUser(t, src, "dup@x.com")
	var buf bytes.Buffer
	if err := src.IO().Export(&buf); err != nil {
		t.Fatalf("Export: %v", err)
	}
	stream := buf.Bytes()

	// Target graph WITH the constraint; strict import must reject + roll back.
	tgt, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 2})
	if err != nil {
		t.Fatalf("tgt New: %v", err)
	}
	defer tgt.Close()
	if err := tgt.Constraints().CreateUnique(ctx, "User", "email"); err != nil {
		t.Fatalf("CreateUnique: %v", err)
	}
	err = tgt.IO().Import(bytes.NewReader(stream), tkgio.ImportOptions{})
	if !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("strict Import err = %v, want ErrUniqueViolation", err)
	}
	// Rolled back — no User nodes landed.
	if n, _ := tgt.Nodes().CountByLabel("User"); n != 0 {
		t.Fatalf("after rolled-back import User count = %d, want 0", n)
	}

	// SkipUniqueValidation lets the trusted restore through (both dups land).
	tgt2, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 3})
	if err != nil {
		t.Fatalf("tgt2 New: %v", err)
	}
	defer tgt2.Close()
	if err := tgt2.Constraints().CreateUnique(ctx, "User", "email"); err != nil {
		t.Fatalf("CreateUnique tgt2: %v", err)
	}
	if err := tgt2.IO().Import(bytes.NewReader(stream), tkgio.ImportOptions{SkipUniqueValidation: true}); err != nil {
		t.Fatalf("trusted Import err = %v, want nil", err)
	}
	if n, _ := tgt2.Nodes().CountByLabel("User"); n != 2 {
		t.Fatalf("after trusted import User count = %d, want 2", n)
	}
}

// Replica apply reproduces the primary's rows VERBATIM and does NOT enforce a
// locally-active unique constraint (ADR-0002 Decision 4). A record that would
// violate the target's constraint still lands.
func TestUnique_ReplicaApplyDoesNotEnforce(t *testing.T) {
	ctx := context.Background()
	primary, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()

	// Primary has NO constraint; create two nodes sharing an email.
	mustAddUser(t, primary, "same@x.com")
	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	lsn0, err := primary.Replication().LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}
	// The second same-email node — its change record is the one we replay.
	mustAddUser(t, primary, "same@x.com")

	// Target bootstraps from the snapshot (aligned registries), THEN installs the
	// constraint (validates against the single bootstrapped node — passes).
	tgt, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 2, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("tgt New: %v", err)
	}
	defer tgt.Close()
	if err := tgt.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("bootstrap Import: %v", err)
	}
	if err := tgt.Constraints().CreateUnique(ctx, "User", "email"); err != nil {
		t.Fatalf("CreateUnique on target: %v", err)
	}

	// Tail the primary's post-snapshot record (the duplicate create) and apply it.
	var recs []store.ChangeRecord
	if err := primary.Replication().ForEachChange(lsn0, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("no records past snapshot LSN")
	}
	if _, err := tgt.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("ApplyChanges err = %v, want nil (apply must not enforce)", err)
	}
	// The duplicate LANDED despite the active constraint — apply does not enforce.
	got, _ := tgt.Nodes().ByLabelAndProperty("User", "email", "same@x.com", store.QueryOpts{})
	if len(got) != 2 {
		t.Fatalf("same@x.com holders on target = %d, want 2 (apply reproduces verbatim)", len(got))
	}
}

// =============================================================================
// GetOrCreateByKey
// =============================================================================

// 100 goroutines GetOrCreateByKey the SAME key: exactly one create, 99 gets, all
// returning the same node ID. Runs with and without an active constraint.
func TestGetOrCreateByKey_IdempotencyStorm(t *testing.T) {
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

				const goroutines = 100
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
						n, created, err := g.Nodes().GetOrCreateByKey(context.Background(), "User", "email", "storm@x.com", map[string]any{"seed": int64(i)})
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
				// Exactly one node with the key exists.
				got, _ := g.Nodes().ByLabelAndProperty("User", "email", "storm@x.com", store.QueryOpts{})
				if len(got) != 1 {
					t.Fatalf("storm@x.com holders = %d, want 1", len(got))
				}
			})
		}
	}
}

// TestGetOrCreateByKey_DoesNotSerializeAgainstPlainConcurrentAddWithoutConstraint
// guards BACKLOG 9s: GetOrCreateByKey's doc says it "works WITHOUT an active
// unique constraint" via its own value stripe, which is true against OTHER
// GetOrCreateByKey callers (TestGetOrCreateByKey_IdempotencyStorm above,
// "no-constraint" case) — but a PLAIN g.Nodes().Add call takes NO value
// stripe at all when no constraint is active on (label, key) (value-stripe
// locking is unique-constraint-enforcement machinery — CLAUDE.md
// "Concurrency": "a create/update/label-add that introduces or changes a
// CONSTRAINED value holds the value stripe"; with no constraint, nothing is
// constrained, so nothing locks). So GetOrCreateByKey's atomicity is scoped
// to callers that participate in the stripe protocol (other
// GetOrCreateByKey calls, or writes enforced by an active constraint) — NOT
// to an arbitrary concurrent plain Add. This test demonstrates that scope
// concretely: many plain Adds racing a single GetOrCreateByKey, with no
// constraint anywhere, CAN and DO produce more than one current node
// holding the same value — the exact behavior the doc needed to state
// explicitly instead of only describing the protected side.
func TestGetOrCreateByKey_DoesNotSerializeAgainstPlainConcurrentAddWithoutConstraint(t *testing.T) {
	const attempts = 20
	const plainWriters = 8

	for attempt := 0; attempt < attempts; attempt++ {
		g, err := graphpkg.New(graphpkg.Config{})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		ctx := context.Background()

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1 + plainWriters)
		go func() {
			defer wg.Done()
			<-start
			_, _, _ = g.Nodes().GetOrCreateByKey(ctx, "User", "email", "race@x.com", nil)
		}()
		for i := 0; i < plainWriters; i++ {
			go func() {
				defer wg.Done()
				<-start
				_, _ = g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "race@x.com"})
			}()
		}
		close(start)
		wg.Wait()

		nodes, err := g.Nodes().ByLabelAndProperty("User", "email", "race@x.com", store.QueryOpts{})
		if err != nil {
			t.Fatalf("attempt %d: ByLabelAndProperty: %v", attempt, err)
		}
		_ = g.Close()
		if len(nodes) > 1 {
			// Reproduced the documented gap — done, no need to keep looping.
			return
		}
	}
	t.Fatalf("plain concurrent Add never landed alongside GetOrCreateByKey across %d attempts — "+
		"could not demonstrate the documented no-constraint scope gap this test exists to pin", attempts)
}

// A hit returns a mutable, independent copy (Get semantics) and the created==false.
func TestGetOrCreateByKey_HitReturnsCopy(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			ctx := context.Background()

			first, created, err := g.Nodes().GetOrCreateByKey(ctx, "User", "email", "x@x.com", map[string]any{"name": "X"})
			if err != nil || !created {
				t.Fatalf("first GetOrCreateByKey = (%v, created=%v)", err, created)
			}
			second, created2, err := g.Nodes().GetOrCreateByKey(ctx, "User", "email", "x@x.com", nil)
			if err != nil {
				t.Fatalf("second GetOrCreateByKey: %v", err)
			}
			if created2 {
				t.Fatalf("second call created = true, want false (hit)")
			}
			if second.ID() != first.ID() {
				t.Fatalf("hit id = %d, want %d", second.ID(), first.ID())
			}
			// Mutating the returned copy must not affect stored state.
			if err := second.SetProperty("scratch", "local"); err != nil {
				t.Fatalf("mutate copy: %v", err)
			}
			reread, _ := g.Nodes().Get(ctx, first.ID())
			if _, ok := reread.GetProperty("scratch"); ok {
				t.Fatalf("mutation of returned copy leaked into stored node")
			}
		})
	}
}

// Float values are rejected (bit-pattern equality trap).
func TestGetOrCreateByKey_FloatRejected(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	_, _, err = g.Nodes().GetOrCreateByKey(context.Background(), "Sensor", "reading", 1.5, nil)
	if !errors.Is(err, graphpkg.ErrUniqueUnsupportedType) {
		t.Fatalf("float GetOrCreateByKey err = %v, want ErrUniqueUnsupportedType", err)
	}
}

// =============================================================================
// Stage D — batch/tx enforcement of UniqueForever ownership
// =============================================================================

// A UniqueForever value owned by a now-deleted entity is still barred on the
// BATCH create door: the batch pre-check consults the durable ownership
// registry, not only current-state equality (there is no current holder after
// the owner is deleted, yet the value stays owned forever).
func TestUniqueForever_BatchBarredAfterDelete(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			ctx := context.Background()
			mustCreateUniqueForever(t, g)
			a := mustAddUser(t, g, "forever@x.com")
			if err := g.Nodes().Delete(ctx, a.ID()); err != nil {
				t.Fatalf("delete owner: %v", err)
			}

			b, err := g.Batch().New()
			if err != nil {
				t.Fatalf("Batch.New: %v", err)
			}
			if _, err := b.AddNode([]string{"User"}, map[string]any{"email": "forever@x.com"}); err != nil {
				t.Fatalf("queue barred: %v", err)
			}
			if _, err := b.AddNode([]string{"User"}, map[string]any{"email": "fresh@x.com"}); err != nil {
				t.Fatalf("queue fresh: %v", err)
			}
			res, err := b.Execute()
			if !errors.Is(err, graphpkg.ErrBatchFailed) {
				t.Fatalf("Execute err = %v, want ErrBatchFailed", err)
			}
			if res.Created != 1 || res.Failed != 1 {
				t.Fatalf("Created=%d Failed=%d, want 1/1", res.Created, res.Failed)
			}
			foundViolation := false
			for _, be := range res.Errors {
				if errors.Is(be.Err, graphpkg.ErrUniqueViolation) {
					foundViolation = true
				}
			}
			if !foundViolation {
				t.Fatalf("no ErrUniqueViolation among batch errors: %+v", res.Errors)
			}
			// The barred value must have zero current holders (the barred create
			// was removed from the write set).
			got, err := g.Nodes().ByLabelAndProperty("User", "email", "forever@x.com", store.QueryOpts{})
			if err != nil {
				t.Fatalf("lookup: %v", err)
			}
			if len(got) != 0 {
				t.Fatalf("forever@x.com holders = %d, want 0 (barred)", len(got))
			}
		})
	}
}

// A batch create of a FRESH UniqueForever value CLAIMS ownership durably: a
// later different-entity create of the same value is barred even after the
// batch-created node is deleted — proving the batch path claims, not just
// checks.
func TestUniqueForever_BatchClaimsOwnership(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			ctx := context.Background()
			mustCreateUniqueForever(t, g)

			b, err := g.Batch().New()
			if err != nil {
				t.Fatalf("Batch.New: %v", err)
			}
			pn, err := b.AddNode([]string{"User"}, map[string]any{"email": "claimed@x.com"})
			if err != nil {
				t.Fatalf("queue claim: %v", err)
			}
			if _, err := b.Execute(); err != nil {
				t.Fatalf("Execute (fresh forever value) err = %v, want nil", err)
			}
			// Delete the batch-created owner; the value stays owned forever.
			if err := g.Nodes().Delete(ctx, pn.ID()); err != nil {
				t.Fatalf("delete batch owner: %v", err)
			}
			_, err = g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "claimed@x.com"})
			if !errors.Is(err, graphpkg.ErrUniqueViolation) {
				t.Fatalf("reclaim of batch-claimed value err = %v, want ErrUniqueViolation", err)
			}
		})
	}
}

// The tx create door enforces UniqueForever through the shared standalone
// kernel (tx.AddNode -> addNodeInternal -> enforceUniqueForNode): a value owned
// by a now-deleted entity is barred inside a transaction.
func TestUniqueForever_TxBarredAfterDelete(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			ctx := context.Background()
			mustCreateUniqueForever(t, g)
			a := mustAddUser(t, g, "txforever@x.com")
			if err := g.Nodes().Delete(ctx, a.ID()); err != nil {
				t.Fatalf("delete owner: %v", err)
			}

			tx, err := g.Tx().Begin()
			if err != nil {
				t.Fatalf("Begin: %v", err)
			}
			_, err = tx.AddNode([]string{"User"}, map[string]any{"email": "txforever@x.com"})
			if !errors.Is(err, graphpkg.ErrUniqueViolation) {
				t.Fatalf("tx create of barred forever value err = %v, want ErrUniqueViolation", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatalf("Rollback: %v", err)
			}
		})
	}
}

// Storm: many goroutines race to claim the SAME UniqueForever value, half via a
// single-node BATCH and half via a standalone create. The batch holds the
// exclusive core write-lock so it serializes with standalone writers; exactly
// one node ends up holding the value and the durable registry names one owner
// (race-detector coverage of the batch claim path).
func TestUniqueForever_BatchStandaloneStorm(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUniqueForever(t, g)

			const goroutines = 40
			var wins atomic.Int32
			var wg sync.WaitGroup
			start := make(chan struct{})
			wg.Add(goroutines)
			for i := 0; i < goroutines; i++ {
				viaBatch := i%2 == 0
				go func() {
					defer wg.Done()
					<-start
					if viaBatch {
						b, err := g.Batch().New()
						if err != nil {
							return
						}
						if _, err := b.AddNode([]string{"User"}, map[string]any{"email": "storm@x.com"}); err != nil {
							return
						}
						res, err := b.Execute()
						if err == nil && res != nil && res.Created == 1 {
							wins.Add(1)
						}
						return
					}
					if _, err := g.Nodes().Add(context.Background(), []string{"User"}, map[string]any{"email": "storm@x.com"}); err == nil {
						wins.Add(1)
					}
				}()
			}
			close(start)
			wg.Wait()

			if wins.Load() != 1 {
				t.Fatalf("successful claimants = %d, want exactly 1", wins.Load())
			}
			got, err := g.Nodes().ByLabelAndProperty("User", "email", "storm@x.com", store.QueryOpts{})
			if err != nil {
				t.Fatalf("lookup: %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("storm@x.com holders = %d, want exactly 1", len(got))
			}
		})
	}
}
