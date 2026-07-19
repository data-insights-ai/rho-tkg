package graph_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/constraints"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Unique constraints on the tiered store (ADR-0005 §3.5).
//
// Reference-label constraints WORK — reference entities all live on the
// reference shard (+ its cold archive), so the ref-shard property index makes
// ref-shard uniqueness GLOBAL uniqueness. Event-label constraints REJECT with
// ErrUniqueEventLabelUnsupported (values span unbounded time shards, no global
// value index). UniqueForever rides the MetaKV ownership registry globally.
//
// Testing Rule 14: NO sub-second ShardWindow. The ref-label doors never touch
// event shards, so the default 1-week window is used and no rotation is needed.
// =============================================================================

// openTieredUniqueGraph opens a tiered graph rooted at dir with "User" as a
// reference label and "Login" as an (implicit) event label. Returns the graph;
// caller closes it. Reopen with the same dir + RefLabels to test durability.
func openTieredUniqueGraph(t *testing.T, dir string, snowflakeID int64) *graphpkg.Graph {
	t.Helper()
	ts, err := tieredpkg.New(tieredpkg.Config{DataDir: dir, RefLabels: []string{"User"}})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: snowflakeID, Store: ts, AllowReset: true})
	if err != nil {
		_ = ts.Close()
		t.Fatalf("new tiered graph: %v", err)
	}
	return g
}

func mustAddTieredUser(t *testing.T, g *graphpkg.Graph, email string) *types.Node {
	t.Helper()
	n, err := g.Nodes().Add(context.Background(), []string{"User"}, map[string]any{"email": email})
	if err != nil {
		t.Fatalf("add User{email:%q}: %v", email, err)
	}
	return n
}

// TestUniqueTiered_ReferenceLabel_CreateViolateFree is the core reference-label
// battery: create the constraint, a duplicate current value violates, and a
// value freed by supersession becomes reusable — all on tiered.
func TestUniqueTiered_ReferenceLabel_CreateViolateFree(t *testing.T) {
	ctx := context.Background()
	g := openTieredUniqueGraph(t, t.TempDir(), 3)
	defer g.Close()

	if err := g.Constraints().CreateUnique(ctx, "User", "email"); err != nil {
		t.Fatalf("CreateUnique(User,email) on tiered: %v", err)
	}
	// The constraint is introspectable.
	got := g.Constraints().UniqueConstraints()
	if len(got) != 1 || got[0].Label != "User" || got[0].PropertyKey != "email" || got[0].Scope != constraints.UniqueCurrent {
		t.Fatalf("UniqueConstraints() = %+v, want one {User,email,UniqueCurrent}", got)
	}

	a := mustAddTieredUser(t, g, "a@x.com")

	// (1) VIOLATE: a second current node with the same value is rejected.
	_, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "a@x.com"})
	if !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("duplicate create err = %v, want ErrUniqueViolation", err)
	}
	// A different value is fine; a keyless node is unconstrained.
	mustAddTieredUser(t, g, "b@x.com")
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"name": "keyless"}); err != nil {
		t.Fatalf("add keyless User: %v", err)
	}

	// (2) FREE: move a off "a@x.com" — the value is now reusable.
	if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"email": "a2@x.com"}); err != nil {
		t.Fatalf("update a -> a2: %v", err)
	}
	reuser := mustAddTieredUser(t, g, "a@x.com")
	if reuser.ID() == a.ID() {
		t.Fatalf("reuser must be a distinct node")
	}
	// And a's freed value is genuinely gone from current state: re-adding a2
	// (still held by a) violates.
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "a2@x.com"}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("re-add a2 (held by a) err = %v, want ErrUniqueViolation", err)
	}
}

// TestUniqueTiered_ReferenceLabel_CrossDoor asserts enforcement fires on all
// three doors ADR-0002 constraint 1 lists — node-create, value-update, and
// label-add — on the tiered backend, not just node-create. Two reference labels
// (both ref-class, so the label-add door does not trip the primary-label-class
// immutability guard) let the label-add seam be exercised cleanly.
func TestUniqueTiered_ReferenceLabel_CrossDoor(t *testing.T) {
	ctx := context.Background()
	ts, err := tieredpkg.New(tieredpkg.Config{DataDir: t.TempDir(), RefLabels: []string{"User", "Member"}})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 4, Store: ts})
	if err != nil {
		t.Fatalf("new tiered graph: %v", err)
	}
	defer g.Close()

	// Constrain the SECONDARY ref label "Member" on "email".
	if err := g.Constraints().CreateUnique(ctx, "Member", "email"); err != nil {
		t.Fatalf("CreateUnique(Member,email): %v", err)
	}

	// node-create door: two Members with the same email → violation.
	if _, err := g.Nodes().Add(ctx, []string{"Member"}, map[string]any{"email": "a@x.com"}); err != nil {
		t.Fatalf("add Member a: %v", err)
	}
	m2, err := g.Nodes().Add(ctx, []string{"Member"}, map[string]any{"email": "b@x.com"})
	if err != nil {
		t.Fatalf("add Member b: %v", err)
	}

	// value-update door: move m2 onto a's value → violation.
	if _, err := g.Nodes().Update(ctx, m2.ID(), map[string]any{"email": "a@x.com"}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("update-into-violation err = %v, want ErrUniqueViolation", err)
	}

	// label-add door: a User (ref) node holding a's value acquires the Member
	// label — rejected because it would make two Members share "a@x.com". Both
	// labels are ref-class, so the primary-label-class guard is not tripped.
	u, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "a@x.com"})
	if err != nil {
		t.Fatalf("add User holding a's email: %v", err)
	}
	if err := g.Nodes().AddLabel(ctx, u.ID(), "Member"); !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("label-add-into-violation err = %v, want ErrUniqueViolation", err)
	}
	// Adding Member to a User holding a FREE email succeeds.
	uFree, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "free@x.com"})
	if err != nil {
		t.Fatalf("add User holding free email: %v", err)
	}
	if err := g.Nodes().AddLabel(ctx, uFree.ID(), "Member"); err != nil {
		t.Fatalf("label-add of free value: %v", err)
	}
}

// TestUniqueTiered_EventLabelRejected asserts an event-class label's unique
// constraint is refused with the dedicated sentinel, via errors.Is at the
// public door. UniqueForever is refused identically.
func TestUniqueTiered_EventLabelRejected(t *testing.T) {
	ctx := context.Background()
	g := openTieredUniqueGraph(t, t.TempDir(), 5)
	defer g.Close()

	err := g.Constraints().CreateUnique(ctx, "Login", "session")
	if !errors.Is(err, graphpkg.ErrUniqueEventLabelUnsupported) {
		t.Fatalf("CreateUnique(event) err = %v, want ErrUniqueEventLabelUnsupported", err)
	}
	// It must NOT be the property-index sentinel (distinct message/boundary).
	if errors.Is(err, tieredpkg.ErrEventPropertyIndex) {
		t.Fatalf("event-label unique rejection should not alias ErrEventPropertyIndex: %v", err)
	}
	// The rejected constraint left no registry entry behind.
	if got := g.Constraints().UniqueConstraints(); len(got) != 0 {
		t.Fatalf("rejected event constraint leaked into registry: %+v", got)
	}
	// UniqueForever on an event label is refused the same way.
	if err := g.Constraints().CreateUniqueForever(ctx, "Login", "session"); !errors.Is(err, graphpkg.ErrUniqueEventLabelUnsupported) {
		t.Fatalf("CreateUniqueForever(event) err = %v, want ErrUniqueEventLabelUnsupported", err)
	}
	// A reference label on the SAME graph still works (positive control).
	if err := g.Constraints().CreateUnique(ctx, "User", "email"); err != nil {
		t.Fatalf("CreateUnique(User) on tiered still works: %v", err)
	}
}

// TestUniqueTiered_DurableAcrossRestart asserts the ref-label constraint
// registry rehydrates from refShard MetaKV: after reopen the constraint is
// still enforced (a duplicate of a pre-restart value is barred) — the very
// correctness the loadUniqueConstraints short-circuit removal restores.
func TestUniqueTiered_DurableAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	g := openTieredUniqueGraph(t, dir, 6)
	if err := g.Constraints().CreateUnique(ctx, "User", "email"); err != nil {
		t.Fatalf("CreateUnique: %v", err)
	}
	mustAddTieredUser(t, g, "keep@x.com")
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	g2 := openTieredUniqueGraph(t, dir, 6)
	defer g2.Close()
	// Constraint rehydrated.
	if got := g2.Constraints().UniqueConstraints(); len(got) != 1 || got[0].PropertyKey != "email" {
		t.Fatalf("after reopen UniqueConstraints() = %+v, want one {User,email}", got)
	}
	// And still enforced against a value written before the restart.
	if _, err := g2.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "keep@x.com"}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("post-reopen duplicate err = %v, want ErrUniqueViolation", err)
	}
	// A fresh value is admitted.
	mustAddTieredUser(t, g2, "fresh@x.com")
}

// TestUniqueTiered_ResetReaps asserts Admin().Reset clears the durable
// constraint AND forever-owner registries on tiered (reapUnique*ForReset run via
// the ref-shard MetaKV) — after Reset a previously-barred value is free.
func TestUniqueTiered_ResetReaps(t *testing.T) {
	ctx := context.Background()
	g := openTieredUniqueGraph(t, t.TempDir(), 7)
	defer g.Close()

	if err := g.Constraints().CreateUniqueForever(ctx, "User", "email"); err != nil {
		t.Fatalf("CreateUniqueForever: %v", err)
	}
	owner := mustAddTieredUser(t, g, "owned@x.com")
	_ = owner
	// A second holder is barred by ownership.
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "owned@x.com"}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("pre-reset duplicate err = %v, want ErrUniqueViolation", err)
	}

	if err := g.Admin().Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	// Constraint registry reaped.
	if got := g.Constraints().UniqueConstraints(); len(got) != 0 {
		t.Fatalf("Reset left constraints behind: %+v", got)
	}
	// Ownership reaped: the value is claimable again (no constraint bars it now).
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "owned@x.com"}); err != nil {
		t.Fatalf("post-reset add of formerly-owned value: %v", err)
	}
}

// TestUniqueTiered_ForeverAcrossRestart is the two-phase forever-ownership
// durability test the ADR requires: claim a value, reopen, assert the claim
// survives (a DIFFERENT node is still barred) and the OWNER may keep it, then
// ReleaseOwnership frees it for a new claimant.
func TestUniqueTiered_ForeverAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	g := openTieredUniqueGraph(t, dir, 8)
	if err := g.Constraints().CreateUniqueForever(ctx, "User", "email"); err != nil {
		t.Fatalf("CreateUniqueForever: %v", err)
	}
	owner := mustAddTieredUser(t, g, "forever@x.com")
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	g2 := openTieredUniqueGraph(t, dir, 8)
	defer g2.Close()
	// (a) The claim survived: a different node cannot take the value.
	if _, err := g2.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "forever@x.com"}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("post-reopen forever duplicate err = %v, want ErrUniqueViolation", err)
	}
	// (b) The OWNER (same ID, a later version) may keep holding the value.
	if _, err := g2.Nodes().Update(ctx, owner.ID(), map[string]any{"email": "forever@x.com", "note": "same-owner keep"}); err != nil {
		t.Fatalf("owner keeping its forever value across restart: %v", err)
	}
	// (c) Even after the owner is hard-deleted the value stays barred (forever).
	if err := g2.Nodes().Delete(ctx, owner.ID()); err != nil {
		t.Fatalf("delete owner: %v", err)
	}
	if _, err := g2.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "forever@x.com"}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("post-delete forever value err = %v, want ErrUniqueViolation (barred forever)", err)
	}
	// (d) ReleaseOwnership frees it for a fresh claimant.
	if err := g2.Constraints().ReleaseOwnership(ctx, "User", "email", "forever@x.com"); err != nil {
		t.Fatalf("ReleaseOwnership: %v", err)
	}
	if _, err := g2.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "forever@x.com"}); err != nil {
		t.Fatalf("post-release add: %v", err)
	}
}

// TestUniqueTiered_GetOrCreateByKey asserts the atomic get-or-create door works
// on tiered for a reference label: the first call creates, later calls with the
// same value return the SAME node (hit), a different value creates a new node.
// GetOrCreateByKey needs no constraint (the value lock alone makes it atomic).
func TestUniqueTiered_GetOrCreateByKey(t *testing.T) {
	ctx := context.Background()
	g := openTieredUniqueGraph(t, t.TempDir(), 9)
	defer g.Close()

	n1, created1, err := g.Nodes().GetOrCreateByKey(ctx, "User", "email", "goc@x.com", map[string]any{"name": "First"})
	if err != nil {
		t.Fatalf("GetOrCreateByKey create: %v", err)
	}
	if !created1 {
		t.Fatalf("first GetOrCreateByKey should have created")
	}
	n2, created2, err := g.Nodes().GetOrCreateByKey(ctx, "User", "email", "goc@x.com", map[string]any{"name": "Second"})
	if err != nil {
		t.Fatalf("GetOrCreateByKey hit: %v", err)
	}
	if created2 {
		t.Fatalf("second GetOrCreateByKey should have hit, not created")
	}
	if n1.ID() != n2.ID() {
		t.Fatalf("GetOrCreateByKey returned different IDs %d vs %d for same value", n1.ID(), n2.ID())
	}
	// A different value creates a distinct node.
	n3, created3, err := g.Nodes().GetOrCreateByKey(ctx, "User", "email", "other@x.com", nil)
	if err != nil {
		t.Fatalf("GetOrCreateByKey distinct: %v", err)
	}
	if !created3 || n3.ID() == n1.ID() {
		t.Fatalf("distinct value must create a new node (created=%v id=%d)", created3, n3.ID())
	}
}

// TestUniqueTiered_BatchAndTxDoors asserts the batch and transaction create
// doors enforce the reference-label constraint on tiered, mirroring the
// standalone door. Batch: two same-value creates in one batch → the second
// fails, the rest commits. Tx: a same-value create inside one tx → the second
// AddNode fails.
func TestUniqueTiered_BatchAndTxDoors(t *testing.T) {
	ctx := context.Background()
	g := openTieredUniqueGraph(t, t.TempDir(), 11)
	defer g.Close()
	if err := g.Constraints().CreateUnique(ctx, "User", "email"); err != nil {
		t.Fatalf("CreateUnique: %v", err)
	}

	// Batch door.
	b, err := g.Batch().New()
	if err != nil {
		t.Fatalf("Batch.New: %v", err)
	}
	if _, err := b.AddNode([]string{"User"}, map[string]any{"email": "batch@x.com"}); err != nil {
		t.Fatalf("queue first: %v", err)
	}
	if _, err := b.AddNode([]string{"User"}, map[string]any{"email": "batch@x.com"}); err != nil {
		t.Fatalf("queue dup: %v", err)
	}
	if _, err := b.AddNode([]string{"User"}, map[string]any{"email": "batch-ok@x.com"}); err != nil {
		t.Fatalf("queue distinct: %v", err)
	}
	res, err := b.Execute()
	if !errors.Is(err, graphpkg.ErrBatchFailed) {
		t.Fatalf("batch Execute err = %v, want ErrBatchFailed", err)
	}
	if res.Created != 2 || res.Failed != 1 {
		t.Fatalf("batch result Created=%d Failed=%d, want 2/1", res.Created, res.Failed)
	}
	matches, err := g.Nodes().ByLabelAndProperty("User", "email", "batch@x.com", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("lookup batch@x.com: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("batch@x.com holders = %d, want 1", len(matches))
	}

	// Tx door: two same-value creates in one tx — the second fails.
	tx, err := g.Tx().Begin()
	if err != nil {
		t.Fatalf("Tx.Begin: %v", err)
	}
	if _, err := tx.AddNode([]string{"User"}, map[string]any{"email": "tx@x.com"}); err != nil {
		t.Fatalf("tx first AddNode: %v", err)
	}
	if _, err := tx.AddNode([]string{"User"}, map[string]any{"email": "tx@x.com"}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("tx duplicate AddNode err = %v, want ErrUniqueViolation", err)
	}
	_ = tx.Rollback()
}

// TestUniqueTiered_ArchivedEntity_UniqueCurrentStillHeld closes a coverage gap:
// no prior test exercised the archive shard even though the "reference labels
// live on refShard (+ its cold archive)" claim is the correctness foundation
// ADR-0005 §3.5 rests on for global uniqueness. g.Tier().Archive moves a node
// from refShard to refArchive — this is NOT a delete (Delete has its own
// tombstone semantics and IS excluded from UniqueCurrent's current-state
// check); the archived node's row is still the CURRENT belief for that
// entity. Reading the implementation: enforceUniqueForNodeHeld's duplicate
// lookup runs with storepkg.QueryOpts{} (Depth: DepthAll, the zero value),
// and tiered.Store.NodesByLabelAndProperty unconditionally includes refArchive
// whenever opts.Depth == DepthAll — so an archived entity is found exactly
// like a live one and the value stays barred. This IS the documented/intended
// behavior (archive != delete), not a bug being asserted as a contract.
func TestUniqueTiered_ArchivedEntity_UniqueCurrentStillHeld(t *testing.T) {
	ctx := context.Background()
	g := openTieredUniqueGraph(t, t.TempDir(), 14)
	defer g.Close()

	if err := g.Constraints().CreateUnique(ctx, "User", "email"); err != nil {
		t.Fatalf("CreateUnique: %v", err)
	}
	owner := mustAddTieredUser(t, g, "archived@x.com")

	if err := g.Tier().Archive(owner.ID()); err != nil {
		t.Fatalf("Archive: %v", err)
	}

	// The archived node still counts as a CURRENT holder of the value: a
	// duplicate create is rejected exactly as it would be pre-archive.
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "archived@x.com"}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("duplicate create after archive err = %v, want ErrUniqueViolation (archive is not delete)", err)
	}
	// A DIFFERENT value is unaffected by the archived neighbor.
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "fresh@x.com"}); err != nil {
		t.Fatalf("add distinct value while a User is archived: %v", err)
	}

	// Restore: the value stays held by the SAME (now-restored) node — no
	// leak, no double-free across the archive/restore round trip.
	if err := g.Tier().Restore(owner.ID()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "archived@x.com"}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("duplicate create after restore err = %v, want ErrUniqueViolation", err)
	}
	// The owner itself may still update in place holding its own value.
	if _, err := g.Nodes().Update(ctx, owner.ID(), map[string]any{"email": "archived@x.com", "note": "kept"}); err != nil {
		t.Fatalf("owner keeping its own value post-restore: %v", err)
	}
}

// TestUniqueTiered_ForeverAcrossArchiveRestore extends the two-phase
// UniqueForever durability battery (TestUniqueTiered_ForeverAcrossRestart,
// which exercises a full Close/reopen) to the archive/restore round trip:
// the durable ownership claim (a MetaKV registry keyed by value, NOT by
// shard location) must keep barring every other node while the owner lives
// on refArchive, survive Restore, and still let the OWNER itself keep the
// value and an operator release it.
func TestUniqueTiered_ForeverAcrossArchiveRestore(t *testing.T) {
	ctx := context.Background()
	g := openTieredUniqueGraph(t, t.TempDir(), 15)
	defer g.Close()

	if err := g.Constraints().CreateUniqueForever(ctx, "User", "email"); err != nil {
		t.Fatalf("CreateUniqueForever: %v", err)
	}
	owner := mustAddTieredUser(t, g, "forever-archived@x.com")

	if err := g.Tier().Archive(owner.ID()); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	// Barred forever even while the owner lives on the archive shard.
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "forever-archived@x.com"}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("duplicate while archived err = %v, want ErrUniqueViolation", err)
	}

	if err := g.Tier().Restore(owner.ID()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	// Still barred after restore.
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "forever-archived@x.com"}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("duplicate after restore err = %v, want ErrUniqueViolation", err)
	}
	// The OWNER (same ID) may keep holding its own value across the round trip.
	if _, err := g.Nodes().Update(ctx, owner.ID(), map[string]any{"email": "forever-archived@x.com", "note": "same-owner keep"}); err != nil {
		t.Fatalf("owner keeping its forever value across archive/restore: %v", err)
	}
	// Even after the owner is hard-deleted the value stays barred forever
	// (mirrors TestUniqueTiered_ForeverAcrossRestart's step (c) — the
	// durable ownership claim, not the owner's current row, is what bars a
	// duplicate here).
	if err := g.Nodes().Delete(ctx, owner.ID()); err != nil {
		t.Fatalf("delete owner: %v", err)
	}
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "forever-archived@x.com"}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("post-delete forever value err = %v, want ErrUniqueViolation (barred forever)", err)
	}
	// ReleaseOwnership frees it for a fresh claimant.
	if err := g.Constraints().ReleaseOwnership(ctx, "User", "email", "forever-archived@x.com"); err != nil {
		t.Fatalf("ReleaseOwnership: %v", err)
	}
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "forever-archived@x.com"}); err != nil {
		t.Fatalf("post-release add: %v", err)
	}
}

// TestUniqueTiered_ConcurrentStorm races many goroutines all trying to claim the
// SAME reference value; exactly one create must win, every other must see
// ErrUniqueViolation, and the store must end with exactly one node holding it.
// Run under -race (shared shard state under checkout/checkin + value stripes).
func TestUniqueTiered_ConcurrentStorm(t *testing.T) {
	ctx := context.Background()
	g := openTieredUniqueGraph(t, t.TempDir(), 10)
	defer g.Close()
	if err := g.Constraints().CreateUnique(ctx, "User", "email"); err != nil {
		t.Fatalf("CreateUnique: %v", err)
	}

	const racers = 24
	var wins, violations, other atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "storm@x.com"})
			switch {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, graphpkg.ErrUniqueViolation):
				violations.Add(1)
			default:
				other.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if other.Load() != 0 {
		t.Fatalf("storm produced %d unexpected (non-violation) errors", other.Load())
	}
	if wins.Load() != 1 {
		t.Fatalf("storm winners = %d, want exactly 1", wins.Load())
	}
	if violations.Load() != racers-1 {
		t.Fatalf("storm violations = %d, want %d", violations.Load(), racers-1)
	}
	// Exactly one node carries the value.
	matches, err := g.Nodes().ByLabelAndProperty("User", "email", "storm@x.com", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperty: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("after storm %d nodes hold storm@x.com, want 1", len(matches))
	}
}
