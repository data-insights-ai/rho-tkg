package core

import (
	"context"
	"errors"
	"fmt"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Round-3 break-the-system tests for the transaction door. Rollback is the
// deepest state-restoration machinery in the codebase (history snapshots,
// registry pointer swaps, deleted-row restoration, created-ID teardown,
// import-over-deleted-ID identity tracking). One adversarial transaction
// exercises ALL of it at once, then asserts the EXACT pre-tx state — any
// phantom history row, leaked registry token, or wrong-identity restoration
// fails an assertion.

// nodeFingerprint captures everything that must survive a rollback.
type nodeFingerprint struct {
	version   uint32
	state     any
	validFrom types.Instant
	validTo   types.Instant
	history   []string // "version:state" per history row
	hash      string
}

func fingerprintNode(t *testing.T, g *Core, id types.NodeID) nodeFingerprint {
	t.Helper()
	ctx := context.Background()
	n, err := g.Nodes.Get(ctx, id)
	if err != nil {
		t.Fatalf("fingerprint Get(%v): %v", id, err)
	}
	fp := nodeFingerprint{version: n.Version()}
	fp.state, _ = n.GetProperty("state")
	if tm := n.Temporal(); tm != nil {
		fp.validFrom, fp.validTo = tm.ValidFrom, tm.ValidTo
	}
	if ig := n.Integrity(); ig != nil {
		fp.hash = ig.Hash
	}
	hist, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("fingerprint History(%v): %v", id, err)
	}
	for _, h := range hist {
		s, _ := h.GetProperty("state")
		fp.history = append(fp.history, fmt.Sprintf("%d:%v", h.Version(), s))
	}
	return fp
}

func TestTxRollbackRestoresExactPreTxState(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	// --- Pre-tx world: two nodes with history, one rel. ---
	a, err := g.Nodes.Add(ctx, []string{"A"}, map[string]any{
		"tkg_valid_from": types.Instant(1000), "state": "a0",
	})
	if err != nil {
		t.Fatalf("add A: %v", err)
	}
	if _, err := g.Nodes.Update(ctx, a.ID(), map[string]any{"state": "a1"}); err != nil {
		t.Fatalf("update A: %v", err)
	}
	b, err := g.Nodes.Add(ctx, []string{"B"}, map[string]any{"state": "b0"})
	if err != nil {
		t.Fatalf("add B: %v", err)
	}
	if _, err := g.Nodes.Update(ctx, b.ID(), map[string]any{"state": "b1"}); err != nil {
		t.Fatalf("update B: %v", err)
	}
	if _, err := g.Rels.Add(ctx, "PRE", a, b, nil); err != nil {
		t.Fatalf("add rel: %v", err)
	}

	fpA := fingerprintNode(t, g, a.ID())
	fpB := fingerprintNode(t, g, b.ID())
	preNodeCount, _ := g.Nodes.Count()
	preRelCount, _ := g.Rels.Count()
	if _, ok := g.labels.Lookup("TxOnlyLabel"); ok {
		t.Fatalf("TxOnlyLabel pre-registered; test setup broken")
	}
	if _, ok := g.relTypes.Lookup("TX_ONLY_TYPE"); ok {
		t.Fatalf("TX_ONLY_TYPE pre-registered; test setup broken")
	}

	// --- The hostile transaction: touch every rollback subsystem. ---
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.UpdateNode(a.ID(), map[string]any{"state": "a2-tx"}); err != nil {
		t.Fatalf("tx update A: %v", err)
	}
	if err := tx.DeleteNode(b.ID()); err != nil {
		t.Fatalf("tx delete B: %v", err)
	}
	created, err := tx.AddNode([]string{"TxOnlyLabel"}, map[string]any{"state": "ghost"})
	if err != nil {
		t.Fatalf("tx add: %v", err)
	}
	if _, err := tx.AddRelationship("TX_ONLY_TYPE", created, a, nil); err != nil {
		t.Fatalf("tx add rel: %v", err)
	}
	// The lesson-17 evil case: re-import B's ID with a DIFFERENT shape after
	// deleting it in the same tx. Rollback must restore the ORIGINAL B, not
	// keep the impostor and not delete the row as "created".
	impostor, err := tx.ImportNodeWithID(ctx, b.ID(), []string{"Impostor"}, map[string]any{"state": "impostor"})
	if err != nil {
		t.Fatalf("tx import over deleted ID: %v", err)
	}
	if impostor.ID() != b.ID() {
		t.Fatalf("import did not reuse B's ID")
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// --- Exact-state assertions. ---
	if got := fingerprintNode(t, g, a.ID()); fmt.Sprint(got) != fmt.Sprint(fpA) {
		t.Errorf("A not restored exactly:\n got %+v\nwant %+v", got, fpA)
	}
	if got := fingerprintNode(t, g, b.ID()); fmt.Sprint(got) != fmt.Sprint(fpB) {
		t.Errorf("B not restored exactly (impostor or deletion leaked):\n got %+v\nwant %+v", got, fpB)
	}
	if _, err := g.Nodes.Get(ctx, created.ID()); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Errorf("tx-created node survived rollback: %v", err)
	}
	if nc, _ := g.Nodes.Count(); nc != preNodeCount {
		t.Errorf("node count %d, want %d", nc, preNodeCount)
	}
	if rc, _ := g.Rels.Count(); rc != preRelCount {
		t.Errorf("rel count %d, want %d", rc, preRelCount)
	}
	if _, ok := g.labels.Lookup("TxOnlyLabel"); ok {
		t.Errorf("rolled-back label leaked into the registry")
	}
	if _, ok := g.relTypes.Lookup("TX_ONLY_TYPE"); ok {
		t.Errorf("rolled-back rel type leaked into the registry")
	}
	// Integrity must verify on both restored chains.
	for name, id := range map[string]types.NodeID{"A": a.ID(), "B": b.ID()} {
		valid, err := g.Hash.VerifyNodeChain(id)
		if err != nil || !valid {
			t.Errorf("%s hash chain broken after rollback: valid=%v err=%v", name, valid, err)
		}
	}
	// Temporal queries unbroken: A at VT=1500 must resolve the genesis state.
	at, err := g.Temporal.NodeAt(a.ID(), 1500)
	if err != nil {
		t.Fatalf("NodeAt(A,1500) after rollback: %v", err)
	}
	if s, _ := at.GetProperty("state"); s != "a0" {
		t.Errorf("NodeAt(A,1500) state = %v, want a0 — rollback disturbed the version timeline", s)
	}
}

// The tx door must preserve the temporal contracts fixed in earlier rounds:
// explicit-VT updates through a transaction tile the timeline, historical
// point queries see pre-update state, and bitemporal queries at (oldVT, now)
// keep working after supersession (lesson 43) when the supersession happened
// inside a committed tx.
func TestTxCommitPreservesTemporalContracts(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	n, err := tx.AddNode([]string{"T"}, map[string]any{
		"tkg_valid_from": types.Instant(1000), "state": "v0",
	})
	if err != nil {
		t.Fatalf("tx add: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit 1: %v", err)
	}

	tx2, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx 2: %v", err)
	}
	if _, err := tx2.UpdateNode(n.ID(), map[string]any{
		"tkg_valid_from": types.Instant(2000), "state": "v1",
	}); err != nil {
		t.Fatalf("tx update: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("commit 2: %v", err)
	}

	at, err := g.Temporal.NodeAt(n.ID(), 1500)
	if err != nil {
		t.Fatalf("NodeAt(1500): %v", err)
	}
	if s, _ := at.GetProperty("state"); s != "v0" {
		t.Fatalf("NodeAt(1500) through tx door = %v, want v0", s)
	}
	txNow := g.now() + 1
	at, err = g.Temporal.NodeAtTx(n.ID(), 1500, txNow)
	if err != nil {
		t.Fatalf("NodeAtTx(1500, now) after tx supersession: %v", err)
	}
	if s, _ := at.GetProperty("state"); s != "v0" {
		t.Fatalf("NodeAtTx(1500, now) through tx door = %v, want v0", s)
	}
	at, err = g.Temporal.NodeAt(n.ID(), 2500)
	if err != nil {
		t.Fatalf("NodeAt(2500): %v", err)
	}
	if s, _ := at.GetProperty("state"); s != "v1" {
		t.Fatalf("NodeAt(2500) = %v, want v1", s)
	}
}
