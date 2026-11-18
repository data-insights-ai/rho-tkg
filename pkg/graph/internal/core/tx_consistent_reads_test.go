package core

import (
	"context"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Tx-side resolution and shadow-property mirror tests.
//
// g.Nodes.Labels / HasLabel / PrimaryLabel, g.Rels.Type / HasType, and
// g.Resolve.NodeProperty / RelProperty take c.mu.RLock. An open *GraphTx
// holds c.txMu, not c.mu, so they work inside a tx (see
// TestTxReadAccessors_NonTxAccessorsWorkInsideTx below). The tx mirrors
// (*GraphTx).Labels et al. call the *Unlocked helpers directly and guard only
// the tx-state mutex; these tests pin their results, run them while the test
// holds c.mu (a mirror that took c.mu.RLock would block there), and pin their
// zero values after Commit.

// withCoreLockHeld runs fn on another goroutine while the caller holds c.mu
// for writing, and fails if fn does not return within 5 s: a tx mirror that
// reached for c.mu.RLock would block until the lock is released.
func withCoreLockHeld(t *testing.T, g *Core, name string, fn func()) {
	t.Helper()
	g.mu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
		g.mu.Unlock()
	case <-time.After(5 * time.Second):
		g.mu.Unlock()
		<-done
		t.Fatalf("%s blocked while c.mu was held; the tx mirror must not take c.mu", name)
	}
}

func TestTxLabels_ResolvesInsideTx(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n, err := g.Nodes.Add(context.Background(), []string{"Person", "Customer"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	tx, _ := g.BeginTx()
	defer func() { _ = tx.Rollback() }()

	var got []string
	withCoreLockHeld(t, g, "tx.Labels", func() { got = tx.Labels(n) })
	if len(got) != 2 || got[0] != "Person" || got[1] != "Customer" {
		t.Fatalf("tx.Labels = %v, want [Person Customer]", got)
	}
}

func TestTxPrimaryLabel_ResolvesInsideTx(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n, err := g.Nodes.Add(context.Background(), []string{"Person", "Customer"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	tx, _ := g.BeginTx()
	defer func() { _ = tx.Rollback() }()

	var got string
	withCoreLockHeld(t, g, "tx.PrimaryLabel", func() { got = tx.PrimaryLabel(n) })
	if got != "Person" {
		t.Fatalf("tx.PrimaryLabel = %q, want Person", got)
	}
}

func TestTxHasLabel_ResolvesInsideTx(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	tx, _ := g.BeginTx()
	defer func() { _ = tx.Rollback() }()

	var has, other bool
	withCoreLockHeld(t, g, "tx.HasLabel", func() { has, other = tx.HasLabel(n, "Person"), tx.HasLabel(n, "Other") })
	if !has {
		t.Errorf("tx.HasLabel(Person) = false, want true")
	}
	if other {
		t.Errorf("tx.HasLabel(Other) = true, want false")
	}
}

func TestTxRelType_ResolvesInsideTx(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)
	ctx := context.Background()

	a, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	r, err := g.Rels.Add(ctx, "KNOWS", a, b, nil)
	if err != nil {
		t.Fatal(err)
	}

	tx, _ := g.BeginTx()
	defer func() { _ = tx.Rollback() }()

	var got string
	var has, other bool
	withCoreLockHeld(t, g, "tx.RelType/HasType", func() {
		got, has, other = tx.RelType(r), tx.HasType(r, "KNOWS"), tx.HasType(r, "OTHER")
	})
	if got != "KNOWS" {
		t.Fatalf("tx.RelType = %q, want KNOWS", got)
	}
	if !has {
		t.Errorf("tx.HasType(KNOWS) = false, want true")
	}
	if other {
		t.Errorf("tx.HasType(OTHER) = true, want false")
	}
}

func TestTxNodeProperty_ResolvesInsideTx(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}

	tx, _ := g.BeginTx()
	defer func() { _ = tx.Rollback() }()

	var v, sv any
	var ok, sok bool
	withCoreLockHeld(t, g, "tx.NodeProperty", func() {
		v, ok = tx.NodeProperty(n, "name")
		sv, sok = tx.NodeProperty(n, types.ShadowLabels)
	})
	if !ok || v != "Alice" {
		t.Errorf("tx.NodeProperty(name) = (%v, %v), want (Alice, true)", v, ok)
	}

	// Shadow key dispatches through nodePropertyUnlocked → nodeLabelsUnlocked.
	if v, ok := sv, sok; !ok {
		t.Errorf("tx.NodeProperty(tkg_labels) = (%v, false), want (..., true)", v)
	} else if labels, _ := v.([]string); len(labels) != 1 || labels[0] != "Person" {
		t.Errorf("tkg_labels = %v, want [Person]", v)
	}
}

func TestTxRelProperty_ResolvesInsideTx(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)
	ctx := context.Background()

	a, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	r, err := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"since": "2024"})
	if err != nil {
		t.Fatal(err)
	}

	tx, _ := g.BeginTx()
	defer func() { _ = tx.Rollback() }()

	var v, sv any
	var ok, sok bool
	withCoreLockHeld(t, g, "tx.RelProperty", func() {
		v, ok = tx.RelProperty(r, "since")
		sv, sok = tx.RelProperty(r, types.ShadowType)
	})
	if !ok || v != "2024" {
		t.Errorf("tx.RelProperty(since) = (%v, %v), want (2024, true)", v, ok)
	}
	if v, ok := sv, sok; !ok || v != "KNOWS" {
		t.Errorf("tx.RelProperty(tkg_type) = (%v, %v), want (KNOWS, true)", v, ok)
	}
}

func TestTxReadAccessors_AfterDoneReturnZero(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)
	ctx := context.Background()

	a, _ := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{"k": "v"})
	b, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	r, _ := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"k": "v"})

	tx, _ := g.BeginTx()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if got := tx.Labels(a); got != nil {
		t.Errorf("Labels after commit = %v, want nil", got)
	}
	if got := tx.PrimaryLabel(a); got != "" {
		t.Errorf("PrimaryLabel after commit = %q, want \"\"", got)
	}
	if tx.HasLabel(a, "Person") {
		t.Errorf("HasLabel after commit = true, want false")
	}
	if got := tx.RelType(r); got != "" {
		t.Errorf("RelType after commit = %q, want \"\"", got)
	}
	if tx.HasType(r, "KNOWS") {
		t.Errorf("HasType after commit = true, want false")
	}
	if v, ok := tx.NodeProperty(a, "k"); ok || v != nil {
		t.Errorf("NodeProperty after commit = (%v, %v), want (nil, false)", v, ok)
	}
	if v, ok := tx.RelProperty(r, "k"); ok || v != nil {
		t.Errorf("RelProperty after commit = (%v, %v), want (nil, false)", v, ok)
	}
}

// TestTxReadAccessors_NonTxAccessorsWorkInsideTx (v4.1.0, Path B): the
// non-tx accessors called from inside an open tx no longer deadlock. The tx
// holds c.txMu (not c.mu.Lock), so a public read's c.mu.RLock (Nodes.Labels,
// Nodes.ByLabel) or readUnderRLock (Temporal.NodesAt) acquires at once. Each
// call runs on another goroutine and must complete while the tx stays open,
// with the committed result. The tx-side mirrors stay preferred for clarity.
func TestTxReadAccessors_NonTxAccessorsWorkInsideTx(t *testing.T) {
	t.Parallel()
	g, aID, _, _, _ := setupTxReadFixture(t)
	a, err := g.Nodes.Get(context.Background(), aID)
	if err != nil {
		t.Fatal(err)
	}
	now := types.Instant(time.Now().UnixMilli() + 1000)

	tx, _ := g.BeginTx()
	defer func() { _ = tx.Rollback() }()

	cases := []struct {
		name string
		read func() (int, error)
		want int
	}{
		{"g.Nodes.Labels", func() (int, error) { return len(g.Nodes.Labels(a)), nil }, 1},
		{"g.Nodes.ByLabel", func() (int, error) {
			nodes, err := g.Nodes.ByLabel("Person", storepkg.QueryOpts{})
			return len(nodes), err
		}, 2},
		{"g.Temporal.NodesAt", func() (int, error) {
			nodes, err := g.Temporal.NodesAt(now)
			return len(nodes), err
		}, 2},
	}
	for _, tc := range cases {
		type result struct {
			n   int
			err error
		}
		done := make(chan result, 1)
		go func() {
			n, err := tc.read()
			done <- result{n, err}
		}()
		select {
		case got := <-done:
			if got.err != nil || got.n != tc.want {
				t.Errorf("%s inside tx = (%d, %v), want (%d, nil)", tc.name, got.n, got.err, tc.want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s deadlocked inside an open tx — Path B regressed", tc.name)
		}
	}
}
