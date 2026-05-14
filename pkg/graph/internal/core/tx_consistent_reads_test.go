package core

import (
	"context"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// Tx-side resolution and shadow-property mirror tests.
//
// These exist because g.Nodes.Labels / HasLabel / PrimaryLabel,
// g.Rels.Type / HasType, and g.Resolve.NodeProperty / RelProperty all
// acquire c.mu.RLock and deadlock when invoked inside an open *GraphTx
// (BeginTx holds c.mu.Lock; sync.RWMutex is not reentrant — lesson 9).
// (*GraphTx).Labels et al. call the *Unlocked helpers directly and
// inherit c.mu.Lock from the tx.

func TestTxLabels_ResolvesInsideTx(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n, err := g.Nodes.Add(context.Background(), []string{"Person", "Customer"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	tx, _ := g.BeginTx()
	defer func() { _ = tx.Rollback() }()

	got := tx.Labels(n)
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

	if got := tx.PrimaryLabel(n); got != "Person" {
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

	if !tx.HasLabel(n, "Person") {
		t.Errorf("tx.HasLabel(Person) = false, want true")
	}
	if tx.HasLabel(n, "Other") {
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

	if got := tx.RelType(r); got != "KNOWS" {
		t.Fatalf("tx.RelType = %q, want KNOWS", got)
	}
	if !tx.HasType(r, "KNOWS") {
		t.Errorf("tx.HasType(KNOWS) = false, want true")
	}
	if tx.HasType(r, "OTHER") {
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

	if v, ok := tx.NodeProperty(n, "name"); !ok || v != "Alice" {
		t.Errorf("tx.NodeProperty(name) = (%v, %v), want (Alice, true)", v, ok)
	}

	// Shadow key dispatches through nodePropertyUnlocked → nodeLabelsUnlocked.
	if v, ok := tx.NodeProperty(n, types.ShadowLabels); !ok {
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

	if v, ok := tx.RelProperty(r, "since"); !ok || v != "2024" {
		t.Errorf("tx.RelProperty(since) = (%v, %v), want (2024, true)", v, ok)
	}
	if v, ok := tx.RelProperty(r, types.ShadowType); !ok || v != "KNOWS" {
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
// upstream-bug shape — calling the non-tx accessor `g.Nodes.Labels(n)` from
// inside an open tx — no longer deadlocks. The tx holds c.txMu (not
// c.mu.Lock), so the public read accessor's c.mu.RLock acquires immediately.
// This test pins the new behavior: a concurrent goroutine's call must
// complete promptly while the tx remains open. The tx-side mirrors
// (tx.Labels et al.) are still preferred for clarity and remain functional;
// they're no longer required for correctness.
func TestTxReadAccessors_NonTxAccessorsWorkInsideTx(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	tx, _ := g.BeginTx()
	defer func() { _ = tx.Rollback() }()

	done := make(chan []string)
	go func() {
		done <- g.Nodes.Labels(n)
	}()

	select {
	case got := <-done:
		if len(got) != 1 || got[0] != "Person" {
			t.Errorf("g.Nodes.Labels = %v, want [Person]", got)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("g.Nodes.Labels deadlocked inside an open tx — Path B regressed")
	}
}
