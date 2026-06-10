package core

import (
	"context"
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Door-equivalence tests for relationship creation (lesson 17 / Phase 3 of
// the architecture-review remediation): the batch door must enforce every
// invariant the standalone door enforces, with the same rollback behavior.
// Each test would pass trivially if batch "silently skipped a check" —
// so every case asserts the FAILURE (or rollback) it expects, plus the
// absence of persisted side effects.

func TestBatchRelSelfLoopParityWithStandalone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("default-rejects-both-doors", func(t *testing.T) {
		t.Parallel()
		g, err := New(Config{})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer g.Close()
		n, err := g.Nodes.Add(ctx, []string{"N"}, nil)
		if err != nil {
			t.Fatalf("add node: %v", err)
		}

		if _, err := g.Rels.Add(ctx, "SELF", n, n, nil); !errors.Is(err, ErrSelfLoop) {
			t.Fatalf("standalone self-loop = %v, want ErrSelfLoop", err)
		}
		b, err := NewBatchBuilder(g)
		if err != nil {
			t.Fatalf("NewBatchBuilder: %v", err)
		}
		if _, err := b.AddRelationship("SELF", n, n, nil); !errors.Is(err, ErrSelfLoop) {
			t.Fatalf("batch self-loop = %v, want ErrSelfLoop", err)
		}
	})

	t.Run("allow-self-loops-permits-both-doors", func(t *testing.T) {
		t.Parallel()
		g, err := New(Config{Validation: ValidationLimits{AllowSelfLoops: true}})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer g.Close()
		n, err := g.Nodes.Add(ctx, []string{"N"}, nil)
		if err != nil {
			t.Fatalf("add node: %v", err)
		}

		if _, err := g.Rels.Add(ctx, "SELF", n, n, nil); err != nil {
			t.Fatalf("standalone self-loop with AllowSelfLoops: %v", err)
		}
		b, err := NewBatchBuilder(g)
		if err != nil {
			t.Fatalf("NewBatchBuilder: %v", err)
		}
		if _, err := b.AddRelationship("SELF", n, n, nil); err != nil {
			t.Fatalf("batch self-loop with AllowSelfLoops: %v", err)
		}
		res, err := b.Execute()
		if err != nil {
			t.Fatalf("Execute: %v (result: %+v)", err, res)
		}
		if res.Created != 1 {
			t.Fatalf("Execute Created = %d, want 1", res.Created)
		}
	})
}

// A batch rel whose endpoints vanished between queue and execute must fail,
// must not persist a row, and must roll back the never-before-seen rel-type
// token it allocated in the kernel. A leaked token would silently consume
// the 65535-token namespace on every failed batch.
func TestBatchRelMissingEndpointRollsBackTypeToken(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, err := g.Nodes.Add(ctx, []string{"N"}, nil)
	if err != nil {
		t.Fatalf("add a: %v", err)
	}
	bn, err := g.Nodes.Add(ctx, []string{"N"}, nil)
	if err != nil {
		t.Fatalf("add b: %v", err)
	}

	b, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	queued, err := b.AddRelationship("GhostType", a, bn, nil)
	if err != nil {
		t.Fatalf("queue rel: %v", err)
	}

	// Delete both endpoints AFTER queueing — execute must fail the rel.
	if err := g.Nodes.Delete(ctx, a.ID()); err != nil {
		t.Fatalf("delete a: %v", err)
	}
	if err := g.Nodes.Delete(ctx, bn.ID()); err != nil {
		t.Fatalf("delete b: %v", err)
	}

	res, execErr := b.Execute()
	if execErr == nil {
		t.Fatalf("Execute succeeded over missing endpoints (result %+v); want failure", res)
	}
	if !errors.Is(execErr, ErrBatchFailed) {
		t.Fatalf("Execute error = %v, want ErrBatchFailed", execErr)
	}
	if res == nil || res.Failed == 0 {
		t.Fatalf("Execute result %+v, want Failed > 0", res)
	}

	// No persisted row.
	if _, err := g.Rels.Get(ctx, queued.ID()); !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("rel after failed batch = %v, want ErrRelNotFound", err)
	}
	// No leaked rel-type token: the kernel must have rolled back the
	// allocation made for the failed create.
	if tok, ok := g.relTypes.Lookup("GhostType"); ok {
		t.Fatalf("rel-type %q leaked token %d after rolled-back batch create", "GhostType", tok)
	}
}

// Explicit world-time assertions must behave identically through both doors:
// stamped on the row, honoured by the temporal resolver, and absent (zero)
// when not asserted.
func TestBatchRelValidTimeParityWithStandalone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, _ := g.Nodes.Add(ctx, []string{"N"}, nil)
	bn, _ := g.Nodes.Add(ctx, []string{"N"}, nil)
	if a == nil || bn == nil {
		t.Fatalf("node setup failed")
	}

	props := func() map[string]any {
		return map[string]any{
			"tkg_valid_from": types.Instant(1000),
			"tkg_valid_to":   types.Instant(2000),
		}
	}

	standalone, err := g.Rels.Add(ctx, "VT", a, bn, props())
	if err != nil {
		t.Fatalf("standalone add: %v", err)
	}

	batch, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	queued, err := batch.AddRelationship("VT", a, bn, props())
	if err != nil {
		t.Fatalf("queue rel: %v", err)
	}
	if res, err := batch.Execute(); err != nil {
		t.Fatalf("Execute: %v (result %+v)", err, res)
	}

	for door, id := range map[string]types.RelID{"standalone": standalone.ID(), "batch": queued.ID()} {
		r, err := g.Rels.Get(ctx, id)
		if err != nil {
			t.Fatalf("%s: get: %v", door, err)
		}
		tm := r.Temporal()
		if tm == nil {
			t.Fatalf("%s: no temporal metadata", door)
		}
		if tm.ValidFrom != 1000 || tm.ValidTo != 2000 {
			t.Fatalf("%s: VT = [%d, %d), want [1000, 2000)", door, tm.ValidFrom, tm.ValidTo)
		}
		if tm.TxFrom == 0 {
			t.Fatalf("%s: TxFrom not stamped", door)
		}

		// Resolver honours the assertion: inside the interval resolves,
		// outside does not.
		if _, err := g.Temporal.RelAt(id, 1500); err != nil {
			t.Fatalf("%s: RelAt(1500): %v", door, err)
		}
		if r, err := g.Temporal.RelAt(id, 999); err == nil {
			t.Fatalf("%s: RelAt(999) resolved %v before tkg_valid_from; want no version", door, r.ID())
		}
		if r, err := g.Temporal.RelAt(id, 2000); err == nil {
			t.Fatalf("%s: RelAt(2000) resolved %v at exclusive tkg_valid_to; want no version", door, r.ID())
		}
	}
}

// Integrity parity: a batch-created relationship must carry the same
// integrity shape as a standalone one — verifiable hash chain and endpoint
// hashes that match the endpoints' live integrity hashes.
func TestBatchRelIntegrityParityWithStandalone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, _ := g.Nodes.Add(ctx, []string{"N"}, nil)
	bn, _ := g.Nodes.Add(ctx, []string{"N"}, nil)
	if a == nil || bn == nil {
		t.Fatalf("node setup failed")
	}
	wantFrom := nodeIntegrityHash(a)
	wantTo := nodeIntegrityHash(bn)
	if wantFrom == "" || wantTo == "" {
		t.Fatalf("endpoint integrity hashes empty; test setup broken")
	}

	standalone, err := g.Rels.Add(ctx, "IG", a, bn, map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("standalone add: %v", err)
	}

	batch, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	queued, err := batch.AddRelationship("IG", a, bn, map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("queue rel: %v", err)
	}
	if res, err := batch.Execute(); err != nil {
		t.Fatalf("Execute: %v (result %+v)", err, res)
	}

	for door, id := range map[string]types.RelID{"standalone": standalone.ID(), "batch": queued.ID()} {
		valid, err := g.Hash.VerifyRelChain(id)
		if err != nil || !valid {
			t.Fatalf("%s: VerifyRelChain = (%v, %v), want (true, nil)", door, valid, err)
		}
		r, err := g.Rels.Get(ctx, id)
		if err != nil {
			t.Fatalf("%s: get: %v", door, err)
		}
		ig := r.Integrity()
		if ig == nil || ig.Hash == "" {
			t.Fatalf("%s: missing integrity hash", door)
		}
		if ig.FromNodeHash != wantFrom || ig.ToNodeHash != wantTo {
			t.Fatalf("%s: endpoint hashes (%q, %q) do not match live endpoints (%q, %q)",
				door, ig.FromNodeHash, ig.ToNodeHash, wantFrom, wantTo)
		}
	}
}

// Reserved-prefix properties must be rejected by both doors before anything
// is persisted.
func TestBatchRelReservedPrefixParityWithStandalone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, _ := g.Nodes.Add(ctx, []string{"N"}, nil)
	bn, _ := g.Nodes.Add(ctx, []string{"N"}, nil)
	if a == nil || bn == nil {
		t.Fatalf("node setup failed")
	}
	hostile := map[string]any{"tkg_hash": "forged"}

	if _, err := g.Rels.Add(ctx, "RP", a, bn, hostile); err == nil {
		t.Fatalf("standalone accepted a reserved tkg_ property")
	}
	b, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	if _, err := b.AddRelationship("RP", a, bn, hostile); err == nil {
		t.Fatalf("batch accepted a reserved tkg_ property")
	}
}
