package graph_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Unique constraints through SetNodeVersionInterval (tasks/backlog.md item 12).
//
// Rule under test: a cascade appends rows carrying its props patch. The patch
// is judged as the update doors judge a write:
//
//   - UniqueCurrent binds the CURRENT row. An open-ended cascade (validTo == 0)
//     replaces the current row with base+patch, so a patch value another
//     current node holds is refused, and moving the node off a value frees it.
//     A bounded cascade leaves the current row's value as it was, so it is not
//     checked against UniqueCurrent (history duplicates are legal).
//   - UniqueForever binds every value ever written. Every patch value is
//     checked against the ownership registry (and against the current holders)
//     whatever the interval, and a passing patch claims it.
//
// Every scenario runs on memory, badger, tiered, sharded × the four doors
// (Temporal, GraphTx, BatchBuilder, ingest Session in strong-sync,
// strong-async and concurrent mode) × UniqueCurrent / UniqueForever. Label
// "Ref" is the tiered store's reference label (unique constraints on event
// labels are refused there).
//
// Catches: a door that never consults the constraint (the patch lands and two
// current nodes share a value), a refusal that still appends rows (History
// grows, NodeAtTx at the latest pin moves), a check that judges a bounded
// patch as if it changed the current row (UniqueCurrent refuses a legal
// history duplicate) or that ignores past intervals for UniqueForever, a self
// re-set refused against the node's own index entry or ownership, a value not
// released by a move (UniqueCurrent), and a refusal that leaves a forever claim
// behind for a sibling key.

const ucKey = "k"

type ucDoor struct {
	name string
	// run applies one cascade through the door and returns the op-level error
	// (the batch door asserts ErrBatchFailed at Execute and returns the op error).
	run func(t *testing.T, g *graphpkg.Graph, id types.NodeID, vf, vt types.Instant, props map[string]any) error
}

func ucDoors() []ucDoor {
	doors := []ucDoor{
		{"temporal", func(t *testing.T, g *graphpkg.Graph, id types.NodeID, vf, vt types.Instant, props map[string]any) error {
			_, err := g.Temporal().SetNodeVersionInterval(context.Background(), id, vf, vt, props)
			return err
		}},
		{"tx", func(t *testing.T, g *graphpkg.Graph, id types.NodeID, vf, vt types.Instant, props map[string]any) error {
			tx, err := g.Tx().Begin()
			if err != nil {
				t.Fatalf("Tx.Begin: %v", err)
			}
			_, opErr := tx.SetNodeVersionInterval(id, vf, vt, props)
			// Commit either way: a refused op must have written nothing, so
			// there is nothing a Rollback could hide.
			if err := tx.Commit(); err != nil {
				t.Fatalf("Tx.Commit: %v", err)
			}
			return opErr
		}},
		{"batch", func(t *testing.T, g *graphpkg.Graph, id types.NodeID, vf, vt types.Instant, props map[string]any) error {
			b, err := g.Batch().New()
			if err != nil {
				t.Fatalf("Batch.New: %v", err)
			}
			if err := b.SetNodeVersionInterval(id, vf, vt, props); err != nil {
				t.Fatalf("Batch queue: %v", err)
			}
			res, err := b.Execute()
			if err == nil {
				return nil
			}
			if !errors.Is(err, graphpkg.ErrBatchFailed) {
				t.Fatalf("Batch.Execute err = %v, want ErrBatchFailed", err)
			}
			if res == nil || res.Failed != 1 || len(res.Errors) != 1 || res.Errors[0].Op != "SetNodeVersionInterval" {
				t.Fatalf("Batch result = %+v, want one failed SetNodeVersionInterval op", res)
			}
			return res.Errors[0].Err
		}},
	}
	for _, m := range ivModes {
		m := m
		doors = append(doors, ucDoor{"session-" + m.name, func(t *testing.T, g *graphpkg.Graph, id types.NodeID, vf, vt types.Instant, props map[string]any) error {
			s, err := g.Ingest().NewSession(m.opts)
			if err != nil {
				t.Fatalf("NewSession: %v", err)
			}
			defer s.Close()
			if err := s.SetNodeVersionInterval(id, vf, vt, props); err != nil {
				t.Fatalf("Session queue: %v", err)
			}
			tok, err := s.Submit()
			return ivOutcome(g, tok, err)
		}})
	}
	return doors
}

type ucScope struct {
	name    string
	forever bool
}

var ucScopes = []ucScope{{"current", false}, {"forever", true}}

func ucCreate(t *testing.T, g *graphpkg.Graph, sc ucScope, key string) {
	t.Helper()
	ctx := context.Background()
	var err error
	if sc.forever {
		err = g.Constraints().CreateUniqueForever(ctx, "Ref", key)
	} else {
		err = g.Constraints().CreateUnique(ctx, "Ref", key)
	}
	if err != nil {
		t.Fatalf("Create unique (%s) Ref.%s: %v", sc.name, key, err)
	}
}

// ucAdd creates an open-ended Ref node valid from ivT.
func ucAdd(t *testing.T, g *graphpkg.Graph, props map[string]any) *types.Node {
	t.Helper()
	p := map[string]any{"tkg_valid_from": ivT}
	for k, v := range props {
		p[k] = v
	}
	n, err := g.Nodes().Add(context.Background(), []string{"Ref"}, p)
	if err != nil {
		t.Fatalf("Add %v: %v", props, err)
	}
	return n
}

// ucSnap is the observable state of one node: history length, current value of
// ucKey, and the newest TxFrom on its chain (the "latest" pin).
type ucSnap struct {
	hist int
	cur  any
	pin  types.Instant
}

func ucState(t *testing.T, g *graphpkg.Graph, id types.NodeID) ucSnap {
	t.Helper()
	n, err := g.Nodes().Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get %d: %v", id, err)
	}
	h, err := g.Nodes().History(id)
	if err != nil {
		t.Fatalf("History %d: %v", id, err)
	}
	cur, _ := n.GetProperty(ucKey)
	pin := n.Temporal().TxFrom
	for _, r := range h {
		if r.Temporal().TxFrom > pin {
			pin = r.Temporal().TxFrom
		}
	}
	return ucSnap{hist: len(h), cur: cur, pin: pin}
}

// ucValueAt is ucKey of id valid at validAt as known at txAt.
func ucValueAt(t *testing.T, g *graphpkg.Graph, id types.NodeID, validAt, txAt types.Instant) any {
	t.Helper()
	n, err := g.Temporal().NodeAtTx(id, validAt, txAt)
	if err != nil {
		t.Fatalf("NodeAtTx(%d, %d, %d): %v", id, validAt, txAt, err)
	}
	v, _ := n.GetProperty(ucKey)
	return v
}

func ucHolders(t *testing.T, g *graphpkg.Graph, value any) int {
	t.Helper()
	rows, err := g.Nodes().ByLabelAndProperty("Ref", ucKey, value, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperty(%v): %v", value, err)
	}
	return len(rows)
}

// ucAssertUnchanged asserts a refused cascade appended nothing: same History
// length, same current value, and the latest pin still reads the old value
// inside the refused interval.
func ucAssertUnchanged(t *testing.T, g *graphpkg.Graph, id types.NodeID, before ucSnap, validAt types.Instant) {
	t.Helper()
	after := ucState(t, g, id)
	if after.hist != before.hist {
		t.Errorf("History = %d rows after the refusal, want %d (a refused cascade appends nothing)", after.hist, before.hist)
	}
	if after.cur != before.cur {
		t.Errorf("current %s = %v after the refusal, want %v", ucKey, after.cur, before.cur)
	}
	if after.pin != before.pin {
		t.Errorf("latest TxFrom moved %d -> %d after the refusal", before.pin, after.pin)
	}
	if got := ucValueAt(t, g, id, validAt, after.pin); got != before.cur {
		t.Errorf("NodeAtTx(valid %d, latest pin) = %v, want %v", validAt, got, before.cur)
	}
}

// forUC runs fn for every backend × door × scope.
func forUC(t *testing.T, fn func(t *testing.T, g *graphpkg.Graph, d ucDoor, sc ucScope)) {
	t.Helper()
	for _, b := range allStoreBackends() {
		for _, d := range ucDoors() {
			for _, sc := range ucScopes {
				b, d, sc := b, d, sc
				t.Run(b.name+"/"+d.name+"/"+sc.name, func(t *testing.T) {
					fn(t, b.open(t), d, sc)
				})
			}
		}
	}
}

// An open-ended patch onto a value another current node holds is refused on
// every door in both scopes; nothing is appended, the pin taken before the
// attempt still reads B's value, and the value keeps exactly one holder.
func TestUniqueCascade_OpenEndedOntoHeldValueRefused(t *testing.T) {
	t.Parallel()
	forUC(t, func(t *testing.T, g *graphpkg.Graph, d ucDoor, sc ucScope) {
		ucCreate(t, g, sc, ucKey)
		a := ucAdd(t, g, map[string]any{ucKey: "a"})
		bn := ucAdd(t, g, map[string]any{ucKey: "b"})
		before := ucState(t, g, bn.ID())

		err := d.run(t, g, bn.ID(), ivT+100, 0, map[string]any{ucKey: "a", "other": int64(7)})
		if !errors.Is(err, graphpkg.ErrUniqueViolation) {
			t.Fatalf("cascade onto A's value: err = %v, want ErrUniqueViolation", err)
		}
		ucAssertUnchanged(t, g, bn.ID(), before, ivT+200)
		if got := ucValueAt(t, g, bn.ID(), ivT+200, before.pin); got != "b" {
			t.Errorf("NodeAtTx(B, pin before) = %v, want b", got)
		}
		if n := ucHolders(t, g, "a"); n != 1 {
			t.Errorf("holders of a = %d, want 1", n)
		}
		if st := ucState(t, g, a.ID()); st.cur != "a" {
			t.Errorf("A current = %v, want a", st.cur)
		}
	})
}

// A bounded patch over a past interval leaves B's current row at "b". It is a
// history duplicate under UniqueCurrent (passes, the past slice reads "a",
// the pin before still reads "b") and a second writer of a value owned forever
// under UniqueForever (refused, nothing appended).
func TestUniqueCascade_PastIntervalOntoHeldValue(t *testing.T) {
	t.Parallel()
	forUC(t, func(t *testing.T, g *graphpkg.Graph, d ucDoor, sc ucScope) {
		ucCreate(t, g, sc, ucKey)
		ucAdd(t, g, map[string]any{ucKey: "a"})
		bn := ucAdd(t, g, map[string]any{ucKey: "b"})
		before := ucState(t, g, bn.ID())

		err := d.run(t, g, bn.ID(), ivT+10, ivT+20, map[string]any{ucKey: "a"})
		if sc.forever {
			if !errors.Is(err, graphpkg.ErrUniqueViolation) {
				t.Fatalf("UniqueForever past patch onto an owned value: err = %v, want ErrUniqueViolation", err)
			}
			ucAssertUnchanged(t, g, bn.ID(), before, ivT+15)
			return
		}
		if err != nil {
			t.Fatalf("UniqueCurrent past patch (history duplicate): err = %v, want nil", err)
		}
		after := ucState(t, g, bn.ID())
		if after.cur != "b" {
			t.Errorf("B current = %v after a bounded patch, want b (current row unchanged)", after.cur)
		}
		if got := ucValueAt(t, g, bn.ID(), ivT+15, after.pin); got != "a" {
			t.Errorf("NodeAtTx(B, ivT+15, latest) = %v, want a (the patch landed)", got)
		}
		if got := ucValueAt(t, g, bn.ID(), ivT+15, before.pin); got != "b" {
			t.Errorf("NodeAtTx(B, ivT+15, pin before) = %v, want b", got)
		}
		if got := ucValueAt(t, g, bn.ID(), ivT+50, after.pin); got != "b" {
			t.Errorf("NodeAtTx(B, ivT+50, latest) = %v, want b (resumption)", got)
		}
		if n := ucHolders(t, g, "a"); n != 1 {
			t.Errorf("current holders of a = %d, want 1", n)
		}
	})
}

// A free value passes in both scopes; the pin before still reads the old
// value, and the new value is then held (a create with it is refused).
func TestUniqueCascade_FreeValuePasses(t *testing.T) {
	t.Parallel()
	forUC(t, func(t *testing.T, g *graphpkg.Graph, d ucDoor, sc ucScope) {
		ucCreate(t, g, sc, ucKey)
		ucAdd(t, g, map[string]any{ucKey: "a"})
		bn := ucAdd(t, g, map[string]any{ucKey: "b"})
		before := ucState(t, g, bn.ID())

		if err := d.run(t, g, bn.ID(), ivT+100, 0, map[string]any{ucKey: "c"}); err != nil {
			t.Fatalf("cascade onto a free value: %v", err)
		}
		after := ucState(t, g, bn.ID())
		if after.cur != "c" || after.hist <= before.hist {
			t.Fatalf("B after = %+v, want current c and appended rows (before %+v)", after, before)
		}
		if got := ucValueAt(t, g, bn.ID(), ivT+200, before.pin); got != "b" {
			t.Errorf("NodeAtTx(B, pin before) = %v, want b", got)
		}
		if _, err := g.Nodes().Add(context.Background(), []string{"Ref"}, map[string]any{ucKey: "c"}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
			t.Errorf("Add with B's new value: err = %v, want ErrUniqueViolation", err)
		}
	})
}

// A bounded patch to a free value writes it into the past only: under
// UniqueForever B now owns it (a create is refused), under UniqueCurrent B's
// current row does not hold it (a create passes).
func TestUniqueCascade_PastFreeValueClaimsForeverOnly(t *testing.T) {
	t.Parallel()
	forUC(t, func(t *testing.T, g *graphpkg.Graph, d ucDoor, sc ucScope) {
		ucCreate(t, g, sc, ucKey)
		bn := ucAdd(t, g, map[string]any{ucKey: "b"})
		if err := d.run(t, g, bn.ID(), ivT+10, ivT+20, map[string]any{ucKey: "p"}); err != nil {
			t.Fatalf("past patch to a free value: %v", err)
		}
		_, err := g.Nodes().Add(context.Background(), []string{"Ref"}, map[string]any{ucKey: "p"})
		if sc.forever && !errors.Is(err, graphpkg.ErrUniqueViolation) {
			t.Errorf("UniqueForever: Add with a value B wrote into its past: err = %v, want ErrUniqueViolation", err)
		}
		if !sc.forever && err != nil {
			t.Errorf("UniqueCurrent: Add with a value only in B's past: err = %v, want nil", err)
		}
	})
}

// Re-setting the node's own value passes, open-ended and bounded.
func TestUniqueCascade_SelfResetPasses(t *testing.T) {
	t.Parallel()
	forUC(t, func(t *testing.T, g *graphpkg.Graph, d ucDoor, sc ucScope) {
		ucCreate(t, g, sc, ucKey)
		ucAdd(t, g, map[string]any{ucKey: "a"})
		bn := ucAdd(t, g, map[string]any{ucKey: "b"})
		if err := d.run(t, g, bn.ID(), ivT+100, 0, map[string]any{ucKey: "b", "n": int64(1)}); err != nil {
			t.Fatalf("open-ended self re-set: %v", err)
		}
		if err := d.run(t, g, bn.ID(), ivT+10, ivT+20, map[string]any{ucKey: "b", "n": int64(2)}); err != nil {
			t.Fatalf("bounded self re-set: %v", err)
		}
		if st := ucState(t, g, bn.ID()); st.cur != "b" {
			t.Errorf("B current = %v, want b", st.cur)
		}
		if n := ucHolders(t, g, "b"); n != 1 {
			t.Errorf("holders of b = %d, want 1", n)
		}
	})
}

// Moving a node off a value (open-ended patch to another value, or deleting
// the key) frees it under UniqueCurrent and keeps it owned under
// UniqueForever. A bounded move does not free it (the current row still holds
// it).
func TestUniqueCascade_MoveReleasesForCurrentOnly(t *testing.T) {
	t.Parallel()
	forUC(t, func(t *testing.T, g *graphpkg.Graph, d ucDoor, sc ucScope) {
		ctx := context.Background()
		ucCreate(t, g, sc, ucKey)
		a := ucAdd(t, g, map[string]any{ucKey: "a"})
		x := ucAdd(t, g, map[string]any{ucKey: "x"})
		other := ucAdd(t, g, map[string]any{ucKey: "o"})

		// Bounded move: A's current row still holds "a".
		if err := d.run(t, g, a.ID(), ivT+10, ivT+20, map[string]any{ucKey: "a2"}); err != nil {
			t.Fatalf("bounded move: %v", err)
		}
		if _, err := g.Nodes().Update(ctx, other.ID(), map[string]any{ucKey: "a"}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
			t.Errorf("after a bounded move, Update onto a: err = %v, want ErrUniqueViolation (still current)", err)
		}

		// Open-ended move of A to "a3", and key deletion on X.
		if err := d.run(t, g, a.ID(), ivT+100, 0, map[string]any{ucKey: "a3"}); err != nil {
			t.Fatalf("open-ended move: %v", err)
		}
		if err := d.run(t, g, x.ID(), ivT+100, 0, map[string]any{ucKey: nil}); err != nil {
			t.Fatalf("open-ended key delete: %v", err)
		}
		if st := ucState(t, g, x.ID()); st.cur != nil {
			t.Fatalf("X current %s = %v, want deleted", ucKey, st.cur)
		}
		for _, v := range []string{"a", "x"} {
			_, err := g.Nodes().Update(ctx, other.ID(), map[string]any{ucKey: v})
			if sc.forever && !errors.Is(err, graphpkg.ErrUniqueViolation) {
				t.Errorf("UniqueForever: Update onto released %q: err = %v, want ErrUniqueViolation (owned forever)", v, err)
			}
			if !sc.forever && err != nil {
				t.Errorf("UniqueCurrent: Update onto released %q: err = %v, want nil (freed)", v, err)
			}
		}
	})
}

// Nil props and a patch that does not name the constrained key pass, even
// when another node holds B's value's neighbours; nothing about the
// constrained value changes.
func TestUniqueCascade_NilPropsAndKeyAbsentPass(t *testing.T) {
	t.Parallel()
	forUC(t, func(t *testing.T, g *graphpkg.Graph, d ucDoor, sc ucScope) {
		ucCreate(t, g, sc, ucKey)
		ucAdd(t, g, map[string]any{ucKey: "a"})
		bn := ucAdd(t, g, map[string]any{ucKey: "b"})
		before := ucState(t, g, bn.ID())
		if err := d.run(t, g, bn.ID(), ivT+100, 0, nil); err != nil {
			t.Fatalf("nil props: %v", err)
		}
		if err := d.run(t, g, bn.ID(), ivT+10, ivT+20, map[string]any{"other": int64(1)}); err != nil {
			t.Fatalf("constrained key absent: %v", err)
		}
		if err := d.run(t, g, bn.ID(), ivT+300, 0, map[string]any{"other": int64(2)}); err != nil {
			t.Fatalf("constrained key absent, open-ended: %v", err)
		}
		after := ucState(t, g, bn.ID())
		if after.cur != "b" || after.hist <= before.hist {
			t.Errorf("B after = %+v, want current b and appended rows (before %+v)", after, before)
		}
	})
}

// A float patch on a constrained key is refused like a float update.
func TestUniqueCascade_FloatRefused(t *testing.T) {
	t.Parallel()
	forUC(t, func(t *testing.T, g *graphpkg.Graph, d ucDoor, sc ucScope) {
		ucCreate(t, g, sc, ucKey)
		bn := ucAdd(t, g, map[string]any{ucKey: "b"})
		before := ucState(t, g, bn.ID())
		if err := d.run(t, g, bn.ID(), ivT+100, 0, map[string]any{ucKey: 1.5}); !errors.Is(err, graphpkg.ErrUniqueUnsupportedType) {
			t.Fatalf("float patch: err = %v, want ErrUniqueUnsupportedType", err)
		}
		ucAssertUnchanged(t, g, bn.ID(), before, ivT+200)
	})
}

// A patch over two constrained keys where one value is taken and the other is
// free is refused as a whole, and the free value is NOT left claimed (a later
// create with it passes) — the BACKLOG 9e shape.
func TestUniqueCascade_RefusalClaimsNothing(t *testing.T) {
	t.Parallel()
	forUC(t, func(t *testing.T, g *graphpkg.Graph, d ucDoor, sc ucScope) {
		ucCreate(t, g, sc, ucKey)
		ucCreate(t, g, sc, "h")
		ucAdd(t, g, map[string]any{ucKey: "a", "h": "ha"})
		bn := ucAdd(t, g, map[string]any{ucKey: "b", "h": "hb"})
		before := ucState(t, g, bn.ID())
		err := d.run(t, g, bn.ID(), ivT+100, 0, map[string]any{ucKey: "free", "h": "ha"})
		if !errors.Is(err, graphpkg.ErrUniqueViolation) {
			t.Fatalf("patch with one taken value: err = %v, want ErrUniqueViolation", err)
		}
		ucAssertUnchanged(t, g, bn.ID(), before, ivT+200)
		if _, err := g.Nodes().Add(context.Background(), []string{"Ref"}, map[string]any{ucKey: "free"}); err != nil {
			t.Errorf("Add with the free half of a refused patch: %v (a refused cascade must claim nothing)", err)
		}
	})
}

// A constraint requested over existing current duplicates is not installed
// (ErrUniqueViolationExisting, CreateUnique's behaviour, unchanged), so a
// cascade then writes freely.
func TestUniqueCascade_ConstraintAfterDuplicate(t *testing.T) {
	t.Parallel()
	for _, b := range allStoreBackends() {
		for _, d := range ucDoors() {
			b, d := b, d
			t.Run(b.name+"/"+d.name, func(t *testing.T) {
				g := b.open(t)
				ctx := context.Background()
				ucAdd(t, g, map[string]any{ucKey: "dup"})
				bn := ucAdd(t, g, map[string]any{ucKey: "dup"})
				if err := g.Constraints().CreateUnique(ctx, "Ref", ucKey); !errors.Is(err, graphpkg.ErrUniqueViolationExisting) {
					t.Fatalf("CreateUnique over duplicates: err = %v, want ErrUniqueViolationExisting", err)
				}
				if err := g.Constraints().CreateUniqueForever(ctx, "Ref", ucKey); !errors.Is(err, graphpkg.ErrUniqueViolationExisting) {
					t.Fatalf("CreateUniqueForever over duplicates: err = %v, want ErrUniqueViolationExisting", err)
				}
				if got := g.Constraints().UniqueConstraints(); len(got) != 0 {
					t.Fatalf("UniqueConstraints = %v, want none installed", got)
				}
				ucAdd(t, g, map[string]any{ucKey: "a"})
				if err := d.run(t, g, bn.ID(), ivT+100, 0, map[string]any{ucKey: "a"}); err != nil {
					t.Fatalf("cascade without an installed constraint: %v", err)
				}
				if n := ucHolders(t, g, "a"); n != 2 {
					t.Errorf("holders of a = %d, want 2 (no constraint)", n)
				}
			})
		}
	}
}

// Batch and Session group semantics mirror an update violation: the refused
// cascade fails its op (the group outcome carries ErrUniqueViolation), the
// sibling op in the same group commits, and the refused node is unchanged.
func TestUniqueCascade_GroupSemantics(t *testing.T) {
	t.Parallel()
	type groupDoor struct {
		name string
		run  func(t *testing.T, g *graphpkg.Graph, bad, good types.NodeID) error
	}
	doors := []groupDoor{{"batch", func(t *testing.T, g *graphpkg.Graph, bad, good types.NodeID) error {
		b, err := g.Batch().New()
		if err != nil {
			t.Fatalf("Batch.New: %v", err)
		}
		if err := b.SetNodeVersionInterval(bad, ivT+100, 0, map[string]any{ucKey: "a"}); err != nil {
			t.Fatalf("queue bad: %v", err)
		}
		if err := b.SetNodeVersionInterval(good, ivT+100, 0, map[string]any{ucKey: "c2"}); err != nil {
			t.Fatalf("queue good: %v", err)
		}
		res, err := b.Execute()
		if !errors.Is(err, graphpkg.ErrBatchFailed) {
			t.Fatalf("Execute err = %v, want ErrBatchFailed", err)
		}
		if res.Failed != 1 || res.Updated != 1 || len(res.Errors) != 1 || res.Errors[0].ID != types.EntityID(bad) {
			t.Fatalf("result = %+v, want 1 failed (the bad id) + 1 updated", res)
		}
		return res.Errors[0].Err
	}}}
	for _, m := range ivModes {
		m := m
		doors = append(doors, groupDoor{"session-" + m.name, func(t *testing.T, g *graphpkg.Graph, bad, good types.NodeID) error {
			s, err := g.Ingest().NewSession(m.opts)
			if err != nil {
				t.Fatalf("NewSession: %v", err)
			}
			defer s.Close()
			if err := s.SetNodeVersionInterval(bad, ivT+100, 0, map[string]any{ucKey: "a"}); err != nil {
				t.Fatalf("queue bad: %v", err)
			}
			if err := s.SetNodeVersionInterval(good, ivT+100, 0, map[string]any{ucKey: "c2"}); err != nil {
				t.Fatalf("queue good: %v", err)
			}
			tok, err := s.Submit()
			return ivOutcome(g, tok, err)
		}})
	}
	for _, b := range allStoreBackends() {
		for _, d := range doors {
			for _, sc := range ucScopes {
				b, d, sc := b, d, sc
				t.Run(b.name+"/"+d.name+"/"+sc.name, func(t *testing.T) {
					g := b.open(t)
					ucCreate(t, g, sc, ucKey)
					ucAdd(t, g, map[string]any{ucKey: "a"})
					bad := ucAdd(t, g, map[string]any{ucKey: "b"})
					good := ucAdd(t, g, map[string]any{ucKey: "c"})
					beforeBad := ucState(t, g, bad.ID())
					if err := d.run(t, g, bad.ID(), good.ID()); !errors.Is(err, graphpkg.ErrUniqueViolation) {
						t.Fatalf("group outcome = %v, want ErrUniqueViolation", err)
					}
					ucAssertUnchanged(t, g, bad.ID(), beforeBad, ivT+200)
					if st := ucState(t, g, good.ID()); st.cur != "c2" {
						t.Errorf("sibling in the same group: current = %v, want c2 (partial success)", st.cur)
					}
				})
			}
		}
	}
}

// Two writers patch two nodes onto the same value at once: exactly one wins,
// the value has one current holder (UniqueCurrent, open-ended) or one owner
// (UniqueForever, bounded past patches — the registry arbitrates).
func TestUniqueCascade_ConcurrentOneWinner(t *testing.T) {
	t.Parallel()
	for _, b := range allStoreBackends() {
		for _, d := range ucDoors() {
			for _, sc := range ucScopes {
				b, d, sc := b, d, sc
				t.Run(b.name+"/"+d.name+"/"+sc.name, func(t *testing.T) {
					g := b.open(t)
					ucCreate(t, g, sc, ucKey)
					const rounds = 8
					for r := 0; r < rounds; r++ {
						n1 := ucAdd(t, g, map[string]any{ucKey: fmt.Sprintf("x%d", r)})
						n2 := ucAdd(t, g, map[string]any{ucKey: fmt.Sprintf("y%d", r)})
						v := fmt.Sprintf("same%d", r)
						vf, vt := ivT+100, types.Instant(0)
						if sc.forever {
							vf, vt = ivT+10, ivT+20
						}
						var wg sync.WaitGroup
						errs := make([]error, 2)
						start := make(chan struct{})
						for i, id := range []types.NodeID{n1.ID(), n2.ID()} {
							wg.Add(1)
							go func(i int, id types.NodeID) {
								defer wg.Done()
								<-start
								errs[i] = d.run(t, g, id, vf, vt, map[string]any{ucKey: v})
							}(i, id)
						}
						close(start)
						wg.Wait()
						wins, losses := 0, 0
						for _, err := range errs {
							switch {
							case err == nil:
								wins++
							case errors.Is(err, graphpkg.ErrUniqueViolation):
								losses++
							default:
								t.Fatalf("round %d: unexpected err %v", r, err)
							}
						}
						if wins != 1 || losses != 1 {
							t.Fatalf("round %d: wins=%d losses=%d (errs %v), want exactly one winner", r, wins, losses, errs)
						}
						if !sc.forever {
							if n := ucHolders(t, g, v); n != 1 {
								t.Fatalf("round %d: current holders of %s = %d, want 1", r, v, n)
							}
						}
					}
				})
			}
		}
	}
}
