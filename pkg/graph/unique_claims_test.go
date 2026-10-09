package graph_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// UniqueForever claims of the node door family (tasks/backlog.md item 29).
//
// Rule under test: a claiming door claims a UniqueForever value for the node
// under the value stripe BEFORE its store write. When the call then fails —
// the store write fails, or a later claim of the same call fails to persist —
// every claim the call made is withdrawn unless a stored row of the node
// carries the value; a value the node owned before the call is never
// withdrawn. The stripe stays held until the withdrawal is done.
//
// Every scenario runs on memory, badger, tiered, sharded (each wrapped in a
// fault-injecting decorator, unique_claims_fault_test.go) × every door that
// claims: Add, AddWithTx, Import, AddByIDIfAbsent, GetOrCreateByKey, Update,
// UpdateWithTx, UpdateInPlace, CompareAndSetProperty, AddLabel, the GraphTx
// twins, BatchBuilder AddNode/AddNodes/UpdateNode/UpdateNodeWithTx and the
// ingest Session AddNode/AddNodes/UpdateNode/UpdateNodeWithTx (strong-sync,
// strong-async, concurrent).
//
// Catches: a door that keeps the claim of a write that never landed (a later
// Add of the value is refused as "permanently owned"), a failed second claim
// that leaves the first one owned, a withdrawal of a value a stored row
// carries or of a value the node owned before the call, and a withdrawal made
// after the stripe was released (a concurrent writer of the value is refused
// although the failed writer never held it).

const ucKey2 = "m"

type claimKind int

const (
	claimCreate claimKind = iota // the door creates a node carrying props
	claimUpdate                  // the door changes target's props
	claimLabel                   // the door adds label Ref to target (a Plain node carrying the values)
)

type claimDoor struct {
	name string
	kind claimKind
	// oneKey: the door changes one property per call (CompareAndSetProperty).
	oneKey bool
	// group: the door is a Batch / ingest Session door (deferred apply).
	group bool
	run   func(g *graphpkg.Graph, target types.NodeID, props map[string]any) error
}

var errClaimSetup = errors.New("claim door setup")

// claimNow is an instant after every TxFrom / TxTo on id's chain (the
// caller-instant doors require one) and not in the future: the graph's clock
// floor may stamp ahead of the wall clock, so it waits for the wall clock.
func claimNow(g *graphpkg.Graph, id types.NodeID) types.Instant {
	var last types.Instant
	if n, err := g.Nodes().Get(context.Background(), id); err == nil && n.Temporal() != nil {
		last = max(n.Temporal().TxFrom, n.Temporal().TxTo)
	}
	if h, err := g.Nodes().History(id); err == nil {
		for _, r := range h {
			if tm := r.Temporal(); tm != nil {
				last = max(last, tm.TxFrom, tm.TxTo)
			}
		}
	}
	for {
		if now := types.InstantFromTime(time.Now()); now > last {
			return now
		}
		time.Sleep(time.Millisecond)
	}
}

func claimBatchOutcome(res *graphpkg.BatchResult, err error) error {
	if err == nil {
		return nil
	}
	if !errors.Is(err, graphpkg.ErrBatchFailed) {
		return fmt.Errorf("%w: Execute err = %v, want ErrBatchFailed", errClaimSetup, err)
	}
	if res == nil || len(res.Errors) == 0 {
		return err // a whole-unit refusal before any op ran
	}
	errs := make([]error, 0, len(res.Errors))
	for _, e := range res.Errors {
		errs = append(errs, e.Err)
	}
	return errors.Join(errs...)
}

func claimTx(g *graphpkg.Graph, op func(tx *graphpkg.GraphTx) error) error {
	tx, err := g.Tx().Begin()
	if err != nil {
		return fmt.Errorf("%w: Tx.Begin: %v", errClaimSetup, err)
	}
	opErr := op(tx)
	// Commit either way: a failed op must have written nothing, so there is
	// nothing a Rollback could hide.
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%w: Tx.Commit: %v (op err %v)", errClaimSetup, err, opErr)
	}
	return opErr
}

func claimBatch(g *graphpkg.Graph, queue func(b *graphpkg.BatchBuilder) error) error {
	b, err := g.Batch().New()
	if err != nil {
		return fmt.Errorf("%w: Batch.New: %v", errClaimSetup, err)
	}
	if err := queue(b); err != nil {
		return fmt.Errorf("%w: Batch queue: %v", errClaimSetup, err)
	}
	return claimBatchOutcome(b.Execute())
}

func claimSession(g *graphpkg.Graph, m ivMode, queue func(s *ingest.Session) error) error {
	s, err := g.Ingest().NewSession(m.opts)
	if err != nil {
		return fmt.Errorf("%w: NewSession: %v", errClaimSetup, err)
	}
	defer s.Close()
	if err := queue(s); err != nil {
		return fmt.Errorf("%w: Session queue: %v", errClaimSetup, err)
	}
	tok, err := s.Submit()
	return ivOutcome(g, tok, err)
}

func withoutKey(props map[string]any, key string) map[string]any {
	out := make(map[string]any, len(props))
	for k, v := range props {
		if k != key {
			out[k] = v
		}
	}
	return out
}

func claimDoors() []claimDoor {
	ctx := context.Background()
	ref := []string{"Ref"}
	doors := []claimDoor{
		{name: "Add", kind: claimCreate, run: func(g *graphpkg.Graph, _ types.NodeID, p map[string]any) error {
			_, err := g.Nodes().Add(ctx, ref, p)
			return err
		}},
		{name: "AddWithTx", kind: claimCreate, run: func(g *graphpkg.Graph, _ types.NodeID, p map[string]any) error {
			_, err := g.Nodes().AddWithTx(ctx, ref, p, types.InstantFromTime(time.Now().Add(-time.Hour)))
			return err
		}},
		{name: "Import", kind: claimCreate, run: func(g *graphpkg.Graph, _ types.NodeID, p map[string]any) error {
			_, err := g.Nodes().Import(ctx, g.Nodes().NextID(), ref, p)
			return err
		}},
		{name: "AddByIDIfAbsent", kind: claimCreate, run: func(g *graphpkg.Graph, _ types.NodeID, p map[string]any) error {
			_, _, err := g.Nodes().AddByIDIfAbsent(ctx, g.Nodes().NextID(), ref, p)
			return err
		}},
		{name: "GetOrCreateByKey", kind: claimCreate, run: func(g *graphpkg.Graph, _ types.NodeID, p map[string]any) error {
			_, _, err := g.Nodes().GetOrCreateByKey(ctx, "Ref", ucKey, p[ucKey], withoutKey(p, ucKey))
			return err
		}},
		{name: "Update", kind: claimUpdate, run: func(g *graphpkg.Graph, id types.NodeID, p map[string]any) error {
			_, err := g.Nodes().Update(ctx, id, p)
			return err
		}},
		{name: "UpdateWithTx", kind: claimUpdate, run: func(g *graphpkg.Graph, id types.NodeID, p map[string]any) error {
			_, err := g.Nodes().UpdateWithTx(ctx, id, p, claimNow(g, id))
			return err
		}},
		{name: "UpdateInPlace", kind: claimUpdate, run: func(g *graphpkg.Graph, id types.NodeID, p map[string]any) error {
			_, err := g.Nodes().UpdateInPlace(ctx, id, p)
			return err
		}},
		{name: "CompareAndSetProperty", kind: claimUpdate, oneKey: true, run: func(g *graphpkg.Graph, id types.NodeID, p map[string]any) error {
			if len(p) != 1 {
				return fmt.Errorf("%w: CAS takes one key, got %v", errClaimSetup, p)
			}
			for k, v := range p {
				cur, err := g.Nodes().Get(ctx, id)
				if err != nil {
					return fmt.Errorf("%w: Get: %v", errClaimSetup, err)
				}
				old, _ := cur.GetProperty(k)
				ok, err := g.Nodes().CompareAndSetProperty(ctx, id, k, old, v)
				if err == nil && !ok {
					return fmt.Errorf("%w: CAS mismatch", errClaimSetup)
				}
				return err
			}
			return nil
		}},
		{name: "AddLabel", kind: claimLabel, run: func(g *graphpkg.Graph, id types.NodeID, _ map[string]any) error {
			return g.Nodes().AddLabel(ctx, id, "Ref")
		}},
		{name: "tx-AddNode", kind: claimCreate, run: func(g *graphpkg.Graph, _ types.NodeID, p map[string]any) error {
			return claimTx(g, func(tx *graphpkg.GraphTx) error { _, err := tx.AddNode(ref, p); return err })
		}},
		{name: "tx-ImportNodeWithID", kind: claimCreate, run: func(g *graphpkg.Graph, _ types.NodeID, p map[string]any) error {
			id := g.Nodes().NextID()
			return claimTx(g, func(tx *graphpkg.GraphTx) error { _, err := tx.ImportNodeWithID(ctx, id, ref, p); return err })
		}},
		{name: "tx-GetOrCreateByKey", kind: claimCreate, run: func(g *graphpkg.Graph, _ types.NodeID, p map[string]any) error {
			return claimTx(g, func(tx *graphpkg.GraphTx) error {
				_, _, err := tx.GetOrCreateByKey("Ref", ucKey, p[ucKey], withoutKey(p, ucKey))
				return err
			})
		}},
		{name: "tx-UpdateNode", kind: claimUpdate, run: func(g *graphpkg.Graph, id types.NodeID, p map[string]any) error {
			return claimTx(g, func(tx *graphpkg.GraphTx) error { _, err := tx.UpdateNode(id, p); return err })
		}},
		{name: "tx-UpdateNodeWithTx", kind: claimUpdate, run: func(g *graphpkg.Graph, id types.NodeID, p map[string]any) error {
			at := claimNow(g, id)
			return claimTx(g, func(tx *graphpkg.GraphTx) error { _, err := tx.UpdateNodeWithTx(id, p, at); return err })
		}},
		{name: "tx-AddNodeLabel", kind: claimLabel, run: func(g *graphpkg.Graph, id types.NodeID, _ map[string]any) error {
			return claimTx(g, func(tx *graphpkg.GraphTx) error { return tx.AddNodeLabel(id, "Ref") })
		}},
		{name: "batch-AddNode", kind: claimCreate, group: true, run: func(g *graphpkg.Graph, _ types.NodeID, p map[string]any) error {
			return claimBatch(g, func(b *graphpkg.BatchBuilder) error { _, err := b.AddNode(ref, p); return err })
		}},
		{name: "batch-AddNodes", kind: claimCreate, group: true, run: func(g *graphpkg.Graph, _ types.NodeID, p map[string]any) error {
			return claimBatch(g, func(b *graphpkg.BatchBuilder) error { return b.AddNodes(ref, p, 1) })
		}},
		{name: "batch-UpdateNode", kind: claimUpdate, group: true, run: func(g *graphpkg.Graph, id types.NodeID, p map[string]any) error {
			return claimBatch(g, func(b *graphpkg.BatchBuilder) error { return b.UpdateNode(id, p) })
		}},
		{name: "batch-UpdateNodeWithTx", kind: claimUpdate, group: true, run: func(g *graphpkg.Graph, id types.NodeID, p map[string]any) error {
			at := claimNow(g, id)
			return claimBatch(g, func(b *graphpkg.BatchBuilder) error { return b.UpdateNodeWithTx(id, p, at) })
		}},
	}
	for _, m := range ivModes {
		m := m
		doors = append(doors,
			claimDoor{name: "session-" + m.name + "-AddNode", kind: claimCreate, group: true, run: func(g *graphpkg.Graph, _ types.NodeID, p map[string]any) error {
				return claimSession(g, m, func(s *ingest.Session) error { _, err := s.AddNode(ref, p); return err })
			}},
			claimDoor{name: "session-" + m.name + "-AddNodes", kind: claimCreate, group: true, run: func(g *graphpkg.Graph, _ types.NodeID, p map[string]any) error {
				return claimSession(g, m, func(s *ingest.Session) error { return s.AddNodes(ref, p, 1) })
			}},
			claimDoor{name: "session-" + m.name + "-UpdateNode", kind: claimUpdate, group: true, run: func(g *graphpkg.Graph, id types.NodeID, p map[string]any) error {
				return claimSession(g, m, func(s *ingest.Session) error { return s.UpdateNode(id, p) })
			}},
			claimDoor{name: "session-" + m.name + "-UpdateNodeWithTx", kind: claimUpdate, group: true, run: func(g *graphpkg.Graph, id types.NodeID, p map[string]any) error {
				at := claimNow(g, id)
				return claimSession(g, m, func(s *ingest.Session) error { return s.UpdateNodeWithTx(id, p, at) })
			}},
		)
	}
	return doors
}

// claimCase is one door call: target (0 for a create door), the props the
// call writes, and the values the target held before it (update doors).
type claimCase struct {
	target types.NodeID
	props  map[string]any
	newVal []claimKV // values the call introduces
	oldVal []claimKV // values the target held before the call (update doors)
}

type claimKV struct {
	key string
	val string
}

// claimSetup prepares one call of door d introducing the values tag-k and
// tag-m (only tag-k for a one-key door).
func claimSetup(t *testing.T, g *graphpkg.Graph, d claimDoor, tag string) claimCase {
	t.Helper()
	ctx := context.Background()
	cs := claimCase{newVal: []claimKV{{ucKey, tag + "-k"}}}
	if !d.oneKey {
		cs.newVal = append(cs.newVal, claimKV{ucKey2, tag + "-m"})
	}
	cs.props = make(map[string]any, len(cs.newVal))
	for _, kv := range cs.newVal {
		cs.props[kv.key] = kv.val
	}
	switch d.kind {
	case claimUpdate:
		cs.oldVal = []claimKV{{ucKey, tag + "-k0"}, {ucKey2, tag + "-m0"}}
		n, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{ucKey: tag + "-k0", ucKey2: tag + "-m0"})
		if err != nil {
			t.Fatalf("setup Add: %v", err)
		}
		cs.target = n.ID()
	case claimLabel:
		n, err := g.Nodes().Add(ctx, []string{"Plain"}, cs.props)
		if err != nil {
			t.Fatalf("setup Add Plain: %v", err)
		}
		cs.target = n.ID()
	}
	return cs
}

func claimConstrain(t *testing.T, g *graphpkg.Graph, forever bool) {
	t.Helper()
	for _, k := range []string{ucKey, ucKey2} {
		ucCreate(t, g, ucScope{name: map[bool]string{true: "forever", false: "current"}[forever], forever: forever}, k)
	}
}

// claimProbe creates a fresh Ref node holding key=val and reports the error.
func claimProbe(g *graphpkg.Graph, key, val string) error {
	_, err := g.Nodes().Add(context.Background(), []string{"Ref"}, map[string]any{key: val})
	return err
}

func claimHolders(t *testing.T, g *graphpkg.Graph, key, val string) int {
	t.Helper()
	rows, err := g.Nodes().ByLabelAndProperty("Ref", key, val, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperty(%s=%s): %v", key, val, err)
	}
	return len(rows)
}

type claimSnap struct {
	exists  bool
	version uint32
	hist    int
	props   map[string]any
	hasRef  bool
}

func claimState(t *testing.T, g *graphpkg.Graph, id types.NodeID) claimSnap {
	t.Helper()
	if id == 0 {
		return claimSnap{}
	}
	ctx := context.Background()
	n, err := g.Nodes().Get(ctx, id)
	if err != nil {
		t.Fatalf("Get %d: %v", id, err)
	}
	h, err := g.Nodes().History(id)
	if err != nil {
		t.Fatalf("History %d: %v", id, err)
	}
	s := claimSnap{exists: true, version: n.Version(), hist: len(h), props: map[string]any{}, hasRef: g.Nodes().HasLabel(n, "Ref")}
	for _, k := range []string{ucKey, ucKey2} {
		v, _ := n.GetProperty(k)
		s.props[k] = v
	}
	return s
}

// claimAssertUnchanged asserts a failed call changed nothing: an update or
// label target keeps version, history, values and labels; a create left no
// Ref node holding any value it introduced.
func claimAssertUnchanged(t *testing.T, g *graphpkg.Graph, cs claimCase, before claimSnap) {
	t.Helper()
	if cs.target != 0 {
		after := claimState(t, g, cs.target)
		if after.version != before.version || after.hist != before.hist || after.hasRef != before.hasRef ||
			after.props[ucKey] != before.props[ucKey] || after.props[ucKey2] != before.props[ucKey2] {
			t.Fatalf("target changed by a failed call: before %+v, after %+v", before, after)
		}
		if cs.oldVal == nil { // label door: the values sit on a Plain node only
			for _, kv := range cs.newVal {
				if n := claimHolders(t, g, kv.key, kv.val); n != 0 {
					t.Fatalf("Ref holders of %s=%s = %d after a failed AddLabel, want 0", kv.key, kv.val, n)
				}
			}
		}
		return
	}
	for _, kv := range cs.newVal {
		if n := claimHolders(t, g, kv.key, kv.val); n != 0 {
			t.Fatalf("holders of %s=%s = %d after a failed create, want 0", kv.key, kv.val, n)
		}
	}
}

// claimAssertFree asserts every value the failed call introduced is free: a
// fresh node may take it.
func claimAssertFree(t *testing.T, g *graphpkg.Graph, vals []claimKV, why string) {
	t.Helper()
	for _, kv := range vals {
		if err := claimProbe(g, kv.key, kv.val); err != nil {
			t.Errorf("Add %s=%s after %s: %v (the call's claim must be withdrawn)", kv.key, kv.val, why, err)
		}
	}
}

// claimAssertHeld asserts a fresh node may NOT take any of vals.
func claimAssertHeld(t *testing.T, g *graphpkg.Graph, vals []claimKV, why string) {
	t.Helper()
	for _, kv := range vals {
		if err := claimProbe(g, kv.key, kv.val); !errors.Is(err, graphpkg.ErrUniqueViolation) {
			t.Errorf("Add %s=%s after %s: err = %v, want ErrUniqueViolation", kv.key, kv.val, why, err)
		}
	}
}

func forClaims(t *testing.T, scopes []bool, fn func(t *testing.T, g *graphpkg.Graph, f *claimFault, d claimDoor, forever bool)) {
	t.Helper()
	forClaimDoors(t, scopes, nil, fn)
}

// forClaimDoors is forClaims over the doors want accepts (nil: all).
func forClaimDoors(t *testing.T, scopes []bool, want func(claimDoor) bool, fn func(t *testing.T, g *graphpkg.Graph, f *claimFault, d claimDoor, forever bool)) {
	t.Helper()
	for _, b := range claimBackends() {
		for _, d := range claimDoors() {
			if want != nil && !want(d) {
				continue
			}
			for _, forever := range scopes {
				b, d, forever := b, d, forever
				sc := "current"
				if forever {
					sc = "forever"
				}
				t.Run(b.name+"/"+d.name+"/"+sc, func(t *testing.T) {
					t.Parallel()
					g, f := b.open(t)
					fn(t, g, f, d, forever)
				})
			}
		}
	}
}

// (a) RED before the fix (forever): the store write of a claiming call fails
// before anything is stored. The entity is unchanged, every value the call
// introduced is free again, the values the target owned before the call stay
// owned (d), and — the counterpart — the same door with no fault succeeds and
// keeps its claim (c). UniqueCurrent subtests are GUARDS (no claim map: they
// pass before the fix too).
func TestUniqueClaims_StoreWriteFailureWithdrawsClaim(t *testing.T) {
	t.Parallel()
	forClaims(t, []bool{true, false}, func(t *testing.T, g *graphpkg.Graph, f *claimFault, d claimDoor, forever bool) {
		claimConstrain(t, g, forever)
		cs := claimSetup(t, g, d, "a")
		before := claimState(t, g, cs.target)
		f.armWrite(cs.newVal[0].val, false, false)
		err := d.run(g, cs.target, cs.props)
		f.disarm()
		if !errors.Is(err, errClaimFault) {
			t.Fatalf("call with a failing store write: err = %v, want the injected fault", err)
		}
		if f.firedWrites() != 1 {
			t.Fatalf("fault fired %d times, want 1", f.firedWrites())
		}
		claimAssertUnchanged(t, g, cs, before)
		claimAssertFree(t, g, cs.newVal, "a failed store write")
		claimAssertHeld(t, g, cs.oldVal, "a failed store write (values the target held before)")

		ok := claimSetup(t, g, d, "c")
		if err := d.run(g, ok.target, ok.props); err != nil {
			t.Fatalf("same door without a fault: %v", err)
		}
		claimAssertHeld(t, g, ok.newVal, "a successful call")
	})
}

// GUARD (passes before the fix: nothing was withdrawn then). The store
// reports the failure AFTER installing the row (a create's cleanup fails too,
// so the partial node stays live): a stored row carries the values, so the
// claims stay — after the node moves off them, they are still barred.
// Catches: a withdrawal that ignores what was stored.
func TestUniqueClaims_WrittenRowKeepsClaim(t *testing.T) {
	t.Parallel()
	forClaims(t, []bool{true}, func(t *testing.T, g *graphpkg.Graph, f *claimFault, d claimDoor, _ bool) {
		claimConstrain(t, g, true)
		cs := claimSetup(t, g, d, "w")
		f.armWrite(cs.newVal[0].val, true, d.kind == claimCreate)
		err := d.run(g, cs.target, cs.props)
		f.disarm()
		if !errors.Is(err, errClaimFault) {
			t.Fatalf("call whose store write failed after storing: err = %v, want the injected fault", err)
		}
		// The stored row holds the values now; move it off them so only the
		// UniqueForever ownership (not the current holder) can bar them.
		ctx := context.Background()
		moveOff := make(map[string]any, len(cs.newVal))
		for _, kv := range cs.newVal {
			moveOff[kv.key] = kv.val + "-moved"
		}
		switch d.kind {
		case claimCreate:
			rows, err := g.Nodes().ByLabelAndProperty("Ref", ucKey, cs.newVal[0].val, storepkg.QueryOpts{})
			if err != nil || len(rows) != 1 {
				t.Fatalf("precondition: stored partial create rows = %d (%v), want 1", len(rows), err)
			}
			if _, err := g.Nodes().Update(ctx, rows[0].ID(), moveOff); err != nil {
				t.Fatalf("move the stored node off its values: %v", err)
			}
		case claimUpdate:
			if st := claimState(t, g, cs.target); st.props[ucKey] != cs.newVal[0].val {
				t.Fatalf("precondition: stored row %v, want %s=%s", st.props, ucKey, cs.newVal[0].val)
			}
			if _, err := g.Nodes().Update(ctx, cs.target, moveOff); err != nil {
				t.Fatalf("move the stored node off its values: %v", err)
			}
		case claimLabel:
			if st := claimState(t, g, cs.target); !st.hasRef {
				t.Fatal("precondition: stored row lacks the added label")
			}
			if err := g.Nodes().RemoveLabel(ctx, cs.target, "Ref"); err != nil {
				t.Fatalf("RemoveLabel: %v", err)
			}
		}
		claimAssertHeld(t, g, cs.newVal, "a failed call whose row was stored, then moved off")
	})
}

// (b) RED before the fix: two UniqueForever values, the registry persist of
// the SECOND claim fails. The call fails, the entity is unchanged, and the
// first claim is withdrawn. A one-key door (CAS) gets its second claim from a
// value the target carries that an operator released (ReleaseOwnership).
// Rounds repeat it so a claim order the map iteration picks cannot hide the
// leak before the fix.
func TestUniqueClaims_SecondClaimFailureWithdrawsFirst(t *testing.T) {
	t.Parallel()
	forClaims(t, []bool{true}, func(t *testing.T, g *graphpkg.Graph, f *claimFault, d claimDoor, _ bool) {
		claimConstrain(t, g, true)
		for r := 0; r < 4; r++ {
			cs := claimSetup(t, g, d, fmt.Sprintf("b%d", r))
			if d.oneKey {
				if err := g.Constraints().ReleaseOwnership(context.Background(), "Ref", ucKey2, cs.oldVal[1].val); err != nil {
					t.Fatalf("ReleaseOwnership: %v", err)
				}
			}
			before := claimState(t, g, cs.target)
			f.armMeta(1)
			err := d.run(g, cs.target, cs.props)
			f.disarm()
			if !errors.Is(err, errClaimFault) {
				t.Fatalf("round %d: call whose second claim fails to persist: err = %v, want the injected fault", r, err)
			}
			claimAssertUnchanged(t, g, cs, before)
			claimAssertFree(t, g, cs.newVal, "a failed second claim")
			if cs.oldVal != nil {
				claimAssertHeld(t, g, cs.oldVal[:1], "a failed second claim (value owned before the call)")
			}
		}
	})
}

// (d) GUARD (passes before the fix: nothing was withdrawn then). A value the
// node owned before the call but its stored row no longer carries — moved off
// it (update doors) or label removed (label doors) — is written again and the
// store write fails: the value stays owned by the node. Catches: a hold that
// records every value it passes (also registry hits of the node itself) and
// withdraws one the stored row does not carry.
func TestUniqueClaims_PreOwnedValueKept(t *testing.T) {
	t.Parallel()
	// A create door's node owns nothing before the call.
	notCreate := func(d claimDoor) bool { return d.kind != claimCreate }
	forClaimDoors(t, []bool{true}, notCreate, func(t *testing.T, g *graphpkg.Graph, f *claimFault, d claimDoor, _ bool) {
		ctx := context.Background()
		claimConstrain(t, g, true)
		const x0 = "pre-x0"
		var target types.NodeID
		switch d.kind {
		case claimUpdate:
			n, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{ucKey: x0})
			if err != nil {
				t.Fatal(err)
			}
			target = n.ID()
			if err := d.run(g, target, map[string]any{ucKey: "pre-x1"}); err != nil {
				t.Fatalf("move off %s: %v", x0, err)
			}
		case claimLabel:
			n, err := g.Nodes().Add(ctx, []string{"Plain", "Ref"}, map[string]any{ucKey: x0})
			if err != nil {
				t.Fatal(err)
			}
			target = n.ID()
			if err := g.Nodes().RemoveLabel(ctx, target, "Ref"); err != nil {
				t.Fatalf("RemoveLabel: %v", err)
			}
		}
		claimAssertHeld(t, g, []claimKV{{ucKey, x0}}, "the node moved off its owned value (precondition)")
		f.armWrite(x0, false, false)
		err := d.run(g, target, map[string]any{ucKey: x0})
		f.disarm()
		if !errors.Is(err, errClaimFault) {
			t.Fatalf("write back to the owned value with a failing store write: err = %v, want the injected fault", err)
		}
		claimAssertHeld(t, g, []claimKV{{ucKey, x0}}, "a failed write back to a value the node owned before the call")
	})
}

// (e) RED before the fix: two writers introduce the same UniqueForever value
// at once and the first store write carrying it fails. The failed writer's
// claim is withdrawn under its stripe, so the other writer wins: exactly one
// success, one injected failure, never a "permanently owned" refusal; the
// value then has one holder and is owned.
func TestUniqueClaims_ConcurrentFailedWriterLeavesNoOwner(t *testing.T) {
	t.Parallel()
	forClaims(t, []bool{true}, func(t *testing.T, g *graphpkg.Graph, f *claimFault, d claimDoor, _ bool) {
		claimConstrain(t, g, true)
		for r := 0; r < 4; r++ {
			v := fmt.Sprintf("same%d", r)
			props := map[string]any{ucKey: v}
			var targets [2]types.NodeID
			for i := range targets {
				switch d.kind {
				case claimUpdate:
					n, err := g.Nodes().Add(context.Background(), []string{"Ref"}, map[string]any{ucKey: fmt.Sprintf("x%d-%d", r, i)})
					if err != nil {
						t.Fatalf("setup: %v", err)
					}
					targets[i] = n.ID()
				case claimLabel:
					n, err := g.Nodes().Add(context.Background(), []string{"Plain"}, props)
					if err != nil {
						t.Fatalf("setup: %v", err)
					}
					targets[i] = n.ID()
				}
			}
			f.armWrite(v, false, false)
			var wg sync.WaitGroup
			errs := make([]error, 2)
			start := make(chan struct{})
			for i := range targets {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					errs[i] = d.run(g, targets[i], props)
				}(i)
			}
			close(start)
			wg.Wait()
			f.disarm()
			wins, faults, sameBatch := 0, 0, 0
			for _, err := range errs {
				switch {
				case err == nil:
					wins++
				case errors.Is(err, errClaimFault):
					faults++
				case d.group && errors.Is(err, graphpkg.ErrUniqueViolation) && strings.Contains(err.Error(), "earlier in this batch"):
					// The strong ingest applier coalesced both groups into one
					// batch: the second create of the value is refused against
					// the first (batch semantics, unchanged). The first one's
					// write then failed, so the value must be free.
					sameBatch++
				default:
					t.Fatalf("round %d: unexpected err %v (errs %v)", r, err, errs)
				}
			}
			if sameBatch == 1 && faults == 1 {
				if n := claimHolders(t, g, ucKey, v); n != 0 {
					t.Fatalf("round %d: holders of %s = %d after both writers failed, want 0", r, v, n)
				}
				claimAssertFree(t, g, []claimKV{{ucKey, v}}, "a coalesced batch whose only stored candidate failed its write")
				continue
			}
			if wins != 1 || faults != 1 {
				t.Fatalf("round %d: wins=%d faults=%d (errs %v), want one winner and one failed write", r, wins, faults, errs)
			}
			if n := claimHolders(t, g, ucKey, v); n != 1 {
				t.Fatalf("round %d: holders of %s = %d, want 1", r, v, n)
			}
			claimAssertHeld(t, g, []claimKV{{ucKey, v}}, "the winner's write")
		}
	})
}

// (f) Group semantics of the deferred doors (Batch, ingest Session in every
// mode): one op of the group fails on its claims, the sibling op of the same
// group commits. Creates: the failing node's second claim fails to persist
// (RED before the fix: its first value stays owned). Updates: the failing
// node's store write fails (RED before the fix: its value stays owned). In
// both the sibling commits and keeps its claim.
func TestUniqueClaims_GroupSemantics(t *testing.T) {
	t.Parallel()
	type groupDoor struct {
		name string
		run  func(g *graphpkg.Graph, queue func(add func(p map[string]any) error, upd func(id types.NodeID, p map[string]any) error) error) error
	}
	doors := []groupDoor{{"batch", func(g *graphpkg.Graph, queue func(add func(p map[string]any) error, upd func(id types.NodeID, p map[string]any) error) error) error {
		return claimBatch(g, func(b *graphpkg.BatchBuilder) error {
			return queue(func(p map[string]any) error { _, err := b.AddNode([]string{"Ref"}, p); return err },
				func(id types.NodeID, p map[string]any) error { return b.UpdateNode(id, p) })
		})
	}}}
	for _, m := range ivModes {
		m := m
		doors = append(doors, groupDoor{"session-" + m.name, func(g *graphpkg.Graph, queue func(add func(p map[string]any) error, upd func(id types.NodeID, p map[string]any) error) error) error {
			return claimSession(g, m, func(s *ingest.Session) error {
				return queue(func(p map[string]any) error { _, err := s.AddNode([]string{"Ref"}, p); return err },
					func(id types.NodeID, p map[string]any) error { return s.UpdateNode(id, p) })
			})
		}})
	}
	for _, b := range claimBackends() {
		for _, d := range doors {
			b, d := b, d
			t.Run(b.name+"/"+d.name+"/create", func(t *testing.T) {
				t.Parallel()
				g, f := b.open(t)
				claimConstrain(t, g, true)
				bad := []claimKV{{ucKey, "gb-k"}, {ucKey2, "gb-m"}}
				f.armMeta(1)
				err := d.run(g, func(add func(map[string]any) error, _ func(types.NodeID, map[string]any) error) error {
					if err := add(map[string]any{ucKey: "gb-k", ucKey2: "gb-m"}); err != nil {
						return err
					}
					return add(map[string]any{ucKey: "gg-k"})
				})
				f.disarm()
				if !errors.Is(err, errClaimFault) {
					t.Fatalf("group outcome = %v, want the injected fault", err)
				}
				for _, kv := range bad {
					if n := claimHolders(t, g, kv.key, kv.val); n != 0 {
						t.Fatalf("failed create stored: holders of %s = %d", kv.val, n)
					}
				}
				if n := claimHolders(t, g, ucKey, "gg-k"); n != 1 {
					t.Fatalf("sibling create in the same group: holders = %d, want 1 (partial success)", n)
				}
				claimAssertFree(t, g, bad, "a group op whose second claim failed")
				claimAssertHeld(t, g, []claimKV{{ucKey, "gg-k"}}, "the committed sibling")
			})
			t.Run(b.name+"/"+d.name+"/update", func(t *testing.T) {
				t.Parallel()
				g, f := b.open(t)
				claimConstrain(t, g, true)
				ctx := context.Background()
				a, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{ucKey: "ga0"})
				if err != nil {
					t.Fatal(err)
				}
				s, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{ucKey: "gs0"})
				if err != nil {
					t.Fatal(err)
				}
				before := claimState(t, g, a.ID())
				f.armWrite("gb-v", false, false)
				err = d.run(g, func(_ func(map[string]any) error, upd func(types.NodeID, map[string]any) error) error {
					if err := upd(a.ID(), map[string]any{ucKey: "gb-v"}); err != nil {
						return err
					}
					return upd(s.ID(), map[string]any{ucKey: "gs1"})
				})
				f.disarm()
				if !errors.Is(err, errClaimFault) {
					t.Fatalf("group outcome = %v, want the injected fault", err)
				}
				claimAssertUnchanged(t, g, claimCase{target: a.ID(), oldVal: []claimKV{{ucKey, "ga0"}}}, before)
				if st := claimState(t, g, s.ID()); st.props[ucKey] != "gs1" {
					t.Fatalf("sibling update in the same group: %s = %v, want gs1 (partial success)", ucKey, st.props[ucKey])
				}
				claimAssertFree(t, g, []claimKV{{ucKey, "gb-v"}}, "a group update whose write failed")
				claimAssertHeld(t, g, []claimKV{{ucKey, "gs1"}, {ucKey, "ga0"}}, "the group")
			})
		}
	}
}
