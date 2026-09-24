package core

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A hard Delete must not rewrite a valid-time close that was already recorded.
//
// Repro (v4.37.2): Add(valid_from=2020) → CloseVersion(2022) → pin → Delete.
// Every delete door stamped the final row IN PLACE with ValidTo = DeletedAt =
// the delete instant, overwriting the recorded close (2022 → 2026). A reader
// pinned BEFORE the delete then un-applied the delete via
// normalizeTemporalVisibleAtTxTime (ValidTo == DeletedAt → ValidTo = 0), so the
// closed entity reappeared as OPEN-ENDED: NodeAtTx(2025, pin) flipped from
// absent to present — a later write changed a historical answer. The plain
// valid-time doors (NodeAt/RelsAt at 2023) flipped too, because the tombstone
// widened the row's valid interval to the delete instant.
//
// The fix: a delete tombstone stamps ValidTo only when the row is open
// (ValidTo == 0) or scheduled to close after the delete (clamped, as before). A
// close at or before the delete instant is kept, and the delete instant is moved
// one tick past a close that lands exactly on it, so ValidTo == DeletedAt stays
// an unambiguous "the delete wrote this ValidTo" marker for the normalizers.
//
// Every test is two-phase (rule 15): the same questions are asked before and
// after the delete and must get the same answers. Node and rel mirrors (rule 2),
// every backend (memory, badger, tiered), and every delete door (standalone, tx,
// batch, ingest strong mode, ingest concurrent mode).

func instantUTC(year int) types.Instant {
	return types.Instant(time.Date(year, time.January, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
}

var (
	dcY2020 = instantUTC(2020)
	dcY2021 = instantUTC(2021)
	dcY2022 = instantUTC(2022)
	dcY2023 = instantUTC(2023)
	dcY2025 = instantUTC(2025)
)

// deleteDoor is one public way to hard-delete an entity. All of them must build
// the same tombstone.
type deleteDoor struct {
	name    string
	delNode func(t *testing.T, g *Core, id types.NodeID)
	delRel  func(t *testing.T, g *Core, id types.RelID)
}

func deleteDoors() []deleteDoor {
	ingest := func(opts IngestOptions) (func(*testing.T, *Core, types.NodeID), func(*testing.T, *Core, types.RelID)) {
		run := func(t *testing.T, g *Core, queue func(s *Session) error) {
			t.Helper()
			s, err := g.Ingest.NewSession(opts)
			if err != nil {
				t.Fatalf("Ingest.NewSession: %v", err)
			}
			if err := queue(s); err != nil {
				t.Fatalf("queue delete: %v", err)
			}
			tok, err := s.Submit()
			if err != nil {
				t.Fatalf("Submit: %v", err)
			}
			if err := g.Ingest.WaitApplied(tok); err != nil {
				t.Fatalf("WaitApplied: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatalf("Session.Close: %v", err)
			}
		}
		return func(t *testing.T, g *Core, id types.NodeID) {
				t.Helper()
				run(t, g, func(s *Session) error { return s.DeleteNode(id) })
			}, func(t *testing.T, g *Core, id types.RelID) {
				t.Helper()
				run(t, g, func(s *Session) error { return s.DeleteRelationship(id) })
			}
	}
	batch := func(t *testing.T, g *Core, queue func(b *BatchBuilder) error) {
		t.Helper()
		b, err := NewBatchBuilder(g)
		if err != nil {
			t.Fatalf("NewBatchBuilder: %v", err)
		}
		if err := queue(b); err != nil {
			t.Fatalf("queue delete: %v", err)
		}
		res, err := b.Execute()
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if res.Failed != 0 || len(res.Errors) != 0 {
			t.Fatalf("Execute: %d failed: %v", res.Failed, res.Errors)
		}
	}
	tx := func(t *testing.T, g *Core, do func(tx *GraphTx) error) {
		t.Helper()
		tx, err := g.BeginTx()
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		if err := do(tx); err != nil {
			_ = tx.Rollback()
			t.Fatalf("tx delete: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
	strongNode, strongRel := ingest(IngestOptions{Sync: true})
	concNode, concRel := ingest(IngestOptions{Concurrent: true})
	return []deleteDoor{
		{
			name: "standalone",
			delNode: func(t *testing.T, g *Core, id types.NodeID) {
				t.Helper()
				if err := g.Nodes.Delete(context.Background(), id); err != nil {
					t.Fatalf("Nodes.Delete: %v", err)
				}
			},
			delRel: func(t *testing.T, g *Core, id types.RelID) {
				t.Helper()
				if err := g.Rels.Delete(context.Background(), id); err != nil {
					t.Fatalf("Rels.Delete: %v", err)
				}
			},
		},
		{
			name: "tx",
			delNode: func(t *testing.T, g *Core, id types.NodeID) {
				t.Helper()
				tx(t, g, func(tx *GraphTx) error { return tx.DeleteNode(id) })
			},
			delRel: func(t *testing.T, g *Core, id types.RelID) {
				t.Helper()
				tx(t, g, func(tx *GraphTx) error { return tx.DeleteRelationship(id) })
			},
		},
		{
			name: "batch",
			delNode: func(t *testing.T, g *Core, id types.NodeID) {
				t.Helper()
				batch(t, g, func(b *BatchBuilder) error { return b.DeleteNode(id) })
			},
			delRel: func(t *testing.T, g *Core, id types.RelID) {
				t.Helper()
				batch(t, g, func(b *BatchBuilder) error { return b.DeleteRelationship(id) })
			},
		},
		{name: "ingest-strong", delNode: strongNode, delRel: strongRel},
		{name: "ingest-concurrent", delNode: concNode, delRel: concRel},
	}
}

// --- assertion helpers --------------------------------------------------------

func assertNodeAtTx(t *testing.T, g *Core, id types.NodeID, validAt, txAt types.Instant, want bool, phase string) {
	t.Helper()
	n, err := g.Temporal.NodeAtTx(id, validAt, txAt)
	switch {
	case want && err != nil:
		t.Fatalf("[%s] NodeAtTx(valid=%d, tx=%d): %v; want present", phase, validAt, txAt, err)
	case want && n.ID() != id:
		t.Fatalf("[%s] NodeAtTx returned %v; want %v", phase, n.ID(), id)
	case !want && err == nil:
		t.Fatalf("[%s] NodeAtTx(valid=%d, tx=%d) = v%d (ValidTo=%d); want absent (closed)",
			phase, validAt, txAt, n.Version(), n.Temporal().ValidTo)
	case !want && !errors.Is(err, storepkg.ErrNoVersionValidAt):
		t.Fatalf("[%s] NodeAtTx(valid=%d, tx=%d) err = %v; want ErrNoVersionValidAt", phase, validAt, txAt, err)
	}
}

func assertRelAtTx(t *testing.T, g *Core, id types.RelID, validAt, txAt types.Instant, want bool, phase string) {
	t.Helper()
	r, err := g.Temporal.RelAtTx(id, validAt, txAt)
	switch {
	case want && err != nil:
		t.Fatalf("[%s] RelAtTx(valid=%d, tx=%d): %v; want present", phase, validAt, txAt, err)
	case want && r.ID() != id:
		t.Fatalf("[%s] RelAtTx returned %v; want %v", phase, r.ID(), id)
	case !want && err == nil:
		t.Fatalf("[%s] RelAtTx(valid=%d, tx=%d) = v%d (ValidTo=%d); want absent (closed)",
			phase, validAt, txAt, r.Version(), r.Temporal().ValidTo)
	case !want && !errors.Is(err, storepkg.ErrNoVersionValidAt):
		t.Fatalf("[%s] RelAtTx(valid=%d, tx=%d) err = %v; want ErrNoVersionValidAt", phase, validAt, txAt, err)
	}
}

// assertNodeValidOnly checks the valid-time-only doors (NodeAt + NodesAt).
func assertNodeValidOnly(t *testing.T, g *Core, id types.NodeID, at types.Instant, want bool, phase string) {
	t.Helper()
	n, err := g.Temporal.NodeAt(id, at)
	switch {
	case want && err != nil:
		t.Fatalf("[%s] NodeAt(%d): %v; want present", phase, at, err)
	case !want && err == nil:
		t.Fatalf("[%s] NodeAt(%d) = v%d (ValidTo=%d); want absent", phase, at, n.Version(), n.Temporal().ValidTo)
	case !want && !errors.Is(err, storepkg.ErrNoVersionValidAt):
		t.Fatalf("[%s] NodeAt(%d) err = %v; want ErrNoVersionValidAt", phase, at, err)
	}
	rows, err := g.Temporal.NodesAt(at)
	if err != nil {
		t.Fatalf("[%s] NodesAt(%d): %v", phase, at, err)
	}
	found := false
	for _, r := range rows {
		if r.ID() == id {
			found = true
		}
	}
	if found != want {
		t.Fatalf("[%s] NodesAt(%d) contains node = %v; want %v", phase, at, found, want)
	}
}

// assertRelValidOnly checks the valid-time-only doors (RelAt + RelsAt).
func assertRelValidOnly(t *testing.T, g *Core, id types.RelID, at types.Instant, want bool, phase string) {
	t.Helper()
	r, err := g.Temporal.RelAt(id, at)
	switch {
	case want && err != nil:
		t.Fatalf("[%s] RelAt(%d): %v; want present", phase, at, err)
	case !want && err == nil:
		t.Fatalf("[%s] RelAt(%d) = v%d (ValidTo=%d); want absent", phase, at, r.Version(), r.Temporal().ValidTo)
	case !want && !errors.Is(err, storepkg.ErrNoVersionValidAt):
		t.Fatalf("[%s] RelAt(%d) err = %v; want ErrNoVersionValidAt", phase, at, err)
	}
	rows, err := g.Temporal.RelsAt(at)
	if err != nil {
		t.Fatalf("[%s] RelsAt(%d): %v", phase, at, err)
	}
	found := false
	for _, x := range rows {
		if x.ID() == id {
			found = true
		}
	}
	if found != want {
		t.Fatalf("[%s] RelsAt(%d) contains rel = %v; want %v", phase, at, found, want)
	}
}

// assertNodeBeliefValidTo checks every as-of belief door (NodeAsOf, NodesAsOf,
// ByLabel{TxPin}) returns the node at pin with exactly wantValidTo and no
// post-pin delete stamp.
func assertNodeBeliefValidTo(t *testing.T, g *Core, id types.NodeID, label string, pin, wantValidTo types.Instant, phase string) {
	t.Helper()
	check := func(door string, n *types.Node) {
		t.Helper()
		tm := n.Temporal()
		if tm == nil {
			t.Fatalf("[%s] %s: no temporal metadata", phase, door)
		}
		if tm.ValidTo != wantValidTo {
			t.Fatalf("[%s] %s ValidTo = %d; want %d", phase, door, tm.ValidTo, wantValidTo)
		}
		if tm.DeletedAt != 0 {
			t.Fatalf("[%s] %s DeletedAt = %d; want 0 (delete post-dates the pin)", phase, door, tm.DeletedAt)
		}
	}
	n, err := g.Temporal.NodeAsOf(id, pin)
	if err != nil {
		t.Fatalf("[%s] NodeAsOf(pin): %v", phase, err)
	}
	check("NodeAsOf", n)
	got, ok := nodeInAsOf(t, g, id, pin)
	if !ok {
		t.Fatalf("[%s] NodesAsOf(pin) missing node", phase)
	}
	check("NodesAsOf", got)
	rows, err := g.Nodes.ByLabel(label, storepkg.QueryOpts{TxPin: pin})
	if err != nil {
		t.Fatalf("[%s] ByLabel(TxPin): %v", phase, err)
	}
	found := false
	for _, r := range rows {
		if r.ID() == id {
			found = true
			check("ByLabel{TxPin}", r)
		}
	}
	if !found {
		t.Fatalf("[%s] ByLabel(%q, TxPin=pin) missing node", phase, label)
	}
}

// assertRelBeliefValidTo is the relationship mirror of assertNodeBeliefValidTo
// (RelAsOf, RelsAsOf, ByType{TxPin}).
func assertRelBeliefValidTo(t *testing.T, g *Core, id types.RelID, relType string, pin, wantValidTo types.Instant, phase string) {
	t.Helper()
	check := func(door string, r *types.Relationship) {
		t.Helper()
		tm := r.Temporal()
		if tm == nil {
			t.Fatalf("[%s] %s: no temporal metadata", phase, door)
		}
		if tm.ValidTo != wantValidTo {
			t.Fatalf("[%s] %s ValidTo = %d; want %d", phase, door, tm.ValidTo, wantValidTo)
		}
		if tm.DeletedAt != 0 {
			t.Fatalf("[%s] %s DeletedAt = %d; want 0 (delete post-dates the pin)", phase, door, tm.DeletedAt)
		}
	}
	r, err := g.Temporal.RelAsOf(id, pin)
	if err != nil {
		t.Fatalf("[%s] RelAsOf(pin): %v", phase, err)
	}
	check("RelAsOf", r)
	got, ok := relInAsOf(t, g, id, pin)
	if !ok {
		t.Fatalf("[%s] RelsAsOf(pin) missing rel", phase)
	}
	check("RelsAsOf", got)
	rows, err := g.Rels.ByType(relType, storepkg.QueryOpts{TxPin: pin})
	if err != nil {
		t.Fatalf("[%s] ByType(TxPin): %v", phase, err)
	}
	found := false
	for _, x := range rows {
		if x.ID() == id {
			found = true
			check("ByType{TxPin}", x)
		}
	}
	if !found {
		t.Fatalf("[%s] ByType(%q, TxPin=pin) missing rel", phase, relType)
	}
}

// nodeTombstone returns the newest history row of a deleted node.
func nodeTombstone(t *testing.T, g *Core, id types.NodeID) *types.TemporalMetadata {
	t.Helper()
	if _, err := g.Nodes.Get(context.Background(), id); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("Nodes.Get after delete err = %v; want ErrNodeNotFound", err)
	}
	hist, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("Nodes.History: %v", err)
	}
	if len(hist) == 0 {
		t.Fatal("deleted node has no history")
	}
	last := hist[0]
	for _, v := range hist {
		if v.Version() > last.Version() {
			last = v
		}
	}
	tm := last.Temporal()
	if tm == nil || tm.DeletedAt == 0 {
		t.Fatalf("newest history row v%d is not a delete tombstone: %+v", last.Version(), tm)
	}
	return tm
}

// relTombstone returns the newest history row of a deleted relationship.
func relTombstone(t *testing.T, g *Core, id types.RelID) *types.TemporalMetadata {
	t.Helper()
	if _, err := g.Rels.Get(context.Background(), id); !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("Rels.Get after delete err = %v; want ErrRelNotFound", err)
	}
	hist, err := g.Rels.History(id)
	if err != nil {
		t.Fatalf("Rels.History: %v", err)
	}
	if len(hist) == 0 {
		t.Fatal("deleted rel has no history")
	}
	last := hist[0]
	for _, v := range hist {
		if v.Version() > last.Version() {
			last = v
		}
	}
	tm := last.Temporal()
	if tm == nil || tm.DeletedAt == 0 {
		t.Fatalf("newest history row v%d is not a delete tombstone: %+v", last.Version(), tm)
	}
	return tm
}

func assertNodeChainOK(t *testing.T, g *Core, id types.NodeID) {
	t.Helper()
	ok, err := g.Hash.VerifyNodeChain(id)
	if err != nil || !ok {
		t.Fatalf("VerifyNodeChain after delete = %v, %v; want true, nil", ok, err)
	}
}

func assertRelChainOK(t *testing.T, g *Core, id types.RelID) {
	t.Helper()
	ok, err := g.Hash.VerifyRelChain(id)
	if err != nil || !ok {
		t.Fatalf("VerifyRelChain after delete = %v, %v; want true, nil", ok, err)
	}
}

// addClosedRel creates two reference-shard endpoints and a rel valid from 2020,
// closed at 2022. Returns the endpoints and the rel id.
func addClosedRel(t *testing.T, g *Core) (types.NodeID, types.NodeID, types.RelID) {
	t.Helper()
	ctx := context.Background()
	start, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{"k": "s"})
	if err != nil {
		t.Fatalf("Add(start): %v", err)
	}
	end, err := g.Nodes.Add(ctx, []string{"User"}, map[string]any{"k": "e"})
	if err != nil {
		t.Fatalf("Add(end): %v", err)
	}
	r, err := g.Rels.AddByID(ctx, "LINK", start.ID(), end.ID(), map[string]any{"tkg_valid_from": dcY2020})
	if err != nil {
		t.Fatalf("AddByID: %v", err)
	}
	if err := g.Rels.CloseVersion(ctx, r.ID(), dcY2022); err != nil {
		t.Fatalf("Rels.CloseVersion: %v", err)
	}
	return start.ID(), end.ID(), r.ID()
}

// --- 1+2+6: closed node / rel, then delete ------------------------------------

func TestDeleteKeepsPastCloseNode(t *testing.T) {
	t.Parallel()
	for _, be := range asofCascadeBackends() {
		for _, door := range deleteDoors() {
			t.Run(be.name+"/"+door.name, func(t *testing.T) {
				g := be.newCore(t)
				ctx := context.Background()
				n, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{"tkg_valid_from": dcY2020})
				if err != nil {
					t.Fatalf("Add: %v", err)
				}
				id := n.ID()
				if err := g.Nodes.CloseVersion(ctx, id, dcY2022); err != nil {
					t.Fatalf("CloseVersion: %v", err)
				}
				pin, err := g.Temporal.NowTx()
				if err != nil {
					t.Fatalf("NowTx: %v", err)
				}

				check := func(phase string) {
					t.Helper()
					assertNodeAtTx(t, g, id, dcY2025, pin, false, phase)
					assertNodeAtTx(t, g, id, dcY2023, pin, false, phase)
					assertNodeAtTx(t, g, id, dcY2021, pin, true, phase)
					assertNodeBeliefValidTo(t, g, id, "Case", pin, dcY2022, phase)
					assertNodeValidOnly(t, g, id, dcY2021, true, phase)
					assertNodeValidOnly(t, g, id, dcY2023, false, phase)
				}
				check("before delete")
				door.delNode(t, g, id)
				check("after delete")

				tomb := nodeTombstone(t, g, id)
				if tomb.ValidTo != dcY2022 {
					t.Fatalf("tombstone ValidTo = %d; want the recorded close %d", tomb.ValidTo, dcY2022)
				}
				if tomb.TxTo != tomb.DeletedAt || tomb.DeletedAt <= pin {
					t.Fatalf("tombstone TxTo=%d DeletedAt=%d pin=%d; want TxTo == DeletedAt > pin", tomb.TxTo, tomb.DeletedAt, pin)
				}
				assertNodeChainOK(t, g, id)
			})
		}
	}
}

func TestDeleteKeepsPastCloseRel(t *testing.T) {
	t.Parallel()
	for _, be := range asofCascadeBackends() {
		for _, door := range deleteDoors() {
			t.Run(be.name+"/"+door.name, func(t *testing.T) {
				g := be.newCore(t)
				_, _, id := addClosedRel(t, g)
				pin, err := g.Temporal.NowTx()
				if err != nil {
					t.Fatalf("NowTx: %v", err)
				}

				check := func(phase string) {
					t.Helper()
					assertRelAtTx(t, g, id, dcY2025, pin, false, phase)
					assertRelAtTx(t, g, id, dcY2023, pin, false, phase)
					assertRelAtTx(t, g, id, dcY2021, pin, true, phase)
					assertRelBeliefValidTo(t, g, id, "LINK", pin, dcY2022, phase)
					assertRelValidOnly(t, g, id, dcY2021, true, phase)
					assertRelValidOnly(t, g, id, dcY2023, false, phase)
				}
				check("before delete")
				door.delRel(t, g, id)
				check("after delete")

				tomb := relTombstone(t, g, id)
				if tomb.ValidTo != dcY2022 {
					t.Fatalf("tombstone ValidTo = %d; want the recorded close %d", tomb.ValidTo, dcY2022)
				}
				if tomb.TxTo != tomb.DeletedAt || tomb.DeletedAt <= pin {
					t.Fatalf("tombstone TxTo=%d DeletedAt=%d pin=%d; want TxTo == DeletedAt > pin", tomb.TxTo, tomb.DeletedAt, pin)
				}
				assertRelChainOK(t, g, id)
			})
		}
	}
}

// --- 3: node delete cascading onto a closed relationship ----------------------

func TestNodeDeleteCascadeKeepsClosedRelValidTo(t *testing.T) {
	t.Parallel()
	for _, be := range asofCascadeBackends() {
		for _, door := range deleteDoors() {
			t.Run(be.name+"/"+door.name, func(t *testing.T) {
				g := be.newCore(t)
				ctx := context.Background()
				start, end, rid := addClosedRel(t, g)
				// A second, OPEN rel on the same node: the cascade must still clamp it.
				open, err := g.Rels.AddByID(ctx, "LINK", start, end, map[string]any{"tkg_valid_from": dcY2020})
				if err != nil {
					t.Fatalf("AddByID(open): %v", err)
				}
				pin, err := g.Temporal.NowTx()
				if err != nil {
					t.Fatalf("NowTx: %v", err)
				}

				check := func(phase string) {
					t.Helper()
					assertRelAtTx(t, g, rid, dcY2025, pin, false, phase)
					assertRelAtTx(t, g, rid, dcY2021, pin, true, phase)
					assertRelBeliefValidTo(t, g, rid, "LINK", pin, dcY2022, phase)
					assertRelValidOnly(t, g, rid, dcY2021, true, phase)
					assertRelValidOnly(t, g, rid, dcY2023, false, phase)
					// The open sibling is open-ended as believed at the pin.
					assertRelAtTx(t, g, open.ID(), dcY2025, pin, true, phase)
					assertRelBeliefValidTo(t, g, open.ID(), "LINK", pin, 0, phase)
				}
				check("before delete")
				door.delNode(t, g, start)
				check("after delete")

				tomb := relTombstone(t, g, rid)
				if tomb.ValidTo != dcY2022 {
					t.Fatalf("cascaded tombstone ValidTo = %d; want the recorded close %d", tomb.ValidTo, dcY2022)
				}
				openTomb := relTombstone(t, g, open.ID())
				if openTomb.ValidTo != openTomb.DeletedAt || openTomb.DeletedAt != tomb.DeletedAt {
					t.Fatalf("open cascaded tombstone ValidTo=%d DeletedAt=%d (closed sibling DeletedAt=%d); want ValidTo == DeletedAt == the cascade instant",
						openTomb.ValidTo, openTomb.DeletedAt, tomb.DeletedAt)
				}
				assertRelChainOK(t, g, rid)
				assertRelChainOK(t, g, open.ID())
				assertNodeChainOK(t, g, start)
			})
		}
	}
}

// --- 4: open entity delete is unchanged --------------------------------------

func TestDeleteOpenEntityStampsValidToAtDelete(t *testing.T) {
	t.Parallel()
	for _, be := range asofCascadeBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			ctx := context.Background()
			n, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{"tkg_valid_from": dcY2020})
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
			start, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{"k": "s"})
			if err != nil {
				t.Fatalf("Add(start): %v", err)
			}
			end, err := g.Nodes.Add(ctx, []string{"User"}, map[string]any{"k": "e"})
			if err != nil {
				t.Fatalf("Add(end): %v", err)
			}
			r, err := g.Rels.AddByID(ctx, "LINK", start.ID(), end.ID(), map[string]any{"tkg_valid_from": dcY2020})
			if err != nil {
				t.Fatalf("AddByID: %v", err)
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}

			check := func(phase string) {
				t.Helper()
				assertNodeAtTx(t, g, n.ID(), dcY2025, pin, true, phase)
				assertRelAtTx(t, g, r.ID(), dcY2025, pin, true, phase)
				assertNodeBeliefValidTo(t, g, n.ID(), "Case", pin, 0, phase)
				assertRelBeliefValidTo(t, g, r.ID(), "LINK", pin, 0, phase)
				assertNodeValidOnly(t, g, n.ID(), dcY2025, true, phase)
				assertRelValidOnly(t, g, r.ID(), dcY2025, true, phase)
			}
			check("before delete")
			if err := g.Nodes.Delete(ctx, n.ID()); err != nil {
				t.Fatalf("Nodes.Delete: %v", err)
			}
			if err := g.Rels.Delete(ctx, r.ID()); err != nil {
				t.Fatalf("Rels.Delete: %v", err)
			}
			check("after delete")

			nt := nodeTombstone(t, g, n.ID())
			if nt.ValidTo != nt.DeletedAt || nt.TxTo != nt.DeletedAt || nt.DeletedAt <= pin {
				t.Fatalf("node tombstone ValidTo=%d DeletedAt=%d TxTo=%d pin=%d; want ValidTo == DeletedAt == TxTo > pin",
					nt.ValidTo, nt.DeletedAt, nt.TxTo, pin)
			}
			rt := relTombstone(t, g, r.ID())
			if rt.ValidTo != rt.DeletedAt || rt.TxTo != rt.DeletedAt || rt.DeletedAt <= pin {
				t.Fatalf("rel tombstone ValidTo=%d DeletedAt=%d TxTo=%d pin=%d; want ValidTo == DeletedAt == TxTo > pin",
					rt.ValidTo, rt.DeletedAt, rt.TxTo, pin)
			}
			// Current knowledge: the entity stops being valid at the delete instant.
			assertNodeValidOnly(t, g, n.ID(), nt.DeletedAt-1, true, "after delete")
			assertNodeValidOnly(t, g, n.ID(), nt.DeletedAt, false, "after delete")
			assertRelValidOnly(t, g, r.ID(), rt.DeletedAt-1, true, "after delete")
			assertRelValidOnly(t, g, r.ID(), rt.DeletedAt, false, "after delete")
			assertNodeChainOK(t, g, n.ID())
			assertRelChainOK(t, g, r.ID())
		})
	}
}

// --- 5: a close scheduled after the delete is clamped to the delete ----------

func TestDeleteClampsFutureScheduledClose(t *testing.T) {
	t.Parallel()
	for _, be := range asofCascadeBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.newCore(t)
			ctx := context.Background()
			future := types.Instant(time.Now().Add(24 * time.Hour).UnixMilli())
			n, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{"tkg_valid_from": dcY2020})
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
			if err := g.Nodes.CloseVersion(ctx, n.ID(), future); err != nil {
				t.Fatalf("Nodes.CloseVersion: %v", err)
			}
			// An unrelated rel closed in the past must stay closed throughout.
			_, _, rid := addClosedRel(t, g)
			start, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{"k": "s2"})
			if err != nil {
				t.Fatalf("Add(start2): %v", err)
			}
			end, err := g.Nodes.Add(ctx, []string{"User"}, map[string]any{"k": "e2"})
			if err != nil {
				t.Fatalf("Add(end2): %v", err)
			}
			r, err := g.Rels.AddByID(ctx, "LINK", start.ID(), end.ID(), map[string]any{"tkg_valid_from": dcY2020})
			if err != nil {
				t.Fatalf("AddByID: %v", err)
			}
			if err := g.Rels.CloseVersion(ctx, r.ID(), future); err != nil {
				t.Fatalf("Rels.CloseVersion: %v", err)
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}

			check := func(phase string) {
				t.Helper()
				// Inside the (still valid) interval as believed at the pin.
				assertNodeAtTx(t, g, n.ID(), dcY2025, pin, true, phase)
				assertRelAtTx(t, g, r.ID(), dcY2025, pin, true, phase)
				// The unrelated past-closed rel stays closed.
				assertRelAtTx(t, g, rid, dcY2025, pin, false, phase)
			}
			check("before delete")
			// Before the delete the scheduled close is honoured at the pin. This
			// is deliberately NOT re-asserted after the delete: the clamp
			// overwrites the scheduled ValidTo in place, so a pin before the
			// delete sees an open-ended row (a known v4 limitation — the
			// scheduled instant is not stored anywhere once clamped).
			assertNodeAtTx(t, g, n.ID(), future, pin, false, "before delete")
			assertRelAtTx(t, g, r.ID(), future, pin, false, "before delete")
			if err := g.Nodes.Delete(ctx, n.ID()); err != nil {
				t.Fatalf("Nodes.Delete: %v", err)
			}
			if err := g.Rels.Delete(ctx, r.ID()); err != nil {
				t.Fatalf("Rels.Delete: %v", err)
			}
			check("after delete")

			// Current knowledge: the scheduled close is clamped to the delete.
			nt := nodeTombstone(t, g, n.ID())
			if nt.ValidTo != nt.DeletedAt || nt.DeletedAt >= future {
				t.Fatalf("node tombstone ValidTo=%d DeletedAt=%d future=%d; want ValidTo == DeletedAt < future (clamped)",
					nt.ValidTo, nt.DeletedAt, future)
			}
			rt := relTombstone(t, g, r.ID())
			if rt.ValidTo != rt.DeletedAt || rt.DeletedAt >= future {
				t.Fatalf("rel tombstone ValidTo=%d DeletedAt=%d future=%d; want ValidTo == DeletedAt < future (clamped)",
					rt.ValidTo, rt.DeletedAt, future)
			}
			assertNodeValidOnly(t, g, n.ID(), nt.DeletedAt, false, "after delete")
			assertRelValidOnly(t, g, r.ID(), rt.DeletedAt, false, "after delete")
			assertNodeValidOnly(t, g, n.ID(), dcY2025, true, "after delete")
			assertRelValidOnly(t, g, r.ID(), dcY2025, true, "after delete")
			assertNodeChainOK(t, g, n.ID())
			assertRelChainOK(t, g, r.ID())
		})
	}
}

// --- a close landing exactly on the delete instant ----------------------------

// switchableClock returns wall time until set, then the set instant (advanced
// by the Core's own monotonic floor on each call).
type switchableClock struct{ at atomic.Int64 }

func (s *switchableClock) Now() time.Time {
	if v := s.at.Load(); v != 0 {
		return time.UnixMilli(v)
	}
	return time.Now()
}

// A recorded close X and a delete whose clock reads exactly X: the tombstone
// must keep ValidTo = X, and the delete instant must not equal X, otherwise a
// reader pinned before the delete cannot tell the recorded close from a
// delete-stamped ValidTo and reopens the entity.
func TestDeleteAtExactCloseInstantKeepsClose(t *testing.T) {
	t.Parallel()
	for _, be := range asofCascadeBackends() {
		t.Run(be.name+"/node", func(t *testing.T) {
			g := be.newCore(t)
			clk := &switchableClock{}
			g.SetClockForTest(t, clk.Now)
			ctx := context.Background()
			closeAt := types.Instant(time.Now().Add(time.Minute).UnixMilli())
			n, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{"tkg_valid_from": dcY2020})
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
			if err := g.Nodes.CloseVersion(ctx, n.ID(), closeAt); err != nil {
				t.Fatalf("CloseVersion: %v", err)
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}
			if pin >= closeAt {
				t.Fatalf("pin %d not before close %d", pin, closeAt)
			}
			check := func(phase string) {
				t.Helper()
				assertNodeAtTx(t, g, n.ID(), closeAt-1, pin, true, phase)
				assertNodeAtTx(t, g, n.ID(), closeAt, pin, false, phase)
				assertNodeBeliefValidTo(t, g, n.ID(), "Case", pin, closeAt, phase)
			}
			check("before delete")
			clk.at.Store(int64(closeAt))
			if err := g.Nodes.Delete(ctx, n.ID()); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			check("after delete")
			tomb := nodeTombstone(t, g, n.ID())
			if tomb.ValidTo != closeAt || tomb.DeletedAt <= closeAt || tomb.TxTo != tomb.DeletedAt {
				t.Fatalf("tombstone ValidTo=%d DeletedAt=%d TxTo=%d; want ValidTo == %d < DeletedAt == TxTo",
					tomb.ValidTo, tomb.DeletedAt, tomb.TxTo, closeAt)
			}
			assertNodeChainOK(t, g, n.ID())
		})
		t.Run(be.name+"/rel", func(t *testing.T) {
			g := be.newCore(t)
			clk := &switchableClock{}
			g.SetClockForTest(t, clk.Now)
			ctx := context.Background()
			closeAt := types.Instant(time.Now().Add(time.Minute).UnixMilli())
			start, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{"k": "s"})
			if err != nil {
				t.Fatalf("Add(start): %v", err)
			}
			end, err := g.Nodes.Add(ctx, []string{"User"}, map[string]any{"k": "e"})
			if err != nil {
				t.Fatalf("Add(end): %v", err)
			}
			r, err := g.Rels.AddByID(ctx, "LINK", start.ID(), end.ID(), map[string]any{"tkg_valid_from": dcY2020})
			if err != nil {
				t.Fatalf("AddByID: %v", err)
			}
			if err := g.Rels.CloseVersion(ctx, r.ID(), closeAt); err != nil {
				t.Fatalf("CloseVersion: %v", err)
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}
			check := func(phase string) {
				t.Helper()
				assertRelAtTx(t, g, r.ID(), closeAt-1, pin, true, phase)
				assertRelAtTx(t, g, r.ID(), closeAt, pin, false, phase)
				assertRelBeliefValidTo(t, g, r.ID(), "LINK", pin, closeAt, phase)
			}
			check("before delete")
			clk.at.Store(int64(closeAt))
			if err := g.Rels.Delete(ctx, r.ID()); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			check("after delete")
			tomb := relTombstone(t, g, r.ID())
			if tomb.ValidTo != closeAt || tomb.DeletedAt <= closeAt || tomb.TxTo != tomb.DeletedAt {
				t.Fatalf("tombstone ValidTo=%d DeletedAt=%d TxTo=%d; want ValidTo == %d < DeletedAt == TxTo",
					tomb.ValidTo, tomb.DeletedAt, tomb.TxTo, closeAt)
			}
			assertRelChainOK(t, g, r.ID())
		})
		t.Run(be.name+"/cascade", func(t *testing.T) {
			g := be.newCore(t)
			clk := &switchableClock{}
			g.SetClockForTest(t, clk.Now)
			ctx := context.Background()
			closeAt := types.Instant(time.Now().Add(time.Minute).UnixMilli())
			start, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{"k": "s"})
			if err != nil {
				t.Fatalf("Add(start): %v", err)
			}
			end, err := g.Nodes.Add(ctx, []string{"User"}, map[string]any{"k": "e"})
			if err != nil {
				t.Fatalf("Add(end): %v", err)
			}
			r, err := g.Rels.AddByID(ctx, "LINK", start.ID(), end.ID(), map[string]any{"tkg_valid_from": dcY2020})
			if err != nil {
				t.Fatalf("AddByID: %v", err)
			}
			if err := g.Rels.CloseVersion(ctx, r.ID(), closeAt); err != nil {
				t.Fatalf("CloseVersion: %v", err)
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}
			check := func(phase string) {
				t.Helper()
				assertRelAtTx(t, g, r.ID(), closeAt-1, pin, true, phase)
				assertRelAtTx(t, g, r.ID(), closeAt, pin, false, phase)
				assertRelBeliefValidTo(t, g, r.ID(), "LINK", pin, closeAt, phase)
			}
			check("before delete")
			clk.at.Store(int64(closeAt))
			if err := g.Nodes.Delete(ctx, start.ID()); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			check("after delete")
			tomb := relTombstone(t, g, r.ID())
			if tomb.ValidTo != closeAt || tomb.DeletedAt <= closeAt {
				t.Fatalf("cascaded tombstone ValidTo=%d DeletedAt=%d; want ValidTo == %d < DeletedAt",
					tomb.ValidTo, tomb.DeletedAt, closeAt)
			}
			if nt := nodeTombstone(t, g, start.ID()); nt.DeletedAt != tomb.DeletedAt || nt.ValidTo != nt.DeletedAt {
				t.Fatalf("node tombstone ValidTo=%d DeletedAt=%d; want ValidTo == DeletedAt == cascade instant %d",
					nt.ValidTo, nt.DeletedAt, tomb.DeletedAt)
			}
			assertRelChainOK(t, g, r.ID())
			assertNodeChainOK(t, g, start.ID())
		})
	}
}
