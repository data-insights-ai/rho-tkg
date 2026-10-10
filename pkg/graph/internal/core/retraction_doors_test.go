package core

import (
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRetraction_DoorMatrix is the per-door matrix of backlog 43: every
// retraction door (standalone, GraphTx, Batch, ingest strong and concurrent;
// each plain and at a caller instant) x memory, badger, sharded, tiered x a
// node retraction (cascading to its relationships) and a relationship
// retraction, read back through every read-door family (rtFamilies) at
// every valid instant of the grid. Two-phase (rule 15): every answer is
// captured before the retraction, then:
//
//   - at pins before T (the pin the answers were captured at, and T-1) every
//     answer is byte-identical to the captured one — the marker must not leak
//     into a pre-T read (pin-stability);
//   - at pins T and NowTx, and through the current-belief doors, every
//     answer equals the captured one minus the retracted entities, at EVERY
//     valid instant — including the past ones a Delete keeps readable.
//
// Exact sets: the bystanders (A, Y, RY, and RXY in the relationship case)
// must answer exactly as before. Faulty implementations this kills: Retract
// as Delete (the past stays readable at pins >= T), a cap at T only, a door
// that skips the marker, a cascade that marks the node but not its
// relationships, a read door that ignores the marker, a marker leaking into
// pre-T answers.
func TestRetraction_DoorMatrix(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		for _, door := range retractDoors() {
			for _, kind := range []string{"node", "rel"} {
				be, door, kind := be, door, kind
				t.Run(be.name+"/"+door.name+"/"+kind, func(t *testing.T) {
					t.Parallel()
					runRetractMatrix(t, be.open(t, true), door, kind)
				})
			}
		}
	}
}

type rtProbe struct {
	fam rtFamily
	v   types.Instant
}

func (p rtProbe) String() string { return fmt.Sprintf("%s v=%d", p.fam.name, p.v) }

func runRetractMatrix(t *testing.T, g *Core, door retractDoor, kind string) {
	w := buildRetractWorld(t, g)

	var T, p0 types.Instant
	if door.withTx {
		var err error
		if T, err = g.Temporal.NowTx(); err != nil {
			t.Fatalf("NowTx: %v", err)
		}
		p0 = T - 1
	} else {
		var err error
		if p0, err = g.Temporal.NowTx(); err != nil {
			t.Fatalf("NowTx: %v", err)
		}
	}

	// Phase 1: capture every answer.
	before := map[string]answer{}
	applicable := 0
	var probes []rtProbe
	for _, fam := range rtFamilies() {
		for _, v := range w.validGrid() {
			pin := types.Instant(0)
			if fam.pinned {
				pin = p0
			}
			a, ok, err := fam.q(g, w, pin, v)
			if err != nil {
				t.Fatalf("before: %s v=%d pin=%d: %v", fam.name, v, pin, err)
			}
			if !ok {
				continue
			}
			p := rtProbe{fam, v}
			before[p.String()] = a
			probes = append(probes, p)
			applicable++
		}
	}
	if applicable < 40*len(w.validGrid()) {
		t.Fatalf("only %d probes applicable: the matrix silently skipped door families", applicable)
	}

	// The retraction.
	retracted := map[string]bool{}
	var tombOf func() *types.TemporalMetadata
	switch kind {
	case "node":
		if err := door.node(t, g, w.x, T); err != nil {
			t.Fatalf("%s RetractNode: %v", door.name, err)
		}
		retracted[nodeKey(w.x)], retracted[relKey(w.rx)], retracted[relKey(w.rxy)] = true, true, true
		tombOf = func() *types.TemporalMetadata { return newestNodeRow(t, g, w.x).Temporal() }
	case "rel":
		if err := door.rel(t, g, w.rx, T); err != nil {
			t.Fatalf("%s RetractRelationship: %v", door.name, err)
		}
		retracted[relKey(w.rx)] = true
		tombOf = func() *types.TemporalMetadata { return newestRelRow(t, g, w.rx).Temporal() }
	}
	tomb := tombOf()
	if door.withTx {
		if tomb.DeletedAt != T || tomb.TxTo != T {
			t.Fatalf("tombstone stamps %+v, want TxTo = DeletedAt = caller instant %d (ignores t / moves t)", *tomb, T)
		}
	} else {
		T = tomb.DeletedAt
		if T <= p0 {
			t.Fatalf("plain retraction instant %d not after the capture pin %d", T, p0)
		}
	}
	if !tomb.Retracted {
		t.Fatalf("%s: the tombstone does not carry the retraction marker: %+v", door.name, *tomb)
	}
	pNow, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}

	// Phase 2: re-read every probe.
	for _, p := range probes {
		want := before[p.String()]
		gone := want.without(retracted)
		check := func(pin types.Instant, exp answer, what string) {
			t.Helper()
			got, _, err := p.fam.q(g, w, pin, p.v)
			if err != nil {
				t.Fatalf("after: %s pin=%d: %v", p, pin, err)
			}
			if !got.equal(exp) {
				t.Fatalf("%s at pin %d (%s; T=%d, captured at %d):\n  got:%s\n  want:%s", p, pin, what, T, p0, got, exp)
			}
		}
		if !p.fam.pinned {
			check(0, gone, "current belief: retracted entities absent at every valid time")
			continue
		}
		check(p0, want, "pin before T: byte-identical to before")
		check(T-1, want, "pin T-1: byte-identical to before")
		check(T, gone, "pin T: retracted entities absent at every valid time")
		check(pNow, gone, "pin now: retracted entities absent at every valid time")
	}
}

// newestNodeRow returns the highest-version history row of a deleted node.
func newestNodeRow(t *testing.T, g *Core, id types.NodeID) *types.Node {
	t.Helper()
	h, err := g.Nodes.History(id)
	if err != nil || len(h) == 0 {
		t.Fatalf("History(%v) = %d rows, %v", id, len(h), err)
	}
	best := h[0]
	for _, r := range h {
		if r.Version() > best.Version() {
			best = r
		}
	}
	return best
}

func newestRelRow(t *testing.T, g *Core, id types.RelID) *types.Relationship {
	t.Helper()
	h, err := g.Rels.History(id)
	if err != nil || len(h) == 0 {
		t.Fatalf("History(%v) = %d rows, %v", id, len(h), err)
	}
	best := h[0]
	for _, r := range h {
		if r.Version() > best.Version() {
			best = r
		}
	}
	return best
}
