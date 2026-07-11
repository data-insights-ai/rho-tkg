package core

// Belief-state pin (QueryOpts.TxPin) tests.
//
// TxPin routes the generic scan doors (ByLabel / ByType / All) through the SAME
// as-of resolver the named door (NodesAsOf / RelsAsOf) uses, with NO valid-time
// filtering. These tests pin the two contracts the WP promises:
//
//   1. Two-door equivalence: ByLabel{TxPin:T} ID/version set == NodesAsOf(T)
//      filtered by label; ByType vs RelsAsOf; All vs NodesAsOf/RelsAsOf — on
//      memory AND badger, over a state exercising every lifecycle shape
//      (updated-after-pin, deleted-after-pin visible-at-pin, deleted-before-pin
//      absent, cascade-corrected around the pin, backfilled via AddWithTx).
//   2. The RT-2 footgun, asserted explicitly: an entity valid only in the past
//      is MISSED by QueryOpts{TxAt: now} (the documented wall-now valid filter)
//      but RETURNED by QueryOpts{TxPin: now}.

import (
	"context"
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func txPinBackends(t *testing.T) []struct {
	name string
	cfg  Config
} {
	t.Helper()
	return []struct {
		name string
		cfg  Config
	}{
		{"memory", Config{AllowTxBackfill: true}},
		{"badger", Config{BadgerInMemory: true, AllowTxBackfill: true}},
	}
}

func nodeVerMap(ns []*types.Node) map[types.NodeID]uint32 {
	m := make(map[types.NodeID]uint32, len(ns))
	for _, n := range ns {
		m[n.ID()] = n.Version()
	}
	return m
}

func relVerMap(rs []*types.Relationship) map[types.RelID]uint32 {
	m := make(map[types.RelID]uint32, len(rs))
	for _, r := range rs {
		m[r.ID()] = r.Version()
	}
	return m
}

func nodeVerEqual(a, b map[types.NodeID]uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func relVerEqual(a, b map[types.RelID]uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// TestTxPinEqualsNamedAsOfDoor drives a state exercising every lifecycle shape,
// captures several knowledge-time pins as the world evolves, then asserts the
// generic TxPin door equals the named as-of door at each pin — on both backends.
func TestTxPinEqualsNamedAsOfDoor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, be := range txPinBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			g, err := New(be.cfg)
			if err != nil {
				t.Fatalf("New(%s): %v", be.name, err)
			}
			defer g.Close()

			pin := func() types.Instant {
				p, err := g.Temporal.NowTx()
				if err != nil {
					t.Fatalf("NowTx: %v", err)
				}
				return p
			}

			mustAddNode := func(labels []string, props map[string]any) types.NodeID {
				n, err := g.Nodes.Add(ctx, labels, props)
				if err != nil {
					t.Fatalf("add node %v: %v", labels, err)
				}
				return n.ID()
			}

			// Two stable anchors for relationship endpoints (never mutated).
			a1 := mustAddNode([]string{"Anchor"}, map[string]any{"tkg_valid_from": types.Instant(100)})
			a2 := mustAddNode([]string{"Anchor"}, map[string]any{"tkg_valid_from": types.Instant(100)})

			// nUpd: created, updated AFTER an early pin.
			nUpd := mustAddNode([]string{"A", "B"}, map[string]any{"tkg_valid_from": types.Instant(200)})
			// nDel: created, deleted AFTER an early pin (visible at that pin).
			nDel := mustAddNode([]string{"A"}, map[string]any{"tkg_valid_from": types.Instant(200)})
			// nCascade: created, then an append-only cascade correction.
			nCascade := mustAddNode([]string{"C"}, map[string]any{"tkg_valid_from": types.Instant(200)})
			// nBackfill: knowledge-time backdated via AddWithTx.
			nbf, err := g.Nodes.AddWithTx(ctx, []string{"B"}, map[string]any{"tkg_valid_from": types.Instant(200)}, types.Instant(500))
			if err != nil {
				t.Fatalf("AddWithTx nBackfill: %v", err)
			}
			nBackfill := nbf.ID()

			rk, err := g.Rels.AddByID(ctx, "R", a1, a2, map[string]any{"tkg_valid_from": types.Instant(200)})
			if err != nil {
				t.Fatalf("add rKeep: %v", err)
			}
			rKeep := rk.ID()
			rd, err := g.Rels.AddByID(ctx, "S", a2, a1, map[string]any{"tkg_valid_from": types.Instant(200)})
			if err != nil {
				t.Fatalf("add rDel: %v", err)
			}
			rDel := rd.ID()

			pinEarly := pin() // nUpd@v0, nDel alive, nCascade@v0, rDel alive

			// --- Mutations after pinEarly ---
			if _, err := g.Nodes.Update(ctx, nUpd, map[string]any{"tkg_valid_from": types.Instant(3000), "k": 1}); err != nil {
				t.Fatalf("update nUpd: %v", err)
			}
			if _, err := g.Temporal.SetNodeVersionInterval(ctx, nCascade, types.Instant(150), types.Instant(2500), map[string]any{"k": 2}); err != nil {
				t.Fatalf("cascade nCascade: %v", err)
			}
			if err := g.Nodes.AddLabel(ctx, nUpd, "C"); err != nil {
				t.Fatalf("addLabel nUpd: %v", err)
			}
			if err := g.Nodes.Delete(ctx, nDel); err != nil {
				t.Fatalf("delete nDel: %v", err)
			}
			if err := g.Rels.Delete(ctx, rDel); err != nil {
				t.Fatalf("delete rDel: %v", err)
			}

			pinLate := pin() // after all mutations & deletes

			// Far-future pin: every entity's newest belief.
			pinFuture := pinLate + 1_000_000

			for _, tc := range []struct {
				name string
				at   types.Instant
			}{
				{"early", pinEarly},
				{"late", pinLate},
				{"future", pinFuture},
			} {
				assertTxPinMatchesAsOf(t, ctx, g, be.name, tc.name, tc.at)
			}

			_ = nBackfill
			_ = rKeep
		})
	}
}

// assertTxPinMatchesAsOf checks All/ByLabel/ByType{TxPin:at} against the named
// as-of door output at the same pin.
func assertTxPinMatchesAsOf(t *testing.T, ctx context.Context, g *Core, backend, pinName string, at types.Instant) {
	t.Helper()

	// --- Named door: the reference belief state at `at`. ---
	asOfNodes, err := g.Temporal.NodesAsOf(at)
	if err != nil {
		t.Fatalf("[%s/%s] NodesAsOf(%d): %v", backend, pinName, at, err)
	}
	asOfRels, err := g.Temporal.RelsAsOf(at)
	if err != nil {
		t.Fatalf("[%s/%s] RelsAsOf(%d): %v", backend, pinName, at, err)
	}

	// --- Generic All door ---
	genNodes, err := g.Nodes.All(storepkg.QueryOpts{TxPin: at})
	if err != nil {
		t.Fatalf("[%s/%s] Nodes.All{TxPin}: %v", backend, pinName, err)
	}
	if want, got := nodeVerMap(asOfNodes), nodeVerMap(genNodes); !nodeVerEqual(want, got) {
		t.Fatalf("[%s/%s] Nodes.All{TxPin:%d} != NodesAsOf: want %v got %v", backend, pinName, at, want, got)
	}
	genRels, err := g.Rels.All(storepkg.QueryOpts{TxPin: at})
	if err != nil {
		t.Fatalf("[%s/%s] Rels.All{TxPin}: %v", backend, pinName, err)
	}
	if want, got := relVerMap(asOfRels), relVerMap(genRels); !relVerEqual(want, got) {
		t.Fatalf("[%s/%s] Rels.All{TxPin:%d} != RelsAsOf: want %v got %v", backend, pinName, at, want, got)
	}

	// --- ByLabel: NodesAsOf filtered to the label ---
	for _, label := range []string{"Anchor", "A", "B", "C"} {
		want := map[types.NodeID]uint32{}
		for _, n := range asOfNodes {
			for _, l := range g.Nodes.Labels(n) {
				if l == label {
					want[n.ID()] = n.Version()
				}
			}
		}
		got, err := g.Nodes.ByLabel(label, storepkg.QueryOpts{TxPin: at})
		if err != nil {
			t.Fatalf("[%s/%s] ByLabel(%s){TxPin}: %v", backend, pinName, label, err)
		}
		if gm := nodeVerMap(got); !nodeVerEqual(want, gm) {
			t.Fatalf("[%s/%s] ByLabel(%s){TxPin:%d} != NodesAsOf|%s: want %v got %v",
				backend, pinName, label, at, label, want, gm)
		}
	}

	// --- ByType: RelsAsOf filtered to the type ---
	relType := map[types.RelID]string{}
	for _, r := range asOfRels {
		relType[r.ID()] = g.Rels.Type(r)
	}
	for _, typ := range []string{"R", "S"} {
		want := map[types.RelID]uint32{}
		for _, r := range asOfRels {
			if relType[r.ID()] == typ {
				want[r.ID()] = r.Version()
			}
		}
		got, err := g.Rels.ByType(typ, storepkg.QueryOpts{TxPin: at})
		if err != nil {
			t.Fatalf("[%s/%s] ByType(%s){TxPin}: %v", backend, pinName, typ, err)
		}
		if gm := relVerMap(got); !relVerEqual(want, gm) {
			t.Fatalf("[%s/%s] ByType(%s){TxPin:%d} != RelsAsOf|%s: want %v got %v",
				backend, pinName, typ, at, typ, want, gm)
		}
	}
}

// TestTxPinFootgunPastValidFact is the RT-2 case asserted directly: an entity
// whose fact is valid only in the PAST (explicit tkg_valid_to before now) is
// MISSED by a naive QueryOpts{TxAt: now} (the documented wall-now valid filter)
// yet RETURNED by QueryOpts{TxPin: now}. Node and relationship mirrors.
func TestTxPinFootgunPastValidFact(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, be := range txPinBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			g, err := New(be.cfg)
			if err != nil {
				t.Fatalf("New(%s): %v", be.name, err)
			}
			defer g.Close()

			// A fact valid only in the distant past (1970): valid_from=1000,
			// valid_to=2000 ms. Well below wall-clock milliseconds.
			pastFact := map[string]any{
				"tkg_valid_from": types.Instant(1000),
				"tkg_valid_to":   types.Instant(2000),
			}
			pn, err := g.Nodes.Add(ctx, []string{"Past"}, pastFact)
			if err != nil {
				t.Fatalf("add past node: %v", err)
			}
			n := pn.ID()
			a1n, err := g.Nodes.Add(ctx, []string{"Anchor"}, nil)
			if err != nil {
				t.Fatalf("add anchor1: %v", err)
			}
			a2n, err := g.Nodes.Add(ctx, []string{"Anchor"}, nil)
			if err != nil {
				t.Fatalf("add anchor2: %v", err)
			}
			pr, err := g.Rels.AddByID(ctx, "PAST", a1n.ID(), a2n.ID(), pastFact)
			if err != nil {
				t.Fatalf("add past rel: %v", err)
			}
			r := pr.ID()

			now, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}

			// TxAt-only door: the wall-now valid filter drops the past fact.
			txAtNodes, err := g.Nodes.ByLabel("Past", storepkg.QueryOpts{TxAt: now})
			if err != nil {
				t.Fatalf("ByLabel{TxAt}: %v", err)
			}
			if _, present := nodeVerMap(txAtNodes)[n]; present {
				t.Fatalf("QueryOpts{TxAt} unexpectedly returned the past-valid node — footgun contract broken")
			}
			txAtRels, err := g.Rels.ByType("PAST", storepkg.QueryOpts{TxAt: now})
			if err != nil {
				t.Fatalf("ByType{TxAt}: %v", err)
			}
			if _, present := relVerMap(txAtRels)[r]; present {
				t.Fatalf("QueryOpts{TxAt} unexpectedly returned the past-valid rel — footgun contract broken")
			}

			// TxPin door: the belief state includes the past-valid fact.
			txPinNodes, err := g.Nodes.ByLabel("Past", storepkg.QueryOpts{TxPin: now})
			if err != nil {
				t.Fatalf("ByLabel{TxPin}: %v", err)
			}
			if _, present := nodeVerMap(txPinNodes)[n]; !present {
				t.Fatalf("QueryOpts{TxPin} did NOT return the past-valid node — belief-state pin broken")
			}
			txPinRels, err := g.Rels.ByType("PAST", storepkg.QueryOpts{TxPin: now})
			if err != nil {
				t.Fatalf("ByType{TxPin}: %v", err)
			}
			if _, present := relVerMap(txPinRels)[r]; !present {
				t.Fatalf("QueryOpts{TxPin} did NOT return the past-valid rel — belief-state pin broken")
			}

			// All-door mirror on both fields.
			allTxAt, err := g.Nodes.All(storepkg.QueryOpts{TxAt: now})
			if err != nil {
				t.Fatalf("All{TxAt}: %v", err)
			}
			if _, present := nodeVerMap(allTxAt)[n]; present {
				t.Fatalf("Nodes.All{TxAt} unexpectedly returned the past-valid node")
			}
			allTxPin, err := g.Nodes.All(storepkg.QueryOpts{TxPin: now})
			if err != nil {
				t.Fatalf("All{TxPin}: %v", err)
			}
			if _, present := nodeVerMap(allTxPin)[n]; !present {
				t.Fatalf("Nodes.All{TxPin} did NOT return the past-valid node")
			}
		})
	}
}

// TestTxPinConflictSentinel checks that TxPin combined with any other temporal
// filter fails with ErrConflictingTemporalOpts through every generic door.
func TestTxPinConflictSentinel(t *testing.T) {
	t.Parallel()

	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	conflicts := []struct {
		name string
		opts storepkg.QueryOpts
	}{
		{"TxPin+ValidAt", storepkg.QueryOpts{TxPin: 100, ValidAt: 200}},
		{"TxPin+Interval", storepkg.QueryOpts{TxPin: 100, ValidStart: 50, ValidEnd: 200}},
		{"TxPin+TxAt", storepkg.QueryOpts{TxPin: 100, TxAt: 200}},
	}
	for _, c := range conflicts {
		if _, err := g.Nodes.All(c.opts); !errors.Is(err, ErrConflictingTemporalOpts) {
			t.Errorf("Nodes.All(%s) = %v, want ErrConflictingTemporalOpts", c.name, err)
		}
		if _, err := g.Rels.All(c.opts); !errors.Is(err, ErrConflictingTemporalOpts) {
			t.Errorf("Rels.All(%s) = %v, want ErrConflictingTemporalOpts", c.name, err)
		}
		if _, err := g.Nodes.ByLabel("X", c.opts); !errors.Is(err, ErrConflictingTemporalOpts) {
			t.Errorf("Nodes.ByLabel(%s) = %v, want ErrConflictingTemporalOpts", c.name, err)
		}
		if _, err := g.Rels.ByType("Y", c.opts); !errors.Is(err, ErrConflictingTemporalOpts) {
			t.Errorf("Rels.ByType(%s) = %v, want ErrConflictingTemporalOpts", c.name, err)
		}
	}

	// A lone TxPin (no conflicting field) must be accepted.
	if _, err := g.Nodes.All(storepkg.QueryOpts{TxPin: 100}); err != nil {
		t.Errorf("Nodes.All{TxPin alone} = %v, want nil", err)
	}
}
