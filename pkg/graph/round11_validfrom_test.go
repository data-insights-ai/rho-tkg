package graph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Round 11 item 2 (sigma-tkgd, 2026-10-17): explicit presence on the write
// side. A tkg_valid_from of 0 read as "no claim" (an untimed version, from
// -inf under UntimedUnbounded or from its TxFrom), so a version could not
// start at the instant 0. A types.ExplicitInstant value is a claim at every
// instant, 0 included; a plain 0 keeps today's meaning. A valid END of 0 is
// still "open" everywhere, so only an entity's first version may start at the
// explicit 0; a write that would end another version at 0 is refused with
// ErrValidEndAtZero.

type r11ZeroWorld struct {
	zero, plain, early types.NodeID
	zeroRel, plainRel  types.RelID
}

// r11Zero creates a node "Z" and a relationship "ZR" valid from the explicit
// instant 0, a node and relationship with a plain 0 (no claim) and a node
// valid from -100 (k=3).
func r11Zero(t *testing.T, g *graphpkg.Graph) r11ZeroWorld {
	t.Helper()
	ctx := context.Background()
	var w r11ZeroWorld
	hub, err := g.Nodes().Add(ctx, []string{"Hub"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	zero, err := g.Nodes().Add(ctx, []string{"Z"}, map[string]any{"k": int64(1), types.ShadowValidFrom: types.ExplicitInstant(0)})
	if err != nil {
		t.Fatalf("create valid from the explicit 0: %v", err)
	}
	w.zero = zero.ID()
	plain, err := g.Nodes().Add(ctx, []string{"Z"}, map[string]any{"k": int64(2), types.ShadowValidFrom: types.Instant(0)})
	if err != nil {
		t.Fatal(err)
	}
	w.plain = plain.ID()
	zr, err := g.Rels().Add(ctx, "ZR", hub, zero, map[string]any{types.ShadowValidFrom: types.ExplicitInstant(0)})
	if err != nil {
		t.Fatalf("rel valid from the explicit 0: %v", err)
	}
	w.zeroRel = zr.ID()
	pr, err := g.Rels().Add(ctx, "ZR", hub, plain, map[string]any{types.ShadowValidFrom: int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	w.plainRel = pr.ID()
	early, err := g.Nodes().Add(ctx, []string{"Z"}, map[string]any{"k": int64(3), types.ShadowValidFrom: types.Instant(-100)})
	if err != nil {
		t.Fatal(err)
	}
	w.early = early.ID()
	return w
}

// r11CheckZero asserts the answers of r11Zero's world on g; untimedFromMinusInf
// says whether a plain 0 reads from -inf (UntimedUnbounded) or from its TxFrom.
func r11CheckZero(t *testing.T, g *graphpkg.Graph, w r11ZeroWorld, untimedFromMinusInf, fresh bool, what string) {
	t.Helper()
	ctx := context.Background()
	at := func(id types.NodeID, ti types.Instant) (int64, bool) {
		n, err := g.Temporal().NodeAt(id, ti)
		if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
			return 0, false
		}
		if err != nil {
			t.Fatalf("%s: NodeAt(%d, %d): %v", what, id, ti, err)
		}
		v, _ := n.GetProperty("k")
		return v.(int64), true
	}
	if _, ok := at(w.zero, -1); ok {
		t.Errorf("%s: a node valid from the explicit 0 is valid at -1", what)
	}
	if k, ok := at(w.zero, 0); !ok || k != 1 {
		t.Errorf("%s: a node valid from the explicit 0 at 0: k=%d ok=%v, want 1", what, k, ok)
	}
	if _, ok := at(w.plain, -1); ok != untimedFromMinusInf {
		t.Errorf("%s: a plain 0 at -1: valid=%v, want %v (no claim, unchanged)", what, ok, untimedFromMinusInf)
	}
	if k, ok := at(w.early, 5); !ok || k != 3 {
		t.Errorf("%s: the node valid from -100 at 5: k=%d ok=%v, want 3 (refused writes changed nothing)", what, k, ok)
	}
	for _, c := range []struct {
		at   types.Instant
		want map[types.NodeID]bool
	}{
		{-1, map[types.NodeID]bool{w.zero: false, w.early: true, w.plain: untimedFromMinusInf}},
		{0, map[types.NodeID]bool{w.zero: true, w.early: true, w.plain: untimedFromMinusInf}},
	} {
		ns, err := g.Nodes().ByLabel("Z", storepkg.QueryOpts{ValidAt: c.at, ValidAtSet: true})
		if err != nil {
			t.Fatal(err)
		}
		got := map[types.NodeID]bool{}
		for _, n := range ns {
			got[n.ID()] = true
		}
		for id, want := range c.want {
			if got[id] != want {
				t.Errorf("%s: ByLabel(Z, ValidAt %d) lists %d = %v, want %v", what, c.at, id, got[id], want)
			}
		}
	}
	ns, err := g.Nodes().ByLabelAndProperty("Z", "k", int64(1), storepkg.QueryOpts{ValidStart: -10, ValidEnd: 0, ValidWindowSet: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(ns) != 0 {
		t.Errorf("%s: ByLabelAndProperty(k=1, window [-10, 0)) lists the node valid from the explicit 0", what)
	}
	// The untimed option of the count timeline starts an untimed create at
	// -inf, never a create valid from the explicit 0.
	if fresh { // after the refusals the current rows moved (a correction's resumption from 60)
		r11CheckUntimedCount(t, g, what)
	}
	rels, err := g.Rels().ByType("ZR", storepkg.QueryOpts{ValidAt: -1, ValidAtSet: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rels {
		if r.ID() == w.zeroRel {
			t.Errorf("%s: a relationship valid from the explicit 0 is valid at -1", what)
		}
	}
	if _, err := g.Temporal().RelAt(w.zeroRel, 0); err != nil {
		t.Errorf("%s: RelAt(explicit 0 rel, 0): %v", what, err)
	}
	now, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	segs, err := g.Temporal().NodeEffectiveTimeline(w.zero, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) == 0 || segs[0].ValidFrom != 0 {
		t.Errorf("%s: effective timeline of the explicit-0 node: %+v, want it to start at 0", what, segs)
	}
	// Members starting at 0 while others hold: a count segment ends at the
	// instant 0 (ToZero), only the last one is open.
	cnt, exact, err := g.Nodes().CountTimelineByLabel("Z", now, storepkg.CountTimelineOpts{})
	if err != nil || !exact {
		t.Fatalf("%s: count timeline: %v exact=%v", what, err, exact)
	}
	sawZero := false
	for i, seg := range cnt {
		last := i == len(cnt)-1
		if seg.To == 0 && !seg.ToZero && !last {
			t.Errorf("%s: count segment %d %+v reads as open but is not the last: %+v", what, i, seg, cnt)
		}
		sawZero = sawZero || seg.ToZero
		if !last && cnt[i+1].From != seg.To {
			t.Errorf("%s: count segments %d and %d not contiguous: %+v", what, i, i+1, cnt)
		}
	}
	if !sawZero {
		t.Errorf("%s: no count segment ends at the instant 0: %+v", what, cnt)
	}
	if len(cnt) == 0 || cnt[len(cnt)-1].To != 0 || cnt[len(cnt)-1].ToZero {
		t.Errorf("%s: count timeline does not end open: %+v", what, cnt)
	}
	if hull, ok, err := g.Nodes().ValidHullByLabel("Z", now, storepkg.CountTimelineOpts{}); err != nil || !ok || !hull.OpenEnd {
		t.Errorf("%s: hull %+v ok=%v err=%v, want an open end", what, hull, ok, err)
	}
	if !fresh {
		return // a correction moved the current slot (its rows start at 50 and 60)
	}
	n, err := g.Nodes().Get(ctx, w.zero)
	if err != nil {
		t.Fatal(err)
	}
	if vf, ok := n.ValidFromClaim(); !ok || vf != 0 {
		t.Errorf("%s: ValidFromClaim of the explicit-0 node = %d, %v; want 0, true", what, vf, ok)
	}
	if tm := n.Temporal(); !tm.HasValidFrom() || !tm.ValidFromZero {
		t.Errorf("%s: metadata of the explicit-0 node: %+v", what, tm)
	}
	p, err := g.Nodes().Get(ctx, w.plain)
	if err != nil {
		t.Fatal(err)
	}
	if vf, ok := p.ValidFromClaim(); ok && vf == 0 {
		t.Errorf("%s: a plain 0 states a claim at 0", what)
	}
}

// TestRound11ExplicitValidFromZero: on every backend and both untimed modes a
// version created with tkg_valid_from = types.ExplicitInstant(0) is valid
// from the instant 0 (not at -1, at 0), through the point, generic, window,
// relationship, timeline and count doors; a plain 0 keeps its meaning; every
// write that would end a version at 0 is refused and changes nothing.
func TestRound11ExplicitValidFromZero(t *testing.T) {
	for _, minusInf := range []bool{true, false} {
		mut := func(c *graphpkg.Config) {
			c.AllowTxBackfill = true
			if !minusInf {
				txFromCfg(c)
			}
		}
		for _, b := range allStoreBackendsWith(mut) {
			name := b.name
			if !minusInf {
				name += "-txfrom"
			}
			t.Run(name, func(t *testing.T) {
				g := b.open(t)
				w := r11Zero(t, g)
				r11CheckZero(t, g, w, minusInf, true, name)
				r11RefusedAtZero(t, g, w, name)
				r11CheckZero(t, g, w, minusInf, false, name+" after the refusals")
			})
		}
	}
}

// r11RefusedAtZero: every write that would end a version at the instant 0 is
// refused with ErrValidEndAtZero (also ErrInvalidTimeRange): an update (node
// and relationship, versioned, in place, with a caller instant, batched), a
// re-import of a deleted ID, a correction or an excise crossing 0 on an
// entity valid from the explicit 0. Corrections that do not cross 0 are
// accepted.
func r11RefusedAtZero(t *testing.T, g *graphpkg.Graph, w r11ZeroWorld, what string) {
	t.Helper()
	ctx := context.Background()
	zero := func() map[string]any { return map[string]any{types.ShadowValidFrom: types.ExplicitInstant(0)} }
	refused := func(op string, err error) {
		t.Helper()
		if !errors.Is(err, graphpkg.ErrValidEndAtZero) || !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
			t.Errorf("%s: %s: %v, want ErrValidEndAtZero (wrapping ErrInvalidTimeRange)", what, op, err)
		}
	}
	_, err := g.Nodes().Update(ctx, w.early, map[string]any{"k": int64(4), types.ShadowValidFrom: types.ExplicitInstant(0)})
	refused("node update", err)
	_, err = g.Nodes().UpdateInPlace(ctx, w.early, zero())
	refused("node update in place", err)
	_, err = g.Rels().Update(ctx, w.plainRel, zero())
	refused("relationship update", err)
	now, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.Nodes().UpdateWithTx(ctx, w.early, zero(), now+1)
	refused("node update with a caller instant", err)
	b, err := g.Batch().New()
	if err != nil {
		t.Fatal(err)
	}
	refused("batch node update (queue)", b.UpdateNode(w.early, zero()))
	_, err = g.Temporal().SetNodeVersionInterval(ctx, w.zero, -10, 10, map[string]any{"k": int64(9)})
	refused("correction crossing 0", err)
	_, err = g.Temporal().SetNodeVersionInterval(ctx, w.zero, -10, 0, map[string]any{"k": int64(9)})
	refused("open correction from before 0", err)
	refused("excise crossing 0", g.Temporal().ExciseNodeValidity(ctx, w.zero, -5, 5))
	_, err = g.Temporal().SetRelVersionInterval(ctx, w.zeroRel, -10, 10, nil)
	refused("relationship correction crossing 0", err)
	gone, err := g.Nodes().Add(ctx, []string{"G"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Nodes().Delete(ctx, gone.ID()); err != nil {
		t.Fatal(err)
	}
	_, err = g.Nodes().Import(ctx, gone.ID(), []string{"G"}, zero())
	refused("re-import of a deleted ID", err)
	// The caller-instant unit pre-flights the refusal before any write: an
	// earlier op of the unit (an update of another node) is not applied.
	if bb, err := g.Batch().New(); err != nil {
		t.Fatal(err)
	} else {
		time.Sleep(3 * time.Millisecond) // a caller instant after every recorded one, not in the future
		now, _ := g.Temporal().NowTx()
		if err := bb.UpdateNodeWithTx(w.plain, map[string]any{"k": int64(77)}, now); err != nil {
			t.Fatal(err)
		}
		if err := bb.SetNodeVersionIntervalWithTx(w.zero, -10, 10, map[string]any{"k": int64(9)}, now); err != nil {
			refused("batched caller-instant correction crossing 0 (queue)", err)
		} else {
			_, err := bb.Execute()
			refused("batched caller-instant correction crossing 0", err)
		}
		if n, err := g.Nodes().Get(ctx, w.plain); err != nil {
			t.Fatal(err)
		} else if k, _ := n.GetProperty("k"); k != int64(2) {
			t.Errorf("%s: the refused unit applied its other op (k=%v)", what, k)
		}
	}
	// Not crossing 0: accepted.
	if _, err := g.Temporal().SetNodeVersionInterval(ctx, w.zero, 50, 60, map[string]any{"k": int64(1)}); err != nil {
		t.Errorf("%s: a correction after 0 on the explicit-0 node: %v", what, err)
	}
	// The current slot moved off version 0: a crossing correction is still
	// refused (version 0 decides).
	_, err = g.Temporal().SetNodeVersionInterval(ctx, w.zero, -10, 10, map[string]any{"k": int64(9)})
	refused("correction crossing 0 after the current slot moved", err)
	// A cascade chain (a correction made it non-monotonic), then a close: the
	// close row starts at 0 and ends every older belief there (a cap of 0 is
	// the instant 0, not "no cap").
	casc, err := g.Nodes().Add(ctx, []string{"C"}, map[string]any{"k": int64(1), types.ShadowValidFrom: types.ExplicitInstant(0)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Temporal().SetNodeVersionInterval(ctx, casc.ID(), 50, 60, map[string]any{"k": int64(9)}); err != nil {
		t.Fatal(err)
	}
	if err := g.Nodes().CloseVersion(ctx, casc.ID(), 100); err != nil {
		t.Fatalf("%s: close after a correction: %v", what, err)
	}
	for _, c := range []struct {
		at    types.Instant
		valid bool
	}{{-1, false}, {10, true}, {55, true}, {99, true}, {100, false}, {150, false}} {
		_, err := g.Temporal().NodeAt(casc.ID(), c.at)
		if (err == nil) != c.valid {
			t.Errorf("%s: corrected and closed explicit-0 node at %d: %v, want valid=%v", what, c.at, err, c.valid)
		}
	}
	// A correction wholly before 0 leaves version 0 current in a cascade
	// chain; its close row starts at 0 and caps version 0 there (empty).
	below, err := g.Nodes().Add(ctx, []string{"C"}, map[string]any{"k": int64(1), types.ShadowValidFrom: types.ExplicitInstant(0)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Temporal().SetNodeVersionInterval(ctx, below.ID(), -60, -50, map[string]any{"k": int64(8)}); err != nil {
		t.Fatal(err)
	}
	if err := g.Nodes().CloseVersion(ctx, below.ID(), 100); err != nil {
		t.Fatalf("%s: close after a correction before 0: %v", what, err)
	}
	for _, c := range []struct {
		at    types.Instant
		valid bool
	}{{-55, true}, {-40, false}, {-1, false}, {0, true}, {99, true}, {100, false}, {150, false}} {
		_, err := g.Temporal().NodeAt(below.ID(), c.at)
		if (err == nil) != c.valid {
			t.Errorf("%s: explicit-0 node corrected before 0 and closed, at %d: %v, want valid=%v", what, c.at, err, c.valid)
		}
	}
	// A close of a version valid from the explicit 0 keeps its start.
	closing, err := g.Nodes().Add(ctx, []string{"C"}, zero())
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Nodes().CloseVersion(ctx, closing.ID(), 100); err != nil {
		t.Fatalf("%s: close of an explicit-0 version: %v", what, err)
	}
	if _, err := g.Temporal().NodeAt(closing.ID(), -1); !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Errorf("%s: the closed explicit-0 version at -1: %v, want ErrNoVersionValidAt", what, err)
	}
	if _, err := g.Temporal().NodeAt(closing.ID(), 50); err != nil {
		t.Errorf("%s: the closed explicit-0 version at 50: %v", what, err)
	}
	if _, err := g.Temporal().NodeAt(closing.ID(), 100); !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Errorf("%s: the closed explicit-0 version at 100: %v, want ErrNoVersionValidAt", what, err)
	}
	if _, err := g.Temporal().SetNodeVersionInterval(ctx, w.early, -60, -50, map[string]any{"k": int64(3)}); err != nil {
		t.Errorf("%s: a correction before 0 on a node valid from -100: %v", what, err)
	}
}

// TestRound11ExplicitValidFromZeroOtherKeys: an explicit instant is a claim
// of a valid start only; on tkg_valid_to, tkg_created_at or tkg_tx_from it is
// refused with ErrInvalidTimeRange (the instant 0 there stays "unset"/"open",
// stated, not silently read). A non-zero explicit instant is the plain claim.
func TestRound11ExplicitValidFromZeroOtherKeys(t *testing.T) {
	g := allStoreBackends()[0].open(t)
	ctx := context.Background()
	for _, key := range []string{types.ShadowValidTo, types.ShadowCreatedAt, types.ShadowTxFrom} {
		_, err := g.Nodes().Add(ctx, []string{"Z"}, map[string]any{key: types.ExplicitInstant(5)})
		if !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
			t.Errorf("%s = ExplicitInstant(5): %v, want ErrInvalidTimeRange", key, err)
		}
	}
	n, err := g.Nodes().Add(ctx, []string{"Z"}, map[string]any{types.ShadowValidFrom: types.ExplicitInstant(7)})
	if err != nil {
		t.Fatal(err)
	}
	if tm := n.Temporal(); tm.ValidFrom != 7 || tm.ValidFromZero {
		t.Errorf("ExplicitInstant(7): %+v, want ValidFrom 7 without the zero marker", tm)
	}
}

// TestRound11ExplicitValidFromZeroReopen: on the durable backends a row valid
// from the explicit 0 keeps its claim across a reopen, and the store's
// format marker is raised so a release that cannot read such rows refuses
// the store at open (played by capping the readable marker at 7).
func TestRound11ExplicitValidFromZeroReopen(t *testing.T) {
	for _, b := range uvsDurable {
		t.Run(b.name, func(t *testing.T) {
			dir := t.TempDir()
			g, _ := uvsMustOpen(t, b, dir, graphpkg.UntimedUnbounded)
			ctx := context.Background()
			n, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{types.ShadowValidFrom: types.ExplicitInstant(0)})
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			g, _ = uvsMustOpen(t, b, dir, graphpkg.UntimedUnbounded)
			got, err := g.Nodes().Get(ctx, n.ID())
			if err != nil {
				t.Fatal(err)
			}
			if vf, ok := got.ValidFromClaim(); !ok || vf != 0 {
				t.Errorf("after a reopen: ValidFromClaim = %d, %v; want 0, true", vf, ok)
			}
			if _, err := g.Temporal().NodeAt(n.ID(), -1); !errors.Is(err, storepkg.ErrNoVersionValidAt) {
				t.Errorf("after a reopen: NodeAt(-1) = %v, want ErrNoVersionValidAt", err)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			restore := badger.SetMaxReadableStoreFormatVersionForTest(7)
			defer restore()
			if err := r11OpenDurable(b.name, dir); !errors.Is(err, storepkg.ErrWireFormatVersionUnsupported) {
				t.Errorf("a reader of markers up to 7 opened the store: %v, want ErrWireFormatVersionUnsupported", err)
			}
		})
	}
}

// r11OpenDurable opens and closes the durable store uvsDurable names, with
// its configuration, returning the open's error.
func r11OpenDurable(name, dir string) error {
	var st storepkg.MandatoryStore
	var err error
	switch name {
	case "badger":
		st, err = badger.New(badger.Config{Dir: dir, TemporalIndexOnDisk: true})
	case "tiered":
		st, err = tiered.New(tiered.Config{DataDir: dir, ShardWindow: 7 * 24 * time.Hour, RefLabels: []string{"Ref"}})
	case "sharded":
		st, err = sharded.New(sharded.Config{Dir: dir, BaseSlot: 0, SlotCount: 2})
	default:
		return fmt.Errorf("unknown durable backend %q", name)
	}
	if err != nil {
		return err
	}
	return st.Close()
}

// TestRound11ExplicitValidFromZeroColumns: the typed column scans mark a row
// valid from the explicit 0 (ColumnBatch / RelColumnBatch .ValidFromZero),
// leave a plain 0 unmarked, and their valid-time filter reads the claim (not
// valid at -1), on every backend that serves them, through the row path, the
// badger column snapshot and the state cut at a pin.
func TestRound11ExplicitValidFromZeroColumns(t *testing.T) {
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			w := r11Zero(t, g)
			now, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range []struct {
				name     string
				opts     storepkg.QueryOpts
				wantZero bool // the explicit-0 node is in the scan
			}{
				{"current", storepkg.QueryOpts{}, true},
				{"at -1", storepkg.QueryOpts{ValidAt: -1, ValidAtSet: true}, false},
				{"at 0", storepkg.QueryOpts{ValidAt: 0, ValidAtSet: true}, true},
				{"state cut at 0", storepkg.QueryOpts{ValidAt: 0, ValidAtSet: true, TxAt: now, TxAtSet: true}, true},
			} {
				seen := map[types.NodeID]bool{}
				ok, err := g.ScanNodeColumns("Z", []string{"k"}, c.opts, func(batch *graphpkg.ColumnBatch) bool {
					for i, id := range batch.IDs {
						z := i < len(batch.ValidFromZero) && batch.ValidFromZero[i]
						seen[id] = true
						if z != (id == w.zero) {
							t.Errorf("%s: row %d (id %d, ValidFrom %d): ValidFromZero %v", c.name, i, id, batch.ValidFrom[i], z)
						}
						if id == w.zero && batch.ValidFrom[i] != 0 {
							t.Errorf("%s: the explicit-0 row reports ValidFrom %d", c.name, batch.ValidFrom[i])
						}
					}
					return true
				})
				if err != nil {
					t.Fatalf("%s: %v", c.name, err)
				}
				if !ok {
					continue // the backend serves no column scan for this form
				}
				if seen[w.zero] != c.wantZero {
					t.Errorf("%s: explicit-0 node in the scan = %v, want %v", c.name, seen[w.zero], c.wantZero)
				}
			}
			seenRel := false
			ok, err := g.ScanRelColumns("ZR", nil, storepkg.QueryOpts{}, func(batch *graphpkg.RelColumnBatch) bool {
				for i, id := range batch.IDs {
					z := i < len(batch.ValidFromZero) && batch.ValidFromZero[i]
					if z != (id == w.zeroRel) {
						t.Errorf("rels: row %d (id %d): ValidFromZero %v", i, id, z)
					}
					seenRel = seenRel || id == w.zeroRel
				}
				return true
			})
			if err != nil {
				t.Fatal(err)
			}
			if ok && !seenRel {
				t.Error("rels: the explicit-0 relationship is not in the scan")
			}
		})
	}
}

// TestRound11ExplicitValidFromZeroEnvelopesAndConversion: the start a row
// valid from the explicit 0 states reaches the store push-down and the
// temporal index envelopes (a label with a temporal index prunes by them: the
// row must not be pruned at 0 nor kept at -1), and a conversion of an
// UntimedFromTxFrom store to UntimedUnbounded restamps untimed first versions
// only, never a create valid from the explicit 0.
func TestRound11ExplicitValidFromZeroEnvelopesAndConversion(t *testing.T) {
	for _, b := range allStoreBackendsWith(txFromCfg) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			if err := g.Index().CreateTemporal("T"); err != nil {
				t.Fatal(err)
			}
			if err := g.Index().CreateProperty("T", "k"); err != nil {
				t.Fatal(err)
			}
			zero, err := g.Nodes().Add(ctx, []string{"T"}, map[string]any{"k": int64(1), types.ShadowValidFrom: types.ExplicitInstant(0)})
			if err != nil {
				t.Fatal(err)
			}
			untimed, err := g.Nodes().Add(ctx, []string{"T"}, map[string]any{"k": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			check := func(when string, untimedAtMinus1 bool) {
				t.Helper()
				for _, c := range []struct {
					at   types.Instant
					want map[types.NodeID]bool
				}{
					{-1, map[types.NodeID]bool{zero.ID(): false, untimed.ID(): untimedAtMinus1}},
					{0, map[types.NodeID]bool{zero.ID(): true, untimed.ID(): untimedAtMinus1}},
				} {
					for door, read := range map[string]func() ([]*types.Node, error){
						"ByLabel": func() ([]*types.Node, error) {
							return g.Nodes().ByLabel("T", storepkg.QueryOpts{ValidAt: c.at, ValidAtSet: true})
						},
						"ByLabelAndProperty": func() ([]*types.Node, error) {
							return g.Nodes().ByLabelAndProperty("T", "k", int64(1), storepkg.QueryOpts{ValidAt: c.at, ValidAtSet: true})
						},
						"NodesByLabelAt": func() ([]*types.Node, error) { return g.Temporal().NodesByLabelAt("T", c.at) },
					} {
						ns, err := read()
						if err != nil {
							t.Fatal(err)
						}
						got := map[types.NodeID]bool{}
						for _, n := range ns {
							got[n.ID()] = true
						}
						for id, want := range c.want {
							if got[id] != want {
								t.Errorf("%s: %s at %d lists %d = %v, want %v", when, door, c.at, id, got[id], want)
							}
						}
					}
				}
			}
			check("UntimedFromTxFrom", false)
			if err := g.Admin().SetUntimedValidStart(ctx, graphpkg.UntimedUnbounded); err != nil {
				t.Fatalf("conversion: %v", err)
			}
			check("after the conversion to UntimedUnbounded", true)
			got, err := g.Nodes().Get(ctx, zero.ID())
			if err != nil {
				t.Fatal(err)
			}
			if vf, ok := got.ValidFromClaim(); !ok || vf != 0 {
				t.Errorf("after the conversion: ValidFromClaim = %d, %v; want 0, true", vf, ok)
			}
		})
	}
}

// r11CheckUntimedCount: the untimed option of the count timeline starts an
// untimed create at -inf, never a create valid from the explicit 0.
func r11CheckUntimedCount(t *testing.T, g *graphpkg.Graph, what string) {
	t.Helper()
	cntU, ok, err := g.Nodes().CountTimelineByLabel("Z", 0, storepkg.CountTimelineOpts{UntimedFromMinusInf: true, CurrentRows: true})
	if err != nil || !ok {
		t.Fatalf("%s: current-rows timeline: %v %v", what, ok, err)
	}
	at := func(x types.Instant) int64 {
		for _, s := range cntU {
			if s.From <= x && ((s.To == 0 && !s.ToZero) || x < s.To) {
				return s.Count
			}
		}
		return 0
	}
	if at(0)-at(-1) < 1 {
		t.Errorf("%s: current-rows timeline with the untimed option: the explicit-0 member does not start at 0 (%+v)", what, cntU)
	}
}
