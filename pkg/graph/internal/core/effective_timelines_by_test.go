package core

// Row 105 (sigma O1-3): RelEffectiveTimelinesByProperties — per probe tuple
// the effective timelines at a pin of the relationships of one type whose rows
// carry the tuple's values, cut to the valid window. The oracle is the
// per-relationship RelEffectiveTimeline of every relationship ever written,
// keeping the segments of the type whose row carries every value and that
// meet the window; the door must equal it per tuple, aligned with tuples.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

const rtbType = "NET"

var rtbKeys = []string{"estate", "device"}

// rtbWorld is one graph of a row-105 scenario, relationships by name.
type rtbWorld struct {
	g     *Core
	ctx   context.Context
	s, e  types.NodeID
	ids   []types.RelID
	names map[types.RelID]string
	byN   map[string]types.RelID
	typ   map[types.RelID]string
}

func newRTBWorld(t *testing.T, g *Core) *rtbWorld {
	t.Helper()
	s, e := txbEndpoints(t, g)
	return &rtbWorld{g: g, ctx: context.Background(), s: s, e: e, names: map[types.RelID]string{}, byN: map[string]types.RelID{}, typ: map[types.RelID]string{}}
}

func (w *rtbWorld) index(t *testing.T, typ string, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if err := w.g.Index.CreateRelProperty(typ, k); err != nil {
			t.Fatalf("CreateRelProperty(%s, %s): %v", typ, k, err)
		}
	}
}

func (w *rtbWorld) add(t *testing.T, name, typ string, props map[string]any) types.RelID {
	t.Helper()
	r, err := w.g.Rels.AddByID(w.ctx, typ, w.s, w.e, props)
	if err != nil {
		t.Fatalf("add %s: %v", name, err)
	}
	w.ids = append(w.ids, r.ID())
	w.names[r.ID()] = name
	w.byN[name] = r.ID()
	w.typ[r.ID()] = typ
	return r.ID()
}

func (w *rtbWorld) pin(t *testing.T) types.Instant {
	t.Helper()
	p, err := w.g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// rtbInWindow is the oracle's valid filter (open end 0 on the legacy axis; an
// infinite end on the literal axis lies above every bound anyway).
func rtbInWindow(pin temporal.Pin, from, to types.Instant) bool {
	switch {
	case pin.ValidAtSet || pin.ValidAt != 0:
		return from <= pin.ValidAt && (to == 0 || to > pin.ValidAt)
	case pin.ValidWindowSet || pin.ValidStart != 0:
		end := pin.ValidEnd
		return (end == 0 && !pin.ValidWindowSet || from < end) && (to == 0 || to > pin.ValidStart)
	}
	return true
}

// rtbCarries reports whether props carries v at key under the equality doors'
// property equality.
func rtbCarries(props map[string]any, key string, v any) bool {
	got, ok := props[key]
	if !ok {
		return false
	}
	return slices.Contains(types.EqualIndexKeys(v), types.IndexablePropertyValueKey(got))
}

// rtbOracle answers the door from the per-relationship timelines of every
// relationship ever written with type typ (another type's retention gate is
// not this door's).
func rtbOracle(t *testing.T, w *rtbWorld, typ string, keys []string, tuples [][]any, pin temporal.Pin) [][]temporal.RelSegment {
	t.Helper()
	tok, known := w.g.relTypes.Lookup(typ)
	out := make([][]temporal.RelSegment, len(tuples))
	ids := slices.Clone(w.ids)
	slices.Sort(ids)
	for i, tu := range tuples {
		for _, id := range ids {
			if w.typ[id] != typ {
				continue
			}
			segs, err := w.g.Temporal.RelEffectiveTimeline(id, pin.Tx)
			if errors.Is(err, storepkg.ErrRelNotFound) {
				continue
			}
			if err != nil {
				t.Fatalf("oracle RelEffectiveTimeline(%s, %d): %v", w.names[id], pin.Tx, err)
			}
			for _, s := range segs {
				if !known || !s.Rel.HasTypeTokenRaw(tok) || !rtbInWindow(pin, s.ValidFrom, s.ValidTo) {
					continue
				}
				props := s.Rel.PropertiesMap()
				all := true
				for j, k := range keys {
					all = all && rtbCarries(props, k, tu[j])
				}
				if all {
					out[i] = append(out[i], s)
				}
			}
		}
	}
	return out
}

// rtbRender prints an aligned answer by relationship name.
func rtbRender(w *rtbWorld, ans [][]temporal.RelSegment) string {
	var b strings.Builder
	for i, segs := range ans {
		fmt.Fprintf(&b, "\n  %d:", i)
		if segs == nil {
			b.WriteString(" nil")
		}
		for _, s := range segs {
			fmt.Fprintf(&b, " %s[%d,%d)v%d", w.names[s.Rel.ID()], s.ValidFrom, s.ValidTo, s.Rel.Version())
		}
	}
	return b.String()
}

// rtbCheck runs the door and compares it with the oracle.
func rtbCheck(t *testing.T, w *rtbWorld, what string, keys []string, tuples [][]any, pin temporal.Pin) [][]temporal.RelSegment {
	t.Helper()
	got, err := w.g.Temporal.RelEffectiveTimelinesByProperties(w.ctx, rtbType, keys, tuples, pin)
	if err != nil {
		t.Fatalf("%s: door: %v", what, err)
	}
	if len(got) != len(tuples) {
		t.Fatalf("%s: %d answers for %d tuples", what, len(got), len(tuples))
	}
	want := rtbOracle(t, w, rtbType, keys, tuples, pin)
	if g, o := rtbRender(w, got), rtbRender(w, want); g != o {
		t.Fatalf("%s: door%s\noracle%s", what, g, o)
	}
	return got
}

// rtbNames lists the relationship names of one tuple's answer, once each.
func rtbNames(w *rtbWorld, segs []temporal.RelSegment) string {
	var out []string
	for _, s := range segs {
		if n := w.names[s.Rel.ID()]; len(out) == 0 || out[len(out)-1] != n {
			out = append(out, n)
		}
	}
	return strings.Join(out, ",")
}

// TestRelEffectiveTimelinesByProperties_MatchesPerTupleTimeline: on every
// backend the door equals the per-relationship oracle at two pins and under a
// valid filter, for tuples unsorted and repeated, two tuples sharing a device,
// a relationship whose device changed (each tuple gets only its own segment),
// a numeric value probed in another numeric type, a relationship of another
// type with the same values, one without the device, one deleted after the
// first pin and one added after it. Breaks: a swapped tuple and an unknown
// value answer nil.
func TestRelEffectiveTimelinesByProperties_MatchesPerTupleTimeline(t *testing.T) {
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			useTestClock(t, g)
			w := newRTBWorld(t, g)
			w.index(t, rtbType, rtbKeys...)
			w.index(t, "OTHER", rtbKeys...)
			w.add(t, "a", rtbType, map[string]any{"estate": "e1", "device": "d1"})
			w.add(t, "b", rtbType, map[string]any{"estate": "e1", "device": "d2"})
			w.add(t, "c", rtbType, map[string]any{"estate": "e2", "device": "d1"})
			m := w.add(t, "m", rtbType, map[string]any{"estate": "e1", "device": "d1"})
			w.add(t, "num", rtbType, map[string]any{"estate": "e1", "device": int64(7)})
			w.add(t, "x", "OTHER", map[string]any{"estate": "e1", "device": "d1"})
			w.add(t, "n", rtbType, map[string]any{"estate": "e1"})
			del := w.add(t, "del", rtbType, map[string]any{"estate": "e1", "device": "d1"})
			pin1 := w.pin(t)
			if _, err := g.Rels.Update(w.ctx, m, map[string]any{"device": "d2"}); err != nil {
				t.Fatal(err)
			}
			if err := g.Rels.Delete(w.ctx, del); err != nil {
				t.Fatal(err)
			}
			w.add(t, "late", rtbType, map[string]any{"estate": "e1", "device": "d1"})
			pin2 := w.pin(t)
			tuples := [][]any{
				{"e1", "d2"}, {"e1", "d1"}, {"e2", "d1"}, {"e1", "d1"},
				{"e9", "d9"}, {"e1", float64(7)}, {"d1", "e1"},
			}
			at1 := rtbCheck(t, w, "pin1", rtbKeys, tuples, temporal.Pin{Tx: pin1})
			at2 := rtbCheck(t, w, "pin2", rtbKeys, tuples, temporal.Pin{Tx: pin2})
			mUpd := at2[0][len(at2[0])-1].ValidFrom // m's d2 segment starts at its update
			rtbCheck(t, w, "pin2 ValidAt before m's update", rtbKeys, tuples, temporal.Pin{Tx: pin2, ValidAt: mUpd - 1})
			rtbCheck(t, w, "pin2 window from m's update", rtbKeys, tuples, temporal.Pin{Tx: pin2, ValidStart: mUpd})

			// del re-imported as another type with the same values: its new
			// rows are OTHER's (never answered), its old NET segment stays.
			sn, err := g.Nodes.Get(w.ctx, w.s)
			if err != nil {
				t.Fatal(err)
			}
			en, err := g.Nodes.Get(w.ctx, w.e)
			if err != nil {
				t.Fatal(err)
			}
			reFrom := pin2 + 1_000_000
			if _, err := g.Rels.Import(w.ctx, del, "OTHER", sn, en, map[string]any{"estate": "e1", "device": "d1", "tkg_valid_from": reFrom}); err != nil {
				t.Fatalf("re-import del as OTHER: %v", err)
			}
			at3 := rtbCheck(t, w, "pin3 after re-import as OTHER", rtbKeys, tuples, temporal.Pin{Tx: w.pin(t)})
			if got := rtbNames(w, at3[1]); got != "a,m,del,late" {
				t.Errorf("pin3 tuple 1 = %q, want a,m,del,late", got)
			}
			for _, s := range at3[1] {
				if s.Rel.ID() == del && s.ValidFrom >= reFrom {
					t.Errorf("re-imported OTHER row answered under NET: [%d,%d)", s.ValidFrom, s.ValidTo)
				}
			}

			for _, c := range []struct {
				ans  [][]temporal.RelSegment
				i    int
				want string
			}{
				{at1, 0, "b"}, {at1, 1, "a,m,del"}, {at1, 2, "c"}, {at1, 5, "num"},
				{at2, 0, "b,m"}, {at2, 1, "a,m,del,late"}, {at2, 2, "c"},
			} {
				if got := rtbNames(w, c.ans[c.i]); got != c.want {
					t.Errorf("tuple %d = %q, want %q", c.i, got, c.want)
				}
			}
			for _, i := range []int{4, 6} {
				if at2[i] != nil {
					t.Errorf("tuple %d %v answered %s, want nil", i, tuples[i], rtbRender(w, at2[i:i+1]))
				}
			}
			if rtbRender(w, at2[1:2]) != rtbRender(w, at2[3:4]) {
				t.Errorf("repeated tuple answers differ")
			}
			for _, i := range []int{0, 1} { // m's segments split by device
				for _, s := range at2[i] {
					if s.Rel.ID() == m && !rtbCarries(s.Rel.PropertiesMap(), "device", tuples[i][1]) {
						t.Errorf("tuple %d holds m's segment with device %v", i, s.Rel.PropertiesMap()["device"])
					}
				}
			}
		})
	}
}

// declineStore is a memory store whose relationship property sidecar declines
// (store.ErrIndexNotFound), as a store does while the index is being built.
type declineStore struct{ *memory.Store }

// RelationshipsByTypeAndProperty is declared so the wrapper keeps the
// relationship property index capability.
func (s *declineStore) RelationshipsByTypeAndProperty(tok uint16, key string, value any, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	return s.Store.RelationshipsByTypeAndProperty(tok, key, value, opts)
}

func (s *declineStore) ForEachRelPropertyTxMember(uint16, string, string, func(types.RelID, types.Instant) bool) error {
	return storepkg.ErrIndexNotFound
}

func (s *declineStore) ForEachRelPropertyTxMemberDuring(uint16, string, string, types.Instant, types.Instant, types.Instant, func(types.RelID, types.Instant) bool) error {
	return storepkg.ErrIndexNotFound
}

// noSidecarStore is a memory store wrapper that keeps the relationship
// property index but offers no membership sidecar (the core does not trust
// an embedded one).
type noSidecarStore struct{ *memory.Store }

func (s *noSidecarStore) RelationshipsByTypeAndProperty(tok uint16, key string, value any, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	return s.Store.RelationshipsByTypeAndProperty(tok, key, value, opts)
}

// TestRelEffectiveTimelinesByProperties_RefusesWithoutIndex: no index on any
// probe key — none at all, one on another key, one on another type, or one
// still being built (the sidecar declines) — is ErrIndexNotFound with no
// timeline read, on every backend; an index on one probe key only (the less
// selective one) answers like the oracle; an unknown type answers nil per
// tuple.
func TestRelEffectiveTimelinesByProperties_RefusesWithoutIndex(t *testing.T) {
	tuples := [][]any{{"e1", "d1"}, {"e2", "d1"}}
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			w := newRTBWorld(t, g)
			w.add(t, "a", rtbType, map[string]any{"estate": "e1", "device": "d1", "port": int64(1)})
			w.add(t, "b", rtbType, map[string]any{"estate": "e2", "device": "d1", "port": int64(1)})
			w.add(t, "o", "OTHER", map[string]any{"estate": "e1", "device": "d1"})
			pin := temporal.Pin{Tx: w.pin(t)}
			reads := 0
			g.scanReadTestHook = func() { reads++ }
			refuse := func(what string) {
				t.Helper()
				reads = 0
				got, err := g.Temporal.RelEffectiveTimelinesByProperties(w.ctx, rtbType, rtbKeys, tuples, pin)
				if !errors.Is(err, storepkg.ErrIndexNotFound) || got != nil || reads != 0 {
					t.Fatalf("%s: %v, %v, %d reads; want ErrIndexNotFound, nil, 0 reads", what, rtbRender(w, got), err, reads)
				}
			}
			refuse("no index")
			w.index(t, rtbType, "port")
			refuse("index on a key outside the probe")
			w.index(t, "OTHER", rtbKeys...)
			refuse("index on another type")
			got, err := g.Temporal.RelEffectiveTimelinesByProperties(w.ctx, "NOPE", rtbKeys, tuples, pin)
			if err != nil || len(got) != 2 || got[0] != nil || got[1] != nil {
				t.Fatalf("unknown type = %s, %v; want nil per tuple", rtbRender(w, got), err)
			}
			w.index(t, rtbType, "estate")
			g.scanReadTestHook = nil
			rtbCheck(t, w, "estate index only", rtbKeys, tuples, pin)
			rtbCheck(t, w, "estate index only, keys reversed", []string{"device", "estate"}, [][]any{{"d1", "e1"}}, pin)
		})
	}
	t.Run("elements index only", func(t *testing.T) {
		g := txbBackends()[0].open(t, false)
		w := newRTBWorld(t, g)
		if err := g.Index.CreateRelPropertyWithOptions(rtbType, "device", storepkg.PropertyIndexOptions{Elements: true}); err != nil {
			t.Fatal(err)
		}
		w.add(t, "a", rtbType, map[string]any{"estate": "e1", "device": "d1"})
		if got, err := g.Temporal.RelEffectiveTimelinesByProperties(w.ctx, rtbType, rtbKeys, tuples, temporal.Pin{Tx: w.pin(t)}); !errors.Is(err, storepkg.ErrIndexNotFound) || got != nil {
			t.Fatalf("Elements index as the only driver: %s, %v; want ErrIndexNotFound", rtbRender(w, got), err)
		}
		w.index(t, rtbType, "estate")
		rtbCheck(t, w, "estate drives past the Elements index", rtbKeys, tuples, temporal.Pin{Tx: w.pin(t)})
	})
	t.Run("store without sidecar", func(t *testing.T) {
		g, err := New(Config{Store: &noSidecarStore{memory.New()}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = g.Close() })
		w := newRTBWorld(t, g)
		w.index(t, rtbType, rtbKeys...)
		w.add(t, "a", rtbType, map[string]any{"estate": "e1", "device": "d1"})
		if got, err := g.Temporal.RelEffectiveTimelinesByProperties(w.ctx, rtbType, rtbKeys, tuples, temporal.Pin{Tx: w.pin(t)}); !errors.Is(err, storepkg.ErrIndexNotFound) || got != nil {
			t.Fatalf("store without the membership sidecar: %s, %v; want ErrIndexNotFound", rtbRender(w, got), err)
		}
	})
	t.Run("mid-build", func(t *testing.T) {
		g, err := New(Config{Store: &declineStore{memory.New()}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = g.Close() })
		w := newRTBWorld(t, g)
		w.index(t, rtbType, rtbKeys...)
		w.add(t, "a", rtbType, map[string]any{"estate": "e1", "device": "d1"})
		for _, pin := range []temporal.Pin{{Tx: w.pin(t)}, {Tx: w.pin(t), ValidStart: 1}} {
			if got, err := g.Temporal.RelEffectiveTimelinesByProperties(w.ctx, rtbType, rtbKeys, tuples, pin); !errors.Is(err, storepkg.ErrIndexNotFound) || got != nil {
				t.Fatalf("declining sidecar %+v: %s, %v; want ErrIndexNotFound", pin, rtbRender(w, got), err)
			}
		}
	})
}

// TestRelEffectiveTimelinesByProperties_PinGates: a pin above the commit
// clock, below compacted knowledge or below the type's retention watermark
// refuses the whole call before any timeline is read (the singular door's
// sentinels); another type's watermark does not; a pin at or below 0 answers
// nil per tuple.
func TestRelEffectiveTimelinesByProperties_PinGates(t *testing.T) {
	g := txbBackends()[0].open(t, false)
	useTestClock(t, g)
	w := newRTBWorld(t, g)
	w.index(t, rtbType, rtbKeys...)
	tuples := [][]any{{"e1", "d1"}, {"e1", "d2"}}
	a := w.add(t, "a", rtbType, map[string]any{"estate": "e1", "device": "d1", "x": int64(0)})
	w.add(t, "o", "OTHER", map[string]any{"estate": "e1", "device": "d1"})
	p0 := w.pin(t)
	reads := 0
	g.scanReadTestHook = func() { reads++ }
	refuse := func(what string, pin types.Instant, want error) {
		t.Helper()
		reads = 0
		got, err := g.Temporal.RelEffectiveTimelinesByProperties(w.ctx, rtbType, rtbKeys, tuples, temporal.Pin{Tx: pin})
		if !errors.Is(err, want) || got != nil || reads != 0 {
			t.Fatalf("%s (pin %d): %s, %v, %d reads; want %v, nil, 0 reads", what, pin, rtbRender(w, got), err, reads, want)
		}
	}
	peek, err := g.Temporal.PeekTx()
	if err != nil {
		t.Fatal(err)
	}
	refuse("above the clock", peek+1000, ErrTxPinTooNew)
	for _, pin := range []types.Instant{0, -5} {
		got, err := g.Temporal.RelEffectiveTimelinesByProperties(w.ctx, rtbType, rtbKeys, tuples, temporal.Pin{Tx: pin})
		if err != nil || len(got) != 2 || got[0] != nil || got[1] != nil {
			t.Fatalf("pin %d = %s, %v; want nil per tuple", pin, rtbRender(w, got), err)
		}
	}
	for i := 1; i <= 2; i++ {
		if _, err := g.Rels.Update(w.ctx, a, map[string]any{"x": int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	pHigh := w.pin(t)
	if _, err := g.Admin.CompactHistoryRels(w.ctx, RetentionPolicy{KeepVersions: 1}); err != nil {
		t.Fatalf("compact: %v", err)
	}
	refuse("below compaction", p0, ErrHistoryCompacted)
	g.scanReadTestHook = nil
	rtbCheck(t, w, "above compaction", rtbKeys, tuples, temporal.Pin{Tx: pHigh})

	wm := w.pin(t)
	tokO, ok := g.relTypes.Lookup("OTHER")
	if !ok {
		t.Fatal("OTHER token")
	}
	if err := g.advanceRelRetentionWatermark(tokO, wm); err != nil {
		t.Fatal(err)
	}
	rtbCheck(t, w, "another type's watermark", rtbKeys, tuples, temporal.Pin{Tx: pHigh})
	tokN, ok := g.relTypes.Lookup(rtbType)
	if !ok {
		t.Fatal("NET token")
	}
	if err := g.advanceRelRetentionWatermark(tokN, wm); err != nil {
		t.Fatal(err)
	}
	g.scanReadTestHook = func() { reads++ }
	refuse("below the type's retention", wm-1, ErrRetentionExpired)
	g.scanReadTestHook = nil
	rtbCheck(t, w, "at the type's retention watermark", rtbKeys, tuples, temporal.Pin{Tx: wm})
}

// TestRelEffectiveTimelinesByProperties_WindowCut: under a window [from, to)
// the segments straddling from and to are kept whole, one ending at from or
// starting at to is dropped (half-open); a point ValidAt and an open end
// [from, inf) keep the segments containing / reaching them; a reversed or
// empty window, a lone ValidEnd and a point with a window are
// ErrInvalidTimeRange. Every backend, oracle-checked.
func TestRelEffectiveTimelinesByProperties_WindowCut(t *testing.T) {
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			w := newRTBWorld(t, g)
			w.index(t, rtbType, rtbKeys...)
			for _, r := range []struct {
				name     string
				from, to types.Instant
			}{
				{"s1", 1000, 5000}, {"s2", 2500, 2800}, {"s3", 2900, 9000},
				{"s4", 100, 2000}, {"s5", 3000, 4000}, {"s6", 500, 0},
			} {
				props := map[string]any{"estate": "e1", "device": "d1", "tkg_valid_from": r.from}
				if r.to != 0 {
					props["tkg_valid_to"] = r.to
				}
				w.add(t, r.name, rtbType, props)
			}
			tx := w.pin(t)
			tuples := [][]any{{"e1", "d1"}}
			for _, c := range []struct {
				what string
				pin  temporal.Pin
				want string
			}{
				{"window [2000,3000)", temporal.Pin{Tx: tx, ValidStart: 2000, ValidEnd: 3000}, "s1,s2,s3,s6"},
				{"ValidAt 2950", temporal.Pin{Tx: tx, ValidAt: 2950}, "s1,s3,s6"},
				{"open end [3500,inf)", temporal.Pin{Tx: tx, ValidStart: 3500}, "s1,s3,s5,s6"},
				{"no filter", temporal.Pin{Tx: tx}, "s1,s2,s3,s4,s5,s6"},
			} {
				got := rtbCheck(t, w, c.what, rtbKeys, tuples, c.pin)
				if n := rtbNames(w, got[0]); n != c.want {
					t.Errorf("%s = %q, want %q", c.what, n, c.want)
				}
				if c.what == "window [2000,3000)" {
					for _, s := range got[0] {
						if w.names[s.Rel.ID()] == "s1" && (s.ValidFrom != 1000 || s.ValidTo != 5000) {
							t.Errorf("s1 clipped to [%d,%d), want whole [1000,5000)", s.ValidFrom, s.ValidTo)
						}
					}
				}
			}
			for _, pin := range []temporal.Pin{
				{Tx: tx, ValidStart: 3000, ValidEnd: 3000},
				{Tx: tx, ValidStart: 3000, ValidEnd: 2000},
				{Tx: tx, ValidEnd: 2000},
				{Tx: tx, ValidAt: 2500, ValidStart: 2000, ValidEnd: 3000},
			} {
				if got, err := g.Temporal.RelEffectiveTimelinesByProperties(w.ctx, rtbType, rtbKeys, tuples, pin); !errors.Is(err, ErrInvalidTimeRange) || got != nil {
					t.Errorf("pin %+v: %s, %v; want ErrInvalidTimeRange", pin, rtbRender(w, got), err)
				}
			}
		})
	}
}

// TestRelEffectiveTimelinesByProperties_BadInput: every malformed argument is
// refused with its sentinel before a timeline is read; empty tuples answer an
// empty result reading nothing; a context cancelled between candidates stops
// the reads.
func TestRelEffectiveTimelinesByProperties_BadInput(t *testing.T) {
	g := txbBackends()[0].open(t, false)
	w := newRTBWorld(t, g)
	w.index(t, rtbType, rtbKeys...)
	for i := range 10 {
		w.add(t, fmt.Sprintf("r%d", i), rtbType, map[string]any{"estate": "e1", "device": "d1"})
	}
	pin := temporal.Pin{Tx: w.pin(t)}
	ok := [][]any{{"e1", "d1"}}
	reads := 0
	g.scanReadTestHook = func() { reads++ }
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, c := range []struct {
		what   string
		ctx    context.Context
		typ    string
		keys   []string
		tuples [][]any
		want   error
	}{
		{"no keys", w.ctx, rtbType, nil, [][]any{{}}, storepkg.ErrInvalidStoreMutation},
		{"five keys", w.ctx, rtbType, []string{"a", "b", "c", "d", "e"}, [][]any{{1, 2, 3, 4, 5}}, storepkg.ErrInvalidStoreMutation},
		{"duplicate key", w.ctx, rtbType, []string{"estate", "estate"}, [][]any{{"e1", "e1"}}, storepkg.ErrInvalidStoreMutation},
		{"reserved prefix", w.ctx, rtbType, []string{"estate", "tkg_valid_from"}, [][]any{{"e1", 1}}, types.ErrReservedPrefix},
		{"empty key", w.ctx, rtbType, []string{"estate", ""}, [][]any{{"e1", 1}}, ErrEmptyName},
		{"short tuple", w.ctx, rtbType, rtbKeys, [][]any{{"e1", "d1"}, {"e1"}}, storepkg.ErrInvalidStoreMutation},
		{"long tuple", w.ctx, rtbType, rtbKeys, [][]any{{"e1", "d1", "x"}}, storepkg.ErrInvalidStoreMutation},
		{"nil value", w.ctx, rtbType, rtbKeys, [][]any{{"e1", nil}}, types.ErrUnsupportedValueType},
		{"list value", w.ctx, rtbType, rtbKeys, [][]any{{"e1", []any{"d1"}}}, types.ErrUnsupportedValueType},
		{"map value", w.ctx, rtbType, rtbKeys, [][]any{{map[string]any{"a": 1}, "d1"}}, types.ErrUnsupportedValueType},
		{"empty type", w.ctx, "", rtbKeys, ok, ErrEmptyName},
		{"nil ctx", nil, rtbType, rtbKeys, ok, ErrNilContext}, //nolint:staticcheck // the nil-context refusal under test
		{"cancelled ctx", cancelled, rtbType, rtbKeys, ok, context.Canceled},
	} {
		reads = 0
		got, err := g.Temporal.RelEffectiveTimelinesByProperties(c.ctx, c.typ, c.keys, c.tuples, pin)
		if !errors.Is(err, c.want) || got != nil || reads != 0 {
			t.Errorf("%s: %s, %v, %d reads; want %v, nil, 0 reads", c.what, rtbRender(w, got), err, reads, c.want)
		}
	}

	reads = 0
	got, err := g.Temporal.RelEffectiveTimelinesByProperties(w.ctx, rtbType, rtbKeys, [][]any{}, pin)
	if err != nil || got == nil || len(got) != 0 || reads != 0 {
		t.Errorf("empty tuples = %v, %v, %d reads; want empty, nil, 0 reads", got, err, reads)
	}

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	reads = 0
	g.scanReadTestHook = func() {
		reads++
		if reads == 2 {
			stop()
		}
	}
	if got, err := g.Temporal.RelEffectiveTimelinesByProperties(ctx, rtbType, rtbKeys, ok, pin); !errors.Is(err, context.Canceled) || got != nil || reads > 3 {
		t.Errorf("cancel between candidates: %s, %v, %d reads; want context.Canceled, nil, <= 3 reads of 10", rtbRender(w, got), err, reads)
	}

	g.scanReadTestHook = nil
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := g.Temporal.RelEffectiveTimelinesByProperties(w.ctx, rtbType, rtbKeys, ok, pin); !errors.Is(err, ErrGraphClosed) || got != nil {
		t.Errorf("closed graph: %v, %v; want ErrGraphClosed", got, err)
	}
}

// countingTiered counts the relationship property sidecar enumerations a core
// asks of a tiered store (each one is a fan-out over every shard).
type countingTiered struct {
	*tiered.Store
	keys, one atomic.Int64
}

// RelationshipsByTypeAndProperty is declared so the wrapper keeps the
// relationship property index capability (the core trusts a wrapper's own
// declaration, not an embedded one).
func (s *countingTiered) RelationshipsByTypeAndProperty(tok uint16, key string, value any, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	return s.Store.RelationshipsByTypeAndProperty(tok, key, value, opts)
}

func (s *countingTiered) ForEachRelPropertyTxMember(tok uint16, key, vk string, fn func(types.RelID, types.Instant) bool) error {
	s.one.Add(1)
	return s.Store.ForEachRelPropertyTxMember(tok, key, vk, fn)
}

func (s *countingTiered) ForEachRelPropertyTxMemberDuring(tok uint16, key, vk string, from, to, txAt types.Instant, fn func(types.RelID, types.Instant) bool) error {
	s.one.Add(1)
	return s.Store.ForEachRelPropertyTxMemberDuring(tok, key, vk, from, to, txAt, fn)
}

func (s *countingTiered) ForEachRelPropertyTxMemberKeys(tok uint16, key string, vks []string, fn func(types.RelID, types.Instant) bool) error {
	s.keys.Add(1)
	return s.Store.ForEachRelPropertyTxMemberKeys(tok, key, vks, fn)
}

func (s *countingTiered) ForEachRelPropertyTxMemberKeysDuring(tok uint16, key string, vks []string, from, to, txAt types.Instant, fn func(types.RelID, types.Instant) bool) error {
	s.keys.Add(1)
	return s.Store.ForEachRelPropertyTxMemberKeysDuring(tok, key, vks, from, to, txAt, fn)
}

// TestRelEffectiveTimelinesByProperties_OneFanoutManyTuples: on a disk tiered
// store whose relationships span a cold closed event shard (minted there) and
// a hot shard, 50 tuples are served by ONE sidecar fan-out per call (plain and
// windowed), equal the oracle, and keep the cold-minted relationships and the
// one deleted inside the window (its closed segment).
func TestRelEffectiveTimelinesByProperties_OneFanoutManyTuples(t *testing.T) {
	ts, err := tiered.New(tiered.Config{
		DataDir:           t.TempDir(),
		RefLabels:         []string{"Ref"},
		ShardWindow:       7 * 24 * time.Hour,
		FlushInterval:     1<<63 - 1,
		MaxOpenColdShards: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	cs := &countingTiered{Store: ts}
	g, err := New(Config{Store: cs, UntimedValidStart: UntimedFromTxFrom})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	w := newRTBWorld(t, g)
	w.index(t, rtbType, rtbKeys...)
	first := hotShardName(ts)
	dev := func(i int) string { return fmt.Sprintf("d%d", i) }
	est := func(i int) string { return fmt.Sprintf("e%d", i%3) }
	for i := range 30 {
		w.add(t, "cold"+dev(i), rtbType, map[string]any{"estate": est(i), "device": dev(i)})
	}
	if err := g.Rels.Delete(w.ctx, w.byN["cold"+dev(4)]); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	for i := 20; i < 40; i++ {
		w.add(t, "hot"+dev(i), rtbType, map[string]any{"estate": est(i), "device": dev(i)})
	}
	demoteToCold(ts, first)
	closeEventShardStore(t, ts.EventShardsForTest()[first])
	tx := w.pin(t)

	tuples := make([][]any, 50)
	for i := range tuples {
		tuples[i] = []any{est(i), dev(i)}
	}
	for _, pin := range []temporal.Pin{{Tx: tx}, {Tx: tx, ValidStart: 1, ValidEnd: tx + 1}} {
		cs.keys.Store(0)
		cs.one.Store(0)
		got := rtbCheck(t, w, fmt.Sprintf("pin %+v", pin), rtbKeys, tuples, pin)
		if k, o := cs.keys.Load(), cs.one.Load(); k != 1 || o != 0 {
			t.Errorf("pin %+v: %d several-key and %d one-key enumerations, want 1 and 0", pin, k, o)
		}
		for i, want := range map[int]string{3: "cold" + dev(3), 4: "cold" + dev(4), 25: "cold" + dev(25) + ",hot" + dev(25), 39: "hot" + dev(39), 45: ""} {
			if n := rtbNames(w, got[i]); n != want {
				t.Errorf("pin %+v tuple %d = %q, want %q", pin, i, n, want)
			}
		}
		if segs := got[4]; len(segs) != 1 || segs[0].ValidTo == 0 {
			t.Errorf("deleted rel: %s, want one closed segment", rtbRender(w, got[4:5]))
		}
	}
}

// TestRelEffectiveTimelinesByProperties_NoWholeTypeScan: 10 estates over
// 1,000 devices (100k relationships; 20k under -short, same assertion): a
// probe of one device reads at most D + r candidates (D = the device's
// relationships, r = those written after the pin, pruned), whatever the keys
// order — the driver is the indexed key with the fewest members, never the
// estate (n/10 candidates).
func TestRelEffectiveTimelinesByProperties_NoWholeTypeScan(t *testing.T) {
	n := 100_000
	if testing.Short() {
		n = 20_000
	}
	const devices = 1000
	g := txbBackends()[0].open(t, false)
	w := newRTBWorld(t, g)
	w.index(t, rtbType, rtbKeys...)
	for i := range n {
		w.add(t, "", rtbType, map[string]any{"estate": fmt.Sprintf("e%d", i%10), "device": fmt.Sprintf("d%d", i%devices)})
	}
	pin := temporal.Pin{Tx: w.pin(t)}
	const r = 5
	for range r {
		w.add(t, "", rtbType, map[string]any{"estate": "e3", "device": "d3"})
	}
	d := n / devices
	reads := 0
	g.scanReadTestHook = func() { reads++ }
	for _, c := range []struct {
		keys   []string
		tuples [][]any
	}{
		{rtbKeys, [][]any{{"e3", "d3"}, {"e4", "d3"}}},
		{[]string{"device", "estate"}, [][]any{{"d3", "e3"}, {"d3", "e4"}}},
	} {
		reads = 0
		got, err := g.Temporal.RelEffectiveTimelinesByProperties(w.ctx, rtbType, c.keys, c.tuples, pin)
		if err != nil {
			t.Fatal(err)
		}
		if reads > d+r {
			t.Errorf("keys %v: %d candidates read, want <= D + r = %d (estate would read %d)", c.keys, reads, d+r, n/10)
		}
		if reads != d { // memory knows each member's first tx: the r are pruned
			t.Errorf("keys %v: %d candidates read, want D = %d (the %d written after the pin pruned)", c.keys, reads, d, r)
		}
		if len(got[0]) != d || got[1] != nil {
			t.Errorf("keys %v: %d segments for (e3, d3), want %d; (e4, d3) = %d, want nil", c.keys, len(got[0]), d, len(got[1]))
		}
		t.Logf("keys %v: %d candidates read (D = %d, r = %d, n = %d)", c.keys, reads, d, r, n)
	}
}
