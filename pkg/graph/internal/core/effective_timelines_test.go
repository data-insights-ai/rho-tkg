package core

import (
	"context"
	"errors"
	"slices"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Row 98 (backlog 79, ai-soc 2026-10-21): NodeEffectiveTimelines(id, pins) /
// RelEffectiveTimelines(id, pins) answer every pin of one call from one walk
// of the entity's history chain. ai-soc measured sigma's ReadDerivedAt on one
// Derived fact of 511 versions at 16 / 64 / 511 pins: 17,408 / 66,560 /
// 524,288 history rows (a whole-chain read per pin). The plural door reads
// about versions + pins rows, flat in the number of pins, and answers at each
// pin exactly what the singular door answers there.
//
// Catches: a door that loops the singular door (rows grow with pins), a result
// not aligned with the input pins (sorted, deduplicated, or shifted), one
// pin's answer computed at another pin, a bad pin refused only after rows were
// read or after earlier pins were answered, a partial result returned with an
// error, nil vs empty confusion for empty pins.

// timelinesRows renders the plural door's answer per pin, as etSegsString
// renders the singular one, and counts the history rows the call read.
func (e *ccEnt) timelinesRows(id int64, pins []types.Instant) ([]string, int, error) {
	e.t.Helper()
	rows := 0
	e.g.historyRead = func(n int) { rows += n }
	defer func() { e.g.historyRead = nil }()
	var out []string
	if e.rel {
		got, err := e.g.Temporal.RelEffectiveTimelines(types.RelID(id), pins)
		if err != nil {
			if got != nil {
				e.t.Fatalf("RelEffectiveTimelines error %v with a result %v", err, got)
			}
			return nil, rows, err
		}
		for _, ss := range got {
			out = append(out, etSegsString(etRelSegs(ss)))
		}
		if len(got) != len(pins) {
			e.t.Fatalf("RelEffectiveTimelines: %d answers for %d pins", len(got), len(pins))
		}
		return out, rows, nil
	}
	got, err := e.g.Temporal.NodeEffectiveTimelines(types.NodeID(id), pins)
	if err != nil {
		if got != nil {
			e.t.Fatalf("NodeEffectiveTimelines error %v with a result %v", err, got)
		}
		return nil, rows, err
	}
	for _, ss := range got {
		out = append(out, etSegsString(etNodeSegs(ss)))
	}
	if len(got) != len(pins) {
		e.t.Fatalf("NodeEffectiveTimelines: %d answers for %d pins", len(got), len(pins))
	}
	return out, rows, nil
}

// singleRows is the singular door at each pin, rendered, and the history rows
// those calls read together.
func (e *ccEnt) singleRows(id int64, pins []types.Instant) ([]string, int) {
	e.t.Helper()
	rows := 0
	e.g.historyRead = func(n int) { rows += n }
	defer func() { e.g.historyRead = nil }()
	out := make([]string, len(pins))
	for i, p := range pins {
		var err error
		if e.rel {
			var ss []temporal.RelSegment
			ss, err = e.g.Temporal.RelEffectiveTimeline(types.RelID(id), p)
			out[i] = etSegsString(etRelSegs(ss))
		} else {
			var ss []temporal.NodeSegment
			ss, err = e.g.Temporal.NodeEffectiveTimeline(types.NodeID(id), p)
			out[i] = etSegsString(etNodeSegs(ss))
		}
		if err != nil {
			e.t.Fatalf("%s singular timeline at %d: %v", e.kind(), p, err)
		}
	}
	return out, rows
}

// sameAsSingular checks the plural door against the singular one pin by pin.
func (e *ccEnt) sameAsSingular(what string, id int64, pins []types.Instant) {
	e.t.Helper()
	got, _, err := e.timelinesRows(id, pins)
	if err != nil {
		e.t.Fatalf("%s: %v", what, err)
	}
	want, _ := e.singleRows(id, pins)
	for i := range pins {
		if got[i] != want[i] {
			e.t.Fatalf("%s: pin %d (#%d) = %s\n  want (singular) %s", what, pins[i], i, got[i], want[i])
		}
	}
}

// stamps is the ascending distinct TxFrom of every row of the chain.
func (e *ccEnt) stamps(id int64) []types.Instant {
	var out []types.Instant
	for _, r := range e.chain(id) {
		out = append(out, r.tm.TxFrom)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// manyPinsVersions is ai-soc's chain length.
const manyPinsVersions = 511

func TestEffectiveTimelinesManyPins_OneChainWalk(t *testing.T) {
	t.Parallel()
	t.Run("break cases", effectiveTimelinesBreakCases)
	t.Run("compacted and retention", effectiveTimelinesCompactedAndRetention)
	t.Run("cost", effectiveTimelinesCost)
	t.Run("retraction and eclipsed chain", func(t *testing.T) { ccRun(t, false, effectiveTimelinesChains) })
	t.Run("whole axis", func(t *testing.T) { ccRunWholeAxis(t, effectiveTimelinesChains) })
	t.Run("rel type retention", effectiveTimelinesRelTypeRetention)
}

// ccRunWholeAxis is ccRun on every txbBackend under UntimedWholeAxis (ccRun
// pins UntimedFromTxFrom).
func ccRunWholeAxis(t *testing.T, body func(t *testing.T, mk func(t *testing.T) *ccEnt)) {
	t.Helper()
	for _, be := range txbBackends(func(c *Config) { c.UntimedValidStart = UntimedWholeAxis }) {
		for _, rel := range []bool{false, true} {
			t.Run(be.name+"/"+map[bool]string{false: "node", true: "rel"}[rel], func(t *testing.T) {
				body(t, func(t *testing.T) *ccEnt {
					t.Helper()
					g := be.open(t, false)
					useTestClock(t, g)
					return newCCEnt(t, g, rel)
				})
			})
		}
	}
}

// retract ends belief in the entity (Retract).
func (e *ccEnt) retract(id int64) {
	e.t.Helper()
	var err error
	if e.rel {
		err = e.g.Rels.Retract(e.ctx, types.RelID(id))
	} else {
		err = e.g.Nodes.Retract(e.ctx, types.NodeID(id))
	}
	if err != nil {
		e.t.Fatalf("%s retract(%d): %v", e.kind(), id, err)
	}
}

// effectiveTimelinesChains: an eclipsed, non-monotonic chain written with
// SetNodeVersionInterval / SetRelVersionInterval (valid starts out of version
// order, a later version swallowing earlier ones whole), then retracted; and
// an untimed create (the start the axis gives it). The plural door answers at
// every stamp, between stamps and after the retraction what the singular door
// answers there.
func effectiveTimelinesChains(t *testing.T, mk func(t *testing.T) *ccEnt) {
	e := mk(t)
	id := e.add("F", 1000, nil)
	var pins []types.Instant
	for i, iv := range [][2]types.Instant{{1100, 1200}, {1500, 1600}, {1050, 1700}, {1120, 1180}, {900, 1000}, {1000, 1800}, {1300, 1400}} {
		e.mustCascade(id, iv[0], iv[1], map[string]any{"x": int64(i + 1)})
		pins = append(pins, e.pin())
	}
	live := e.pin()
	e.retract(id)
	gone := e.pin()
	all := append(e.stamps(id), pins...)
	all = append(all, gone, live, gone)
	slices.Reverse(all)
	e.sameAsSingular("eclipsed non-monotonic chain, retracted", id, all)
	want, _ := e.singleRows(id, []types.Instant{live, gone})
	if want[0] == "" || want[0] == want[1] {
		t.Fatalf("retraction not visible to the singular door: before %q, after %q", want[0], want[1])
	}

	plain := e.addNoVF("U", 0)
	p0 := e.pin()
	e.mustUpdate(plain, map[string]any{"x": int64(1)})
	p1 := e.pin()
	e.mustCascade(plain, 1100, 1200, map[string]any{"x": int64(2)})
	p2 := e.pin()
	e.sameAsSingular("untimed create", plain, []types.Instant{p2, p0, p1, p0})
}

// effectiveTimelinesRelTypeRetention: a relationship-type retention watermark
// (no node watermark) refuses the plural door below the relationship's own
// type watermark, exactly where the singular door refuses, while a
// relationship of a type purged lower answers at the same pin.
func effectiveTimelinesRelTypeRetention(t *testing.T) {
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			useTestClock(t, g)
			e := newCCEnt(t, g, true)
			purged := e.add("F", 1000, nil)
			r, err := g.Rels.AddByID(e.ctx, "M", e.start, e.end, map[string]any{"tkg_valid_from": types.Instant(1000), "x": int64(0)})
			if err != nil {
				t.Fatalf("AddByID(M): %v", err)
			}
			other := int64(r.ID())
			p0 := e.pin()
			e.mustCascade(purged, 1100, 1200, map[string]any{"x": int64(1)})
			e.mustCascade(other, 1100, 1200, map[string]any{"x": int64(1)})
			low := e.pin()
			e.mustCascade(purged, 1150, 1300, map[string]any{"x": int64(2)})
			e.mustCascade(other, 1150, 1300, map[string]any{"x": int64(2)})
			w := e.pin()
			tokL, okL := g.relTypes.Lookup(ccType)
			tokM, okM := g.relTypes.Lookup("M")
			if !okL || !okM {
				t.Fatalf("rel type tokens: %v %v", okL, okM)
			}
			if err := g.advanceRelRetentionWatermark(tokM, low); err != nil {
				t.Fatalf("advanceRelRetentionWatermark(M): %v", err)
			}
			if err := g.advanceRelRetentionWatermark(tokL, w); err != nil {
				t.Fatalf("advanceRelRetentionWatermark(L): %v", err)
			}
			if _, err := g.Temporal.RelEffectiveTimeline(types.RelID(purged), low); !errors.Is(err, ErrRetentionExpired) {
				t.Fatalf("singular door below the type watermark: %v, want ErrRetentionExpired", err)
			}
			for _, pins := range [][]types.Instant{{w, low}, {low, w}, {w - 1}} {
				if _, rows, err := e.timelinesRows(purged, pins); !errors.Is(err, ErrRetentionExpired) || rows != 0 {
					t.Fatalf("pins %v below the type watermark: %v, %d rows read; want ErrRetentionExpired, 0 rows", pins, err, rows)
				}
			}
			e.sameAsSingular("at the type watermark", purged, []types.Instant{w, w})
			e.sameAsSingular("other type, above its watermark", other, []types.Instant{w, low, w - 1})
			if _, _, err := e.timelinesRows(other, []types.Instant{p0}); !errors.Is(err, ErrRetentionExpired) {
				t.Fatalf("other type below its watermark: %v, want ErrRetentionExpired", err)
			}
		})
	}
}

func effectiveTimelinesCost(t *testing.T) {
	ccRun(t, false, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		// One fact of 511 versions, each a correction of its own valid
		// interval: at the k-th stamp the timeline holds k segments, so the
		// singular door hydrates up to k rows per pin even on a store with
		// temporal skeletons (badger) — the cost the plural door must not
		// multiply by the pins.
		id := e.add("F", 1000, nil)
		for i := 1; i < manyPinsVersions; i++ {
			from := types.Instant(1000 + 10*i)
			e.mustCascade(id, from, from+10, map[string]any{"x": int64(i)})
		}
		stamps := e.stamps(id)
		versions := len(e.chain(id))
		if versions < manyPinsVersions {
			t.Fatalf("chain has %d rows, want >= %d", versions, manyPinsVersions)
		}
		limit := versions + 8 // one chain walk plus a few point reads
		for _, p := range []int{16, 64, 511} {
			pins := make([]types.Instant, p)
			for i := range pins {
				pins[i] = stamps[i*len(stamps)/p]
			}
			slices.Reverse(pins) // unsorted on purpose
			got, rows, err := e.timelinesRows(id, pins)
			if err != nil {
				t.Fatalf("P=%d: %v", p, err)
			}
			want, before := e.singleRows(id, pins)
			t.Logf("%s P=%d: history rows read: plural %d, singular per pin %d (versions %d)", e.kind(), p, rows, before, versions)
			for i := range pins {
				if got[i] != want[i] {
					t.Fatalf("P=%d pin %d (#%d) = %s\n  want (singular) %s", p, pins[i], i, got[i], want[i])
				}
			}
			if rows > limit {
				t.Errorf("P=%d: %d history rows read, want <= %d (versions + 8, flat in pins)", p, rows, limit)
			}
		}
	})
}

// The break cases on a short chain: alignment, empty, out-of-range and
// refused pins, unknown, invalid, deleted and history-less entities.
func effectiveTimelinesBreakCases(t *testing.T) {
	ccRun(t, false, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		id := e.add("F", 1000, nil)
		p1 := e.pin()
		e.mustCascade(id, 1100, 1200, map[string]any{"x": int64(1)})
		p2 := e.pin()
		e.mustCascade(id, 1150, 1300, map[string]any{"x": int64(2)})
		p3 := e.pin()

		e.sameAsSingular("duplicate pins", id, []types.Instant{p2, p2, p3, p2})
		e.sameAsSingular("unsorted pins", id, []types.Instant{p3, p1, p2})
		e.sameAsSingular("pins at or below 0", id, []types.Instant{0, -1, p2, 0})
		e.sameAsSingular("one pin", id, []types.Instant{p3})

		for _, pins := range [][]types.Instant{nil, {}} {
			got, rows, err := e.timelinesRows(id, pins)
			if err != nil || len(got) != 0 || rows != 0 {
				t.Fatalf("empty pins %v: %v, %v, %d rows read; want empty, nil, 0", pins, got, err, rows)
			}
		}

		peek, err := e.g.Temporal.PeekTx()
		if err != nil {
			t.Fatal(err)
		}
		for _, pins := range [][]types.Instant{{p1, peek + 3_600_000}, {peek + 3_600_000, p1}} {
			if _, rows, err := e.timelinesRows(id, pins); !errors.Is(err, ErrTxPinTooNew) || rows != 0 {
				t.Fatalf("pin above the clock %v: %v, %d rows read; want ErrTxPinTooNew, 0 rows", pins, err, rows)
			}
		}
		if _, _, err := e.timelinesRows(id+1<<22, []types.Instant{p1}); !errors.Is(err, e.notFound()) {
			t.Fatalf("unknown id: %v, want %v", err, e.notFound())
		}
		if _, _, err := e.timelinesRows(0, []types.Instant{p1}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("id 0: %v, want ErrInvalidStoreMutation", err)
		}

		// An entity without history and one deleted between the pins.
		plain := e.add("P", 1000, nil)
		gone := e.add("G", 1000, nil)
		e.mustCascade(gone, 1100, 1200, map[string]any{"x": int64(1)})
		pBefore := e.pin()
		e.mustDel(gone)
		pAfter := e.pin()
		e.sameAsSingular("entity without history", plain, []types.Instant{pAfter, p1, pBefore})
		e.sameAsSingular("deleted entity", gone, []types.Instant{pAfter, p1, pBefore, pAfter})
	})
}

// Compacted and retention-expired pins refuse the whole call before a row is
// read, with the singular door's sentinel, wherever the bad pin stands.
// Memory: compaction and retention are core-level.
func effectiveTimelinesCompactedAndRetention(t *testing.T) {
	for _, rel := range []bool{false, true} {
		t.Run(map[bool]string{false: "node", true: "rel"}[rel], func(t *testing.T) {
			g := txbBackends()[0].open(t, false)
			useTestClock(t, g)
			e := newCCEnt(t, g, rel)
			id := e.add("F", 1000, nil)
			p0 := e.pin()
			e.mustUpdate(id, map[string]any{"tkg_valid_from": types.Instant(2000), "x": int64(1)})
			e.mustUpdate(id, map[string]any{"tkg_valid_from": types.Instant(3000), "x": int64(2)})
			pHigh := e.pin()
			var err error
			if rel {
				_, err = g.Admin.CompactHistoryRels(context.Background(), RetentionPolicy{KeepVersions: 1})
			} else {
				_, err = g.Admin.CompactHistoryNodes(context.Background(), RetentionPolicy{KeepVersions: 1})
			}
			if err != nil {
				t.Fatalf("compact: %v", err)
			}
			for _, pins := range [][]types.Instant{{pHigh, p0}, {p0, pHigh}} {
				if _, rows, err := e.timelinesRows(id, pins); !errors.Is(err, ErrHistoryCompacted) || rows != 0 {
					t.Fatalf("compacted pin %v: %v, %d rows read; want ErrHistoryCompacted, 0 rows", pins, err, rows)
				}
			}
			e.sameAsSingular("above compaction", id, []types.Instant{pHigh, pHigh})

			w := e.pin()
			if err := g.advanceRetentionWatermark(nodeLabelToken(t, g, ccLabel), w); err != nil {
				t.Fatalf("advanceRetentionWatermark: %v", err)
			}
			if _, rows, err := e.timelinesRows(id, []types.Instant{w, w - 1}); !errors.Is(err, ErrRetentionExpired) || rows != 0 {
				t.Fatalf("pin below retention: %v, %d rows read; want ErrRetentionExpired, 0 rows", err, rows)
			}
			e.sameAsSingular("at the retention watermark", id, []types.Instant{w})
		})
	}
}
