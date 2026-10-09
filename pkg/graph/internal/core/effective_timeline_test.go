package core

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Targeted break-the-code tests of the effective-timeline doors (handover
// effective-read-cost fix 1c, backlog 27). The pointwise property over the
// randomized oracle is in effective_timeline_oracle_test.go.

// timeline returns T's segments at pin as "[from,to) vN" lines (node or rel).
func (e *ccEnt) timeline(id int64, pin types.Instant) (string, error) {
	e.t.Helper()
	var segs []etSeg
	if e.rel {
		ss, err := e.g.Temporal.RelEffectiveTimeline(types.RelID(id), pin)
		if err != nil {
			return "", err
		}
		segs = etRelSegs(ss)
	} else {
		ss, err := e.g.Temporal.NodeEffectiveTimeline(types.NodeID(id), pin)
		if err != nil {
			return "", err
		}
		segs = etNodeSegs(ss)
	}
	return etSegLine(segs), nil
}

func etSegLine(segs []etSeg) string {
	parts := make([]string, len(segs))
	for i, s := range segs {
		parts[i] = fmt.Sprintf("[%d,%d) v%d", s.from, s.to, s.version)
	}
	return strings.Join(parts, " ")
}

func (e *ccEnt) mustTimeline(id int64, pin types.Instant) string {
	e.t.Helper()
	s, err := e.timeline(id, pin)
	if err != nil {
		e.t.Fatalf("%s timeline(%d, %d): %v", e.kind(), id, pin, err)
	}
	return s
}

// versionWith returns the version of T's row with the given own interval.
func (e *ccEnt) versionWith(id int64, vf, vt types.Instant) uint32 {
	e.t.Helper()
	for _, r := range e.chain(id) {
		if r.tm.ValidFrom == vf && r.tm.ValidTo == vt && r.tm.DeletedAt == 0 {
			return r.version
		}
	}
	e.t.Fatalf("no row [%d,%d):%s", vf, vt, e.chainString(id))
	return 0
}

func (e *ccEnt) deletedAt(id int64) types.Instant {
	var d types.Instant
	for _, r := range e.chain(id) {
		d = max(d, r.tm.DeletedAt)
	}
	return d
}

// addNoVF creates T without tkg_valid_from (plain or AddWithTx at txFrom).
func (e *ccEnt) addNoVF(name string, txFrom types.Instant) int64 {
	e.t.Helper()
	props := map[string]any{"x": int64(0)}
	var id int64
	switch {
	case e.rel && txFrom != 0:
		s, _ := e.g.Nodes.Get(e.ctx, e.start)
		en, _ := e.g.Nodes.Get(e.ctx, e.end)
		r, err := e.g.Rels.AddWithTx(e.ctx, ccType, s, en, props, txFrom)
		if err != nil {
			e.t.Fatalf("Rels.AddWithTx: %v", err)
		}
		id = int64(r.ID())
	case e.rel:
		r, err := e.g.Rels.AddByID(e.ctx, ccType, e.start, e.end, props)
		if err != nil {
			e.t.Fatalf("AddByID: %v", err)
		}
		id = int64(r.ID())
	case txFrom != 0:
		n, err := e.g.Nodes.AddWithTx(e.ctx, []string{ccLabel}, props, txFrom)
		if err != nil {
			e.t.Fatalf("Nodes.AddWithTx: %v", err)
		}
		id = int64(n.ID())
	default:
		n, err := e.g.Nodes.Add(e.ctx, []string{ccLabel}, props)
		if err != nil {
			e.t.Fatalf("Add: %v", err)
		}
		id = int64(n.ID())
	}
	e.names[id] = name
	return id
}

func (e *ccEnt) mint(id int64) types.Instant {
	if e.rel {
		return storeutil.SnowflakeInstant(types.RelID(id).SnowflakeID())
	}
	return storeutil.SnowflakeInstant(types.NodeID(id).SnowflakeID())
}

// TestEffectiveTimeline_DerivedStart — (b). A row without a recorded
// valid-from (plain Add, AddWithTx backfill) starts at the ID's mint instant:
// the first segment's ValidFrom is that instant — not 0, not the TxFrom — and
// the point door agrees on both sides of it. A later Update without a
// valid-from starts at its UpdatedAt. Catches a door that reports 0 (the raw
// stored value), the backfill's TxFrom, or the write time as the start.
func TestEffectiveTimeline_DerivedStart(t *testing.T) {
	t.Parallel()
	ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		for _, backfill := range []bool{false, true} {
			t.Run(fmt.Sprintf("backfill=%v", backfill), func(t *testing.T) {
				e := mk(t)
				var txFrom types.Instant
				if backfill {
					txFrom = e.pin() - 60_000
				}
				id := e.addNoVF("T", txFrom)
				mint := e.mint(id)
				pin := e.pin()
				if got, want := e.mustTimeline(id, pin), fmt.Sprintf("[%d,0) v0", mint); got != want {
					t.Fatalf("%s timeline = %q, want %q (derived start = mint instant)%s", e.kind(), got, want, e.chainString(id))
				}
				if e.atTxPresent(id, mint-1, pin) || !e.atTxPresent(id, mint, pin) {
					t.Fatalf("point door disagrees around the mint instant %d", mint)
				}
				if backfill {
					// Pinned between the backfill's TxFrom and now: the row is
					// recorded, its start is still the mint instant.
					if got, want := e.mustTimeline(id, txFrom), fmt.Sprintf("[%d,0) v0", mint); got != want {
						t.Fatalf("timeline at the backfill pin = %q, want %q", got, want)
					}
					if got := e.mustTimeline(id, txFrom-1); got != "" {
						t.Fatalf("timeline before the backfill's TxFrom = %q, want empty", got)
					}
				}
				e.mustUpdate(id, map[string]any{"x": int64(1)})
				var upd types.Instant
				for _, r := range e.chain(id) {
					if r.current {
						upd = r.tm.UpdatedAt
					}
				}
				pin2 := e.pin()
				if got, want := e.mustTimeline(id, pin2), fmt.Sprintf("[%d,%d) v0 [%d,0) v1", mint, upd, upd); got != want {
					t.Fatalf("%s timeline after an Update without valid-from = %q, want %q%s", e.kind(), got, want, e.chainString(id))
				}
				if got, want := e.mustTimeline(id, pin), fmt.Sprintf("[%d,0) v0", mint); got != want {
					t.Fatalf("timeline at the earlier pin changed: %q, want %q", got, want)
				}
			})
		}
	})
}

// TestEffectiveTimeline_Shapes pins the segments of the lifecycle shapes the
// consumer reads (written down from the writes): a bounded cascade, a close, a
// close with a gap piece, a delete after a bounded cascade (last segment ends
// at the delete instant), at a pin before and after each write. Catches a
// segment end off by one, the current row reported at a pin before the
// write that made it current, a dropped gap, a deleted entity's last segment
// running open, and an entity created after the pin reported.
func TestEffectiveTimeline_Shapes(t *testing.T) {
	t.Parallel()
	ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		p0 := e.pin()
		id := e.add("T", 1000, nil)
		p1 := e.pin()
		if got := e.mustTimeline(id, p0); got != "" {
			t.Fatalf("created after the pin: %q, want empty", got)
		}
		if got := e.mustTimeline(id, p1); got != "[1000,0) v0" {
			t.Fatalf("plain: %q", got)
		}
		e.mustCascade(id, 2000, 3000, map[string]any{"x": int64(1)})
		p2 := e.pin()
		piece, res := e.versionWith(id, 2000, 3000), e.versionWith(id, 3000, 0)
		want2 := fmt.Sprintf("[1000,2000) v0 [2000,3000) v%d [3000,0) v%d", piece, res)
		if got := e.mustTimeline(id, p2); got != want2 {
			t.Fatalf("bounded cascade: %q, want %q%s", got, want2, e.chainString(id))
		}
		e.mustClose(id, 4000)
		p3 := e.pin()
		closed := e.versionWith(id, 3000, 4000)
		want3 := fmt.Sprintf("[1000,2000) v0 [2000,3000) v%d [3000,4000) v%d", piece, closed)
		if got := e.mustTimeline(id, p3); got != want3 {
			t.Fatalf("close: %q, want %q%s", got, want3, e.chainString(id))
		}
		e.mustCascade(id, 5000, 6000, map[string]any{"x": int64(2)})
		p4 := e.pin()
		gap := e.versionWith(id, 5000, 6000)
		want4 := want3 + fmt.Sprintf(" [5000,6000) v%d", gap)
		if got := e.mustTimeline(id, p4); got != want4 {
			t.Fatalf("gap piece: %q, want %q%s", got, want4, e.chainString(id))
		}
		b := e.add("B", 1000, nil)
		e.mustCascade(b, 2000, 3000, map[string]any{"x": int64(1)})
		bp, br := e.versionWith(b, 2000, 3000), e.versionWith(b, 3000, 0)
		e.mustDel(b)
		p5 := e.pin()
		d := e.deletedAt(b)
		want5 := fmt.Sprintf("[1000,2000) v0 [2000,3000) v%d [3000,%d) v%d", bp, d, br)
		if got := e.mustTimeline(b, p5); got != want5 {
			t.Fatalf("delete after bounded cascade: %q, want %q%s", got, want5, e.chainString(b))
		}
		// Earlier pins are unchanged by every later write (rule 15).
		for pin, want := range map[types.Instant]string{p0: "", p1: "[1000,0) v0", p2: want2, p3: want3, p4: want4} {
			if got := e.mustTimeline(id, pin); got != want {
				t.Fatalf("pin %d changed after later writes: %q, want %q", pin, got, want)
			}
		}
	})
}

// TestEffectiveTimeline_Errors — (d). Pin validation and sentinels, through
// errors.Is, on every door: pin <= 0 → ErrInvalidTimeRange; a pin ahead of
// the commit clock → ErrTxPinTooNew; unknown ID → not-found; invalid ID →
// ErrInvalidStoreMutation; closed graph → ErrGraphClosed; nil callback →
// ErrNilCallback; an unknown label/type answers nothing only after the pin
// is validated. Catches a door that treats pin 0 as "no pin", accepts a
// future pin, or shortcuts an unknown type before validating.
func TestEffectiveTimeline_Errors(t *testing.T) {
	t.Parallel()
	ccRun(t, false, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		id := e.add("T", 1000, nil)
		pin := e.pin()
		peek, err := e.g.Temporal.PeekTx()
		if err != nil {
			t.Fatalf("PeekTx: %v", err)
		}
		future := peek + 3_600_000
		unknown := int64(types.NodeID(id)) + 1<<22
		scan := func(name string, p types.Instant, fn func() bool) error {
			if e.rel {
				return e.g.Temporal.ForEachRelEffectiveByType(name, p, func(temporal.RelSegment) bool { return fn() })
			}
			return e.g.Temporal.ForEachNodeEffectiveByLabel(name, p, func(temporal.NodeSegment) bool { return fn() })
		}
		never := func() bool { t.Fatalf("callback called"); return false }
		for _, c := range []struct {
			name string
			run  func() error
			want error
		}{
			{"timeline pin 0", func() error { _, err := e.timeline(id, 0); return err }, ErrInvalidTimeRange},
			{"timeline pin -1", func() error { _, err := e.timeline(id, -1); return err }, ErrInvalidTimeRange},
			{"timeline future pin", func() error { _, err := e.timeline(id, future); return err }, ErrTxPinTooNew},
			{"timeline unknown id", func() error { _, err := e.timeline(unknown, pin); return err }, e.notFound()},
			{"timeline id 0", func() error { _, err := e.timeline(0, pin); return err }, storepkg.ErrInvalidStoreMutation},
			{"timeline id -5", func() error { _, err := e.timeline(-5, pin); return err }, storepkg.ErrInvalidStoreMutation},
			{"scan pin 0", func() error { return scan(map[bool]string{false: ccLabel, true: ccType}[e.rel], 0, never) }, ErrInvalidTimeRange},
			{"scan future pin", func() error { return scan(map[bool]string{false: ccLabel, true: ccType}[e.rel], future, never) }, ErrTxPinTooNew},
			{"scan unknown name, pin 0", func() error { return scan("NoSuchName", 0, never) }, ErrInvalidTimeRange},
			{"scan unknown name, future pin", func() error { return scan("NoSuchName", future, never) }, ErrTxPinTooNew},
			{"scan empty name", func() error { return scan("", pin, never) }, nil},
		} {
			err := c.run()
			if c.want == nil {
				if err == nil {
					t.Fatalf("%s: nil error, want a validation error", c.name)
				}
				continue
			}
			if !errors.Is(err, c.want) {
				t.Fatalf("%s: err = %v, want errors.Is %v", c.name, err, c.want)
			}
		}
		// Nil callback.
		var err2 error
		if e.rel {
			err2 = e.g.Temporal.ForEachRelEffectiveByType(ccType, pin, nil)
		} else {
			err2 = e.g.Temporal.ForEachNodeEffectiveByLabel(ccLabel, pin, nil)
		}
		if !errors.Is(err2, grapherr.ErrNilCallback) {
			t.Fatalf("nil callback: %v, want ErrNilCallback", err2)
		}
		// Unknown name with a valid pin: nothing, no error.
		if err := scan("NoSuchName", pin, never); err != nil {
			t.Fatalf("unknown name: %v", err)
		}
		// The pin at the clock is accepted.
		if _, err := e.timeline(id, peek); err != nil {
			t.Fatalf("pin == PeekTx: %v", err)
		}
		// Closed graph.
		if err := e.g.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if _, err := e.timeline(id, pin); !errors.Is(err, ErrGraphClosed) {
			t.Fatalf("closed timeline: %v, want ErrGraphClosed", err)
		}
		if err := scan(map[bool]string{false: ccLabel, true: ccType}[e.rel], pin, never); !errors.Is(err, ErrGraphClosed) {
			t.Fatalf("closed scan: %v, want ErrGraphClosed", err)
		}
	})
}

// TestEffectiveTimeline_CompactionAndRetention — (d) continued: a pin below a
// compacted chain's knowledge or below a retention watermark is refused with
// the point doors' sentinel (per entity) and the scan doors' (graph
// watermark); a pin at or above answers. Memory (compaction and retention are
// core-level; the store only holds the trimmed chain).
func TestEffectiveTimeline_CompactionAndRetention(t *testing.T) {
	t.Parallel()
	for _, rel := range []bool{false, true} {
		t.Run(map[bool]string{false: "node", true: "rel"}[rel], func(t *testing.T) {
			g := txbBackends()[0].open(t, false)
			useTestClock(t, g)
			e := newCCEnt(t, g, rel)
			id := e.add("T", 1000, nil)
			// P: a plain entity of another label / type; nothing of it is
			// compacted or purged, so only the graph watermarks refuse its scan.
			var pID int64
			if rel {
				r, err := g.Rels.AddByID(context.Background(), "Q", e.start, e.end, map[string]any{"tkg_valid_from": types.Instant(1000)})
				if err != nil {
					t.Fatalf("add P: %v", err)
				}
				pID = int64(r.ID())
			} else {
				n, err := g.Nodes.Add(context.Background(), []string{"Q"}, map[string]any{"tkg_valid_from": types.Instant(1000)})
				if err != nil {
					t.Fatalf("add P: %v", err)
				}
				pID = int64(n.ID())
			}
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
			if _, err := e.timeline(id, p0); !errors.Is(err, ErrHistoryCompacted) {
				t.Fatalf("timeline below compaction: %v, want ErrHistoryCompacted", err)
			}
			scanName := map[bool]string{false: ccLabel, true: ccType}[rel]
			scan := func(p types.Instant) error {
				if rel {
					return g.Temporal.ForEachRelEffectiveByType(scanName, p, func(temporal.RelSegment) bool { return true })
				}
				return g.Temporal.ForEachNodeEffectiveByLabel(scanName, p, func(temporal.NodeSegment) bool { return true })
			}
			if err := scan(p0); !errors.Is(err, ErrHistoryCompacted) {
				t.Fatalf("scan below compaction: %v, want ErrHistoryCompacted", err)
			}
			// P's own chain is whole: its timeline answers at p0 (as NodeAtTx
			// does), but a scan pinned there fails on the graph watermark —
			// even of a label / type none of whose entities was compacted.
			if _, err := e.timeline(pID, p0); err != nil {
				t.Fatalf("timeline of the uncompacted P below the watermark: %v", err)
			}
			scanQ := func(p types.Instant) error {
				if rel {
					return g.Temporal.ForEachRelEffectiveByType("Q", p, func(temporal.RelSegment) bool { return true })
				}
				return g.Temporal.ForEachNodeEffectiveByLabel("Q", p, func(temporal.NodeSegment) bool { return true })
			}
			if err := scanQ(p0); !errors.Is(err, ErrHistoryCompacted) {
				t.Fatalf("scan of Q below the graph compaction watermark: %v, want ErrHistoryCompacted", err)
			}
			if _, err := e.timeline(id, pHigh); err != nil {
				t.Fatalf("timeline above compaction: %v", err)
			}
			if err := scan(pHigh); err != nil {
				t.Fatalf("scan above compaction: %v", err)
			}
			// Retention watermark above every pin used so far.
			w := e.pin()
			tok := nodeLabelToken(t, g, ccLabel)
			if err := g.advanceRetentionWatermark(tok, w); err != nil {
				t.Fatalf("advanceRetentionWatermark: %v", err)
			}
			if _, err := e.timeline(id, w-1); !errors.Is(err, ErrRetentionExpired) {
				t.Fatalf("timeline below retention: %v, want ErrRetentionExpired", err)
			}
			if err := scan(w - 1); !errors.Is(err, ErrRetentionExpired) {
				t.Fatalf("scan below retention: %v, want ErrRetentionExpired", err)
			}
			if err := scanQ(w - 1); !errors.Is(err, ErrRetentionExpired) {
				t.Fatalf("scan of Q below the graph retention watermark: %v, want ErrRetentionExpired", err)
			}
			if _, err := e.timeline(id, w); err != nil {
				t.Fatalf("timeline at the retention watermark: %v", err)
			}
		})
	}
}

// scanSegs runs the scan door at pin and returns entity name → segment line,
// failing on non-contiguous or descending segments of one entity.
func (e *ccEnt) scanSegs(name string, pin types.Instant) map[string]string {
	e.t.Helper()
	out := map[string][]etSeg{}
	var order []int64
	last := int64(0)
	visit := func(id int64, s etSeg) {
		if id != last {
			for _, seen := range order {
				if seen == id {
					e.t.Fatalf("segments of %d are not contiguous", id)
				}
			}
			order = append(order, id)
			last = id
		}
		n := e.names[id]
		if n == "" {
			n = fmt.Sprintf("?%d", id)
		}
		out[n] = append(out[n], s)
	}
	var err error
	if e.rel {
		err = e.g.Temporal.ForEachRelEffectiveByType(name, pin, func(s temporal.RelSegment) bool {
			visit(int64(s.Rel.ID()), etRelSegs([]temporal.RelSegment{s})[0])
			return true
		})
	} else {
		err = e.g.Temporal.ForEachNodeEffectiveByLabel(name, pin, func(s temporal.NodeSegment) bool {
			visit(int64(s.Node.ID()), etNodeSegs([]temporal.NodeSegment{s})[0])
			return true
		})
	}
	if err != nil {
		e.t.Fatalf("scan(%q, %d): %v", name, pin, err)
	}
	res := map[string]string{}
	for n, ss := range out {
		if err := etStructure(ss); err != nil {
			e.t.Fatalf("scan entity %s: %v%s", n, err, etSegsString(ss))
		}
		res[n] = etSegLine(ss)
	}
	return res
}

func renderScan(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "\n  %s: %s", k, m[k])
	}
	return b.String()
}

// TestEffectiveTimeline_ScanDeletedEntities — (c). The scan forms enumerate
// every entity of the type / label recorded by the pin, INCLUDING those
// deleted before the pin, each with its full timeline at the pin: a live, a
// closed, a cascaded, a deleted-before-pin, a deleted-after-pin and a
// created-after-pin entity plus a bystander of another type / label, at a pin
// between the writes and after them (exact sets). Nodes add a label gained
// after the first pin and a label lost after it: a node is listed with the
// segments whose row carries the label. fn returning false stops the scan.
// Catches a scan that drops history-only (deleted) entities, lists an entity
// created after the pin, reports current rows at an earlier pin, or ignores
// fn's stop.
func TestEffectiveTimeline_ScanDeletedEntities(t *testing.T) {
	t.Parallel()
	ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		name := map[bool]string{false: ccLabel, true: ccType}[e.rel]
		live := e.add("Live", 1000, nil)
		closed := e.add("Closed", 1000, nil)
		e.mustClose(closed, 4000)
		casc := e.add("Casc", 1000, nil)
		e.mustCascade(casc, 2000, 3000, map[string]any{"x": int64(1)})
		delBefore := e.add("DelBefore", 1000, nil)
		e.mustDel(delBefore)
		delAfter := e.add("DelAfter", 1000, nil)
		// A bystander of another type / label.
		if e.rel {
			if _, err := e.g.Rels.AddByID(e.ctx, "Other", e.start, e.end, map[string]any{"tkg_valid_from": types.Instant(1000)}); err != nil {
				t.Fatalf("add bystander: %v", err)
			}
		} else if _, err := e.g.Nodes.Add(e.ctx, []string{"Other"}, map[string]any{"tkg_valid_from": types.Instant(1000)}); err != nil {
			t.Fatalf("add bystander: %v", err)
		}
		var gains, loses int64
		if !e.rel {
			n, err := e.g.Nodes.Add(e.ctx, []string{"Other"}, map[string]any{"tkg_valid_from": types.Instant(1000)})
			if err != nil {
				t.Fatalf("add gains: %v", err)
			}
			gains = int64(n.ID())
			e.names[gains] = "Gains"
			ln, err := e.g.Nodes.Add(e.ctx, []string{ccLabel, "Other"}, map[string]any{"tkg_valid_from": types.Instant(1000), "x": int64(0)})
			if err != nil {
				t.Fatalf("add loses: %v", err)
			}
			loses = int64(ln.ID())
			e.names[loses] = "Loses"
		}
		p1 := e.pin()
		e.mustDel(delAfter)
		created := e.add("CreatedAfter", 1000, nil)
		if !e.rel {
			if err := e.g.Nodes.AddLabel(e.ctx, types.NodeID(gains), ccLabel); err != nil {
				t.Fatalf("AddLabel: %v", err)
			}
			if err := e.g.Nodes.RemoveLabel(e.ctx, types.NodeID(loses), ccLabel); err != nil {
				t.Fatalf("RemoveLabel: %v", err)
			}
		}
		p2 := e.pin()
		_ = created
		dB, dA := e.deletedAt(delBefore), e.deletedAt(delAfter)
		cascPiece, cascRes := e.versionWith(casc, 2000, 3000), e.versionWith(casc, 3000, 0)
		closedV := e.versionWith(closed, 1000, 4000)
		common := map[string]string{
			"Live":      "[1000,0) v0",
			"Closed":    fmt.Sprintf("[1000,4000) v%d", closedV),
			"Casc":      fmt.Sprintf("[1000,2000) v0 [2000,3000) v%d [3000,0) v%d", cascPiece, cascRes),
			"DelBefore": fmt.Sprintf("[1000,%d) v0", dB),
		}
		want1 := map[string]string{"DelAfter": "[1000,0) v0"}
		want2 := map[string]string{"DelAfter": fmt.Sprintf("[1000,%d) v0", dA), "CreatedAfter": "[1000,0) v0"}
		if !e.rel {
			want1["Loses"] = "[1000,0) v0"
			// A label change starts its row at the write (UpdatedAt): the
			// gained label holds from then on, the lost one until then.
			var gv uint32
			var gAt, lAt types.Instant
			for _, r := range e.chain(gains) {
				if r.current {
					gv, gAt = r.version, r.tm.UpdatedAt
				}
			}
			for _, r := range e.chain(loses) {
				if r.current {
					lAt = r.tm.UpdatedAt
				}
			}
			want2["Gains"] = fmt.Sprintf("[%d,0) v%d", gAt, gv)
			want2["Loses"] = fmt.Sprintf("[1000,%d) v0", lAt)
		}
		for _, w := range []map[string]string{want1, want2} {
			for k, v := range common {
				w[k] = v
			}
		}
		_ = live
		for _, c := range []struct {
			pin  types.Instant
			want map[string]string
		}{{p1, want1}, {p2, want2}} {
			if got := e.scanSegs(name, c.pin); renderScan(got) != renderScan(c.want) {
				t.Fatalf("%s scan at pin %d:\n got %s\nwant %s", e.kind(), c.pin, renderScan(got), renderScan(c.want))
			}
		}
		// Stop: fn returning false after the first segment ends the scan.
		calls := 0
		stop := func() bool { calls++; return false }
		var err error
		if e.rel {
			err = e.g.Temporal.ForEachRelEffectiveByType(name, p2, func(temporal.RelSegment) bool { return stop() })
		} else {
			err = e.g.Temporal.ForEachNodeEffectiveByLabel(name, p2, func(temporal.NodeSegment) bool { return stop() })
		}
		if err != nil || calls != 1 {
			t.Fatalf("stop: err=%v calls=%d, want nil and 1", err, calls)
		}
	})
}

// TestEffectiveTimeline_ScanMatchesGenericDoor — rule 17 between the scan
// forms and the generic pinned doors, on the oracle's chains: at every pin and
// every valid instant t where any entity's rows change, the scan's segments
// containing t are exactly ByType / ByLabel{ValidAt: t, TxAt: pin} (same
// entities, same rows), and each listed entity's segments equal the
// per-entity door's segments whose row carries the type / label. Catches a
// scan that drops history-only (deleted) entities, invents an entity created
// after the pin, filters by the current row's label instead of the row valid
// at t, or diverges from the per-entity timeline.
func TestEffectiveTimeline_ScanMatchesGenericDoor(t *testing.T) {
	t.Parallel()
	x0 := etX0()
	segments := 0
	nSeeds := 4
	if isRaceEnabled() {
		nSeeds = 2 // stated bound under -race (see TestEffectiveTimeline_PointwiseOracle)
	}
	for _, be := range txbBackends() {
		for i := 0; i < nSeeds; i++ {
			seed := uint64(0x5CA7) + uint64(i)
			t.Run(fmt.Sprintf("%s/seed=%d", be.name, seed), func(t *testing.T) {
				_, _, _, pins, o := txbOracleRun(t, be, seed, 40, x0, nil, nil)
				g := o.g
				k := &etChecker{t: t, g: g}
				var probes []types.Instant
				for _, id := range o.allNodeIDs() {
					probes = append(probes, k.nodeStamps(id)...)
				}
				for _, id := range o.relIDs {
					probes = append(probes, k.relStamps(id)...)
				}
				probes = etProbes(probes, nil)
				pins = append(etSample(pins, 5), o.x)
				for _, pin := range pins {
					if pin > o.x {
						continue
					}
					for _, typ := range oracleRelTypes {
						got := map[types.RelID][]temporal.RelSegment{}
						if err := g.Temporal.ForEachRelEffectiveByType(typ, pin, func(s temporal.RelSegment) bool {
							got[s.Rel.ID()] = append(got[s.Rel.ID()], s)
							segments++
							return true
						}); err != nil {
							t.Fatalf("scan %s pin %d: %v", typ, pin, err)
						}
						etCheckRelScan(t, g, typ, pin, got, probes)
					}
					for _, label := range oracleNodeLabels {
						got := map[types.NodeID][]temporal.NodeSegment{}
						if err := g.Temporal.ForEachNodeEffectiveByLabel(label, pin, func(s temporal.NodeSegment) bool {
							got[s.Node.ID()] = append(got[s.Node.ID()], s)
							segments++
							return true
						}); err != nil {
							t.Fatalf("scan %s pin %d: %v", label, pin, err)
						}
						etCheckNodeScan(t, g, label, pin, got, probes)
					}
				}
			})
		}
	}
	if !t.Failed() && segments == 0 {
		t.Fatalf("the scans returned no segment at all")
	}
}

// etSample keeps at most n evenly spaced values of a sorted copy of vs.
func etSample(vs []types.Instant, n int) []types.Instant {
	s := append([]types.Instant(nil), vs...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	if len(s) <= n {
		return s
	}
	out := make([]types.Instant, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, s[i*(len(s)-1)/(n-1)])
	}
	return out
}

func etCheckRelScan(t *testing.T, g *Core, typ string, pin types.Instant, got map[types.RelID][]temporal.RelSegment, probes []types.Instant) {
	t.Helper()
	tok, ok := g.lookupRelTypeQueryToken(typ)
	for id, segs := range got {
		want, err := g.Temporal.RelEffectiveTimeline(id, pin)
		if err != nil {
			t.Fatalf("RelEffectiveTimeline(%v, %d): %v", id, pin, err)
		}
		var filtered []temporal.RelSegment
		for _, s := range want {
			if ok && s.Rel.HasTypeTokenRaw(tok) {
				filtered = append(filtered, s)
			}
		}
		if a, b := etSegsString(etRelSegs(segs)), etSegsString(etRelSegs(filtered)); a != b {
			t.Fatalf("type %s pin %d rel %v:\n scan   %s\n entity %s", typ, pin, id, a, b)
		}
	}
	for _, va := range probes {
		rs, err := g.Rels.ByType(typ, storepkg.QueryOpts{ValidAt: va, TxAt: pin})
		if err != nil {
			t.Fatalf("ByType: %v", err)
		}
		want := map[types.RelID]string{}
		for _, r := range rs {
			want[r.ID()] = etRelKey(r)
		}
		have := map[types.RelID]string{}
		for id, segs := range got {
			if s, in := etAt(etRelSegs(segs), va); in {
				have[id] = s.key
			}
		}
		if fmt.Sprint(want) != fmt.Sprint(have) {
			t.Fatalf("type %s pin %d valid %d:\n ByType %v\n scan   %v", typ, pin, va, want, have)
		}
	}
}

func etCheckNodeScan(t *testing.T, g *Core, label string, pin types.Instant, got map[types.NodeID][]temporal.NodeSegment, probes []types.Instant) {
	t.Helper()
	tok, ok := g.labels.Lookup(label)
	for id, segs := range got {
		want, err := g.Temporal.NodeEffectiveTimeline(id, pin)
		if err != nil {
			t.Fatalf("NodeEffectiveTimeline(%v, %d): %v", id, pin, err)
		}
		var filtered []temporal.NodeSegment
		for _, s := range want {
			if ok && s.Node.HasLabelTokenRaw(tok) {
				filtered = append(filtered, s)
			}
		}
		if a, b := etSegsString(etNodeSegs(segs)), etSegsString(etNodeSegs(filtered)); a != b {
			t.Fatalf("label %s pin %d node %v:\n scan   %s\n entity %s", label, pin, id, a, b)
		}
	}
	for _, va := range probes {
		ns, err := g.Nodes.ByLabel(label, storepkg.QueryOpts{ValidAt: va, TxAt: pin})
		if err != nil {
			t.Fatalf("ByLabel: %v", err)
		}
		want := map[types.NodeID]string{}
		for _, n := range ns {
			want[n.ID()] = etNodeKey(n)
		}
		have := map[types.NodeID]string{}
		for id, segs := range got {
			if s, in := etAt(etNodeSegs(segs), va); in {
				have[id] = s.key
			}
		}
		if fmt.Sprint(want) != fmt.Sprint(have) {
			t.Fatalf("label %s pin %d valid %d:\n ByLabel %v\n scan    %v", label, pin, va, want, have)
		}
	}
}

// TestEffectiveTimeline_MergeAdjacentSameRow — (e). Where a losing row's
// bound falls inside a winner's span the cut-and-resolve produces two pieces
// with the same winner; they merge into one segment. A synthetic chain (row
// v0 [1000,5000) with an explicit end, v1 open from 2000): the cut at 5000
// lies inside v1's span. Catches a builder that emits [2000,5000) v1 and
// [5000,0) v1 separately, and one that merges distinct rows.
func TestEffectiveTimeline_MergeAdjacentSameRow(t *testing.T) {
	t.Parallel()
	g := txbBackends()[0].open(t, false)
	id := types.NodeID(1 << 40)
	mk := func(v uint32, vf, vt, tx types.Instant, x int64) *types.Node {
		n := types.NewNode(id, 0, nil)
		if err := n.SetProperty("x", x); err != nil {
			t.Fatalf("SetProperty: %v", err)
		}
		n.SetVersion(v)
		n.SetTemporal(&types.TemporalMetadata{ValidFrom: vf, ValidTo: vt, TxFrom: tx, UpdatedAt: tx})
		return n
	}
	v0, v1 := mk(0, 1000, 5000, 10, 0), mk(1, 2000, 0, 20, 0)
	segs := g.nodeEffectivePieces([]*types.Node{v0, v1}, 30)
	var got []string
	for _, s := range segs {
		got = append(got, fmt.Sprintf("[%d,%d) v%d", s.from, s.to, s.row.Version()))
	}
	if strings.Join(got, " ") != "[1000,2000) v0 [2000,0) v1" {
		t.Fatalf("pieces = %v, want [1000,2000) v0 [2000,0) v1", got)
	}
	// Distinct rows with equal content never merge.
	w0, w1 := mk(0, 1000, 0, 10, 7), mk(1, 2000, 0, 20, 7)
	segs = g.nodeEffectivePieces([]*types.Node{w0, w1}, 30)
	if len(segs) != 2 {
		t.Fatalf("distinct rows with equal content merged: %d pieces", len(segs))
	}
}

// TestEffectiveTimeline_ConcurrentWriters — (g). Scans and per-entity reads at
// a pin while writers update, cascade, close and delete rows of the same
// type / label and add new ones: every scan answers exactly what it answered
// before the writers started (a pinned read is stable), and -race finds no
// data race. Every backend, node and rel.
func TestEffectiveTimeline_ConcurrentWriters(t *testing.T) {
	t.Parallel()
	ccRun(t, false, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		name := map[bool]string{false: ccLabel, true: ccType}[e.rel]
		var ids []int64
		for i := 0; i < 12; i++ {
			id := e.add(fmt.Sprintf("E%02d", i), 1000, nil)
			if i%3 == 0 {
				e.mustCascade(id, 2000, 3000, map[string]any{"x": int64(i)})
			}
			ids = append(ids, id)
		}
		pin := e.pin()
		beforeMap := e.scanSegs(name, pin)
		if len(beforeMap) != len(ids) {
			t.Fatalf("scan before the writers lists %d entities, want %d:%s", len(beforeMap), len(ids), renderScan(beforeMap))
		}
		before := renderScan(beforeMap)
		var wg sync.WaitGroup
		errc := make(chan error, 16)
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for j := w; j < len(ids); j += 4 {
					id := ids[j]
					var err error
					switch j % 4 {
					case 0:
						err = e.update(id, map[string]any{"x": int64(100 + j)})
					case 1:
						err = e.cascade(id, 1500, 2500, map[string]any{"x": int64(200 + j)})
					case 2:
						err = e.closeAt(id, 9000)
					case 3:
						err = e.del(id)
					}
					if err != nil {
						errc <- fmt.Errorf("writer %d on %d: %w", w, id, err)
						return
					}
				}
			}(w)
		}
		for r := 0; r < 6; r++ {
			if got := renderScan(e.scanSegs(name, pin)); got != before {
				t.Fatalf("pinned scan changed under concurrent writers:\n before %s\n now    %s", before, got)
			}
		}
		wg.Wait()
		close(errc)
		for err := range errc {
			t.Fatal(err)
		}
		if got := renderScan(e.scanSegs(name, pin)); got != before {
			t.Fatalf("pinned scan changed after the writers:\n before %s\n now    %s", before, got)
		}
	})
}
