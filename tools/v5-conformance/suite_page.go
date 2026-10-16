package conformance

import (
	"testing"
)

func init() {
	register("RelsEffectivePage",
		testCase{"KeysetWalkEqualsTheFilteredSet", pageWalk},
		testCase{"FullLastPageAndEmptyTail", pageExactMultiple},
		testCase{"PinsSeePastValuesAndDeletes", pagePins},
		testCase{"ValidTimeSelectors", pageValidTime},
		testCase{"RefusalsReturnNoRows", pageRefusals},
		testCase{"CursorIsStableUnderLaterWrites", pageStable},
		testCase{"SegmentsEqualPerRelationshipReads", pagePerRel},
	)
}

// pageFixture creates two endpoints and n relationships; relationship i carries
// est = estates[i%len(estates)] and idx = i, valid [100,200) unless open is set.
type pageFixture struct {
	g    Graph
	rels []Rel
}

func newPageFixture(t *testing.T, a Adapter, n int, estates []string, open bool) *pageFixture {
	g := openMem(t, a)
	x := addNode(t, g, NodeSpec{Labels: []string{"A"}, TxFrom: 1000})
	y := addNode(t, g, NodeSpec{Labels: []string{"A"}, TxFrom: 1000})
	f := &pageFixture{g: g}
	for i := 0; i < n; i++ {
		v := Interval{From: 100, To: 200}
		if open {
			v = Interval{From: 100}
		}
		f.rels = append(f.rels, addRel(t, g, RelSpec{
			Type: "R", From: x.ID, To: y.ID, Valid: v, TxFrom: 2000 + int64(i)*10,
			Props: map[string]any{"est": estates[i%len(estates)], "idx": i},
		}))
	}
	return f
}

func openMem(t *testing.T, a Adapter) Graph { return open(t, a, true) }

func (f *pageFixture) ofEstate(est string) []uint64 {
	var out []uint64
	for i, r := range f.rels {
		if f.rels[i].Props["est"] == est {
			out = append(out, r.ID)
		}
	}
	return out
}

// relSegIDs lists the distinct relationship IDs of a page in order: a page
// holds at most limit relationships, each with all of its matching segments.
func relSegIDs(segs []RelSegment) []uint64 {
	var out []uint64
	for _, s := range segs {
		if len(out) == 0 || out[len(out)-1] != s.Rel.ID {
			out = append(out, s.Rel.ID)
		}
	}
	return out
}

// walk follows next until it returns 0, asserting every page is within limit
// and strictly ascending, and returns the pages.
func walk(t *testing.T, g Graph, pin Pin, f Filter, limit int) [][]RelSegment {
	t.Helper()
	var pages [][]RelSegment
	var after uint64
	for guard := 0; ; guard++ {
		if guard > 100 {
			t.Fatalf("keyset walk does not terminate")
		}
		segs, next, err := g.RelsEffectivePage(ctx, pin, f, after, limit)
		ok(t, err)
		rels := relSegIDs(segs)
		if len(rels) > limit {
			t.Fatalf("page of %d relationships exceeds limit %d", len(rels), limit)
		}
		for i := 1; i < len(segs); i++ {
			if segs[i-1].Rel.ID > segs[i].Rel.ID {
				t.Fatalf("page not ascending by relationship ID: %v", relSegIDs(segs))
			}
			if segs[i-1].Rel.ID == segs[i].Rel.ID && segs[i-1].ValidFrom >= segs[i].ValidFrom {
				t.Fatalf("segments of relationship %d not ascending by valid time", segs[i].Rel.ID)
			}
		}
		if len(rels) > 0 && after != 0 && rels[0] <= after {
			t.Fatalf("page repeats IDs at or below the cursor %d: %v", after, rels)
		}
		if len(rels) < limit && next != 0 {
			t.Fatalf("short page (%d < %d) returned next=%d, want 0", len(rels), limit, next)
		}
		if len(rels) == limit && next != rels[len(rels)-1] {
			t.Fatalf("full page returned next=%d, want its last ID %d", next, rels[len(rels)-1])
		}
		pages = append(pages, segs)
		if next == 0 {
			return pages
		}
		after = next
	}
}

func flatten(pages [][]RelSegment) []uint64 {
	var out []uint64
	for _, p := range pages {
		out = append(out, relSegIDs(p)...)
	}
	return out
}

func pageWalk(t *testing.T, a Adapter) {
	f := newPageFixture(t, a, 9, []string{"e1", "e2", "e3"}, false)
	pin := Pin{Tx: 3000}
	for _, limit := range []int{1, 2, 4, 100} {
		pages := walk(t, f.g, pin, Filter{Prop: "est", Eq: "e1"}, limit)
		wantIDs(t, "walk e1", flatten(pages), f.ofEstate("e1"))
		for _, p := range pages {
			for _, s := range p {
				if s.ValidFrom != 100 || s.ValidTo != 200 {
					t.Fatalf("segment valid [%d,%d), want [100,200)", s.ValidFrom, s.ValidTo)
				}
				wantProps(t, "segment row", map[string]any{"est": s.Rel.Props["est"]}, map[string]any{"est": "e1"})
			}
		}
	}
	// A value no relationship carries yields an empty page and no cursor.
	segs, next, err := f.g.RelsEffectivePage(ctx, pin, Filter{Prop: "est", Eq: "nobody"}, 0, 10)
	ok(t, err)
	if len(segs) != 0 || next != 0 {
		t.Fatalf("absent value: %d rows next=%d", len(segs), next)
	}
	segs, _, err = f.g.RelsEffectivePage(ctx, pin, Filter{Prop: "noprop", Eq: "e1"}, 0, 10)
	ok(t, err)
	if len(segs) != 0 {
		t.Fatalf("absent property: %d rows", len(segs))
	}
	// A cursor at or past the last matching ID is empty.
	last := f.ofEstate("e1")[len(f.ofEstate("e1"))-1]
	segs, next, err = f.g.RelsEffectivePage(ctx, pin, Filter{Prop: "est", Eq: "e1"}, last, 10)
	ok(t, err)
	if len(segs) != 0 || next != 0 {
		t.Fatalf("cursor past the end: %d rows next=%d", len(segs), next)
	}
}

// pageExactMultiple: when the matching set is a multiple of limit, the last
// full page still carries a cursor and the following page is empty.
func pageExactMultiple(t *testing.T, a Adapter) {
	f := newPageFixture(t, a, 4, []string{"e1"}, false)
	pages := walk(t, f.g, Pin{Tx: 3000}, Filter{Prop: "est", Eq: "e1"}, 2)
	if len(pages) != 3 || len(pages[0]) != 2 || len(pages[1]) != 2 || len(pages[2]) != 0 {
		t.Fatalf("pages = %d (sizes %d %d ...), want 2,2 and an empty tail", len(pages), len(pages[0]), len(pages[1]))
	}
	wantIDs(t, "walk", flatten(pages), f.ofEstate("e1"))
}

// pagePins: mutate (move a relationship to another estate, delete another),
// then page at pins before and after. The delete ends validity only: a pin
// before it and a pin after it both still list the relationship's recorded
// valid-time segment.
func pagePins(t *testing.T, a Adapter) {
	f := newPageFixture(t, a, 5, []string{"e1", "e2"}, true) // e1: 0,2,4; e2: 1,3
	moved, deleted := f.rels[0], f.rels[2]
	_, err := f.g.UpdateRel(ctx, moved.ID, map[string]any{"est": "e2"}, 4000)
	ok(t, err)
	ok(t, f.g.DeleteRel(ctx, deleted.ID, 5000))

	e1 := Filter{Prop: "est", Eq: "e1"}
	e2 := Filter{Prop: "est", Eq: "e2"}
	at := func(p Pin, flt Filter) []uint64 {
		return flatten(walk(t, f.g, p, flt, 10))
	}
	r := f.rels
	wantIDs(t, "e1 @3500", at(Pin{Tx: 3500}, e1), []uint64{r[0].ID, r[2].ID, r[4].ID})
	wantIDs(t, "e2 @3500", at(Pin{Tx: 3500}, e2), []uint64{r[1].ID, r[3].ID})
	// The move is a valid-time change at 4000: with no valid-time filter the
	// relationship keeps its e1 segment [100,4000) and gains an e2 segment [4000,inf).
	wantIDs(t, "e1 @4500, any valid time", at(Pin{Tx: 4500}, e1), []uint64{r[0].ID, r[2].ID, r[4].ID})
	wantIDs(t, "e1 @4500 valid 5000", at(Pin{Tx: 4500, ValidAt: 5000}, e1), []uint64{r[2].ID, r[4].ID})
	wantIDs(t, "e2 @4500 valid 5000", at(Pin{Tx: 4500, ValidAt: 5000}, e2), []uint64{r[0].ID, r[1].ID, r[3].ID})
	wantIDs(t, "e1 @4500 valid 3000 (before the move)", at(Pin{Tx: 4500, ValidAt: 3000}, e1), []uint64{r[0].ID, r[2].ID, r[4].ID})
	segs, _, err := f.g.RelsEffectivePage(ctx, Pin{Tx: 4500}, e1, 0, 1)
	ok(t, err)
	if len(segs) != 1 || segs[0].Rel.ID != moved.ID || segs[0].ValidFrom != 100 || segs[0].ValidTo != 4000 {
		t.Fatalf("moved relationship under e1 = %+v, want one segment [100,4000)", segs)
	}
	segs, _, err = f.g.RelsEffectivePage(ctx, Pin{Tx: 4500}, e2, 0, 1)
	ok(t, err)
	if len(segs) != 1 || segs[0].Rel.ID != moved.ID || segs[0].ValidFrom != 4000 || segs[0].ValidTo != 0 {
		t.Fatalf("moved relationship under e2 = %+v, want one open segment from 4000", segs)
	}
	// At the pin before the update the relationship was only ever e1.
	wantIDs(t, "e2 @3500 does not hold the moved rel", at(Pin{Tx: 3500}, e2), []uint64{r[1].ID, r[3].ID})
	wantIDs(t, "e1 @2005 (only the first existed)", at(Pin{Tx: 2005}, e1), []uint64{r[0].ID})
	wantIDs(t, "e1 @1999 (before any)", at(Pin{Tx: 1999}, e1), nil)
	// The delete ends validity at 5000: the segment survives for earlier valid
	// instants at a later pin and is gone for later ones.
	wantIDs(t, "e1 @5500 valid 3999", at(Pin{Tx: 5500, ValidAt: 3999}, e1), []uint64{r[0].ID, r[2].ID, r[4].ID})
	wantIDs(t, "e1 @5500 valid 6000", at(Pin{Tx: 5500, ValidAt: 6000}, e1), []uint64{r[4].ID})
	wantIDs(t, "e1 @4500 valid 6000 (delete not yet recorded)", at(Pin{Tx: 4500, ValidAt: 6000}, e1), []uint64{r[2].ID, r[4].ID})
	_ = deleted
}

func pageValidTime(t *testing.T, a Adapter) {
	f := newPageFixture(t, a, 3, []string{"e1"}, false) // all valid [100,200)
	flt := Filter{Prop: "est", Eq: "e1"}
	count := func(p Pin) int {
		t.Helper()
		return len(flatten(walk(t, f.g, p, flt, 10)))
	}
	for _, c := range []struct {
		name string
		pin  Pin
		want int
	}{
		{"valid at 150", Pin{Tx: 3000, ValidAt: 150}, 3},
		{"valid at 100 (inclusive start)", Pin{Tx: 3000, ValidAt: 100}, 3},
		{"valid at 200 (exclusive end)", Pin{Tx: 3000, ValidAt: 200}, 0},
		{"valid at 99", Pin{Tx: 3000, ValidAt: 99}, 0},
		{"interval [150,250)", Pin{Tx: 3000, ValidStart: 150, ValidEnd: 250}, 3},
		{"interval [200,300) touches the end only", Pin{Tx: 3000, ValidStart: 200, ValidEnd: 300}, 0},
		{"interval [0..100) ends at the start", Pin{Tx: 3000, ValidStart: 1, ValidEnd: 100}, 0},
		{"open-ended from 150", Pin{Tx: 3000, ValidStart: 150}, 3},
		{"open-ended from 250", Pin{Tx: 3000, ValidStart: 250}, 0},
		{"no valid filter", Pin{Tx: 3000}, 3},
	} {
		if got := count(c.pin); got != c.want {
			t.Fatalf("%s: %d relationships, want %d", c.name, got, c.want)
		}
	}
	_, _, err := f.g.RelsEffectivePage(ctx, Pin{Tx: 3000, ValidAt: 150, ValidStart: 100, ValidEnd: 200}, flt, 0, 10)
	is(t, "ValidAt together with an interval", err, ErrInvalidTimeRange)
}

func pageRefusals(t *testing.T, a Adapter) {
	f := newPageFixture(t, a, 3, []string{"e1"}, false)
	pin := Pin{Tx: 3000}
	flt := Filter{Prop: "est", Eq: "e1"}
	for _, c := range []struct {
		name  string
		pin   Pin
		f     Filter
		limit int
		want  []error
	}{
		{"limit zero", pin, flt, 0, []error{ErrInvalidArgument}},
		{"limit negative", pin, flt, -1, []error{ErrInvalidArgument}},
		{"limit above 10000", pin, flt, 10001, []error{ErrInvalidArgument}},
		{"nil value", pin, Filter{Prop: "est", Eq: nil}, 10, []error{ErrInvalidArgument}},
		{"list value", pin, Filter{Prop: "est", Eq: []string{"e1"}}, 10, []error{ErrInvalidArgument}},
		{"map value", pin, Filter{Prop: "est", Eq: map[string]any{"a": 1}}, 10, []error{ErrInvalidArgument}},
		{"empty property", pin, Filter{Prop: "", Eq: "e1"}, 10, []error{ErrInvalidArgument}},
		{"pin above the commit clock", Pin{Tx: 1 << 60}, flt, 10, []error{ErrPinTooNew}},
		{"zero pin", Pin{}, flt, 10, []error{ErrInvalidTimeRange, ErrInvalidArgument}},
		{"negative pin", Pin{Tx: -4}, flt, 10, []error{ErrInvalidTimeRange, ErrInvalidArgument}},
	} {
		segs, next, err := f.g.RelsEffectivePage(ctx, c.pin, c.f, 0, c.limit)
		if err == nil {
			t.Fatalf("%s: accepted (%d rows)", c.name, len(segs))
		}
		isAny(t, c.name, err, c.want...)
		if len(segs) != 0 || next != 0 {
			t.Fatalf("%s: a refused page returned %d rows and next=%d", c.name, len(segs), next)
		}
	}
	// limit 10000 is the largest accepted value.
	_, _, err := f.g.RelsEffectivePage(ctx, pin, flt, 0, 10000)
	ok(t, err)
}

// pageStable: page one at a pin, then write more matching relationships and
// move one away; the second page at the same pin lists exactly the remainder of
// the pinned state, so a cursor neither repeats nor skips.
func pageStable(t *testing.T, a Adapter) {
	f := newPageFixture(t, a, 6, []string{"e1"}, true)
	pin := Pin{Tx: 3000}
	flt := Filter{Prop: "est", Eq: "e1"}
	first, next, err := f.g.RelsEffectivePage(ctx, pin, flt, 0, 3)
	ok(t, err)
	wantIDs(t, "first page", relSegIDs(first), []uint64{f.rels[0].ID, f.rels[1].ID, f.rels[2].ID})
	if next != f.rels[2].ID {
		t.Fatalf("cursor = %d, want %d", next, f.rels[2].ID)
	}

	x := addNode(t, f.g, NodeSpec{Labels: []string{"A"}, TxFrom: 3500})
	y := addNode(t, f.g, NodeSpec{Labels: []string{"A"}, TxFrom: 3500})
	extra := addRel(t, f.g, RelSpec{Type: "R", From: x.ID, To: y.ID, Valid: Interval{From: 100}, TxFrom: 4000, Props: map[string]any{"est": "e1"}})
	_, err = f.g.UpdateRel(ctx, f.rels[4].ID, map[string]any{"est": "e2"}, 4500)
	ok(t, err)
	ok(t, f.g.DeleteRel(ctx, f.rels[3].ID, 5000))

	rest, _, err := f.g.RelsEffectivePage(ctx, pin, flt, next, 10)
	ok(t, err)
	wantIDs(t, "second page at the old pin", relSegIDs(rest), []uint64{f.rels[3].ID, f.rels[4].ID, f.rels[5].ID})
	// The present pin sees the writes.
	nowPin, err := f.g.CommittedTx()
	ok(t, err)
	present := flatten(walk(t, f.g, Pin{Tx: nowPin, ValidAt: 6000}, flt, 10))
	// At valid 6000: rel 3 was deleted (ended 5000), rel 4 moved to e2 at 4500.
	wantIDs(t, "present", present, []uint64{f.rels[0].ID, f.rels[1].ID, f.rels[2].ID, f.rels[5].ID, extra.ID})
}

// pagePerRel: every segment equals the per-relationship bitemporal read at a
// valid instant inside it.
func pagePerRel(t *testing.T, a Adapter) {
	f := newPageFixture(t, a, 4, []string{"e1", "e2"}, false)
	// Correct one relationship so its timeline has two segments.
	_, err := f.g.SetRelVersionInterval(ctx, f.rels[0].ID, 150, 300, map[string]any{"est": "e1", "idx": 100}, 4000)
	ok(t, err)
	pin := Pin{Tx: 4500}
	pages := walk(t, f.g, pin, Filter{Prop: "est", Eq: "e1"}, 10)
	var n int
	for _, p := range pages {
		for _, s := range p {
			n++
			mid := s.ValidFrom + (s.ValidTo-s.ValidFrom)/2
			if s.ValidTo == 0 {
				mid = s.ValidFrom + 1
			}
			want, err := f.g.RelAtTx(s.Rel.ID, mid, pin.Tx)
			if err != nil {
				t.Fatalf("RelAtTx(%d, %d, %d): %v", s.Rel.ID, mid, pin.Tx, err)
			}
			if want.Version != s.Rel.Version || want.ValidFrom != s.Rel.ValidFrom || want.ValidTo != s.Rel.ValidTo {
				t.Fatalf("segment [%d,%d) of %d is version %d, the per-relationship read says version %d", s.ValidFrom, s.ValidTo, s.Rel.ID, s.Rel.Version, want.Version)
			}
			wantProps(t, "segment vs read", s.Rel.Props, want.Props)
		}
	}
	if n < 3 {
		t.Fatalf("only %d segments for the corrected fixture; expected the corrected relationship to contribute two", n)
	}
}
