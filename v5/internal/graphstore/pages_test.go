package graphstore

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type pageFixture struct {
	fs     vfs.FS
	db     *raftlog.Store
	root   Root
	index  uint64
	axis   temporal.Axis
	key    graphstate.ComponentKey
	limits PageLimits
}

func fixturePages(t *testing.T, profile temporal.Profile, l PageLimits) *pageFixture {
	t.Helper()
	fs := vfs.NewMem()
	db, r := newStore(t, fs)
	a := testAxis(t, 1, profile)
	c := openCatalog(t, db, 1, Limits{})
	s := stage(t, c)
	for _, entity := range []graphstate.EntityRecord{{ID: 1, Kind: graphstate.Node, Axis: a}, {ID: 2, Kind: graphstate.Relationship, Axis: a, Type: "LINK", Source: 1, Target: 1, Mode: graphstate.IdentityReference}} {
		if err := s.Entity(t.Context(), refEntity(uint64(entity.ID)), entity); err != nil {
			t.Fatal(err)
		}
		if err := s.Life(t.Context(), refLife(uint64(entity.ID), 11), graphstate.LifeRecord{Owner: entity.ID, Life: 11}); err != nil {
			t.Fatal(err)
		}
	}
	for _, owner := range []graphstate.EntityKind{graphstate.Node, graphstate.Relationship} {
		for _, p := range []graphstate.PropertyDefinition{{Name: "scalar", Owner: owner, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}, {Name: "set", Owner: owner, Type: graphstate.ScalarI64, Cardinality: graphstate.SetCardinality}} {
			if err := s.Property(t.Context(), p); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := s.Value(t.Context(), refValue(99), graphstate.I64(7)); err != nil {
		t.Fatal(err)
	}
	r, index := commitStage(t, db, r, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.view.Close(); err != nil {
		t.Fatal(err)
	}
	resolved, err := l.resolve()
	if err != nil {
		t.Fatal(err)
	}
	return &pageFixture{fs, db, r, index, a, graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence}, resolved}
}
func pagePoint(t *testing.T, a temporal.Axis, n int) temporal.Scope {
	t.Helper()
	integer, err := temporal.ParseInteger(fmt.Sprint(n), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	var p temporal.Position
	switch a.Descriptor().Profile {
	case temporal.ProfileIntegerZ:
		p, err = temporal.IntegerPosition(a, integer)
	case temporal.ProfileRationalQ:
		r, e := temporal.Fraction(integer, temporal.Int64(3), temporal.Limits{})
		if e != nil {
			t.Fatal(e)
		}
		p, err = temporal.RationalPosition(a, r)
	case temporal.ProfileLexicographicQN:
		r, e := temporal.Fraction(integer, temporal.Int64(3), temporal.Limits{})
		if e != nil {
			t.Fatal(e)
		}
		p, err = temporal.LexPosition(a, r, temporal.Int64(0))
	}
	if err != nil {
		t.Fatal(err)
	}
	scope, err := temporal.Point(p)
	if err != nil {
		t.Fatal(err)
	}
	return scope
}
func pagePatch(t *testing.T, k graphstate.ComponentKey, s state.State, w temporal.Scope, value state.ValueRef, revision uint64, unset bool) graphstate.ComponentPatch {
	t.Helper()
	r, err := state.NewRevision(revision, revision+1000)
	if err != nil {
		t.Fatal(err)
	}
	var result state.Result
	if unset {
		result, err = s.Unset(w, r, state.Limits{})
	} else {
		result, err = s.Set(w, value, r, state.Limits{})
	}
	if err != nil {
		t.Fatal(err)
	}
	part, err := result.State().Slice(w, state.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return graphstate.ComponentPatch{Key: k, Owned: w, State: part, Changes: result.Changes()}
}
func emptyPageState(t *testing.T, a temporal.Axis) state.State {
	t.Helper()
	s, err := state.New(a, state.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func presentLife(t *testing.T) state.ValueRef {
	t.Helper()
	v, err := state.NewValueRef(11, 0)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func (f *pageFixture) commit(t *testing.T, patches ...graphstate.ComponentPatch) StagedComponents {
	t.Helper()
	c := openCatalog(t, f.db, f.index, Limits{})
	s := stage(t, c)
	result, err := StageComponentPatches(t.Context(), s, f.root, patches, f.limits)
	if err != nil {
		t.Fatal(err)
	}
	f.root, f.index = commitStage(t, f.db, result.Root, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.view.Close(); err != nil {
		t.Fatal(err)
	}
	return result
}
func (f *pageFixture) reader(t *testing.T, index uint64, l PageLimits) *PageReader {
	t.Helper()
	c := openCatalog(t, f.db, index, Limits{})
	p, err := NewPageReader(c, l)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	return p
}
func assertPageState(t *testing.T, a, b state.State) {
	t.Helper()
	x, err := state.AppendState(nil, a, state.CodecLimits{})
	if err != nil {
		t.Fatal(err)
	}
	y, err := state.AppendState(nil, b, state.CodecLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(x, y) {
		t.Fatalf("state mismatch\n%x\n%x", x, y)
	}
}
func pointRead(t *testing.T, p *PageReader, k graphstate.ComponentKey, w temporal.Scope) graphstate.ComponentPage {
	t.Helper()
	page, err := p.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: k, Window: w}, 0, graphstate.ReadBudget{Rows: 2, Bytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	if !page.Complete || page.Next != 0 {
		t.Fatal("point incomplete")
	}
	same, err := sameScope(w, page.Owned, temporal.Limits{})
	if err != nil || !same {
		t.Fatal("point coverage")
	}
	return page
}

func TestPagesOldCurrentReopenAndSequentialCDC(t *testing.T) {
	for _, owner := range []graphstate.EntityID{1, 2} {
		t.Run(fmt.Sprint(owner), func(t *testing.T) {
			f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{MaxTailRecords: 2})
			f.key.Owner = owner
			w := pagePoint(t, f.axis, 5)
			initial := pagePatch(t, f.key, emptyPageState(t, f.axis), w, presentLife(t), 1, false)
			f.commit(t, initial)
			oldIndex := f.index
			old := f.reader(t, oldIndex, f.limits)
			after := pagePatch(t, f.key, initial.State, w, state.ValueRef{}, 2, true)
			again := pagePatch(t, f.key, after.State, w, presentLife(t), 3, false)
			groups := f.commit(t, after, again)
			if len(groups.Groups) != 2 || groups.Groups[0].Changes[0].After().Revision().ID() != 2 || groups.Groups[1].Changes[0].Before().Revision().ID() != 2 || groups.Groups[1].Changes[0].After().Revision().Provenance() != 1003 {
				t.Fatal("sequential CDC flattened/lost")
			}
			assertPageState(t, pointRead(t, old, f.key, w).Data, initial.State)
			current := f.reader(t, f.index, f.limits)
			assertPageState(t, pointRead(t, current, f.key, w).Data, again.State)
			if current.LastWork().CheckpointPages != 1 {
				t.Fatal("tail checkpoint not used")
			}
			if err := old.c.view.Close(); err != nil {
				t.Fatal(err)
			}
			if err := current.c.view.Close(); err != nil {
				t.Fatal(err)
			}
			if err := f.db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := raftlog.Open(raftlog.Config{Dir: "catalog", FS: f.fs, Application: raftlog.DefaultApplicationPolicy(1)})
			if err != nil {
				t.Fatal(err)
			}
			f.db = reopened
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			})
			assertPageState(t, pointRead(t, f.reader(t, oldIndex, f.limits), f.key, w).Data, initial.State)
			assertPageState(t, pointRead(t, f.reader(t, f.index, f.limits), f.key, w).Data, again.State)
		})
	}
}

func TestPagesSplitsNativeCoverageAndCursors(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		t.Run(fmt.Sprint(profile), func(t *testing.T) {
			f := fixturePages(t, profile, PageLimits{MaxCells: 2, MaxChildren: 4, MaxTailRecords: 1})
			for n := 0; n < 13; n++ {
				w := pagePoint(t, f.axis, 2*n)
				f.commit(t, pagePatch(t, f.key, emptyPageState(t, f.axis), w, presentLife(t), uint64(n+1), false))
			}
			p := f.reader(t, f.index, f.limits)
			all, _ := temporal.All(f.axis)
			query := graphstate.ComponentQuery{Key: f.key, Window: all}
			coverage, _ := temporal.Empty(f.axis)
			found := make(map[string]uint64)
			var token graphstate.Cursor
			for pages := 0; ; pages++ {
				if pages > 100 {
					t.Fatal("no progress")
				}
				page, err := p.ComponentPage(t.Context(), query, token, graphstate.ReadBudget{Rows: 2, Bytes: 4096})
				if err != nil {
					t.Fatal(err)
				}
				overlap, err := coverage.Overlaps(page.Owned, temporal.Limits{})
				if err != nil || overlap {
					t.Fatal("duplicate coverage")
				}
				coverage, err = coverage.Union(page.Owned, temporal.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				for _, piece := range page.Data.Pieces() {
					wire, err := temporal.AppendScope(nil, piece.Scope(), temporal.Limits{})
					if err != nil {
						t.Fatal(err)
					}
					key := string(wire)
					if _, exists := found[key]; exists {
						t.Fatal("duplicate cell")
					}
					found[key] = piece.Cell().Revision().ID()
				}
				if page.Complete {
					if page.Next != 0 {
						t.Fatal("complete with cursor")
					}
					break
				}
				if page.Next == 0 || page.Next == token {
					t.Fatal("cursor not advancing")
				}
				token = page.Next
			}
			same, err := sameScope(coverage, all, temporal.Limits{})
			if err != nil || !same || len(found) != 13 {
				t.Fatalf("coverage/cells %v %d", same, len(found))
			}
			for n := 0; n < 13; n++ {
				w := pagePoint(t, f.axis, 2*n)
				wire, _ := temporal.AppendScope(nil, w, temporal.Limits{})
				if found[string(wire)] != uint64(n+1) {
					t.Fatal("missing native point")
				}
				gap := pointRead(t, p, f.key, pagePoint(t, f.axis, 2*n+1))
				if len(gap.Data.Pieces()) != 0 {
					t.Fatal("phantom gap value")
				}
			}
		})
	}
}

func TestPagesSequenceFailureAtomicityAndPrivateRootReuse(t *testing.T) {
	f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
	c := openCatalog(t, f.db, f.index, Limits{})
	s := stage(t, c)
	if err := s.Property(t.Context(), graphstate.PropertyDefinition{Name: "reserved", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}); err != nil {
		t.Fatal(err)
	}
	before, err := s.Writes()
	if err != nil {
		t.Fatal(err)
	}
	w := pagePoint(t, f.axis, 5)
	first := pagePatch(t, f.key, emptyPageState(t, f.axis), w, presentLife(t), 1, false)
	bad := first
	bad.State = emptyPageState(t, f.axis)
	for _, sequence := range [][]graphstate.ComponentPatch{{first, first}, {first, bad}} {
		result, err := StageComponentPatches(t.Context(), s, f.root, sequence, f.limits)
		if !errors.Is(err, ErrPatchConflict) || result.Root != (Root{}) || len(result.Groups) != 0 {
			t.Fatalf("late conflict: %v %+v", err, result)
		}
		after, e := s.Writes()
		if e != nil || !sameWrites(before, after) {
			t.Fatal("partial staged mutation")
		}
	}
	result, err := StageComponentPatches(t.Context(), s, f.root, []graphstate.ComponentPatch{first}, f.limits)
	if err != nil {
		t.Fatal(err)
	}
	if result.Root.EffectDigest() != f.root.EffectDigest() || result.Root.SemanticEpoch() != f.root.SemanticEpoch() || result.Root.NextPhysicalID() <= f.root.NextPhysicalID() {
		t.Fatal("private allocation changed semantics")
	}
	after, e := s.Writes()
	if e != nil {
		t.Fatal(e)
	}
	second := pagePatch(t, f.key, first.State, w, state.ValueRef{}, 2, true)
	if _, err := StageComponentPatches(t.Context(), s, f.root, []graphstate.ComponentPatch{second}, f.limits); !errors.Is(err, ErrInvalid) {
		t.Fatalf("root reuse %v", err)
	}
	now, e := s.Writes()
	if e != nil || !sameWrites(after, now) {
		t.Fatal("reuse mutated writes")
	}
	if _, err := StageComponentPatches(t.Context(), s, result.Root, []graphstate.ComponentPatch{second}, f.limits); err != nil {
		t.Fatal(err)
	}
}

func TestPagesCursorBindingCapacityAndClose(t *testing.T) {
	f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
	for n := 0; n < 4; n++ {
		w := pagePoint(t, f.axis, n*3)
		f.commit(t, pagePatch(t, f.key, emptyPageState(t, f.axis), w, presentLife(t), uint64(n+1), false))
	}
	a := f.reader(t, f.index, PageLimits{MaxCursors: 1})
	b := f.reader(t, f.index, PageLimits{})
	if a.Identity() != b.Identity() || a.Identity() == (graphstate.ViewID{}) {
		t.Fatal("unstable view identity")
	}
	all, _ := temporal.All(f.axis)
	query := graphstate.ComponentQuery{Key: f.key, Window: all}
	pa, err := a.ComponentPage(t.Context(), query, 0, graphstate.ReadBudget{Rows: 2, Bytes: 8192})
	if err != nil || pa.Complete {
		t.Fatalf("first cursor %v", err)
	}
	pb, err := b.ComponentPage(t.Context(), query, 0, graphstate.ReadBudget{Rows: 3, Bytes: 8192})
	if err != nil || pb.Complete {
		t.Fatalf("second cursor %v", err)
	}
	if pa.Next == pb.Next {
		t.Fatal("issuing reader alias")
	}
	if _, err := b.ComponentPage(t.Context(), query, pa.Next, graphstate.ReadBudget{Rows: 2, Bytes: 8192}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("cross-reader token %v", err)
	}
	wrong := query
	wrong.MergeContext = true
	if _, err := a.ComponentPage(t.Context(), wrong, pa.Next, graphstate.ReadBudget{Rows: 2, Bytes: 8192}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := a.ComponentPage(t.Context(), query, 0, graphstate.ReadBudget{Rows: 2, Bytes: 8192}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := a.ComponentPage(t.Context(), query, pa.Next, graphstate.ReadBudget{Rows: 1, Bytes: 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	next, err := a.ComponentPage(t.Context(), query, pa.Next, graphstate.ReadBudget{Rows: 2, Bytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.ComponentPage(t.Context(), query, pa.Next, graphstate.ReadBudget{Rows: 2, Bytes: 8192}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if next.Next == 0 {
		t.Fatal("remaining lost")
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			_, err := a.ComponentPage(t.Context(), query, next.Next, graphstate.ReadBudget{Rows: 2, Bytes: 8192})
			if err != nil && !errors.Is(err, ErrClosed) && !errors.Is(err, ErrInvalid) {
				t.Error(err)
			}
		})
	}
	wg.Go(func() {
		if err := a.Close(); err != nil {
			t.Error(err)
		}
	})
	wg.Wait()
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.c.Root(); err != nil {
		t.Fatal("reader closed borrowed catalog", err)
	}
	if _, err := a.ComponentPage(t.Context(), query, 0, graphstate.ReadBudget{Rows: 2, Bytes: 8192}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestPagesScalarNullSetAndRetraction(t *testing.T) {
	f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{MaxTailRecords: 1})
	w := pagePoint(t, f.axis, 5)
	c := openCatalog(t, f.db, f.index, Limits{})
	entry, found, err := c.Value(t.Context(), refValue(99))
	if err != nil || !found {
		t.Fatal(err)
	}
	payload, err := state.NewValueRef(99, entry.PayloadBytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, owner := range []graphstate.EntityID{1, 2} {
		for _, kind := range []graphstate.ComponentKind{graphstate.Label, graphstate.ScalarProperty, graphstate.SetMember} {
			if owner == 2 && kind == graphstate.Label {
				continue
			}
			key := graphstate.ComponentKey{Owner: owner, Life: 11, Kind: kind, Name: "label"}
			value := state.Null()
			if kind == graphstate.ScalarProperty {
				key.Name = "scalar"
				value = payload
			}
			if kind == graphstate.SetMember {
				key.Name = "set"
				key.Member = 99
			}
			first := pagePatch(t, key, emptyPageState(t, f.axis), w, value, 1, false)
			f.commit(t, first)
			oldIndex := f.index
			second := pagePatch(t, key, first.State, w, state.Null(), 2, false)
			f.commit(t, second)
			third := pagePatch(t, key, second.State, w, state.ValueRef{}, 3, true)
			f.commit(t, third)
			old := pointRead(t, f.reader(t, oldIndex, f.limits), key, w)
			assertPageState(t, old.Data, first.State)
			current := pointRead(t, f.reader(t, f.index, f.limits), key, w)
			assertPageState(t, current.Data, third.State)
			cell := current.Data.Pieces()[0].Cell()
			if cell.Present() || cell.Revision().Provenance() != 1003 {
				t.Fatal("retraction lost")
			}
		}
	}
}

func TestPagesTighterPoliciesAndNestedReadAdmission(t *testing.T) {
	f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{MaxCells: 2, MaxTailRecords: 2})
	for n := 0; n < 8; n++ {
		w := pagePoint(t, f.axis, n*3)
		f.commit(t, pagePatch(t, f.key, emptyPageState(t, f.axis), w, presentLife(t), uint64(n+1), false))
	}
	query := graphstate.ComponentQuery{Key: f.key, Window: pagePoint(t, f.axis, 21)}
	for _, limits := range []PageLimits{{MaxLevels: 1}, {MaxChildren: 4}, {MaxCells: 1}, {MaxCheckpointBytes: 1}} {
		p := f.reader(t, f.index, limits)
		_, err := p.ComponentPage(t.Context(), query, 0, graphstate.ReadBudget{Rows: 8, Bytes: 8192})
		if !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("tighter %+v: %v", limits, err)
		}
		if _, err := p.c.Root(); err != nil {
			t.Fatal("resource refusal poisoned", err)
		}
		wide, err := NewPageReader(p.c, f.limits)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := wide.ComponentPage(t.Context(), query, 0, graphstate.ReadBudget{Rows: 8, Bytes: 8192}); err != nil {
			t.Fatal(err)
		}
		if err := wide.Close(); err != nil {
			t.Fatal(err)
		}
	}
	p := f.reader(t, f.index, PageLimits{MaxWorkRecords: 1})
	if _, err := p.ComponentPage(t.Context(), query, 0, graphstate.ReadBudget{Rows: 8, Bytes: 8192}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if p.LastWork().Records != 1 {
		t.Fatalf("nested axis read exceeded cap: %+v", p.LastWork())
	}
	q, _ := p.c.reader(t.Context())
	meta, found, err := q.get(componentKey(testNamespace(), f.key))
	if err != nil || !found {
		t.Fatal(err)
	}
	capBytes := rootBytes + len(componentKey(testNamespace(), f.key)) + len(meta) + 64
	q, _ = p.c.reader(t.Context())
	q.maxBytes = capBytes
	q.maxRows = 10
	read := pageReader{q: q, limits: f.limits}
	if _, _, err := read.readMeta(f.key); !errors.Is(err, ErrResourceLimit) || q.rows != 1 || q.bytes != capBytes {
		t.Fatalf("nested byte cap rows=%d bytes=%d err=%v", q.rows, q.bytes, err)
	}
}

func TestPagesNilInvalidFiniteAndAbsent(t *testing.T) {
	if _, err := NewPageReader(nil, PageLimits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := (*PageReader)(nil).Close(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if (*PageReader)(nil).Identity() != (graphstate.ViewID{}) || (*PageReader)(nil).LastWork() != (PageWork{}) {
		t.Fatal("nil output")
	}
	if _, err := (*PageReader)(nil).ComponentPage(t.Context(), graphstate.ComponentQuery{}, 0, graphstate.ReadBudget{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := StageComponentPatches(t.Context(), nil, Root{}, nil, PageLimits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := DefaultPageLimits().Validate(); err != nil {
		t.Fatal(err)
	}
	for _, l := range []PageLimits{{MaxCells: -1}, {MaxChildren: 1}, {MaxLevels: 9}, {MaxTailRecords: 33}, {MaxCursors: 4097}, {MaxCursorBytes: 1}, {MaxWorkBytes: 1}, {MaxPatches: math.MaxInt}} {
		if err := l.Validate(); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
	p := f.reader(t, f.index, f.limits)
	w := pagePoint(t, f.axis, 9)
	if page := pointRead(t, p, f.key, w); len(page.Data.Pieces()) != 0 {
		t.Fatal("absent phantom")
	}
	//nolint:staticcheck // Deliberate invalid-context contract test.
	if _, err := p.ComponentPage(nil, graphstate.ComponentQuery{Key: f.key, Window: w}, 0, graphstate.ReadBudget{Rows: 2, Bytes: 8192}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := NewPageReader(p.c, PageLimits{MaxCells: -1}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	s := stage(t, p.c)
	r := f.root
	r.next = math.MaxUint64
	if _, err := StageComponentPatches(t.Context(), s, r, []graphstate.ComponentPatch{pagePatch(t, f.key, emptyPageState(t, f.axis), w, presentLife(t), 1, false)}, f.limits); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := p.c.Root(); err != nil {
		t.Fatal(err)
	}
	p.c.root.epoch = math.MaxUint64
	page := pointRead(t, p, f.key, w)
	if page.Version != graphstate.ReadVersion(f.index) {
		t.Fatal("version overflow")
	}

}

func TestPagesDenseQComplementarySplits(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			f := fixturePages(t, temporal.ProfileRationalQ, PageLimits{MaxCells: 1, MaxTailRecords: 1})
			zero := pagePoint(t, f.axis, 0)
			one := pagePoint(t, f.axis, 3)
			zeroPos, _, _ := zero.Bounds()
			onePos, _, _ := one.Bounds()
			p, _ := zeroPos.Position()
			cut, err := temporal.FiniteBound(p, false)
			if err != nil {
				t.Fatal(err)
			}
			tail, err := temporal.Span(f.axis, cut, onePos, temporal.Limits{})
			if reverse {
				p, _ = onePos.Position()
				cut, err = temporal.FiniteBound(p, false)
				if err != nil {
					t.Fatal(err)
				}
				tail, err = temporal.Span(f.axis, zeroPos, cut, temporal.Limits{})
				zero = one
			}
			if err != nil {
				t.Fatal(err)
			}
			initial := pagePatch(t, f.key, emptyPageState(t, f.axis), zero, presentLife(t), 1, false)
			f.commit(t, initial)
			old := f.index
			next := pagePatch(t, f.key, emptyPageState(t, f.axis), tail, presentLife(t), 2, false)
			f.commit(t, next)
			point := pointRead(t, f.reader(t, f.index, f.limits), f.key, zero)
			assertPageState(t, point.Data, initial.State)
			reader := f.reader(t, f.index, f.limits)
			result, err := reader.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: f.key, Window: tail}, 0, graphstate.ReadBudget{Rows: 2, Bytes: 8192})
			if err != nil || !result.Complete {
				t.Fatalf("tail %v", err)
			}
			assertPageState(t, result.Data, next.State)
			historical, err := f.reader(t, old, f.limits).ComponentPage(t.Context(), graphstate.ComponentQuery{Key: f.key, Window: tail}, 0, graphstate.ReadBudget{Rows: 2, Bytes: 8192})
			if err != nil || historical.Data.Usage().Pieces() != 0 {
				t.Fatal("old tail phantom", err)
			}
			if reader.LastWork().DirectoryPages != 2 {
				t.Fatalf("split missing: %+v", reader.LastWork())
			}
			if err := f.db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := raftlog.Open(raftlog.Config{Dir: "catalog", FS: f.fs, Application: raftlog.DefaultApplicationPolicy(1)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			f.db = db
			assertPageState(t, pointRead(t, f.reader(t, old, f.limits), f.key, zero).Data, initial.State)
			assertPageState(t, pointRead(t, f.reader(t, f.index, f.limits), f.key, zero).Data, initial.State)
		})
	}
}

func TestPagesTighterTailPolicyNoPoison(t *testing.T) {
	f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{MaxTailRecords: 3})
	for n := 0; n < 2; n++ {
		w := pagePoint(t, f.axis, n*3)
		f.commit(t, pagePatch(t, f.key, emptyPageState(t, f.axis), w, presentLife(t), uint64(n+1), false))
	}
	w := pagePoint(t, f.axis, 3)
	for _, l := range []PageLimits{{MaxTailRecords: 1}, {MaxTailAtoms: 1}, {MaxTailBytes: 1}} {
		p := f.reader(t, f.index, l)
		if _, err := p.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: f.key, Window: w}, 0, graphstate.ReadBudget{Rows: 2, Bytes: 8192}); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
		wide, err := NewPageReader(p.c, f.limits)
		if err != nil {
			t.Fatal(err)
		}
		if len(pointRead(t, wide, f.key, w).Data.Pieces()) != 1 {
			t.Fatal("resource refusal poisoned")
		}
		if err := wide.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPagesCorruptReferencesAndTypeConfusionFailClosed(t *testing.T) {
	for _, mutation := range []string{"zero-axis", "axis-hash", "missing-leaf", "wrong-key", "foreign-partition", "wrong-kind", "tail-count", "checkpoint", "cycle", "bad-before"} {
		t.Run(mutation, func(t *testing.T) {
			f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{MaxTailRecords: 1})
			w := pagePoint(t, f.axis, 5)
			initial := pagePatch(t, f.key, emptyPageState(t, f.axis), w, presentLife(t), 1, false)
			f.commit(t, initial)
			if mutation == "checkpoint" {
				f.commit(t, pagePatch(t, f.key, initial.State, w, state.ValueRef{}, 2, true))
			}
			c := openCatalog(t, f.db, f.index, Limits{})
			base, _ := c.reader(t.Context())
			q := pageReader{q: base, limits: f.limits}
			m, found, err := q.readMeta(f.key)
			if err != nil || !found {
				t.Fatal(err)
			}
			d, err := q.directory(m.Root, f.key, f.axis)
			if err != nil {
				t.Fatal(err)
			}
			var key, b []byte
			switch mutation {
			case "zero-axis", "axis-hash":
				key = componentKey(testNamespace(), f.key)
				b, _, err = q.get(key)
				b = exactCopy(b)
				offset := 28 + 29
				if mutation == "zero-axis" {
					clear(b[offset : offset+16])
				} else {
					b[offset+16] ^= 1
				}
			case "missing-leaf":
				m.Root = f.root.NextPhysicalID() + 100
				key = componentKey(testNamespace(), f.key)
				b, err = encodeMeta(testNamespace(), m, c.limits)
			case "wrong-key":
				key = physicalKey(testNamespace(), directoryRecord, d.ID)
				d.Key.Owner = 2
				b, err = encodeDirectory(testNamespace(), d, c, f.limits)
			case "foreign-partition":
				key = physicalKey(testNamespace(), directoryRecord, d.ID)
				ns := testNamespace()
				ns.Partition++
				b, err = encodeDirectory(ns, d, c, f.limits)
			case "wrong-kind":
				key = physicalKey(testNamespace(), directoryRecord, d.ID)
				b = encodeNumber(testNamespace(), bucketRecord, 1)
			case "tail-count":
				key = physicalKey(testNamespace(), directoryRecord, d.ID)
				d.TailBytes++
				b, err = encodeDirectory(testNamespace(), d, c, f.limits)
			case "checkpoint":
				key = physicalKey(testNamespace(), checkpointRecord, d.Base)
				b = encodeNumber(testNamespace(), bucketRecord, 1)
			case "cycle", "bad-before":
				p, _, e := q.patch(d.Head, f.key, f.axis)
				if e != nil {
					t.Fatal(e)
				}
				key = physicalKey(testNamespace(), patchRecord, p.ID)
				if mutation == "cycle" {
					p.Previous = p.ID
				} else {
					bad := pagePatch(t, f.key, initial.State, w, state.ValueRef{}, 3, true)
					p.Changes = bad.Changes
				}
				b, err = encodePatch(testNamespace(), p, c, f.limits)
				d.TailBytes = len(b)
				directory, e := encodeDirectory(testNamespace(), d, c, f.limits)
				if e != nil {
					t.Fatal(e)
				}
				f.root, f.index = commitRows(t, f.db, f.root, []raftlog.KV{{Key: physicalKey(testNamespace(), directoryRecord, d.ID), Value: directory}})
			}
			if err != nil {
				t.Fatal(err)
			}
			f.root, f.index = commitRows(t, f.db, f.root, []raftlog.KV{{Key: key, Value: b}})
			p := f.reader(t, f.index, f.limits)
			page, err := p.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: f.key, Window: w}, 0, graphstate.ReadBudget{Rows: 2, Bytes: 8192})
			if !errors.Is(err, ErrCorrupt) || page.View != (graphstate.ViewID{}) || page.Data.Usage().Pieces() != 0 {
				t.Fatalf("corruption result %+v %v", page, err)
			}
			if _, err := p.c.Root(); !errors.Is(err, ErrPoisoned) {
				t.Fatalf("not stopped %v", err)
			}
		})
	}
}

func TestPagesStagingResourceAndInputFailuresLeaveBytes(t *testing.T) {
	f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
	c := openCatalog(t, f.db, f.index, Limits{})
	s := stage(t, c)
	w := pagePoint(t, f.axis, 5)
	first := pagePatch(t, f.key, emptyPageState(t, f.axis), w, presentLife(t), 1, false)
	before, _ := s.Writes()
	for _, l := range []PageLimits{{MaxWorkRecords: 1}, {MaxChangeBytes: 1}, {MaxTailBytes: 1, MaxCheckpointBytes: 1}, {MaxPatches: 1}} {
		sequence := []graphstate.ComponentPatch{first}
		if l.MaxPatches == 1 {
			sequence = append(sequence, first)
		}
		result, err := StageComponentPatches(t.Context(), s, f.root, sequence, l)
		if !errors.Is(err, ErrResourceLimit) || result.Root != (Root{}) {
			t.Fatal(err)
		}
		after, e := s.Writes()
		if e != nil || !sameWrites(before, after) {
			t.Fatal("resource partial bytes", e)
		}
	}
	for _, bad := range []graphstate.ComponentPatch{{}, {Key: graphstate.ComponentKey{Owner: 42, Kind: graphstate.Presence}, Owned: w, State: first.State, Changes: first.Changes}, {Key: first.Key, Owned: w, State: state.State{}, Changes: first.Changes}} {
		if _, err := StageComponentPatches(t.Context(), s, f.root, []graphstate.ComponentPatch{bad}, f.limits); !errors.Is(err, ErrInvalid) {
			t.Fatalf("bad input %v", err)
		}
		after, e := s.Writes()
		if e != nil || !sameWrites(before, after) {
			t.Fatal("input poisoned/mutated", e)
		}
	}
	if _, err := StageComponentPatches(t.Context(), s, f.root, []graphstate.ComponentPatch{first}, f.limits); err != nil {
		t.Fatal("negative then valid", err)
	}
}

func TestPagesStageReferenceShapeAndNoopAdversaries(t *testing.T) {
	f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
	c := openCatalog(t, f.db, f.index, Limits{})
	s := stage(t, c)
	w := pagePoint(t, f.axis, 5)
	baseline, _ := s.Writes()
	presence := pagePatch(t, f.key, emptyPageState(t, f.axis), w, presentLife(t), 1, false)
	unknown, _ := state.NewValueRef(777, 0)
	wrongBytes, _ := state.NewValueRef(11, 1)
	cases := []graphstate.ComponentPatch{pagePatch(t, f.key, emptyPageState(t, f.axis), w, state.Null(), 1, false), pagePatch(t, f.key, emptyPageState(t, f.axis), w, unknown, 1, false), pagePatch(t, f.key, emptyPageState(t, f.axis), w, wrongBytes, 1, false)}
	for _, k := range []graphstate.ComponentKey{{Owner: 1, Life: 777, Kind: graphstate.Label, Name: "l"}, {Owner: 1, Life: 11, Kind: graphstate.Label, Name: "l"}, {Owner: 1, Life: 11, Kind: graphstate.ScalarProperty, Name: "missing"}, {Owner: 1, Life: 11, Kind: graphstate.ScalarProperty, Name: "set"}, {Owner: 1, Life: 11, Kind: graphstate.SetMember, Name: "scalar", Member: 99}, {Owner: 1, Life: 11, Kind: graphstate.SetMember, Name: "set", Member: 777}, {Owner: 1, Life: 11, Kind: graphstate.ScalarProperty, Name: "scalar"}, {Owner: 1, Life: 11, Kind: graphstate.SetMember, Name: "set", Member: 99}} {
		value := state.Null()
		if k.Kind == graphstate.Label && k.Life == 11 || k.Kind == graphstate.ScalarProperty && k.Name == "scalar" || k.Kind == graphstate.SetMember && k.Member == 99 && k.Name == "set" {
			value = wrongBytes
		}
		cases = append(cases, pagePatch(t, k, emptyPageState(t, f.axis), w, value, 1, false))
	}
	for _, p := range cases {
		if _, err := StageComponentPatches(t.Context(), s, f.root, []graphstate.ComponentPatch{p}, f.limits); !errors.Is(err, ErrInvalid) {
			t.Fatalf("key=%+v invalid ref %v", p.Key, err)
		}
		after, err := s.Writes()
		if err != nil || !sameWrites(baseline, after) {
			t.Fatal("reference refusal mutated/poisoned", err)
		}
	}
	empty := emptyPageState(t, f.axis)
	noop := graphstate.ComponentPatch{Key: f.key, Owned: w, State: empty}
	result, err := StageComponentPatches(t.Context(), s, f.root, []graphstate.ComponentPatch{noop}, f.limits)
	if err != nil || result.Root != f.root || len(result.Groups) != 1 {
		t.Fatal("absent noop", err)
	}
	after, _ := s.Writes()
	if !sameWrites(baseline, after) {
		t.Fatal("absent noop allocated")
	}
	result, err = StageComponentPatches(t.Context(), s, f.root, []graphstate.ComponentPatch{presence}, f.limits)
	if err != nil {
		t.Fatal(err)
	}
	after, _ = s.Writes()
	noop.State = presence.State
	again, err := StageComponentPatches(t.Context(), s, result.Root, []graphstate.ComponentPatch{noop}, f.limits)
	if err != nil || again.Root != result.Root {
		t.Fatal("existing noop", err)
	}
	now, _ := s.Writes()
	if !sameWrites(after, now) {
		t.Fatal("existing noop wrote")
	}
	for _, r := range []Root{func() Root { r := result.Root; r.namespace.Graph[0]++; return r }(), func() Root { r := result.Root; r.owner++; return r }(), func() Root { r := result.Root; r.epoch++; return r }(), func() Root { r := result.Root; r.effect[0]++; return r }()} {
		_, err := StageComponentPatches(t.Context(), s, r, nil, f.limits)
		if !errors.Is(err, ErrNamespace) && !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
}

func TestPagesOwnedRegionCorrectionsAndLeafRollback(t *testing.T) {
	f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{MaxCells: 1, MaxChildren: 3})
	a, b := pagePoint(t, f.axis, 0), pagePoint(t, f.axis, 30)
	region, err := temporal.Region(f.axis, []temporal.Scope{a, b}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	first := pagePatch(t, f.key, emptyPageState(t, f.axis), region, presentLife(t), 1, false)
	f.commit(t, first)
	old := f.index
	second := pagePatch(t, f.key, first.State, region, state.ValueRef{}, 2, true)
	c := openCatalog(t, f.db, f.index, Limits{})
	s := stage(t, c)
	baseline, _ := s.Writes()
	stale := pagePatch(t, f.key, emptyPageState(t, f.axis), a, presentLife(t), 3, false)
	if _, err := StageComponentPatches(t.Context(), s, f.root, []graphstate.ComponentPatch{second, stale}, f.limits); !errors.Is(err, ErrPatchConflict) {
		t.Fatal(err)
	}
	after, e := s.Writes()
	if e != nil || !sameWrites(baseline, after) {
		t.Fatal("multi-leaf rollback", e)
	}
	result := f.commit(t, second)
	if len(result.Groups) != 1 || len(result.Groups[0].Changes) != 2 {
		t.Fatal("original region CDC altered")
	}
	for _, w := range []temporal.Scope{a, b} {
		historical := pointRead(t, f.reader(t, old, f.limits), f.key, w)
		current := pointRead(t, f.reader(t, f.index, f.limits), f.key, w)
		if !historical.Data.Pieces()[0].Cell().Present() || current.Data.Pieces()[0].Cell().Present() {
			t.Fatal("region history")
		}
	}
}

func TestPagesHeightAndFinalStageCapacityRefuseAtomically(t *testing.T) {
	f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{MaxCells: 1, MaxLevels: 1})
	w := pagePoint(t, f.axis, 0)
	f.commit(t, pagePatch(t, f.key, emptyPageState(t, f.axis), w, presentLife(t), 1, false))
	c := openCatalog(t, f.db, f.index, Limits{})
	s := stage(t, c)
	before, _ := s.Writes()
	other := pagePoint(t, f.axis, 30)
	patch := pagePatch(t, f.key, emptyPageState(t, f.axis), other, presentLife(t), 2, false)
	result, err := StageComponentPatches(t.Context(), s, f.root, []graphstate.ComponentPatch{patch}, f.limits)
	if !errors.Is(err, ErrResourceLimit) || result.Root != (Root{}) {
		t.Fatal(err)
	}
	after, e := s.Writes()
	if e != nil || !sameWrites(before, after) {
		t.Fatal("height failure mutated", e)
	}
	if page := pointRead(t, f.reader(t, f.index, f.limits), f.key, other); len(page.Data.Pieces()) != 0 {
		t.Fatal("failed split leaked")
	}
	small := openCatalog(t, f.db, f.index, Limits{MaxStageRecords: 1})
	stage := stage(t, small)
	if _, err := StageComponentPatches(t.Context(), stage, f.root, []graphstate.ComponentPatch{patch}, PageLimits{}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	writes, err := stage.Writes()
	if err != nil || len(writes) != 0 {
		t.Fatal("stage capacity leaked", err)
	}
}

func TestPagesObservedHandlesBelowCounter(t *testing.T) {
	for _, counter := range []uint64{1, 2} {
		t.Run(fmt.Sprint(counter), func(t *testing.T) {
			f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
			w := pagePoint(t, f.axis, 5)
			first := pagePatch(t, f.key, emptyPageState(t, f.axis), w, presentLife(t), 1, false)
			f.commit(t, first)
			badRoot := f.root
			badRoot.next = counter
			f.root, f.index = commitRows(t, f.db, badRoot, nil)
			c := openCatalog(t, f.db, f.index, Limits{})
			s := stage(t, c)
			before, _ := s.Writes()
			patch := pagePatch(t, f.key, first.State, w, state.ValueRef{}, 2, true)
			result, err := StageComponentPatches(t.Context(), s, f.root, []graphstate.ComponentPatch{patch}, f.limits)
			if !errors.Is(err, ErrCorrupt) || result.Root != (Root{}) {
				t.Fatal("counter corruption", err)
			}
			s.mu.Lock()
			if len(s.writes) != len(before) || s.bytes != 128 {
				t.Fatal("corrupt counter changed private stage")
			}
			s.mu.Unlock()
			if _, err := c.Root(); !errors.Is(err, ErrPoisoned) {
				t.Fatal(err)
			}
			reader := f.reader(t, f.index, f.limits)
			if _, err := reader.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: f.key, Window: w}, 0, graphstate.ReadBudget{Rows: 2, Bytes: 8192}); !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
		})
	}
}

func TestPagesAbsentOwnerNativeAxisAndCreateAbsence(t *testing.T) {
	f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
	p := f.reader(t, f.index, f.limits)
	foreign := testAxis(t, 2, temporal.ProfileIntegerZ)
	descriptor := f.axis.Descriptor()
	descriptor.Reference = "other-clock"
	rebound, err := temporal.NewAxis(descriptor, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, owner := range []graphstate.EntityID{1, 2} {
		key := f.key
		key.Owner = owner
		for _, axis := range []temporal.Axis{foreign, rebound} {
			page, err := p.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: key, Window: pagePoint(t, axis, 5)}, 0, graphstate.ReadBudget{Rows: 2, Bytes: 8192})
			if !errors.Is(err, ErrInvalid) || !errors.Is(err, temporal.ErrAxisMismatch) || page.View != (graphstate.ViewID{}) {
				t.Fatal("foreign native axis", err)
			}
			if _, err := p.c.Root(); err != nil {
				t.Fatal("caller mismatch poisoned", err)
			}
		}
		if len(pointRead(t, p, key, pagePoint(t, f.axis, 5)).Data.Pieces()) != 0 {
			t.Fatal("valid empty coverage")
		}
	}
	for _, owner := range []graphstate.EntityID{777, 778} {
		key := f.key
		key.Owner = owner
		if page := pointRead(t, p, key, pagePoint(t, foreign, 5)); len(page.Data.Pieces()) != 0 {
			t.Fatal("genuine create absence lost")
		}
	}
	for _, limits := range []PageLimits{{MaxWorkRecords: 1}, {MaxWorkRecords: 2}} {
		tight := f.reader(t, f.index, limits)
		if _, err := tight.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: f.key, Window: pagePoint(t, f.axis, 5)}, 0, graphstate.ReadBudget{Rows: 2, Bytes: 8192}); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
		if tight.LastWork().Records > limits.MaxWorkRecords {
			t.Fatal("absent nested budget exceeded")
		}
		if _, err := tight.c.Root(); err != nil {
			t.Fatal(err)
		}
	}
	f.commit(t, pagePatch(t, f.key, emptyPageState(t, f.axis), pagePoint(t, f.axis, 5), presentLife(t), 1, false))
	current := f.reader(t, f.index, f.limits)
	if _, err := current.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: f.key, Window: pagePoint(t, foreign, 5)}, 0, graphstate.ReadBudget{Rows: 2, Bytes: 8192}); !errors.Is(err, ErrInvalid) || !errors.Is(err, temporal.ErrAxisMismatch) {
		t.Fatal("present mismatch sentinel", err)
	}
}

func TestPagesAddressabilityIncludesUnsetAndNoop(t *testing.T) {
	f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
	c := openCatalog(t, f.db, f.index, Limits{})
	s := stage(t, c)
	if err := s.Property(t.Context(), graphstate.PropertyDefinition{Name: "boolset", Owner: graphstate.Node, Type: graphstate.ScalarBool, Cardinality: graphstate.SetCardinality}); err != nil {
		t.Fatal(err)
	}
	baseline, _ := s.Writes()
	w := pagePoint(t, f.axis, 5)
	empty := emptyPageState(t, f.axis)
	for _, key := range []graphstate.ComponentKey{{Owner: 2, Life: 11, Kind: graphstate.Label, Name: "label"}, {Owner: 1, Life: 11, Kind: graphstate.ScalarProperty, Name: "missing"}, {Owner: 1, Life: 11, Kind: graphstate.ScalarProperty, Name: "set"}, {Owner: 2, Life: 11, Kind: graphstate.SetMember, Name: "scalar", Member: 99}, {Owner: 1, Life: 11, Kind: graphstate.SetMember, Name: "set", Member: 777}, {Owner: 1, Life: 11, Kind: graphstate.SetMember, Name: "boolset", Member: 99}} {
		for _, mode := range []string{"assert", "unset", "noop"} {
			patch := graphstate.ComponentPatch{Key: key, Owned: w, State: empty}
			if mode != "noop" {
				patch = pagePatch(t, key, empty, w, state.Null(), 1, mode == "unset")
			}
			result, err := StageComponentPatches(t.Context(), s, f.root, []graphstate.ComponentPatch{patch}, f.limits)
			if !errors.Is(err, ErrInvalid) || !errors.Is(err, graphstate.ErrTypeMismatch) || result.Root != (Root{}) {
				t.Fatalf("mode%s key%+v %v", mode, key, err)
			}
			after, e := s.Writes()
			if e != nil || !sameWrites(baseline, after) {
				t.Fatal("addressability refusal mutated/poisoned", e)
			}
		}
	}
	for _, owner := range []graphstate.EntityID{1, 2} {
		for _, key := range []graphstate.ComponentKey{{Owner: owner, Life: 11, Kind: graphstate.ScalarProperty, Name: "scalar"}, {Owner: owner, Life: 11, Kind: graphstate.SetMember, Name: "set", Member: 99}} {
			result, err := StageComponentPatches(t.Context(), s, f.root, []graphstate.ComponentPatch{{Key: key, Owned: w, State: empty}}, f.limits)
			if err != nil || result.Root != f.root {
				t.Fatal("valid empty address", err)
			}
		}
	}
	after, _ := s.Writes()
	if !sameWrites(baseline, after) {
		t.Fatal("valid noop allocated")
	}
}
