package graphstore

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// These records deliberately bypass semantic staging while retaining canonical
// codecs, framing, page ownership, replay before-images and root allocation.
func installBindingPage(t *testing.T, f *pageFixture, key graphstate.ComponentKey, axis temporal.Axis, value state.ValueRef, layout string, retracted bool) temporal.Scope {
	t.Helper()
	w := pagePoint(t, axis, 5)
	all, err := temporal.All(axis)
	if err != nil {
		t.Fatal(err)
	}
	c := openCatalog(t, f.db, f.index, Limits{})
	root, id, err := f.root.ReservePhysical(4)
	if err != nil {
		t.Fatal(err)
	}
	initial := pagePatch(t, key, emptyPageState(t, axis), w, value, 1, retracted)
	d := directoryPage{ID: id, Key: key, Owned: all, Cells: 1}
	rows := []raftlog.KV{}
	put := func(kind recordKind, handle uint64, wire []byte, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, raftlog.KV{Key: physicalKey(testNamespace(), kind, handle), Value: wire})
	}
	if layout == "checkpoint" || layout == "checkpoint-masked" || layout == "checkpoint-alias-payload" || layout == "empty" {
		d.Base = id + 1
		s := initial.State
		if layout == "empty" {
			s = emptyPageState(t, axis)
			d.Cells = 0
		}
		wire, err := encodeCheckpoint(testNamespace(), d.Base, key, s, c, f.limits)
		put(checkpointRecord, d.Base, wire, err)
	} else {
		p := patchPage{ID: id + 2, Key: key, Owned: all, Changes: initial.Changes}
		wire, err := encodePatch(testNamespace(), p, c, f.limits)
		put(patchRecord, p.ID, wire, err)
		d.Head, d.TailRecords, d.TailAtoms, d.TailBytes = p.ID, 1, len(p.Changes), len(wire)
	}
	if layout == "checkpoint-masked" || layout == "tail-masked" || layout == "checkpoint-alias-payload" {
		removed := pagePatch(t, key, initial.State, w, state.ValueRef{}, 2, true)
		if layout == "checkpoint-alias-payload" {
			badPayload, _ := state.NewValueRef(value.ID(), value.PayloadBytes()+1)
			removed = pagePatch(t, key, initial.State, w, badPayload, 2, false)
		}
		p := patchPage{ID: id + 3, Previous: d.Head, Key: key, Owned: all, Changes: removed.Changes}
		wire, err := encodePatch(testNamespace(), p, c, f.limits)
		put(patchRecord, p.ID, wire, err)
		d.Head = p.ID
		d.TailRecords++
		d.TailAtoms += len(p.Changes)
		d.TailBytes += len(wire)
	}
	wire, err := encodeDirectory(testNamespace(), d, c, f.limits)
	put(directoryRecord, d.ID, wire, err)
	wire, err = encodeMeta(testNamespace(), componentMeta{key, axis, id}, c.limits)
	if err != nil {
		t.Fatal(err)
	}
	rows = append(rows, raftlog.KV{Key: componentKey(testNamespace(), key), Value: wire})
	slices.SortFunc(rows, func(a, b raftlog.KV) int { return slices.Compare(a.Key, b.Key) })
	f.root, f.index = commitRows(t, f.db, root, rows)
	return w
}

func TestPersistedComponentCellsRejectBindingViolations(t *testing.T) {
	for _, owner := range []graphstate.EntityID{1, 2} {
		for _, bad := range []string{"null-presence", "unknown-presence-life", "presence-payload", "life-valued-label", "life-valued-set", "unknown-scalar-value", "scalar-payload"} {
			if owner == 2 && bad == "life-valued-label" {
				continue // Relationship label addressability has its own test.
			}
			for _, layout := range []string{"checkpoint", "tail", "checkpoint-masked", "tail-masked"} {
				t.Run(fmt.Sprint(owner)+"/"+bad+"/"+layout, func(t *testing.T) {
					f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
					key := graphstate.ComponentKey{Owner: owner, Kind: graphstate.Presence}
					value := state.Null()
					switch bad {
					case "unknown-presence-life":
						value, _ = state.NewValueRef(777, 0)
					case "presence-payload":
						value, _ = state.NewValueRef(11, 1)
					case "life-valued-label":
						key.Life, key.Kind, key.Name = 11, graphstate.Label, "label"
						value = presentLife(t)
					case "life-valued-set":
						key.Life, key.Kind, key.Name, key.Member = 11, graphstate.SetMember, "set", 99
						value = presentLife(t)
					case "unknown-scalar-value":
						key.Life, key.Kind, key.Name = 11, graphstate.ScalarProperty, "scalar"
						value, _ = state.NewValueRef(777, 9)
					case "scalar-payload":
						key.Life, key.Kind, key.Name = 11, graphstate.ScalarProperty, "scalar"
						value, _ = state.NewValueRef(99, 1)
					}
					w := installBindingPage(t, f, key, f.axis, value, layout, false)
					p := f.reader(t, f.index, f.limits)
					page, err := p.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: key, Window: w}, 0, graphstate.ReadBudget{Rows: 2, Bytes: 8192})
					if !errors.Is(err, ErrCorrupt) || page.View != (graphstate.ViewID{}) || page.Data.Usage().Pieces() != 0 || page.Next != 0 {
						t.Fatalf("persisted invalid cell returned: view=%x pieces=%d next=%d err=%v", page.View, page.Data.Usage().Pieces(), page.Next, err)
					}
					if _, err := p.c.Root(); !errors.Is(err, ErrPoisoned) {
						t.Fatal("semantic corruption did not stop catalog", err)
					}
				})
			}
		}
	}
}

func TestPersistedComponentAddressabilityAlsoGovernsEmptyAndRetractedPages(t *testing.T) {
	for _, owner := range []graphstate.EntityID{1, 2} {
		for _, bad := range []string{"missing-owner", "foreign-axis", "missing-life", "relationship-label", "missing-schema", "scalar-cardinality", "set-cardinality", "missing-member", "member-type", "scalar-type"} {
			if owner == 1 && bad == "relationship-label" {
				continue
			}
			for _, layout := range []string{"checkpoint", "tail", "empty", "retracted"} {
				if bad == "scalar-type" && (layout == "empty" || layout == "retracted") {
					continue // An absent scalar cell contains no typed value to check.
				}
				t.Run(fmt.Sprint(owner)+"/"+bad+"/"+layout, func(t *testing.T) {
					f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
					c := openCatalog(t, f.db, f.index, Limits{})
					s := stage(t, c)
					foreign := testAxis(t, 2, temporal.ProfileIntegerZ)
					if err := s.Axis(t.Context(), foreign); err != nil {
						t.Fatal(err)
					}
					kind := graphstate.Node
					if owner == 2 {
						kind = graphstate.Relationship
					}
					for _, definition := range []graphstate.PropertyDefinition{
						{Name: "boolset", Owner: kind, Type: graphstate.ScalarBool, Cardinality: graphstate.SetCardinality},
						{Name: "boolscalar", Owner: kind, Type: graphstate.ScalarBool, Cardinality: graphstate.ScalarCardinality},
					} {
						if err := s.Property(t.Context(), definition); err != nil {
							t.Fatal(err)
						}
					}
					f.root, f.index = commitStage(t, f.db, f.root, s)
					key := graphstate.ComponentKey{Owner: owner, Kind: graphstate.Presence}
					axis, value := f.axis, presentLife(t)
					switch bad {
					case "missing-owner":
						key.Owner = 777
					case "foreign-axis":
						axis = foreign
					case "missing-life":
						key.Life, key.Kind, key.Name = 777, graphstate.Label, "label"
						value = state.Null()
					case "relationship-label":
						key.Life, key.Kind, key.Name = 11, graphstate.Label, "label"
						value = state.Null()
					case "missing-schema", "scalar-cardinality", "scalar-type":
						key.Life, key.Kind, key.Name = 11, graphstate.ScalarProperty, "missing"
						value = state.Null()
						switch bad {
						case "scalar-cardinality":
							key.Name = "set"
						case "scalar-type":
							key.Name = "boolscalar"
							value, _ = state.NewValueRef(99, 9)
						}
					case "set-cardinality", "missing-member", "member-type":
						key.Life, key.Kind, key.Name, key.Member = 11, graphstate.SetMember, "set", 99
						value = state.Null()
						switch bad {
						case "set-cardinality":
							key.Name = "scalar"
						case "missing-member":
							key.Member = 777
						default:
							key.Name = "boolset"
						}
					}
					w := installBindingPage(t, f, key, axis, value, layout, layout == "retracted")
					p := f.reader(t, f.index, f.limits)
					page, err := p.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: key, Window: w}, 0, graphstate.ReadBudget{Rows: 2, Bytes: 8192})
					if !errors.Is(err, ErrCorrupt) || errors.Is(err, ErrInvalid) || page.View != (graphstate.ViewID{}) {
						t.Fatalf("stored invalid address returned: view=%x err=%v", page.View, err)
					}
					if _, err := p.c.Root(); !errors.Is(err, ErrPoisoned) {
						t.Fatal("invalid stored address did not stop catalog", err)
					}
				})
			}
		}
	}
}

func TestPersistedBindingValidationPreservesHistoricalValuesAndGaps(t *testing.T) {
	for _, key := range []graphstate.ComponentKey{
		{Owner: 1, Kind: graphstate.Presence},
		{Owner: 2, Kind: graphstate.Presence},
		{Owner: 1, Life: 11, Kind: graphstate.Label, Name: "label"},
		{Owner: 1, Life: 11, Kind: graphstate.ScalarProperty, Name: "scalar"},
		{Owner: 2, Life: 11, Kind: graphstate.ScalarProperty, Name: "scalar"},
		{Owner: 1, Life: 11, Kind: graphstate.SetMember, Name: "set", Member: 99},
		{Owner: 2, Life: 11, Kind: graphstate.SetMember, Name: "set", Member: 99},
	} {
		t.Run(fmt.Sprint(key), func(t *testing.T) {
			f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
			w := pagePoint(t, f.axis, 5)
			value := state.Null()
			switch key.Kind {
			case graphstate.Presence:
				value = presentLife(t)
			case graphstate.ScalarProperty:
				value, _ = state.NewValueRef(99, 9)
			}
			first := pagePatch(t, key, emptyPageState(t, f.axis), w, value, 1, false)
			f.commit(t, first)
			old := f.index
			f.commit(t, pagePatch(t, key, first.State, w, state.ValueRef{}, 2, true))
			for _, index := range []uint64{old, f.index} {
				p := f.reader(t, index, f.limits)
				page := pointRead(t, p, key, w)
				pieces := page.Data.Pieces()
				if len(pieces) != 1 || pieces[0].Cell().Present() != (index == old) || pieces[0].Cell().Revision().ID() != map[bool]uint64{true: 1, false: 2}[index == old] {
					t.Fatal("current state substituted for retained history")
				}
				if index == old {
					assertPageState(t, page.Data, first.State)
				}
				if len(pointRead(t, p, key, pagePoint(t, f.axis, 6)).Data.Pieces()) != 0 {
					t.Fatal("never-asserted gap became a retraction")
				}
			}
		})
	}
}

type cancelBindingContext struct {
	context.Context
	checks int
	err    error
	before func()
	path   []string
}

func (c *cancelBindingContext) Err() error {
	c.checks++
	if c.checks == 6 {
		var callers [32]uintptr
		n := runtime.Callers(2, callers[:])
		frames := runtime.CallersFrames(callers[:n])
		for {
			frame, more := frames.Next()
			c.path = append(c.path, frame.Function)
			if !more {
				break
			}
		}
		if c.before != nil {
			c.before()
		}
	}
	if c.checks >= 6 && c.err != nil {
		return c.err
	}
	return c.Context.Err()
}

func assertStoredBindingCallChain(t *testing.T, ctx *cancelBindingContext) {
	t.Helper()
	for _, suffix := range []string{"(*pageReader).storedBinding", "(*pageReader).validateKey", "(*reader).entity", "(*reader).get"} {
		if !slices.ContainsFunc(ctx.path, func(name string) bool { return strings.HasSuffix(name, suffix) }) {
			t.Fatalf("failure boundary missed %s: %v", suffix, ctx.path)
		}
	}
}

func TestStoredBindingOperationalRefusalsDoNotBecomeCorruption(t *testing.T) {
	f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
	w := pagePoint(t, f.axis, 5)
	f.commit(t, pagePatch(t, f.key, emptyPageState(t, f.axis), w, presentLife(t), 1, false))
	for _, limits := range []PageLimits{{MaxWorkRecords: 3}, {MaxCheckpointBytes: 1024, MaxWorkBytes: 1152}} {
		p := f.reader(t, f.index, limits)
		page, err := p.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: f.key, Window: w}, 0, graphstate.ReadBudget{Rows: 2, Bytes: 8192})
		if !errors.Is(err, ErrResourceLimit) || errors.Is(err, ErrCorrupt) || page.View != (graphstate.ViewID{}) {
			t.Fatal("bounded validation refusal changed identity", err)
		}
		if _, err := p.c.Root(); err != nil {
			t.Fatal("resource refusal poisoned", err)
		}
	}
	p := f.reader(t, f.index, f.limits)
	ctx := &cancelBindingContext{Context: t.Context(), err: context.Canceled}
	if _, err := p.ComponentPage(ctx, graphstate.ComponentQuery{Key: f.key, Window: w}, 0, graphstate.ReadBudget{Rows: 2, Bytes: 8192}); !errors.Is(err, context.Canceled) || errors.Is(err, ErrCorrupt) {
		t.Fatal("canceled nested binding read changed identity", err)
	}
	assertStoredBindingCallChain(t, ctx)
	if len(pointRead(t, p, f.key, w).Data.Pieces()) != 1 {
		t.Fatal("cancellation poisoned valid reader")
	}
	closed := f.reader(t, f.index, f.limits)
	var closeErr error
	ctx = &cancelBindingContext{Context: t.Context(), before: func() { closeErr = closed.c.view.Close() }}
	if _, err := closed.ComponentPage(ctx, graphstate.ComponentQuery{Key: f.key, Window: w}, 0, graphstate.ReadBudget{Rows: 2, Bytes: 8192}); closeErr != nil || !errors.Is(err, raftlog.ErrClosed) || errors.Is(err, ErrCorrupt) {
		t.Fatal("closed borrowed view inside binding changed error identity", closeErr, err)
	}
	assertStoredBindingCallChain(t, ctx)
	closed.c.mu.Lock()
	poison := closed.c.poison
	closed.c.mu.Unlock()
	if poison != nil {
		t.Fatal("closed borrowed view poisoned catalog", poison)
	}
	for _, original := range []error{context.Canceled, context.DeadlineExceeded, ErrResourceLimit, ErrClosed, raftlog.ErrClosed, errors.New("disk read unavailable")} {
		if got := storedBindingError(original); got != original || !errors.Is(got, original) {
			t.Fatal("operational error identity changed", got)
		}
	}
}

func TestStoredReferenceReuseDoesNotIgnoreChangedPayloadBytes(t *testing.T) {
	f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
	key := graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.ScalarProperty, Name: "scalar"}
	value, _ := state.NewValueRef(99, 9)
	w := installBindingPage(t, f, key, f.axis, value, "checkpoint-alias-payload", false)
	p := f.reader(t, f.index, f.limits)
	page, err := p.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: key, Window: w}, 0, graphstate.ReadBudget{Rows: 2, Bytes: 8192})
	if !errors.Is(err, ErrCorrupt) || page.View != (graphstate.ViewID{}) {
		t.Fatal("ID-only reference cache accepted a different payload ledger", err)
	}
	// Equal-content dictionary aliases remain distinct retained references.
	// Reuse validates them; it must not rewrite their IDs to a cached alias.
	healthy := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
	c := openCatalog(t, healthy.db, healthy.index, Limits{})
	s := stage(t, c)
	if err := s.Value(t.Context(), refValue(100), graphstate.I64(7)); err != nil {
		t.Fatal(err)
	}
	healthy.root, healthy.index = commitStage(t, healthy.db, healthy.root, s)
	w = pagePoint(t, healthy.axis, 5)
	first := pagePatch(t, key, emptyPageState(t, healthy.axis), w, value, 1, false)
	healthy.commit(t, first)
	old := healthy.index
	alias, _ := state.NewValueRef(100, 9)
	healthy.commit(t, pagePatch(t, key, first.State, w, alias, 2, false))
	for _, index := range []uint64{old, healthy.index} {
		page := pointRead(t, healthy.reader(t, index, healthy.limits), key, w)
		want := value
		if index != old {
			want = alias
		}
		if pieces := page.Data.Pieces(); len(pieces) != 1 || pieces[0].Cell().Value() != want {
			t.Fatal("historical alias identity was rewritten by validation reuse")
		}
	}
}

func TestStoredBindingCorruptionRejectsUnchangedStageWithoutWrites(t *testing.T) {
	for _, layout := range []string{"checkpoint", "tail"} {
		t.Run(layout, func(t *testing.T) {
			f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
			w := installBindingPage(t, f, f.key, f.axis, state.Null(), layout, false)
			c := openCatalog(t, f.db, f.index, Limits{})
			s := stage(t, c)
			before, err := s.Writes()
			if err != nil {
				t.Fatal(err)
			}
			// A syntactically valid no-op must still inspect the stored state;
			// it cannot return success just because it proposes no changes.
			noop := graphstate.ComponentPatch{Key: f.key, Owned: w, State: emptyPageState(t, f.axis)}
			result, err := StageComponentPatches(t.Context(), s, f.root, []graphstate.ComponentPatch{noop}, f.limits)
			if !errors.Is(err, ErrCorrupt) || result.Root != (Root{}) {
				t.Fatal("unchanged stage skipped invalid checkpoint/tail", err)
			}
			s.mu.Lock()
			if len(s.writes) != len(before) || s.bytes != 128 {
				t.Fatal("stored corruption changed private staging")
			}
			s.mu.Unlock()
			if _, err := c.Root(); !errors.Is(err, ErrPoisoned) {
				t.Fatal("stored corruption left stage catalog usable", err)
			}
		})
	}
}

func TestMissingComponentMetadataStillAllowsCreateWindows(t *testing.T) {
	f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
	for _, key := range []graphstate.ComponentKey{
		{Owner: 777, Kind: graphstate.Presence},
		{Owner: 777, Life: 22, Kind: graphstate.Label, Name: "new"},
		{Owner: 1, Life: 22, Kind: graphstate.ScalarProperty, Name: "scalar"},
		{Owner: 1, Life: 22, Kind: graphstate.SetMember, Name: "set", Member: 99},
	} {
		p := f.reader(t, f.index, f.limits)
		w := pagePoint(t, f.axis, 5)
		page := pointRead(t, p, key, w)
		if len(page.Data.Pieces()) != 0 || !page.Complete || page.Next != 0 {
			t.Fatal("missing metadata turned a create window into invalid state")
		}
		same, err := page.Owned.SameSupport(w, temporal.Limits{})
		if err != nil || !same {
			t.Fatal("missing window coverage", err)
		}
		if _, err := p.c.Root(); err != nil {
			t.Fatal("genuine absence poisoned", err)
		}
	}
}
