package core

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// SetNodeVersionInterval / SetRelVersionInterval take `props` as a PATCH (a nil
// value deletes a key). The patch must be applied to the state that was valid
// — as believed before the correction — at each instant of [validFrom,
// validTo), NOT to the entity's most recent version. The pre-fix cascade built
// the inserted row from the current row ("template") and so copied every
// property the patch did not name from TODAY's state into the past:
//
//	Add {city:X, name:A, vf:2020}; Update {city:Y, vf:2024}
//	SetNodeVersionInterval(2021, 2022, {name:B})
//	NodeAt(2021) == {city:Y, name:B}   // wrong: city:Y was never true in 2021
//
// These tests pin the then-valid-base semantics on every backend (memory,
// badger, tiered) for the node AND relationship mirrors (testing rule 2),
// including the two-phase transaction-time checks (rule 15, lesson 46): a pin
// taken before the correction must still see the uncorrected belief.

// cpU scales the fixture's "year" instants to milliseconds-ish spacing. Raw
// adjacent integers would make a one-year piece [2024, 2025) exactly 1 wide —
// indistinguishable from the cascade's eclipse sentinel (ValidTo ==
// ValidFrom+1), which the resolver deliberately skips.
const cpU = 1000

// cpState is the expected (city, name) projection of one resolved version. An
// empty string means "key must be absent".
type cpState struct {
	city string
	name string
}

// cpRow is one stored version row projected to its valid interval + state.
type cpRow struct {
	vf, vt types.Instant
	st     cpState
}

// cpSubject abstracts the node / relationship mirror so every scenario runs
// identically against both entity kinds.
type cpSubject interface {
	correct(ctx context.Context, vf, vt types.Instant, patch map[string]any) (cpRow, error)
	at(validAt, txAt types.Instant) (cpState, error)
	asOf(pin types.Instant) (cpState, uint32, bool)
	current(ctx context.Context) (cpState, error)
	verify() (bool, error)
	history() ([]cpRow, error)
}

func cpProject(get func(string) (any, bool)) cpState {
	var st cpState
	if v, ok := get("city"); ok {
		st.city, _ = v.(string)
	}
	if v, ok := get("name"); ok {
		st.name, _ = v.(string)
	}
	return st
}

func cpRowOf(tm *types.TemporalMetadata, get func(string) (any, bool)) cpRow {
	r := cpRow{st: cpProject(get)}
	if tm != nil {
		r.vf, r.vt = tm.ValidFrom, tm.ValidTo
	}
	return r
}

// ---- node subject -----------------------------------------------------------

type cpNodeSubject struct {
	g  *Core
	id types.NodeID
}

func (s *cpNodeSubject) correct(ctx context.Context, vf, vt types.Instant, patch map[string]any) (cpRow, error) {
	n, err := s.g.Temporal.SetNodeVersionInterval(ctx, s.id, vf, vt, patch)
	if err != nil {
		return cpRow{}, err
	}
	return cpRowOf(n.Temporal(), n.GetProperty), nil
}

func (s *cpNodeSubject) at(validAt, txAt types.Instant) (cpState, error) {
	n, err := s.g.Temporal.NodeAtTx(s.id, validAt, txAt)
	if err != nil {
		return cpState{}, err
	}
	return cpProject(n.GetProperty), nil
}

func (s *cpNodeSubject) asOf(pin types.Instant) (cpState, uint32, bool) {
	rows, err := s.g.Temporal.NodesAsOf(pin)
	if err != nil {
		return cpState{}, 0, false
	}
	for _, n := range rows {
		if n.ID() == s.id {
			return cpProject(n.GetProperty), n.Version(), true
		}
	}
	return cpState{}, 0, false
}

func (s *cpNodeSubject) current(ctx context.Context) (cpState, error) {
	n, err := s.g.Nodes.Get(ctx, s.id)
	if err != nil {
		return cpState{}, err
	}
	return cpProject(n.GetProperty), nil
}

func (s *cpNodeSubject) verify() (bool, error) { return s.g.Hash.VerifyNodeChain(s.id) }

func (s *cpNodeSubject) history() ([]cpRow, error) {
	h, err := s.g.Nodes.History(s.id)
	if err != nil {
		return nil, err
	}
	out := make([]cpRow, 0, len(h))
	for _, n := range h {
		out = append(out, cpRowOf(n.Temporal(), n.GetProperty))
	}
	return out, nil
}

// ---- relationship subject ---------------------------------------------------

type cpRelSubject struct {
	g  *Core
	id types.RelID
}

func (s *cpRelSubject) correct(ctx context.Context, vf, vt types.Instant, patch map[string]any) (cpRow, error) {
	r, err := s.g.Temporal.SetRelVersionInterval(ctx, s.id, vf, vt, patch)
	if err != nil {
		return cpRow{}, err
	}
	return cpRowOf(r.Temporal(), r.GetProperty), nil
}

func (s *cpRelSubject) at(validAt, txAt types.Instant) (cpState, error) {
	r, err := s.g.Temporal.RelAtTx(s.id, validAt, txAt)
	if err != nil {
		return cpState{}, err
	}
	return cpProject(r.GetProperty), nil
}

func (s *cpRelSubject) asOf(pin types.Instant) (cpState, uint32, bool) {
	rows, err := s.g.Temporal.RelsAsOf(pin)
	if err != nil {
		return cpState{}, 0, false
	}
	for _, r := range rows {
		if r.ID() == s.id {
			return cpProject(r.GetProperty), r.Version(), true
		}
	}
	return cpState{}, 0, false
}

func (s *cpRelSubject) current(ctx context.Context) (cpState, error) {
	r, err := s.g.Rels.Get(ctx, s.id)
	if err != nil {
		return cpState{}, err
	}
	return cpProject(r.GetProperty), nil
}

func (s *cpRelSubject) verify() (bool, error) { return s.g.Hash.VerifyRelChain(s.id) }

func (s *cpRelSubject) history() ([]cpRow, error) {
	h, err := s.g.Rels.History(s.id)
	if err != nil {
		return nil, err
	}
	out := make([]cpRow, 0, len(h))
	for _, r := range h {
		out = append(out, cpRowOf(r.Temporal(), r.GetProperty))
	}
	return out, nil
}

// ---- fixture ----------------------------------------------------------------

// cpFixture builds the probe timeline on a fresh graph:
//
//	v0: [2020, …) {city:X, name:A}         (all instants ×cpU)
//	v1: [2024, ∞) {city:Y, name:A}   (Update — city changes, name carried)
//
// and returns the subject plus a transaction-time pin strictly before any
// correction the scenario records (the clock is advanced past it).
func cpFixture(t *testing.T, g *Core, clk *testClock, rel bool) (cpSubject, types.Instant) {
	t.Helper()
	ctx := context.Background()
	genesis := map[string]any{"tkg_valid_from": types.Instant(2020 * cpU), "city": "X", "name": "A"}
	update := map[string]any{"tkg_valid_from": types.Instant(2024 * cpU), "city": "Y"}

	var s cpSubject
	if rel {
		a, err := g.Nodes.Add(ctx, []string{"Case"}, nil)
		if err != nil {
			t.Fatalf("Add endpoint a: %v", err)
		}
		b, err := g.Nodes.Add(ctx, []string{"Case"}, nil)
		if err != nil {
			t.Fatalf("Add endpoint b: %v", err)
		}
		r, err := g.Rels.Add(ctx, "LIVES", a, b, genesis)
		if err != nil {
			t.Fatalf("Add rel: %v", err)
		}
		clk.Advance(time.Millisecond)
		if _, err := g.Rels.Update(ctx, r.ID(), update); err != nil {
			t.Fatalf("Update rel: %v", err)
		}
		s = &cpRelSubject{g: g, id: r.ID()}
	} else {
		n, err := g.Nodes.Add(ctx, []string{"Case"}, genesis)
		if err != nil {
			t.Fatalf("Add node: %v", err)
		}
		clk.Advance(time.Millisecond)
		if _, err := g.Nodes.Update(ctx, n.ID(), update); err != nil {
			t.Fatalf("Update node: %v", err)
		}
		s = &cpNodeSubject{g: g, id: n.ID()}
	}
	pinBefore := clk.PeekInstant()
	clk.Advance(time.Millisecond)
	return s, pinBefore
}

func cpAssertAt(t *testing.T, s cpSubject, validAt, txAt types.Instant, want cpState) {
	t.Helper()
	got, err := s.at(validAt, txAt)
	if err != nil {
		t.Fatalf("at(valid=%d, tx=%d): %v", validAt, txAt, err)
	}
	if got != want {
		t.Fatalf("at(valid=%d, tx=%d) = %+v, want %+v", validAt, txAt, got, want)
	}
}

// cpAssertHistoryHas asserts that history lists a row with exactly the given
// valid interval and state (the appended correction rows are history rows or
// the current row; callers pass only rows expected in history).
func cpAssertHistoryHas(t *testing.T, s cpSubject, want cpRow) {
	t.Helper()
	rows, err := s.history()
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	for _, r := range rows {
		if r == want {
			return
		}
	}
	t.Fatalf("history has no row %+v; rows=%+v", want, rows)
}

func cpAssertVerifies(t *testing.T, s cpSubject) {
	t.Helper()
	ok, err := s.verify()
	if err != nil {
		t.Fatalf("Verify*Chain: %v", err)
	}
	if !ok {
		t.Fatal("Verify*Chain = false after correction, want true")
	}
}

// cpAssertPreCorrectionBelief is the two-phase transaction-time check: at a pin
// taken BEFORE the correction, the fixture's original belief is intact.
func cpAssertPreCorrectionBelief(t *testing.T, s cpSubject, pinBefore types.Instant) {
	t.Helper()
	cpAssertAt(t, s, 2020*cpU, pinBefore, cpState{"X", "A"})
	cpAssertAt(t, s, 2021*cpU, pinBefore, cpState{"X", "A"})
	cpAssertAt(t, s, 2023*cpU, pinBefore, cpState{"X", "A"})
	cpAssertAt(t, s, 2024*cpU, pinBefore, cpState{"Y", "A"})
	cpAssertAt(t, s, 2030*cpU, pinBefore, cpState{"Y", "A"})
	st, ver, ok := s.asOf(pinBefore)
	if !ok {
		t.Fatalf("*AsOf(pinBefore=%d): entity absent, want the pre-correction current", pinBefore)
	}
	if st != (cpState{"Y", "A"}) || ver != 1 {
		t.Fatalf("*AsOf(pinBefore) = %+v v%d, want {Y A} v1 (unchanged by the correction)", st, ver)
	}
}

func runCascadePatchBase(t *testing.T, fn func(t *testing.T, s cpSubject, pinBefore types.Instant)) {
	t.Helper()
	for _, backend := range storeContractBackends() {
		backend := backend
		for _, rel := range []bool{false, true} {
			rel := rel
			kind := "node"
			if rel {
				kind = "rel"
			}
			t.Run(backend.name+"/"+kind, func(t *testing.T) {
				g := backend.newGraph(t, backend.snowflakeNodeID)
				clk := useTestClock(t, g)
				s, pinBefore := cpFixture(t, g, clk, rel)
				fn(t, s, pinBefore)
			})
		}
	}
}

// 1. The probe: a patch inside ONE old version keeps that version's other
// properties; the timeline outside the interval is unchanged.
func TestCascadePatch_AppliedToThenValidVersion(t *testing.T) {
	runCascadePatchBase(t, func(t *testing.T, s cpSubject, pinBefore types.Instant) {
		ret, err := s.correct(context.Background(), 2021*cpU, 2022*cpU, map[string]any{"name": "B"})
		if err != nil {
			t.Fatalf("correct: %v", err)
		}
		if want := (cpRow{2021 * cpU, 2022 * cpU, cpState{"X", "B"}}); ret != want {
			t.Fatalf("returned row = %+v, want %+v", ret, want)
		}
		cpAssertAt(t, s, 2020*cpU, 0, cpState{"X", "A"})
		cpAssertAt(t, s, 2021*cpU, 0, cpState{"X", "B"})
		cpAssertAt(t, s, 2022*cpU, 0, cpState{"X", "A"})
		cpAssertAt(t, s, 2023*cpU, 0, cpState{"X", "A"})
		cpAssertAt(t, s, 2025*cpU, 0, cpState{"Y", "A"})
		if cur, err := s.current(context.Background()); err != nil || cur != (cpState{"Y", "A"}) {
			t.Fatalf("current = %+v (err %v), want {Y A}", cur, err)
		}
		cpAssertPreCorrectionBelief(t, s, pinBefore)
		cpAssertHistoryHas(t, s, cpRow{2021 * cpU, 2022 * cpU, cpState{"X", "B"}})
		cpAssertVerifies(t, s)
	})
}

// 2. A correction spanning TWO underlying versions is split at the old version
// boundary; each piece keeps its own then-valid base.
func TestCascadePatch_SpansTwoVersions(t *testing.T) {
	runCascadePatchBase(t, func(t *testing.T, s cpSubject, pinBefore types.Instant) {
		ret, err := s.correct(context.Background(), 2021*cpU, 2025*cpU, map[string]any{"name": "B"})
		if err != nil {
			t.Fatalf("correct: %v", err)
		}
		if want := (cpRow{2021 * cpU, 2024 * cpU, cpState{"X", "B"}}); ret != want {
			t.Fatalf("returned row = %+v, want the segment covering validFrom %+v", ret, want)
		}
		cpAssertAt(t, s, 2020*cpU, 0, cpState{"X", "A"})
		cpAssertAt(t, s, 2021*cpU, 0, cpState{"X", "B"})
		cpAssertAt(t, s, 2023*cpU, 0, cpState{"X", "B"})
		cpAssertAt(t, s, 2024*cpU, 0, cpState{"Y", "B"})
		cpAssertAt(t, s, 2025*cpU, 0, cpState{"Y", "A"})
		cpAssertAt(t, s, 2030*cpU, 0, cpState{"Y", "A"})
		cpAssertPreCorrectionBelief(t, s, pinBefore)
		cpAssertHistoryHas(t, s, cpRow{2021 * cpU, 2024 * cpU, cpState{"X", "B"}})
		cpAssertHistoryHas(t, s, cpRow{2024 * cpU, 2025 * cpU, cpState{"Y", "B"}})
		cpAssertVerifies(t, s)
	})
}

// 3. Open-ended correction (validTo == 0) starting inside the OLD version: the
// old-version piece keeps city:X, the tail piece (which becomes current) keeps
// city:Y.
func TestCascadePatch_OpenEndedFromOldVersion(t *testing.T) {
	runCascadePatchBase(t, func(t *testing.T, s cpSubject, pinBefore types.Instant) {
		ret, err := s.correct(context.Background(), 2021*cpU, 0, map[string]any{"name": "B"})
		if err != nil {
			t.Fatalf("correct: %v", err)
		}
		if want := (cpRow{2021 * cpU, 2024 * cpU, cpState{"X", "B"}}); ret != want {
			t.Fatalf("returned row = %+v, want %+v", ret, want)
		}
		cpAssertAt(t, s, 2020*cpU, 0, cpState{"X", "A"})
		cpAssertAt(t, s, 2021*cpU, 0, cpState{"X", "B"})
		cpAssertAt(t, s, 2023*cpU, 0, cpState{"X", "B"})
		cpAssertAt(t, s, 2024*cpU, 0, cpState{"Y", "B"})
		cpAssertAt(t, s, 2030*cpU, 0, cpState{"Y", "B"})
		if cur, err := s.current(context.Background()); err != nil || cur != (cpState{"Y", "B"}) {
			t.Fatalf("current = %+v (err %v), want {Y B}", cur, err)
		}
		cpAssertPreCorrectionBelief(t, s, pinBefore)
		cpAssertHistoryHas(t, s, cpRow{2021 * cpU, 2024 * cpU, cpState{"X", "B"}})
		cpAssertVerifies(t, s)
	})
}

// 4. A nil patch value deletes the key only inside the interval, on each
// underlying version's own base.
func TestCascadePatch_NilDeletesOnlyInsideInterval(t *testing.T) {
	runCascadePatchBase(t, func(t *testing.T, s cpSubject, pinBefore types.Instant) {
		if _, err := s.correct(context.Background(), 2021*cpU, 2025*cpU, map[string]any{"name": nil}); err != nil {
			t.Fatalf("correct: %v", err)
		}
		cpAssertAt(t, s, 2020*cpU, 0, cpState{"X", "A"})
		cpAssertAt(t, s, 2021*cpU, 0, cpState{"X", ""})
		cpAssertAt(t, s, 2024*cpU, 0, cpState{"Y", ""})
		cpAssertAt(t, s, 2025*cpU, 0, cpState{"Y", "A"})
		cpAssertPreCorrectionBelief(t, s, pinBefore)
		cpAssertVerifies(t, s)
	})
}

// 6. Gap segments (no version valid — here before the entity's first
// valid-from) keep the pre-fix base: the most recent non-eclipsed version.
// A correction straddling the gap and the first version uses the gap base for
// the gap piece and the then-valid version for the rest.
func TestCascadePatch_GapSegmentUsesMostRecentVersion(t *testing.T) {
	runCascadePatchBase(t, func(t *testing.T, s cpSubject, pinBefore types.Instant) {
		ret, err := s.correct(context.Background(), 2000*cpU, 2010*cpU, map[string]any{"name": "B"})
		if err != nil {
			t.Fatalf("correct gap-only: %v", err)
		}
		if want := (cpRow{2000 * cpU, 2010 * cpU, cpState{"Y", "B"}}); ret != want {
			t.Fatalf("gap-only returned row = %+v, want %+v", ret, want)
		}
		cpAssertAt(t, s, 2005*cpU, 0, cpState{"Y", "B"})
		cpAssertAt(t, s, 2020*cpU, 0, cpState{"X", "A"})

		ret, err = s.correct(context.Background(), 2015*cpU, 2021*cpU, map[string]any{"name": "C"})
		if err != nil {
			t.Fatalf("correct straddling: %v", err)
		}
		if want := (cpRow{2015 * cpU, 2020 * cpU, cpState{"Y", "C"}}); ret != want {
			t.Fatalf("straddling returned row = %+v, want %+v", ret, want)
		}
		cpAssertAt(t, s, 2005*cpU, 0, cpState{"Y", "B"})
		cpAssertAt(t, s, 2015*cpU, 0, cpState{"Y", "C"})
		cpAssertAt(t, s, 2020*cpU, 0, cpState{"X", "C"})
		cpAssertAt(t, s, 2021*cpU, 0, cpState{"X", "A"})
		cpAssertAt(t, s, 2025*cpU, 0, cpState{"Y", "A"})
		cpAssertPreCorrectionBelief(t, s, pinBefore)
		cpAssertHistoryHas(t, s, cpRow{2020 * cpU, 2021 * cpU, cpState{"X", "C"}})
		cpAssertVerifies(t, s)
	})
}

// A second correction layered over the first must patch the FIRST
// correction's state (the then-believed winner), not the untouched original
// version underneath it.
func TestCascadePatch_StackedCorrectionsPatchNewestBelief(t *testing.T) {
	runCascadePatchBase(t, func(t *testing.T, s cpSubject, pinBefore types.Instant) {
		ctx := context.Background()
		if _, err := s.correct(ctx, 2021*cpU, 2023*cpU, map[string]any{"city": "Z"}); err != nil {
			t.Fatalf("first correct: %v", err)
		}
		if _, err := s.correct(ctx, 2022*cpU, 2025*cpU, map[string]any{"name": "B"}); err != nil {
			t.Fatalf("second correct: %v", err)
		}
		cpAssertAt(t, s, 2020*cpU, 0, cpState{"X", "A"})
		cpAssertAt(t, s, 2021*cpU, 0, cpState{"Z", "A"})
		cpAssertAt(t, s, 2022*cpU, 0, cpState{"Z", "B"})
		cpAssertAt(t, s, 2023*cpU, 0, cpState{"X", "B"})
		cpAssertAt(t, s, 2024*cpU, 0, cpState{"Y", "B"})
		cpAssertAt(t, s, 2025*cpU, 0, cpState{"Y", "A"})
		cpAssertPreCorrectionBelief(t, s, pinBefore)
		cpAssertVerifies(t, s)
	})
}

// Labels come from each piece's base row too (node-only: relationship type
// and endpoints are immutable). A label added AFTER the corrected interval
// must not leak into it, and an open-ended correction whose tail reaches the
// labelled current row keeps the current labels — the store's current slot
// cannot change label tokens, so the tail's base must be the current row.
func TestCascadePatch_NodeLabelsComeFromBase(t *testing.T) {
	for _, backend := range storeContractBackends() {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			g := backend.newGraph(t, backend.snowflakeNodeID)
			clk := useTestClock(t, g)
			ctx := context.Background()
			n, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{
				"tkg_valid_from": types.Instant(2020 * cpU), "name": "A",
			})
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
			clk.Advance(time.Millisecond)
			if err := g.Nodes.AddLabel(ctx, n.ID(), "User"); err != nil {
				t.Fatalf("AddLabel: %v", err)
			}
			clk.Advance(time.Millisecond)
			labelsAt := func(at types.Instant) ([]string, string) {
				t.Helper()
				v, err := g.Temporal.NodeAt(n.ID(), at)
				if err != nil {
					t.Fatalf("NodeAt(%d): %v", at, err)
				}
				name, _ := v.GetProperty("name")
				s, _ := name.(string)
				return g.Nodes.Labels(v), s
			}

			if _, err := g.Temporal.SetNodeVersionInterval(ctx, n.ID(), 2021*cpU, 2022*cpU, map[string]any{"name": "B"}); err != nil {
				t.Fatalf("bounded correction: %v", err)
			}
			if l, name := labelsAt(2021 * cpU); !cpSameStrings(l, []string{"Case"}) || name != "B" {
				t.Fatalf("NodeAt(2021) = labels %v name %q, want [Case] B (label added later must not leak back)", l, name)
			}

			if _, err := g.Temporal.SetNodeVersionInterval(ctx, n.ID(), 2023*cpU, 0, map[string]any{"name": "C"}); err != nil {
				t.Fatalf("open-ended correction: %v", err)
			}
			if l, name := labelsAt(2023 * cpU); !cpSameStrings(l, []string{"Case"}) || name != "C" {
				t.Fatalf("NodeAt(2023) = labels %v name %q, want [Case] C", l, name)
			}
			cur, err := g.Nodes.Get(ctx, n.ID())
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if l := g.Nodes.Labels(cur); !cpSameStrings(l, []string{"Case", "User"}) {
				t.Fatalf("current labels = %v, want [Case User]", l)
			}
			if name, _ := cur.GetProperty("name"); name != "C" {
				t.Fatalf("current name = %v, want C", name)
			}
			if ok, err := g.Hash.VerifyNodeChain(n.ID()); err != nil || !ok {
				t.Fatalf("VerifyNodeChain = %v, %v; want true, nil", ok, err)
			}
		})
	}
}

func cpSameStrings(got, want []string) bool {
	g := slices.Clone(got)
	w := slices.Clone(want)
	slices.Sort(g)
	slices.Sort(w)
	return slices.Equal(g, w)
}
