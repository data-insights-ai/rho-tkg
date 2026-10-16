package conformance

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
)

// Profile names a body of semantics. See doc.go.
type Profile string

const (
	// Native is what v5 keeps: mutation then an older cut taken from CommittedTx,
	// valid time as explicit coordinates, cursors followed to their end, history
	// compared by content and order. Writes use the graph clock only.
	Native Profile = "native"
	// Compat is the v4 contract v5 drops natively on purpose (PLAN 8a): caller
	// stamps as pins, ErrTxOrder, millisecond clock pins, ID-keyset cursors,
	// dense per-entity versions. Only a v4-shaped facade serves it.
	Compat Profile = "compat"
)

// ProfileAdapter is implemented by an Adapter that serves a subset of the
// profiles. An Adapter that does not implement it serves both.
type ProfileAdapter interface{ Profiles() []Profile }

// Run runs the suite against the adapter built by f, once per profile the
// adapter serves (native first). Each case opens its own graph.
func Run(t *testing.T, f Factory) {
	t.Helper()
	served := []Profile{Native, Compat}
	if pa, ok := f(t).(ProfileAdapter); ok {
		served = pa.Profiles()
	}
	defer func() { curProfile = Native }()
	for _, p := range served {
		t.Run(string(p), func(t *testing.T) {
			curProfile = p
			for _, g := range groups {
				t.Run(g.name, func(t *testing.T) {
					for _, c := range g.cases {
						if !c.serves(p) {
							continue
						}
						t.Run(c.name, func(t *testing.T) { c.fn(t, f(t)) })
					}
				})
			}
		})
	}
}

type testCase struct {
	name     string
	fn       func(t *testing.T, a Adapter)
	profiles []Profile
}

func (c testCase) serves(p Profile) bool {
	for _, q := range c.profiles {
		if q == p {
			return true
		}
	}
	return false
}

// compat tags a case that needs the v4 contract; native one that needs only
// the v5-kept semantics; both a case that is written once and passes under
// either (it uses ts, open and pinNow so stamps are caller-chosen only in compat).
func compat(name string, fn func(*testing.T, Adapter)) testCase {
	return testCase{name, fn, []Profile{Compat}}
}
func native(name string, fn func(*testing.T, Adapter)) testCase {
	return testCase{name, fn, []Profile{Native}}
}
func both(name string, fn func(*testing.T, Adapter)) testCase {
	return testCase{name, fn, []Profile{Native, Compat}}
}

type group struct {
	name  string
	cases []testCase
}

var groups []group

func register(name string, cases ...testCase) { groups = append(groups, group{name, cases}) }

// curProfile is the profile of the running case (cases run sequentially).
var curProfile = Native

// ts returns the caller transaction instant a fixture write passes: n under
// compat, 0 (graph clock) under native.
func ts(n int64) int64 {
	if curProfile == Compat {
		return n
	}
	return 0
}

// pinNow is the pin after the writes so far: the graph clock's committed instant.
func pinNow(t *testing.T, g Graph) int64 {
	t.Helper()
	p, err := g.CommittedTx()
	ok(t, err)
	return p
}

var ctx = context.Background()

// open opens an in-memory graph; backfill enables the caller-instant doors.
func open(t *testing.T, a Adapter, backfill bool) Graph {
	t.Helper()
	g, err := a.Open(t, Options{AllowTxBackfill: backfill && curProfile == Compat})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return g
}

func openDir(t *testing.T, a Adapter) (Graph, string) {
	t.Helper()
	dir := t.TempDir()
	g, err := a.Open(t, Options{Dir: dir, AllowTxBackfill: curProfile == Compat})
	if err != nil {
		t.Fatalf("open persistent: %v", err)
	}
	return g, dir
}

func ok(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func is(t *testing.T, what string, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("%s: err = %v, want errors.Is %v", what, err, want)
	}
}

func isAny(t *testing.T, what string, err error, want ...error) {
	t.Helper()
	for _, w := range want {
		if errors.Is(err, w) {
			return
		}
	}
	t.Fatalf("%s: err = %v, want one of %v", what, err, want)
}

func addNode(t *testing.T, g Graph, s NodeSpec) Node {
	t.Helper()
	n, err := g.AddNode(ctx, s)
	ok(t, err)
	if n.ID == 0 {
		t.Fatalf("AddNode returned the zero ID")
	}
	return n
}

func addRel(t *testing.T, g Graph, s RelSpec) Rel {
	t.Helper()
	r, err := g.AddRel(ctx, s)
	ok(t, err)
	return r
}

func ids(ns []Node) []int64 {
	out := make([]int64, len(ns))
	for i, n := range ns {
		out[i] = n.ID
	}
	return out
}

func wantIDs(t *testing.T, what string, got, want []int64) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: ids = %v, want %v", what, got, want)
	}
}

func sortedIDs(ns []Node) []int64 {
	out := ids(ns)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// wantProps compares property maps by printed value so integer widths do not matter.
func wantProps(t *testing.T, what string, got, want map[string]any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: props = %v, want %v", what, got, want)
	}
	for k, w := range want {
		g, found := got[k]
		if !found || fmt.Sprint(g) != fmt.Sprint(w) {
			t.Fatalf("%s: props = %v, want %v", what, got, want)
		}
	}
}

func wantNodeAsOf(t *testing.T, g Graph, id int64, pin int64, want map[string]any, version uint32) {
	t.Helper()
	n, err := g.NodeAsOf(id, pin)
	if err != nil {
		t.Fatalf("NodeAsOf(%d): %v", pin, err)
	}
	wantProps(t, fmt.Sprintf("NodeAsOf(%d)", pin), n.Props, want)
	if curProfile == Compat && n.Version != version {
		t.Fatalf("NodeAsOf(%d): version %d, want %d", pin, n.Version, version)
	}
}

func wantNoNodeAsOf(t *testing.T, g Graph, id int64, pin int64) {
	t.Helper()
	n, err := g.NodeAsOf(id, pin)
	if err == nil {
		t.Fatalf("NodeAsOf(%d) = v%d %v, want a refusal", pin, n.Version, n.Props)
	}
	isAny(t, fmt.Sprintf("NodeAsOf(%d)", pin), err, ErrNoVersionAsOf, ErrNotFound)
}

// historyUnchanged snapshots a node's history and current row and returns a
// check that fails when a refused write changed either.
func historyUnchanged(t *testing.T, g Graph, id int64) func() {
	t.Helper()
	before, err := g.NodeHistory(id)
	ok(t, err)
	cur, err := g.GetNode(ctx, id)
	ok(t, err)
	return func() {
		t.Helper()
		after, err := g.NodeHistory(id)
		ok(t, err)
		now, err := g.GetNode(ctx, id)
		ok(t, err)
		if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(cur, now) {
			t.Fatalf("a refused write changed the entity:\nhistory before %+v\nhistory after  %+v\ncurrent before %+v\ncurrent after  %+v", before, after, cur, now)
		}
	}
}
