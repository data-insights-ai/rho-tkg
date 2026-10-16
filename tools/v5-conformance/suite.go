package conformance

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
)

// Run runs the whole suite against the adapter built by f. Each case opens its
// own graph, so cases are independent and can run in any order.
func Run(t *testing.T, f Factory) {
	t.Helper()
	for _, g := range groups {
		t.Run(g.name, func(t *testing.T) {
			for _, c := range g.cases {
				t.Run(c.name, func(t *testing.T) { c.fn(t, f(t)) })
			}
		})
	}
}

type testCase struct {
	name string
	fn   func(t *testing.T, a Adapter)
}

type group struct {
	name  string
	cases []testCase
}

var groups []group

func register(name string, cases ...testCase) { groups = append(groups, group{name, cases}) }

var ctx = context.Background()

// open opens an in-memory graph; backfill enables the caller-instant doors.
func open(t *testing.T, a Adapter, backfill bool) Graph {
	t.Helper()
	g, err := a.Open(t, Options{AllowTxBackfill: backfill})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return g
}

func openDir(t *testing.T, a Adapter) (Graph, string) {
	t.Helper()
	dir := t.TempDir()
	g, err := a.Open(t, Options{Dir: dir, AllowTxBackfill: true})
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

func ids(ns []Node) []uint64 {
	out := make([]uint64, len(ns))
	for i, n := range ns {
		out[i] = n.ID
	}
	return out
}

func wantIDs(t *testing.T, what string, got, want []uint64) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: ids = %v, want %v", what, got, want)
	}
}

func sortedIDs(ns []Node) []uint64 {
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

func wantNodeAsOf(t *testing.T, g Graph, id uint64, pin int64, want map[string]any, version uint32) {
	t.Helper()
	n, err := g.NodeAsOf(id, pin)
	if err != nil {
		t.Fatalf("NodeAsOf(%d): %v", pin, err)
	}
	wantProps(t, fmt.Sprintf("NodeAsOf(%d)", pin), n.Props, want)
	if n.Version != version {
		t.Fatalf("NodeAsOf(%d): version %d, want %d", pin, n.Version, version)
	}
}

func wantNoNodeAsOf(t *testing.T, g Graph, id uint64, pin int64) {
	t.Helper()
	n, err := g.NodeAsOf(id, pin)
	if err == nil {
		t.Fatalf("NodeAsOf(%d) = v%d %v, want a refusal", pin, n.Version, n.Props)
	}
	isAny(t, fmt.Sprintf("NodeAsOf(%d)", pin), err, ErrNoVersionAsOf, ErrNotFound)
}

// historyUnchanged snapshots a node's history and current row and returns a
// check that fails when a refused write changed either.
func historyUnchanged(t *testing.T, g Graph, id uint64) func() {
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
