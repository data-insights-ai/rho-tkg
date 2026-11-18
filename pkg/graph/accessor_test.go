package graph_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// Tests for the v4.2.0 field-to-method conversion on *Graph.
//
// Three layers of safety must hold:
//
//  1. (*Graph)(nil).X() returns nil — calling on a nil pointer is safe and
//     does NOT panic. The returned sub-API is also nil; its methods then
//     fail closed via the sub-API's own nil-receiver path.
//
//  2. (&Graph{}).X() returns nil — the zero-value *Graph has unexported
//     fields all set to nil. Accessors return those nils. Same fallthrough
//     to nil-sub-API methods.
//
//  3. g.X() after g.Close() — accessors still return the live sub-API
//     pointer (we don't tear it down on close), so calls reach the
//     underlying *Core which returns ErrGraphClosed. This matches the
//     close-then-call semantics every other path uses.
//
// The per-method nil-pointer + zero-value surface of every sub-API is the
// reflection walk in subapi_nil_receiver_test.go. This file specifically targets the Graph-level
// accessors that didn't exist before v4.2.0.

func TestGraphAccessor_NilGraphReturnsNilForEveryAccessor(t *testing.T) {
	t.Parallel()
	var g *graphpkg.Graph // typed-nil pointer

	// Reflection-driven enumeration so a new accessor can't be silently
	// added without test coverage. Every exported method on *Graph that
	// takes no arguments and returns a single value is treated as an
	// accessor; for those, calling on nil *Graph must return a nil result
	// without panicking.
	gType := reflect.TypeOf(g)
	for i := 0; i < gType.NumMethod(); i++ {
		m := gType.Method(i)
		// Skip Close() — it's not an accessor; it returns an error.
		if m.Name == "Close" {
			continue
		}
		// Each accessor takes one receiver and no args; returns one value.
		mt := m.Func.Type()
		if mt.NumIn() != 1 || mt.NumOut() != 1 {
			continue
		}
		t.Run(m.Name, func(t *testing.T) {
			t.Parallel()
			out := m.Func.Call([]reflect.Value{reflect.ValueOf(g)})
			if len(out) != 1 {
				t.Fatalf("%s: NumOut = %d, want 1", m.Name, len(out))
			}
			if !out[0].IsNil() {
				t.Fatalf("(*Graph)(nil).%s() = %v, want nil", m.Name, out[0].Interface())
			}
		})
	}
}

func TestGraphAccessor_ZeroValueReturnsNilForEveryAccessor(t *testing.T) {
	t.Parallel()
	g := &graphpkg.Graph{} // zero-value struct

	// The same reflective walk as the nil-pointer test, so an accessor added
	// to *Graph reaches this check without editing it.
	gType := reflect.TypeOf(g)
	n := 0
	for i := 0; i < gType.NumMethod(); i++ {
		m := gType.Method(i)
		mt := m.Func.Type()
		if m.Name == "Close" || mt.NumIn() != 1 || mt.NumOut() != 1 {
			continue
		}
		n++
		out := m.Func.Call([]reflect.Value{reflect.ValueOf(g)})
		if !out[0].IsNil() {
			t.Errorf("zero Graph.%s() = %v, want nil", m.Name, out[0].Interface())
		}
	}
	if n == 0 {
		t.Fatal("found no accessors on *Graph; the method filter is wrong")
	}
}

// TestGraphAccessor_NilGraphChainedCallFailsClosed combines the two safety
// properties: (*Graph)(nil).Nodes().Add(...) does not panic and returns
// ErrNilGraph (the nil-sub-API fail-closed path).
func TestGraphAccessor_NilGraphChainedCallFailsClosed(t *testing.T) {
	t.Parallel()
	var g *graphpkg.Graph
	ctx := context.Background()

	if _, err := g.Nodes().Add(ctx, []string{"X"}, nil); !errors.Is(err, graphpkg.ErrNilGraph) {
		t.Errorf("(nil *Graph).Nodes().Add err = %v, want ErrNilGraph", err)
	}
	if _, err := g.Rels().Get(ctx, 0); !errors.Is(err, graphpkg.ErrNilGraph) {
		t.Errorf("(nil *Graph).Rels().Get err = %v, want ErrNilGraph", err)
	}
	if _, err := g.Temporal().NodesAt(0); !errors.Is(err, graphpkg.ErrNilGraph) {
		t.Errorf("(nil *Graph).Temporal().NodesAt err = %v, want ErrNilGraph", err)
	}
	if _, err := g.Stats().Get(); !errors.Is(err, graphpkg.ErrNilGraph) {
		t.Errorf("(nil *Graph).Stats().Get err = %v, want ErrNilGraph", err)
	}
	if _, err := g.Tx().Begin(); !errors.Is(err, graphpkg.ErrNilGraph) {
		t.Errorf("(nil *Graph).Tx().Begin err = %v, want ErrNilGraph", err)
	}
	if _, err := g.Batch().New(); !errors.Is(err, graphpkg.ErrNilGraph) {
		t.Errorf("(nil *Graph).Batch().New err = %v, want ErrNilGraph", err)
	}
}

// TestGraphAccessor_AfterCloseReturnsLiveSubAPIThatFailsClosed: after
// Close(), accessor methods still return the original sub-API pointer
// (we don't nil them out — the *Core stays addressable; only the
// closed-flag is set). Calls to those sub-APIs return ErrGraphClosed.
func TestGraphAccessor_AfterCloseReturnsLiveSubAPIThatFailsClosed(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Accessors still return non-nil sub-API pointers.
	if g.Nodes() == nil {
		t.Fatal("Nodes() after Close = nil, want non-nil sub-API")
	}
	if g.Rels() == nil {
		t.Fatal("Rels() after Close = nil, want non-nil sub-API")
	}

	// But calls fail closed with ErrGraphClosed.
	ctx := context.Background()
	if _, err := g.Nodes().Add(ctx, []string{"X"}, nil); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Errorf("Nodes().Add after Close err = %v, want ErrGraphClosed", err)
	}
	if _, err := g.Nodes().ByLabel("X", storepkg.QueryOpts{}); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Errorf("Nodes().ByLabel after Close err = %v, want ErrGraphClosed", err)
	}
	if _, err := g.Rels().Count(); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Errorf("Rels().Count after Close err = %v, want ErrGraphClosed", err)
	}
	if _, err := g.Temporal().NodesAt(0); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Errorf("Temporal().NodesAt after Close err = %v, want ErrGraphClosed", err)
	}
	if _, err := g.Stats().Get(); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Errorf("Stats().Get after Close err = %v, want ErrGraphClosed", err)
	}
	if _, err := g.Tx().Begin(); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Errorf("Tx().Begin after Close err = %v, want ErrGraphClosed", err)
	}
	if _, err := g.Batch().New(); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Errorf("Batch().New after Close err = %v, want ErrGraphClosed", err)
	}
}

// TestGraphAccessor_AccessorsReturnSamePointerEachCall: stability — the
// accessor isn't expected to allocate a new wrapper per call. Two calls
// must return the same *X pointer for caller-side caching to work.
func TestGraphAccessor_AccessorsReturnSamePointerEachCall(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	// Each pair is bound to locals: the assertion IS that two calls yield one
	// pointer, which staticcheck reads as a tautology when written inline
	// (SA4000).
	if aNodes, bNodes := g.Nodes(), g.Nodes(); aNodes != bNodes {
		t.Error("Nodes() returned different pointers on consecutive calls")
	}
	if aRels, bRels := g.Rels(), g.Rels(); aRels != bRels {
		t.Error("Rels() returned different pointers on consecutive calls")
	}
	if aTemporal, bTemporal := g.Temporal(), g.Temporal(); aTemporal != bTemporal {
		t.Error("Temporal() returned different pointers on consecutive calls")
	}
	if aTx, bTx := g.Tx(), g.Tx(); aTx != bTx {
		t.Error("Tx() returned different pointers on consecutive calls")
	}
}

// TestGraphAccessor_NoExportedFields (compile-time + reflective): the
// v4.2.0 conversion removed the 14 exported sub-API fields. This test
// asserts the *Graph struct now has zero exported fields, so an attempt
// to use the old field-access shape (g.Nodes.Add) would fail to compile.
// If anyone adds a new exported field by mistake, this catches it.
func TestGraphAccessor_NoExportedFields(t *testing.T) {
	t.Parallel()
	var g graphpkg.Graph
	rt := reflect.TypeOf(g)
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.IsExported() {
			t.Errorf("Graph has unexpected exported field %q (v4.2.0 moved every sub-API to an accessor method)", f.Name)
		}
	}
}

// TestGraphAccessor_TypeReflectionShape: independent verification that the
// named accessor methods exist with the expected signatures (returns a single
// non-error pointer). Catches accidental removal or signature change; the
// reflective walks above cannot, since a removed method is simply not walked.
func TestGraphAccessor_TypeReflectionShape(t *testing.T) {
	t.Parallel()
	rt := reflect.TypeOf((*graphpkg.Graph)(nil))
	wantAccessors := []string{
		"Nodes", "Rels", "Temporal", "Index", "Events",
		"Constraints", "IO", "Admin", "Tier", "Stats",
		"Hash", "Replication", "Ingest", "Resolve", "Tx", "Batch",
	}
	for _, name := range wantAccessors {
		m, ok := rt.MethodByName(name)
		if !ok {
			t.Errorf("*Graph.%s method missing", name)
			continue
		}
		mt := m.Func.Type()
		// receiver + zero args
		if mt.NumIn() != 1 {
			t.Errorf("*Graph.%s NumIn = %d, want 1 (receiver only)", name, mt.NumIn())
		}
		if mt.NumOut() != 1 {
			t.Errorf("*Graph.%s NumOut = %d, want 1", name, mt.NumOut())
		}
		// Output must be a pointer to a sub-API struct.
		if mt.NumOut() == 1 && mt.Out(0).Kind() != reflect.Pointer {
			t.Errorf("*Graph.%s returns %v, want pointer", name, mt.Out(0))
		}
	}
}
