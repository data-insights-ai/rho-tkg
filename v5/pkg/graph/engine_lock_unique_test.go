package graph

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/testutil"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// TestOpenLockChild is the child body of TestSecondOpenRefused; it acts only
// when RHO_WP34_DIR is set. Exit 0 = the Open answered as RHO_WP34_WANT says.
func TestOpenLockChild(t *testing.T) {
	dir := os.Getenv("RHO_WP34_DIR")
	if dir == "" {
		return
	}
	g, err := Open(context.Background(), engineConfig(t, dir))
	switch os.Getenv("RHO_WP34_WANT") {
	case "locked":
		if !errors.Is(err, ErrLocked) {
			t.Fatalf("child Open = %v, want ErrLocked", err)
		}
	case "open":
		if err != nil {
			t.Fatalf("child Open = %v, want success", err)
		}
		_ = g.Close()
	}
}

func TestSecondOpenRefused(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := engineConfig(t, dir)
	g, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, cfg); !errors.Is(err, ErrLocked) {
		t.Fatalf("same-process second Open = %v, want ErrLocked", err)
	}
	if strings.Contains(err0(Open(ctx, cfg)), dir) {
		t.Fatal("ErrLocked echoes the directory")
	}
	r := testutil.RunChild(t, "TestOpenLockChild", []string{"RHO_WP34_DIR=" + dir, "RHO_WP34_WANT=locked"}, time.Minute)
	if r.ExitCode != 0 {
		t.Fatalf("child process was not refused: %s", r.Output)
	}
	// The refused Opens must not have damaged or released the first graph.
	if _, err := g.Update(ctx, func(tx Tx) error {
		_, err := tx.CreateNode(CreateNode{Props: map[string]Value{"n": Int(1)}, Scope: scopeAll(t)})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	// Close releases the lock: same process and child process can open.
	g2, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	if err := g2.Close(); err != nil {
		t.Fatal(err)
	}
	r = testutil.RunChild(t, "TestOpenLockChild", []string{"RHO_WP34_DIR=" + dir, "RHO_WP34_WANT=open"}, time.Minute)
	if r.ExitCode != 0 {
		t.Fatalf("child Open after Close: %s", r.Output)
	}
	// A failed Open (config mismatch) must not leave the directory locked.
	bad := cfg
	bad.NodeProperties = []PropertySchema{{Key: "n", Kind: KindString}}
	if _, err := Open(ctx, bad); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("mismatch Open = %v", err)
	}
	g3, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open after failed Open: %v", err)
	}
	_ = g3.Close()
	// Memory graphs need no lock.
	m := goodConfig(t)
	m.Dir, m.Memory, m.Durability = "", true, DurabilityBuffered
	a, err := Open(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Open(ctx, m)
	if err != nil {
		t.Fatalf("second memory Open: %v", err)
	}
	_ = a.Close()
	_ = b.Close()
}

func err0(_ *Graph, err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func putProp(t *testing.T, g *Graph, key, val string, sc func() Scope2) types.NodeID {
	t.Helper()
	var id types.NodeID
	if _, err := g.Update(context.Background(), func(tx Tx) (err error) {
		id, err = tx.CreateNode(CreateNode{Labels: []string{"U"}, Props: map[string]Value{key: String(val)}, Scope: sc()})
		return
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

// Scope2 aliases the scope type so helper signatures stay short.
type Scope2 = temporal.Scope

func TestRuntimeUniqueDeclarationBackfills(t *testing.T) {
	ctx := context.Background()
	all := func() Scope2 { return scopeAll(t) }

	t.Run("duplicates refused naming key and two IDs", func(t *testing.T) {
		g, err := Open(ctx, engineConfig(t, t.TempDir()))
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()
		a, b := putProp(t, g, "ext", "same", all), putProp(t, g, "ext", "same", all)
		putProp(t, g, "ext", "other", all)
		_, err = g.DeclareUnique(ctx, "ext", KindString)
		if !errors.Is(err, ErrUniqueDuplicates) || !errors.Is(err, ErrUniqueViolation) {
			t.Fatalf("DeclareUnique = %v", err)
		}
		msg := err.Error()
		if !strings.Contains(msg, "ext") || !strings.Contains(msg, strconv.FormatInt(int64(a), 10)) || !strings.Contains(msg, strconv.FormatInt(int64(b), 10)) || strings.Contains(msg, "same") {
			t.Fatalf("error must name key and both IDs, not the value: %q", msg)
		}
		// Nothing was declared: duplicates are still writable.
		putProp(t, g, "ext", "same", all)
	})

	t.Run("duplicates in disjoint valid time are not duplicates", func(t *testing.T) {
		g, err := Open(ctx, engineConfig(t, t.TempDir()))
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()
		putProp(t, g, "ext", "v", func() Scope2 { return spanMS(t, 0, 100) })
		putProp(t, g, "ext", "v", func() Scope2 { return spanMS(t, 200, 300) })
		if _, err := g.DeclareUnique(ctx, "ext", KindString); err != nil {
			t.Fatalf("disjoint claims refused: %v", err)
		}
		putProp(t, g, "ext", "v", func() Scope2 { return spanMS(t, 400, 500) })
		if _, err := g.Update(ctx, func(tx Tx) error {
			_, err := tx.CreateNode(CreateNode{Props: map[string]Value{"ext": String("v")}, Scope: spanMS(t, 50, 60)})
			return err
		}); !errors.Is(err, ErrUniqueViolation) {
			t.Fatalf("overlapping claim after declaration = %v", err)
		}
	})

	t.Run("clean data declared, enforced, survives reopen", func(t *testing.T) {
		cfg := engineConfig(t, t.TempDir())
		g, err := Open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		a := putProp(t, g, "ext", "a", all)
		putProp(t, g, "ext", "b", all)
		rc, err := g.DeclareUnique(ctx, "ext", KindString)
		if err != nil || rc.IsZero() {
			t.Fatalf("DeclareUnique = %v %v", rc, err)
		}
		check := func(g *Graph) {
			t.Helper()
			_, err := g.Update(ctx, func(tx Tx) error {
				_, err := tx.CreateNode(CreateNode{Props: map[string]Value{"ext": String("a")}, Scope: scopeAll(t)})
				return err
			})
			if !errors.Is(err, ErrUniqueViolation) {
				t.Fatalf("duplicate create = %v", err)
			}
			var id types.NodeID
			var created bool
			if _, err := g.Update(ctx, func(tx Tx) (err error) {
				id, created, err = tx.GetOrCreate(GetOrCreate{Label: "U", Key: "ext", Value: String("a"), Scope: scopeAll(t)})
				return
			}); err != nil || created || id != a {
				t.Fatalf("GetOrCreate hit = %v %v %v, want node %v", id, created, err, a)
			}
			if _, err := g.Update(ctx, func(tx Tx) (err error) {
				id, created, err = tx.GetOrCreate(GetOrCreate{Label: "U", Key: "ext", Value: String("fresh"), Scope: scopeAll(t)})
				return
			}); err != nil || !created {
				t.Fatalf("GetOrCreate miss = %v %v", created, err)
			}
			// Setting an existing node to another node's value is refused.
			if _, err := g.Update(ctx, func(tx Tx) error {
				ref, err := NodeRef(id)
				if err != nil {
					return err
				}
				return tx.Set(Set{Target: ref, Key: "ext", Value: String("a"), Scope: scopeAll(t)})
			}); !errors.Is(err, ErrUniqueViolation) {
				t.Fatalf("Set to a held value = %v", err)
			}
		}
		check(g)
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		g, err = Open(ctx, cfg)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer g.Close()
		if _, err := g.Update(ctx, func(tx Tx) error {
			_, err := tx.CreateNode(CreateNode{Props: map[string]Value{"ext": String("b")}, Scope: scopeAll(t)})
			return err
		}); !errors.Is(err, ErrUniqueViolation) {
			t.Fatalf("duplicate after reopen = %v", err)
		}
		var id types.NodeID
		var created bool
		if _, err := g.Update(ctx, func(tx Tx) (err error) {
			id, created, err = tx.GetOrCreate(GetOrCreate{Label: "U", Key: "ext", Value: String("a"), Scope: scopeAll(t)})
			return
		}); err != nil || created || id != a {
			t.Fatalf("GetOrCreate after reopen = %v %v %v", id, created, err)
		}
	})

	t.Run("break cases", func(t *testing.T) {
		g, err := Open(ctx, engineConfig(t, t.TempDir()))
		if err != nil {
			t.Fatal(err)
		}
		putProp(t, g, "ext", "a", all)
		if _, err := g.DeclareUnique(ctx, "ext", KindString); err != nil {
			t.Fatal(err)
		}
		if _, err := g.DeclareUnique(ctx, "ext", KindString); !errors.Is(err, ErrUniqueDeclared) {
			t.Fatalf("declare twice = %v", err)
		}
		if _, err := g.DeclareUnique(ctx, "", KindString); !errors.Is(err, ErrInvalidOp) {
			t.Fatalf("empty key = %v", err)
		}
		if _, err := g.DeclareUnique(ctx, "x", ValueKind(0)); !errors.Is(err, ErrInvalidValue) {
			t.Fatalf("zero kind = %v", err)
		}
		if _, err := g.DeclareUnique(ctx, "x", KindFloat); !errors.Is(err, ErrInvalidValue) {
			t.Fatalf("float kind = %v", err)
		}
		putProp(t, g, "plain", "p", all)
		if _, err := g.DeclareUnique(ctx, "plain", KindInt); !errors.Is(err, ErrInvalidValue) {
			t.Fatalf("kind differs from the declared kind = %v", err)
		}
		// A seed that is already unique is declared twice too.
		// A key never written is declared fresh and enforced from then on.
		if _, err := g.DeclareUnique(ctx, "fresh", KindString); err != nil {
			t.Fatalf("fresh key: %v", err)
		}
		putProp(t, g, "fresh", "q", all)
		if _, err := g.Update(ctx, func(tx Tx) error {
			_, err := tx.CreateNode(CreateNode{Props: map[string]Value{"fresh": String("q")}, Scope: scopeAll(t)})
			return err
		}); !errors.Is(err, ErrUniqueViolation) {
			t.Fatalf("fresh key not enforced: %v", err)
		}
		if _, err := g.DeclareUnique(nil, "z", KindString); !errors.Is(err, ErrInvalidRequest) { //nolint:staticcheck // nil context is the case under test
			t.Fatalf("nil ctx = %v", err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := g.DeclareUnique(ctx, "z", KindString); !errors.Is(err, ErrClosed) {
			t.Fatalf("closed graph = %v", err)
		}
	})
}

// TestDeclareUniquePromotesSeedAndMultiCellNode: a non-unique Config seed with
// data is promoted, the unchanged Config still opens, a node holding the same
// value in two time pieces is not its own duplicate, and the node keeps working.
func TestDeclareUniquePromotesSeedAndMultiCellNode(t *testing.T) {
	ctx := context.Background()
	cfg := engineConfig(t, t.TempDir())
	g, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var a types.NodeID
	if _, err := g.Update(ctx, func(tx Tx) (err error) {
		a, err = tx.CreateNode(CreateNode{Props: map[string]Value{"n": Int(1)}, Scope: spanMS(t, 0, 100)})
		if err != nil {
			return err
		}
		ref, err := NodeRef(a)
		if err != nil {
			return err
		}
		if err = tx.Set(Set{Target: ref, Key: "n", Value: Int(1), Scope: spanMS(t, 50, 100)}); err != nil {
			return err
		}
		_, err = tx.CreateNode(CreateNode{Props: map[string]Value{"n": Int(2)}, Scope: scopeAll(t)})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.DeclareUnique(ctx, "n", KindInt); err != nil {
		t.Fatalf("seed promotion: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g, err = Open(ctx, cfg)
	if err != nil {
		t.Fatalf("reopen with the original seeds: %v", err)
	}
	defer g.Close()
	if _, err := g.Update(ctx, func(tx Tx) error {
		_, err := tx.CreateNode(CreateNode{Props: map[string]Value{"n": Int(2)}, Scope: scopeAll(t)})
		return err
	}); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("duplicate after promotion + reopen = %v", err)
	}
	if _, err := g.Update(ctx, func(tx Tx) error {
		ref, err := NodeRef(a)
		if err != nil {
			return err
		}
		return tx.Set(Set{Target: ref, Key: "n", Value: Int(3), Scope: spanMS(t, 0, 100)})
	}); err != nil {
		t.Fatalf("rewriting the multi-cell node: %v", err)
	}
}
