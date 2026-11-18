package badger

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestNoIteratorPrefetchesValues guards docs/badger.md rule 3 (PREFETCH): rho's
// values are inline, so a prefetching iterator only starts a goroutine per row
// (iterator.go:707-713). It parses the package's sources: no IteratorOptions
// literal or field assignment sets PrefetchValues to anything but false, no
// NewIterator takes badger's DefaultIteratorOptions (PrefetchValues true)
// directly, and an options variable copied from DefaultIteratorOptions (or from
// such a variable) has PrefetchValues = false assigned before NewIterator gets it.
func TestNoIteratorPrefetchesValues(t *testing.T) {
	t.Parallel()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	checked := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			found, tracked := checkIteratorPrefetch(fset, fn.Body)
			checked += tracked
			for _, msg := range found {
				t.Error(msg)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no DefaultIteratorOptions copy found: the guard checks nothing")
	}

	// The checker itself: each prefetching shape is reported, the safe ones are not.
	const src = `package p
func f() {
	a := badgerv4.DefaultIteratorOptions
	txn.NewIterator(a) // line 4: prefetch left on
	b := badgerv4.DefaultIteratorOptions
	b.PrefetchValues = false
	c := b
	c.Reverse = true
	txn.NewIterator(c) // safe: copied after prefetch was turned off
	txn.NewIterator(badgerv4.DefaultIteratorOptions) // line 10
	txn.NewIterator(badgerv4.IteratorOptions{PrefetchValues: true}) // line 11
	d := badgerv4.DefaultIteratorOptions
	d.PrefetchValues = true // line 13
	txn.NewIterator(d) // line 14
}
`
	sf, err := parser.ParseFile(fset, "synthetic.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	found, tracked := checkIteratorPrefetch(fset, sf.Decls[0].(*ast.FuncDecl).Body)
	var lines []int
	for _, msg := range found {
		var line int
		if _, err := fmt.Sscanf(strings.TrimPrefix(msg, "synthetic.go:"), "%d", &line); err != nil {
			t.Fatalf("finding %q has no line", msg)
		}
		lines = append(lines, line)
	}
	if want := []int{4, 10, 11, 13, 14}; !slices.Equal(lines, want) || tracked != 3 {
		t.Fatalf("synthetic findings on lines %v (%d copies tracked), want %v (3): %q", lines, tracked, want, found)
	}
}

// checkIteratorPrefetch walks one function body in source order and returns
// every prefetching iterator it finds and how many option copies it tracked.
func checkIteratorPrefetch(fset *token.FileSet, body *ast.BlockStmt) (found []string, tracked int) {
	report := func(pos token.Pos, format string, args ...any) {
		found = append(found, fset.Position(pos).String()+": "+fmt.Sprintf(format, args...))
	}
	isDefault := func(e ast.Expr) bool {
		sel, ok := e.(*ast.SelectorExpr)
		return ok && sel.Sel.Name == "DefaultIteratorOptions"
	}
	isFalse := func(e ast.Expr) bool {
		id, ok := e.(*ast.Ident)
		return ok && id.Name == "false"
	}
	prefetch := map[string]bool{} // tracked options variable -> prefetch still on
	assign := func(name string, rhs ast.Expr) {
		switch r := rhs.(type) {
		case *ast.SelectorExpr:
			if isDefault(r) {
				prefetch[name] = true
				tracked++
				return
			}
		case *ast.Ident:
			if on, ok := prefetch[r.Name]; ok {
				prefetch[name] = on
				return
			}
		}
		delete(prefetch, name)
	}
	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			for i, lhs := range x.Lhs {
				if i >= len(x.Rhs) {
					break
				}
				switch l := lhs.(type) {
				case *ast.Ident:
					assign(l.Name, x.Rhs[i])
				case *ast.SelectorExpr:
					if l.Sel.Name != "PrefetchValues" {
						continue
					}
					if !isFalse(x.Rhs[i]) {
						report(x.Pos(), "PrefetchValues set to a value other than false")
					}
					if id, ok := l.X.(*ast.Ident); ok {
						if _, ok := prefetch[id.Name]; ok {
							prefetch[id.Name] = !isFalse(x.Rhs[i])
						}
					}
				}
			}
		case *ast.ValueSpec:
			for i, name := range x.Names {
				if i < len(x.Values) {
					assign(name.Name, x.Values[i])
				}
			}
		case *ast.KeyValueExpr:
			if k, ok := x.Key.(*ast.Ident); ok && k.Name == "PrefetchValues" && !isFalse(x.Value) {
				report(x.Pos(), "IteratorOptions literal sets PrefetchValues to a value other than false")
			}
		case *ast.CallExpr:
			sel, ok := x.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "NewIterator" || len(x.Args) != 1 {
				return true
			}
			switch a := x.Args[0].(type) {
			case *ast.SelectorExpr:
				if isDefault(a) {
					report(x.Pos(), "NewIterator takes DefaultIteratorOptions unchanged (prefetch on)")
				}
			case *ast.Ident:
				if prefetch[a.Name] {
					report(x.Pos(), "NewIterator(%s): copied from DefaultIteratorOptions without PrefetchValues = false", a.Name)
				}
			}
		}
		return true
	})
	return found, tracked
}
