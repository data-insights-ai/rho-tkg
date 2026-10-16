package graphstore

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func distinctRecordKinds(kinds []recordKind) error {
	seen := make(map[recordKind]bool, len(kinds))
	for _, k := range kinds {
		if seen[k] {
			return fmt.Errorf("record kind %#x registered twice", byte(k))
		}
		seen[k] = true
	}
	return nil
}

// recordKindSource scans the package's non-test sources: every constant typed
// recordKind (including iota-implied ones), every constant named *Record, every
// recordKind(literal) conversion, and the names listed in the recordKinds registry.
func recordKindSource(t *testing.T) (declared, untyped, registered map[string]bool, literals []uint64) {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	declared, untyped, registered = map[string]bool{}, map[string]bool{}, map[string]bool{}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.GenDecl:
				typed := false
				for _, spec := range n.Specs {
					v, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					if n.Tok == token.CONST {
						if v.Type != nil || len(v.Values) > 0 {
							id, ok := v.Type.(*ast.Ident)
							typed = ok && id.Name == "recordKind"
						}
						for _, id := range v.Names {
							if typed {
								declared[id.Name] = true
							} else if strings.HasSuffix(id.Name, "Record") {
								untyped[id.Name] = true
							}
						}
					}
					if n.Tok == token.VAR && len(v.Names) == 1 && v.Names[0].Name == "recordKinds" && len(v.Values) == 1 {
						lit, ok := v.Values[0].(*ast.CompositeLit)
						if !ok {
							t.Fatal("recordKinds is not a composite literal")
						}
						for _, e := range lit.Elts {
							id, ok := e.(*ast.Ident)
							if !ok {
								t.Fatalf("recordKinds lists a non-constant %T", e)
							}
							registered[id.Name] = true
						}
					}
				}
			case *ast.CallExpr:
				fn, ok := n.Fun.(*ast.Ident)
				if !ok || fn.Name != "recordKind" || len(n.Args) != 1 {
					return true
				}
				if lit, ok := n.Args[0].(*ast.BasicLit); ok {
					v, err := strconv.ParseUint(lit.Value, 0, 8)
					if err != nil {
						t.Fatal(err)
					}
					literals = append(literals, v)
				}
			}
			return true
		})
	}
	return declared, untyped, registered, literals
}

func TestRecordKindsDistinct(t *testing.T) {
	if err := distinctRecordKinds(recordKinds); err != nil {
		t.Fatal(err)
	}
	for _, c := range [][]recordKind{
		append(append([]recordKind{}, recordKinds...), recordKinds[0]),
		append(append([]recordKind{}, recordKinds...), recordKinds[len(recordKinds)-1]),
		{evidenceRecord, evidenceRecord},
		{ownershipDeclarationRecord, recordKind(0x16)},
	} {
		if distinctRecordKinds(c) == nil {
			t.Fatalf("duplicate kinds accepted: %v", c)
		}
	}
	declared, untyped, registered, literals := recordKindSource(t)
	if len(declared) < 20 || len(registered) != len(recordKinds) {
		t.Fatalf("source scan found %d kinds, %d registry names for %d entries", len(declared), len(registered), len(recordKinds))
	}
	for name := range untyped {
		t.Errorf("%s is a record constant not typed recordKind", name)
	}
	for name := range declared {
		if !registered[name] {
			t.Errorf("record kind %s missing from recordKinds", name)
		}
	}
	for name := range registered {
		if !declared[name] {
			t.Errorf("recordKinds lists %s, not a recordKind constant", name)
		}
	}
	known := map[recordKind]bool{}
	for _, k := range recordKinds {
		known[k] = true
	}
	for _, v := range literals {
		if !known[recordKind(v)] {
			t.Errorf("recordKind(%#x) conversion is not a registered kind", v)
		}
	}
}
