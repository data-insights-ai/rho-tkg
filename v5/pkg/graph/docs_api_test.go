package graph

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// apiDoc is the versioned public-surface document. Every exported identifier
// of this package must appear in it; a new export fails this test until the
// document names it (WP-17). Methods are written Type.Method in the document.
const apiDoc = "../../../docs/v5/API.md"

// exportedIdentifiers returns the exported top-level names of the package's
// non-test files, plus exported methods of exported types as "Type.Method".
func exportedIdentifiers(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		af, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range af.Decls {
			switch d := d.(type) {
			case *ast.FuncDecl:
				if !d.Name.IsExported() {
					continue
				}
				if d.Recv == nil {
					seen[d.Name.Name] = true
					continue
				}
				if recv := recvName(d.Recv.List[0].Type); recv != "" && ast.IsExported(recv) {
					seen[recv+"."+d.Name.Name] = true
				}
			case *ast.GenDecl:
				for _, s := range d.Specs {
					switch s := s.(type) {
					case *ast.TypeSpec:
						if s.Name.IsExported() {
							seen[s.Name.Name] = true
						}
					case *ast.ValueSpec:
						for _, n := range s.Names {
							if n.IsExported() {
								seen[n.Name] = true
							}
						}
					}
				}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func recvName(e ast.Expr) string {
	switch e := e.(type) {
	case *ast.StarExpr:
		return recvName(e.X)
	case *ast.Ident:
		return e.Name
	case *ast.IndexExpr:
		return recvName(e.X)
	}
	return ""
}

func TestDocsAPIListsEveryExportedIdentifier(t *testing.T) {
	raw, err := os.ReadFile(apiDoc)
	if err != nil {
		t.Fatalf("docs/v5/API.md is the versioned surface and must exist: %v", err)
	}
	doc := string(raw)
	names := exportedIdentifiers(t)
	if len(names) < 100 {
		t.Fatalf("found only %d exported identifiers; the scanner is broken", len(names))
	}
	var missing []string
	for _, n := range names {
		if !regexp.MustCompile(`(^|[^A-Za-z0-9_.])` + regexp.QuoteMeta(n) + `([^A-Za-z0-9_]|$)`).MatchString(doc) {
			missing = append(missing, n)
		}
	}
	if len(missing) > 0 {
		t.Errorf("docs/v5/API.md lacks %d exported identifier(s) of pkg/graph:\n%s", len(missing), strings.Join(missing, "\n"))
	}
}

// The scanner must see what the document contract depends on; a rename of the
// scan itself would otherwise pass vacuously.
func TestDocsAPIScannerSeesTheSurface(t *testing.T) {
	got := map[string]bool{}
	for _, n := range exportedIdentifiers(t) {
		got[n] = true
	}
	for _, want := range []string{"Open", "Graph", "Graph.Update", "Snapshot.Scan", "ErrConflict", "ViewDeclared", "MaxPageRows", "Value.Equal"} {
		if !got[want] {
			t.Errorf("scanner missed %s", want)
		}
	}
	for _, not := range []string{"testHelper", "Graph.close"} {
		if got[not] {
			t.Errorf("scanner returned unexported %s", not)
		}
	}
}
