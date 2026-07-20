package graph_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// TestGraphPackageDocListsEveryPublicMethod guards BACKLOG 7f: graph.go's
// package doc comment claims to enumerate "the complete public surface on
// *Graph itself", but the list is hand-maintained prose — a new exported
// New func or *Graph method added later has no structural link back to that
// list, so it silently drifts (exactly what happened with Replication() and
// SetReplicationSource(), both real accessors missing from the doc).
//
// This parses graph.go directly with go/parser, collects every exported
// top-level func named New and every exported method with receiver *Graph,
// and asserts each one's name appears in the package doc comment text. A
// future accessor added without updating the doc list now fails HERE at the
// source of truth, instead of silently drifting until a human notices.
func TestGraphPackageDocListsEveryPublicMethod(t *testing.T) {
	fset := token.NewFileSet()
	astFile, err := parser.ParseFile(fset, "graph.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("failed to parse graph.go: %v", err)
	}
	if astFile.Doc == nil {
		t.Fatal("graph.go has no package doc comment — go/parser regression, or the doc comment moved")
	}
	docText := astFile.Doc.Text()

	var surface []string
	for _, decl := range astFile.Decls {
		funcDecl, ok := decl.(*ast.FuncDecl)
		if !ok || !funcDecl.Name.IsExported() {
			continue
		}
		if funcDecl.Recv == nil {
			if funcDecl.Name.Name == "New" {
				surface = append(surface, funcDecl.Name.Name)
			}
			continue
		}
		if recvIsGraphPointer(funcDecl) {
			surface = append(surface, funcDecl.Name.Name)
		}
	}

	if len(surface) == 0 {
		t.Fatal("no exported New func or *Graph methods found in graph.go — go/parser regression, or the file was restructured")
	}

	for _, name := range surface {
		if !strings.Contains(docText, name+"(") {
			t.Errorf("graph.go declares %s but it does not appear in the package doc comment's "+
				"\"complete public surface\" list — add it there", name)
		}
	}
}

func recvIsGraphPointer(funcDecl *ast.FuncDecl) bool {
	if funcDecl.Recv == nil || len(funcDecl.Recv.List) != 1 {
		return false
	}
	star, ok := funcDecl.Recv.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	ident, ok := star.X.(*ast.Ident)
	return ok && ident.Name == "Graph"
}
