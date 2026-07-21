package core

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
)

// TestMetaKVReapPolicyCoversEveryKey is the BACKLOG 13l enforcement mechanism:
// it AST-walks every non-test .go file in this package for mk.MetaSet(...)
// call sites and fails closed if the key expression is not registered in
// metakv_reap_policy.go's metaKVFixedKeys/metaKVKeyFuncs maps. This is what
// makes the classification a GLOBAL, CI-enforced guarantee rather than a
// second hand-maintained checklist sitting next to the first one —a future
// contributor adding a new MetaKV-backed feature cannot land it without this
// test forcing an explicit reap-vs-preserve decision.
//
// Scope is deliberately narrow: only literal mk.MetaSet(...) calls in THIS
// package. The label/reltype/property-key registries persist via a SEPARATE
// mechanism (persistRegistries -> store.SaveRegistries), never a raw MetaSet
// call, so they are correctly outside this check's surface — they already
// have their own dedicated, tested reap-vs-preserve behavior (preserved,
// re-persisted after Clear).
func TestMetaKVReapPolicyCoversEveryKey(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	fset := token.NewFileSet()
	files := make([]*ast.File, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || filepath.Ext(name) != ".go" {
			continue
		}
		if len(name) >= 8 && name[len(name)-8:] == "_test.go" {
			continue // production call sites only — test files may exercise MetaSet directly on other keys for unrelated fixtures
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatalf("ParseFile(%s): %v", name, err)
		}
		files = append(files, f)
	}

	// Pass 1: collect every package-level `const X = "literal"` binding, so a
	// MetaSet(constIdent, ...) argument can be resolved to its string value.
	constVals := make(map[string]string)
	for _, f := range files {
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, name := range vs.Names {
					if i >= len(vs.Values) {
						continue
					}
					lit, ok := vs.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					v, err := strconv.Unquote(lit.Value)
					if err != nil {
						continue
					}
					constVals[name.Name] = v
				}
			}
		}
	}

	// Pass 2: walk every mk.MetaSet(...) / c.metaKV.MetaSet(...) call
	// (matched by selector name "MetaSet", regardless of receiver — this
	// package always calls it through a *store.MetaKVCapability handle
	// named mk or c.metaKV) and classify its first argument.
	var unclassified []string
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "MetaSet" {
				return true
			}
			if len(call.Args) == 0 {
				return true
			}
			pos := fset.Position(call.Pos())
			loc := pos.Filename + ":" + strconv.Itoa(pos.Line)

			switch arg := call.Args[0].(type) {
			case *ast.Ident:
				key, ok := constVals[arg.Name]
				if !ok {
					unclassified = append(unclassified, loc+": MetaSet key identifier %q does not resolve to a known package-level string const — add it to metakv_reap_policy.go's metaKVFixedKeys (or, if it is a variable, register its BUILDER via metaKVKeyFuncs and refactor the call site to go through a named function)"+"["+arg.Name+"]")
					return true
				}
				if _, ok := metaKVFixedKeys[key]; !ok {
					unclassified = append(unclassified, loc+": MetaSet key "+strconv.Quote(key)+" is not classified in metakv_reap_policy.go's metaKVFixedKeys — decide Reap (Clear's wipe stands) or Preserve (capture-before-Clear, restore-after, mirroring persistRegistries/restoreIDSlotLeaseAfterReset) and add it")
				}
			case *ast.BasicLit:
				if arg.Kind != token.STRING {
					unclassified = append(unclassified, loc+": MetaSet key is a non-string literal — add explicit handling to this check")
					return true
				}
				key, err := strconv.Unquote(arg.Value)
				if err != nil {
					unclassified = append(unclassified, loc+": MetaSet key literal could not be unquoted — add explicit handling to this check")
					return true
				}
				if _, ok := metaKVFixedKeys[key]; !ok {
					unclassified = append(unclassified, loc+": MetaSet key "+strconv.Quote(key)+" is not classified in metakv_reap_policy.go's metaKVFixedKeys — decide Reap or Preserve and add it")
				}
			case *ast.CallExpr:
				fnIdent, ok := arg.Fun.(*ast.Ident)
				if !ok {
					unclassified = append(unclassified, loc+": MetaSet key is a call to a non-identifier function expression — add explicit handling to this check")
					return true
				}
				if _, ok := metaKVKeyFuncs[fnIdent.Name]; !ok {
					unclassified = append(unclassified, loc+": MetaSet key builder function "+strconv.Quote(fnIdent.Name)+" is not classified in metakv_reap_policy.go's metaKVKeyFuncs — decide Reap or Preserve for every key it builds and add it")
				}
			default:
				unclassified = append(unclassified, loc+": MetaSet key expression shape not recognized by this check (neither an identifier, a string literal, nor a function call) — add explicit handling to metakv_reap_policy_check_test.go")
			}
			return true
		})
	}

	if len(unclassified) > 0 {
		sort.Strings(unclassified)
		t.Fatalf("BACKLOG 13l: found %d MetaKV key(s) with no reap-vs-preserve classification:\n%s",
			len(unclassified), joinLines(unclassified))
	}
}

func joinLines(lines []string) string {
	out := ""
	for _, l := range lines {
		out += "  - " + l + "\n"
	}
	return out
}
