package types

import (
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// shadowConstsFromSource reads every Shadow* string constant declared in
// shadow.go, so a constant added there without a row below fails the test.
func shadowConstsFromSource(t *testing.T) map[string]string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "shadow.go", nil, 0)
	if err != nil {
		t.Fatalf("parse shadow.go: %v", err)
	}
	got := map[string]string{}
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}
		for _, spec := range gd.Specs {
			vs := spec.(*ast.ValueSpec)
			for i, name := range vs.Names {
				if !strings.HasPrefix(name.Name, "Shadow") || i >= len(vs.Values) {
					continue
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					t.Fatalf("%s is not a string literal", name.Name)
				}
				v, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatalf("unquote %s: %v", name.Name, err)
				}
				got[name.Name] = v
			}
		}
	}
	return got
}

// TestIsShadowKey pins every shadow key constant: its exact value, that the
// table below names exactly the Shadow* constants shadow.go declares, and that
// IsShadowKey accepts each one.
func TestIsShadowKey(t *testing.T) {
	t.Parallel()

	want := map[string]string{
		"ShadowID":           ShadowID,
		"ShadowLabels":       ShadowLabels,
		"ShadowType":         ShadowType,
		"ShadowValidFrom":    ShadowValidFrom,
		"ShadowValidTo":      ShadowValidTo,
		"ShadowTxFrom":       ShadowTxFrom,
		"ShadowTxTo":         ShadowTxTo,
		"ShadowCreatedAt":    ShadowCreatedAt,
		"ShadowUpdatedAt":    ShadowUpdatedAt,
		"ShadowDeletedAt":    ShadowDeletedAt,
		"ShadowCreatedBy":    ShadowCreatedBy,
		"ShadowUpdatedBy":    ShadowUpdatedBy,
		"ShadowVersion":      ShadowVersion,
		"ShadowHash":         ShadowHash,
		"ShadowPrevHash":     ShadowPrevHash,
		"ShadowFromHash":     ShadowFromHash,
		"ShadowToHash":       ShadowToHash,
		"ShadowAuthorID":     ShadowAuthorID,
		"ShadowSignature":    ShadowSignature,
		"ShadowAuthorizedBy": ShadowAuthorizedBy,
		"ShadowAuthLevel":    ShadowAuthLevel,
		"ShadowBaseEntity":   ShadowBaseEntity,
	}
	literal := map[string]string{
		"ShadowID": "tkg_id", "ShadowLabels": "tkg_labels", "ShadowType": "tkg_type",
		"ShadowValidFrom": "tkg_valid_from", "ShadowValidTo": "tkg_valid_to",
		"ShadowTxFrom": "tkg_tx_from", "ShadowTxTo": "tkg_tx_to",
		"ShadowCreatedAt": "tkg_created_at", "ShadowUpdatedAt": "tkg_updated_at", "ShadowDeletedAt": "tkg_deleted_at",
		"ShadowCreatedBy": "tkg_created_by", "ShadowUpdatedBy": "tkg_updated_by", "ShadowVersion": "tkg_version",
		"ShadowHash": "tkg_hash", "ShadowPrevHash": "tkg_prev_hash",
		"ShadowFromHash": "tkg_from_hash", "ShadowToHash": "tkg_to_hash",
		"ShadowAuthorID": "tkg_author_id", "ShadowSignature": "tkg_signature",
		"ShadowAuthorizedBy": "tkg_authorized_by", "ShadowAuthLevel": "tkg_auth_level",
		"ShadowBaseEntity": "tkg_base_entity",
	}
	if !maps.Equal(want, literal) {
		t.Errorf("shadow constants = %v, want %v", want, literal)
	}
	src := shadowConstsFromSource(t)
	if !maps.Equal(src, literal) {
		t.Errorf("shadow.go declares %v, want exactly %v", slices.Sorted(maps.Keys(src)), slices.Sorted(maps.Keys(literal)))
	}
	for name, key := range want {
		if !IsShadowKey(key) {
			t.Errorf("IsShadowKey(%s = %q) = false, want true", name, key)
		}
	}
}

func TestIsShadowKeyRejectsNonShadow(t *testing.T) {
	t.Parallel()

	nonShadow := []string{"name", "age", "", "tkglabels", "TKG_labels", "shadow_key"}
	for _, key := range nonShadow {
		if IsShadowKey(key) {
			t.Errorf("IsShadowKey(%q) = true, want false", key)
		}
	}
}
