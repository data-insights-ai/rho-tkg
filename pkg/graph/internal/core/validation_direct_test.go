package core

import (
	"errors"
	"reflect"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

type sizeLimitCustomProperty struct {
	Name  string
	Tags  []string
	Meta  map[string]any
	Child *sizeLimitCustomProperty
}

func (p sizeLimitCustomProperty) HashBytes() []byte {
	return []byte(p.Name)
}

func (p sizeLimitCustomProperty) DeepCopyValue() any {
	cp := p
	if p.Tags != nil {
		cp.Tags = append([]string(nil), p.Tags...)
	}
	if p.Meta != nil {
		cp.Meta = make(map[string]any, len(p.Meta))
		for k, v := range p.Meta {
			cp.Meta[k] = v
		}
	}
	if p.Child != nil {
		child := p.Child.DeepCopyValue().(sizeLimitCustomProperty)
		cp.Child = &child
	}
	return cp
}

func TestCoreIsBlankNameUnicodeAndInvalidUTF8(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want bool
	}{
		{name: "empty", in: "", want: true},
		{name: "ascii space", in: " \t\n", want: true},
		{name: "non breaking space", in: "\u00a0", want: true},
		{name: "em space", in: "\u2003", want: true},
		{name: "ascii letter", in: "A", want: false},
		{name: "unicode letter", in: "λ", want: false},
		{name: "unicode letter after whitespace", in: "\u2003λ", want: false},
		{name: "invalid utf8", in: string([]byte{0xff}), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isBlankName(tt.in); got != tt.want {
				t.Fatalf("isBlankName(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestValidatePropertyValueLimitTypedDirectBranches(t *testing.T) {
	g, err := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 3}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	tests := []struct {
		name string
		val  any
		dep  int
		want error
	}{
		{name: "nil", val: nil},
		{name: "scalar", val: int64(1)},
		{name: "string too large", val: "xxxx", want: ErrValueTooLarge},
		{name: "non-empty int slice too deep", val: []int{1}, dep: maxPropertyValueLimitDepth, want: types.ErrMaxDepthExceeded},
		{name: "empty int slice at max depth", val: []int{}, dep: maxPropertyValueLimitDepth},
		{name: "string slice too deep", val: []string{"ok"}, dep: maxPropertyValueLimitDepth, want: types.ErrMaxDepthExceeded},
		{name: "string slice value too large", val: []string{"ok", "xxxx"}, want: ErrValueTooLarge},
		{name: "any slice nested too large", val: []any{[]any{"xxxx"}}, want: ErrValueTooLarge},
		{name: "string map key too large", val: map[string]string{"xxxx": "ok"}, want: ErrValueTooLarge},
		{name: "string map too deep", val: map[string]string{"ok": "ok"}, dep: maxPropertyValueLimitDepth, want: types.ErrMaxDepthExceeded},
		{name: "string map value too large", val: map[string]string{"ok": "xxxx"}, want: ErrValueTooLarge},
		{name: "any map key too large", val: map[string]any{"xxxx": "ok"}, want: ErrValueTooLarge},
		{name: "any map nested too large", val: map[string]any{"ok": []any{"xxxx"}}, want: ErrValueTooLarge},
		{name: "struct with no string fields", val: struct{ X int }{X: 1}},
		{name: "struct string too large", val: struct{ X string }{X: "xxxx"}, want: ErrValueTooLarge},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := g.validatePropertyValueLimitTyped("k", tt.val, tt.dep)
			if tt.want == nil {
				if err != nil {
					t.Fatalf("validatePropertyValueLimitTyped: %v", err)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("validatePropertyValueLimitTyped error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestValidatePropertyValueLimitReflectBranches(t *testing.T) {
	g, err := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 3}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	var nilInterface any
	var nilSlice []string
	var nilMap map[string]any
	var nilCustom *sizeLimitCustomProperty

	tests := []struct {
		name string
		rv   reflect.Value
		dep  int
		want error
	}{
		{name: "invalid reflect value", rv: reflect.Value{}},
		{name: "too deep before kind switch", rv: reflect.ValueOf("ok"), dep: maxPropertyValueLimitDepth + 1, want: types.ErrMaxDepthExceeded},
		{name: "nil interface", rv: reflect.ValueOf(&nilInterface).Elem()},
		{name: "interface unwrap too large", rv: reflect.ValueOf(any("xxxx")), want: ErrValueTooLarge},
		{name: "string too large", rv: reflect.ValueOf("xxxx"), want: ErrValueTooLarge},
		{name: "nil slice", rv: reflect.ValueOf(nilSlice)},
		{name: "slice nested too large", rv: reflect.ValueOf([]any{"xxxx"}), want: ErrValueTooLarge},
		{name: "nil map", rv: reflect.ValueOf(nilMap)},
		{name: "map key too large", rv: reflect.ValueOf(map[string]any{"xxxx": "ok"}), want: ErrValueTooLarge},
		{name: "map value too large", rv: reflect.ValueOf(map[string]any{"ok": "xxxx"}), want: ErrValueTooLarge},
		{name: "nil pointer", rv: reflect.ValueOf(nilCustom)},
		{name: "pointer string field too large", rv: reflect.ValueOf(&sizeLimitCustomProperty{Name: "xxxx"}), want: ErrValueTooLarge},
		{name: "struct nested string slice too large", rv: reflect.ValueOf(sizeLimitCustomProperty{Tags: []string{"xxxx"}}), want: ErrValueTooLarge},
		{name: "struct nested map string too large", rv: reflect.ValueOf(sizeLimitCustomProperty{Meta: map[string]any{"ok": "xxxx"}}), want: ErrValueTooLarge},
		{name: "struct nested pointer string too large", rv: reflect.ValueOf(sizeLimitCustomProperty{Child: &sizeLimitCustomProperty{Name: "xxxx"}}), want: ErrValueTooLarge},
		{name: "default kind", rv: reflect.ValueOf(1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := g.validatePropertyValueLimit("k", tt.rv, tt.dep)
			if tt.want == nil {
				if err != nil {
					t.Fatalf("validatePropertyValueLimit: %v", err)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("validatePropertyValueLimit error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestValidatePropertiesPrevalidatesUnsupportedReflectShapesBeforeSizeLimit(t *testing.T) {
	t.Parallel()

	type unsupportedStruct struct {
		Name string
	}
	type aliasString string

	g, err := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 3}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	tests := []struct {
		name  string
		props map[string]any
	}{
		{name: "top-level struct", props: map[string]any{"bad": unsupportedStruct{Name: "xxxx"}}},
		{name: "nested struct", props: map[string]any{"bad": map[string]any{"nested": unsupportedStruct{Name: "xxxx"}}}},
		{name: "named scalar alias", props: map[string]any{"bad": aliasString("xxxx")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := g.validateProperties(tt.props)
			if !errors.Is(err, types.ErrUnsupportedValueType) {
				t.Fatalf("validateProperties error = %v, want ErrUnsupportedValueType", err)
			}
			if errors.Is(err, ErrValueTooLarge) {
				t.Fatalf("validateProperties error = %v, should preserve unsupported-type precedence", err)
			}
		})
	}
}

func TestGraphMutationsRejectOversizedStringInsideRegisteredPropertyStruct(t *testing.T) {
	if err := types.RegisterPropertyStructType(sizeLimitCustomProperty{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}

	newGraph := func(t *testing.T) *Core {
		t.Helper()
		g, err := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 3}})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return g
	}
	valid := sizeLimitCustomProperty{Name: "ok"}
	oversized := sizeLimitCustomProperty{Name: "xxxx"}

	tests := []struct {
		name string
		run  func(*Core) error
	}{
		{
			name: "node add",
			run: func(g *Core) error {
				_, err := g.Nodes.Add([]string{"Custom"}, map[string]any{"custom": oversized})
				return err
			},
		},
		{
			name: "node update",
			run: func(g *Core) error {
				n, err := g.Nodes.Add([]string{"Custom"}, map[string]any{"custom": valid})
				if err != nil {
					return err
				}
				_, err = g.Nodes.Update(n.ID(), map[string]any{"custom": oversized})
				return err
			},
		},
		{
			name: "relationship add pointer",
			run: func(g *Core) error {
				a, err := g.Nodes.Add([]string{"Custom"}, nil)
				if err != nil {
					return err
				}
				b, err := g.Nodes.Add([]string{"Custom"}, nil)
				if err != nil {
					return err
				}
				_, err = g.Rels.Add("LINKS", a, b, map[string]any{"custom": &oversized})
				return err
			},
		},
		{
			name: "relationship update pointer",
			run: func(g *Core) error {
				a, err := g.Nodes.Add([]string{"Custom"}, nil)
				if err != nil {
					return err
				}
				b, err := g.Nodes.Add([]string{"Custom"}, nil)
				if err != nil {
					return err
				}
				r, err := g.Rels.Add("LINKS", a, b, map[string]any{"custom": valid})
				if err != nil {
					return err
				}
				_, err = g.Rels.Update(r.ID(), map[string]any{"custom": &oversized})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run(newGraph(t))
			if !errors.Is(err, ErrValueTooLarge) {
				t.Fatalf("error = %v, want ErrValueTooLarge", err)
			}
		})
	}
}

func TestPrepareQueuedUpdatePropertiesOwnsSignature(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	sig := []byte{1, 2, 3}
	got, err := g.prepareQueuedUpdateProperties(map[string]any{
		"tkg_author_id":     "author",
		"tkg_signature":     sig,
		"tkg_authorized_by": "approver",
		"tkg_auth_level":    uint8(7),
		"ordinary":          "kept",
	}, "test update")
	if err != nil {
		t.Fatalf("prepareQueuedUpdateProperties: %v", err)
	}

	if got.originalLen != 5 {
		t.Fatalf("originalLen = %d, want 5", got.originalLen)
	}
	sig[0] = 9
	if got.provenance.signature[0] != 1 {
		t.Fatalf("signature clone aliased caller slice: %v", got.provenance.signature)
	}
	if got.provenance.authorID != "author" || got.provenance.authorizedBy != "approver" || got.provenance.authLevel != 7 {
		t.Fatalf("provenance = %+v", got.provenance)
	}
	if got.properties["ordinary"] != "kept" {
		t.Fatalf("ordinary property = %#v", got.properties["ordinary"])
	}
	if _, ok := got.properties["tkg_author_id"]; ok {
		t.Fatalf("provenance key was stored as property: %#v", got.properties)
	}
}

func TestPrepareQueuedUpdatePropertiesRejectsNonByteSignature(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	if _, err := g.prepareQueuedUpdateProperties(map[string]any{
		"tkg_signature": "not bytes",
	}, "test update"); err == nil {
		t.Fatal("prepareQueuedUpdateProperties accepted non-byte tkg_signature")
	}
}
