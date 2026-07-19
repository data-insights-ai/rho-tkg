package core

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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
	g, err := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 3, MaxPropertyContainerLength: 3}})
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
		// BACKLOG 14a fix: []int/[]string never actually recurse (they have no
		// depth+1 call), so the old `depth+1 > maxPropertyValueLimitDepth`
		// check inside these leaf-container cases was dead/inconsistent — it
		// fired ONLY at the exact depth==maxPropertyValueLimitDepth boundary
		// (every deeper depth was already caught by this function's OWN
		// top-of-body `depth > maxPropertyValueLimitDepth` guard, which still
		// applies here unchanged). Removing it makes the boundary consistent
		// with every other leaf type; these two now correctly pass (they were
		// never a real depth violation, just a 1-element/1-string container).
		{name: "non-empty int slice at max depth is not a depth violation", val: []int{1}, dep: maxPropertyValueLimitDepth},
		{name: "empty int slice at max depth", val: []int{}, dep: maxPropertyValueLimitDepth},
		{name: "string slice at max depth is not a depth violation", val: []string{"ok"}, dep: maxPropertyValueLimitDepth},
		{name: "string slice value too large", val: []string{"ok", "xxxx"}, want: ErrValueTooLarge},
		{name: "any slice nested too large", val: []any{[]any{"xxxx"}}, want: ErrValueTooLarge},
		{name: "string map key too large", val: map[string]string{"xxxx": "ok"}, want: ErrValueTooLarge},
		{name: "string map too deep", val: map[string]string{"ok": "ok"}, dep: maxPropertyValueLimitDepth, want: types.ErrMaxDepthExceeded},
		{name: "string map value too large", val: map[string]string{"ok": "xxxx"}, want: ErrValueTooLarge},
		{name: "any map key too large", val: map[string]any{"xxxx": "ok"}, want: ErrValueTooLarge},
		{name: "any map nested too large", val: map[string]any{"ok": []any{"xxxx"}}, want: ErrValueTooLarge},
		{name: "struct with no string fields", val: struct{ X int }{X: 1}},
		{name: "struct string too large", val: struct{ X string }{X: "xxxx"}, want: ErrValueTooLarge},

		// BACKLOG 14b: aggregate container length/entry cap, governed by the
		// SEPARATE MaxPropertyContainerLength field (=3 in this test's
		// config) — deliberately distinct from MaxPropertyValueSize (string
		// length), so a legitimate large numeric container (e.g. a
		// vector-index embedding) is not accidentally bound by a
		// string-length knob. []string keeps its ORIGINAL per-element-only
		// behavior (no new aggregate-count cap) — the backlog's own finding
		// scoped []string as "correctly checking length" already.
		{name: "int slice within container cap", val: []int{1, 2, 3}},
		{name: "int slice exceeds container cap", val: []int{1, 2, 3, 4}, want: ErrValueTooLarge},
		{name: "int64 slice exceeds container cap", val: []int64{1, 2, 3, 4}, want: ErrValueTooLarge},
		{name: "float32 slice exceeds container cap", val: []float32{1, 2, 3, 4}, want: ErrValueTooLarge},
		{name: "float64 slice exceeds container cap", val: []float64{1, 2, 3, 4}, want: ErrValueTooLarge},
		{name: "bool slice exceeds container cap", val: []bool{true, true, true, true}, want: ErrValueTooLarge},
		{name: "byte slice within cap (byte length)", val: []byte{1, 2, 3}},
		{name: "byte slice exceeds cap", val: []byte{1, 2, 3, 4}, want: ErrValueTooLarge},
		{name: "string slice unaffected by container cap (unchanged scope)", val: []string{"a", "b", "c", "d"}},
		{name: "any slice exceeds container cap", val: []any{1, 2, 3, 4}, want: ErrValueTooLarge},
		{name: "string map exceeds entry cap", val: map[string]string{"a": "1", "b": "2", "c": "3", "d": "4"}, want: ErrValueTooLarge},
		{name: "any map exceeds entry cap", val: map[string]any{"a": 1, "b": 2, "c": 3, "d": 4}, want: ErrValueTooLarge},
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
	g, err := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 3, MaxPropertyContainerLength: 3}})
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

		// BACKLOG 14b: the reflect-based fallback (arbitrary slice/map kinds not
		// in the typed fast-path switch, e.g. []int32) gets the same aggregate
		// container length/entry cap.
		{name: "reflect slice within container cap", rv: reflect.ValueOf([]int32{1, 2, 3})},
		{name: "reflect slice exceeds container cap", rv: reflect.ValueOf([]int32{1, 2, 3, 4}), want: ErrValueTooLarge},
		{name: "reflect map exceeds entry cap", rv: reflect.ValueOf(map[string]int32{"a": 1, "b": 2, "c": 3, "d": 4}), want: ErrValueTooLarge},
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
				_, err := g.Nodes.Add(context.Background(), []string{"Custom"}, map[string]any{"custom": oversized})
				return err
			},
		},
		{
			name: "node update",
			run: func(g *Core) error {
				n, err := g.Nodes.Add(context.Background(), []string{"Custom"}, map[string]any{"custom": valid})
				if err != nil {
					return err
				}
				_, err = g.Nodes.Update(context.Background(), n.ID(), map[string]any{"custom": oversized})
				return err
			},
		},
		{
			name: "relationship add pointer",
			run: func(g *Core) error {
				a, err := g.Nodes.Add(context.Background(), []string{"Custom"}, nil)
				if err != nil {
					return err
				}
				b, err := g.Nodes.Add(context.Background(), []string{"Custom"}, nil)
				if err != nil {
					return err
				}
				_, err = g.Rels.Add(context.Background(), "LINKS", a, b, map[string]any{"custom": &oversized})
				return err
			},
		},
		{
			name: "relationship update pointer",
			run: func(g *Core) error {
				a, err := g.Nodes.Add(context.Background(), []string{"Custom"}, nil)
				if err != nil {
					return err
				}
				b, err := g.Nodes.Add(context.Background(), []string{"Custom"}, nil)
				if err != nil {
					return err
				}
				r, err := g.Rels.Add(context.Background(), "LINKS", a, b, map[string]any{"custom": valid})
				if err != nil {
					return err
				}
				_, err = g.Rels.Update(context.Background(), r.ID(), map[string]any{"custom": &oversized})
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

// TestGraphMutationsRejectOversizedContainerProperty is the BACKLOG 14b
// regression, exercised end-to-end through the real Add/Update doors (not
// just the validator function directly): a slice/map property with more
// elements than MaxPropertyValueSize must be rejected with ErrValueTooLarge,
// not silently accepted. Only []int/[]int64/[]float32/[]float64/[]bool/
// []any/map never bounded aggregate container size before this fix — only
// per-leaf string length and property *count* (MaxPropertiesPerEntity) were
// checked, so e.g. a many-million-element []float64 or a huge []byte would
// have passed unrejected.
func TestGraphMutationsRejectOversizedContainerProperty(t *testing.T) {
	newGraph := func(t *testing.T) *Core {
		t.Helper()
		g, err := New(Config{Validation: ValidationLimits{MaxPropertyContainerLength: 3}})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return g
	}
	oversizedInts := []int64{1, 2, 3, 4} // 4 elements > MaxPropertyContainerLength(3)
	validInts := []int64{1, 2, 3}

	tests := []struct {
		name string
		run  func(*Core) error
	}{
		{
			name: "node add oversized []int64",
			run: func(g *Core) error {
				_, err := g.Nodes.Add(context.Background(), []string{"Metric"}, map[string]any{"vals": oversizedInts})
				return err
			},
		},
		{
			name: "node update oversized []int64",
			run: func(g *Core) error {
				n, err := g.Nodes.Add(context.Background(), []string{"Metric"}, map[string]any{"vals": validInts})
				if err != nil {
					return err
				}
				_, err = g.Nodes.Update(context.Background(), n.ID(), map[string]any{"vals": oversizedInts})
				return err
			},
		},
		{
			name: "node add oversized map[string]any",
			run: func(g *Core) error {
				_, err := g.Nodes.Add(context.Background(), []string{"Metric"}, map[string]any{
					"meta": map[string]any{"a": 1, "b": 2, "c": 3, "d": 4},
				})
				return err
			},
		},
		{
			name: "relationship add oversized []int64",
			run: func(g *Core) error {
				a, err := g.Nodes.Add(context.Background(), []string{"P"}, nil)
				if err != nil {
					return err
				}
				b, err := g.Nodes.Add(context.Background(), []string{"P"}, nil)
				if err != nil {
					return err
				}
				_, err = g.Rels.Add(context.Background(), "LINKS", a, b, map[string]any{"vals": oversizedInts})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run(newGraph(t))
			if !errors.Is(err, ErrValueTooLarge) {
				t.Fatalf("error = %v, want ErrValueTooLarge (unbounded container size — BACKLOG 14b regression)", err)
			}
		})
	}

	// A container WITHIN the cap must still be accepted (no over-rejection).
	t.Run("node add within-cap []int64 succeeds", func(t *testing.T) {
		g := newGraph(t)
		if _, err := g.Nodes.Add(context.Background(), []string{"Metric"}, map[string]any{"vals": validInts}); err != nil {
			t.Fatalf("Add within-cap []int64: %v", err)
		}
	})
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
