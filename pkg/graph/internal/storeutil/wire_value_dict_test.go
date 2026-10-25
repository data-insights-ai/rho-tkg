package storeutil

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type mapTokens map[uint32]string

func (m mapTokens) ValueOf(t uint32) (string, bool) { s, ok := m[t]; return s, ok }

// dictNodeWire is a node with a string property "action" (coded as token 7
// when coded) and an int property "port", as the wire a store encodes.
func dictNodeWire(t *testing.T, coded bool) NodeWire {
	t.Helper()
	n := types.NewNode(types.NodeID(4243), 3, nil)
	n.SetVersion(1)
	if err := n.SetProperty("action", "connection_accepted"); err != nil {
		t.Fatal(err)
	}
	if err := n.SetProperty("port", int64(443)); err != nil {
		t.Fatal(err)
	}
	w, err := NodeToWireChecked(n)
	if err != nil {
		t.Fatal(err)
	}
	if coded {
		for i := range w.Properties {
			if w.Properties[i].Key == "action" {
				w.Properties[i].Value, w.Properties[i].DictToken = nil, 7
			}
		}
	}
	return w
}

// TestValueDictRow_FormatVersion8 (DICT): a row with a value-dictionary token
// is written at ValueDictWireFormatVersion, is shorter than the plain row by
// about the value's length, decodes back to the same property once resolved,
// and a reader capped at 7 refuses it with ErrWireFormatVersionUnsupported.
// A row without a token keeps its version. Catches an encoder that writes the
// token without raising fv (an older binary would skip `dt` and read nil), one
// that writes `v` as well, a decoder that drops `dt`, and a resolver that
// leaves the token or the value unset.
func TestValueDictRow_FormatVersion8(t *testing.T) {
	plain := must(t)(marshalWirePooled(dictNodeWire(t, false)))
	coded := must(t)(marshalWirePooled(dictNodeWire(t, true)))
	if _, fv := fvOf(t, plain); fv == ValueDictWireFormatVersion {
		t.Fatalf("a row without a token was written at fv %d", fv)
	}
	if _, fv := fvOf(t, coded); fv != ValueDictWireFormatVersion {
		t.Fatalf("coded row fv %d, want %d", fv, ValueDictWireFormatVersion)
	}
	if saved := len(plain) - len(coded); saved < len("connection_accepted")-3 {
		t.Fatalf("coded row %d B, plain %d B: saved %d B, want about the value's %d B", len(coded), len(plain), saved, len("connection_accepted"))
	}
	w := mustDecodeNodeWire(t, coded)
	if err := ResolvePropertyValueTokens(w.Properties, w.FormatVersion, mapTokens{7: "connection_accepted"}); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	n, err := WireToNodeChecked(w)
	if err != nil {
		t.Fatalf("decode resolved row: %v", err)
	}
	if v, _ := n.GetProperty("action"); v != "connection_accepted" {
		t.Fatalf("action = %#v, want the dictionary's value", v)
	}
	if v, _ := n.GetProperty("port"); v != int64(443) {
		t.Fatalf("port = %#v, want 443", v)
	}
	restore := SetMaxReadableWireFormatVersionForTest(OpenVoidWireFormatVersion)
	_, _, err = DecodeWireTemporalMeta(coded)
	restore()
	if !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
		t.Fatalf("reader capped at %d: %v, want ErrWireFormatVersionUnsupported", OpenVoidWireFormatVersion, err)
	}
}

// TestValueDictRow_FailsClosed: a token that cannot be resolved to the right
// string is store.ErrCorruptWire, never a wrong or nil value: an unresolved
// token at validation, an unknown token, no dictionary, a token on a row
// below 8, next to a value, or on a non-string property. Catches a resolver
// that skips any of these checks.
func TestValueDictRow_FailsClosed(t *testing.T) {
	dict := mapTokens{7: "connection_accepted"}
	if err := ValidateNodeWire(mustDecodeNodeWire(t, must(t)(marshalWirePooled(dictNodeWire(t, true))))); !errors.Is(err, storecontract.ErrCorruptWire) {
		t.Fatalf("unresolved token validated: %v", err)
	}
	for _, c := range []struct {
		name string
		fv   uint8
		dict ValueTokens
		edit func(p *PropertyWire)
	}{
		{"unknown token", ValueDictWireFormatVersion, dict, func(p *PropertyWire) { p.DictToken = 8 }},
		{"no dictionary", ValueDictWireFormatVersion, nil, func(*PropertyWire) {}},
		{"row below 8", OpenVoidWireFormatVersion, dict, func(*PropertyWire) {}},
		{"token next to a value", ValueDictWireFormatVersion, dict, func(p *PropertyWire) { p.Value = "x" }},
		{"token on an int", ValueDictWireFormatVersion, dict, func(p *PropertyWire) { p.Type = ptInt }},
		{"token on a typed nil", ValueDictWireFormatVersion, dict, func(p *PropertyWire) { p.Nil = true }},
	} {
		w := dictNodeWire(t, true)
		for i := range w.Properties {
			if w.Properties[i].DictToken != 0 {
				c.edit(&w.Properties[i])
			}
		}
		if err := ResolvePropertyValueTokens(w.Properties, c.fv, c.dict); !errors.Is(err, storecontract.ErrCorruptWire) {
			t.Fatalf("%s: resolve = %v, want ErrCorruptWire", c.name, err)
		}
	}
}
