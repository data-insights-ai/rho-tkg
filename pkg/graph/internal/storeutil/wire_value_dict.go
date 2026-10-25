package storeutil

import (
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// ValueTokens resolves value-dictionary tokens (DICT) for the store that wrote
// the row: a token means something only there.
type ValueTokens interface {
	// ValueOf returns the string a token stands for, false for an unknown one.
	ValueOf(token uint32) (string, bool)
}

// hasDictToken reports whether a property carries a value-dictionary token
// (the row is then ValueDictWireFormatVersion).
func hasDictToken(pw []PropertyWire) bool {
	for i := range pw {
		if pw[i].DictToken != 0 {
			return true
		}
	}
	return false
}

// ResolvePropertyValueTokens replaces each value-dictionary token with its
// string and clears the token, so the property reads as if stored inline. A
// token on a row below ValueDictWireFormatVersion, next to a value, on a
// non-string or typed-nil property, with no dictionary, or unknown to it is
// store.ErrCorruptWire: a wrong string is never returned. Call after the
// row's decode, before its validation.
func ResolvePropertyValueTokens(pw []PropertyWire, fv uint8, vt ValueTokens) error {
	for i := range pw {
		p := &pw[i]
		if p.DictToken == 0 {
			continue
		}
		switch {
		case fv < ValueDictWireFormatVersion:
			return fmt.Errorf("property[%d]: value-dictionary token on a row of format %d: %w", i, fv, storepkg.ErrCorruptWire)
		case p.Value != nil || p.Nil || p.Type != ptString:
			return fmt.Errorf("property[%d]: value-dictionary token on a property with a value, typed nil or type tag %d: %w", i, p.Type, storepkg.ErrCorruptWire)
		case vt == nil:
			return fmt.Errorf("property[%d]: value-dictionary token %d with no dictionary: %w", i, p.DictToken, storepkg.ErrCorruptWire)
		}
		s, ok := vt.ValueOf(p.DictToken)
		if !ok {
			return fmt.Errorf("property[%d]: value-dictionary token %d not in the dictionary: %w", i, p.DictToken, storepkg.ErrCorruptWire)
		}
		p.Value, p.DictToken = s, 0
	}
	return nil
}
