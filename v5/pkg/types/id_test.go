package types_test

import (
	"encoding"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// idKind adapts NodeID and RelID to one table so both get identical cases.
type idKind struct {
	name   string
	typ    reflect.Type
	newID  func(uint64) (uint64, error)
	parse  func(string) (uint64, error)
	str    func(uint64) string
	isZero func(uint64) bool
	marsh  func(uint64) ([]byte, error)
	unmar  func(prev uint64, b []byte) (uint64, error)
}

func kinds() []idKind {
	return []idKind{{
		name:   "NodeID",
		typ:    reflect.TypeOf(types.NodeID(0)),
		newID:  func(v uint64) (uint64, error) { id, err := types.NewNodeID(v); return id.Uint64(), err },
		parse:  func(s string) (uint64, error) { id, err := types.ParseNodeID(s); return id.Uint64(), err },
		str:    func(v uint64) string { return types.NodeID(v).String() },
		isZero: func(v uint64) bool { return types.NodeID(v).IsZero() },
		marsh:  func(v uint64) ([]byte, error) { return types.NodeID(v).MarshalText() },
		unmar: func(prev uint64, b []byte) (uint64, error) {
			id := types.NodeID(prev)
			err := id.UnmarshalText(b)
			return id.Uint64(), err
		},
	}, {
		name:   "RelID",
		typ:    reflect.TypeOf(types.RelID(0)),
		newID:  func(v uint64) (uint64, error) { id, err := types.NewRelID(v); return id.Uint64(), err },
		parse:  func(s string) (uint64, error) { id, err := types.ParseRelID(s); return id.Uint64(), err },
		str:    func(v uint64) string { return types.RelID(v).String() },
		isZero: func(v uint64) bool { return types.RelID(v).IsZero() },
		marsh:  func(v uint64) ([]byte, error) { return types.RelID(v).MarshalText() },
		unmar: func(prev uint64, b []byte) (uint64, error) {
			id := types.RelID(prev)
			err := id.UnmarshalText(b)
			return id.Uint64(), err
		},
	}}
}

func TestIDShape(t *testing.T) {
	// Opaque 64-bit, no clock or placement (PLAN:63): exactly a uint64, no extra state.
	for _, k := range kinds() {
		if k.typ.Kind() != reflect.Uint64 || k.typ.Size() != 8 {
			t.Fatalf("%s: kind=%v size=%d, want uint64/8", k.name, k.typ.Kind(), k.typ.Size())
		}
	}
	if reflect.TypeOf(types.NodeID(0)) == reflect.TypeOf(types.RelID(0)) {
		t.Fatal("NodeID and RelID must be distinct types")
	}
	var _ encoding.TextMarshaler = types.NodeID(1)
	var _ encoding.TextUnmarshaler = new(types.RelID)
	// No clock accessor: neither type exposes MintInstant or placement methods.
	for _, k := range kinds() {
		for _, banned := range []string{"MintInstant", "Placement", "Shard", "Owner", "SnowflakeID"} {
			if _, ok := k.typ.MethodByName(banned); ok {
				t.Fatalf("%s must not expose %s", k.name, banned)
			}
		}
	}
}

func TestNewIDBoundaries(t *testing.T) {
	cases := []struct {
		name string
		in   uint64
		ok   bool
	}{
		{"zero refused", 0, false},
		{"one", 1, true},
		{"max int64", math.MaxInt64, true},
		{"max int64 plus one", math.MaxInt64 + 1, true},
		{"max uint64", math.MaxUint64, true},
	}
	for _, k := range kinds() {
		for _, c := range cases {
			got, err := k.newID(c.in)
			switch {
			case c.ok && (err != nil || got != c.in):
				t.Errorf("%s New(%s)=%d,%v want %d,nil", k.name, c.name, got, err, c.in)
			case !c.ok && (!errors.Is(err, types.ErrInvalidID) || got != 0):
				t.Errorf("%s New(%s)=%d,%v want 0,ErrInvalidID", k.name, c.name, got, err)
			}
		}
	}
}

func TestIsZero(t *testing.T) {
	for _, k := range kinds() {
		if !k.isZero(0) || k.isZero(1) || k.isZero(math.MaxUint64) {
			t.Errorf("%s IsZero wrong", k.name)
		}
	}
}

func TestParseFormatRoundTrip(t *testing.T) {
	vals := []uint64{1, 2, 9, 10, 1 << 31, 1<<32 + 5, math.MaxInt64, math.MaxInt64 + 1, math.MaxUint64}
	for _, k := range kinds() {
		for _, v := range vals {
			s := k.str(v)
			if s != strconv.FormatUint(v, 10) {
				t.Errorf("%s String(%d)=%q want canonical decimal", k.name, v, s)
			}
			got, err := k.parse(s)
			if err != nil || got != v {
				t.Errorf("%s Parse(String(%d))=%d,%v", k.name, v, got, err)
			}
			b, err := k.marsh(v)
			if err != nil || string(b) != s {
				t.Errorf("%s MarshalText(%d)=%q,%v want %q", k.name, v, b, err, s)
			}
			back, err := k.unmar(0, b)
			if err != nil || back != v {
				t.Errorf("%s UnmarshalText(%q)=%d,%v", k.name, b, back, err)
			}
		}
	}
}

func TestParseInvalid(t *testing.T) {
	bad := []string{
		"", " ", "0", "00", "-1", "-0", "+1", " 1", "1 ", "1\n", "0x10", "1_000", "1e3", "1.0", "١٢٣", // non-ASCII digits
		"01",                   // leading zero is not canonical
		"18446744073709551616", // MaxUint64 + 1
		"99999999999999999999999999",
		"abc", "1a", "NaN", "null", strings.Repeat("9", 4096),
	}
	for _, k := range kinds() {
		for _, s := range bad {
			got, err := k.parse(s)
			if !errors.Is(err, types.ErrInvalidID) || got != 0 {
				t.Errorf("%s Parse(%.24q)=%d,%v want 0,ErrInvalidID", k.name, s, got, err)
			}
			prev := uint64(42)
			after, err := k.unmar(prev, []byte(s))
			if !errors.Is(err, types.ErrInvalidID) || after != prev {
				t.Errorf("%s UnmarshalText(%.24q) err=%v after=%d, want ErrInvalidID and receiver unchanged", k.name, s, err, after)
			}
		}
	}
}

func TestMarshalZeroRefused(t *testing.T) {
	for _, k := range kinds() {
		b, err := k.marsh(0)
		if !errors.Is(err, types.ErrInvalidID) || len(b) != 0 {
			t.Errorf("%s MarshalText(0)=%q,%v want empty,ErrInvalidID", k.name, b, err)
		}
	}
}

func TestStringOfZeroIsNotAValidID(t *testing.T) {
	// The zero value prints as "0" for diagnostics and never parses back.
	for _, k := range kinds() {
		if k.str(0) != "0" {
			t.Errorf("%s String(0)=%q", k.name, k.str(0))
		}
		if _, err := k.parse(k.str(0)); !errors.Is(err, types.ErrInvalidID) {
			t.Errorf("%s zero string parsed: %v", k.name, err)
		}
	}
}

func TestIDsAreComparableMapKeys(t *testing.T) {
	m := map[types.NodeID]types.RelID{1: 2, math.MaxUint64: 3}
	if m[1] != 2 || m[math.MaxUint64] != 3 || len(m) != 2 {
		t.Fatal("IDs must be usable as map keys")
	}
}
