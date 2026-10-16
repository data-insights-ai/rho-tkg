package graph

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// TestBytesAndListPropertiesSurviveEngineReopen: ai-soc shaped bytes and
// sigma shaped []int64 properties are declared by first use, stored, and read
// back identical after a reopen.
func TestBytesAndListPropertiesSurviveEngineReopen(t *testing.T) {
	ctx := context.Background()
	cfg := bareConfig(t, t.TempDir())
	g, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	wire := Bytes([]byte{0, 0xff, 0, 1})
	support := mustVList(t, Int(3), Int(-7), Int(0))
	all := scopeAll(t)
	var id types.NodeID
	update(t, g, func(tx Tx) (err error) {
		id, err = tx.CreateNode(CreateNode{Props: map[string]Value{"fact_wire": wire, "ov_support": support, "empty": Bytes(nil)}, Scope: all})
		return err
	})
	_ = g.Close()
	if g, err = Open(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	p := lookupProps(t, g, id, "fact_wire", "ov_support", "empty")
	if b, ok := p["fact_wire"].AsBytes(); !ok || string(b) != "\x00\xff\x00\x01" {
		t.Fatalf("fact_wire %v", p["fact_wire"])
	}
	if !p["ov_support"].Equal(support) {
		t.Fatalf("ov_support %v", p["ov_support"])
	}
	if b, ok := p["empty"].AsBytes(); !ok || len(b) != 0 {
		t.Fatalf("empty %v", p["empty"])
	}
}

// Equality scans find Bytes bytewise and List elementwise by the numeric rule
// (a list property is looked up by its EqualityKey; [3,-7,0] finds [3.0,-7,0]).
func TestBytesAndListEqualityScan(t *testing.T) {
	cfg := bareConfig(t, t.TempDir())
	g, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	all := scopeAll(t)
	var a, b types.NodeID
	update(t, g, func(tx Tx) (err error) {
		if a, err = tx.CreateNode(CreateNode{Props: map[string]Value{"w": Bytes([]byte{1, 2}), "l": mustVList(t, Int(3), Int(-7))}, Scope: all}); err != nil {
			return
		}
		b, err = tx.CreateNode(CreateNode{Props: map[string]Value{"w": Bytes([]byte{2, 1}), "l": mustVList(t, Int(-7), Int(3))}, Scope: all})
		return err
	})
	sel := func(k string, v Value) Selector {
		return Selector{View: ViewDeclared, Valid: all, Known: knownAll(t), Equals: []PropertyEq{{Key: k, Value: v}}}
	}
	for _, c := range []struct {
		name string
		s    Selector
		want []string
	}{
		{"bytes", sel("w", Bytes([]byte{1, 2})), []string{n(a)}},
		{"bytes reversed", sel("w", Bytes([]byte{2, 1})), []string{n(b)}},
		{"bytes prefix", sel("w", Bytes([]byte{1})), nil},
		{"bytes as string", sel("w", String("\x01\x02")), nil},
		{"list float element", sel("l", mustVList(t, mustFloat(t, 3), Int(-7))), []string{n(a)}},
		{"list order", sel("l", mustVList(t, Int(-7), Int(3))), []string{n(b)}},
		{"list shorter", sel("l", mustVList(t, Int(3))), nil},
	} {
		got, err := scanAll(t, g, c.s, 10)
		if err != nil || !slices.Equal(got.ids, c.want) {
			t.Errorf("%s: got %v err %v want %v", c.name, got.ids, err, c.want)
		}
	}
}

// A unique List claim follows the one equality rule (Value.Equal): GetOrCreate
// of [3.0,4] returns the holder of [3,4], a second node with [3.0,4] under the
// unique key is ErrUniqueViolation, and [3,5], [4,3] and [] stay separate claims.
func TestUniqueListClaimUsesEquality(t *testing.T) {
	cfg := bareConfig(t, t.TempDir())
	cfg.NodeProperties = []PropertySchema{{Key: "l", Kind: KindList, Unique: true}}
	g, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	all := scopeAll(t)
	f3 := mustFloat(t, 3)
	get := func(v Value) (id types.NodeID, created bool) {
		t.Helper()
		update(t, g, func(tx Tx) (err error) {
			id, created, err = tx.GetOrCreate(GetOrCreate{Label: "L", Key: "l", Value: v, Scope: all})
			return err
		})
		return
	}
	a, created := get(mustVList(t, Int(3), Int(4)))
	if !created {
		t.Fatal("first claim not created")
	}
	for _, v := range []Value{mustVList(t, Int(3), Int(4)), mustVList(t, f3, Int(4)), mustVList(t, f3, mustFloat(t, 4))} {
		if id, created := get(v); created || id != a {
			t.Errorf("%v: id %v created %t, want holder %v", v, id, created, a)
		}
	}
	_, err = g.Update(context.Background(), func(tx Tx) error {
		_, err := tx.CreateNode(CreateNode{Labels: []string{"L"}, Props: map[string]Value{"l": mustVList(t, f3, Int(4))}, Scope: all})
		return err
	})
	if !errors.Is(err, ErrUniqueViolation) {
		t.Errorf("[3.0,4] next to [3,4]: %v", err)
	}
	for _, v := range []Value{mustVList(t, Int(3), Int(5)), mustVList(t, Int(4), Int(3)), mustVList(t), mustVList(t, mustFloat(t, 3.5), Int(4))} {
		if id, created := get(v); !created || id == a {
			t.Errorf("%v: id %v created %t, want a new claim", v, id, created)
		}
	}
}
