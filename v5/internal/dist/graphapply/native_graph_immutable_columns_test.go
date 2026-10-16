package graphapply

import (
	"errors"
	"testing"
)

// Authored only; these synthetic row guards are not actual publication/cache
// evidence. The separate six-process parent must produce the authentic joins.
func TestNativeImmutableColumnsFreshProfileAndExactProbePredecessor(t *testing.T) {
	if _, err := nativeImmutableColumnsRetireIdle(nil, nil, processRequest{}); !errors.Is(err, errInvalid) {
		t.Fatal("nil diagnostic authority", err)
	}
	if _, err := nativeImmutableColumnsRetireInput(nativeImmutableColumnsRetireQuestion{}); !errors.Is(err, errInvalid) {
		t.Fatal("missing genuine owner identity", err)
	}
	s := nativeSelectorsSpec{1, nativeImmutableColumnsFamily, 40, 50, 32, "columns"}
	p := nativeImmutableColumnsCost(s)
	if nativeImmutableColumnsValidate(p) != nil || nativeCreateUpdateValidate(p) == nil || nativeSelectorsValidate(p) == nil {
		t.Fatal("fresh column profile reused old authority")
	}
	p.Version = 9
	if !errors.Is(nativeImmutableColumnsValidate(p), errInvalid) {
		t.Fatal("old9 admitted as10")
	}
	p = nativeImmutableColumnsCost(s)
	probes, err := newNativeImmutableColumnsProbes(p)
	if err != nil {
		t.Fatal(err)
	}
	owner, life, target, targetLife := p.edge(0)
	row := nativeColumnRow{Owner: owner, Component: 1, Kind: 2, Mode: 1, Type: "edge", Source: 1, Target: target, SourceLife: 2, TargetLife: targetLife, Axis: []byte{1}, Scope: []byte{1}, After: nativeColumnCell{Present: true, Handle: life, Revision: 10, Provenance: 4}}
	if err := probes.accept(row); err != nil {
		t.Fatal(err)
	}
	property := row
	property.Component = 3
	property.Life = life
	property.Name = "p"
	property.After = nativeColumnCell{Present: true, Handle: 5, Value: 5, Revision: 10, Provenance: 4}
	if err := probes.accept(property); err != nil {
		t.Fatal(err)
	}
	wrong := property
	wrong.Before = property.After
	wrong.Before.Revision++
	wrong.After = nativeColumnCell{Present: true, Handle: 6, Value: 6, Revision: 20, Provenance: 44}
	if !errors.Is(probes.accept(wrong), errCorrupt) {
		t.Fatal("same-count wrong native before-image accepted")
	}
	wrong.Before = property.After
	if err := probes.accept(wrong); err != nil {
		t.Fatal(err)
	}
	if _, err := probes.complete(6); !errors.Is(err, errCorrupt) {
		t.Fatal("omitted second point/new presence or partial scan became complete", err)
	}
}
