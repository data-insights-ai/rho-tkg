package graphstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
	"unsafe"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestFullRepresentationSizes(t *testing.T) {
	t.Log("EntityRead", unsafe.Sizeof(graphstate.EntityRead{}), "LifeRead", unsafe.Sizeof(graphstate.LifeRead{}), "PropertyRead", unsafe.Sizeof(graphstate.PropertyRead{}), "ValueRead", unsafe.Sizeof(graphstate.ValueRead{}), "Scalar", unsafe.Sizeof(graphstate.Scalar{}), "ComponentPage", unsafe.Sizeof(graphstate.ComponentPage{}), "ClaimPage", unsafe.Sizeof(graphstate.ClaimPage{}), "KeyPage", unsafe.Sizeof(graphstate.KeyPage{}), "EntityPage", unsafe.Sizeof(graphstate.EntityPage{}), "Projection", unsafe.Sizeof(graphstate.Projection{}), "PropertyValue", unsafe.Sizeof(graphstate.PropertyValue{}))
	t.Log("ReadView", unsafe.Sizeof(ReadView{}), "PageReader", unsafe.Sizeof(PageReader{}), "Descriptor", unsafe.Sizeof(fullIndexDescriptor{}), "Cursor", unsafe.Sizeof(fullContinuation{}), "PostingIterator", unsafe.Sizeof(postingIterator{}), "PostingKey", unsafe.Sizeof(postingKey{}), "PostingChild", unsafe.Sizeof(postingTreeChild{}), "PostingFrame", unsafe.Sizeof(postingFrame{}), "FullStage", unsafe.Sizeof(fullStageState{}), "Effects", unsafe.Sizeof(GraphEffects{}), "Delta", unsafe.Sizeof(graphstate.Delta{}), "Group", unsafe.Sizeof(ComponentChangeGroup{}), "Dependency", unsafe.Sizeof(graphstate.Dependency{}))
	t.Log("Scope", unsafe.Sizeof(temporal.Scope{}), "Axis", unsafe.Sizeof(temporal.Axis{}), "Bound", unsafe.Sizeof(temporal.Bound{}), "Position", unsafe.Sizeof(temporal.Position{}))
}

func TestFullFixedAllowancesCoverIndependentStructSizes(t *testing.T) {
	for _, test := range []struct {
		name    string
		actual  uintptr
		allowed int
	}{{"entity", unsafe.Sizeof(graphstate.EntityRead{}), fullEntityOutputBytes}, {"life", unsafe.Sizeof(graphstate.LifeRead{}), fullLifeOutputBytes}, {"property", unsafe.Sizeof(graphstate.PropertyRead{}), fullPropertyOutputBytes}, {"value", unsafe.Sizeof(graphstate.ValueRead{}), fullValueOutputBytes}, {"component", unsafe.Sizeof(graphstate.ComponentPage{}), fullComponentOutputBytes}, {"claim", unsafe.Sizeof(graphstate.ClaimPage{}), fullPageOutputBytes}, {"keys", unsafe.Sizeof(graphstate.KeyPage{}), fullPageOutputBytes}, {"incident", unsafe.Sizeof(graphstate.EntityPage{}), fullPageOutputBytes}, {"cursor", unsafe.Sizeof(fullContinuation{}), 256}, {"iterator", unsafe.Sizeof(postingIterator{}), 1024}, {"full-stage", unsafe.Sizeof(fullStageState{}), fullStageMetadataBytes}, {"operation", unsafe.Sizeof(graphstate.Operation{}), 640}, {"dependency", unsafe.Sizeof(graphstate.Dependency{}), fullDependencyOutputBytes}, {"patch", unsafe.Sizeof(graphstate.ComponentPatch{}), fullPatchOutputBytes}, {"group", unsafe.Sizeof(ComponentChangeGroup{}), fullGroupOutputBytes}, {"effects", unsafe.Sizeof(GraphEffects{}), 512}, {"interval", 2 * unsafe.Sizeof(temporal.Bound{}), scopeIntervalOwnedBytes}, {"posting-child", unsafe.Sizeof(postingTreeChild{}), 320}} {
		if test.actual > uintptr(test.allowed) {
			t.Fatal("fixed representation undercount", test)
		}
	}
	if unsafe.Sizeof(ReadView{})+unsafe.Sizeof(PageReader{})+unsafe.Sizeof(fullIndexDescriptor{})+128 > fullViewMetadataBytes {
		t.Fatal("fixed complete handle allowance undercounts independent objects")
	}
}

func TestFullOperationScopeOwnershipPreflightUsesMaterializedBacking(t *testing.T) {
	f := newFullFixture(t, GraphLimits{})
	var points []temporal.Scope
	for i := range 32 {
		p, _ := temporal.IntegerPosition(f.axis, temporal.Int64(int64(2*i)))
		scope, _ := temporal.Point(p)
		points = append(points, scope)
	}
	region, err := temporal.Region(f.axis, points, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	wideAxis := testAxis(t, 2, temporal.ProfileRationalQ)
	n, err := temporal.ParseInteger("340282366920938463463374607431768211457", temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := temporal.Fraction(n, temporal.Int64(3), temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	position, err := temporal.RationalPosition(wideAxis, r)
	if err != nil {
		t.Fatal(err)
	}
	wide, err := temporal.Point(position)
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range []temporal.Scope{region, wide} {
		wire, err := temporal.AppendScope(nil, scope, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		backing, err := scopeOwnedBacking(wire)
		if err != nil {
			t.Fatal(err)
		}
		if backing < len(wire) {
			t.Fatal("owned scope reduced to wire bytes")
		}
		for _, scalar := range []bool{false, true} {
			c := f.catalog(t, f.index)
			v, err := OpenReadView(t.Context(), c, GraphLimits{})
			if err != nil {
				t.Fatal(err)
			}
			op := graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope}
			if scalar {
				op.Scope = f.span(t, 0, 10)
				op.Value = graphstate.ScopeValue(scope)
			}
			// Measure a complete successful clone first so a ScalarScope refusal
			// cannot accidentally stop while cloning the operation's own scope.
			before := v.Work().Bytes
			if _, err := cloneOperations(t.Context(), v, []graphstate.Operation{op}); err != nil {
				t.Fatal(err)
			}
			ownedCost := v.Work().Bytes - before
			if ownedCost < 640+backing {
				t.Fatal("materialized interval backing omitted", ownedCost, backing)
			}
			beforeWire := append([]byte{}, wire...)
			v.limits.MaxSourceBytes = v.Work().Bytes + ownedCost - 1
			if _, err := cloneOperations(t.Context(), v, []graphstate.Operation{op}); !errors.Is(err, ErrResourceLimit) {
				t.Fatal("one-short compact/wide owned scope admitted", scalar, err)
			}
			v.limits.MaxSourceBytes = v.Work().Bytes + ownedCost
			if _, err := cloneOperations(t.Context(), v, []graphstate.Operation{op}); err != nil {
				t.Fatal("exact compact/wide owned scope refused", scalar, err)
			}
			after, err := temporal.AppendScope(nil, scope, temporal.Limits{})
			if err != nil || !bytes.Equal(after, beforeWire) {
				t.Fatal("ownership refusal mutated input", err)
			}
			_ = v.Close()
			_ = c.view.Close()
		}
	}
}

func TestFullOwnedScopeLedgerRejectsUnknownFramingAndCountsSpecialShapes(t *testing.T) {
	for _, wire := range [][]byte{nil, make([]byte, 53), append([]byte{'T', 'S', 2}, make([]byte, 50)...)} {
		if _, err := scopeOwnedBacking(wire); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
	}
	f := newFullFixture(t, GraphLimits{})
	for _, scope := range []temporal.Scope{func() temporal.Scope { s, _ := temporal.Empty(f.axis); return s }(), func() temporal.Scope { s, _ := temporal.Unplaced(f.axis); return s }(), func() temporal.Scope { s, _ := temporal.All(f.axis); return s }()} {
		wire, err := temporal.AppendScope(nil, scope, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		owned, err := scopeOwnedBacking(wire)
		if err != nil || owned < 4*len(wire) {
			t.Fatal(owned, err)
		}
	}
	bad := make([]byte, 57)
	copy(bad, []byte{'T', 'S', 1})
	bad[52] = byte(temporal.ScopeRegion)
	binary.BigEndian.PutUint32(bad[53:57], 65537)
	if _, err := scopeOwnedBacking(bad); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	bad[52] = 99
	if _, err := scopeOwnedBacking(bad); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}
