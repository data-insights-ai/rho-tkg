package graph

import (
	"errors"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// Each test below has exactly one defect, so only the check named in its
// comment can refuse it (review S2 survivors m8, m9, m13, m16, m17, m22, m27,
// m30, m34: an earlier check used to mask the one under test).

func TestGapCursorValidateOversize(t *testing.T) { // m8
	if err := (Cursor{b: make([]byte, MaxCursorBytes+1)}).Validate(); !errors.Is(err, ErrInvalidCursor) {
		t.Fatal(err)
	}
	if err := (Cursor{b: make([]byte, MaxCursorBytes)}).Validate(); err != nil {
		t.Fatal("ceiling itself is allowed", err)
	}
}

func TestGapLookupCeilingWithValidIDs(t *testing.T) { // m9
	ids := make([]types.NodeID, MaxLookupIDs+1)
	for i := range ids {
		ids[i] = types.NodeID(i + 1)
	}
	r := LookupRequest{Selector: goodSel(t), Nodes: ids, Paging: PageLimits{Rows: 1, MaxBytes: 1}}
	if err := r.Validate(temporal.Limits{}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
	r.Nodes = ids[:MaxLookupIDs]
	if err := r.Validate(temporal.Limits{}); err != nil {
		t.Fatal("ceiling itself is allowed", err)
	}
}

func TestGapWriteScopeAxisIsValidatedAgainstLimits(t *testing.T) { // m13
	// valid under default limits, over the descriptor budget of l
	tight := temporal.Limits{MaxDescriptorBytes: 8}
	if err := tight.Validate(); err != nil {
		t.Fatal(err)
	}
	op := CreateNode{Scope: scopeAll(t)}
	if err := op.Validate(temporal.Limits{}); err != nil {
		t.Fatal(err)
	}
	if err := op.Validate(tight); !errors.Is(err, ErrInvalidScope) {
		t.Fatal(err)
	}
}

func TestGapCreateNodeEmptyPropertyKey(t *testing.T) { // m16
	op := CreateNode{Labels: []string{"A"}, Props: map[string]Value{"": Int(1)}, Scope: scopeAll(t)}
	if err := op.Validate(temporal.Limits{}); !errors.Is(err, ErrInvalidOp) {
		t.Fatal(err)
	}
}

func TestGapGetOrCreateEmptyLabel(t *testing.T) { // m17
	op := GetOrCreate{Key: "k", Value: String("v"), Scope: scopeAll(t)}
	if err := op.Validate(temporal.Limits{}); !errors.Is(err, ErrInvalidOp) {
		t.Fatal(err)
	}
}

func TestGapNegativeRelID(t *testing.T) { // m22
	if _, err := RelRef(-1); !errors.Is(err, ErrInvalidID) {
		t.Fatal(err)
	}
	if err := (Ref{kind: refRel, rel: -1}).Validate(); !errors.Is(err, ErrInvalidID) {
		t.Fatal(err)
	}
	r := LookupRequest{Selector: goodSel(t), Rels: []types.RelID{-1}, Paging: PageLimits{Rows: 1, MaxBytes: 1}}
	if err := r.Validate(temporal.Limits{}); !errors.Is(err, ErrInvalidID) {
		t.Fatal(err)
	}
}

func TestGapUnsetEmptyKey(t *testing.T) { // m27
	n5, _ := NodeRef(5)
	if err := (Unset{Target: n5, Scope: scopeAll(t)}).Validate(temporal.Limits{}); !errors.Is(err, ErrInvalidOp) {
		t.Fatal(err)
	}
}

// The default-axis check also refuses these configs, so the cause is asserted:
// the config's own check must name the defect.
func TestGapConfigLimitsNamedByOwnCheck(t *testing.T) { // m30
	c := goodConfig(t)
	c.Limits = temporal.Limits{MaxInputBytes: -1}
	err := c.Validate()
	if !errors.Is(err, ErrInvalidConfig) || !strings.Contains(err.Error(), "limits") || strings.Contains(err.Error(), "default axis") {
		t.Fatal(err)
	}
}

func TestGapConfigZeroGraphNamedByOwnCheck(t *testing.T) { // m34
	c := goodConfig(t)
	c.GraphID = types.GraphID{}
	err := c.Validate()
	if !errors.Is(err, ErrInvalidConfig) || !strings.Contains(err.Error(), "GraphID is zero") || strings.Contains(err.Error(), "default axis") {
		t.Fatal(err)
	}
}
