package core

import (
	"errors"
	"strings"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

func TestResolveLabelTokenRejectsInvalidNames(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Validation: ValidationLimits{MaxNameLength: 5}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	for _, tc := range []struct {
		name string
		want error
	}{
		{name: "", want: ErrEmptyName},
		{name: "  ", want: ErrEmptyName},
		{name: "TooLong", want: ErrNameTooLong},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if _, err := g.Resolve.GetOrCreateLabel(tc.name); !errors.Is(err, tc.want) {
				t.Fatalf("GetOrCreateLabel(%q) = %v, want %v", tc.name, err, tc.want)
			}
			if tok, ok := g.Resolve.LookupLabel(tc.name); ok || tok != 0 {
				t.Fatalf("LookupLabel(%q) = %d, %v; want zero, false", tc.name, tok, ok)
			}
		})
	}
}

func TestResolveRelTypeTokenRejectsInvalidNames(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Validation: ValidationLimits{MaxNameLength: 5}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	for _, tc := range []struct {
		name string
		want error
	}{
		{name: "", want: ErrEmptyName},
		{name: "  ", want: ErrEmptyName},
		{name: "TooLong", want: ErrNameTooLong},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if _, err := g.Resolve.GetOrCreateRelType(tc.name); !errors.Is(err, tc.want) {
				t.Fatalf("GetOrCreateRelType(%q) = %v, want %v", tc.name, err, tc.want)
			}
			if tok, ok := g.Resolve.LookupRelType(tc.name); ok || tok != 0 {
				t.Fatalf("LookupRelType(%q) = %d, %v; want zero, false", tc.name, tok, ok)
			}
		})
	}
}

func TestResolveBooleanHelpersRejectMalformedNames(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Validation: ValidationLimits{MaxNameLength: 5}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	labelTok, err := g.Resolve.GetOrCreateLabel("Short")
	if err != nil {
		t.Fatalf("GetOrCreateLabel: %v", err)
	}
	relTok, err := g.Resolve.GetOrCreateRelType("Rel")
	if err != nil {
		t.Fatalf("GetOrCreateRelType: %v", err)
	}

	node := types.NewNode(types.NodeID(snowflake.ID(1)), labelTok, nil)
	rel := types.NewRelationship(types.RelID(snowflake.ID(2)), relTok, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(3)))

	if !g.Nodes.HasLabel(node, "Short") {
		t.Fatal("HasLabel(Short) = false, want true")
	}
	if !g.Rels.HasType(rel, "Rel") {
		t.Fatal("HasType(Rel) = false, want true")
	}

	for _, name := range []string{"", "  ", strings.Repeat("x", 6)} {
		if g.Nodes.HasLabel(node, name) {
			t.Fatalf("HasLabel(%q) = true, want false", name)
		}
		if g.Rels.HasType(rel, name) {
			t.Fatalf("HasType(%q) = true, want false", name)
		}
	}
}
