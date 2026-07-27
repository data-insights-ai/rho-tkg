package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var publicExactErasureBounds = adminpkg.ExactErasureBounds{
	MaxRelationshipIdentities: 32,
	MaxRelationshipVersions:   128,
	MaxEndpointNodeIdentities: 64,
}

func TestExactErasePublicContract(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{AllowExactErasure: true})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	erased, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"email": "erase@example.test"})
	if err != nil {
		t.Fatal(err)
	}
	survivor, err := g.Nodes().Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rel, err := g.Rels().AddByID(ctx, "KNOWS", erased.ID(), survivor.ID(), nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := g.Admin().ExactErase(ctx, adminpkg.ExactErasureRequest{
		NodeIDs: []types.NodeID{erased.ID()},
		Bounds:  publicExactErasureBounds,
	}); !errors.Is(err, graphpkg.ErrExactErasureRelationshipEscape) {
		t.Fatalf("incomplete scope = %v, want ErrExactErasureRelationshipEscape", err)
	}

	resolution, err := g.Admin().ResolveExactErasure(ctx, adminpkg.ExactErasureRequest{
		NodeIDs:         []types.NodeID{erased.ID(), erased.ID()},
		RelationshipIDs: []types.RelID{rel.ID()},
		Bounds:          publicExactErasureBounds,
	})
	if err != nil {
		t.Fatalf("ResolveExactErasure: %v", err)
	}
	if len(resolution.Request.NodeIDs) != 1 ||
		len(resolution.Request.RelationshipIDs) != 1 ||
		resolution.Request.RelationshipIDs[0] != rel.ID() ||
		len(resolution.EndpointNodeIDs) != 2 ||
		len(resolution.RelationshipBindings) != 1 ||
		resolution.RelationshipBindings[0].Type != "KNOWS" {
		t.Fatalf("resolved request = %+v", resolution)
	}
	first, err := g.Admin().ExactErase(ctx, resolution.Request)
	if err != nil {
		t.Fatalf("ExactErase: %v", err)
	}
	second, err := g.Admin().ExactErase(ctx, adminpkg.ExactErasureRequest{
		RelationshipIDs: []types.RelID{rel.ID()},
		NodeIDs:         []types.NodeID{erased.ID()},
		Bounds:          publicExactErasureBounds,
	})
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if first != second || first.Digest == "" || first.NodeCount != 1 || first.RelationshipCount != 1 {
		t.Fatalf("receipts first=%+v second=%+v", first, second)
	}
	if _, err := g.Nodes().Get(ctx, erased.ID()); !errors.Is(err, graphpkg.ErrNodeNotFound) {
		t.Fatalf("erased node = %v", err)
	}
	if _, err := g.Nodes().Get(ctx, survivor.ID()); err != nil {
		t.Fatalf("survivor = %v", err)
	}
}

func TestExactErasePublicGates(t *testing.T) {
	ctx := context.Background()
	disabled, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer disabled.Close()
	if _, err := disabled.Admin().ExactErase(ctx, adminpkg.ExactErasureRequest{NodeIDs: []types.NodeID{1}}); !errors.Is(err, graphpkg.ErrExactErasureDisabled) {
		t.Fatalf("disabled = %v, want ErrExactErasureDisabled", err)
	}

	withLog, err := graphpkg.New(graphpkg.Config{
		Store:             memory.New(memory.WithChangeLog()),
		AllowExactErasure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer withLog.Close()
	if _, err := withLog.Admin().ExactErase(ctx, adminpkg.ExactErasureRequest{
		NodeIDs: []types.NodeID{1},
		Bounds:  publicExactErasureBounds,
	}); !errors.Is(err, graphpkg.ErrExactErasureChangeLogRetained) {
		t.Fatalf("change-log = %v, want ErrExactErasureChangeLogRetained", err)
	}
}
