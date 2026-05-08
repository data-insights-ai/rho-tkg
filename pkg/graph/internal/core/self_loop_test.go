package core

import (
	"errors"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestSelfLoop_ZeroValueRejects(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{}) // AllowSelfLoops zero value = false
	defer g.Close()       //nolint:errcheck
	nA, _ := g.Nodes.Add([]string{"A"}, nil)

	_, err := g.Rels.Add("SELF", nA, nA, nil)
	if !errors.Is(err, ErrSelfLoop) {
		t.Fatalf("AddRelationship self-loop with zero Config: got %v, want ErrSelfLoop", err)
	}
}

func TestSelfLoop_ExplicitFalseRejects(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Validation: ValidationLimits{AllowSelfLoops: false}})
	defer g.Close() //nolint:errcheck
	nA, _ := g.Nodes.Add([]string{"A"}, nil)

	_, err := g.Rels.Add("SELF", nA, nA, nil)
	if !errors.Is(err, ErrSelfLoop) {
		t.Fatalf("AddRelationship self-loop with AllowSelfLoops=false: got %v, want ErrSelfLoop", err)
	}
}

func TestSelfLoop_AllowedByConfig(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Validation: ValidationLimits{AllowSelfLoops: true}})
	defer g.Close() //nolint:errcheck
	nA, _ := g.Nodes.Add([]string{"A"}, nil)

	r, err := g.Rels.Add("SELF", nA, nA, nil)
	if err != nil {
		t.Fatalf("AddRelationship self-loop with AllowSelfLoops=true: %v", err)
	}
	if r == nil {
		t.Fatal("AddRelationship returned nil relationship")
	}
}

func TestSelfLoop_ImportRejected(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{}) // AllowSelfLoops = false
	defer g.Close()       //nolint:errcheck
	nA, _ := g.Nodes.Add([]string{"A"}, nil)

	// Use a synthetic rel ID that won't collide.
	relID := types.RelID(nA.ID().SnowflakeID() + 1)
	_, err := g.Rels.Import(
		t.Context(), relID, "SELF", nA, nA, nil,
	)
	if !errors.Is(err, ErrSelfLoop) {
		t.Fatalf("ImportRelationshipWithID self-loop: got %v, want ErrSelfLoop", err)
	}
}

func TestSelfLoop_DifferentNodesStillWork(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{}) // AllowSelfLoops = false
	defer g.Close()       //nolint:errcheck
	nA, _ := g.Nodes.Add([]string{"A"}, nil)
	nB, _ := g.Nodes.Add([]string{"B"}, nil)

	r, err := g.Rels.Add("EDGE", nA, nB, nil)
	if err != nil {
		t.Fatalf("AddRelationship different nodes with AllowSelfLoops=false: %v", err)
	}
	if r == nil {
		t.Fatal("AddRelationship returned nil relationship")
	}
}
