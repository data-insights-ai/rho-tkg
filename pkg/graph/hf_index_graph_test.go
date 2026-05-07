package graph

import (
	"testing"
	"time"
)

// Integration tests for the Graph-level high-frequency index API.
// These tests exercise CreateHighFrequencyIndex / DropHighFrequencyIndex
// against the public Graph surface; the unit-level coverage of the
// underlying index lives in pkg/graph/internal/index/hf_index_test.go.

func TestCreateHighFrequencyIndex_Graph(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Register a label first
	_, err = g.AddNode([]string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// Create HFI for the label
	err = g.CreateHighFrequencyIndex("Event", time.Hour)
	if err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}

	// Drop it
	err = g.DropHighFrequencyIndex("Event")
	if err != nil {
		t.Fatalf("DropHighFrequencyIndex: %v", err)
	}
}

func TestHFIndex_ReplacesTemporalIndex(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Create a temporal index first
	_, err = g.AddNode([]string{"Widget"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	err = g.CreateTemporalIndex("Widget")
	if err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	// Replace with HFI — should either succeed or return ErrTemporalIndexExists
	// The spec says only one type can be set at a time.
	// Drop first, then create HFI.
	err = g.DropTemporalIndex("Widget")
	if err != nil {
		t.Fatalf("DropTemporalIndex: %v", err)
	}

	err = g.CreateHighFrequencyIndex("Widget", time.Hour)
	if err != nil {
		t.Fatalf("CreateHighFrequencyIndex after drop: %v", err)
	}
}

func TestHFIndex_DuplicateCreate(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	_, err = g.AddNode([]string{"Alpha"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	err = g.CreateHighFrequencyIndex("Alpha", time.Hour)
	if err != nil {
		t.Fatalf("first CreateHighFrequencyIndex: %v", err)
	}

	// Second create must return ErrTemporalIndexExists
	err = g.CreateHighFrequencyIndex("Alpha", time.Hour)
	if err == nil {
		t.Fatal("expected ErrTemporalIndexExists on duplicate create, got nil")
	}
	if err != ErrTemporalIndexExists {
		t.Errorf("expected ErrTemporalIndexExists, got %v", err)
	}
}

func TestHFIndex_DropNotFound(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	_, err = g.AddNode([]string{"Beta"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// Drop when no HFI exists — should return ErrTemporalIndexNotFound
	err = g.DropHighFrequencyIndex("Beta")
	if err == nil {
		t.Fatal("expected ErrTemporalIndexNotFound on drop of non-existent index, got nil")
	}
	if err != ErrTemporalIndexNotFound {
		t.Errorf("expected ErrTemporalIndexNotFound, got %v", err)
	}
}

func TestHFIndex_ConflictsWithTemporalIndex(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	_, err = g.AddNode([]string{"Gamma"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// Create temporal index first
	err = g.CreateTemporalIndex("Gamma")
	if err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	// Attempt to create HFI while temporal index exists — must fail
	err = g.CreateHighFrequencyIndex("Gamma", time.Hour)
	if err == nil {
		t.Fatal("expected ErrTemporalIndexExists when temporal index already exists, got nil")
	}
	if err != ErrTemporalIndexExists {
		t.Errorf("expected ErrTemporalIndexExists, got %v", err)
	}
}

func TestHFIndex_UnknownLabel(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Create/Drop on never-registered label — should return nil (not found is OK per spec)
	err = g.CreateHighFrequencyIndex("NoSuchLabel", time.Hour)
	if err != nil {
		t.Errorf("CreateHighFrequencyIndex on unknown label should return nil, got %v", err)
	}

	err = g.DropHighFrequencyIndex("NoSuchLabel")
	if err != nil {
		t.Errorf("DropHighFrequencyIndex on unknown label should return nil, got %v", err)
	}
}
