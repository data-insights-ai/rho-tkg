package core

import (
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
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
	_, err = g.Nodes.Add([]string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// Create HFI for the label
	err = g.Index.CreateHighFrequency("Event", time.Hour)
	if err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}

	// Drop it
	err = g.Index.DropHighFrequency("Event")
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
	_, err = g.Nodes.Add([]string{"Widget"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	err = g.Index.CreateTemporal("Widget")
	if err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	// Replace with HFI — should either succeed or return storepkg.ErrTemporalIndexExists
	// The spec says only one type can be set at a time.
	// Drop first, then create HFI.
	err = g.Index.DropTemporal("Widget")
	if err != nil {
		t.Fatalf("DropTemporalIndex: %v", err)
	}

	err = g.Index.CreateHighFrequency("Widget", time.Hour)
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

	_, err = g.Nodes.Add([]string{"Alpha"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	err = g.Index.CreateHighFrequency("Alpha", time.Hour)
	if err != nil {
		t.Fatalf("first CreateHighFrequencyIndex: %v", err)
	}

	// Second create must return storepkg.ErrTemporalIndexExists
	err = g.Index.CreateHighFrequency("Alpha", time.Hour)
	if err == nil {
		t.Fatal("expected storepkg.ErrTemporalIndexExists on duplicate create, got nil")
	}
	if err != storepkg.ErrTemporalIndexExists {
		t.Errorf("expected storepkg.ErrTemporalIndexExists, got %v", err)
	}
}

func TestHFIndex_DropNotFound(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	_, err = g.Nodes.Add([]string{"Beta"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// Drop when no HFI exists — should return storepkg.ErrTemporalIndexNotFound
	err = g.Index.DropHighFrequency("Beta")
	if err == nil {
		t.Fatal("expected storepkg.ErrTemporalIndexNotFound on drop of non-existent index, got nil")
	}
	if err != storepkg.ErrTemporalIndexNotFound {
		t.Errorf("expected storepkg.ErrTemporalIndexNotFound, got %v", err)
	}
}

func TestHFIndex_ConflictsWithTemporalIndex(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	_, err = g.Nodes.Add([]string{"Gamma"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// Create temporal index first
	err = g.Index.CreateTemporal("Gamma")
	if err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	// Attempt to create HFI while temporal index exists — must fail
	err = g.Index.CreateHighFrequency("Gamma", time.Hour)
	if err == nil {
		t.Fatal("expected storepkg.ErrTemporalIndexExists when temporal index already exists, got nil")
	}
	if err != storepkg.ErrTemporalIndexExists {
		t.Errorf("expected storepkg.ErrTemporalIndexExists, got %v", err)
	}
}

func TestHFIndex_UnknownLabel(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Create/Drop on never-registered label — should return nil (not found is OK per spec)
	err = g.Index.CreateHighFrequency("NoSuchLabel", time.Hour)
	if err != nil {
		t.Errorf("CreateHighFrequencyIndex on unknown label should return nil, got %v", err)
	}

	err = g.Index.DropHighFrequency("NoSuchLabel")
	if err != nil {
		t.Errorf("DropHighFrequencyIndex on unknown label should return nil, got %v", err)
	}
}
