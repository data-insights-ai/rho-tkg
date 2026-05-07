// Internal-package tests for BadgerStore implementations of
// RemoveNodeLabelToken, CreateVectorIndex, DropVectorIndex, SearchNearestNodes.
package badgerstore

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- BadgerStore: RemoveNodeLabelToken ---

func TestBadgerStore_RemoveNodeLabelToken_Basic(t *testing.T) {
	bs := newTestBadgerStore(t)

	primary := uint16(1)
	extra := uint16(2)
	n := types.NewNode(types.NodeID(snowflake.ID(100)), primary, []uint16{extra})
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	id := n.ID()

	// Simulate RemoveLabelTokenRaw on a copy.
	copy := n.DeepCopy()
	copy.RemoveLabelTokenRaw(extra)

	if err := bs.RemoveNodeLabelToken(id, extra, copy); err != nil {
		t.Fatalf("RemoveNodeLabelToken: %v", err)
	}

	// Verify label index no longer contains id under extra token.
	set, hasSet := bs.LabelIndexForTest(extra)
	if hasSet {
		for _, sid := range set {
			if sid == id {
				t.Error("node still in label index after RemoveNodeLabelToken")
			}
		}
	}
}

func TestBadgerStore_RemoveNodeLabelToken_NotFound(t *testing.T) {
	bs := newTestBadgerStore(t)

	copy := types.NewNode(types.NodeID(snowflake.ID(999)), 1, nil)
	err := bs.RemoveNodeLabelToken(types.NodeID(999), 1, copy)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}
}

// --- BadgerStore: CreateVectorIndex / DropVectorIndex / SearchNearestNodes ---

func TestBadgerStore_VectorIndex_CreateAndSearch(t *testing.T) {
	bs := newTestBadgerStore(t)
	labelTok := uint16(3)
	key := "vec"

	// Put two nodes with float32 vector properties.
	n1 := types.NewNode(types.NodeID(snowflake.ID(101)), labelTok, nil)
	ps1, _ := types.NewPropertySlice(map[string]any{key: []float32{1, 0, 0}})
	n1.SetProperties(ps1)
	bs.PutNode(n1)

	n2 := types.NewNode(types.NodeID(snowflake.ID(102)), labelTok, nil)
	ps2, _ := types.NewPropertySlice(map[string]any{key: []float32{0, 1, 0}})
	n2.SetProperties(ps2)
	bs.PutNode(n2)

	if err := bs.CreateVectorIndex(labelTok, key, 3, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := bs.SearchNearestNodes(labelTok, key, []float32{1, 0, 0}, 2, QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected results, got none")
	}
	if results[0].ID() != n1.ID() {
		t.Error("expected n1 as closest to [1,0,0]")
	}
}

func TestBadgerStore_VectorIndex_AlreadyExists(t *testing.T) {
	bs := newTestBadgerStore(t)
	bs.CreateVectorIndex(1, "v", 2, DistanceCosine)
	err := bs.CreateVectorIndex(1, "v", 2, DistanceCosine)
	if !errors.Is(err, ErrVectorIndexExists) {
		t.Errorf("expected ErrVectorIndexExists, got %v", err)
	}
}

func TestBadgerStore_VectorIndex_DropNotFound(t *testing.T) {
	bs := newTestBadgerStore(t)
	err := bs.DropVectorIndex(1, "missing")
	if !errors.Is(err, ErrVectorIndexNotFound) {
		t.Errorf("expected ErrVectorIndexNotFound, got %v", err)
	}
}

func TestBadgerStore_VectorIndex_Drop(t *testing.T) {
	bs := newTestBadgerStore(t)
	bs.CreateVectorIndex(1, "v", 2, DistanceCosine)
	if err := bs.DropVectorIndex(1, "v"); err != nil {
		t.Fatalf("DropVectorIndex: %v", err)
	}
	// After drop, SearchNearestNodes should return ErrVectorIndexNotFound.
	_, err := bs.SearchNearestNodes(1, "v", []float32{1, 0}, 1, QueryOpts{})
	if !errors.Is(err, ErrVectorIndexNotFound) {
		t.Errorf("expected ErrVectorIndexNotFound after drop, got %v", err)
	}
}

func TestBadgerStore_VectorIndex_SearchNotFound(t *testing.T) {
	bs := newTestBadgerStore(t)
	_, err := bs.SearchNearestNodes(1, "nonexistent", []float32{1, 0}, 1, QueryOpts{})
	if !errors.Is(err, ErrVectorIndexNotFound) {
		t.Errorf("expected ErrVectorIndexNotFound, got %v", err)
	}
}

func TestBadgerStore_VectorIndex_DimensionMismatch(t *testing.T) {
	bs := newTestBadgerStore(t)
	bs.CreateVectorIndex(1, "v", 3, DistanceCosine)
	_, err := bs.SearchNearestNodes(1, "v", []float32{1, 0}, 1, QueryOpts{})
	if !errors.Is(err, ErrDimensionMismatch) {
		t.Errorf("expected ErrDimensionMismatch, got %v", err)
	}
}
