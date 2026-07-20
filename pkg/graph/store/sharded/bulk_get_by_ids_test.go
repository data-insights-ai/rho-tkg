package sharded

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestGetNodesByIDsMultiShard covers BACKLOG 20l: GetNodesByIDs used to apply
// its per-shard buckets sequentially, unlike every other multi-shard read in
// this file (which already goes through the bounded worker pool via
// forEachShardErr — TestRunShardPoolBoundsConcurrency locks in the pool's
// concurrency/bound behavior directly). Rewiring the bucket loop through
// runShardPool changes the concurrency profile but must NOT change the
// result: this proves a request spanning FOUR shards (one holding two IDs)
// still returns the exact sorted union, and a missing ID anywhere in the
// request still fails closed with an errors.Is-detectable ErrNodeNotFound —
// the switch from "return on first sequential error" to "errors.Join every
// bucket's error" must not lose sentinel detectability.
func TestGetNodesByIDsMultiShard(t *testing.T) {
	t.Parallel()
	st := newMemStore(t, 0, 4)

	a := mkNodeID(0, 1)
	b := mkNodeID(1, 1)
	c := mkNodeID(2, 1)
	d := mkNodeID(2, 2) // shares a shard with c
	e := mkNodeID(3, 1)
	for _, id := range []types.NodeID{a, b, c, d, e} {
		putNode(t, st, id, 7)
	}

	got, err := st.GetNodesByIDs([]types.NodeID{e, c, a, d, b})
	if err != nil {
		t.Fatalf("GetNodesByIDs: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("got %d nodes, want 5", len(got))
	}
	// Snowflake ID ordering is n-dominant (n is shifted left of slot), so the
	// ascending-ID order is a < b < c < e < d, not input-append order.
	wantOrder := []types.NodeID{a, b, c, e, d}
	for i, n := range got {
		if n.ID() != wantOrder[i] {
			t.Errorf("got[%d] = %d, want %d (result must be sorted by ID across shards)", i, n.ID(), wantOrder[i])
		}
	}

	// A missing ID anywhere in a multi-shard request must fail the whole call,
	// detectable via errors.Is despite now being routed through errors.Join.
	missing := mkNodeID(1, 99)
	_, err = st.GetNodesByIDs([]types.NodeID{a, missing})
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNodesByIDs with a missing ID = %v, want errors.Is-detectable ErrNodeNotFound", err)
	}
}

// TestGetRelationshipsByIDsMultiShard mirrors TestGetNodesByIDsMultiShard for
// GetRelationshipsByIDs (BACKLOG 20l).
func TestGetRelationshipsByIDsMultiShard(t *testing.T) {
	t.Parallel()
	st := newMemStore(t, 0, 4)

	hub := mkNodeID(0, 1)
	nbr := mkNodeID(0, 2)
	putNode(t, st, hub, 7)
	putNode(t, st, nbr, 7)

	r0 := putRel(t, st, mkRelID(0, 1), 5, hub, nbr)
	r1 := putRel(t, st, mkRelID(1, 1), 5, hub, nbr)
	r2a := putRel(t, st, mkRelID(2, 1), 5, hub, nbr)
	r2b := putRel(t, st, mkRelID(2, 2), 5, hub, nbr) // shares a shard with r2a
	r3 := putRel(t, st, mkRelID(3, 1), 5, hub, nbr)

	got, err := st.GetRelationshipsByIDs([]types.RelID{r3.ID(), r2a.ID(), r0.ID(), r2b.ID(), r1.ID()})
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("got %d relationships, want 5", len(got))
	}
	// Snowflake ID ordering is n-dominant, so the ascending-ID order is
	// r0 < r1 < r2a < r3 < r2b (r2b's n=2 outweighs r3's higher slot).
	wantOrder := []types.RelID{r0.ID(), r1.ID(), r2a.ID(), r3.ID(), r2b.ID()}
	for i, r := range got {
		if r.ID() != wantOrder[i] {
			t.Errorf("got[%d] = %d, want %d (result must be sorted by ID across shards)", i, r.ID(), wantOrder[i])
		}
	}

	missing := mkRelID(1, 99)
	_, err = st.GetRelationshipsByIDs([]types.RelID{r0.ID(), missing})
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationshipsByIDs with a missing ID = %v, want errors.Is-detectable ErrRelNotFound", err)
	}
}
