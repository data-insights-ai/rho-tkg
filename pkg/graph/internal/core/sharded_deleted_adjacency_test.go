package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestShardedDeletedIterationCapabilityWired proves the AFTER state of BACKLOG
// 21d/20f: a sharded-backed Core now wires c.deletedIter (non-nil), where before
// this change it was always nil for *sharded.Store (the type was absent from
// nativeStoreTypes/isExactNativeStore AND, before stats_iter.go grew
// ForEachDeletedNodeID/ForEachDeletedRelID, did not even satisfy
// storepkg.DeletedIterationCapability). This is a WHITE-BOX check
// (deliberately, not a black-box query-result assertion): the deleted-rel fold's
// fallback path (forEachRelHistoryIDByDepthTrusted, a full history scan) is
// ALSO correct, just O(total history) instead of O(deleted only) — so a pure
// "the query returns the right relationships" assertion would pass identically
// whether or not the capability is wired, and would not distinguish the
// before/after states this WP is supposed to prove.
func TestShardedDeletedIterationCapabilityWired(t *testing.T) {
	t.Parallel()
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if g.deletedIter == nil {
		t.Fatal("c.deletedIter is nil for a sharded-backed Core — DeletedIterationCapability not wired (BACKLOG 21d regression)")
	}
}

// TestShardedDeletedIterationCapability_AdjacencyAtTFold is the downstream-
// consumer proof: OutgoingRelsAt/IncomingRelsAt correctly fold in a DELETED
// relationship on a sharded-backed graph — i.e. the capability is not just
// wired (see above) but actually load-bearing for the adjacency-at-t door that
// documents itself as using DeletedIterationCapability
// (temporal_queries.go:OutgoingRelsAt's doc comment).
//
// This package cannot exercise the "rel's own shard differs from its
// endpoints' shard" case with hand-crafted IDs (sharded's mkNodeID/mkRelID
// helpers are unexported to pkg/graph/store/sharded's own test files, and this
// package cannot import that internal test helper). It does not need to: with
// the default dual generator (Config.SnowflakeNodeID=0), nodes mint on the
// EVEN node-field slot and relationships mint on the ODD node-field slot
// (Design Rules > Data Model > "Dual generators") — so a rel's own ID already
// routes to a DIFFERENT shard than either endpoint's node ID under the
// standard two-shard sharded.Config{BaseSlot:0, SlotCount:2} topology used
// throughout this package's other sharded-backed tests (e.g.
// TestTemporalCandidatePruneEquivalence). The cross-shard case this WP cares
// about (a deleted rel's shard != its endpoints' shard) is therefore the
// DEFAULT case here, not a constructed edge case.
func TestShardedDeletedIterationCapability_AdjacencyAtTFold(t *testing.T) {
	t.Parallel()
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.deletedIter == nil {
		t.Fatal("c.deletedIter is nil for a sharded-backed Core — precondition for this test failed")
	}

	ctx := context.Background()
	// Explicit tkg_valid_from on the nodes so the query instants below are
	// evaluated against a known world-time window rather than the snowflake-ID
	// fallback (which would be "now", far later than any small test instant —
	// Design Rules > Temporal Queries > "Effective valid-from").
	nodesValidFrom := types.Instant(100)
	a, err := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "a", "tkg_valid_from": nodesValidFrom})
	if err != nil {
		t.Fatalf("Add(a): %v", err)
	}
	b, err := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "b", "tkg_valid_from": nodesValidFrom})
	if err != nil {
		t.Fatalf("Add(b): %v", err)
	}

	before := types.Instant(200)
	rel, err := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"tkg_valid_from": before})
	if err != nil {
		t.Fatalf("Rels.Add: %v", err)
	}
	// A rel's own ID mints on the odd node-field slot; a's/b's on the even slot
	// (Design Rules > Data Model > Dual generators) — confirm the cross-shard
	// premise this test relies on actually holds for this store/config.
	if rel.ID().SnowflakeID() == a.ID().SnowflakeID() || rel.ID().SnowflakeID() == b.ID().SnowflakeID() {
		t.Fatalf("rel ID unexpectedly collides with an endpoint ID: rel=%v a=%v b=%v", rel.ID(), a.ID(), b.ID())
	}

	if err := g.Rels.Delete(ctx, rel.ID()); err != nil {
		t.Fatalf("Rels.Delete: %v", err)
	}
	// Delete stamps DeletedAt/ValidTo from the real wall clock (c.now()), not
	// from the small synthetic instants used above — so "after" must be
	// strictly later than that real stamp, not just later than `before`.
	after := g.now() + 1

	// Two-phase test (rule 15): rel existed and was valid at `before`, was
	// deleted after that, so a query pinned at `before` must still surface it —
	// proving the deleted-rel fold (not just the live adjacency index, which no
	// longer has this rel) is exercised.
	out, err := g.Temporal.OutgoingRelsAt(a.ID(), before)
	if err != nil {
		t.Fatalf("OutgoingRelsAt: %v", err)
	}
	if len(out) != 1 || out[0].ID() != rel.ID() {
		t.Fatalf("OutgoingRelsAt(a, before) = %v, want exactly [%v]", out, rel.ID())
	}

	in, err := g.Temporal.IncomingRelsAt(b.ID(), before)
	if err != nil {
		t.Fatalf("IncomingRelsAt: %v", err)
	}
	if len(in) != 1 || in[0].ID() != rel.ID() {
		t.Fatalf("IncomingRelsAt(b, before) = %v, want exactly [%v]", in, rel.ID())
	}

	// Negative assertion (rule 16): after deletion, a query pinned at `after`
	// must NOT surface the now-deleted relationship.
	outAfter, err := g.Temporal.OutgoingRelsAt(a.ID(), after)
	if err != nil {
		t.Fatalf("OutgoingRelsAt(after): %v", err)
	}
	for _, r := range outAfter {
		if r.ID() == rel.ID() {
			t.Fatalf("OutgoingRelsAt(a, after) must NOT contain deleted rel %v, got %v", rel.ID(), outAfter)
		}
	}
}
