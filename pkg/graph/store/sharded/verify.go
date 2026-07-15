package sharded

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Cross-shard consistency verification (ADR-0007 S2 Risk 1) ---
//
// VerifyConsistency is the crash-window DIAGNOSIS tool the ADR promises: a
// public, offline-cheap scan for the cross-shard dangling references a
// non-atomic cascade/split-write can leave behind. It performs NO repair (that
// is an S5 decision) — it only reports. A caller runs it after an unclean
// shutdown, or in a periodic audit, to decide whether to re-drive a cascade or
// run a repair.

// AdjacencyOrphan is an adjacency index entry whose relationship row is gone —
// a torn write or a cascade that removed the rel row but not (all of) its index
// entries. Detected on the incoming keyspace (co-located with the rel row on the
// rel's shard); by the single-shard co-commit of a rel's row+out+in, an orphan
// outgoing entry appears together with the incoming one it is reported through.
type AdjacencyOrphan struct {
	Shard int          // shard the orphan entry lives on
	Node  snowflake.ID // the endpoint node the entry indexes
	Rel   snowflake.ID // the relationship row that is absent
}

// RelEndpointOrphan is a live relationship whose start or end node ROW is fully
// gone (no current row AND no history). A node deleted WITH history is NOT an
// orphan — its history remains queryable (B32) — and is deliberately excluded.
type RelEndpointOrphan struct {
	Shard   int          // the rel's shard
	Rel     snowflake.ID // the dangling relationship
	Missing snowflake.ID // the vanished endpoint node
	IsStart bool         // true: start endpoint gone; false: end endpoint gone
}

// ShardMismatch is a row physically stored on a shard that its slot does not
// route to — a catalog/shard disagreement or ID corruption. Kind is "node" or
// "rel".
type ShardMismatch struct {
	Shard         int          // shard the row was found on
	ID            snowflake.ID // the misrouted entity ID
	Slot          uint8        // the slot carried in the ID
	ExpectedShard int          // shard the catalog routes that slot to (-1 = unclaimed)
	Kind          string
}

// Report is the typed result of VerifyConsistency. OK() is true iff every
// category is empty.
type Report struct {
	AdjacencyOrphans   []AdjacencyOrphan
	RelEndpointOrphans []RelEndpointOrphan
	ShardMismatches    []ShardMismatch
}

// OK reports whether the store is free of the diagnosed inconsistencies.
func (r Report) OK() bool {
	return len(r.AdjacencyOrphans) == 0 &&
		len(r.RelEndpointOrphans) == 0 &&
		len(r.ShardMismatches) == 0
}

// Total is the count of all reported inconsistencies across every category.
func (r Report) Total() int {
	return len(r.AdjacencyOrphans) + len(r.RelEndpointOrphans) + len(r.ShardMismatches)
}

// VerifyConsistency scans every shard for cross-shard dangling references and
// returns a typed Report. It is read-only and does NOT repair. Categories:
//
//   - AdjacencyOrphans: incoming adjacency entries whose rel row is gone.
//   - RelEndpointOrphans: live rels whose endpoint node row is fully gone
//     (distinguished from legitimately deleted-with-history — those are skipped).
//   - ShardMismatches: a node/rel row whose slot does not route to the shard it
//     was found on (catalog/shard disagreement).
func (s *Store) VerifyConsistency() (Report, error) {
	if err := s.checkOpen(); err != nil {
		return Report{}, err
	}
	var rep Report
	for k, shard := range s.shards {
		// (1) Relationships on this shard: routing + endpoint liveness.
		relIDs, err := shard.AllRelIDs(QueryOpts{})
		if err != nil {
			return Report{}, err
		}
		for _, rid := range relIDs {
			sfid := rid.SnowflakeID()
			if exp, ok := s.catalog.shardIndexForSlot(slotOf(sfid)); !ok || exp != k {
				rep.ShardMismatches = append(rep.ShardMismatches, ShardMismatch{
					Shard: k, ID: sfid, Slot: slotOf(sfid), ExpectedShard: expectedShard(ok, exp), Kind: "rel",
				})
			}
			r, gerr := shard.GetRelationship(rid)
			if gerr != nil {
				// The ID is indexed but the row is unreadable — a genuine orphan
				// on the rel keyspace; report via the adjacency category using
				// the rel's own ID (best available) is misleading, so skip: the
				// AllRelIDs source already implies a row. A read error here is a
				// deeper corruption surfaced by GetRelationship's own error.
				return Report{}, gerr
			}
			if o, dangling := s.endpointOrphan(k, r, r.StartNodeID(), true); dangling {
				rep.RelEndpointOrphans = append(rep.RelEndpointOrphans, o)
			}
			if o, dangling := s.endpointOrphan(k, r, r.EndNodeID(), false); dangling {
				rep.RelEndpointOrphans = append(rep.RelEndpointOrphans, o)
			}
		}

		// (2) Nodes on this shard: routing.
		nodeIDs, err := shard.AllNodeIDs(QueryOpts{})
		if err != nil {
			return Report{}, err
		}
		for _, nid := range nodeIDs {
			sfid := nid.SnowflakeID()
			if exp, ok := s.catalog.shardIndexForSlot(slotOf(sfid)); !ok || exp != k {
				rep.ShardMismatches = append(rep.ShardMismatches, ShardMismatch{
					Shard: k, ID: sfid, Slot: slotOf(sfid), ExpectedShard: expectedShard(ok, exp), Kind: "node",
				})
			}
		}

		// (3) Dangling incoming adjacency: an entry whose rel row (co-located on
		// this shard) is absent.
		for _, e := range shard.IncomingIndexEntries() {
			if !shard.HasRelID(e.RelID) {
				rep.AdjacencyOrphans = append(rep.AdjacencyOrphans, AdjacencyOrphan{
					Shard: k, Node: e.EndID, Rel: e.RelID,
				})
			}
		}
	}
	return rep, nil
}

// endpointOrphan reports whether endpoint (of rel r on shard relShardIdx) points
// at a node whose row is fully gone. A node with history rows but no current row
// was deleted-with-history and is NOT an orphan.
func (s *Store) endpointOrphan(relShardIdx int, r *types.Relationship, endpoint types.NodeID, isStart bool) (RelEndpointOrphan, bool) {
	shard, err := s.shardForNodeID(endpoint)
	if err != nil {
		// The endpoint's slot is not local — a cross-partition edge whose node
		// lives on another machine (horizontal stage). Not an orphan here.
		return RelEndpointOrphan{}, false
	}
	if shard.HasNodeID(endpoint.SnowflakeID()) {
		return RelEndpointOrphan{}, false
	}
	// No current row — legitimate only if history remains (deleted-with-history).
	hist, herr := shard.GetNodeHistory(endpoint)
	if herr == nil && len(hist) > 0 {
		return RelEndpointOrphan{}, false
	}
	return RelEndpointOrphan{
		Shard: relShardIdx, Rel: r.ID().SnowflakeID(), Missing: endpoint.SnowflakeID(), IsStart: isStart,
	}, true
}

// expectedShard maps the catalog lookup result to a report field: the shard
// index for a claimed slot, or -1 when the slot is unclaimed.
func expectedShard(ok bool, idx int) int {
	if !ok {
		return -1
	}
	return idx
}
