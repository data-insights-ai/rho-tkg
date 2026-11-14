package tiered

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// EMPTYBYID: a closed cold shard that HoldsNoRows proved empty (es.noRows) is
// not opened by a read of an ID the shard's window owns, by the history, adjacency,
// batch and create-existence probes of such an ID, or by the idle sweep's reopen
// cycle. Every probe is counted in ColdOpens (ShardAccessStats), never in time or
// memory, and answers what it answered when the shard was read.

// emptyByID is a tiered store whose first shard (es) was rotated out, demoted and
// closed holding no row, beside a hot shard with a few rows. missNode, missRel
// and crossRel were minted inside es's window; only crossRel was stored, on the hot
// shard, with its start node.
type emptyByID struct {
	ts       *Store
	cfg      Config
	es       *EventShard
	missNode types.NodeID // stored nowhere
	missRel  types.RelID  // stored nowhere
	crossRel types.RelID  // minted in es's window, stored on the hot shard (start node sig)
	lateNode types.NodeID // minted in es's window, written late by the tests that wake es
	lateRel  types.RelID  // likewise
	lateRef  types.NodeID // a reference-class ID minted in es's window
	a, b     types.NodeID // reference nodes
	sig      types.NodeID // event node on the hot shard
	lone     types.NodeID // event node on the hot shard with no relationship
	rel      types.RelID  // a -> b, placed by mint on the hot shard
}

func newEmptyByID(t *testing.T) *emptyByID {
	t.Helper()
	cfg := mintCfg(t.TempDir(), true)
	ts := openMintStore(t, cfg)
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	f := &emptyByID{
		ts: ts, cfg: cfg,
		missNode: types.NodeID(ng.Generate()),
		lateNode: types.NodeID(ng.Generate()),
		lateRef:  types.NodeID(ng.Generate()),
		missRel:  types.RelID(rg.Generate()),
		crossRel: types.RelID(rg.Generate()),
		lateRel:  types.RelID(rg.Generate()),
	}
	name := hotShardName(ts)
	rotate(t, ts)
	demoteToCold(ts, name)
	closeColdStores(t, ts)
	ts.mu.RLock()
	f.es = ts.eventShards[name]
	ts.mu.RUnlock()
	for _, raw := range []snowflake.ID{f.missNode.SnowflakeID(), f.lateNode.SnowflakeID(), f.lateRef.SnowflakeID(), f.missRel.SnowflakeID(), f.crossRel.SnowflakeID(), f.lateRel.SnowflakeID()} {
		if ts.timestampToEventShardEntry(raw) != f.es {
			t.Fatalf("setup: ID %d was not minted in the empty shard's window", raw)
		}
	}
	f.a = putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	f.b = putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
	f.sig = putMintNode(t, ts, types.NodeID(ng.Generate()), mintSignalTok)
	f.lone = putMintNode(t, ts, types.NodeID(ng.Generate()), mintSignalTok)
	f.rel = types.RelID(rg.Generate())
	putMintRel(t, ts, f.rel, 1, f.a, f.b)
	putMintRel(t, ts, f.crossRel, 1, f.sig, f.a)
	f.reseal(t)
	return f
}

// reseal closes every open cold shard again and checks the empty shard is still
// proven empty, so the next probe starts from a closed shard.
func (f *emptyByID) reseal(t *testing.T) {
	t.Helper()
	closeColdStores(t, f.ts)
	if !f.es.noRows.Load() {
		t.Fatal("the empty shard lost its no-rows mark")
	}
}

func (f *emptyByID) coldOpens() uint64 { return f.ts.ShardAccessStats().ColdOpens }

func fmtNodes(ns []*types.Node) string {
	ids := make([]string, len(ns))
	for i, n := range ns {
		if n != nil {
			ids[i] = fmt.Sprint(n.ID())
		}
	}
	return "[" + strings.Join(ids, " ") + "]"
}

func fmtRels(rs []*types.Relationship) string {
	ids := make([]string, len(rs))
	for i, r := range rs {
		if r != nil {
			ids[i] = fmt.Sprint(r.ID())
		}
	}
	return "[" + strings.Join(ids, " ") + "]"
}

func fmtRelMap(m map[types.NodeID][]*types.Relationship) string {
	var parts []string
	for id, rs := range m {
		parts = append(parts, fmt.Sprintf("%d:%s", id, fmtRels(rs)))
	}
	slices.Sort(parts)
	return strings.Join(parts, ",")
}

func ans(parts ...any) string { return fmt.Sprint(parts...) }

// byIDProbe is one read path with the answer it gives on this store.
type byIDProbe struct {
	name string
	run  func(f *emptyByID) string
}

// byIDProbes lists every by-ID read path of the todo row: the node and
// relationship ID doors, history, adjacency, and the batched forms.
func byIDProbes() []byIDProbe {
	return []byIDProbe{
		// Node by ID.
		{"GetNode miss", func(f *emptyByID) string { n, err := f.ts.GetNode(f.missNode); return ans(n != nil, err) }},
		{"LendNode miss", func(f *emptyByID) string { n, err := f.ts.LendNode(f.missNode); return ans(n != nil, err) }},
		{"NodeIntegrityHash miss", func(f *emptyByID) string { h, err := f.ts.NodeIntegrityHash(f.missNode); return ans(h, err) }},
		{"EndpointIntegrityHashes miss", func(f *emptyByID) string {
			h1, h2, err := f.ts.EndpointIntegrityHashes(f.missNode, f.a)
			return ans(h1, h2, err)
		}},
		{"NodeCurrentStamps miss", func(f *emptyByID) string { s, err := f.ts.NodeCurrentStamps(f.missNode); return ans(s, err) }},
		{"NodeStoredSizes miss", func(f *emptyByID) string { s, err := f.ts.NodeStoredSizes(f.missNode); return ans(s, err) }},
		// Node history.
		{"GetNodeHistory miss", func(f *emptyByID) string { h, err := f.ts.GetNodeHistory(f.missNode); return ans(fmtNodes(h), err) }},
		{"HasNodeHistory miss", func(f *emptyByID) string { h, err := f.ts.HasNodeHistory(f.missNode); return ans(h, err) }},
		{"GetNodeVersion miss", func(f *emptyByID) string { n, err := f.ts.GetNodeVersion(f.missNode, 0); return ans(n != nil, err) }},
		{"NodeHistoryStamps miss", func(f *emptyByID) string {
			a, b, ok, err := f.ts.NodeHistoryStamps(f.missNode)
			return ans(a, b, ok, err)
		}},
		{"NodeHistoryTemporalMeta miss", func(f *emptyByID) string {
			m, err := f.ts.NodeHistoryTemporalMeta(f.missNode)
			return ans(m, err)
		}},
		{"NodeHistoryVersionsFrom miss", func(f *emptyByID) string {
			h, err := f.ts.NodeHistoryVersionsFrom(f.missNode, 0, 10)
			return ans(fmtNodes(h), err)
		}},
		{"NodeAsOfWalk miss", func(f *emptyByID) string {
			n, ok, err := f.ts.NodeAsOfWalk(f.missNode, types.Instant(1<<60))
			return ans(n != nil, ok, err)
		}},
		// Relationship by ID.
		{"GetRelationship miss", func(f *emptyByID) string { r, err := f.ts.GetRelationship(f.missRel); return ans(r != nil, err) }},
		{"LendRelationship miss", func(f *emptyByID) string { r, err := f.ts.LendRelationship(f.missRel); return ans(r != nil, err) }},
		{"RelCurrentStamps miss", func(f *emptyByID) string { s, err := f.ts.RelCurrentStamps(f.missRel); return ans(s, err) }},
		{"GetRelationship cross-shard rel", func(f *emptyByID) string {
			r, err := f.ts.GetRelationship(f.crossRel)
			if err != nil || r == nil || r.ID() != f.crossRel {
				return ans("not found", err)
			}
			return ans("found", r.ID())
		}},
		{"LendRelationship cross-shard rel", func(f *emptyByID) string {
			r, err := f.ts.LendRelationship(f.crossRel)
			if err != nil || r == nil || r.ID() != f.crossRel {
				return ans("not found", err)
			}
			return ans("found", r.ID())
		}},
		{"RelCurrentStamps cross-shard rel", func(f *emptyByID) string { s, err := f.ts.RelCurrentStamps(f.crossRel); return ans(s, err) }},
		// Relationship history.
		{"GetRelHistory miss", func(f *emptyByID) string { h, err := f.ts.GetRelHistory(f.missRel); return ans(fmtRels(h), err) }},
		{"HasRelHistory miss", func(f *emptyByID) string { h, err := f.ts.HasRelHistory(f.missRel); return ans(h, err) }},
		{"GetRelVersion miss", func(f *emptyByID) string { r, err := f.ts.GetRelVersion(f.missRel, 0); return ans(r != nil, err) }},
		{"RelHistoryStamps miss", func(f *emptyByID) string {
			a, b, ok, err := f.ts.RelHistoryStamps(f.missRel)
			return ans(a, b, ok, err)
		}},
		{"RelHistoryTemporalMeta miss", func(f *emptyByID) string {
			m, err := f.ts.RelHistoryTemporalMeta(f.missRel)
			return ans(m, err)
		}},
		{"RelHistoryVersionsFrom miss", func(f *emptyByID) string {
			h, err := f.ts.RelHistoryVersionsFrom(f.missRel, 0, 10)
			return ans(fmtRels(h), err)
		}},
		{"GetRelHistory cross-shard rel", func(f *emptyByID) string {
			h, err := f.ts.GetRelHistory(f.crossRel)
			return ans(fmtRels(h), err)
		}},
		{"HasRelHistory cross-shard rel", func(f *emptyByID) string { h, err := f.ts.HasRelHistory(f.crossRel); return ans(h, err) }},
		// Adjacency of an ID the empty shard would own.
		{"OutgoingRelationships miss", func(f *emptyByID) string {
			r, err := f.ts.OutgoingRelationships(f.missNode, 0)
			return ans(fmtRels(r), err)
		}},
		{"IncomingRelationships miss", func(f *emptyByID) string {
			r, err := f.ts.IncomingRelationships(f.missNode, 0)
			return ans(fmtRels(r), err)
		}},
		{"OutgoingDegree miss", func(f *emptyByID) string { d, err := f.ts.OutgoingDegree(f.missNode, 0); return ans(d, err) }},
		{"IncomingDegree miss", func(f *emptyByID) string { d, err := f.ts.IncomingDegree(f.missNode, 0); return ans(d, err) }},
		{"ForEachExAdjacentRelID miss", func(f *emptyByID) string {
			var got []types.RelID
			err := f.ts.ForEachExAdjacentRelID(f.missNode, false, func(id types.RelID) bool { got = append(got, id); return true })
			return ans(got, err)
		}},
		{"ForEachExAdjacentRelID incoming miss", func(f *emptyByID) string {
			var got []types.RelID
			err := f.ts.ForEachExAdjacentRelID(f.missNode, true, func(id types.RelID) bool { got = append(got, id); return true })
			return ans(got, err)
		}},
		{"OutgoingRelationships of the event node sig", func(f *emptyByID) string {
			r, err := f.ts.OutgoingRelationships(f.sig, 0)
			return ans(fmtRels(r), err)
		}},
		{"IncomingRelationships of the reference node a", func(f *emptyByID) string {
			r, err := f.ts.IncomingRelationships(f.a, 0)
			return ans(fmtRels(r), err)
		}},
		// Batched forms.
		{"GetNodesByIDs with a miss", func(f *emptyByID) string {
			n, err := f.ts.GetNodesByIDs([]types.NodeID{f.a, f.missNode, f.sig})
			return ans(fmtNodes(n), err)
		}},
		{"GetNodesByIDs of misses only", func(f *emptyByID) string {
			n, err := f.ts.GetNodesByIDs([]types.NodeID{f.missNode, f.lateNode})
			return ans(fmtNodes(n), err)
		}},
		{"GetNodesByIDs with a duplicate miss", func(f *emptyByID) string {
			n, err := f.ts.GetNodesByIDs([]types.NodeID{f.a, f.missNode, f.a})
			return ans(fmtNodes(n), err)
		}},
		{"GetRelationshipsByIDs with a miss", func(f *emptyByID) string {
			r, err := f.ts.GetRelationshipsByIDs([]types.RelID{f.rel, f.missRel, f.crossRel})
			return ans(fmtRels(r), err)
		}},
		{"GetRelationshipsByIDs of cross-shard and miss", func(f *emptyByID) string {
			r, err := f.ts.GetRelationshipsByIDs([]types.RelID{f.crossRel, f.missRel})
			return ans(fmtRels(r), err)
		}},
		{"LendNodesSorted with a miss", func(f *emptyByID) string {
			ids := []types.NodeID{f.a, f.missNode, f.sig, f.lateNode}
			out := make([]*types.Node, len(ids))
			err := f.ts.LendNodesSorted(ids, out)
			return ans(fmtNodes(out), err)
		}},
		{"LendRelationshipsSorted with a miss", func(f *emptyByID) string {
			ids := []types.RelID{f.rel, f.missRel, f.crossRel, f.lateRel}
			out := make([]*types.Relationship, len(ids))
			err := f.ts.LendRelationshipsSorted(ids, out)
			return ans(fmtRels(out), err)
		}},
		{"OutgoingRelationshipsForNodes with a miss", func(f *emptyByID) string {
			m, err := f.ts.OutgoingRelationshipsForNodes([]types.NodeID{f.a, f.missNode}, 0)
			return ans(fmtRelMap(m), err)
		}},
		{"IncomingRelationshipsForNodes with a miss", func(f *emptyByID) string {
			m, err := f.ts.IncomingRelationshipsForNodes([]types.NodeID{f.a, f.missNode}, 0)
			return ans(fmtRelMap(m), err)
		}},
		{"OutgoingRelationshipsForNodes of misses only", func(f *emptyByID) string {
			m, err := f.ts.OutgoingRelationshipsForNodes([]types.NodeID{f.missNode, f.lateNode}, 0)
			return ans(fmtRelMap(m), err)
		}},
		// The create-existence probe of an ID.
		{"relIDExists miss", func(f *emptyByID) string { e, err := f.ts.relIDExists(f.missRel); return ans(e, err) }},
		{"relIDExists cross-shard rel", func(f *emptyByID) string { e, err := f.ts.relIDExists(f.crossRel); return ans(e, err) }},
	}
}

// TestEmptyByID_ReadsKeepTheShardClosed: every by-ID read path answers the same
// whether or not the empty shard is marked, and with the mark it opens nothing.
// The unmarked run is the answer before the skip: it reads the shard.
func TestEmptyByID_ReadsKeepTheShardClosed(t *testing.T) {
	f := newEmptyByID(t)
	for _, p := range byIDProbes() {
		t.Run(p.name, func(t *testing.T) {
			f.reseal(t)
			before := f.coldOpens()
			got := p.run(f)
			if opens := f.coldOpens() - before; opens != 0 {
				t.Errorf("opened the empty shard %d times (answer %q)", opens, got)
			}

			// Control: the mark off, the shard is read as before the skip.
			f.reseal(t)
			f.es.noRows.Store(false)
			want := p.run(f)
			f.es.noRows.Store(true) // a control that never opened the shard left the mark off
			f.reseal(t)
			if got != want {
				t.Errorf("answer with the mark %q, without it %q", got, want)
			}
		})
	}
}

// TestEmptyByID_MissAnswersAreTheMissContract pins the answers themselves, so a
// probe that skips the shard cannot agree with an equally wrong control.
func TestEmptyByID_MissAnswersAreTheMissContract(t *testing.T) {
	f := newEmptyByID(t)
	if _, err := f.ts.GetNode(f.missNode); !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("GetNode miss = %v, want ErrNodeNotFound", err)
	}
	if _, err := f.ts.GetRelationship(f.missRel); !errors.Is(err, ErrRelNotFound) {
		t.Errorf("GetRelationship miss = %v, want ErrRelNotFound", err)
	}
	if h, err := f.ts.HasNodeHistory(f.missNode); err != nil || h {
		t.Errorf("HasNodeHistory miss = %v, %v", h, err)
	}
	if r, err := f.ts.GetRelationship(f.crossRel); err != nil || r.ID() != f.crossRel {
		t.Errorf("the cross-shard relationship minted in the empty window is not found: %v", err)
	}
	if rels, err := f.ts.IncomingRelationships(f.a, 0); err != nil || len(rels) != 1 || rels[0].ID() != f.crossRel {
		t.Errorf("IncomingRelationships(a) = %s, %v; want the cross-shard rel", fmtRels(rels), err)
	}
	if rels, err := f.ts.OutgoingRelationships(f.a, 0); err != nil || len(rels) != 1 || rels[0].ID() != f.rel {
		t.Errorf("OutgoingRelationships(a) = %s, %v; want the mint-placed rel", fmtRels(rels), err)
	}
	if _, err := f.ts.OutgoingRelationships(f.missNode, 0); !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("OutgoingRelationships miss = %v, want ErrNodeNotFound", err)
	}
	if exists, err := f.ts.relIDExists(f.crossRel); err != nil || !exists {
		t.Errorf("relIDExists(cross-shard rel) = %v, %v", exists, err)
	}
	if exists, err := f.ts.relIDExists(f.missRel); err != nil || exists {
		t.Errorf("relIDExists(miss) = %v, %v", exists, err)
	}
	f.reseal(t)
}

// TestEmptyByID_RepeatedSweepsAndMissesOpenNothing: misses, idle sweeps and more
// misses, three rounds, cost no open of the empty shard and leave it closed and
// marked.
func TestEmptyByID_RepeatedSweepsAndMissesOpenNothing(t *testing.T) {
	f := newEmptyByID(t)
	before := f.coldOpens()
	for round := range 3 {
		for _, p := range byIDProbes() {
			p.run(f)
		}
		f.ts.closeIdleShards()
		if opens := f.coldOpens() - before; opens != 0 {
			t.Fatalf("round %d: the empty shard was opened %d times", round, opens)
		}
	}
	f.es.shardMu.Lock()
	open := f.es.store != nil
	f.es.shardMu.Unlock()
	if open || !f.es.noRows.Load() {
		t.Errorf("after the rounds the empty shard is open %v, marked %v; want closed and marked", open, f.es.noRows.Load())
	}
}

// TestEmptyByID_CreateExistenceProbesOpenNothing: creating a reference node and a
// relationship under an ID the empty shard's window owns probes for an earlier
// row, and the probe skips the shard. The rows are written where they belong (the
// reference shard, the start node's shard) and read back.
func TestEmptyByID_CreateExistenceProbesOpenNothing(t *testing.T) {
	f := newEmptyByID(t)
	before := f.coldOpens()
	if err := f.ts.PutNode(types.NewNode(f.lateRef, mintCaseTok, nil)); err != nil {
		t.Fatalf("PutNode of a reference node: %v", err)
	}
	if err := f.ts.PutRelationship(types.NewRelationship(f.lateRel, 1, f.sig, f.b)); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if opens := f.coldOpens() - before; opens != 0 {
		t.Errorf("the create-existence probes opened the empty shard %d times", opens)
	}
	if !f.es.noRows.Load() {
		t.Error("a probe dropped the empty shard's mark")
	}
	if n, err := f.ts.GetNode(f.lateRef); err != nil || n.ID() != f.lateRef {
		t.Errorf("GetNode(created reference node) = %v", err)
	}
	if r, err := f.ts.GetRelationship(f.lateRel); err != nil || r.ID() != f.lateRel {
		t.Errorf("GetRelationship(created rel) = %v", err)
	}
	if err := f.ts.PutNode(types.NewNode(f.lateRef, mintCaseTok, nil)); !errors.Is(err, ErrNodeExists) {
		t.Errorf("a second create of the node = %v, want ErrNodeExists", err)
	}
	if err := f.ts.PutRelationship(types.NewRelationship(f.lateRel, 1, f.sig, f.b)); !errors.Is(err, ErrRelExists) {
		t.Errorf("a second create of the rel = %v, want ErrRelExists", err)
	}
}

// TestEmptyByID_BatchCreateExistenceProbesOpenNothing is the batched form.
func TestEmptyByID_BatchCreateExistenceProbesOpenNothing(t *testing.T) {
	f := newEmptyByID(t)
	before := f.coldOpens()
	refs := []*types.Node{types.NewNode(f.lateRef, mintCaseTok, nil)}
	if err := f.ts.PutNodesBatch(refs); err != nil {
		t.Fatalf("PutNodesBatch: %v", err)
	}
	rels := []*types.Relationship{types.NewRelationship(f.lateRel, 1, f.sig, f.b)}
	if err := f.ts.PutRelationshipsBatch(rels); err != nil {
		t.Fatalf("PutRelationshipsBatch: %v", err)
	}
	if opens := f.coldOpens() - before; opens != 0 {
		t.Errorf("the batched create-existence probes opened the empty shard %d times", opens)
	}
	if n, err := f.ts.GetNodesByIDs([]types.NodeID{f.lateRef, f.a}); err != nil || len(n) != 2 {
		t.Errorf("GetNodesByIDs(created) = %d, %v", len(n), err)
	}
}

// TestEmptyByID_LateWriteWakesTheShardForEveryRead: a write into the empty
// shard's window wakes it (the mark leaves the saved catalog before the write);
// from then on the ID reads find the row there, on every path, also once the
// shard is closed again with its row.
func TestEmptyByID_LateWriteWakesTheShardForEveryRead(t *testing.T) {
	f := newEmptyByID(t)
	if err := f.ts.PutNode(types.NewNode(f.lateNode, mintSignalTok, nil)); err != nil {
		t.Fatalf("late PutNode: %v", err)
	}
	if f.es.noRows.Load() {
		t.Fatal("the late write left the shard marked empty")
	}
	if f.ts.catalogEntryNoRows(t, f.cfg, f.es.name) {
		t.Fatal("the saved catalog still marks the written shard")
	}
	// The write needs a rel too: an event node with a rel to a reference node, both in the late shard's window by ID.
	if err := f.ts.PutRelationship(types.NewRelationship(f.lateRel, 1, f.lateNode, f.a)); err != nil {
		t.Fatalf("late PutRelationship: %v", err)
	}
	check := func(when string) {
		t.Helper()
		if n, err := f.ts.GetNode(f.lateNode); err != nil || n.ID() != f.lateNode {
			t.Errorf("%s: GetNode(late) = %v", when, err)
		}
		if n, err := f.ts.LendNode(f.lateNode); err != nil || n.ID() != f.lateNode {
			t.Errorf("%s: LendNode(late) = %v", when, err)
		}
		if _, err := f.ts.NodeCurrentStamps(f.lateNode); err != nil {
			t.Errorf("%s: NodeCurrentStamps(late) = %v", when, err)
		}
		if r, err := f.ts.GetRelationship(f.lateRel); err != nil || r.ID() != f.lateRel {
			t.Errorf("%s: GetRelationship(late) = %v", when, err)
		}
		if r, err := f.ts.OutgoingRelationships(f.lateNode, 0); err != nil || len(r) != 1 {
			t.Errorf("%s: OutgoingRelationships(late) = %d, %v", when, len(r), err)
		}
		if d, err := f.ts.OutgoingDegree(f.lateNode, 0); err != nil || d != 1 {
			t.Errorf("%s: OutgoingDegree(late) = %d, %v", when, d, err)
		}
		if n, err := f.ts.GetNodesByIDs([]types.NodeID{f.lateNode, f.a}); err != nil || len(n) != 2 {
			t.Errorf("%s: GetNodesByIDs(late) = %d, %v", when, len(n), err)
		}
		if r, err := f.ts.GetRelationshipsByIDs([]types.RelID{f.lateRel, f.rel}); err != nil || len(r) != 2 {
			t.Errorf("%s: GetRelationshipsByIDs(late) = %d, %v", when, len(r), err)
		}
		ids := []types.NodeID{f.lateNode, f.a}
		out := make([]*types.Node, len(ids))
		if err := f.ts.LendNodesSorted(ids, out); err != nil || out[0] == nil || out[1] == nil {
			t.Errorf("%s: LendNodesSorted(late) = %v, %v", when, fmtNodes(out), err)
		}
		rids := []types.RelID{f.lateRel, f.rel}
		rout := make([]*types.Relationship, len(rids))
		if err := f.ts.LendRelationshipsSorted(rids, rout); err != nil || rout[0] == nil || rout[1] == nil {
			t.Errorf("%s: LendRelationshipsSorted(late) = %v, %v", when, fmtRels(rout), err)
		}
		if h, err := f.ts.HasNodeHistory(f.lateNode); err != nil {
			t.Errorf("%s: HasNodeHistory(late) = %v, %v", when, h, err)
		}
	}
	check("open")
	closeColdStores(t, f.ts)
	if f.es.noRows.Load() {
		t.Fatal("a shard holding rows is marked empty after its close")
	}
	check("closed again")
}

// TestEmptyByID_WakeFindsTheReadAfterAMarkedMiss: a read that skipped the shard
// while it was empty does not poison the later answer: the same ID, written,
// reads back.
func TestEmptyByID_WakeFindsTheReadAfterAMarkedMiss(t *testing.T) {
	f := newEmptyByID(t)
	if _, err := f.ts.GetNode(f.lateNode); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode before the write = %v, want ErrNodeNotFound", err)
	}
	if err := f.ts.PutNode(types.NewNode(f.lateNode, mintSignalTok, nil)); err != nil {
		t.Fatal(err)
	}
	if n, err := f.ts.GetNode(f.lateNode); err != nil || n.ID() != f.lateNode {
		t.Fatalf("GetNode after the write = %v", err)
	}
	if h, err := f.ts.GetNodeHistory(f.lateNode); err != nil {
		t.Fatalf("GetNodeHistory after the write: %v (%d rows)", err, len(h))
	}
}

// TestEmptyByID_FanOutReadsAreAlreadyFine records that the fan-out reads which
// were skipping empty shards before EMPTYBYID still do.
func TestEmptyByID_FanOutReadsAreAlreadyFine(t *testing.T) {
	f := newEmptyByID(t)
	reads := map[string]func() error{
		"AllNodes":            func() error { _, err := f.ts.AllNodes(QueryOpts{Depth: DepthAll}); return err },
		"AllNodeIDs":          func() error { _, err := f.ts.AllNodeIDs(QueryOpts{Depth: DepthAll}); return err },
		"AllRelationships":    func() error { _, err := f.ts.AllRelationships(QueryOpts{Depth: DepthAll}); return err },
		"NodesByLabel":        func() error { _, err := f.ts.NodesByLabel(mintSignalTok, QueryOpts{Depth: DepthAll}); return err },
		"RelationshipsByType": func() error { _, err := f.ts.RelationshipsByType(1, QueryOpts{Depth: DepthAll}); return err },
		"AllNodeHistoryIDs":   func() error { _, err := f.ts.AllNodeHistoryIDs(); return err },
		"AllRelHistoryIDs":    func() error { _, err := f.ts.AllRelHistoryIDs(); return err },
		"NodeCountByLabel":    func() error { _, err := f.ts.NodeCountByLabel(mintSignalTok); return err },
		"OutgoingRelationships of a reference node (mint fan-out)": func() error {
			_, err := f.ts.OutgoingRelationships(f.a, 0)
			return err
		},
		"IncomingRelationships of a reference node (mint fan-out)": func() error {
			_, err := f.ts.IncomingRelationships(f.b, 0)
			return err
		},
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			f.reseal(t)
			before := f.coldOpens()
			if err := read(); err != nil {
				t.Fatal(err)
			}
			if opens := f.coldOpens() - before; opens != 0 {
				t.Errorf("opened the empty shard %d times", opens)
			}
		})
	}
}

// TestEmptyByID_MarkSurvivesAReopenAndAWriteClearsItForGood: after a restart the
// saved mark still keeps the by-ID reads off the shard; a late write clears it in
// the saved catalog, so after the next restart the row is read from the shard.
func TestEmptyByID_MarkSurvivesAReopenAndAWriteClearsItForGood(t *testing.T) {
	f := newEmptyByID(t)
	name := f.es.name
	if err := f.ts.Close(); err != nil {
		t.Fatal(err)
	}

	ts := openMintStore(t, f.cfg)
	ts.mu.RLock()
	es := ts.eventShards[name]
	ts.mu.RUnlock()
	if !es.noRows.Load() {
		t.Fatal("the mark did not survive the restart")
	}
	before := ts.ShardAccessStats().ColdOpens
	if _, err := ts.GetNode(f.missNode); !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("GetNode miss after the restart = %v", err)
	}
	if _, err := ts.GetRelationship(f.missRel); !errors.Is(err, ErrRelNotFound) {
		t.Errorf("GetRelationship miss after the restart = %v", err)
	}
	if r, err := ts.GetRelationship(f.crossRel); err != nil || r.ID() != f.crossRel {
		t.Errorf("the cross-shard rel after the restart = %v", err)
	}
	if opens := ts.ShardAccessStats().ColdOpens - before; opens != 0 {
		t.Errorf("by-ID reads after the restart opened the empty shard %d times", opens)
	}

	if err := ts.PutNode(types.NewNode(f.lateNode, mintSignalTok, nil)); err != nil {
		t.Fatal(err)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	ts = openMintStore(t, f.cfg)
	ts.mu.RLock()
	es = ts.eventShards[name]
	ts.mu.RUnlock()
	if es.noRows.Load() {
		t.Fatal("the written shard is marked empty after the restart")
	}
	if n, err := ts.GetNode(f.lateNode); err != nil || n.ID() != f.lateNode {
		t.Errorf("GetNode(late) after the second restart = %v", err)
	}
}

// A marked shard that gets a writable handle without the cold open (the
// retention drop's rollback reopens it with setStoreLocked) is still woken by
// the next write, so the row is not hidden behind the mark (EMPTYBYID review,
// finding 1).
func TestEmptyByID_WriteToAMarkedOpenHandleWakesTheShard(t *testing.T) {
	f := newEmptyByID(t)
	f.es.shardMu.Lock()
	store, err := f.ts.openShardStoreWithRecovery(f.es.path, true)
	if err == nil {
		f.es.setStoreLocked(f.ts, store)
	}
	f.es.shardMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if !f.es.noRows.Load() {
		t.Fatal("setup: the reopened shard lost its mark")
	}
	if err := f.ts.PutNode(types.NewNode(f.lateNode, mintSignalTok, nil)); err != nil {
		t.Fatalf("late PutNode: %v", err)
	}
	if f.es.noRows.Load() {
		t.Fatal("the write left the shard marked empty")
	}
	if n, err := f.ts.GetNode(f.lateNode); err != nil || n.ID() != f.lateNode {
		t.Fatalf("GetNode(late) after the write = %v: the mark hid the row", err)
	}
}
