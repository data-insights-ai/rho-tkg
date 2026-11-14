package tiered

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// coldOpenDoorsDir writes a store whose one event shard (day 20) is demoted,
// closed and sealed, and closes the store: 31 Signal nodes (every 4th also
// carries User), relationships of two types among them, a node with two
// versions and a removed label, a node and a relationship deleted. It returns
// the directory, the config and the IDs the doors are asked about.
func coldOpenDoorsDir(t *testing.T) (cfg Config, nodes []types.NodeID, rels []types.RelID, shard string) {
	t.Helper()
	cfg = mintCfg(t.TempDir(), true)
	cfg.MemTableSize = 8 << 20 // an open allocates an arena of about 1.3 times it
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	installDefaultTestLabelRegistry(t, ts)
	ng, rg := tieredNodeGen(t), tieredRelGen(t)
	const userTok = uint16(2)
	for i := 0; i < 31; i++ { // node 30 stays unconnected: it is the one deleted
		id := types.NodeID(ng.Generate())
		var extra []uint16
		if i%4 == 0 {
			extra = []uint16{userTok}
		}
		n := types.NewNode(id, mintSignalTok, extra)
		_ = n.SetProperty("k", int64(i%5))
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		nodes = append(nodes, id)
	}
	for i := 0; i+1 < 30; i += 2 {
		for typ := uint16(5); typ <= 6; typ++ {
			if typ == 6 && i%4 != 0 {
				continue
			}
			rid := types.RelID(rg.Generate())
			putMintRel(t, ts, rid, typ, nodes[i], nodes[i+1])
			rels = append(rels, rid)
		}
	}
	// History: a second version of node 1, a removed label on node 4 (a former User member).
	v0, err := ts.GetNode(nodes[1])
	if err != nil {
		t.Fatal(err)
	}
	v1 := types.NewNode(nodes[1], mintSignalTok, nil)
	_ = v1.SetProperty("k", int64(99))
	v1.SetVersion(1)
	if err := ts.ReplaceNodeWithHistory(v1, 0, v0); err != nil {
		t.Fatalf("ReplaceNodeWithHistory: %v", err)
	}
	w0, err := ts.GetNode(nodes[4])
	if err != nil {
		t.Fatal(err)
	}
	w1 := types.NewNode(nodes[4], mintSignalTok, nil)
	w1.SetVersion(1)
	if err := ts.RemoveNodeLabelTokenWithHistory(nodes[4], userTok, w1, 0, w0); err != nil {
		t.Fatalf("RemoveNodeLabelTokenWithHistory: %v", err)
	}
	if err := ts.DeleteRelationship(rels[len(rels)-1]); err != nil {
		t.Fatal(err)
	}
	rels = rels[:len(rels)-1]
	if err := ts.DeleteNode(nodes[30]); err != nil {
		t.Fatal(err)
	}
	shard = hotShardName(ts)
	rotate(t, ts)
	demoteToCold(ts, shard)
	closeColdStores(t, ts)
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	return cfg, nodes, rels, shard
}

// coldOpenDoors renders what the read doors answer over the demoted shard: by-ID
// reads (live, deleted and unknown IDs), label and type reads, adjacency and
// degrees, history and presence probes, counts, the sealed-shard catalog
// doors (former label members, node and rel ID listings) and integrity.
func coldOpenDoors(t *testing.T, ts *Store, nodes []types.NodeID, rels []types.RelID) string {
	t.Helper()
	var out []string
	add := func(format string, args ...any) { out = append(out, fmt.Sprintf(format, args...)) }
	must := func(what string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	idsOf := func(ns []*types.Node) []types.NodeID {
		var ids []types.NodeID
		for _, n := range ns {
			ids = append(ids, n.ID())
		}
		slices.Sort(ids)
		return ids
	}
	relsOf := func(rs []*types.Relationship) []types.RelID {
		var ids []types.RelID
		for _, r := range rs {
			ids = append(ids, r.ID())
		}
		slices.Sort(ids)
		return ids
	}
	unknown := types.NodeID(nodes[0] - 1)
	for i, id := range append(slices.Clone(nodes), unknown) {
		n, err := ts.GetNode(id)
		if err != nil {
			add("GetNode %d: %v", i, err)
			continue
		}
		hash, herr := ts.NodeIntegrityHash(id)
		add("GetNode %d: labels=%d version=%d props=%v hash=%s/%v", i, n.LabelTokenCount(), n.Version(), n.Properties(), hash, herr)
		stamps, serr := ts.NodeCurrentStamps(id)
		add("NodeCurrentStamps %d: %+v %v", i, stamps, serr)
		has, herr := ts.HasNodeHistory(id)
		add("HasNodeHistory %d: %v %v", i, has, herr)
		in, err := ts.IncomingRelationships(id, 0)
		must("IncomingRelationships", err)
		out2, err := ts.OutgoingRelationships(id, 0)
		must("OutgoingRelationships", err)
		add("rels of %d: in=%v out=%v", i, relsOf(in), relsOf(out2))
		for typ := uint16(5); typ <= 6; typ++ {
			od, err := ts.OutgoingDegree(id, typ)
			must("OutgoingDegree", err)
			id2, err := ts.IncomingDegree(id, typ)
			must("IncomingDegree", err)
			add("degrees of %d type %d: out=%d in=%d", i, typ, od, id2)
		}
	}
	got, err := ts.GetNodesByIDs(nodes[:10])
	must("GetNodesByIDs", err)
	add("GetNodesByIDs %v", idsOf(got))
	hist, err := ts.GetNodeHistory(nodes[1])
	must("GetNodeHistory", err)
	add("GetNodeHistory(1) %d versions", len(hist))
	for i, rid := range append(slices.Clone(rels), types.RelID(rels[0]-1)) {
		r, err := ts.GetRelationship(rid)
		if err != nil {
			add("GetRelationship %d: %v", i, err)
			continue
		}
		hr, herr := ts.HasRelHistory(rid)
		add("GetRelationship %d: type=%d start=%d end=%d hist=%v/%v", i, r.TypeToken().Value(), r.StartNodeID(), r.EndNodeID(), hr, herr)
	}
	rr, err := ts.GetRelationshipsByIDs(rels)
	must("GetRelationshipsByIDs", err)
	add("GetRelationshipsByIDs %v", relsOf(rr))
	for _, tok := range []uint16{2, 3, 9} {
		ids, err := ts.NodeIDsByLabel(tok)
		must("NodeIDsByLabel", err)
		slices.Sort(ids)
		ns, err := ts.NodesByLabel(tok, storecontract.QueryOpts{})
		must("NodesByLabel", err)
		c, err := ts.NodeCountByLabel(tok)
		must("NodeCountByLabel", err)
		add("label %d: ids=%v nodes=%v count=%d", tok, ids, idsOf(ns), c)
		var former []string
		must("ForEachLabelTxMember", ts.ForEachLabelTxMember(tok, func(id types.NodeID, tx types.Instant) bool {
			former = append(former, fmt.Sprintf("%d@%d", id, tx))
			return true
		}))
		slices.Sort(former)
		var of []string
		must("LabelTxMembersOf", ts.LabelTxMembersOf(tok, nodes, func(id types.NodeID, tx types.Instant) bool {
			of = append(of, fmt.Sprintf("%d@%d", id, tx))
			return true
		}))
		slices.Sort(of)
		add("label %d tx members: %v of=%v", tok, former, of)
	}
	for _, typ := range []uint16{5, 6, 9} {
		ids, err := ts.RelIDsByType(typ)
		must("RelIDsByType", err)
		slices.Sort(ids)
		rs, err := ts.RelationshipsByType(typ, storecontract.QueryOpts{})
		must("RelationshipsByType", err)
		c, err := ts.RelCountByType(typ)
		must("RelCountByType", err)
		add("type %d: ids=%v rels=%v count=%d", typ, ids, relsOf(rs), c)
	}
	allN, err := ts.AllNodeIDs(storecontract.QueryOpts{})
	must("AllNodeIDs", err)
	allR, err := ts.AllRelIDs(storecontract.QueryOpts{})
	must("AllRelIDs", err)
	nc, err := ts.NodeCount()
	must("NodeCount", err)
	rc, err := ts.RelationshipCount()
	must("RelationshipCount", err)
	add("all nodes %v rels %v counts %d %d", allN, allR, nc, rc)
	return strings.Join(out, "\n")
}

// TestColdOpen_ReadDoorsAnswerTheSameThroughATransientHandle: every read door
// over a closed cold shard answers the same through the cheap handle a read
// opens at MaxOpenColdShards 0 (a 16 MB block cache, label maps built on first
// use, ID sets kept encoded) as through the handle a cap keeps (the configured
// cache, the label maps and ID sets built at the open): by-ID reads of live,
// deleted and unknown IDs, label and type reads, adjacency, degrees, history
// and presence probes, counts, the sealed-shard catalog doors and integrity.
// The second pass is the one that checks: the same directory, reopened.
func TestColdOpen_ReadDoorsAnswerTheSameThroughATransientHandle(t *testing.T) {
	cfg, nodes, rels, shard := coldOpenDoorsDir(t)
	// What the doors leave in the catalog: a transient close seals the shard's counts,
	// its former label members and its rel ID range from the open handle
	// (sealCountsLocked); the seal must come out as it went in, with no save.
	type sealed struct {
		entry ShardEntry
		saves int64
	}
	var sealedBefore, sealedAfter sealed
	pass := func(maxOpen int) (string, int64) {
		t.Helper()
		c := cfg
		c.MaxOpenColdShards = maxOpen
		ts, err := New(c)
		if err != nil {
			t.Fatalf("reopen at MaxOpenColdShards %d: %v", maxOpen, err)
		}
		defer func() { _ = ts.Close() }()
		installDefaultTestLabelRegistry(t, ts)
		before := coldOpensOf(t, ts, shard)
		sealedBefore = sealed{catalogEntry(t, ts.CatalogForTest(), shard), ts.CatalogForTest().SavesForTest()}
		doors := coldOpenDoors(t, ts, nodes, rels)
		sealedAfter = sealed{catalogEntry(t, ts.CatalogForTest(), shard), ts.CatalogForTest().SavesForTest()}
		return doors, coldOpensOf(t, ts, shard) - before
	}
	want, retainedOpens := pass(2)
	got, transientOpens := pass(0)
	if !sealedBefore.entry.CountsSealed || !sealedBefore.entry.FormerLabelsKnown || !sealedBefore.entry.RelIDs {
		t.Fatalf("setup: the catalog entry the transient pass starts from is not fully sealed: %+v", sealedBefore.entry)
	}
	if !reflect.DeepEqual(sealedBefore.entry, sealedAfter.entry) || sealedAfter.saves != sealedBefore.saves {
		t.Fatalf("the transient closes changed the sealed entry or saved the catalog (%d saves):\n before: %+v\n  after: %+v",
			sealedAfter.saves-sealedBefore.saves, sealedBefore.entry, sealedAfter.entry)
	}
	if retainedOpens != 1 {
		t.Fatalf("the retained pass opened the cold shard %d times, want 1", retainedOpens)
	}
	if transientOpens < 20 {
		t.Fatalf("the transient pass opened the cold shard %d times: the doors did not run on transient handles", transientOpens)
	}
	if got != want {
		gl, wl := strings.Split(got, "\n"), strings.Split(want, "\n")
		for i := 0; i < len(gl) && i < len(wl); i++ {
			if gl[i] != wl[i] {
				t.Fatalf("door answers differ at line %d:\n transient: %s\n  retained: %s", i+1, gl[i], wl[i])
			}
		}
		t.Fatalf("door answers differ in length: transient %d lines, retained %d", len(gl), len(wl))
	}
}
