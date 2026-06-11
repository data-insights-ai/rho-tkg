package badger

import (
	"fmt"
	"sort"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Disk-resident adjacency index probes: both-arms parity with the
// in-memory map mode — same discipline as the label suite.

// seedAdjStores builds one mem-mode and one disk-mode store with an
// identical hub topology: hub --KNOWS--> spoke_i and spoke_i --LIKES--> hub.
func seedAdjStores(t *testing.T, n int) (mem, disk *Store, hub types.NodeID, spokes []types.NodeID) {
	t.Helper()
	var err error
	mem, err = New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("New mem: %v", err)
	}
	t.Cleanup(func() { mem.Close() })
	disk, err = New(Config{InMemory: true, AdjacencyIndexOnDisk: true})
	if err != nil {
		t.Fatalf("New disk: %v", err)
	}
	t.Cleanup(func() { disk.Close() })

	hub = types.NodeID(1)
	const knows, likes = uint16(1), uint16(2)
	for _, bs := range []*Store{mem, disk} {
		if err := bs.PutNode(types.NewNode(hub, 1, nil)); err != nil {
			t.Fatalf("put hub: %v", err)
		}
	}
	for i := 0; i < n; i++ {
		sid := types.NodeID(100 + i)
		spokes = append(spokes, sid)
		for _, bs := range []*Store{mem, disk} {
			if err := bs.PutNode(types.NewNode(sid, 1, nil)); err != nil {
				t.Fatalf("put spoke: %v", err)
			}
			r1 := types.NewRelationship(types.RelID(1000+i), knows, hub, sid)
			if err := bs.PutRelationship(r1); err != nil {
				t.Fatalf("put KNOWS: %v", err)
			}
			r2 := types.NewRelationship(types.RelID(2000+i), likes, sid, hub)
			if err := bs.PutRelationship(r2); err != nil {
				t.Fatalf("put LIKES: %v", err)
			}
		}
	}
	return mem, disk, hub, spokes
}

func relIDsOf(rels []*types.Relationship) []int64 {
	ids := make([]int64, 0, len(rels))
	for _, r := range rels {
		ids = append(ids, int64(r.ID()))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// TestAdjacencyOnDisk_ParityWithMemoryMode compares every adjacency read
// surface across both modes, BEFORE flush (pending-overlay visibility) and
// after, then after deletes.
func TestAdjacencyOnDisk_ParityWithMemoryMode(t *testing.T) {
	t.Parallel()
	mem, disk, hub, spokes := seedAdjStores(t, 15)
	const knows, likes = uint16(1), uint16(2)

	compare := func(step string) {
		t.Helper()
		for _, tok := range []uint16{0, knows, likes} {
			mo, err1 := mem.OutgoingRelationships(hub, tok)
			do, err2 := disk.OutgoingRelationships(hub, tok)
			if err1 != nil || err2 != nil {
				t.Fatalf("%s: outgoing tok=%d errs %v/%v", step, tok, err1, err2)
			}
			if fmt.Sprint(relIDsOf(mo)) != fmt.Sprint(relIDsOf(do)) {
				t.Fatalf("%s: outgoing tok=%d mem=%v disk=%v", step, tok, relIDsOf(mo), relIDsOf(do))
			}
			mi, err1 := mem.IncomingRelationships(hub, tok)
			di, err2 := disk.IncomingRelationships(hub, tok)
			if err1 != nil || err2 != nil {
				t.Fatalf("%s: incoming tok=%d errs %v/%v", step, tok, err1, err2)
			}
			if fmt.Sprint(relIDsOf(mi)) != fmt.Sprint(relIDsOf(di)) {
				t.Fatalf("%s: incoming tok=%d mem=%v disk=%v", step, tok, relIDsOf(mi), relIDsOf(di))
			}
			md, _ := mem.OutgoingDegree(hub, tok)
			dd, _ := disk.OutgoingDegree(hub, tok)
			if md != dd {
				t.Fatalf("%s: out degree tok=%d mem=%d disk=%d", step, tok, md, dd)
			}
			md, _ = mem.IncomingDegree(hub, tok)
			dd, _ = disk.IncomingDegree(hub, tok)
			if md != dd {
				t.Fatalf("%s: in degree tok=%d mem=%d disk=%d", step, tok, md, dd)
			}
		}
		// Batched + streaming arms on a spoke sample.
		for _, bs := range []*Store{} {
			_ = bs
		}
		mm, err1 := mem.OutgoingRelationshipsForNodes(spokes[:5], 0)
		dm, err2 := disk.OutgoingRelationshipsForNodes(spokes[:5], 0)
		if err1 != nil || err2 != nil {
			t.Fatalf("%s: forNodes errs %v/%v", step, err1, err2)
		}
		if len(mm) != len(dm) {
			t.Fatalf("%s: forNodes sizes mem=%d disk=%d", step, len(mm), len(dm))
		}
		var streamed []int64
		if err := disk.ForEachOutgoingRel(hub, knows, func(r *types.Relationship) bool {
			streamed = append(streamed, int64(r.ID()))
			return true
		}); err != nil {
			t.Fatalf("%s: ForEachOutgoingRel: %v", step, err)
		}
		mo, _ := mem.OutgoingRelationships(hub, knows)
		sort.Slice(streamed, func(i, j int) bool { return streamed[i] < streamed[j] })
		if fmt.Sprint(streamed) != fmt.Sprint(relIDsOf(mo)) {
			t.Fatalf("%s: streamed=%v want %v", step, streamed, relIDsOf(mo))
		}
	}

	compare("pre-flush") // pending overlay: nothing flushed yet
	for _, bs := range []*Store{mem, disk} {
		if err := bs.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	compare("post-flush")

	// Delete a few rels (one direction each), compare pre- and post-flush.
	for _, bs := range []*Store{mem, disk} {
		if err := bs.DeleteRelationship(types.RelID(1003)); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if err := bs.DeleteRelationship(types.RelID(2007)); err != nil {
			t.Fatalf("delete: %v", err)
		}
	}
	compare("pre-flush delete") // pending delete overlay
	for _, bs := range []*Store{mem, disk} {
		if err := bs.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	compare("post-delete-flush")
}

// TestAdjacencyOnDisk_MapsStayEmpty pins the RAM win: after writes, scans,
// and deletes, the in-memory adjacency maps hold NOTHING in disk mode.
func TestAdjacencyOnDisk_MapsStayEmpty(t *testing.T) {
	t.Parallel()
	_, disk, hub, _ := seedAdjStores(t, 8)
	if err := disk.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if _, err := disk.OutgoingRelationships(hub, 0); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := disk.DeleteRelationship(types.RelID(1002)); err != nil {
		t.Fatalf("delete: %v", err)
	}

	disk.idxMu.RLock()
	entries := 0
	for _, set := range disk.outIdx {
		entries += len(set)
	}
	for _, set := range disk.inIdx {
		entries += len(set)
	}
	disk.idxMu.RUnlock()
	if entries != 0 {
		t.Fatalf("disk mode populated adjacency maps with %d entries", entries)
	}
}

// TestAdjacencyOnDisk_ConnectedNodeDeleteStillRejected pins the guard that
// MUST consult the keyspace: with empty RAM maps, a plain DeleteNode on a
// connected node would otherwise slip through and leave dangling rels.
func TestAdjacencyOnDisk_ConnectedNodeDeleteStillRejected(t *testing.T) {
	t.Parallel()
	_, disk, hub, _ := seedAdjStores(t, 3)

	if err := disk.DeleteNode(hub); err == nil {
		t.Fatal("DeleteNode on connected node accepted in disk mode (pre-flush)")
	}
	if err := disk.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := disk.DeleteNode(hub); err == nil {
		t.Fatal("DeleteNode on connected node accepted in disk mode (post-flush)")
	}
}

// TestAdjacencyOnDisk_CascadeDeleteParity pins cascade deletion: the
// cascade collects the doomed node's rel IDs through the disk arm; both
// modes must remove the same relationships and leave the same survivors.
func TestAdjacencyOnDisk_CascadeDeleteParity(t *testing.T) {
	t.Parallel()
	mem, disk, hub, spokes := seedAdjStores(t, 6)
	for _, bs := range []*Store{mem, disk} {
		if err := bs.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
		if err := bs.DeleteNodeCascade(hub); err != nil {
			t.Fatalf("cascade: %v", err)
		}
	}
	mc, _ := mem.RelationshipCount()
	dc, _ := disk.RelationshipCount()
	if mc != dc || dc != 0 {
		t.Fatalf("post-cascade rel counts mem=%d disk=%d, want 0", mc, dc)
	}
	// Spokes survive with no dangling adjacency.
	for _, sid := range spokes {
		rels, err := disk.OutgoingRelationships(sid, 0)
		if err != nil || len(rels) != 0 {
			t.Fatalf("spoke %d: outgoing %d rels err %v, want 0", sid, len(rels), err)
		}
	}
}

// TestAdjacencyOnDisk_SurvivesReopen pins persistence: a legacy directory
// written WITHOUT the flag serves adjacency reads when reopened WITH it
// (the keyspaces have always been written — no migration).
func TestAdjacencyOnDisk_SurvivesReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := bs.PutNode(types.NewNode(types.NodeID(1), 1, nil)); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := bs.PutNode(types.NewNode(types.NodeID(2), 1, nil)); err != nil {
		t.Fatalf("put: %v", err)
	}
	for i := 0; i < 5; i++ {
		r := types.NewRelationship(types.RelID(10+i), 1, types.NodeID(1), types.NodeID(2))
		if err := bs.PutRelationship(r); err != nil {
			t.Fatalf("put rel: %v", err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	bs2, err := New(Config{Dir: dir, AdjacencyIndexOnDisk: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { bs2.Close() })

	rels, err := bs2.OutgoingRelationships(types.NodeID(1), 0)
	if err != nil || len(rels) != 5 {
		t.Fatalf("reopened outgoing: %d rels err %v, want 5", len(rels), err)
	}
	deg, err := bs2.IncomingDegree(types.NodeID(2), 1)
	if err != nil || deg != 5 {
		t.Fatalf("reopened degree: %d err %v, want 5", deg, err)
	}
	bs2.idxMu.RLock()
	mapLen := len(bs2.outIdx) + len(bs2.inIdx)
	bs2.idxMu.RUnlock()
	if mapLen != 0 {
		t.Fatalf("reopened disk mode kept %d adjacency map entries", mapLen)
	}
}
