package badger

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestPurgeNodesByIDs_FreesRAM (PURGEIDS, PURGERESIDUE): at two sizes the named
// nodes leave with their edges and whole history in PurgeByIDsChunk calls, a
// survivor of the same label stays, and the history presence and stamps
// sidecars keep no entry of a purged node or edge (a plain delete leaves one
// "unknown" entry per ID). More than PurgeByIDsChunk IDs is refused.
func TestPurgeNodesByIDs_FreesRAM(t *testing.T) {
	t.Parallel()
	const label, other, relType = uint16(10), uint16(11), uint16(5)
	for _, n := range []int{300, 600} {
		bs := newTestBadgerStore(t)
		hub := types.NodeID(idAtTimeField(1000, 0x7FFF))
		if err := bs.PutNode(types.NewNode(hub, other, nil)); err != nil {
			t.Fatal(err)
		}
		survivor := types.NodeID(idAtTimeField(3000, 1))
		if err := bs.PutNode(types.NewNode(survivor, label, nil)); err != nil {
			t.Fatal(err)
		}
		ids := make([]types.NodeID, 0, n)
		for i := range n {
			id := types.NodeID(idAtTimeField(2000+uint64(i), 1))
			cur := types.NewNode(id, label, nil)
			cur.SetVersion(1)
			if err := bs.PutNode(cur); err != nil {
				t.Fatal(err)
			}
			hist := types.NewNode(id, label, nil)
			hist.SetVersion(0)
			if err := bs.PutNodeVersion(id, 0, hist); err != nil {
				t.Fatal(err)
			}
			rid := types.RelID(idAtTimeField(2000+uint64(i), 2))
			if err := bs.PutRelationshipCoLocated(types.NewRelationship(rid, relType, id, hub)); err != nil {
				t.Fatal(err)
			}
			rh := types.NewRelationship(rid, relType, id, hub)
			rh.SetVersion(0)
			if err := bs.PutRelVersion(rid, 0, rh); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		// Build the four sidecars.
		if _, err := bs.HasNodeHistory(ids[0]); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := bs.NodeHistoryStamps(ids[0]); err != nil {
			t.Fatal(err)
		}
		if _, err := bs.HasRelHistory(types.RelID(idAtTimeField(2000, 2))); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := bs.RelHistoryStamps(types.RelID(idAtTimeField(2000, 2))); err != nil {
			t.Fatal(err)
		}
		if p, s := bs.HistoryPresenceStats(), bs.HistoryStampsStats(); p.NodeIDs != n || p.RelIDs != n || s.NodeIDs != n || s.RelIDs != n {
			t.Fatalf("n=%d before: presence %+v stamps %+v, want %d each", n, p, s, n)
		}

		if _, err := bs.PurgeNodesByIDs(ids, nil); n > storecontract.PurgeByIDsChunk && !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
			t.Fatalf("n=%d: %d IDs in one call: err %v, want a refusal", n, n, err)
		}
		nodes, rels, calls := 0, 0, 0
		for rest := ids; len(rest) > 0; calls++ {
			k := min(len(rest), storecontract.PurgeByIDsChunk)
			if k == storecontract.PurgeByIDsChunk {
				if _, err := bs.PurgeNodesByIDs(append(rest[:k:k], types.NodeID(idAtTimeField(9000, 3))), nil); err == nil {
					t.Fatal("PurgeByIDsChunk+1 IDs accepted")
				}
			}
			res, err := bs.PurgeNodesByIDs(rest[:k], nil)
			if err != nil {
				t.Fatal(err)
			}
			if res.NodesPurged != k {
				t.Fatalf("n=%d call %d: purged %d of %d", n, calls, res.NodesPurged, k)
			}
			nodes, rels, rest = nodes+res.NodesPurged, rels+res.RelsPurged, rest[k:]
		}
		if want := (n + storecontract.PurgeByIDsChunk - 1) / storecontract.PurgeByIDsChunk; nodes != n || rels != n || calls != want {
			t.Fatalf("n=%d: purged %d nodes %d rels in %d calls, want %d %d %d", n, nodes, rels, calls, n, n, want)
		}
		for _, id := range ids {
			if _, err := bs.GetNode(id); !errors.Is(err, ErrNodeNotFound) {
				t.Fatalf("purged node survived: %v", err)
			}
			if h, _ := bs.GetNodeHistory(id); len(h) != 0 {
				t.Fatalf("purged node kept %d history rows", len(h))
			}
		}
		if _, err := bs.GetNode(survivor); err != nil {
			t.Fatalf("survivor: %v", err)
		}
		if in, _ := bs.IncomingRelationships(hub, 0); len(in) != 0 {
			t.Fatalf("hub keeps %d incoming edges", len(in))
		}
		if p, s := bs.HistoryPresenceStats(), bs.HistoryStampsStats(); p.NodeIDs != 0 || p.RelIDs != 0 || s.NodeIDs != 0 || s.RelIDs != 0 {
			t.Fatalf("n=%d after: presence %+v stamps %+v, want no entry", n, p, s)
		}
		// Idempotent: the IDs again purge nothing and leave nothing.
		if res, err := bs.PurgeNodesByIDs(ids[:1], nil); err != nil || res.NodesPurged != 0 {
			t.Fatalf("re-purge: %+v %v", res, err)
		}
	}
}

// TestPurgeNodesByIDs_DuringSidecarBuild (guard): a purge that lands between a
// sidecar build's scan and its install must not leave the scanned rows as the
// purged ID's answer; the build's install skips the IDs noted since the scan.
func TestPurgeNodesByIDs_DuringSidecarBuild(t *testing.T) {
	t.Parallel()
	const label = uint16(10)
	for _, stamps := range []bool{false, true} {
		bs := newTestBadgerStore(t)
		id := types.NodeID(idAtTimeField(2000, 1))
		cur := types.NewNode(id, label, nil)
		cur.SetVersion(1)
		if err := bs.PutNode(cur); err != nil {
			t.Fatal(err)
		}
		hist := types.NewNode(id, label, nil)
		hist.SetVersion(0)
		if err := bs.PutNodeVersion(id, 0, hist); err != nil {
			t.Fatal(err)
		}
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		purge := func() {
			if res, err := bs.PurgeNodesByIDs([]types.NodeID{id}, nil); err != nil || res.NodesPurged != 1 {
				t.Errorf("purge during the build: %+v %v", res, err)
			}
		}
		var has bool
		var err error
		if stamps {
			bs.historyStampsBuildHook = purge
			_, _, has, err = bs.NodeHistoryStamps(id)
			bs.historyStampsBuildHook = nil
			if err == nil && !has {
				_, _, has, err = bs.NodeHistoryStamps(id)
			}
		} else {
			bs.historyPresenceBuildHook = purge
			has, err = bs.HasNodeHistory(id)
			bs.historyPresenceBuildHook = nil
			if err == nil && !has {
				has, err = bs.HasNodeHistory(id)
			}
		}
		if err != nil || has {
			t.Fatalf("stamps=%v: purged node reads history present=%v err=%v", stamps, has, err)
		}
	}
}
