package store_test

import (
	"errors"
	"fmt"
	"sort"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 8: the store half of the property membership sidecars
// (RelPropertyTxMembershipCapability / NodePropertyTxMembershipCapability /
// PropertyTxMembershipStatsCapability), driven through every write door of the
// Store contract on every backend that offers them.
//
// Every door writes a value no other door writes, so a door that forgets to
// record its row fails on its own value. After every (re)build the sidecar
// must EQUAL the oracle computed from the stored rows (current + history);
// between builds it must be a superset of it with the same or a lower first
// transaction time.

type propTxBackend struct {
	name string
	open func(t *testing.T) storecontract.Store
}

func propTxBackends() []propTxBackend {
	return []propTxBackend{
		{name: "memory", open: func(t *testing.T) storecontract.Store { return memory.New() }},
		{name: "badger", open: func(t *testing.T) storecontract.Store {
			bs, err := badger.New(badger.Config{InMemory: true})
			if err != nil {
				t.Fatalf("badger.New: %v", err)
			}
			return bs
		}},
		{name: "badger-delta", open: func(t *testing.T) storecontract.Store {
			bs, err := badger.New(badger.Config{InMemory: true, HistoryDeltaEncoding: true, HistoryAnchorInterval: 2})
			if err != nil {
				t.Fatalf("badger.New: %v", err)
			}
			return bs
		}},
		{name: "sharded", open: func(t *testing.T) storecontract.Store {
			st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				t.Fatalf("sharded.New: %v", err)
			}
			return st
		}},
	}
}

type propTxCaps interface {
	storecontract.RelPropertyIndexCapability
	storecontract.RelPropertyTxMembershipCapability
	storecontract.NodePropertyTxMembershipCapability
	storecontract.PropertyTxMembershipStatsCapability
}

func propTxCapsOf(t *testing.T, st storecontract.Store) propTxCaps {
	t.Helper()
	c, ok := st.(propTxCaps)
	if !ok {
		t.Fatalf("%T does not implement the property membership capabilities", st)
	}
	return c
}

const (
	ptxTypeT  = uint16(11)
	ptxTypeU  = uint16(12)
	ptxLabelL = uint16(21)
	ptxLabelM = uint16(22)
	ptxLabelP = uint16(23)
)

func ptxVK(v int64) string { return types.IndexablePropertyValueKey(v) }

func ptxRel(id int64, typ uint16, seat int64, version uint32, tx types.Instant) *types.Relationship {
	r := types.NewRelationship(types.RelID(id), typ, types.NodeID(1), types.NodeID(2))
	_ = r.SetProperty("seat", seat)
	r.SetVersion(version)
	r.SetTemporal(&types.TemporalMetadata{TxFrom: tx})
	return r
}

func ptxNode(id int64, labels []uint16, seat int64, version uint32, tx types.Instant) *types.Node {
	n := types.NewNode(types.NodeID(id), labels[0], labels[1:])
	_ = n.SetProperty("seat", seat)
	n.SetVersion(version)
	n.SetTemporal(&types.TemporalMetadata{TxFrom: tx})
	return n
}

func relMembers(t *testing.T, c propTxCaps, typ uint16, vk string) map[types.RelID]types.Instant {
	t.Helper()
	out := map[types.RelID]types.Instant{}
	if err := c.ForEachRelPropertyTxMember(typ, "seat", vk, func(id types.RelID, tx types.Instant) bool {
		out[id] = tx
		return true
	}); err != nil {
		t.Fatalf("ForEachRelPropertyTxMember(%d, %s): %v", typ, vk, err)
	}
	return out
}

func nodeMembers(t *testing.T, c propTxCaps, label uint16, vk string) map[types.NodeID]types.Instant {
	t.Helper()
	out := map[types.NodeID]types.Instant{}
	if err := c.ForEachNodePropertyTxMember(label, "seat", vk, func(id types.NodeID, tx types.Instant) bool {
		out[id] = tx
		return true
	}); err != nil {
		t.Fatalf("ForEachNodePropertyTxMember(%d, %s): %v", label, vk, err)
	}
	return out
}

// relOracle computes {value key: {rel: lowest TxFrom}} from every stored row
// (current + history) of the given rels under type typ.
func relOracle(t *testing.T, st storecontract.Store, typ uint16, ids []types.RelID) map[string]map[types.RelID]types.Instant {
	t.Helper()
	out := map[string]map[types.RelID]types.Instant{}
	note := func(r *types.Relationship) {
		if !r.HasTypeTokenRaw(typ) {
			return
		}
		vk, ok := r.IndexablePropertyValueKey("seat")
		if !ok {
			return
		}
		var tx types.Instant
		if tm := r.Temporal(); tm != nil {
			tx = tm.TxFrom
		}
		m := out[vk]
		if m == nil {
			m = map[types.RelID]types.Instant{}
			out[vk] = m
		}
		if prev, seen := m[r.ID()]; !seen || (prev != 0 && (tx == 0 || tx < prev)) {
			m[r.ID()] = tx
		}
	}
	for _, id := range ids {
		if cur, err := st.GetRelationship(id); err == nil {
			note(cur)
		} else if !errors.Is(err, storecontract.ErrRelNotFound) {
			t.Fatalf("GetRelationship(%d): %v", id, err)
		}
		hist, err := st.GetRelHistory(id)
		if err != nil {
			t.Fatalf("GetRelHistory(%d): %v", id, err)
		}
		for _, r := range hist {
			note(r)
		}
	}
	return out
}

func nodeOracle(t *testing.T, st storecontract.Store, label uint16, ids []types.NodeID) map[string]map[types.NodeID]types.Instant {
	t.Helper()
	out := map[string]map[types.NodeID]types.Instant{}
	note := func(n *types.Node) {
		if !n.HasLabelTokenRaw(label) {
			return
		}
		vk, ok := n.IndexablePropertyValueKey("seat")
		if !ok {
			return
		}
		var tx types.Instant
		if tm := n.Temporal(); tm != nil {
			tx = tm.TxFrom
		}
		m := out[vk]
		if m == nil {
			m = map[types.NodeID]types.Instant{}
			out[vk] = m
		}
		if prev, seen := m[n.ID()]; !seen || (prev != 0 && (tx == 0 || tx < prev)) {
			m[n.ID()] = tx
		}
	}
	for _, id := range ids {
		if cur, err := st.GetNode(id); err == nil {
			note(cur)
		} else if !errors.Is(err, storecontract.ErrNodeNotFound) {
			t.Fatalf("GetNode(%d): %v", id, err)
		}
		hist, err := st.GetNodeHistory(id)
		if err != nil {
			t.Fatalf("GetNodeHistory(%d): %v", id, err)
		}
		for _, n := range hist {
			note(n)
		}
	}
	return out
}

func fmtMembers[ID ~int64](m map[ID]types.Instant) string {
	ids := make([]ID, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	s := "{"
	for i, id := range ids {
		if i > 0 {
			s += " "
		}
		s += fmt.Sprintf("%d:%d", int64(id), m[id])
	}
	return s + "}"
}

// assertRelSidecar checks every oracle value: exact equality when exact,
// else superset with a first tx no later than the oracle's.
func assertRelSidecar(t *testing.T, st storecontract.Store, c propTxCaps, ids []types.RelID, exact bool, stage string) {
	t.Helper()
	for vk, want := range relOracle(t, st, ptxTypeT, ids) {
		got := relMembers(t, c, ptxTypeT, vk)
		for id, tx := range want {
			g, ok := got[id]
			if !ok || (exact && g != tx) || (!exact && g != 0 && (tx == 0 || g > tx)) {
				t.Fatalf("%s value %s: sidecar %s, oracle %s", stage, vk, fmtMembers(got), fmtMembers(want))
			}
		}
		if exact && len(got) != len(want) {
			t.Fatalf("%s value %s: sidecar %s over-reports after a fresh build, oracle %s", stage, vk, fmtMembers(got), fmtMembers(want))
		}
	}
}

func assertNodeSidecar(t *testing.T, st storecontract.Store, c propTxCaps, ids []types.NodeID, exact bool, stage string) {
	t.Helper()
	for vk, want := range nodeOracle(t, st, ptxLabelL, ids) {
		got := nodeMembers(t, c, ptxLabelL, vk)
		for id, tx := range want {
			g, ok := got[id]
			if !ok || (exact && g != tx) || (!exact && g != 0 && (tx == 0 || g > tx)) {
				t.Fatalf("%s value %s: sidecar %s, oracle %s", stage, vk, fmtMembers(got), fmtMembers(want))
			}
		}
		if exact && len(got) != len(want) {
			t.Fatalf("%s value %s: sidecar %s over-reports after a fresh build, oracle %s", stage, vk, fmtMembers(got), fmtMembers(want))
		}
	}
}

func mustOK(t *testing.T, what string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

// TestRelPropertyTxMembersEveryDoor drives each rel write door after the lazy
// build with a value only it writes. Faulty implementations caught: a door
// that does not record (its value has no member), recording only the current
// value (the history doors' values), dropping deleted rels (tombstone value),
// leaking another type (U), building from current rows only (the history rows
// written before the build), not keeping the lowest TxFrom.
func TestRelPropertyTxMembersEveryDoor(t *testing.T) {
	for _, be := range propTxBackends() {
		t.Run(be.name, func(t *testing.T) {
			st := be.open(t)
			defer func() { _ = st.Close() }()
			c := propTxCapsOf(t, st)
			mustOK(t, "put node 1", st.PutNode(ptxNode(1, []uint16{ptxLabelP}, 0, 0, 1)))
			mustOK(t, "put node 2", st.PutNode(ptxNode(2, []uint16{ptxLabelP}, 0, 0, 1)))

			// No index declared for T yet: not found, before any callback.
			called := false
			err := c.ForEachRelPropertyTxMember(ptxTypeT, "seat", ptxVK(1), func(types.RelID, types.Instant) bool { called = true; return true })
			if !errors.Is(err, storecontract.ErrIndexNotFound) || called {
				t.Fatalf("undeclared index: err=%v called=%v, want ErrIndexNotFound and no callback", err, called)
			}
			mustOK(t, "create index", c.CreateRelPropertyIndex(ptxTypeT, "seat"))

			// Before the build: current rows and history rows of every shape.
			mustOK(t, "put 101", st.PutRelationship(ptxRel(101, ptxTypeT, 1, 0, 100)))
			mustOK(t, "put 102", st.PutRelationship(ptxRel(102, ptxTypeT, 1, 0, 110)))
			mustOK(t, "replace 102", st.ReplaceRelWithHistory(ptxRel(102, ptxTypeT, 2, 1, 120), 0, ptxRel(102, ptxTypeT, 1, 0, 110)))
			mustOK(t, "put 103 (type U)", st.PutRelationship(ptxRel(103, ptxTypeU, 1, 0, 130)))
			mustOK(t, "put 104", st.PutRelationship(ptxRel(104, ptxTypeT, 1, 0, 140)))
			tomb := ptxRel(104, ptxTypeT, 1, 0, 140)
			tomb.Temporal().DeletedAt, tomb.Temporal().TxTo = 150, 150
			mustOK(t, "delete 104 with history", st.DeleteRelWithHistory(104, 0, tomb))
			mustOK(t, "put 105", st.PutRelationship(ptxRel(105, ptxTypeT, 3, 1, 200)))
			mustOK(t, "put version 105/0", st.PutRelVersion(105, 0, ptxRel(105, ptxTypeT, 1, 0, 50)))
			ids := []types.RelID{101, 102, 103, 104, 105}

			if got := relMembers(t, c, ptxTypeT, ptxVK(1)); fmtMembers(got) != "{101:100 102:110 104:140 105:50}" {
				t.Fatalf("lazy build members(1) = %s", fmtMembers(got))
			}
			// U carries the same values but has no index: never served.
			if err := c.ForEachRelPropertyTxMember(ptxTypeU, "seat", ptxVK(1), func(types.RelID, types.Instant) bool { return true }); !errors.Is(err, storecontract.ErrIndexNotFound) {
				t.Fatalf("type U without index: %v, want ErrIndexNotFound", err)
			}
			assertRelSidecar(t, st, c, ids, true, "after lazy build")

			// After the build: one value per door.
			mustOK(t, "put 106", st.PutRelationship(ptxRel(106, ptxTypeT, 1001, 0, 300)))
			mustOK(t, "batch 107/108", st.PutRelationshipsBatch([]*types.Relationship{ptxRel(107, ptxTypeT, 1002, 0, 310), ptxRel(108, ptxTypeU, 1002, 0, 311)}))
			mustOK(t, "replace 101 in place", st.ReplaceRelationship(ptxRel(101, ptxTypeT, 1003, 0, 100)))
			mustOK(t, "replace 102 with history", st.ReplaceRelWithHistory(ptxRel(102, ptxTypeT, 1004, 2, 320), 1, ptxRel(102, ptxTypeT, 1005, 1, 120)))
			mustOK(t, "put 109", st.PutRelationship(ptxRel(109, ptxTypeT, 7, 0, 330)))
			tomb = ptxRel(109, ptxTypeT, 1006, 0, 330)
			tomb.Temporal().DeletedAt, tomb.Temporal().TxTo = 340, 340
			mustOK(t, "delete 109 with history", st.DeleteRelWithHistory(109, 0, tomb))
			mustOK(t, "put version 105/2", st.PutRelVersion(105, 2, ptxRel(105, ptxTypeT, 1007, 2, 350)))
			mustOK(t, "put node 3", st.PutNode(ptxNode(3, []uint16{ptxLabelP}, 0, 0, 1)))
			r110 := types.NewRelationship(types.RelID(110), ptxTypeT, types.NodeID(3), types.NodeID(2))
			_ = r110.SetProperty("seat", int64(8))
			r110.SetTemporal(&types.TemporalMetadata{TxFrom: 360})
			mustOK(t, "put 110", st.PutRelationship(r110))
			r110t := r110.DeepCopy()
			_ = r110t.SetProperty("seat", int64(1008))
			r110t.Temporal().DeletedAt, r110t.Temporal().TxTo = 370, 370
			n3t := ptxNode(3, []uint16{ptxLabelP}, 0, 0, 1)
			n3t.Temporal().DeletedAt, n3t.Temporal().TxTo = 370, 370
			mustOK(t, "delete node 3 with history", st.DeleteNodeWithHistory(3, 0, n3t, []storecontract.RelTombstone{{ID: 110, PrevVersion: 0, Tombstone: r110t}}))
			mustOK(t, "plain delete 106", st.DeleteRelationship(106))
			mustOK(t, "truncate 102", st.TruncateRelHistory(102, 1))
			ids = append(ids, 106, 107, 108, 109, 110)

			for _, door := range []struct {
				name  string
				value int64
				id    types.RelID
				tx    types.Instant
			}{
				{"PutRelationship (then deleted)", 1001, 106, 300},
				{"PutRelationshipsBatch", 1002, 107, 310},
				{"ReplaceRelationship", 1003, 101, 100},
				{"ReplaceRelWithHistory current", 1004, 102, 320},
				{"ReplaceRelWithHistory prevState", 1005, 102, 120},
				{"DeleteRelWithHistory tombstone", 1006, 109, 330},
				{"PutRelVersion", 1007, 105, 350},
				{"DeleteNodeWithHistory rel tombstone", 1008, 110, 360},
			} {
				got := relMembers(t, c, ptxTypeT, ptxVK(door.value))
				if tx, ok := got[door.id]; !ok || tx != door.tx || len(got) != 1 {
					t.Fatalf("door %s: members(%d) = %s, want {%d:%d}", door.name, door.value, fmtMembers(got), door.id, door.tx)
				}
			}
			// Append-only: value 1 keeps every member, also 101 (replaced in
			// place: no stored row carries 1 any more) and 104 (deleted).
			if got := relMembers(t, c, ptxTypeT, ptxVK(1)); fmtMembers(got) != "{101:100 102:110 104:140 105:50}" {
				t.Fatalf("append-only members(1) = %s", fmtMembers(got))
			}
			assertRelSidecar(t, st, c, ids, false, "incremental")

			// Early stop.
			calls := 0
			mustOK(t, "early stop", c.ForEachRelPropertyTxMember(ptxTypeT, "seat", ptxVK(1), func(types.RelID, types.Instant) bool { calls++; return false }))
			if calls != 1 {
				t.Fatalf("fn returned false but was called %d times", calls)
			}

			stats, err := c.PropertyTxMembershipStats()
			mustOK(t, "stats", err)
			if stats.RelSidecars < 1 || stats.RelPostings < 12 || stats.Builds < 1 { // sharded: one per shard
				t.Fatalf("stats after build = %+v", stats)
			}

			// Drop: not found. Re-create over existing history: the rebuild is
			// exact again — no stored row carries 1 for 101 (replaced in place)
			// or 102 (its v0 was truncated) any more.
			mustOK(t, "drop index", c.DropRelPropertyIndex(ptxTypeT, "seat"))
			if err := c.ForEachRelPropertyTxMember(ptxTypeT, "seat", ptxVK(1), func(types.RelID, types.Instant) bool { return true }); !errors.Is(err, storecontract.ErrIndexNotFound) {
				t.Fatalf("after drop: %v, want ErrIndexNotFound", err)
			}
			mustOK(t, "re-create index", c.CreateRelPropertyIndex(ptxTypeT, "seat"))
			assertRelSidecar(t, st, c, ids, true, "after re-create")
			if got := relMembers(t, c, ptxTypeT, ptxVK(1)); fmtMembers(got) != "{104:140 105:50}" {
				t.Fatalf("rebuilt members(1) = %s", fmtMembers(got))
			}

			// Clear: nothing is left.
			mustOK(t, "clear", st.Clear())
			if err := c.ForEachRelPropertyTxMember(ptxTypeT, "seat", ptxVK(1), func(types.RelID, types.Instant) bool { return true }); err != nil && !errors.Is(err, storecontract.ErrIndexNotFound) {
				t.Fatalf("after Clear: %v", err)
			} else if err == nil {
				if got := relMembers(t, c, ptxTypeT, ptxVK(1)); len(got) != 0 {
					t.Fatalf("after Clear members(1) = %s", fmtMembers(got))
				}
			}

			// Argument and lifecycle errors.
			if err := c.ForEachRelPropertyTxMember(ptxTypeT, "seat", ptxVK(1), nil); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
				t.Fatalf("nil fn: %v, want ErrInvalidStoreMutation", err)
			}
			if err := c.ForEachRelPropertyTxMember(0, "seat", ptxVK(1), func(types.RelID, types.Instant) bool { return true }); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
				t.Fatalf("token 0: %v, want ErrInvalidStoreMutation", err)
			}
			mustOK(t, "close", st.Close())
			if err := c.ForEachRelPropertyTxMember(ptxTypeT, "seat", ptxVK(1), func(types.RelID, types.Instant) bool { return true }); !errors.Is(err, storecontract.ErrStoreClosed) {
				t.Fatalf("closed: %v, want ErrStoreClosed", err)
			}
			if _, err := c.PropertyTxMembershipStats(); !errors.Is(err, storecontract.ErrStoreClosed) {
				t.Fatalf("closed stats: %v, want ErrStoreClosed", err)
			}
		})
	}
}

// TestNodePropertyTxMembersEveryDoor is the node twin: one value per node
// write door, keyed (label, key). Extra fault caught: recording a row under a
// label it does not carry (M rows, and the label-remove door's updated row).
func TestNodePropertyTxMembersEveryDoor(t *testing.T) {
	for _, be := range propTxBackends() {
		t.Run(be.name, func(t *testing.T) {
			st := be.open(t)
			defer func() { _ = st.Close() }()
			c := propTxCapsOf(t, st)
			pidx, ok := st.(storecontract.PropertyIndexCapability)
			if !ok {
				t.Fatalf("%T has no node property index", st)
			}
			if err := c.ForEachNodePropertyTxMember(ptxLabelL, "seat", ptxVK(1), func(types.NodeID, types.Instant) bool { return true }); !errors.Is(err, storecontract.ErrIndexNotFound) {
				t.Fatalf("undeclared index: %v, want ErrIndexNotFound", err)
			}
			mustOK(t, "create index", pidx.CreatePropertyIndex(ptxLabelL, "seat"))

			L, LM, M := []uint16{ptxLabelL}, []uint16{ptxLabelL, ptxLabelM}, []uint16{ptxLabelM}
			mustOK(t, "put 201", st.PutNode(ptxNode(201, L, 1, 0, 100)))
			mustOK(t, "put 202 (M)", st.PutNode(ptxNode(202, M, 1, 0, 110)))
			mustOK(t, "put 203", st.PutNode(ptxNode(203, L, 1, 0, 120)))
			mustOK(t, "replace 203 with history", st.ReplaceNodeWithHistory(ptxNode(203, L, 2, 1, 130), 0, ptxNode(203, L, 1, 0, 120)))
			mustOK(t, "put 204", st.PutNode(ptxNode(204, L, 1, 0, 140)))
			tomb := ptxNode(204, L, 1, 0, 140)
			tomb.Temporal().DeletedAt, tomb.Temporal().TxTo = 150, 150
			mustOK(t, "delete 204 with history", st.DeleteNodeWithHistory(204, 0, tomb, nil))
			mustOK(t, "put 205", st.PutNode(ptxNode(205, L, 3, 1, 200)))
			mustOK(t, "put version 205/0", st.PutNodeVersion(205, 0, ptxNode(205, L, 1, 0, 50)))
			ids := []types.NodeID{201, 202, 203, 204, 205}
			if got := nodeMembers(t, c, ptxLabelL, ptxVK(1)); fmtMembers(got) != "{201:100 203:120 204:140 205:50}" {
				t.Fatalf("lazy build members(1) = %s", fmtMembers(got))
			}
			assertNodeSidecar(t, st, c, ids, true, "after lazy build")

			mustOK(t, "put 206", st.PutNode(ptxNode(206, L, 2001, 0, 300)))
			mustOK(t, "batch 207/208", st.PutNodesBatch([]*types.Node{ptxNode(207, L, 2002, 0, 310), ptxNode(208, M, 2002, 0, 311)}))
			mustOK(t, "replace 201 in place", st.ReplaceNode(ptxNode(201, L, 2003, 0, 100)))
			mustOK(t, "replace 203 with history", st.ReplaceNodeWithHistory(ptxNode(203, L, 2004, 2, 320), 1, ptxNode(203, L, 2005, 1, 130)))
			// 202 gains L (label-add door), the row carrying both L and 2006.
			mustOK(t, "replace 202 value", st.ReplaceNode(ptxNode(202, M, 2006, 0, 110)))
			mustOK(t, "add label 202", st.AddNodeLabelToken(202, ptxLabelL, ptxNode(202, LM, 2006, 0, 330)))
			// 209 gains L through the history door; its prior row had no L.
			mustOK(t, "put 209 (M)", st.PutNode(ptxNode(209, M, 2007, 0, 335)))
			mustOK(t, "add label 209 with history", st.AddNodeLabelTokenWithHistory(209, ptxLabelL, ptxNode(209, LM, 2007, 1, 340), 0, ptxNode(209, M, 2007, 0, 335)))
			// 210 loses L: the updated row (no L) must not be recorded under L,
			// the prior row (with L) must.
			mustOK(t, "put 210", st.PutNode(ptxNode(210, LM, 2008, 0, 345)))
			mustOK(t, "remove label 210 with history", st.RemoveNodeLabelTokenWithHistory(210, ptxLabelL, ptxNode(210, M, 2009, 1, 350), 0, ptxNode(210, LM, 2008, 0, 345)))
			mustOK(t, "put version 205/2", st.PutNodeVersion(205, 2, ptxNode(205, L, 2010, 2, 355)))
			mustOK(t, "put 211", st.PutNode(ptxNode(211, L, 9, 0, 360)))
			tomb = ptxNode(211, L, 2011, 0, 360)
			tomb.Temporal().DeletedAt, tomb.Temporal().TxTo = 370, 370
			mustOK(t, "delete 211 with history", st.DeleteNodeWithHistory(211, 0, tomb, nil))
			mustOK(t, "plain delete 206", st.DeleteNode(206))
			ids = append(ids, 206, 207, 208, 209, 210, 211)

			for _, door := range []struct {
				name  string
				value int64
				want  string
			}{
				{"PutNode (then deleted)", 2001, "{206:300}"},
				{"PutNodesBatch (M row excluded)", 2002, "{207:310}"},
				{"ReplaceNode", 2003, "{201:100}"},
				{"ReplaceNodeWithHistory current", 2004, "{203:320}"},
				{"ReplaceNodeWithHistory prevState", 2005, "{203:130}"},
				{"AddNodeLabelToken", 2006, "{202:330}"},
				{"AddNodeLabelTokenWithHistory", 2007, "{209:340}"},
				{"RemoveNodeLabelTokenWithHistory prevState", 2008, "{210:345}"},
				{"RemoveNodeLabelTokenWithHistory updated (no L)", 2009, "{}"},
				{"PutNodeVersion", 2010, "{205:355}"},
				{"DeleteNodeWithHistory tombstone", 2011, "{211:360}"},
			} {
				if got := nodeMembers(t, c, ptxLabelL, ptxVK(door.value)); fmtMembers(got) != door.want {
					t.Fatalf("door %s: members(%d) = %s, want %s", door.name, door.value, fmtMembers(got), door.want)
				}
			}
			assertNodeSidecar(t, st, c, ids, false, "incremental")

			mustOK(t, "drop index", pidx.DropPropertyIndex(ptxLabelL, "seat"))
			if err := c.ForEachNodePropertyTxMember(ptxLabelL, "seat", ptxVK(1), func(types.NodeID, types.Instant) bool { return true }); !errors.Is(err, storecontract.ErrIndexNotFound) {
				t.Fatalf("after drop: %v, want ErrIndexNotFound", err)
			}
			mustOK(t, "re-create index", pidx.CreatePropertyIndex(ptxLabelL, "seat"))
			assertNodeSidecar(t, st, c, ids, true, "after re-create")
			stats, err := c.PropertyTxMembershipStats()
			mustOK(t, "stats", err)
			if stats.NodeSidecars < 1 || stats.NodePostings == 0 {
				t.Fatalf("stats = %+v", stats)
			}
			if err := c.ForEachNodePropertyTxMember(ptxLabelL, "seat", ptxVK(1), nil); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
				t.Fatalf("nil fn: %v, want ErrInvalidStoreMutation", err)
			}
			if err := c.ForEachNodePropertyTxMember(0, "seat", ptxVK(1), func(types.NodeID, types.Instant) bool { return true }); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
				t.Fatalf("token 0: %v, want ErrInvalidStoreMutation", err)
			}
			mustOK(t, "close", st.Close())
			if err := c.ForEachNodePropertyTxMember(ptxLabelL, "seat", ptxVK(1), func(types.NodeID, types.Instant) bool { return true }); !errors.Is(err, storecontract.ErrStoreClosed) {
				t.Fatalf("closed: %v, want ErrStoreClosed", err)
			}
		})
	}
}
