package store_test

import (
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 74 (ai-soc): every declared property sidecar of one kind is built by
// ONE store pass. ai-soc's first RelsEffectivePage after an open asked 29 rel
// types and paid 29 full scans (buildRelPropTx, 123 of 259 s CPU).
//
// Faulty builds caught: one scan per (type, key) (Builds grows after the
// first lookup); a pass keyed by type only (T carries two indexed keys, zone
// loses its members); a node row recorded under one label only (LM rows
// belong to L and M); recording an unindexed type or label (W, P carry the
// same values); a pass over current rows only (values 1 and 11 live only in
// history rows). Each sidecar must EQUAL the oracle computed from the stored
// rows, and an index declared after the pass gets its own build.

const (
	opTypeV = uint16(13)
	opTypeW = uint16(14) // carries the same values, no index
)

func opRel(id int64, typ uint16, seat, zone int64, version uint32, tx types.Instant) *types.Relationship {
	r := ptxRel(id, typ, seat, version, tx)
	_ = r.SetProperty("zone", zone)
	return r
}

func opNode(id int64, labels []uint16, seat, zone int64, version uint32, tx types.Instant) *types.Node {
	n := ptxNode(id, labels, seat, version, tx)
	_ = n.SetProperty("zone", zone)
	return n
}

func TestPropertyTxMembersOnePassBuildsEveryDeclaredIndex(t *testing.T) {
	t.Parallel()
	for _, be := range propTxBackends() {
		if be.name == "memory" {
			continue // builds from RAM, no store scan
		}
		t.Run(be.name, func(t *testing.T) {
			st := be.open(t)
			defer func() { _ = st.Close() }()
			c := propTxCapsOf(t, st)
			pidx := st.(storecontract.PropertyIndexCapability)
			relKeys := []struct {
				typ uint16
				key string
			}{{ptxTypeT, "seat"}, {ptxTypeT, "zone"}, {ptxTypeU, "seat"}, {opTypeV, "zone"}}
			nodeKeys := []struct {
				label uint16
				key   string
			}{{ptxLabelL, "seat"}, {ptxLabelL, "zone"}, {ptxLabelM, "zone"}}
			for _, k := range relKeys {
				mustOK(t, "create rel index", c.CreateRelPropertyIndex(k.typ, k.key))
			}
			for _, k := range nodeKeys {
				mustOK(t, "create node index", pidx.CreatePropertyIndex(k.label, k.key))
			}

			L, M, LM, P := []uint16{ptxLabelL}, []uint16{ptxLabelM}, []uint16{ptxLabelL, ptxLabelM}, []uint16{ptxLabelP}
			mustOK(t, "put node 1", st.PutNode(opNode(1, P, 0, 0, 0, 1)))
			mustOK(t, "put node 2", st.PutNode(opNode(2, P, 0, 0, 0, 1)))
			var relIDs []types.RelID
			for i, typ := range []uint16{ptxTypeT, ptxTypeU, opTypeV, opTypeW} {
				base := int64(100 * (i + 1))
				mustOK(t, "put", st.PutRelationship(opRel(base+1, typ, 1, 11, 0, 100)))
				mustOK(t, "replace with history", st.ReplaceRelWithHistory(opRel(base+1, typ, 2, 12, 1, 110), 0, opRel(base+1, typ, 1, 11, 0, 100)))
				mustOK(t, "put", st.PutRelationship(opRel(base+2, typ, 2, 13, 0, 120)))
				tomb := opRel(base+2, typ, 3, 13, 0, 120)
				tomb.Temporal().DeletedAt, tomb.Temporal().TxTo = 130, 130
				mustOK(t, "delete with history", st.DeleteRelWithHistory(types.RelID(base+2), 0, tomb))
				mustOK(t, "put", st.PutRelationship(opRel(base+3, typ, 4, 14, 1, 140)))
				mustOK(t, "put version", st.PutRelVersion(types.RelID(base+3), 0, opRel(base+3, typ, 1, 11, 0, 50)))
				relIDs = append(relIDs, types.RelID(base+1), types.RelID(base+2), types.RelID(base+3))
			}
			var nodeIDs []types.NodeID
			for i, labels := range [][]uint16{L, M, LM, P} {
				base := int64(1000 + 100*i)
				mustOK(t, "put", st.PutNode(opNode(base+1, labels, 1, 11, 0, 100)))
				mustOK(t, "replace with history", st.ReplaceNodeWithHistory(opNode(base+1, labels, 2, 12, 1, 110), 0, opNode(base+1, labels, 1, 11, 0, 100)))
				mustOK(t, "put", st.PutNode(opNode(base+2, labels, 4, 14, 1, 140)))
				mustOK(t, "put version", st.PutNodeVersion(types.NodeID(base+2), 0, opNode(base+2, labels, 1, 11, 0, 50)))
				nodeIDs = append(nodeIDs, types.NodeID(base+1), types.NodeID(base+2))
			}

			builds := func() int64 {
				t.Helper()
				st, err := c.PropertyTxMembershipStats()
				mustOK(t, "stats", err)
				return st.Builds
			}
			checkRel := func(typ uint16, key, stage string) {
				t.Helper()
				oracle := relOracleOf(t, st, typ, key, relIDs)
				if len(oracle) == 0 {
					t.Fatalf("%s: empty oracle for (%d, %s)", stage, typ, key)
				}
				for vk, want := range oracle {
					if got := relMembersOf(t, c, typ, key, vk); fmtMembers(got) != fmtMembers(want) {
						t.Fatalf("%s (%d, %s) value %s: sidecar %s, oracle %s", stage, typ, key, vk, fmtMembers(got), fmtMembers(want))
					}
				}
				if got := relMembersOf(t, c, typ, key, ptxVK(999)); len(got) != 0 {
					t.Fatalf("%s (%d, %s): phantom value has members %s", stage, typ, key, fmtMembers(got))
				}
			}
			checkNode := func(label uint16, key, stage string) {
				t.Helper()
				oracle := nodeOracleOf(t, st, label, key, nodeIDs)
				if len(oracle) == 0 {
					t.Fatalf("%s: empty oracle for (%d, %s)", stage, label, key)
				}
				for vk, want := range oracle {
					if got := nodeMembersOf(t, c, label, key, vk); fmtMembers(got) != fmtMembers(want) {
						t.Fatalf("%s (%d, %s) value %s: sidecar %s, oracle %s", stage, label, key, vk, fmtMembers(got), fmtMembers(want))
					}
				}
			}

			if b := builds(); b != 0 {
				t.Fatalf("builds before any lookup = %d", b)
			}
			checkRel(relKeys[0].typ, relKeys[0].key, "first rel lookup")
			afterRel := builds()
			if afterRel == 0 {
				t.Fatal("the first rel lookup built nothing")
			}
			for _, k := range relKeys {
				checkRel(k.typ, k.key, "every rel index")
			}
			if b := builds(); b != afterRel {
				t.Fatalf("rel lookups on %d declared indexes ran %d build passes, want %d (one pass per store)", len(relKeys), b, afterRel)
			}
			checkNode(nodeKeys[0].label, nodeKeys[0].key, "first node lookup")
			afterNode := builds()
			if afterNode-afterRel != afterRel {
				t.Fatalf("the first node lookup ran %d passes, want %d", afterNode-afterRel, afterRel)
			}
			for _, k := range nodeKeys {
				checkNode(k.label, k.key, "every node index")
			}
			if b := builds(); b != afterNode {
				t.Fatalf("node lookups on %d declared indexes ran %d build passes, want %d", len(nodeKeys), b-afterRel, afterRel)
			}

			// An index declared after the pass builds on its own first use;
			// the built ones are not rebuilt.
			mustOK(t, "create late index", c.CreateRelPropertyIndex(ptxTypeU, "zone"))
			checkRel(ptxTypeU, "zone", "late index")
			if b := builds(); b != afterNode+afterRel {
				t.Fatalf("late index: %d passes, want %d", b-afterNode, afterRel)
			}
			for _, k := range relKeys {
				checkRel(k.typ, k.key, "after the late index")
			}
			if b := builds(); b != afterNode+afterRel {
				t.Fatalf("built indexes were rebuilt: %d passes after the late one", b-afterNode-afterRel)
			}
		})
	}
}
