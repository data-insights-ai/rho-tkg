package memory

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// collectLabelMembers drains ForEachLabelTxMember into a map.
func collectLabelMembers(t *testing.T, ms *Store, tok uint16) map[types.NodeID]types.Instant {
	t.Helper()
	out := make(map[types.NodeID]types.Instant)
	if err := ms.ForEachLabelTxMember(tok, func(id types.NodeID, firstTx types.Instant) bool {
		out[id] = firstTx
		return true
	}); err != nil {
		t.Fatalf("ForEachLabelTxMember: %v", err)
	}
	return out
}

// TestMemoryLabelTxMembership_LazyBuildAndAppendOnly verifies the sidecar is
// built lazily from current+history state on first use and that removal/delete
// never drops a member (append-only), while a new label acquisition after the
// build is captured incrementally.
func TestMemoryLabelTxMembership_LazyBuildAndAppendOnly(t *testing.T) {
	ms := New()
	defer ms.Close()

	const tokL uint16 = 1
	const tokK uint16 = 2

	// Pre-build state: node with L (and K so L is removable through the label
	// door contract, not exercised here — we mutate the store directly).
	n1 := mustNode(t, 101, []uint16{tokL, tokK}, 10)
	if err := ms.PutNode(n1); err != nil {
		t.Fatalf("put n1: %v", err)
	}
	n2 := mustNode(t, 102, []uint16{tokL}, 20)
	if err := ms.PutNode(n2); err != nil {
		t.Fatalf("put n2: %v", err)
	}

	// First use builds lazily and must include both current L members.
	members := collectLabelMembers(t, ms, tokL)
	if len(members) != 2 {
		t.Fatalf("built members = %d, want 2 (%v)", len(members), members)
	}
	if _, ok := members[types.NodeID(101)]; !ok {
		t.Fatal("n1 missing from built L members")
	}

	// Delete n2 — append-only means it stays a historical L member.
	if err := ms.DeleteNode(types.NodeID(102)); err != nil {
		t.Fatalf("delete n2: %v", err)
	}
	members = collectLabelMembers(t, ms, tokL)
	if _, ok := members[types.NodeID(102)]; !ok {
		t.Fatal("deleted n2 dropped from L members (append-only violated)")
	}

	// A new L node after the build is captured incrementally with its own
	// firstTxFrom stamp.
	n3 := mustNode(t, 103, []uint16{tokL}, 30)
	if err := ms.PutNode(n3); err != nil {
		t.Fatalf("put n3: %v", err)
	}
	members = collectLabelMembers(t, ms, tokL)
	if tx, ok := members[types.NodeID(103)]; !ok {
		t.Fatal("post-build n3 not captured incrementally")
	} else if tx != 30 {
		t.Fatalf("n3 firstTxFrom = %d, want 30", tx)
	}

	// Clear drops the sidecar; a rebuild after Clear sees an empty store.
	if err := ms.Clear(); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if m := collectLabelMembers(t, ms, tokL); len(m) != 0 {
		t.Fatalf("after Clear, L members = %d, want 0", len(m))
	}
}

// mustNode builds a node with the given raw label tokens and TxFrom stamp.
func mustNode(t *testing.T, id int64, tokens []uint16, txFrom types.Instant) *types.Node {
	t.Helper()
	var primary uint16
	var extra []uint16
	if len(tokens) > 0 {
		primary = tokens[0]
		extra = tokens[1:]
	}
	n := types.NewNode(types.NodeID(id), primary, extra)
	n.SetTemporal(&types.TemporalMetadata{TxFrom: txFrom, ValidFrom: txFrom})
	return n
}
