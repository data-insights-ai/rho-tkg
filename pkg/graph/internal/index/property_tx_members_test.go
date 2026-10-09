package index

import (
	"sort"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func membersMap(ms []TxMember[int]) map[int]types.Instant {
	out := make(map[int]types.Instant, len(ms))
	for _, m := range ms {
		out[m.ID] = m.FirstTx
	}
	return out
}

// TestMergeFirstTx: the lower bound is the minimum, and 0 (unknown: a row a
// TxAt read sees at every pin) is never raised by a later stamped row. A
// plain min, or K1's "replace 0 by the first stamped tx", breaks the 0 cases.
func TestMergeFirstTx(t *testing.T) {
	for _, c := range []struct {
		prev, tx, want types.Instant
	}{
		{5, 3, 3},
		{3, 5, 3},
		{4, 4, 4},
		{0, 7, 0}, // unknown stays unknown
		{7, 0, 0}, // an unstamped row makes the bound unknown
		{0, 0, 0},
	} {
		if got := MergeFirstTx(c.prev, c.tx); got != c.want {
			t.Fatalf("MergeFirstTx(%d, %d) = %d, want %d", c.prev, c.tx, got, c.want)
		}
	}
}

// TestPropertyTxMembersRecord breaks the posting bookkeeping: duplicates must
// not double-count, the inline-to-overflow move must keep the inline member,
// an empty value key is not indexable, and every member keeps its lowest tx.
func TestPropertyTxMembersRecord(t *testing.T) {
	m := NewPropertyTxMembers[int]()
	m.Record("", 1, 10) // not indexable
	if m.Postings() != 0 || m.Members("") != nil {
		t.Fatalf("empty value key recorded: postings=%d", m.Postings())
	}
	m.Record("a", 1, 10)
	m.Record("a", 1, 12) // same member, later row: bound stays 10
	m.Record("a", 1, 8)  // earlier row: bound drops to 8
	if m.Postings() != 1 {
		t.Fatalf("postings after one member = %d, want 1", m.Postings())
	}
	m.Record("a", 2, 20) // overflow starts
	m.Record("a", 2, 15)
	m.Record("a", 3, 0) // unstamped
	m.Record("a", 3, 30)
	m.Record("b", 1, 40) // same entity, another value: its own posting
	if got := m.Postings(); got != 4 {
		t.Fatalf("postings = %d, want 4", got)
	}
	got := membersMap(m.Members("a"))
	want := map[int]types.Instant{1: 8, 2: 15, 3: 0}
	if len(got) != len(want) {
		t.Fatalf("members(a) = %v, want %v", got, want)
	}
	for id, tx := range want {
		if got[id] != tx {
			t.Fatalf("members(a) = %v, want %v", got, want)
		}
	}
	if got := membersMap(m.Members("b")); len(got) != 1 || got[1] != 40 {
		t.Fatalf("members(b) = %v, want {1:40}", got)
	}
	if m.Members("phantom") != nil {
		t.Fatal("phantom value has members")
	}
}

// TestPropertyTxMembersSnapshotIsCallerOwned: mutating the returned slice or
// recording afterwards must not change an earlier snapshot.
func TestPropertyTxMembersSnapshotIsCallerOwned(t *testing.T) {
	m := NewPropertyTxMembers[int]()
	m.Record("a", 1, 5)
	m.Record("a", 2, 6)
	snap := m.Members("a")
	snap[0].FirstTx = 999
	m.Record("a", 3, 7)
	if len(snap) != 2 {
		t.Fatalf("snapshot grew to %d", len(snap))
	}
	again := membersMap(m.Members("a"))
	if again[1] != 5 || again[2] != 6 || again[3] != 7 {
		t.Fatalf("store changed through a snapshot: %v", again)
	}
}

// TestPropertyTxMembersMerge: a merge of a build's scan into postings that a
// concurrent writer already recorded keeps the minimum per member and the
// union of members, in either order.
func TestPropertyTxMembersMerge(t *testing.T) {
	live := NewPropertyTxMembers[int]()
	live.Record("a", 1, 50) // written during the build
	live.Record("a", 9, 60)
	scan := NewPropertyTxMembers[int]()
	scan.Record("a", 1, 10) // the older row the scan found
	scan.Record("a", 2, 20)
	scan.Record("c", 3, 0)
	live.Merge(scan)
	got := membersMap(live.Members("a"))
	if len(got) != 3 || got[1] != 10 || got[2] != 20 || got[9] != 60 {
		t.Fatalf("merged members(a) = %v", got)
	}
	if c := membersMap(live.Members("c")); len(c) != 1 || c[3] != 0 {
		t.Fatalf("merged members(c) = %v", c)
	}
	if live.Postings() != 4 {
		t.Fatalf("postings = %d, want 4", live.Postings())
	}
	if scan.Postings() != 3 {
		t.Fatalf("merge changed its source: %d postings", scan.Postings())
	}
	ids := make([]int, 0)
	for _, m := range live.Members("a") {
		ids = append(ids, m.ID)
	}
	sort.Ints(ids)
	if len(ids) != 3 || ids[0] != 1 || ids[1] != 2 || ids[2] != 9 {
		t.Fatalf("member ids %v", ids)
	}
}

// TestPropertyTxMembersNil: a nil sidecar is empty and inert.
func TestPropertyTxMembersNil(t *testing.T) {
	var m *PropertyTxMembers[int]
	m.Record("a", 1, 1)
	m.Merge(NewPropertyTxMembers[int]())
	NewPropertyTxMembers[int]().Merge(nil)
	if m.Members("a") != nil || m.Postings() != 0 {
		t.Fatal("nil sidecar is not empty")
	}
}
