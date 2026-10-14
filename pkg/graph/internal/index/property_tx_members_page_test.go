package index

import (
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestPropertyTxMembers_MembersAfter — the ordered view of one value (backlog
// 69): MembersAfter refuses (ordered=false) until OrderValue opened it, then
// pages the members above a cursor ascending with their bounds; members
// recorded later (below and above the existing ones, a known member with a
// lower bound) keep the order; an absent value answers empty without a view,
// another value is not opened by the first, a cursor past the end answers
// empty, a limit cuts, a Merge feeds the view.
func TestPropertyTxMembers_MembersAfter(t *testing.T) {
	t.Parallel()
	m := NewPropertyTxMembers[types.RelID]()
	for _, id := range []types.RelID{50, 10, 30} {
		m.Record("a", id, types.Instant(id))
	}
	m.Record("b", 20, 1)
	if _, ok := m.MembersAfter("a", 0, 10); ok {
		t.Fatal("MembersAfter answered before OrderValue")
	}
	if got, ok := m.MembersAfter("zz", 0, 10); !ok || len(got) != 0 {
		t.Fatalf("absent value: %v %v, want empty and answered", got, ok)
	}
	if m.OrderValue("zz") {
		t.Fatal("OrderValue opened a view for an absent value")
	}
	for range 2 { // the second call finds the view open
		if !m.OrderValue("a") {
			t.Fatal("OrderValue(a) = false")
		}
	}
	m.Record("a", 40, 40)
	m.Record("a", 5, 0)
	m.Record("a", 30, 3) // known member, lower bound
	other := NewPropertyTxMembers[types.RelID]()
	other.Record("a", 60, 60)
	m.Merge(other)
	page := func(after types.RelID, limit int) string {
		got, ok := m.MembersAfter("a", after, limit)
		if !ok {
			t.Fatal("not ordered")
		}
		return fmt.Sprint(got)
	}
	for _, c := range []struct {
		after types.RelID
		limit int
		want  string
	}{
		{0, 100, "[{5 0} {10 10} {30 3} {40 40} {50 50} {60 60}]"},
		{10, 2, "[{30 3} {40 40}]"},
		{30, 1, "[{40 40}]"},
		{29, 1, "[{30 3}]"},
		{60, 5, "[]"},
		{1 << 62, 5, "[]"},
		{-7, 1, "[{5 0}]"},
		{0, 0, "[]"},
	} {
		if got := page(c.after, c.limit); got != c.want {
			t.Errorf("after %d limit %d: %s, want %s", c.after, c.limit, got, c.want)
		}
	}
	if got, ok := m.MembersAfter("b", 0, 10); ok {
		t.Fatalf("value b answered without its own OrderValue: %v", got)
	}
	// Many members recorded descending: the view splits chunks and stays
	// ascending across pages.
	big := NewPropertyTxMembers[types.NodeID]()
	big.Record("v", 1, 1)
	big.OrderValue("v")
	for i := 5000; i > 1; i-- {
		big.Record("v", types.NodeID(i*2), 1)
	}
	var prev types.NodeID
	n := 0
	for after := types.NodeID(0); ; {
		got, _ := big.MembersAfter("v", after, 333)
		if len(got) == 0 {
			break
		}
		for _, mb := range got {
			if mb.ID <= prev {
				t.Fatalf("not ascending: %d after %d", mb.ID, prev)
			}
			prev = mb.ID
			n++
		}
		after = got[len(got)-1].ID
	}
	if n != 5000 {
		t.Fatalf("paged %d members, want 5000", n)
	}
	// Ascending members (new exact chunks) mixed with ones below the last
	// (insert into a full chunk, split): one ascending page holds them all.
	asc := NewPropertyTxMembers[types.RelID]()
	asc.Record("v", 2, 1)
	asc.OrderValue("v")
	for i := 2; i <= 1500; i++ {
		asc.Record("v", types.RelID(i*2), 1)
	}
	for _, id := range []types.RelID{1, 3, 1025, 2999} {
		asc.Record("v", id, 1)
	}
	got, _ := asc.MembersAfter("v", 0, 5000)
	if len(got) != 1504 {
		t.Fatalf("ascending view holds %d members, want 1504", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].ID <= got[i-1].ID {
			t.Fatalf("ascending view out of order at %d: %d after %d", i, got[i].ID, got[i-1].ID)
		}
	}
	var nilM *PropertyTxMembers[types.RelID]
	if nilM.OrderValue("a") {
		t.Fatal("nil OrderValue = true")
	}
	if got, ok := nilM.MembersAfter("a", 0, 1); !ok || got != nil {
		t.Fatalf("nil MembersAfter = %v %v", got, ok)
	}
}
