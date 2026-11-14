package index

import (
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestPropertyTxMembersDuringAdmit (R2 cumulative, RAM sidecar): a window read
// under an admission charges what its walk examines before it does it, and
// stops at the first refusal. 64 members, each valid one instant after the one
// before, all last stamped after the pin, so the recent view examines all 64
// (and the late view the one that meets the window, which the recent view
// decided already). The control (no admission) answers that one member, equal
// to MembersDuring's; the admission sees 65 entries, 1 member added and 64
// entries of the decided map (the native counts of the walk, kept by the test,
// not by any meter); a refusal at the 4th entry stops the walk there (3 entries
// let through and the 4th refused before it is looked at, not 64) and answers
// nothing.
func TestPropertyTxMembersDuringAdmit(t *testing.T) {
	t.Parallel()
	const n = 64
	m := NewPropertyTxMembers[int64]()
	for i := int64(0); i < n; i++ {
		m.RecordRow("v", i+1, row(1, 200, 0, types.Instant(100+i), types.Instant(101+i)), true)
	}
	if !m.OpenWindow("v") || m.Count("v") != n {
		t.Fatalf("fixture: %d members, want %d", m.Count("v"), n)
	}
	const pin = 150 // before every row's last stamp: the recent view examines every member
	want, ok := m.MembersDuring("v", 100, 101, pin)
	if !ok || len(want) != 1 {
		t.Fatalf("control: %d members, %v; want 1", len(want), ok)
	}

	entries, adds, laters := 0, 0, 0
	count := &WindowAdmit{
		Entry: func() error { entries++; return nil },
		Add:   func() error { adds++; return nil },
		Later: func() error { laters++; return nil },
	}
	got, ok, err := m.MembersDuringAdmit("v", 100, 101, pin, count)
	if err != nil || !ok || !slices.Equal(got, want) || entries != n+1 || adds != 1 || laters != n {
		t.Fatalf("counting admission: %v, %v, %v, %d entries, %d adds, %d later; want the control's member after %d entries, 1 add, %d later", got, ok, err, entries, adds, laters, n+1, n)
	}

	errStop := errors.New("budget")
	entries, adds, laters = 0, 0, 0
	stop := &WindowAdmit{
		Entry: func() error {
			if entries++; entries > 3 {
				return errStop
			}
			return nil
		},
		Add:   func() error { adds++; return nil },
		Later: func() error { laters++; return nil },
	}
	got, ok, err = m.MembersDuringAdmit("v", 100, 101, pin, stop)
	if !errors.Is(err, errStop) || got != nil || !ok || entries != 4 || laters != 3 {
		t.Fatalf("refusing admission: %v, %v, %v after %d entries, %d later; want errStop, no member, the walk stopped at the 4th entry before it was looked at", got, ok, err, entries, laters)
	}

	// A refusal on a member added stops the late walk too: nothing is answered.
	adds = 0
	got, _, err = m.MembersDuringAdmit("v", 0, 1000, 0, &WindowAdmit{
		Entry: func() error { return nil },
		Add: func() error {
			if adds++; adds > 2 {
				return errStop
			}
			return nil
		},
		Later: func() error { return nil },
	})
	if !errors.Is(err, errStop) || got != nil || adds != 3 {
		t.Fatalf("late walk: %v, %v after %d adds; want errStop after the 3rd add", got, err, adds)
	}
}
