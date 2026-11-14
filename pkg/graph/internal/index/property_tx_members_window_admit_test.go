package index

import (
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestPropertyTxMembersDuringAdmit (R2 cumulative, RAM sidecar): a window read
// under an admission charges what its walk examines, and stops at the first
// refusal. 64 members, each valid one instant after the one before, all last
// stamped after the pin, so the recent view judges all 64 and the window
// [100, 101) keeps 1. The control (no admission) answers that one member, equal
// to MembersDuring's; the admission counts 63 rejects and 1 member (the native
// counts of the walk, kept by the test, not by any meter); a refusal at the
// 4th reject stops the walk there (4 entries judged, not 64) and answers
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
	const pin = 150 // before every row's last stamp: the recent view judges every member
	want, ok := m.MembersDuring("v", 100, 101, pin)
	if !ok || len(want) != 1 {
		t.Fatalf("control: %d members, %v; want 1", len(want), ok)
	}

	rejects, adds := 0, 0
	count := &WindowAdmit{Reject: func() error { rejects++; return nil }, Add: func() error { adds++; return nil }}
	got, ok, err := m.MembersDuringAdmit("v", 100, 101, pin, count)
	if err != nil || !ok || !slices.Equal(got, want) || rejects != n-1 || adds != 1 {
		t.Fatalf("counting admission: %v, %v, %v, %d rejects, %d adds; want the control's member after %d rejects and 1 add", got, ok, err, rejects, adds, n-1)
	}

	errStop := errors.New("budget")
	rejects, adds = 0, 0
	stop := &WindowAdmit{Reject: func() error {
		if rejects++; rejects > 3 {
			return errStop
		}
		return nil
	}, Add: func() error { adds++; return nil }}
	got, ok, err = m.MembersDuringAdmit("v", 100, 101, pin, stop)
	if !errors.Is(err, errStop) || got != nil || !ok || rejects != 4 {
		t.Fatalf("refusing admission: %v, %v, %v after %d rejects; want errStop, no member, the walk stopped at the 4th reject", got, ok, err, rejects)
	}

	// A refusal on the first member added stops the late walk too: nothing is answered.
	adds = 0
	got, _, err = m.MembersDuringAdmit("v", 0, 1000, 0, &WindowAdmit{Reject: func() error { return nil }, Add: func() error {
		if adds++; adds > 2 {
			return errStop
		}
		return nil
	}})
	if !errors.Is(err, errStop) || got != nil || adds != 3 {
		t.Fatalf("late walk: %v, %v after %d adds; want errStop after the 3rd add", got, err, adds)
	}
}
