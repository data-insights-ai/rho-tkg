package storeutil

import (
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestSelectAsOfWithCurrent_Table pins the current arm and the tombstone
// life-end rule of the shared as-of selection (backlog 18, handover 2a). Each
// case names the faulty implementation it catches. History is given in
// version order and also reversed (the memory backend hands a map's rows in
// any order).
func TestSelectAsOfWithCurrent_Table(t *testing.T) {
	t.Parallel()
	cur := func(version uint32, txFrom, txTo types.Instant) *fakeRow {
		r := row(version, txFrom, txTo, 0)
		return &r
	}
	tests := []struct {
		name      string
		history   []fakeRow
		current   *fakeRow
		pin       types.Instant
		wantFound bool
		wantVer   uint32
	}{
		{
			name:      "current recorded by the pin, nothing above it: current",
			history:   []fakeRow{row(0, 10, 20, 0)},
			current:   cur(1, 20, 0),
			pin:       100,
			wantFound: true, wantVer: 1,
		},
		{
			// Catches the old current-arm preference: the cascade row above the
			// current version is the newest row recorded by the pin.
			name:      "cascade row above the current version, recorded by the pin: that row",
			history:   []fakeRow{row(0, 10, 0, 0), row(2, 30, 0, 0)},
			current:   cur(1, 20, 0),
			pin:       100,
			wantFound: true, wantVer: 2,
		},
		{
			// Catches an arm that takes every row above the current version.
			name:      "row above the current version recorded after the pin: current",
			history:   []fakeRow{row(2, 50, 0, 0)},
			current:   cur(1, 20, 0),
			pin:       40,
			wantFound: true, wantVer: 1,
		},
		{
			// Boundary: recorded exactly at the pin counts.
			name:      "row above the current version recorded at the pin: that row",
			history:   []fakeRow{row(2, 40, 0, 0)},
			current:   cur(1, 20, 0),
			pin:       40,
			wantFound: true, wantVer: 2,
		},
		{
			// Catches a re-imported ID's earlier life answering for the new one
			// (its rows have higher versions than the re-imported current row).
			name:      "rows above the current version recorded before it (earlier life): current",
			history:   []fakeRow{row(0, 5, 0, 0), row(1, 6, 9, 9)},
			current:   cur(0, 20, 0),
			pin:       100,
			wantFound: true, wantVer: 0,
		},
		{
			name:      "the highest of several rows above the current version wins",
			history:   []fakeRow{row(2, 30, 0, 0), row(4, 31, 0, 0), row(3, 31, 0, 0)},
			current:   cur(1, 20, 0),
			pin:       100,
			wantFound: true, wantVer: 4,
		},
		{
			name:      "the row above the current version was retracted by the pin: absent",
			history:   []fakeRow{row(2, 30, 35, 0)},
			current:   cur(1, 20, 0),
			pin:       100,
			wantFound: false,
		},
		{
			name:      "current recorded after the pin: history arm",
			history:   []fakeRow{row(0, 10, 50, 0)},
			current:   cur(1, 50, 0),
			pin:       30,
			wantFound: true, wantVer: 0,
		},
		{
			// Handover 2a: a delete tombstoned the row holding the slot (v1)
			// after a cascade appended v2 above it.
			name:      "tombstone below the newest row, deleted after it and by the pin: absent",
			history:   []fakeRow{row(0, 10, 0, 0), row(1, 20, 40, 40), row(2, 30, 0, 0)},
			pin:       100,
			wantFound: false,
		},
		{
			name:      "boundary: deleted exactly at the pin: absent",
			history:   []fakeRow{row(1, 20, 40, 40), row(2, 30, 0, 0)},
			pin:       40,
			wantFound: false,
		},
		{
			// Rule 15: a pin before the delete keeps its answer.
			name:      "pin before the delete: the newest row",
			history:   []fakeRow{row(1, 20, 40, 40), row(2, 30, 0, 0)},
			pin:       39,
			wantFound: true, wantVer: 2,
		},
		{
			// Catches a rule that lets any tombstone end the life: the delete
			// is not after the newest row was recorded.
			name:      "boundary: deleted at the newest row's TxFrom: the newest row",
			history:   []fakeRow{row(1, 20, 30, 30), row(2, 30, 0, 0)},
			pin:       100,
			wantFound: true, wantVer: 2,
		},
		{
			name:      "the slot holder was superseded, not deleted: the newest row",
			history:   []fakeRow{row(1, 20, 40, 0), row(2, 30, 0, 0)},
			pin:       100,
			wantFound: true, wantVer: 2,
		},
		{
			// The rule consults the highest lower row carrying a TxTo only.
			name:      "a deeper tombstone below a superseded slot holder is not consulted",
			history:   []fakeRow{row(0, 10, 40, 40), row(1, 20, 30, 0), row(2, 30, 0, 0)},
			pin:       100,
			wantFound: true, wantVer: 2,
		},
		{
			name:      "the newest row itself deleted by the pin: absent",
			history:   []fakeRow{row(0, 10, 20, 0), row(1, 20, 40, 40)},
			pin:       100,
			wantFound: false,
		},
	}
	for _, tc := range tests {
		for _, reversed := range []bool{false, true} {
			hist := slices.Clone(tc.history)
			if reversed {
				slices.Reverse(hist)
			}
			var current fakeRow
			if tc.current != nil {
				current = *tc.current
			}
			got, found := SelectAsOfWithCurrent(hist, current, tc.current != nil, tc.pin)
			if found != tc.wantFound || (found && got.Version() != tc.wantVer) {
				t.Errorf("%s (reversed=%v): found=%v version=%d; want found=%v version=%d",
					tc.name, reversed, found, got.Version(), tc.wantFound, tc.wantVer)
			}
			if tc.current == nil {
				// Without a current row SelectAsOf is the same rule.
				g2, f2 := SelectAsOf(hist, tc.pin)
				if f2 != found || g2.Version() != got.Version() {
					t.Errorf("%s: SelectAsOf disagrees with SelectAsOfWithCurrent(no current)", tc.name)
				}
			}
		}
	}
}
