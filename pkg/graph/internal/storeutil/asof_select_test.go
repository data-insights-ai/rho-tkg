package storeutil

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// fakeRow is a minimal TemporalRow used to exercise the pure selection rule
// without constructing full *types.Node / *types.Relationship values.
type fakeRow struct {
	v  uint32
	tm *types.TemporalMetadata
}

func (f fakeRow) Version() uint32                   { return f.v }
func (f fakeRow) Temporal() *types.TemporalMetadata { return f.tm }

func row(version uint32, txFrom, txTo, deletedAt types.Instant) fakeRow {
	return fakeRow{
		v: version,
		tm: &types.TemporalMetadata{
			TxFrom:    txFrom,
			TxTo:      txTo,
			DeletedAt: deletedAt,
		},
	}
}

// TestSelectAsOf_Table is the direct table test of the shared AS-OF selection
// rule: newest version (by VERSION order) with 0 < TxFrom <= pin; ABSENT when
// no such version exists or that decisive newest belief was retracted
// (TxTo != 0 && TxTo <= pin) or hard-deleted (DeletedAt != 0 && DeletedAt <= pin).
func TestSelectAsOf_Table(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		rows      []fakeRow
		pin       types.Instant
		wantFound bool
		wantVer   uint32
	}{
		{
			name:      "empty chain returns absent",
			rows:      nil,
			pin:       100,
			wantFound: false,
		},
		{
			name:      "single open row committed by pin",
			rows:      []fakeRow{row(0, 10, 0, 0)},
			pin:       100,
			wantFound: true,
			wantVer:   0,
		},
		{
			name:      "single row not yet committed at pin is absent",
			rows:      []fakeRow{row(0, 200, 0, 0)},
			pin:       100,
			wantFound: false,
		},
		{
			name:      "zero TxFrom row is never a candidate",
			rows:      []fakeRow{row(0, 0, 0, 0)},
			pin:       100,
			wantFound: false,
		},
		{
			name: "newest belief by version wins over older open row",
			rows: []fakeRow{
				row(0, 10, 20, 0), // genesis, superseded at tx=20
				row(1, 20, 0, 0),  // update, current
			},
			pin:       100,
			wantFound: true,
			wantVer:   1,
		},
		{
			name: "pin before the update sees the genesis",
			rows: []fakeRow{
				row(0, 10, 20, 0),
				row(1, 20, 0, 0),
			},
			pin:       15,
			wantFound: true,
			wantVer:   0,
		},
		{
			name: "retraction: decisive newest belief superseded by pin is absent, never falls through to older open row (lesson 62)",
			rows: []fakeRow{
				row(0, 10, 0, 0),  // genesis left OPEN (append-only cascade demoted it without stamping TxTo)
				row(1, 30, 50, 0), // corrected tile, superseded at tx=50
			},
			pin:       60,
			wantFound: false,
		},
		{
			name: "deletion: decisive newest belief hard-deleted by pin is absent",
			rows: []fakeRow{
				row(0, 10, 20, 0),
				row(1, 20, 40, 40), // deleted: TxTo == DeletedAt == 40
			},
			pin:       50,
			wantFound: false,
		},
		{
			name: "deletion in the future of the pin is still visible",
			rows: []fakeRow{
				row(1, 20, 40, 40),
			},
			pin:       30,
			wantFound: true,
			wantVer:   1,
		},
		{
			name: "version-vs-TxFrom inversion: higher version with LOWER TxFrom is still the newest belief (lesson 62)",
			rows: []fakeRow{
				// A later cascade row (version 2) carries a plain now() TxFrom (25)
				// BELOW an update's validInstantAfter-derived TxFrom (30, version 1).
				// Version order is authoritative, so version 2 is the decisive belief.
				row(0, 10, 30, 0),
				row(1, 30, 0, 0),
				row(2, 25, 0, 0),
			},
			pin:       100,
			wantFound: true,
			wantVer:   2,
		},
		{
			name: "only later-recorded rows exist: all TxFrom > pin is absent",
			rows: []fakeRow{
				row(0, 200, 0, 0),
				row(1, 300, 0, 0),
			},
			pin:       100,
			wantFound: false,
		},
		{
			name: "boundary: TxFrom exactly equal to pin is committed",
			rows: []fakeRow{
				row(0, 100, 0, 0),
			},
			pin:       100,
			wantFound: true,
			wantVer:   0,
		},
		{
			name: "boundary: TxTo exactly equal to pin retracts",
			rows: []fakeRow{
				row(0, 10, 100, 0),
			},
			pin:       100,
			wantFound: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, found := SelectAsOf(tc.rows, tc.pin)
			if found != tc.wantFound {
				t.Fatalf("found = %v, want %v", found, tc.wantFound)
			}
			if found && got.Version() != tc.wantVer {
				t.Fatalf("version = %d, want %d", got.Version(), tc.wantVer)
			}
		})
	}
}

// TestSelectAsOf_RealNodes proves the rule works over the concrete *types.Node
// values the memory backend and core resolver actually hand it.
func TestSelectAsOf_RealNodes(t *testing.T) {
	t.Parallel()

	mk := func(version uint32, txFrom, txTo types.Instant) *types.Node {
		n := types.NewNode(types.NodeID(snowflake.ID(1000+version)), 1, nil)
		n.SetVersion(version)
		n.SetTemporal(&types.TemporalMetadata{TxFrom: txFrom, TxTo: txTo})
		return n
	}

	chain := []*types.Node{
		mk(0, 10, 20),
		mk(1, 20, 0),
	}
	got, found := SelectAsOf(chain, 100)
	if !found || got.Version() != 1 {
		t.Fatalf("current belief: found=%v version=%d, want true/1", found, got.Version())
	}

	past, found := SelectAsOf(chain, 15)
	if !found || past.Version() != 0 {
		t.Fatalf("past belief: found=%v version=%d, want true/0", found, past.Version())
	}
}

// TestSelectAsOf_RealRels mirrors TestSelectAsOf_RealNodes for relationships
// (Node/Rel parity — both satisfy TemporalRow).
func TestSelectAsOf_RealRels(t *testing.T) {
	t.Parallel()

	mk := func(version uint32, txFrom, txTo types.Instant) *types.Relationship {
		r := types.NewRelationship(types.RelID(snowflake.ID(2000+version)), 1, types.NodeID(1), types.NodeID(2))
		r.SetVersion(version)
		r.SetTemporal(&types.TemporalMetadata{TxFrom: txFrom, TxTo: txTo})
		return r
	}

	chain := []*types.Relationship{
		mk(0, 10, 20),
		mk(1, 20, 0),
	}
	got, found := SelectAsOf(chain, 100)
	if !found || got.Version() != 1 {
		t.Fatalf("current belief: found=%v version=%d, want true/1", found, got.Version())
	}
}
