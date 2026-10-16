package store_test

import (
	"errors"
	"math"
	"slices"
	"testing"
	"time"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ledger row 63: with its presence flag set, a valid-time filter at 0 or with
// a bound at 0 is a real filter in every store's current-row push-down (scan
// doors, temporal and high-frequency index, column scans), on every backend.
func TestStoreTimeFilterExplicitPresence(t *testing.T) {
	t.Parallel()
	rows := []struct {
		id       int64
		from, to types.Instant
	}{
		{1, types.UnboundedValidFrom, 1}, // untimed, closed at 1
		{2, -5000, -4000},
		{3, -1, 1}, // valid at 0
		{4, 10_000, 0},
	}
	cases := []struct {
		name    string
		opts    storecontract.QueryOpts
		want    []int64
		wantErr bool
	}{
		{"AT TIME 0", storecontract.QueryOpts{ValidAtSet: true}, []int64{1, 3}, false},
		{"AT TIME -4500", storecontract.QueryOpts{ValidAtSet: true, ValidAt: -4500}, []int64{1, 2}, false},
		{"AT TIME 1", storecontract.QueryOpts{ValidAtSet: true, ValidAt: 1}, []int64{}, false},
		{"BETWEEN -4500 AND 0", storecontract.QueryOpts{ValidWindowSet: true, ValidStart: -4500}, []int64{1, 2, 3}, false},
		{"BETWEEN -4000 AND -1", storecontract.QueryOpts{ValidWindowSet: true, ValidStart: -4000, ValidEnd: -1}, []int64{1}, false},
		{"BETWEEN 0 AND 1", storecontract.QueryOpts{ValidWindowSet: true, ValidEnd: 1}, []int64{1, 3}, false},
		{"BETWEEN 0 AND max", storecontract.QueryOpts{ValidWindowSet: true, ValidEnd: math.MaxInt64}, []int64{1, 3, 4}, false},
		{"AT TIME 0 beats the window", storecontract.QueryOpts{ValidAtSet: true, ValidWindowSet: true, ValidStart: 10_000, ValidEnd: math.MaxInt64}, []int64{1, 3}, false},
		{"empty window [0, 0)", storecontract.QueryOpts{ValidWindowSet: true}, nil, true},
		{"reversed window [5, 0)", storecontract.QueryOpts{ValidWindowSet: true, ValidStart: 5}, nil, true},
		{"unset ValidAt 0", storecontract.QueryOpts{}, []int64{1, 2, 3, 4}, false},
		{"unset [-4500, 0)", storecontract.QueryOpts{ValidStart: -4500}, []int64{1, 2, 3, 4}, false},
		{"unset [0, 1)", storecontract.QueryOpts{ValidEnd: 1}, []int64{1, 2, 3, 4}, false},
	}
	for _, b := range stampsBackends() {
		for _, index := range []string{"", "temporal", "highfrequency"} {
			name := b.name
			if index != "" {
				name += "/" + index
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				st := b.open(t)
				t.Cleanup(func() { _ = st.Close() })
				switch index {
				case "temporal":
					ti, ok := st.(storecontract.TemporalIndexCapability)
					if !ok {
						t.Skip("no temporal index capability")
					}
					if err := ti.CreateTemporalIndex(1); err != nil {
						t.Fatal(err)
					}
					if rt, ok := st.(storecontract.RelTypeTemporalIndexCapability); ok {
						if err := rt.CreateRelTemporalIndex(5); err != nil {
							t.Fatal(err)
						}
					}
				case "highfrequency":
					hf, ok := st.(storecontract.HighFrequencyIndexCapability)
					if !ok {
						t.Skip("no high-frequency index capability")
					}
					if err := hf.CreateHighFrequencyIndex(1, time.Second); err != nil {
						t.Fatal(err)
					}
				}
				for _, r := range rows {
					n := types.NewNode(currentNodeID(r.id), 1, nil)
					n.SetTemporal(&types.TemporalMetadata{ValidFrom: r.from, ValidTo: r.to, TxFrom: 1000})
					if err := n.SetProperty("w", r.id); err != nil {
						t.Fatal(err)
					}
					if err := st.PutNode(n); err != nil {
						t.Fatal(err)
					}
				}
				for _, r := range rows {
					rel := types.NewRelationship(currentRelID(r.id+10), 5, currentNodeID(1), currentNodeID(2))
					rel.SetTemporal(&types.TemporalMetadata{ValidFrom: r.from, ValidTo: r.to, TxFrom: 1000})
					if err := rel.SetProperty("w", r.id); err != nil {
						t.Fatal(err)
					}
					if err := st.PutRelationship(rel); err != nil {
						t.Fatal(err)
					}
				}
				nodeIDs := func(ns []*types.Node) []int64 {
					out := []int64{}
					for _, n := range ns {
						out = append(out, int64(n.ID())-int64(currentNodeID(0)))
					}
					slices.Sort(out)
					return out
				}
				relIDs := func(rs []*types.Relationship) []int64 {
					out := []int64{}
					for _, r := range rs {
						out = append(out, int64(r.ID())-int64(currentRelID(10)))
					}
					slices.Sort(out)
					return out
				}
				for _, c := range cases {
					check := func(door string, got []int64, err error) {
						t.Helper()
						if c.wantErr {
							if !errors.Is(err, storecontract.ErrInvalidTimeRange) {
								t.Errorf("%s %s: err = %v, want ErrInvalidTimeRange", c.name, door, err)
							}
							return
						}
						if err != nil || !slices.Equal(got, c.want) {
							t.Errorf("%s %s = (%v, %v), want %v", c.name, door, got, err, c.want)
						}
					}
					ns, err := st.NodesByLabel(1, c.opts)
					check("NodesByLabel", nodeIDs(ns), err)
					ns, err = st.AllNodes(c.opts)
					check("AllNodes", nodeIDs(ns), err)
					rs, err := st.RelationshipsByType(5, c.opts)
					check("RelationshipsByType", relIDs(rs), err)
					rs, err = st.AllRelationships(c.opts)
					check("AllRelationships", relIDs(rs), err)
					if c.wantErr {
						continue
					}
					if cs, ok := st.(storecontract.NodeColumnScanCapability); ok {
						got := []int64{}
						err := cs.ScanNodeColumns(1, []string{"w"}, c.opts, func(b *storecontract.ColumnBatch) bool {
							for _, id := range b.IDs {
								got = append(got, int64(id)-int64(currentNodeID(0)))
							}
							return true
						})
						slices.Sort(got)
						check("ScanNodeColumns", got, err)
					}
					if cs, ok := st.(storecontract.RelColumnScanCapability); ok {
						got := []int64{}
						err := cs.ScanRelColumns(5, []string{"w"}, c.opts, func(b *storecontract.RelColumnBatch) bool {
							for _, id := range b.IDs {
								got = append(got, int64(id)-int64(currentRelID(10)))
							}
							return true
						})
						slices.Sort(got)
						check("ScanRelColumns", got, err)
					}
				}
			})
		}
	}
}

// The Has* methods are the one presence rule: a flag makes its filter present
// at any instant; unset, 0 is "no filter" as before.
func TestQueryOptsTimePresence(t *testing.T) {
	t.Parallel()
	type has struct{ at, window, valid, txAt, txPin, any bool }
	for _, c := range []struct {
		opts storecontract.QueryOpts
		want has
	}{
		{storecontract.QueryOpts{}, has{}},
		{storecontract.QueryOpts{ValidAtSet: true}, has{at: true, valid: true, any: true}},
		{storecontract.QueryOpts{ValidAt: -3}, has{at: true, valid: true, any: true}},
		{storecontract.QueryOpts{ValidWindowSet: true}, has{window: true, valid: true, any: true}},
		{storecontract.QueryOpts{ValidWindowSet: true, ValidStart: -5}, has{window: true, valid: true, any: true}},
		{storecontract.QueryOpts{ValidStart: -5}, has{any: true}}, // a lone bound: no filter, still a coordinate
		{storecontract.QueryOpts{ValidStart: -5, ValidEnd: 5}, has{window: true, valid: true, any: true}},
		{storecontract.QueryOpts{TxAtSet: true}, has{txAt: true, any: true}},
		{storecontract.QueryOpts{TxAt: -1}, has{txAt: true, any: true}},
		{storecontract.QueryOpts{TxPinSet: true}, has{txPin: true, any: true}},
		{storecontract.QueryOpts{TxPin: 7}, has{txPin: true, any: true}},
		{storecontract.QueryOpts{Limit: 3, After: 9, NoSort: true}, has{}},
	} {
		o := c.opts
		got := has{o.HasValidAt(), o.HasValidInterval(), o.HasValidTimeFilter(), o.HasTxAt(), o.HasTxPin(), o.HasTimeCoordinate()}
		if got != c.want {
			t.Errorf("%+v: got %+v, want %+v", o, got, c.want)
		}
	}
	for _, opts := range []storecontract.QueryOpts{
		{ValidWindowSet: true},
		{ValidWindowSet: true, ValidStart: 5},
		{ValidWindowSet: true, ValidStart: -5, ValidEnd: -5},
	} {
		if err := storecontract.ValidateQueryOpts(opts); !errors.Is(err, storecontract.ErrInvalidTimeRange) {
			t.Errorf("ValidateQueryOpts(%+v) = %v, want ErrInvalidTimeRange", opts, err)
		}
	}
	for _, opts := range []storecontract.QueryOpts{
		{ValidWindowSet: true, ValidStart: -5},
		{ValidAtSet: true, ValidWindowSet: true}, // ValidAt wins; the window is not read
		{ValidStart: 5},                          // unset lone bound: no filter
	} {
		if err := storecontract.ValidateQueryOpts(opts); err != nil {
			t.Errorf("ValidateQueryOpts(%+v) = %v, want nil", opts, err)
		}
	}
}
