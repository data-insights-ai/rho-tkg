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

// Ledger row 60: a store's current-row push-down reads a negative ValidStart
// or ValidEnd as a real bound (0 is the only unset value), with and without a
// temporal envelope index, on every backend; the scan doors agree.
func TestStoreNegativeValidIntervalIsAFilter(t *testing.T) {
	t.Parallel()
	rows := []struct {
		id       int64
		from, to types.Instant
	}{
		{1, types.UnboundedValidFrom, 1}, // untimed under UntimedUnbounded, closed at 1
		{2, -5000, -4000},
		{3, types.UnboundedValidFrom, -6000},
		{4, 10_000, 0},
	}
	cases := []struct {
		name       string
		start, end types.Instant
		want       []int64
		wantErr    bool
	}{
		{"any valid time", types.UnboundedValidFrom, math.MaxInt64, []int64{1, 2, 3, 4}, false},
		{"neg window", -5000, -4000, []int64{1, 2}, false},
		{"inf tail", -7000, -5500, []int64{1, 3}, false},
		{"after close", 2, 3, []int64{}, false},
		{"start 0 is unset", 0, -4000, []int64{1, 2, 3, 4}, false},
		{"end 0 is unset", -4000, 0, []int64{1, 2, 3, 4}, false},
		{"reversed negative", -4000, -5000, nil, true},
	}
	// Point filters, served by the column scans too (ValidAt -1 once made
	// badger's columnar window [-1, 0), read as "no filter").
	points := []struct {
		at   types.Instant
		want []int64
	}{
		{-1, []int64{1}},
		{-4500, []int64{1, 2}},
		{-6500, []int64{1, 3}},
		{2, []int64{}},
		{20_000, []int64{4}},
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
					opts := storecontract.QueryOpts{ValidStart: c.start, ValidEnd: c.end}
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
					ns, err := st.NodesByLabel(1, opts)
					check("NodesByLabel", nodeIDs(ns), err)
					ns, err = st.AllNodes(opts)
					check("AllNodes", nodeIDs(ns), err)
					rs, err := st.RelationshipsByType(5, opts)
					check("RelationshipsByType", relIDs(rs), err)
					rs, err = st.AllRelationships(opts)
					check("AllRelationships", relIDs(rs), err)
				}
				for _, p := range points {
					opts := storecontract.QueryOpts{ValidAt: p.at}
					want := func(door string, got []int64, err error) {
						t.Helper()
						if err != nil || !slices.Equal(got, p.want) {
							t.Errorf("ValidAt %d %s = (%v, %v), want %v", p.at, door, got, err, p.want)
						}
					}
					ns, err := st.NodesByLabel(1, opts)
					want("NodesByLabel", nodeIDs(ns), err)
					rs, err := st.RelationshipsByType(5, opts)
					want("RelationshipsByType", relIDs(rs), err)
					if cs, ok := st.(storecontract.NodeColumnScanCapability); ok {
						got := []int64{}
						err := cs.ScanNodeColumns(1, []string{"w"}, opts, func(b *storecontract.ColumnBatch) bool {
							for _, id := range b.IDs {
								got = append(got, int64(id)-int64(currentNodeID(0)))
							}
							return true
						})
						slices.Sort(got)
						want("ScanNodeColumns", got, err)
					}
					if cs, ok := st.(storecontract.RelColumnScanCapability); ok {
						got := []int64{}
						err := cs.ScanRelColumns(5, []string{"w"}, opts, func(b *storecontract.RelColumnBatch) bool {
							for _, id := range b.IDs {
								got = append(got, int64(id)-int64(currentRelID(10)))
							}
							return true
						})
						slices.Sort(got)
						want("ScanRelColumns", got, err)
					}
				}
			})
		}
	}
}

// HasValidInterval is the one rule: 0 is unset, any other instant (negative
// and types.UnboundedValidFrom included) is a real bound.
func TestQueryOptsHasValidInterval(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		opts             storecontract.QueryOpts
		interval, filter bool
	}{
		{storecontract.QueryOpts{}, false, false},
		{storecontract.QueryOpts{ValidStart: 5}, false, false},
		{storecontract.QueryOpts{ValidEnd: 5}, false, false},
		{storecontract.QueryOpts{ValidStart: -5}, false, false},
		{storecontract.QueryOpts{ValidStart: 0, ValidEnd: -5}, false, false},
		{storecontract.QueryOpts{ValidStart: 5, ValidEnd: 9}, true, true},
		{storecontract.QueryOpts{ValidStart: -9, ValidEnd: -5}, true, true},
		{storecontract.QueryOpts{ValidStart: types.UnboundedValidFrom, ValidEnd: math.MaxInt64}, true, true},
		{storecontract.QueryOpts{ValidAt: -3}, false, true},
		{storecontract.QueryOpts{TxAt: 7, TxPin: 7}, false, false},
	} {
		if got := c.opts.HasValidInterval(); got != c.interval {
			t.Errorf("%+v HasValidInterval = %v, want %v", c.opts, got, c.interval)
		}
		if got := c.opts.HasValidTimeFilter(); got != c.filter {
			t.Errorf("%+v HasValidTimeFilter = %v, want %v", c.opts, got, c.filter)
		}
	}
	for _, opts := range []storecontract.QueryOpts{
		{ValidStart: -4000, ValidEnd: -5000},
		{ValidStart: -5, ValidEnd: -5},
		{ValidStart: 5, ValidEnd: -5},
	} {
		if err := storecontract.ValidateQueryOpts(opts); !errors.Is(err, storecontract.ErrInvalidTimeRange) {
			t.Errorf("ValidateQueryOpts(%+v) = %v, want ErrInvalidTimeRange", opts, err)
		}
	}
	if err := storecontract.ValidateQueryOpts(storecontract.QueryOpts{ValidStart: types.UnboundedValidFrom, ValidEnd: math.MaxInt64}); err != nil {
		t.Errorf("ValidateQueryOpts(any valid time) = %v", err)
	}
}
