package index

import (
	"fmt"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// footprintCase is one structure TestFootprintCounts and the measurement
// TestFootprintMatchesHeap build: the values it holds, and the build that
// returns the structure (kept alive across the heap reading) and its
// Footprint.
type footprintCase struct {
	name   string
	values int64
	build  func() (any, func() Footprint)
}

// footprintCases are the shapes ai-soc's stores have, n postings each: every
// value distinct, 20 rows a value, numeric values (a second posting list per
// value), range counts, composites and sidecars.
func footprintCases(n int) []footprintCase {
	str := func(i int) string { return types.IndexablePropertyValueKey(fmt.Sprintf("value-%07d", i)) }
	num := func(i int) string { return types.IndexablePropertyValueKey(float64(i) + 0.5) }
	return []footprintCase{
		{"strings, every value distinct", int64(n), func() (any, func() Footprint) {
			pi := NewPropertyIndex()
			for i := range n {
				pi.AddKey(snowflake.ID(i+1), str(i))
			}
			return pi, pi.Footprint
		}},
		{"strings, 20 rows a value", int64(n / 20), func() (any, func() Footprint) {
			pi := NewPropertyIndex()
			for i := range n {
				pi.AddKey(snowflake.ID(i+1), str(i/20))
			}
			return pi, pi.Footprint
		}},
		{"numbers, every value distinct", int64(n), func() (any, func() Footprint) {
			pi := NewPropertyIndex()
			for i := range n {
				pi.AddKey(snowflake.ID(i+1), num(i))
			}
			return pi, pi.Footprint
		}},
		{"numbers with range counts, 20 rows a value", int64(n / 20), func() (any, func() Footprint) {
			pi := NewPropertyIndexWith(true, false)
			for i := range n {
				pi.AddKey(snowflake.ID(i+1), num(i/20))
			}
			return pi, pi.Footprint
		}},
		{"composite, half the tuples held by two", int64(n * 3 / 4), func() (any, func() Footprint) {
			ci := NewCompositePropertyIndex([]string{"a", "b"})
			for i := range n {
				k := i
				if i < n/2 {
					k = n + i/2 // pairs
				}
				ci.AddKey(snowflake.ID(i+1), str(k))
			}
			return ci, ci.Footprint
		}},
		{"sidecar, every value distinct", int64(n), func() (any, func() Footprint) {
			m := NewPropertyTxMembers[types.NodeID]()
			for i := range n {
				m.Record(str(i), types.NodeID(i+1), types.Instant(i+1))
			}
			return m, m.Footprint
		}},
		{"sidecar, 20 rows a value", int64(n / 20), func() (any, func() Footprint) {
			m := NewPropertyTxMembers[types.NodeID]()
			for i := range n {
				m.Record(str(i/20), types.NodeID(i+1), types.Instant(i+1))
			}
			return m, m.Footprint
		}},
		{"sidecar, 2 rows a value", int64(n / 2), func() (any, func() Footprint) {
			m := NewPropertyTxMembers[types.NodeID]()
			for i := range n {
				m.Record(str(i/2), types.NodeID(i+1), types.Instant(i+1))
			}
			return m, m.Footprint
		}},
		{"sidecar, 20 rows a value, each value's list ordered", int64(n / 20), func() (any, func() Footprint) {
			m := NewPropertyTxMembers[types.NodeID]()
			for i := range n {
				m.Record(str(i/20), types.NodeID(i+1), types.Instant(i+1))
			}
			for v := range n / 20 {
				m.OrderValue(str(v))
			}
			return m, m.Footprint
		}},
	}
}

// TestFootprintCounts (IDXSTATS): each structure's Footprint counts its values
// and postings exactly, with some bytes. That the bytes lie within a fifth of
// the heap the structure takes is TestFootprintMatchesHeap, at the
// measurement level.
func TestFootprintCounts(t *testing.T) {
	t.Parallel()
	n := 200_000
	if testing.Short() {
		n = 20_000
	}
	for _, c := range footprintCases(n) {
		_, footprint := c.build()
		if f := footprint(); f.Values != c.values || f.Postings != int64(n) || f.Bytes <= 0 {
			t.Errorf("%s: %d values, %d postings, %d bytes; want %d, %d and some bytes", c.name, f.Values, f.Postings, f.Bytes, c.values, n)
		}
	}
}
