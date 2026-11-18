package store_test

import (
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A store's current-row push-down reads a negative ValidStart, ValidEnd or
// ValidAt as a real bound (0 is the only unset value), with and without a
// temporal envelope index, on every backend; the scan doors agree.
func TestStoreNegativeValidIntervalIsAFilter(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	q := func(start, end types.Instant) storecontract.QueryOpts {
		return storecontract.QueryOpts{ValidStart: start, ValidEnd: end}
	}
	at := func(v types.Instant) storecontract.QueryOpts { return storecontract.QueryOpts{ValidAt: v} }
	checkTimeFilterDoors(t, []timeFilterRow{
		{1, types.UnboundedValidFrom, 1}, // untimed under UntimedUnbounded, closed at 1
		{2, -5000, -4000},
		{3, types.UnboundedValidFrom, -6000},
		{4, 10_000, 0},
	}, []timeFilterCase{
		{"any valid time", q(types.UnboundedValidFrom, math.MaxInt64), []int64{1, 2, 3, 4}, false},
		{"neg window", q(-5000, -4000), []int64{1, 2}, false},
		{"inf tail", q(-7000, -5500), []int64{1, 3}, false},
		{"after close", q(2, 3), []int64{}, false},
		{"start 0 is unset", q(0, -4000), []int64{1, 2, 3, 4}, false},
		{"end 0 is unset", q(-4000, 0), []int64{1, 2, 3, 4}, false},
		{"reversed negative", q(-4000, -5000), nil, true},
		// Point filters (ValidAt -1 once made badger's columnar window
		// [-1, 0), read as "no filter").
		{"ValidAt -1", at(-1), []int64{1}, false},
		{"ValidAt -4500", at(-4500), []int64{1, 2}, false},
		{"ValidAt -6500", at(-6500), []int64{1, 3}, false},
		{"ValidAt 2", at(2), []int64{}, false},
		{"ValidAt 20000", at(20_000), []int64{4}, false},
	})
}
