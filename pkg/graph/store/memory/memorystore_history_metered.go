package memory

import (
	"cmp"
	"slices"
	"sync/atomic"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.HistoryReadMeteredCapability = (*Store)(nil)

// copyHistory's caller holds the store read lock. The chain count admits
// every native visit and clone before either temporary list is allocated.
func copyHistory[T any](inner map[uint32]T, tally *storecontract.ReadTally, clone func(T) T, copies *atomic.Int64) ([]T, error) {
	if len(inner) == 0 {
		return nil, nil
	}
	if err := tally.History(len(inner)); err != nil {
		return nil, err
	}
	if err := tally.Buffer((4 + 8) * len(inner)); err != nil {
		return nil, err
	}
	versions := make([]uint32, 0, len(inner))
	for v := range inner {
		versions = append(versions, v)
	}
	slices.Sort(versions)
	result := make([]T, len(versions))
	for i, v := range versions {
		result[i] = clone(inner[v])
	}
	copies.Add(int64(len(result)))
	return result, nil
}

// copyHistoryMetas owns list and temporal-block copies. TemporalByValue does
// not allocate a frozen row's defensive block before its buffer admission.
func copyHistoryMetas[T interface {
	TemporalByValue() (types.TemporalMetadata, bool)
}](inner map[uint32]T, tally *storecontract.ReadTally, copies *atomic.Int64) ([]storecontract.VersionTemporalMeta, error) {
	if len(inner) == 0 {
		return nil, nil
	}
	if err := tally.History(len(inner)); err != nil {
		return nil, err
	}
	if err := tally.Buffer(16 * len(inner)); err != nil {
		return nil, err
	}
	out := make([]storecontract.VersionTemporalMeta, 0, len(inner))
	for v, row := range inner {
		m := storecontract.VersionTemporalMeta{Version: v}
		if tm, has := row.TemporalByValue(); has {
			// Seven instants, two string headers, base ID and flags/alignment.
			if err := tally.Buffer(104); err != nil {
				return nil, err
			}
			m.Temporal = new(types.TemporalMetadata)
			*m.Temporal = tm
		}
		out = append(out, m)
		copies.Add(1)
	}
	slices.SortFunc(out, func(a, b storecontract.VersionTemporalMeta) int { return cmp.Compare(a.Version, b.Version) })
	return out, nil
}
