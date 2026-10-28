package storeutil

import (
	"cmp"
	"slices"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// SortPropertyIndexStats orders stats by shard, node definitions before
// relationship ones, scope token, single-key before composite, then keys: one
// order on every store, whatever its maps' order.
func SortPropertyIndexStats(stats []storecontract.PropertyIndexStat) {
	slices.SortFunc(stats, func(a, b storecontract.PropertyIndexStat) int {
		return cmp.Or(
			cmp.Compare(a.Shard, b.Shard),
			cmp.Compare(a.RelTypeToken, b.RelTypeToken),
			cmp.Compare(a.LabelToken, b.LabelToken),
			compareBool(a.Composite, b.Composite),
			slices.Compare(a.Keys, b.Keys),
		)
	})
}

func compareBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case a:
		return 1
	}
	return -1
}
