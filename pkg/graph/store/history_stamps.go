package store

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// HistoryStampsCapability is OPTIONAL: the newest transaction stamps of one
// entity's history rows without reading them (backlog 30, the history half of
// Nodes().LatestStamps / Rels().LatestStamps). For id it returns, at every
// moment and pending writes included, the fold (FoldTxStamps) of the rows
// GetNodeHistory(id) / GetRelHistory(id) return: the largest TxFrom, the
// largest TxTo or DeletedAt, and whether there is any row (has ==
// len(GetNodeHistory(id)) > 0). An ID without history answers zeros and
// false; an invalid id fails like the history door. Memory keeps a per-ID
// cache validated against the ID's row count; badger keeps a lazily built RAM
// sidecar maintained where every history key enters the write buffer;
// sharded routes by slot; tiered walks the shards GetNodeHistory reads. A
// store without it is answered from the history rows.
type HistoryStampsCapability interface {
	NodeHistoryStamps(id types.NodeID) (txFrom, txTo types.Instant, has bool, err error)
	RelHistoryStamps(id types.RelID) (txFrom, txTo types.Instant, has bool, err error)
}

// FoldTxStamps folds one row's transaction stamps into the running answer
// (from, to): from is the largest TxFrom, to the largest TxTo or DeletedAt (a
// tombstone's delete stamp counts as an end stamp even where TxTo is unset).
// The one definition the stores and the graph core share.
func FoldTxStamps(from, to, txFrom, txTo, deletedAt types.Instant) (types.Instant, types.Instant) {
	return max(from, txFrom), max(to, txTo, deletedAt)
}
