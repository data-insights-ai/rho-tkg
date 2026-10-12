package store

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// RowStamps is one row's transaction stamps and version: what the current-row
// half of Nodes().LatestStamps / Rels().LatestStamps reads (backlog 42). Zero
// stamps where the row carries no temporal metadata.
type RowStamps struct {
	TxFrom, TxTo, DeletedAt types.Instant
	Version                 uint32
}

// NodeRowStamps returns n's stamps and version (zero for nil).
func NodeRowStamps(n *types.Node) RowStamps {
	if n == nil {
		return RowStamps{}
	}
	f, t, d := n.TxStamps()
	return RowStamps{TxFrom: f, TxTo: t, DeletedAt: d, Version: n.Version()}
}

// RelRowStamps returns r's stamps and version (zero for nil).
func RelRowStamps(r *types.Relationship) RowStamps {
	if r == nil {
		return RowStamps{}
	}
	f, t, d := r.TxStamps()
	return RowStamps{TxFrom: f, TxTo: t, DeletedAt: d, Version: r.Version()}
}

// CurrentStampsCapability is OPTIONAL: the stamps and version of one entity's
// CURRENT row without reading the row (backlog 42, the current-row half of
// Nodes().LatestStamps / Rels().LatestStamps). For id it returns, at every
// moment and pending writes included, NodeRowStamps(GetNode(id)) /
// RelRowStamps(GetRelationship(id)), and the same error where that door fails:
// the typed not-found sentinel without a current row (a deleted entity
// included), ErrInvalidStoreMutation for an invalid id, ErrStoreClosed after
// Close. Memory reads its held row; badger answers from the entity cache and,
// on a miss, reads the row's key and decodes only its temporal block (a row
// whose temporal block decodes but whose body is corrupt is answered where
// GetNode fails, as for HistoryStampsCapability); sharded asks the owner slot;
// tiered routes as GetNode / GetRelationship route and answers the one row
// that route finds. A store without it is answered from the current row.
type CurrentStampsCapability interface {
	NodeCurrentStamps(id types.NodeID) (RowStamps, error)
	RelCurrentStamps(id types.RelID) (RowStamps, error)
}
