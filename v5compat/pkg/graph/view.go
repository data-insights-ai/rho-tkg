package graph

import (
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/types"
)

// rowFlags are the per-row facts a v4 row carries beyond the stored version.
type rowFlags struct {
	txTo      int64
	deletedAt int64
	retracted bool
	void      bool
}

func (r row) tm(f rowFlags) *types.TemporalMetadata {
	return &types.TemporalMetadata{
		ValidFrom: types.Instant(r.vf), ValidTo: types.Instant(r.vt),
		TxFrom: types.Instant(r.tf), TxTo: types.Instant(f.txTo),
		CreatedAt: types.Instant(r.tf), UpdatedAt: types.Instant(r.tf),
		DeletedAt: types.Instant(f.deletedAt), Retracted: f.retracted, Void: f.void,
	}
}

func (r row) node(f rowFlags) *types.Node {
	return types.NewNode(types.NodeID(r.id), r.labels, userMap(r.vals), uint32(r.ver), r.tm(f))
}

func (r row) relationship(f rowFlags) *types.Relationship {
	return types.NewRelationship(types.RelID(r.id), r.relType, types.NodeID(r.from), types.NodeID(r.to), userMap(r.vals), uint32(r.ver), r.tm(f))
}
