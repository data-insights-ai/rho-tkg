package core

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// DeleteNodeWithTx — RED STUB: routed to the plain twin until the seam lands.
func (tx *GraphTx) DeleteNodeWithTx(id types.NodeID, txTo types.Instant) error {
	return tx.DeleteNode(id)
}

// UpdateNodeWithTx — RED STUB: routed to the plain twin until the seam lands.
func (tx *GraphTx) UpdateNodeWithTx(id types.NodeID, updates map[string]any, txFrom types.Instant) (*types.Node, error) {
	return tx.UpdateNode(id, updates)
}
