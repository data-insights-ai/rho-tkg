package badger

import (
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func decodeNodeWireForKey(w storepkg.NodeWire, expected snowflake.ID) (*types.Node, error) {
	n, err := storepkg.WireToNodeChecked(w)
	if err != nil {
		return nil, err
	}
	if got := n.ID().SnowflakeID(); got != expected {
		return nil, fmt.Errorf("%w: node wire id %d does not match key %d", ErrInvalidStoreMutation, got, expected)
	}
	return n, nil
}

func decodeNodeHistoryWireForKey(w storepkg.NodeWire, expected snowflake.ID, version uint64) (*types.Node, error) {
	n, err := decodeNodeWireForKey(w, expected)
	if err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeHistoryKeySnapshot(types.NodeID(expected), version, n); err != nil {
		return nil, err
	}
	return n, nil
}

func decodeRelWireForKey(w storepkg.RelWire, expected snowflake.ID) (*types.Relationship, error) {
	r, err := storepkg.WireToRelChecked(w)
	if err != nil {
		return nil, err
	}
	if got := r.ID().SnowflakeID(); got != expected {
		return nil, fmt.Errorf("%w: relationship wire id %d does not match key %d", ErrInvalidStoreMutation, got, expected)
	}
	return r, nil
}

func decodeRelHistoryWireForKey(w storepkg.RelWire, expected snowflake.ID, version uint64) (*types.Relationship, error) {
	r, err := decodeRelWireForKey(w, expected)
	if err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelationshipHistoryKeySnapshot(types.RelID(expected), version, r); err != nil {
		return nil, err
	}
	return r, nil
}
