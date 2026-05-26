package badger

import (
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/storeutil"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

func (bs *Store) decodeNodeWireForKey(w storepkg.NodeWire, expected snowflake.ID) (*types.Node, error) {
	if err := bs.resolveNodeWireKeys(&w); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidStoreMutation, err)
	}
	n, err := storepkg.WireToNodeChecked(w)
	if err != nil {
		return nil, err
	}
	if got := n.ID().SnowflakeID(); got != expected {
		return nil, fmt.Errorf("%w: node wire id %d does not match key %d", ErrInvalidStoreMutation, got, expected)
	}
	return n, nil
}

func (bs *Store) decodeNodeHistoryWireForKey(w storepkg.NodeWire, expected snowflake.ID, version uint64) (*types.Node, error) {
	n, err := bs.decodeNodeWireForKey(w, expected)
	if err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeHistoryKeySnapshot(types.NodeID(expected), version, n); err != nil {
		return nil, err
	}
	return n, nil
}

func (bs *Store) decodeRelWireForKey(w storepkg.RelWire, expected snowflake.ID) (*types.Relationship, error) {
	if err := bs.resolveRelWireKeys(&w); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidStoreMutation, err)
	}
	r, err := storepkg.WireToRelChecked(w)
	if err != nil {
		return nil, err
	}
	if got := r.ID().SnowflakeID(); got != expected {
		return nil, fmt.Errorf("%w: relationship wire id %d does not match key %d", ErrInvalidStoreMutation, got, expected)
	}
	return r, nil
}

func (bs *Store) decodeRelHistoryWireForKey(w storepkg.RelWire, expected snowflake.ID, version uint64) (*types.Relationship, error) {
	r, err := bs.decodeRelWireForKey(w, expected)
	if err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelationshipHistoryKeySnapshot(types.RelID(expected), version, r); err != nil {
		return nil, err
	}
	return r, nil
}
