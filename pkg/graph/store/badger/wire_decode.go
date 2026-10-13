package badger

import (
	"fmt"
	"sync/atomic"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// rowDecodeHook, when set, runs once per entity row (current or history)
// decoded by any Store in the process. A test seam only (open-cost tests count
// the rows a reopen decodes); nil in production, so a decode pays one atomic
// load.
var rowDecodeHook atomic.Pointer[func()]

func noteRowDecode() {
	if h := rowDecodeHook.Load(); h != nil {
		(*h)()
	}
}

func (bs *Store) decodeNodeWireForKey(w storepkg.NodeWire, expected snowflake.ID) (*types.Node, error) {
	noteRowDecode()
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
	_ = n.SetOrdinal(bs.ords.node(n.ID(), false)) // freshly decoded, not frozen
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
	noteRowDecode()
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
	_ = r.SetOrdinal(bs.ords.rel(r.ID(), false)) // freshly decoded, not frozen
	_ = r.SetEndpointOrdinals(bs.relEndpointOrdinals(r))
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
	_ = r.SetEndpointOrdinals(0, 0) // a history row carries none
	return r, nil
}
