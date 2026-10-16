package graph

import (
	"encoding/binary"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// allocState is the ID counter: an idalloc.State (one ID per reservation, so IDs
// are dense) plus the seed offset. It is persisted as one row in every batch
// that mints IDs, so IDs are never reused across reopen.
type allocState struct {
	state  idalloc.State
	offset uint64
}

func newAllocState(c Config) (allocState, error) {
	au, err := idalloc.NewAuthority([16]byte(c.GraphID), 1)
	if err != nil {
		return allocState{}, err
	}
	st, err := idalloc.NewState(idalloc.GraphID(c.GraphID), au, 1)
	if err != nil {
		return allocState{}, err
	}
	return allocState{state: st, offset: c.Allocation.Seed}, nil
}

func (a allocState) row() (raftlog.KV, error) {
	cp, err := idalloc.MarshalCheckpoint(&a.state)
	if err != nil {
		return raftlog.KV{}, err
	}
	return raftlog.KV{Key: allocName, Value: binary.BigEndian.AppendUint64(cp, a.offset)}, nil
}

func decodeAllocState(b []byte) (allocState, error) {
	if len(b) != idalloc.CheckpointSize+8 {
		return allocState{}, idalloc.ErrCorrupt
	}
	st, err := idalloc.DecodeCheckpoint(b[:idalloc.CheckpointSize])
	if err != nil {
		return allocState{}, err
	}
	off := binary.BigEndian.Uint64(b[idalloc.CheckpointSize:])
	if off >= idalloc.MaxPublicID {
		return allocState{}, idalloc.ErrCorrupt
	}
	return allocState{state: st, offset: off}, nil
}

func (a allocState) dirty(then allocState) bool { return a != then }

// next mints one ID; the state is unchanged on error.
func (a *allocState) next() (uint64, error) {
	v, err := idalloc.Inspect(&a.state)
	if err != nil {
		return 0, err
	}
	req := idalloc.Request{Graph: v.Graph, Authority: v.Authority, Sequence: v.LastSequence + 1, Count: 1}
	st, blk, _, err := idalloc.Reserve(&a.state, req)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if blk.First > idalloc.MaxPublicID-a.offset {
		return 0, fmt.Errorf("%w: the entity ID space is exhausted", ErrInvalidRequest)
	}
	a.state = st
	return a.offset + blk.First, nil
}
