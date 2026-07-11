package core

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
)

// graphEpochMeta is the MetaKV key holding this graph's durable lineage id — a
// random 8-byte value minted once on first use. It identifies a graph instance
// so a delta cursor (io.Cursor.Epoch) carried from a DIFFERENT graph (e.g. a
// tenant restored from scratch into a new data dir) is detectable: ExportSince
// rejects a cursor whose Epoch does not match, forcing a full-export fallback
// rather than producing a meaningless delta. Distinct from replicaAppliedLSNMeta
// and idSlotLeaseMeta.
const graphEpochMeta = "graph_epoch"

// graphEpochLocked returns this graph's lineage id, minting and persisting a
// fresh random value on first use. Returns 0 when the backend cannot persist
// metadata (no MetaKVCapability) — delta export is unavailable there regardless,
// since it also requires a change-log. The caller MUST hold c.mu.Lock: minting
// is a read-then-write that two concurrent first-callers would otherwise race
// into two divergent epochs.
func (c *Core) graphEpochLocked() (uint64, error) {
	mk := c.metaKV
	if mk == nil {
		return 0, nil
	}
	v, err := mk.MetaGet(graphEpochMeta)
	if err != nil {
		return 0, fmt.Errorf("graph: read epoch: %w", err)
	}
	if len(v) == 8 {
		return binary.BigEndian.Uint64(v), nil
	}
	if len(v) != 0 {
		return 0, fmt.Errorf("graph: read epoch: corrupt lineage id (%d bytes)", len(v))
	}
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 0, fmt.Errorf("graph: mint epoch: %w", err)
	}
	// 0 is the zero-cursor sentinel; never mint it.
	if binary.BigEndian.Uint64(buf[:]) == 0 {
		buf[7] = 1
	}
	if err := mk.MetaSet(graphEpochMeta, buf[:]); err != nil {
		return 0, fmt.Errorf("graph: persist epoch: %w", err)
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}
