package badger

import (
	"errors"
	"math"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A stripe collision invalidates a prefetch conservatively. No entity can
// inherit a deleted entity's proof: deletes advance the same stripe, and Clear
// advances every stripe. Exhausted stripes become permanently unusable proofs.
const identityRevisionStripeCount = 4096

type identityRevisionState struct {
	nodes [identityRevisionStripeCount]uint64
	rels  [identityRevisionStripeCount]uint64
}

var _ storecontract.CurrentIdentityCapability = (*Store)(nil)

// UnsupportedIdentityProbeError identifies a legacy boolean identity probe
// used with disk identity mode. Use NodeExists or RelationshipExists instead;
// their errors distinguish absence from unavailable storage.
type UnsupportedIdentityProbeError struct {
	Operation string
}

func (e UnsupportedIdentityProbeError) Error() string {
	return "graph: " + e.Operation + " is unavailable in disk identity mode; use the error-returning existence API"
}

// Unwrap exposes the unsupported capability sentinel.
func (e UnsupportedIdentityProbeError) Unwrap() error {
	return storecontract.ErrCapabilityNotSupported
}

// IdentityCallbackMaintenanceError refuses maintenance while an identity
// callback is active. Retry after iteration; no store state was changed.
type IdentityCallbackMaintenanceError struct {
	Operation string
}

func (e IdentityCallbackMaintenanceError) Error() string {
	return "graph: " + e.Operation + " requires identity iteration callbacks to finish"
}

// Unwrap exposes the unsupported capability sentinel.
func (e IdentityCallbackMaintenanceError) Unwrap() error {
	return storecontract.ErrCapabilityNotSupported
}

func identityRevisionStripe(id types.EntityID) uint64 {
	x := uint64(id)
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	return x & (identityRevisionStripeCount - 1)
}

func advanceIdentityRevision(stamp *uint64) {
	if *stamp == math.MaxUint64 {
		return
	}
	*stamp++
}

func usableIdentityRevision(stamp uint64) uint64 {
	if stamp == math.MaxUint64 {
		return 0
	}
	return stamp
}

// NodeExists reports canonical current-row existence. Storage and corruption
// errors remain distinguishable from absence, including pending writes.
func (bs *Store) NodeExists(id types.NodeID) (bool, error) {
	if err := bs.checkOpen(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateNodeID(id); err != nil {
		return false, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	return bs.nodeExistsLocked(id)
}

// RelationshipExists is NodeExists for a relationship's current row.
func (bs *Store) RelationshipExists(id types.RelID) (bool, error) {
	if err := bs.checkOpen(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateRelID(id); err != nil {
		return false, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	return bs.relExistsLocked(id)
}

func (bs *Store) nodeExistsLocked(id types.NodeID) (bool, error) {
	if !bs.entityIdentityOnDisk {
		_, exists := bs.nodeIDs[id]
		return exists, nil
	}
	_, err := bs.getNodeLocked(id)
	if errors.Is(err, ErrNodeNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (bs *Store) relExistsLocked(id types.RelID) (bool, error) {
	if !bs.entityIdentityOnDisk {
		_, exists := bs.relIDs[id]
		return exists, nil
	}
	_, err := bs.getRelLocked(id)
	if errors.Is(err, ErrRelNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (bs *Store) addNodeIdentityLocked(id types.NodeID) {
	if !bs.entityIdentityOnDisk {
		bs.nodeIDs[id] = struct{}{}
	}
}

func (bs *Store) addRelIdentityLocked(id types.RelID) {
	if !bs.entityIdentityOnDisk {
		bs.relIDs[id] = struct{}{}
	}
}

func (bs *Store) removeNodeIdentityLocked(id types.NodeID) {
	delete(bs.nodeIDs, id)
}

func (bs *Store) removeRelIdentityLocked(id types.RelID) {
	delete(bs.relIDs, id)
}

func (bs *Store) setNodeHashLocked(id types.NodeID, hash string) {
	if !bs.entityIdentityOnDisk {
		bs.nodeHashes[id] = hash
	}
}

func (bs *Store) nodeRevisionLocked(id types.NodeID) uint64 {
	if bs.entityIdentityOnDisk {
		return usableIdentityRevision(bs.identityRevisions.nodes[identityRevisionStripe(types.EntityID(id))])
	}
	return bs.nodeRevs[id]
}

func (bs *Store) relRevisionLocked(id types.RelID) uint64 {
	if bs.entityIdentityOnDisk {
		return usableIdentityRevision(bs.identityRevisions.rels[identityRevisionStripe(types.EntityID(id))])
	}
	return bs.relRevs[id]
}

// Decode the captured buffered value before consulting disk. The caller holds
// idxMu, so a writer cannot replace it during this lookup; a completed flush
// can remove the overlay only after the exact value is durable.
func (bs *Store) bufferedNodeLocked(id types.NodeID) (*types.Node, bool, error) {
	op, ok := bs.lookupPending(string(storeutil.NodeKey(id.SnowflakeID())))
	if !ok {
		return nil, false, nil
	}
	if op.opType == writeOpDelete {
		return nil, true, ErrNodeNotFound
	}
	var wire storeutil.NodeWire
	if err := storeutil.SafeUnmarshal(op.value, &wire); err != nil {
		return nil, true, err
	}
	n, err := bs.decodeNodeWireForKey(wire, id.SnowflakeID())
	if err == nil {
		n.Freeze()
	}
	return n, true, err
}

func (bs *Store) bufferedRelLocked(id types.RelID) (*types.Relationship, bool, error) {
	op, ok := bs.lookupPending(string(storeutil.RelKey(id.SnowflakeID())))
	if !ok {
		return nil, false, nil
	}
	if op.opType == writeOpDelete {
		return nil, true, ErrRelNotFound
	}
	var wire storeutil.RelWire
	if err := storeutil.SafeUnmarshal(op.value, &wire); err != nil {
		return nil, true, err
	}
	r, err := bs.decodeRelWireForKey(wire, id.SnowflakeID())
	if err == nil {
		r.Freeze()
	}
	return r, true, err
}
