package badger

import (
	"fmt"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A read that finds no row at a live entity's key meets damage, not a delete.
// badger's point read and its iterator report a table block that fails its
// checksum as a missing key, and since EXISTMAP a clean open takes the ID sets
// from the open image instead of walking the keys (the walk met the damage and
// the open failed closed). So a reader captures the entity's revision while
// the ID set holds it, before it reads; on a miss, the ID still held with the
// revision unchanged means no write or delete ran in between, and the miss is
// corruption (ErrCorruptWire), never "not found". A changed revision (a delete,
// a Clear, or a write sharing the stripe) keeps "not found".

// errLiveRowMissing is that corruption. Paths that delete may match it to
// scrub the entity as they scrub an orphan (the cascade's locked re-read).
var errLiveRowMissing = fmt.Errorf("%w: a live entity's row reads as missing (a damaged table)", storecontract.ErrCorruptWire)

// liveNodeRev returns nid's revision while nodeIDs holds it, else 0 (0 never
// reports corruption). Takes idxMu for reading.
func (bs *Store) liveNodeRev(nid types.NodeID) uint64 {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	return bs.liveNodeRevLocked(nid)
}

func (bs *Store) liveNodeRevLocked(nid types.NodeID) uint64 {
	if !bs.nodeIDs.Has(int64(nid)) {
		return 0
	}
	return bs.nodeRevs.get(int64(nid))
}

// nodeRowMissing answers a read of nid's row that found none, rev captured by
// liveNodeRev before the read. Takes idxMu for reading.
func (bs *Store) nodeRowMissing(nid types.NodeID, rev uint64) error {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	return bs.nodeRowMissingLocked(nid, rev)
}

func (bs *Store) nodeRowMissingLocked(nid types.NodeID, rev uint64) error {
	if rev != 0 && bs.liveNodeRevLocked(nid) == rev {
		return fmt.Errorf("%w: node %d", errLiveRowMissing, nid.SnowflakeID())
	}
	return ErrNodeNotFound
}

// liveRelRev is liveNodeRev for relationships.
func (bs *Store) liveRelRev(rid types.RelID) uint64 {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	return bs.liveRelRevLocked(rid)
}

func (bs *Store) liveRelRevLocked(rid types.RelID) uint64 {
	if !bs.relIDs.Has(int64(rid)) {
		return 0
	}
	return bs.relRevs.get(int64(rid))
}

// relRowMissing is nodeRowMissing for relationships.
func (bs *Store) relRowMissing(rid types.RelID, rev uint64) error {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	return bs.relRowMissingLocked(rid, rev)
}

func (bs *Store) relRowMissingLocked(rid types.RelID, rev uint64) error {
	if rev != 0 && bs.liveRelRevLocked(rid) == rev {
		return fmt.Errorf("%w: relationship %d", errLiveRowMissing, rid.SnowflakeID())
	}
	return ErrRelNotFound
}
