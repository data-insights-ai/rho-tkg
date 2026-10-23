package core

import (
	"cmp"
	"errors"
	"reflect"
	"slices"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Effective scans read through one store snapshot (backlog 55, ai-soc restart
// replay and sigma restore: 33.4 s of 61.9 s CPU in one badger View per read)
//
// A scan reads its candidates' timelines effScanChunk at a time under one
// hold of the graph read lock, each entity under its own entity lock as
// before, through one store.ReadSnapshot where the store offers it (badger:
// one transaction and forward-walking iterators; sharded: one per slot;
// tiered: one for the reference shard, row 121), and
// hands the chunk's segments to fn after the lock is released. Each entity is
// still read at one moment, and a snapshot read equals the store's own read
// at that moment (store.ReadSnapshot's contract), so every answer is the
// per-ID path's; the store opens one transaction per chunk plus one per flush
// that lands during it, none while fn runs. Memory reads entity by entity
// through the store; tiered reads its event shards and archive so (it checks
// a shard out per read, and a cold shard may close between reads).
// =============================================================================

// effScanChunk is how many entities a scan reads per hold of the graph read
// lock: the segments it buffers, and the reads a Close or an admin writer may
// wait behind.
const effScanChunk = 256

// readSnapshotCapability resolves store.ReadSnapshotCapability with the K1
// discipline: an exact native store or one declaring NewReadSnapshot itself;
// a wrapper that merely embeds a native store is refused (its own reads may
// differ from the embedded store's).
func readSnapshotCapability(store storepkg.MandatoryStore) storepkg.ReadSnapshotCapability {
	snaps, ok := store.(storepkg.ReadSnapshotCapability)
	if !ok {
		return nil
	}
	if isExactNativeStore(store) {
		return snaps
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.ReadSnapshotCapability)(nil)).Elem(), "NewReadSnapshot") {
		return nil
	}
	return snaps
}

// entityReads is where a per-entity timeline reads its rows: the store (snap
// nil) or a store read snapshot. Rows pass the same trust checks either way.
type entityReads struct {
	c    *Core
	snap storepkg.ReadSnapshot
	// scan marks a multi-entity scan: it neither reads nor fills the
	// head-pin cache, which serves repeated point reads of a small hot set.
	scan bool
}

func (r entityReads) currentNode(id types.NodeID) (*types.Node, error) {
	if r.snap == nil {
		return r.c.getCurrentNode(id)
	}
	n, err := r.snap.GetNode(id)
	return r.c.trustedCurrentNode(id, n, err)
}

// lentCurrentNode is currentNode without the defensive copy where the store
// (or its read snapshot) lends its frozen row (trusted stores only): for a
// reader that only hands the row out frozen, such as an effective timeline.
func (r entityReads) lentCurrentNode(id types.NodeID) (*types.Node, error) {
	if !r.c.storeRowsTrust {
		return r.currentNode(id)
	}
	if r.snap != nil {
		if lender, ok := r.snap.(storepkg.ReadSnapshotLendCapability); ok {
			return lender.LendNode(id)
		}
	} else if lender, ok := r.c.store.(storepkg.EntityLendCapability); ok {
		return lender.LendNode(id)
	}
	return r.currentNode(id)
}

// lentCurrentRel is lentCurrentNode for relationships.
func (r entityReads) lentCurrentRel(id types.RelID) (*types.Relationship, error) {
	if !r.c.storeRowsTrust {
		return r.currentRel(id)
	}
	if r.snap != nil {
		if lender, ok := r.snap.(storepkg.ReadSnapshotLendCapability); ok {
			return lender.LendRelationship(id)
		}
	} else if lender, ok := r.c.store.(storepkg.EntityLendCapability); ok {
		return lender.LendRelationship(id)
	}
	return r.currentRel(id)
}

func (r entityReads) currentRel(id types.RelID) (*types.Relationship, error) {
	if r.snap == nil {
		return r.c.getCurrentRelationship(id)
	}
	rel, err := r.snap.GetRelationship(id)
	return r.c.trustedCurrentRel(id, rel, err)
}

// hasNodeHistory is store.HistoryPresenceCapability's answer; known is false
// when the store does not offer it.
func (r entityReads) hasNodeHistory(id types.NodeID) (has, known bool, err error) {
	p, ok := r.c.store.(storepkg.HistoryPresenceCapability)
	if !ok {
		return false, false, nil
	}
	if r.snap != nil {
		has, err = r.snap.HasNodeHistory(id)
	} else {
		has, err = p.HasNodeHistory(id)
	}
	return has, true, err
}

func (r entityReads) hasRelHistory(id types.RelID) (has, known bool, err error) {
	p, ok := r.c.store.(storepkg.HistoryPresenceCapability)
	if !ok {
		return false, false, nil
	}
	if r.snap != nil {
		has, err = r.snap.HasRelHistory(id)
	} else {
		has, err = p.HasRelHistory(id)
	}
	return has, true, err
}

// nodeMeta and relMeta are called only when c.temporalMetaHistory is set.
func (r entityReads) nodeMeta(id types.NodeID) ([]storepkg.VersionTemporalMeta, error) {
	if r.snap == nil {
		return r.c.temporalMetaHistory.NodeHistoryTemporalMeta(id)
	}
	return r.snap.NodeHistoryTemporalMeta(id)
}

func (r entityReads) relMeta(id types.RelID) ([]storepkg.VersionTemporalMeta, error) {
	if r.snap == nil {
		return r.c.temporalMetaHistory.RelHistoryTemporalMeta(id)
	}
	return r.snap.RelHistoryTemporalMeta(id)
}

func (r entityReads) nodeVersion(id types.NodeID, version uint32) (*types.Node, error) {
	if r.snap == nil {
		return r.c.getNodeVersion(id, version)
	}
	n, err := r.snap.GetNodeVersion(id, version)
	return r.c.trustedNodeVersion(id, version, n, err)
}

func (r entityReads) relVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	if r.snap == nil {
		return r.c.getRelVersion(id, version)
	}
	rel, err := r.snap.GetRelVersion(id, version)
	return r.c.trustedRelVersion(id, version, rel, err)
}

func (r entityReads) nodeHistory(id types.NodeID) ([]*types.Node, error) {
	if r.snap == nil {
		return r.c.getNodeHistory(id)
	}
	h, err := r.snap.GetNodeHistory(id)
	return r.c.trustedNodeHistory(id, h, err)
}

func (r entityReads) relHistory(id types.RelID) ([]*types.Relationship, error) {
	if r.snap == nil {
		return r.c.getRelHistory(id)
	}
	h, err := r.snap.GetRelHistory(id)
	return r.c.trustedRelHistory(id, h, err)
}

// streamEntityTimelines reads each id's timeline in order (read, under the
// graph read lock) effScanChunk at a time and hands each to emit after the
// lock is released; emit returning false stops. A read error stops the scan
// once the entities read before it are emitted.
func streamEntityTimelines[ID any, S any](c *Core, ids []ID, read func(entityReads, ID) ([]S, error), emit func(ID, []S) bool) error {
	rd := entityReads{c: c, scan: true}
	if c.readSnapshots != nil {
		rd.snap = c.readSnapshots.NewReadSnapshot()
		defer rd.snap.Close()
	}
	type entry struct {
		id   ID
		segs []S
	}
	buf := make([]entry, 0, min(len(ids), effScanChunk))
	for start := 0; start < len(ids); start += effScanChunk {
		chunk := ids[start:min(start+effScanChunk, len(ids))]
		buf = buf[:0]
		var readErr error
		if err := c.readUnderRLock(func() error {
			if rd.snap != nil {
				defer rd.snap.Release() // no iterator (table pins) or read mark while fn runs
			}
			for _, id := range chunk {
				if c.scanReadTestHook != nil {
					c.scanReadTestHook()
				}
				segs, err := read(rd, id)
				if err != nil {
					readErr = err
					return nil
				}
				buf = append(buf, entry{id, segs})
			}
			return nil
		}); err != nil {
			return err
		}
		for _, e := range buf {
			if !emit(e.id, e.segs) {
				return nil
			}
		}
		if readErr != nil {
			return readErr
		}
	}
	return nil
}

// ascending returns a sorted copy of a scan door's candidates (their order is
// unspecified): a snapshot then walks each keyspace forward. A copy, since a
// gather may hand out a store's own slice.
func ascending[T cmp.Ordered](ids []T) []T {
	out := slices.Clone(ids)
	slices.Sort(out)
	return out
}

// skipUnknown maps an unknown entity (notFound) to no segments.
func skipUnknown[S any](segs []S, err, notFound error) ([]S, error) {
	if errors.Is(err, notFound) {
		return nil, nil
	}
	return segs, err
}
