package badger

import (
	"unsafe"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/dgraph-io/badger/v4/skl"
)

// MemoryAccount is what an open store holds in RAM by holder (MEMACCT): the
// per-entity and bounded holders by the index estimators (footprint.go), the
// badger arenas by their configured sizes. The decoded-entity caches are
// EntityCacheUsage's. Not counted, though held per entity once built: the
// property indexes and membership sidecars (their Footprint), the label and
// adjacency maps of a store that keeps them in RAM, the lazy label sets,
// the pinned-scan member lists, the kept scan orders and the rels' valid
// stamps. A closed store's account is zero.
type MemoryAccount struct {
	// IDSets is the live node and rel ID sets and the rel-type index, about
	// 8 B a live node and 17 B a live rel. A set kept in the open image's
	// encoding counts its own stream (about 5 B an ID), not the rest of the
	// image blob it keeps alive.
	IDSets int64
	// Revisions is the two striped revision tables, 128 KiB each.
	Revisions int64
	// NodeHashes is the node integrity hash cache: NodeHashEntries of at most
	// NodeHashCap, filled by NodeIntegrityHash (the endpoint hashes a rel
	// write reads), emptied with the handle. NodeHashHits and NodeHashMisses
	// count its lookups since the open that found a cached hash or not; a
	// miss recomputes the hash from the cached or stored row and fills it on
	// the node's second lookup (a 128 KiB doorkeeper, counted here, once
	// the first miss allocates it; HASHFILL).
	// Summed over a tier's stores, NodeHashCap is the sum of their caps.
	NodeHashes      int64
	NodeHashEntries int
	NodeHashCap     int
	NodeHashHits    uint64
	NodeHashMisses  uint64
	// Sketches is the HyperLogLog sketches of the node and rel property
	// statistics, one per label (or rel type) and property: sparse up to a
	// quarter of its 16,384 registers, then 16,384 B for good. SketchesDense
	// counts the dense ones.
	Sketches      int64
	SketchesDense int
	// MemTableArena is the active memtable's arena, allocated whole at a
	// writable open (MemTableSize and badger's two batch reserves of 15 %
	// each; none read-only). MemTableArenaMax is the bound under ingest, not
	// in Bytes: badger v4.9.2 keeps the active arena, NumMemtables rotated
	// ones queued in flushChan (db.go:235, ensureRoomForWrite db.go:1025-1031)
	// and the one its flusher took off the queue and still lists in db.imm
	// (db.go:1105-1126): NumMemtables + 2 arenas. BlockCache and IndexCache
	// are the caches' configured caps (ristretto's MaxCost), which they fill
	// lazily: what they hold now is StorageStats' Held(). An IndexCache of 0
	// keeps the table indexes uncapped (StorageStats.IndexBytes).
	MemTableArena    int64
	MemTableArenaMax int64
	BlockCache       int64
	IndexCache       int64
}

// Bytes sums the account: what its holders keep now, the active arena and
// the caches at their caps (MemTableArenaMax, the bound under ingest, is not
// in it).
func (a MemoryAccount) Bytes() int64 {
	return a.IDSets + a.Revisions + a.NodeHashes + a.Sketches + a.MemTableArena + a.BlockCache + a.IndexCache
}

// Add sums b into a (a tier's open stores).
func (a *MemoryAccount) Add(b MemoryAccount) {
	a.IDSets += b.IDSets
	a.Revisions += b.Revisions
	a.NodeHashes += b.NodeHashes
	a.NodeHashEntries += b.NodeHashEntries
	a.NodeHashCap += b.NodeHashCap
	a.NodeHashHits += b.NodeHashHits
	a.NodeHashMisses += b.NodeHashMisses
	a.Sketches += b.Sketches
	a.SketchesDense += b.SketchesDense
	a.MemTableArena += b.MemTableArena
	a.MemTableArenaMax += b.MemTableArenaMax
	a.BlockCache += b.BlockCache
	a.IndexCache += b.IndexCache
}

// MemoryAccount returns the store's account (MemoryAccount); zero once
// closing or closed. It holds idxMu shared while it walks the ID sets' chunk directories
// (one step per 512 to 1,024 IDs) and the statistics' sketches (one per
// label and property): it reads no row and opens nothing.
func (bs *Store) MemoryAccount() MemoryAccount {
	if bs == nil || bs.db == nil || bs.isClosingOrClosed() {
		return MemoryAccount{}
	}
	opts := bs.db.Opts()
	a := MemoryAccount{
		NodeHashHits:   bs.nodeHashHits.Load(),
		NodeHashMisses: bs.nodeHashMisses.Load(),
		BlockCache:     opts.BlockCacheSize,
		IndexCache:     opts.IndexCacheSize,
	}
	if !opts.ReadOnly {
		batch := 15 * opts.MemTableSize / 100 // badger's maxBatchSize; arenaSize adds maxBatchCount nodes
		a.MemTableArena = opts.MemTableSize + batch + batch/int64(skl.MaxNodeSize)*int64(skl.MaxNodeSize)
		a.MemTableArenaMax = int64(opts.NumMemtables+2) * a.MemTableArena
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	a.IDSets = bs.nodeIDs.Bytes() + bs.relIDs.Bytes() + indexpkg.MapBytes[uint16, *indexpkg.IDSet](len(bs.typeIdx))
	for _, set := range bs.typeIdx {
		a.IDSets += indexpkg.AllocBytes(int64(unsafe.Sizeof(*set))) + set.Bytes()
	}
	a.Revisions = bs.nodeRevs.bytes() + bs.relRevs.bytes()
	a.NodeHashEntries, a.NodeHashCap = len(bs.nodeHashes), bs.nodeHashLimit()
	a.NodeHashes = indexpkg.MapBytes[types.NodeID, string](len(bs.nodeHashes)) + bs.nodeHashStrBytes + bs.nodeHashDoor.bytes()
	for _, acc := range bs.propertyStats {
		a.addSketch(acc)
	}
	for _, acc := range bs.relPropertyStats {
		a.addSketch(acc)
	}
	return a
}

func (a *MemoryAccount) addSketch(acc *indexpkg.PropertyStatsAccumulator) {
	a.Sketches += acc.SketchBytes()
	if acc.SketchDense() {
		a.SketchesDense++
	}
}
