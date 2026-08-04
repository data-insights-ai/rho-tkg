package badger

import (
	"errors"
	"strconv"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Persisted columns (CP2/CP3), behind Config.ColumnsOnDisk.
//
// A persisted column is a REBUILD ACCELERATOR and never a read authority — the rule
// Config.TemporalIndexOnDisk already states. Every failure mode is "discard and
// rebuild": a stale epoch stamp, an unreadable blob, a missing key, a column that
// does not cover the requested properties. None of them can produce a wrong answer,
// only a slower one, which is why this needs no wire-format bump and no migration.
//
// ONE DEPARTURE FROM THE TEMPORAL-INDEX PRECEDENT, deliberate. That index is
// maintained at ENTITY-WRITE time, so its rows must ride the same appendOps batch as
// the entity for crash consistency. Columns are built LAZILY AT READ time, so there
// is no entity write to ride along with, and coupling them would mean building a
// column on the write path — the opposite of what the cache is for. Instead each
// blob carries the epoch it was built at, and a crash mid-write leaves either no key
// or an unreadable one. Both mean rebuild.
//
// The blob is written through MetaSet, outside any entity transaction, precisely
// because it must NOT be able to fail an entity write.

// columnDiskKey names the persisted blob for one label's columns. Keyed by label
// token; the epoch lives INSIDE the blob so a stale key is detected on read rather
// than needing a key-space sweep on every write.
func columnDiskKey(labelToken uint16) string {
	return "doccol_n_" + strconv.FormatUint(uint64(labelToken), 10)
}

// relColumnDiskKey is the relationship-type sibling.
func relColumnDiskKey(typeToken uint16) string {
	return "doccol_r_" + strconv.FormatUint(uint64(typeToken), 10)
}

// columnChunkBytes splits a blob across meta keys. Badger refuses a value over
// 1 MiB, and a 50,000-row column encodes to ~1.6 MB — so an unchunked write FAILED
// for exactly the labels persistence is most worth having. 512 KiB leaves headroom
// under that ceiling.
//
// A var, not a const, so a test can shrink it and exercise multi-chunk paths without
// building a million-row fixture.
var columnChunkBytes = 512 * 1024

// persistColumns writes a built snapshot across chunk keys, best-effort.
//
// CHUNKS FIRST, HEADER LAST. The header names the chunk count, so a crash or a
// partial write leaves orphan chunks and NO header — and a missing header means
// "rebuild". The reverse order would leave a header pointing at chunks that were
// never written, which is the one ordering that could serve a torn column.
//
// A write failure is swallowed because a cache must never fail a query. That is not
// free of consequence and the caller should know it: the next rebuild will try
// again, so a systematically unwritable blob costs an encode per rebuild. Sizing it
// out of that failure mode, rather than tolerating it, is why chunking exists.
func persistColumns[T indexpkg.EntityID](bs *Store, key string, col *indexpkg.DocValues[T]) {
	if !bs.columnsOnDisk || col == nil {
		return
	}
	blob := indexpkg.EncodeColumns(col)
	n := (len(blob) + columnChunkBytes - 1) / columnChunkBytes
	for i := 0; i < n; i++ {
		lo := i * columnChunkBytes
		hi := min(lo+columnChunkBytes, len(blob))
		if err := bs.MetaSet(columnChunkKey(key, i), blob[lo:hi]); err != nil {
			return // leave the header unwritten; the partial chunks are inert
		}
	}
	_ = bs.MetaSet(key, []byte(strconv.Itoa(n)))
}

// columnChunkKey names one chunk of a persisted column.
func columnChunkKey(key string, i int) string {
	return key + "_c" + strconv.Itoa(i)
}

// readColumnBlob reassembles a chunked blob, or nil if the header is missing or any
// chunk is. Both mean rebuild.
func readColumnBlob(bs *Store, key string) []byte {
	head, err := bs.MetaGet(key)
	if err != nil || len(head) == 0 {
		return nil
	}
	n, err := strconv.Atoi(string(head))
	if err != nil || n <= 0 || n > 1<<20 {
		return nil
	}
	out := make([]byte, 0, n*columnChunkBytes)
	for i := 0; i < n; i++ {
		part, err := bs.MetaGet(columnChunkKey(key, i))
		if err != nil || len(part) == 0 {
			return nil // a chunk this header claims is absent
		}
		out = append(out, part...)
	}
	return out
}

// loadColumns returns a persisted snapshot if one exists AND is current for gen AND
// covers keys. Any other outcome returns nil, meaning "build it".
//
// The epoch check is what makes this safe without a format contract: a blob written
// before any write since is simply not used.
func loadColumns[T indexpkg.EntityID](bs *Store, key string, gen uint64, keys []string,
	mk func(int64) T) *indexpkg.DocValues[T] {

	if !bs.columnsOnDisk {
		return nil
	}
	blob := readColumnBlob(bs, key)
	if len(blob) == 0 {
		return nil
	}
	col, err := indexpkg.DecodeColumns(blob, mk)
	if err != nil {
		// Unreadable: a blob from a newer binary, a truncation, a corrupt
		// dictionary. Every one of them means rebuild.
		if !errors.Is(err, indexpkg.ErrColumnBlobUnreadable) {
			return nil
		}
		return nil
	}
	if col.Epoch() != gen || !col.HasAll(keys) {
		return nil
	}
	return col
}

// loadNodeColumns / loadRelColumns bind the ID constructor for each side.
func (bs *Store) loadNodeColumns(labelToken uint16, gen uint64, keys []string) *indexpkg.LabelDocValues {
	return loadColumns(bs, columnDiskKey(labelToken), gen, keys,
		func(v int64) types.NodeID { return types.NodeID(v) })
}

func (bs *Store) loadRelColumnsDisk(typeToken uint16, gen uint64, keys []string) *indexpkg.DocValues[types.RelID] {
	return loadColumns(bs, relColumnDiskKey(typeToken), gen, keys,
		func(v int64) types.RelID { return types.RelID(v) })
}
