package badger

import (
	"fmt"
	"runtime"
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// parallelDecodeMinIDs is the candidate-count floor below which parallel decode is
// NOT worth the goroutine fan-out + raw-byte staging overhead — small scans stay on
// the serial forEachNodeBulk path.
const parallelDecodeMinIDs = 2048

// parallelDecodeBatch bounds how many misses' raw value bytes are staged at once: the
// serial pass copies up to this many, the workers decode them, then the buffer is
// reused for the next batch. This caps the collector's TRANSIENT memory (raw bytes)
// to O(batch) instead of O(all misses) — the decoded nodes are the caller's result
// and are inherently O(N), but the raw-byte staging need not be. Large enough that the
// per-batch goroutine fan-out is amortized.
const parallelDecodeBatch = 8192

// decodeJob carries a badger miss to be decoded off-thread, tagged with its output slot.
type decodeJob struct {
	idx int
	id  snowflake.ID
	raw []byte
}

// collectNodesBulkParallel materializes the given (sorted, ascending) node IDs like
// forEachNodeBulk, but decodes the badger-resident misses ACROSS CORES. The badger
// read transaction / iterator is NOT safe for concurrent use, so the SERIAL pass only
// SEEKS + copies each miss's raw value bytes (cheap); the CPU-bound decode
// (SafeUnmarshal → decodeNodeWireForKey → Freeze) is then fanned across a worker pool.
// Decode is data-parallel-safe: it reads the atomic-loaded property-key registry
// (RLock-protected) and mutates only a per-decode local wire; every node is fresh and
// frozen independently, written to its OWN result slot (no shared writes).
//
// Returns present nodes in ID order (absent/deleted IDs skipped), identical to
// forEachNodeBulk's callback set. No early-stop — this path is for full, unbounded
// label scans (MATCH (n:L) RETURN n); Limit'd scans keep the serial early-stopping
// door. Cache hits are served inline (no promote — scan discipline) exactly as the
// serial path.
func (bs *Store) collectNodesBulkParallel(ids []types.NodeID) ([]*types.Node, error) {
	results := make([]*types.Node, len(ids))
	var jobs []decodeJob

	// Serial pass: cache hits fill results inline; misses stage their raw bytes.
	// Misses read through a scanSnapshot, which re-pins after a flush (see its
	// doc comment for the dropped/replaced row a single snapshot would cause).
	snap := newScanSnapshot(bs.db, bs.nodeCache.FlushEpoch)
	defer snap.close()
	for i, nid := range ids {
		id := nid.SnowflakeID()
		v, status := bs.nodeCache.GetNoPromote(id)
		switch status {
		case indexpkg.CacheHit:
			results[i] = v
			continue
		case indexpkg.CacheDeleted:
			continue // tombstone — leave nil (skipped in compaction)
		}
		item := snap.seekAfterMiss(storepkg.NodeKey(id))
		if item == nil {
			continue // deleted / orphaned index entry
		}
		raw, cerr := item.ValueCopy(nil) // copy — bytes must outlive the txn
		if cerr != nil {
			return nil, fmt.Errorf("graph: read node value: %w", cerr)
		}
		jobs = append(jobs, decodeJob{idx: i, id: id, raw: raw})
		// Flush a full batch: decode it in parallel, then reuse the buffer so the
		// staged raw bytes stay bounded (jobs[:0] keeps the array; the decoded
		// batch's raw slices become unreferenced and GC-able).
		if len(jobs) >= parallelDecodeBatch {
			if derr := bs.decodeJobsParallel(jobs, results); derr != nil {
				return nil, derr
			}
			jobs = jobs[:0]
		}
	}

	// Final partial batch (raw bytes are copies, valid after the txn closed).
	if derr := bs.decodeJobsParallel(jobs, results); derr != nil {
		return nil, derr
	}

	// Compact in ID order, skipping absent/deleted slots.
	out := make([]*types.Node, 0, len(ids))
	for _, n := range results {
		if n != nil {
			out = append(out, n)
		}
	}
	return out, nil
}

// decodeJobsParallel decodes each staged job into its result slot across GOMAXPROCS
// contiguous chunks (deadlock-free: each goroutine owns a fixed range and writes only
// its own slots). The first decode error wins; the rest of that chunk is abandoned.
func (bs *Store) decodeJobsParallel(jobs []decodeJob, results []*types.Node) error {
	if len(jobs) == 0 {
		return nil
	}
	workers := runtime.GOMAXPROCS(0)
	if workers > len(jobs) {
		workers = len(jobs)
	}
	if workers < 1 {
		workers = 1
	}
	chunk := (len(jobs) + workers - 1) / workers

	var (
		wg      sync.WaitGroup
		errOnce sync.Once
		decErr  error
	)
	for w := 0; w < workers; w++ {
		lo := w * chunk
		if lo >= len(jobs) {
			break
		}
		hi := lo + chunk
		if hi > len(jobs) {
			hi = len(jobs)
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			for j := lo; j < hi; j++ {
				jb := jobs[j]
				var wire storepkg.NodeWire
				if uerr := storepkg.SafeUnmarshal(jb.raw, &wire); uerr != nil {
					errOnce.Do(func() { decErr = fmt.Errorf("graph: unmarshal node: %w", uerr) })
					return
				}
				n, derr := bs.decodeNodeWireForKey(wire, jb.id)
				if derr != nil {
					errOnce.Do(func() { decErr = derr })
					return
				}
				n.Freeze()
				results[jb.idx] = n
			}
		}(lo, hi)
	}
	wg.Wait()
	return decErr
}
