package badger

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
)

// postingsBatchCap bounds one batch of walkPostings after the first.
const postingsBatchCap = 1 << 14

// walkPostings feeds visit the IDs of the sorted posting list p above after,
// ascending, in batches copied under idxMu's read lock and released before
// visit runs (visit reads rows, which must not happen under idxMu). The first
// batch holds limit IDs (every ID when limit <= 0, in one batch), each later
// one twice the previous up to postingsBatchCap; visit returns how many more
// rows the page still needs, and the walk stops at 0 or the list's end. A page
// therefore reads its rows plus the candidates its filters drop, never the
// whole key (backlog 72). p is the index's own list (a composite single-ID key's
// is a fresh one-ID list, row 66): a write between batches is seen by the next
// batch, as a later read would see it; every row is re-checked against the
// current row by visit, so a moved ID is dropped.
// visit must not keep batch.
func (bs *Store) walkPostings(p *indexpkg.IDPostings, after snowflake.ID, limit int, visit func(batch []snowflake.ID) (need int, err error)) error {
	size := limit
	var batch []snowflake.ID
	for {
		bs.idxMu.RLock()
		batch = p.AppendAfter(batch[:0], after, size)
		bs.idxMu.RUnlock()
		if len(batch) == 0 {
			return nil
		}
		need, err := visit(batch)
		if err != nil || need <= 0 || size <= 0 || len(batch) < size {
			return err
		}
		after = batch[len(batch)-1]
		size = min(2*size, postingsBatchCap)
	}
}

// pagePostings is one keyset page (opts.After, opts.Limit) of an equality
// door over the sorted posting list p: fetch turns a batch of candidate IDs
// into the rows that pass the door's filters, at most page.Limit of them
// (page is opts with After cleared and Limit the rows still needed). nil when
// empty, as the doors return.
func pagePostings[T any](bs *Store, p *indexpkg.IDPostings, opts QueryOpts, fetch func(batch []snowflake.ID, page QueryOpts) ([]T, error)) ([]T, error) {
	var out []T
	page := opts
	page.After = 0
	err := bs.walkPostings(p, opts.After.SnowflakeID(), opts.Limit, func(batch []snowflake.ID) (int, error) {
		if opts.Limit > 0 {
			page.Limit = opts.Limit - len(out)
		}
		rows, err := fetch(batch, page)
		if err != nil {
			return 0, err
		}
		out = append(out, rows...)
		return opts.Limit - len(out), nil
	})
	if err != nil || len(out) == 0 {
		return nil, err
	}
	return out, nil
}

// fillFromBuild adds a build's postings to idx in ascending ID order per key,
// so a rebuild that read rows in label-index (map) order still fills exact
// chunks (backlog 72).
func fillFromBuild(b *indexpkg.PostingsBuild, add func(id snowflake.ID, vk string)) {
	b.Range(func(vk string, ids []snowflake.ID) bool {
		for _, id := range ids {
			add(id, vk)
		}
		return true
	})
}
