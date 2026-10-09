package core

import (
	"cmp"
	"slices"
	"sync"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Supersession: where a replacing write ends an older belief.
//
// Two kinds of write append a row. A cascade (SetVersionInterval) asserts its
// rows over their own [ValidFrom, ValidTo) and leaves every row it overlays
// untouched — including the row it moves out of the current slot (its TxTo
// stays 0). Every other appending write (Update, CloseVersion, a label change,
// a property CAS, UpdateWithTx, a replica's apply of one of them) REPLACES the
// entity's state from the new row's valid start S on: it stamps the row it
// replaced with TxTo = the new row's TxFrom, and its row ends where its own
// ValidTo says (CloseVersion: nothing after it).
//
// So a replacing row s ends, at S = start(s), every belief older than s that
// started at or before S — not only the row s replaced: a cascade before the
// close moved the genesis out of the current slot without a TxTo, and the close
// ends the genesis too. Beliefs older than s that start after S (a correction
// of a later valid span) are not ended by it, and beliefs newer than s are not
// touched. This is the monotonic arm's positional rule (a row ends where the
// next one starts) stated for the own-bounds arm, which reads a chain a cascade
// made non-monotonic: without it, the genesis's open interval answered again
// beyond a close wherever no newer row reached (consumer report, sigma-tkgd
// 2026-10-09; review repros cascade-close-cascade and its variants).
//
// A replacing row is one recorded at the TxTo of a row it replaced: a row r
// with TxTo after its own TxFrom and no DeletedAt (a tombstone's TxTo is the
// delete, ended by lifeEnds; a TxTo below the row's TxFrom was copied by a
// pre-4.46 cascade and replaces nothing) and a row s above r's version with
// TxFrom == r.TxTo. When s is not in the chain (recorded after the pin) r is
// not ended: at that pin it was not yet replaced. Belief age is the resolver's
// (TxFrom, then version; among equals the earlier row in chain order is the
// newer, as resolveNodeVersionAtCapped keeps the first).

// supersessionCaps returns, for a chain the resolver sorted for its own-bounds
// arm (ascending by start, ties by version — sortNodeChainForResolve), the end
// of each row's belief: the smallest start S >= start(row) of a replacing row
// newer than it (0: none), aligned with chain; nil when no row of the chain
// was replaced. start is the resolver's start key (nodeSortValidFrom /
// relSortValidFrom), so chain is sorted by it. The returned caps live in sc
// (supersessionScratchPool): the caller reads them before releasing sc.
//
// O(n log n) without a sort on the usual chain: the replaced (TxTo, version)
// pairs come in write order (sorted then; sorted here otherwise), and one walk
// from the latest start to the earliest keeps a stack of the replacing rows
// seen so far that no other seen row dominates (a dominating row starts no
// later and is a newer belief); bottom to top their starts fall and their
// beliefs age, so the replacing rows newer than a row are a bottom run of the
// stack, and the top of that run (found by binary search) has the smallest
// start among them.
func supersessionCaps[T storeutil.TemporalRow](chain []T, start func(T) types.Instant, sc *supersessionScratch) []types.Instant {
	n := len(chain)
	if cap(sc.inst) < 3*n {
		sc.inst = make([]types.Instant, 3*n)
	}
	inst := sc.inst[:3*n]
	starts, caps, txs := inst[:n], inst[n:2*n], inst[2*n:]
	if cap(sc.versions) < n {
		sc.versions = make([]uint32, n)
	}
	versions := sc.versions[:n]
	replaced := sc.replaced[:0]
	for i, r := range chain {
		tm := r.Temporal()
		versions[i] = r.Version()
		txs[i] = beliefTx(tm)
		if tm != nil && tm.TxTo > tm.TxFrom && tm.DeletedAt == 0 {
			replaced = append(replaced, replacedRow{tm.TxTo, versions[i]})
		}
	}
	sc.replaced = replaced
	if len(replaced) == 0 {
		return nil
	}
	byTx := func(a, b replacedRow) int {
		if c := cmp.Compare(a.tx, b.tx); c != 0 {
			return c
		}
		return cmp.Compare(a.version, b.version)
	}
	if !slices.IsSortedFunc(replaced, byTx) {
		slices.SortFunc(replaced, byTx)
	}
	clear(caps)
	for i, r := range chain {
		starts[i] = start(r)
	}
	// replacing: a row recorded at the TxTo of a lower version it replaced
	// (replaced is sorted by TxTo, then version: the first match is the
	// lowest version replaced at that instant).
	//
	// The walk visits rows by falling start, and a row's start is usually its
	// write time, so the search gallops from the previous answer (hint): O(1)
	// per row on such chains, O(log m) otherwise.
	hint := len(replaced)
	replacing := func(i int) bool {
		tx := txs[i]
		// Lower bound: the first k with replaced[k].tx >= tx.
		lo, hi := 0, len(replaced)
		if hint == len(replaced) || replaced[hint].tx >= tx {
			// The answer is at most hint: gallop down from it.
			hi = hint
			step := 1
			for hi-step >= 0 && replaced[hi-step].tx >= tx {
				hi -= step
				step *= 2
			}
			lo = max(0, hi-step+1)
		} else {
			lo = hint + 1
		}
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if replaced[mid].tx < tx {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		hint = lo
		return lo < len(replaced) && replaced[lo].tx == tx && replaced[lo].version < versions[i]
	}
	// newer: a is a newer belief than b — (TxFrom, version), and among equals
	// the earlier position (the resolver keeps the first it meets).
	newer := func(a, b int) bool {
		if txs[a] != txs[b] {
			return txs[a] > txs[b]
		}
		if versions[a] != versions[b] {
			return versions[a] > versions[b]
		}
		return a < b
	}
	stack := sc.ints[:0]
	capped := false
	for i := n - 1; i >= 0; {
		j := i
		for j > 0 && starts[j-1] == starts[i] {
			j--
		}
		// Rows [j, i] share a start: every replacing row among them is a
		// candidate for each of them ("at or before S").
		for p := i; p >= j; p-- {
			if !replacing(p) {
				continue
			}
			for len(stack) > 0 && !newer(stack[len(stack)-1], p) {
				stack = stack[:len(stack)-1]
			}
			stack = append(stack, p)
		}
		for p := i; p >= j; p-- {
			// Usually p is older than the whole stack (falling start, falling
			// write time): then the answer is the top.
			lo, hi := 0, len(stack)
			if hi > 0 && newer(stack[hi-1], p) {
				lo = hi
			}
			for lo < hi {
				mid := int(uint(lo+hi) >> 1)
				if newer(stack[mid], p) {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
			m := lo
			if m > 0 {
				caps[p] = starts[stack[m-1]]
				capped = true
			}
		}
		i = j - 1
	}
	sc.ints = stack
	if !capped {
		return nil
	}
	return caps
}

// supersessionScratch is supersessionCaps' reusable working memory (a long
// chain needs several arrays of its length per point resolve).
type supersessionScratch struct {
	replaced []replacedRow
	ints     []int
	versions []uint32
	inst     []types.Instant
}

var supersessionScratchPool = sync.Pool{New: func() any { return new(supersessionScratch) }}

// maxPooledChain bounds the chain length whose scratch goes back to the pool,
// so one huge chain does not pin its arrays.
const maxPooledChain = 1 << 16

func getSupersessionScratch() *supersessionScratch {
	return supersessionScratchPool.Get().(*supersessionScratch)
}

func putSupersessionScratch(sc *supersessionScratch) {
	if cap(sc.inst) > 3*maxPooledChain {
		return
	}
	supersessionScratchPool.Put(sc)
}

// replacedRow is a row a replacing write superseded: the write's TxFrom (the
// row's TxTo) and the row's version.
type replacedRow struct {
	tx      types.Instant
	version uint32
}

// capEnd caps a valid end (0 = open) at cap (0 = none).
func capEnd(vEnd, cap types.Instant) types.Instant {
	if cap != 0 && (vEnd == 0 || vEnd > cap) {
		return cap
	}
	return vEnd
}
