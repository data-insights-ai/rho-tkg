package core

import (
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// supersessionEnds maps each row of a (TxAt-filtered, tombstone-normalized)
// chain that a positional write superseded to the valid start of the row that
// superseded it. The point resolver's own-bounds arm (a chain a cascade made
// non-monotonic) applies it as a cap on the row's own end.
//
// Two kinds of write append a row. A cascade asserts its rows over their own
// [ValidFrom, ValidTo) and leaves the rows it overlays untouched (TxTo stays
// 0). Every other appending write (Update, CloseVersion, a label change, a
// property CAS, UpdateWithTx, a replica's apply of one of them) replaces the
// current row from the new row's valid start onward and stamps the replaced
// row's TxTo with the new row's TxFrom. The monotonic arm reads that
// replacement positionally (a row ends where the next one starts); the
// own-bounds arm read every row over its own interval, so after a cascade
// made the chain non-monotonic the replaced row's open interval answered
// again wherever the replacing row did not reach: after CloseVersion(4000)
// and a bounded cascade inside [1000,4000), NodeAt(5000) answered the
// pre-close row (consumer report, sigma-tkgd 2026-10-09).
//
// A row is superseded when it carries a TxTo after its own TxFrom and no
// DeletedAt (a tombstone's TxTo is the delete, capped by lifeEnds; a TxTo
// below the row's TxFrom was copied by a pre-4.46 cascade from its source row
// and supersedes nothing). Its successor is the lowest-version row above it
// recorded at that TxTo; when the successor is not in the chain (recorded
// after the pin) the row is not capped — at that pin it was not yet replaced.
// A nil map caps nothing.
func supersessionEnds[T interface {
	comparable
	storeutil.TemporalRow
}](chain []T, start func(T) types.Instant) lifeEnds[T] {
	var caps lifeEnds[T]
	for _, r := range chain {
		tm := r.Temporal()
		if tm == nil || tm.TxTo == 0 || tm.DeletedAt != 0 || tm.TxTo <= tm.TxFrom {
			continue
		}
		var succ T
		found := false
		for _, s := range chain {
			stm := s.Temporal()
			if stm == nil || stm.TxFrom != tm.TxTo || s.Version() <= r.Version() {
				continue
			}
			if !found || s.Version() < succ.Version() {
				succ, found = s, true
			}
		}
		if !found {
			continue
		}
		if caps == nil {
			caps = make(lifeEnds[T], len(chain))
		}
		caps[r] = start(succ)
	}
	return caps
}
