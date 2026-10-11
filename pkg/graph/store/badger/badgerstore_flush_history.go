package badger

import (
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

func historySetWrite(op writeOp) bool {
	return op.opType == writeOpSet && len(op.key) > 0 && (op.key[0] == storepkg.KeyHistNode || op.key[0] == storepkg.KeyHistRel)
}

// A current-to-history move may place an interval anchor and dependent delta
// in the same pending snapshot. Badger can commit WriteBatch transactions out
// of submission order. Finish the full-snapshot batch before submitting any
// delta, then finish deltas before submitting current source rows.
func (bs *Store) flushHistorySetsBeforeCurrent(ops map[string]writeOp) error {
	for _, kind := range []storepkg.HistoryKind{storepkg.HistoryFull, storepkg.HistoryDelta} {
		var batch *badgerv4.WriteBatch
		for _, op := range ops {
			if !historySetWrite(op) || storepkg.HistoryValueKindOf(op.value) != kind {
				continue
			}
			if batch == nil {
				batch = bs.db.NewWriteBatch()
			}
			if err := batch.Set(op.key, op.value); err != nil {
				batch.Cancel()
				return fmt.Errorf("graph: history write-ahead set: %w", err)
			}
		}
		if batch == nil {
			continue
		}
		if bs.dbClosed.Load() {
			batch.Cancel()
			return badgerv4.ErrDBClosed
		}
		err := batch.Flush()
		batch.Cancel()
		if err != nil {
			return fmt.Errorf("graph: history write-ahead flush: %w", err)
		}
		if h := bs.historicalMembershipFlushTestHook; h != nil {
			if kind == storepkg.HistoryFull {
				h("after-history-anchors")
			} else {
				h("after-history-deltas")
			}
		}
	}
	return nil
}
