package badger

import (
	"encoding/binary"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

var _ storecontract.CommitFloorCapability = (*Store)(nil)

// Commit floor (store.CommitFloorCapability, backlog 44).
//
// Every flush that writes rows persists the newest transaction stamp (TxFrom,
// TxTo, DeletedAt) of every entity and history row this store has written,
// decoded from the rows themselves, as the FIRST entry of its WriteBatch. A
// WriteBatch may commit as several transactions, in order, and a crash keeps a
// prefix of them, so any row that survives a crash has its floor durable too —
// whichever door wrote it (a local write, a replica apply, an import). The open
// loads the key; the graph raises its commit clock to it.
//
// With Config.OnCommitFloor set (tiered event and archive shards) the floor is
// handed to the hook before the batch instead, which persists it where the
// owner's open reads it (the reference shard: cold shards are not opened at
// open).

// commitFloorKey holds the persisted floor (8 bytes, big-endian).
var commitFloorKey = storepkg.MetaKey("tx_commit_floor")

// StoredCommitFloor implements store.CommitFloorCapability: the newest
// transaction stamp of any row this store has persisted (loaded at open,
// raised by every flush), 0 for a store that holds none.
func (bs *Store) StoredCommitFloor() (types.Instant, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, err
	}
	return types.Instant(bs.commitFloor.Load()), nil
}

// loadCommitFloor reads the persisted floor at open; a malformed value fails
// the open (getCounter: wrong size or negative).
func (bs *Store) loadCommitFloor(txn *badgerv4.Txn) error {
	floor, err := getCounter(txn, commitFloorKey)
	if err != nil {
		return fmt.Errorf("graph: commit floor: %w", err)
	}
	bs.commitFloor.Store(floor)
	bs.commitFloorDurable = floor
	return nil
}

// batchCommitFloor returns the floor after ops: the larger of the current
// floor and every transaction stamp of an entity or history row ops sets. A row
// that does not decode fails the flush (it would fail every read too).
func (bs *Store) batchCommitFloor(ops map[string]writeOp) (int64, error) {
	floor := bs.commitFloor.Load()
	for _, op := range ops {
		if op.opType != writeOpSet || len(op.key) == 0 {
			continue
		}
		var txFrom, txTo, deletedAt types.Instant
		var err error
		switch {
		case (op.key[0] == storepkg.KeyNode || op.key[0] == storepkg.KeyRel) && len(op.key) == storepkg.SizeNodeKey:
			txFrom, txTo, deletedAt, err = storepkg.DecodeWireTxStamps(op.value)
		case (op.key[0] == storepkg.KeyHistNode || op.key[0] == storepkg.KeyHistRel) && len(op.key) == storepkg.SizeHistKey:
			txFrom, txTo, deletedAt, err = storepkg.HistoryValueTxStamps(op.value, op.key[0] == storepkg.KeyHistNode)
		default:
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("graph: commit floor: row %x: %w", op.key, err)
		}
		floor = max(floor, int64(txFrom), int64(txTo), int64(deletedAt))
	}
	return floor, nil
}

// commitFloorEntry encodes the floor for the WriteBatch.
func commitFloorEntry(floor int64) *badgerv4.Entry {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(floor)) // #nosec G115 -- non-negative: max of stored stamps and the loaded floor
	return badgerv4.NewEntry(commitFloorKey, buf)
}
