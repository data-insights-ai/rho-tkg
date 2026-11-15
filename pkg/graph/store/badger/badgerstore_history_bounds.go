package badger

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.ReadSnapshotHistoryBoundsCapability = (*readSnapshot)(nil)

func (s *readSnapshot) NodeHistoryBounds(id types.NodeID) (uint32, types.Instant, bool, error) {
	if err := storecontract.ValidateNodeID(id); err != nil {
		return 0, 0, false, err
	}
	return s.bs.historyBounds(storepkg.KeyHistNode, id.SnowflakeID(), s.tally)
}

func (s *readSnapshot) RelHistoryBounds(id types.RelID) (uint32, types.Instant, bool, error) {
	if err := storecontract.ValidateRelID(id); err != nil {
		return 0, 0, false, err
	}
	return s.bs.historyBounds(storepkg.KeyHistRel, id.SnowflakeID(), s.tally)
}

// historyBounds trusts only the existing committed ordinary-chain proof. Its
// strict version order gives the maximum version; its positive contiguous
// closed intervals bound every transaction/deletion stamp by lastTxTo. Unknown
// chains decline without a scan. A fresh fenced view preserves snapshot reads'
// live-call semantics while keeping the proof and generation in one transaction.
func (bs *Store) historyBounds(kind byte, id snowflake.ID, t *storecontract.ReadTally) (version uint32, newest types.Instant, known bool, err error) {
	defer recoverBadgerPanic(&err)
	if err := t.Exceeded(); err != nil {
		return 0, 0, false, err
	}
	prefix := storepkg.HistNodePrefix(id)
	prefix[0] = kind
	txn, pin, err := bs.asOfIndexTxn(prefix)
	if err != nil || txn == nil {
		return 0, 0, false, err
	}
	defer func() { bs.endReadTxn(txn, pin, !bs.dbClosed.Load() && !bs.closing.Load()) }()
	proof, valid, err := admittedAsOfProof(txn, kind, id, t)
	if err != nil || !valid {
		return 0, 0, false, err
	}
	// decodeAsOfProof has validated the version and timestamp ranges.
	return uint32(proof.version), types.Instant(proof.lastTxTo), true, nil // #nosec G115 -- validated proof bounds
}
