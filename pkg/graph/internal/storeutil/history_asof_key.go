package storeutil

import (
	"encoding/binary"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// KeyHistoryAsOf holds a disk-only transaction-time seek index. Each entity
// has a completeness/order proof, one seek key per indexed version, and one
// version alias so a rewrite or deletion removes its old seek key by lookup.
const KeyHistoryAsOf byte = 0x11

// HistoryAsOfProofSize, HistoryAsOfSeekSize and HistoryAsOfAliasSize are the
// encoded key lengths, including the keyspace tag, record kind and entity ID.
const (
	HistoryAsOfProofSize = 10
	HistoryAsOfSeekSize  = 26
	HistoryAsOfAliasSize = 18
)

var HistoryAsOfGenerationKey = MetaKey("history_asof_generation")

func HistoryAsOfProofKey(historyKind byte, id snowflake.ID) []byte {
	k := make([]byte, HistoryAsOfProofSize)
	k[0], k[1] = KeyHistoryAsOf, historyKind
	PutUint64(k, 2, int64(id))
	return k
}

func HistoryAsOfSeekKey(historyKind byte, id snowflake.ID, txFrom, version uint64) []byte {
	k := make([]byte, HistoryAsOfSeekSize)
	k[0], k[1] = KeyHistoryAsOf, historyKind+16
	PutUint64(k, 2, int64(id))
	binary.BigEndian.PutUint64(k[10:], txFrom)
	binary.BigEndian.PutUint64(k[18:], version)
	return k
}

func HistoryAsOfAliasKey(historyKind byte, id snowflake.ID, version uint64) []byte {
	k := make([]byte, HistoryAsOfAliasSize)
	k[0], k[1] = KeyHistoryAsOf, historyKind+32
	PutUint64(k, 2, int64(id))
	binary.BigEndian.PutUint64(k[10:], version)
	return k
}
