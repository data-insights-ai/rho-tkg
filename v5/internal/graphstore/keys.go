package graphstore

import (
	"encoding/binary"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type recordKind byte

const (
	axisRecord recordKind = iota + 1
	schemaRecord
	entityRecord
	lifeRecord
	valueRecord
	bucketRecord
	bucketMemberRecord
)

// Keys are provisional canonical lookup/routing keys, not temporal order keys.
// Numeric fields are unsigned big-endian. Names are explicitly sized; schema
// identity includes OwnerKind. Every key includes graph and logical partition.
func keyPrefix(n Namespace, kind recordKind) []byte {
	b := append([]byte{byte(kind)}, n.Graph[:]...)
	return binary.BigEndian.AppendUint64(b, n.Partition)
}
func axisKey(n Namespace, id temporal.AxisID) []byte {
	return append(keyPrefix(n, axisRecord), id[:]...)
}
func schemaKey(n Namespace, owner graphstate.EntityKind, name string) []byte {
	b := append(keyPrefix(n, schemaRecord), byte(owner))
	b = binary.BigEndian.AppendUint32(b, uint32(len(name)))
	return append(b, name...)
} // #nosec G115 -- callers validate names <= 65535 bytes.
func entityKey(n Namespace, id graphstate.EntityID) []byte {
	return binary.BigEndian.AppendUint64(keyPrefix(n, entityRecord), uint64(id))
}
func lifeKey(n Namespace, owner graphstate.EntityID, id graphstate.LifeID) []byte {
	b := binary.BigEndian.AppendUint64(keyPrefix(n, lifeRecord), uint64(owner))
	return binary.BigEndian.AppendUint64(b, uint64(id))
}
func valueKey(n Namespace, id graphstate.ValueID) []byte {
	return binary.BigEndian.AppendUint64(keyPrefix(n, valueRecord), uint64(id))
}
func bucketKey(n Namespace, digest [32]byte) []byte {
	return append(keyPrefix(n, bucketRecord), digest[:]...)
}
func memberKey(n Namespace, digest [32]byte, ordinal uint64) []byte {
	return binary.BigEndian.AppendUint64(append(keyPrefix(n, bucketMemberRecord), digest[:]...), ordinal)
}
