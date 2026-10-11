package badger

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

const historicalMembershipWriterVersion uint16 = 5

const (
	diskMembershipPrefix        byte = 0x0c
	diskMembershipReversePrefix byte = 0x0d
	diskMemberNodeLabel         byte = 1
	diskMemberRelType           byte = 2
	diskMemberNodeProperty      byte = 3
	diskMemberRelProperty       byte = 4
)

// Postings are append-only acquisition stamps. Canonical as-of resolution
// still decides whether a candidate matches. Value digests only narrow the
// candidate set; a hash collision cannot manufacture an accepted match.
func diskMembershipScope(kind byte, token uint16, property, value string) []byte {
	key := make([]byte, 4)
	key[0], key[1] = diskMembershipPrefix, kind
	binary.BigEndian.PutUint16(key[2:], token)
	if kind == diskMemberNodeProperty || kind == diskMemberRelProperty {
		// A digest over the unambiguous property/value pair avoids retaining
		// arbitrary large string values in index keys or auxiliary data.
		h := sha256.New()
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(property)))
		h.Write(size[:])
		h.Write([]byte(property))
		h.Write([]byte(value))
		key = h.Sum(key)
	}
	return key
}

func diskMembershipOp(scope []byte, id types.EntityID, tx types.Instant) writeOp {
	key := make([]byte, len(scope)+16)
	copy(key, scope)
	binary.BigEndian.PutUint64(key[len(scope):], uint64(id)) // #nosec G115 — validated positive entity ID, preserving its exact key bits
	// Positive TX stamps and zero (unknown) use their exact numeric order.
	// A negative raw-store stamp is conservatively unknown for pruning.
	if tx < 0 {
		tx = 0
	}
	binary.BigEndian.PutUint64(key[len(scope)+8:], uint64(tx))
	return writeOp{opType: writeOpSet, key: key, value: []byte{1}}
}

func diskMembershipRecord(key, scope []byte) (types.EntityID, types.Instant, error) {
	if len(key) != len(scope)+16 {
		return 0, 0, fmt.Errorf("graph: corrupt historical membership key")
	}
	rawID, rawTX := binary.BigEndian.Uint64(key[len(scope):]), binary.BigEndian.Uint64(key[len(scope)+8:])
	if rawID == 0 || rawID > math.MaxInt64 || rawTX > math.MaxInt64 {
		return 0, 0, fmt.Errorf("graph: invalid historical membership ID or stamp")
	}
	return types.EntityID(rawID), types.Instant(rawTX), nil // #nosec G115 — both raw values checked within the signed range above
}

func diskMembershipPair(scope []byte, id types.EntityID, tx types.Instant) []writeOp {
	op := diskMembershipOp(scope, id, tx)
	reverse := make([]byte, 10+sha256.Size)
	reverse[0], reverse[1] = diskMembershipReversePrefix, scope[1]
	binary.BigEndian.PutUint64(reverse[2:], uint64(id)) // #nosec G115 — validated positive entity ID, preserving its exact key bits
	hash := sha256.Sum256(op.key)
	copy(reverse[10:], hash[:])
	return []writeOp{op, {opType: writeOpSet, key: reverse, value: op.key}}
}

func diskMembershipPairWithReverse(scope []byte, id types.EntityID, tx types.Instant, compact bool) []writeOp {
	if !compact {
		return diskMembershipPair(scope, id, tx)
	}
	op := diskMembershipOp(scope, id, tx)
	return []writeOp{op, compactReverseMembershipOp(op.key)}
}

func nodeDiskMembershipOpsWithReverse(n *types.Node, compact bool) []writeOp {
	var ops []writeOp
	tx := nodeTxFrom(n)
	for i := range n.LabelTokenCount() {
		token := n.LabelTokenRawAt(i)
		ops = append(ops, diskMembershipPairWithReverse(diskMembershipScope(diskMemberNodeLabel, token, "", ""), types.EntityID(n.ID()), tx, compact)...)
		n.ForEachIndexablePropertyValueKey(func(property, value string) bool {
			ops = append(ops, diskMembershipPairWithReverse(diskMembershipScope(diskMemberNodeProperty, token, property, value), types.EntityID(n.ID()), tx, compact)...)
			return true
		})
	}
	return ops
}

func relDiskMembershipOpsWithReverse(r *types.Relationship, compact bool) []writeOp {
	token, tx := r.TypeToken().Value(), relTxFrom(r)
	ops := diskMembershipPairWithReverse(diskMembershipScope(diskMemberRelType, token, "", ""), types.EntityID(r.ID()), tx, compact)
	r.ForEachIndexablePropertyValueKey(func(property, value string) bool {
		ops = append(ops, diskMembershipPairWithReverse(diskMembershipScope(diskMemberRelProperty, token, property, value), types.EntityID(r.ID()), tx, compact)...)
		return true
	})
	return ops
}

func historicalMembershipWrite(op writeOp) bool {
	return len(op.key) > 0 && (op.key[0] == diskMembershipPrefix || op.key[0] == diskMembershipReversePrefix || op.key[0] == diskMembershipCompactReversePrefix)
}
