package badger

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

const compactHistoricalReverseWriterVersion uint16 = 6
const diskMembershipCompactReversePrefix byte = 0x0e

var diskMembershipReverseCRC = crc32.MakeTable(crc32.Castagnoli)

// The reverse key is a lossless permutation of the forward selector:
// prefix/kind/owner/token/[property-value digest]/TX. Its CRC is auxiliary
// corruption detection; original row, proposition and source hashes are intact.
func compactReverseMembershipOp(forward []byte) writeOp {
	key := make([]byte, len(forward))
	key[0], key[1] = diskMembershipCompactReversePrefix, forward[1]
	copy(key[2:10], forward[len(forward)-16:len(forward)-8])
	copy(key[10:len(key)-8], forward[2:len(forward)-16])
	copy(key[len(key)-8:], forward[len(forward)-8:])
	value := make([]byte, 4)
	binary.BigEndian.PutUint32(value, crc32.Checksum(key, diskMembershipReverseCRC))
	return writeOp{opType: writeOpSet, key: key, value: value}
}

func validateForwardMembershipOwner(forward []byte, kind byte, owner types.EntityID) error {
	size := 20
	switch kind {
	case diskMemberNodeLabel, diskMemberRelType:
	case diskMemberNodeProperty, diskMemberRelProperty:
		size = 52
	default:
		return fmt.Errorf("%w: historical membership kind", ErrInvalidStoreMutation)
	}
	if len(forward) != size || forward[0] != diskMembershipPrefix || forward[1] != kind || binary.BigEndian.Uint16(forward[2:4]) == 0 {
		return fmt.Errorf("%w: historical membership selector shape", ErrInvalidStoreMutation)
	}
	id, _, err := diskMembershipRecord(forward, forward[:size-16])
	if err != nil || owner <= 0 || id != owner {
		return fmt.Errorf("%w: historical membership ownership/stamp", ErrInvalidStoreMutation)
	}
	return nil
}

// Ownership is structural, independent of the CRC: a valid checksum on an
// arbitrary other owner's key cannot redirect this owner's exact erasure.
func compactReverseMembershipForward(key, value []byte, kind byte, owner types.EntityID) ([]byte, error) {
	if (len(key) != 20 && len(key) != 52) || key[0] != diskMembershipCompactReversePrefix || key[1] != kind || owner <= 0 {
		return nil, fmt.Errorf("%w: compact reverse shape/owner", ErrInvalidStoreMutation)
	}
	if binary.BigEndian.Uint64(key[2:10]) != uint64(owner) { // #nosec G115 — owner checked positive before conversion
		return nil, fmt.Errorf("%w: compact reverse ownership", ErrInvalidStoreMutation)
	}
	if len(value) != 4 || binary.BigEndian.Uint32(value) != crc32.Checksum(key, diskMembershipReverseCRC) {
		return nil, fmt.Errorf("%w: compact reverse CRC mismatch", ErrInvalidStoreMutation)
	}
	forward := make([]byte, len(key))
	forward[0], forward[1] = diskMembershipPrefix, kind
	copy(forward[2:len(forward)-16], key[10:len(key)-8])
	copy(forward[len(forward)-16:len(forward)-8], key[2:10])
	copy(forward[len(forward)-8:], key[len(key)-8:])
	if err := validateForwardMembershipOwner(forward, kind, owner); err != nil {
		return nil, err
	}
	return forward, nil
}

func legacyReverseMembershipForward(key, value []byte, kind byte, owner types.EntityID) ([]byte, error) {
	if len(key) != 42 || key[0] != diskMembershipReversePrefix || key[1] != kind || owner <= 0 {
		return nil, fmt.Errorf("%w: legacy reverse shape/owner", ErrInvalidStoreMutation)
	}
	if binary.BigEndian.Uint64(key[2:10]) != uint64(owner) { // #nosec G115 — owner checked positive before conversion
		return nil, fmt.Errorf("%w: legacy reverse ownership", ErrInvalidStoreMutation)
	}
	hash := sha256.Sum256(value)
	if !bytes.Equal(key[10:], hash[:]) {
		return nil, fmt.Errorf("%w: legacy reverse digest mismatch", ErrInvalidStoreMutation)
	}
	if err := validateForwardMembershipOwner(value, kind, owner); err != nil {
		return nil, err
	}
	return bytes.Clone(value), nil
}
