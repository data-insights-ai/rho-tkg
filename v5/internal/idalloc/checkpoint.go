package idalloc

import (
	"encoding/binary"
	"hash/crc32"
)

// CheckpointSize is the fixed v1 image size, including CRC32C. There are no
// attacker-supplied collection lengths or variable-sized decoded allocations.
const CheckpointSize = 121

// MarshalCheckpoint returns a derived image, NOT a durable checkpoint receipt.
// The embedding log/manifest must publish it atomically at its exact applied
// position; replay must never apply entries preceding that position again.
func MarshalCheckpoint(s *State) ([]byte, error) {
	if !s.valid() {
		return nil, ErrInvalid
	}
	b := make([]byte, CheckpointSize)
	copy(b[:4], "RIDA")
	b[4] = 1
	copy(b[5:21], s.graph[:])
	copy(b[21:37], s.authority.owner[:])
	for index, value := range [...]uint64{s.authority.epoch, s.high, s.maxBlock, s.sequence, s.count, s.first} {
		binary.BigEndian.PutUint64(b[37+8*index:45+8*index], value)
	}
	copy(b[85:117], s.digest[:])
	binary.BigEndian.PutUint32(b[117:], crc32.Checksum(b[:117], crc32.MakeTable(crc32.Castagnoli)))
	return b, nil
}

// DecodeCheckpoint strictly validates version, exact size, CRC and state shape
// before returning any state. Missing/corrupt images must never become NewState
// for an existing graph. CRC detects corruption; it is not an authenticity proof.
func DecodeCheckpoint(b []byte) (State, error) {
	if len(b) != CheckpointSize || string(b[:4]) != "RIDA" || b[4] != 1 || binary.BigEndian.Uint32(b[117:]) != crc32.Checksum(b[:117], crc32.MakeTable(crc32.Castagnoli)) {
		return State{}, ErrCorrupt
	}
	var s State
	copy(s.graph[:], b[5:21])
	copy(s.authority.owner[:], b[21:37])
	s.authority.epoch = binary.BigEndian.Uint64(b[37:45])
	s.high = binary.BigEndian.Uint64(b[45:53])
	s.maxBlock = binary.BigEndian.Uint64(b[53:61])
	s.sequence = binary.BigEndian.Uint64(b[61:69])
	s.count = binary.BigEndian.Uint64(b[69:77])
	s.first = binary.BigEndian.Uint64(b[77:85])
	copy(s.digest[:], b[85:117])
	if !s.valid() {
		return State{}, ErrCorrupt
	}
	return s, nil
}
