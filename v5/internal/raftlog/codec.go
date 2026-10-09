package raftlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const frameOverhead = 74 // Plus the nine-byte ordered entry key and Pebble's WAL/LSM overhead.

const metadataOverhead = 252 // Magic, checksum, eight counters, four hashes and three blob lengths.
const readyEnvelopeBytes = 128
const maxMetadataBytes = 65536

// metadataBytes measures the persisted envelope without serializing or cloning it.
// Snapshot images are stored separately and must be absent from m.Snap.
func metadataBytes(m metadata) uint64 {
	n := uint64(metadataOverhead) + unsignedLimit(proto.Size(m.Hard)) + unsignedLimit(proto.Size(m.Conf)) + unsignedLimit(proto.Size(m.Snap))
	if m.App.Policy.Enabled() {
		n += 4 + 21*8
	}
	if m.Transfer.enabled() {
		n += 4 + 40 + 80 + 48
	}
	if m.Gen.Limits.enabled() {
		n += generationMetaBytes
	}
	return n
}

func checkMetadataLimit(m metadata, l Limits) error {
	n := metadataBytes(m)
	if n > maxMetadataBytes || n+readyEnvelopeBytes > unsignedLimit(l.MaxReadyBytes) {
		return ErrLimit
	}
	return nil
}

func entryKey(index uint64) []byte {
	key := make([]byte, 9)
	key[0] = 1
	binary.BigEndian.PutUint64(key[1:], index)
	return key
}

func encodeEntry(e *pb.Entry, prev [32]byte) ([]byte, [32]byte) {
	b := make([]byte, frameOverhead+len(e.GetData()))
	b[0] = 1
	binary.BigEndian.PutUint64(b[1:9], e.GetTerm())
	switch e.GetType() {
	case pb.EntryNormal:
		b[9] = 0
	case pb.EntryConfChange:
		b[9] = 1
	case pb.EntryConfChangeV2:
		b[9] = 2
	}
	copy(b[10:42], prev[:])
	copy(b[74:], e.GetData())
	h := entryHash(e.GetIndex(), b)
	copy(b[42:74], h[:])
	return b, h
}

func entryHash(index uint64, b []byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write(entryKey(index))
	_, _ = h.Write(b[:42])
	_, _ = h.Write(b[74:])
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}

func decodeEntry(index uint64, b []byte, limit int) (*pb.Entry, [32]byte, [32]byte, error) {
	term, prev, hash, err := inspectEntry(index, b, limit)
	if err != nil {
		return nil, prev, hash, err
	}
	return &pb.Entry{Index: new(index), Term: new(term), Type: pb.EntryType(b[9]).Enum(), Data: bytes.Clone(b[74:])}, prev, hash, nil
}

// inspectEntry checks the frame without cloning its payload. Ledger scans and
// term lookups therefore do not allocate a scratch image for each retained entry.
func inspectEntry(index uint64, b []byte, limit int) (uint64, [32]byte, [32]byte, error) {
	var prev, hash [32]byte
	if len(b) < frameOverhead || len(b) > limit || b[0] != 1 || b[9] > byte(pb.EntryConfChangeV2) || index == 0 || index == math.MaxUint64 {
		return 0, prev, hash, ErrCorrupt
	}
	copy(prev[:], b[10:42])
	copy(hash[:], b[42:74])
	if hash != entryHash(index, b) || binary.BigEndian.Uint64(b[1:9]) == 0 {
		return 0, prev, hash, ErrCorrupt
	}
	return binary.BigEndian.Uint64(b[1:9]), prev, hash, nil
}

// metadata holds bounded membership and fixed-size image references only.
// Frame chains certify LOCAL recovery bytes, not cross-replica semantic effects.
// Incoming snapshots start a new local chain; protocol digests live in commands.
// Membership and Applied describe the SAME reducer checkpoint.
type metadata struct {
	Base, BaseTerm, Last, Applied, LogBytes, LogCount, ImageBytes, SnapBytes uint64
	BaseHash, LastHash, ImageHash, SnapHash                                  [32]byte
	Hard                                                                     *pb.HardState
	Conf                                                                     *pb.ConfState
	Snap                                                                     *pb.Snapshot
	App                                                                      applicationMetadata
	Gen                                                                      generationMetadata
	Transfer                                                                 ApplicationTransferConfig
}

func encodeMeta(m metadata) ([]byte, error) {
	magic := "RLM2"
	if m.App.Policy.Enabled() {
		magic = "RLM3"
	}
	if m.Transfer.enabled() {
		magic = "RLM4"
	}
	if m.Gen.Limits.enabled() {
		magic = "RLM5"
	}
	b := append([]byte(magic), make([]byte, 32)...)
	for _, n := range []uint64{m.Base, m.BaseTerm, m.Last, m.Applied, m.LogBytes, m.LogCount, m.ImageBytes, m.SnapBytes} {
		b = binary.BigEndian.AppendUint64(b, n)
	}
	b = append(b, m.BaseHash[:]...)
	b = append(b, m.LastHash[:]...)
	b = append(b, m.ImageHash[:]...)
	b = append(b, m.SnapHash[:]...)
	for _, msg := range []proto.Message{m.Hard, m.Conf, m.Snap} {
		v, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
		if err != nil {
			return nil, err
		}
		b = binary.BigEndian.AppendUint64(b, uint64(len(v)))
		b = append(b, v...)
	}
	if m.App.Policy.Enabled() {
		b = appendApplicationMeta(b, m.App)
	}
	if m.Transfer.enabled() {
		b = appendTransferConfig(b, m.Transfer)
	}
	if m.Gen.Limits.enabled() {
		b = appendGenerationMeta(b, m.Gen)
	}
	h := sha256.Sum256(b[36:])
	copy(b[4:36], h[:])
	return b, nil
}

func decodeMeta(b []byte, l Limits) (metadata, error) {
	m := metadata{Hard: &pb.HardState{}, Conf: &pb.ConfState{}, Snap: &pb.Snapshot{}}
	if len(b) < metadataOverhead || len(b) > maxMetadataBytes || (string(b[:4]) != "RLM2" && string(b[:4]) != "RLM3" && string(b[:4]) != "RLM4" && string(b[:4]) != "RLM5") {
		return m, ErrCorrupt
	}
	version5 := string(b[:4]) == "RLM5"
	version4 := string(b[:4]) == "RLM4" || version5
	version3 := string(b[:4]) == "RLM3" || version4
	h := sha256.Sum256(b[36:])
	if !bytes.Equal(b[4:36], h[:]) {
		return m, ErrCorrupt
	}
	if uint64(len(b))+readyEnvelopeBytes > unsignedLimit(l.MaxReadyBytes) {
		return m, ErrLimit
	}
	b = b[36:]
	for _, p := range []*uint64{&m.Base, &m.BaseTerm, &m.Last, &m.Applied, &m.LogBytes, &m.LogCount, &m.ImageBytes, &m.SnapBytes} {
		*p = binary.BigEndian.Uint64(b[:8])
		b = b[8:]
	}
	copy(m.BaseHash[:], b[:32])
	b = b[32:]
	copy(m.LastHash[:], b[:32])
	b = b[32:]
	copy(m.ImageHash[:], b[:32])
	b = b[32:]
	copy(m.SnapHash[:], b[:32])
	b = b[32:]
	for _, msg := range []proto.Message{m.Hard, m.Conf, m.Snap} {
		v, tail, err := takeBlob(b)
		if err != nil {
			return m, err
		}
		if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(v, msg); err != nil {
			return m, fmt.Errorf("%w: protobuf: %v", ErrCorrupt, err)
		}
		if len(msg.ProtoReflect().GetUnknown()) != 0 {
			return m, ErrCorrupt
		}
		b = tail
	}
	if version3 {
		var err error
		m.App, b, err = decodeApplicationMeta(b)
		if err != nil {
			return m, err
		}
		if !m.App.Policy.Enabled() {
			return m, ErrCorrupt
		}
	}
	if version4 {
		var err error
		m.Transfer, b, err = decodeTransferConfig(b)
		if err != nil {
			return m, err
		}
	}
	if version5 {
		var err error
		m.Gen, b, err = decodeGenerationMeta(b)
		if err != nil {
			return m, err
		}
	}
	if len(b) != 0 || m.ImageBytes > unsignedLimit(l.MaxSnapshotBytes) || m.SnapBytes > unsignedLimit(l.MaxSnapshotBytes) || len(m.Snap.GetData()) != 0 {
		return m, ErrCorrupt
	}
	return m, nil
}

func takeBlob(b []byte) ([]byte, []byte, error) {
	if len(b) < 8 {
		return nil, nil, ErrCorrupt
	}
	n := binary.BigEndian.Uint64(b[:8])
	b = b[8:]
	if n > uint64(len(b)) {
		return nil, nil, ErrCorrupt
	}
	return b[:n], b[n:], nil
}

// All configured limits pass finite-ceiling validation at Open; rejecting a
// negative value here additionally makes the conversion safe in codec tests.
func unsignedLimit(n int) uint64 {
	if n < 0 {
		return 0
	}
	return uint64(n)
}
