package raftlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"slices"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const snapshotManifestOverhead = 4 + 40 + 80 + 16 + 64 + 64 + 8
const maxSnapshotManifestBytes = 64<<20 + 32768 + snapshotManifestOverhead
const dormantDataTag = appDataTag + 4

var dormantKey = []byte{16}
var dormantEnd = []byte{17}

func appendIdentity(b []byte, n ApplicationIdentity) []byte {
	b = append(b, n.Graph[:]...)
	b = binary.BigEndian.AppendUint64(b, n.Partition)
	return append(b, n.Group[:]...)
}
func readIdentity(b []byte) ApplicationIdentity {
	var n ApplicationIdentity
	copy(n.Graph[:], b[:16])
	n.Partition = binary.BigEndian.Uint64(b[16:24])
	copy(n.Group[:], b[24:40])
	return n
}
func appendContract(b []byte, c ApplicationContract) []byte {
	for _, n := range c.numbers() {
		b = binary.BigEndian.AppendUint64(b, n)
	}
	return b
}
func readContract(b []byte) (ApplicationContract, error) {
	var n [10]uint64
	for j := range n {
		n[j] = binary.BigEndian.Uint64(b[j*8:])
		if n[j] > 64<<20 {
			return ApplicationContract{}, ErrCorrupt
		}
	}
	c := ApplicationContract{uint32(n[0]), int(n[1]), int(n[2]), int(n[3]), int(n[4]), int(n[5]), int(n[6]), int(n[7]), int(n[8]), int(n[9])}
	return c, c.validate()
} // #nosec G115 -- all decoded integers bounded to 64 MiB before conversion.
func appendTransferConfig(b []byte, c ApplicationTransferConfig) []byte {
	b = append(b, 'A', 'T', 1, 0)
	b = appendIdentity(b, c.Identity)
	b = appendContract(b, c.Contract)
	l := c.Limits
	for _, n := range []uint64{unsignedLimit(l.MaxExports), unsignedLimit(l.MaxChunkRows), unsignedLimit(l.MaxChunkBytes), l.MaxStagedBytes, l.MaxStagedRecords, l.MaxPinnedLogicalBytes} {
		b = binary.BigEndian.AppendUint64(b, n)
	}
	return b
}
func decodeTransferConfig(b []byte) (ApplicationTransferConfig, []byte, error) {
	const size = 4 + 40 + 80 + 48
	if len(b) < size || !bytes.Equal(b[:4], []byte{'A', 'T', 1, 0}) {
		return ApplicationTransferConfig{}, nil, ErrCorrupt
	}
	c := ApplicationTransferConfig{Identity: readIdentity(b[4:44])}
	var err error
	c.Contract, err = readContract(b[44:124])
	if err != nil {
		return c, nil, err
	}
	var n [6]uint64
	for j := range n {
		n[j] = binary.BigEndian.Uint64(b[124+j*8:])
	}
	if n[0] > 16 || n[1] > 4096 || n[2] > 32<<20 {
		return c, nil, ErrCorrupt
	}
	c.Limits = ApplicationTransferLimits{int(n[0]), int(n[1]), int(n[2]), n[3], n[4], n[5]}
	if err := c.Identity.validate(); err != nil {
		return c, nil, ErrCorrupt
	}
	if err := c.Limits.validate(); err != nil {
		return c, nil, ErrCorrupt
	}
	return c, b[size:], nil
} // #nosec G115 -- local int limits bounded before conversion.
func canonicalConf(c *pb.ConfState) *pb.ConfState {
	cs := proto.Clone(c).(*pb.ConfState)
	for _, ids := range [][]uint64{cs.Voters, cs.VotersOutgoing, cs.Learners, cs.LearnersNext} {
		slices.Sort(ids)
	}
	return cs
}
func validateManifest(m ApplicationSnapshotManifest) error {
	if err := m.Identity.validate(); err != nil {
		return err
	}
	if err := m.Contract.validate(); err != nil {
		return err
	}
	if m.Index == 0 || m.Index == math.MaxUint64 || m.Term == 0 || m.Term >= math.MaxUint64-1 || m.ConfState == nil || len(m.ConfState.GetVoters()) == 0 {
		return ErrInvalid
	}
	if len(m.Image) > m.Contract.MaxImageBytes {
		return ErrLimit
	}
	if sha256.Sum256(m.Image) != m.ImageHash {
		return ErrCorrupt
	}
	if err := validateConf(m.ConfState, m.Index); err != nil {
		return err
	}
	for _, ids := range [][]uint64{m.ConfState.Voters, m.ConfState.VotersOutgoing, m.ConfState.Learners, m.ConfState.LearnersNext} {
		if !slices.IsSorted(ids) {
			return ErrInvalid
		}
	}
	if proto.Size(m.ConfState) > 32768 {
		return ErrLimit
	}
	if m.Index > math.MaxUint64/(9+appFrameBytes) {
		return ErrLimit
	}
	for j := 1; j < 4; j++ {
		if m.NamespaceRecords[j] != m.Index || m.NamespaceBytes[j] < m.Index*(9+appFrameBytes) {
			return ErrCorrupt
		}
	}
	_, _, err := snapshotTotals(m)
	return err
}

// EncodeApplicationSnapshotManifest emits a bounded canonical transfer header.
// It does not authorize installation or accept receiver-local policy metadata.
func EncodeApplicationSnapshotManifest(m ApplicationSnapshotManifest) ([]byte, error) {
	if err := validateManifest(m); err != nil {
		return nil, err
	}
	cs, err := proto.MarshalOptions{Deterministic: true}.Marshal(m.ConfState)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 0, snapshotManifestOverhead+len(cs)+len(m.Image))
	b = append(b, 'A', 'S', 1, 0)
	b = appendIdentity(b, m.Identity)
	b = appendContract(b, m.Contract)
	b = binary.BigEndian.AppendUint64(b, m.Index)
	b = binary.BigEndian.AppendUint64(b, m.Term)
	for _, ns := range [][4]uint64{m.NamespaceBytes, m.NamespaceRecords} {
		for _, n := range ns {
			b = binary.BigEndian.AppendUint64(b, n)
		}
	}
	b = append(b, m.ImageHash[:]...)
	b = append(b, m.RecordsHash[:]...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(cs)))
	b = binary.BigEndian.AppendUint32(b, uint32(len(m.Image)))
	b = append(b, cs...)
	b = append(b, m.Image...)
	return b, nil
} // #nosec G115 -- protobuf/image lengths explicitly bounded before uint32 conversion.
// DecodeApplicationSnapshotManifest rejects malformed lengths before allocation.
// maxImageBytes is a concrete receiver ceiling, not an allocation request.
func DecodeApplicationSnapshotManifest(b []byte, maxImageBytes int) (ApplicationSnapshotManifest, error) {
	var m ApplicationSnapshotManifest
	if maxImageBytes < 1 || maxImageBytes > 64<<20 {
		return m, ErrInvalid
	}
	if len(b) < snapshotManifestOverhead || len(b) > maxSnapshotManifestBytes || !bytes.Equal(b[:4], []byte{'A', 'S', 1, 0}) {
		return m, ErrCorrupt
	}
	m.Identity = readIdentity(b[4:44])
	var err error
	m.Contract, err = readContract(b[44:124])
	if err != nil {
		return m, err
	}
	m.Index = binary.BigEndian.Uint64(b[124:132])
	m.Term = binary.BigEndian.Uint64(b[132:140])
	offset := 140
	for _, ns := range []*[4]uint64{&m.NamespaceBytes, &m.NamespaceRecords} {
		for j := range ns {
			ns[j] = binary.BigEndian.Uint64(b[offset:])
			offset += 8
		}
	}
	copy(m.ImageHash[:], b[offset:offset+32])
	offset += 32
	copy(m.RecordsHash[:], b[offset:offset+32])
	offset += 32
	cn := uint64(binary.BigEndian.Uint32(b[offset:]))
	in := uint64(binary.BigEndian.Uint32(b[offset+4:]))
	offset += 8
	if cn > 32768 || in > uint64(maxImageBytes) {
		return ApplicationSnapshotManifest{}, ErrLimit
	}
	if cn+in != uint64(len(b)-offset) {
		return ApplicationSnapshotManifest{}, ErrCorrupt
	}
	m.ConfState = &pb.ConfState{}
	if err := (proto.UnmarshalOptions{RecursionLimit: 4}).Unmarshal(b[offset:offset+int(cn)], m.ConfState); err != nil {
		return ApplicationSnapshotManifest{}, ErrCorrupt
	}
	m.Image = copyApplicationBytes(b[offset+int(cn):])
	if err := validateManifest(m); err != nil {
		return ApplicationSnapshotManifest{}, err
	}
	return m, nil
} // #nosec G115 -- cn <= 32768 and input lengths checked before indexing/conversion.
func manifestID(m ApplicationSnapshotManifest) ([32]byte, error) {
	b, err := EncodeApplicationSnapshotManifest(m)
	if err != nil {
		return [32]byte{}, err
	}
	h := sha256.New()
	_, _ = h.Write([]byte("rho-tkg:application-snapshot-manifest:v1\x00"))
	_, _ = h.Write(b)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out, nil
}
func snapshotSeed(m ApplicationSnapshotManifest) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("rho-tkg:application-snapshot-records:v1\x00"))
	_, _ = h.Write(appendContract(appendIdentity(nil, m.Identity), m.Contract))
	var ns [16]byte
	binary.BigEndian.PutUint64(ns[:8], m.Index)
	binary.BigEndian.PutUint64(ns[8:], m.Term)
	_, _ = h.Write(ns[:])
	_, _ = h.Write(m.ImageHash[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
func appendSnapshotFrame(b, k, v []byte) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(k)))
	b = binary.BigEndian.AppendUint32(b, uint32(len(v)))
	b = append(b, k...)
	return append(b, v...)
} // #nosec G115 -- frames bounded by contract before encoding.
func walkSnapshotChunk(b []byte, c ApplicationContract, fn func([]byte, []byte) error) error {
	for len(b) > 0 {
		if len(b) < 8 {
			return ErrCorrupt
		}
		kn, vn := uint64(binary.BigEndian.Uint32(b)), uint64(binary.BigEndian.Uint32(b[4:]))
		if kn == 0 || kn > uint64(2*c.MaxKeyBytes+11) || vn < appFrameBytes || vn > uint64(max(c.MaxValueBytes, c.MaxImageBytes, c.MaxChangeBytes, c.MaxOutcomeBytes)+appFrameBytes) {
			return ErrLimit
		}
		b = b[8:]
		if kn+vn > uint64(len(b)) {
			return ErrCorrupt
		}
		if err := fn(b[:int(kn)], b[int(kn):int(kn+vn)]); err != nil {
			return err
		}
		b = b[int(kn+vn):]
	}
	return nil
} // #nosec G115 -- lengths checked against delivered bytes and bounded contract before int conversion.

type snapshotVerifier struct {
	last                          []byte
	bytes, rows                   uint64
	namespaceBytes, namespaceRows [4]uint64
	envelopes                     [3]uint64
	digest                        [32]byte
}

func (v snapshotVerifier) accept(m ApplicationSnapshotManifest, k, raw []byte) (snapshotVerifier, error) {
	if len(k) == 0 || k[0] < appDataTag || k[0] > appOutcomeTag || len(v.last) > 0 && bytes.Compare(v.last, k) >= 0 {
		return v, ErrCorrupt
	}
	slot := int(k[0] - appDataTag)
	limit := m.Contract.MaxValueBytes
	if slot == 0 {
		_, index, err := decodeAppKey(k, m.Contract.MaxKeyBytes)
		if err != nil || index > m.Index {
			return v, ErrCorrupt
		}
	} else {
		if len(k) != 9 {
			return v, ErrCorrupt
		}
		index := binary.BigEndian.Uint64(k[1:])
		if index != v.envelopes[slot-1]+1 || index > m.Index {
			return v, ErrCorrupt
		}
		v.envelopes[slot-1] = index
		switch slot {
		case 1:
			limit = m.Contract.MaxImageBytes
		case 2:
			limit = m.Contract.MaxChangeBytes
		case 3:
			limit = m.Contract.MaxOutcomeBytes
		}
	}
	data, deleted, err := inspectAppFrame(k, raw, limit)
	if err != nil || deleted && slot != 0 {
		return v, ErrCorrupt
	}
	if slot == 1 && v.envelopes[0] == m.Index && !bytes.Equal(data, m.Image) {
		return v, ErrCorrupt
	}
	cost := uint64(len(k) + len(raw))
	if v.namespaceBytes[slot] > m.NamespaceBytes[slot] || cost > m.NamespaceBytes[slot]-v.namespaceBytes[slot] || v.namespaceRows[slot] >= m.NamespaceRecords[slot] {
		return v, ErrCorrupt
	}
	if cost > math.MaxUint64-v.bytes || v.rows == math.MaxUint64 {
		return v, ErrLimit
	}
	v.namespaceBytes[slot] += cost
	v.namespaceRows[slot]++
	v.bytes += cost
	v.rows++
	h := sha256.New()
	_, _ = h.Write(v.digest[:])
	var lengths [8]byte
	binary.BigEndian.PutUint32(lengths[:4], uint32(len(k)))
	binary.BigEndian.PutUint32(lengths[4:], uint32(len(raw)))
	_, _ = h.Write(lengths[:])
	_, _ = h.Write(k)
	_, _ = h.Write(raw)
	copy(v.digest[:], h.Sum(nil))
	v.last = copyApplicationBytes(k)
	return v, nil
} // #nosec G115 -- key/frame lengths bounded before hash framing.
func (v snapshotVerifier) complete(m ApplicationSnapshotManifest) error {
	if v.namespaceBytes != m.NamespaceBytes || v.namespaceRows != m.NamespaceRecords || v.digest != m.RecordsHash {
		return ErrCorrupt
	}
	for _, index := range v.envelopes {
		if index != m.Index {
			return ErrCorrupt
		}
	}
	return nil
}
