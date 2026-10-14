package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
)

var errRangeMovePending = errors.New("graphapply: range move pending; resume original identity")

// Move phases are database application transitions. They neither change Raft
// membership nor reinterpret a snapshot's immutable ApplicationIdentity.
type rangeMovePhase byte

const (
	rangeMoveFence rangeMovePhase = iota + 1
	rangeMoveCapture
	rangeMoveDescribe
	rangeMoveImport
	rangeMoveSeal
	rangeMoveDecide
	rangeMoveActivate
	rangeMoveFinish
	rangeMoveAbort
)

const (
	rangeMoveApplied byte = iota + 1
	rangeMoveBlocked
	rangeMoveMismatch
	rangeMoveLimit
)

// Intent identity is source-qualified request identity. The range, destination
// and payload cannot change an existing retry's meaning or its native outcome.
type rangeMoveIntent struct {
	id                            requestID
	source, target                allocationScope
	publication                   graphstore.RangePublication
	epoch                         uint64
	maxPages, pageRows, pageBytes int
}

func rangeMoveID(n namespace, id requestID) [16]byte {
	b := appendNamespace([]byte("rho-tkg:range-move-id:v1\x00"), n)
	b = append(b, id[:]...)
	h := sha256.Sum256(b)
	return [16]byte(h[:16])
}

func (r rangeMoveIntent) valid() bool {
	return r.id != (requestID{}) && r.source.valid() && r.target.valid() &&
		r.source.partition != r.target.partition && r.source.graph == r.target.graph &&
		r.source.declaration == r.target.declaration && r.source.topology == r.target.topology &&
		r.source.semantic == r.target.semantic && rangeMoveGraphAgreement(r.source.semantic) &&
		r.publication.Validate() == nil && [16]byte(r.publication.Graph) == [16]byte(r.source.graph) &&
		r.epoch >= r.publication.InitialEpoch && r.epoch < math.MaxUint64 &&
		r.maxPages > 0 && r.maxPages <= 1024 && r.pageRows > 0 && r.pageRows <= 256 &&
		r.pageBytes > 0 && r.pageBytes <= 1<<20
}

func encodeRangeMoveIntent(r rangeMoveIntent) ([]byte, error) {
	if !r.valid() {
		return nil, errInvalid
	}
	publication, err := graphstore.EncodeRangePublication(r.publication)
	if err != nil {
		return nil, err
	}
	return boundedEncoding(1024, func(w *boundedWriter) {
		w.add([]byte{'G', 'M', 'I', 1})
		w.add(r.id[:])
		w.add(appendAllocationScope(nil, r.source))
		w.add(appendAllocationScope(nil, r.target))
		w.field(publication)
		w.u64(r.epoch)
		w.u32(r.maxPages)
		w.u32(r.pageRows)
		w.u32(r.pageBytes)
	})
}

func decodeRangeMoveIntent(wire []byte) (rangeMoveIntent, error) {
	body, err := graphBody(wire, "GMI\x01", 1024)
	if err != nil {
		return rangeMoveIntent{}, err
	}
	c := graphCursor{b: body}
	r := rangeMoveIntent{id: requestID(c.array())}
	left, right := c.take(120), c.take(120)
	publication := c.field(256)
	r.epoch = c.u64()
	r.maxPages, r.pageRows, r.pageBytes = c.count(1024, 0), c.count(256, 0), c.count(1<<20, 0)
	if c.err != nil || len(c.b) != 0 {
		return rangeMoveIntent{}, errors.Join(errCorrupt, c.err)
	}
	r.source = readAllocationScope(&decoder{b: left})
	r.target = readAllocationScope(&decoder{b: right})
	r.publication, err = graphstore.DecodeRangePublication(publication)
	if err != nil || !r.valid() {
		return rangeMoveIntent{}, errors.Join(errCorrupt, err)
	}
	return r, nil
}

type rangeMoveStatus struct {
	ns                   namespace
	move                 [16]byte
	intent               [32]byte
	phase                rangeMovePhase
	index, floor, round  uint64
	sequence, rows, size uint64
	digest               [32]byte
	result               byte
}

func encodeRangeMoveStatus(s rangeMoveStatus) ([]byte, error) {
	if !s.ns.valid() || s.move == ([16]byte{}) || s.intent == ([32]byte{}) || s.phase < rangeMoveFence || s.phase > rangeMoveAbort || s.index == 0 || s.result < rangeMoveApplied || s.result > rangeMoveLimit || s.sequence > 1024 {
		return nil, errInvalid
	}
	return boundedEncoding(256, func(w *boundedWriter) {
		w.add([]byte{'G', 'M', 'O', 1})
		w.add(appendNamespace(nil, s.ns))
		w.add(s.move[:])
		w.add(s.intent[:])
		w.tag(byte(s.phase))
		w.tag(s.result)
		for _, n := range []uint64{s.index, s.floor, s.round, s.sequence, s.rows, s.size} {
			w.u64(n)
		}
		w.add(s.digest[:])
	})
}

func decodeRangeMoveStatus(wire []byte) (rangeMoveStatus, error) {
	body, err := graphBody(wire, "GMO\x01", 256)
	if err != nil {
		return rangeMoveStatus{}, err
	}
	c := graphCursor{b: body}
	ns := c.take(24)
	s := rangeMoveStatus{move: c.array()}
	copy(s.intent[:], c.take(32))
	s.phase, s.result = rangeMovePhase(c.tag()), c.tag()
	s.index, s.floor, s.round, s.sequence, s.rows, s.size = c.u64(), c.u64(), c.u64(), c.u64(), c.u64(), c.u64()
	copy(s.digest[:], c.take(32))
	if c.err != nil || len(c.b) != 0 {
		return rangeMoveStatus{}, errors.Join(errCorrupt, c.err)
	}
	d := decoder{b: ns}
	s.ns = namespace{graph: d.array(), partition: d.number()}
	expected, err := encodeRangeMoveStatus(s)
	if err != nil || !bytes.Equal(expected, wire) {
		return rangeMoveStatus{}, errors.Join(errCorrupt, err)
	}
	return s, nil
}

func rangeMoveActiveKey(n namespace) []byte { return recordKey(n, 17) }
func rangeMoveStatusKey(n namespace, id [16]byte) []byte {
	return append(recordKey(n, 18), id[:]...)
}
func rangeMoveControlKey(n namespace, id [16]byte, phase byte) []byte {
	b := appendNamespace([]byte{'G', 'M', phase, 1}, n)
	return append(b, id[:]...)
}
func rangeMoveProgressKey(n namespace, id [16]byte) []byte {
	return append(recordKey(n, 19), id[:]...)
}
func rangeMoveReplayKey(n namespace, origin uint64, id [16]byte, kind byte) []byte {
	b := binary.BigEndian.AppendUint64(recordKey(n, 20), origin)
	b = append(b, kind)
	return append(b, id[:]...)
}
