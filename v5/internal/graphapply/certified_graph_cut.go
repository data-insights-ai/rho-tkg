package graphapply

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

var errGraphCutPending = errors.New("graphapply: graph cut has unresolved work")
var errGraphCutScope = graphstore.ErrScopeUnavailable

const (
	graphCutFence   byte = 1
	graphCutCertify byte = 2
	graphCutApplied byte = 1
	graphCutPending byte = 2
	graphCutStale   byte = 3
	graphCutLimit   byte = 4
)

type graphCutCommand struct {
	scope         allocationScope
	configuration [32]byte
	round         uint64
	phase         byte
}
type graphCertificate struct {
	scope                                  allocationScope
	configuration                          [32]byte
	round, index, sourceRound, sourceIndex uint64
	image                                  [172]byte
}
type graphCutResult struct {
	round, index  uint64
	phase, status byte
}

func graphFenceKey(n namespace) []byte { return recordKey(n, 14) }
func graphCertificateKey(n namespace, b uint64) []byte {
	return binary.BigEndian.AppendUint64(recordKey(n, 15), b)
}
func encodeGraphFence(c [32]byte, b uint64) []byte {
	return seal(binary.BigEndian.AppendUint64(append([]byte{'G', 'F', 'N', 1}, c[:]...), b))
}
func readGraphFence(q *reader, c [32]byte) (uint64, error) {
	wire, found, err := q.getBounded(graphFenceKey(q.ns), 76)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, errCorrupt
	}
	body, err := graphBody(wire, "GFN\x01", 76)
	if err != nil || len(body) != 40 || !bytes.Equal(body[:32], c[:]) {
		return 0, errors.Join(errCorrupt, err)
	}
	b := binary.BigEndian.Uint64(body[32:])
	if b == math.MaxUint64 {
		return 0, errLimit
	}
	return b, nil
}
func (m *declaredMaterializer) allocationFloor(q *reader, c [32]byte, installed uint64) (uint64, error) {
	if !m.certifiedReads() {
		return installed, nil
	}
	f, e := readGraphFence(q, c)
	return max(installed, f), e
}
func encodeGraphCutCommand(c graphCutCommand) ([]byte, error) {
	if !c.scope.valid() || !certifiedGraphAgreement(c.scope.semantic) || c.configuration == ([32]byte{}) || c.round == 0 || c.round == math.MaxUint64 || c.phase < 1 || c.phase > 2 {
		return nil, errInvalid
	}
	b := appendAllocationScope([]byte{'G', 'S', 'Q', 1, c.phase}, c.scope)
	b = append(b, c.configuration[:]...)
	return seal(binary.BigEndian.AppendUint64(b, c.round)), nil
}
func decodeGraphCutCommand(wire []byte) (graphCutCommand, error) {
	body, err := graphBody(wire, "GSQ\x01", 512)
	if err != nil || len(body) != 1+allocationScopeBytes-36+40 {
		return graphCutCommand{}, errors.Join(errCorrupt, err)
	}
	d := decoder{body[1:]}
	c := graphCutCommand{phase: body[0], scope: readAllocationScope(&d)}
	copy(c.configuration[:], d.b[:32])
	d.b = d.b[32:]
	c.round = d.number()
	expected, err := encodeGraphCutCommand(c)
	if err != nil || !bytes.Equal(expected, wire) {
		return graphCutCommand{}, errors.Join(errCorrupt, err)
	}
	return c, nil
}
func isGraphCutWire(w []byte) bool {
	return len(w) >= 4 && bytes.Equal(w[:4], []byte{'G', 'S', 'Q', 1})
}
func encodeGraphCertificate(c graphCertificate) ([]byte, error) {
	if !c.scope.valid() || !certifiedGraphAgreement(c.scope.semantic) || c.configuration == ([32]byte{}) || c.round == 0 || c.index == 0 || c.sourceRound > c.round || c.sourceIndex == 0 || c.sourceIndex >= c.index {
		return nil, errInvalid
	}
	b := appendAllocationScope([]byte{'G', 'S', 'C', 1}, c.scope)
	b = append(b, c.configuration[:]...)
	for _, x := range []uint64{c.round, c.index, c.sourceRound, c.sourceIndex} {
		b = binary.BigEndian.AppendUint64(b, x)
	}
	return seal(append(b, c.image[:]...)), nil
}
func decodeGraphCertificate(w []byte) (graphCertificate, error) {
	body, err := graphBody(w, "GSC\x01", 512)
	if err != nil || len(body) != allocationScopeBytes-36+32+32+172 {
		return graphCertificate{}, errors.Join(errCorrupt, err)
	}
	d := decoder{body}
	c := graphCertificate{scope: readAllocationScope(&d)}
	copy(c.configuration[:], d.b[:32])
	d.b = d.b[32:]
	c.round, c.index, c.sourceRound, c.sourceIndex = d.number(), d.number(), d.number(), d.number()
	copy(c.image[:], d.b)
	expected, err := encodeGraphCertificate(c)
	if err != nil || !bytes.Equal(expected, w) {
		return graphCertificate{}, errors.Join(errCorrupt, err)
	}
	return c, nil
}
func encodeGraphCutResult(r graphCutResult) []byte {
	b := []byte{'G', 'S', 'O', 1, r.phase, r.status}
	b = binary.BigEndian.AppendUint64(b, r.round)
	return seal(binary.BigEndian.AppendUint64(b, r.index))
}
func decodeGraphCutResult(w []byte) (graphCutResult, error) {
	body, e := graphBody(w, "GSO\x01", 54)
	if e != nil || len(body) != 18 {
		return graphCutResult{}, errors.Join(errCorrupt, e)
	}
	r := graphCutResult{phase: body[0], status: body[1], round: binary.BigEndian.Uint64(body[2:10]), index: binary.BigEndian.Uint64(body[10:])}
	if r.phase < 1 || r.phase > 2 || r.status < 1 || r.status > 4 || r.round == 0 || r.index == 0 {
		return graphCutResult{}, errCorrupt
	}
	return r, nil
}
func graphResultError(status byte) error {
	switch status {
	case graphCutApplied:
		return nil
	case graphCutPending:
		return errGraphCutPending
	case graphCutStale:
		return errInvalid
	case graphCutLimit:
		return errLimit
	default:
		return errCorrupt
	}
}

// Graph installs and routing publications strictly advance the installed floor,
// co-committing one immutable map. YES locks exclude intervening installations.
// The predecessor is therefore an exact round prefix; its index is only a local
// MVCC coordinate. A fence is deliberately NOT an installed round.
func readGraphRoundPredecessor(q *reader, c [32]byte, b uint64) (round, index uint64, image [172]byte, err error) {
	lower := recordKey(q.ns, 9)
	upper := partitionRoundMapKey(q.ns, b+1)
	var before []byte
	for {
		rows := min(q.limits.readRows-q.rows, q.view.ReadLimits().Rows)
		n := min(q.limits.readBytes-q.bytes, q.view.ReadLimits().Bytes)
		if rows < 1 || n < 512 {
			return 0, 0, image, errLimit
		}
		if e := reserveComposer(q, 2048); e != nil {
			return 0, 0, image, e
		}
		page, e := q.view.ScanReverse(q.ctx, lower, upper, before, raftlog.ReadBudget{Rows: 1, Bytes: min(n, 2048)})
		q.rows += page.Visited
		q.bytes += page.Bytes
		if e != nil {
			return 0, 0, image, e
		}
		if len(page.Rows) > 1 {
			return 0, 0, image, errCorrupt
		}
		if len(page.Rows) == 1 {
			row := page.Rows[0]
			body, e := graphBody(row.Value, "PLM\x01", 256)
			if e != nil || len(body) != 220 || len(row.Key) != len(lower)+8 || !bytes.Equal(row.Key[:len(lower)], lower) || !bytes.Equal(body[:32], c[:]) {
				return 0, 0, image, errors.Join(errCorrupt, e)
			}
			round, index = binary.BigEndian.Uint64(body[32:40]), binary.BigEndian.Uint64(body[40:48])
			copy(image[:], body[48:])
			if round > b || round != binary.BigEndian.Uint64(row.Key[len(lower):]) || index == 0 || index > q.base.Index {
				return 0, 0, image, errCorrupt
			}
			if e := chargeGraphWork(q, graphstore.PageWork{Bytes: 1024}); e != nil {
				return 0, 0, image, e
			}
			root, e := q.view.RootAt(q.ctx, index, 172)
			if e != nil || !bytes.Equal(root.Image, image[:]) {
				return 0, 0, image, errors.Join(errCorrupt, e)
			}
			return round, index, image, nil
		}
		if page.Complete {
			return 0, 0, image, errCorrupt
		}
		if len(page.Next) == 0 || len(before) > 0 && bytes.Compare(page.Next, before) >= 0 {
			return 0, 0, image, errCorrupt
		}
		before = page.Next
	}
}
func (m *declaredMaterializer) stageGraphCut(q *reader, entry replica.Entry, budget raftlog.ApplicationBudget) (batch raftlog.ApplicationBatch, err error) {
	c, e := decodeGraphCutCommand(entry.Data)
	if e != nil {
		return raftlog.ApplicationBatch{}, e
	}
	if !m.certifiedReads() || c.scope != m.scope() {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	if e := joinGraphOutputBudget(q, graphRequest{}, 4096, m.limits); e != nil {
		return raftlog.ApplicationBatch{}, e
	}
	result := graphCutResult{round: c.round, index: entry.Index, phase: c.phase, status: graphCutApplied}
	refuse := func(status byte) (raftlog.ApplicationBatch, error) {
		b := batchAt(q.base)
		result.status = status
		b.Outcome = encodeGraphCutResult(result)
		return b, m.preflight(b, budget)
	}
	// Resource refusal publishes no fence/certificate or partial graph state. It
	// is a normal operation result, including a completion-owner decode cliff.
	defer func() {
		if err != nil && (errors.Is(err, errLimit) || errors.Is(err, raftlog.ErrLimit) || errors.Is(err, graphstore.ErrResourceLimit)) {
			batch, err = refuse(graphCutLimit)
		}
	}()
	if m.rangeMoves() {
		move, e := readRangeMoveActive(q)
		if e != nil {
			return raftlog.ApplicationBatch{}, e
		}
		if move != ([16]byte{}) {
			return refuse(graphCutPending)
		}
	}
	cfg, e := m.initialized(q)
	if e != nil {
		return raftlog.ApplicationBatch{}, e
	}
	configuration, e := cfg.digest()
	if e != nil {
		return raftlog.ApplicationBatch{}, e
	}
	if configuration != c.configuration {
		return refuse(graphCutStale)
	}
	f, e := readGraphFence(q, configuration)
	if e != nil {
		return raftlog.ApplicationBatch{}, e
	}
	if c.phase == graphCutFence {
		if c.round > f {
			if e := q.put(graphFenceKey(q.ns), encodeGraphFence(configuration, c.round)); e != nil {
				return refuse(graphCutLimit)
			}
		}
	} else {
		old, found, e := q.getBounded(graphCertificateKey(q.ns, c.round), 512)
		if e != nil {
			return raftlog.ApplicationBatch{}, e
		}
		if found {
			cert, e := decodeGraphCertificate(old)
			if e != nil || cert.scope != m.scope() || cert.configuration != configuration {
				return raftlog.ApplicationBatch{}, errors.Join(errCorrupt, e)
			}
		} else {
			if f < c.round {
				return refuse(graphCutPending)
			}
			_, present, e := m.readCrossResourceOwner(q)
			if e != nil {
				return raftlog.ApplicationBatch{}, e
			}
			if present {
				return refuse(graphCutPending)
			}
			lock, e := crossLock(q)
			if e != nil {
				return raftlog.ApplicationBatch{}, e
			}
			if lock != ([16]byte{}) {
				return refuse(graphCutPending)
			}
			round, index, image, e := readGraphRoundPredecessor(q, configuration, c.round)
			if e != nil {
				if errors.Is(e, errLimit) || errors.Is(e, raftlog.ErrLimit) || errors.Is(e, graphstore.ErrResourceLimit) {
					return refuse(graphCutLimit)
				}
				return raftlog.ApplicationBatch{}, e
			}
			cert := graphCertificate{scope: m.scope(), configuration: configuration, round: c.round, index: entry.Index, sourceRound: round, sourceIndex: index, image: image}
			wire, e := encodeGraphCertificate(cert)
			if e != nil {
				return raftlog.ApplicationBatch{}, e
			}
			if e := q.put(graphCertificateKey(q.ns, c.round), wire); e != nil {
				return refuse(graphCutLimit)
			}
		}
	}
	b := batchAt(q.base)
	b.Writes = q.writes
	b.Outcome = encodeGraphCutResult(result)
	if e := sortPartitionWrites(b.Writes); e != nil {
		return raftlog.ApplicationBatch{}, e
	}
	return b, m.preflight(b, budget)
}

// PrepareGraphFence produces a source-owned durable allocation fence.
func (h *allocationHost) PrepareGraphFence(b uint64) (allocationProposal, error) {
	return h.prepareGraphCut(b, graphCutFence)
}

// PrepareGraphCertificate proposes closure; APPLY may return typed pending.
func (h *allocationHost) PrepareGraphCertificate(b uint64) (allocationProposal, error) {
	return h.prepareGraphCut(b, graphCutCertify)
}
func (h *allocationHost) prepareGraphCut(b uint64, phase byte) (allocationProposal, error) {
	if h == nil {
		return allocationProposal{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unavailableLocked() {
		return allocationProposal{}, replica.ErrStopped
	}
	if !h.machine.certifiedReads() {
		return allocationProposal{}, errInvalid
	}
	cfg, e := h.currentConfiguration()
	if e != nil {
		return allocationProposal{}, e
	}
	c, e := cfg.digest()
	if e != nil {
		return allocationProposal{}, e
	}
	wire, e := encodeGraphCutCommand(graphCutCommand{scope: h.machine.scope(), configuration: c, round: b, phase: phase})
	if e != nil {
		return allocationProposal{}, e
	}
	return allocationProposal{target: h.machine.scope(), wire: wire}, nil
}
func (m *declaredMaterializer) checkCertifiedPrerequisites(q *reader, c [32]byte) error {
	if !m.certifiedReads() {
		return nil
	}
	if _, e := readGraphFence(q, c); e != nil {
		return e
	}
	wire, found, e := q.getBounded(partitionRoundMapKey(q.ns, 0), 256)
	if e != nil {
		return e
	}
	if !found {
		return errCorrupt
	}
	body, e := graphBody(wire, "PLM\x01", 256)
	if e != nil || len(body) != 220 || !bytes.Equal(body[:32], c[:]) || binary.BigEndian.Uint64(body[32:40]) != 0 {
		return errors.Join(errCorrupt, e)
	}
	index := binary.BigEndian.Uint64(body[40:48])
	if index == 0 || index > q.base.Index {
		return errCorrupt
	}
	if e := chargeGraphWork(q, graphstore.PageWork{Records: 1, Bytes: 1024}); e != nil {
		return e
	}
	if e := reserveComposer(q, 1024); e != nil {
		return e
	}
	initial, e := q.view.RootAt(q.ctx, index, 172)
	if e != nil || !bytes.Equal(initial.Image, body[48:]) {
		return errors.Join(errCorrupt, e)
	}
	root, e := graphstore.DecodeRoot(initial.Image)
	if e != nil || root.Namespace() != (graphstore.Namespace{Graph: graphstate.GraphID(m.ns.graph), Partition: m.ns.partition}) || root.OwnershipEpoch() != m.owner || root.SemanticEpoch() != 1 {
		return errors.Join(errCorrupt, e)
	}
	return nil
}
