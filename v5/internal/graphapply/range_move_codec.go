package graphapply

import (
	"bytes"
	"crypto/sha256"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

type rangeMoveCommand struct {
	scope    allocationScope
	phase    rangeMovePhase
	intent   []byte
	sequence uint64
	data     []byte
	proofs   []rangeMoveStatus
}

func isRangeMoveWire(wire []byte) bool {
	return len(wire) >= 4 && bytes.Equal(wire[:4], []byte{'G', 'M', 'Q', 1})
}

func encodeRangeMoveCommand(c rangeMoveCommand) ([]byte, error) {
	intent, err := decodeRangeMoveIntent(c.intent)
	if err != nil || !c.scope.valid() || c.phase < rangeMoveFence || c.phase > rangeMoveAbort || c.sequence > uint64(intent.maxPages) || len(c.data) > (1<<20)-2048 || len(c.proofs) > 2 { // #nosec G115 -- decoded valid intent bounds maxPages to1..1024; short-circuit refuses decode errors before conversion.
		return nil, errors.Join(errInvalid, err)
	}
	if c.scope != intent.source && c.scope != intent.target {
		return nil, errInvalid
	}
	return boundedEncoding(1<<20, func(w *boundedWriter) {
		w.add([]byte{'G', 'M', 'Q', 1})
		w.add(appendAllocationScope(nil, c.scope))
		w.tag(byte(c.phase))
		w.u64(c.sequence)
		w.field(c.intent)
		w.field(c.data)
		w.u32(len(c.proofs))
		for _, p := range c.proofs {
			wire, err := encodeRangeMoveStatus(p)
			if err != nil {
				w.err = err
				return
			}
			w.field(wire)
		}
	})
}

func decodeRangeMoveCommand(wire []byte) (rangeMoveCommand, error) {
	body, err := graphBody(wire, "GMQ\x01", 1<<20)
	if err != nil {
		return rangeMoveCommand{}, err
	}
	c := graphCursor{b: body}
	scope := c.take(120)
	r := rangeMoveCommand{phase: rangeMovePhase(c.tag()), sequence: c.u64()}
	r.intent, r.data = c.field(1024), c.field((1<<20)-2048)
	n := c.count(2, 4)
	if c.err != nil {
		return rangeMoveCommand{}, c.err
	}
	r.proofs = make([]rangeMoveStatus, n)
	for i := range n {
		r.proofs[i], err = decodeRangeMoveStatus(c.field(256))
		if err != nil {
			return rangeMoveCommand{}, err
		}
	}
	if c.err != nil || len(c.b) != 0 {
		return rangeMoveCommand{}, errors.Join(errCorrupt, c.err)
	}
	r.scope = readAllocationScope(&decoder{b: scope})
	expected, err := encodeRangeMoveCommand(r)
	if err != nil || !bytes.Equal(expected, wire) {
		return rangeMoveCommand{}, errors.Join(errCorrupt, err)
	}
	return r, nil
}

// This provenance capability is sealed only by native completed export or a
// matched reply on an authenticated configured endpoint. Codec data has no seal.
type rangeMoveAuthority struct {
	scope       allocationScope
	id          allocationReadID
	question    rangeMoveQuery
	observation rangeMoveObservation
	stamp       *rangeMoveAuthorityStamp
}

type rangeMoveAuthorityStamp struct {
	scope allocationScope
	id    allocationReadID
	query rangeMoveQuery
	hash  [32]byte
}

func rangeMoveObservationHash(o rangeMoveObservation) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("rho-tkg:range-move-observation:v2\x00"))
	field := func(b []byte) {
		w := boundedWriter{max: 8, b: make([]byte, 0, 8)}
		w.u64(uint64(len(b)))
		_, _ = h.Write(w.b)
		_, _ = h.Write(b)
	}
	_, _ = h.Write(appendAllocationScope(nil, o.scope))
	_, _ = h.Write(o.configuration[:])
	w := boundedWriter{max: 128, b: make([]byte, 0, 128)}
	for _, n := range []uint64{o.index, o.floor, o.root.Generation, o.root.Index, uint64(o.sourceRows), uint64(o.sourceBytes), uint64(o.ownedBytes)} { // #nosec G115 -- canonical bit encoding of signed work metadata for hashing only; authority admission separately requires nonnegative bounded counters.
		w.u64(n)
	}
	_, _ = h.Write(w.b)
	_, _ = h.Write(o.root.ImageHash[:])
	field(o.root.Image)
	for _, s := range []rangeMoveStatus{o.status, o.decision} {
		// Hash absent status fields too. No mutation may borrow the stamp merely
		// because the phase still denotes absence.
		w := boundedWriter{max: 256, b: make([]byte, 0, 256)}
		w.add(appendNamespace(nil, s.ns))
		w.add(s.move[:])
		w.add(s.intent[:])
		w.tag(byte(s.phase))
		w.tag(s.result)
		for _, n := range []uint64{s.index, s.floor, s.round, s.sequence, s.rows, s.size} {
			w.u64(n)
		}
		w.add(s.digest[:])
		field(w.b)
	}
	field(o.intent)
	field(o.progress)
	field(o.original)
	w = boundedWriter{max: 9, b: make([]byte, 0, 9)}
	if o.originalFound {
		w.tag(1)
	} else {
		w.tag(0)
	}
	w.u64(o.originalIndex)
	_, _ = h.Write(w.b)
	var digest [32]byte
	h.Sum(digest[:0])
	return digest
}

func sealRangeMoveAuthority(p rangeMoveAuthority) rangeMoveAuthority {
	p.stamp = &rangeMoveAuthorityStamp{p.scope, p.id, p.question, rangeMoveObservationHash(p.observation)}
	return p
}

func (p rangeMoveAuthority) validSeal() bool {
	return p.stamp != nil && p.scope == p.stamp.scope && p.id == p.stamp.id && p.question == p.stamp.query && p.stamp.hash == rangeMoveObservationHash(p.observation)
}

func (h *allocationHost) exportRangeMoveAuthority(p rangeMoveProof) (rangeMoveAuthority, error) {
	if h == nil {
		return rangeMoveAuthority{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if p.owner != h || p.observation.index > h.driver.Applied() {
		return rangeMoveAuthority{}, errInvalid
	}
	if err := checkRangeMoveProof(p); err != nil {
		return rangeMoveAuthority{}, err
	}
	return sealRangeMoveAuthority(rangeMoveAuthority{scope: p.observation.scope, id: p.id, question: p.question, observation: p.observation}), nil
}

func (h *allocationHost) prepareRangeMoveCommand(intentWire []byte, phase rangeMovePhase, partition uint64, sequence uint64, data []byte, evidence []rangeMoveAuthority, output *graphstate.OutputBudget) (allocationProposal, error) {
	if h == nil || output == nil {
		return allocationProposal{}, errInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unavailableLocked() || h.machine == nil || !h.machine.rangeMoves() {
		return allocationProposal{}, errInvalid
	}
	intent, err := decodeRangeMoveIntent(intentWire)
	if err != nil || intent.source != h.machine.scope() {
		return allocationProposal{}, errors.Join(errInvalid, err)
	}
	target, err := h.target(partition)
	if err != nil {
		return allocationProposal{}, err
	}
	if target != intent.source && target != intent.target || len(evidence) > 2 {
		return allocationProposal{}, errInvalid
	}
	if err := output.Reserve(4096 + 8*(len(intentWire)+len(data)) + 512*len(evidence)); err != nil {
		return allocationProposal{}, outputBudgetFailure(err)
	}
	move := rangeMoveID(namespace{intent.source.graph, intent.source.partition}, intent.id)
	hash := sha256.Sum256(intentWire)
	proofs := make([]rangeMoveStatus, 0, len(evidence))
	for _, p := range evidence {
		if !p.validSeal() || p.scope != intent.source && p.scope != intent.target || p.question.move != move || p.id.session == ([16]byte{}) || p.id.sequence == 0 || p.observation.scope != p.scope || p.observation.status.move != move || p.observation.status.intent != hash || p.observation.index < p.observation.status.index || p.observation.status.result != rangeMoveApplied {
			return allocationProposal{}, errInvalid
		}
		proofs = append(proofs, p.observation.status)
	}
	wire, err := encodeRangeMoveCommand(rangeMoveCommand{target, phase, owned(intentWire), sequence, owned(data), proofs})
	if err != nil {
		return allocationProposal{}, err
	}
	return allocationProposal{target: target, wire: wire}, nil
}
