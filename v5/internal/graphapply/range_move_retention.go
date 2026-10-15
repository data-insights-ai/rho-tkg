package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const rangeMoveRetentionBytes = 320 << 10
const rangeMoveRetentionHeaderBytes = 8192
const rangeMoveRetentionOwnerBytes = graphPinManifestBytes + 2048

// Frozen handoff lineage is a descriptive snapshot, NEVER a live pin or a
// target-bank reservation. Original logical groups remain the native keepers;
// their current owner/bank/epoch observations authorize every later token use.
type rangeMoveRetainedLineage struct {
	scope                   allocationScope
	move                    [16]byte
	intent, configuration   [32]byte
	root                    raftlog.ApplicationRoot
	floor                   uint64
	head                    graphPinHead
	planCommand, planResult []byte
	header                  []byte
	owners                  [][]byte
}

func rangeMoveRetentionKey(n namespace, move [16]byte, shadow bool, ordinal uint64) []byte {
	phase := byte('H')
	if shadow {
		phase = 'J'
	}
	return binary.BigEndian.AppendUint64(rangeMoveControlKey(n, move, phase), ordinal)
}

func encodeMoveRetainedOwner(id [32]byte, manifest []byte, owner graphPinOwner, output *graphstate.OutputBudget) ([]byte, error) {
	if output == nil {
		return nil, errInvalid
	}
	if err := output.Reserve(4096 + 8*len(manifest)); err != nil {
		return nil, outputBudgetFailure(err)
	}
	wire, err := encodePinOwner(owner)
	if err != nil {
		return nil, err
	}
	return boundedEncoding(rangeMoveRetentionOwnerBytes, func(w *boundedWriter) {
		w.add([]byte{'G', 'L', 'P', 1})
		w.add(id[:])
		w.field(manifest)
		w.field(wire)
	})
}

func decodeMoveRetainedOwner(wire []byte, scope allocationScope, index uint64) ([32]byte, error) {
	var id [32]byte
	body, err := graphBody(wire, "GLP\x01", rangeMoveRetentionOwnerBytes)
	if err != nil {
		return id, err
	}
	d := graphCursor{b: body}
	copy(id[:], d.take(32))
	manifestWire, ownerWire := d.field(graphPinManifestBytes), d.field(128)
	if d.err != nil || len(d.b) != 0 {
		return id, errors.Join(errCorrupt, d.err)
	}
	manifest, err := decodeGraphPinManifest(manifestWire)
	if err != nil || manifest.id() != id {
		return id, errors.Join(errCorrupt, err)
	}
	ownerBody, err := graphBody(ownerWire, "GPA\x01", 128)
	if err != nil {
		return id, err
	}
	c := graphCursor{b: ownerBody}
	var owner graphPinOwner
	copy(owner.digest[:], c.take(32))
	owner.publication, owner.feedStart = c.u64(), c.u64()
	owner.terminal, owner.terminalIndex = c.tag(), c.u64()
	canonical, err := encodePinOwner(owner)
	part, found := pinPart(manifest, scope.partition)
	if c.err != nil || len(c.b) != 0 || err != nil || !bytes.Equal(canonical, ownerWire) || !found || part.certificate.scope != scope || part.certificate.index > index || part.certificate.sourceIndex > index || owner.digest != sha256.Sum256(manifestWire) || owner.publication == 0 || owner.publication > index || owner.terminal != 0 || owner.terminalIndex != 0 || owner.feedStart < part.feedStart || owner.feedStart > index+1 {
		return id, errors.Join(errCorrupt, c.err, err)
	}
	return id, nil
}

func encodeMoveRetentionHeader(p rangeMoveRetainedLineage, output *graphstate.OutputBudget) ([]byte, error) {
	if output == nil || !p.scope.valid() || !collectedGraphAgreement(p.scope.semantic) || p.move == ([16]byte{}) || p.intent == ([32]byte{}) || p.configuration == ([32]byte{}) || p.root.Index == 0 || len(p.root.Image) != 172 || sha256.Sum256(p.root.Image) != p.root.ImageHash || len(p.owners) != len(p.head.owners) || len(p.owners) > maxGraphPins {
		return nil, errInvalid
	}
	if err := output.Reserve(8192 + 256*len(p.owners)); err != nil {
		return nil, outputBudgetFailure(err)
	}
	head, err := encodePinHead(p.head)
	if err != nil {
		return nil, err
	}
	return boundedEncoding(rangeMoveRetentionHeaderBytes, func(w *boundedWriter) {
		w.add([]byte{'G', 'L', 'H', 1})
		w.add(appendAllocationScope(nil, p.scope))
		w.add(p.move[:])
		w.add(p.intent[:])
		w.add(p.configuration[:])
		w.u64(p.root.Index)
		w.add(p.root.ImageHash[:])
		w.field(p.root.Image)
		w.u64(p.floor)
		w.field(head)
		w.field(p.planCommand)
		w.field(p.planResult)
		w.u32(len(p.owners))
		for i, owner := range p.owners {
			w.add(p.head.owners[i][:])
			sum := sha256.Sum256(owner)
			w.add(sum[:])
		}
	})
}

func decodeMoveRetentionHeader(wire []byte) (p rangeMoveRetainedLineage, sums [][32]byte, err error) {
	body, err := graphBody(wire, "GLH\x01", rangeMoveRetentionHeaderBytes)
	if err != nil {
		return p, nil, err
	}
	d := graphCursor{b: body}
	p.scope = readAllocationScope(&decoder{b: d.take(120)})
	p.move = d.array()
	copy(p.intent[:], d.take(32))
	copy(p.configuration[:], d.take(32))
	p.root.Index = d.u64()
	copy(p.root.ImageHash[:], d.take(32))
	p.root.Image = d.field(172)
	p.floor = d.u64()
	head := d.field(1024)
	p.planCommand, p.planResult = d.field(graphRetentionCommandBytes), d.field(256)
	n := d.count(maxGraphPins, 64)
	if d.err != nil {
		return p, nil, d.err
	}
	// The complete pin-head codec remains authoritative; compare its canonical
	// representation below instead of accepting a caller's owner count/hash.
	hb, e := graphBody(head, "GPH\x01", 1024)
	if e != nil {
		return p, nil, e
	}
	h := graphCursor{b: hb}
	p.head.epoch, p.head.certificateFloor, p.head.sourceFloor, p.head.feedFloor = h.u64(), h.u64(), h.u64(), h.u64()
	count := h.count(maxGraphPins, 32)
	p.head.owners = make([][32]byte, count)
	for i := range p.head.owners {
		copy(p.head.owners[i][:], h.take(32))
	}
	canonical, e := encodePinHead(p.head)
	if h.err != nil || len(h.b) != 0 || e != nil || !bytes.Equal(canonical, head) || count != n {
		return p, nil, errors.Join(errCorrupt, h.err, e)
	}
	sums = make([][32]byte, n)
	for i := range n {
		var id [32]byte
		copy(id[:], d.take(32))
		copy(sums[i][:], d.take(32))
		if id != p.head.owners[i] {
			return p, nil, errCorrupt
		}
	}
	if d.err != nil || len(d.b) != 0 || !p.scope.valid() || !collectedGraphAgreement(p.scope.semantic) || p.move == ([16]byte{}) || p.intent == ([32]byte{}) || p.configuration == ([32]byte{}) || p.root.Index == 0 || len(p.root.Image) != 172 || sha256.Sum256(p.root.Image) != p.root.ImageHash || (len(p.planCommand) == 0) != (len(p.planResult) == 0) {
		return p, nil, errors.Join(errCorrupt, d.err)
	}
	if len(p.planCommand) != 0 {
		command, e := decodeGraphRetentionCommand(p.planCommand)
		result, re := decodeGraphRetentionResult(p.planResult)
		if e != nil || re != nil || command.scope != p.scope || command.configuration != p.configuration || result.status != graphRetentionApplied || result.id != sha256.Sum256(p.planCommand) || result.index > p.root.Index {
			return p, nil, errors.Join(errCorrupt, e, re)
		}
	}
	p.header = wire
	return p, sums, nil
}

func encodeMoveRetainedLineage(p rangeMoveRetainedLineage, output *graphstate.OutputBudget) ([]byte, error) {
	if output == nil {
		return nil, errInvalid
	}
	size := len(p.header) + 4096
	for _, wire := range p.owners {
		size += len(wire) + 8
	}
	if size > rangeMoveRetentionBytes {
		return nil, errLimit
	}
	if err := output.Reserve(4 * size); err != nil {
		return nil, outputBudgetFailure(err)
	}
	return boundedEncoding(rangeMoveRetentionBytes, func(w *boundedWriter) {
		w.add([]byte{'G', 'L', 'B', 1})
		w.field(p.header)
		w.u32(len(p.owners))
		for _, wire := range p.owners {
			w.field(wire)
		}
	})
}

func decodeMoveRetainedLineage(wire []byte, output *graphstate.OutputBudget) (p rangeMoveRetainedLineage, err error) {
	if output == nil {
		return p, errInvalid
	}
	body, err := graphBody(wire, "GLB\x01", rangeMoveRetentionBytes)
	if err != nil {
		return p, err
	}
	// Includes every manifest/canonical decoder and the owned packet arrays.
	if err := output.Reserve(16384 + 16*len(wire)); err != nil {
		return p, outputBudgetFailure(err)
	}
	d := graphCursor{b: body}
	header := d.field(rangeMoveRetentionHeaderBytes)
	n := d.count(maxGraphPins, 4)
	if d.err != nil {
		return p, d.err
	}
	p, sums, err := decodeMoveRetentionHeader(header)
	if err != nil || len(sums) != n {
		return p, errors.Join(errCorrupt, err)
	}
	p.header, p.root.Image, p.planCommand, p.planResult = owned(header), owned(p.root.Image), owned(p.planCommand), owned(p.planResult)
	p.owners = make([][]byte, n)
	for i := range n {
		part := d.field(rangeMoveRetentionOwnerBytes)
		if d.err != nil || sha256.Sum256(part) != sums[i] {
			return p, errors.Join(errCorrupt, d.err)
		}
		id, e := decodeMoveRetainedOwner(part, p.scope, p.root.Index)
		if e != nil || id != p.head.owners[i] {
			return p, errors.Join(errCorrupt, e)
		}
		p.owners[i] = owned(part)
	}
	if d.err != nil || len(d.b) != 0 {
		return p, errors.Join(errCorrupt, d.err)
	}
	return p, nil
}

func (m *declaredMaterializer) captureMoveRetainedLineage(q *reader, move [16]byte, intent, configuration [32]byte, floor uint64) ([]raftlog.ApplicationControlPut, error) {
	if !m.collectsHistory() {
		return nil, nil
	}
	if err := m.checkRetainedPinRoster(q, true); err != nil {
		return nil, err
	}
	plan, command, result, err := activeGraphRetention(q)
	if err != nil {
		return nil, err
	}
	head, err := readPinHead(q)
	if err != nil {
		return nil, err
	}
	if err := reserveComposer(q, 8192+256*len(head.owners)); err != nil {
		return nil, err
	}
	p := rangeMoveRetainedLineage{scope: m.scope(), move: move, intent: intent, configuration: configuration, root: q.base, floor: floor, head: head, owners: make([][]byte, len(head.owners))}
	if plan.ID != ([32]byte{}) {
		if err := reserveComposer(q, 8192); err != nil {
			return nil, err
		}
		p.planCommand, err = encodeGraphRetentionCommand(command)
		if err != nil {
			return nil, err
		}
		p.planResult, err = encodeGraphRetentionResult(result)
		if err != nil {
			return nil, err
		}
	}
	for i, id := range head.owners {
		_, wire, found, e := readPinOriginal(q, id)
		if e != nil || !found {
			return nil, errors.Join(errCorrupt, e)
		}
		owner, found, e := readPinOwner(q, id)
		if e != nil || !found {
			return nil, errors.Join(errCorrupt, e)
		}
		p.owners[i], err = encodeMoveRetainedOwner(id, wire, owner, q.arena)
		if err != nil {
			return nil, err
		}
	}
	p.header, err = encodeMoveRetentionHeader(p, q.arena)
	if err != nil {
		return nil, err
	}
	return moveRetentionControls(q, move, false, p)
}

func moveRetentionControls(q *reader, move [16]byte, shadow bool, p rangeMoveRetainedLineage) ([]raftlog.ApplicationControlPut, error) {
	cost := 4096 + 4*len(p.header)
	for _, part := range p.owners {
		cost += 256 + 4*len(part)
	}
	if err := reserveComposer(q, cost); err != nil {
		return nil, err
	}
	puts := make([]raftlog.ApplicationControlPut, 0, len(p.owners)+1)
	puts = append(puts, raftlog.ApplicationControlPut{Key: rangeMoveRetentionKey(q.ns, move, shadow, 0), Value: owned(p.header)})
	for i, part := range p.owners {
		puts = append(puts, raftlog.ApplicationControlPut{Key: rangeMoveRetentionKey(q.ns, move, shadow, uint64(i+1)), Value: owned(part)})
	}
	return puts, nil
}

func readMoveRetainedLineage(q *reader, move [16]byte, shadow bool) (p rangeMoveRetainedLineage, found bool, err error) {
	header, found, err := crossControl(q, rangeMoveRetentionKey(q.ns, move, shadow, 0), rangeMoveRetentionHeaderBytes)
	if err != nil || !found {
		return p, found, err
	}
	if err := reserveComposer(q, 16384+16*len(header.Value)); err != nil {
		return p, false, err
	}
	p, sums, err := decodeMoveRetentionHeader(header.Value)
	if err != nil {
		return p, false, err
	}
	if !shadow && header.Index != p.root.Index+1 {
		return p, false, errCorrupt
	}
	if err := reserveComposer(q, 256*len(sums)); err != nil {
		return p, false, err
	}
	p.owners = make([][]byte, len(sums))
	for i := range sums {
		part, present, e := crossControl(q, rangeMoveRetentionKey(q.ns, move, shadow, uint64(i+1)), rangeMoveRetentionOwnerBytes)
		if e != nil {
			return p, false, e
		}
		if !present || part.Index != header.Index || sha256.Sum256(part.Value) != sums[i] {
			return p, false, errCorrupt
		}
		if err := reserveComposer(q, 4096+16*len(part.Value)); err != nil {
			return p, false, err
		}
		id, e := decodeMoveRetainedOwner(part.Value, p.scope, p.root.Index)
		if e != nil || id != p.head.owners[i] {
			return p, false, errors.Join(errCorrupt, e)
		}
		p.owners[i] = part.Value
	}
	_, extra, err := crossControl(q, rangeMoveRetentionKey(q.ns, move, shadow, uint64(len(sums)+1)), rangeMoveRetentionOwnerBytes)
	if err != nil {
		return p, false, err
	}
	if extra {
		return p, false, errCorrupt
	}
	return p, true, nil
}

func checkMoveRetainedBinding(p rangeMoveRetainedLineage, intent rangeMoveIntent, move [16]byte, digest [32]byte, root raftlog.ApplicationRoot, floor uint64, source bool) error {
	scope := intent.target
	if source {
		scope = intent.source
	}
	if p.scope != scope || p.move != move || p.intent != digest || p.configuration != intent.publication.Configuration || p.root.Index != root.Index || p.root.ImageHash != root.ImageHash || !bytes.Equal(p.root.Image, root.Image) || p.floor != floor {
		return errCorrupt
	}
	return nil
}

// This audit authenticates durable frozen identities, not current liveness. A
// terminal original may outlive its old root; only active native owners demand
// source data, checked separately by checkRetainedPinRoster.
func (m *declaredMaterializer) checkMoveRetainedLineages(q *reader) error {
	if !m.collectsHistory() {
		return nil
	}
	var after []byte
	for {
		page, err := rangeMoveReplayScan(q, recordKey(q.ns, 18), after, 8192, 1)
		if err != nil {
			return err
		}
		for _, row := range page.Rows {
			status, e := decodeRangeMoveStatus(row.Value)
			if e != nil {
				return e
			}
			if status.ns != q.ns || status.index > q.base.Index || !bytes.Equal(row.Key, rangeMoveStatusKey(q.ns, status.move)) {
				return errCorrupt
			}
			capture, present, e := crossControl(q, rangeMoveControlKey(q.ns, status.move, 'C'), 1024)
			if e != nil {
				return e
			}
			if !present {
				continue
			}
			intentRecord, present, e := crossControl(q, rangeMoveControlKey(q.ns, status.move, 'I'), 1024)
			if e != nil || !present {
				return errors.Join(errCorrupt, e)
			}
			if sha256.Sum256(intentRecord.Value) != status.intent {
				return errCorrupt
			}
			intent, e := decodeRangeMoveIntent(intentRecord.Value)
			if e != nil {
				return e
			}
			root, floor, e := moveCaptureRoot(capture.Value, m.scope())
			if e != nil {
				return e
			}
			p, present, e := readMoveRetainedLineage(q, status.move, false)
			if e != nil || !present {
				return errors.Join(errCorrupt, e)
			}
			if e := checkMoveRetainedBinding(p, intent, status.move, status.intent, root, floor, m.scope() == intent.source); e != nil {
				return e
			}
			if m.scope() == intent.source {
				progressWire, hasProgress, e := q.getBounded(rangeMoveProgressKey(q.ns, status.move), 1<<20)
				if e != nil {
					return e
				}
				if hasProgress && len(progressWire) >= 4 && bytes.Equal(progressWire[:3], []byte{'G', 'M', 'P'}) {
					progress, e := decodeRangeMoveProgress(progressWire, q.arena)
					if e != nil {
						return e
					}
					packet, e := encodeMoveRetainedLineage(p, q.arena)
					if e != nil {
						return e
					}
					if rangeMoveSpans(m.SemanticContractID()) {
						if e = progress.bindRetention(p, packet); e != nil {
							return e
						}
					}
					if !progress.retainedMatches(packet) {
						return errCorrupt
					}
					decision, hasDecision, e := crossControl(q, rangeMoveControlKey(q.ns, status.move, 'D'), 256)
					if e != nil {
						return e
					}
					if hasDecision {
						_, _, digest := progress.transferSummary()
						d, e := decodeRangeMoveStatus(decision.Value)
						if e != nil || d.ns != q.ns || d.move != status.move || d.intent != status.intent || d.phase != rangeMoveDecide || d.digest != digest || d.round == 0 {
							return errors.Join(errCorrupt, e)
						}
					}
				}
			}
			shadow, present, e := readMoveRetainedLineage(q, status.move, true)
			if e != nil {
				return e
			}
			if !present {
				if status.phase == rangeMoveActivate || status.phase == rangeMoveFinish {
					if m.scope() == intent.target {
						return errCorrupt
					}
				}
				continue
			}
			wire, present, e := q.getBounded(rangeMoveProgressKey(q.ns, status.move), 1<<20)
			if e != nil || !present {
				return errors.Join(errCorrupt, e)
			}
			progress, e := decodeRangeMoveProgress(wire, q.arena)
			if e != nil {
				return e
			}
			packet, e := encodeMoveRetainedLineage(shadow, q.arena)
			if e == nil && rangeMoveSpans(m.SemanticContractID()) {
				e = progress.bindRetention(shadow, packet)
			}
			if e != nil || !progress.retainedMatches(packet) || shadow.scope != intent.source || shadow.move != status.move || shadow.intent != status.intent {
				return errors.Join(errCorrupt, e)
			}
			if status.phase == rangeMoveActivate || status.phase == rangeMoveFinish {
				_, _, digest := progress.transferSummary()
				decision, present, e := crossControl(q, rangeMoveControlKey(q.ns, status.move, 'D'), 256)
				if e != nil || !present {
					return errors.Join(errCorrupt, e)
				}
				d, e := decodeRangeMoveStatus(decision.Value)
				if e != nil || d.ns != (namespace{intent.source.graph, intent.source.partition}) || d.intent != status.intent || d.digest != digest || d.phase != rangeMoveDecide || d.round == 0 {
					return errors.Join(errCorrupt, e)
				}
			}
		}
		if page.Complete {
			return nil
		}
		after = page.Next
	}
}

func checkMoveRetentionDescriptor(p rangeMoveRetainedLineage, d graphstore.RangeMoveDescriptor) error {
	b := d.Source.Binding
	if !collectedGraphAgreement(p.scope.semantic) || b.SemanticContractID != p.scope.semantic || b.Identity.Graph != [16]byte(p.scope.graph) || b.Identity.Partition != p.scope.partition || b.Identity.Group != p.scope.group || d.Declaration != p.scope.declaration || d.Configuration != p.configuration || d.Source.Index != p.root.Index || d.Source.ImageHash != p.root.ImageHash || !bytes.Equal(d.Source.Image[:], p.root.Image) {
		return errCorrupt
	}
	return nil
}
