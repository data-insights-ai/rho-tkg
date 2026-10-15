package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

type retainedFeedEnvelope struct {
	kind         byte
	tx           [16]byte
	round        uint64
	participants []uint64
}

// Only actual first-install graph changes become business heads. Empty current
// CDC on a later replay is checked against its original separately; it is not a
// second transaction and requires no history-sized deduplication set.
func (m *declaredMaterializer) feedEnvelope(f retainedFeedFrame, arena *graphstate.OutputBudget) (retainedFeedEnvelope, error) {
	var empty retainedFeedEnvelope
	if f.partition != m.ns.partition || f.index < 2 || arena == nil {
		return empty, errInvalid
	}
	root, err := graphstore.DecodeRoot(f.root[:])
	if err != nil || root.Namespace() != (graphstore.Namespace{Graph: graphstate.GraphID(m.ns.graph), Partition: f.partition}) {
		return empty, errors.Join(errInvalid, err)
	}
	if len(f.changes) == 0 {
		return empty, nil
	}
	if bytes.HasPrefix(f.changes, []byte("GML\x01")) {
		status, err := decodeRangeMoveStatus(f.outcome)
		if err != nil || status.ns != m.ns || status.index != f.index || status.round != f.round || status.phase != rangeMoveActivate || status.result != rangeMoveApplied || status.format != rangeMoveStatusFormat(m.SemanticContractID()) {
			return empty, errors.Join(errInvalid, err)
		}
		intent, err := decodeRangeMoveIntent(f.intent)
		if err != nil || rangeMoveID(namespace{intent.source.graph, intent.source.partition}, intent.id) != status.move || sha256.Sum256(f.intent) != status.intent {
			return empty, errors.Join(errInvalid, err)
		}
		decision, err := decodeRangeMoveStatus(f.decision)
		if err != nil || decision.ns != (namespace{intent.source.graph, intent.source.partition}) || decision.move != status.move || decision.intent != status.intent || decision.phase != rangeMoveDecide || decision.result != rangeMoveApplied || decision.round != status.round || decision.digest != status.digest {
			return empty, errors.Join(errInvalid, err)
		}
		logical, err := boundedEncoding(2048, func(w *boundedWriter) {
			w.add([]byte("GML\x01"))
			w.add(status.move[:])
			w.add(status.intent[:])
			w.u64(intent.publication.RangeID)
			w.u64(intent.source.partition)
			w.u64(intent.target.partition)
			w.u64(intent.epoch)
			w.u64(intent.epoch + 1)
			w.u64(decision.round)
			w.add(decision.digest[:])
		})
		if err != nil || !bytes.Equal(logical, f.changes) {
			return empty, errors.Join(errInvalid, err)
		}
		parts := []uint64{intent.source.partition, intent.target.partition}
		if parts[0] > parts[1] {
			parts[0], parts[1] = parts[1], parts[0]
		}
		return retainedFeedEnvelope{kind: feedMoveEnvelope, tx: status.move, round: status.round, participants: parts}, nil
	}
	if bytes.HasPrefix(f.outcome, []byte("GCS\x01")) {
		s, err := decodeCrossStatus(f.outcome)
		if err != nil || s.ns != m.ns || s.index != f.index || s.phase != crossResolve || !s.commit || s.round != f.round {
			return empty, errors.Join(errInvalid, err)
		}
		manifest, err := decodeCrossManifest(f.registration, m.limits, arena)
		if err != nil || manifest.tx != s.tx || manifest.digest != s.digest || len(manifest.parts) != m.declaration.Len() {
			return empty, errors.Join(errInvalid, err)
		}
		if _, err = decodeTypedChangeEnvelope(f.changes, outcome{ns: m.ns, kind: graphOperations, identity: s.tx, hash: s.digest, index: s.index, disposition: applied}, m.limits, true); err != nil {
			return empty, err
		}
		parts := make([]uint64, len(manifest.parts))
		for i, p := range manifest.parts {
			actual, _ := m.declaration.PartitionAt(i)
			if p.binding.Partition != actual.Partition {
				return empty, errInvalid
			}
			parts[i] = actual.Partition
		}
		return retainedFeedEnvelope{kind: feedCrossEnvelope, tx: s.tx, round: s.round, participants: parts}, nil
	}
	if bytes.HasPrefix(f.changes, []byte("GCE\x01")) {
		o, err := decodePartitionMappedOutcome(f.outcome, m.ns)
		if err != nil || o.index != f.index || o.reason != reasonNone || o.disposition != applied || o.kind != graphOperations && o.kind != guardedGraphOperations {
			return empty, errors.Join(errInvalid, err)
		}
		g, err := decodeTypedChangeEnvelope(f.changes, o, m.limits, true)
		if err != nil || g.initialized || f.round == 0 {
			return empty, errors.Join(errInvalid, err)
		}
		return retainedFeedEnvelope{kind: feedLocalEnvelope, tx: crossTransactionID(m.ns, requestID(o.identity)), round: f.round, participants: []uint64{f.partition}}, nil
	}
	// Pin/GC/feed metadata and allocation control CDC are authenticated native
	// progress frames. They are retained in the page, never invented graph deltas.
	if bytes.Equal(f.changes, f.outcome) {
		return empty, nil
	}
	return empty, errInvalid
}

func feedEnvelopeEqual(a, b retainedFeedEnvelope) bool {
	return a.kind == b.kind && a.tx == b.tx && a.round == b.round && feedVectorEqual(a.participants, b.participants)
}

func (m *declaredMaterializer) checkFeedPageShape(p retainedFeedPage, arena *graphstate.OutputBudget) error {
	if m == nil || arena == nil || !validFeedPageIdentity(p) {
		return errInvalid
	}
	var first retainedFeedEnvelope
	var registration, intent, decision, logical []byte
	hasDecision := false
	parts := make([]uint64, 0, len(p.frames))
	for _, frame := range p.frames {
		local := *m
		local.ns.partition = frame.partition
		head, err := local.feedEnvelope(frame, arena)
		if err != nil {
			return err
		}
		if p.kind == 0 {
			if head.kind != 0 {
				return errInvalid
			}
			continue
		}
		if head.kind != p.kind || head.tx != p.tx || head.round != p.round {
			return errInvalid
		}
		if len(parts) == 0 {
			first = head
			registration = frame.registration
			intent = frame.intent
			decision = frame.decision
			logical = frame.changes
		} else if !feedEnvelopeEqual(first, head) {
			return errInvalid
		}
		parts = append(parts, frame.partition)
		if p.kind == feedCrossEnvelope {
			if !bytes.Equal(registration, frame.registration) {
				return errInvalid
			}
			if len(frame.decision) > 0 {
				d, err := decodeCrossStatus(frame.decision)
				manifest, err2 := decodeCrossManifest(frame.registration, m.limits, arena)
				if err != nil || err2 != nil || d.ns.graph != m.ns.graph || d.ns.partition != manifest.coordinator || d.tx != p.tx || d.digest != manifest.digest || !d.commit || d.phase != crossDecide || d.round != p.round {
					return errors.Join(errInvalid, err, err2)
				}
				if frame.partition == manifest.coordinator {
					hasDecision = true
				}
			}
		}
		if p.kind == feedMoveEnvelope && (!bytes.Equal(intent, frame.intent) || !bytes.Equal(decision, frame.decision) || !bytes.Equal(logical, frame.changes)) {
			return errInvalid
		}
	}
	if p.kind == 0 {
		return nil
	}
	if !feedVectorEqual(parts, first.participants) || p.kind == feedCrossEnvelope && !hasDecision {
		return errInvalid
	}
	return nil
}

// With every declared participant locked, two committed cross transactions
// cannot have opposite first-install orders. Resolve requires round>localfloor;
// ordinary installs also refuse under that lock. Protocol-only and replay frames
// need no such order, and do not enter this merge. Refuse rather than skip a
// different first-install head or retain an unbounded set of deferred TxIDs.
func completeFeedEnvelope(heads []retainedFeedEnvelope, parts []uint64) (retainedFeedEnvelope, []bool, error) {
	if len(heads) == 0 || len(heads) != len(parts) || len(parts) > maxGraphPins {
		return retainedFeedEnvelope{}, nil, errInvalid
	}
	selected := -1
	for i, h := range heads {
		if parts[i] == 0 || i > 0 && parts[i-1] >= parts[i] {
			return retainedFeedEnvelope{}, nil, errInvalid
		}
		if h.kind == 0 {
			continue
		}
		if h.round == 0 || h.tx == ([16]byte{}) {
			return retainedFeedEnvelope{}, nil, errInvalid
		}
		if selected < 0 || h.round < heads[selected].round || h.round == heads[selected].round && (bytes.Compare(h.tx[:], heads[selected].tx[:]) < 0 || h.tx == heads[selected].tx && parts[i] < parts[selected]) {
			selected = i
		}
	}
	consume := make([]bool, len(parts))
	if selected < 0 {
		return retainedFeedEnvelope{}, consume, nil
	}
	want := heads[selected]
	for _, part := range want.participants {
		found := -1
		for i, p := range parts {
			if p == part {
				found = i
				break
			}
		}
		if found < 0 {
			return retainedFeedEnvelope{}, nil, errGraphCutScope
		}
		if heads[found].kind == 0 {
			return retainedFeedEnvelope{}, nil, errRetainedFeedPending
		}
		if !feedEnvelopeEqual(want, heads[found]) {
			return retainedFeedEnvelope{}, nil, errInvalid
		}
		consume[found] = true
	}
	return want, consume, nil
}

func (m *declaredMaterializer) feedAt(q *reader, index uint64, fn func(*reader) error) (err error) {
	// Only active native Stage/source readers call this method. Snapshot audit
	// and recovery classification must use their supplied bank, never this door.
	if q == nil || q.preparedHistory != nil || index == 0 || index > q.base.Index || fn == nil {
		return errInvalid
	}
	if err = reserveComposer(q, 2048); err != nil {
		return err
	}
	if err = chargeGraphWork(q, graphstore.PageWork{Records: 1, Bytes: 2048}); err != nil {
		return err
	}
	view, err := m.store.ApplicationView(index)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, view.Close()) }()
	base, err := view.RootBounded(q.ctx, 172)
	if err != nil {
		return err
	}
	child := reader{ctx: q.ctx, view: view, ns: q.ns, base: base, arena: q.arena, limits: q.limits, rows: q.rows, bytes: q.bytes}
	defer func() { q.rows, q.bytes = child.rows, child.bytes }()
	return fn(&child)
}

// Native history APIs preflight complete delivered frames before copying. The
// existing operation ledger first admits their documented worst source ceiling;
// completed work is charged from actual delivered lengths, failure retires the
// offered ceiling. The output reservation is never refunded on uncertainty.
func (m *declaredMaterializer) feedRecord(q *reader, index uint64, outcome bool, maximum int) ([]byte, error) {
	p := m.store.ApplicationLimits()
	limit := p.MaxChangeBytes
	if outcome {
		limit = p.MaxOutcomeBytes
	}
	maximum = min(maximum, p.MaxPageBytes)
	ceiling := 1082 + 3*limit
	if maximum < 0 || q.rows >= q.limits.readRows || ceiling > q.limits.readBytes-q.bytes {
		return nil, errLimit
	}
	if q.arena == nil || 2048+2*maximum > q.arena.Remaining() {
		return nil, errLimit
	}
	q.rows++
	wire, err := q.view.ApplicationRecord(q.ctx, index, outcome, maximum)
	if err != nil {
		q.bytes += ceiling
		return nil, errors.Join(err, reserveComposer(q, 2048+2*maximum))
	}
	q.bytes += 1082 + 3*len(wire)
	if err = reserveComposer(q, 2048+2*cap(wire)); err != nil {
		return nil, err
	}
	if wire == nil {
		return nil, errCorrupt
	}
	return wire, nil
}

func (m *declaredMaterializer) feedNativeFrame(q *reader, index uint64, maximum int, acknowledged uint64) (out retainedFeedFrame, err error) {
	err = m.feedAt(q, index, func(at *reader) error {
		out.partition, out.index = m.ns.partition, index
		copy(out.root[:], at.base.Image)
		cfg, e := m.initialized(at)
		if e != nil {
			return e
		}
		configuration, e := cfg.digest()
		if e != nil {
			return e
		}
		out.round, e = readPartitionRoundFloor(at, configuration)
		if e != nil {
			return e
		}
		out.outcome, e = m.feedRecord(at, index, true, maximum)
		if e != nil {
			return e
		}
		out.changes, e = m.feedRecord(at, index, false, maximum)
		if e != nil {
			return e
		}
		original := uint64(0)
		switch {
		case len(out.outcome) == 0:
			if len(out.changes) != 0 {
				return errCorrupt
			}
		case bytes.HasPrefix(out.outcome, []byte("GCS\x01")):
			s, e := decodeCrossStatus(out.outcome)
			if e != nil || s.ns != m.ns || s.index > index {
				return errors.Join(errCorrupt, e)
			}
			row, found, e := crossControl(at, crossControlKey(m.ns, s.tx, 'R'), m.limits.commandBytes)
			if e != nil || !found || sha256.Sum256(row.Value) != s.digest {
				return errors.Join(errCorrupt, e)
			}
			out.registration = row.Value
			manifest, e := m.crossManifest(at, row.Value)
			if e != nil || manifest.tx != s.tx {
				return errors.Join(errCorrupt, e)
			}
			if s.phase >= crossDecide {
				decision, found, e := crossControl(at, crossControlKey(m.ns, s.tx, 'D'), 256)
				if e != nil {
					return e
				}
				if found {
					out.decision = decision.Value
				}
			}
			if s.index < index {
				original = s.index
			}
		case moveStatusWireFor(m.SemanticContractID(), out.outcome):
			s, e := decodeRangeMoveStatus(out.outcome)
			if e != nil || s.ns != m.ns || s.index > index {
				return errors.Join(errCorrupt, e)
			}
			row, found, e := crossControl(at, rangeMoveControlKey(m.ns, s.move, 'I'), 1024)
			if e != nil {
				return e
			}
			if found {
				out.intent = row.Value
			}
			if s.phase >= rangeMoveDecide && s.phase != rangeMoveAbort {
				row, found, e = crossControl(at, rangeMoveControlKey(m.ns, s.move, 'D'), 256)
				if e != nil {
					return e
				}
				if found {
					out.decision = row.Value
				}
			}
			if s.index < index {
				original = s.index
			}
		case bytes.HasPrefix(out.outcome, []byte("GPO\x01")):
			r, e := decodeGraphPinResult(out.outcome)
			if e != nil || r.index > index {
				return errors.Join(errCorrupt, e)
			}
			if r.status != pinPending {
				out.registration, e = feedMetadataOriginal(at, r.index, out.outcome, m.SemanticContractID())
				if e != nil {
					return e
				}
			} else if r.index != index || len(out.changes) != 0 {
				return errCorrupt
			}
			if r.index < index {
				original = r.index
			}
		case bytes.HasPrefix(out.outcome, []byte("GGO\x01")):
			r, e := decodeGraphRetentionResult(out.outcome)
			if e != nil || r.index > index || len(out.changes) != 0 {
				return errors.Join(errCorrupt, e)
			}
			out.registration, e = feedMetadataOriginal(at, r.index, out.outcome, m.SemanticContractID())
			if e != nil {
				return e
			}
			if r.index < index {
				original = r.index
			}
		case retainedFeedOutcomeFor(m.SemanticContractID(), out.outcome):
			r, e := decodeRetainedFeedResult(out.outcome)
			if e != nil || r.index > index || len(out.changes) != 0 {
				return errors.Join(errCorrupt, e)
			}
			if r.status != feedPending {
				out.registration, e = feedMetadataOriginal(at, r.index, out.outcome, m.SemanticContractID())
				if e != nil {
					return e
				}
			} else if r.index != index {
				return errCorrupt
			}
			if r.index < index {
				original = r.index
			}
		case bytes.HasPrefix(out.outcome, []byte("GSO\x01")):
			r, e := decodeGraphCutResult(out.outcome)
			if e != nil || r.index != index || len(out.changes) != 0 {
				return errors.Join(errCorrupt, e)
			}
		default:
			o, e := decodePartitionMappedOutcome(out.outcome, m.ns)
			if e != nil || o.index > index {
				return errors.Join(errCorrupt, e)
			}
			if o.index < index {
				original = o.index
			}
		}
		if original != 0 {
			if len(out.changes) != 0 {
				return errCorrupt
			}
			// An original below the authenticated acknowledged cursor (initially
			// C's feed start) can legitimately have lost its raw envelopes. Its
			// retained immutable control still authenticates this new replay;
			// this flag never means that the original business CDC was empty.
			if original < acknowledged {
				out.originalBeforeCut = true
				switch {
				case bytes.HasPrefix(out.outcome, []byte("GCS\x01")):
					s, e := decodeCrossStatus(out.outcome)
					if e != nil {
						return e
					}
					actual, found, e := crossCurrent(at, s.tx)
					wire, e2 := encodeCrossStatus(actual)
					if e != nil || e2 != nil || !found || !bytes.Equal(wire, out.outcome) {
						return errors.Join(errCorrupt, e, e2)
					}
					out.originalOutcome = wire
				case moveStatusWireFor(m.SemanticContractID(), out.outcome):
					s, e := decodeRangeMoveStatus(out.outcome)
					if e != nil {
						return e
					}
					if rangeMovePoolsEnabled(m.SemanticContractID()) {
						original, e := readPooledMoveOriginal(at, m.scope(), s)
						if e != nil {
							return e
						}
						if e := reserveComposer(at, 1024+4*(len(original.Key)+len(original.Value)+len(out.outcome))); e != nil {
							return e
						}
						out.registration, e = boundedEncoding(maximum, func(w *boundedWriter) {
							w.add([]byte("NCO1"))
							w.field(original.Key)
							w.u64(original.Index)
							w.field(original.Value)
						})
						if e != nil {
							return e
						}
						out.originalOutcome = owned(out.outcome)
					} else {
						actual, found, e := readRangeMoveStatus(at, s.move)
						wire, e2 := encodeRangeMoveStatus(actual)
						if e != nil || e2 != nil || !found || !bytes.Equal(wire, out.outcome) {
							return errors.Join(errCorrupt, e, e2)
						}
						out.originalOutcome = wire
					}
				case bytes.HasPrefix(out.outcome, []byte("GPO\x01")), bytes.HasPrefix(out.outcome, []byte("GGO\x01")), retainedFeedOutcomeFor(m.SemanticContractID(), out.outcome):
					// feedMetadataOriginal already authenticated the immutable native
					// locator, complete original result and actual first-install index.
					if len(out.registration) == 0 {
						return errCorrupt
					}
					out.originalOutcome = owned(out.outcome)
				default:
					o, e := decodePartitionMappedOutcome(out.outcome, m.ns)
					if e != nil {
						return e
					}
					wire, found, e := at.getBounded(declaredRequestKey(m.ns, o.kind, o.identity), graphOutcomeBytes)
					if e != nil || !found {
						return errors.Join(errCorrupt, e)
					}
					out.originalOutcome = wire
				}
			} else {
				out.originalOutcome, e = m.feedRecord(at, original, true, maximum)
				if e != nil {
					return e
				}
				out.originalChanges, e = m.feedRecord(at, original, false, maximum)
				if e != nil {
					return e
				}
			}
			// Partition requestReplay changes only the disposition. Cross/move and
			// metadata replays retain the exact earlier outcome bytes.
			if o, e := decodePartitionMappedOutcome(out.outcome, m.ns); e == nil {
				prior, e := decodePartitionMappedOutcome(out.originalOutcome, m.ns)
				if e != nil || o.identity != prior.identity || o.hash != prior.hash || o.kind != prior.kind || o.index != prior.index || o.reason != prior.reason || prior.disposition != applied || o.disposition != requestReplay {
					return errors.Join(errCorrupt, e)
				}
			} else if !bytes.Equal(out.outcome, out.originalOutcome) {
				return errCorrupt
			}
		}
		if len(out.changes) == 0 {
			if err := reserveComposer(at, 2048); err != nil {
				return err
			}
			if err := chargeGraphWork(at, graphstore.PageWork{Records: 1, Bytes: 2048}); err != nil {
				return err
			}
			previous, e := at.view.RootAt(at.ctx, index-1, 172)
			if e != nil {
				return e
			}
			if !bytes.Equal(previous.Image, out.root[:]) {
				return errCorrupt
			}
			return nil
		}
		head, e := m.feedEnvelope(out, at.arena)
		if e != nil {
			return e
		}
		if head.kind != 0 {
			mapped, found, e := at.getBounded(partitionRoundMapKey(m.ns, head.round), 256)
			if e != nil || !found {
				return errors.Join(errCorrupt, e)
			}
			body, e := graphBody(mapped, "PLM\x01", 256)
			if e != nil || len(body) != 220 || binary.BigEndian.Uint64(body[32:40]) != head.round || binary.BigEndian.Uint64(body[40:48]) != index || !bytes.Equal(body[48:], out.root[:]) {
				return errors.Join(errCorrupt, e)
			}
		}
		return nil
	})
	if err != nil {
		return retainedFeedFrame{}, err
	}
	return out, nil
}

func feedNativeFrameEqual(a, b retainedFeedFrame) bool {
	if a.partition != b.partition || a.index != b.index || a.round != b.round || a.root != b.root || a.originalBeforeCut != b.originalBeforeCut || a.progressNext != b.progressNext || a.census != b.census {
		return false
	}
	return bytes.Equal(a.outcome, b.outcome) && bytes.Equal(a.changes, b.changes) && bytes.Equal(a.registration, b.registration) && bytes.Equal(a.intent, b.intent) && bytes.Equal(a.decision, b.decision) && bytes.Equal(a.originalOutcome, b.originalOutcome) && bytes.Equal(a.originalChanges, b.originalChanges)
}

// A census is bounded by BOTH the certified boundary and a fixed native-record
// ceiling. Every covered frame is canonically decoded and its exact original
// authenticated before hashing. It never crosses the first business envelope.
// Keeping just the first frame and rolling hash avoids copying a history-sized
// metadata batch into the durable page. Source and Stage use this same verifier.
func (m *declaredMaterializer) feedProgress(q *reader, index, through uint64, maximum int, acknowledged uint64) (retainedFeedFrame, error) {
	if index == 0 || through < index || through-index >= retainedFeedProgressBatch {
		return retainedFeedFrame{}, errInvalid
	}
	var first retainedFeedFrame
	var digest [32]byte
	for at := index; at <= through; at++ {
		f, err := m.feedNativeFrame(q, at, maximum, acknowledged)
		if err != nil {
			return retainedFeedFrame{}, err
		}
		head, err := m.feedEnvelope(f, q.arena)
		if err != nil {
			return retainedFeedFrame{}, err
		}
		if at == index {
			first = f
		}
		if head.kind != 0 {
			break
		}
		// This serial operation protects peak scratch BEFORE encoding, then
		// charges actual delivered ownership. A small protocol record must not be
		// charged as a maximum-size business envelope on every census step.
		if 1024+4*maximum > q.arena.Remaining() {
			return retainedFeedFrame{}, errLimit
		}
		wire, err := boundedEncoding(maximum, func(w *boundedWriter) { emitFeedFrame(w, f) })
		if err != nil {
			return retainedFeedFrame{}, errors.Join(err, reserveComposer(q, 1024+4*maximum))
		}
		if err = reserveComposer(q, 1024+4*cap(wire)); err != nil {
			return retainedFeedFrame{}, err
		}
		h := sha256.New()
		_, _ = h.Write([]byte("rho-tkg:feed-native-progress:v1\x00"))
		_, _ = h.Write(digest[:])
		_, _ = h.Write(wire)
		h.Sum(digest[:0])
		first.progressNext = at + 1
		first.census = digest
	}
	return first, nil
}

// The exact installed native control is the original authority, including after
// its old source data was collected. Do not re-read expired roots on replay.
func feedActionOriginal(q *reader, c retainedFeedCommand, wire []byte) (retainedFeedResult, bool, error) {
	r, found, err := crossControl(q, retainedFeedOriginalKey(q.ns, c.issuer, c.action), 512)
	if err != nil || !found {
		return retainedFeedResult{}, found, err
	}
	body, err := graphBody(r.Value, "GFX\x01", 512)
	hash := sha256.Sum256(wire)
	if err != nil || len(body) < 32 || !bytes.Equal(body[:32], hash[:]) {
		return retainedFeedResult{}, false, errors.Join(errInvalid, err)
	}
	out, err := decodeRetainedFeedResult(body[32:])
	if err != nil || c.original == ([32]byte{}) || out.original != c.original || out.command != hash || out.action != c.action || out.phase != c.phase || out.index != r.Index || out.index > q.base.Index {
		return retainedFeedResult{}, false, errors.Join(errCorrupt, err)
	}
	return out, true, nil
}

func (m *declaredMaterializer) feedLive(q *reader, d retainedFeedDefinition) (graphPinManifest, graphPinOwner, byte, error) {
	manifest, err := decodeGraphPinManifest(d.manifest)
	if err != nil {
		return manifest, graphPinOwner{}, 0, err
	}
	if err = m.bindPinManifest(manifest); err != nil {
		return manifest, graphPinOwner{}, 0, err
	}
	c := graphPinCommand{scope: m.scope(), issuer: manifest.issuer, id: manifest.id(), digest: sha256.Sum256(d.manifest)}
	owner, actual, found, err := m.pinKnownOwner(q, c)
	if err != nil {
		return manifest, owner, 0, err
	}
	if !found {
		return manifest, owner, feedPending, nil
	}
	if owner.terminal == pinExpire {
		return actual, owner, feedExpired, nil
	}
	if owner.terminal != 0 {
		return actual, owner, feedReleased, nil
	}
	head, err := readPinHead(q)
	if err != nil {
		return actual, owner, 0, err
	}
	if head.epoch >= actual.expires {
		return actual, owner, feedExpired, nil
	}
	if err = m.checkRetainedPinRoster(q, false); err != nil {
		return actual, owner, 0, err
	}
	if err = m.checkPinRetainedSource(q, actual, owner, head); err != nil {
		return actual, owner, 0, err
	}
	return actual, owner, feedApplied, nil
}

func feedReader(ctx context.Context, m *declaredMaterializer, view *raftlog.ApplicationView, base raftlog.ApplicationRoot, arena *graphstate.OutputBudget) reader {
	l := m.limits.allocation
	l.readRows = min(l.readRows, m.limits.sourceRows)
	l.readBytes = min(l.readBytes, m.limits.sourceBytes)
	return reader{ctx: ctx, view: view, base: base, ns: m.ns, arena: arena, limits: l, bytes: cap(base.Image)}
}

// The locator is authored only by fresh-profile Stage alongside its immutable
// original. Its index is an installed coordinate, never a caller-selected key.
func retainedFeedMetadataKey(n namespace, index uint64) []byte {
	return binary.BigEndian.AppendUint64(recordKey(n, 37), index)
}
func feedMetadataLocator(key []byte, command [32]byte) ([]byte, error) {
	if len(key) == 0 || len(key) > 512 || command == ([32]byte{}) {
		return nil, errInvalid
	}
	return boundedEncoding(1024, func(w *boundedWriter) { w.add([]byte("GFM\x01")); w.field(key); w.add(command[:]) })
}
func feedMetadataOriginal(q *reader, index uint64, outcome []byte, semantic raftlog.ApplicationSemanticContractID) ([]byte, error) {
	locator, found, err := crossControl(q, retainedFeedMetadataKey(q.ns, index), 1024)
	if err != nil || !found || locator.Index != index {
		return nil, errors.Join(errCorrupt, err)
	}
	body, err := graphBody(locator.Value, "GFM\x01", 1024)
	if err != nil {
		return nil, err
	}
	c := graphCursor{b: body}
	key := c.field(512)
	var hash [32]byte
	copy(hash[:], c.take(32))
	canonical, err := feedMetadataLocator(key, hash)
	if c.err != nil || len(c.b) != 0 || err != nil || !bytes.Equal(canonical, locator.Value) {
		return nil, errors.Join(errCorrupt, c.err, err)
	}
	row, found, err := crossControl(q, key, retainedFeedCommandBytes+512)
	if err != nil || !found || row.Index != index {
		return nil, errors.Join(errCorrupt, err)
	}
	switch {
	case bytes.HasPrefix(outcome, []byte("GPO\x01")):
		prefix := recordKey(q.ns, 24)
		b, e := graphBody(row.Value, "GPX\x01", 512)
		r, e2 := decodeGraphPinResult(outcome)
		if e != nil || e2 != nil || len(key) != len(prefix)+32 || !bytes.HasPrefix(key, prefix) || !bytes.Equal(key[len(prefix)+16:], r.action[:]) || len(b) < 32 || !bytes.Equal(b[:32], hash[:]) || !bytes.Equal(b[32:], outcome) {
			return nil, errors.Join(errCorrupt, e, e2)
		}
	case bytes.HasPrefix(outcome, []byte("GGO\x01")):
		command, result, wire, found, e := graphRetentionRecord(q, key)
		encoded, e2 := encodeGraphRetentionResult(result)
		if e != nil || e2 != nil || !found || command.scope.partition != q.ns.partition || sha256.Sum256(wire) != hash || !bytes.Equal(key, graphRetentionOriginalKey(q.ns, command.issuer, command.request)) || !bytes.Equal(encoded, outcome) {
			return nil, errors.Join(errCorrupt, e, e2)
		}
	case retainedFeedOutcomeFor(semantic, outcome):
		prefix := recordKey(q.ns, 35)
		b, e := graphBody(row.Value, "GFX\x01", 512)
		r, e2 := decodeRetainedFeedResult(outcome)
		if e != nil || e2 != nil || len(key) != len(prefix)+32 || !bytes.HasPrefix(key, prefix) || !bytes.Equal(key[len(prefix)+16:], r.action[:]) || len(b) < 32 || !bytes.Equal(b[:32], hash[:]) || !bytes.Equal(b[32:], outcome) {
			return nil, errors.Join(errCorrupt, e, e2)
		}
	default:
		return nil, errInvalid
	}
	return locator.Value, nil
}

func readFeedCursor(q *reader, id [32]byte) (retainedFeedCursor, bool, error) {
	wire, found, err := q.getBounded(retainedFeedCursorKey(q.ns, id), graphPinManifestBytes+2048)
	if err != nil || !found {
		return retainedFeedCursor{}, found, err
	}
	c, err := decodeRetainedFeedCursor(wire)
	if err != nil || c.original != id {
		return retainedFeedCursor{}, false, errors.Join(errCorrupt, err)
	}
	return c, true, nil
}

func (m *declaredMaterializer) checkFeedPayload(q *reader, manifest graphPinManifest, p retainedFeedPayload, round uint64, configuration [32]byte) error {
	if p.partition != m.ns.partition {
		return errInvalid
	}
	_, index, image, err := m.feedPayloadSource(q, manifest, configuration, round)
	if err != nil {
		return err
	}
	if index != p.index || image != p.root {
		return errInvalid
	}
	return m.feedAt(q, index, func(at *reader) error {
		for _, proof := range p.lookups {
			if len(proof.key) == 0 || proof.rows < 1 || proof.bytes < 1 || proof.rows > at.view.ReadLimits().Rows || proof.bytes > at.view.ReadLimits().Bytes {
				return errInvalid
			}
			if proof.rows > at.limits.readRows-at.rows || proof.bytes > at.limits.readBytes-at.bytes {
				return errLimit
			}
			if 2048+4*proof.bytes > at.arena.Remaining() {
				return errLimit
			}
			if err := reserveComposer(at, 1024+4*(len(proof.key)+len(proof.upper)+len(proof.after))); err != nil {
				return err
			}
			switch proof.kind {
			case 1:
				if proof.rows != 1 || len(proof.upper)+len(proof.after)+len(proof.next) != 0 || proof.complete || len(proof.values) > 1 || proof.found != (len(proof.values) == 1) {
					return errInvalid
				}
				row, found, err := at.view.Get(at.ctx, proof.key, proof.bytes)
				at.rows++
				if err != nil {
					at.bytes += proof.bytes
					return err
				}
				at.bytes += len(proof.key) + len(row.Key) + len(row.Value) + 128
				if err = reserveComposer(at, 256+4*(cap(row.Key)+cap(row.Value))); err != nil {
					return err
				}
				if found != proof.found || found && (!bytes.Equal(row.Key, proof.values[0].Key) || !bytes.Equal(row.Value, proof.values[0].Value)) {
					return errInvalid
				}
			case 2:
				if proof.found {
					return errInvalid
				}
				page, err := at.view.Scan(at.ctx, proof.key, proof.upper, proof.after, raftlog.ReadBudget{Rows: proof.rows, Bytes: proof.bytes})
				at.rows += page.Visited
				at.bytes += page.Bytes
				if err != nil {
					return err
				}
				if err = reserveComposer(at, 256+4*page.Bytes); err != nil {
					return err
				}
				if page.Complete != proof.complete || page.Visited != proof.visited || page.Bytes != proof.workBytes || !bytes.Equal(page.Next, proof.next) || len(page.Rows) != len(proof.values) {
					return errInvalid
				}
				for i, row := range page.Rows {
					if !bytes.Equal(row.Key, proof.values[i].Key) || !bytes.Equal(row.Value, proof.values[i].Value) {
						return errInvalid
					}
				}
			default:
				return errInvalid
			}
		}
		return nil
	})
}

func (m *declaredMaterializer) checkFeedPage(q *reader, d retainedFeedDefinition, manifest graphPinManifest, cursor retainedFeedCursor, p retainedFeedPage) error {
	if err := m.checkFeedPageShape(p, q.arena); err != nil {
		return err
	}
	if p.original != manifest.id() || p.sequence != cursor.sequence+1 || p.previous != cursor.acknowledged || !feedVectorEqual(p.from, cursor.positions) || !feedCutEqual(p.priorCut, cursor.completed) || len(p.next) != len(manifest.parts) || len(p.boundary.certificates) != len(manifest.parts) || len(p.tips) != len(manifest.parts) || len(p.future) != len(manifest.parts) {
		return errInvalid
	}
	local := -1
	done := true
	for _, frame := range p.frames {
		position := -1
		for i, part := range manifest.parts {
			if part.certificate.scope.partition == frame.partition {
				position = i
			}
		}
		end := frame.index + 1
		if frame.progressNext != 0 {
			end = frame.progressNext
		}
		if position < 0 || frame.index != p.from[position] || p.next[position] != end {
			return errInvalid
		}
	}
	for i, part := range manifest.parts {
		c := p.boundary.certificates[i]
		if c.scope != part.certificate.scope || c.configuration != part.certificate.configuration || c.round < cursor.completed.certificates[i].round || p.next[i] < p.from[i] || p.next[i]-p.from[i] > retainedFeedProgressBatch || p.tips[i].index < c.index || p.next[i] > max(p.from[i], p.tips[i].index+1) {
			return errInvalid
		}
		if p.next[i] <= c.sourceIndex || p.next[i] <= p.tips[i].index && !p.future[i] {
			done = false
		}
		hasFrame := false
		for _, f := range p.frames {
			if f.partition == c.scope.partition {
				hasFrame = true
			}
		}
		if hasFrame != (p.next[i] > p.from[i]) {
			return errInvalid
		}
		if c.scope.partition == m.ns.partition {
			local = i
		}
	}
	if local < 0 || p.eof != done {
		return errInvalid
	}
	cert := p.boundary.certificates[local]
	stored, found, err := q.getBounded(graphCertificateKey(m.ns, cert.round), 512)
	if err != nil || !found {
		return errors.Join(replica.ErrUnavailable, err)
	}
	actual, err := decodeGraphCertificate(stored)
	if err != nil || actual != cert {
		return errors.Join(errInvalid, err)
	}
	if err = m.checkFeedTip(q, p.tips[local], cert); err != nil {
		return err
	}
	if p.future[local] {
		if p.next[local] > p.tips[local].index {
			return errInvalid
		}
		f, e := m.feedNativeFrame(q, p.next[local], d.policy.pageBytes, cursor.positions[local])
		if e != nil {
			return e
		}
		head, e := m.feedEnvelope(f, q.arena)
		if e != nil {
			return e
		}
		if head.kind == 0 || head.round <= cert.round {
			return errInvalid
		}
	}
	_, retained, err := m.feedRetention(q, manifest)
	if err != nil {
		return err
	}
	if !retained {
		if err = m.checkPinSource(q, graphPinPart{certificate: cert, feedStart: p.from[local]}, graphPinHead{}); err != nil {
			return err
		}
	}
	var frame *retainedFeedFrame
	for i := range p.frames {
		if p.frames[i].partition == m.ns.partition {
			frame = &p.frames[i]
		}
	}
	if (frame != nil) != (p.next[local] > p.from[local]) {
		return errInvalid
	}
	if frame != nil {
		if frame.index != p.from[local] {
			return errInvalid
		}
		through := frame.index
		if frame.progressNext != 0 {
			through = frame.progressNext - 1
		}
		actual, err := m.feedProgress(q, frame.index, through, d.policy.pageBytes, cursor.positions[local])
		if err != nil {
			return err
		}
		if !feedNativeFrameEqual(actual, *frame) {
			return errInvalid
		}
		head, err := m.feedEnvelope(actual, q.arena)
		if err != nil {
			return err
		}
		if head.kind == 0 {
			if p.kind != 0 {
				return errInvalid
			}
		} else if p.kind != head.kind || p.tx != head.tx || p.round != head.round {
			return errInvalid
		}
	}
	if p.kind == 0 {
		if len(p.payloads) != 0 {
			return errInvalid
		}
		return nil
	}
	if p.round <= manifest.parts[0].certificate.round || p.round > cert.round || len(p.payloads) != len(manifest.parts) {
		return errInvalid
	}
	if p.payloads[local].partition != m.ns.partition {
		return errInvalid
	}
	return m.checkFeedPayload(q, manifest, p.payloads[local], p.round, cert.configuration)
}

func (m *declaredMaterializer) checkFeedTip(q *reader, tip retainedFeedTip, cert graphCertificate) error {
	if q == nil || q.preparedHistory != nil || tip.index < cert.index || tip.index > q.base.Index || tip.index == 0 {
		return errInvalid
	}
	if err := reserveComposer(q, 2048); err != nil {
		return err
	}
	if err := chargeGraphWork(q, graphstore.PageWork{Records: 1, Bytes: 1082 + 4*m.store.ApplicationLimits().MaxImageBytes}); err != nil {
		return err
	}
	root, err := q.view.RootAt(q.ctx, tip.index, 172)
	if err != nil {
		return err
	}
	if !bytes.Equal(root.Image, tip.root[:]) {
		return errInvalid
	}
	return nil
}

func (m *declaredMaterializer) stageRetainedFeed(q *reader, entry replica.Entry, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	if !retainedFeedAgreement(m.SemanticContractID()) {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	if err := joinGraphOutputBudget(q, graphRequest{}, 8192+16*len(entry.Data), m.limits); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	c, err := decodeRetainedFeedCommand(entry.Data)
	if err != nil || c.scope != m.scope() {
		return raftlog.ApplicationBatch{}, errors.Join(errInvalid, err)
	}
	if original, found, err := feedActionOriginal(q, c, entry.Data); err != nil {
		return raftlog.ApplicationBatch{}, err
	} else if found {
		b := batchAt(q.base)
		b.Outcome, err = encodeRetainedFeedResult(original)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		return b, m.preflight(b, budget)
	}
	d, err := decodeRetainedFeedDefinition(c.definition)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if !d.policy.fits(m.limits) || d.policy.pageBytes > m.store.ApplicationLimits().MaxValueBytes {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	manifest, owner, status, err := m.feedLive(q, d)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	result := retainedFeedResult{original: manifest.id(), command: sha256.Sum256(entry.Data), action: c.action, index: entry.Index, phase: c.phase, status: status}
	puts := make([]raftlog.ApplicationControlPut, 0, 5)
	finish := func(remember bool) (raftlog.ApplicationBatch, error) {
		wire, e := encodeRetainedFeedResult(result)
		if e != nil {
			return raftlog.ApplicationBatch{}, e
		}
		if remember {
			if e = reserveComposer(q, 8192+8*len(wire)); e != nil {
				return raftlog.ApplicationBatch{}, e
			}
			key := retainedFeedOriginalKey(m.ns, c.issuer, c.action)
			record, e := boundedEncoding(512, func(w *boundedWriter) { w.add([]byte("GFX\x01")); w.add(result.command[:]); w.add(wire) })
			if e != nil {
				return raftlog.ApplicationBatch{}, e
			}
			locator, e := feedMetadataLocator(key, result.command)
			if e != nil {
				return raftlog.ApplicationBatch{}, e
			}
			puts = append(puts, raftlog.ApplicationControlPut{Key: key, Value: record}, raftlog.ApplicationControlPut{Key: retainedFeedMetadataKey(m.ns, entry.Index), Value: locator})
		}
		b := batchAt(q.base)
		b.Outcome, b.ControlPuts, b.Writes = wire, puts, q.writes
		if e = sortPartitionWrites(b.Writes); e != nil {
			return raftlog.ApplicationBatch{}, e
		}
		slices.SortFunc(b.ControlPuts, func(a, b raftlog.ApplicationControlPut) int { return bytes.Compare(a.Key, b.Key) })
		return b, m.preflight(b, budget)
	}
	if status != feedApplied {
		return finish(status != feedPending)
	}
	cursor, found, err := readFeedCursor(q, manifest.id())
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if c.phase == feedInitialize {
		if found {
			prior, present, e := crossControl(q, retainedFeedDefinitionKey(m.ns, manifest.id()), graphPinManifestBytes+128)
			if e != nil || !present || !bytes.Equal(prior.Value, c.definition) {
				return raftlog.ApplicationBatch{}, errors.Join(errInvalid, e)
			}
			result.sequence = cursor.sequence
			return finish(true)
		}
		if err = reserveComposer(q, 4096+4*len(c.definition)+1024*len(manifest.parts)); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		cursor = retainedFeedCursor{original: manifest.id(), positions: make([]uint64, len(manifest.parts)), completed: graphCut{certificates: make([]graphCertificate, len(manifest.parts))}}
		for i, p := range manifest.parts {
			cursor.positions[i] = p.certificate.sourceIndex + 1
			cursor.completed.certificates[i] = p.certificate
		}
		puts = append(puts, raftlog.ApplicationControlPut{Key: retainedFeedDefinitionKey(m.ns, manifest.id()), Value: owned(c.definition)})
	} else {
		if !found {
			result.status = feedPending
			return finish(false)
		}
		prior, present, e := crossControl(q, retainedFeedDefinitionKey(m.ns, manifest.id()), graphPinManifestBytes+128)
		if e != nil || !present || !bytes.Equal(prior.Value, c.definition) {
			return raftlog.ApplicationBatch{}, errors.Join(errInvalid, e)
		}
		if c.phase == feedAdmitOperation {
			if cursor.admitted >= d.policy.maxAttempts {
				result.status = feedLimited
				return finish(true)
			}
			cursor.admitted++
			cursor.spentRows += uint64(d.policy.operationRows)
			cursor.spentBytes += uint64(d.policy.operationBytes)
			cursor.spentOutput += uint64(d.policy.operationOutput)
			cursor.spentCalls += 1024
			result.sequence = cursor.admitted
			encoded, e := encodeRetainedFeedCursor(cursor)
			if e != nil {
				return raftlog.ApplicationBatch{}, e
			}
			if e = q.put(retainedFeedCursorKey(m.ns, manifest.id()), encoded); e != nil {
				return raftlog.ApplicationBatch{}, e
			}
			return finish(true)
		}
		if c.phase == feedRetainOnly {
			prior, retained, e := m.feedRetention(q, manifest)
			if e != nil {
				return raftlog.ApplicationBatch{}, e
			}
			if retained {
				result.status = feedMismatch // the SAME action returned above
				return finish(true)
			}
			if cursor.pending != ([32]byte{}) {
				result.status = feedPending
				return finish(false)
			}
			prior.action, prior.installed = c.action, entry.Index
			anchor, e := m.feedAnchor(q, manifest, cursor.positions, prior)
			if e != nil {
				return raftlog.ApplicationBatch{}, e
			}
			encoded, e := encodeFeedRetention(anchor)
			if e != nil {
				return raftlog.ApplicationBatch{}, e
			}
			if e = q.put(retainedFeedRetentionKey(m.ns, manifest.id()), encoded); e != nil {
				return raftlog.ApplicationBatch{}, e
			}
			result.sequence = cursor.sequence
			return finish(true)
		}
		p, e := decodeRetainedFeedPage(c.page, d.policy.pageBytes, q.arena)
		if e != nil {
			return raftlog.ApplicationBatch{}, e
		}
		result.page = sha256.Sum256(c.page)
		result.sequence = p.sequence
		action := p.request
		if c.phase == feedAcknowledge {
			action = retainedFeedPhaseAction(p.request, feedAcknowledge)
		}
		if action != c.action || p.original != manifest.id() {
			return raftlog.ApplicationBatch{}, errInvalid
		}
		if c.phase == feedSealPage {
			if cursor.pending != ([32]byte{}) || cursor.sequence >= d.policy.maxPages {
				result.status = feedPending
				return finish(false)
			}
			if p.sequence != cursor.sequence+1 || p.previous != cursor.acknowledged || !feedVectorEqual(p.from, cursor.positions) {
				result.status = feedMismatch
				return finish(true)
			}
			if err = m.checkFeedPage(q, d, manifest, cursor, p); err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			cursor.sequence = p.sequence
			cursor.pending = result.page
			if err = reserveComposer(q, 2048+4*len(c.page)); err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			puts = append(puts, raftlog.ApplicationControlPut{Key: retainedFeedPageKey(m.ns, manifest.id(), result.page), Value: owned(c.page)})
		} else {
			// ACK's action is phase-qualified on the native original key. Its page
			// request remains unchanged, preventing a changed vector on retry.
			if cursor.pending != result.page || cursor.sequence != p.sequence || !feedVectorEqual(cursor.positions, p.from) {
				result.status = feedMismatch
				return finish(true)
			}
			stored, present, e := crossControl(q, retainedFeedPageKey(m.ns, manifest.id(), result.page), d.policy.pageBytes)
			if e != nil || !present || !bytes.Equal(stored.Value, c.page) {
				return raftlog.ApplicationBatch{}, errors.Join(errInvalid, e)
			}
			local := -1
			for i, part := range manifest.parts {
				if part.certificate.scope.partition == m.ns.partition {
					local = i
				}
			}
			if local < 0 {
				return raftlog.ApplicationBatch{}, errInvalid
			}
			if owner.feedStart < p.next[local] {
				result.status = feedPending
				return finish(false)
			}
			retention, retained, e := m.feedRetention(q, manifest)
			if e != nil {
				return raftlog.ApplicationBatch{}, e
			}
			if retained {
				retention, e = m.feedAnchor(q, manifest, p.next, retention)
				if e != nil {
					return raftlog.ApplicationBatch{}, e
				}
				encoded, e := encodeFeedRetention(retention)
				if e != nil {
					return raftlog.ApplicationBatch{}, e
				}
				if e = q.put(retainedFeedRetentionKey(m.ns, manifest.id()), encoded); e != nil {
					return raftlog.ApplicationBatch{}, e
				}
			}
			cursor.positions = p.next
			cursor.pending = [32]byte{}
			cursor.acknowledged = result.page
			if p.eof {
				cursor.completed = p.boundary
			}
		}
	}
	wire, err := encodeRetainedFeedCursor(cursor)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if err = q.put(retainedFeedCursorKey(m.ns, manifest.id()), wire); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	return finish(true)
}

func (h *allocationHost) preflightRetainedFeed(c retainedFeedCommand, wire []byte) (err error) {
	if !retainedFeedAgreement(h.machine.SemanticContractID()) || c.scope != h.machine.scope() {
		return errInvalid
	}
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, view.Close()) }()
	base, err := view.RootBounded(context.Background(), 172)
	if err != nil {
		return err
	}
	q := feedReader(context.Background(), h.machine, view, base, nil)
	if err = joinGraphOutputBudget(&q, graphRequest{}, 8192+16*len(wire), h.machine.limits); err != nil {
		return err
	}
	if _, found, e := feedActionOriginal(&q, c, wire); e != nil || found {
		return e
	}
	d, err := decodeRetainedFeedDefinition(c.definition)
	if err != nil {
		return err
	}
	if !d.policy.fits(h.machine.limits) || d.policy.pageBytes > h.machine.store.ApplicationLimits().MaxValueBytes {
		return errLimit
	}
	manifest, owner, status, err := h.machine.feedLive(&q, d)
	if err != nil {
		return err
	}
	if status == feedPending {
		return replica.ErrUnavailable
	}
	if status != feedApplied || c.phase == feedInitialize {
		return nil
	}
	cursor, found, err := readFeedCursor(&q, manifest.id())
	if err != nil {
		return err
	}
	if !found {
		return replica.ErrUnavailable
	}
	if c.phase == feedAdmitOperation {
		if cursor.admitted >= d.policy.maxAttempts {
			return errLimit
		}
		return nil
	}
	if c.phase == feedRetainOnly {
		_, retained, err := h.machine.feedRetention(&q, manifest)
		if err != nil {
			return err
		}
		if retained {
			return errInvalid
		}
		if cursor.pending != ([32]byte{}) {
			return errRetainedFeedPending
		}
		return nil // Stage captures its actual predecessor, never caller data.
	}
	page, err := decodeRetainedFeedPage(c.page, d.policy.pageBytes, q.arena)
	if err != nil {
		return err
	}
	action := page.request
	if c.phase == feedAcknowledge {
		action = retainedFeedPhaseAction(page.request, feedAcknowledge)
	}
	if c.action != action || page.original != manifest.id() {
		return errInvalid
	}
	if c.phase == feedSealPage {
		if cursor.pending != ([32]byte{}) {
			return errors.Join(errRetainedFeedPending, replica.ErrUnavailable)
		}
		return h.machine.checkFeedPage(&q, d, manifest, cursor, page)
	}
	digest := sha256.Sum256(c.page)
	if cursor.pending != digest || cursor.sequence != page.sequence || !feedVectorEqual(cursor.positions, page.from) {
		return errInvalid
	}
	stored, present, err := crossControl(&q, retainedFeedPageKey(q.ns, manifest.id(), digest), d.policy.pageBytes)
	if err != nil || !present || !bytes.Equal(stored.Value, c.page) {
		return errors.Join(errInvalid, err)
	}
	for i, part := range manifest.parts {
		if part.certificate.scope.partition == q.ns.partition && owner.feedStart < page.next[i] {
			return errors.Join(errRetainedFeedPending, replica.ErrUnavailable)
		}
	}
	return nil
}

// Snapshot/reopen validation stays on q.view, including a prepared incoming
// bank. Immutable page originals authenticate previously checked payloads;
// re-reading their old active-bank roots here would be both wrong and needless.
func (m *declaredMaterializer) checkRetainedFeedLedger(q *reader) error {
	if !retainedFeedAgreement(m.SemanticContractID()) {
		return nil
	}
	head, err := readPinHead(q)
	if err != nil {
		return err
	}
	for _, id := range head.owners {
		cursor, found, err := readFeedCursor(q, id)
		if err != nil {
			return err
		}
		if !found {
			continue
		} // an issued/published pin need not be a feed
		definition, present, err := crossControl(q, retainedFeedDefinitionKey(m.ns, id), graphPinManifestBytes+128)
		if err != nil || !present {
			return errors.Join(errCorrupt, err)
		}
		d, err := decodeRetainedFeedDefinition(definition.Value)
		if err != nil {
			return err
		}
		manifest, wire, present, err := readPinOriginal(q, id)
		if err != nil || !present || !bytes.Equal(wire, d.manifest) || cursor.sequence > d.policy.maxPages || cursor.admitted > d.policy.maxAttempts || cursor.spentRows != cursor.admitted*uint64(d.policy.operationRows) || cursor.spentBytes != cursor.admitted*uint64(d.policy.operationBytes) || cursor.spentOutput != cursor.admitted*uint64(d.policy.operationOutput) || cursor.spentCalls != cursor.admitted*1024 {
			return errors.Join(errCorrupt, err)
		}
		if len(cursor.positions) != len(manifest.parts) || len(cursor.completed.certificates) != len(manifest.parts) {
			return errCorrupt
		}
		if _, _, err := m.feedRetention(q, manifest); err != nil {
			return err
		}
		for i, p := range manifest.parts {
			c := cursor.completed.certificates[i]
			if c.scope != p.certificate.scope || c.configuration != p.certificate.configuration || c.round < p.certificate.round || cursor.positions[i] < p.certificate.sourceIndex+1 || cursor.positions[i] < c.sourceIndex+1 {
				return errCorrupt
			}
		}
		pageID := cursor.pending
		if pageID == ([32]byte{}) {
			pageID = cursor.acknowledged
		}
		if pageID == ([32]byte{}) {
			if cursor.sequence != 0 {
				return errCorrupt
			}
			continue
		}
		row, present, err := crossControl(q, retainedFeedPageKey(m.ns, id, pageID), d.policy.pageBytes)
		if err != nil || !present || sha256.Sum256(row.Value) != pageID {
			return errors.Join(errCorrupt, err)
		}
		page, err := decodeRetainedFeedPage(row.Value, d.policy.pageBytes, q.arena)
		if err != nil || page.original != id || page.sequence != cursor.sequence {
			return errors.Join(errCorrupt, err)
		}
		phase, action := feedSealPage, page.request
		if cursor.pending == ([32]byte{}) {
			phase, action = feedAcknowledge, retainedFeedPhaseAction(page.request, feedAcknowledge)
			if !feedVectorEqual(cursor.positions, page.next) {
				return errCorrupt
			}
		} else if !feedVectorEqual(cursor.positions, page.from) {
			return errCorrupt
		}
		command := retainedFeedCommand{scope: m.scope(), issuer: manifest.issuer, action: action, phase: phase, definition: definition.Value, page: row.Value, original: manifest.id()}
		encoded, err := encodeRetainedFeedCommand(command)
		if err != nil {
			return err
		}
		original, present, err := feedActionOriginal(q, command, encoded)
		if err != nil || !present || original.page != pageID || original.sequence != cursor.sequence || original.status != feedApplied {
			return errors.Join(errCorrupt, err)
		}
	}
	return nil
}
