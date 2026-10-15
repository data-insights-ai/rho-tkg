package graphapply

import (
	"bytes"
	"crypto/sha256"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

func rangeMoveRecoveryID(n namespace, move [16]byte) [32]byte {
	return sha256.Sum256(rangeMoveControlKey(n, move, 'R'))
}

func readRangeMoveActive(q *reader) ([16]byte, error) {
	wire, present, err := q.getBounded(rangeMoveActiveKey(q.ns), 16)
	if err != nil || !present || len(wire) == 0 {
		return [16]byte{}, err
	}
	if len(wire) != 16 {
		return [16]byte{}, errCorrupt
	}
	var move [16]byte
	copy(move[:], wire)
	if move == ([16]byte{}) {
		return move, errCorrupt
	}
	return move, nil
}

func readRangeMoveStatus(q *reader, move [16]byte) (rangeMoveStatus, bool, error) {
	wire, present, err := q.getBounded(rangeMoveStatusKey(q.ns, move), 256)
	if err != nil || !present {
		return rangeMoveStatus{}, present, err
	}
	status, err := decodeRangeMoveStatus(wire)
	if err != nil || status.ns != q.ns || status.move != move || status.index > q.base.Index {
		return rangeMoveStatus{}, false, errors.Join(errCorrupt, err)
	}
	return status, true, nil
}

func (m *declaredMaterializer) rangeMoveFenceOwner(q *reader, intent rangeMoveIntent) (bool, error) {
	remaining := raftlog.ReadBudget{Rows: min(q.view.ReadLimits().Rows, q.limits.readRows-q.rows), Bytes: min(q.view.ReadLimits().Bytes, q.limits.readBytes-q.bytes)}
	owner, work, err := q.view.RecoveryOwner(q.ctx, remaining)
	if charged := chargeGraphWork(q, graphstore.PageWork{Records: work.Records, Bytes: work.Bytes}); charged != nil {
		return false, errors.Join(charged, err)
	}
	if err != nil {
		return false, err
	}
	if owner.ID == ([32]byte{}) {
		return false, nil
	}
	wire, err := encodeRangeMoveIntent(intent)
	if err != nil {
		return false, err
	}
	move := rangeMoveID(namespace{intent.source.graph, intent.source.partition}, intent.id)
	// This exact reserve recipe was classified before Stage. It is not yet an
	// applied active record; treating it as a different cross owner is corrupt.
	if owner.ID == rangeMoveRecoveryID(q.ns, move) && owner.Digest == sha256.Sum256(wire) {
		return false, nil
	}
	_, held, err := m.readCrossResourceOwner(q)
	return held, err
}

// Classification and Stage use exactly the same eligibility law. In particular
// a publication/epoch refusal must never classify a protected owner first.
func (m *declaredMaterializer) rangeMoveFenceEligibility(q *reader, intent rangeMoveIntent) (byte, error) {
	active, err := readRangeMoveActive(q)
	if err != nil {
		return 0, err
	}
	if active != ([16]byte{}) {
		return rangeMoveBlocked, nil
	}
	cfg, err := m.initialized(q)
	if err != nil {
		return 0, err
	}
	configuration, err := cfg.digest()
	if err != nil {
		return 0, err
	}
	if configuration != intent.publication.Configuration || intent.source.check(m.declaration, cfg) != nil || intent.target.check(m.declaration, cfg) != nil {
		return rangeMoveMismatch, nil
	}
	held, err := m.rangeMoveFenceOwner(q, intent)
	if err != nil {
		return 0, err
	}
	lock, err := crossLock(q)
	if err != nil {
		return 0, err
	}
	if active != ([16]byte{}) || held || lock != ([16]byte{}) {
		return rangeMoveBlocked, nil
	}
	if m.ns.partition == intent.source.partition {
		p, owner, work, err := graphstore.LookupPublishedRange(q.ctx, q.view, graphstore.Namespace{Graph: intent.publication.Graph, Partition: m.ns.partition}, intent.publication.First, configuration, graphstore.OwnershipBudget{SourceRows: q.limits.readRows - q.rows, SourceBytes: q.limits.readBytes - q.bytes, OutputBytes: q.arena.Remaining()})
		if charged := chargeGraphWork(q, work); charged != nil {
			return 0, errors.Join(charged, err)
		}
		if err != nil {
			return 0, err
		}
		if p != intent.publication || owner.Check(p, m.ns.partition, intent.epoch) != nil {
			return rangeMoveMismatch, nil
		}
	}
	return rangeMoveApplied, nil
}

func (m *declaredMaterializer) moveOwnsRecovery(q *reader, reservation raftlog.ApplicationRecoveryOwner) (bool, error) {
	if !m.rangeMoves() {
		return false, nil
	}
	move, err := readRangeMoveActive(q)
	if err != nil || move == ([16]byte{}) {
		return false, err
	}
	status, present, err := readRangeMoveStatus(q, move)
	if err != nil || !present || rangeMoveTerminal(status) {
		return false, errors.Join(errCorrupt, err)
	}
	if reservation.ID != rangeMoveRecoveryID(q.ns, move) || reservation.Digest != status.intent {
		return false, errCorrupt
	}
	intent, found, err := crossControl(q, rangeMoveControlKey(q.ns, move, 'I'), 1024)
	if err != nil || !found || sha256.Sum256(intent.Value) != status.intent {
		return false, errors.Join(errCorrupt, err)
	}
	r, err := decodeRangeMoveIntent(intent.Value)
	if err != nil || r.source != m.scope() && r.target != m.scope() || rangeMoveID(namespace{r.source.graph, r.source.partition}, r.id) != move {
		return false, errors.Join(errCorrupt, err)
	}
	return true, nil
}

func (m *declaredMaterializer) moveReservation(r rangeMoveIntent) (raftlog.ApplicationRecoveryReservation, error) {
	if rangeMoveSpans(m.SemanticContractID()) {
		return m.spanMoveReservation(r)
	}
	wire, err := encodeRangeMoveIntent(r)
	if err != nil {
		return raftlog.ApplicationRecoveryReservation{}, err
	}
	move := rangeMoveID(namespace{r.source.graph, r.source.partition}, r.id)
	reservation := raftlog.ApplicationRecoveryReservation{ID: rangeMoveRecoveryID(m.ns, move), Digest: sha256.Sum256(wire)}
	add := func(phase uint32, command, bytes, rows int) {
		reservation.Credits = append(reservation.Credits, raftlog.ApplicationRecoveryCredit{Phase: phase, CommandBytes: command, RetainedBytes: uint64(bytes), RetainedRecords: uint64(rows)}) // #nosec G115 -- all call sites use positive fixed credits or validated pageBytes<=1MiB; computed bytes<=18MiB and rows<=128.
	}
	if m.collectsHistory() {
		// Complete <=16-owner capture, separate bounded native controls, and the
		// repeated authentic RS2 envelope spend existing retained/log quotas.
		add(1, 4096, rangeMoveRetentionBytes+(128<<10), 32)
	} else {
		add(1, 4096, 8192, 16)
	} // legacy captured root/floor bytes unchanged
	if m.ns.partition == r.source.partition {
		for sequence := range r.maxPages + 1 {
			retained := 256 << 10
			if m.collectsHistory() {
				retained = 1 << 20
			} // GMP3+both cursors+receipt+RS2
			add(uint32(100+sequence), 8192, retained, 16)
		}
	} else {
		for sequence := range r.maxPages + 1 {
			if sequence == 0 && m.collectsHistory() {
				add(1200, (1<<20)-2048, 2<<20, 128)
			} else {
				retained := (256 << 10) + 16*r.pageBytes + 16384
				if m.collectsHistory() {
					retained += 1 << 20
				}
				add(uint32(1200+sequence), min((1<<20)-2048, r.pageBytes+8192), retained, 128)
			}
		}
	}
	if m.collectsHistory() {
		add(2500, 8192, (1<<20)+(128<<10), 16) // complete GMP3 plus native RS2
		add(2501, 8192, 128<<10, 16)
		add(2502, 8192, (256<<10)+(128<<10), 128)
	} else {
		add(2500, 8192, 256<<10, 16)
		add(2501, 8192, 8192, 16)
		add(2502, 8192, 256<<10, 128)
	}
	if m.ns.partition == r.target.partition {
		for sequence := range r.maxPages {
			add(uint32(3000+sequence), 8192, (256<<10)+16*r.pageBytes+16384, 128)
		}
	}
	// Both success and a pre-COMMIT abort consume the SAME highest final phase.
	// RetireBefore removes unused stage/undo phases on the actual final install.
	terminal := 8192
	if m.collectsHistory() {
		terminal = 128 << 10
	}
	add(5001, 8192, terminal, 16)
	return reservation, nil
}

// Intrinsic feasibility uses immutable store quotas, never this invocation's
// consumed budget or current free space. A missing free quota remains retryable.
func (m *declaredMaterializer) moveReservationFits(r raftlog.ApplicationRecoveryReservation) bool {
	if rangeMoveSpans(m.SemanticContractID()) {
		return m.spanMoveFits(r)
	}
	p, l := m.store.ApplicationLimits(), m.store.Limits()
	if _, _, err := raftlog.ApplicationRecoveryRootOverhead(len(r.Credits)); err != nil {
		return false
	}
	var bytes, records, logBytes uint64
	for _, c := range r.Credits {
		if c.CommandBytes > l.MaxEntryBytes-74 {
			return false
		}
		bytes += c.RetainedBytes
		records += c.RetainedRecords
		logBytes += uint64(c.CommandBytes + 83) // #nosec G115 -- native credit command sizes are positive and validated below the fixed entry-byte cap before conversion.
	}
	return bytes <= p.RetainedApplicationBytes && records <= p.RetainedApplicationRecords && logBytes <= l.MaxRetainedBytes && uint64(len(r.Credits)) <= l.MaxRetainedEntries
}

func rangeMoveCommandReplayed(c rangeMoveCommand, current rangeMoveStatus) bool {
	if c.phase != current.phase {
		return false
	}
	// Abort0 closes numbered reverse progress; zero is not an earlier undo page.
	if c.phase == rangeMoveAbort && c.sequence == 0 {
		return current.sequence == 0
	}
	return c.sequence <= current.sequence
}

func rangeMoveCredit(c rangeMoveCommand) uint32 {
	if rangeMoveSpans(c.scope.semantic) {
		return moveSpanCredit(c)
	}
	switch c.phase {
	case rangeMoveCapture:
		return 1
	case rangeMoveDescribe:
		return uint32(100 + c.sequence) // #nosec G115 -- decoded command sequence<=validated maxPages<=1024, so phase<=1124.
	case rangeMoveImport:
		return uint32(1200 + c.sequence) // #nosec G115 -- decoded command sequence<=validated maxPages<=1024, so phase<=2224.
	case rangeMoveSeal:
		return 2500
	case rangeMoveDecide:
		return 2501
	case rangeMoveActivate:
		return 2502
	case rangeMoveAbort:
		if c.sequence != 0 {
			return uint32(3000 + c.sequence - 1) // #nosec G115 -- decoded positive sequence<=validated maxPages<=1024, so phase<=4023.
		}
		return 5001
	case rangeMoveFinish:
		return 5001
	default:
		return 0
	}
}

func (m *declaredMaterializer) rangeMoveRecovery(q *reader, c rangeMoveCommand, r rangeMoveIntent) (raftlog.ApplicationRecoveryPurpose, error) {
	move := rangeMoveID(namespace{r.source.graph, r.source.partition}, r.id)
	current, present, err := readRangeMoveStatus(q, move)
	if err != nil {
		return raftlog.ApplicationRecoveryPurpose{}, err
	}
	if present && (current.intent != sha256.Sum256(c.intent) || current.format != rangeMoveStatusFormat(m.SemanticContractID())) {
		return raftlog.ApplicationRecoveryPurpose{}, errInvalid
	}
	if present && rangeMoveSpans(m.SemanticContractID()) {
		wire, found, e := q.getBounded(rangeMoveProgressKey(m.ns, move), rangeMoveSpanProgressBytes)
		if e != nil {
			return raftlog.ApplicationRecoveryPurpose{}, e
		}
		if found && (bytes.HasPrefix(wire, []byte("GMP\x04")) || bytes.HasPrefix(wire, []byte("GMP\x05"))) {
			p, e := decodeRangeMoveProgress(wire, q.arena)
			if e != nil {
				return raftlog.ApplicationRecoveryPurpose{}, e
			}
			if e = m.checkMoveReference(q, move, p); e != nil {
				return raftlog.ApplicationRecoveryPurpose{}, e
			}
		}
	}
	if c.phase == rangeMoveFence {
		if present {
			return raftlog.ApplicationRecoveryPurpose{}, nil
		}
		if err := m.checkRangeMoveRecoveryBinding(q, c, r); err != nil {
			return raftlog.ApplicationRecoveryPurpose{}, err
		}
		active, err := readRangeMoveActive(q)
		if err != nil {
			return raftlog.ApplicationRecoveryPurpose{}, err
		}
		if active != ([16]byte{}) {
			return m.recoveryAfterRangeMove(q)
		}
		eligibility, err := m.rangeMoveFenceEligibility(q, r)
		if err != nil || eligibility != rangeMoveApplied {
			return raftlog.ApplicationRecoveryPurpose{}, err
		}
		reservation, err := m.moveReservation(r)
		if err == nil && !m.moveReservationFits(reservation) {
			return raftlog.ApplicationRecoveryPurpose{}, nil
		}
		return raftlog.ApplicationRecoveryPurpose{Reserve: &reservation}, err
	}
	if !present {
		if m.unadmittedMoveAbort(c, r) {
			return raftlog.ApplicationRecoveryPurpose{}, nil
		}
	}
	if present && (rangeMoveTerminal(current) || rangeMoveCommandReplayed(c, current)) {
		return raftlog.ApplicationRecoveryPurpose{}, nil
	}
	if err := m.checkRangeMoveRecoveryBinding(q, c, r); err != nil {
		return raftlog.ApplicationRecoveryPurpose{}, err
	}
	phase := rangeMoveCredit(c)
	if phase == 0 {
		return raftlog.ApplicationRecoveryPurpose{}, errInvalid
	}
	wire, err := encodeRangeMoveCommand(c)
	if err != nil {
		return raftlog.ApplicationRecoveryPurpose{}, err
	}
	use := raftlog.ApplicationRecoveryUse{ID: rangeMoveRecoveryID(m.ns, move), Digest: sha256.Sum256(c.intent), CommandDigest: sha256.Sum256(wire), Phase: phase}
	if phase == 5001 || phase == moveSpanTerminal {
		use.RetireBefore = phase
	}
	return raftlog.ApplicationRecoveryPurpose{Use: &use}, nil
}

func (m *declaredMaterializer) stageRangeMove(q *reader, entry replica.Entry, budget raftlog.ApplicationBudget) (batch raftlog.ApplicationBatch, err error) {
	c, err := decodeRangeMoveCommand(entry.Data)
	if err != nil || !m.rangeMoves() || c.scope != m.scope() {
		return raftlog.ApplicationBatch{}, errors.Join(errInvalid, err)
	}
	intent, err := decodeRangeMoveIntent(c.intent)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if err := joinGraphOutputBudget(q, graphRequest{}, 8*len(entry.Data)+4096, m.limits); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	// Classification may recognize prospective Use capacity against an earlier
	// queued Fence. Effects still require its actually applied registration.
	if c.phase != rangeMoveFence {
		_, present, err := readRangeMoveStatus(q, rangeMoveID(namespace{intent.source.graph, intent.source.partition}, intent.id))
		if err != nil || !present && !m.unadmittedMoveAbort(c, intent) {
			return raftlog.ApplicationBatch{}, errors.Join(errInvalid, err)
		}
	}
	purpose, err := m.rangeMoveRecovery(q, c, intent)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	defer func() {
		if err == nil {
			batch.RecoveryReserve, batch.RecoveryUse = purpose.Reserve, purpose.Use
			err = m.preflight(batch, budget)
			if err != nil {
				batch = raftlog.ApplicationBatch{}
			}
		}
	}()
	cfg, err := m.initialized(q)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	configuration, err := cfg.digest()
	if err != nil || configuration != intent.publication.Configuration || intent.source.check(m.declaration, cfg) != nil || intent.target.check(m.declaration, cfg) != nil {
		return raftlog.ApplicationBatch{}, errors.Join(errInvalid, err)
	}
	move := rangeMoveID(namespace{intent.source.graph, intent.source.partition}, intent.id)
	digest := sha256.Sum256(c.intent)
	current, present, err := readRangeMoveStatus(q, move)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	status := rangeMoveStatus{format: rangeMoveStatusFormat(m.SemanticContractID()), ns: m.ns, move: move, intent: digest, phase: c.phase, index: entry.Index, sequence: c.sequence, result: rangeMoveApplied}
	finish := func(next rangeMoveStatus, publish bool, controls []raftlog.ApplicationControlPut) (raftlog.ApplicationBatch, error) {
		wire, err := encodeRangeMoveStatus(next)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if publish {
			if err := q.put(rangeMoveStatusKey(m.ns, move), wire); err != nil {
				return raftlog.ApplicationBatch{}, err
			}
		}
		batch := batchAt(q.base)
		batch.Writes, batch.ControlPuts, batch.Outcome = q.writes, controls, wire
		if err := sortPartitionWrites(batch.Writes); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		return batch, m.preflight(batch, budget)
	}
	if present {
		if rangeMoveTerminal(current) || rangeMoveCommandReplayed(c, current) || c.phase == rangeMoveFence {
			return finish(current, false, nil)
		}
	}
	if c.phase == rangeMoveFence {
		eligibility, err := m.rangeMoveFenceEligibility(q, intent)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if eligibility != rangeMoveApplied {
			status.result = eligibility
			if eligibility == rangeMoveMismatch {
				return finish(status, true, []raftlog.ApplicationControlPut{{Key: rangeMoveControlKey(m.ns, move, 'I'), Value: owned(c.intent)}})
			}
			return finish(status, false, nil)
		}
		reservation, err := m.moveReservation(intent)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if !m.moveReservationFits(reservation) {
			// Persist an exact no-effect terminal refusal without installing an
			// admission fence or claiming a reservation the store cannot fund.
			status.phase, status.result = rangeMoveAbort, rangeMoveLimit
			return finish(status, true, []raftlog.ApplicationControlPut{{Key: rangeMoveControlKey(m.ns, move, 'I'), Value: owned(c.intent)}})
		}
		active, err := readRangeMoveActive(q)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		held, err := m.rangeMoveFenceOwner(q, intent)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		lock, err := crossLock(q)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if active != ([16]byte{}) || held || lock != ([16]byte{}) {
			status.result = rangeMoveBlocked
			return finish(status, false, nil)
		}
		if m.ns.partition == intent.source.partition {
			publication, owner, work, err := graphstore.LookupPublishedRange(q.ctx, q.view, graphstore.Namespace{Graph: graphstate.GraphID(m.ns.graph), Partition: m.ns.partition}, intent.publication.First, configuration, graphstore.OwnershipBudget{SourceRows: q.limits.readRows - q.rows, SourceBytes: q.limits.readBytes - q.bytes, OutputBytes: q.arena.Remaining()})
			if charged := chargeGraphWork(q, work); charged != nil {
				return raftlog.ApplicationBatch{}, errors.Join(charged, err)
			}
			if err != nil || publication != intent.publication || owner.Check(publication, m.ns.partition, intent.epoch) != nil {
				status.result = rangeMoveMismatch
				return finish(status, false, nil)
			}
		} else {
			source, found := rangeMoveFindProof(c.proofs, intent.source.partition)
			if !found || source.intent != digest || source.phase < rangeMoveFence || source.phase > rangeMoveDescribe || source.result != rangeMoveApplied {
				return raftlog.ApplicationBatch{}, errInvalid
			}
			// The original source installed this exact publication-bound fence.
			// Target capture includes its immutable foreign routing provenance;
			// no genesis ownership or global bucket assignment changes.
			n := graphstore.Namespace{Graph: intent.publication.Graph, Partition: m.ns.partition}
			publication, err := graphstore.EncodeRangePublication(intent.publication)
			if err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			ownership, err := graphstore.EncodeRangeOwnership(graphstore.RangeOwnership{Graph: intent.publication.Graph, RangeID: intent.publication.RangeID, Partition: intent.source.partition, Epoch: intent.epoch, Configuration: configuration})
			if err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			rows := []raftlog.KV{{Key: graphstore.RangePublicationKey(n, intent.publication.RangeID), Value: publication}, {Key: graphstore.RangeEndKey(n, intent.publication.Last), Value: publication}, {Key: graphstore.RangeOwnershipKey(n, intent.publication.RangeID), Value: ownership}}
			undo, err := encodeRangeMoveUndo(q, rows)
			if err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			for _, row := range rows {
				if err := q.put(row.Key, row.Value); err != nil {
					return raftlog.ApplicationBatch{}, err
				}
			}
			if err := q.put(rangeMoveActiveKey(m.ns), move[:]); err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			return finish(status, true, []raftlog.ApplicationControlPut{{Key: rangeMoveControlKey(m.ns, move, 'I'), Value: owned(c.intent)}, {Key: rangeMoveUndoControlKey(m.ns, move, 0), Value: undo}})
		}
		if err := q.put(rangeMoveActiveKey(m.ns), move[:]); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		controls := []raftlog.ApplicationControlPut{{Key: rangeMoveControlKey(m.ns, move, 'I'), Value: owned(c.intent)}}
		return finish(status, true, controls)
	}
	if !present && m.unadmittedMoveAbort(c, intent) {
		// Remember the original source's durable negative decision even if
		// this target Fence is still in flight. No active fence or credit is
		// needed for a target which never admitted the move.
		status.floor = c.proofs[0].floor
		return finish(status, true, []raftlog.ApplicationControlPut{{Key: rangeMoveControlKey(m.ns, move, 'I'), Value: owned(c.intent)}})
	}
	if !present || current.result != rangeMoveApplied {
		status.result = rangeMoveBlocked
		return finish(status, false, nil)
	}
	if c.phase == rangeMoveCapture {
		if current.phase != rangeMoveFence || len(c.data) != 0 || c.sequence != 0 {
			status.result = rangeMoveMismatch
			return finish(status, false, nil)
		}
		installed, err := readPartitionRoundFloor(q, configuration)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		status.floor, err = m.allocationFloor(q, configuration, installed)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		capture, err := boundedEncoding(1024, func(w *boundedWriter) {
			w.add([]byte{'G', 'M', 'C', 1})
			w.add(appendAllocationScope(nil, m.scope()))
			w.u64(status.floor)
			shared := q.base
			shared.Generation = 0 // Local bank generations never become replicated placement identity.
			emitSourceRoot(w, shared)
		})
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		status.digest = sha256.Sum256(capture)
		if err := q.put(rangeMoveProgressKey(m.ns, move), capture); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		controls := []raftlog.ApplicationControlPut{{Key: rangeMoveControlKey(m.ns, move, 'C'), Value: capture}}
		lineage, e := m.captureMoveRetainedLineage(q, move, digest, configuration, status.floor)
		if e != nil {
			return raftlog.ApplicationBatch{}, e
		}
		controls = append(controls, lineage...)
		return finish(status, true, controls)
	}
	return m.stageRangeMoveContinuation(q, c, intent, current, status, budget)
}

func (m *declaredMaterializer) checkRangeMoveState(q *reader) error {
	if !m.rangeMoves() {
		return nil
	}
	if err := m.checkMoveRetainedLineages(q); err != nil {
		return err
	}
	move, err := readRangeMoveActive(q)
	if err != nil || move == ([16]byte{}) {
		return err
	}
	remaining := raftlog.ReadBudget{Rows: min(q.view.ReadLimits().Rows, q.limits.readRows-q.rows), Bytes: min(q.view.ReadLimits().Bytes, q.limits.readBytes-q.bytes)}
	reservation, work, err := q.view.RecoveryOwner(q.ctx, remaining)
	if charged := chargeGraphWork(q, graphstore.PageWork{Records: work.Records, Bytes: work.Bytes}); charged != nil {
		return errors.Join(charged, err)
	}
	if err != nil {
		return err
	}
	owned, err := m.moveOwnsRecovery(q, reservation)
	if err != nil || !owned {
		return errors.Join(errCorrupt, err)
	}
	return nil
}

func moveCaptureRoot(wire []byte, expected allocationScope) (raftlog.ApplicationRoot, uint64, error) {
	body, err := graphBody(wire, "GMC\x01", 1024)
	if err != nil {
		return raftlog.ApplicationRoot{}, 0, err
	}
	c := graphCursor{b: body}
	scope := c.take(120)
	floor := c.u64()
	root := readSourceRoot(&c)
	if c.err != nil || len(c.b) != 0 || readAllocationScope(&decoder{b: scope}) != expected || len(root.Image) != 172 || sha256.Sum256(root.Image) != root.ImageHash {
		return raftlog.ApplicationRoot{}, 0, errors.Join(errCorrupt, c.err)
	}
	return root, floor, nil
}

func (m *declaredMaterializer) unadmittedMoveAbort(c rangeMoveCommand, r rangeMoveIntent) bool {
	if c.phase != rangeMoveAbort || c.sequence != 0 || len(c.data) != 0 || len(c.proofs) != 1 || m.scope() != r.target {
		return false
	}
	proof := c.proofs[0]
	return proof.ns == (namespace{r.source.graph, r.source.partition}) && proof.move == rangeMoveID(proof.ns, r.id) && proof.intent == sha256.Sum256(c.intent) && proof.phase == rangeMoveAbort && proof.sequence == 0 && proof.result == rangeMoveApplied && proof.round == 0
}
