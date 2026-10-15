package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// These are hard limits, NOT authenticated source-size predictions. The
// materializer must refuse before publishing an unfunded page. Completion of
// a particular workload additionally requires its full resource proof.
type rangeMovePools struct{ describe, imported, undo uint64 }

const movePoolRefusalBytes uint64 = 64 << 10
const movePoolRefusalOutput = 16 << 10

func rangeMovePooledSemanticContractID() raftlog.ApplicationSemanticContractID {
	base := rangeMoveGraphBatchSemanticContractID()
	return raftlog.ApplicationSemanticContractID(sha256.Sum256(append(base[:], []byte("range-move=hard-pooled-retained-v1\x00undo=before-image-debt-v1\x00capacity=nonterminal-exact-original-v1\x00")...)))
}
func rangeMovePoolsEnabled(id raftlog.ApplicationSemanticContractID) bool {
	return id == rangeMovePooledSemanticContractID()
}
func newRangeMovePooledMaterializer(s *raftlog.Store, n namespace, d graphstore.OwnershipDeclaration, l materializerLimits) (*declaredMaterializer, error) {
	if s == nil || s.ApplicationRecovery().Version != 4 || s.ApplicationRetention().Version != 1 || s.ApplicationControls().Version != 1 {
		return nil, errInvalid
	}
	return newDeclaredMaterializerWithAgreement(s, n, d, l, rangeMovePooledSemanticContractID())
}
func (p rangeMovePools) valid(fresh bool) bool {
	if !fresh {
		return p == (rangeMovePools{})
	}
	return p.describe > movePoolRefusalBytes && p.imported > movePoolRefusalBytes && p.undo > 0 && p.describe <= 1<<40 && p.imported <= 1<<40 && p.undo <= 1<<40
}
func applyMovePools(r *raftlog.ApplicationRecoveryReservation, p rangeMovePools) error {
	if r == nil || !p.valid(true) {
		return errInvalid
	}
	for i := range r.Spans {
		s := &r.Spans[i]
		if s.First == 0 || s.Last < s.First || s.CommandBytes < 0 || s.CommandBytes > (1<<20) || s.RetainedBytes == 0 {
			return errInvalid
		}
		var total uint64
		switch s.First {
		case moveSpanDescribe:
			total = p.describe
		case moveSpanImport + 1:
			total = p.imported
		case moveSpanUndo:
			total = p.undo
		}
		if total == 0 {
			continue
		}
		count := uint64(s.Last) - uint64(s.First) + 1
		log := uint64(s.CommandBytes + 83) // #nosec G115 -- fixed positive admitted command cap.
		if count > math.MaxUint64/log || count > math.MaxUint64/s.RetainedBytes || total > count*s.RetainedBytes {
			return errLimit
		}
		s.BytePool = raftlog.ApplicationRecoveryBytePool{RetainedLimit: total, RetainedRemaining: total, LogLimit: count * log, LogRemaining: count * log}
	}
	return nil
}

func movePoolUse(c rangeMoveCommand) (raftlog.ApplicationRecoveryUse, error) {
	r, err := decodeRangeMoveIntent(c.intent)
	if err != nil {
		return raftlog.ApplicationRecoveryUse{}, err
	}
	wire, err := encodeRangeMoveCommand(c)
	if err != nil {
		return raftlog.ApplicationRecoveryUse{}, err
	}
	move := rangeMoveID(namespace{r.source.graph, r.source.partition}, r.id)
	return raftlog.ApplicationRecoveryUse{ID: rangeMoveRecoveryID(namespace{c.scope.graph, c.scope.partition}, move), Digest: sha256.Sum256(c.intent), CommandDigest: sha256.Sum256(wire), Phase: rangeMoveCredit(c)}, nil
}
func (m *declaredMaterializer) movePoolStep(q *reader, c rangeMoveCommand) (raftlog.ApplicationRecoveryStepBudget, error) {
	use, err := movePoolUse(c)
	if err != nil {
		return raftlog.ApplicationRecoveryStepBudget{}, err
	}
	b := raftlog.ReadBudget{Rows: min(q.view.ReadLimits().Rows, q.limits.readRows-q.rows), Bytes: min(q.view.ReadLimits().Bytes, q.limits.readBytes-q.bytes)}
	s, w, err := q.view.RecoveryStepBudget(q.ctx, use, b)
	return s, errors.Join(err, chargeGraphWork(q, graphstore.PageWork{Records: w.Records, Bytes: w.Bytes}))
}

func movePoolCandidateFits(p raftlog.ApplicationPolicy, b raftlog.ApplicationBatch, s raftlog.ApplicationRecoveryStepBudget, floor uint64) error {
	n, rows, _, err := p.PooledRecoveryBatchBound(b, 0)
	if err != nil {
		return errors.Join(errLimitIfNative(err), err)
	}
	if n > s.Maximum.RetainedBytes || rows > s.Maximum.RetainedRecords || s.Pool.RetainedRemaining < floor || n > s.Pool.RetainedRemaining-floor {
		return errLimit
	}
	return nil
}

// Refusal is an exact attempted command, independent of the last accepted
// cursor. It consumes its original Use once and cannot be retried as a new page.
func movePoolRefusalReplay(c rangeMoveCommand, s rangeMoveStatus) (bool, error) {
	if s.result != rangeMoveBudgetLimit {
		return false, nil
	}
	if c.phase == rangeMoveAbort {
		return false, nil
	}
	if c.phase == s.failedPhase && c.sequence == s.failedSequence {
		wire, err := encodeRangeMoveCommand(c)
		if err != nil || sha256.Sum256(wire) != s.failedCommand {
			return false, errors.Join(errInvalid, err)
		}
		return true, nil
	}
	// Earlier accepted commands remain inspection-only; no new import or
	// reclassification is allowed after the refused exact-next attempt.
	if c.phase == s.phase && c.sequence <= s.sequence {
		return true, nil
	}
	return false, errRangeMovePending
}
func moveFailureFollows(s rangeMoveStatus) bool {
	return s.phase == rangeMoveCapture && s.sequence == 0 && s.failedPhase == rangeMoveDescribe && s.failedSequence == 0 || (s.phase == rangeMoveDescribe || s.phase == rangeMoveImport) && s.failedPhase == s.phase && s.failedSequence == s.sequence+1
}

// A separate output envelope is PREPAID on the original arena before staging.
// Candidate work/source debits survive refusal; there is no budget reset.
func prepayMoveRefusal(q *reader) (*graphstate.OutputBudget, error) {
	if q == nil || q.arena == nil {
		return nil, errInvalid
	}
	if err := reserveComposer(q, movePoolRefusalOutput); err != nil {
		return nil, err
	}
	return graphstate.NewOutputBudget(movePoolRefusalOutput)
}
func (m *declaredMaterializer) finishMoveRefusal(q *reader, c rangeMoveCommand, current rangeMoveStatus, index uint64, out *graphstate.OutputBudget, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	wire, err := encodeRangeMoveCommand(c)
	if err != nil || out == nil {
		return raftlog.ApplicationBatch{}, errors.Join(errInvalid, err)
	}
	s := current
	s.result, s.index = rangeMoveBudgetLimit, index
	s.failedPhase, s.failedSequence, s.failedCommand = c.phase, c.sequence, sha256.Sum256(wire)
	fallback := *q
	fallback.arena, fallback.writes, fallback.stageBytes = out, nil, 128
	if err = out.Reserve(2048); err != nil {
		return raftlog.ApplicationBatch{}, outputBudgetFailure(err)
	}
	control, err := movePoolOriginal(c, s)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	return m.finishMovePage(&fallback, s, []raftlog.ApplicationControlPut{control}, budget)
}

func movePoolProgressWire(wire []byte) bool { return bytes.HasPrefix(wire, []byte("GMP\x06")) }

func movePoolOriginalKey(c rangeMoveCommand) []byte {
	r, _ := decodeRangeMoveIntent(c.intent) // caller has admitted the canonical command.
	key := rangeMoveControlKey(namespace{c.scope.graph, c.scope.partition}, rangeMoveID(namespace{r.source.graph, r.source.partition}, r.id), 'V')
	key = binary.BigEndian.AppendUint32(key, rangeMoveCredit(c))
	return key
}
func movePoolOriginal(c rangeMoveCommand, s rangeMoveStatus) (raftlog.ApplicationControlPut, error) {
	wire, err := encodeRangeMoveCommand(c)
	if err != nil {
		return raftlog.ApplicationControlPut{}, err
	}
	status, err := encodeRangeMoveStatus(s)
	if err != nil {
		return raftlog.ApplicationControlPut{}, err
	}
	hash := sha256.Sum256(wire)
	return raftlog.ApplicationControlPut{Key: movePoolOriginalKey(c), Value: append(hash[:], status...)}, nil
}
func readMovePoolOriginal(q *reader, c rangeMoveCommand) (rangeMoveStatus, bool, error) {
	r, present, err := crossControl(q, movePoolOriginalKey(c), 32+256)
	if err != nil || !present {
		return rangeMoveStatus{}, present, err
	}
	wire, err := encodeRangeMoveCommand(c)
	if err != nil {
		return rangeMoveStatus{}, false, err
	}
	hash := sha256.Sum256(wire)
	if len(r.Value) < 32 || !bytes.Equal(r.Value[:32], hash[:]) {
		return rangeMoveStatus{}, false, errInvalid
	}
	s, err := decodeRangeMoveStatus(r.Value[32:])
	intent, e := decodeRangeMoveIntent(c.intent)
	move := rangeMoveID(namespace{intent.source.graph, intent.source.partition}, intent.id)
	if err != nil || e != nil || s.format != 3 || s.index != r.Index || s.ns != q.ns || s.move != move || s.intent != sha256.Sum256(c.intent) || s.result != rangeMoveBudgetLimit && s.phase != c.phase || s.result == rangeMoveBudgetLimit && (s.failedPhase != c.phase || s.failedSequence != c.sequence || s.failedCommand != hash) {
		return rangeMoveStatus{}, false, errors.Join(errCorrupt, err)
	}
	return s, true, nil
}

func sortMoveControls(puts []raftlog.ApplicationControlPut) error {
	slices.SortFunc(puts, func(a, b raftlog.ApplicationControlPut) int { return bytes.Compare(a.Key, b.Key) })
	for i := 1; i < len(puts); i++ {
		if bytes.Equal(puts[i-1].Key, puts[i].Key) {
			return errCorrupt
		}
	}
	return nil
}

// Undo has its own immutable pool, not the unused import pool. Reading its
// exact-next native phase is descriptive and does not consume that phase.
func (m *declaredMaterializer) moveUndoBudget(q *reader, c rangeMoveCommand, next uint64) (raftlog.ApplicationRecoveryStepBudget, error) {
	undo := c
	undo.phase = rangeMoveAbort
	undo.sequence = next
	undo.data = nil
	undo.proofs = nil
	return m.movePoolStep(q, undo)
}

func (m *declaredMaterializer) moveUndoBound(q *reader, c rangeMoveCommand, rows []raftlog.KV, progress []byte, status rangeMoveStatus, maximum raftlog.ApplicationRecoveryCredit) (uint64, error) {
	return moveUndoBoundWithPolicy(m.store.ApplicationLimits(), q, c, rows, progress, status, maximum)
}
func moveUndoBoundWithPolicy(policy raftlog.ApplicationPolicy, q *reader, c rangeMoveCommand, rows []raftlog.KV, progress []byte, status rangeMoveStatus, maximum raftlog.ApplicationRecoveryCredit) (uint64, error) {
	if len(progress) > rangeMoveSpanProgressBytes {
		return 0, errLimit
	}
	if err := reserveComposer(q, 4096+64*(len(rows)+2)); err != nil {
		return 0, err
	}
	undo := c
	undo.phase = rangeMoveAbort
	undo.sequence = 1
	undo.data = nil
	undo.proofs = nil
	s := status
	s.phase = rangeMoveAbort
	s.sequence = 1
	wire, err := encodeRangeMoveStatus(s)
	if err != nil {
		return 0, err
	}
	control, err := movePoolOriginal(undo, s)
	if err != nil {
		return 0, err
	}
	b := batchAt(q.base)
	b.Writes = append(slices.Clone(rows), raftlog.KV{Key: rangeMoveProgressKey(q.ns, status.move), Value: progress}, raftlog.KV{Key: rangeMoveStatusKey(q.ns, status.move), Value: wire})
	if err = sortPartitionWrites(b.Writes); err != nil {
		return 0, err
	}
	b.Outcome, b.ControlPuts = wire, []raftlog.ApplicationControlPut{control}
	// Future counters/phase may change escaped-key length, never actual old
	// values. Pad to the declared progress cap and the full possible key escape
	// expansion (only increasing native growth/work; no fabricated graph data).
	padding := uint64(rangeMoveSpanProgressBytes - len(progress) + 2*len(control.Key)) // #nosec G115 -- checked positive <=33KiB.
	n, records, _, err := policy.PooledRecoveryBatchBound(b, padding)
	if err == nil && (n > maximum.RetainedBytes || records > maximum.RetainedRecords) {
		err = errLimit
	}
	return n, errors.Join(errLimitIfNative(err), err)
}
func errLimitIfNative(err error) error {
	if errors.Is(err, raftlog.ErrLimit) {
		return errLimit
	}
	return nil
}

// Restore checks scalar current authority and the exact accepted boundary.
// Full immutable undo folding is incremental in the portable snapshot auditor;
// a large valid transfer must not require one history-sized Restore operation.
func (m *declaredMaterializer) checkMovePoolState(q *reader, intent rangeMoveIntent, status rangeMoveStatus) error {
	// Authentic scalar budgets must still match the immutable original recipe.
	// A live permitted next page always retains one installable refusal.
	if !rangeMoveTerminal(status) && status.result == rangeMoveApplied {
		c := rangeMoveCommand{scope: m.scope()}
		var expected uint64
		if m.scope() == intent.source && (status.phase == rangeMoveCapture || status.phase == rangeMoveDescribe) {
			c.phase, c.sequence, expected = rangeMoveDescribe, status.sequence+1, intent.pools.describe
			if status.phase == rangeMoveCapture {
				c.sequence = 0
			}
		} else if m.scope() == intent.target && status.phase == rangeMoveImport {
			c.phase, c.sequence, expected = rangeMoveImport, status.sequence+1, intent.pools.imported
		}
		if expected != 0 && c.sequence <= uint64(intent.maxPages) { // #nosec G115 -- canonical finite intent.
			var err error
			c.intent, err = encodeRangeMoveIntent(intent)
			if err != nil {
				return err
			}
			step, err := m.movePoolStep(q, c)
			if err != nil || step.Pool.RetainedLimit != expected || step.Pool.RetainedRemaining < movePoolRefusalBytes {
				return errors.Join(errCorrupt, err)
			}
		}
	}
	wire, found, err := q.getBounded(rangeMoveProgressKey(m.ns, status.move), rangeMoveSpanProgressBytes)
	if err != nil || !found {
		return err
	}
	if !movePoolProgressWire(wire) {
		if bytes.HasPrefix(wire, []byte("GMC\x01")) {
			return nil
		}
		return errCorrupt
	}
	p, err := decodeRangeMoveProgress(wire, q.arena)
	if err != nil {
		return err
	}
	if p.imported == nil {
		if p.undoDebt != 0 {
			return errCorrupt
		}
		return nil
	}
	if (status.phase == rangeMoveImport || status.phase == rangeMoveSeal || status.phase == rangeMoveActivate) && p.undoTotal != status.sequence {
		return errCorrupt
	}
	if p.undoNext == p.undoTotal && p.undoDebt != 0 {
		return errCorrupt
	}
	if err = checkMovePoolBoundary(q, intent, status, p); err != nil {
		return err
	}
	if !rangeMoveTerminal(status) && p.undoDebt != 0 {
		c := rangeMoveCommand{scope: m.scope(), phase: rangeMoveAbort, intent: nil, sequence: p.undoNext + 1}
		c.intent, err = encodeRangeMoveIntent(intent)
		if err != nil {
			return err
		}
		step, e := m.movePoolStep(q, c)
		if e != nil || step.Pool.RetainedLimit != intent.pools.undo || p.undoDebt > step.Pool.RetainedRemaining {
			return errors.Join(errCorrupt, e)
		}
	}
	return nil
}

func movePoolPhaseKey(n namespace, move [16]byte, phase uint32) []byte {
	return binary.BigEndian.AppendUint32(rangeMoveControlKey(n, move, 'V'), phase)
}
func checkMovePoolBoundary(q *reader, intent rangeMoveIntent, status rangeMoveStatus, p rangeMoveProgress) error {
	for _, sequence := range []uint64{p.undoTotal, p.undoTotal + 1} {
		if sequence == 0 || sequence > uint64(intent.maxPages) {
			continue
		} // #nosec G115 -- valid finite intent.
		r, found, err := crossControl(q, movePoolPhaseKey(q.ns, status.move, moveSpanImport+uint32(sequence)), 288)
		if err != nil {
			return err
		}
		if !found {
			if sequence <= p.undoTotal {
				return errCorrupt
			}
			continue
		}
		if len(r.Value) < 32 {
			return errCorrupt
		}
		s, err := decodeRangeMoveStatus(r.Value[32:])
		if err != nil || s.format != 3 || s.ns != q.ns || s.move != status.move || s.intent != status.intent || s.index != r.Index {
			return errors.Join(errCorrupt, err)
		}
		if sequence <= p.undoTotal {
			if s.result != rangeMoveApplied || s.phase != rangeMoveImport || s.sequence != sequence {
				return errCorrupt
			}
		} else if s.result != rangeMoveBudgetLimit || s.failedPhase != rangeMoveImport || s.failedSequence != sequence || s.sequence != p.undoTotal {
			return errCorrupt
		}
	}
	return nil
}

// One independently bounded GMU per Advance. Only a fixed cursor, original
// intent and scalar debt survive calls; graph-sized history is never retained.
// Every call has the framework's finite work/owned grant and caller lifetime.
type movePoolAudit struct {
	after                      []byte
	move                       [16]byte
	status                     rangeMoveStatus
	intent                     []byte
	next, total, sum, expected uint64
}

func (a *movePoolAudit) advance(q *reader, m *declaredMaterializer, policy raftlog.ApplicationPolicy) (bool, error) {
	if a.move == ([16]byte{}) {
		page, err := rangeMoveReplayScan(q, recordKey(q.ns, 18), a.after, 8192, 1)
		if err != nil {
			return false, err
		}
		if len(page.Rows) == 0 {
			return page.Complete, nil
		}
		row := page.Rows[0]
		s, err := decodeRangeMoveStatus(row.Value)
		if err != nil || !bytes.Equal(row.Key, rangeMoveStatusKey(q.ns, s.move)) || s.format != 3 {
			return false, errors.Join(errCorrupt, err)
		}
		if err = reserveComposer(q, 4096); err != nil {
			return false, err
		}
		a.after = owned(row.Key)
		wire, found, err := q.getBounded(rangeMoveProgressKey(q.ns, s.move), rangeMoveSpanProgressBytes)
		if err != nil {
			return false, err
		}
		if !found || !movePoolProgressWire(wire) {
			return false, nil
		}
		p, err := decodeRangeMoveProgress(wire, q.arena)
		if err != nil {
			return false, err
		}
		if p.imported == nil || p.undoNext == p.undoTotal {
			if p.undoDebt != 0 {
				return false, errCorrupt
			}
			return false, nil
		}
		r, found, err := crossControl(q, rangeMoveControlKey(q.ns, s.move, 'I'), 1024)
		if err != nil || !found {
			return false, errors.Join(errCorrupt, err)
		}
		if sha256.Sum256(r.Value) != s.intent {
			return false, errCorrupt
		}
		a.move, a.status, a.intent = s.move, s, owned(r.Value)
		a.next, a.total, a.expected = p.undoNext, p.undoTotal, p.undoDebt
		return false, nil
	}
	original := a.total - a.next
	r, found, err := crossControl(q, rangeMoveUndoControlKey(q.ns, a.move, original), (1<<20)-2048)
	if err != nil || !found {
		return false, errors.Join(errCorrupt, err)
	}
	if err = reserveComposer(q, 4096+4*len(r.Value)); err != nil {
		return false, err
	}
	rows, bound, err := decodePooledMoveUndo(r.Value)
	if err != nil || bound > math.MaxUint64-a.sum {
		return false, errors.Join(errCorrupt, err)
	}
	v, found, err := crossControl(q, movePoolPhaseKey(q.ns, a.move, moveSpanImport+uint32(original)), 288) // #nosec G115 -- canonical GMP total<=2^24.
	if err != nil || !found || len(v.Value) < 32 {
		return false, errors.Join(errCorrupt, err)
	}
	s, err := decodeRangeMoveStatus(v.Value[32:])
	if err != nil || s.format != 3 || s.index != v.Index || r.Index != v.Index || s.result != rangeMoveApplied || s.phase != rangeMoveImport || s.sequence != original || s.move != a.move || s.intent != a.status.intent || s.ns != q.ns {
		return false, errors.Join(errCorrupt, err)
	}
	progress, found, err := q.getBounded(rangeMoveProgressKey(q.ns, a.move), rangeMoveSpanProgressBytes)
	if err != nil || !found {
		return false, errors.Join(errCorrupt, err)
	}
	intent, err := decodeRangeMoveIntent(a.intent)
	if err != nil {
		return false, err
	}
	reservation, err := m.spanMoveReservation(intent)
	if err != nil {
		return false, err
	}
	var maximum raftlog.ApplicationRecoveryCredit
	for _, span := range reservation.Spans {
		if span.First == moveSpanUndo {
			maximum = raftlog.ApplicationRecoveryCredit{Phase: span.First, CommandBytes: span.CommandBytes, RetainedBytes: span.RetainedBytes, RetainedRecords: span.RetainedRecords}
		}
	}
	c := rangeMoveCommand{scope: m.scope(), phase: rangeMoveAbort, intent: a.intent, sequence: 1}
	required, err := moveUndoBoundWithPolicy(policy, q, c, rows, progress, s, maximum)
	if err != nil || required != bound {
		return false, errors.Join(errCorrupt, err)
	}
	a.sum += bound
	a.next++
	if a.next == a.total {
		if a.sum != a.expected {
			return false, errCorrupt
		}
		a.move = [16]byte{}
		a.intent = nil
		a.sum = 0
	}
	return false, nil
}
