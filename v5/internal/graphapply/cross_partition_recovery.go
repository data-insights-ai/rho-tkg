package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func crossRecoveryID(n namespace, tx [16]byte) [32]byte {
	b := crossControlKey(n, tx, 'C')
	return sha256.Sum256(b)
}
func (m *declaredMaterializer) crossReservation(manifest crossManifest, phase crossPhase) (raftlog.ApplicationRecoveryReservation, error) {
	part, found := manifest.part(m.ns.partition)
	if !found {
		return raftlog.ApplicationRecoveryReservation{}, errInvalid
	}
	// These are logical storage envelopes, not representation/RSS allowances.
	// Kernel bytes already contain every physical row and ordered CDC group.
	commandBytes := len(manifest.original) + 4096
	for _, p := range manifest.parts {
		commandBytes += len(p.kernel) + 32
	}
	commandBytes += 256 * len(manifest.parts)
	if manifest.condition != nil {
		// GCM2 adds its two-field/seal framing plus GUC1's fixed framing and
		// one fixed392-byte GSC1 certificate/prefix/authority per participant.
		// This is sizing only: do not allocate another encoded manifest here.
		commandBytes += 84 + 436*len(manifest.condition.parts)
	}
	if commandBytes > m.limits.commandBytes {
		return raftlog.ApplicationRecoveryReservation{}, errLimit
	}
	resolveBytes := uint64(8192 + 8*len(part.kernel))
	resolveRecords := uint64(9 + part.identities.WriteRecords) // #nosec G115 -- Inspected kernel counts are nonnegative and bounded by the decoded record budget.
	if _, _, err := raftlog.ApplicationRecoveryRootOverhead(2); err != nil {
		return raftlog.ApplicationRecoveryReservation{}, err
	}
	reservation := raftlog.ApplicationRecoveryReservation{ID: crossRecoveryID(m.ns, manifest.tx), Digest: manifest.digest}
	if phase == crossRegister {
		reservation.Credits = append(reservation.Credits, raftlog.ApplicationRecoveryCredit{Phase: uint32(crossPrepare), CommandBytes: commandBytes, RetainedBytes: uint64(8192 + 2*commandBytes), RetainedRecords: 10}, raftlog.ApplicationRecoveryCredit{Phase: uint32(crossDecide), CommandBytes: commandBytes, RetainedBytes: 8192, RetainedRecords: 7})
	}
	reservation.Credits = append(reservation.Credits, raftlog.ApplicationRecoveryCredit{Phase: uint32(crossResolve), CommandBytes: commandBytes, RetainedBytes: resolveBytes, RetainedRecords: resolveRecords})
	return reservation, nil
}
func (m *declaredMaterializer) crossRecovery(q *reader, r crossCommand, manifest crossManifest) (raftlog.ApplicationRecoveryPurpose, error) {
	current, present, err := crossCurrent(q, manifest.tx)
	if err != nil {
		return raftlog.ApplicationRecoveryPurpose{}, err
	}
	if present && current.digest != manifest.digest {
		return raftlog.ApplicationRecoveryPurpose{}, errInvalid
	}
	if present && current.phase >= r.phase {
		return raftlog.ApplicationRecoveryPurpose{}, nil
	}
	if r.phase == crossRegister {
		_, exists, e := q.getBounded(declaredRequestKey(q.ns, graphOperations, manifest.request.identity()), graphOutcomeBytes)
		if e != nil {
			return raftlog.ApplicationRecoveryPurpose{}, e
		}
		if exists {
			return raftlog.ApplicationRecoveryPurpose{}, errInvalid
		}
	}
	if r.phase == crossRegister || r.phase == crossPrepare && m.ns.partition != manifest.coordinator {
		reservation, err := m.crossReservation(manifest, r.phase)
		return raftlog.ApplicationRecoveryPurpose{Reserve: &reservation}, err
	}
	// This is a capacity recipe, not applied registration authority. A follower
	// may receive its original Reserve earlier in this same Ready. Store binds
	// Use to that exact applied/queued owner; Stage checks applied dependencies.
	wire, err := encodeCrossCommand(r, m.limits)
	if err != nil {
		return raftlog.ApplicationRecoveryPurpose{}, err
	}
	use := raftlog.ApplicationRecoveryUse{ID: crossRecoveryID(m.ns, manifest.tx), Digest: manifest.digest, CommandDigest: sha256.Sum256(wire), Phase: uint32(r.phase)}
	if r.phase == crossDecide && r.conflict != nil {
		use.RetireBefore = uint32(crossDecide)
	}
	if r.phase == crossResolve && len(r.proofs) == 1 && !r.proofs[0].commit {
		use.RetireBefore = uint32(crossResolve)
	}
	return raftlog.ApplicationRecoveryPurpose{Use: &use}, nil
}

// VerifyApplicationRecovery is selected once by the immutable bound Driver.
// It parses the complete typed protocol and checks local registration when
// applied. Store requires genuine applied/queued ownership for every Use; a
// caller prefix, TxID or supplied credit cannot claim capacity.
func (m *declaredMaterializer) VerifyApplicationRecovery(ctx context.Context, data []byte, view *raftlog.ApplicationView) (purpose raftlog.ApplicationRecoveryPurpose, err error) {
	if m == nil || m.store == nil || ctx == nil || view == nil || !crossGraphAgreement(m.SemanticContractID()) {
		return purpose, errInvalid
	}
	// Store owns this genuine active or prepared incoming-bank view. Borrow it
	// through the callback: reopening by index would select the wrong bank.
	binding, err := view.ApplicationBinding(ctx)
	if err != nil || binding != m.binding {
		return purpose, errors.Join(errInvalid, err)
	}
	base, err := view.RootBounded(ctx, 172)
	if err != nil {
		return purpose, err
	}
	if len(data) == 0 {
		return purpose, nil
	}
	if isRetainedFeedWire(data) {
		command, err := decodeRetainedFeedCommand(data)
		if err != nil || !retainedFeedAgreement(m.SemanticContractID()) || command.scope != m.scope() {
			return purpose, errors.Join(errInvalid, err)
		}
		// No historical reopen here: view may be the prepared incoming bank.
		// Native ordinary admission funds feed controls. It cannot borrow the
		// transient coordinator reserve or independently funded pin cleanup.
		return purpose, nil
	}
	if isGraphRetentionWire(data) {
		command, err := decodeGraphRetentionCommand(data)
		if err != nil || !m.collectsHistory() || command.scope != m.scope() {
			return purpose, errors.Join(errInvalid, err)
		}
		return purpose, nil // ordinary credit, never a transient or terminal claim
	}
	if isGraphPinWire(data) {
		if !m.retainedPins() {
			return purpose, errInvalid
		}
		l := m.limits.allocation
		l.readRows = min(l.readRows, m.limits.sourceRows)
		l.readBytes = min(l.readBytes, m.limits.sourceBytes)
		q := reader{ctx: ctx, view: view, ns: m.ns, base: base, limits: l, bytes: cap(base.Image)}
		if err := joinGraphOutputBudget(&q, graphRequest{}, 4096+8*len(data), m.limits); err != nil {
			return purpose, err
		}
		c, err := decodeGraphPinCommand(data)
		if err != nil {
			return purpose, err
		}
		return m.retainedPinPurpose(&q, c, data)
	}
	if isRangeMoveWire(data) {
		command, err := decodeRangeMoveCommand(data)
		if err != nil || !m.rangeMoves() || command.scope != m.scope() {
			return purpose, errors.Join(errInvalid, err)
		}
		intent, err := decodeRangeMoveIntent(command.intent)
		if err != nil {
			return purpose, err
		}
		l := m.limits.allocation
		l.readRows = min(l.readRows, m.limits.sourceRows)
		l.readBytes = min(l.readBytes, m.limits.sourceBytes)
		q := reader{ctx: ctx, view: view, ns: m.ns, base: base, limits: l, bytes: cap(base.Image)}
		if err := joinGraphOutputBudget(&q, graphRequest{}, 8*len(data)+4096, m.limits); err != nil {
			return purpose, err
		}
		return m.rangeMoveRecovery(&q, command, intent)
	}
	if isGraphCutWire(data) {
		c, e := decodeGraphCutCommand(data)
		if e != nil || !m.certifiedReads() || c.scope != m.scope() {
			return purpose, errors.Join(errInvalid, e)
		}
		return purpose, nil
	}
	if !isCrossPartitionWire(data) {
		// Existing bootstrap/allocation and PGQ2 local writes have ordinary credit.
		// Stage remains the authority for those commands and their complete effects.
		if bytes.HasPrefix(data, []byte{'P', 'G', 'Q', 2}) {
			_, err = decodePartitionGraphCommand(data, m.limits)
			return purpose, err
		}
		if len(data) >= 5 && commandKind(data[4]) == initDeclaredPartition {
			_, err = decodeDeclaredInit(data, m.limits)
			return purpose, err
		}
		_, err = decodeAllocationProtocolCommand(data, m.limits)
		return purpose, err
	}
	r, decoded, err := decodeCrossCommand(data, m.limits)
	if err != nil || r.ns != m.ns {
		return purpose, errors.Join(errInvalid, err)
	}
	l := m.limits.allocation
	l.readRows, l.readBytes = min(l.readRows, m.limits.sourceRows), min(l.readBytes, m.limits.sourceBytes)
	q := reader{ctx: ctx, view: view, ns: m.ns, base: base, limits: l, bytes: cap(base.Image)}
	if err := joinGraphOutputBudget(&q, graphRequest{}, decoded, m.limits); err != nil {
		return purpose, err
	}
	if m.rangeMoves() {
		active, e := readRangeMoveActive(&q)
		if e != nil {
			return purpose, e
		}
		if active != ([16]byte{}) {
			// The supplied view may be an incoming prepared bank. Completed
			// replay needs only its exact immutable configuration and registration;
			// fresh fenced work cannot inspect the active store's baseline catalog.
			cfg, e := m.readGenesisConfig(&q)
			if e != nil {
				return purpose, e
			}
			manifest, e := decodeCrossManifest(r.manifest, m.limits, q.arena)
			if e != nil {
				return purpose, e
			}
			if e := m.validateCrossManifestBinding(manifest, cfg); e != nil {
				return purpose, e
			}
			if e := validateCrossPhaseProofs(r, manifest); e != nil {
				return purpose, e
			}
			if r.conflict != nil {
				if e := m.checkCrossCapacityBindings(r.conflict, manifest); e != nil {
					return purpose, e
				}
			}
			current, present, e := crossCurrent(&q, manifest.tx)
			if e != nil {
				return purpose, e
			}
			if present && current.digest != manifest.digest {
				return purpose, errInvalid
			}
			if present && current.phase >= r.phase {
				original, found, e := crossControl(&q, crossControlKey(q.ns, manifest.tx, 'R'), m.limits.commandBytes)
				if e != nil || !found || !bytes.Equal(original.Value, r.manifest) {
					return purpose, errors.Join(errCorrupt, e)
				}
				return purpose, nil
			}
			return m.recoveryAfterRangeMove(&q)
		}
	}
	manifest, err := m.crossManifest(&q, r.manifest)
	if err != nil {
		return purpose, err
	}
	if err := validateCrossPhaseProofs(r, manifest); err != nil {
		return purpose, err
	}
	if r.conflict != nil {
		if err := m.checkCrossCapacityBindings(r.conflict, manifest); err != nil {
			return purpose, err
		}
	}
	return m.crossRecovery(&q, r, manifest)
}
