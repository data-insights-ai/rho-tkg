package raftlog

import (
	"context"
	"crypto/sha256"
	"errors"
	"math"
)

// ApplicationRecoveryPurpose is a trusted materializer's typed capacity intent.
// It is never parsed from a generic command prefix by Store. The verifier must
// check complete canonical command/binding/phase semantics, not just a header.
type ApplicationRecoveryPurpose struct {
	Reserve         *ApplicationRecoveryReservation
	Use             *ApplicationRecoveryUse
	ApplyBefore     *ApplicationRecoveryApplyBefore
	TerminalReserve *ApplicationRecoveryReservation
	TerminalUse     *ApplicationRecoveryUse
}

// ApplicationRecoveryApplyBefore requests reclassification after retirement of
// an observed applied owner. It grants no capacity. Only Store can match an
// earlier canonical terminal Use, and only actual committed-prefix progress may
// install it. Refusal preserves the trusted protocol's ordinary unready cause.
type ApplicationRecoveryApplyBefore struct {
	Owner   ApplicationRecoveryOwner
	Refusal error
}

// ApplicationRecoveryVerifier is selected once by the already-bound Driver's
// trusted materializer. Verification grants capacity only; Stage still validates
// dependencies, decisions and complete graph effects. It must retain no payload.
type ApplicationRecoveryVerifier interface {
	VerifyApplicationRecovery(context.Context, []byte, *ApplicationView) (ApplicationRecoveryPurpose, error)
}

// ApplicationRecoveryAdmission is an opaque same-Store/exact-command capability.
// Neither a caller-created purpose nor a command/no-op prefix constructs it.
// An admission can be reused only while its exact captured base remains current.
type ApplicationRecoveryAdmission struct {
	self    *ApplicationRecoveryAdmission
	s       *Store
	base    ApplicationRoot
	hash    [32]byte
	purpose ApplicationRecoveryPurpose
}

// BindApplicationRecoveryVerifier fixes the trusted verifier for this Store's
// exact immutable application binding. It changes no durable mode or agreement.
// A Driver must bind before accepting recovery traffic; legacy disabled mode
// refuses this extra capability instead of inferring one from command bytes.
func (s *Store) BindApplicationRecoveryVerifier(binding ApplicationBinding, verifier ApplicationRecoveryVerifier) error {
	if s == nil || verifier == nil {
		return ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return err
	}
	if !s.meta.Recovery.Config.enabled() || binding != (ApplicationBinding{Identity: s.meta.Transfer.Identity, SemanticContractID: s.meta.Rep.SemanticContractID}) || binding.Validate() != nil || s.recoveryVerifier != nil {
		return ErrInvalid
	}
	s.recoveryVerifier = verifier
	return nil
}

// AdmitApplicationRecovery verifies through the selected materializer before
// issuing capacity admission. No callback supplied by an individual request can
// authorize a credit. Returned base/image fields grant no cut or write authority.
func (s *Store) AdmitApplicationRecovery(ctx context.Context, data []byte) (*ApplicationRecoveryAdmission, error) {
	if s == nil || ctx == nil {
		return nil, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if err := s.check(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	verifier := s.recoveryVerifier
	if !s.meta.Recovery.Config.enabled() || verifier == nil || s.meta.Applied == 0 || len(data) > s.limits.MaxEntryBytes-frameOverhead {
		s.mu.Unlock()
		return nil, ErrInvalid
	}
	index, generation, imageHash := s.meta.Applied, s.activeGeneration(), s.meta.ImageHash
	s.mu.Unlock()
	// Use the actual registered immutable view. The verifier runs without the
	// Store lock so its real point/dependency reads cannot invert lock order.
	view, err := s.ApplicationView(index)
	if err != nil {
		return nil, err
	}
	base, err := view.RootBounded(ctx, s.applicationPolicy.MaxImageBytes)
	if err != nil {
		return nil, errors.Join(err, view.Close())
	}
	purpose, err := verifier.VerifyApplicationRecovery(ctx, data, view)
	err = errors.Join(err, view.Close())
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return nil, err
	}
	if s.meta.Applied != index || s.activeGeneration() != generation || s.meta.ImageHash != imageHash {
		return nil, ErrInvalid
	}
	if !validRecoveryPurpose(purpose) {
		return nil, ErrInvalid
	}
	if purpose.ApplyBefore != nil {
		if purpose.ApplyBefore.Owner != (ApplicationRecoveryOwner{ID: s.meta.Recovery.Reservation.ID, Digest: s.meta.Recovery.Reservation.Digest}) || purpose.ApplyBefore.Owner.ID == ([32]byte{}) {
			return nil, ErrInvalid
		}
		return nil, errors.Join(ErrApplicationPrerequisite, purpose.ApplyBefore.Refusal)
	}
	if purpose.Reserve == nil && purpose.Use == nil && purpose.TerminalReserve == nil && purpose.TerminalUse == nil {
		if err := s.admitApplicationLocked(len(data)); err != nil {
			return nil, err
		}
	} else if purpose.TerminalReserve != nil {
		if err := s.admitTerminalCreationLocked(len(data), *purpose.TerminalReserve); err != nil {
			return nil, err
		}
		owned, err := s.meta.Recovery.reserveTerminal(*purpose.TerminalReserve, s.meta.App.Policy, s.limits)
		if err != nil {
			return nil, err
		}
		r := owned.Terminal[owned.Terminal.find(purpose.TerminalReserve.ID)]
		purpose.TerminalReserve = &r
	} else if purpose.TerminalUse != nil {
		use := *purpose.TerminalUse
		if use.CommandDigest != sha256.Sum256(data) {
			return nil, ErrInvalid
		}
		credit, err := s.meta.Recovery.terminalCredit(use)
		if err != nil {
			return nil, err
		}
		if len(data) > credit.CommandBytes {
			return nil, ErrLimit
		}
		if err := s.admitTerminalUseLocked(len(data), use, credit); err != nil {
			return nil, err
		}
		purpose.TerminalUse = &use
	} else if purpose.Reserve != nil {
		if _, err := recoveryGrowth(*purpose.Reserve, s.meta.App.Policy, s.limits); err != nil {
			return nil, err
		}
		if err := s.admitRecoveryCreationLocked(len(data), *purpose.Reserve); err != nil {
			return nil, err
		}
		// The exact bounded metadata array is retained independently of a verifier
		// that may reuse its scratch after returning. Caller buffers stay borrowed.
		owned, err := s.meta.Recovery.reserve(*purpose.Reserve, s.meta.App.Policy, s.limits)
		if err != nil {
			return nil, err
		}
		purpose.Reserve = &owned.Reservation
	} else {
		if purpose.Use.CommandDigest != sha256.Sum256(data) {
			return nil, ErrInvalid
		}
		credit, err := s.meta.Recovery.credit(*purpose.Use)
		if err != nil {
			return nil, err
		}
		if len(data) > credit.CommandBytes {
			return nil, ErrLimit
		}
		if err := s.admitRecoveryUseLocked(len(data), *purpose.Use, credit); err != nil {
			return nil, err
		}
		use := *purpose.Use
		purpose.Use = &use
	}
	admitted := &ApplicationRecoveryAdmission{s: s, base: base, hash: sha256.Sum256(data), purpose: purpose}
	admitted.self = admitted
	return admitted, nil
}

func (s *Store) admitRecoveryCreationLocked(commandBytes int, reserve ApplicationRecoveryReservation) error {
	if err := s.admitApplicationLocked(commandBytes); err != nil {
		return err
	}
	future, err := recoveryGrowth(reserve, s.meta.App.Policy, s.limits)
	if err != nil {
		return err
	}
	current, err := recoveryOutstanding(s.meta, s.limits)
	if err != nil {
		return err
	}
	if s.meta.Recovery.Reservation.ID != ([32]byte{}) && !sameRecoveryReservation(s.meta.Recovery.Reservation, reserve) {
		return ErrLimit
	}
	if sameRecoveryReservation(s.meta.Recovery.Reservation, reserve) {
		future = current
	} else if s.meta.Recovery.Config.Version == 2 {
		terminal, err := terminalGrowth(s.meta.Recovery.Terminal, s.meta.App.Policy, s.limits)
		if err != nil {
			return err
		}
		future, err = addRecoveryGrowth(future, terminal)
		if err != nil {
			return err
		}
	}
	p := s.meta.App.Policy
	bytes, records := applicationTotals(s.meta)
	pending := s.meta.Last - s.meta.Applied + 2
	bytes += pending * unsignedLimit(p.MaxInstallBytes)
	records += pending * applicationInstallRecords(s.meta)
	if err := recoveryFits(future, bytes, records, s.meta.LogBytes+uint64(commandBytes+2*(frameOverhead+9)), s.meta.LogCount+2, p, s.limits); err != nil { // #nosec G115 -- admitApplicationLocked validates commandBytes 0..MaxEntryBytes-74 with MaxEntryBytes<=16 MiB; +166 fits int/uint64.
		return err
	}
	prospective := s.meta
	prospective.Recovery, err = prospective.Recovery.reserve(reserve, p, s.limits)
	if err != nil {
		return err
	}
	return generationCharge(prospective, pending, s.limits)
}

// Every additional pending Use, including an exact retry, owns pending install
// and log backing. Applied credit alone cannot admit arbitrary queued copies.
// This is a non-reserving preflight; serialized Persist repeats the same checks.
func (s *Store) admitRecoveryUseLocked(commandBytes int, use ApplicationRecoveryUse, credit ApplicationRecoveryCredit) error {
	return s.admitCreditUseLocked(commandBytes, applicationRecoveryQueuedPurpose{Use: &use}, credit)
}
func (s *Store) admitTerminalUseLocked(commandBytes int, use ApplicationRecoveryUse, credit ApplicationRecoveryCredit) error {
	return s.admitCreditUseLocked(commandBytes, applicationRecoveryQueuedPurpose{TerminalUse: &use}, credit)
}
func (s *Store) admitCreditUseLocked(commandBytes int, purpose applicationRecoveryQueuedPurpose, credit ApplicationRecoveryCredit) error {
	use := purpose.use()
	m := s.meta
	if m.Last >= math.MaxUint64-1 || len(m.Recovery.Queued)+1 > (maxMetadataBytes-recoveryMetadataFixedBytes)/256 {
		return ErrLimit
	}
	n := len(m.Recovery.Queued)
	queued := make([]applicationRecoveryQueued, n+1)
	copy(queued, m.Recovery.Queued)
	queued[n] = applicationRecoveryQueued{Index: m.Last + 1, CommandDigest: use.CommandDigest, Purpose: purpose, Credit: credit}
	m.Recovery.Queued = queued
	m.Last++
	extra := uint64(commandBytes + frameOverhead + 9) // #nosec G115 -- sole caller passes len(data)<=validated credit.CommandBytes<=16 MiB-74; nonnegative +83 fits int/uint64.
	var err error
	m.LogBytes, err = checkedRecoveryAdd(m.LogBytes, extra)
	if err != nil {
		return err
	}
	m.LogCount++
	m.App.TailBytes, err = checkedRecoveryAdd(m.App.TailBytes, extra)
	if err != nil {
		return err
	}
	p := m.App.Policy
	pending := m.Last - m.Applied
	if pending > p.MaxTailEntries || m.App.TailBytes > p.MaxTailBytes {
		return ErrLimit
	}
	protected, pendingBytes, pendingRecords, err := recoveryProjected(m, s.limits, pending)
	if err != nil {
		return err
	}
	protected.logBytes += m.Recovery.Staged.logBytes
	protected.logRecords += m.Recovery.Staged.logRecords
	retained, records := applicationTotals(m)
	if err := recoveryFits(protected, retained, records, m.LogBytes, m.LogCount, p, s.limits); err != nil {
		return err
	}
	if pendingBytes > p.RetainedApplicationBytes-retained-protected.bytes || pendingRecords > p.RetainedApplicationRecords-records-protected.records {
		return ErrLimit
	}
	if err := checkMetadataLimit(m, s.limits); err != nil {
		return err
	}
	return generationCharge(m, pending, s.limits)
}

func (a *ApplicationRecoveryAdmission) matches(s *Store, data []byte, use ApplicationRecoveryUse) bool {
	return a != nil && a.self == a && a.s == s && a.base.Generation == s.activeGeneration() && a.base.Index <= s.meta.Applied && a.hash == sha256.Sum256(data) && a.purpose.Use != nil && *a.purpose.Use == use
}

// RecoveryAdmissionForCommittedEntry binds already-classified pending capacity
// to the exact next committed installation. It reserves no fresh proposal
// allowance and grants no graph/decision authority. Driver still invokes its
// semantically bound Stage and Store rechecks the actual batch growth.
func (s *Store) RecoveryAdmissionForCommittedEntry(ctx context.Context, index uint64) (*ApplicationRecoveryAdmission, error) {
	if s == nil || ctx == nil {
		return nil, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return nil, err
	}
	if !s.meta.Recovery.Config.enabled() || index != s.meta.Applied+1 || index > s.meta.Hard.GetCommit() {
		return nil, ErrInvalid
	}
	e, _, _, err := s.get(index)
	if err != nil {
		return nil, err
	}
	for _, q := range s.meta.Recovery.Queued {
		if q.Index != index {
			continue
		}
		if q.CommandDigest != sha256.Sum256(e.GetData()) {
			return nil, ErrCorrupt
		}
		a := &ApplicationRecoveryAdmission{s: s, base: ApplicationRoot{Generation: s.activeGeneration(), Index: s.meta.Applied, ImageHash: s.meta.ImageHash}, hash: q.CommandDigest, purpose: ApplicationRecoveryPurpose{Reserve: q.Purpose.Reserve, Use: q.Purpose.Use, TerminalReserve: q.Purpose.TerminalReserve, TerminalUse: q.Purpose.TerminalUse}}
		a.self = a
		return a, nil
	}
	// An ordinary/replayed entry has no completion-credit purpose.
	return nil, nil
}
