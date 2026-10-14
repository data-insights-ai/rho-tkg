package raftlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
)

// ErrApplicationExpired identifies a known fresh-profile history floor. It is
// distinct from unavailable authority, an empty record and corrupt durable data.
// Disabled retention preserves every existing error and wire agreement.
var ErrApplicationExpired = errors.New("raftlog: application history expired")

// ApplicationRetentionAdmission is an opaque native Stage receipt, not a caller
// plan certificate. Its exact source/command/bank binding survives source Close;
// Install still rechecks the durable committed entry and all competing owners.
// It grants no graph pin, semantic predicate or authority to delete rows.
type ApplicationRetentionAdmission struct {
	self        *ApplicationRetentionAdmission
	s           *Store
	source      *ApplicationView
	bank        byte
	generation  uint64
	base        uint64
	imageHash   [32]byte
	entryHash   [32]byte
	commandHash [32]byte
	previous    [32]byte
	state       ApplicationRetentionState
	workBytes   int
}

// A local cursor is separately funded, never a portable namespace record. Its
// bounded logical-key/index encoding fits MaxKeyBytes+32; physical escaping is
// charged separately to each collector call. No transient recovery owner is used.
func retentionCursorGrowth(m metadata) applicationRecoveryGrowth {
	if !m.Retention.Config.enabled() || m.Retention.State.ID == ([32]byte{}) || m.Retention.Cursor.Section == 6 {
		return applicationRecoveryGrowth{}
	}
	return applicationRecoveryGrowth{bytes: unsignedLimit(1 + appFrameBytes + m.App.Policy.MaxKeyBytes + 32), records: 1}
}

func retentionViewBytes(s ApplicationRetentionState) int { return 64 * len(s.Roots) }
func retainedAnchor(s ApplicationRetentionState, index uint64, hash [32]byte, data bool) bool {
	for _, root := range s.Roots {
		if root.Index == index && root.ImageHash == hash && (!data || root.Data) {
			return true
		}
	}
	return false
}
func retentionFloor(s ApplicationRetentionState, tag byte) uint64 {
	switch tag {
	case appRootTag:
		return s.RootFloor
	case appChangeTag:
		return s.FeedFloor
	case appOutcomeTag:
		return s.OutcomeFloor
	default:
		return 0
	}
}
func retentionMonotone(old, next ApplicationRetentionState) bool {
	return next.PreviousID == old.ID && next.ID != old.ID && next.RootFloor >= old.RootFloor && next.FeedFloor >= old.FeedFloor && next.OutcomeFloor >= old.OutcomeFloor && next.RecoveryFloor >= old.RecoveryFloor && next.DataFloor >= old.DataFloor
}

// RetentionPlan returns an owned copy of this genuine view's CAPTURED native
// plan. Enabled distinguishes the fresh zero/uninstalled state from a disabled
// store. It does not re-read a later active plan, authorize anchor access or
// grant deletion rights. Prepared views retain their actual imported bank/state.
// The offered read ledger funds header/array ownership before any copy; no
// canonical encoding/hash or point lookup is needed for this captured value.
func (v *ApplicationView) RetentionPlan(ctx context.Context, budget ReadBudget) (state ApplicationRetentionState, enabled bool, work ApplicationProofWork, err error) {
	if err := v.lock(ctx); err != nil {
		return state, false, work, err
	}
	defer v.unlock()
	p := v.s.meta.App.Policy
	if budget.Rows < 1 || budget.Bytes < 1 {
		return state, false, work, ErrInvalid
	}
	if budget.Rows > p.MaxPageRows || budget.Bytes > p.MaxPageBytes {
		return state, false, work, ErrLimit
	}
	// One captured header and its owned root array; the conservative fixed
	// envelope covers value copies independently of the wire representation.
	cost := 512 + 2*retentionViewBytes(v.retention)
	if cost > budget.Bytes {
		return state, false, work, ErrLimit
	}
	work = ApplicationProofWork{Records: 1, Bytes: cost}
	return cloneRetentionState(v.retention), v.s.meta.Retention.Config.enabled(), work, nil
}

// PrepareApplicationRetention authenticates an inert plan against this genuine
// current root, complete native RS2 bank, retained anchors and the next ACTUAL
// committed normal command. All observed lengths and owned plan arrays spend the
// offered read ledger before hash/decode/copy. Unknown/missing data is never an
// invented empty root. The trusted graph materializer must separately prove its
// live pin, semantic-anchor, retry and safe-drain obligations.
func (v *ApplicationView) PrepareApplicationRetention(ctx context.Context, next ApplicationRetentionState, command []byte, budget ReadBudget) (out *ApplicationRetentionAdmission, work ApplicationProofWork, err error) {
	if err = v.lock(ctx); err != nil {
		return nil, work, err
	}
	defer v.unlock()
	s, p := v.s, v.s.meta.App.Policy
	if !s.meta.Retention.Config.enabled() || v.prepared != nil || v.bank != s.activeBank() || v.generation != s.activeGeneration() || v.index != s.meta.Applied || next.SourceIndex != v.index || next.SourceHash != s.meta.ImageHash || next.validate(v.index+1) != nil || !retentionMonotone(s.meta.Retention.State, next) || next.RetiredCutID != s.meta.Gen.Publication.ID || len(command) == 0 || budget.Rows < 1 || budget.Bytes < 1 {
		return nil, work, ErrInvalid
	}
	if budget.Rows > p.MaxPageRows || budget.Bytes > p.MaxPageBytes || len(command) > s.limits.MaxEntryBytes-frameOverhead {
		return nil, work, ErrLimit
	}
	if section := s.meta.Retention.Cursor.Section; section > 0 && section < 6 {
		return nil, work, ErrLimit
	}
	if next.InstalledIndex > s.meta.Hard.GetCommit() || next.InstalledIndex > s.meta.Last {
		return nil, work, ErrInvalid
	}
	// Own copies, immutable metadata sealing and variable command comparison
	// are admitted before their allocation/encoding. One shared cumulative ledger.
	fixed := 1024 + 2*nativeRecoveryRootBytes(s.meta.Recovery) + 2*len(v.image) + 4*len(command) + 4*portableRetentionBytes(next) + 2*retentionViewBytes(next)
	if fixed > budget.Bytes {
		return nil, work, ErrLimit
	}
	work.Bytes = fixed
	if sha256.Sum256(v.image) != s.meta.ImageHash {
		return nil, work, v.fail(ErrCorrupt)
	}
	read := func(key []byte, maxBytes, factor int, inspect func([]byte) error) (opErr error) {
		if work.Records >= budget.Rows {
			return ErrLimit
		}
		work.Records++
		raw, closer, e := s.db.Get(key)
		if e != nil {
			return v.fail(storedReadFailure(e))
		}
		defer func() {
			if e := closer.Close(); e != nil {
				opErr = errors.Join(opErr, v.fail(e))
			}
		}()
		if len(raw) > maxBytes {
			return v.fail(ErrCorrupt)
		}
		if len(raw) > (budget.Bytes-work.Bytes-len(key)-64)/factor {
			return ErrLimit
		}
		work.Bytes += factor*len(raw) + len(key) + 64
		if e := ctx.Err(); e != nil {
			return e
		}
		return inspect(raw)
	}
	var entryHash [32]byte
	err = read(entryKey(next.InstalledIndex), s.limits.MaxEntryBytes, 3, func(raw []byte) error {
		_, _, sum, e := inspectEntry(next.InstalledIndex, raw, s.limits.MaxEntryBytes)
		if e != nil {
			return v.fail(e)
		}
		if raw[9] != 0 || !bytes.Equal(raw[frameOverhead:], command) {
			return ErrInvalid
		}
		entryHash = sum
		return nil
	})
	if err != nil {
		return nil, work, err
	}
	var bankHash [32]byte
	err = read(appIndexKey(recoveryRootTag+v.bank, v.index), appFrameBytes+min(p.MaxValueBytes, maxMetadataBytes), 8, func(raw []byte) error {
		data, deleted, e := inspectAppFrame(appIndexKey(recoveryRootTag+v.bank, v.index), raw, min(p.MaxValueBytes, maxMetadataBytes))
		if e != nil || deleted {
			return v.fail(ErrCorrupt)
		}
		bankHash = sha256.Sum256(data)
		if bankHash != nativeRecoveryRootDigest(s.meta.Recovery) {
			return v.fail(ErrCorrupt)
		}
		// Hash equality alone is not a caller-supplied roster: inspect the
		// authentic complete retained bank and exact current immutable owner.
		r, bank, e := readNativeRecoveryRoot(data, p, s.limits)
		if e != nil || !sameRecoveryReservation(r, s.meta.Recovery.Reservation) || !sameTerminalBank(bank, s.meta.Recovery.Terminal) {
			return v.fail(errors.Join(ErrCorrupt, e))
		}
		return nil
	})
	if err != nil {
		return nil, work, err
	}
	roots := append([]ApplicationRetentionRoot(nil), next.Roots...)
	// The source checkpoint is required independently of explicit anchors.
	roots = append(roots, ApplicationRetentionRoot{Index: v.index, ImageHash: next.SourceHash})
	for _, root := range roots {
		if root.Index < v.retention.RootFloor && !retainedAnchor(v.retention, root.Index, root.ImageHash, root.Data) || root.Data && root.Index < v.retention.DataFloor && !retainedAnchor(v.retention, root.Index, root.ImageHash, true) {
			return nil, work, ErrApplicationExpired
		}
		key := bankIndexKey(v.bank, appRootTag, root.Index)
		err = read(key, appFrameBytes+p.MaxImageBytes, 3, func(raw []byte) error {
			data, deleted, e := inspectAppFrame(key, raw, p.MaxImageBytes)
			if e != nil || deleted {
				return v.fail(errors.Join(ErrCorrupt, e))
			}
			if sha256.Sum256(data) != root.ImageHash {
				return ErrInvalid
			}
			return nil
		})
		if err != nil {
			return nil, work, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, work, err
	}
	out = &ApplicationRetentionAdmission{s: s, source: v, bank: v.bank, generation: v.generation, base: v.index, imageHash: next.SourceHash, entryHash: entryHash, commandHash: sha256.Sum256(command), previous: s.meta.Retention.State.ID, state: cloneRetentionState(next), workBytes: fixed}
	out.self = out
	return out, work, nil
}

// Called with publicationMu then Store.mu. All physical generation owners are
// registered views/exports/imports/claims or the publication capture excluded by
// publicationMu. Numerical generation reference counts provide no exemption.
func (a *ApplicationRetentionAdmission) admitLocked(s *Store, b ApplicationBatch) error {
	if a == nil || a.self != a || a.s != s || a.source == nil || !s.meta.Retention.Config.enabled() || a.bank != s.activeBank() || a.generation != s.activeGeneration() || a.base != s.meta.Applied || a.imageHash != s.meta.ImageHash || a.previous != s.meta.Retention.State.ID || a.state.validate(s.meta.Applied+1) != nil || !retentionMonotone(s.meta.Retention.State, a.state) || a.state.RetiredCutID != s.meta.Gen.Publication.ID || sha256.Sum256(b.Image) != a.imageHash || len(b.Writes) != 0 {
		return ErrInvalid
	}
	if s.applicationImport != nil || s.applicationVerifier || len(s.applicationExports) != 0 || len(s.applicationSnapshotClaims) != 0 {
		return ErrLimit
	}
	for owner := s.applicationViews; owner != nil; owner = owner.next {
		if owner != a.source || owner.self != owner || owner.s != s || owner.bank != a.bank || owner.generation != a.generation || owner.index != a.base {
			return ErrLimit
		}
	}
	return nil
}

// RetentionAnchor reads only an exact image anchor from this view's authentic
// installed plan. It never opens an old cut, exposes old data or overrides a
// RootAt expiry floor. Expected coordinates supply no authority.
func (v *ApplicationView) RetentionAnchor(ctx context.Context, planID [32]byte, installedIndex, rootIndex uint64, maxImageBytes int) (ApplicationRoot, error) {
	if err := v.lock(ctx); err != nil {
		return ApplicationRoot{}, err
	}
	defer v.unlock()
	if !v.s.meta.Retention.Config.enabled() || planID == ([32]byte{}) || planID != v.retention.ID || installedIndex != v.retention.InstalledIndex || installedIndex > v.index || maxImageBytes < 0 {
		return ApplicationRoot{}, ErrInvalid
	}
	if maxImageBytes > v.s.meta.App.Policy.MaxImageBytes {
		return ApplicationRoot{}, ErrLimit
	}
	var root *ApplicationRetentionRoot
	for i := range v.retention.Roots {
		if v.retention.Roots[i].Index == rootIndex {
			root = &v.retention.Roots[i]
			break
		}
	}
	if root == nil {
		return ApplicationRoot{}, ErrApplicationExpired
	}
	image, err := v.historyPayloadAnchor(ctx, rootIndex, v.s.meta.App.Policy.MaxImageBytes, maxImageBytes)
	if err != nil {
		return ApplicationRoot{}, err
	}
	if sha256.Sum256(image) != root.ImageHash {
		return ApplicationRoot{}, v.fail(ErrCorrupt)
	}
	return ApplicationRoot{Generation: v.generation, Index: rootIndex, Image: image, ImageHash: root.ImageHash}, nil
}
