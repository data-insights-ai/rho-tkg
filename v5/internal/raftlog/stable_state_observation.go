package raftlog

import (
	"crypto/sha256"
	"math"

	"github.com/cockroachdb/pebble/v2"
)

// StableStateKind names actual native atomic batches, not fields within them.
// None is a business decision/ACK, portable authority or power-loss model.
type StableStateKind uint8

const (
	StableStateCheckpoint StableStateKind = iota + 1
	StableStatePublication
	StableStateImportBegin
	StableStateImportAppend
	StableStateImportSeal
	StableStateActivation
	StableStateRetentionPage
	StableStateImportRetire
	StableStateGenerationClear
	StableStateDormantClear
	StableStateInitialize
)

// StableStateIdentity copies already validated native coordinates. Applied and
// the active image/bank describe the actual prospective local metadata. Import
// coordinates describe its separate dormant target; they do not activate it.
// CurrentPublicationID may name an earlier publication. ManifestID is explicitly
// zero when the operation has no actual retained manifest; it is never guessed.
// ProgressSHA256 is the native import stream/cursor digest, not a fresh proof.
// SourceCutID/Image identify the actual import/publication source when present;
// an installed retention plan does not pretend its retired cut is that source.
type StableStateIdentity struct {
	Applied, SourceIndex, ActiveGeneration, TargetGeneration, Sequence    uint64
	ActiveBank, TargetBank, Section                                       uint8
	Final, Verified                                                       bool
	ImageSHA256, CurrentPublicationID, ManifestID, PlanID, ProgressSHA256 [32]byte
	SourceCutID, SourceImageSHA256                                        [32]byte
}

// StableStateSelection binds one live diagnostic arm to the complete actual
// identity supplied by native code; it grants no publication or deletion right.
type StableStateSelection struct {
	Cookie   [16]byte
	Sequence uint64
	Identity StableStateIdentity
}

// StableStateObservation binds the same complete native batch before and after
// Commit(pebble.Sync). Stage2.Synced means ONLY the actual call returned nil.
// Later batch Close, Restore, ownership cleanup and acknowledgments may fail.
// Payload excludes only Pebble's mutable8-byte sequence header. Metadata hashes
// cover the exact complete encoded local envelopes, not caller root summaries.
type StableStateObservation struct {
	Selection                            StableStateSelection
	Kind                                 StableStateKind
	Stage                                uint8
	Synced                               bool
	BeforeMetadataBytes, MetadataBytes   uint64
	BeforeMetadataSHA256, MetadataSHA256 [32]byte
	BatchBytes                           uint64
	BatchPayloadSHA256                   [32]byte
	Work                                 StableStorageWork
}

// StableStateObserver is an optional internal capability of Config.FS, captured
// once at Open. It is diagnostic scheduling only. Callbacks run under Store.mu
// and MUST NOT call/reenter Store methods or retain native buffers/locks. The
// owner supplies finite cancellation, grant and receipt ownership, releasing its
// own locks before any pause. Missing selection, mismatched identity or denied
// grants leave native persistence unchanged and cannot qualify a fault cell.
type StableStateObserver interface {
	StableStateSelection(StableStateKind, StableStateIdentity) (StableStateSelection, bool)
	ReserveStableStateObservation(StableStateSelection, StableStorageWork) bool
	ObserveStableState(StableStateObservation)
}

type stableStateCommit struct {
	kind     StableStateKind
	identity StableStateIdentity
}

func stateIdentity(m metadata) StableStateIdentity {
	identity := StableStateIdentity{Applied: m.Applied, ActiveBank: m.Gen.Active, TargetBank: m.Gen.Active, ImageSHA256: m.ImageHash, CurrentPublicationID: m.Gen.Publication.ID}
	if m.Gen.Limits.enabled() {
		identity.ActiveGeneration = m.Gen.Banks[m.Gen.Active].Generation
		identity.TargetGeneration = identity.ActiveGeneration
	}
	return identity
}

func importStateIdentity(m metadata, i *ApplicationImport) StableStateIdentity {
	identity := stateIdentity(m)
	identity.SourceIndex, identity.ManifestID = i.manifest.Index, i.id
	identity.SourceCutID, identity.SourceImageSHA256 = i.manifest.CutID, i.manifest.ImageHash
	identity.TargetBank, identity.TargetGeneration = i.bank, i.generation
	identity.Sequence, identity.ProgressSHA256 = i.sequence, i.state.digest
	identity.Final, identity.Verified = i.final, i.verified
	return identity
}

// All callers have already validated the native operation and built its exact
// writes. No observer may replace the batch, import, plan, claim or cut identity.
func (s *Store) commitState(m metadata, batch *pebble.Batch, kind StableStateKind, identity StableStateIdentity) error {
	state := stableStateCommit{kind: kind, identity: identity}
	return s.commitObservedState(m, batch, 0, nil, &state)
}

func (s *Store) commitImportState(m metadata, batch *pebble.Batch, kind StableStateKind, i *ApplicationImport) error {
	if s.stateObserver == nil {
		return s.commit(m, batch)
	}
	return s.commitState(m, batch, kind, importStateIdentity(m, i))
}

func (s *Store) commitStableStateBatch(batch *pebble.Batch, prospective []byte, state stableStateCommit) error {
	observer := s.stateObserver
	if observer == nil || state.kind < StableStateCheckpoint || state.kind > StableStateInitialize {
		return batch.Commit(pebble.Sync)
	}
	selection, selected := observer.StableStateSelection(state.kind, state.identity)
	if !selected || selection.Cookie == ([16]byte{}) || selection.Sequence == 0 || selection.Identity != state.identity {
		return batch.Commit(pebble.Sync)
	}
	// Fund full before-envelope encoding (including bounded protobuf/retention
	// ownership), native checksum + observation hashing, and the borrowed exact
	// prospective envelope/batch BEFORE diagnostic serialization/traversal. This
	// is a theoretical grant, never a quota-sized allocation or measured RSS.
	grant := uint64(8*maxMetadataBytes + 4096)
	for _, size := range [2]uint64{unsignedLimit(len(prospective)), unsignedLimit(batch.Len())} {
		if size > (math.MaxUint64-grant)/4 {
			return batch.Commit(pebble.Sync) // Unfundable diagnostic, native unchanged.
		}
		grant += 4 * size
	}
	work := StableStorageWork{Entries: 3, Bytes: grant, OutputBytes: 2 * 1024}
	if !observer.ReserveStableStateObservation(selection, work) {
		return batch.Commit(pebble.Sync)
	}
	before, err := encodeMeta(s.meta)
	if err != nil || len(before) > maxMetadataBytes || len(prospective) > maxMetadataBytes {
		return batch.Commit(pebble.Sync) // A diagnostic cannot create native errors.
	}
	repr := batch.Repr()
	if len(repr) < 12 {
		return batch.Commit(pebble.Sync)
	}
	event := StableStateObservation{Selection: selection, Kind: state.kind, Stage: 1,
		BeforeMetadataBytes: unsignedLimit(len(before)), MetadataBytes: unsignedLimit(len(prospective)),
		BeforeMetadataSHA256: sha256.Sum256(before), MetadataSHA256: sha256.Sum256(prospective),
		BatchBytes: unsignedLimit(len(repr)), BatchPayloadSHA256: sha256.Sum256(repr[8:]), Work: work}
	observer.ObserveStableState(event)
	err = batch.Commit(pebble.Sync)
	event.Stage, event.Synced = 2, err == nil
	observer.ObserveStableState(event)
	return err
}
