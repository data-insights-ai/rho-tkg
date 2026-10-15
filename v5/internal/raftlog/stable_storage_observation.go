package raftlog

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/cockroachdb/pebble/v2"
	pb "go.etcd.io/raft/v3/raftpb"
)

// StableStorageKind distinguishes two actual synchronous storage operations.
// These internal embedding diagnostics are not consensus/application authority,
// a public consumer API, a graph cut or a power-loss simulation. Checkpoint,
// publication and snapshot activation are intentionally not aliases for either.
type StableStorageKind uint8

const (
	StableStorageReady StableStorageKind = iota + 1
	StableStorageApplication
)

// StableStorageSelection identifies one live diagnostic arm. CommandSHA256 is
// inert selection input; only exact unique membership in the actual native
// batch permits an observation. A decoded command cannot authorize persistence.
type StableStorageSelection struct {
	Cookie        [16]byte
	Sequence      uint64
	CommandSHA256 [32]byte
}

// StableStorageWork is a nonrefundable conservative hashing/receipt allowance.
// It funds existing borrowed native input, not a quota-sized allocation or RSS.
type StableStorageWork struct {
	Entries, Bytes, OutputBytes uint64
}

// StableStorageObservation owns only value fields. Stage1 is immediately before
// the actual Commit(pebble.Sync). Stage2 follows that call; Synced is true ONLY
// when it returned nil. Stage0 records ambiguous membership and never qualifies
// a storage boundary. A successful Sync is not a quorum decision, application
// result or operation ACK; later Close/Restore may still fail. No mutable native
// entry/batch/metadata bytes are exposed.
type StableStorageObservation struct {
	Selection                            StableStorageSelection
	Kind                                 StableStorageKind
	Stage                                uint8
	Synced                               bool
	Matches, EntryCount, Position        uint64
	FirstIndex, LastIndex                uint64
	Index, Term, EntryType, CommandBytes uint64
	CommandSHA256, EntryManifestSHA256   [32]byte
	BatchBytes                           uint64
	BatchPayloadSHA256                   [32]byte
	Work                                 StableStorageWork
}

// StableStorageObserver is an optional internal diagnostic capability of the
// existing Config.FS owner, captured immutably at Open. Ordinary filesystems
// take the original direct commit path with no hashing, reads or allocations.
//
// As with Config.FS calls, these callbacks run under Store.mu: implementations
// MUST NOT call Store methods or retain/re-enter its locks. Their own bounded
// owner supplies cancellation/lifetime, prepayment and nonblocking cleanup;
// it must release its own mutex before an intentional bounded pause. No callback
// error can reject native work or invent a transaction outcome. Exhaustion and
// cancellation withhold diagnostic qualification while persistence continues.
type StableStorageObserver interface {
	StableStorageSelection(StableStorageKind) (StableStorageSelection, bool)
	ReserveStableStorageObservation(StableStorageSelection, StableStorageWork) bool
	ObserveStableStorage(StableStorageObservation)
}

// Caller has already validated the native Ready or the retained application
// entry and constructed this exact batch. There is no observer-driven mutation,
// retry, alternative entry or additional storage read at this seam.
func (s *Store) commitStableBatch(batch *pebble.Batch, kind StableStorageKind, entries []*pb.Entry) error {
	observer := s.storageObserver
	if observer == nil || len(entries) == 0 || kind != StableStorageReady && kind != StableStorageApplication {
		return batch.Commit(pebble.Sync)
	}
	selection, selected := observer.StableStorageSelection(kind)
	if !selected || selection.Cookie == ([16]byte{}) || selection.Sequence == 0 || selection.CommandSHA256 == ([32]byte{}) || len(entries) > 4096 {
		return batch.Commit(pebble.Sync)
	}
	// MaxReadyBytes bounds the already validated entry input, including the
	// application's single retained entry. Metadata/manifest/receipt ownership
	// is prepaid before reading command bytes or traversing the batch payload.
	work := StableStorageWork{Entries: unsignedLimit(len(entries)) + 1,
		Bytes:       unsignedLimit(batch.Len()) + unsignedLimit(s.limits.MaxReadyBytes) + 64*unsignedLimit(len(entries)+1) + 1024,
		OutputBytes: 2 * 512}
	if !observer.ReserveStableStorageObservation(selection, work) {
		return batch.Commit(pebble.Sync)
	}
	event, complete := stableStorageIdentity(kind, selection, entries, s.limits, work)
	if !complete || event.Matches == 0 {
		return batch.Commit(pebble.Sync) // Nonmatching traffic never changes.
	}
	// Repr only fills the existing header count for this populated native batch.
	// Its first8 bytes are Pebble's assigned sequence, which Commit may change.
	// Hash the exact count+record payload, excluding ONLY that mutable sequence.
	repr := batch.Repr()
	if len(repr) < 12 {
		return batch.Commit(pebble.Sync)
	}
	event.BatchBytes = unsignedLimit(len(repr))
	event.BatchPayloadSHA256 = sha256.Sum256(repr[8:])
	if event.Matches != 1 {
		// Full manifest is real, but no one entry can stand for both matches.
		// Diagnostic owners report unavailable/ambiguous; native Sync proceeds.
		observer.ObserveStableStorage(event)
		return batch.Commit(pebble.Sync)
	}
	event.Stage = 1
	observer.ObserveStableStorage(event)
	err := batch.Commit(pebble.Sync)
	event.Stage, event.Synced = 2, err == nil
	observer.ObserveStableStorage(event)
	return err
}

func stableStorageIdentity(kind StableStorageKind, selection StableStorageSelection, entries []*pb.Entry, limits Limits, work StableStorageWork) (StableStorageObservation, bool) {
	event := StableStorageObservation{Selection: selection, Kind: kind, EntryCount: unsignedLimit(len(entries)), Work: work}
	manifest := sha256.New()
	_, _ = manifest.Write([]byte("rho-tkg:stable-storage-entry-manifest:v1\x00"))
	var count [8]byte
	binary.BigEndian.PutUint64(count[:], event.EntryCount)
	_, _ = manifest.Write(count[:])
	var total uint64
	for i, entry := range entries {
		if entry == nil || entry.GetIndex() == 0 || entry.GetTerm() == 0 || entry.GetType() < pb.EntryNormal || entry.GetType() > pb.EntryConfChangeV2 || len(entry.GetData()) > limits.MaxEntryBytes-frameOverhead {
			return StableStorageObservation{}, false
		}
		size := unsignedLimit(len(entry.GetData()))
		if size > unsignedLimit(limits.MaxReadyBytes)-total {
			return StableStorageObservation{}, false
		}
		total += size
		digest := sha256.Sum256(entry.GetData())
		var identity [64]byte
		binary.BigEndian.PutUint64(identity[0:8], entry.GetIndex())
		binary.BigEndian.PutUint64(identity[8:16], entry.GetTerm())
		entryType := uint64(entry.GetType()) // #nosec G115 -- native entry enum range validated above.
		binary.BigEndian.PutUint64(identity[16:24], entryType)
		binary.BigEndian.PutUint64(identity[24:32], size)
		copy(identity[32:], digest[:])
		_, _ = manifest.Write(identity[:])
		if i == 0 {
			event.FirstIndex = entry.GetIndex()
		}
		event.LastIndex = entry.GetIndex()
		if digest != selection.CommandSHA256 {
			continue
		}
		event.Matches++
		if event.Matches == 1 {
			event.Position, event.Index, event.Term = unsignedLimit(i)+1, entry.GetIndex(), entry.GetTerm()
			event.EntryType, event.CommandBytes, event.CommandSHA256 = entryType, size, digest
		} else {
			// An ambiguous selector cannot retain the first entry as authority.
			event.Position, event.Index, event.Term, event.EntryType, event.CommandBytes = 0, 0, 0, 0, 0
			event.CommandSHA256 = [32]byte{}
		}
	}
	copy(event.EntryManifestSHA256[:], manifest.Sum(nil))
	return event, true
}
