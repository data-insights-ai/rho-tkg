package raftlog

import (
	"crypto/sha256"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	pb "go.etcd.io/raft/v3/raftpb"
)

// One preowned arm and four fixed receipt slots. These storage-unit callbacks
// do not claim a six-process graph schedule or native semantic authority.
type stableObservationFS struct {
	vfs.FS
	kind      StableStorageKind
	selection StableStorageSelection
	work      StableStorageWork
	reserves  int
	deny      bool
	events    [4]StableStorageObservation
	count     int
	overflow  bool
}

func (f *stableObservationFS) StableStorageSelection(kind StableStorageKind) (StableStorageSelection, bool) {
	return f.selection, kind == f.kind
}
func (f *stableObservationFS) ReserveStableStorageObservation(selection StableStorageSelection, work StableStorageWork) bool {
	f.reserves++
	f.work = work // Failed/unknown observation grants are never refunded.
	return !f.deny && selection == f.selection && work.Entries <= 4097 && work.Bytes <= 128<<20 && work.OutputBytes == 1024
}
func (f *stableObservationFS) ObserveStableStorage(event StableStorageObservation) {
	if f.count == len(f.events) {
		f.overflow = true
		return
	}
	f.events[f.count] = event
	f.count++
	// This local value cannot mutate the actual entry, arm, native batch or the
	// event later returned after Sync. No borrowed slices are present.
	event.Index++
	event.Selection.Sequence++
}
func (f *stableObservationFS) arm(kind StableStorageKind, command string) {
	f.kind = kind
	f.selection = StableStorageSelection{Cookie: [16]byte{1}, Sequence: f.selection.Sequence + 1, CommandSHA256: sha256.Sum256([]byte(command))}
	f.events, f.count, f.overflow = [4]StableStorageObservation{}, 0, false
}

func stableObservationPair(t *testing.T, f *stableObservationFS, kind StableStorageKind, index, term, position, count uint64) StableStorageObservation {
	t.Helper()
	if f.overflow || f.count != 2 || f.reserves == 0 {
		t.Fatal("missing funded bounded actual Sync pair", f.count, f.overflow)
	}
	before, after := f.events[0], f.events[1]
	if before.Stage != 1 || before.Synced || after.Stage != 2 || !after.Synced || before.Kind != kind || before.Selection != f.selection || before.Matches != 1 || before.Index != index || before.Term != term || before.Position != position || before.EntryCount != count || before.CommandSHA256 != f.selection.CommandSHA256 || before.EntryManifestSHA256 == ([32]byte{}) || before.BatchPayloadSHA256 == ([32]byte{}) || before.BatchBytes < 12 || before.Work != f.work {
		t.Fatal("actual entry/batch/manifest/arm lineage", before, after)
	}
	after.Stage, after.Synced = before.Stage, before.Synced
	if before != after {
		t.Fatal("before/after Sync substituted native batch identity")
	}
	return before
}

func TestStableStorageObservationBindsCompleteReadyAndUniqueTarget(t *testing.T) {
	fs := &stableObservationFS{FS: vfs.NewCrashableMem()}
	s := initialized(t, fs)
	fs.arm(StableStorageReady, "selected")
	entries := []*pb.Entry{ent(2, 2, "other"), ent(3, 2, "selected")}
	persist(t, s, 2, 1, entries...)
	event := stableObservationPair(t, fs, StableStorageReady, 3, 2, 2, 2)
	if event.FirstIndex != 2 || event.LastIndex != 3 {
		t.Fatal("complete multi-entry Ready boundaries")
	}
	changed := []*pb.Entry{ent(2, 2, "changed-other"), ent(3, 2, "selected")}
	comparison, valid := stableStorageIdentity(StableStorageReady, fs.selection, changed, s.limits, fs.work)
	if !valid || comparison.CommandSHA256 != event.CommandSHA256 || comparison.EntryManifestSHA256 == event.EntryManifestSHA256 {
		t.Fatal("selected entry hash stood in for another Ready entry")
	}
	entries[1].Data[0] = 'X'
	stored, err := s.Entries(2, 4, 1<<20)
	if err != nil || len(stored) != 2 || string(stored[0].GetData()) != "other" || string(stored[1].GetData()) != "selected" || fs.events[1].Index != 3 {
		t.Fatal("observer or caller changed actual persisted entry", err)
	}
	fs.arm(StableStorageReady, "same")
	persist(t, s, 2, 1, ent(4, 2, "same"), ent(5, 2, "same"))
	if fs.count != 1 || fs.events[0].Stage != 0 || fs.events[0].Synced || fs.events[0].Matches != 2 || fs.events[0].Index != 0 || fs.events[0].CommandSHA256 != ([32]byte{}) || fs.events[0].EntryCount != 2 {
		t.Fatal("ambiguous target qualified a storage boundary")
	}
	fs.arm(StableStorageReady, "absent")
	prior := fs.reserves
	persist(t, s, 2, 1, ent(6, 2, "unrelated"))
	if fs.count != 0 || fs.reserves != prior+1 {
		t.Fatal("nonmatching traffic changed or refunded its inspection grant")
	}
	fs.arm(StableStorageReady, "next")
	fs.deny = true
	persist(t, s, 2, 1, ent(7, 2, "next"))
	if fs.count != 0 {
		t.Fatal("exhausted diagnostic owner supplied a receipt")
	}
	if last, err := s.LastIndex(); err != nil || last != 7 {
		t.Fatal("diagnostic refusal changed native persistence", last, err)
	}
}

func TestStableStorageObservationApplicationUsesRetainedEntryAndKeepsOldRoot(t *testing.T) {
	fs := &stableObservationFS{FS: vfs.NewCrashableMem()}
	s := openApplication(t, fs, DefaultApplicationPolicy(1))
	fs.arm(StableStorageApplication, "old")
	first := applyApplication(t, s, "old", KV{Key: []byte("key"), Value: []byte("old")})
	before := stableObservationPair(t, fs, StableStorageApplication, first, 2, 1, 1)
	old := viewApplication(t, s, first)
	fs.arm(StableStorageApplication, "new")
	second := applyApplication(t, s, "new", KV{Key: []byte("key"), Value: []byte("new")})
	after := stableObservationPair(t, fs, StableStorageApplication, second, 2, 1, 1)
	current := viewApplication(t, s, second)
	getApplication(t, old, "key", "old", true, false)
	getApplication(t, current, "key", "new", true, false)
	if before.EntryManifestSHA256 == after.EntryManifestSHA256 || before.BatchPayloadSHA256 == after.BatchPayloadSHA256 {
		t.Fatal("later application batch reused old native identity")
	}
	record, err := s.ApplicationRecord(t.Context(), second, true, 1024)
	if err != nil || string(record) != "outcome:new" {
		t.Fatal("observation mutated actual retained outcome", err)
	}
	root, err := old.Root()
	if err != nil || root.Index != first || string(root.Image) != "old" {
		t.Fatal("old actual root changed after new installation", err)
	}
}
