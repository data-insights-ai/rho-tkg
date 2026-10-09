package raftlog

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func openTest(t *testing.T, fs vfs.FS, limits Limits) *Store {
	t.Helper()
	s, err := Open(Config{Dir: "db", FS: fs, Create: true, Limits: limits})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil && s.poison == nil {
			t.Error(err)
		}
	})
	return s
}
func ent(i, term uint64, data string) *pb.Entry {
	return &pb.Entry{Index: new(i), Term: new(term), Type: pb.EntryNormal.Enum(), Data: []byte(data)}
}
func persist(t *testing.T, s *Store, term, commit uint64, es ...*pb.Entry) {
	t.Helper()
	if err := s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(term), Commit: new(commit)}, Entries: es}); err != nil {
		t.Fatal(err)
	}
}
func initialized(t *testing.T, fs vfs.FS) *Store {
	t.Helper()
	s := openTest(t, fs, Limits{})
	if err := s.Initialize([]uint64{1, 2, 3}, []byte("empty")); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestStorageContractRecoveryAndReplacement(t *testing.T) {
	fs := vfs.NewCrashableMem()
	s := initialized(t, fs)
	persist(t, s, 2, 2, ent(2, 2, "two"), ent(3, 2, "obsolete"), ent(4, 2, "phantom"))
	persist(t, s, 3, 3, ent(3, 3, "new"))
	if last, _ := s.LastIndex(); last != 3 {
		t.Fatalf("replacement left suffix: %d", last)
	}
	if _, err := s.Term(4); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatal(err)
	}
	if _, err := s.Entries(1, 2, 100); !errors.Is(err, raft.ErrCompacted) {
		t.Fatal(err)
	}
	if term, err := s.Term(1); err != nil || term != 1 {
		t.Fatalf("dummy: %d %v", term, err)
	}
	if _, err := s.Term(0); !errors.Is(err, raft.ErrCompacted) {
		t.Fatal(err)
	}
	if es, err := s.Entries(2, 4, math.MaxUint64); err != nil || len(es) != 2 || string(es[1].Data) != "new" {
		t.Fatalf("entries: %v %v", es, err)
	}
	if es, err := s.Entries(2, 4, 0); err != nil || len(es) != 1 {
		t.Fatalf("at least first: %v %v", es, err)
	}
	if es, err := s.Entries(3, 3, 100); err != nil || len(es) != 0 {
		t.Fatal(es, err)
	}
	if _, err := s.Entries(4, 3, 100); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := s.Entries(2, 5, 100); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatal(err)
	}
	h, cs, err := s.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	cs.Voters[0] = 999
	h.Term = new(uint64(999))
	h, cs, err = s.InitialState()
	if err != nil || h.GetTerm() != 3 || cs.Voters[0] != 1 {
		t.Fatal(h, cs, err)
	}
	if err := s.SaveCheckpoint(3, cs, []byte("applied three")); err != nil {
		t.Fatal(err)
	}
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	if first, _ := s.FirstIndex(); first != 4 {
		t.Fatalf("first %d", first)
	}
	if term, err := s.Term(3); err != nil || term != 3 {
		t.Fatal(term, err)
	}
	if err := s.PublishSnapshot(); !errors.Is(err, raft.ErrSnapOutOfDate) {
		t.Fatal(err)
	}
	if err := s.Scrub(); err != nil {
		t.Fatal(err)
	}
	crash := fs.CrashClone(vfs.CrashCloneCfg{})
	r, err := Open(Config{Dir: "db", FS: crash})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	idx, image, err := r.Checkpoint()
	if err != nil || idx != 3 || string(image) != "applied three" {
		t.Fatalf("checkpoint: %d %s %v", idx, image, err)
	}
	snap, err := r.Snapshot()
	if err != nil || snap.GetMetadata().GetIndex() != 3 || string(snap.Data) != "applied three" {
		t.Fatal(snap, err)
	}
	snap.Data[0] = 'X'
	snap2, _ := r.Snapshot()
	if snap2.Data[0] == 'X' {
		t.Fatal("borrowed snapshot")
	}
	persist(t, r, 4, 4, ent(4, 4, "after snapshot"))
	if err := r.Scrub(); err != nil {
		t.Fatal(err)
	}
}

func TestBoundedPagesAndLimits(t *testing.T) {
	l := DefaultLimits()
	l.MaxEntryBytes = 256
	l.MaxReadBytes = 512
	l.MaxReadyBytes = 1024
	l.MaxReadEntries = 2
	l.MaxRetainedBytes = 4096
	l.MaxRetainedEntries = 10
	l.MaxSnapshotBytes = 128
	s := openTest(t, vfs.NewMem(), l)
	if err := s.Initialize([]uint64{1}, nil); err != nil {
		t.Fatal(err)
	}
	persist(t, s, 2, 6, ent(2, 2, "a"), ent(3, 2, "b"), ent(4, 2, "c"), ent(5, 2, "d"), ent(6, 2, "e"))
	for lo := uint64(2); lo <= 6; {
		es, err := s.Entries(lo, 7, math.MaxUint64)
		if err != nil || len(es) > 2 {
			t.Fatal(es, err)
		}
		lo = es[len(es)-1].GetIndex() + 1
	}
	if err := s.Persist(raft.Ready{Entries: []*pb.Entry{ent(7, 2, string(make([]byte, 300)))}}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if err := s.SaveCheckpoint(6, &pb.ConfState{Voters: []uint64{1}}, make([]byte, 129)); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if err := s.SaveCheckpoint(7, &pb.ConfState{Voters: []uint64{1}}, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.SaveCheckpoint(0, &pb.ConfState{}, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.Initialize([]uint64{1}, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	bad := l
	bad.MaxReadBytes = bad.MaxEntryBytes
	if err := bad.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	bad = l
	bad.MaxRetainedEntries = 1
	extra, err := Open(Config{Dir: "bad", FS: vfs.NewMem(), Create: true, Limits: bad})
	if err != nil {
		t.Fatal(err)
	}
	if err := extra.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Scrub(); err != nil {
		t.Fatal(err)
	}
}

func TestRejectInvalidDurableTransitions(t *testing.T) {
	s := initialized(t, vfs.NewMem())
	persist(t, s, 2, 2, ent(2, 2, "committed"), ent(3, 2, "suffix"))
	cases := []raft.Ready{
		{HardState: &pb.HardState{Term: new(uint64(1)), Commit: new(uint64(2))}},
		{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(1))}},
		{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(4))}},
		{Entries: []*pb.Entry{ent(2, 2, "rewrite committed")}},
		{Entries: []*pb.Entry{ent(5, 2, "hole")}},
		{Entries: []*pb.Entry{ent(4, 3, "future term")}},
		{Entries: []*pb.Entry{ent(4, 2, "four"), ent(6, 2, "gap")}},
		{Entries: []*pb.Entry{nil}},
		{Entries: []*pb.Entry{{Index: new(uint64(4)), Term: new(uint64(2)), Type: pb.EntryType(99).Enum()}}},
		{Snapshot: &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(1)), Term: new(uint64(1))}}},
	}
	for i, rd := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			if err := s.Persist(rd); !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
			if last, _ := s.LastIndex(); last != 3 {
				t.Fatal("invalid mutation changed log")
			}
		})
	}
	persist(t, s, 2, 2)
	if err := s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(3)), Vote: new(uint64(1)), Commit: new(uint64(2))}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(3)), Vote: new(uint64(2)), Commit: new(uint64(2))}}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for _, cs := range []*pb.ConfState{nil, {Voters: []uint64{0}}, {Voters: []uint64{1, 1}}, {Voters: []uint64{1}, AutoLeave: new(true)}, {Voters: []uint64{1}, Learners: []uint64{1}}} {
		if err := s.SaveCheckpoint(2, cs, nil); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
}

func TestIncomingSnapshotRecovery(t *testing.T) {
	s := initialized(t, vfs.NewMem())
	persist(t, s, 2, 2, ent(2, 2, "old"), ent(3, 2, "uncommitted"))
	cs := &pb.ConfState{Voters: []uint64{1, 2}, VotersOutgoing: []uint64{1, 2, 3}, LearnersNext: []uint64{3}}
	snap := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(5)), Term: new(uint64(3)), ConfState: cs}, Data: []byte("state at five")}
	if err := s.Persist(raft.Ready{Snapshot: snap, HardState: &pb.HardState{Term: new(uint64(3)), Commit: new(uint64(6))}, Entries: []*pb.Entry{ent(6, 3, "six")}}); err != nil {
		t.Fatal(err)
	}
	if idx, image, err := s.Checkpoint(); err != nil || idx != 5 || string(image) != "state at five" {
		t.Fatal(idx, image, err)
	}
	if _, err := s.Term(3); !errors.Is(err, raft.ErrCompacted) {
		t.Fatal(err)
	}
	if err := s.Scrub(); err != nil {
		t.Fatal(err)
	}
	_, got, err := s.InitialState()
	if err != nil || cs.Equivalent(got) != nil {
		t.Fatal(got, err)
	}
}

func TestMetadataMissingCorruptAndClosed(t *testing.T) {
	if _, err := Open(Config{Dir: " ", Create: true}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := Open(Config{Dir: "missing", FS: vfs.NewMem()}); !errors.Is(err, pebble.ErrDBDoesNotExist) {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Store) error{
		func(s *Store) error { return s.db.Delete(metaKey, pebble.Sync) },
		func(s *Store) error { return s.db.Set(metaKey, []byte("bad"), pebble.Sync) },
		func(s *Store) error {
			m := s.meta
			m.Applied = 99
			b, _ := encodeMeta(m)
			return s.db.Set(metaKey, b, pebble.Sync)
		},
	} {
		fs := vfs.NewMem()
		s := initialized(t, fs)
		if err := mutate(s); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(Config{Dir: "db", FS: fs}); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
		if _, err := Open(Config{Dir: "db", FS: fs, Create: true}); !errors.Is(err, pebble.ErrDBAlreadyExists) {
			t.Fatal(err)
		}
	}
	s := initialized(t, vfs.NewMem())
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.InitialState(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := s.FirstIndex(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := s.LastIndex(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := s.Term(1); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := s.Entries(1, 2, 0); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := s.Persist(raft.Ready{}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, _, err := s.Checkpoint(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := s.Initialize([]uint64{1}, nil); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := s.SaveCheckpoint(1, nil, nil); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := s.PublishSnapshot(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := s.Scrub(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestEntryCorruptionFailsClosed(t *testing.T) {
	for _, kind := range []string{"missing", "payload", "chain"} {
		t.Run(kind, func(t *testing.T) {
			s := initialized(t, vfs.NewMem())
			persist(t, s, 2, 4, ent(2, 2, "a"), ent(3, 2, "b"), ent(4, 2, "c"))
			switch kind {
			case "missing":
				if err := s.db.Delete(entryKey(3), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			case "payload":
				b, closer, err := s.db.Get(entryKey(3))
				if err != nil {
					t.Fatal(err)
				}
				b = bytes.Clone(b)
				if err := closer.Close(); err != nil {
					t.Fatal(err)
				}
				b[len(b)-1] ^= 1
				if err := s.db.Set(entryKey(3), b, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			case "chain":
				b, _ := encodeEntry(ent(3, 2, "b"), [32]byte{1})
				if err := s.db.Set(entryKey(3), b, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Scrub(); !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
			if _, err := s.LastIndex(); !errors.Is(err, ErrPoisoned) {
				t.Fatal(err)
			}
		})
	}
}

func TestSyncedCrashImages(t *testing.T) {
	fs := vfs.NewCrashableMem()
	s := initialized(t, fs)
	persist(t, s, 2, 2, ent(2, 2, "durable"))
	_, cs, _ := s.InitialState()
	for _, at := range []string{"log", "checkpoint", "snapshot"} {
		t.Run(at, func(t *testing.T) {
			if at == "checkpoint" {
				if err := s.SaveCheckpoint(2, cs, []byte("two")); err != nil {
					t.Fatal(err)
				}
			}
			if at == "snapshot" {
				if err := s.PublishSnapshot(); err != nil {
					t.Fatal(err)
				}
			}
			crashed := fs.CrashClone(vfs.CrashCloneCfg{})
			r, err := Open(Config{Dir: "db", FS: crashed})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			h, _, err := r.InitialState()
			if err != nil || h.GetCommit() != 2 {
				t.Fatal(h, err)
			}
			idx, image, err := r.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			if at == "log" {
				if idx != 1 || string(image) != "empty" {
					t.Fatal(idx, string(image))
				}
			} else if idx != 2 || string(image) != "two" {
				t.Fatal(idx, string(image))
			}
			if err := r.Scrub(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCodecRejectsMalformed(t *testing.T) {
	m := metadata{Hard: &pb.HardState{}, Conf: &pb.ConfState{}, Snap: &pb.Snapshot{}}
	b, err := encodeMeta(m)
	if err != nil {
		t.Fatal(err)
	}
	for n := range len(b) {
		if _, err := decodeMeta(b[:n], DefaultLimits()); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("prefix %d: %v", n, err)
		}
	}
	if _, err := decodeMeta(append(b, 0), DefaultLimits()); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if _, _, err := takeBlob([]byte{0}); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	b, _ = encodeEntry(ent(2, 1, "payload"), [32]byte{})
	for n := range frameOverhead {
		if _, _, _, err := decodeEntry(2, b[:n], 1024); !errors.Is(err, ErrCorrupt) {
			t.Fatal(n, err)
		}
	}
	got, _, _, err := decodeEntry(2, b, 1024)
	if err != nil || !proto.Equal(got, ent(2, 1, "payload")) {
		t.Fatal(got, err)
	}
}

func TestStrictUnknownFieldsAndLedgerIntegrity(t *testing.T) {
	s := initialized(t, vfs.NewMem())
	unknown := []byte{0xa0, 0x06, 1}
	h := &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(1))}
	h.ProtoReflect().SetUnknown(unknown)
	if err := s.Persist(raft.Ready{HardState: h}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	cs := &pb.ConfState{Voters: []uint64{1}}
	cs.ProtoReflect().SetUnknown(unknown)
	if err := s.SaveCheckpoint(1, cs, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for _, kind := range []string{"snapshot", "metadata", "conf", "entry"} {
		t.Run(kind, func(t *testing.T) {
			snap := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(2)), Term: new(uint64(2)), ConfState: &pb.ConfState{Voters: []uint64{1}}}}
			e := ent(2, 2, "data")
			rd := raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(2))}, Snapshot: snap}
			switch kind {
			case "snapshot":
				snap.ProtoReflect().SetUnknown(unknown)
			case "metadata":
				snap.Metadata.ProtoReflect().SetUnknown(unknown)
			case "conf":
				snap.Metadata.ConfState.ProtoReflect().SetUnknown(unknown)
			case "entry":
				rd.Snapshot = nil
				e.ProtoReflect().SetUnknown(unknown)
				rd.Entries = []*pb.Entry{e}
			}
			if err := s.Persist(rd); !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
			if last, _ := s.LastIndex(); last != 1 {
				t.Fatal(last)
			}
		})
	}
	persist(t, s, 2, 3, ent(2, 2, "two"), ent(3, 2, "three"))
	if err := s.Persist(raft.Ready{Entries: []*pb.Entry{ent(4, 1, "term regressed")}}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for _, bad := range []Limits{func() Limits { l := DefaultLimits(); l.MaxEntryBytes = math.MaxInt; return l }(), func() Limits { l := DefaultLimits(); l.MaxReadyBytes = math.MaxInt; return l }(), func() Limits { l := DefaultLimits(); l.MaxRetainedBytes = math.MaxUint64; return l }()} {
		if err := bad.Validate(); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if unsignedLimit(-1) != 0 {
		t.Fatal("negative limit")
	}
	for _, kind := range []string{"bytes", "hash", "image", "missing image"} {
		t.Run(kind, func(t *testing.T) {
			r := initialized(t, vfs.NewMem())
			persist(t, r, 2, 2, ent(2, 2, "two"))
			switch kind {
			case "bytes":
				r.meta.LogBytes++
			case "hash":
				r.meta.LastHash = [32]byte{1}
			case "image":
				if err := r.db.Set(imageKey, []byte("corrupt"), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			case "missing image":
				if err := r.db.Delete(snapshotKey, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			if err := r.Scrub(); !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
		})
	}
}

func TestMetadataOnlyAppendAndSerializedScrub(t *testing.T) {
	s := initialized(t, vfs.NewMem())
	image := bytes.Repeat([]byte("x"), 1<<20)
	if err := s.SaveCheckpoint(1, &pb.ConfState{Voters: []uint64{1, 2, 3}}, image); err != nil {
		t.Fatal(err)
	}
	b, closer, err := s.db.Get(metaKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > 2048 {
		t.Fatal("image embedded in metadata", len(b))
	}
	closer.Close()
	persist(t, s, 2, 2, ent(2, 2, "tiny"))
	if _, got, err := s.Checkpoint(); err != nil || !bytes.Equal(got, image) {
		t.Fatal("append changed checkpoint", err)
	}
	if s.Limits() != DefaultLimits() {
		t.Fatal("limits accessor mismatch")
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 10 {
				if err := s.Scrub(); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Go(func() {
		for i := uint64(3); i < 13; i++ {
			persist(t, s, 2, i, ent(i, 2, "write"))
			if err := s.SaveCheckpoint(i, &pb.ConfState{Voters: []uint64{1, 2, 3}}, image); err != nil {
				t.Error(err)
			}
			if err := s.PublishSnapshot(); err != nil {
				t.Error(err)
			}
		}
	})
	wg.Wait()
}

func TestIteratorFaultPreservesIOErrorAndPoison(t *testing.T) {
	l := DefaultLimits()
	l.CacheBytes = 1
	base := vfs.NewMem()
	toggle := &errorfs.Toggle{Injector: errorfs.InjectorFunc(func(op errorfs.Op) error {
		if op.Kind == errorfs.OpFileReadAt {
			return errorfs.ErrInjected
		}
		return nil
	})}
	s := openTest(t, errorfs.Wrap(base, toggle), l)
	if err := s.Initialize([]uint64{1}, nil); err != nil {
		t.Fatal(err)
	}
	persist(t, s, 2, 3, ent(2, 2, "two"), ent(3, 2, "three"))
	if err := s.db.Flush(); err != nil {
		t.Fatal(err)
	}
	toggle.On()
	err := s.checkEntryBounds()
	if !errors.Is(err, errorfs.ErrInjected) || errors.Is(err, ErrCorrupt) {
		t.Fatalf("iterator I/O became corruption: %v", err)
	}
	if err := s.Scrub(); !errors.Is(err, errorfs.ErrInjected) {
		t.Fatal(err)
	}
	if _, err := s.LastIndex(); !errors.Is(err, ErrPoisoned) || !errors.Is(err, errorfs.ErrInjected) {
		t.Fatal(err)
	}
	toggle.Off()
	var absent *Store
	if absent.Limits() != (Limits{}) {
		t.Fatal("nil limits accessor")
	}
}
