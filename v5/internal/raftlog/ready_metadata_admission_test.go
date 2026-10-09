package raftlog

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func readyMetadataFingerprint(t *testing.T, s *Store) [32]byte {
	t.Helper()
	it, err := s.db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	for valid := it.First(); valid; valid = it.Next() {
		var lengths [16]byte
		binary.BigEndian.PutUint64(lengths[:8], uint64(len(it.Key())))
		binary.BigEndian.PutUint64(lengths[8:], uint64(len(it.Value())))
		_, _ = h.Write(lengths[:])
		_, _ = h.Write(it.Key())
		_, _ = h.Write(it.Value())
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		t.Fatal(err)
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}
func readyMetadataVoters() []uint64 {
	ids := make([]uint64, 256)
	for j := range ids {
		ids[j] = uint64(j + 1)
	}
	return ids
}
func readyMetadataLimits() Limits {
	l := DefaultLimits()
	l.MaxEntryBytes = 256
	l.MaxReadBytes = 512
	l.MaxReadyBytes = 1024
	return l
}
func readyMetadataSize(t *testing.T, m metadata) int {
	t.Helper()
	b, err := encodeMeta(m)
	if err != nil {
		t.Fatal(err)
	}
	return len(b)
}
func readyMetadataConfig(t *testing.T, version string, fs vfs.FS, l Limits) Config {
	t.Helper()
	c := Config{Dir: "db", FS: fs, Create: true, Limits: l}
	if version != "RLM2" {
		c.Application = DefaultApplicationPolicy(uint64(1) << 63)
		if version == "RLM4" {
			_, c.Transfer = transferConfig(c.Application.LocalVoter)
			c.Transfer.Contract = ApplicationContractForPolicy(c.Application)
		}
	}
	return c
}
func readyMetadataOpen(t *testing.T, c Config) *Store {
	t.Helper()
	s, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}
func readyMetadataUnchanged(t *testing.T, s *Store, fs *vfs.MemFS, c Config, before [32]byte) {
	t.Helper()
	if readyMetadataFingerprint(t, s) != before {
		t.Error("rejected operation changed durable keys")
	}
	if s.poison != nil {
		t.Errorf("admission poisoned store: %v", s.poison)
	}
	c.Create = false
	c.FS = fs.CrashClone(vfs.CrashCloneCfg{})
	r, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if readyMetadataFingerprint(t, r) != before {
		t.Error("rejected operation changed crash-recovered keys")
	}
}
func readyMetadataSnapshot(voters []uint64) raft.Ready {
	return raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Vote: new(uint64(1)), Commit: new(uint64(100))}, Snapshot: &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(100)), Term: new(uint64(2)), ConfState: &pb.ConfState{Voters: voters}}, Data: []byte("new image")}}
}
func TestReadyMetadataAdmissionRejectsEntryFreeOversizeWithoutDurableChanges(t *testing.T) {
	fs := vfs.NewCrashableMem()
	c := readyMetadataConfig(t, "RLM2", fs, readyMetadataLimits())
	s := readyMetadataOpen(t, c)
	if err := s.Initialize([]uint64{1}, []byte("old image")); err != nil {
		t.Fatal(err)
	}
	before := readyMetadataFingerprint(t, s)
	rd := readyMetadataSnapshot(readyMetadataVoters())
	if err := s.Persist(rd); !errors.Is(err, ErrLimit) {
		t.Errorf("entry-free snapshot escaped metadata budget: %v", err)
	}
	readyMetadataUnchanged(t, s, fs, c, before)
}
func TestReadyMetadataAdmissionCountsProspectiveSnapshotAndEntriesAtExactBoundary(t *testing.T) {
	for _, entryCount := range []int{0, 1} {
		for _, delta := range []int{-1, 0} {
			t.Run(fmt.Sprintf("entries=%d/budget=%d", entryCount, delta), func(t *testing.T) {
				fs := vfs.NewCrashableMem()
				rd := readyMetadataSnapshot(readyMetadataVoters())
				if entryCount > 0 {
					rd.Entries = []*pb.Entry{ent(101, 2, "x")}
					rd.Commit = new(uint64(101))
				}
				m := metadata{Hard: rd.HardState, Conf: rd.Snapshot.Metadata.ConfState, Snap: &pb.Snapshot{Metadata: rd.Snapshot.Metadata}}
				need := readyMetadataSize(t, m) + 128 + entryCount*(frameOverhead+9+32+1)
				if readyMetadataSize(t, m) != 1550 {
					t.Fatal("256-voter fixture wire size drifted", readyMetadataSize(t, m))
				}
				l := readyMetadataLimits()
				l.MaxReadyBytes = need + delta
				c := readyMetadataConfig(t, "RLM2", fs, l)
				s := readyMetadataOpen(t, c)
				if err := s.Initialize([]uint64{1}, []byte("old")); err != nil {
					t.Fatal(err)
				}
				before := readyMetadataFingerprint(t, s)
				err := s.Persist(rd)
				if delta < 0 {
					if !errors.Is(err, ErrLimit) {
						t.Errorf("one-byte-over Ready admitted: %v", err)
					}
					readyMetadataUnchanged(t, s, fs, c, before)
				} else {
					if err != nil {
						t.Fatal("exact-boundary Ready refused", err)
					}
					if err := s.Scrub(); err != nil {
						t.Fatal(err)
					}
					c.Create = false
					c.FS = fs.CrashClone(vfs.CrashCloneCfg{})
					r := readyMetadataOpen(t, c)
					if readyMetadataFingerprint(t, r) != readyMetadataFingerprint(t, s) {
						t.Fatal("accepted state did not survive crash")
					}
				}
			})
		}
	}
}
func TestReadyMetadataAdmissionHardStateGrowthAcrossFormats(t *testing.T) {
	for _, version := range []string{"RLM2", "RLM3", "RLM4"} {
		t.Run(version, func(t *testing.T) {
			fs := vfs.NewCrashableMem()
			l := readyMetadataLimits()
			l.MaxEntryBytes = frameOverhead
			l.MaxReadBytes = frameOverhead + 32
			seed := readyMetadataConfig(t, version, vfs.NewMem(), l)
			seed.Limits.MaxReadyBytes = 4096
			probe := readyMetadataOpen(t, seed)
			voters := readyMetadataVoters()
			if seed.Application.Enabled() {
				voters = []uint64{seed.Application.LocalVoter}
			}
			if err := probe.Initialize(voters, nil); err != nil {
				t.Fatal(err)
			}
			l.MaxReadyBytes = readyMetadataSize(t, probe.meta) + 128
			c := readyMetadataConfig(t, version, fs, l)
			s := readyMetadataOpen(t, c)
			if err := s.Initialize(voters, nil); err != nil {
				t.Fatal(err)
			}
			if err := s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(127)), Commit: new(uint64(1))}}); err != nil {
				t.Fatal("exact-fit term refused", err)
			}
			before := readyMetadataFingerprint(t, s)
			if err := s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(128)), Commit: new(uint64(1))}}); !errors.Is(err, ErrLimit) {
				t.Errorf("HardState varint growth ignored: %v", err)
			}
			readyMetadataUnchanged(t, s, fs, c, before)
			if err := s.Persist(raft.Ready{}); err != nil {
				t.Fatal("unchanged Ready refused", err)
			}
		})
	}
}
func TestReadyMetadataAdmissionPreservesLargerBudgetAndSeparateImageLimit(t *testing.T) {
	fs := vfs.NewCrashableMem()
	l := readyMetadataLimits()
	l.MaxReadyBytes = 2048
	l.MaxSnapshotBytes = 16
	c := readyMetadataConfig(t, "RLM2", fs, l)
	s := readyMetadataOpen(t, c)
	if err := s.Initialize([]uint64{1}, nil); err != nil {
		t.Fatal(err)
	}
	rd := readyMetadataSnapshot(readyMetadataVoters())
	if err := s.Persist(rd); err != nil {
		t.Fatal("valid snapshot refused", err)
	}
	if err := s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(3)), Commit: new(uint64(100))}}); err != nil {
		t.Fatal("valid HardState refused", err)
	}
	before := readyMetadataFingerprint(t, s)
	rd.Snapshot.Data = make([]byte, l.MaxSnapshotBytes+1)
	rd.Snapshot.Metadata.Index = new(uint64(101))
	rd.Term = new(uint64(3))
	rd.Commit = new(uint64(101))
	if err := s.Persist(rd); !errors.Is(err, ErrLimit) {
		t.Errorf("snapshot image budget ignored: %v", err)
	}
	readyMetadataUnchanged(t, s, fs, c, before)
}
func TestReadyMetadataAdmissionSiblingWritersAndLowerLimitReopen(t *testing.T) {
	for _, kind := range []string{"initialize", "checkpoint", "publish", "reopen"} {
		t.Run(kind, func(t *testing.T) {
			fs := vfs.NewCrashableMem()
			l := readyMetadataLimits()
			if kind == "publish" {
				l.MaxReadyBytes = 1100
			}
			c := readyMetadataConfig(t, "RLM2", fs, l)
			s := readyMetadataOpen(t, c)
			if kind == "initialize" {
				before := readyMetadataFingerprint(t, s)
				if err := s.Initialize(readyMetadataVoters(), []byte("initial")); !errors.Is(err, ErrLimit) {
					t.Errorf("oversized initial metadata admitted: %v", err)
				}
				readyMetadataUnchanged(t, s, fs, c, before)
				if err := s.Initialize([]uint64{1}, nil); err != nil {
					t.Fatal("admission consumed initialization", err)
				}
				return
			}
			if err := s.Initialize([]uint64{1}, nil); err != nil {
				t.Fatal(err)
			}
			if kind == "checkpoint" {
				cs := &pb.ConfState{Voters: readyMetadataVoters(), VotersOutgoing: readyMetadataVoters()}
				before := readyMetadataFingerprint(t, s)
				if err := s.SaveCheckpoint(1, cs, []byte("new")); !errors.Is(err, ErrLimit) {
					t.Errorf("oversized checkpoint metadata admitted: %v", err)
				}
				readyMetadataUnchanged(t, s, fs, c, before)
				return
			}
			if kind == "publish" {
				persist(t, s, 2, 2, ent(2, 2, "entry"))
				if err := s.SaveCheckpoint(2, &pb.ConfState{Voters: readyMetadataVoters()}, []byte("checkpoint")); err != nil {
					t.Fatal(err)
				}
				before := readyMetadataFingerprint(t, s)
				if err := s.PublishSnapshot(); !errors.Is(err, ErrLimit) {
					t.Errorf("duplicated snapshot membership escaped metadata budget: %v", err)
				}
				readyMetadataUnchanged(t, s, fs, c, before)
				return
			}
			s.limits.MaxReadyBytes = 2048 // write under larger limits; recovery below uses the original limits.
			if err := s.Persist(readyMetadataSnapshot(readyMetadataVoters())); err != nil {
				t.Fatal(err)
			}
			before := readyMetadataFingerprint(t, s)
			c.Create = false
			c.FS = fs.CrashClone(vfs.CrashCloneCfg{})
			r, err := Open(c)
			if r != nil {
				_ = r.Close()
			}
			if !errors.Is(err, ErrLimit) {
				t.Errorf("lower limits accepted incompatible metadata: %v", err)
			}
			c.Limits.MaxReadyBytes = 2048
			r = readyMetadataOpen(t, c)
			if readyMetadataFingerprint(t, r) != before {
				t.Fatal("failed recovery changed durable keys")
			}
		})
	}
}
func TestReadyMetadataAdmissionUnknownFieldsRemainInvalidAndNonmutating(t *testing.T) {
	for _, kind := range []string{"hard", "snapshot", "metadata", "conf", "entry"} {
		t.Run(kind, func(t *testing.T) {
			fs := vfs.NewCrashableMem()
			c := readyMetadataConfig(t, "RLM2", fs, readyMetadataLimits())
			s := readyMetadataOpen(t, c)
			if err := s.Initialize([]uint64{1}, nil); err != nil {
				t.Fatal(err)
			}
			rd := readyMetadataSnapshot([]uint64{1})
			unknown := []byte{0xa0, 0x06, 1}
			switch kind {
			case "hard":
				rd.HardState.ProtoReflect().SetUnknown(unknown)
			case "snapshot":
				rd.Snapshot.ProtoReflect().SetUnknown(unknown)
			case "metadata":
				rd.Snapshot.Metadata.ProtoReflect().SetUnknown(unknown)
			case "conf":
				rd.Snapshot.Metadata.ConfState.ProtoReflect().SetUnknown(unknown)
			case "entry":
				rd.Entries = []*pb.Entry{ent(101, 2, "x")}
				rd.Entries[0].ProtoReflect().SetUnknown(unknown)
			}
			before := readyMetadataFingerprint(t, s)
			if err := s.Persist(rd); !errors.Is(err, ErrInvalid) {
				t.Errorf("unknown %s field escaped validation: %v", kind, err)
			}
			readyMetadataUnchanged(t, s, fs, c, before)
		})
	}
}
