package raftlog

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestReadyMetadataBytesMatchesEncodingAcrossFormats(t *testing.T) {
	for _, version := range []string{"RLM2", "RLM3", "RLM4"} {
		for _, populated := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/populated=%v", version, populated), func(t *testing.T) {
				c := readyMetadataConfig(t, version, vfs.NewMem(), DefaultLimits())
				m := metadata{Hard: &pb.HardState{}, Conf: &pb.ConfState{}, Snap: &pb.Snapshot{}, App: applicationMetadata{Policy: c.Application}, Transfer: c.Transfer}
				if populated {
					m.Hard = &pb.HardState{Term: new(^uint64(0)), Vote: new(uint64(1) << 63), Commit: new(uint64(128))}
					m.Conf = &pb.ConfState{Voters: readyMetadataVoters(), VotersOutgoing: readyMetadataVoters(), Learners: []uint64{1000}, LearnersNext: []uint64{1}, AutoLeave: new(true)}
					m.Snap = &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(128)), Term: new(uint64(128)), ConfState: proto.Clone(m.Conf).(*pb.ConfState)}}
					m.Base = 128
					m.Last = 256
					m.Applied = 129
					m.App.Bytes = ^uint64(0)
					m.App.TailBytes = ^uint64(0)
				}
				b, err := encodeMeta(m)
				if err != nil {
					t.Fatal(err)
				}
				if string(b[:4]) != version {
					t.Fatal("wrong format", string(b[:4]))
				}
				if got := metadataBytes(m); got != uint64(len(b)) {
					t.Fatalf("size=%d, encoded=%d", got, len(b))
				}
				l := DefaultLimits()
				l.MaxReadyBytes = len(b) + 128
				if err := checkMetadataLimit(m, l); err != nil {
					t.Fatal("exact metadata boundary refused", err)
				}
				l.MaxReadyBytes--
				if err := checkMetadataLimit(m, l); !errors.Is(err, ErrLimit) {
					t.Fatal("one-byte metadata overflow ignored", err)
				}
			})
		}
	}
}
func TestReadyMetadataCodecDistinguishesPolicyLimitFromCorruption(t *testing.T) {
	for _, version := range []string{"RLM2", "RLM3", "RLM4"} {
		t.Run(version, func(t *testing.T) {
			c := readyMetadataConfig(t, version, vfs.NewMem(), DefaultLimits())
			m := metadata{Hard: &pb.HardState{}, Conf: &pb.ConfState{}, Snap: &pb.Snapshot{}, App: applicationMetadata{Policy: c.Application}, Transfer: c.Transfer}
			b, err := encodeMeta(m)
			if err != nil {
				t.Fatal(err)
			}
			l := DefaultLimits()
			l.MaxReadyBytes = len(b) + 128
			if _, err := decodeMeta(b, l); err != nil {
				t.Fatal("exact-fit metadata decode refused", err)
			}
			l.MaxReadyBytes--
			if _, err := decodeMeta(b, l); !errors.Is(err, ErrLimit) || errors.Is(err, ErrCorrupt) {
				t.Fatal("smaller policy was not a clean limit refusal", err)
			}
			broken := bytes.Clone(b)
			broken[4] ^= 1
			if _, err := decodeMeta(broken, l); !errors.Is(err, ErrCorrupt) || errors.Is(err, ErrLimit) {
				t.Fatal("bad checksum became policy refusal", err)
			}
			if _, err := decodeMeta(make([]byte, 65537), DefaultLimits()); !errors.Is(err, ErrCorrupt) {
				t.Fatal("hard-format ceiling ignored", err)
			}
			m.Snap.Data = make([]byte, 65537)
			if err := checkMetadataLimit(m, DefaultLimits()); !errors.Is(err, ErrLimit) {
				t.Fatal("writer admits unrecoverable metadata frame", err)
			}
		})
	}
}
func TestReadyMetadataFreshOpenRefusesBeforeCreatingDatabase(t *testing.T) {
	l := readyMetadataLimits()
	l.MaxEntryBytes = frameOverhead
	l.MaxReadBytes = frameOverhead + 32
	l.MaxReadyBytes = frameOverhead + 512
	for _, version := range []string{"RLM2", "RLM3", "RLM4"} {
		t.Run(version, func(t *testing.T) {
			mem := vfs.NewMem()
			c := readyMetadataConfig(t, version, mem, l)
			s, err := Open(c)
			if version == "RLM4" {
				if s != nil {
					_ = s.Close()
				}
				if !errors.Is(err, ErrLimit) {
					t.Fatal("fresh transfer envelope admitted", err)
				}
				if _, err := mem.Stat(c.Dir); !errors.Is(err, fs.ErrNotExist) {
					t.Fatal("refused config created database directory", err)
				}
			} else {
				if err != nil {
					t.Fatal("smaller format refused", err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
func TestReadyMetadataDefensiveCommitRefusesAndClosesBatch(t *testing.T) {
	mem := vfs.NewCrashableMem()
	c := readyMetadataConfig(t, "RLM2", mem, readyMetadataLimits())
	s := readyMetadataOpen(t, c)
	if err := s.Initialize([]uint64{1}, nil); err != nil {
		t.Fatal(err)
	}
	before := readyMetadataFingerprint(t, s)
	m := s.meta
	m.Conf = &pb.ConfState{Voters: readyMetadataVoters(), VotersOutgoing: readyMetadataVoters()}
	b := s.db.NewBatch()
	if err := b.Set([]byte("must never appear"), []byte("value"), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.commit(m, b); !errors.Is(err, ErrLimit) {
		t.Errorf("defensive commit admitted oversized metadata: %v", err)
	}
	if b.Count() != 0 {
		t.Error("refused commit did not release its owned batch")
	}
	readyMetadataUnchanged(t, s, mem, c, before)
}
func TestReadyMetadataEntryTotalsIncludeHardStateGrowthAcrossFormats(t *testing.T) {
	for _, version := range []string{"RLM2", "RLM3", "RLM4"} {
		for _, delta := range []int{-1, 0} {
			t.Run(fmt.Sprintf("%s/budget=%d", version, delta), func(t *testing.T) {
				mem := vfs.NewCrashableMem()
				l := readyMetadataLimits()
				l.MaxEntryBytes = frameOverhead
				l.MaxReadBytes = frameOverhead + 32
				c := readyMetadataConfig(t, version, vfs.NewMem(), l)
				c.Limits.MaxReadyBytes = 4096
				seed := readyMetadataOpen(t, c)
				voters := readyMetadataVoters()
				if c.Application.Enabled() {
					voters = []uint64{c.Application.LocalVoter}
				}
				if err := seed.Initialize(voters, nil); err != nil {
					t.Fatal(err)
				}
				rd := raft.Ready{HardState: &pb.HardState{Term: new(uint64(128)), Commit: new(uint64(2))}, Entries: []*pb.Entry{ent(2, 128, "")}}
				m := seed.meta
				m.Hard = rd.HardState
				l.MaxReadyBytes = readyMetadataSize(t, m) + 128 + frameOverhead + 9 + 32 + delta
				c.FS = mem
				c.Limits = l
				s := readyMetadataOpen(t, c)
				if err := s.Initialize(voters, nil); err != nil {
					t.Fatal(err)
				}
				before := readyMetadataFingerprint(t, s)
				err := s.Persist(rd)
				if delta < 0 {
					if !errors.Is(err, ErrLimit) {
						t.Errorf("entry total ignored prospective HardState growth: %v", err)
					}
					readyMetadataUnchanged(t, s, mem, c, before)
				} else {
					if err != nil {
						t.Fatal("exact-fit append refused", err)
					}
					if err := s.Scrub(); err != nil {
						t.Fatal(err)
					}
					c.Create = false
					c.FS = mem.CrashClone(vfs.CrashCloneCfg{})
					r := readyMetadataOpen(t, c)
					if readyMetadataFingerprint(t, r) != readyMetadataFingerprint(t, s) {
						t.Fatal("exact-fit append not crash durable")
					}
				}
			})
		}
	}
}
