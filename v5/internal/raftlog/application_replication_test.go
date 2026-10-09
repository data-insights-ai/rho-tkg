package raftlog

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestApplicationReplicationRequiresFreshExactFixedMembership(t *testing.T) {
	for _, voters := range [][3]uint64{{0, 2, 3}, {1, 1, 3}, {3, 2, 1}, {2, 3, 4}, {1, 2, ^uint64(0)}} {
		t.Run("invalid", func(t *testing.T) {
			p, tc := transferConfig(1)
			fs := vfs.NewMem()
			_, err := Open(Config{Dir: "bad", FS: fs, Create: true, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{2 << 30, 2000000}, PublishedCuts: ApplicationPublishedCutLimits{100}, Replication: ApplicationReplicationConfig{voters}})
			if !errors.Is(err, ErrInvalid) {
				t.Fatal(voters, err)
			}
			if _, err := fs.Stat("bad"); err == nil {
				t.Fatal("invalid config created directory")
			}
		})
	}
	s := receiverStore(t, vfs.NewMem(), 1)
	before := readyMetadataFingerprint(t, s)
	for _, rd := range []raft.Ready{{HardState: &pb.HardState{Term: new(uint64(2)), Vote: new(uint64(4)), Commit: new(uint64(1))}}, {Entries: []*pb.Entry{{Type: pb.EntryConfChange.Enum()}}}} {
		if err := s.Persist(rd); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
		if before != readyMetadataFingerprint(t, s) {
			t.Fatal("membership refusal wrote")
		}
	}
	if err := s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Vote: new(uint64(3)), Commit: new(uint64(1))}}); err != nil {
		t.Fatal("agreed remote vote rejected", err)
	}
	if err := s.SaveCheckpoint(1, &pb.ConfState{Voters: []uint64{1, 2, 4}}, []byte("initial")); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.SaveCheckpoint(1, &pb.ConfState{Voters: []uint64{1, 2, 3}}, []byte("initial")); err != nil {
		t.Fatal(err)
	}
	p, tc := transferConfig(1)
	cfg := Config{Dir: "new", FS: vfs.NewMem(), Create: true, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{2 << 30, 2000000}, PublishedCuts: ApplicationPublishedCutLimits{100}, Replication: ApplicationReplicationConfig{[3]uint64{1, 2, 3}}}
	fresh, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	before = readyMetadataFingerprint(t, fresh)
	for _, bad := range [][]uint64{{1}, {1, 2, 4}, {3, 2, 1}} {
		if err := fresh.Initialize(bad, nil); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
		if before != readyMetadataFingerprint(t, fresh) {
			t.Fatal("Initialize config refusal wrote")
		}
	}
	if _, err := fresh.ApplicationActivationAudit(); err != nil {
		t.Fatal(err)
	}
	legacy := publicationStore(t, vfs.NewMem())
	cfg = Config{Dir: "db", FS: vfs.NewMem(), Application: legacy.meta.App.Policy, Transfer: legacy.meta.Transfer, Generations: legacy.meta.Gen.Limits, PublishedCuts: legacy.meta.Gen.Publication.Limits, Replication: s.meta.Rep.Config}
	// Format acquisition is tested on a closed actual existing legacy store.
	mem := vfs.NewMem()
	old := publicationStore(t, mem)
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.FS = mem
	if _, err := Open(cfg); !errors.Is(err, ErrInvalid) {
		t.Fatal("legacy acquired RLM6", err)
	}
}
func TestApplicationReplicationMetadataFailClosedBeforeCleanup(t *testing.T) {
	s := receiverStore(t, vfs.NewCrashableMem(), 1)
	wire, err := encodeMeta(s.meta)
	if err != nil {
		t.Fatal(err)
	}
	if string(wire[:4]) != "RLM6" || metadataBytes(s.meta) != uint64(len(wire)) {
		t.Fatal("RLM6 envelope size", len(wire), metadataBytes(s.meta))
	}
	m, err := decodeMeta(wire, s.limits)
	if err != nil || !reflect.DeepEqual(m.Rep, s.meta.Rep) {
		t.Fatal(m, err)
	}
	trailer := appendReplicationMeta(nil, s.meta.Rep)
	for n := 0; n < len(trailer); n++ {
		if _, _, err := decodeReplicationMeta(trailer[:n]); !errors.Is(err, ErrCorrupt) {
			t.Fatal(n, err)
		}
	}
	for _, change := range []func(*metadata){func(m *metadata) { m.Rep.Config.Voters = [3]uint64{} }, func(m *metadata) { m.Rep.Config.Voters = [3]uint64{1, 2, 4} }, func(m *metadata) { m.Conf = &pb.ConfState{Voters: []uint64{1, 2, 4}} }, func(m *metadata) { m.Snap.Metadata.ConfState = &pb.ConfState{Voters: []uint64{1}} }, func(m *metadata) { m.Hard.Vote = new(uint64(7)) }} {
		fs := vfs.NewCrashableMem()
		original := receiverStore(t, fs, 1)
		cfg := Config{Dir: "db", Application: original.meta.App.Policy, Transfer: original.meta.Transfer, Generations: original.meta.Gen.Limits, PublishedCuts: original.meta.Gen.Publication.Limits, Replication: original.meta.Rep.Config}
		altered := original.meta
		altered.Hard = proto.Clone(altered.Hard).(*pb.HardState)
		altered.Snap = proto.Clone(altered.Snap).(*pb.Snapshot)
		change(&altered)
		raw, err := encodeMeta(altered)
		if err != nil {
			t.Fatal(err)
		}
		if err := original.db.Set(metaKey, raw, pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if err := original.db.Set(dormantKey, []byte("untouched"), pebble.Sync); err != nil {
			t.Fatal(err)
		}
		cfg.FS = fs.CrashClone(vfs.CrashCloneCfg{})
		_, err = Open(cfg)
		if !errors.Is(err, ErrCorrupt) && !errors.Is(err, ErrInvalid) {
			t.Fatal("bad metadata accepted", err)
		}
		db, err := pebble.Open("db", &pebble.Options{FS: cfg.FS})
		if err != nil {
			t.Fatal(err)
		}
		v, closer, err := db.Get(dormantKey)
		if err != nil || !bytes.Equal(v, []byte("untouched")) {
			t.Fatal("cleanup preceded validation", err)
		}
		if err := closer.Close(); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestApplicationReplicationFreshReadyEnvelopeAndDescriptorHeadroom(t *testing.T) {
	p, tc := transferConfig(1)
	l := DefaultLimits()
	l.MaxEntryBytes = 256
	l.MaxReadBytes = 512
	l.MaxReadyBytes = 4096
	cfg := Config{Dir: "probe", FS: vfs.NewMem(), Create: true, Limits: l, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{2 << 30, 2000000}, PublishedCuts: ApplicationPublishedCutLimits{1000000}, Replication: ApplicationReplicationConfig{[3]uint64{1, 2, 3}}}
	probe, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	// Measure the declared maximum-width envelope independently by serialization.
	// Fixed-membership false AutoLeave presence is valid and occupies two bytes.
	worst := probe.meta
	cs := &pb.ConfState{Voters: []uint64{1, 2, 3}, AutoLeave: new(false)}
	worst.Hard = &pb.HardState{Term: new(uint64(math.MaxUint64)), Vote: new(uint64(3)), Commit: new(uint64(math.MaxUint64))}
	worst.Conf = cs
	worst.Snap = &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(math.MaxUint64)), Term: new(uint64(math.MaxUint64)), ConfState: cs}}
	encoded, err := encodeMeta(worst)
	if err != nil {
		t.Fatal(err)
	}
	need := len(encoded) + readyEnvelopeBytes + applicationSnapshotDescriptorFixedBytes + proto.Size(cs)
	for _, delta := range []int{-1, 0, 1} {
		t.Run(fmt.Sprint(delta), func(t *testing.T) {
			config := cfg
			config.Dir = "db"
			config.FS = vfs.NewMem()
			config.Limits.MaxReadyBytes = need + delta
			if err := config.Limits.Validate(); err != nil {
				t.Fatal("invalid fixture", err)
			}
			s, err := Open(config)
			if delta < 0 {
				if !errors.Is(err, ErrLimit) {
					t.Fatal("one-short fresh envelope accepted", err)
				}
				if _, err := config.FS.Stat("db"); err == nil {
					t.Fatal("refusal created directory")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestApplicationReplicationDefensiveAuditAndCapacityValidation(t *testing.T) {
	legacy := publicationStore(t, vfs.NewMem())
	m := legacy.meta
	m.Rep.LastActivatedManifestID[0] = 1
	before := readyMetadataFingerprint(t, legacy)
	if err := legacy.commit(m, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal("disabled mode accepted audit extension", err)
	}
	if before != readyMetadataFingerprint(t, legacy) || legacy.poison != nil {
		t.Fatal("defensive audit refusal mutated")
	}
	p, tc := transferConfig(1)
	cfg := Config{Dir: "db", FS: vfs.NewMem(), Create: true, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{2 << 30, 2000000}, PublishedCuts: ApplicationPublishedCutLimits{1000000}, Replication: ApplicationReplicationConfig{[3]uint64{1, 2, 3}}}
	fresh, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	m = fresh.meta
	m.Rep.LastActivatedManifestID[0] = 1
	before = readyMetadataFingerprint(t, fresh)
	if err := fresh.commit(m, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal("uninitialized audit accepted", err)
	}
	if before != readyMetadataFingerprint(t, fresh) || fresh.poison != nil {
		t.Fatal("uninitialized refusal mutated")
	}
	cfg.Dir = "small"
	cfg.Transfer.Limits.MaxPinnedLogicalBytes = preparedSnapshotFixedBytes + applicationSnapshotClaimFixedBytes + uint64(p.MaxImageBytes) - 1
	if _, err := Open(cfg); !errors.Is(err, ErrInvalid) {
		t.Fatal("known impossible capability quota accepted", err)
	}
	if _, err := cfg.FS.Stat("small"); err == nil {
		t.Fatal("invalid quota created directory")
	}
}

func TestApplicationReplicationApplicationMembershipGuardIsIndependent(t *testing.T) {
	s := receiverStore(t, vfs.NewMem(), 1)
	if err := s.validateApplicationMeta(s.meta); err != nil {
		t.Fatal("valid exact membership refused", err)
	}
	for _, snapshot := range []bool{false, true} {
		t.Run(fmt.Sprint(snapshot), func(t *testing.T) {
			m := s.meta
			m.Snap = proto.Clone(m.Snap).(*pb.Snapshot)
			if snapshot {
				m.Snap.Metadata.ConfState = &pb.ConfState{Voters: []uint64{1, 2, 4}}
			} else {
				m.Conf = &pb.ConfState{Voters: []uint64{1}}
			}
			before := readyMetadataFingerprint(t, s)
			if err := s.validateApplicationMeta(m); !errors.Is(err, ErrInvalid) {
				t.Fatal("application guard relied on earlier replication validation", err)
			}
			if before != readyMetadataFingerprint(t, s) || s.poison != nil {
				t.Fatal("validation changed active data")
			}
		})
	}
}
