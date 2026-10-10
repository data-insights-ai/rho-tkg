package raftlog

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func semanticStore(t *testing.T, fs vfs.FS, id ApplicationSemanticContractID, voter uint64) *Store {
	t.Helper()
	return semanticStoreLimits(t, fs, id, voter, Limits{})
}
func semanticStoreLimits(t *testing.T, fs vfs.FS, id ApplicationSemanticContractID, voter uint64, l Limits) *Store {
	t.Helper()
	p, tc := transferConfig(voter)
	s, err := Open(Config{Dir: "db", FS: fs, Create: true, Limits: l, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{2 << 30, 2000000}, PublishedCuts: ApplicationPublishedCutLimits{1000000}, Replication: ApplicationReplicationConfig{[3]uint64{1, 2, 3}}, SemanticContractID: id})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := s.Initialize([]uint64{1, 2, 3}, []byte("initial")); err != nil {
		t.Fatal(err)
	}
	return s
}
func TestApplicationSemanticBindingPersistsAndCannotRetrofit(t *testing.T) {
	fs := vfs.NewMem()
	id := ApplicationSemanticContractID{7}
	s := semanticStore(t, fs, id, 1)
	b := s.ApplicationBinding()
	if b.SemanticContractID != id || b.Identity != s.ApplicationIdentity() {
		t.Fatal(b)
	}
	cut, err := s.PublishedApplicationCut()
	if err != nil || cut.SemanticContractID != id {
		t.Fatal(cut, err)
	}
	cfg := Config{Dir: "db", FS: fs, Application: s.meta.App.Policy, Transfer: s.meta.Transfer, Generations: s.meta.Gen.Limits, PublishedCuts: s.meta.Gen.Publication.Limits, Replication: s.meta.Rep.Config, SemanticContractID: id}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.ApplicationBinding() != b {
		t.Fatal("reopen lost semantics")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.SemanticContractID = ApplicationSemanticContractID{8}
	if _, err := Open(cfg); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestApplicationSemanticBindingValidateAndGetterLifetimes(t *testing.T) {
	binding := ApplicationBinding{Identity: ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{2}}, SemanticContractID: ApplicationSemanticContractID{3}}
	if err := binding.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*ApplicationBinding){func(b *ApplicationBinding) { b.SemanticContractID = ApplicationSemanticContractID{} }, func(b *ApplicationBinding) { b.Identity.Graph = [16]byte{} }, func(b *ApplicationBinding) { b.Identity.Group = [16]byte{} }, func(b *ApplicationBinding) { b.Identity.Partition = 0 }} {
		b := binding
		change(&b)
		if err := b.Validate(); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if got := (*Store)(nil).ApplicationBinding(); got != (ApplicationBinding{}) {
		t.Fatal(got)
	}
	plain := receiverStore(t, vfs.NewMem(), 1)
	if got := plain.ApplicationBinding(); got != (ApplicationBinding{}) {
		t.Fatal(got)
	}
	s := semanticStore(t, vfs.NewMem(), binding.SemanticContractID, 1)
	before := s.ApplicationBinding()
	owned := before
	owned.Identity.Graph[0]++
	owned.SemanticContractID[0]++
	if s.ApplicationBinding() != before {
		t.Fatal("getter aliases metadata")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.ApplicationBinding() != before {
		t.Fatal("closed configuration disappeared")
	}
	// Whole-struct metadata publication shares the same lock as this getter.
	live := semanticStore(t, vfs.NewMem(), binding.SemanticContractID, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 1000 {
			if live.ApplicationBinding().SemanticContractID != binding.SemanticContractID {
				t.Error("torn binding")
			}
		}
	}()
	generationApply(t, live, "changed")
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("binding reader did not join")
	}
}

func semanticConfig(s *Store, fs vfs.FS) Config {
	return Config{Dir: "db", FS: fs, Application: s.meta.App.Policy, Transfer: s.meta.Transfer, Generations: s.meta.Gen.Limits, PublishedCuts: s.meta.Gen.Publication.Limits, Replication: s.meta.Rep.Config, SemanticContractID: s.meta.Rep.SemanticContractID}
}
func TestApplicationSemanticMetadataFailClosedBeforeCreationAndCleanup(t *testing.T) {
	fs := vfs.NewMem()
	if _, err := Open(Config{Dir: "invalid", FS: fs, Create: true, SemanticContractID: ApplicationSemanticContractID{1}}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := fs.Stat("invalid"); err == nil {
		t.Fatal("invalid binding created directory")
	}
	crashFS := vfs.NewCrashableMem()
	s := semanticStore(t, crashFS, ApplicationSemanticContractID{1}, 1)
	raw, err := encodeMeta(s.meta)
	if err != nil || uint64(len(raw)) != metadataBytes(s.meta) {
		t.Fatal("AR2 accounting", err)
	}
	trailer := appendReplicationMeta(nil, s.meta.Rep)
	if len(trailer) != replicationMetaBytes+32 || trailer[2] != 2 {
		t.Fatal("AR2 shape", trailer)
	}
	decoded, tail, err := decodeReplicationMeta(append(trailer, 42))
	if err != nil || decoded != s.meta.Rep || !bytes.Equal(tail, []byte{42}) {
		t.Fatal(decoded, tail, err)
	}
	for n := 0; n < len(trailer); n++ {
		if _, _, err := decodeReplicationMeta(trailer[:n]); !errors.Is(err, ErrCorrupt) {
			t.Fatal(n, err)
		}
	}
	zero := bytes.Clone(trailer)
	clear(zero[replicationMetaBytes:])
	if _, _, err := decodeReplicationMeta(zero); !errors.Is(err, ErrCorrupt) {
		t.Fatal("AR2 zero id", err)
	}
	bad := bytes.Clone(trailer)
	bad[2] = 3
	if _, _, err := decodeReplicationMeta(bad); !errors.Is(err, ErrCorrupt) {
		t.Fatal("future AR", err)
	}
	for _, retrofit := range []bool{false, true} {
		t.Run(fmt.Sprint(retrofit), func(t *testing.T) {
			fs := vfs.NewCrashableMem()
			var original *Store
			if retrofit {
				original = receiverStore(t, fs, 1)
			} else {
				original = semanticStore(t, fs, ApplicationSemanticContractID{1}, 1)
			}
			cfg := semanticConfig(original, fs.CrashClone(vfs.CrashCloneCfg{}))
			if err := original.db.Set(dormantKey, []byte("untouched"), pebble.Sync); err != nil {
				t.Fatal(err)
			}
			cfg.FS = fs.CrashClone(vfs.CrashCloneCfg{})
			cfg.SemanticContractID = ApplicationSemanticContractID{2}
			if _, err := Open(cfg); !errors.Is(err, ErrInvalid) {
				t.Fatal("binding changed on reopen", err)
			}
			db, err := pebble.Open("db", &pebble.Options{FS: cfg.FS})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			v, closer, err := db.Get(dormantKey)
			if err != nil || string(v) != "untouched" {
				t.Fatal("cleanup preceded binding", err)
			}
			if err := closer.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
	// AR2 must not lose its semantic field while keeping a bound publication ID.
	m := s.meta
	m.Rep.SemanticContractID = ApplicationSemanticContractID{}
	if err := s.db.Set(dormantKey, []byte("untouched"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	altered, err := encodeMeta(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.Set(metaKey, altered, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	cfg := semanticConfig(s, crashFS.CrashClone(vfs.CrashCloneCfg{}))
	cfg.SemanticContractID = ApplicationSemanticContractID{}
	if _, err := Open(cfg); !errors.Is(err, ErrCorrupt) {
		t.Fatal("dropped AR2 binding accepted", err)
	}
	db, err := pebble.Open("db", &pebble.Options{FS: cfg.FS})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	v, closer, err := db.Get(dormantKey)
	if err != nil || string(v) != "untouched" {
		t.Fatal("malformed bound cut cleanup", err)
	}
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestApplicationSemanticFreshMetadataAndDescriptorBudgetBoundary(t *testing.T) {
	l := DefaultLimits()
	l.MaxEntryBytes = 256
	l.MaxReadBytes = 512
	l.MaxReadyBytes = 4096
	probe := semanticStoreLimits(t, vfs.NewMem(), ApplicationSemanticContractID{1}, 1, l)
	worst := probe.meta
	cs := &pb.ConfState{Voters: []uint64{1, 2, 3}, AutoLeave: new(false)}
	worst.Hard = &pb.HardState{Term: new(uint64(math.MaxUint64)), Vote: new(uint64(3)), Commit: new(uint64(math.MaxUint64))}
	worst.Conf = cs
	worst.Snap = &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(math.MaxUint64)), Term: new(uint64(math.MaxUint64)), ConfState: cs}}
	wire, err := encodeMeta(worst)
	if err != nil {
		t.Fatal(err)
	}
	if metadataBytes(worst) != uint64(len(wire)) {
		t.Fatal("prospective AR2 size mismatch")
	}
	old := worst
	old.Rep.SemanticContractID = ApplicationSemanticContractID{}
	oldwire, err := encodeMeta(old)
	if err != nil || len(wire)-len(oldwire) != 32 {
		t.Fatal("AR2 must charge exactly its ID", err)
	}
	need := len(wire) + readyEnvelopeBytes + applicationSnapshotDescriptorFixedBytes + 32 + proto.Size(cs)
	for _, delta := range []int{-1, 0, 1} {
		t.Run(fmt.Sprint(delta), func(t *testing.T) {
			fs := vfs.NewMem()
			cfg := semanticConfig(probe, fs)
			cfg.Create = true
			cfg.Limits = l
			cfg.Limits.MaxReadyBytes = need + delta
			if err := cfg.Limits.Validate(); err != nil {
				t.Fatal("invalid boundary fixture", err)
			}
			s, err := Open(cfg)
			if delta < 0 {
				if !errors.Is(err, ErrLimit) {
					t.Fatal(err)
				}
				if _, err := fs.Stat("db"); err == nil {
					t.Fatal("one short created DB")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Initialize([]uint64{1, 2, 3}, []byte("initial")); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
