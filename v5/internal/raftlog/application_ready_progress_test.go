package raftlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// These synthetic Ready values test Store ownership/admission contracts only.
// Genuine RawNode authority and advancement are covered by the Driver regressions.
func progressTestStore(t *testing.T) *Store {
	t.Helper()
	p, tc := transferConfig(1)
	p.RetainedApplicationBytes = 16 << 20
	p.ReclaimEntries = 1
	tc.Contract = ApplicationContractForPolicy(p)
	s, err := Open(Config{Dir: "progress", FS: vfs.NewMem(), Create: true, Application: p, Transfer: tc,
		Generations: ApplicationGenerationLimits{2 << 30, 2_000_000}, PublishedCuts: ApplicationPublishedCutLimits{10000},
		Replication: ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: ApplicationSemanticContractID{41}})
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

func progressTestReady(first, count, commit uint64) raft.Ready {
	rd := raft.Ready{HardState: &pb.HardState{Term: new(uint64(3)), Vote: new(uint64(2)), Commit: new(commit)}}
	for i := range count {
		rd.Entries = append(rd.Entries, ent(first+i, 3, fmt.Sprintf("new-%d", first+i)))
	}
	return rd
}

func progressTestBegin(t *testing.T, s *Store, rd raft.Ready) *ApplicationReadyProgress {
	t.Helper()
	p, err := s.BeginApplicationReadyProgress(s.ApplicationBinding(), rd, rd.GetCommit(), s.meta.Applied)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func progressTestInstall(t *testing.T, s *Store, index uint64, image string) {
	t.Helper()
	base, previous, err := s.Checkpoint()
	if err != nil || base+1 != index {
		t.Fatal("installation order", base, index, err)
	}
	if err := s.InstallApplication(index, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: base,
		BaseImageHash: sha256.Sum256(previous), Image: []byte(image)}); err != nil {
		t.Fatal(err)
	}
}

func TestApplicationReadyProgressNilAndZeroOwners(t *testing.T) {
	var s *Store
	if p, err := s.BeginApplicationReadyProgress(ApplicationBinding{}, raft.Ready{}, 0, 0); p != nil || !errors.Is(err, ErrInvalid) {
		t.Fatal(p, err)
	}
	for _, p := range []*ApplicationReadyProgress{nil, {}} {
		if ok, err := p.PersistFull(); ok || !errors.Is(err, ErrInvalid) {
			t.Fatal(ok, err)
		}
		if n, err := p.CommitPredecessor(); n != 0 || !errors.Is(err, ErrInvalid) {
			t.Fatal(n, err)
		}
		if n, err := p.PersistPrefix(); n != 0 || !errors.Is(err, ErrInvalid) {
			t.Fatal(n, err)
		}
		if n, done, err := p.AppliedLimit(); n != 0 || done || !errors.Is(err, ErrInvalid) {
			t.Fatal(n, done, err)
		}
		if err := p.AdvanceRaw(0); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
		if err := p.Close(); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
}

func TestApplicationReadyProgressValidatesCompleteInputBeforeMutation(t *testing.T) {
	for _, name := range []string{"binding", "raw-applied", "commit-authority", "snapshot", "nil-late", "gap-late", "term-late", "config-late", "unknown-late", "committed-config"} {
		t.Run(name, func(t *testing.T) {
			s := progressTestStore(t)
			rd := progressTestReady(2, 4, 5)
			b, commit, applied := s.ApplicationBinding(), uint64(5), uint64(1)
			switch name {
			case "binding":
				b.SemanticContractID[0]++
			case "raw-applied":
				applied = 0
			case "commit-authority":
				commit = 4
			case "snapshot":
				rd.Snapshot = &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(2)), Term: new(uint64(2))}}
			case "nil-late":
				rd.Entries[3] = nil
			case "gap-late":
				rd.Entries[3].Index = new(uint64(6))
			case "term-late":
				rd.Entries[3].Term = new(uint64(0))
			case "config-late":
				rd.Entries[3].Type = pb.EntryConfChangeV2.Enum()
			case "unknown-late":
				rd.Entries[3].ProtoReflect().SetUnknown([]byte{0x80, 0x01, 0x01})
			case "committed-config":
				rd.CommittedEntries = []*pb.Entry{{Type: pb.EntryConfChange.Enum(), Index: new(uint64(2))}}
			}
			before := readyMetadataFingerprint(t, s)
			if p, err := s.BeginApplicationReadyProgress(b, rd, commit, applied); p != nil || !errors.Is(err, ErrInvalid) {
				t.Fatal(p, err)
			}
			if before != readyMetadataFingerprint(t, s) || s.readyProgress != nil || s.poison != nil {
				t.Fatal("static refusal changed durable state or owner")
			}
		})
	}
	legacy := receiverStore(t, vfs.NewMem(), 1)
	if _, err := legacy.BeginApplicationReadyProgress(legacy.ApplicationBinding(), raft.Ready{}, 1, 1); !errors.Is(err, ErrInvalid) {
		t.Fatal("unbound owner", err)
	}
	s := progressTestStore(t)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginApplicationReadyProgress(s.ApplicationBinding(), raft.Ready{}, 1, 1); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestApplicationReadyProgressFullPersistenceAndLifecycle(t *testing.T) {
	s := progressTestStore(t)
	rd := progressTestReady(2, 1, 2)
	original := proto.Clone(rd.HardState)
	p := progressTestBegin(t, s, rd)
	if _, err := s.BeginApplicationReadyProgress(s.ApplicationBinding(), rd, 2, 1); !errors.Is(err, ErrInvalid) {
		t.Fatal("duplicate owner", err)
	}
	if err := p.Close(); !errors.Is(err, ErrInvalid) {
		t.Fatal("premature close", err)
	}
	if _, err := p.CommitPredecessor(); !errors.Is(err, ErrInvalid) {
		t.Fatal("preflight omitted", err)
	}
	if _, err := p.PersistPrefix(); !errors.Is(err, ErrInvalid) {
		t.Fatal("preflight omitted", err)
	}
	copyOwner := *p
	if _, _, err := copyOwner.AppliedLimit(); !errors.Is(err, ErrInvalid) {
		t.Fatal("copied owner", err)
	}
	copyOwner.self = &copyOwner
	if err := copyOwner.AdvanceRaw(1); !errors.Is(err, ErrInvalid) {
		t.Fatal("foreign owner", err)
	}
	if n, done, err := p.AppliedLimit(); err != nil || n != 1 || done {
		t.Fatal(n, done, err)
	}
	if ok, err := p.PersistFull(); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if !proto.Equal(original, rd.HardState) || string(rd.Entries[0].Data) != "new-2" {
		t.Fatal("borrowed input mutated")
	}
	if n, done, err := p.AppliedLimit(); err != nil || n != 2 || !done {
		t.Fatal(n, done, err)
	}
	if _, err := p.PersistFull(); !errors.Is(err, ErrInvalid) {
		t.Fatal("repeated full write", err)
	}
	if _, err := p.PersistPrefix(); !errors.Is(err, ErrInvalid) {
		t.Fatal("completed prefix", err)
	}
	progressTestInstall(t, s, 2, "new-2")
	if err := p.AdvanceRaw(3); !errors.Is(err, ErrInvalid) {
		t.Fatal("raw ahead of physical", err)
	}
	if err := p.AdvanceRaw(0); !errors.Is(err, ErrInvalid) {
		t.Fatal("raw rollback", err)
	}
	if err := p.Close(); !errors.Is(err, ErrInvalid) {
		t.Fatal("released while raw lagged", err)
	}
	if err := p.AdvanceRaw(2); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.AppliedLimit(); !errors.Is(err, ErrClosed) {
		t.Fatal("stale owner", err)
	}
	if err := p.Close(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestApplicationReadyProgressFirstFittingPrefixesPreserveInputAndFence(t *testing.T) {
	s := progressTestStore(t)
	rd := progressTestReady(2, 4, 5)
	original := proto.Clone(rd.HardState)
	p := progressTestBegin(t, s, rd)
	before := readyMetadataFingerprint(t, s)
	if ok, err := p.PersistFull(); err != nil || ok {
		t.Fatal("whole Ready must refuse quota without writing", ok, err)
	}
	if before != readyMetadataFingerprint(t, s) {
		t.Fatal("full refusal wrote")
	}
	if n, err := p.CommitPredecessor(); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	for index := uint64(2); index <= 5; index++ {
		last, err := p.PersistPrefix()
		if err != nil || last != index {
			t.Fatal("must choose first fitting, not largest prefix", last, index, err)
		}
		if n, done, err := p.AppliedLimit(); err != nil || n != index || done != (index == 5) {
			t.Fatal(n, done, err)
		}
		if s.meta.Hard.GetTerm() != 3 || s.meta.Hard.GetVote() != 2 || s.meta.Hard.GetCommit() != index {
			t.Fatal("capped HardState lost term/vote", s.meta.Hard)
		}
		progressTestInstall(t, s, index, fmt.Sprintf("new-%d", index))
		if err := s.PublishSnapshot(); !errors.Is(err, ErrInvalid) {
			t.Fatal("published ahead of raw applied", err)
		}
		if err := s.ReclaimApplication(); !errors.Is(err, ErrInvalid) {
			t.Fatal("reclaimed ahead of raw applied", err)
		}
		if s.meta.Base != 1 {
			t.Fatal("protected prefix compacted", s.meta.Base)
		}
		if _, err := s.Entries(2, index+1, 1<<20); err != nil {
			t.Fatal("raw backlog became unreadable", err)
		}
		if err := p.Close(); !errors.Is(err, ErrInvalid) {
			t.Fatal("early release", err)
		}
	}
	if !proto.Equal(original, rd.HardState) {
		t.Fatal("capping mutated actual HardState")
	}
	for i, e := range rd.Entries {
		if string(e.Data) != fmt.Sprintf("new-%d", i+2) {
			t.Fatal("borrowed entry mutated")
		}
	}
	if err := p.AdvanceRaw(5); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Entries(2, 3, 1<<20); !errors.Is(err, raft.ErrCompacted) {
		t.Fatal("completion did not release publication fence", err)
	}
}

func TestApplicationReadyProgressOverlapNeverCommitsOldPayload(t *testing.T) {
	s := progressTestStore(t)
	persist(t, s, 2, 1, ent(2, 2, "old-2"), ent(3, 2, "old-3"))
	p := progressTestBegin(t, s, progressTestReady(2, 4, 5))
	if ok, err := p.PersistFull(); err != nil || ok {
		t.Fatal(ok, err)
	}
	if n, err := p.CommitPredecessor(); err != nil || n != 1 || s.meta.Hard.GetCommit() != 1 {
		t.Fatal("old replaced suffix became committed", n, err)
	}
	if last, err := p.PersistPrefix(); err != nil || last != 2 {
		t.Fatal(last, err)
	}
	es, err := s.Entries(2, 3, 1<<20)
	if err != nil || len(es) != 1 || !bytes.Equal(es[0].Data, []byte("new-2")) {
		t.Fatal("old payload survived replacement", es, err)
	}
	if s.meta.Last != 2 || s.meta.Hard.GetCommit() != 2 || s.meta.Applied != 1 {
		t.Fatal("replacement frontier", s.meta)
	}
	progressTestInstall(t, s, 2, string(es[0].Data))
	_, image, err := s.Checkpoint()
	if err != nil || string(image) != "new-2" {
		t.Fatal("installed old payload", string(image), err)
	}
}

func TestApplicationReadyProgressNoFittingPrefixRetainsFence(t *testing.T) {
	s := progressTestStore(t)
	persist(t, s, 2, 1, ent(2, 2, "old-2"), ent(3, 2, "old-3"), ent(4, 2, "old-4"))
	p := progressTestBegin(t, s, progressTestReady(5, 1, 5))
	if ok, err := p.PersistFull(); err != nil || ok {
		t.Fatal(ok, err)
	}
	before := readyMetadataFingerprint(t, s)
	if n, err := p.PersistPrefix(); n != 0 || !errors.Is(err, ErrLimit) {
		t.Fatal("genuine quota refusal", n, err)
	}
	if before != readyMetadataFingerprint(t, s) || s.poison != nil {
		t.Fatal("prefix refusal wrote or poisoned")
	}
	if err := p.Close(); !errors.Is(err, ErrInvalid) {
		t.Fatal("refusal released fence", err)
	}
	if n, err := p.CommitPredecessor(); err != nil || n != 4 {
		t.Fatal("unchanged predecessor", n, err)
	}
	for i := uint64(2); i <= 4; i++ {
		progressTestInstall(t, s, i, fmt.Sprintf("old-%d", i))
	}
	if last, err := p.PersistPrefix(); err != nil || last != 5 {
		t.Fatal("real application progress must free pending allowance", last, err)
	}
	progressTestInstall(t, s, 5, "new-5")
	if err := p.AdvanceRaw(5); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestApplicationReadyProgressVerifierReentrancyAndLinearWork(t *testing.T) {
	s := boundedControlStore(t, func(c *Config) {
		c.Recovery = ApplicationRecoveryConfig{Version: 1}
		c.Application.RetainedApplicationBytes = 6500
	})
	var p *ApplicationReadyProgress
	calls := 0
	verifier := recoveryAdmissionTestVerifier(func(ctx context.Context, _ []byte, view *ApplicationView) (ApplicationRecoveryPurpose, error) {
		calls++
		root, err := view.RootBounded(ctx, 64)
		if err != nil || root.Index != s.meta.Applied {
			t.Fatal("wrong immutable view", root, err)
		}
		if _, _, err := p.AppliedLimit(); err != nil {
			t.Fatal("read reentry deadlocked or refused", err)
		}
		if err := p.Close(); !errors.Is(err, ErrInvalid) {
			t.Fatal("busy owner closed", err)
		}
		if _, err := p.PersistPrefix(); !errors.Is(err, ErrInvalid) {
			t.Fatal("recursive prefix", err)
		}
		return ApplicationRecoveryPurpose{}, nil
	})
	if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), verifier); err != nil {
		t.Fatal(err)
	}
	p = progressTestBegin(t, s, progressTestReady(2, 4, 5))
	if ok, err := p.PersistFull(); err != nil || ok {
		t.Fatal(ok, err)
	}
	for i := uint64(2); i <= 5; i++ {
		if last, err := p.PersistPrefix(); err != nil || last != i {
			t.Fatal(last, err)
		}
		progressTestInstall(t, s, i, fmt.Sprintf("new-%d", i))
	}
	if calls < 4 || calls > 8 {
		t.Fatal("verification must stay within full pass plus linear prefix walk", calls)
	}
	if err := p.AdvanceRaw(5); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestApplicationReadyProgressStoreCloseDuringVerifier(t *testing.T) {
	s := boundedControlStore(t, func(c *Config) { c.Recovery = ApplicationRecoveryConfig{Version: 1} })
	if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), recoveryAdmissionTestVerifier(func(context.Context, []byte, *ApplicationView) (ApplicationRecoveryPurpose, error) {
		return ApplicationRecoveryPurpose{}, s.Close()
	})); err != nil {
		t.Fatal(err)
	}
	p := progressTestBegin(t, s, progressTestReady(2, 1, 2))
	if ok, err := p.PersistFull(); ok || !errors.Is(err, ErrClosed) {
		t.Fatal("Store lifecycle invalidation lost", ok, err)
	}
	if _, err := p.PersistPrefix(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := p.CommitPredecessor(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := p.AdvanceRaw(1); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := p.Close(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestApplicationReadyProgressVerifierErrorsAreNotCapacityFallback(t *testing.T) {
	for _, joined := range []bool{false, true} {
		t.Run(fmt.Sprint(joined), func(t *testing.T) {
			s := boundedControlStore(t, func(c *Config) { c.Recovery = ApplicationRecoveryConfig{Version: 1} })
			sentinel := errors.New("verifier refuses genuine command")
			want := sentinel
			if joined {
				want = errors.Join(ErrLimit, sentinel)
			}
			calls := 0
			if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), recoveryAdmissionTestVerifier(func(context.Context, []byte, *ApplicationView) (ApplicationRecoveryPurpose, error) {
				calls++
				return ApplicationRecoveryPurpose{}, want
			})); err != nil {
				t.Fatal(err)
			}
			p := progressTestBegin(t, s, progressTestReady(2, 1, 2))
			before := readyMetadataFingerprint(t, s)
			if ok, err := p.PersistFull(); ok || !errors.Is(err, sentinel) {
				t.Fatal("verifier failure disguised as retry permission", ok, err)
			}
			if calls != 1 || before != readyMetadataFingerprint(t, s) {
				t.Fatal("failure retried or wrote", calls)
			}
			if err := p.Close(); !errors.Is(err, ErrInvalid) {
				t.Fatal("failure released owner", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if _, _, err := p.AppliedLimit(); !errors.Is(err, ErrClosed) {
				t.Fatal("Store close left owner live", err)
			}
		})
	}
}

func TestApplicationReadyProgressBlocksPreparedSnapshotActivation(t *testing.T) {
	s, donor := progressTestStore(t), progressTestStore(t)
	generationApply(t, donor, "incoming")
	if err := donor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := donor.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	export, err := donor.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer export.Close()
	manifest := buildPublished(t, export, ReadBudget{1, 4096})
	imp, err := s.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range collectPublished(t, export, ReadBudget{1, 4096}) {
		if err := imp.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := imp.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared, err := imp.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	descriptor, err := EncodeApplicationSnapshotDescriptor(manifest)
	if err != nil {
		t.Fatal(err)
	}
	snap := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(manifest.Index), Term: new(manifest.Term), ConfState: manifest.ConfState}, Data: descriptor}
	claim, err := prepared.Claim(snap)
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Close()
	p, err := s.BeginApplicationReadyProgress(s.ApplicationBinding(), raft.Ready{}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	rd := raft.Ready{Snapshot: snap, HardState: &pb.HardState{Term: new(uint64(5)), Vote: new(uint64(2)), Commit: new(uint64(2))}}
	before := readyMetadataFingerprint(t, s)
	if _, err := s.PersistApplicationReady(rd, claim); !errors.Is(err, ErrInvalid) {
		t.Fatal("activation ignored owner fence", err)
	}
	if before != readyMetadataFingerprint(t, s) || claim.consumed || s.ApplicationGeneration() != 1 {
		t.Fatal("fenced activation mutated or consumed claim")
	}
	if ok, err := p.PersistFull(); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	root, err := s.PersistApplicationReady(rd, claim)
	if err != nil || root.Index != 2 || string(root.Image) != "incoming" {
		t.Fatal("valid activation failed after fence released", root, err)
	}
}

func TestApplicationReadyProgressBeginSerializesWithCapturedPublication(t *testing.T) {
	s := progressTestStore(t)
	generationApply(t, s, "published")
	reached, resume, release := publicationTestGate(t)
	s.publicationCaptureHook = func() { close(reached); <-resume }
	published := publicationTestWorker(t, release, s.PublishSnapshot)
	publicationTestReached(t, reached)
	started := make(chan struct{})
	var owner *ApplicationReadyProgress
	binding := s.ApplicationBinding()
	begun := publicationTestWorker(t, release, func() error {
		close(started)
		var err error
		owner, err = s.BeginApplicationReadyProgress(binding, raft.Ready{}, 2, 2)
		return err
	})
	publicationTestReached(t, started)
	select {
	case err := <-begun:
		t.Fatal("Begin passed captured publication before commit", err)
	case <-time.After(20 * time.Millisecond):
	}
	s.mu.Lock()
	active := s.readyProgress
	s.mu.Unlock()
	if active != nil {
		t.Fatal("owner installed inside earlier captured publication")
	}
	release()
	if err := publicationTestResult(t, published); err != nil {
		t.Fatal(err)
	}
	if err := publicationTestResult(t, begun); err != nil {
		t.Fatal(err)
	}
	if owner == nil || s.meta.Base != 2 {
		t.Fatal("publication/owner order", owner, s.meta.Base)
	}
	if err := s.PublishSnapshot(); !errors.Is(err, ErrInvalid) {
		t.Fatal("new publication passed live fence", err)
	}
	if err := s.ReclaimApplication(); !errors.Is(err, ErrInvalid) {
		t.Fatal("new reclaim passed live fence", err)
	}
	if ok, err := owner.PersistFull(); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
}
