package raftlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Privileged Store tests use synthetic Ready values, never purported RawNode,
// graph, quorum, or snapshot authority. Graphapply tests supply real commands.
func recoveryDependencyFixture(t *testing.T, edits ...func(*Config)) (*Store, ApplicationRecoveryReservation, *recoveryAdmissionTestVerifier) {
	t.Helper()
	s := boundedControlStore(t, func(c *Config) {
		c.Recovery = ApplicationRecoveryConfig{Version: 1}
		for _, edit := range edits {
			edit(c)
		}
	})
	reserve := ApplicationRecoveryReservation{ID: [32]byte{31}, Digest: [32]byte{32}, Credits: []ApplicationRecoveryCredit{{Phase: 1, CommandBytes: 128, RetainedBytes: 1024, RetainedRecords: 8}}}
	persist(t, s, 2, 2, ent(2, 2, "old-register"))
	_, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.InstallApplication(2, ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: 1, BaseImageHash: sha256.Sum256(image), Image: []byte("old"), RecoveryReserve: &reserve}); err != nil {
		t.Fatal(err)
	}
	selected := recoveryAdmissionTestVerifier(func(_ context.Context, data []byte, view *ApplicationView) (ApplicationRecoveryPurpose, error) {
		if bytes.Equal(data, []byte("finish")) {
			return ApplicationRecoveryPurpose{Use: &ApplicationRecoveryUse{ID: reserve.ID, Digest: reserve.Digest, CommandDigest: sha256.Sum256(data), Phase: 1, RetireBefore: 1}}, nil
		}
		if bytes.Equal(data, []byte("new")) {
			root, err := view.RootBounded(t.Context(), 16)
			if err != nil {
				return ApplicationRecoveryPurpose{}, err
			}
			if bytes.Equal(root.Image, []byte("old")) {
				return ApplicationRecoveryPurpose{ApplyBefore: &ApplicationRecoveryApplyBefore{Owner: ApplicationRecoveryOwner{ID: reserve.ID, Digest: reserve.Digest}}}, nil
			}
			return ApplicationRecoveryPurpose{}, nil
		}
		return ApplicationRecoveryPurpose{}, ErrInvalid
	})
	if err := s.BindApplicationRecoveryVerifier(s.ApplicationBinding(), recoveryAdmissionTestVerifier(func(ctx context.Context, data []byte, view *ApplicationView) (ApplicationRecoveryPurpose, error) {
		return selected(ctx, data, view)
	})); err != nil {
		t.Fatal(err)
	}
	return s, reserve, &selected
}

func recoveryDependencyReady() raft.Ready {
	return raft.Ready{HardState: &pb.HardState{Term: new(uint64(3)), Vote: new(uint64(2)), Commit: new(uint64(4))}, Entries: []*pb.Entry{ent(3, 3, "finish"), ent(4, 3, "new")}}
}

func TestRecoveryDependencyExactReceiptRejectsCopiesMutationAndStaleFrontiers(t *testing.T) {
	s, _, _ := recoveryDependencyFixture(t)
	rd := recoveryDependencyReady()
	before := readyMetadataFingerprint(t, s)
	_, refusal, err := s.classifyRecoveryReadyOutcome(rd.Entries)
	if !errors.Is(err, ErrApplicationPrerequisite) || refusal == nil || refusal.dependency == nil {
		t.Fatal("exact applied owner/terminal did not mint dependency", err)
	}
	d := refusal.dependency
	s.mu.Lock()
	if !refusal.matches(s, len(rd.Entries), 4) || refusal.matches(s, len(rd.Entries), 2) {
		t.Fatal("actual committed predecessor guard")
	}
	copyReceipt := *d
	if copyReceipt.matches(s, 4) || (&applicationRecoveryDependency{}).matches(s, 4) {
		t.Fatal("copied/zero receipt granted progress")
	}
	old := s.meta.Hard.GetVote()
	s.meta.Hard.Vote = new(old + 1)
	if d.matches(s, 4) {
		t.Fatal("changed durable vote retained authority")
	}
	s.meta.Hard.Vote = new(old)
	oldGeneration := s.meta.Gen.Active
	s.meta.Gen.Active ^= 1
	if d.matches(s, 4) {
		t.Fatal("changed generation retained authority")
	}
	s.meta.Gen.Active = oldGeneration
	s.mu.Unlock()
	rd.Entries[1].Data = []byte("changed")
	s.mu.Lock()
	if d.matches(s, 4) {
		t.Fatal("changed deferring command retained authority")
	}
	s.mu.Unlock()
	rd.Entries[1].Data = []byte("new")
	if before != readyMetadataFingerprint(t, s) {
		t.Fatal("receipt inspection changed disk")
	}
}

func TestRecoveryDependencyCallbacksCannotReplayReceiptAndCloseFailureIsTerminal(t *testing.T) {
	for _, name := range []string{"direct-receipt", "joined-receipt", "view-close"} {
		t.Run(name, func(t *testing.T) {
			s, reserve, selected := recoveryDependencyFixture(t)
			rd := recoveryDependencyReady()
			_, refusal, err := s.classifyRecoveryReadyOutcome(rd.Entries)
			if refusal == nil || !errors.Is(err, ErrApplicationPrerequisite) {
				t.Fatal(err)
			}
			sentinel := errors.New("real classified view close failure")
			original := *selected
			*selected = recoveryAdmissionTestVerifier(func(ctx context.Context, data []byte, view *ApplicationView) (ApplicationRecoveryPurpose, error) {
				if bytes.Equal(data, []byte("new")) {
					if name == "direct-receipt" {
						return ApplicationRecoveryPurpose{}, refusal.dependency
					}
					if name == "joined-receipt" {
						return ApplicationRecoveryPurpose{}, errors.Join(refusal.dependency, sentinel)
					}
					// Poisoning an actual view's generation release exercises its
					// real Close error, not a fabricated caller return value.
					s.mu.Lock()
					s.poison = sentinel
					s.mu.Unlock()
					return ApplicationRecoveryPurpose{ApplyBefore: &ApplicationRecoveryApplyBefore{Owner: ApplicationRecoveryOwner{ID: reserve.ID, Digest: reserve.Digest}}}, nil
				}
				return original(ctx, data, view)
			})
			p := progressTestBegin(t, s, rd)
			before := readyMetadataFingerprint(t, s)
			if full, err := p.PersistFull(); full || !errors.Is(err, ErrApplicationPrerequisite) || name != "direct-receipt" && !errors.Is(err, sentinel) {
				t.Fatal("callback/close error became prefix permission", full, err)
			}
			if s.views != 0 || before != readyMetadataFingerprint(t, s) {
				t.Fatal("error leaked view or wrote prefix", s.views)
			}
			if name == "view-close" {
				if _, err := p.PersistPrefix(); !errors.Is(err, ErrPoisoned) || !errors.Is(err, sentinel) {
					t.Fatal("close failure allowed prefix retry", err)
				}
				// Permit only deterministic cleanup; the poisoned operation was
				// asserted above and no new persistence follows.
				s.mu.Lock()
				s.poison = nil
				s.mu.Unlock()
			}
		})
	}
}

func TestRecoveryDependencyOrphanUncommittedOwnerAndMalformedTailRefuseWithoutWrite(t *testing.T) {
	for _, name := range []string{"orphan", "uncommitted", "wrong-owner", "two-purposes", "malformed-tail"} {
		t.Run(name, func(t *testing.T) {
			s, reserve, selected := recoveryDependencyFixture(t)
			rd := recoveryDependencyReady()
			original := *selected
			switch name {
			case "orphan":
				rd.Entries = rd.Entries[1:]
			case "uncommitted":
				rd.Commit = new(uint64(2))
			case "malformed-tail":
				rd.Entries = append(rd.Entries, ent(5, 3, "malformed"))
				rd.Commit = new(uint64(5))
			case "wrong-owner", "two-purposes":
				*selected = recoveryAdmissionTestVerifier(func(ctx context.Context, data []byte, view *ApplicationView) (ApplicationRecoveryPurpose, error) {
					purpose, err := original(ctx, data, view)
					if purpose.ApplyBefore != nil {
						if name == "wrong-owner" {
							purpose.ApplyBefore.Owner.Digest[0] ^= 1
						} else {
							purpose.Reserve = &reserve
						}
					}
					return purpose, err
				})
			}
			before := readyMetadataFingerprint(t, s)
			if name == "orphan" {
				if err := s.Persist(rd); !errors.Is(err, ErrApplicationPrerequisite) {
					t.Fatal("ordinary orphan admitted", err)
				}
			} else {
				p := progressTestBegin(t, s, rd)
				full, err := p.PersistFull()
				want := ErrInvalid
				if name == "uncommitted" {
					want = ErrApplicationPrerequisite
				}
				if full || !errors.Is(err, want) {
					t.Fatal("unmatched dependency granted progress", full, err)
				}
			}
			if before != readyMetadataFingerprint(t, s) || s.views != 0 {
				t.Fatal("refusal wrote or leaked", name)
			}
		})
	}
}

func TestRecoveryDependencyHashBindsEntryOrderHeaderAndData(t *testing.T) {
	rd := recoveryDependencyReady()
	before := recoveryDependencyInputHash(rd.Entries)
	for _, name := range []string{"nil", "term", "index", "type", "data", "order"} {
		t.Run(name, func(t *testing.T) {
			entries := []*pb.Entry{ent(3, 3, "finish"), ent(4, 3, "new")}
			switch name {
			case "nil":
				entries[0] = nil
			case "term":
				entries[0].Term = new(uint64(4))
			case "index":
				entries[0].Index = new(uint64(2))
			case "type":
				entries[0].Type = new(pb.EntryConfChange)
			case "data":
				entries[0].Data = []byte("different")
			case "order":
				entries[0], entries[1] = entries[1], entries[0]
			}
			if before == recoveryDependencyInputHash(entries) {
				t.Fatalf("changed %s retained exact Ready hash", name)
			}
		})
	}
}

func TestRecoveryDependencyWriteAndSyncFailureNeverReturnPrefix(t *testing.T) {
	if phase := os.Getenv("RHO_RECOVERY_DEPENDENCY_FAULT"); phase != "" {
		toggle := &errorfs.Toggle{Injector: errorfs.InjectorFunc(func(op errorfs.Op) error {
			if phase == "write" && op.Kind == errorfs.OpFileWrite || phase == "sync" && (op.Kind == errorfs.OpFileSync || op.Kind == errorfs.OpFileSyncData) {
				return errorfs.ErrInjected
			}
			return nil
		})}
		s, _, _ := recoveryDependencyFixture(t, func(c *Config) {
			c.Dir = os.Getenv("RHO_RECOVERY_DEPENDENCY_DIR")
			c.FS = errorfs.Wrap(vfs.Default, toggle)
		})
		p := progressTestBegin(t, s, recoveryDependencyReady())
		if full, err := p.PersistFull(); full || err != nil {
			t.Fatal("actual dependency did not choose prefix", full, err)
		}
		toggle.On()
		if _, err := p.CommitPredecessor(); err != nil {
			t.Fatal(err)
		}
		_, _ = os.Stdout.WriteString("DEPENDENCY_PREFIX_RETURNED\n")
		t.Fatal("fault did not interrupt real prefix sync")
	}
	for _, phase := range []string{"write", "sync"} {
		t.Run(phase, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "db")
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRecoveryDependencyWriteAndSyncFailureNeverReturnPrefix$", "-test.count=1")
			child.Env = append(os.Environ(), "RHO_RECOVERY_DEPENDENCY_FAULT="+phase, "RHO_RECOVERY_DEPENDENCY_DIR="+dir)
			output, err := child.CombinedOutput()
			exit, ok := errors.AsType[*exec.ExitError](err)
			if !ok || ctx.Err() != nil || exit.ExitCode() != 1 || !bytes.Contains(output, []byte("fatal commit error")) || bytes.Contains(output, []byte("DEPENDENCY_PREFIX_RETURNED")) {
				t.Fatalf("actual uncertain prefix failure/reap missing: %v %s", err, output)
			}
			p, transfer := transferConfig(1)
			p.MaxInstallWrites, p.MaxInstallBytes = 1, 1664
			controls, recovery := ApplicationControlConfig{Version: 1}, ApplicationRecoveryConfig{Version: 1}
			transfer.Contract, err = ApplicationContractForPolicyWithRecovery(p, controls, recovery)
			if err != nil {
				t.Fatal(err)
			}
			s, err := Open(Config{Dir: dir, Application: p, Transfer: transfer, Controls: controls, Recovery: recovery, Generations: ApplicationGenerationLimits{2 << 30, 2000000}, PublishedCuts: ApplicationPublishedCutLimits{10000}, Replication: ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: ApplicationSemanticContractID{41}})
			if err != nil {
				t.Fatal("uncertain operation must recover retained store", err)
			}
			defer s.Close()
			index, image, err := s.Checkpoint()
			if err != nil || index != 2 || !bytes.Equal(image, []byte("old")) || s.meta.Last != 2 || s.meta.Recovery.Reservation.ID != ([32]byte{31}) {
				t.Fatal("failed prefix exposed retirement/new state", index, s.meta.Last, err)
			}
		})
	}
}
