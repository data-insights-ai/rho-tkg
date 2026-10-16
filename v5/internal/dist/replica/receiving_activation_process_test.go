package replica

import (
	"bytes"

	"errors"
	"fmt"
	"os"

	"path/filepath"

	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/testutil"
)

func TestApplicationReceiverActivationRealProcessFaultsWithholdACK(t *testing.T) {
	if mode := os.Getenv("RHO_REPLACEMENT_ACTIVATION_FAULT"); mode != "" {
		toggle := &errorfs.Toggle{Injector: errorfs.InjectorFunc(func(op errorfs.Op) error {
			if mode == "write" && op.Kind == errorfs.OpFileWrite || mode == "sync" && (op.Kind == errorfs.OpFileSync || op.Kind == errorfs.OpFileSyncData) {
				_, _ = fmt.Fprintf(os.Stdout, "REPLACEMENT_ACTIVATION_INJECTED_OP mode=%s kind=%v error=%v\n", mode, op.Kind, errorfs.ErrInjected)
				return errorfs.ErrInjected
			}
			return nil
		})}
		f := receivingActivationNativeFixtureAt(t, errorfs.Wrap(vfs.Default, toggle), os.Getenv("RHO_REPLACEMENT_ACTIVATION_DIR"))
		factory := f.options.NewDriver
		f.options.NewDriver = func() (Config, error) {
			_, _ = os.Stdout.WriteString("REPLACEMENT_DRIVER_FACTORY_CALLED\n")
			return factory()
		}
		checks := 0
		f.options.Check = func() error {
			checks++
			// The genuine RawNode already produced its snapshot Ready; the
			// following sync is specifically Store snapshot activation.
			if checks == 3 {
				if mode == "before-sync" {
					return killSelfForCrashTest()
				}
				toggle.On()
			}
			return nil
		}
		if mode == "after-sync" {
			f.options.NewDriver = func() (Config, error) { return Config{}, killSelfForCrashTest() }
		}
		_, _ = os.Stdout.WriteString("REPLACEMENT_ACTIVATION_READY\n")
		d, out, err := f.r.ActivateApplicationSnapshot(t.Context(), f.packet, f.options)
		_, _ = os.Stdout.WriteString("REPLACEMENT_ACTIVATION_RETURNED_OUTPUT\n")
		t.Fatalf("fatal activation unexpectedly returned: %v %+v %v", d, out, err)
	}
	for _, mode := range []string{"before-sync", "write", "sync", "after-sync"} {
		t.Run(mode, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "replacement")
			r := testutil.RunChild(t, "TestApplicationReceiverActivationRealProcessFaultsWithholdACK", []string{"RHO_REPLACEMENT_ACTIVATION_FAULT=" + mode, "RHO_REPLACEMENT_ACTIVATION_DIR=" + dir}, 15*time.Second)
			if r.Exit == nil || r.TimedOut || !r.Contains("REPLACEMENT_ACTIVATION_READY") || r.Contains("REPLACEMENT_ACTIVATION_RETURNED_OUTPUT") {
				t.Fatalf("activation fault produced output or never reached its seam: %v %s", r.Err, r.Output)
			}
			if mode != "after-sync" && r.Contains("REPLACEMENT_DRIVER_FACTORY_CALLED") {
				t.Fatal("persistence failure constructed semantic Driver/Host factory")
			}
			if mode == "before-sync" || mode == "after-sync" {
				if !r.Killed() {
					t.Fatal("missing actual SIGKILL", mode, r.Exit)
				}
			} else if r.ExitCode != 1 || !r.Contains("fatal commit error") || !r.Contains("REPLACEMENT_ACTIVATION_INJECTED_OP mode="+mode+" kind=") || !r.Contains("error="+errorfs.ErrInjected.Error()) {
				t.Fatalf("activation I/O failure not fail-stop: %v %s", r.Err, r.Output)
			}
			for line := range bytes.SplitSeq(r.Output, []byte{'\n'}) {
				if bytes.HasPrefix(line, []byte("REPLACEMENT_ACTIVATION_INJECTED_OP ")) {
					t.Logf("actualChildPID=%d injectorWitness=%s", r.PID, line)
				}
			}
			// Match the real fixture's persisted binding/profile; never initialize
			// or reuse this receiving identity after a known term or activation.
			p := raftlog.DefaultApplicationPolicy(3)
			p.MaxInstallWrites, p.RetainedApplicationRecords, p.RetainedApplicationBytes = 256, 8192, 16<<20
			c := raftlog.Config{Dir: dir, Application: p,
				Transfer:    raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{101}, Partition: 1, Group: [16]byte{102}}, Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.DefaultApplicationTransferLimits()},
				Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 256 << 20, MaxRecords: 32768}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 100000}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: raftlog.ApplicationSemanticContractID{17}}
			s, err := raftlog.Open(c)
			if err != nil {
				t.Fatal("whole acknowledged state did not reopen", err)
			}
			defer s.Close()
			index, image, err := s.Checkpoint()
			hard, conf, stateErr := s.InitialState()
			if err != nil || stateErr != nil || hard.GetTerm() != 2 || hard.GetVote() != 0 {
				t.Fatal(index, image, hard, err, stateErr)
			}
			if err := s.Initialize([]uint64{1, 2, 3}, nil); !errors.Is(err, raftlog.ErrInvalid) {
				t.Fatal("restart reused known receiving identity", err)
			}
			if index == 0 {
				if mode == "after-sync" || len(image) != 0 || hard.GetCommit() != 0 || len(conf.GetVoters()) != 0 {
					t.Fatal("acknowledged activation lost or partial old state", mode, index, hard, conf)
				}
				if _, err := s.ApplicationReceivingState(); err != nil {
					t.Fatal("pre-activation restart lost receiving state", err)
				}
				m := &capacityRegressionMachine{t: t, store: s}
				if d, err := Open(Config{ID: 3, Store: s, ApplicationMachine: m, ApplicationSnapshotSends: ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}}); d != nil || !errors.Is(err, ErrInvalid) {
					t.Fatal("empty receiving state became normal Driver", d, err)
				}
			} else {
				if mode == "before-sync" || index != 6 || hard.GetCommit() != index || len(conf.GetVoters()) != 3 {
					t.Fatal("partial activated consensus/image", mode, index, hard, conf)
				}
				m := &capacityRegressionMachine{t: t, store: s}
				d, err := Open(Config{ID: 3, Store: s, ApplicationMachine: m, ApplicationSnapshotSends: ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
				if err != nil {
					t.Fatal("retained activated state unavailable", err)
				}
				if d.Applied() != index || m.value != 10 {
					t.Fatal("acknowledged snapshot lost/doubled", d.Applied(), m.value)
				}
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
			}
			t.Logf("mode=%s actualChildPID=%d waitStatus=%d signal=%s returnedACK=false recoveredIndex=%d term=%d", mode, r.PID, int(r.Status), r.Status.Signal(), index, hard.GetTerm())
		})
	}
}
