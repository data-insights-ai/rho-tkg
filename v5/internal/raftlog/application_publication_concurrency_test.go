package raftlog

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/data-insights-ai/rho-tkg/v5/internal/testutil"
)

const publicationTestWait = 5 * time.Second

func publicationTestGate(t *testing.T) (chan struct{}, chan struct{}, func()) {
	t.Helper()
	reached, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	t.Cleanup(release)
	return reached, resume, release
}
func publicationTestWorker(t *testing.T, release func(), work func() error) <-chan error {
	t.Helper()
	result := make(chan error, 1)
	done := make(chan struct{})
	// Release before joining within this cleanup, independent of registration
	// order among workers or a Fatal before the main test reaches release.
	t.Cleanup(func() {
		release()
		select {
		case <-done:
		case <-time.After(publicationTestWait):
			t.Error("publication worker cleanup join timed out")
		}
	})
	go func() { defer close(done); result <- work() }()
	return result
}
func publicationTestResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(publicationTestWait):
		t.Fatal("publication worker result timed out")
		return ErrInvalid
	}
}
func publicationTestReached(t *testing.T, reached <-chan struct{}) {
	t.Helper()
	select {
	case <-reached:
	case <-time.After(publicationTestWait):
		t.Fatal("publication gate not reached")
	}
}

func TestPublicationGateEarlyFailureCleanlyJoinsProcess(t *testing.T) {
	if phase := os.Getenv("RHO_PUBLICATION_TEST_FAILURE"); phase != "" {
		t.Cleanup(func() { _, _ = os.Stdout.WriteString("PUBLICATION_FAILURE_CLEANUP_COMPLETED\n") })
		s := publicationStore(t, vfs.NewMem())
		generationApply(t, s, "old", KV{Key: []byte("a"), Value: []byte("old")})
		reached, resume, release := publicationTestGate(t)
		if phase == "export" {
			if err := s.PublishSnapshot(); err != nil {
				t.Fatal(err)
			}
			cut, _ := s.PublishedApplicationCut()
			e, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			publicationTestWorker(t, func() { release(); cancel() }, func() error {
				_, err := e.BuildManifest(ctx, ReadBudget{1, 4096})
				close(reached)
				select {
				case <-resume:
				case <-ctx.Done():
					return ctx.Err()
				}
				if err != nil {
					return err
				}
				_, err = e.BuildManifest(ctx, ReadBudget{1, 4096})
				return err
			})
		} else {
			s.publicationCaptureHook = func() { close(reached); <-resume }
			publicationTestWorker(t, release, s.PublishSnapshot)
		}
		publicationTestReached(t, reached)
		if phase == "reclaim" {
			publicationTestWorker(t, release, s.ReclaimApplication)
		}
		if phase == "close" {
			publicationTestWorker(t, release, s.Close)
		}
		t.Fatal("intentional gated failure")
	}
	for _, phase := range []string{"capture", "reclaim", "close", "export"} {
		t.Run(phase, func(t *testing.T) {
			r := testutil.RunChild(t, "TestPublicationGateEarlyFailureCleanlyJoinsProcess", []string{"RHO_PUBLICATION_TEST_FAILURE=" + phase}, 15*time.Second)
			if r.Exit == nil || r.ExitCode != 1 || r.TimedOut || !r.Contains("intentional gated failure") || !r.Contains("PUBLICATION_FAILURE_CLEANUP_COMPLETED") || r.Contains("cleanup join timed out") {
				t.Fatalf("failure cleanup did not finish: %v %s", r.Err, r.Output)
			}
		})
	}
}
