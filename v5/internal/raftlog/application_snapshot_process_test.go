package raftlog

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
)

func snapshotKill() error {
	if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
		return err
	}
	for {
		time.Sleep(time.Hour)
	}
}
func TestSnapshotProcessKillLeavesOnlyCompleteActiveHistory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL process seam")
	}
	if phase := os.Getenv("RHO_APPLICATION_SNAPSHOT_KILL"); phase != "" {
		p, tc := transferConfig(1)
		toggle := new(errorfs.Toggle)
		var fs vfs.FS = vfs.Default
		if strings.HasPrefix(phase, "fault-") {
			fs = errorfs.Wrap(fs, toggle)
		}
		s, err := Open(Config{Dir: os.Getenv("RHO_APPLICATION_SNAPSHOT_DIR"), FS: fs, Create: true, Application: p, Transfer: tc})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Initialize([]uint64{1}, []byte("initial")); err != nil {
			t.Fatal(err)
		}
		applyApplication(t, s, "X", KV{Key: []byte("a"), Value: []byte("old")})
		applyApplication(t, s, "Y", KV{Key: []byte("a"), Deleted: true})
		if err := s.PublishSnapshot(); err != nil {
			t.Fatal(err)
		}
		e, err := s.BeginApplicationExport(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		m, _ := e.Manifest()
		chunks := exportChunks(t, e)
		applyApplication(t, s, "Z", KV{Key: []byte("b"), Value: []byte("latest")})
		if phase == "fault-begin" {
			toggle.On()
		}
		imp, err := s.BeginApplicationImport(t.Context(), m)
		if err != nil {
			t.Fatal(err)
		}
		if phase == "begin" {
			if err := snapshotKill(); err != nil {
				t.Fatal(err)
			}
		}
		if phase == "fault-page" {
			toggle.On()
		}
		for j, c := range chunks {
			if err := imp.Append(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			if phase == "page" && j == 0 {
				if err := snapshotKill(); err != nil {
					t.Fatal(err)
				}
			}
		}
		if phase == "final" {
			if err := snapshotKill(); err != nil {
				t.Fatal(err)
			}
		}
		if phase == "fault-verified" {
			toggle.On()
		}
		if err := imp.Verify(t.Context()); err != nil {
			t.Fatal(err)
		}
		if phase == "verified" {
			if err := snapshotKill(); err != nil {
				t.Fatal(err)
			}
		}
		if phase == "fault-abort" {
			toggle.On()
		}
		if err := imp.Abort(); err != nil {
			t.Fatal(err)
		}
		if phase == "abort" {
			if err := snapshotKill(); err != nil {
				t.Fatal(err)
			}
		}
		t.Fatal("crash seam returned")
	}
	for _, phase := range []string{"begin", "page", "final", "verified", "abort", "fault-begin", "fault-page", "fault-verified", "fault-abort"} {
		t.Run(phase, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "db")
			ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSnapshotProcessKillLeavesOnlyCompleteActiveHistory$")
			child.Env = append(os.Environ(), "RHO_APPLICATION_SNAPSHOT_KILL="+phase, "RHO_APPLICATION_SNAPSHOT_DIR="+dir)
			out, err := child.CombinedOutput()
			exit, ok := errors.AsType[*exec.ExitError](err)
			if !ok || ctx.Err() != nil {
				t.Fatalf("not SIGKILL: %v %s", err, out)
			}
			if strings.HasPrefix(phase, "fault-") {
				if exit.ExitCode() != 1 || !bytes.Contains(out, []byte("fatal commit error")) {
					t.Fatalf("storage fault returned: %v %s", err, out)
				}
			} else {
				ws, ok := exit.Sys().(syscall.WaitStatus)
				if !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
					t.Fatalf("not SIGKILL: %v %s", err, out)
				}
			}
			p, tc := transferConfig(1)
			s, err := Open(Config{Dir: dir, Application: p, Transfer: tc})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if err := s.ScrubApplication(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := s.Scrub(); err != nil {
				t.Fatal(err)
			}
			usage, err := s.ApplicationTransferUsage()
			if err != nil || usage.Import || usage.StagedRecords != 0 {
				t.Fatal(usage, err)
			}
			idx, image, err := s.Checkpoint()
			if err != nil || idx != 4 || !bytes.Equal(image, []byte("Z")) {
				t.Fatal(idx, string(image), err)
			}
			getApplication(t, viewApplication(t, s, 2), "a", "old", true, false)
			getApplication(t, viewApplication(t, s, 3), "a", "", true, true)
			getApplication(t, viewApplication(t, s, 4), "b", "latest", true, false)
			for index := uint64(1); index <= 4; index++ {
				for _, outcome := range []bool{false, true} {
					record, err := s.ApplicationRecord(t.Context(), index, outcome, 1024)
					want := ""
					if index > 1 {
						prefix := "change:"
						if outcome {
							prefix = "outcome:"
						}
						want = prefix + map[uint64]string{2: "X", 3: "Y", 4: "Z"}[index]
					}
					if err != nil || string(record) != want {
						t.Fatal("recovered envelope lost content", index, outcome, string(record), want, err)
					}
				}
			}
		})
	}
}
