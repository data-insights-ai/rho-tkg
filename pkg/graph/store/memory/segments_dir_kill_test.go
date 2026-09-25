//go:build unix

package memory

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/segdir"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ADR-0011 S3 correctness gate: kill the process (SIGKILL, no deferred
// cleanup, no Close) at every step of a seal to disk, then reopen the
// directory. The reopened store must equal the oracle — an undeclared store
// holding exactly the rows of the COMMITTED seals, as sealed:
//
//   - killed after the segment write, after its fsync, or after the manifest
//     .tmp write: the killed seal never happened (its file is an orphan or a
//     .tmp, removed at reopen);
//   - killed after the manifest is durable, before the rows leave the row
//     store: the seal happened (the manifest is the truth).
//
// The child process is this test binary re-run on TestSegmentDirKillChild
// with the directory and the step in its environment.

const (
	killEnvDir  = "RHO_TKG_SEGDIR_KILL_DIR"
	killEnvStep = "RHO_TKG_SEGDIR_KILL_STEP"
	killNone    = "none" // the seal completes; the process exits without Close
)

// killPlan is what the parent needs to rebuild the oracle.
type killPlan struct {
	sealedA []*types.Relationship // rows of seal A, as sealed
	newB    []types.RelID         // the rows seal B takes
}

// runKillWorkload is the child's (and the parent's replayed) workload:
// phase A (60 HOP + 10 OTHER rows), seal A; phase B (3 updates and 2 deletes
// of rows sealed by A — faulted in, so seal B does not take them — and 50 new
// HOP rows), then sealB.
func runKillWorkload(t *testing.T, tw *segTwin, sealB func()) killPlan {
	t.Helper()
	r := rand.New(rand.NewSource(211))
	for i := 0; i < 60; i++ {
		tw.put(r, segTestHOP)
	}
	for i := 0; i < 10; i++ {
		tw.put(r, segTestOther)
	}
	var plan killPlan
	rows, err := tw.plain.RelationshipsByType(segTestHOP, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		plan.sealedA = append(plan.sealedA, row.DeepCopy())
	}
	if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
		t.Fatalf("seal A: %v", err)
	}
	for k := 0; k < 3; k++ {
		id := plan.sealedA[k*7].ID()
		cur, err := tw.plain.GetRelationship(id)
		if err != nil {
			t.Fatal(err)
		}
		prev := cur.DeepCopy()
		prev.Temporal().TxTo = tw.tick()
		next := cur.DeepCopy()
		next.SetVersion(cur.Version() + 1)
		next.Temporal().TxFrom = prev.Temporal().TxTo
		if err := next.SetProperty("weight", float64(777)); err != nil {
			t.Fatal(err)
		}
		segHashed(next)
		tw.both("ReplaceRelWithHistory", func(s *Store) error { return s.ReplaceRelWithHistory(next, cur.Version(), prev) })
	}
	for k := 0; k < 2; k++ {
		id := plan.sealedA[k*11+1].ID()
		tw.both("DeleteRelationship", func(s *Store) error { return s.DeleteRelationship(id) })
	}
	for i := 0; i < 50; i++ {
		plan.newB = append(plan.newB, tw.put(r, segTestHOP))
	}
	sealB()
	return plan
}

// TestSegmentDirKillChild is the child process body; it skips unless the
// parent set its environment.
func TestSegmentDirKillChild(t *testing.T) {
	dir, step := os.Getenv(killEnvDir), os.Getenv(killEnvStep)
	if dir == "" || step == "" {
		t.Skip("child of TestSegmentDir_KillAtEverySealStep")
	}
	tw := newSegTwin(t, 1<<40, 8)
	if err := tw.declared.OpenRelSegmentDir(dir); err != nil {
		t.Fatal(err)
	}
	runKillWorkload(t, tw, func() {
		setSegDirHookForTest(t, tw.declared, func(s string) error {
			if s == step {
				fmt.Fprintf(os.Stdout, "KILLED-AT %s\n", s)
				_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
				time.Sleep(time.Minute) // SIGKILL is not deliverable-late; never reached
			}
			return nil
		})
		if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
			t.Fatalf("seal B: %v", err)
		}
	})
	fmt.Fprintf(os.Stdout, "SEALED-B\n")
	os.Exit(0) // a crash after a completed seal: no Close
}

func TestSegmentDir_KillAtEverySealStep(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	steps := []struct {
		step      string
		committed bool
	}{
		{segdir.StepSegmentWritten, false},
		{segdir.StepSegmentSynced, false},
		{segdir.StepManifestWritten, false},
		{segdir.StepManifestDurable, true},
		{killNone, true},
	}
	for _, tc := range steps {
		t.Run(tc.step, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestSegmentDirKillChild$", "-test.count=1", "-test.v")
			cmd.Env = append(os.Environ(), killEnvDir+"="+dir, killEnvStep+"="+tc.step)
			out, err := cmd.CombinedOutput()
			var ee *exec.ExitError
			switch {
			case tc.step == killNone:
				if err != nil || !strings.Contains(string(out), "SEALED-B") {
					t.Fatalf("child: %v\n%s", err, out)
				}
			case !errors.As(err, &ee) || !ee.Sys().(syscall.WaitStatus).Signaled() || !strings.Contains(string(out), "KILLED-AT "+tc.step):
				t.Fatalf("child was not killed at %s: %v\n%s", tc.step, err, out)
			}
			// The directory as the crash left it (before any cleanup).
			var leftTmp, leftSeg int
			for _, name := range segDirFiles(t, dir) {
				if strings.HasSuffix(name, ".tmp") {
					leftTmp++
				}
			}
			leftSeg = len(segDirSegmentFiles(t, dir))
			t.Logf("crash left %d segment files, %d .tmp files: %v", leftSeg, leftTmp, segDirFiles(t, dir))

			// Replay the workload without a directory to know the rows.
			ref := newSegTwin(t, 1<<40, 8)
			plan := runKillWorkload(t, ref, func() {})
			want := append([]*types.Relationship(nil), plan.sealedA...)
			if tc.committed {
				for _, id := range plan.newB {
					row, err := ref.plain.GetRelationship(id)
					if err != nil {
						t.Fatal(err)
					}
					want = append(want, row)
				}
			}
			re := reopenSegDir(t, dir, segTestDecl(1<<40), ref.nodes)
			assertSegDirConsistent(t, re, dir)
			wantSegs := 1
			if tc.committed {
				wantSegs = 2
			}
			if st, err := re.RelSegmentStats(segTestHOP); err != nil || st.Segments != wantSegs || st.SealedRows != int64(len(want)) {
				t.Fatalf("reopened after a kill at %s: %+v %v; want %d segments, %d rows", tc.step, st, err, wantSegs, len(want))
			}
			tw := &segTwin{t: t, plain: oracleOf(t, ref.nodes, want), declared: re, nodes: ref.nodes, rels: ref.rels, clock: ref.clock}
			tw.compare("reopened after a kill at " + tc.step)
		})
	}
}
