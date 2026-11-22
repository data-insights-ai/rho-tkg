package tiered

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// COMMITORDER. ai-soc's cut writes event rows (day shards) and then its cut
// record on the reference shard, which names them. A process that dies
// between the shard flushes must never keep the reference record without the
// event rows: the reference shard commits last on every path. The child runs
// the store's own code on ai-soc's configuration (cfCfg: SyncWrites, a durable
// group's flush hold), fails the hot shard's next flush (the injected fault
// between the shard flushes) and exits without Close; the parent reopens.

const (
	coEnvDir  = "RHO_TIERED_COMMITORDER_DIR"
	coEnvCase = "RHO_TIERED_COMMITORDER_CASE"
	coLogons  = 3
)

var errCommitOrderInjected = errors.New("injected hot-shard flush fault")

// coOpen opens ai-soc's configuration with fixed label tokens: Logon (event)
// and User (reference).
func coOpen(t *testing.T, dir string) (ts *Store, logon, user uint16) {
	t.Helper()
	clock := &tierClock{}
	clock.set(julDay(22, 12))
	ts, err := New(cfCfg(dir, clock))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	logon, _ = reg.GetOrCreate("Logon")
	user, _ = reg.GetOrCreate("User")
	return ts, logon, user
}

// TestDurableFlush_ReferenceRecordImpliesEventRows: after the kill, a
// reference record that survived implies every event row written before it.
// "standalone": the group's hold is released and a standalone reference write
// flushes the reference shard (as any write after a group's release does
// before its DurableFlush) while the hot shard's flush fails: the reference
// write is refused and its record is not durable. "durable-flush": the group's
// DurableFlush meets the failing hot shard: the reference shard's flush
// retries it first, so the record and every event row are durable.
func TestDurableFlush_ReferenceRecordImpliesEventRows(t *testing.T) {
	for _, which := range []string{"standalone", "durable-flush"} {
		t.Run(which, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestCommitOrderChild$", "-test.count=1")
			cmd.Env = append(os.Environ(), coEnvDir+"="+dir, coEnvCase+"="+which)
			out, err := cmd.CombinedOutput()
			if cmd.ProcessState == nil {
				t.Fatalf("start the child: %v", err)
			}
			if code := cmd.ProcessState.ExitCode(); code != 3 {
				t.Fatalf("the child did not die in %q (exit %d):\n%s", which, code, out)
			}
			raw, err := os.ReadFile(filepath.Join(dir, "ids"))
			if err != nil {
				t.Fatal(err)
			}
			var ids []types.NodeID
			for _, f := range strings.Fields(string(raw)) {
				n, err := strconv.ParseUint(f, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, types.NodeID(n))
			}
			record, logons := ids[0], ids[1:]
			ts, _, _ := coOpen(t, dir)
			defer func() { _ = ts.Close() }()
			_, err = ts.GetNode(record)
			recordKept := err == nil
			if err != nil && !errors.Is(err, ErrNodeNotFound) {
				t.Fatalf("read the reference record: %v", err)
			}
			lost := 0
			for _, id := range logons {
				if _, err := ts.GetNode(id); errors.Is(err, ErrNodeNotFound) {
					lost++
				} else if err != nil {
					t.Fatalf("read a logon: %v", err)
				}
			}
			if recordKept && lost > 0 {
				t.Fatalf("torn commit: the reference record is durable, %d of its %d event rows are lost", lost, len(logons))
			}
			if which == "durable-flush" && (!recordKept || lost > 0) {
				t.Fatalf("durable flush: record kept %v, %d of %d event rows lost; want all durable", recordKept, lost, len(logons))
			}
		})
	}
}

// TestCommitOrderChild is the child of TestDurableFlush_ReferenceRecordImpliesEventRows.
func TestCommitOrderChild(t *testing.T) {
	dir := os.Getenv(coEnvDir)
	if dir == "" {
		t.Skip("child of the COMMITORDER test")
	}
	ts, logonTok, userTok := coOpen(t, dir)
	ts.HoldMutationFlushes() // a DurableCommit group (core holdGroupFlushes)
	ids := make([]string, 0, coLogons+1)
	record := types.NodeID(tieredNodeGen(t).Generate())
	ids = append(ids, strconv.FormatUint(uint64(record), 10))
	for range coLogons {
		id, err := cfPut(t, ts, logonTok, julDay(22, 12))
		if err != nil {
			t.Fatalf("PutNode logon: %v", err)
		}
		ids = append(ids, strconv.FormatUint(uint64(id), 10))
	}
	if err := ts.PutNode(types.NewNode(record, userTok, nil)); err != nil {
		t.Fatalf("PutNode record: %v", err)
	}
	ts.ReleaseMutationFlushes()
	if err := os.WriteFile(filepath.Join(dir, "ids"), []byte(strings.Join(ids, "\n")), 0o600); err != nil {
		t.Fatal(err)
	}
	ts.HotShardForTest().Store().FailNextFlushForTest(errCommitOrderInjected)
	switch os.Getenv(coEnvCase) {
	case "standalone":
		err := ts.PutNode(types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), userTok, nil))
		if err != nil && !errors.Is(err, errCommitOrderInjected) {
			t.Logf("standalone write: %v", err)
			os.Exit(4)
		}
	case "durable-flush":
		_ = ts.DurableFlush() // the hot shard's first flush fails; the joined error is expected
	}
	os.Exit(3) // the kill
}

// TestReferenceFlush_FlushesRowShardsFirst, without SyncWrites: a reference
// flush first flushes the hot shard's buffered rows (a background or Flush
// pass), and a durable reference flush (a durable meta write's path) first
// fsyncs the hot shard's rows an earlier flush wrote unsynced.
func TestReferenceFlush_FlushesRowShardsFirst(t *testing.T) {
	ts, caseTok, signalTok := newDurableTieredStore(t)
	hot, ref := ts.HotShardForTest().Store(), ts.RefShardForTest()
	put := func(tok uint16) {
		t.Helper()
		if err := ts.PutNode(types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	put(signalTok)
	put(caseTok)
	if hot.PendingWriteCount() == 0 || ref.PendingWriteCount() == 0 {
		t.Fatal("setup: the writes are not pending")
	}
	syncs := hot.DurableSyncCountForTest()
	if err := ref.Flush(); err != nil {
		t.Fatalf("reference Flush: %v", err)
	}
	if n := hot.PendingWriteCount(); n != 0 {
		t.Fatalf("hot shard: %d writes pending after the reference flush, want 0", n)
	}
	if got := hot.DurableSyncCountForTest(); got != syncs {
		t.Fatalf("a background reference flush synced the hot shard (%d -> %d)", syncs, got)
	}
	put(caseTok)
	if err := ref.DurableFlush(); err != nil {
		t.Fatalf("reference DurableFlush: %v", err)
	}
	if got := hot.DurableSyncCountForTest(); got != syncs+1 {
		t.Fatalf("hot shard syncs %d -> %d after a durable reference flush, want one sync of its unsynced rows", syncs, got)
	}
}

// TestReferenceFlush_RefusedAfterAShardLostRows: a row shard whose final flush
// at Close fails drops its buffered rows; every later reference flush is
// refused (its rows stay pending) until RecoverBackgroundError. ai-soc's
// configuration (no flush loop, so Close's final flush is the only one) with
// the writes held as in a durable group.
func TestReferenceFlush_RefusedAfterAShardLostRows(t *testing.T) {
	ts, logonTok, userTok := coOpen(t, t.TempDir())
	t.Cleanup(func() { _ = ts.Close() })
	ts.HoldMutationFlushes()
	if _, err := cfPut(t, ts, logonTok, julDay(22, 12)); err != nil {
		t.Fatalf("PutNode logon: %v", err)
	}
	if err := ts.PutNode(types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), userTok, nil)); err != nil {
		t.Fatalf("PutNode record: %v", err)
	}
	ts.ReleaseMutationFlushes()
	hot := ts.HotShardForTest().Store()
	hot.FailNextFlushForTest(errCommitOrderInjected)
	if err := hot.Close(); !errors.Is(err, errCommitOrderInjected) {
		t.Fatalf("close the hot shard: %v, want the injected fault", err)
	}
	ref := ts.RefShardForTest()
	for range 2 {
		if err := ref.Flush(); !errors.Is(err, errCommitOrderInjected) {
			t.Fatalf("reference flush after the loss: %v, want refused with the shard's fault", err)
		}
	}
	if ref.PendingWriteCount() == 0 {
		t.Fatal("the refused reference rows are not pending")
	}
	if err := ts.RecoverBackgroundError(); err != nil {
		t.Fatalf("RecoverBackgroundError: %v", err)
	}
	if err := ref.Flush(); err != nil {
		t.Fatalf("reference flush after recovery: %v", err)
	}
	if n := ref.PendingWriteCount(); n != 0 {
		t.Fatalf("reference shard: %d writes pending after recovery's flush, want 0", n)
	}
}

// TestReferenceFlush_WaitsForAClosingRowShard: a reference flush that meets a
// row shard whose Close has begun but not yet run its final flush waits for
// that flush before it writes its own rows. The hot shard's flushMu holds its
// Close before the final flush; the reference flush must park in the wait
// (seen on its goroutine's stack), not return.
func TestReferenceFlush_WaitsForAClosingRowShard(t *testing.T) {
	ts, logonTok, userTok := coOpen(t, t.TempDir())
	t.Cleanup(func() { _ = ts.Close() })
	ts.HoldMutationFlushes()
	if _, err := cfPut(t, ts, logonTok, julDay(22, 12)); err != nil {
		t.Fatalf("PutNode logon: %v", err)
	}
	if err := ts.PutNode(types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), userTok, nil)); err != nil {
		t.Fatalf("PutNode record: %v", err)
	}
	ts.ReleaseMutationFlushes()
	hot, ref := ts.HotShardForTest().Store(), ts.RefShardForTest()
	hot.LockFlushMuForTest()
	locked := true
	defer func() {
		if locked {
			hot.UnlockFlushMuForTest()
		}
	}()
	closeDone := make(chan error, 1)
	go func() { closeDone <- hot.Close() }()
	for !hot.Closing() {
		runtime.Gosched()
	}
	refDone := make(chan error, 1)
	go func() { refDone <- ref.Flush() }()
	for !coParkedInWait() {
		select {
		case err := <-refDone:
			t.Fatalf("the reference flush returned (%v) before the closing hot shard's final flush", err)
		default:
		}
		runtime.Gosched()
	}
	hot.UnlockFlushMuForTest()
	locked = false
	if err := <-closeDone; err != nil {
		t.Fatalf("close the hot shard: %v", err)
	}
	if err := <-refDone; err != nil {
		t.Fatalf("reference flush: %v", err)
	}
}

// coParkedInWait reports whether a goroutine is parked on a channel receive
// inside flushRowShardsBeforeReference.
func coParkedInWait() bool {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	for g := range bytes.SplitSeq(buf, []byte("\n\n")) {
		if bytes.Contains(g, []byte("[chan receive")) && bytes.Contains(g, []byte("flushRowShardsBeforeReference")) {
			return true
		}
	}
	return false
}
