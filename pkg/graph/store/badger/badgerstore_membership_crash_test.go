package badger

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

func membershipCrashConfig(dir string) Config {
	return Config{Dir: dir, HistoricalMembershipOnDisk: true, SyncWrites: true, MemTableSize: 8 << 20, ValueLogFileSize: 8 << 20, CacheCapacity: 128, CacheBudgetBytes: 1 << 20}
}
func membershipCrashSeed(t *testing.T, dir string) {
	t.Helper()
	cfg := membershipCrashConfig(dir)
	cfg.PropertyKeyRegistry = registrypkg.NewPropertyKeyRegistry()
	bs, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	n := types.NewNode(1, 1, nil)
	n.SetTemporal(&types.TemporalMetadata{TxFrom: 10})
	if e = n.SetProperty("value", "seed"); e != nil {
		t.Fatal(e)
	}
	if e = n.SetProperty("payload", ""); e != nil {
		t.Fatal(e)
	}
	if e = bs.PutNode(n); e != nil {
		t.Fatal(e)
	}
	if e = bs.CreatePropertyIndex(1, "value"); e != nil {
		t.Fatal(e)
	}
	for {
		p, e := bs.BuildHistoricalMembership(t.Context(), HistoricalMembershipLimits{MaxRows: 1000, MaxBytes: 8 << 20})
		if e != nil {
			t.Fatal(e)
		}
		if p.Complete {
			break
		}
	}
	if e = bs.Close(); e != nil {
		t.Fatal(e)
	}
}
func TestHistoricalMembershipCrashChild(t *testing.T) {
	dir := os.Getenv("RHO_MEMBERSHIP_CRASH_DIR")
	if dir == "" {
		t.Skip("subprocess only")
	}
	mode := os.Getenv("RHO_MEMBERSHIP_CRASH_MODE")
	bs, e := New(membershipCrashConfig(dir))
	if e != nil {
		t.Fatal(e)
	}
	switch mode {
	case "after-posting-sets", "before-membership-delete", "bootstrap-before-cursor":
		bs.historicalMembershipFlushTestHook = func(stage string) {
			if stage == "after-posting-sets" {
				os.Exit(17)
			}
		}
	case "after-source-delete":
		bs.historicalMembershipFlushTestHook = func(stage string) {
			if stage == "after-source-rows" {
				os.Exit(17)
			}
		}
	case "mid-source":
		bs.historicalMembershipDataTestHook = func(n int) {
			if n == 5000 {
				count := 0
				deadline := time.Now().Add(5 * time.Second)
				for count == 0 && time.Now().Before(deadline) {
					_ = bs.db.View(func(txn *badgerv4.Txn) error {
						opts := badgerv4.DefaultIteratorOptions
						opts.PrefetchValues = false
						it := txn.NewIterator(opts)
						defer it.Close()
						p := []byte{storepkg.KeyNode}
						for it.Seek(p); it.ValidForPrefix(p); it.Next() {
							count++
						}
						return nil
					})
					if count <= 1 {
						count = 0
						time.Sleep(time.Millisecond)
					}
				}
				if count <= 1 {
					os.Exit(19)
				}
				os.Exit(17)
			}
		}
	default:
		t.Fatal("unknown subprocess mode")
	}
	switch mode {
	case "bootstrap-before-cursor":
		_, e = bs.BuildHistoricalMembership(t.Context(), HistoricalMembershipLimits{MaxRows: 10000, MaxBytes: 64 << 20})
		if e != nil {
			t.Fatal(e)
		}
	case "before-membership-delete", "after-source-delete":
		_, e = bs.ExactErase(storecontract.ExactErasureRequest{NodeIDs: []types.NodeID{1}, Bounds: storecontract.ExactErasureBounds{MaxRelationshipIdentities: 10, MaxRelationshipVersions: 10, MaxEndpointNodeIdentities: 10}})
		if e != nil {
			t.Fatal(e)
		}
	default:
		bs.BeginGroupCommit()
		for i := 2; i < 20002; i++ {
			n := types.NewNode(types.NodeID(i), 1, nil)
			n.SetTemporal(&types.TemporalMetadata{TxFrom: 20})
			if e = n.SetProperty("value", fmt.Sprintf("new-%d", i)); e != nil {
				t.Fatal(e)
			}
			if e = n.SetProperty("payload", strings.Repeat("p", 2048)); e != nil {
				t.Fatal(e)
			}
			if e = bs.PutNode(n); e != nil {
				t.Fatal(e)
			}
		}
		if e = bs.EndGroupCommit(); e != nil {
			t.Fatal(e)
		}
	}
	t.Fatal("crash seam not reached")
}
func TestHistoricalMembershipLargeSplitFlushCrashKeepsEveryDurableCandidate(t *testing.T) {
	for _, mode := range []string{"after-posting-sets", "mid-source", "before-membership-delete", "after-source-delete"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			membershipCrashSeed(t, dir)
			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestHistoricalMembershipCrashChild$")
			cmd.Env = append(os.Environ(), "RHO_MEMBERSHIP_CRASH_DIR="+dir, "RHO_MEMBERSHIP_CRASH_MODE="+mode)
			out, e := cmd.CombinedOutput()
			exit, ok := errors.AsType[*exec.ExitError](e)
			if !ok || exit.ExitCode() != 17 {
				t.Fatalf("expected injected crash17: %v %s", e, out)
			}
			bs, e := New(membershipCrashConfig(dir))
			if e != nil {
				t.Fatal(e)
			}
			defer bs.Close()
			if !bs.historicalMembershipReady.Load() {
				t.Fatal("complete pre-crash inventory lost")
			}
			members := map[types.NodeID]bool{}
			if e = bs.ForEachLabelTxMember(1, func(id types.NodeID, _ types.Instant) bool { members[id] = true; return true }); e != nil {
				t.Fatal(e)
			}
			rows, e := bs.AllNodes(QueryOpts{})
			if e != nil {
				t.Fatal(e)
			}
			for _, n := range rows {
				if !members[n.ID()] {
					t.Fatal("durable row lost historical membership", n.ID())
				}
				v, _ := n.GetProperty("value")
				seen := false
				if e = bs.ForEachNodePropertyTxMember(1, "value", types.IndexablePropertyValueKey(v), func(id types.NodeID, _ types.Instant) bool { seen = seen || id == n.ID(); return true }); e != nil || !seen {
					t.Fatal("durable property lost candidate", n.ID(), e)
				}
			}
			if mode == "mid-source" && (len(rows) <= 1 || len(rows) >= 20001) {
				t.Fatal("test did not cross a split transaction", len(rows))
			}
			if mode == "before-membership-delete" && (len(rows) != 1 || !members[1]) {
				t.Fatal("posting cleanup preceded durable erasure", len(rows), members[1])
			}
			if mode == "after-source-delete" && len(rows) != 0 {
				t.Fatal("source deletion seam did not commit", len(rows))
			}
			t.Logf("crash=%s durable rows=%d candidate IDs=%d", mode, len(rows), len(members))
		})
	}
}
func TestHistoricalMembershipSplitBootstrapCrashDoesNotPublishCursor(t *testing.T) {
	dir := t.TempDir()
	cfg := membershipCrashConfig(dir)
	cfg.HistoricalMembershipOnDisk = false
	cfg.PropertyKeyRegistry = registrypkg.NewPropertyKeyRegistry()
	bs, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	if e = bs.CreatePropertyIndex(1, "value"); e != nil {
		t.Fatal(e)
	}
	bs.BeginGroupCommit()
	for i := 1; i <= 12000; i++ {
		n := types.NewNode(types.NodeID(i), 1, nil)
		n.SetTemporal(&types.TemporalMetadata{TxFrom: 10})
		if e = n.SetProperty("value", fmt.Sprintf("old-%d", i)); e != nil {
			t.Fatal(e)
		}
		if e = n.SetProperty("payload", strings.Repeat("b", 2048)); e != nil {
			t.Fatal(e)
		}
		if e = bs.PutNode(n); e != nil {
			t.Fatal(e)
		}
	}
	if e = bs.EndGroupCommit(); e != nil {
		t.Fatal(e)
	}
	if e = bs.Close(); e != nil {
		t.Fatal(e)
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestHistoricalMembershipCrashChild$")
	cmd.Env = append(os.Environ(), "RHO_MEMBERSHIP_CRASH_DIR="+dir, "RHO_MEMBERSHIP_CRASH_MODE=bootstrap-before-cursor")
	out, e := cmd.CombinedOutput()
	exit, ok := errors.AsType[*exec.ExitError](e)
	if !ok || exit.ExitCode() != 17 {
		t.Fatalf("expected crash17: %v %s", e, out)
	}
	bs, e = New(membershipCrashConfig(dir))
	if e != nil {
		t.Fatal(e)
	}
	defer bs.Close()
	p, e := bs.historicalMembershipProgress()
	if e != nil || p.Rows != 0 || p.Phase != 0 || p.Complete || bs.historicalMembershipReady.Load() {
		t.Fatal("cursor/Complete preceded posting barrier", p, e)
	}
	for !p.Complete {
		p, e = bs.BuildHistoricalMembership(t.Context(), HistoricalMembershipLimits{MaxRows: 1000, MaxBytes: 8 << 20})
		if e != nil {
			t.Fatal(e)
		}
	}
	seen := map[types.NodeID]bool{}
	if e = bs.ForEachLabelTxMember(1, func(id types.NodeID, _ types.Instant) bool { seen[id] = true; return true }); e != nil {
		t.Fatal(e)
	}
	for i := 1; i <= 12000; i++ {
		if !seen[types.NodeID(i)] {
			t.Fatal("resumed bootstrap omitted row", i)
		}
	}
	if len(seen) != 12000 {
		t.Fatal("phantom bootstrap member", len(seen))
	}
	t.Logf("split bootstrap resumed all%d exact members; original cursor remained0 after posting-only crash", len(seen))
}
