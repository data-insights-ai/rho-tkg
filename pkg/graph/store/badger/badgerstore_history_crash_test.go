package badger

import (
	"os"
	"strings"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

func historyCrashConfig(dir string) Config {
	cfg := membershipCrashConfig(dir)
	cfg.HistoryDeltaEncoding = true
	cfg.HistoryAnchorInterval = 2
	return cfg
}

func TestHistoryWriteAheadCrashChild(t *testing.T) {
	dir := os.Getenv("RHO_HISTORY_CRASH_DIR")
	if dir == "" {
		t.Skip("subprocess only")
	}
	bs, e := New(historyCrashConfig(dir))
	if e != nil {
		t.Fatal(e)
	}
	mode := os.Getenv("RHO_HISTORY_CRASH_MODE")
	if mode == "anchors" || mode == "deltas" {
		bs.historicalMembershipFlushTestHook = func(stage string) {
			if stage == "after-history-"+mode {
				os.Exit(17)
			}
		}
	} else {
		bs.historicalMembershipDataTestHook = func(n int) {
			if n != 3500 {
				return
			}
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				changed := 0
				e := bs.db.View(func(txn *badgerv4.Txn) error {
					it := txn.NewIterator(badgerv4.DefaultIteratorOptions)
					defer it.Close()
					p := []byte{storepkg.KeyNode}
					for it.Seek(p); it.ValidForPrefix(p); it.Next() {
						raw, e := it.Item().ValueCopy(nil)
						if e != nil {
							return e
						}
						var w storepkg.NodeWire
						if e = storepkg.SafeUnmarshal(raw, &w); e != nil {
							return e
						}
						if w.TxFrom == 50 {
							changed++
						}
					}
					return nil
				})
				if e != nil {
					t.Fatal(e)
				}
				if changed > 0 && changed < 6000 {
					os.Exit(17)
				}
				time.Sleep(time.Millisecond)
			}
			os.Exit(19)
		}
	}
	ns, e := bs.AllNodes(QueryOpts{})
	if e != nil {
		t.Fatal(e)
	}
	rs, e := bs.AllRelationships(QueryOpts{})
	if e != nil {
		t.Fatal(e)
	}
	bs.BeginGroupCommit()
	for _, n := range ns {
		for j := range 4 {
			next := n.DeepCopy()
			next.SetVersion(n.Version() + 1)
			tm := *next.Temporal()
			tm.TxFrom = types.Instant(20 + j*10)
			tm.TxTo = 0
			next.SetTemporal(&tm)
			prev := n.DeepCopy()
			oldTM := *prev.Temporal()
			oldTM.TxTo = tm.TxFrom
			prev.SetTemporal(&oldTM)
			if e = next.SetProperty("value", "changed"); e != nil {
				t.Fatal(e)
			}
			if e = next.SetProperty("payload", strings.Repeat("n", 2048)); e != nil {
				t.Fatal(e)
			}
			if e = bs.ReplaceNodeWithHistory(next, n.Version(), prev); e != nil {
				t.Fatal(e)
			}
			n = next
		}
	}
	for _, r := range rs {
		for j := range 4 {
			next := r.DeepCopy()
			next.SetVersion(r.Version() + 1)
			tm := *next.Temporal()
			tm.TxFrom = types.Instant(20 + j*10)
			tm.TxTo = 0
			next.SetTemporal(&tm)
			prev := r.DeepCopy()
			oldTM := *prev.Temporal()
			oldTM.TxTo = tm.TxFrom
			prev.SetTemporal(&oldTM)
			if e = next.SetProperty("value", "changed"); e != nil {
				t.Fatal(e)
			}
			if e = next.SetProperty("payload", strings.Repeat("r", 2048)); e != nil {
				t.Fatal(e)
			}
			if e = bs.ReplaceRelWithHistory(next, r.Version(), prev); e != nil {
				t.Fatal(e)
			}
			r = next
		}
	}
	if e = bs.EndGroupCommit(); e != nil {
		t.Fatal(e)
	}
	t.Fatal("crash seam not reached")
}
