package badger_test

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	rho "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func historyCrashConfig(dir string) badger.Config {
	return badger.Config{Dir: dir, HistoricalMembershipOnDisk: true, SyncWrites: true, HistoryDeltaEncoding: true, HistoryAnchorInterval: 2, MemTableSize: 8 << 20, ValueLogFileSize: 8 << 20, CacheCapacity: 128, CacheBudgetBytes: 1 << 20}
}
func TestHistoryWriteAheadSplitCrashPreservesPublishedNodeAndRelPins(t *testing.T) {
	dir := t.TempDir()
	bs, e := badger.New(historyCrashConfig(dir))
	if e != nil {
		t.Fatal(e)
	}
	g, e := rho.New(rho.Config{Store: bs, AllowTxBackfill: true})
	if e != nil {
		t.Fatal(e)
	}
	if e = g.Index().CreateProperty("N", "value"); e != nil {
		t.Fatal(e)
	}
	if e = g.Index().CreateRelProperty("R", "value"); e != nil {
		t.Fatal(e)
	}
	bs.BeginGroupCommit()
	var first, second *types.Node
	for i := range 6000 {
		n, e := g.Nodes().AddWithTx(t.Context(), []string{"N"}, map[string]any{"value": "published", "payload": strings.Repeat("p", 2048), "tkg_valid_from": types.Instant(1)}, 10)
		if e != nil {
			t.Fatal(e)
		}
		if i == 0 {
			first = n
		}
		if i == 1 {
			second = n
		}
	}
	for range 6000 {
		if _, e = g.Rels().AddWithTx(t.Context(), "R", first, second, map[string]any{"value": "published", "payload": strings.Repeat("p", 2048), "tkg_valid_from": types.Instant(1)}, 10); e != nil {
			t.Fatal(e)
		}
	}
	if e = bs.EndGroupCommit(); e != nil {
		t.Fatal(e)
	}
	for {
		p, e := bs.BuildHistoricalMembership(t.Context(), badger.HistoricalMembershipLimits{MaxRows: 1000, MaxBytes: 8 << 20})
		if e != nil {
			t.Fatal(e)
		}
		if p.Complete {
			break
		}
	}
	if e = g.Close(); e != nil {
		t.Fatal(e)
	}
	// Each crash happens later in the same retained directory; history writes
	// are idempotently present after prior crashes, while current rows stay old
	// until the real split-current barrier. No failed application receipt is
	// needed to recover the already published old transaction pin.
	for _, mode := range []string{"anchors", "deltas", "source"} {
		t.Run(mode, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestHistoryWriteAheadCrashChild$")
			cmd.Env = append(os.Environ(), "RHO_HISTORY_CRASH_DIR="+dir, "RHO_HISTORY_CRASH_MODE="+mode)
			out, e := cmd.CombinedOutput()
			exit, ok := errors.AsType[*exec.ExitError](e)
			if !ok || exit.ExitCode() != 17 {
				t.Fatalf("expected crash17: %v %s", e, out)
			}
			bs, e := badger.New(historyCrashConfig(dir))
			if e != nil {
				t.Fatal(e)
			}
			g, e := rho.New(rho.Config{Store: bs, AllowTxBackfill: true})
			if e != nil {
				t.Fatal(e)
			}
			defer g.Close()
			ns, e := g.Nodes().ByLabelAndProperty("N", "value", "published", rho.QueryOpts{TxPin: 10})
			if e != nil || len(ns) != 6000 {
				t.Fatal("published node pin lost", len(ns), e)
			}
			rs, e := g.Rels().ByTypeAndProperty("R", "value", "published", rho.QueryOpts{TxPin: 10})
			if e != nil || len(rs) != 6000 {
				t.Fatal("published relationship pin lost", len(rs), e)
			}
			for _, n := range ns {
				v, _ := n.GetProperty("value")
				if v != "published" || n.Temporal().TxFrom != 10 {
					t.Fatal(n.ID(), v, n.Temporal())
				}
			}
			for _, r := range rs {
				v, _ := r.GetProperty("value")
				if v != "published" || r.Temporal().TxFrom != 10 {
					t.Fatal(r.ID(), v, r.Temporal())
				}
			}
			current, e := bs.AllNodes(badger.QueryOpts{})
			if e != nil {
				t.Fatal(e)
			}
			changed := 0
			for _, n := range current {
				if n.Temporal().TxFrom == 50 {
					changed++
				}
			}
			if mode == "source" && (changed == 0 || changed >= 6000) {
				t.Fatal("did not cross real current WriteBatch split", changed)
			}
			currentR, e := bs.AllRelationships(badger.QueryOpts{})
			if e != nil {
				t.Fatal(e)
			}
			changedR := 0
			for _, r := range currentR {
				if r.Temporal().TxFrom == 50 {
					changedR++
				}
			}
			if mode == "source" && (changedR == 0 || changedR >= 6000) {
				t.Fatal("did not cross actual relationship source split", changedR)
			}
			t.Logf("crash=%s changed current nodes=%d rels=%d, exact old pin nodes=6000 rels=6000", mode, changed, changedR)
		})
	}
}
