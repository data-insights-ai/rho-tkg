package tiered

import (
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerstore "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestReadBudget_ArchiveRepairRefusesBeforeScan(t *testing.T) {
	testtier.Slow(t)
	// Serial: the row decoder counter is process-wide.
	for _, kind := range []string{"property", "composite", "temporal", "high frequency"} {
		t.Run(kind, func(t *testing.T) {
			cfg := volumeCfg(t.TempDir(), "", "")
			ts, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			reg := registrypkg.NewLabelRegistry()
			ts.SetLabelRegistry(reg)
			tok, _ := reg.GetOrCreate("Case")
			node := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)
			if err := ts.PutNode(node); err != nil {
				t.Fatal(err)
			}
			if err := ts.ArchiveNode(node.ID()); err != nil {
				t.Fatal(err)
			}
			if err := ts.Close(); err != nil {
				t.Fatal(err)
			}
			ts, err = New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = ts.Close() })
			switch kind {
			case "property":
				err = ts.refShard.CreatePropertyIndex(tok, "late")
			case "composite":
				err = ts.refShard.CreateCompositePropertyIndex(tok, []string{"late", "other"})
			case "temporal":
				ts.tempIdxMu.Lock()
				ts.tempIdxLabels = append(ts.tempIdxLabels, tok)
				ts.tempIdxMu.Unlock()
			case "high frequency":
				ts.tempIdxMu.Lock()
				ts.hfIdxBuckets[tok] = time.Hour
				ts.tempIdxMu.Unlock()
			}
			if err != nil {
				t.Fatal(err)
			}
			var native, writable atomic.Int64
			ts.SetShardOpenHookForTest(func(cfg BadgerStoreConfig) {
				if filepath.Base(cfg.Dir) == "archive" {
					native.Add(1)
					if !cfg.ReadOnly {
						writable.Add(1)
					}
				}
			})
			read := func(meter *storecontract.ReadMeter) (*types.Node, error) {
				snap := ts.NewReadSnapshot()
				defer snap.Close()
				snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(meter.Tally("archive"))
				return snap.GetNode(node.ID())
			}
			ctx, outer := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{DecodedBytes: 1})
			_, inner := storecontract.WithReadBudget(ctx, storecontract.ReadBudget{})
			var got *types.Node
			decoded := badgerstore.CountRowDecodesForTest(func() { got, err = read(inner) })
			var rb *storecontract.ReadBudgetError
			if got != nil || !errors.As(err, &rb) || rb.Limit != "unsupported" || decoded != 0 || writable.Load() != 0 || ts.refArchive.Load() != nil || outer.Exceeded() == nil || inner.Exceeded() == nil {
				t.Fatalf("pending %s repair = %v, %v, %d native decodes/%d writable opens; want shared refusal before writable maintenance", kind, got, err, decoded, writable.Load())
			}
			if inner.Work().ShardOpens != uint64(native.Load()) {
				t.Fatalf("repair refusal: %d native opens, work %+v", native.Load(), inner.Work())
			}
			if got, err := ts.GetNode(node.ID()); err != nil || got == nil || got.ID() != node.ID() {
				t.Fatalf("plain repair = %v, %v", got, err)
			}
			closeArchive := func() {
				ts.archiveMu.Lock()
				defer ts.archiveMu.Unlock()
				if err := ts.refArchive.Load().Close(); err != nil {
					t.Fatal(err)
				}
				ts.refArchive.Store(nil)
			}
			for _, budget := range []storecontract.ReadBudget{{}, {ShardOpens: 2}} {
				closeArchive()
				before := native.Load()
				_, meter := storecontract.WithReadBudget(t.Context(), budget)
				got, err := read(meter)
				if err != nil || got == nil || got.ID() != node.ID() || native.Load()-before != 2 || meter.Work().ShardOpens != 2 {
					t.Fatalf("healthy retry at %+v = %v, %v, %d native opens, work %+v; want exact node with both opens charged", budget, got, err, native.Load()-before, meter.Work())
				}
			}
			closeArchive()
			native.Store(0)
			writable.Store(0)
			_, one := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{ShardOpens: 1})
			got, err = read(one)
			if got != nil || !errors.As(err, &rb) || rb.Limit != "shard opens" || native.Load() != 1 || writable.Load() != 0 || ts.refArchive.Load() != nil {
				t.Fatalf("one-open allowance = %v, %v, %d native/%d writable opens; want refusal before writable open", got, err, native.Load(), writable.Load())
			}
		})
	}
}
