package badger

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

func TestReadBudgetOpenRefusesRecovery(t *testing.T) {
	for _, pending := range []string{"WAL", "uncommitted staging", "committed staging"} {
		t.Run(pending, func(t *testing.T) {
			cfg := Config{Dir: t.TempDir()}
			bs, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := bs.PutNode(types.NewNode(1, 1, nil)); err != nil {
				t.Fatal(err)
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			wal := filepath.Join(cfg.Dir, "999999.mem")
			stage := append(stagePrefix(1), storeutil.NodeKey(1)...)
			if pending == "WAL" {
				// The engine cannot parse this sentinel. A typed refusal, with
				// its contents unchanged, proves replay was never attempted.
				if err := os.WriteFile(wal, []byte("unopened WAL"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				db, err := badgerv4.Open(badgerv4.DefaultOptions(cfg.Dir).WithLogger(nil))
				if err != nil {
					t.Fatal(err)
				}
				if err := db.Update(func(txn *badgerv4.Txn) error {
					item, err := txn.Get(storeutil.NodeKey(1))
					if err != nil {
						return err
					}
					row, err := item.ValueCopy(nil)
					if err != nil {
						return err
					}
					if err := txn.Set(stage, append([]byte{byte(writeOpSet)}, row...)); err != nil {
						return err
					}
					if pending == "committed staging" {
						marker := make([]byte, stageMarkerLen)
						binary.BigEndian.PutUint64(marker, 1)
						binary.BigEndian.PutUint64(marker[8:], 1)
						return txn.Set(storeutil.FlushStageMarkerKey, marker)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			for _, readOnly := range []bool{true, false} {
				_, meter := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
				cfg.ReadOnly, cfg.OpenReadTally = readOnly, meter.Tally("native recovery")
				decoded := CountRowDecodesForTest(func() { bs, err = New(cfg) })
				if bs != nil {
					_ = bs.Close()
				}
				var rb *storecontract.ReadBudgetError
				if !errors.As(err, &rb) || rb.Limit != "unsupported" || decoded != 0 {
					t.Fatalf("pending %s, readOnly=%v: %v / %d decodes", pending, readOnly, err, decoded)
				}
			}
			if pending == "WAL" {
				got, err := os.ReadFile(wal)
				if err != nil || !bytes.Equal(got, []byte("unopened WAL")) {
					t.Fatalf("WAL changed before refusal: %q, %v", got, err)
				}
				if err := os.Remove(wal); err != nil {
					t.Fatal(err)
				}
			} else {
				db, err := badgerv4.Open(badgerv4.DefaultOptions(cfg.Dir).WithLogger(nil))
				if err != nil {
					t.Fatal(err)
				}
				if err := db.View(func(txn *badgerv4.Txn) error { _, err := txn.Get(stage); return err }); err != nil {
					t.Fatalf("staging was repaired before refusal: %v", err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			cfg.ReadOnly, cfg.OpenReadTally = false, nil
			bs, err = New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			_, meter := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
			cfg.ReadOnly, cfg.OpenReadTally = true, meter.Tally("native recovery")
			bs, err = New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer bs.Close()
			if n, err := bs.GetNode(1); err != nil || n == nil || n.ID() != 1 {
				t.Fatalf("healthy retry = %v, %v", n, err)
			}
		})
	}
}

// Serial: the native open-walk and row-decode hooks are process-wide.
func TestReadBudgetOpenRefusesImageRebuild(t *testing.T) {
	for _, damage := range []string{"absent", "dirty", "corrupt", "header", "node IDs", "rel IDs", "property", "rel property", "composite", "temporal", "rel temporal", "high frequency", "vector", "disk configuration", "property disk configuration"} {
		t.Run(damage, func(t *testing.T) {
			cfg := Config{Dir: t.TempDir(), LabelIndexOnDisk: true, AdjacencyIndexOnDisk: true, DisablePlannerStats: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry()}
			bs, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			for id := int64(1); id <= 3; id++ {
				n := types.NewNode(types.NodeID(id), 1, []uint16{2})
				_ = n.SetProperty("x", id)
				_ = n.SetProperty("y", id)
				_ = n.SetProperty("vec", []float32{1, 2})
				if err := bs.PutNode(n); err != nil {
					t.Fatal(err)
				}
			}
			rel := types.NewRelationship(11, 2, 1, 2)
			_ = rel.SetProperty("x", int64(1))
			if err := bs.PutRelationship(rel); err != nil {
				t.Fatal(err)
			}
			for _, create := range []func() error{
				func() error { return bs.CreatePropertyIndex(1, "x") },
				func() error { return bs.CreateRelPropertyIndex(2, "x") },
				func() error { return bs.CreateCompositePropertyIndex(1, []string{"x", "y"}) },
				func() error { return bs.CreateTemporalIndex(1) },
				func() error { return bs.CreateRelTemporalIndex(2) },
				func() error { return bs.CreateHighFrequencyIndex(2, time.Hour) },
				func() error { return bs.CreateVectorIndex(1, "vec", 2, DistanceEuclidean) },
			} {
				if err := create(); err != nil {
					t.Fatal(err)
				}
			}
			section := map[string]byte{"node IDs": imgSecNodeIDs, "rel IDs": imgSecRelIDs, "property": imgSecNodeProp, "rel property": imgSecRelProp, "composite": imgSecComposite, "temporal": imgSecTemporal, "rel temporal": imgSecRelTemporal, "high frequency": imgSecHF, "vector": imgSecVector}[damage]
			if section != 0 {
				bs.imageSectionFilterForTest = func(kind byte) bool { return kind != section }
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			if damage == "absent" || damage == "dirty" || damage == "corrupt" || damage == "header" {
				db, err := badgerv4.Open(badgerv4.DefaultOptions(cfg.Dir).WithLogger(nil))
				if err != nil {
					t.Fatal(err)
				}
				err = db.Update(func(txn *badgerv4.Txn) error {
					switch damage {
					case "absent":
						return DeleteOpenStateForTest(txn)
					case "corrupt":
						return CorruptOpenStateForTest(txn)
					case "header":
						return txn.Set(openImageHeaderKey, []byte("future image format"))
					default:
						// Rewriting the same row advances its version: delta opens
						// must inspect it even though its answer stays unchanged.
						key := storeutil.NodeKey(1)
						item, err := txn.Get(key)
						if err != nil {
							return err
						}
						val, err := item.ValueCopy(nil)
						if err != nil {
							return err
						}
						return txn.Set(key, val)
					}
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if damage == "disk configuration" {
				cfg.TemporalIndexOnDisk = true
			}
			if damage == "property disk configuration" {
				cfg.PropertyIndexOnDisk = true
			}
			for _, readOnly := range []bool{true, false} {
				if readOnly && damage == "property disk configuration" {
					continue // read-only safely restores its existing RAM image
				}
				ctx, outer := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{DecodedBytes: 1})
				_, meter := storecontract.WithReadBudget(ctx, storecontract.ReadBudget{})
				cfg.ReadOnly, cfg.OpenReadTally = readOnly, meter.Tally("native open")
				var walks int
				openWalkTestHook = func(byte) { walks++ }
				decoded := CountRowDecodesForTest(func() { bs, err = New(cfg) })
				openWalkTestHook = nil
				if bs != nil {
					_ = bs.Close()
				}
				var rb *storecontract.ReadBudgetError
				if !errors.As(err, &rb) || rb.Limit != "unsupported" || walks != 0 || decoded != 0 || meter.Exceeded() == nil || outer.Exceeded() == nil {
					t.Fatalf("readOnly=%v: open = %v, %d native walks/%d decodes; want shared refusal before rebuilding", readOnly, err, walks, decoded)
				}
			}
			// Normal maintenance heals the same files. A budgeted clean reopen
			// must keep the exact non-empty answer and do no native rebuild.
			cfg.ReadOnly, cfg.OpenReadTally = false, nil
			bs, err = New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			for _, readOnly := range []bool{true, false} {
				_, meter := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
				cfg.ReadOnly, cfg.OpenReadTally = readOnly, meter.Tally("native open")
				decoded := CountRowDecodesForTest(func() { bs, err = New(cfg) })
				if err != nil {
					t.Fatal(err)
				}
				var ids []types.NodeID
				bs.nodeIDs.Ascend(func(id int64) bool { ids = append(ids, types.NodeID(id)); return true })
				if !slices.Equal(ids, []types.NodeID{1, 2, 3}) || bs.relIDs.Len() != 1 || !bs.relIDs.Has(11) || decoded != 0 {
					t.Errorf("healthy open = %v / %d rels, %d native decodes", ids, bs.relIDs.Len(), decoded)
				}
				if err := bs.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
