package badger

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

func TestHistoricalMembershipDiskBootstrapResumeAndReadonly(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Dir: dir, HistoricalMembershipOnDisk: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), SyncWrites: true}
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.CreatePropertyIndex(1, "value"); err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		n := types.NewNode(types.NodeID(i+1), 1, nil)
		n.SetVersion(1)
		n.SetTemporal(&types.TemporalMetadata{TxFrom: 10})
		if err := n.SetProperty("value", "old"); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.CreateRelPropertyIndex(1, "value"); err != nil {
		t.Fatal(err)
	}
	r := types.NewRelationship(1, 1, 1, 2)
	r.SetVersion(1)
	r.SetTemporal(&types.TemporalMetadata{TxFrom: 10})
	if err := r.SetProperty("value", "old"); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	if err := bs.ForEachNodePropertyTxMember(1, "value", types.IndexablePropertyValueKey("old"), func(types.NodeID, types.Instant) bool { return true }); !errors.Is(err, ErrIndexNotFound) {
		t.Fatal("partial build was trusted", err)
	}
	p, err := bs.BuildHistoricalMembership(t.Context(), HistoricalMembershipLimits{MaxRows: 1, MaxBytes: 1 << 20})
	if err != nil || p.Complete || p.Rows != 1 {
		t.Fatal(p, err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.PropertyKeyRegistry = nil
	bs, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for !p.Complete {
		p, err = bs.BuildHistoricalMembership(t.Context(), HistoricalMembershipLimits{MaxRows: 1, MaxBytes: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, reader := range []bool{false, true} {
		if reader {
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			cfg.ReadOnly = true
			bs, err = New(cfg)
			if err != nil {
				t.Fatal(err)
			}
		}
		got := map[types.NodeID]types.Instant{}
		if err := bs.ForEachNodePropertyTxMember(1, "value", types.IndexablePropertyValueKey("old"), func(id types.NodeID, first types.Instant) bool { got[id] = first; return true }); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, map[types.NodeID]types.Instant{1: 10, 2: 10, 3: 10, 4: 10}) {
			t.Fatal("exact node postings lost", got)
		}
		rels := map[types.RelID]types.Instant{}
		if err := bs.ForEachRelPropertyTxMember(1, "value", types.IndexablePropertyValueKey("old"), func(id types.RelID, first types.Instant) bool { rels[id] = first; return true }); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(rels, map[types.RelID]types.Instant{1: 10}) {
			t.Fatal("exact relationship postings lost", rels)
		}
		if bs.nodePropTx != nil || bs.relPropTx != nil || bs.labelTxMembers != nil || bs.relTypeTxMembers != nil {
			t.Fatal("disk lookup rebuilt RAM membership tables")
		}
	}
	if err := verifyAndStampWireFormatVersionForReader(bs.db, true, 4); !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
		t.Fatal("old writer was not fenced", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestHistoricalMembershipClearResetsIndexAndKeepsWriterFence(t *testing.T) {
	for _, logged := range []bool{false, true} {
		t.Run(fmt.Sprint(logged), func(t *testing.T) {
			cfg := Config{Dir: t.TempDir(), HistoricalMembershipOnDisk: true, SyncWrites: true, ChangeLog: logged, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry()}
			bs, e := New(cfg)
			if e != nil {
				t.Fatal(e)
			}
			n := types.NewNode(1, 1, nil)
			if e = n.SetProperty("value", "old"); e != nil {
				t.Fatal(e)
			}
			if e = bs.PutNode(n); e != nil {
				t.Fatal(e)
			}
			for {
				p, e := bs.BuildHistoricalMembership(t.Context(), HistoricalMembershipLimits{MaxRows: 10, MaxBytes: 1 << 20})
				if e != nil {
					t.Fatal(e)
				}
				if p.Complete {
					break
				}
			}
			if e = bs.Clear(); e != nil {
				t.Fatal(e)
			}
			if bs.historicalMembershipReady.Load() {
				t.Fatal("Clear retained memory readiness")
			}
			for _, prefix := range []byte{diskMembershipPrefix, diskMembershipReversePrefix} {
				present := false
				e = bs.db.View(func(txn *badgerv4.Txn) error {
					opts := badgerv4.DefaultIteratorOptions
					opts.PrefetchValues = false
					it := txn.NewIterator(opts)
					defer it.Close()
					p := []byte{prefix}
					it.Seek(p)
					present = it.ValidForPrefix(p)
					return nil
				})
				if e != nil || present {
					t.Fatal("Clear retained auxiliary index", prefix, e)
				}
			}
			n = types.NewNode(2, 1, nil)
			if e = n.SetProperty("value", "new"); e != nil {
				t.Fatal(e)
			}
			if e = bs.PutNode(n); e != nil {
				t.Fatal(e)
			}
			if e = verifyAndStampWireFormatVersionForReader(bs.db, true, 4); !errors.Is(e, storecontract.ErrWireFormatVersionUnsupported) {
				t.Fatal("Clear removed minimum writer fence", e)
			}
			if e = bs.Close(); e != nil {
				t.Fatal(e)
			}
			cfg.PropertyKeyRegistry = nil
			bs, e = New(cfg)
			if e != nil {
				t.Fatal(e)
			}
			defer bs.Close()
			p, e := bs.historicalMembershipProgress()
			if e != nil || p.Complete || p.Rows != 0 {
				t.Fatal("Clear retained durable completion", p, e)
			}
		})
	}
}
