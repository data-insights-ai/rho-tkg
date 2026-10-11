package badger

import (
	"errors"
	"strings"
	"testing"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	store "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

func TestEndpointPresencePersistsFullAndDeltaAcrossReopen(t *testing.T) {
	for _, delta := range []bool{false, true} {
		t.Run(map[bool]string{false: "full", true: "delta"}[delta], func(t *testing.T) {
			dir := t.TempDir()
			bs, err := New(Config{Dir: dir, SyncWrites: true, HistoryDeltaEncoding: delta})
			if err != nil {
				t.Fatal(err)
			}
			if err := bs.PutNode(types.NewNode(1003, 1, nil)); err != nil {
				t.Fatal(err)
			}
			ranges := []types.TemporalRange{{From: types.TemporalEndpoint{Value: -100, Finite: true}, To: types.TemporalEndpoint{Finite: true}}, {From: types.TemporalEndpoint{Finite: true}, To: types.TemporalEndpoint{Value: 100, Finite: true}}, {}, {To: types.TemporalEndpoint{Finite: true}}}
			for i, rng := range ranges {
				tm := &types.TemporalMetadata{TxFrom: types.Instant(100 + i)}
				if err := tm.SetValidBounds(rng); err != nil {
					t.Fatal(err)
				}
				n := types.NewNode(1001, 1, nil)
				if err := n.SetProperty("unchanged_fixture", strings.Repeat("identifier;", 512)); err != nil {
					t.Fatal(err)
				}
				n.SetVersion(uint32(i))
				n.SetTemporal(tm)
				r := types.NewRelationship(1002, 1, 1001, 1003)
				if err := r.SetProperty("unchanged_fixture", strings.Repeat("identifier;", 512)); err != nil {
					t.Fatal(err)
				}
				r.SetVersion(uint32(i))
				r.SetTemporal(tm)
				if err := bs.PutNodeVersion(n.ID(), n.Version(), n); err != nil {
					t.Fatal(err)
				}
				if err := bs.PutRelVersion(r.ID(), r.Version(), r); err != nil {
					t.Fatal(err)
				}
				if i == len(ranges)-1 {
					if err := bs.PutNode(n); err != nil {
						t.Fatal(err)
					}
					if err := bs.PutRelationship(r); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			bs, err = New(Config{Dir: dir, SyncWrites: true, HistoryDeltaEncoding: delta})
			if err != nil {
				t.Fatal(err)
			}
			defer bs.Close()
			for i, want := range ranges {
				n, err := bs.GetNodeVersion(1001, uint32(i))
				if err != nil {
					t.Fatal(err)
				}
				got, explicit := n.Temporal().ValidBounds()
				if !explicit || got != want {
					t.Fatal("node history changed", i, got, want)
				}
				r, err := bs.GetRelVersion(1002, uint32(i))
				if err != nil {
					t.Fatal(err)
				}
				got, explicit = r.Temporal().ValidBounds()
				if !explicit || got != want {
					t.Fatal("relationship history changed", i, got, want)
				}
			}
			n, err := bs.GetNode(1001)
			if err != nil {
				t.Fatal(err)
			}
			got, explicit := n.Temporal().ValidBounds()
			if !explicit || got != ranges[len(ranges)-1] {
				t.Fatal("current node", got)
			}
			r, err := bs.GetRelationship(1002)
			if err != nil {
				t.Fatal(err)
			}
			got, explicit = r.Temporal().ValidBounds()
			if !explicit || got != ranges[len(ranges)-1] {
				t.Fatal("current rel", got)
			}
			if delta {
				raw, err := bs.readHistoryNodeRaw(1001, 1)
				if err != nil || storeutil.HistoryValueKindOf(raw) != storeutil.HistoryDelta {
					t.Fatal("node delta not exercised", err)
				}
				raw, err = bs.readHistoryRelRaw(1002, 1)
				if err != nil || storeutil.HistoryValueKindOf(raw) != storeutil.HistoryDelta {
					t.Fatal("rel delta not exercised", err)
				}
			}
		})
	}
}
func TestEndpointPresenceOlderDirectoryReaderFailsBeforeMutation(t *testing.T) {
	dir := t.TempDir()
	newDiskStoreWithNode(t, dir, 1001)
	before, present := readRawMarker(t, dir)
	if !present || before != 4 {
		t.Fatal("new format directory notprotected", before, present)
	}
	db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, ceiling := range []uint16{2, 3} {
		for _, readOnly := range []bool{false, true} {
			if err := verifyAndStampWireFormatVersionForReader(db, readOnly, ceiling); !errors.Is(err, store.ErrWireFormatVersionUnsupported) {
				t.Fatal("oldreader guard", ceiling, readOnly, err)
			}
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	after, present := readRawMarker(t, dir)
	if !present || after != before {
		t.Fatal("refused reader mutated marker", before, after, present)
	}
}
