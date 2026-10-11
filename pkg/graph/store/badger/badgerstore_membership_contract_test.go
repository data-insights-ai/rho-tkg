package badger

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

func TestOrderedHistoricalCandidatesDirectContract(t *testing.T) {
	bs, err := New(Config{Dir: t.TempDir(), HistoricalMembershipOnDisk: true, SyncWrites: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	if err = bs.CreatePropertyIndex(1, "v"); err != nil {
		t.Fatal(err)
	}
	if err = bs.CreateRelPropertyIndex(1, "v"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{4, 1, 3, 2} {
		n := types.NewNode(types.NodeID(id), 1, nil)
		n.SetVersion(1)
		n.SetTemporal(&types.TemporalMetadata{TxFrom: 10})
		if err = n.SetProperty("v", "old"); err != nil {
			t.Fatal(err)
		}
		if err = bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []int64{4, 1, 3, 2} {
		r := types.NewRelationship(types.RelID(id), 1, 1, 2)
		r.SetVersion(1)
		r.SetTemporal(&types.TemporalMetadata{TxFrom: 10})
		if err = r.SetProperty("v", "old"); err != nil {
			t.Fatal(err)
		}
		if err = bs.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}
	nodeFn := func(types.NodeID, types.Instant) bool { return true }
	relFn := func(types.RelID, types.Instant) bool { return true }
	if err = bs.ForEachNodeHistoricalCandidate(1, "", "", 0, nodeFn); !errors.Is(err, ErrIndexNotFound) {
		t.Fatal(err)
	}
	if err = bs.ForEachRelHistoricalCandidate(1, "", "", 0, relFn); !errors.Is(err, ErrIndexNotFound) {
		t.Fatal(err)
	}
	for {
		p, e := bs.BuildHistoricalMembership(t.Context(), HistoricalMembershipLimits{MaxRows: 2, MaxBytes: 1 << 20})
		if e != nil {
			t.Fatal(e)
		}
		if p.Complete {
			break
		}
	}
	// A later history-only acquisition must not replace the earliest stamp.
	n := types.NewNode(3, 1, nil)
	n.SetVersion(2)
	n.SetTemporal(&types.TemporalMetadata{TxFrom: 20})
	if err = n.SetProperty("v", "old"); err != nil {
		t.Fatal(err)
	}
	if err = bs.PutNodeVersion(n.ID(), n.Version(), n); err != nil {
		t.Fatal(err)
	}
	r := types.NewRelationship(3, 1, 1, 2)
	r.SetVersion(2)
	r.SetTemporal(&types.TemporalMetadata{TxFrom: 20})
	if err = r.SetProperty("v", "old"); err != nil {
		t.Fatal(err)
	}
	if err = bs.PutRelVersion(r.ID(), r.Version(), r); err != nil {
		t.Fatal(err)
	}
	for _, prop := range []string{"", "v"} {
		value := ""
		if prop != "" {
			value = types.IndexablePropertyValueKey("old")
		}
		var nodes []types.NodeID
		var rels []types.RelID
		err = bs.ForEachNodeHistoricalCandidate(1, prop, value, 1, func(id types.NodeID, first types.Instant) bool {
			if first != 10 {
				t.Fatal(first)
			}
			nodes = append(nodes, id)
			return len(nodes) < 2
		})
		if err != nil {
			t.Fatal(err)
		}
		err = bs.ForEachRelHistoricalCandidate(1, prop, value, 1, func(id types.RelID, first types.Instant) bool {
			if first != 10 {
				t.Fatal(first)
			}
			rels = append(rels, id)
			return len(rels) < 2
		})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(nodes, []types.NodeID{2, 3}) || !reflect.DeepEqual(rels, []types.RelID{2, 3}) {
			t.Fatal(nodes, rels)
		}
		nodes = nil
		rels = nil
		if err = bs.ForEachNodeHistoricalCandidate(1, prop, value, 3, func(id types.NodeID, _ types.Instant) bool { nodes = append(nodes, id); return true }); err != nil {
			t.Fatal(err)
		}
		if err = bs.ForEachRelHistoricalCandidate(1, prop, value, 3, func(id types.RelID, _ types.Instant) bool { rels = append(rels, id); return true }); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(nodes, []types.NodeID{4}) || !reflect.DeepEqual(rels, []types.RelID{4}) {
			t.Fatal(nodes, rels)
		}
	}
	for _, test := range []struct {
		name      string
		node, rel func() error
		want      error
	}{
		{"nil", func() error { return bs.ForEachNodeHistoricalCandidate(1, "", "", 0, nil) }, func() error { return bs.ForEachRelHistoricalCandidate(1, "", "", 0, nil) }, ErrInvalidStoreMutation},
		{"token", func() error { return bs.ForEachNodeHistoricalCandidate(0, "", "", 0, nodeFn) }, func() error { return bs.ForEachRelHistoricalCandidate(0, "", "", 0, relFn) }, ErrInvalidStoreMutation},
		{"cursor", func() error { return bs.ForEachNodeHistoricalCandidate(1, "", "", -1, nodeFn) }, func() error { return bs.ForEachRelHistoricalCandidate(1, "", "", -1, relFn) }, storecontract.ErrInvalidQueryCursor},
		{"missing index", func() error { return bs.ForEachNodeHistoricalCandidate(1, "missing", "", 0, nodeFn) }, func() error { return bs.ForEachRelHistoricalCandidate(1, "missing", "", 0, relFn) }, ErrIndexNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			if e := test.node(); !errors.Is(e, test.want) {
				t.Fatal(e)
			}
			if e := test.rel(); !errors.Is(e, test.want) {
				t.Fatal(e)
			}
		})
	}
	if err = bs.Close(); err != nil {
		t.Fatal(err)
	}
	if err = bs.ForEachNodeHistoricalCandidate(1, "", "", 0, nodeFn); !errors.Is(err, ErrStoreClosed) {
		t.Fatal(err)
	}
	if err = bs.ForEachRelHistoricalCandidate(1, "", "", 0, relFn); !errors.Is(err, ErrStoreClosed) {
		t.Fatal(err)
	}
}

func TestHistoricalMembershipBootstrapValidationAndFailure(t *testing.T) {
	if _, err := New(Config{Dir: t.TempDir(), HistoricalMembershipOnDisk: true}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatal("async writer admitted", err)
	}
	cfg := Config{Dir: t.TempDir(), HistoricalMembershipOnDisk: true, SyncWrites: true}
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	valid := HistoricalMembershipLimits{MaxRows: 10, MaxBytes: 1 << 20}
	for _, l := range []HistoricalMembershipLimits{{0, 1}, {10001, 1}, {1, 0}, {1, 65 << 20}} {
		if _, e := bs.BuildHistoricalMembership(t.Context(), l); !errors.Is(e, ErrInvalidStoreMutation) {
			t.Fatal(e)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, e := bs.BuildHistoricalMembership(ctx, valid); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	bs.BeginGroupCommit()
	if _, e := bs.BuildHistoricalMembership(t.Context(), valid); !errors.Is(e, ErrInvalidStoreMutation) {
		t.Fatal(e)
	}
	if e := bs.EndGroupCommit(); e != nil {
		t.Fatal(e)
	}
	n := types.NewNode(1, 1, nil)
	if err = bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if _, e := bs.BuildHistoricalMembership(t.Context(), HistoricalMembershipLimits{MaxRows: 1, MaxBytes: 1}); !errors.Is(e, ErrInvalidStoreMutation) {
		t.Fatal(e)
	}
	p, e := bs.HistoricalMembershipStatus()
	if e != nil || p.Rows != 0 || p.Complete {
		t.Fatal(p, e)
	}
	injected := errors.New("posting flush failure")
	bs.failNextFlush.Store(&injected)
	if _, e = bs.BuildHistoricalMembership(t.Context(), valid); !errors.Is(e, injected) {
		t.Fatal(e)
	}
	p, e = bs.HistoricalMembershipStatus()
	if e != nil || p.Rows != 0 || p.Complete {
		t.Fatal(p, e)
	}
	for {
		p, e = bs.BuildHistoricalMembership(t.Context(), valid)
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
	cfg.ReadOnly = true
	cfg.SyncWrites = false
	bs, e = New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer bs.Close()
	if p, e := bs.HistoricalMembershipStatus(); e != nil || !p.Complete {
		t.Fatal(p, e)
	}
	if _, e = bs.BuildHistoricalMembership(t.Context(), valid); !errors.Is(e, ErrInvalidStoreMutation) {
		t.Fatal(e)
	}
}

func TestHistoricalMembershipRejectsForgedProgress(t *testing.T) {
	bs, e := New(Config{Dir: t.TempDir(), HistoricalMembershipOnDisk: true, SyncWrites: true})
	if e != nil {
		t.Fatal(e)
	}
	defer bs.Close()
	for _, p := range []HistoricalMembershipProgress{
		{Version: 2}, {Version: 1, Phase: 4}, {Version: 1, Phase: 4, Complete: true, After: storepkg.NodeKey(1)},
		{Version: 1, Phase: 0, After: storepkg.RelKey(1)}, {Version: 1, Phase: 1, After: storepkg.NodeKey(1)},
		{Version: 1, After: storepkg.NodeKey(0)}, {Version: 1, Rows: -1},
	} {
		raw, e := json.Marshal(p)
		if e != nil {
			t.Fatal(e)
		}
		if e = bs.MetaSet(historicalMembershipProgressKey, raw); e != nil {
			t.Fatal(e)
		}
		if _, e = bs.historicalMembershipProgress(); !errors.Is(e, ErrInvalidStoreMutation) {
			t.Fatal(p, e)
		}
	}
}

func TestHistoricalMembershipConcurrentBootstrapAndExactErase(t *testing.T) {
	cfg := Config{Dir: t.TempDir(), HistoricalMembershipOnDisk: true, SyncWrites: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry()}
	bs, e := New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer bs.Close()
	if e = bs.CreatePropertyIndex(1, "v"); e != nil {
		t.Fatal(e)
	}
	if e = bs.CreateRelPropertyIndex(1, "v"); e != nil {
		t.Fatal(e)
	}
	for i := range 20 {
		n := types.NewNode(types.NodeID(i+1), 1, nil)
		n.SetVersion(1)
		if e = n.SetProperty("v", "old"); e != nil {
			t.Fatal(e)
		}
		if e = bs.PutNode(n); e != nil {
			t.Fatal(e)
		}
	}
	r := types.NewRelationship(101, 1, 1, 2)
	r.SetVersion(1)
	if e = r.SetProperty("v", "old"); e != nil {
		t.Fatal(e)
	}
	if e = bs.PutRelationship(r); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() {
		for {
			p, e := bs.BuildHistoricalMembership(t.Context(), HistoricalMembershipLimits{MaxRows: 1, MaxBytes: 1 << 20})
			if e != nil || p.Complete {
				done <- e
				return
			}
		}
	}()
	for i := range 20 {
		n := types.NewNode(types.NodeID(i+21), 1, nil)
		n.SetVersion(1)
		if e = n.SetProperty("v", "new"); e != nil {
			t.Fatal(e)
		}
		if e = bs.PutNode(n); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = bs.ExactErase(storecontract.ExactErasureRequest{NodeIDs: []types.NodeID{1}, RelIDs: []types.RelID{101}, Bounds: badgerExactErasureBounds}); e != nil {
		t.Fatal(e)
	}
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	assert := func() {
		t.Helper()
		var ids []types.NodeID
		if e = bs.ForEachNodeHistoricalCandidate(1, "", "", 0, func(id types.NodeID, _ types.Instant) bool { ids = append(ids, id); return true }); e != nil {
			t.Fatal(e)
		}
		want := make([]types.NodeID, 39)
		for i := range want {
			want[i] = types.NodeID(i + 2)
		}
		if !reflect.DeepEqual(ids, want) {
			t.Fatal(ids, want)
		}
		if ops, e := bs.historicalMembershipEraseOps(true, 1); e != nil || len(ops) != 0 {
			t.Fatal("erased node retained reverse data", len(ops), e)
		}
		if ops, e := bs.historicalMembershipEraseOps(false, 101); e != nil || len(ops) != 0 {
			t.Fatal("erased rel retained reverse data", len(ops), e)
		}
		var rels []types.RelID
		if e = bs.ForEachRelHistoricalCandidate(1, "", "", 0, func(id types.RelID, _ types.Instant) bool { rels = append(rels, id); return true }); e != nil || len(rels) != 0 {
			t.Fatal(rels, e)
		}
	}
	assert()
	if e = bs.Close(); e != nil {
		t.Fatal(e)
	}
	cfg.PropertyKeyRegistry = nil
	bs, e = New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer bs.Close()
	assert()
}

func TestHistoricalMembershipStatusUnsupportedAndClosed(t *testing.T) {
	bs, e := New(Config{Dir: t.TempDir()})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = bs.HistoricalMembershipStatus(); !errors.Is(e, storecontract.ErrCapabilityNotSupported) {
		t.Fatal(e)
	}
	if e = bs.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e = bs.HistoricalMembershipStatus(); !errors.Is(e, ErrStoreClosed) {
		t.Fatal(e)
	}
}

func TestHistoricalMembershipOrderedPendingOverlayAndCorruption(t *testing.T) {
	bs, e := New(Config{Dir: t.TempDir(), HistoricalMembershipOnDisk: true, SyncWrites: true})
	if e != nil {
		t.Fatal(e)
	}
	defer bs.Close()
	for _, id := range []types.NodeID{1, 5} {
		n := types.NewNode(id, 1, nil)
		n.SetVersion(1)
		n.SetTemporal(&types.TemporalMetadata{TxFrom: 20})
		if e = bs.PutNode(n); e != nil {
			t.Fatal(e)
		}
	}
	for {
		p, e := bs.BuildHistoricalMembership(t.Context(), HistoricalMembershipLimits{MaxRows: 100, MaxBytes: 1 << 20})
		if e != nil {
			t.Fatal(e)
		}
		if p.Complete {
			break
		}
	}
	bs.BeginGroupCommit()
	for _, id := range []types.NodeID{2, 3, math.MaxInt64} {
		n := types.NewNode(id, 1, nil)
		n.SetTemporal(&types.TemporalMetadata{TxFrom: 20})
		if e = bs.PutNode(n); e != nil {
			t.Fatal(e)
		}
	}
	h := types.NewNode(1, 1, nil)
	h.SetVersion(2)
	h.SetTemporal(&types.TemporalMetadata{TxFrom: 10})
	if e = bs.PutNodeVersion(1, 2, h); e != nil {
		t.Fatal(e)
	}
	var ids []types.NodeID
	if e = bs.ForEachNodeHistoricalCandidate(1, "", "", 0, func(id types.NodeID, first types.Instant) bool {
		ids = append(ids, id)
		if id == 1 && first != 10 {
			t.Fatal("pending older acquisition missed", first)
		}
		return true
	}); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(ids, []types.NodeID{1, 2, 3, 5, math.MaxInt64}) {
		t.Fatal(ids)
	}
	if e = bs.EndGroupCommit(); e != nil {
		t.Fatal(e)
	}
	scope := diskMembershipScope(diskMemberNodeLabel, 1, "", "")
	key := diskMembershipOp(scope, 1, 10).key
	for _, bad := range []struct{ key, value []byte }{{append(bytes.Clone(scope), make([]byte, 17)...), []byte{1}}, {key, []byte{2}}, {diskMembershipOp(scope, 0, 10).key, []byte{1}}} {
		if e = bs.db.Update(func(txn *badgerv4.Txn) error { return txn.Set(bad.key, bad.value) }); e != nil {
			t.Fatal(e)
		}
		if e = bs.ForEachNodeHistoricalCandidate(1, "", "", 0, func(types.NodeID, types.Instant) bool { return true }); e == nil {
			t.Fatal("corrupt key was trusted")
		}
		if e = bs.db.Update(func(txn *badgerv4.Txn) error { return txn.Delete(bad.key) }); e != nil {
			t.Fatal(e)
		}
	}
}

func TestHistoricalMembershipCorruptReverseRefusesErasureBeforeMutation(t *testing.T) {
	for _, mode := range []string{"length", "digest", "ownership"} {
		t.Run(mode, func(t *testing.T) {
			bs, e := New(Config{Dir: t.TempDir(), HistoricalMembershipOnDisk: true, SyncWrites: true})
			if e != nil {
				t.Fatal(e)
			}
			defer bs.Close()
			n := types.NewNode(1, 1, nil)
			if e = bs.PutNode(n); e != nil {
				t.Fatal(e)
			}
			pair := diskMembershipPair(diskMembershipScope(diskMemberNodeLabel, 1, "", ""), 1, 0)
			key, value := bytes.Clone(pair[1].key), bytes.Clone(pair[1].value)
			switch mode {
			case "length":
				key = append(key, 0)
			case "digest":
				value[len(value)-1] = 1
			case "ownership":
				value = diskMembershipOp(diskMembershipScope(diskMemberNodeLabel, 1, "", ""), 2, 0).key
				h := sha256.Sum256(value)
				copy(key[10:], h[:])
			}
			if e = bs.db.Update(func(txn *badgerv4.Txn) error { return txn.Set(key, value) }); e != nil {
				t.Fatal(e)
			}
			if _, e = bs.ExactErase(storecontract.ExactErasureRequest{NodeIDs: []types.NodeID{1}, Bounds: badgerExactErasureBounds}); e == nil {
				t.Fatal("corrupt cleanup proof accepted")
			}
			if got, e := bs.GetNode(1); e != nil || got.ID() != 1 {
				t.Fatal("failed proof destroyed source", e)
			}
		})
	}
}

func TestHistoricalMembershipStatusAndOpenRejectTrailingProgress(t *testing.T) {
	for _, tail := range []string{" {}", " trailing garbage"} {
		t.Run(tail, func(t *testing.T) {
			cfg := Config{Dir: t.TempDir(), HistoricalMembershipOnDisk: true, SyncWrites: true}
			bs, e := New(cfg)
			if e != nil {
				t.Fatal(e)
			}
			p, e := bs.BuildHistoricalMembership(t.Context(), HistoricalMembershipLimits{MaxRows: 10, MaxBytes: 1 << 20})
			if e != nil || !p.Complete {
				t.Fatal(p, e)
			}
			raw, e := json.Marshal(p)
			if e != nil {
				t.Fatal(e)
			}
			raw = append(raw, tail...)
			if e = bs.MetaSet(historicalMembershipProgressKey, raw); e != nil {
				t.Fatal(e)
			}
			if _, e = bs.HistoricalMembershipStatus(); !errors.Is(e, ErrInvalidStoreMutation) {
				t.Fatal("trailing progress accepted by status", e)
			}
			if e = bs.Close(); e != nil {
				t.Fatal(e)
			}
			reopened, e := New(cfg)
			if reopened != nil {
				reopened.Close()
			}
			if !errors.Is(e, ErrInvalidStoreMutation) {
				t.Fatal("trailing Complete accepted by open", e)
			}
		})
	}
}
