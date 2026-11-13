package badger

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"
	"unsafe"

	snowflake "github.com/bds421/rho-snowflake-2026"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// TestMemberWorkEmptyWindowReadsNothing: a definition on disk (span entries,
// RELDISKWIN) answers an empty or reversed window without reading a key; a
// window that meets the 6 members of the value seeks once (its late axis, no
// pin: option B), walks their 6 entries and holds the 6 it answers.
func TestMemberWorkEmptyWindowReadsNothing(t *testing.T) {
	bs, r := memberPageStore(t, 8)
	for _, end := range []types.Instant{0, -1, math.MaxInt64} {
		calls := 0
		got, err := bs.DiagnoseRelPropertyTxMembersDuring(r, "k", types.IndexablePropertyValueKey("v"), 0, end, 0, func(types.RelID, types.Instant) bool { calls++; return false })
		if err != nil {
			t.Fatal(err)
		}
		if !got.Supported || !got.Completed {
			t.Fatalf("work: %+v", got)
		}
		if end <= 0 {
			if calls != 0 || got.CallbackStopped || got.IteratorSteps != 0 || got.IteratorSeeks != 0 || got.DiskEntriesEmitted != 0 || got.FoldMembersPeak != 0 {
				t.Fatalf("empty window read: %+v calls %d", got, calls)
			}
			continue
		}
		if calls != 1 || !got.CallbackStopped || got.MembersEmitted != 1 || got.IteratorSteps != 6 || got.IteratorSeeks != 1 || got.FoldMembersPeak != 6 || got.MemberSliceLen != 6 {
			t.Fatalf("stopped: %+v", got)
		}
	}
}

func TestMemberWorkNodeAndRelCounts(t *testing.T) {
	for _, n := range []int{2, 8, 32} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			bs, err := New(Config{InMemory: true, PropertyIndexOnDisk: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry()})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = bs.Close() })
			if err = bs.CreatePropertyIndex(1, "k"); err != nil {
				t.Fatal(err)
			}
			for i := 1; i <= n; i++ {
				if err = bs.PutNode(memberWorkNode(types.NodeID(i))); err != nil {
					t.Fatal(err)
				}
			}
			if err = bs.Flush(); err != nil {
				t.Fatal(err)
			}
			got, err := bs.DiagnoseNodePropertyTxMembersDuring(1, "k", types.IndexablePropertyValueKey("v"), 0, math.MaxInt64, 0, func(types.NodeID, types.Instant) bool { return true })
			if err != nil {
				t.Fatal(err)
			}
			if !got.Supported || !got.Completed || got.IteratorSteps != uint64(n) || got.IteratorSeeks != 1 || got.FoldMembersPeak != uint64(n) || got.MemberSliceLen != uint64(n) || got.MembersEmitted != uint64(n) {
				t.Fatalf("node: %+v", got)
			}
			rel, r := memberPageStore(t, int64(n))
			got, err = rel.DiagnoseRelPropertyTxMembersDuring(r, "k", types.IndexablePropertyValueKey("v"), 0, math.MaxInt64, 0, func(types.RelID, types.Instant) bool { return true })
			if err != nil {
				t.Fatal(err)
			}
			var normal, diagnosed []types.RelID
			if err = rel.ForEachRelPropertyTxMemberDuring(r, "k", types.IndexablePropertyValueKey("v"), 0, math.MaxInt64, 0, func(id types.RelID, _ types.Instant) bool { normal = append(normal, id); return true }); err != nil {
				t.Fatal(err)
			}
			if _, err = rel.DiagnoseRelPropertyTxMembersDuring(r, "k", types.IndexablePropertyValueKey("v"), 0, math.MaxInt64, 0, func(id types.RelID, _ types.Instant) bool { diagnosed = append(diagnosed, id); return true }); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(normal, diagnosed) {
				t.Fatalf("ordinary/diagnostic IDs: %v vs %v", normal, diagnosed)
			}
			want := uint64(n - n/3)
			if got.FoldMembersPeak != want || got.MemberSliceLen != want || got.MembersEmitted != want {
				t.Fatalf("rel: %+v", got)
			}
		})
	}
}

func TestMemberWorkNestedConcurrentAndErrors(t *testing.T) {
	bs, r := memberPageStore(t, 8)
	read := func(key string, fn func(types.RelID, types.Instant) bool) (storecontract.PropertyMemberWork, error) {
		return bs.DiagnoseRelPropertyTxMembersDuring(r, "k", types.IndexablePropertyValueKey(key), 0, math.MaxInt64, 0, fn)
	}
	out, err := read("v", func(types.RelID, types.Instant) bool {
		inner, e := read("w", func(types.RelID, types.Instant) bool { return true })
		if e != nil || inner.MembersEmitted != 2 {
			t.Errorf("nested: %+v %v", inner, e)
		}
		return true
	})
	if err != nil || out.MembersEmitted != 6 {
		t.Fatalf("outer: %+v %v", out, err)
	}
	var wg sync.WaitGroup
	for _, k := range []string{"v", "w"} {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			got, e := read(k, func(types.RelID, types.Instant) bool { return true })
			want := uint64(6)
			if k == "w" {
				want = 2
			}
			if e != nil || got.MembersEmitted != want || got.FoldMembersPeak != want {
				t.Errorf("concurrent: %+v %v", got, e)
			}
		}(k)
	}
	wg.Wait()
	for _, token := range []uint16{0, r} {
		got, e := bs.DiagnoseRelPropertyTxMembersDuring(token, "missing", "v", 0, math.MaxInt64, 0, nil)
		ordinary := bs.ForEachRelPropertyTxMemberDuring(token, "missing", "v", 0, math.MaxInt64, 0, nil)
		if e == nil || ordinary == nil || e.Error() != ordinary.Error() || got.Completed || got.MembersEmitted != 0 {
			t.Fatalf("invalid: %+v %v vs %v", got, e, ordinary)
		}
	}
	bs.entityIndexWalkTestHook = func() { panic("partial diagnostic") }
	partial, broken := read("v", func(types.RelID, types.Instant) bool { return true })
	ordinaryBroken := bs.ForEachRelPropertyTxMemberDuring(r, "k", types.IndexablePropertyValueKey("v"), 0, math.MaxInt64, 0, func(types.RelID, types.Instant) bool { return true })
	bs.entityIndexWalkTestHook = nil
	if !errors.Is(broken, storecontract.ErrCorruptWire) || ordinaryBroken == nil || ordinaryBroken.Error() != broken.Error() || !partial.Supported || partial.Completed || partial.IteratorSteps != 1 || partial.MembersEmitted != 0 {
		t.Fatalf("partial: %+v %v vs %v", partial, broken, ordinaryBroken)
	}
	missing, e := bs.DiagnoseRelPropertyTxMembersDuring(r, "missing", "v", 0, math.MaxInt64, 0, func(types.RelID, types.Instant) bool { return true })
	if !errors.Is(e, ErrIndexNotFound) || missing.Completed {
		t.Fatalf("missing %+v %v", missing, e)
	}
	ram, e := New(Config{InMemory: true})
	if e != nil {
		t.Fatal(e)
	}
	defer ram.Close()
	if e = ram.CreatePropertyIndex(1, "k"); e != nil {
		t.Fatal(e)
	}
	if e = ram.PutNode(memberWorkNode(1)); e != nil {
		t.Fatal(e)
	}
	got, e := ram.DiagnoseNodePropertyTxMembersDuring(1, "k", types.IndexablePropertyValueKey("v"), 100, 101, 0, func(types.NodeID, types.Instant) bool { return false })
	if e != nil || got.Supported || !got.Completed || got.MembersEmitted != 1 || !got.CallbackStopped || got.IteratorSteps != 0 {
		t.Fatalf("RAM: %+v %v", got, e)
	}
}

func TestMemberWorkScannerVersionsBoundaryAndPartialError(t *testing.T) {
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	prefix, ok := storepkg.AppendEntityIndexValue(storepkg.AppendEntityIndexHead(nil, storepkg.EntityIndexRelMember, 1, 1), types.IndexablePropertyValueKey("v"))
	if !ok {
		t.Fatal("prefix")
	}
	key := storepkg.AppendEntityIndexEntry(slices.Clone(prefix), storepkg.EntityIndexRelMember, 1, 0, 0)
	for range 2 {
		if err = bs.db.Update(func(tx *badgerv4.Txn) error { return tx.Set(key, nil) }); err != nil {
			t.Fatal(err)
		}
	}
	longer := append(slices.Clone(prefix), 0xff)
	longer = append(longer, make([]byte, 16)...)
	if err = bs.db.Update(func(tx *badgerv4.Txn) error { return tx.Set(longer, nil) }); err != nil {
		t.Fatal(err)
	}
	var got storecontract.PropertyMemberWork
	calls := 0
	err = bs.entityIndexWalkLockedWork(storepkg.EntityIndexRelMember, [][]byte{prefix}, func([]byte) { calls++ }, &got)
	if err != nil || got.IteratorSteps != 3 || got.DiskEntriesEmitted != 1 || calls != 1 {
		t.Fatalf("versions+boundary: %+v %d %v", got, calls, err)
	}
	bs.entityIndexWalkTestHook = func() { panic("diagnostic partial error") }
	defer func() { bs.entityIndexWalkTestHook = nil }()
	var partial storecontract.PropertyMemberWork
	err = bs.entityIndexWalkLockedWork(storepkg.EntityIndexRelMember, [][]byte{prefix}, func([]byte) {}, &partial)
	plain := bs.entityIndexWalkLocked(storepkg.EntityIndexRelMember, [][]byte{prefix}, func([]byte) {})
	if err == nil || plain == nil || err.Error() != plain.Error() || partial.IteratorSteps != 1 {
		t.Fatalf("error mismatch: %v vs %v", err, plain)
	}
}

func TestMemberWorkOverlayCounts(t *testing.T) {
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	prefix, ok := storepkg.AppendEntityIndexValue(storepkg.AppendEntityIndexHead(nil, storepkg.EntityIndexRelMember, 1, 1), types.IndexablePropertyValueKey("v"))
	if !ok {
		t.Fatal("prefix")
	}
	keys := make([]string, 3)
	for i := range keys {
		keys[i] = string(storepkg.AppendEntityIndexEntry(slices.Clone(prefix), storepkg.EntityIndexRelMember, snowflake.ID(i+1), 0, 0))
	}
	bs.flushMu.Lock()
	defer bs.flushMu.Unlock()
	bs.wbMu.Lock()
	bs.flushing = map[string]writeOp{keys[0]: {opType: writeOpSet}, keys[1]: {opType: writeOpSet}}
	bs.pending = map[string]writeOp{keys[0]: {opType: writeOpDelete}, keys[2]: {opType: writeOpSet}}
	bs.flushingByPrefix = map[string][]string{string(prefix): {keys[0], keys[1]}}
	bs.pendingByPrefix = map[string][]string{string(prefix): {keys[0], keys[2]}}
	bs.wbMu.Unlock()
	defer func() {
		bs.wbMu.Lock()
		clear(bs.flushing)
		clear(bs.pending)
		clear(bs.flushingByPrefix)
		clear(bs.pendingByPrefix)
		bs.wbMu.Unlock()
	}()
	var got storecontract.PropertyMemberWork
	calls := 0
	err = bs.entityIndexWalkLockedWork(storepkg.EntityIndexRelMember, [][]byte{prefix}, func([]byte) { calls++ }, &got)
	if err != nil || got.OverlayOpsInspected != 4 || got.OverlayOpsMatched != 4 || got.OverlaySetsEmitted != 2 || got.OverlaySetsPeak != 2 || got.OverlayDeletesPeak != 1 || calls != 2 {
		t.Fatalf("overlay: %+v %d %v", got, calls, err)
	}
	bs.wbMu.Lock()
	for i := range 5 {
		bs.pending[fmt.Sprintf("unrelated-%d", i)] = writeOp{opType: writeOpSet}
	}
	bs.wbMu.Unlock()
	var ranged storecontract.PropertyMemberWork
	calls = 0
	err = bs.entityIndexRangeLockedWork(prefix, prefix, nil, func([]byte) { calls++ }, &ranged)
	if err != nil || ranged.OverlayOpsInspected != 9 || ranged.OverlayOpsMatched != 4 || ranged.OverlaySetsEmitted != 2 || calls != 2 {
		t.Fatalf("range overlay: %+v %d %v", ranged, calls, err)
	}

}

func TestMemberWorkResultSize(t *testing.T) {
	t.Logf("PropertyMemberWork sizeof=%d bytes; twelve uint64 counters and three bools, constant per invocation, no fields added to Store or retained entity/index records", unsafe.Sizeof(storecontract.PropertyMemberWork{}))
}

func memberWorkNode(id types.NodeID) *types.Node {
	n := types.NewNode(id, 1, []uint16{1})
	_ = n.SetProperty("k", "v")
	n.SetTemporal(&types.TemporalMetadata{TxFrom: 1, ValidFrom: 100, ValidTo: 200})
	return n
}
