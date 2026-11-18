package tiered

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type dropAbortFixture struct {
	ts       *Store
	cfg      Config
	reg      *registrypkg.LabelRegistry
	es       *EventShard
	tok      uint16
	ids      []types.NodeID
	rels     []types.RelID
	ref, hot types.NodeID
}

func newDropAbortFixture(t *testing.T) *dropAbortFixture {
	t.Helper()
	f := &dropAbortFixture{cfg: Config{DataDir: t.TempDir(), RefLabels: []string{"Case"}, MemTableSize: 16 << 20,
		ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1}}
	var err error
	f.ts, err = New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.ts.Close() })
	f.reg = registrypkg.NewLabelRegistry()
	f.ts.SetLabelRegistry(f.reg)
	f.tok, _ = f.reg.GetOrCreate("Signal")
	refTok, _ := f.reg.GetOrCreate("Case")
	hotTok, _ := f.reg.GetOrCreate("Alert")
	gen := tieredNodeGen(t)
	put := func(tok uint16) types.NodeID {
		id := types.NodeID(gen.Generate())
		if err := f.ts.PutNode(types.NewNode(id, tok, nil)); err != nil {
			t.Fatal(err)
		}
		return id
	}
	for range 10 {
		f.ids = append(f.ids, put(f.tok))
	}
	f.ref = put(refTok)
	f.es = f.ts.HotShardForTest()
	forceRotation(t, f.ts)
	f.hot = put(hotTok)
	rg := tieredRelGen(t)
	for _, pair := range [][2]types.NodeID{{f.ids[0], f.ref}, {f.ref, f.ids[1]}, {f.ids[2], f.hot}, {f.hot, f.ids[3]}} {
		r := types.NewRelationship(types.RelID(rg.Generate()), 1, pair[0], pair[1])
		if err := f.ts.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
		f.rels = append(f.rels, r.ID())
	}
	if err := f.ts.Flush(); err != nil {
		t.Fatal(err)
	}
	f.ts.SweepHandlesForTest()
	return f
}

func (f *dropAbortFixture) purge() (storecontract.RetentionPurgeResult, error) {
	return f.ts.PurgeNodesByLabelBefore(f.tok, 1<<50, 8)
}

func (f *dropAbortFixture) readable(t *testing.T) {
	t.Helper()
	for _, id := range append(slices.Clone(f.ids), f.ref, f.hot) {
		if n, err := f.ts.GetNode(id); err != nil || n.ID() != id {
			t.Fatalf("row %d after abort: %v", id, err)
		}
	}
}

func (f *dropAbortFixture) wantPurged(t *testing.T, res storecontract.RetentionPurgeResult) {
	t.Helper()
	got, want := slices.Clone(res.PurgedNodeIDs), slices.Clone(f.ids)
	slices.Sort(got)
	slices.Sort(want)
	if res.NodesPurged != 10 || res.RelsPurged != 4 || !slices.Equal(got, want) {
		t.Fatalf("purge result %+v, want 10 exact node IDs and 4 relationships", res)
	}
	for _, id := range f.ids {
		if _, err := f.ts.GetNode(id); !errors.Is(err, ErrNodeNotFound) {
			t.Fatalf("purged row %d: %v", id, err)
		}
	}
	for _, id := range f.rels {
		if _, err := f.ts.GetRelationship(id); !errors.Is(err, ErrRelNotFound) {
			t.Fatalf("purged relationship %d: %v", id, err)
		}
	}
	for _, id := range []types.NodeID{f.ref, f.hot} {
		if _, err := f.ts.GetNode(id); err != nil {
			t.Fatal(err)
		}
		if rels, err := f.ts.IncomingRelationships(id, 0); err != nil || len(rels) != 0 {
			t.Fatalf("survivor incoming: %v %v", rels, err)
		}
		if rels, err := f.ts.OutgoingRelationships(id, 0); err != nil || len(rels) != 0 {
			t.Fatalf("survivor outgoing: %v %v", rels, err)
		}
	}
}

func TestFastDropAbortRestoresRouting(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"control", "checkout", "collect", "sweep", "collect_reopen", "sweep_reopen"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			f := newDropAbortFixture(t)
			if kind == "control" {
				res, err := f.purge()
				if err != nil {
					t.Fatal(err)
				}
				f.wantPurged(t, res)
				return
			}
			handle, err := f.es.CheckoutStoreForTest(f.ts)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { _, err := f.purge(); done <- err }()
			deadline := time.Now().Add(5 * time.Second)
			for {
				f.ts.mu.RLock()
				linked := f.ts.eventShards[f.es.name] != nil
				f.ts.mu.RUnlock()
				if !linked {
					break
				}
				if time.Now().After(deadline) {
					f.es.CheckinStoreForTest()
					<-done
					t.Fatal("purge never unlinked candidate")
				}
				time.Sleep(time.Millisecond)
			}
			fault := errors.New("injected checkout failure")
			switch kind {
			case "checkout":
				f.ts.recordBackgroundError(fault)
			case "collect", "collect_reopen":
				handle.SetDBClosedForTest(true)
			default:
				hot, _ := f.ts.HotShardForTest().StoreStateForTest()
				hot.SetDBClosedForTest(true)
			}
			f.es.CheckinStoreForTest()
			err = <-done
			handle.SetDBClosedForTest(false)
			hot, _ := f.ts.HotShardForTest().StoreStateForTest()
			hot.SetDBClosedForTest(false)
			if err == nil {
				t.Fatal("faulted purge succeeded")
			}
			if kind == "checkout" && !errors.Is(err, fault) {
				t.Fatalf("checkout fault lost: %v", err)
			}
			f.ts.mu.RLock()
			linked := f.ts.eventShards[f.es.name] == f.es
			reserved := len(f.ts.droppingShards)
			f.ts.mu.RUnlock()
			if !linked || reserved != 0 {
				t.Fatalf("after aborted %s: linked %v, reservations %d", kind, linked, reserved)
			}
			if _, ok := f.ts.catalog.GetShard(f.es.name); !ok {
				t.Fatal("aborted drop lost catalog entry")
			}
			if _, err := os.Stat(f.ts.shardDir(f.es.path)); err != nil {
				t.Fatal(err)
			}
			if err := f.ts.RecoverBackgroundError(); err != nil {
				t.Fatal(err)
			}
			f.readable(t)
			if kind == "collect_reopen" || kind == "sweep_reopen" {
				if err := f.ts.Close(); err != nil {
					t.Fatal(err)
				}
				f.ts, err = New(f.cfg)
				if err != nil {
					t.Fatal(err)
				}
				f.ts.SetLabelRegistry(f.reg)
				f.readable(t)
			}
			res, err := f.purge()
			if err != nil {
				t.Fatal(err)
			}
			f.wantPurged(t, res)
		})
	}
}

func TestFastDropRollbackReopenFailureGoesCold(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("requires unprivileged filesystem permissions")
	}
	f := newDropAbortFixture(t)
	meta := filepath.Join(f.cfg.DataDir, "meta")
	if err := os.Chmod(meta, 0o500); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(meta, 0o700) }()
	var fault manifestFault
	f.ts.SetShardOpenHookForTest(func(cfg BadgerStoreConfig) {
		if filepath.Base(cfg.Dir) == f.es.name {
			fault.breakAt(t, cfg.Dir)
		}
	})
	_, err := f.purge()
	f.ts.SetShardOpenHookForTest(nil)
	if err == nil || !fault.broken() {
		t.Fatalf("rollback reopen: %v, fault fired %v", err, fault.broken())
	}
	fault.restore(t)
	if err := os.Chmod(meta, 0o700); err != nil {
		t.Fatal(err)
	}
	wantColdInMemory(t, f.ts, f.es)
	f.readable(t)
	res, err := f.purge()
	if err != nil {
		t.Fatal(err)
	}
	f.wantPurged(t, res)
}

func TestFastDropCommittedRemovalFailureKeepsNameReserved(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("requires unprivileged filesystem permissions")
	}
	f := newDropAbortFixture(t)
	dir := f.ts.shardDir(f.es.path)
	defer func() { _ = os.Chmod(dir, 0o700) }()
	f.ts.dropRemoveHook = func() {
		if err := os.Chmod(dir, 0o500); err != nil {
			t.Error(err)
		}
	}
	res, err := f.purge()
	f.ts.dropRemoveHook = nil
	if err != nil {
		t.Fatalf("committed drop reported no result: %+v, %v", res, err)
	}
	f.wantPurged(t, res)
	if _, ok := f.ts.catalog.GetShard(f.es.name); ok {
		t.Fatal("committed drop is still cataloged")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("fault did not leave directory: %v", err)
	}
	f.ts.mu.RLock()
	_, reserved := f.ts.droppingShards[f.es.name]
	f.ts.mu.RUnlock()
	if !reserved {
		t.Fatal("unremoved directory's name is free")
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	forceRotation(t, f.ts)
	if f.ts.HotShardForTest().Name() == f.es.name {
		t.Fatal("rotation adopted purged shard's directory")
	}
	f.wantPurged(t, res)
	if err := f.ts.Close(); err != nil {
		t.Fatal(err)
	}
	f.ts, err = New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.ts.SetLabelRegistry(f.reg)
	forceRotation(t, f.ts)
	if f.ts.HotShardForTest().Name() == f.es.name {
		t.Fatal("rotation after restart adopted the purged directory")
	}
	f.wantPurged(t, res)
}

func TestNewEventShardRefusesAnExistingDirectory(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	f := newDropAbortFixture(t)
	dir := filepath.Join("events", "unowned")
	full := filepath.Join(f.cfg.DataDir, dir)
	if err := os.Mkdir(full, 0o700); err != nil {
		t.Fatal(err)
	}
	proof := filepath.Join(full, "proof")
	if err := os.WriteFile(proof, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	opens := 0
	f.ts.SetShardOpenHookForTest(func(BadgerStoreConfig) { opens++ })
	defer f.ts.SetShardOpenHookForTest(nil)
	if s, err := f.ts.openNewEventShard("unowned", dir, TierWarm); !errors.Is(err, os.ErrExist) {
		if s != nil {
			_ = s.Close()
		}
		t.Fatalf("existing directory: %v, want ErrExist", err)
	}
	if opens != 0 {
		t.Fatalf("existing directory opened %d stores, want 0", opens)
	}
	if data, err := os.ReadFile(proof); err != nil || string(data) != "keep" {
		t.Fatalf("existing contents: %s, %v", data, err)
	}
	s, err := f.ts.openNewEventShard("fresh", filepath.Join("events", "fresh"), TierWarm)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	id := types.NodeID(tieredNodeGen(t).Generate())
	if err := s.PutNode(types.NewNode(id, f.tok, nil)); err != nil {
		t.Fatal(err)
	}
	if n, err := s.GetNode(id); err != nil || n.ID() != id {
		t.Fatalf("new directory control: %v", err)
	}
	if opens != 1 {
		t.Fatalf("new directory control: %d opens, want 1", opens)
	}
}
