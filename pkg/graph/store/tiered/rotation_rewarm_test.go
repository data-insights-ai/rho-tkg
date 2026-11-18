package tiered

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// L3: a calendar rotation promotes the window shard holding the clock, which
// must be warm. A warm window shard ahead of the clock whose close failed (a
// move, a Clear) is cold in memory with its catalog entry warm; once the
// clock reaches its window the rotation makes it warm again first
// (rewarmLocked): it closes an idle cold handle, unseals the shard, opens it
// at the warm size and re-applies the relationship temporal definitions a
// cold writable open dropped. A reader pinning the cold handle refuses the
// triggering write ("in use"); a failed open refuses it with the shard left
// cold and closed. Native counters: the shard's .mem apparent size (2 x the
// open's memtable), the open cold count, the handle's relationship temporal
// definitions.

const (
	rwDay     = "2026-07-23"
	rwRelType = 7
)

type rewarmFix struct {
	ts         *Store
	cfg        Config
	clock      *tierClock
	tok, other uint16
	es         *EventShard
	rows       map[types.NodeID]string // every row written to day 23, by its pad
}

// newRewarmFix is a calendar store at MemTableSize 16 MiB with a warm volume
// and a relationship temporal index; the clock at day 22, day 23 a warm
// window shard holding one row.
func newRewarmFix(t *testing.T, maxCold int) *rewarmFix {
	t.Helper()
	f := &rewarmFix{clock: &tierClock{}, rows: map[types.NodeID]string{}}
	f.clock.set(julDay(22, 12))
	f.cfg = tierClockCfg(t.TempDir(), f.clock)
	f.cfg.MemTableSize = cgHot
	f.cfg.ValueLogFileSize = 1 << 20
	f.cfg.CacheCapacity = 100
	f.cfg.WarmDir = t.TempDir()
	f.cfg.MaxOpenColdShards = maxCold
	ts, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.ts = ts
	t.Cleanup(func() { _ = f.ts.Close() })
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	f.tok, _ = reg.GetOrCreate("Signal")
	f.other, _ = reg.GetOrCreate("Other")
	if err := ts.CreateRelTemporalIndex(rwRelType); err != nil {
		t.Fatal(err)
	}
	f.put(t, f.tok, 3)
	f.es = ts.EventShardsForTest()[rwDay]
	if f.es == nil || f.es.Tier() != TierWarm {
		t.Fatalf("fixture: day 23 is %+v, want a warm window shard", f.es)
	}
	ts.SweepHandlesForTest()
	return f
}

func (f *rewarmFix) putErr(t *testing.T, tok uint16, hour int) (types.NodeID, error) {
	t.Helper()
	id, ok, err := f.ts.MintNodeIDAt(tok, ms(julDay(23, hour)), true)
	if err != nil || !ok {
		t.Fatalf("MintNodeIDAt: %v, %v", ok, err)
	}
	pad := fmt.Sprintf("h%d/", hour) + strings.Repeat("p", 4<<10)
	n := types.NewNode(id, tok, nil)
	if err := n.SetProperties(mustTieredProps(t, map[string]any{"pad": pad})); err != nil {
		t.Fatal(err)
	}
	if err := f.ts.PutNode(n); err != nil {
		return id, err
	}
	f.rows[id] = pad
	return id, f.ts.Flush()
}

func (f *rewarmFix) put(t *testing.T, tok uint16, hour int) types.NodeID {
	t.Helper()
	id, err := f.putErr(t, tok, hour)
	if err != nil {
		t.Fatalf("PutNode into day 23: %v", err)
	}
	return id
}

// moveCloseFault fails day 23's close in MoveShard (the fence, the shard
// cold in memory, the catalog warm), then lifts the fence. Control: the move
// succeeds.
func (f *rewarmFix) moveCloseFault(t *testing.T, fault bool) {
	t.Helper()
	if fault {
		s, _ := f.es.StoreStateForTest()
		s.FailNextCloseForTest(errors.New("injected close fault"))
	}
	err := f.ts.MoveShard(context.Background(), rwDay, VolumeWarm)
	if !fault {
		if err != nil {
			t.Fatalf("control MoveShard: %v", err)
		}
		return
	}
	if err == nil {
		t.Fatal("MoveShard with a close fault succeeded")
	}
	wantColdInMemory(t, f.ts, f.es)
	if err := f.ts.RecoverBackgroundError(); err != nil {
		t.Fatal(err)
	}
}

// clearReopenFault runs a Clear whose reopen of day 23 fails (its recovery
// probe reads a broken MANIFEST): the shard is cold in memory, its catalog
// entry warm, and no fence. Control: the Clear succeeds.
func (f *rewarmFix) clearReopenFault(t *testing.T, fault bool) {
	t.Helper()
	var m manifestFault
	if fault {
		f.ts.SetShardOpenHookForTest(func(cfg BadgerStoreConfig) {
			if cfg.ReadOnly && filepath.Base(cfg.Dir) == rwDay {
				m.breakAt(t, cfg.Dir)
			}
		})
	}
	err := f.ts.Clear()
	f.ts.SetShardOpenHookForTest(nil)
	for id := range f.rows {
		f.rows[id] = "" // cleared
	}
	if !fault {
		if err != nil {
			t.Fatalf("control Clear: %v", err)
		}
		// A completed Clear removes index definitions. Recreate the caller's
		// requested index so both paths enter rotation with the same anchor.
		if err := f.ts.CreateRelTemporalIndex(rwRelType); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err == nil {
		t.Fatal("Clear with a broken reopen succeeded")
	}
	m.restore(t)
	if bg := f.ts.backgroundError(); bg != nil {
		t.Fatalf("background error %v after the Clear, want none", bg)
	}
	wantColdInMemory(t, f.ts, f.es)
}

// rotate moves the clock to day 23 and writes a row there: the write rotates.
func (f *rewarmFix) rotate(t *testing.T) error {
	t.Helper()
	f.clock.set(julDay(23, 8))
	_, err := f.putErr(t, f.tok, 8)
	return err
}

// answers is what the store holds after the rotation: the hot shard, day
// 23's tier in memory and in the catalog, its handle (writable, WAL, the
// relationship temporal definitions), the open cold count, every row's pad
// and the label counts.
func (f *rewarmFix) answers(t *testing.T) string {
	t.Helper()
	var b strings.Builder
	entry, _ := f.ts.CatalogForTest().GetShard(rwDay)
	s, ro := f.es.StoreStateForTest()
	fmt.Fprintf(&b, "hot %s, day 23 %s, catalog %s, open %v, read-only %v", f.ts.HotShardForTest().Name(), f.es.Tier(), entry.Tier, s != nil, ro)
	if s != nil {
		defs, err := s.RelTemporalIndexTypes()
		fmt.Fprintf(&b, ", memtable %d, defs %v %v", s.MemTableSize(), defs, err)
	}
	fmt.Fprintf(&b, ", WAL %d, open cold %d\n", maxMemFileSizeTiered(t, f.ts.shardDir(f.es.path)), f.ts.OpenColdShardsForTest())
	b.WriteString(rewarmRows(t, f.ts, f.rows))
	for _, tok := range []uint16{f.tok, f.other} {
		ns, err := f.ts.NodesByLabel(tok, QueryOpts{})
		fmt.Fprintf(&b, "label %d: %d rows %v\n", tok, len(ns), err)
	}
	return b.String()
}

// rewarmRows reads every row: its pad as written, or not-found for a row
// cleared ("").
func rewarmRows(t *testing.T, ts *Store, rows map[types.NodeID]string) string {
	t.Helper()
	ids := make([]types.NodeID, 0, len(rows))
	for id := range rows {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	var b strings.Builder
	for i, id := range ids {
		n, err := ts.GetNode(id)
		switch {
		case err != nil:
			fmt.Fprintf(&b, "row %d: %v (want cleared %v)\n", i, errors.Is(err, ErrNodeNotFound), rows[id] == "")
		default:
			pad, _ := n.GetProperty("pad")
			fmt.Fprintf(&b, "row %d: pad as written %v\n", i, fmt.Sprint(pad) == rows[id])
		}
	}
	return b.String()
}

// wantRewarmed asserts the rotation's state: day 23 hot on a writable
// hot-size handle (WAL 2 x 16 MiB), no cold handle counted, the relationship
// temporal definition on the handle, every row as written.
func (f *rewarmFix) wantRewarmed(t *testing.T) {
	t.Helper()
	entry, _ := f.ts.CatalogForTest().GetShard(rwDay)
	s, ro := f.es.StoreStateForTest()
	if f.ts.HotShardForTest() != f.es || f.es.Tier() != TierHot || entry.Tier != TierHot || s == nil || ro {
		t.Fatalf("after the rotation: %s", f.answers(t))
	}
	if w := maxMemFileSizeTiered(t, f.ts.shardDir(f.es.path)); w != 2*cgHot {
		t.Errorf("day 23's WAL %d, want 2 x %d", w, cgHot)
	}
	if n := f.ts.OpenColdShardsForTest(); n != 0 {
		t.Errorf("open cold shards %d, want 0", n)
	}
	if defs, err := s.RelTemporalIndexTypes(); err != nil || !slices.Equal(defs, []uint16{rwRelType}) {
		t.Errorf("day 23's relationship temporal definitions %v (%v), want [%d]", defs, err, rwRelType)
	}
}

// reopenRows closes the store, opens it again and reads every row.
func (f *rewarmFix) reopenRows(t *testing.T) string {
	t.Helper()
	if err := f.ts.Close(); err != nil {
		t.Fatal(err)
	}
	re, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.ts = re
	return rewarmRows(t, re, f.rows)
}

// TestRotation_RewarmsShardColdInMemory: each case reaches day 23 cold in
// memory with its catalog entry warm (a close fault in MoveShard, or a
// Clear's failed reopen), then what may happen to it since (a cold read that
// keeps a read-only handle, a late write's small handle, a read that closes
// and seals it), then the clock reaches day 23 and a create rotates. The
// rotation makes day 23 warm again and promotes it. Control: no fault, the
// same sequence, the same answers before and after a reopen.
func TestRotation_RewarmsShardColdInMemory(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	for _, tc := range []struct {
		name    string
		maxCold int
		entry   func(*rewarmFix, *testing.T, bool)
		since   func(*rewarmFix, *testing.T, bool)
	}{
		{"move close fault", 0, (*rewarmFix).moveCloseFault, nil},
		{"clear reopen failure", 0, (*rewarmFix).clearReopenFault, nil},
		{"cold read since", 1, (*rewarmFix).moveCloseFault, func(f *rewarmFix, t *testing.T, fault bool) {
			for id := range f.rows {
				if _, err := f.ts.GetNode(id); err != nil {
					t.Fatal(err)
				}
			}
			if s, ro := f.es.StoreStateForTest(); fault && (s == nil || !ro) {
				t.Fatalf("after the cold read: open %v read-only %v, want a read-only handle kept", s != nil, ro)
			}
		}},
		{"late write since", 1, (*rewarmFix).moveCloseFault, func(f *rewarmFix, t *testing.T, fault bool) {
			f.put(t, f.tok, 5)
			if s, ro := f.es.StoreStateForTest(); fault && (s == nil || ro || s.MemTableSize() != coldMemTableSize) {
				t.Fatalf("after the late write: open %v read-only %v, want the small writable handle", s != nil, ro)
			}
		}},
		{"sealed since", 0, (*rewarmFix).moveCloseFault, func(f *rewarmFix, t *testing.T, fault bool) {
			for id := range f.rows {
				if _, err := f.ts.GetNode(id); err != nil {
					t.Fatal(err)
				}
			}
			if s, _ := f.es.StoreStateForTest(); fault && s != nil {
				t.Fatal("after the cap-0 read day 23 is still open")
			}
		}},
	} {
		run := func(t *testing.T, fault bool) (before, after string) {
			f := newRewarmFix(t, tc.maxCold)
			tc.entry(f, t, fault)
			if tc.since != nil {
				tc.since(f, t, fault)
			}
			if err := f.rotate(t); err != nil {
				t.Fatalf("the create that rotates (fault %v): %v", fault, err)
			}
			f.wantRewarmed(t)
			f.put(t, f.other, 9) // a label the shard did not hold when it was sealed
			before = f.answers(t)
			return before, f.reopenRows(t)
		}
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, gotAfter := run(t, true)
			ctl, ctlAfter := run(t, false)
			t.Logf("answers:\n%s", got)
			if got != ctl {
				t.Errorf("answers differ from the control:\n%s\ncontrol:\n%s", got, ctl)
			}
			if !strings.Contains(ctl, "pad as written true") || !strings.Contains(ctl, fmt.Sprintf("defs [%d]", rwRelType)) {
				t.Fatalf("control answers empty:\n%s", ctl)
			}
			if tc.name == "clear reopen failure" {
				// The failed Clear's marker stays: the reopen finishes it.
				if strings.Contains(gotAfter, "pad as written") {
					t.Errorf("after the reopen a row survived the interrupted Clear:\n%s", gotAfter)
				}
				if !strings.Contains(ctlAfter, "pad as written true") {
					t.Errorf("control after the reopen lost the rows written after its Clear:\n%s", ctlAfter)
				}
				return
			}
			if gotAfter != ctlAfter || strings.Contains(gotAfter, "false") || strings.Contains(gotAfter, "(want") {
				t.Errorf("after the reopen:\n%s\ncontrol:\n%s", gotAfter, ctlAfter)
			}
		})
	}

	t.Run("pin held", func(t *testing.T) {
		t.Parallel()
		f := newRewarmFix(t, 0)
		f.moveCloseFault(t, true)
		_, release, err := f.es.checkoutStoreReadOnly(f.ts)
		if err != nil {
			t.Fatal(err)
		}
		err = f.rotate(t)
		entry, _ := f.ts.CatalogForTest().GetShard(rwDay)
		hotEntry, _ := f.ts.CatalogForTest().GetShard("2026-07-22")
		if err == nil || !strings.Contains(err.Error(), "in use") {
			release()
			t.Fatalf("the create under a pin of the cold handle = %v, want the in-use refusal", err)
		}
		if f.ts.HotShardForTest().Name() != "2026-07-22" || hotEntry.Tier != TierHot || entry.Tier != TierWarm || f.es.Tier() != TierCold {
			release()
			t.Fatalf("after the refusal: %s; want day 22 hot, day 23 cold in memory and warm in the catalog", f.answers(t))
		}
		release()
		if err := f.rotate(t); err != nil {
			t.Fatalf("the create after the release: %v", err)
		}
		f.wantRewarmed(t)
		if rows := f.reopenRows(t); strings.Contains(rows, "false") || strings.Contains(rows, "(want") {
			t.Fatalf("after the reopen:\n%s", rows)
		}
	})

	t.Run("reopen fails", func(t *testing.T) {
		t.Parallel()
		f := newRewarmFix(t, 0)
		f.moveCloseFault(t, true)
		var m manifestFault
		f.ts.SetShardOpenHookForTest(func(cfg BadgerStoreConfig) {
			if filepath.Base(cfg.Dir) == rwDay {
				m.breakAt(t, cfg.Dir)
			}
		})
		err := f.rotate(t)
		f.ts.SetShardOpenHookForTest(nil)
		entry, _ := f.ts.CatalogForTest().GetShard(rwDay)
		s, _ := f.es.StoreStateForTest()
		if err == nil || !m.broken() {
			t.Fatalf("the create with day 23's open broken = %v (broken %v), want the open's error", err, m.broken())
		}
		if f.ts.HotShardForTest().Name() != "2026-07-22" || entry.Tier != TierWarm || f.es.Tier() != TierCold || s != nil {
			t.Fatalf("after the failed open: hot %s, day 23 %s, catalog %s, open %v; want day 22 hot, day 23 cold and closed, catalog warm",
				f.ts.HotShardForTest().Name(), f.es.Tier(), entry.Tier, s != nil)
		}
		m.restore(t)
		if err := f.rotate(t); err != nil {
			t.Fatalf("the create after the restore: %v", err)
		}
		f.wantRewarmed(t)
		if rows := f.reopenRows(t); strings.Contains(rows, "false") || strings.Contains(rows, "(want") {
			t.Fatalf("after the reopen:\n%s", rows)
		}
	})
}

// Promotion uses the same durable move as an explicit move. Every cut before
// its catalog save leaves the warm copy authoritative; every later cut leaves
// the hot-volume copy authoritative. Retry and reopen must preserve all rows.
func TestRotation_RestoresMovedWindow(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	for _, step := range []string{"gated", "copied", "renamed", "saved", "removed", ""} {
		t.Run("fault_"+step, func(t *testing.T) {
			t.Parallel()
			f := newRewarmFix(t, 1)
			f.moveCloseFault(t, false)
			before := rewarmRows(t, f.ts, f.rows)
			fault := errors.New("injected promotion move failure")
			fired := false
			f.ts.SetMoveFaultForTest(func(at string) error {
				if at == step && !fired {
					fired = true
					return fault
				}
				return nil
			})
			err := f.rotate(t)
			f.ts.SetMoveFaultForTest(nil)
			wantVolume := ""
			if step != "" {
				if !fired || !errors.Is(err, fault) {
					t.Fatalf("fault %q fired %v, rotation %v", step, fired, err)
				}
				if f.ts.HotShardForTest().Name() != "2026-07-22" {
					t.Fatal("failed move advanced the hot shard")
				}
				if step == "gated" || step == "copied" || step == "renamed" {
					wantVolume = VolumeWarm
				}
				if got := rewarmRows(t, f.ts, f.rows); got != before {
					t.Fatalf("rows changed on failed move: %s; before %s", got, before)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			entry, _ := f.ts.catalog.GetShard(rwDay)
			if entry.Volume != wantVolume {
				t.Fatalf("volume %q after %q, want %q", entry.Volume, step, wantVolume)
			}
			wantRows := rewarmRows(t, f.ts, f.rows)
			if got := f.reopenRows(t); got != wantRows || strings.Contains(got, "false") {
				t.Fatalf("rows after reopen: %s; want %s", got, wantRows)
			}
			f.es = f.ts.EventShardsForTest()[rwDay]
			if err := f.rotate(t); err != nil {
				t.Fatalf("retry: %v", err)
			}
			f.wantRewarmed(t)
			want := filepath.Join(f.cfg.DataDir, "events", rwDay)
			if copies := shardCopies(t, f.cfg, rwDay); !slices.Equal(copies, []string{want}) {
				t.Fatalf("copies after retry: %v, want %s", copies, want)
			}
		})
	}
}

func TestRotation_MovedWindowWaitsForPinOutsideStoreLock(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	f := newRewarmFix(t, 1)
	f.moveCloseFault(t, false)
	s, release, err := f.es.checkoutStoreReadOnly(f.ts)
	if err != nil {
		t.Fatal(err)
	}
	release = sync.OnceFunc(release)
	defer release()
	gated := make(chan struct{})
	copied := make(chan struct{})
	f.ts.SetMoveFaultForTest(func(step string) error {
		switch step {
		case "gated":
			close(gated)
		case "copied":
			close(copied)
		}
		return nil
	})
	f.clock.set(julDay(23, 8))
	done := make(chan error, 1)
	go func() { done <- f.ts.checkRotation() }()
	select {
	case <-gated:
	case err := <-done:
		t.Fatalf("rotation before move gate: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("rotation never reached move gate")
	}
	// An existing reader can still acquire ts.mu and read its pinned handle.
	read := make(chan error, 1)
	go func() {
		f.ts.mu.RLock()
		defer f.ts.mu.RUnlock()
		for id := range f.rows {
			if _, err := s.GetNode(id); err != nil {
				read <- err
				return
			}
		}
		read <- nil
	}()
	select {
	case err := <-read:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("promotion holds the store lock while waiting for the reader")
	}
	select {
	case <-copied:
		t.Fatal("promotion copied a pinned shard")
	case err := <-done:
		t.Fatalf("promotion returned under a pin: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("promotion stuck after the pin was released")
	}
	f.ts.SetMoveFaultForTest(nil)
	f.wantRewarmed(t)
	if got := f.reopenRows(t); strings.Contains(got, "false") || strings.Contains(got, "(want") {
		t.Fatalf("rows after reopen: %s", got)
	}
}
