package tiered

import (
	"slices"
	"testing"
	"time"

	badgerstore "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A build of a former-label set runs with no cut's allowance behind it (the
// handle sweep's build step, and the closes that build on the handle they
// close), so what it holds must not grow with the shard. These tests count,
// at the badger sidecar's record, the former members such a build keeps in
// RAM (none), with the decode count as proof that it walked the history and
// the sidecar of a pinned scan over the same shard as the exact control. The
// counters are process-wide: no test here runs in parallel.

// cfSidecarControl opens day 20, closed, on a handle of its own and builds the
// membership sidecar a pinned scan builds: the labels it names, and the
// former members it took in.
func cfSidecarControl(t *testing.T, ts *Store) (tokens []uint16, members int64) {
	t.Helper()
	store, err := ts.newShardStore(ts.coldReadCfg(cfDay20(t, ts).path, false))
	if err != nil {
		t.Fatalf("control: open day 20: %v", err)
	}
	defer func() { _ = store.Close() }()
	recorded, stop := badgerstore.CountLabelTxMembersRecordedForTest()
	defer stop()
	if err := store.ForEachLabelTxMember(1, func(types.NodeID, types.Instant) bool { return true }); err != nil {
		t.Fatalf("control: pinned scan: %v", err)
	}
	tokens, ok := store.FormerLabelTokens()
	if !ok {
		t.Fatal("control: no sidecar after a pinned scan")
	}
	slices.Sort(tokens)
	return tokens, recorded()
}

// The handle sweep's build step.
func TestCrashFix_BuildStepKeepsNoMember(t *testing.T) {
	dir := t.TempDir()
	cfSealedWithoutSet(t, dir)
	hook, entered, release := cfBlockAt("build")
	ts, _ := cfReopenDay23Cfg(t, dir, func(c *Config) { c.sweepFault = hook })
	cfWait(t, entered, "the build step")
	recorded, stopRecorded := badgerstore.CountLabelTxMembersRecordedForTest()
	decoded, stopDecoded := badgerstore.CountMembershipDecodesForTest()
	close(release)
	ts.SweepHandlesForTest()
	kept, walked := recorded(), decoded()
	stopRecorded()
	stopDecoded()

	cfWantSettledEntry(t, ts, dir, "after the build step")
	wantTokens, members := cfSidecarControl(t, ts)
	if len(wantTokens) != 1 || members != cfFormerRows {
		t.Fatalf("control: the sidecar names %v with %d former members; want one label with %d", wantTokens, members, cfFormerRows)
	}
	if e := cfEntry(ts, cfDay20Name); !slices.Equal(e.FormerLabels, wantTokens) {
		t.Errorf("day 20's former labels %v; want the control's %v", e.FormerLabels, wantTokens)
	}
	if walked < cfHistoryRows {
		t.Errorf("the build step decoded %d rows; want at least day 20's %d history rows", walked, cfHistoryRows)
	}
	if kept != 0 {
		t.Errorf("the build step kept %d former members in RAM; want 0 (a pinned scan's sidecar keeps %d): its heap grows with the shard", kept, members)
	}
}

// The closes that build on the handle they close: the idle close and Close,
// each after a late logon opened day 20 writable and unsealed it.
func TestCrashFix_ClosingBuildKeepsNoMember(t *testing.T) {
	for _, how := range []string{"idle close", "Close"} {
		t.Run(how, func(t *testing.T) {
			dir := t.TempDir()
			ts, tok, _ := cfHistoryColdDay(t, dir, &tierClock{})
			defer func() { _ = ts.Close() }()
			wantTokens, members := cfSidecarControl(t, ts)
			if len(wantTokens) != 1 || members != cfFormerRows {
				t.Fatalf("control: the sidecar names %v with %d former members; want one label with %d", wantTokens, members, cfFormerRows)
			}
			if _, err := cfPut(t, ts, tok, julDay(20, 4)); err != nil {
				t.Fatalf("late logon into day 20: %v", err)
			}
			if e := cfEntry(ts, cfDay20Name); e.CountsSealed || e.FormerLabelsKnown {
				t.Fatalf("day 20 after the write: %+v; want unsealed", e)
			}
			if s, ro := cfDay20(t, ts).StoreStateForTest(); s == nil || ro {
				t.Fatalf("day 20 after the write: open %v, read-only %v; want a writable handle", s != nil, ro)
			}

			recorded, stopRecorded := badgerstore.CountLabelTxMembersRecordedForTest()
			defer stopRecorded()
			decoded, stopDecoded := badgerstore.CountMembershipDecodesForTest()
			defer stopDecoded()
			if how == "Close" {
				if err := ts.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
			} else {
				ts.HoldSettleStepsForTest() // the close's own build, not a later step's
				ts.SetIdleTimeoutForTest(time.Millisecond)
				time.Sleep(5 * time.Millisecond)
				ts.CloseIdleShardsForTest()
				if s, _ := cfDay20(t, ts).StoreStateForTest(); s != nil {
					t.Fatal("day 20 open after the idle close")
				}
			}
			e := cfSavedEntry(t, dir, cfDay20Name)
			if !cfEntrySettled(e) || !slices.Equal(e.FormerLabels, wantTokens) {
				t.Errorf("day 20 saved after the %s: %+v; want settled with the control's labels %v", how, e, wantTokens)
			}
			if got := decoded(); got < cfHistoryRows {
				t.Errorf("the %s decoded %d rows; want at least day 20's %d history rows", how, got, cfHistoryRows)
			}
			if got := recorded(); got != 0 {
				t.Errorf("the %s kept %d former members in RAM; want 0 (a pinned scan's sidecar keeps %d)", how, got, members)
			}
		})
	}
}

// ShardAccessStats names the build step's memory from the configuration: the
// step's block cache (the store's, at most the transient 16 MiB), its index
// cache, the walk's token set and the handle's base; a store without a sweep
// reports none.
func TestShardAccessStats_SweepBuildBytes(t *testing.T) {
	t.Parallel()
	const fixed = badgerstore.FormerLabelTokenSetBytes + sweepBuildHandleBytes
	if badgerstore.FormerLabelTokenSetBytes != 8<<10+128<<10 {
		t.Fatalf("the token set's bound is %d B; want 8 KiB of bits and 128 KiB of tokens", badgerstore.FormerLabelTokenSetBytes)
	}
	for _, c := range []struct {
		name string
		edit func(*Config)
		want int64
	}{
		{"default caches", func(*Config) {}, 16<<20 + fixed},
		{"a smaller block cache", func(c *Config) { c.BlockCacheSize = 4 << 20 }, 4<<20 + fixed},
		{"a larger block cache", func(c *Config) { c.BlockCacheSize = 64 << 20 }, 16<<20 + fixed},
		{"an index cache", func(c *Config) { c.IndexCacheSize = 8 << 20 }, 16<<20 + 8<<20 + fixed},
		{"in memory: no sweep", func(c *Config) { c.DataDir, c.InMemory, c.SyncWrites = "", true, false }, 0},
	} {
		clock := &tierClock{}
		clock.set(julDay(20, 12))
		cfg := cfCfg(t.TempDir(), clock)
		c.edit(&cfg)
		ts, _ := cfOpenCfg(t, cfg)
		if got := ts.ShardAccessStats().SweepBuildBytes; got != c.want {
			t.Errorf("%s: SweepBuildBytes %d; want %d", c.name, got, c.want)
		}
		if err := ts.Close(); err != nil {
			t.Fatalf("%s: close: %v", c.name, err)
		}
	}
}
