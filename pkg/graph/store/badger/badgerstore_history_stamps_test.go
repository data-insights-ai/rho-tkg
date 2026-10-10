package badger

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 30: the badger history-stamps sidecar (badgerstore_history_stamps.go).
// Every test compares the sidecar's answer with the fold of the history door
// over the same ID, so a test that asserts a wrong expectation fails on its
// own oracle.

type stampKind struct {
	name     string
	put      func(bs *Store, id int64, ver uint32, tf, tt, da types.Instant) error
	trimFrom func(bs *Store, id int64, minVer uint32) error
	truncate func(bs *Store, id int64, keep int) error
	stamps   func(bs *Store, id int64) (types.Instant, types.Instant, bool, error)
	fold     func(bs *Store, id int64) (types.Instant, types.Instant, bool, error)
	built    func(s HistoryStampsStats) (bool, int)
}

func stampTM(tf, tt, da types.Instant) *types.TemporalMetadata {
	return &types.TemporalMetadata{TxFrom: tf, TxTo: tt, DeletedAt: da}
}

func foldTMs(tms []*types.TemporalMetadata) (from, to types.Instant, has bool) {
	for _, tm := range tms {
		if tm != nil {
			from = max(from, tm.TxFrom)
			to = max(to, tm.TxTo, tm.DeletedAt)
		}
	}
	return from, to, len(tms) > 0
}

func stampKinds() []stampKind {
	return []stampKind{
		{
			name: "node",
			put: func(bs *Store, id int64, ver uint32, tf, tt, da types.Instant) error {
				n := types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)
				n.SetVersion(ver)
				n.SetTemporal(stampTM(tf, tt, da))
				return bs.PutNodeVersion(n.ID(), ver, n)
			},
			trimFrom: func(bs *Store, id int64, minVer uint32) error {
				return bs.TrimNodeHistoryFrom(types.NodeID(snowflake.ID(id)), minVer)
			},
			truncate: func(bs *Store, id int64, keep int) error {
				return bs.TruncateNodeHistory(types.NodeID(snowflake.ID(id)), keep)
			},
			stamps: func(bs *Store, id int64) (types.Instant, types.Instant, bool, error) {
				return bs.NodeHistoryStamps(types.NodeID(snowflake.ID(id)))
			},
			fold: func(bs *Store, id int64) (types.Instant, types.Instant, bool, error) {
				h, err := bs.GetNodeHistory(types.NodeID(snowflake.ID(id)))
				tms := make([]*types.TemporalMetadata, 0, len(h))
				for _, n := range h {
					tms = append(tms, n.Temporal())
				}
				f, to, has := foldTMs(tms)
				return f, to, has, err
			},
			built: func(s HistoryStampsStats) (bool, int) { return s.NodesBuilt, s.NodeIDs },
		},
		{
			name: "rel",
			put: func(bs *Store, id int64, ver uint32, tf, tt, da types.Instant) error {
				r := types.NewRelationship(types.RelID(snowflake.ID(id)), 5, types.NodeID(1), types.NodeID(2))
				r.SetVersion(ver)
				r.SetTemporal(stampTM(tf, tt, da))
				return bs.PutRelVersion(r.ID(), ver, r)
			},
			trimFrom: func(bs *Store, id int64, minVer uint32) error {
				return bs.TrimRelHistoryFrom(types.RelID(snowflake.ID(id)), minVer)
			},
			truncate: func(bs *Store, id int64, keep int) error {
				return bs.TruncateRelHistory(types.RelID(snowflake.ID(id)), keep)
			},
			stamps: func(bs *Store, id int64) (types.Instant, types.Instant, bool, error) {
				return bs.RelHistoryStamps(types.RelID(snowflake.ID(id)))
			},
			fold: func(bs *Store, id int64) (types.Instant, types.Instant, bool, error) {
				h, err := bs.GetRelHistory(types.RelID(snowflake.ID(id)))
				tms := make([]*types.TemporalMetadata, 0, len(h))
				for _, r := range h {
					tms = append(tms, r.Temporal())
				}
				f, to, has := foldTMs(tms)
				return f, to, has, err
			},
			built: func(s HistoryStampsStats) (bool, int) { return s.RelsBuilt, s.RelIDs },
		},
	}
}

// expectStampsB asserts stamps(id) == fold(history(id)) == want.
func expectStampsB(t *testing.T, k stampKind, bs *Store, step string, id int64, wantFrom, wantTo types.Instant, wantHas bool) {
	t.Helper()
	ff, ft, fh, err := k.fold(bs, id)
	if err != nil {
		t.Fatalf("%s %s: history(%d): %v", k.name, step, id, err)
	}
	if ff != wantFrom || ft != wantTo || fh != wantHas {
		t.Fatalf("%s %s: test expectation wrong: history(%d) folds to (%d, %d, %v), want (%d, %d, %v)", k.name, step, id, ff, ft, fh, wantFrom, wantTo, wantHas)
	}
	gf, gt, gh, err := k.stamps(bs, id)
	if err != nil || gf != ff || gt != ft || gh != fh {
		t.Fatalf("%s %s: stamps(%d) = (%d, %d, %v, %v), history folds to (%d, %d, %v)", k.name, step, id, gf, gt, gh, err, ff, ft, fh)
	}
}

// expectStampsAgree asserts stamps(id) == fold(history(id)) without a fixed want.
func expectStampsAgree(t *testing.T, k stampKind, bs *Store, step string, id int64) {
	t.Helper()
	ff, ft, fh, err := k.fold(bs, id)
	if err != nil {
		t.Fatalf("%s %s: history(%d): %v", k.name, step, id, err)
	}
	gf, gt, gh, err := k.stamps(bs, id)
	if err != nil || gf != ff || gt != ft || gh != fh {
		t.Fatalf("%s %s: stamps(%d) = (%d, %d, %v, %v), history folds to (%d, %d, %v)", k.name, step, id, gf, gt, gh, err, ff, ft, fh)
	}
}

// The first call builds while the rows sit in the pending buffer, then while
// they are parked in `flushing`; a buffered overwrite with lower stamps and a
// buffered delete win over the committed rows they replace. A build that reads
// committed values only, or keys only, answers differently.
func TestHistoryStamps_BuildSeesPendingAndFlushing(t *testing.T) {
	for _, k := range stampKinds() {
		t.Run(k.name+"/pending", func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			if err := k.put(bs, 10, 0, 100, 200, 0); err != nil {
				t.Fatal(err)
			}
			expectStampsB(t, k, bs, "pending, first call", 10, 100, 200, true)
			expectStampsB(t, k, bs, "pending, neighbour", 11, 0, 0, false)
		})
		t.Run(k.name+"/flushing", func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			if err := k.put(bs, 20, 0, 100, 0, 300); err != nil {
				t.Fatal(err)
			}
			parkPendingIntoFlushing(t, bs)
			expectStampsB(t, k, bs, "flushing, first call", 20, 100, 300, true)
		})
		t.Run(k.name+"/buffered overwrite and delete over committed rows", func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			for _, p := range []struct {
				ver    uint32
				tf, tt types.Instant
			}{{0, 100, 200}, {1, 900, 950}, {2, 300, 0}} {
				if err := k.put(bs, 30, p.ver, p.tf, p.tt, 0); err != nil {
					t.Fatal(err)
				}
			}
			if err := k.put(bs, 31, 0, 500, 600, 0); err != nil {
				t.Fatal(err)
			}
			if err := k.put(bs, 31, 1, 700, 800, 0); err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := k.put(bs, 30, 1, 10, 20, 0); err != nil { // overwrite the max row lower
				t.Fatal(err)
			}
			if err := k.trimFrom(bs, 31, 1); err != nil { // delete the max row
				t.Fatal(err)
			}
			expectStampsB(t, k, bs, "buffered overwrite", 30, 300, 200, true)
			expectStampsB(t, k, bs, "buffered delete", 31, 500, 600, true)
		})
	}
}

// Every history write after the build is maintained, in pending, after a
// flush and after a reopen: a raise, an overwrite that lowers the max, a
// delete of the max row, a delete of every row, a row after that.
func TestHistoryStamps_Maintenance(t *testing.T) {
	for _, k := range stampKinds() {
		t.Run(k.name, func(t *testing.T) {
			dir := t.TempDir()
			bs, err := New(Config{Dir: dir, FlushInterval: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = bs.Close() }()
			put := func(ver uint32, tf, tt, da types.Instant) {
				t.Helper()
				if err := k.put(bs, 40, ver, tf, tt, da); err != nil {
					t.Fatal(err)
				}
			}
			put(0, 100, 200, 0)
			expectStampsB(t, k, bs, "built", 40, 100, 200, true)
			put(1, 300, 0, 0)
			expectStampsB(t, k, bs, "raise", 40, 300, 200, true)
			put(2, 250, 0, 900)
			expectStampsB(t, k, bs, "delete stamp", 40, 300, 900, true)
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			put(1, 50, 60, 0)
			expectStampsB(t, k, bs, "overwrite lowers", 40, 250, 900, true)
			put(2, 260, 0, 0)
			expectStampsB(t, k, bs, "overwrite drops the delete stamp", 40, 260, 200, true)
			if err := k.trimFrom(bs, 40, 2); err != nil {
				t.Fatal(err)
			}
			expectStampsB(t, k, bs, "trim the max row", 40, 100, 200, true)
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			if bs, err = New(Config{Dir: dir, FlushInterval: time.Hour}); err != nil {
				t.Fatal(err)
			}
			expectStampsB(t, k, bs, "reopened", 40, 100, 200, true)
			if err := k.truncate(bs, 40, 1); err != nil {
				t.Fatal(err)
			}
			expectStampsB(t, k, bs, "truncate keep 1", 40, 50, 60, true)
			if err := k.trimFrom(bs, 40, 0); err != nil {
				t.Fatal(err)
			}
			expectStampsB(t, k, bs, "trim all", 40, 0, 0, false)
			put(5, 7, 8, 0)
			expectStampsB(t, k, bs, "row after empty", 40, 7, 8, true)
		})
	}
}

// Writes that land between the build's scan and its install: the scan saw
// the old rows, the writes changed them. A build that installs its scanned
// values over IDs a write touched keeps the stale max (A: its max row was
// trimmed; C: its max row was overwritten lower), and one that drops IDs it
// did not scan loses B's new row.
func TestHistoryStamps_BuildGuardKeepsWritesDuringScan(t *testing.T) {
	for _, k := range stampKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			must := func(err error) {
				if err != nil {
					t.Fatal(err)
				}
			}
			must(k.put(bs, 70, 0, 100, 200, 0))
			must(k.put(bs, 70, 1, 900, 0, 0))
			must(k.put(bs, 72, 0, 100, 200, 0))
			must(k.put(bs, 72, 1, 800, 850, 0))
			must(k.put(bs, 73, 0, 100, 200, 0))
			must(bs.Flush())
			fired := 0
			bs.historyStampsBuildHook = func() {
				fired++
				if fired > 1 {
					return
				}
				must(k.trimFrom(bs, 70, 1))           // A loses its max row
				must(k.put(bs, 71, 0, 5, 6, 0))       // B gains a first row
				must(k.put(bs, 72, 1, 10, 20, 0))     // C's max row overwritten lower
				must(k.put(bs, 73, 1, 1000, 1100, 0)) // D raised
			}
			if _, _, _, err := k.stamps(bs, 74); err != nil { // the first call builds; the hook writes
				t.Fatal(err)
			}
			expectStampsB(t, k, bs, "lost its max row during the scan", 70, 100, 200, true)
			expectStampsB(t, k, bs, "gained a row during the scan", 71, 5, 6, true)
			expectStampsB(t, k, bs, "overwritten lower during the scan", 72, 100, 200, true)
			expectStampsB(t, k, bs, "raised during the scan", 73, 1000, 1100, true)
			if fired == 0 {
				t.Fatal("the build hook never ran: the first call did not build")
			}
			bs.historyStampsBuildHook = nil
			must(bs.Flush())
			expectStampsB(t, k, bs, "flushed", 72, 100, 200, true)
		})
	}
}

// A write that lands while a probe resolves an unknown ID is not overwritten
// by the probe's older answer: the probe read the rows before the write, the
// write raised the max, so the next call must read the raise. A probe that
// installs without its per-ID stamp check caches the stale value.
func TestHistoryStamps_ProbeKeepsWriteDuringProbe(t *testing.T) {
	for _, k := range stampKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			for ver, tf := range []types.Instant{100, 200} {
				if err := k.put(bs, 100, uint32(ver), tf, tf+1, 0); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			expectStampsB(t, k, bs, "built", 100, 200, 201, true)
			if err := k.trimFrom(bs, 100, 1); err != nil { // unknown: row 0 remains
				t.Fatal(err)
			}
			fired := 0
			bs.historyStampsProbeHook = func() {
				fired++
				if fired == 1 {
					if err := k.put(bs, 100, 5, 700, 0, 750); err != nil {
						t.Error(err)
					}
				}
			}
			if f, to, has, err := k.stamps(bs, 100); err != nil || !has || f != 100 || to != 101 {
				t.Fatalf("probe answer = (%d, %d, %v, %v); want (100, 101, true) (the rows when it probed)", f, to, has, err)
			}
			if fired != 1 {
				t.Fatalf("probe hook fired %d times, want 1", fired)
			}
			bs.historyStampsProbeHook = nil
			expectStampsB(t, k, bs, "after the write in the probe window", 100, 700, 750, true)
		})
	}
}

// A flush that commits the parked rows and clears `flushing` between the
// build's overlay capture and its badger view (historyScanTestHook): a build
// that opens the view first and reads the overlay second sees the rows in
// neither and misses them for the store's lifetime.
func TestHistoryStamps_BuildAcrossCommitWindow(t *testing.T) {
	for _, k := range stampKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			if err := k.put(bs, 50, 0, 100, 0, 0); err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := k.put(bs, 50, 1, 600, 650, 0); err != nil {
				t.Fatal(err)
			}
			if err := k.put(bs, 51, 0, 300, 400, 0); err != nil {
				t.Fatal(err)
			}
			parkPendingIntoFlushing(t, bs)
			fired := 0
			bs.historyScanTestHook = func() {
				fired++
				if fired == 1 {
					commitFlushingToBadger(t, bs)
				}
			}
			defer func() { bs.historyScanTestHook = nil }()
			expectStampsB(t, k, bs, "committed in the build window", 50, 600, 650, true)
			expectStampsB(t, k, bs, "first row committed in the build window", 51, 300, 400, true)
			if fired == 0 {
				t.Fatal("the commit-window hook never fired inside the build")
			}
		})
	}
}

// Clear issued while a build sits between its scan and its install must not
// let the install resurrect the wiped rows (Clear waits for the build).
func TestHistoryStamps_ClearDuringBuild(t *testing.T) {
	for _, k := range stampKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			if err := k.put(bs, 80, 0, 100, 200, 0); err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			cleared := make(chan error, 1)
			fired := false
			bs.historyStampsBuildHook = func() {
				fired = true
				go func() { cleared <- bs.Clear() }()
				select {
				case err := <-cleared:
					cleared <- err
				case <-time.After(200 * time.Millisecond):
				}
			}
			if _, _, _, err := k.stamps(bs, 80); err != nil {
				t.Fatal(err)
			}
			bs.historyStampsBuildHook = nil
			if !fired {
				t.Fatal("the build hook never ran: the first call did not build")
			}
			if err := <-cleared; err != nil {
				t.Fatal(err)
			}
			expectStampsB(t, k, bs, "cleared while the build was in its window", 80, 0, 0, false)
		})
	}
}

// After Clear the sidecar answers for what the store holds then: an ID whose
// rows were wiped reads no history, a row written after the wipe reads alone.
func TestHistoryStamps_Clear(t *testing.T) {
	for _, k := range stampKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			if err := k.put(bs, 90, 0, 100, 900, 0); err != nil {
				t.Fatal(err)
			}
			expectStampsB(t, k, bs, "built", 90, 100, 900, true)
			if err := bs.Clear(); err != nil {
				t.Fatal(err)
			}
			if built, _ := k.built(bs.HistoryStampsStats()); built {
				t.Fatal("Clear left the sidecar built")
			}
			expectStampsB(t, k, bs, "cleared", 90, 0, 0, false)
			if err := k.put(bs, 90, 3, 5, 6, 0); err != nil {
				t.Fatal(err)
			}
			expectStampsB(t, k, bs, "after clear", 90, 5, 6, true)
		})
	}
}

// A store opened with HistoryPresenceProbeOnly (tiered cold shards) never
// builds the sidecar and computes per ID, with the same answers.
func TestHistoryStamps_ProbeOnly(t *testing.T) {
	for _, k := range stampKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, func(c *Config) { c.HistoryPresenceProbeOnly = true })
			if err := k.put(bs, 90, 0, 100, 0, 400); err != nil {
				t.Fatal(err)
			}
			expectStampsB(t, k, bs, "pending", 90, 100, 400, true)
			expectStampsB(t, k, bs, "plain", 91, 0, 0, false)
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := k.put(bs, 90, 0, 50, 60, 0); err != nil {
				t.Fatal(err)
			}
			expectStampsB(t, k, bs, "overwritten", 90, 50, 60, true)
			if err := k.trimFrom(bs, 90, 0); err != nil {
				t.Fatal(err)
			}
			expectStampsB(t, k, bs, "trimmed", 90, 0, 0, false)
			if built, _ := k.built(bs.HistoryStampsStats()); built {
				t.Fatal("probe-only store built the sidecar")
			}
		})
	}
}

// Errors match the history door: invalid IDs ErrInvalidStoreMutation, a nil
// store ErrNilStore, a closed store ErrStoreClosed; HistoryStampsStats is zero
// on a nil or closed store and reports the built sidecar otherwise.
func TestHistoryStamps_ErrorsAndStats(t *testing.T) {
	for _, k := range stampKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			for _, bad := range []int64{0, -1} {
				if _, _, _, err := k.stamps(bs, bad); !errors.Is(err, ErrInvalidStoreMutation) {
					t.Fatalf("stamps(%d) = %v, want ErrInvalidStoreMutation", bad, err)
				}
			}
			if built, n := k.built(bs.HistoryStampsStats()); built || n != 0 {
				t.Fatalf("fresh stats built=%v ids=%d", built, n)
			}
			for id := int64(1); id <= 3; id++ {
				if err := k.put(bs, id, 0, 10, 0, 0); err != nil {
					t.Fatal(err)
				}
			}
			expectStampsB(t, k, bs, "built", 2, 10, 0, true)
			if built, n := k.built(bs.HistoryStampsStats()); !built || n != 3 {
				t.Fatalf("stats built=%v ids=%d, want true 3", built, n)
			}
			var nilStore *Store
			if _, _, _, err := k.stamps(nilStore, 1); !errors.Is(err, ErrNilStore) {
				t.Fatalf("nil store = %v, want ErrNilStore", err)
			}
			if s := nilStore.HistoryStampsStats(); s != (HistoryStampsStats{}) {
				t.Fatalf("nil store stats = %+v", s)
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := k.stamps(bs, 1); !errors.Is(err, ErrStoreClosed) {
				t.Fatalf("closed = %v, want ErrStoreClosed", err)
			}
			if s := bs.HistoryStampsStats(); s != (HistoryStampsStats{}) {
				t.Fatalf("closed store stats = %+v", s)
			}
		})
	}
}

// Randomized store-level differential on an on-disk store: puts with random
// stamps at new and existing versions (overwrites), partial and full trims,
// truncations, flushes, parks into `flushing`, failed flushes, Clear and
// reopens; after every checked step stamps(id) == fold(history(id)) for every
// id. After a reopen or a Clear the next steps run unchecked, so several ops
// per ID sit in the buffer when the first call builds.
func TestHistoryStamps_RandomizedDifferential(t *testing.T) {
	for _, k := range stampKinds() {
		for seed := int64(1); seed <= 4; seed++ {
			t.Run(fmt.Sprintf("%s/seed=%d", k.name, seed), func(t *testing.T) {
				r := rand.New(rand.NewSource(seed)) // #nosec G404 -- deterministic test sequence
				dir := t.TempDir()
				open := func() *Store {
					bs, err := New(Config{Dir: dir, FlushInterval: time.Hour})
					if err != nil {
						t.Fatal(err)
					}
					return bs
				}
				bs := open()
				t.Cleanup(func() { _ = bs.Close() })
				const ids = 20
				ver := map[int64]uint32{}
				check := func(step string) {
					for id := int64(1); id <= ids; id++ {
						expectStampsAgree(t, k, bs, step, id)
					}
				}
				stamp := func() types.Instant { return types.Instant(1 + r.Intn(10000)) }
				unchecked := 0
				for step := 0; step < 300; step++ {
					id := int64(1 + r.Intn(ids))
					name := ""
					switch op := r.Intn(100); {
					case op < 30:
						name = "put"
						var da types.Instant
						if r.Intn(5) == 0 {
							da = stamp()
						}
						if err := k.put(bs, id, ver[id], stamp(), stamp()*types.Instant(r.Intn(2)), da); err != nil {
							t.Fatal(err)
						}
						ver[id]++
					case op < 40:
						name = "overwrite"
						if ver[id] > 0 {
							if err := k.put(bs, id, uint32(r.Intn(int(ver[id]))), stamp(), 0, 0); err != nil {
								t.Fatal(err)
							}
						}
					case op < 48:
						name = "trimSome"
						if ver[id] > 0 {
							if err := k.trimFrom(bs, id, uint32(r.Intn(int(ver[id])))); err != nil {
								t.Fatal(err)
							}
						}
					case op < 54:
						name = "trimAll"
						if err := k.trimFrom(bs, id, 0); err != nil {
							t.Fatal(err)
						}
					case op < 62:
						name = "truncateKeep"
						if err := k.truncate(bs, id, 1+r.Intn(2)); err != nil {
							t.Fatal(err)
						}
					case op < 72:
						name = "flush"
						if err := bs.Flush(); err != nil {
							t.Fatal(err)
						}
					case op < 80:
						name = "park"
						if pendingLen(bs) > 0 && flushingLen(bs) == 0 {
							parkPendingIntoFlushing(t, bs)
							if unchecked == 0 {
								check("parked")
							}
							requeueParked(bs)
						}
					case op < 85:
						name = "failedFlush"
						bs.FailNextFlushForTest(errors.New("injected"))
						_ = bs.Flush()
						bs.failNextFlush.Store(nil)
					case op < 90:
						name = "clear"
						if err := bs.Clear(); err != nil {
							t.Fatal(err)
						}
						ver = map[int64]uint32{}
						unchecked = 5
					default:
						name = "reopen"
						if err := bs.Close(); err != nil {
							t.Fatal(err)
						}
						bs = open()
						unchecked = 5
					}
					if unchecked > 0 {
						unchecked--
						continue
					}
					check(fmt.Sprintf("step %d %s", step, name))
				}
				check("final")
			})
		}
	}
}

// Writers append rows with rising stamps to their own IDs while the first
// calls build the sidecar; each writer reads its ID back right after its
// write returns (a read that starts after a write completed must see it), and
// readers of the seeded IDs see their seeded value. Settled, every ID equals
// its history fold. Run under -race.
func TestHistoryStamps_ConcurrentBuildWithWriters(t *testing.T) {
	for _, k := range stampKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			const seeded = 3000
			for id := int64(1); id <= seeded; id++ {
				if err := k.put(bs, id*2, 0, types.Instant(id), types.Instant(id+1), 0); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			const writers, perWriter = 4, 120
			var wg sync.WaitGroup
			errs := make(chan error, writers*perWriter+16)
			start := make(chan struct{})
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					<-start
					id := int64(1_000_000 + w)
					for i := 0; i < perWriter; i++ {
						tf := types.Instant(10_000 + i)
						if err := k.put(bs, id, uint32(i), tf, 0, 0); err != nil {
							errs <- err
							return
						}
						f, _, has, err := k.stamps(bs, id)
						if err != nil || !has || f < tf {
							errs <- fmt.Errorf("writer %d: stamps(%d) right after writing TxFrom %d = (%d, %v, %v)", w, id, tf, f, has, err)
							return
						}
						if i%40 == 0 {
							if err := bs.Flush(); err != nil {
								errs <- err
								return
							}
						}
					}
				}(w)
			}
			for rdr := 0; rdr < 4; rdr++ {
				wg.Add(1)
				go func(rdr int) {
					defer wg.Done()
					<-start
					for i := 0; i < 300; i++ {
						n := int64(1 + (i*7+rdr)%seeded)
						f, to, has, err := k.stamps(bs, 2*n)
						if err != nil || !has || f != types.Instant(n) || to != types.Instant(n+1) {
							errs <- fmt.Errorf("reader %d: seeded stamps(%d) = (%d, %d, %v, %v)", rdr, 2*n, f, to, has, err)
							return
						}
					}
				}(rdr)
			}
			close(start)
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatal(err)
			}
			for w := 0; w < writers; w++ {
				expectStampsB(t, k, bs, "settled writer id", int64(1_000_000+w), 10_000+perWriter-1, 0, true)
			}
			expectStampsB(t, k, bs, "settled plain id", 1, 0, 0, false)
		})
	}
}
