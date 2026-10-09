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

// presenceKind adapts the node and relationship doors so every presence test
// runs on both (testing rule 2: node / relationship parity).
type presenceKind struct {
	name     string
	put      func(bs *Store, id int64, ver uint32) error
	trimFrom func(bs *Store, id int64, minVer uint32) error
	has      func(bs *Store, id int64) (bool, error)
	rows     func(bs *Store, id int64) (int, error)
	built    func(s HistoryPresenceStats) (bool, int)
}

func presenceKinds() []presenceKind {
	return []presenceKind{
		{
			name: "node",
			put: func(bs *Store, id int64, ver uint32) error {
				n := types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)
				n.SetVersion(ver)
				return bs.PutNodeVersion(n.ID(), ver, n)
			},
			trimFrom: func(bs *Store, id int64, minVer uint32) error {
				return bs.TrimNodeHistoryFrom(types.NodeID(snowflake.ID(id)), minVer)
			},
			has: func(bs *Store, id int64) (bool, error) { return bs.HasNodeHistory(types.NodeID(snowflake.ID(id))) },
			rows: func(bs *Store, id int64) (int, error) {
				h, err := bs.GetNodeHistory(types.NodeID(snowflake.ID(id)))
				return len(h), err
			},
			built: func(s HistoryPresenceStats) (bool, int) { return s.NodesBuilt, s.NodeIDs },
		},
		{
			name: "rel",
			put: func(bs *Store, id int64, ver uint32) error {
				r := types.NewRelationship(types.RelID(snowflake.ID(id)), 5, types.NodeID(1), types.NodeID(2))
				r.SetVersion(ver)
				return bs.PutRelVersion(r.ID(), ver, r)
			},
			trimFrom: func(bs *Store, id int64, minVer uint32) error {
				return bs.TrimRelHistoryFrom(types.RelID(snowflake.ID(id)), minVer)
			},
			has: func(bs *Store, id int64) (bool, error) { return bs.HasRelHistory(types.RelID(snowflake.ID(id))) },
			rows: func(bs *Store, id int64) (int, error) {
				h, err := bs.GetRelHistory(types.RelID(snowflake.ID(id)))
				return len(h), err
			},
			built: func(s HistoryPresenceStats) (bool, int) { return s.RelsBuilt, s.RelIDs },
		},
	}
}

// requeueParked merges a parked (never committed) `flushing` snapshot back into
// pending the way a failed flush requeues it, newer pending ops winning.
func requeueParked(bs *Store) {
	bs.wbMu.Lock()
	parked := bs.flushing
	bs.flushing = nil
	bs.wbMu.Unlock()
	bs.requeueOps(parked)
}

// expectPresence asserts has(id) == want AND that it agrees with the rows the
// history door returns (the contract), naming the step.
func expectPresence(t *testing.T, k presenceKind, bs *Store, step string, id int64, want bool) {
	t.Helper()
	got, err := k.has(bs, id)
	if err != nil {
		t.Fatalf("%s %s: has(%d): %v", k.name, step, id, err)
	}
	rows, err := k.rows(bs, id)
	if err != nil {
		t.Fatalf("%s %s: history(%d): %v", k.name, step, id, err)
	}
	if (rows > 0) != want {
		t.Fatalf("%s %s: test expectation wrong: history(%d) has %d rows, want presence %v", k.name, step, id, rows, want)
	}
	if got != want {
		t.Fatalf("%s %s: has(%d) = %v, history has %d rows", k.name, step, id, got, rows)
	}
}

// Invalid IDs fail like the history door (ErrInvalidStoreMutation), a nil store
// with ErrNilStore and a closed one with ErrStoreClosed: a door that answers
// false for id 0 or after Close fails here.
func TestHistoryPresence_ErrorsMatchHistoryDoor(t *testing.T) {
	for _, k := range presenceKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			for _, bad := range []int64{0, -1, -1 << 40} {
				if _, err := k.has(bs, bad); !errors.Is(err, ErrInvalidStoreMutation) {
					t.Fatalf("has(%d) = %v, want ErrInvalidStoreMutation", bad, err)
				}
				if _, err := k.rows(bs, bad); !errors.Is(err, ErrInvalidStoreMutation) {
					t.Fatalf("history(%d) = %v: the sibling door changed", bad, err)
				}
			}
			var nilStore *Store
			if _, err := k.has(nilStore, 1); !errors.Is(err, ErrNilStore) {
				t.Fatalf("nil store has = %v, want ErrNilStore", err)
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := k.has(bs, 1); !errors.Is(err, ErrStoreClosed) {
				t.Fatalf("closed has = %v, want ErrStoreClosed", err)
			}
		})
	}
}

// The first call builds the set while the history rows are still in the
// pending buffer, and again while they are parked in `flushing` (the commit
// window): a build that reads only committed badger keys answers false.
func TestHistoryPresence_BuildSeesPendingAndFlushing(t *testing.T) {
	for _, k := range presenceKinds() {
		t.Run(k.name+"/pending", func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			if err := k.put(bs, 10, 0); err != nil {
				t.Fatal(err)
			}
			expectPresence(t, k, bs, "pending, first call", 10, true)
			expectPresence(t, k, bs, "pending, neighbour", 11, false)
			expectPresence(t, k, bs, "pending, lower neighbour", 9, false)
		})
		t.Run(k.name+"/flushing", func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			if err := k.put(bs, 20, 0); err != nil {
				t.Fatal(err)
			}
			parkPendingIntoFlushing(t, bs)
			expectPresence(t, k, bs, "flushing, first call", 20, true)
			expectPresence(t, k, bs, "flushing, neighbour", 21, false)
		})
	}
}

// Every history-key delete is maintained: trimming the last rows of an entity
// (what a GraphTx rollback does) turns it false, trimming some keeps it true,
// in pending, in `flushing`, after a flush and after a reopen. A set that only
// grows answers true after the trim.
func TestHistoryPresence_DeleteMaintenance(t *testing.T) {
	for _, k := range presenceKinds() {
		t.Run(k.name, func(t *testing.T) {
			dir := t.TempDir()
			bs, err := New(Config{Dir: dir, FlushInterval: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			for _, ver := range []uint32{0, 1, 2} {
				if err := k.put(bs, 30, ver); err != nil {
					t.Fatal(err)
				}
			}
			if err := k.put(bs, 31, 0); err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			expectPresence(t, k, bs, "built", 30, true)
			// Partial trim: rows 1 and 2 go, row 0 stays (on disk).
			if err := k.trimFrom(bs, 30, 1); err != nil {
				t.Fatal(err)
			}
			expectPresence(t, k, bs, "partial trim pending", 30, true)
			parkPendingIntoFlushing(t, bs)
			expectPresence(t, k, bs, "partial trim flushing", 30, true)
			// Full trim with the earlier delete still parked.
			if err := k.trimFrom(bs, 30, 0); err != nil {
				t.Fatal(err)
			}
			expectPresence(t, k, bs, "full trim pending", 30, false)
			requeueParked(bs) // the parked snapshot was never committed: merge it back like a failed flush
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			expectPresence(t, k, bs, "full trim flushed", 30, false)
			expectPresence(t, k, bs, "bystander", 31, true)
			// Re-add after the full trim.
			if err := k.put(bs, 30, 5); err != nil {
				t.Fatal(err)
			}
			expectPresence(t, k, bs, "re-added", 30, true)
			if err := k.trimFrom(bs, 31, 0); err != nil {
				t.Fatal(err)
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			bs, err = New(Config{Dir: dir, FlushInterval: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = bs.Close() })
			expectPresence(t, k, bs, "reopened", 30, true)
			expectPresence(t, k, bs, "reopened, trimmed bystander", 31, false)
			expectPresence(t, k, bs, "reopened, never written", 32, false)
		})
	}
}

// The deleted row parked in `flushing` while a probe resolves an entity whose
// only other row is on disk: the probe must mask the disk row with the parked
// delete (false), and see a parked set (true).
func TestHistoryPresence_ProbeSeesFlushingDeletes(t *testing.T) {
	for _, k := range presenceKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			if err := k.put(bs, 40, 0); err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			expectPresence(t, k, bs, "built", 40, true)
			if err := k.trimFrom(bs, 40, 0); err != nil {
				t.Fatal(err)
			}
			parkPendingIntoFlushing(t, bs)
			expectPresence(t, k, bs, "delete parked", 40, false)
		})
	}
}

// A failed flush requeues the ops: the presence answer does not change.
func TestHistoryPresence_FailedFlushRequeue(t *testing.T) {
	for _, k := range presenceKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			expectPresence(t, k, bs, "empty, built", 50, false)
			if err := k.put(bs, 50, 0); err != nil {
				t.Fatal(err)
			}
			bs.FailNextFlushForTest(errors.New("injected"))
			if err := bs.Flush(); err == nil {
				t.Fatal("injected flush failure did not fail")
			}
			expectPresence(t, k, bs, "after failed flush", 50, true)
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			expectPresence(t, k, bs, "after retry", 50, true)
		})
	}
}

// Clear drops the set (a set that survives Clear answers true for wiped IDs);
// the next call rebuilds it and it keeps working.
func TestHistoryPresence_Clear(t *testing.T) {
	for _, k := range presenceKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			if err := k.put(bs, 60, 0); err != nil {
				t.Fatal(err)
			}
			if err := k.put(bs, 61, 0); err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := k.put(bs, 62, 0); err != nil { // still pending at Clear
				t.Fatal(err)
			}
			expectPresence(t, k, bs, "built", 60, true)
			if err := bs.Clear(); err != nil {
				t.Fatal(err)
			}
			if built, n := k.built(bs.HistoryPresenceStats()); built || n != 0 {
				t.Fatalf("stats right after clear: built=%v ids=%d, want the set dropped", built, n)
			}
			for _, id := range []int64{60, 61, 62} {
				expectPresence(t, k, bs, "cleared", id, false)
			}
			if err := k.put(bs, 61, 3); err != nil {
				t.Fatal(err)
			}
			expectPresence(t, k, bs, "written after clear", 61, true)
			if built, n := k.built(bs.HistoryPresenceStats()); !built || n != 1 {
				t.Fatalf("stats after clear + one write: built=%v ids=%d, want true 1", built, n)
			}
		})
	}
}

// A Clear whose keyspace drop fails leaves the committed rows in place; the
// presence set must not stay built and empty (it reset before the drop), or
// every entity whose rows survived reads false until it is written again.
func TestHistoryPresence_FailedClearRebuilds(t *testing.T) {
	for _, k := range presenceKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			if err := k.put(bs, 110, 0); err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			expectPresence(t, k, bs, "built", 110, true)
			bs.clearDropTestHook = func() error { return errors.New("injected drop failure") }
			if err := bs.Clear(); err == nil {
				t.Fatal("injected drop failure did not fail Clear")
			}
			bs.clearDropTestHook = nil
			expectPresence(t, k, bs, "rows survived the failed Clear", 110, true)
			expectPresence(t, k, bs, "never written", 111, false)
		})
	}
}

// Writes that land between the build's key scan and its install are not lost
// (lessons 63 / 74): the hook runs after the scan collected its IDs. In it, a
// scanned ID loses its only row, a fresh ID gains one, and a scanned ID with
// two rows loses one. A build that installs its scan over those notes (or that
// does not record notes while it scans) answers true / false / - wrongly.
func TestHistoryPresence_BuildGuardKeepsWritesDuringScan(t *testing.T) {
	for _, k := range presenceKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			if err := k.put(bs, 70, 0); err != nil {
				t.Fatal(err)
			}
			for _, ver := range []uint32{0, 1} {
				if err := k.put(bs, 72, ver); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			fired := 0
			bs.historyPresenceBuildHook = func() {
				fired++
				if fired > 1 {
					return
				}
				if err := k.trimFrom(bs, 70, 0); err != nil {
					t.Error(err)
				}
				if err := k.put(bs, 71, 0); err != nil {
					t.Error(err)
				}
				if err := k.trimFrom(bs, 72, 1); err != nil {
					t.Error(err)
				}
			}
			expectPresence(t, k, bs, "lost its only row during the scan", 70, false)
			expectPresence(t, k, bs, "gained a row during the scan", 71, true)
			expectPresence(t, k, bs, "lost one of two rows during the scan", 72, true)
			if fired == 0 {
				t.Fatal("the build hook never ran: the first call did not build")
			}
			bs.historyPresenceBuildHook = nil
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			expectPresence(t, k, bs, "flushed", 70, false)
		})
	}
}

// A history delete that lands while a probe resolves an unknown ID is not
// overwritten by the probe's older answer: the probe saw a row (true), the
// write in its window removed the last one, so the next call must read false.
// A probe that installs without its per-ID stamp check caches the stale true.
func TestHistoryPresence_ProbeKeepsWriteDuringProbe(t *testing.T) {
	for _, k := range presenceKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			for _, ver := range []uint32{0, 1} {
				if err := k.put(bs, 100, ver); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			expectPresence(t, k, bs, "built", 100, true)
			if err := k.trimFrom(bs, 100, 1); err != nil { // unknown: row 0 remains
				t.Fatal(err)
			}
			fired := 0
			bs.historyPresenceProbeHook = func() {
				fired++
				if fired == 1 {
					if err := k.trimFrom(bs, 100, 0); err != nil {
						t.Error(err)
					}
				}
			}
			if got, err := k.has(bs, 100); err != nil || !got {
				t.Fatalf("probe answer = %v, %v; want true (row 0 existed when it probed)", got, err)
			}
			if fired != 1 {
				t.Fatalf("probe hook fired %d times, want 1", fired)
			}
			expectPresence(t, k, bs, "after the delete in the probe window", 100, false)
		})
	}
}

// A Clear issued while a build sits between its scan and its install must not
// wipe the keyspace under the build: the build's scanned IDs would come back
// as phantoms after the wipe. The hook starts Clear concurrently and gives it
// up to 200 ms to finish; Clear waits for the build (presence buildMu), so
// with the exclusion it never finishes inside the window and the reset it
// does afterwards leaves the wiped ID false. Without the exclusion Clear
// finishes inside the window and the install resurrects the ID (red).
func TestHistoryPresence_ClearDuringBuild(t *testing.T) {
	for _, k := range presenceKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			if err := k.put(bs, 80, 0); err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			cleared := make(chan error, 1)
			bs.historyPresenceBuildHook = func() {
				go func() { cleared <- bs.Clear() }()
				select {
				case err := <-cleared:
					cleared <- err // Clear ran inside the build window
				case <-time.After(200 * time.Millisecond):
				}
			}
			if _, err := k.has(bs, 80); err != nil {
				t.Fatal(err)
			}
			bs.historyPresenceBuildHook = nil
			if err := <-cleared; err != nil {
				t.Fatal(err)
			}
			expectPresence(t, k, bs, "cleared while the build was in its window", 80, false)
		})
	}
}

// A store opened with HistoryPresenceProbeOnly (tiered cold shards) never
// builds the set and answers by a per-ID key probe, with the same answers.
func TestHistoryPresence_ProbeOnly(t *testing.T) {
	for _, k := range presenceKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, func(c *Config) { c.HistoryPresenceProbeOnly = true })
			if err := k.put(bs, 90, 0); err != nil {
				t.Fatal(err)
			}
			expectPresence(t, k, bs, "pending", 90, true)
			expectPresence(t, k, bs, "plain", 91, false)
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			expectPresence(t, k, bs, "flushed", 90, true)
			if err := k.trimFrom(bs, 90, 0); err != nil {
				t.Fatal(err)
			}
			expectPresence(t, k, bs, "trimmed", 90, false)
			if built, _ := k.built(bs.HistoryPresenceStats()); built {
				t.Fatal("probe-only store built the set")
			}
		})
	}
}

// Randomized store-level differential on an on-disk store: puts, partial and
// full trims, truncations that keep the newest rows, flushes, parks into
// `flushing`, failed flushes, Clear and reopens; after every checked step
// has(id) == len(history(id)) > 0 for every id, and the ID walk and the history
// count list exactly the ids with rows. After a reopen or a Clear the next
// steps run unchecked, so several ops per ID (SETs and DELETEs of different
// versions) sit in the buffer when the first call builds the set.
func TestHistoryPresence_RandomizedDifferential(t *testing.T) {
	for _, k := range idOverlayKinds() {
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
				const ids = 24
				ver := map[int64]uint32{}
				check := func(step string) {
					var want []int64
					for id := int64(1); id <= ids; id++ {
						got, err := k.has(bs, id)
						if err != nil {
							t.Fatalf("%s: has(%d): %v", step, id, err)
						}
						rows, err := k.rows(bs, id)
						if err != nil {
							t.Fatalf("%s: history(%d): %v", step, id, err)
						}
						if got != (rows > 0) {
							t.Fatalf("%s: has(%d) = %v, history has %d rows", step, id, got, rows)
						}
						if rows > 0 {
							want = append(want, id)
						}
					}
					listed, err := k.allIDs(bs)
					if err != nil {
						t.Fatal(err)
					}
					if fmt.Sprint(listed) != fmt.Sprint(want) {
						t.Fatalf("%s: AllHistoryIDs = %v, ids with rows %v", step, listed, want)
					}
					if n, err := k.count(bs); err != nil || n != len(want) {
						t.Fatalf("%s: HistoryCount = %d, %v; want %d", step, n, err, len(want))
					}
				}
				unchecked := 0
				for step := 0; step < 250; step++ {
					id := int64(1 + r.Intn(ids))
					name := ""
					switch op := r.Intn(100); {
					case op < 36:
						name = "put"
						if err := k.put(bs, id, ver[id]); err != nil {
							t.Fatal(err)
						}
						ver[id]++
					case op < 46:
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
					case op < 64:
						name = "truncateKeep"
						if err := k.truncate(bs, id, 1+r.Intn(2)); err != nil {
							t.Fatal(err)
						}
					case op < 74:
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
						bs.failNextFlush.Store(nil) // disarm when the buffer was empty
					case op < 90:
						name = "clear"
						if err := bs.Clear(); err != nil {
							t.Fatal(err)
						}
						ver = map[int64]uint32{}
						unchecked = 5
					case op < 95:
						name = "flush2"
						if err := bs.Flush(); err != nil {
							t.Fatal(err)
						}
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

// Writers add history for fresh IDs while the first calls build the set; each
// writer reads its own ID back right after its write returns (a read that
// starts after a write completed must see it), and when everything settles
// every written ID is true and every untouched ID false. Run under -race.
func TestHistoryPresence_ConcurrentBuildWithWriters(t *testing.T) {
	for _, k := range presenceKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			// A few thousand IDs with history so the build's scan takes long
			// enough for the writers to overlap it.
			const seeded = 3000
			for id := int64(1); id <= seeded; id++ {
				if err := k.put(bs, id*2, 0); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			const writers, perWriter = 4, 200
			var wg sync.WaitGroup
			errs := make(chan error, writers*perWriter+16)
			start := make(chan struct{})
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					<-start
					for i := 0; i < perWriter; i++ {
						id := int64(1_000_000 + w*perWriter + i)
						if err := k.put(bs, id, 0); err != nil {
							errs <- err
							return
						}
						got, err := k.has(bs, id)
						if err != nil || !got {
							errs <- fmt.Errorf("writer %d: has(%d) right after its write = %v, %v", w, id, got, err)
							return
						}
						if i%50 == 0 {
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
						id := int64(2 * (1 + (i*7+rdr)%seeded))
						got, err := k.has(bs, id)
						if err != nil || !got {
							errs <- fmt.Errorf("reader %d: seeded has(%d) = %v, %v", rdr, id, got, err)
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
				for i := 0; i < perWriter; i++ {
					expectPresence(t, k, bs, "settled writer id", int64(1_000_000+w*perWriter+i), true)
				}
			}
			for _, id := range []int64{1, 3, 999_999, 2_000_000} {
				expectPresence(t, k, bs, "settled plain id", id, false)
			}
			if built, n := k.built(bs.HistoryPresenceStats()); !built || n != seeded+writers*perWriter {
				t.Fatalf("stats: built=%v ids=%d, want true %d", built, n, seeded+writers*perWriter)
			}
		})
	}
}
