package badger

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// NodesAsOf / RelsAsOf answer "no history row for this entity" from the RAM
// history presence set instead of reading the key current+1 per entity
// (bulkPresence in badgerstore_history_presence.go). The set is live while the
// scan reads an older snapshot, so these tests pin both halves: the set is
// used (probe counts), and no state of the set changes an answer (the oracle
// is storeutil.SelectAsOfWithCurrent over the entity's own history rows and
// current row, read through doors that do not consult the set).

const bulkPresencePin = types.Instant(400)

// bulkPresenceKind adapts the node and relationship doors (testing rule 2).
type bulkPresenceKind struct {
	name string
	id   func(n int) int64 // n-th entity id of the kind (nodes even, rels odd)
	// put writes version ver (TxFrom txFrom, open valid interval) as the
	// current row (current) or a history row.
	put   func(t *testing.T, bs *Store, id int64, ver uint32, txFrom types.Instant, current bool)
	trim  func(bs *Store, id int64, minVer uint32) error
	bulk  func(bs *Store, pin types.Instant) (map[int64]uint32, error)
	want  func(bs *Store, id int64, pin types.Instant) (uint32, bool, error)
	built func(bs *Store) bool
}

func bulkPresenceKinds() []bulkPresenceKind {
	return []bulkPresenceKind{
		{
			name: "node",
			id:   func(n int) int64 { return 8_000_000 + int64(n)*2 },
			put: func(t *testing.T, bs *Store, id int64, ver uint32, txFrom types.Instant, current bool) {
				t.Helper()
				nid := types.NodeID(snowflake.ID(id))
				n := types.NewNode(nid, 1, nil)
				n.SetVersion(ver)
				n.SetTemporal(&types.TemporalMetadata{ValidFrom: 1, TxFrom: txFrom})
				var err error
				if current {
					err = bs.PutNode(n)
				} else {
					err = bs.PutNodeVersion(nid, ver, n)
				}
				if err != nil {
					t.Fatalf("put node %d v%d: %v", id, ver, err)
				}
			},
			trim: func(bs *Store, id int64, minVer uint32) error {
				return bs.TrimNodeHistoryFrom(types.NodeID(snowflake.ID(id)), minVer)
			},
			bulk: func(bs *Store, pin types.Instant) (map[int64]uint32, error) {
				got, err := bs.NodesAsOf(pin)
				m := make(map[int64]uint32, len(got))
				for _, n := range got {
					m[int64(n.ID().SnowflakeID())] = n.Version()
				}
				return m, err
			},
			want: func(bs *Store, id int64, pin types.Instant) (uint32, bool, error) {
				nid := types.NodeID(snowflake.ID(id))
				hist, err := bs.GetNodeHistory(nid)
				if err != nil {
					return 0, false, err
				}
				cur, cerr := bs.GetNode(nid)
				if cerr != nil && !errors.Is(cerr, ErrNodeNotFound) {
					return 0, false, cerr
				}
				v, ok := cascadeAsofWant(hist, cur, cerr == nil, pin)
				return v, ok, nil
			},
			built: func(bs *Store) bool { return bs.HistoryPresenceStats().NodesBuilt },
		},
		{
			name: "rel",
			id:   func(n int) int64 { return 8_000_001 + int64(n)*2 },
			put: func(t *testing.T, bs *Store, id int64, ver uint32, txFrom types.Instant, current bool) {
				t.Helper()
				rid := types.RelID(snowflake.ID(id))
				r := types.NewRelationship(rid, 1, types.NodeID(snowflake.ID(7)), types.NodeID(snowflake.ID(9)))
				r.SetVersion(ver)
				r.SetTemporal(&types.TemporalMetadata{ValidFrom: 1, TxFrom: txFrom})
				var err error
				if current {
					err = bs.PutRelationship(r)
				} else {
					err = bs.PutRelVersion(rid, ver, r)
				}
				if err != nil {
					t.Fatalf("put rel %d v%d: %v", id, ver, err)
				}
			},
			trim: func(bs *Store, id int64, minVer uint32) error {
				return bs.TrimRelHistoryFrom(types.RelID(snowflake.ID(id)), minVer)
			},
			bulk: func(bs *Store, pin types.Instant) (map[int64]uint32, error) {
				got, err := bs.RelsAsOf(pin)
				m := make(map[int64]uint32, len(got))
				for _, r := range got {
					m[int64(r.ID().SnowflakeID())] = r.Version()
				}
				return m, err
			},
			want: func(bs *Store, id int64, pin types.Instant) (uint32, bool, error) {
				rid := types.RelID(snowflake.ID(id))
				hist, err := bs.GetRelHistory(rid)
				if err != nil {
					return 0, false, err
				}
				cur, cerr := bs.GetRelationship(rid)
				if cerr != nil && !errors.Is(cerr, ErrRelNotFound) {
					return 0, false, cerr
				}
				v, ok := cascadeAsofWant(hist, cur, cerr == nil, pin)
				return v, ok, nil
			},
			built: func(bs *Store) bool { return bs.HistoryPresenceStats().RelsBuilt },
		},
	}
}

func putBulkPresenceEndpoints(t *testing.T, bs *Store) {
	t.Helper()
	for _, ep := range []int64{7, 9} {
		if err := bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(ep)), 1, nil)); err != nil {
			t.Fatalf("endpoint %d: %v", ep, err)
		}
	}
}

func openBulkPresenceStore(t *testing.T, dir string, probeOnly bool) *Store {
	t.Helper()
	bs, err := New(Config{Dir: dir, FlushInterval: time.Hour, HistoryPresenceProbeOnly: probeOnly})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return bs
}

// assertBulkMatchesOracle compares the bulk door with the oracle for ids at
// every pin.
func assertBulkMatchesOracle(t *testing.T, k bulkPresenceKind, bs *Store, ids []int64, stage string) {
	t.Helper()
	for _, pin := range []types.Instant{0, 1, 5, 12, 25, 50, 75, 100, 150, 200, 250, 300, 400, 10_000} {
		got, err := k.bulk(bs, pin)
		if err != nil {
			t.Fatalf("%s %s pin %d: bulk: %v", stage, k.name, pin, err)
		}
		for _, id := range ids {
			wantV, wantOK, err := k.want(bs, id, pin)
			if err != nil {
				t.Fatalf("%s %s %d pin %d: oracle: %v", stage, k.name, id, pin, err)
			}
			gotV, gotOK := got[id]
			if gotOK != wantOK || (gotOK && gotV != wantV) {
				t.Fatalf("%s %s %d pin %d: bulk (v%d, %v), SelectAsOf oracle (v%d, %v)", stage, k.name, id, pin, gotV, gotOK, wantV, wantOK)
			}
		}
	}
}

// TestBulkAsOfPresence_EquivalentAcrossStates drives randomized chains through
// every state the presence set can see - flushed, pending (unflushed rows
// above the current version), trimmed (an ID the set marks unknown), deleted,
// reopened (set rebuilt), cleared (set dropped and rebuilt) - and requires the
// bulk door to equal the oracle after each.
func TestBulkAsOfPresence_EquivalentAcrossStates(t *testing.T) {
	t.Parallel()
	for _, k := range bulkPresenceKinds() {
		t.Run(k.name, func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewPCG(0xB017, 0xA50F))
			dir := t.TempDir()
			bs := openBulkPresenceStore(t, dir, false)
			t.Cleanup(func() { _ = bs.Close() })
			putBulkPresenceEndpoints(t, bs)

			// populate writes entities from the cascade chain generator (rows
			// above a current or tombstone slot holder) and returns the ids.
			populate := func(base, n int) []int64 {
				ids := make([]int64, 0, n)
				for e := 0; e < n; e++ {
					id := k.id(base + e)
					ids = append(ids, id)
					for _, v := range genCascadeAsofChain(rng) {
						k.put(t, bs, id, v.version, v.txFrom, v.current)
					}
				}
				return ids
			}
			// Entities without any history row, and with exactly one.
			plain := func(base, n int) []int64 {
				ids := make([]int64, 0, n)
				for e := 0; e < n; e++ {
					id := k.id(base + e)
					ids = append(ids, id)
					k.put(t, bs, id, 0, types.Instant(10+e), true)
				}
				return ids
			}
			ids := append(populate(0, 80), plain(1000, 40)...)
			flush := func() {
				if err := bs.Flush(); err != nil {
					t.Fatalf("Flush: %v", err)
				}
			}
			check := func(stage string) {
				t.Helper()
				assertBulkMatchesOracle(t, k, bs, ids, stage)
				if !k.built(bs) {
					t.Fatalf("%s: the bulk door left the presence set unbuilt", stage)
				}
			}

			flush()
			check("flushed")

			// Pending: one more row above the highest version, unflushed, for a
			// third of the entities (plain ones included: they gain history).
			for i, id := range ids {
				if i%3 != 0 {
					continue
				}
				k.put(t, bs, id, nextVersion(t, k, bs, id), types.Instant(100+rng.IntN(100)), false)
			}
			check("pending")
			flush()
			check("pending flushed")

			// Trimmed: delete the top row of every fourth entity (its ID turns
			// unknown, rows may remain), then the top rows of its neighbour twice.
			for i, id := range ids {
				if i%4 != 0 {
					continue
				}
				top := nextVersion(t, k, bs, id)
				if top > 0 {
					if err := k.trim(bs, id, top-1); err != nil {
						t.Fatalf("trim %d: %v", id, err)
					}
				}
			}
			check("trimmed pending")
			flush()
			check("trimmed flushed")

			// Reopened: the set is rebuilt from the keys.
			if err := bs.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			bs = openBulkPresenceStore(t, dir, false)
			if k.built(bs) {
				t.Fatal("a reopened store starts with the presence set built")
			}
			check("reopened")

			// Cleared: the set is dropped with the keys and rebuilt.
			if err := bs.Clear(); err != nil {
				t.Fatalf("Clear: %v", err)
			}
			putBulkPresenceEndpoints(t, bs)
			ids = append(populate(5000, 30), plain(6000, 10)...)
			flush()
			check("cleared")
		})
	}
}

// nextVersion is the first version above every row of the entity.
func nextVersion(t *testing.T, k bulkPresenceKind, bs *Store, id int64) uint32 {
	t.Helper()
	var max uint32
	switch k.name {
	case "node":
		nid := types.NodeID(snowflake.ID(id))
		hist, err := bs.GetNodeHistory(nid)
		if err != nil {
			t.Fatalf("history %d: %v", id, err)
		}
		for _, n := range hist {
			max = maxU32(max, n.Version()+1)
		}
		if cur, err := bs.GetNode(nid); err == nil {
			max = maxU32(max, cur.Version()+1)
		}
	default:
		rid := types.RelID(snowflake.ID(id))
		hist, err := bs.GetRelHistory(rid)
		if err != nil {
			t.Fatalf("history %d: %v", id, err)
		}
		for _, r := range hist {
			max = maxU32(max, r.Version()+1)
		}
		if cur, err := bs.GetRelationship(rid); err == nil {
			max = maxU32(max, cur.Version()+1)
		}
	}
	return max
}

func maxU32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

// seedAboveCurrent writes, per kind, n entities each holding current version 1
// (TxFrom 200), history version 0 (TxFrom 100) and a row above the current
// version (version 2, TxFrom 300) that outranks it at bulkPresencePin.
func seedAboveCurrent(t *testing.T, k bulkPresenceKind, bs *Store, n int) []int64 {
	t.Helper()
	ids := make([]int64, 0, n)
	for e := 0; e < n; e++ {
		id := k.id(e)
		ids = append(ids, id)
		k.put(t, bs, id, 0, 100, false)
		k.put(t, bs, id, 1, 200, true)
		k.put(t, bs, id, 2, 300, false)
	}
	return ids
}

// TestBulkAsOfPresence_SkipsKeyProbeWithoutHistory is the feature test: an
// entity without a history row costs the bulk scan no key probe; entities with
// history still do. (Red before the presence check: every entity probed.)
func TestBulkAsOfPresence_SkipsKeyProbeWithoutHistory(t *testing.T) {
	t.Parallel()
	for _, k := range bulkPresenceKinds() {
		t.Run(k.name, func(t *testing.T) {
			t.Parallel()
			bs := openBulkPresenceStore(t, t.TempDir(), false)
			t.Cleanup(func() { _ = bs.Close() })
			putBulkPresenceEndpoints(t, bs)
			const withHistory, without = 7, 23
			withIDs := seedAboveCurrent(t, k, bs, withHistory)
			for e := 0; e < without; e++ {
				k.put(t, bs, k.id(100+e), 0, 100, true)
			}
			if err := bs.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			var probes atomic.Int64
			bs.bulkAsOfKeyProbeTestHook = func() { probes.Add(1) }
			got, err := k.bulk(bs, bulkPresencePin)
			if err != nil {
				t.Fatalf("bulk: %v", err)
			}
			bs.bulkAsOfKeyProbeTestHook = nil
			if probes.Load() < int64(withHistory) {
				t.Fatalf("%d probes for %d entities with history, want at least one each", probes.Load(), withHistory)
			}
			// Nodes: the two endpoint nodes are plain entities too (no history).
			if limit := int64(withHistory) * 2; probes.Load() > limit {
				t.Fatalf("%d key probes for %d entities with history and %d without: entities without history were probed", probes.Load(), withHistory, without)
			}
			for _, id := range withIDs {
				if got[id] != 2 {
					t.Fatalf("%s %d: bulk v%d, want the row above the current version (v2)", k.name, id, got[id])
				}
			}
			if len(got) < withHistory+without {
				t.Fatalf("bulk returned %d entities, want at least %d", len(got), withHistory+without)
			}
		})
	}
}

// TestBulkAsOfPresence_UnknownIDStillProbed: a history delete removes one row
// of an entity and leaves the one above the current version; the set marks the
// ID unknown and the scan must still read the key. A mutant that reads an
// unknown ID as "no history" answers the current row.
func TestBulkAsOfPresence_UnknownIDStillProbed(t *testing.T) {
	t.Parallel()
	for _, k := range bulkPresenceKinds() {
		t.Run(k.name, func(t *testing.T) {
			t.Parallel()
			bs := openBulkPresenceStore(t, t.TempDir(), false)
			t.Cleanup(func() { _ = bs.Close() })
			putBulkPresenceEndpoints(t, bs)
			ids := seedAboveCurrent(t, k, bs, 5)
			for _, id := range ids {
				k.put(t, bs, id, 3, 301, false) // the row the trim removes
			}
			if err := bs.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if _, err := k.bulk(bs, bulkPresencePin); err != nil { // builds the set
				t.Fatalf("bulk: %v", err)
			}
			for _, id := range ids {
				if err := k.trim(bs, id, 3); err != nil {
					t.Fatalf("trim %d: %v", id, err)
				}
			}
			for _, flushed := range []bool{false, true} {
				if flushed {
					if err := bs.Flush(); err != nil {
						t.Fatalf("Flush: %v", err)
					}
				}
				got, err := k.bulk(bs, bulkPresencePin)
				if err != nil {
					t.Fatalf("bulk: %v", err)
				}
				for _, id := range ids {
					wantV, wantOK, err := k.want(bs, id, bulkPresencePin)
					if err != nil || !wantOK || wantV != 2 {
						t.Fatalf("oracle %d: (v%d, %v, %v), scenario must select the row above the current version", id, wantV, wantOK, err)
					}
					if got[id] != 2 {
						t.Fatalf("%s %d (flushed=%v): bulk v%d, want v2: an unknown ID was read as having no history", k.name, id, flushed, got[id])
					}
				}
			}
		})
	}
}

// TestBulkAsOfPresence_DeleteMidScanKeepsSnapshot: a history delete that lands
// after the scan's snapshot (the trim doors take no idxMu) and is resolved by a
// HasHistory probe leaves the ID in neither set; the scan must keep answering
// from its snapshot, which still holds the rows. A mutant that ignores the
// delete counter reads the ID as "no history".
func TestBulkAsOfPresence_DeleteMidScanKeepsSnapshot(t *testing.T) {
	t.Parallel()
	for _, k := range bulkPresenceKinds() {
		t.Run(k.name, func(t *testing.T) {
			t.Parallel()
			bs := openBulkPresenceStore(t, t.TempDir(), false)
			t.Cleanup(func() { _ = bs.Close() })
			putBulkPresenceEndpoints(t, bs)
			ids := seedAboveCurrent(t, k, bs, 6)
			if err := bs.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if _, err := k.bulk(bs, bulkPresencePin); err != nil { // builds the set
				t.Fatalf("bulk: %v", err)
			}
			has := func(id int64) (bool, error) {
				if k.name == "node" {
					return bs.HasNodeHistory(types.NodeID(snowflake.ID(id)))
				}
				return bs.HasRelHistory(types.RelID(snowflake.ID(id)))
			}
			fired := false
			bs.bulkAsOfScanTestHook = func(idx int) {
				if idx != 0 || fired {
					return
				}
				fired = true
				for _, id := range ids {
					if err := k.trim(bs, id, 0); err != nil {
						t.Errorf("trim %d: %v", id, err)
					}
					if present, err := has(id); err != nil || present { // resolves the unknown mark
						t.Errorf("HasHistory(%d) after the trim = (%v, %v), want (false, nil)", id, present, err)
					}
				}
			}
			t.Cleanup(func() { bs.bulkAsOfScanTestHook = nil })
			got, err := k.bulk(bs, bulkPresencePin)
			if err != nil {
				t.Fatalf("bulk: %v", err)
			}
			if !fired {
				t.Fatal("the mid-scan hook never fired")
			}
			for _, id := range ids {
				if got[id] != 2 {
					t.Fatalf("%s %d: bulk v%d, want v2 from the scan-start snapshot", k.name, id, got[id])
				}
			}
			// The next scan sees the delete.
			bs.bulkAsOfScanTestHook = nil
			got, err = k.bulk(bs, bulkPresencePin)
			if err != nil {
				t.Fatalf("bulk after: %v", err)
			}
			for _, id := range ids {
				if got[id] != 1 {
					t.Fatalf("%s %d: bulk after the delete v%d, want the current row (v1)", k.name, id, got[id])
				}
			}
		})
	}
}

// TestBulkAsOfPresence_ProbeOnlyStillCorrect: a probe-only store never builds
// the set; the bulk door must not read the empty set as "no history".
func TestBulkAsOfPresence_ProbeOnlyStillCorrect(t *testing.T) {
	t.Parallel()
	for _, k := range bulkPresenceKinds() {
		t.Run(k.name, func(t *testing.T) {
			t.Parallel()
			bs := openBulkPresenceStore(t, t.TempDir(), true)
			t.Cleanup(func() { _ = bs.Close() })
			putBulkPresenceEndpoints(t, bs)
			ids := seedAboveCurrent(t, k, bs, 8)
			if err := bs.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			got, err := k.bulk(bs, bulkPresencePin)
			if err != nil {
				t.Fatalf("bulk: %v", err)
			}
			for _, id := range ids {
				if got[id] != 2 {
					t.Fatalf("%s %d: bulk v%d, want v2", k.name, id, got[id])
				}
			}
			if k.built(bs) {
				t.Fatal("a probe-only store built the presence set")
			}
		})
	}
}

// TestBulkAsOfPresence_WritersRace runs history writers and trims against
// concurrent bulk scans (-race) and requires the final state to equal the
// oracle. A scan that overlaps a writer may answer from either side of it;
// it must not fail, and a quiescent scan must be exact.
func TestBulkAsOfPresence_WritersRace(t *testing.T) {
	t.Parallel()
	for _, k := range bulkPresenceKinds() {
		t.Run(k.name, func(t *testing.T) {
			t.Parallel()
			bs := openBulkPresenceStore(t, t.TempDir(), false)
			t.Cleanup(func() { _ = bs.Close() })
			putBulkPresenceEndpoints(t, bs)
			const n = 40
			var ids []int64
			for e := 0; e < n; e++ {
				id := k.id(e)
				ids = append(ids, id)
				k.put(t, bs, id, 0, 100, true)
			}
			if err := bs.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if _, err := k.bulk(bs, bulkPresencePin); err != nil {
				t.Fatalf("bulk: %v", err)
			}

			stop := make(chan struct{})
			var wg sync.WaitGroup
			errc := make(chan error, 8)
			fail := func(err error) {
				select {
				case errc <- err:
				default:
				}
			}
			// Each writer owns the ids with index%2 == w and keeps their
			// versions dense above the current row (version 0): add the next
			// version, or trim the top one (the as-of fast paths assume dense
			// versions, version_alloc.go).
			for w := 0; w < 2; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					rng := rand.New(rand.NewPCG(uint64(w), 77))
					next := make(map[int64]uint32)
					for {
						select {
						case <-stop:
							return
						default:
						}
						i := 2*rng.IntN(len(ids)/2) + w
						id := ids[i]
						if next[id] == 0 {
							next[id] = 1
						}
						if next[id] > 1 && rng.IntN(3) == 0 {
							next[id]--
							if err := k.trim(bs, id, next[id]); err != nil {
								fail(fmt.Errorf("trim: %w", err))
							}
							continue
						}
						k.putNoFatal(bs, id, next[id], types.Instant(150+10*next[id]), fail)
						next[id]++
					}
				}(w)
			}
			var scans atomic.Int64
			for r := 0; r < 2; r++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case <-stop:
							return
						default:
						}
						if _, err := k.bulk(bs, bulkPresencePin); err != nil {
							fail(fmt.Errorf("bulk: %w", err))
							return
						}
						scans.Add(1)
					}
				}()
			}
			deadline := time.After(1500 * time.Millisecond)
			select {
			case err := <-errc:
				close(stop)
				wg.Wait()
				t.Fatal(err)
			case <-deadline:
			}
			close(stop)
			wg.Wait()
			select {
			case err := <-errc:
				t.Fatal(err)
			default:
			}
			if scans.Load() == 0 {
				t.Fatal("no scan completed")
			}
			if err := bs.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			assertBulkMatchesOracle(t, k, bs, ids, "after writers")
		})
	}
}

// putNoFatal is put for goroutines: errors go to fail instead of t.Fatalf.
func (k bulkPresenceKind) putNoFatal(bs *Store, id int64, ver uint32, txFrom types.Instant, fail func(error)) {
	var err error
	if k.name == "node" {
		nid := types.NodeID(snowflake.ID(id))
		n := types.NewNode(nid, 1, nil)
		n.SetVersion(ver)
		n.SetTemporal(&types.TemporalMetadata{ValidFrom: 1, TxFrom: txFrom})
		err = bs.PutNodeVersion(nid, ver, n)
	} else {
		rid := types.RelID(snowflake.ID(id))
		r := types.NewRelationship(rid, 1, types.NodeID(snowflake.ID(7)), types.NodeID(snowflake.ID(9)))
		r.SetVersion(ver)
		r.SetTemporal(&types.TemporalMetadata{ValidFrom: 1, TxFrom: txFrom})
		err = bs.PutRelVersion(rid, ver, r)
	}
	if err != nil {
		fail(fmt.Errorf("put %s %d v%d: %w", k.name, id, ver, err))
	}
}
