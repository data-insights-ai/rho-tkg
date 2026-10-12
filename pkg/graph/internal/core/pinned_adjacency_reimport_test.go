package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Backlog 50: a relationship ID deleted and re-imported with other endpoints
// keeps its earlier life's rows (backlog 38), so the endpoints an ID names are
// not fixed. Every adjacency door must still reach an earlier life through the
// endpoints it named. The oracle resolves EVERY relationship of the world on
// its own, the way the door resolves one candidate, and keeps it when the node
// is its endpoint in the door's direction: a door equals its oracle exactly
// when its candidate gather misses nothing.

// paCases: per case one relationship's lives, as start/end indexes among the
// case's four nodes; del = deleted after its last life.
var paCases = []struct {
	name  string
	lives [][2]int
	del   bool
}{
	{"other src", [][2]int{{0, 1}, {2, 1}}, false},
	{"other dst", [][2]int{{0, 1}, {0, 2}}, false},
	{"same endpoints", [][2]int{{0, 1}, {0, 1}}, false},
	{"re-imported twice", [][2]int{{0, 1}, {2, 1}, {2, 3}}, false},
	{"deleted, never re-imported", [][2]int{{0, 1}}, true},
	{"re-imported, deleted again", [][2]int{{0, 1}, {2, 3}}, true},
}

// Life i is valid from 1000+4000i; paValids probe inside each life.
var paValids = []types.Instant{3000, 7000, 11000}

type paWorld struct {
	cases [][]types.NodeID // the four nodes of each case
	nodes []types.NodeID
	rels  []types.RelID   // one per case
	pins  []types.Instant // after each life phase; the last after the final deletes
}

// paBuild writes the cases: phase 0 adds each relationship, phase i deletes
// and re-imports it with life i, a pin after each phase; then the del cases
// are deleted and a last pin taken.
func paBuild(t *testing.T, g *Core) *paWorld {
	t.Helper()
	ctx := context.Background()
	w := &paWorld{rels: make([]types.RelID, len(paCases))}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	for range paCases {
		var ns []types.NodeID
		for i := 0; i < 4; i++ {
			// Nodes 2 and 3 are "Case" nodes: on tiered (RefLabels "Case")
			// they live on the reference shard, so a re-import onto them moves
			// the relationship to another shard as well as to other endpoints.
			label := map[bool]string{false: "A", true: "Case"}[i >= 2]
			n, err := g.Nodes.Add(ctx, []string{label}, map[string]any{"tkg_valid_from": types.Instant(1000)})
			must(err)
			ns = append(ns, n.ID())
		}
		w.cases = append(w.cases, ns)
		w.nodes = append(w.nodes, ns...)
	}
	pin := func() {
		p, err := g.Temporal.NowTx()
		must(err)
		w.pins = append(w.pins, p)
	}
	for life := 0; life < 3; life++ {
		for ci, pc := range paCases {
			if life >= len(pc.lives) {
				continue
			}
			s, e := w.cases[ci][pc.lives[life][0]], w.cases[ci][pc.lives[life][1]]
			props := map[string]any{"tkg_valid_from": types.Instant(1000 + 4000*life)}
			if life == 0 {
				r, err := g.Rels.AddByID(ctx, "R", s, e, props)
				must(err)
				w.rels[ci] = r.ID()
				continue
			}
			must(g.Rels.Delete(ctx, w.rels[ci]))
			sn, err := g.Nodes.Get(ctx, s)
			must(err)
			en, err := g.Nodes.Get(ctx, e)
			must(err)
			_, err = g.Rels.Import(ctx, w.rels[ci], "R", sn, en, props)
			must(err)
		}
		pin()
	}
	for ci, pc := range paCases {
		if pc.del {
			must(g.Rels.Delete(ctx, w.rels[ci]))
		}
	}
	pin()
	return w
}

func paRow(r *types.Relationship) string {
	return fmt.Sprintf("%d:%d>%d@%d", r.ID(), r.StartNodeID(), r.EndNodeID(), r.Version())
}

// paEnd is r's endpoint in the direction (out: start, in: end).
func paEnd(r *types.Relationship, out bool) types.NodeID {
	if out {
		return r.StartNodeID()
	}
	return r.EndNodeID()
}

// paDoor is one adjacency door in one direction at one instant: got asks the
// door for node n; one is the oracle's contribution of relationship id.
type paDoor struct {
	name string
	got  func(n types.NodeID) ([]string, error)
	one  func(id types.RelID, n types.NodeID) ([]string, error)
}

// paRowDoor: a door answering rows, each candidate resolved by resolve; the
// oracle keeps the resolved row when n is its endpoint in the direction.
func paRowDoor(g *Core, name string, out bool, rows func(n types.NodeID) ([]*types.Relationship, error), resolve func(id types.RelID, n types.NodeID) (*types.Relationship, error), show func(*types.Relationship) string) paDoor {
	return paDoor{
		name: name,
		got: func(n types.NodeID) ([]string, error) {
			rs, err := rows(n)
			var s []string
			for _, r := range rs {
				s = append(s, show(r))
			}
			return s, err
		},
		one: func(id types.RelID, n types.NodeID) ([]string, error) {
			var r *types.Relationship
			err := g.readUnderRLock(func() error {
				var err error
				r, err = resolve(id, n)
				if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
					r, err = nil, nil
				}
				return err
			})
			if err != nil || r == nil || paEnd(r, out) != n {
				return nil, err
			}
			return []string{show(r)}, nil
		},
	}
}

// paDoors lists every adjacency door of g in both directions at every pin and
// valid instant of w.
func paDoors(g *Core, w *paWorld) []paDoor {
	var doors []paDoor
	for _, out := range []bool{true, false} {
		dir, tdir := "out", temporal.Outgoing
		if !out {
			dir, tdir = "in", temporal.Incoming
		}
		other := func(r *types.Relationship) string { return fmt.Sprintf("%d>%d", r.ID(), paEnd(r, !out)) }
		edge := func(n types.NodeID) func(*types.Relationship) bool {
			return func(r *types.Relationship) bool { return paEnd(r, out) == n }
		}
		for pi, pin := range w.pins {
			doors = append(doors,
				paRowDoor(g, fmt.Sprintf("ForNodesAtPin/%s/pin%d", dir, pi), out, func(n types.NodeID) ([]*types.Relationship, error) {
					f := map[bool]func([]types.NodeID, string, types.Instant) (map[types.NodeID][]*types.Relationship, error){true: g.Rels.OutgoingForNodesAtPin, false: g.Rels.IncomingForNodesAtPin}[out]
					m, err := f([]types.NodeID{n}, "", pin)
					return m[n], err
				}, func(id types.RelID, _ types.NodeID) (*types.Relationship, error) {
					return g.findRelVersionForOpts(id, storepkg.QueryOpts{TxPin: pin}, nil)
				}, paRow),
				paRowDoor(g, fmt.Sprintf("ForNodesAtTx/%s/pin%d", dir, pi), out, func(n types.NodeID) ([]*types.Relationship, error) {
					f := map[bool]func([]types.NodeID, string, types.Instant) (map[types.NodeID][]*types.Relationship, error){true: g.Rels.OutgoingForNodesAtTx, false: g.Rels.IncomingForNodesAtTx}[out]
					m, err := f([]types.NodeID{n}, "", pin)
					return m[n], err
				}, func(id types.RelID, _ types.NodeID) (*types.Relationship, error) {
					return g.findRelVersionForOpts(id, g.normalizeTxAtOnlyOpts(storepkg.QueryOpts{TxAt: pin}), nil)
				}, paRow),
				paDoor{
					name: fmt.Sprintf("ForEachRelEffectiveAtNodes/%s/pin%d", dir, pi),
					got: func(n types.NodeID) ([]string, error) {
						var s []string
						err := g.Temporal.ForEachRelEffectiveAtNodes([]types.NodeID{n}, tdir, nil, pin, func(seg temporal.RelSegment) bool {
							s = append(s, fmt.Sprintf("%s[%d,%d)", paRow(seg.Rel), seg.ValidFrom, seg.ValidTo))
							return true
						})
						return s, err
					},
					one: func(id types.RelID, n types.NodeID) ([]string, error) {
						segs, err := g.Temporal.RelEffectiveTimeline(id, pin)
						var s []string
						for _, seg := range segs {
							if paEnd(seg.Rel, out) == n {
								s = append(s, fmt.Sprintf("%s[%d,%d)", paRow(seg.Rel), seg.ValidFrom, seg.ValidTo))
							}
						}
						return s, err
					},
				})
		}
		var optsList []storepkg.QueryOpts
		for _, pin := range w.pins {
			optsList = append(optsList, storepkg.QueryOpts{TxPin: pin})
		}
		for _, v := range paValids {
			optsList = append(optsList, storepkg.QueryOpts{ValidAt: v})
			for _, pin := range w.pins {
				optsList = append(optsList, storepkg.QueryOpts{ValidAt: v, TxAt: pin})
			}
		}
		for _, opts := range optsList {
			resolve := func(id types.RelID, n types.NodeID) (*types.Relationship, error) {
				return g.findRelVersionForOpts(id, g.normalizeTxAtOnlyOpts(opts), edge(n))
			}
			doors = append(doors,
				paRowDoor(g, fmt.Sprintf("ForEachAdjacentRelAt/%s/%+v", dir, opts), out, func(n types.NodeID) ([]*types.Relationship, error) {
					var rs []*types.Relationship
					err := g.Rels.ForEachAdjacentRelAt(n, "", !out, opts, func(r *types.Relationship) bool { rs = append(rs, r); return true })
					return rs, err
				}, resolve, paRow),
				paRowDoor(g, fmt.Sprintf("ForEachAdjacentEndpointAt/%s/%+v", dir, opts), out, func(n types.NodeID) ([]*types.Relationship, error) {
					// Rows rebuilt from the yielded (id, far endpoint) pair.
					var rs []*types.Relationship
					err := g.Rels.ForEachAdjacentEndpointAt(n, "", !out, opts, func(id types.RelID, o types.NodeID) bool {
						s, e := n, o
						if !out {
							s, e = o, n
						}
						rs = append(rs, types.NewRelationship(id, 0, s, e))
						return true
					})
					return rs, err
				}, resolve, other))
		}
		for vi, v := range paValids {
			doors = append(doors, paRowDoor(g, fmt.Sprintf("RelsAt/%s/valid%d", dir, vi), out, func(n types.NodeID) ([]*types.Relationship, error) {
				return map[bool]func(types.NodeID, types.Instant) ([]*types.Relationship, error){true: g.Temporal.OutgoingRelsAt, false: g.Temporal.IncomingRelsAt}[out](n, v)
			}, func(id types.RelID, _ types.NodeID) (*types.Relationship, error) {
				return g.relAtLocked(id, v) // every node of the world is valid at v
			}, paRow))
		}
	}
	for vi, v := range paValids {
		// NeighborsAt: both directions; the oracle names the far endpoint.
		doors = append(doors, paDoor{
			name: fmt.Sprintf("NeighborsAt/valid%d", vi),
			got: func(n types.NodeID) ([]string, error) {
				ns, err := g.Temporal.NeighborsAt(n, v)
				var s []string
				for _, x := range ns {
					s = append(s, fmt.Sprint(x.ID()))
				}
				return s, err
			},
			one: func(id types.RelID, n types.NodeID) ([]string, error) {
				var r *types.Relationship
				err := g.readUnderRLock(func() error {
					var err error
					if r, err = g.relAtLocked(id, v); errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
						r, err = nil, nil
					}
					return err
				})
				switch {
				case err != nil || r == nil:
					return nil, err
				case r.StartNodeID() == n:
					return []string{fmt.Sprint(r.EndNodeID())}, nil
				case r.EndNodeID() == n:
					return []string{fmt.Sprint(r.StartNodeID())}, nil
				}
				return nil, nil
			},
		})
	}
	return doors
}

// paCheck fails unless every door equals its oracle on every node of w, and
// the pinned door answers each case's life at each pin by construction (the
// oracle is not vacuous).
func paCheck(t *testing.T, g *Core, w *paWorld) {
	t.Helper()
	for ci, pc := range paCases {
		for pi, pin := range w.pins {
			life := pc.lives[min(pi, len(pc.lives)-1)]
			gone := pi == len(w.pins)-1 && pc.del
			for k, n := range w.cases[ci] {
				for side, f := range []func([]types.NodeID, string, types.Instant) (map[types.NodeID][]*types.Relationship, error){g.Rels.OutgoingForNodesAtPin, g.Rels.IncomingForNodesAtPin} {
					m, err := f([]types.NodeID{n}, "", pin)
					if err != nil {
						t.Fatal(err)
					}
					has := slices.ContainsFunc(m[n], func(r *types.Relationship) bool { return r.ID() == w.rels[ci] })
					if want := !gone && k == life[side]; has != want {
						t.Errorf("%s: pin%d node %d side %d: has rel %v, want %v", pc.name, pi, k, side, has, want)
					}
				}
			}
		}
	}
	for _, d := range paDoors(g, w) {
		for _, n := range w.nodes {
			got, err := d.got(n)
			if err != nil {
				t.Fatalf("%s(%d): %v", d.name, n, err)
			}
			var want []string
			for _, id := range w.rels {
				s, err := d.one(id, n)
				if err != nil {
					t.Fatalf("%s(%d) oracle: %v", d.name, n, err)
				}
				want = append(want, s...)
			}
			slices.Sort(got)
			slices.Sort(want)
			if !slices.Equal(slices.Compact(got), slices.Compact(want)) {
				t.Errorf("%s(%d) = %v\n want %v", d.name, n, got, want)
			}
		}
	}
}

// paStripExAdjacency makes the badger store at dir look written before backlog
// 50: no ex-adjacency keys (0x0D) and no completeness marker.
func paStripExAdjacency(t *testing.T, dir string) {
	t.Helper()
	bs, err := badger.New(badger.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	db := bs.DBForTest()
	if err := db.DropPrefix([]byte{0x0D}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(txn *badgerv4.Txn) error {
		for _, k := range []string{"rel_exadj_built", "rel_reimported_built"} {
			if err := txn.Delete(storeutil.MetaKey(k)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
}

// paCountStore counts the history reads made through it.
type paCountStore struct {
	*badger.Store
	reads atomic.Int64
}

func (s *paCountStore) GetRelHistory(id types.RelID) ([]*types.Relationship, error) {
	s.reads.Add(1)
	return s.Store.GetRelHistory(id)
}

func (s *paCountStore) ForEachRelHistoryID(fn func(types.RelID) bool) error {
	s.reads.Add(1)
	return s.Store.ForEachRelHistoryID(fn)
}

// TestPinnedAdjacencyFindsReimportedEarlierLife: every adjacency door finds
// every life of a relationship re-imported with other endpoints and of a
// deleted one, on every backend, on a replica fed by the change feed, after
// export → import, on a replica of a primary filled by IO.Import (history rows
// arrive after the current row), after a badger reopen, and on a badger store
// written before the ex-adjacency existed (read-only: no history scan at open,
// all-history fold; writable: the one-time backfill).
//
// Catches: the deleted-only fold ("endpoints are immutable"), ex-adjacency kept
// in RAM only (reopen), a write door that skips it (replica delete or history
// apply, IO.Import history rows), a missing or wasted backfill.
func TestPinnedAdjacencyFindsReimportedEarlierLife(t *testing.T) {
	t.Parallel()
	for _, be := range pastDatedBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g := be.open(t, Config{}, true)
			useTestClock(t, g)
			w := paBuild(t, g)
			replicaOf := func(t *testing.T, src *Core) *Core {
				r := be.open(t, Config{SnowflakeNodeID: 1, ReadOnlyReplica: true, ReplicationSource: src.Repl}, false)
				useTestClock(t, r) // the pins lie ahead of a fresh wall clock
				if _, err := r.Repl.ApplyChanges(changeFeed(t, src)); err != nil {
					t.Fatalf("ApplyChanges: %v", err)
				}
				return r
			}
			importOf := func(t *testing.T, src *Core, changeLog bool) *Core {
				var buf bytes.Buffer
				if err := src.IO.Export(&buf); err != nil {
					t.Fatal(err)
				}
				dst := be.open(t, Config{AllowTxBackfill: true}, changeLog)
				useTestClock(t, dst)
				if err := dst.IO.Import(&buf, tkgio.ImportOptions{}); err != nil {
					t.Fatal(err)
				}
				return dst
			}
			for _, v := range []struct {
				name string
				g    func(t *testing.T) *Core
			}{
				{"graph", func(*testing.T) *Core { return g }},
				{"replica", func(t *testing.T) *Core { return replicaOf(t, g) }},
				{"export-import", func(t *testing.T) *Core { return importOf(t, g, false) }},
				{"replica of an imported primary", func(t *testing.T) *Core { return replicaOf(t, importOf(t, g, true)) }},
			} {
				t.Run(v.name, func(t *testing.T) { paCheck(t, v.g(t), w) })
			}
		})
	}
	t.Run("badger on disk", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		g1, err := New(Config{BadgerDir: dir})
		if err != nil {
			t.Fatal(err)
		}
		useTestClock(t, g1)
		w := paBuild(t, g1)
		if err := g1.Close(); err != nil {
			t.Fatal(err)
		}
		open := func(t *testing.T, cfg badger.Config) (*Core, *paCountStore) {
			cfg.Dir = dir
			bs, err := badger.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			st := &paCountStore{Store: bs}
			g, err := New(Config{Store: st})
			if err != nil {
				t.Fatal(err)
			}
			useTestClock(t, g)
			return g, st
		}
		for _, v := range []struct {
			name           string
			strip          bool
			cfg            badger.Config
			maxOpenReads   int64
			openReadsFloor int64
		}{
			{name: "reopen"},
			{name: "written before, read-only", strip: true, cfg: badger.Config{ReadOnly: true}},
			{name: "written before, backfilled", strip: true, openReadsFloor: 1, maxOpenReads: 1 << 20},
			{name: "backfilled, reopened", maxOpenReads: 0},
		} {
			if v.strip {
				paStripExAdjacency(t, dir)
			}
			g, st := open(t, v.cfg)
			if n := st.reads.Load(); n > v.maxOpenReads || n < v.openReadsFloor {
				t.Errorf("%s: %d history reads at open, want [%d, %d]", v.name, n, v.openReadsFloor, v.maxOpenReads)
			}
			t.Run(v.name, func(t *testing.T) { paCheck(t, g, w) })
			if err := g.Close(); err != nil && !v.cfg.ReadOnly { // a read-only Close cannot persist the registries
				t.Fatal(err)
			}
		}
	})
}

// Stores the graph must not trust: no ex-adjacency, no MetaKV for its marker,
// a marker it cannot write; and one whose ex-adjacency lists an invalid ID.
type (
	paNoExStore     struct{ storepkg.MandatoryStore }
	paExNoMetaStore struct {
		storepkg.MandatoryStore
		storepkg.ExAdjacencyCapability
	}
	paExStore struct {
		storepkg.MandatoryStore
		storepkg.ExAdjacencyCapability
		storepkg.MetaKVCapability
		failMarker, badID bool
	}
)

func (s *paExStore) MetaSet(key string, value []byte) error {
	if s.failMarker && key == "rel_exadj_built" {
		return errors.New("marker write refused")
	}
	return s.MetaKVCapability.MetaSet(key, value)
}

func (s *paExStore) ForEachExAdjacentRelID(nid types.NodeID, incoming bool, fn func(types.RelID) bool) error {
	if s.badID {
		fn(0)
		return nil
	}
	return s.ExAdjacencyCapability.ForEachExAdjacentRelID(nid, incoming, fn)
}

// TestExAdjacencyFallbacks: a graph that cannot trust its store's
// ex-adjacency folds every relationship with history and still answers every
// door like the oracle; an invalid ID from the store fails the doors with
// ErrInvalidStoreMutation.
//
// Catches: a gather that trusts a missing or unmarked index (misses earlier
// lives), an untrusted store's ID passed through unvalidated.
func TestExAdjacencyFallbacks(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		st   func(ms *memory.Store) storepkg.MandatoryStore
	}{
		{"no ex-adjacency", func(ms *memory.Store) storepkg.MandatoryStore { return &paNoExStore{ms} }},
		{"no MetaKV", func(ms *memory.Store) storepkg.MandatoryStore { return &paExNoMetaStore{ms, ms} }},
		{"marker write fails", func(ms *memory.Store) storepkg.MandatoryStore { return &paExStore{ms, ms, ms, true, false} }},
		{"invalid ID", func(ms *memory.Store) storepkg.MandatoryStore { return &paExStore{ms, ms, ms, false, true} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g, err := New(Config{Store: tc.st(memory.New())})
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			useTestClock(t, g)
			w := paBuild(t, g)
			if tc.name != "invalid ID" {
				if g.exAdj != nil {
					t.Fatalf("the graph trusts an ex-adjacency it cannot mark complete")
				}
				paCheck(t, g, w)
				return
			}
			n := w.cases[0][0]
			if _, err := g.Rels.OutgoingForNodesAtPin([]types.NodeID{n}, "", w.pins[0]); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("OutgoingForNodesAtPin = %v, want ErrInvalidStoreMutation", err)
			}
			if err := g.Temporal.ForEachRelEffectiveAtNodes([]types.NodeID{n}, temporal.Outgoing, nil, w.pins[0], func(temporal.RelSegment) bool { return true }); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("ForEachRelEffectiveAtNodes = %v, want ErrInvalidStoreMutation", err)
			}
		})
	}
}
