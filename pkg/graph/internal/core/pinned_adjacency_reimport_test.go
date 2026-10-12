package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 50: a relationship ID deleted and re-imported with other endpoints
// keeps its earlier life's rows (backlog 38), so the endpoints a relationship
// names are NOT fixed for its ID. Every adjacency door gathers candidates from
// the node's current adjacency plus the relationships that may have named the
// node before; it must still reach an earlier life through the endpoints that
// life named. The oracle resolves EVERY relationship ID of the world on its
// own, the way the door resolves one candidate, and keeps it when the node is
// its endpoint in the door's direction — so a door equals its oracle exactly
// when its candidate gather misses nothing (and adds nothing it keeps).

// paCase is one relationship's lives: per life the indexes of its start and
// end among the case's four nodes.
type paCase struct {
	name  string
	lives [][2]int
	del   bool // deleted after its last life
	moved bool // a life names another endpoint than an earlier one
}

var paCases = []paCase{
	{name: "other src", lives: [][2]int{{0, 1}, {2, 1}}, moved: true},
	{name: "other dst", lives: [][2]int{{0, 1}, {0, 2}}, moved: true},
	{name: "same endpoints", lives: [][2]int{{0, 1}, {0, 1}}},
	{name: "re-imported twice", lives: [][2]int{{0, 1}, {2, 1}, {2, 3}}, moved: true},
	{name: "deleted, never re-imported", lives: [][2]int{{0, 1}}, del: true},
	{name: "re-imported, deleted again", lives: [][2]int{{0, 1}, {2, 3}}, del: true, moved: true},
}

// paValidFrom is the valid start of life i; paValids probe inside each life.
func paValidFrom(life int) types.Instant { return types.Instant(1000 + 4000*life) }

var paValids = []types.Instant{3000, 7000, 11000}

type paWorld struct {
	cases [][]types.NodeID // the four nodes of each case
	nodes []types.NodeID
	rels  []types.RelID   // one per case
	pins  []types.Instant // after each life phase; the last after the final deletes
}

// paBuild writes every case: phase 0 adds each relationship, phase i deletes
// and re-imports it with its life i (valid from paValidFrom(i)), a pin after
// each phase; then the del cases are deleted and a last pin taken.
func paBuild(t *testing.T, g *Core) *paWorld {
	t.Helper()
	ctx := context.Background()
	w := &paWorld{rels: make([]types.RelID, len(paCases))}
	for range paCases {
		var ns []types.NodeID
		for i := 0; i < 4; i++ {
			n, err := g.Nodes.Add(ctx, []string{"A"}, map[string]any{"tkg_valid_from": types.Instant(1000)})
			if err != nil {
				t.Fatalf("add node: %v", err)
			}
			ns = append(ns, n.ID())
		}
		w.cases = append(w.cases, ns)
		w.nodes = append(w.nodes, ns...)
	}
	pin := func() {
		p, err := g.Temporal.NowTx()
		if err != nil {
			t.Fatalf("NowTx: %v", err)
		}
		w.pins = append(w.pins, p)
	}
	for life := 0; life < 3; life++ {
		for ci, pc := range paCases {
			if life >= len(pc.lives) {
				continue
			}
			s, e := w.cases[ci][pc.lives[life][0]], w.cases[ci][pc.lives[life][1]]
			props := map[string]any{"tkg_valid_from": paValidFrom(life)}
			if life == 0 {
				r, err := g.Rels.AddByID(ctx, "R", s, e, props)
				if err != nil {
					t.Fatalf("%s: add: %v", pc.name, err)
				}
				w.rels[ci] = r.ID()
				continue
			}
			if err := g.Rels.Delete(ctx, w.rels[ci]); err != nil {
				t.Fatalf("%s: delete before life %d: %v", pc.name, life, err)
			}
			sn, err := g.Nodes.Get(ctx, s)
			if err != nil {
				t.Fatal(err)
			}
			en, err := g.Nodes.Get(ctx, e)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels.Import(ctx, w.rels[ci], "R", sn, en, props); err != nil {
				t.Fatalf("%s: re-import life %d: %v", pc.name, life, err)
			}
		}
		pin()
	}
	for ci, pc := range paCases {
		if pc.del {
			if err := g.Rels.Delete(ctx, w.rels[ci]); err != nil {
				t.Fatalf("%s: final delete: %v", pc.name, err)
			}
		}
	}
	pin()
	return w
}

func paRow(r *types.Relationship) string {
	return fmt.Sprintf("%d:%d>%d@%d", r.ID(), r.StartNodeID(), r.EndNodeID(), r.Version())
}

func paEnd(r *types.Relationship, out bool) types.NodeID {
	if out {
		return r.StartNodeID()
	}
	return r.EndNodeID()
}

func paSorted(xs []string) string {
	slices.Sort(xs)
	return fmt.Sprint(xs)
}

// paResolved maps "no row" outcomes of a per-ID resolution to nil.
func paResolved(r *types.Relationship, err error) (*types.Relationship, error) {
	if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
		return nil, nil
	}
	return r, err
}

// paDoor is one adjacency door at one instant: got asks the door for node n,
// want builds the oracle for n from every relationship of the world.
type paDoor struct {
	name string
	got  func(n types.NodeID) ([]string, error)
	want func(n types.NodeID) ([]string, error)
}

// paDoors lists every adjacency door of g, in both directions, at every pin
// and valid instant of w.
func paDoors(g *Core, w *paWorld) []paDoor {
	var doors []paDoor
	// rowDoor: a door answering rows, resolved per candidate by resolve; the
	// oracle keeps a row whose endpoint in the direction is n.
	rowDoor := func(name string, out bool, door func(n types.NodeID) ([]*types.Relationship, error), resolve func(id types.RelID, n types.NodeID) (*types.Relationship, error)) paDoor {
		return paDoor{
			name: name,
			got: func(n types.NodeID) ([]string, error) {
				rows, err := door(n)
				var s []string
				for _, r := range rows {
					s = append(s, paRow(r))
				}
				return s, err
			},
			want: func(n types.NodeID) ([]string, error) {
				var s []string
				for _, id := range w.rels {
					var r *types.Relationship
					err := g.readUnderRLock(func() error {
						var err error
						r, err = paResolved(resolve(id, n))
						return err
					})
					if err != nil {
						return nil, err
					}
					if r != nil && paEnd(r, out) == n {
						s = append(s, paRow(r))
					}
				}
				return s, nil
			},
		}
	}
	for _, out := range []bool{true, false} {
		dir := map[bool]string{true: "out", false: "in"}[out]
		edgePred := func(n types.NodeID) func(*types.Relationship) bool {
			return func(r *types.Relationship) bool { return paEnd(r, out) == n }
		}
		for pi, pin := range w.pins {
			doors = append(doors,
				rowDoor(fmt.Sprintf("ForNodesAtPin/%s/pin%d", dir, pi), out, func(n types.NodeID) ([]*types.Relationship, error) {
					var m map[types.NodeID][]*types.Relationship
					var err error
					if out {
						m, err = g.Rels.OutgoingForNodesAtPin([]types.NodeID{n}, "", pin)
					} else {
						m, err = g.Rels.IncomingForNodesAtPin([]types.NodeID{n}, "", pin)
					}
					return m[n], err
				}, func(id types.RelID, _ types.NodeID) (*types.Relationship, error) {
					return g.findRelVersionForOpts(id, storepkg.QueryOpts{TxPin: pin}, nil)
				}),
				rowDoor(fmt.Sprintf("ForNodesAtTx/%s/pin%d", dir, pi), out, func(n types.NodeID) ([]*types.Relationship, error) {
					var m map[types.NodeID][]*types.Relationship
					var err error
					if out {
						m, err = g.Rels.OutgoingForNodesAtTx([]types.NodeID{n}, "", pin)
					} else {
						m, err = g.Rels.IncomingForNodesAtTx([]types.NodeID{n}, "", pin)
					}
					return m[n], err
				}, func(id types.RelID, _ types.NodeID) (*types.Relationship, error) {
					return g.findRelVersionForOpts(id, g.normalizeTxAtOnlyOpts(storepkg.QueryOpts{TxAt: pin}), nil)
				}),
				paDoor{
					name: fmt.Sprintf("ForEachRelEffectiveAtNodes/%s/pin%d", dir, pi),
					got: func(n types.NodeID) ([]string, error) {
						d := temporal.Incoming
						if out {
							d = temporal.Outgoing
						}
						var s []string
						err := g.Temporal.ForEachRelEffectiveAtNodes([]types.NodeID{n}, d, nil, pin, func(seg temporal.RelSegment) bool {
							s = append(s, fmt.Sprintf("%s[%d,%d)", paRow(seg.Rel), seg.ValidFrom, seg.ValidTo))
							return true
						})
						return s, err
					},
					want: func(n types.NodeID) ([]string, error) {
						var s []string
						for _, id := range w.rels {
							segs, err := g.Temporal.RelEffectiveTimeline(id, pin)
							if err != nil {
								return nil, err
							}
							for _, seg := range segs {
								if paEnd(seg.Rel, out) == n {
									s = append(s, fmt.Sprintf("%s[%d,%d)", paRow(seg.Rel), seg.ValidFrom, seg.ValidTo))
								}
							}
						}
						return s, nil
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
			doors = append(doors,
				rowDoor(fmt.Sprintf("ForEachAdjacentRelAt/%s/%+v", dir, opts), out, func(n types.NodeID) ([]*types.Relationship, error) {
					var rows []*types.Relationship
					err := g.Rels.ForEachAdjacentRelAt(n, "", !out, opts, func(r *types.Relationship) bool {
						rows = append(rows, r)
						return true
					})
					return rows, err
				}, func(id types.RelID, n types.NodeID) (*types.Relationship, error) {
					return g.findRelVersionForOpts(id, g.normalizeTxAtOnlyOpts(opts), edgePred(n))
				}),
				paDoor{
					// The endpoint door yields (id, far endpoint); the oracle row
					// is cut to the same pair.
					name: fmt.Sprintf("ForEachAdjacentEndpointAt/%s/%+v", dir, opts),
					got: func(n types.NodeID) ([]string, error) {
						var s []string
						err := g.Rels.ForEachAdjacentEndpointAt(n, "", !out, opts, func(id types.RelID, other types.NodeID) bool {
							s = append(s, fmt.Sprintf("%d>%d", id, other))
							return true
						})
						return s, err
					},
					want: func(n types.NodeID) ([]string, error) {
						var s []string
						for _, id := range w.rels {
							var r *types.Relationship
							if err := g.readUnderRLock(func() error {
								var err error
								r, err = paResolved(g.findRelVersionForOpts(id, g.normalizeTxAtOnlyOpts(opts), edgePred(n)))
								return err
							}); err != nil {
								return nil, err
							}
							if r != nil && paEnd(r, out) == n {
								s = append(s, fmt.Sprintf("%d>%d", id, paEnd(r, !out)))
							}
						}
						return s, nil
					},
				})
		}
		for vi, v := range paValids {
			doors = append(doors, rowDoor(fmt.Sprintf("RelsAt/%s/valid%d", dir, vi), out, func(n types.NodeID) ([]*types.Relationship, error) {
				if out {
					return g.Temporal.OutgoingRelsAt(n, v)
				}
				return g.Temporal.IncomingRelsAt(n, v)
			}, func(id types.RelID, _ types.NodeID) (*types.Relationship, error) {
				return g.relAtLocked(id, v) // every node of the world is valid at v
			}))
		}
	}
	for vi, v := range paValids {
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
			want: func(n types.NodeID) ([]string, error) {
				set := map[string]struct{}{}
				for _, id := range w.rels {
					var r *types.Relationship
					if err := g.readUnderRLock(func() error {
						var err error
						r, err = paResolved(g.relAtLocked(id, v))
						return err
					}); err != nil {
						return nil, err
					}
					switch {
					case r == nil:
					case r.StartNodeID() == n:
						set[fmt.Sprint(r.EndNodeID())] = struct{}{}
					case r.EndNodeID() == n:
						set[fmt.Sprint(r.StartNodeID())] = struct{}{}
					}
				}
				var s []string
				for k := range set {
					s = append(s, k)
				}
				return s, nil
			},
		})
	}
	return doors
}

// paCheck fails unless every door equals its oracle on every node of w, and the
// pinned door answers each case's life at each pin by construction (the oracle
// itself is not vacuous).
func paCheck(t *testing.T, g *Core, w *paWorld) {
	t.Helper()
	for ci, pc := range paCases {
		for pi, pin := range w.pins {
			life := min(pi, len(pc.lives)-1)
			gone := pi == len(w.pins)-1 && pc.del
			for k, n := range w.cases[ci] {
				out, err := g.Rels.OutgoingForNodesAtPin([]types.NodeID{n}, "", pin)
				if err != nil {
					t.Fatal(err)
				}
				in, err := g.Rels.IncomingForNodesAtPin([]types.NodeID{n}, "", pin)
				if err != nil {
					t.Fatal(err)
				}
				has := func(rows []*types.Relationship) bool {
					return slices.ContainsFunc(rows, func(r *types.Relationship) bool { return r.ID() == w.rels[ci] })
				}
				wantOut := !gone && k == pc.lives[life][0]
				wantIn := !gone && k == pc.lives[life][1]
				if has(out[n]) != wantOut || has(in[n]) != wantIn {
					t.Errorf("%s: pin%d node %d: outgoing has rel %v (want %v), incoming %v (want %v)", pc.name, pi, k, has(out[n]), wantOut, has(in[n]), wantIn)
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
			want, err := d.want(n)
			if err != nil {
				t.Fatalf("%s(%d) oracle: %v", d.name, n, err)
			}
			if g, w := paSorted(got), paSorted(want); g != w {
				t.Errorf("%s(%d) = %s\n want %s", d.name, n, g, w)
			}
		}
	}
}

// paNoted fails unless the store's re-imported set holds every live case
// whose lives name other endpoints and no case whose lives all name the same
// ones (a same-endpoint re-import is never noted: the live adjacency reaches
// every life). A moved case deleted again may be in it or not (the deleted
// IDs reach it): the primary noted it while live, an import of its deleted
// rows need not.
func paNoted(t *testing.T, g *Core, w *paWorld) {
	t.Helper()
	type reimported interface {
		ForEachReimportedRelID(fn func(types.RelID) bool) error
	}
	st, ok := g.store.(reimported)
	if !ok {
		t.Errorf("%T keeps no set of relationships re-imported with other endpoints", g.store)
		return
	}
	got := map[types.RelID]bool{}
	if err := st.ForEachReimportedRelID(func(id types.RelID) bool { got[id] = true; return true }); err != nil {
		t.Fatal(err)
	}
	for ci, pc := range paCases {
		in := got[w.rels[ci]]
		if pc.moved && !pc.del && !in {
			t.Errorf("%s: not in the re-imported set", pc.name)
		}
		if !pc.moved && in {
			t.Errorf("%s: in the re-imported set, but its lives name the same endpoints", pc.name)
		}
	}
}

// TestPinnedAdjacencyFindsReimportedEarlierLife: every adjacency door finds
// every life of a relationship re-imported with other endpoints, on every
// backend, on a replica fed by the change feed, after export → import and
// after a badger reopen; the store records only the moved relationships.
//
// Catches: the deleted-only candidate fold ("endpoints are immutable"), a
// re-import set kept in RAM only (reopen), a write door that does not note
// (replica apply, IO import), a set that notes same-endpoint re-imports.
func TestPinnedAdjacencyFindsReimportedEarlierLife(t *testing.T) {
	t.Parallel()
	for _, be := range pastDatedBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g := be.open(t, Config{SnowflakeNodeID: 0}, true)
			useTestClock(t, g)
			w := paBuild(t, g)
			t.Run("graph", func(t *testing.T) {
				paCheck(t, g, w)
				paNoted(t, g, w)
			})
			t.Run("replica", func(t *testing.T) {
				replica := be.open(t, Config{SnowflakeNodeID: 1, ReadOnlyReplica: true, ReplicationSource: g.Repl}, false)
				useTestClock(t, replica) // the pins of g lie ahead of a fresh wall clock
				if _, err := replica.Repl.ApplyChanges(changeFeed(t, g)); err != nil {
					t.Fatalf("ApplyChanges: %v", err)
				}
				paCheck(t, replica, w)
				paNoted(t, replica, w)
			})
			t.Run("export-import", func(t *testing.T) {
				var buf bytes.Buffer
				if err := g.IO.Export(&buf); err != nil {
					t.Fatalf("Export: %v", err)
				}
				dst := be.open(t, Config{AllowTxBackfill: true}, false)
				useTestClock(t, dst)
				if err := dst.IO.Import(&buf, tkgio.ImportOptions{}); err != nil {
					t.Fatalf("Import: %v", err)
				}
				paCheck(t, dst, w)
				paNoted(t, dst, w)
			})
		})
	}
	t.Run("badger reopen", func(t *testing.T) {
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
		g2, err := New(Config{BadgerDir: dir})
		if err != nil {
			t.Fatal(err)
		}
		defer g2.Close()
		useTestClock(t, g2)
		paCheck(t, g2, w)
		paNoted(t, g2, w)
	})
	t.Run("badger written before the set", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		bs, err := badger.New(badger.Config{Dir: dir})
		if err != nil {
			t.Fatal(err)
		}
		g1, err := New(Config{Store: &paLegacyStore{bs}})
		if err != nil {
			t.Fatal(err)
		}
		useTestClock(t, g1)
		w := paBuild(t, g1)
		if err := g1.Close(); err != nil {
			t.Fatal(err)
		}
		g2, err := New(Config{BadgerDir: dir}) // the open-time scan fills the set
		if err != nil {
			t.Fatal(err)
		}
		defer g2.Close()
		useTestClock(t, g2)
		paCheck(t, g2, w)
		paNoted(t, g2, w)
	})
}

// paLegacyStore is a badger store as a version before backlog 50 leaves it:
// no re-import is noted and the set is never marked complete.
type paLegacyStore struct{ *badger.Store }

func (*paLegacyStore) NoteReimportedRel(types.RelID) error { return nil }

func (s *paLegacyStore) MetaSet(key string, value []byte) error {
	if key == "rel_reimported_built" { // reimportedRelsBuiltMeta
		return nil
	}
	return s.Store.MetaSet(key, value)
}

// paNoSetStore is a store without the re-imported set; paSetNoMetaStore has
// the set but no MetaKV for the completeness marker; paMarkerFailStore cannot
// write the marker; paBadSetStore lists an invalid ID.
type paNoSetStore struct{ storepkg.MandatoryStore }

type paSetNoMetaStore struct {
	storepkg.MandatoryStore
	storepkg.ReimportedRelCapability
}

type paMarkerFailStore struct {
	storepkg.MandatoryStore
	storepkg.ReimportedRelCapability
	storepkg.MetaKVCapability
}

var errPaMarker = errors.New("marker write refused")

func (s *paMarkerFailStore) MetaSet(key string, value []byte) error {
	if key == "rel_reimported_built" {
		return errPaMarker
	}
	return s.MetaKVCapability.MetaSet(key, value)
}

type paBadSetStore struct{ paMarkerFailStore }

func (*paBadSetStore) MetaSet(string, []byte) error { return nil }

func (*paBadSetStore) ForEachReimportedRelID(fn func(types.RelID) bool) error {
	fn(0)
	return nil
}

// TestReimportedSetFallbacks: a graph whose store keeps no set, has no MetaKV
// for its marker, or cannot write the marker folds every relationship with
// history and still answers every adjacency door like the oracle; a store
// whose set lists an invalid ID fails the doors with ErrInvalidStoreMutation.
//
// Catches: a gather that trusts a missing or unmarked set (misses earlier
// lives), an untrusted store's IDs passed through unvalidated.
func TestReimportedSetFallbacks(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		st   func() storepkg.MandatoryStore
	}{
		{"no set", func() storepkg.MandatoryStore { return &paNoSetStore{memory.New()} }},
		{"set without MetaKV", func() storepkg.MandatoryStore { ms := memory.New(); return &paSetNoMetaStore{ms, ms} }},
		{"marker write fails", func() storepkg.MandatoryStore { ms := memory.New(); return &paMarkerFailStore{ms, ms, ms} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g, err := New(Config{Store: tc.st()})
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			if g.reimportedRels != nil {
				t.Fatalf("the graph trusts a set it cannot mark complete")
			}
			useTestClock(t, g)
			paCheck(t, g, paBuild(t, g))
		})
	}
	t.Run("invalid ID in the set", func(t *testing.T) {
		t.Parallel()
		ms := memory.New()
		g, err := New(Config{Store: &paBadSetStore{paMarkerFailStore{ms, ms, ms}}})
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()
		useTestClock(t, g)
		w := paBuild(t, g)
		n := w.cases[0][0]
		if _, err := g.Rels.OutgoingForNodesAtPin([]types.NodeID{n}, "", w.pins[0]); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("OutgoingForNodesAtPin = %v, want ErrInvalidStoreMutation", err)
		}
		if err := g.Rels.ForEachAdjacentRelAt(n, "", false, storepkg.QueryOpts{TxPin: w.pins[0]}, func(*types.Relationship) bool { return true }); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("ForEachAdjacentRelAt = %v, want ErrInvalidStoreMutation", err)
		}
		if err := g.Temporal.ForEachRelEffectiveAtNodes([]types.NodeID{n}, temporal.Outgoing, nil, w.pins[0], func(temporal.RelSegment) bool { return true }); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("ForEachRelEffectiveAtNodes = %v, want ErrInvalidStoreMutation", err)
		}
	})
}
