package graphstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math/big"
	"slices"
	"testing"
	"unsafe"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func (f *cpTreeFixture) seed(t *testing.T, entities []graphstate.EntityRecord, lives []graphstate.LifeRecord) {
	t.Helper()
	c := openCatalog(t, f.db, f.index, Limits{})
	s := stage(t, c)
	if err := s.Axis(t.Context(), f.axis); err != nil {
		t.Fatal(err)
	}
	for _, r := range entities {
		if err := s.Entity(t.Context(), refEntity(uint64(r.ID)), r); err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range lives {
		if err := s.Life(t.Context(), refLife(uint64(r.Owner), uint64(r.Life)), r); err != nil {
			t.Fatal(err)
		}
	}
	f.root, f.index = commitStage(t, f.db, f.root, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.view.Close(); err != nil {
		t.Fatal(err)
	}
}
func cpSeedRelationships(t *testing.T, f *cpTreeFixture) {
	t.Helper()
	records := []graphstate.EntityRecord{{ID: 1, Kind: graphstate.Node, Axis: f.axis}, {ID: 2, Kind: graphstate.Node, Axis: f.axis}}
	lives := []graphstate.LifeRecord{{Owner: 1, Life: 11}, {Owner: 1, Life: 12}, {Owner: 2, Life: 11}, {Owner: 2, Life: 12}}
	for _, r := range []struct {
		id, source, target uint64
		mode               graphstate.ReferenceMode
		typ                string
	}{{3, 1, 2, graphstate.LifeBound, "LINK"}, {4, 1, 1, graphstate.LifeBound, "LINK"}, {5, 1, 1, graphstate.IdentityReference, "REF"}, {6, 1, 2, graphstate.LifeBound, "LINK"}, {7, 2, 1, graphstate.LifeBound, "LINK"}} {
		records = append(records, graphstate.EntityRecord{ID: graphstate.EntityID(r.id), Kind: graphstate.Relationship, Axis: f.axis, Type: r.typ, Source: graphstate.EntityID(r.source), Target: graphstate.EntityID(r.target), Mode: r.mode})
		life := graphstate.LifeRecord{Owner: graphstate.EntityID(r.id), Life: 31}
		if r.mode == graphstate.LifeBound {
			life.SourceLife = 11
			life.TargetLife = 12
		}
		lives = append(lives, life)
	}
	f.seed(t, records, lives)
}
func cpFixtureRoleAtoms(t *testing.T, f *cpTreeFixture) []currentPresenceAtom {
	t.Helper()
	atoms := []currentPresenceAtom{}
	for _, r := range []struct {
		id, source, target uint64
		mode               graphstate.ReferenceMode
	}{{3, 1, 2, graphstate.LifeBound}, {4, 1, 1, graphstate.LifeBound}, {5, 1, 1, graphstate.IdentityReference}, {6, 1, 2, graphstate.LifeBound}, {7, 2, 1, graphstate.LifeBound}} {
		if r.mode == graphstate.IdentityReference {
			a := cpNativeAtom(t, f, 1, 0, r.id, 31, "0", cpSource|cpTarget)
			a.mode = r.mode
			atoms = append(atoms, a)
		} else {
			atoms = append(atoms, cpNativeAtom(t, f, r.source, 11, r.id, 31, "0", cpSource), cpNativeAtom(t, f, r.target, 12, r.id, 31, "0", cpTarget))
		}
	}
	cpSortAtoms(t, atoms)
	return atoms
}
func cpCollectCandidates(t *testing.T, it *currentPresenceIterator, rows int) []currentPresenceCandidate {
	t.Helper()
	var out []currentPresenceCandidate
	cursor := graphstate.Cursor(0)
	for range 100 {
		page, e := it.next(t.Context(), cursor, graphstate.ReadBudget{Rows: rows, Bytes: 4096})
		if e != nil {
			t.Fatal(e)
		}
		out = append(out, page.candidates...)
		if page.complete {
			return out
		}
		if page.next == 0 || page.next == cursor {
			t.Fatal("nonadvancing cursor")
		}
		cursor = page.next
	}
	t.Fatal("too many pages")
	return nil
}
func cpExactCandidates(t *testing.T, got, want []currentPresenceCandidate) {
	t.Helper()
	seen := map[[2]uint64]bool{}
	for _, r := range got {
		k := [2]uint64{uint64(r.relationship), uint64(r.life)}
		if seen[k] {
			t.Fatalf("duplicate qualified candidate %+v", r)
		}
		seen[k] = true
	}
	compare := func(a, b currentPresenceCandidate) int {
		if a.relationship < b.relationship {
			return -1
		}
		if a.relationship > b.relationship {
			return 1
		}
		if a.life < b.life {
			return -1
		}
		if a.life > b.life {
			return 1
		}
		return int(a.roles) - int(b.roles)
	}
	slices.SortFunc(got, compare)
	slices.SortFunc(want, compare)
	if !slices.Equal(got, want) {
		t.Fatalf("got%+v want%+v", got, want)
	}
}
func TestCurrentPresenceIteratorRolesTypesAndQualifiedLives(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		t.Run(cpProfileName(profile), func(t *testing.T) {
			f := cpFixture(t, profile)
			f.limits.codec.maxRows = 2
			f.limits.codec.maxChildren = 4
			cpSeedRelationships(t, f)
			f.commit(t, nil, cpFixtureRoleAtoms(t, f))
			c := openCatalog(t, f.db, f.index, Limits{})
			for _, tc := range []struct {
				name      string
				life      graphstate.LifeID
				mode      graphstate.ReferenceMode
				direction byte
				typ       string
				want      []currentPresenceCandidate
			}{
				{"all", 0, 0, cpSource | cpTarget, "", []currentPresenceCandidate{{3, 31, cpSource}, {4, 31, cpSource | cpTarget}, {5, 31, cpSource | cpTarget}, {6, 31, cpSource}, {7, 31, cpTarget}}},
				{"life12", 12, 0, cpSource | cpTarget, "", []currentPresenceCandidate{{4, 31, cpTarget}, {5, 31, cpSource | cpTarget}, {7, 31, cpTarget}}},
				{"out-life12", 12, 0, cpSource, "", []currentPresenceCandidate{{5, 31, cpSource}}},
				{"in-life11", 11, 0, cpTarget, "", []currentPresenceCandidate{{5, 31, cpTarget}}},
				{"life11", 11, graphstate.LifeBound, cpSource | cpTarget, "LINK", []currentPresenceCandidate{{3, 31, cpSource}, {4, 31, cpSource}, {6, 31, cpSource}}},
				{"reference", 0, graphstate.IdentityReference, cpTarget, "REF", []currentPresenceCandidate{{5, 31, cpTarget}}},
				{"phantom", 0, 0, cpSource | cpTarget, "missing", nil},
			} {
				t.Run(tc.name, func(t *testing.T) {
					query := currentPresenceQuery{endpoint: 1, life: tc.life, at: cpTestPosition(t, f.axis, "0", 0), mode: tc.mode, direction: tc.direction, typ: tc.typ}
					it, e := newCurrentPresenceIterator(t.Context(), c, f.tree, query, f.limits)
					if e != nil {
						t.Fatal(e)
					}
					got := cpCollectCandidates(t, it, 1)
					cpExactCandidates(t, got, tc.want)
					if e := it.close(); e != nil {
						t.Fatal(e)
					}
				})
			}
		})
	}
}
func TestCurrentPresenceIteratorOwnEndingsAndRetainedViews(t *testing.T) {
	f := cpFixture(t, temporal.ProfileIntegerZ)
	cpSeedRelationships(t, f)
	atoms := []currentPresenceAtom{cpNativeAtom(t, f, 1, 11, 3, 31, "0", cpSource), cpNativeAtom(t, f, 2, 12, 3, 31, "0", cpTarget)}
	for i := range atoms {
		atoms[i].flags &= ^cpPoint
		atoms[i].flags |= cpLowerClosed
		atoms[i].upper = cpTestBody(t, cpTestPosition(t, f.axis, "100", 0), temporal.Limits{})
	}
	f.commit(t, nil, atoms)
	oldTree, oldIndex := f.tree, f.index
	closed := slices.Clone(atoms)
	for i := range closed {
		closed[i].upper = cpTestBody(t, cpTestPosition(t, f.axis, "50", 0), temporal.Limits{})
	}
	f.commit(t, atoms, closed)
	query := currentPresenceQuery{endpoint: 1, at: cpTestPosition(t, f.axis, "75", 0), direction: cpSource | cpTarget}
	current, e := newCurrentPresenceIterator(t.Context(), openCatalog(t, f.db, f.index, Limits{}), f.tree, query, f.limits)
	if e != nil {
		t.Fatal(e)
	}
	cpExactCandidates(t, cpCollectCandidates(t, current, 8), nil)
	if current.op.counters.WitnessEntityCalls != 0 || current.op.counters.WitnessLifeCalls != 0 || current.op.counters.ResidualRecords != 0 {
		t.Fatal("ended payload metadata accessed", current.op.counters)
	}
	old, e := newCurrentPresenceIterator(t.Context(), openCatalog(t, f.db, oldIndex, Limits{}), oldTree, query, f.limits)
	if e != nil {
		t.Fatal(e)
	}
	cpExactCandidates(t, cpCollectCandidates(t, old, 8), []currentPresenceCandidate{{3, 31, cpSource}})
	if e := old.close(); e != nil {
		t.Fatal(e)
	}
	if e := current.close(); e != nil {
		t.Fatal(e)
	}
}
func TestCurrentPresenceIteratorCursorCancellationAndOwnership(t *testing.T) {
	f := cpFixture(t, temporal.ProfileIntegerZ)
	cpSeedRelationships(t, f)
	f.commit(t, nil, cpFixtureRoleAtoms(t, f))
	c := openCatalog(t, f.db, f.index, Limits{})
	query := currentPresenceQuery{endpoint: 1, at: cpTestPosition(t, f.axis, "0", 0), direction: cpSource | cpTarget}
	it, e := newCurrentPresenceIterator(t.Context(), c, f.tree, query, f.limits)
	if e != nil {
		t.Fatal(e)
	}
	for _, v := range []struct {
		size uintptr
		cap  int
	}{{unsafe.Sizeof(currentPresenceIterator{}), cpIteratorOwned}, {unsafe.Sizeof(cpIteratorFrame{}), cpIteratorFrameOwned}, {unsafe.Sizeof(currentPresenceCandidate{}), cpCandidateOwned}, {unsafe.Sizeof(currentPresenceCandidatePage{}), cpCandidatePageOwned}, {unsafe.Sizeof(cpPredicate{}), cpPredicateOwned}} {
		if int(v.size) > v.cap {
			t.Fatalf("size%d cap%d", v.size, v.cap)
		}
	}
	first, e := it.next(t.Context(), 0, graphstate.ReadBudget{Rows: 1, Bytes: 4096})
	if e != nil || first.complete || len(first.candidates) != 1 {
		t.Fatal(e)
	}
	oldCursor, oldOutput, oldRows := it.cursor, it.outputBytes, it.op.q.rows
	oldBytes := it.op.q.bytes
	cancelled := &cpCancelAfter{Context: t.Context(), after: 12}
	page, e := it.next(cancelled, first.next, graphstate.ReadBudget{Rows: 8, Bytes: 4096})
	if !errors.Is(e, context.Canceled) || len(page.candidates) != 0 || page.next != 0 || it.cursor != oldCursor || it.outputBytes != oldOutput || it.op.q.rows <= oldRows || it.op.q.bytes <= oldBytes {
		t.Fatal("partial cancelled publication", e)
	}
	rest := []currentPresenceCandidate{}
	cursor := first.next
	for range 100 {
		page, e := it.next(t.Context(), cursor, graphstate.ReadBudget{Rows: 8, Bytes: 4096})
		if e != nil {
			t.Fatal(e)
		}
		rest = append(rest, page.candidates...)
		if page.complete {
			break
		}
		cursor = page.next
	}
	cpExactCandidates(t, append(first.candidates, rest...), []currentPresenceCandidate{{3, 31, cpSource}, {4, 31, cpSource | cpTarget}, {5, 31, cpSource | cpTarget}, {6, 31, cpSource}, {7, 31, cpTarget}})
	other, e := newCurrentPresenceIterator(t.Context(), c, f.tree, query, f.limits)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := other.next(t.Context(), first.next, graphstate.ReadBudget{Rows: 1, Bytes: 4096}); !errors.Is(e, ErrInvalid) {
		t.Fatal("foreign cursor", e)
	}
	if e := it.close(); e != nil {
		t.Fatal(e)
	}
	if _, e := it.next(t.Context(), 0, graphstate.ReadBudget{Rows: 1, Bytes: 4096}); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
	if _, e := c.Root(); e != nil {
		t.Fatal("closed borrowed application view", e)
	}
}

func TestCurrentPresenceIteratorWideQuerySmallPageSourceBoundary(t *testing.T) {
	f := cpFixture(t, temporal.ProfileRationalQ)
	cpSeedRelationships(t, f)
	f.commit(t, nil, []currentPresenceAtom{cpNativeAtom(t, f, 1, 11, 3, 31, "0", cpSource)})
	wide := new(big.Int).Lsh(big.NewInt(1), 2047).String() + "/3"
	query := currentPresenceQuery{endpoint: 1, at: cpTestPosition(t, f.axis, wide, 0), direction: cpSource | cpTarget}
	c := openCatalog(t, f.db, f.index, Limits{})
	it, e := newCurrentPresenceIterator(t.Context(), c, f.tree, query, f.limits)
	if e != nil {
		t.Fatal(e)
	}
	initial := it.op.q.bytes
	scratch := 2048 + 64*(len(it.predicate.coordinate)+32)
	// Enough room for the small page decode/attempt tables, but not the much
	// wider query's comparison scratch. Refusal precedes its GCD/product work.
	it.op.q.maxBytes = initial + scratch - 1
	p, e := it.next(t.Context(), 0, graphstate.ReadBudget{Rows: 8, Bytes: 4096})
	if !errors.Is(e, ErrResourceLimit) || p.candidates != nil || p.next != 0 || p.complete || it.cursor != 0 || it.outputBytes != 0 {
		t.Fatal("wide comparison partially published", e)
	}
	if it.op.counters.LoadedPages != 1 || it.op.counters.WitnessEntityCalls != 0 || it.op.counters.WitnessLifeCalls != 0 || it.op.q.bytes <= initial {
		t.Fatal("did not reach comparison preflight", it.op.counters)
	}
	good, e := newCurrentPresenceIterator(t.Context(), c, f.tree, query, f.limits)
	if e != nil {
		t.Fatal(e)
	}
	page, e := good.next(t.Context(), 0, graphstate.ReadBudget{Rows: 8, Bytes: 4096})
	if e != nil || !page.complete || len(page.candidates) != 0 {
		t.Fatal(e)
	}
	if good.op.q.bytes-initial < scratch {
		t.Fatal("comparison scratch omitted from aggregate ledger")
	}
}

func TestCurrentPresenceIteratorMultipleAtomsOneLifeAndSparsePages(t *testing.T) {
	f := cpFixture(t, temporal.ProfileIntegerZ)
	f.limits.codec.maxRows = 2
	cpSeedRelationships(t, f)
	atoms := []currentPresenceAtom{}
	for _, at := range []string{"-4", "-2", "0", "2"} {
		atoms = append(atoms, cpNativeAtom(t, f, 1, 11, 3, 31, at, cpSource))
	}
	f.commit(t, nil, atoms)
	c := openCatalog(t, f.db, f.index, Limits{})
	it, e := newCurrentPresenceIterator(t.Context(), c, f.tree, currentPresenceQuery{endpoint: 1, at: cpTestPosition(t, f.axis, "0", 0), direction: cpSource}, f.limits)
	if e != nil {
		t.Fatal(e)
	}
	first, e := it.next(t.Context(), 0, graphstate.ReadBudget{Rows: 1, Bytes: 4096})
	if e != nil || first.complete || len(first.candidates) != 1 {
		t.Fatal("point0 omitted", e)
	}
	firstKey := exactCopy(it.last.lower)
	rest, e := it.next(t.Context(), first.next, graphstate.ReadBudget{Rows: 1, Bytes: 4096})
	if e != nil || !rest.complete || len(rest.candidates) != 0 || bytes.Equal(firstKey, it.last.lower) {
		t.Fatal("atom key cursor collapsed one life", e)
	}
	cpExactCandidates(t, first.candidates, []currentPresenceCandidate{{3, 31, cpSource}})
	phantom, e := newCurrentPresenceIterator(t.Context(), c, f.tree, currentPresenceQuery{endpoint: 1, at: cpTestPosition(t, f.axis, "1", 0), direction: cpSource}, f.limits)
	if e != nil {
		t.Fatal(e)
	}
	cpExactCandidates(t, cpCollectCandidates(t, phantom, 1), nil)
	if phantom.op.counters.WitnessEntityCalls != 0 {
		t.Fatal("gap decoded relationship payload")
	}
	// Residual type filtering yields advancing empty pages under small visited
	// caps, rather than reporting an incomplete scan as complete.
	wrong, e := newCurrentPresenceIterator(t.Context(), c, f.tree, currentPresenceQuery{endpoint: 1, at: cpTestPosition(t, f.axis, "0", 0), direction: cpSource, typ: "missing"}, f.limits)
	if e != nil {
		t.Fatal(e)
	}
	cpExactCandidates(t, cpCollectCandidates(t, wrong, 1), nil)
}
func TestCurrentPresenceIteratorEmptyDefaultsAndClosedBorrow(t *testing.T) {
	f := cpFixture(t, temporal.ProfileIntegerZ)
	cpSeedRelationships(t, f)
	f.commit(t, nil, nil)
	for _, pages := range []PageLimits{{}, {MaxWorkRecords: 32}} {
		limits := f.limits
		limits.pages = pages
		c := openCatalog(t, f.db, f.index, Limits{})
		it, e := newCurrentPresenceIterator(t.Context(), c, f.tree, currentPresenceQuery{endpoint: 1, at: cpTestPosition(t, f.axis, "0", 0), direction: cpSource}, limits)
		if e != nil {
			t.Fatal(e)
		}
		page, e := it.next(t.Context(), 0, graphstate.ReadBudget{Rows: 1, Bytes: cpCandidatePageOwned})
		if e != nil || !page.complete || len(page.candidates) != 0 || page.next != 0 {
			t.Fatal("empty tree", e)
		}
		if it.op.counters.LoadedPages != 1 || it.op.counters.NativeRows != 0 {
			t.Fatal(it.op.counters)
		}
		if e := it.close(); e != nil {
			t.Fatal(e)
		}
	}
	c := openCatalog(t, f.db, f.index, Limits{})
	it, e := newCurrentPresenceIterator(t.Context(), c, f.tree, currentPresenceQuery{endpoint: 1, at: cpTestPosition(t, f.axis, "0", 0), direction: cpSource}, f.limits)
	if e != nil {
		t.Fatal(e)
	}
	if e := c.view.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e := it.next(t.Context(), 0, graphstate.ReadBudget{Rows: 1, Bytes: 4096}); !errors.Is(e, raftlog.ErrClosed) {
		t.Fatal(e)
	}
	var none *currentPresenceIterator
	if e := none.close(); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if _, e := none.next(t.Context(), 0, graphstate.ReadBudget{Rows: 1, Bytes: 4096}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if _, e := newCurrentPresenceIterator(t.Context(), nil, f.tree, currentPresenceQuery{}, f.limits); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
}
func TestCurrentPresenceIteratorReopenAndCrossAxis(t *testing.T) {
	f := cpFixture(t, temporal.ProfileIntegerZ)
	cpSeedRelationships(t, f)
	f.commit(t, nil, cpFixtureRoleAtoms(t, f))
	other := testAxis(t, 2, temporal.ProfileRationalQ)
	c := openCatalog(t, f.db, f.index, Limits{})
	s := stage(t, c)
	if e := s.Axis(t.Context(), other); e != nil {
		t.Fatal(e)
	}
	f.root, f.index = commitStage(t, f.db, f.root, s)
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	if e := c.view.Close(); e != nil {
		t.Fatal(e)
	}
	if e := f.db.Close(); e != nil {
		t.Fatal(e)
	}
	db, e := raftlog.Open(raftlog.Config{Dir: "catalog", FS: f.fs, Application: raftlog.DefaultApplicationPolicy(1)})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := db.Close(); e != nil {
			t.Error(e)
		}
	})
	f.db = db
	c = openCatalog(t, db, f.index, Limits{})
	query := currentPresenceQuery{endpoint: 1, at: cpTestPosition(t, f.axis, "0", 0), direction: cpSource | cpTarget}
	it, e := newCurrentPresenceIterator(t.Context(), c, f.tree, query, f.limits)
	if e != nil {
		t.Fatal(e)
	}
	cpExactCandidates(t, cpCollectCandidates(t, it, 2), []currentPresenceCandidate{{3, 31, cpSource}, {4, 31, cpSource | cpTarget}, {5, 31, cpSource | cpTarget}, {6, 31, cpSource}, {7, 31, cpTarget}})
	query.at = cpTestPosition(t, other, "0", 0)
	cross, e := newCurrentPresenceIterator(t.Context(), c, f.tree, query, f.limits)
	if e != nil {
		t.Fatal(e)
	}
	cpExactCandidates(t, cpCollectCandidates(t, cross, 2), nil)
	if cross.op.counters.WitnessEntityCalls != 0 {
		t.Fatal("cross-axis relationship decoded")
	}
}
func TestCurrentPresenceIteratorStoredCorruptionAndWitnessFailures(t *testing.T) {
	for _, scenario := range []string{"digest", "count", "cycle", "role", "life"} {
		t.Run(scenario, func(t *testing.T) {
			f := cpFixture(t, temporal.ProfileIntegerZ)
			f.limits.codec.maxRows = 2
			cpSeedRelationships(t, f)
			atoms := cpFixtureRoleAtoms(t, f)
			if scenario == "role" {
				atoms[0].flags &^= cpSource
				atoms[0].flags |= cpTarget
			}
			if scenario == "life" {
				atoms[0].life = 99
			}
			f.commit(t, nil, atoms)
			tree := f.tree
			c := openCatalog(t, f.db, f.index, Limits{})
			if scenario == "digest" || scenario == "count" || scenario == "cycle" {
				q := cpReadOperation(t, c, f.limits)
				root, e := q.load(tree)
				if e != nil {
					t.Fatal(e)
				}
				wire := exactCopy(root.wire)
				if scenario == "digest" {
					wire[len(wire)-1] ^= 1
				} else {
					if root.level == 0 {
						t.Fatal("no internal root")
					}
					if scenario == "count" {
						root.children[0].count++
						tree.count++
					}
					wire, _, e = encodeCurrentPresencePage(t.Context(), testNamespace(), root, f.limits.codec)
					if e != nil {
						t.Fatal(e)
					}
					if scenario == "cycle" {
						offset := currentPresenceHeaderBytes + 49*len(root.axes)
						for i := 0; i < len(root.groups); {
							g := root.groups[i]
							end := i + 1
							for end < len(root.groups) && root.groups[end].endpoint == g.endpoint && root.groups[end].axis == g.axis && root.groups[end].mode == g.mode {
								end++
							}
							offset += 12
							for j := i; j < end; j++ {
								offset += 2
								if root.groups[j].mode == graphstate.LifeBound {
									offset += 8
								}
							}
							i = end
						}
						binary.BigEndian.PutUint64(wire[offset:offset+8], root.id)
					}
					tree.digest = currentPresenceDigest(wire)
				}
				f.root, f.index = commitRows(t, f.db, f.root, []raftlog.KV{{Key: physicalKey(testNamespace(), currentPresenceRecord, tree.id), Value: wire}})
				if e := c.view.Close(); e != nil {
					t.Fatal(e)
				}
				c = openCatalog(t, f.db, f.index, Limits{})
			}
			it, e := newCurrentPresenceIterator(t.Context(), c, tree, currentPresenceQuery{endpoint: 1, at: cpTestPosition(t, f.axis, "0", 0), direction: cpSource | cpTarget}, f.limits)
			if e != nil {
				t.Fatal(e)
			}
			page, e := it.next(t.Context(), 0, graphstate.ReadBudget{Rows: 8, Bytes: 4096})
			if !errors.Is(e, ErrCorrupt) || page.candidates != nil || page.next != 0 || page.complete || it.outputBytes != 0 || it.cursor != 0 {
				t.Fatal("corruption published partial candidates", e)
			}
			if _, e := c.Root(); !errors.Is(e, ErrPoisoned) {
				t.Fatal("corruption not stopped", e)
			}
		})
	}
}

func TestCurrentPresenceIteratorFiniteOpenAndInfiniteBounds(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		t.Run(cpProfileName(profile), func(t *testing.T) {
			f := cpFixture(t, profile)
			f.limits.codec.maxRows = 2
			cpSeedRelationships(t, f)
			left := cpNativeAtom(t, f, 1, 11, 3, 31, "0", cpSource)
			left.flags = cpSource | cpLowerInfinite | cpUpperClosed
			left.upper, left.lower = left.lower, nil
			middle := cpNativeAtom(t, f, 1, 11, 6, 31, "0", cpSource)
			middle.flags = cpSource | cpUpperClosed
			middle.upper = cpTestBody(t, cpTestPosition(t, f.axis, "10", 0), temporal.Limits{})
			right := cpNativeAtom(t, f, 1, 12, 7, 31, "10", cpTarget)
			right.flags = cpTarget | cpUpperInfinite
			if profile != temporal.ProfileRationalQ {
				// Discrete profiles use canonical closed-lower/open-upper rows.
				left.flags &^= cpUpperClosed
				middle.flags = cpSource | cpLowerClosed
				right.flags |= cpLowerClosed
			}
			atoms := []currentPresenceAtom{left, middle, right}
			cpSortAtoms(t, atoms)
			f.commit(t, nil, atoms)
			c := openCatalog(t, f.db, f.index, Limits{})
			cases := []struct {
				at   string
				want currentPresenceCandidate
			}{{"-1", currentPresenceCandidate{3, 31, cpSource}}, {"0", currentPresenceCandidate{3, 31, cpSource}}, {"1", currentPresenceCandidate{6, 31, cpSource}}, {"10", currentPresenceCandidate{6, 31, cpSource}}, {"11", currentPresenceCandidate{7, 31, cpTarget}}}
			if profile != temporal.ProfileRationalQ {
				cases[1].want = currentPresenceCandidate{6, 31, cpSource}
				cases[3].want = currentPresenceCandidate{7, 31, cpTarget}
			}
			for _, tc := range cases {
				t.Run(tc.at, func(t *testing.T) {
					query := currentPresenceQuery{endpoint: 1, at: cpTestPosition(t, f.axis, tc.at, 0), direction: cpSource | cpTarget}
					it, err := newCurrentPresenceIterator(t.Context(), c, f.tree, query, f.limits)
					if err != nil {
						t.Fatal(err)
					}
					cpExactCandidates(t, cpCollectCandidates(t, it, 1), []currentPresenceCandidate{tc.want})
					if err := it.close(); err != nil {
						t.Fatal(err)
					}
				})
			}
			// Tightening the upper boundary excludes the formerly covered query in
			// the new view; the retained old view still returns its old candidate.
			oldTree, oldIndex := f.tree, f.index
			opened := middle
			opened.flags &^= cpUpperClosed
			at := "10"
			if profile != temporal.ProfileRationalQ {
				opened.upper = cpTestBody(t, cpTestPosition(t, f.axis, "8", 0), temporal.Limits{})
				at = "9"
			}
			f.commit(t, []currentPresenceAtom{middle}, []currentPresenceAtom{opened})
			query := currentPresenceQuery{endpoint: 1, at: cpTestPosition(t, f.axis, at, 0), direction: cpSource | cpTarget}
			for _, tc := range []struct {
				index uint64
				tree  currentPresenceTreeRoot
				want  []currentPresenceCandidate
			}{{f.index, f.tree, nil}, {oldIndex, oldTree, []currentPresenceCandidate{{6, 31, cpSource}}}} {
				it, err := newCurrentPresenceIterator(t.Context(), openCatalog(t, f.db, tc.index, Limits{}), tc.tree, query, f.limits)
				if err != nil {
					t.Fatal(err)
				}
				cpExactCandidates(t, cpCollectCandidates(t, it, 1), tc.want)
				if err := it.close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestCurrentPresenceIteratorConstructorPolicies(t *testing.T) {
	f := cpFixture(t, temporal.ProfileIntegerZ)
	cpSeedRelationships(t, f)
	f.commit(t, nil, cpFixtureRoleAtoms(t, f))
	c := openCatalog(t, f.db, f.index, Limits{})
	base := currentPresenceQuery{endpoint: 1, at: cpTestPosition(t, f.axis, "0", 0), direction: cpSource}
	for _, tc := range []struct {
		name string
		edit func(*currentPresenceQuery, *currentPresenceTreeRoot, *currentPresenceTreeLimits)
		err  error
	}{
		{"zero-endpoint", func(q *currentPresenceQuery, _ *currentPresenceTreeRoot, _ *currentPresenceTreeLimits) {
			q.endpoint = 0
		}, ErrInvalid},
		{"unknown-mode", func(q *currentPresenceQuery, _ *currentPresenceTreeRoot, _ *currentPresenceTreeLimits) {
			q.mode = graphstate.ReferenceMode(99)
		}, ErrInvalid},
		{"zero-direction", func(q *currentPresenceQuery, _ *currentPresenceTreeRoot, _ *currentPresenceTreeLimits) {
			q.direction = 0
		}, ErrInvalid},
		{"unknown-direction", func(q *currentPresenceQuery, _ *currentPresenceTreeRoot, _ *currentPresenceTreeLimits) {
			q.direction = 128
		}, ErrInvalid},
		{"blank-type", func(q *currentPresenceQuery, _ *currentPresenceTreeRoot, _ *currentPresenceTreeLimits) { q.typ = " " }, ErrInvalid},
		{"missing-axis", func(q *currentPresenceQuery, _ *currentPresenceTreeRoot, _ *currentPresenceTreeLimits) {
			q.at = cpTestPosition(t, testAxis(t, 9, temporal.ProfileIntegerZ), "0", 0)
		}, temporal.ErrAxisMismatch},
		{"zero-tree", func(_ *currentPresenceQuery, tree *currentPresenceTreeRoot, _ *currentPresenceTreeLimits) {
			*tree = currentPresenceTreeRoot{}
		}, ErrInvalid},
		{"unreserved-tree", func(_ *currentPresenceQuery, tree *currentPresenceTreeRoot, _ *currentPresenceTreeLimits) {
			tree.id = f.root.next
		}, ErrInvalid},
		{"zero-digest", func(_ *currentPresenceQuery, tree *currentPresenceTreeRoot, _ *currentPresenceTreeLimits) {
			tree.digest = [32]byte{}
		}, ErrInvalid},
		{"invalid-codec", func(_ *currentPresenceQuery, _ *currentPresenceTreeRoot, l *currentPresenceTreeLimits) {
			l.codec.maxRows = 0
		}, ErrInvalid},
		{"invalid-pages", func(_ *currentPresenceQuery, _ *currentPresenceTreeRoot, l *currentPresenceTreeLimits) {
			l.pages.MaxWorkRecords = -1
		}, ErrInvalid},
		{"invalid-target", func(_ *currentPresenceQuery, _ *currentPresenceTreeRoot, l *currentPresenceTreeLimits) {
			l.targetBytes = 1
		}, ErrInvalid},
		{"cursor-retention", func(_ *currentPresenceQuery, _ *currentPresenceTreeRoot, l *currentPresenceTreeLimits) {
			l.pages.MaxCursorBytes = 256
		}, ErrResourceLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query, tree, limits := base, f.tree, f.limits
			tc.edit(&query, &tree, &limits)
			it, err := newCurrentPresenceIterator(t.Context(), c, tree, query, limits)
			if !errors.Is(err, tc.err) || it != nil {
				t.Fatal("constructor returned partial ownership", it, err)
			}
		})
	}
	for _, policy := range []temporal.Limits{{}, {MaxValueBytes: 4096, MaxInputBytes: 4096}} {
		limits := f.limits
		limits.codec.temporal = policy
		it, err := newCurrentPresenceIterator(t.Context(), c, f.tree, base, limits)
		if err != nil {
			t.Fatal("default or explicit temporal policies rejected", err)
		}
		cpExactCandidates(t, cpCollectCandidates(t, it, 1), []currentPresenceCandidate{{3, 31, cpSource}, {4, 31, cpSource}, {5, 31, cpSource}, {6, 31, cpSource}})
		if err := it.close(); err != nil {
			t.Fatal(err)
		}
	}
}
