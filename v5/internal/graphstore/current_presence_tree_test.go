package graphstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"runtime"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type cpTreeFixture struct {
	db     *raftlog.Store
	root   Root
	index  uint64
	axis   temporal.Axis
	tree   currentPresenceTreeRoot
	limits currentPresenceTreeLimits
	fs     vfs.FS
}

func cpFixture(t *testing.T, profile temporal.Profile) *cpTreeFixture {
	t.Helper()
	fs := vfs.NewMem()
	db, r := newStore(t, fs)
	return &cpTreeFixture{db: db, root: r, index: 1, axis: testAxis(t, 1, profile), limits: defaultCurrentPresenceTreeLimits(), fs: fs}
}
func cpNativeAtom(t *testing.T, f *cpTreeFixture, endpoint, bound, rel, life uint64, at string, flags byte) currentPresenceAtom {
	t.Helper()
	a := f.axis
	d := a.Descriptor()
	return currentPresenceAtom{axis: currentPresenceAxis{d.ID, a.DefinitionHash(), d.Profile}, endpoint: graphstate.EntityID(endpoint), bound: graphstate.LifeID(bound), mode: graphstate.LifeBound, relationship: graphstate.EntityID(rel), life: graphstate.LifeID(life), flags: cpPoint | flags, lower: cpTestBody(t, cpTestPosition(t, a, at, 0), temporal.Limits{})}
}
func cpSortAtoms(t *testing.T, a []currentPresenceAtom) {
	t.Helper()
	slices.SortFunc(a, func(a, b currentPresenceAtom) int {
		c, e := cpAtomCompare(a, b, defaultCurrentPresenceLimits())
		if e != nil {
			t.Fatal(e)
		}
		return c
	})
}
func (f *cpTreeFixture) commit(t *testing.T, before, after []currentPresenceAtom) currentPresenceTreeWork {
	t.Helper()
	c := openCatalog(t, f.db, f.index, Limits{})
	s := stage(t, c)
	out, err := stageCurrentPresenceAtoms(t.Context(), s, f.root, f.tree, before, after, f.limits)
	if err != nil {
		t.Fatal(err)
	}
	f.root, f.index = commitStage(t, f.db, out.root, s)
	f.tree = out.tree
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.view.Close(); err != nil {
		t.Fatal(err)
	}
	return out.work
}
func cpReadOperation(t *testing.T, c *Catalog, l currentPresenceTreeLimits) *cpTreeOperation {
	t.Helper()
	r, e := c.reader(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	r.maxRows, r.maxBytes = l.pages.MaxWorkRecords, l.pages.MaxWorkBytes
	return &cpTreeOperation{pageStage: &pageStage{pageReader: &pageReader{q: r, limits: l.pages}, root: c.root}, limits: l, cache: make(map[uint64]currentPresencePage), dirty: make(map[uint64][]byte)}
}
func cpAllNativeAtoms(t *testing.T, c *Catalog, tree currentPresenceTreeRoot, l currentPresenceTreeLimits) []currentPresenceAtom {
	t.Helper()
	q := cpReadOperation(t, c, l)
	root, e := q.load(tree)
	if e != nil {
		t.Fatal(e)
	}
	out := []currentPresenceAtom{}
	var walk func(currentPresencePage)
	walk = func(n currentPresencePage) {
		if n.level == 0 {
			for _, r := range n.rows {
				a, e := cpAtomFromRow(n, r, l.codec)
				if e != nil {
					t.Fatal(e)
				}
				out = append(out, a)
			}
			return
		}
		for i := range n.children {
			ch, e := q.child(n, i)
			if e != nil {
				t.Fatal(e)
			}
			if ch.level+1 != n.level {
				t.Fatal("unequal heights")
			}
			walk(ch)
		}
	}
	walk(root)
	if uint64(len(out)) != tree.count {
		t.Fatal("count mismatch")
	}
	return out
}
func cpExactAtoms(t *testing.T, got, want []currentPresenceAtom) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("atoms%d want%d", len(got), len(want))
	}
	for i := range want {
		if !cpAtomEqual(got[i], want[i]) {
			t.Fatalf("atom%d rel%d/life%d wrong", i, got[i].relationship, got[i].life)
		}
	}
}
func TestCurrentPresenceTreeSplitsDeletesAndOldRoots(t *testing.T) {
	f := cpFixture(t, temporal.ProfileIntegerZ)
	f.limits.codec.maxRows = 4
	f.limits.codec.maxChildren = 4
	f.commit(t, nil, nil)
	empty, emptyIndex := f.tree, f.index
	all := make([]currentPresenceAtom, 0, 72)
	for i := range 36 {
		for _, end := range []uint64{1, 2} {
			role := cpSource
			if end == 2 {
				role = cpTarget
			}
			all = append(all, cpNativeAtom(t, f, end, 11, uint64(i+3), 31, fmt.Sprint(i*2), role))
		}
	}
	cpSortAtoms(t, all)
	f.commit(t, nil, all)
	populated, populatedIndex := f.tree, f.index
	if f.tree.level < 2 {
		t.Fatal("not multi-level")
	}
	cpExactAtoms(t, cpAllNativeAtoms(t, openCatalog(t, f.db, f.index, Limits{}), f.tree, f.limits), all)
	cpExactAtoms(t, cpAllNativeAtoms(t, openCatalog(t, f.db, emptyIndex, Limits{}), empty, f.limits), nil)
	removed := slices.Clone(all[:len(all)-5])
	f.commit(t, removed, nil)
	remaining := all[len(all)-5:]
	cpExactAtoms(t, cpAllNativeAtoms(t, openCatalog(t, f.db, f.index, Limits{}), f.tree, f.limits), remaining)
	cpExactAtoms(t, cpAllNativeAtoms(t, openCatalog(t, f.db, populatedIndex, Limits{}), populated, f.limits), all)
	f.commit(t, remaining, nil)
	if f.tree.count != 0 || f.tree.level != 0 {
		t.Fatal("missing real empty root", f.tree)
	}
	cpExactAtoms(t, cpAllNativeAtoms(t, openCatalog(t, f.db, f.index, Limits{}), f.tree, f.limits), nil)
	f.commit(t, nil, all[:2])
	cpExactAtoms(t, cpAllNativeAtoms(t, openCatalog(t, f.db, f.index, Limits{}), f.tree, f.limits), all[:2])
}
func TestCurrentPresenceTreeCheckedEditsAndAtomicRefusals(t *testing.T) {
	f := cpFixture(t, temporal.ProfileIntegerZ)
	f.commit(t, nil, nil)
	old := cpNativeAtom(t, f, 1, 11, 3, 31, "0", cpSource)
	f.commit(t, nil, []currentPresenceAtom{old})
	c := openCatalog(t, f.db, f.index, Limits{})
	s := stage(t, c)
	if err := s.Axis(t.Context(), f.axis); err != nil {
		t.Fatal(err)
	}
	prior, err := s.Writes()
	if err != nil {
		t.Fatal(err)
	}
	priorRoot, priorTree := f.root, f.tree
	assertFailure := func(ctx context.Context, before, after []currentPresenceAtom, l currentPresenceTreeLimits, sentinel error) {
		t.Helper()
		out, e := stageCurrentPresenceAtoms(ctx, s, f.root, f.tree, before, after, l)
		if !errors.Is(e, sentinel) || out != (stagedCurrentPresence{}) {
			t.Fatalf("refusal %+v %v", out, e)
		}
		now, e := s.Writes()
		if e != nil || !sameWrites(prior, now) || f.root != priorRoot || f.tree != priorTree {
			t.Fatal("partial staging", e)
		}
	}
	wrong := old
	wrong.flags = cpPoint | cpTarget
	assertFailure(t.Context(), []currentPresenceAtom{wrong}, nil, f.limits, ErrPatchConflict)
	fresh := cpNativeAtom(t, f, 2, 11, 4, 31, "0", cpTarget)
	assertFailure(t.Context(), nil, []currentPresenceAtom{fresh, old}, f.limits, ErrInvalid)
	invalid := old
	invalid.upper = []byte{0, 0, 0}
	assertFailure(t.Context(), nil, []currentPresenceAtom{invalid}, f.limits, ErrInvalid)
	tight := f.limits
	tight.pages.MaxWorkBytes = 512
	tight.pages.MaxCheckpointBytes = 128
	assertFailure(t.Context(), nil, nil, tight, ErrResourceLimit)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	assertFailure(ctx, nil, nil, f.limits, context.Canceled)
	tight = f.limits
	tight.maxEdits = 1
	assertFailure(t.Context(), []currentPresenceAtom{old}, []currentPresenceAtom{fresh}, tight, ErrResourceLimit)
	// The collection is completely validated before a missing before image can
	// hide an invalid later atom or reserve physical pages.
	missing := old
	missing.relationship = 99
	assertFailure(t.Context(), []currentPresenceAtom{missing}, []currentPresenceAtom{invalid}, f.limits, ErrInvalid)
	var nilCtx context.Context
	if _, e := stageCurrentPresenceAtoms(nilCtx, s, f.root, f.tree, nil, nil, f.limits); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if _, e := stageCurrentPresenceAtoms(t.Context(), nil, f.root, f.tree, nil, nil, f.limits); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
}
func TestCurrentPresenceTreeOneAtomUpdateCounters(t *testing.T) {
	for _, mode := range []string{"same-key-upper", "moved-lower"} {
		t.Run(mode, func(t *testing.T) {
			f := cpFixture(t, temporal.ProfileIntegerZ)
			f.limits.codec.maxRows = 8
			f.limits.codec.maxChildren = 4
			atoms := []currentPresenceAtom{}
			for i := range 40 {
				for _, endpoint := range []uint64{1, 2} {
					role, bound := cpSource, uint64(11)
					if endpoint == 2 {
						role, bound = cpTarget, 12
					}
					a := cpNativeAtom(t, f, endpoint, bound, uint64(i+3), 31, fmt.Sprint(i), role)
					a.flags &^= cpPoint
					a.flags |= cpLowerClosed
					a.upper = cpTestBody(t, cpTestPosition(t, f.axis, fmt.Sprint(i+4), 0), temporal.Limits{})
					atoms = append(atoms, a)
				}
			}
			cpSortAtoms(t, atoms)
			f.commit(t, nil, atoms)
			before := []currentPresenceAtom{atoms[5], atoms[45]}
			after := slices.Clone(before)
			for i := range after {
				if mode == "same-key-upper" {
					after[i].upper = cpTestBody(t, cpTestPosition(t, f.axis, "7", 0), temporal.Limits{})
				} else {
					after[i].lower = cpTestBody(t, cpTestPosition(t, f.axis, "-5", 0), temporal.Limits{})
				}
			}
			cpSortAtoms(t, before)
			cpSortAtoms(t, after)
			height := int(f.tree.level) + 1
			work := f.commit(t, before, after)
			if mode == "same-key-upper" {
				if work.Splits != 0 || work.Rebalances != 0 || work.LoadedPages > 2*height-1 || work.FinalPageWrites > 2*height-1 {
					t.Fatalf("same-key paths %+v h%d", work, height)
				}
			} else {
				// Four old/new endpoint paths can participate; rebalance is measured under
				// finite fanout/height and the shared source cap, never called two paths.
				bound := 4 * height * (f.limits.codec.maxChildren + 1)
				if work.LoadedPages > bound || work.Rebalances > bound || work.FinalPageWrites > bound {
					t.Fatalf("moved-key work %+v bound%d", work, bound)
				}
			}
			if work.LogicalRewriteBytes > work.EncodedPages*f.limits.codec.maxPageBytes || work.Records < work.LoadedPages {
				t.Fatal(work)
			}
			t.Logf("ONE support atom/two roles %s height%d actual %+v", mode, height, work)
			for i := range atoms {
				for _, old := range before {
					if cpAtomEqual(atoms[i], old) {
						for _, fresh := range after {
							if atoms[i].endpoint == fresh.endpoint {
								atoms[i] = fresh
							}
						}
					}
				}
			}
			cpSortAtoms(t, atoms)
			cpExactAtoms(t, cpAllNativeAtoms(t, openCatalog(t, f.db, f.index, Limits{}), f.tree, f.limits), atoms)
		})
	}
}
func TestCurrentPresenceTreePhysicalOverflowAndLedgerSizes(t *testing.T) {
	for _, v := range []struct {
		size uintptr
		cap  int
	}{{unsafe.Sizeof(currentPresenceAtom{}), cpAtomOwned}, {unsafe.Sizeof(cpReference{}), cpReferenceOwned}, {unsafe.Sizeof(stagedCurrentPresence{}), cpTreeResultOwned}, {unsafe.Sizeof(cpTreeOperation{}), cpTreeOperationOwned}} {
		if int(v.size) > v.cap {
			t.Fatalf("size%d cap%d", v.size, v.cap)
		}
	}
	f := cpFixture(t, temporal.ProfileIntegerZ)
	c := openCatalog(t, f.db, f.index, Limits{})
	s := stage(t, c)
	r := f.root
	r.next = math.MaxUint64
	out, e := stageCurrentPresenceAtoms(t.Context(), s, r, currentPresenceTreeRoot{}, nil, nil, f.limits)
	if !errors.Is(e, ErrResourceLimit) || out != (stagedCurrentPresence{}) {
		t.Fatal(e)
	}
	tree := currentPresenceTreeRoot{count: math.MaxUint64}
	out, e = stageCurrentPresenceAtoms(t.Context(), s, f.root, tree, nil, []currentPresenceAtom{cpNativeAtom(t, f, 1, 11, 3, 31, "0", cpSource)}, f.limits)
	if !errors.Is(e, ErrResourceLimit) || out != (stagedCurrentPresence{}) {
		t.Fatal(e)
	}
	writes, e := s.Writes()
	if e != nil || len(writes) != 0 {
		t.Fatal("overflow staged writes", e)
	}
}

func TestCurrentPresenceTreeThreeChildJumboAndHardCapRefusal(t *testing.T) {
	f := cpFixture(t, temporal.ProfileIntegerZ)
	f.limits.targetBytes = 200
	f.limits.codec.maxPageBytes = 4096
	number := new(big.Int).Lsh(big.NewInt(1), 2047)
	atoms := []currentPresenceAtom{}
	for i := range 3 {
		value := new(big.Int).Add(number, big.NewInt(int64(i)))
		atoms = append(atoms, cpNativeAtom(t, f, 1, 11, uint64(i+3), 31, value.String(), cpSource))
	}
	cpSortAtoms(t, atoms)
	work := f.commit(t, nil, atoms)
	if f.tree.level != 1 || f.tree.count != 3 {
		t.Fatal("three-child jumbo did not succeed", f.tree)
	}
	q := cpReadOperation(t, openCatalog(t, f.db, f.index, Limits{}), f.limits)
	root, e := q.load(f.tree)
	if e != nil || len(root.children) != 3 || len(root.wire) <= f.limits.targetBytes || len(root.wire) > f.limits.codec.maxPageBytes {
		t.Fatal("jumbo shape", e)
	}
	cpExactAtoms(t, cpAllNativeAtoms(t, openCatalog(t, f.db, f.index, Limits{}), f.tree, f.limits), atoms)
	t.Logf("three wide points: %+v", work)
	// Same legal atoms under a smaller hard page cap must fail atomically rather
	// than changing numeric width, target policy or authenticated fence shape.
	other := cpFixture(t, temporal.ProfileIntegerZ)
	other.limits = f.limits
	other.limits.codec.maxPageBytes = 2500
	c := openCatalog(t, other.db, other.index, Limits{})
	s := stage(t, c)
	if e := s.Axis(t.Context(), other.axis); e != nil {
		t.Fatal(e)
	}
	prior, e := s.Writes()
	if e != nil {
		t.Fatal(e)
	}
	out, e := stageCurrentPresenceAtoms(t.Context(), s, other.root, other.tree, nil, atoms, other.limits)
	if !errors.Is(e, ErrResourceLimit) || out != (stagedCurrentPresence{}) {
		t.Fatal("hard cap silently enlarged", e)
	}
	now, e := s.Writes()
	if e != nil || !sameWrites(prior, now) || other.root.next != 1 || other.tree != (currentPresenceTreeRoot{}) {
		t.Fatal("hard-cap partial effects", e)
	}
}
func TestCurrentPresenceTreePageDefaultsAndPartialOverrides(t *testing.T) {
	for _, pages := range []PageLimits{{}, {MaxWorkRecords: 32}} {
		f := cpFixture(t, temporal.ProfileIntegerZ)
		f.limits.pages = pages
		f.commit(t, nil, nil)
		a := cpNativeAtom(t, f, 1, 11, 3, 31, "0", cpSource)
		f.commit(t, nil, []currentPresenceAtom{a})
		cpExactAtoms(t, cpAllNativeAtoms(t, openCatalog(t, f.db, f.index, Limits{}), f.tree, defaultCurrentPresenceTreeLimits()), []currentPresenceAtom{a})
	}
}

type cpCountingContext struct {
	context.Context
	calls int
}

func (c *cpCountingContext) Err() error { c.calls++; return c.Context.Err() }
func TestCurrentPresenceTreeLateSharedCapAndCancellation(t *testing.T) {
	f := cpFixture(t, temporal.ProfileIntegerZ)
	f.limits.codec.maxRows = 2
	f.commit(t, nil, nil)
	c := openCatalog(t, f.db, f.index, Limits{})
	held := stage(t, c)
	if e := held.Axis(t.Context(), f.axis); e != nil {
		t.Fatal(e)
	}
	s := stage(t, c)
	if e := s.Axis(t.Context(), f.axis); e != nil {
		t.Fatal(e)
	}
	prior, e := s.Writes()
	if e != nil {
		t.Fatal(e)
	}
	records, bytes := c.records, c.stageBytes
	c.limits.MaxStageRecords = 4
	atoms := []currentPresenceAtom{cpNativeAtom(t, f, 1, 11, 3, 31, "0", cpSource), cpNativeAtom(t, f, 2, 12, 3, 31, "0", cpTarget)}
	// Force two leaves plus parent; local pending fits four records, but two
	// preexisting shared records make final publication exceed the shared cap.
	limits := f.limits
	limits.codec.maxRows = 1
	out, e := stageCurrentPresenceAtoms(t.Context(), s, f.root, f.tree, nil, atoms, limits)
	if !errors.Is(e, ErrResourceLimit) || out != (stagedCurrentPresence{}) {
		t.Fatal(e)
	}
	now, e := s.Writes()
	if e != nil || !sameWrites(prior, now) || c.records != records || c.stageBytes != bytes || f.tree.count != 0 {
		t.Fatal("shared-cap partial publication", e)
	}
	c.limits.MaxStageRecords = 1024
	if e := held.Close(); e != nil {
		t.Fatal(e)
	}
	many := []currentPresenceAtom{}
	for i := range 16 {
		many = append(many, cpNativeAtom(t, f, 1, 11, uint64(i+3), 31, fmt.Sprint(i*2), cpSource))
	}
	probe := stage(t, c)
	counter := &cpCountingContext{Context: t.Context()}
	successful, e := stageCurrentPresenceAtoms(counter, probe, f.root, f.tree, nil, many, f.limits)
	if e != nil || successful.work.Splits == 0 {
		t.Fatal(e)
	}
	if e := probe.Close(); e != nil {
		t.Fatal(e)
	}
	cancelled := &cpCancelAfter{Context: t.Context(), after: counter.calls - 5}
	out, e = stageCurrentPresenceAtoms(cancelled, s, f.root, f.tree, nil, many, f.limits)
	if !errors.Is(e, context.Canceled) || out != (stagedCurrentPresence{}) {
		t.Fatal("late cancellation", e)
	}
	now, e = s.Writes()
	if e != nil || !sameWrites(prior, now) || f.tree.count != 0 {
		t.Fatal("cancelled split changed prior staging", e)
	}
	if _, e := c.Root(); e != nil {
		t.Fatal("ordinary refusal poisoned catalog", e)
	}
}

func TestCurrentPresenceTreeWideReplacementThenUnchangedSuffix(t *testing.T) {
	f := cpFixture(t, temporal.ProfileIntegerZ)
	f.limits.targetBytes = 400
	f.limits.codec.maxRows = 4
	f.limits.codec.maxChildren = 8
	all := []currentPresenceAtom{}
	for i := range 16 {
		a := cpNativeAtom(t, f, 1, 11, uint64(i+3), 31, fmt.Sprint(i*2), cpSource)
		a.flags &^= cpPoint
		a.flags |= cpLowerClosed
		a.upper = cpTestBody(t, cpTestPosition(t, f.axis, fmt.Sprint(i*2+2), 0), temporal.Limits{})
		all = append(all, a)
	}
	f.commit(t, nil, all)
	// A single same-key wide upper replacement splits its early leaf into more
	// references; later unaffected children must grow the exact-cap reference
	// buffer through the charged path too.
	old := all[1]
	wide := old
	wide.upper = cpTestBody(t, cpTestPosition(t, f.axis, new(big.Int).Lsh(big.NewInt(1), 2047).String(), 0), temporal.Limits{})
	work := f.commit(t, []currentPresenceAtom{old}, []currentPresenceAtom{wide})
	all[1] = wide
	cpExactAtoms(t, cpAllNativeAtoms(t, openCatalog(t, f.db, f.index, Limits{}), f.tree, f.limits), all)
	if work.Splits < 1 {
		t.Fatal("did not exercise reference growth")
	}
	// Direct capacity ledger catches the suffix allocation omitted in review1.
	c := openCatalog(t, f.db, f.index, Limits{})
	q := cpReadOperation(t, c, f.limits)
	refs := make([]cpReference, 1)
	before := q.q.bytes
	grown, e := q.appendReference(refs, cpReference{})
	if e != nil || cap(grown) != 2 || q.q.bytes-before != 2*cpReferenceOwned {
		t.Fatal("uncharged exact suffix growth", e)
	}
}
func TestCurrentPresenceTreeWeightedBranchSplit(t *testing.T) {
	f := cpFixture(t, temporal.ProfileIntegerZ)
	f.limits.targetBytes = 150
	f.limits.codec.maxPageBytes = 4096
	f.limits.codec.maxChildren = 8
	atoms := []currentPresenceAtom{}
	number := new(big.Int).Lsh(big.NewInt(1), 2047)
	for i := range 8 {
		at := fmt.Sprint(i)
		if i < 2 {
			at = new(big.Int).Add(number, big.NewInt(int64(i))).String()
		}
		atoms = append(atoms, cpNativeAtom(t, f, uint64(i+1), 11, uint64(i+3), 31, at, cpSource))
	}
	f.commit(t, nil, atoms)
	q := cpReadOperation(t, openCatalog(t, f.db, f.index, Limits{}), f.limits)
	root, e := q.load(f.tree)
	if e != nil {
		t.Fatal(e)
	}
	if root.level != 2 || len(root.children) != 3 || root.children[0].count != 2 {
		t.Fatalf("weighted wide-fence split shape level%d children%+v", root.level, root.children)
	}
	cpExactAtoms(t, cpAllNativeAtoms(t, openCatalog(t, f.db, f.index, Limits{}), f.tree, f.limits), atoms)
}
func TestCurrentPresenceTreePrecopyWirePolicy(t *testing.T) {
	f := cpFixture(t, temporal.ProfileIntegerZ)
	c := openCatalog(t, f.db, f.index, Limits{})
	q := cpReadOperation(t, c, f.limits)
	invalid := cpNativeAtom(t, f, 1, 11, 3, 31, "0", cpSource)
	invalid.lower = make([]byte, 200)
	q.limits.codec.temporal.MaxValueBytes = 60
	before := q.q.bytes
	_, e := cpCloneEdits(q, []currentPresenceAtom{invalid})
	if !errors.Is(e, ErrResourceLimit) || q.q.bytes != before {
		t.Fatal("oversized coordinate copied before policy check", e)
	}
	invalid.lower = []byte{1, 0, 2, 1}
	_, e = cpCloneEdits(q, []currentPresenceAtom{invalid})
	if !errors.Is(e, ErrInvalid) || q.q.bytes != before {
		t.Fatal("malformed coordinate copied before framing check", e)
	}
}

func cpCollapseFixture(t *testing.T) (*cpTreeFixture, []currentPresenceAtom) {
	t.Helper()
	f := cpFixture(t, temporal.ProfileIntegerZ)
	f.limits.codec.maxRows = 2
	f.limits.codec.maxChildren = 4
	atoms := make([]currentPresenceAtom, 48)
	for i := range atoms {
		atoms[i] = cpNativeAtom(t, f, 1, 11, uint64(i+3), 31, fmt.Sprint(i*2), cpSource)
	}
	f.commit(t, nil, atoms)
	if f.tree.level < 2 {
		t.Fatal("collapse fixture lacks a multilevel neighbor", f.tree)
	}
	return f, atoms
}

func cpCollapseRange(t *testing.T, f *cpTreeFixture, atoms []currentPresenceAtom, side string) (int, int) {
	t.Helper()
	q := cpReadOperation(t, openCatalog(t, f.db, f.index, Limits{}), f.limits)
	n, err := q.load(f.tree)
	if err != nil || len(n.children) < 2 {
		t.Fatal("collapse requires adjacent root children", err)
	}
	if side == "left" {
		return 1, int(n.children[0].count)
	}
	if side == "right" {
		return len(atoms) - int(n.children[len(n.children)-1].count), len(atoms) - 1
	}
	return len(atoms)/4 + 1, 3*len(atoms)/4 - 1
}

func cpAssertDeleted(t *testing.T, c *Catalog, tree currentPresenceTreeRoot, limits currentPresenceTreeLimits, removed []currentPresenceAtom) {
	t.Helper()
	q := cpReadOperation(t, c, limits)
	n, err := q.load(tree)
	if err != nil {
		t.Fatal(err)
	}
	for _, atom := range removed {
		_, found, err := q.findAtom(n, atom)
		if err != nil || found {
			t.Fatalf("deleted relationship %d still has its atom: found=%t error=%v", atom.relationship, found, err)
		}
	}
}

func TestCurrentPresenceTreeCollapsedSubtreesAndReopen(t *testing.T) {
	for _, side := range []string{"left", "right", "middle"} {
		t.Run(side, func(t *testing.T) {
			f, all := cpCollapseFixture(t)
			oldTree, oldIndex := f.tree, f.index
			oldView := openCatalog(t, f.db, oldIndex, Limits{})
			start, end := cpCollapseRange(t, f, all, side)
			removed := slices.Clone(all[start:end])
			want := append(slices.Clone(all[:start]), all[end:]...)
			work := f.commit(t, removed, nil)
			if side != "middle" && work.Rebalances == 0 {
				t.Fatal("contiguous deletion did not rebalance", work)
			}
			if side == "middle" && work.FinalPageWrites < 2 {
				t.Fatal("middle deletion did not span multiple pages", work)
			}
			current := openCatalog(t, f.db, f.index, Limits{})
			cpExactAtoms(t, cpAllNativeAtoms(t, current, f.tree, f.limits), want)
			cpAssertDeleted(t, current, f.tree, f.limits, removed)
			cpExactAtoms(t, cpAllNativeAtoms(t, oldView, oldTree, f.limits), all)
			t.Logf("%s delete [%d,%d), old level%d new level%d: %+v", side, start, end, oldTree.level, f.tree.level, work)
			if err := oldView.view.Close(); err != nil {
				t.Fatal(err)
			}
			if err := current.view.Close(); err != nil {
				t.Fatal(err)
			}
			if err := f.db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := raftlog.Open(raftlog.Config{Dir: "catalog", FS: f.fs, Application: raftlog.DefaultApplicationPolicy(1)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			reopened := openCatalog(t, db, f.index, Limits{})
			cpExactAtoms(t, cpAllNativeAtoms(t, reopened, f.tree, f.limits), want)
			cpAssertDeleted(t, reopened, f.tree, f.limits, removed)
			cpExactAtoms(t, cpAllNativeAtoms(t, openCatalog(t, db, oldIndex, Limits{}), oldTree, f.limits), all)
		})
	}
}

// The context observes the actual join stack, so refusal is tied to rebalance
// execution rather than an earlier generic admission check or a guessed count.
type cpJoinContext struct {
	context.Context
	calls, joins int
	enter        func() error
}

func (c *cpJoinContext) Err() error {
	c.calls++
	if err := c.Context.Err(); err != nil {
		return err
	}
	var pcs [32]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.Function, ".(*cpTreeOperation).join") {
			c.joins++
			if c.enter != nil {
				return c.enter()
			}
			break
		}
		if !more {
			break
		}
	}
	return nil
}

func TestCurrentPresenceTreeJoinRefusalsPreserveStaging(t *testing.T) {
	f, all := cpCollapseFixture(t)
	start, end := cpCollapseRange(t, f, all, "left")
	removed := slices.Clone(all[start:end])
	c := openCatalog(t, f.db, f.index, Limits{})
	probe := stage(t, c)
	observed := &cpJoinContext{Context: t.Context()}
	success, err := stageCurrentPresenceAtoms(observed, probe, f.root, f.tree, removed, nil, f.limits)
	if err != nil || observed.joins == 0 || success.work.Rebalances == 0 {
		t.Fatal("probe did not traverse an actual unequal-height join", observed.joins, success.work, err)
	}
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"cancel", "resource"} {
		t.Run(mode, func(t *testing.T) {
			s := stage(t, c)
			if err := s.Axis(t.Context(), f.axis); err != nil {
				t.Fatal(err)
			}
			prior, err := s.Writes()
			if err != nil {
				t.Fatal(err)
			}
			records, bytes, readCap := c.records, c.stageBytes, c.limits.MaxReadBytes
			ctx := &cpJoinContext{Context: t.Context()}
			sentinel := context.Canceled
			ctx.enter = func() error { return context.Canceled }
			if mode == "resource" {
				sentinel = ErrResourceLimit
				ctx.enter = func() error { c.limits.MaxReadBytes = rootBytes; return nil }
			}
			out, err := stageCurrentPresenceAtoms(ctx, s, f.root, f.tree, removed, nil, f.limits)
			c.limits.MaxReadBytes = readCap
			if !errors.Is(err, sentinel) || out != (stagedCurrentPresence{}) || ctx.joins == 0 {
				t.Fatal("refusal did not execute inside join", ctx.joins, err)
			}
			now, err := s.Writes()
			if err != nil || !sameWrites(prior, now) || c.records != records || c.stageBytes != bytes {
				t.Fatal("join refusal changed existing staging", err)
			}
			cpExactAtoms(t, cpAllNativeAtoms(t, c, f.tree, f.limits), all)
			t.Logf("%s refused after %d context checks, %d checks inside join; successful probe %+v", mode, ctx.calls, ctx.joins, success.work)
		})
	}
}

func TestCurrentPresenceTreeReferenceCapacityBoundaries(t *testing.T) {
	f := cpFixture(t, temporal.ProfileIntegerZ)
	c := openCatalog(t, f.db, f.index, Limits{})
	ref := func(id uint64) cpReference { return cpReference{child: currentPresenceChild{id: id}} }
	for _, tc := range []struct {
		name string
		cost int
		call func(*cpTreeOperation) ([]cpReference, error)
		want []uint64
	}{
		{"append-many-growth", 3 * cpReferenceOwned, func(q *cpTreeOperation) ([]cpReference, error) {
			return q.appendReferences([]cpReference{ref(1)}, []cpReference{ref(2), ref(3)})
		}, []uint64{1, 2, 3}},
		{"append-one-growth", 2 * cpReferenceOwned, func(q *cpTreeOperation) ([]cpReference, error) {
			return q.appendReference([]cpReference{ref(1)}, ref(2))
		}, []uint64{1, 2}},
		{"replace-growth", 5 * cpReferenceOwned, func(q *cpTreeOperation) ([]cpReference, error) {
			return q.replaceReferences([]cpReference{ref(1), ref(2), ref(3)}, 1, 2, []cpReference{ref(4), ref(5), ref(6)})
		}, []uint64{1, 4, 5, 6, 3}},
		{"concat-owned", 3 * cpReferenceOwned, func(q *cpTreeOperation) ([]cpReference, error) {
			return q.concatReferences([]cpReference{ref(1)}, []cpReference{ref(2), ref(3)})
		}, []uint64{1, 2, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := cpReadOperation(t, c, f.limits)
			before := q.q.bytes
			q.q.maxBytes = before + tc.cost - 1
			got, err := tc.call(q)
			if !errors.Is(err, ErrResourceLimit) || got != nil || q.q.bytes != before {
				t.Fatal("capacity refused after uncharged allocation", got, err)
			}
			q.q.maxBytes++
			got, err = tc.call(q)
			if err != nil || cap(got) != len(tc.want) || q.q.bytes-before != tc.cost {
				t.Fatal("exact capacity allowance failed", err)
			}
			ids := make([]uint64, len(got))
			for i := range got {
				ids[i] = got[i].child.id
			}
			if !slices.Equal(ids, tc.want) {
				t.Fatal("growth lost existing or replacement references", ids, tc.want)
			}
		})
	}
}
