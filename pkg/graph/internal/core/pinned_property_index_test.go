package core

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
)

// Item M (backlog 8, tasks/handover-pinned-index-churn-20261009.md): a pinned
// property lookup (ByTypeAndProperty / ByLabelAndProperty with a temporal
// filter) must cost the rels that ever carried the value, not the whole
// history of the graph. These tests pin the ANSWER (T1: exact sets per pin on
// every backend, with and without the index) and the COST (T5: chain loads).

// pinnedPropWrapStore embeds a native store: core must decline every native
// capability it promotes (the wrapper may override reads), so the property
// doors take the full-history fold on it.
type pinnedPropWrapStore struct {
	*memory.Store
}

type pinnedPropBackend struct {
	name     string
	canIndex bool // tiered declines rel property index creation
	segments bool // memory with a declared + sealed RelSegments type
	open     func(t *testing.T) *Core
	// wrapped, when set, is the native store under a wrapper: core declines
	// every capability the wrapper promotes (including index DDL), so the
	// fixture declares the index on the inner store directly, after the first
	// write registered the type.
	wrapped func(g *Core) *memory.Store
}

func pinnedPropOpen(t *testing.T, cfg Config) *Core {
	t.Helper()
	cfg.AllowTxBackfill = true
	g, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func pinnedPropBackends() []pinnedPropBackend {
	return []pinnedPropBackend{
		{name: "memory", canIndex: true, open: func(t *testing.T) *Core {
			return pinnedPropOpen(t, Config{Store: memory.New()})
		}},
		{name: "memory-segments", canIndex: true, segments: true, open: func(t *testing.T) *Core {
			return pinnedPropOpen(t, Config{Store: memory.New(), RelSegments: []storepkg.RelSegmentSpec{{
				Type: "T", Columns: []storepkg.SegmentColumn{{Name: "seat", Kind: storepkg.SegmentInt64}},
			}}})
		}},
		{name: "badger", canIndex: true, open: func(t *testing.T) *Core {
			bs, err := badger.New(badger.Config{InMemory: true})
			if err != nil {
				t.Fatalf("badger.New: %v", err)
			}
			return pinnedPropOpen(t, Config{Store: bs})
		}},
		{name: "badger-delta", canIndex: true, open: func(t *testing.T) *Core {
			// Anchor+delta history (ADR-0009): every second version is a delta
			// that carries only the properties that changed vs its anchor.
			bs, err := badger.New(badger.Config{InMemory: true, HistoryDeltaEncoding: true, HistoryAnchorInterval: 2})
			if err != nil {
				t.Fatalf("badger.New: %v", err)
			}
			return pinnedPropOpen(t, Config{Store: bs})
		}},
		{name: "sharded", canIndex: true, open: func(t *testing.T) *Core {
			st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				t.Fatalf("sharded.New: %v", err)
			}
			return pinnedPropOpen(t, Config{Store: st})
		}},
		{name: "tiered", canIndex: false, open: func(t *testing.T) *Core {
			st, err := tiered.New(tiered.Config{
				InMemory:      true,
				RefLabels:     []string{"P"},
				ShardWindow:   7 * 24 * time.Hour,
				FlushInterval: 1<<63 - 1,
			})
			if err != nil {
				t.Fatalf("tiered.New: %v", err)
			}
			t.Cleanup(func() { _ = st.Close() })
			return pinnedPropOpen(t, Config{Store: st})
		}},
		{name: "wrapper", canIndex: true, open: func(t *testing.T) *Core {
			return pinnedPropOpen(t, Config{Store: &pinnedPropWrapStore{Store: memory.New()}})
		}, wrapped: func(g *Core) *memory.Store {
			return g.store.(*pinnedPropWrapStore).Store
		}},
	}
}

// relVersionSet is a pinned answer as {rel ID: resolved version}.
type relVersionSet map[types.RelID]uint32

func relVersionSetOf(rels []*types.Relationship) relVersionSet {
	out := make(relVersionSet, len(rels))
	for _, r := range rels {
		out[r.ID()] = r.Version()
	}
	return out
}

func (s relVersionSet) String() string {
	ids := make([]types.RelID, 0, len(s))
	for id := range s {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := "{"
	for i, id := range ids {
		if i > 0 {
			out += " "
		}
		out += fmt.Sprintf("%d:v%d", id.SnowflakeID(), s[id])
	}
	return out + "}"
}

func relVersionSetsEqual(a, b relVersionSet) bool {
	if len(a) != len(b) {
		return false
	}
	for id, v := range a {
		if w, ok := b[id]; !ok || w != v {
			return false
		}
	}
	return true
}

// pinnedPropFixture is the T1 scenario (handover §6): on type T with key
// "seat",
//
//	A: 1 -> 2 -> 1           B: created 2, deleted     C: 1 -> 3, then deleted
//	D: 1, untouched          E: created after the first pin (1)
//	G: carried 1 only on v0  H: backfilled (AddWithTx) between two stamps (1)
//	X: another type U, same values, churned
type pinnedPropFixture struct {
	g      *Core
	rel    map[string]types.RelID
	name   map[types.RelID]string
	phases []pinnedPropPhase
	hTx    types.Instant
}

type pinnedPropPhase struct {
	name string
	pin  types.Instant
}

// buildPinnedPropFixture writes the T1 scenario. warm runs a pinned lookup
// right after the first write so the sidecar (when the store has one) is
// built early and every later write is recorded incrementally instead of by
// the lazy build.
func buildPinnedPropFixture(t *testing.T, be pinnedPropBackend, withIndex, warm bool) *pinnedPropFixture {
	t.Helper()
	g := be.open(t)
	ctx := context.Background()
	f := &pinnedPropFixture{g: g, rel: map[string]types.RelID{}, name: map[types.RelID]string{}}
	if withIndex && be.wrapped == nil {
		if err := g.Index.CreateRelProperty("T", "seat"); err != nil {
			t.Fatalf("CreateRelProperty: %v", err)
		}
	}
	a, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatalf("add node: %v", err)
	}
	b, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatalf("add node: %v", err)
	}
	phase := func(name string) {
		pin, err := g.Temporal.NowTx()
		if err != nil {
			t.Fatalf("NowTx: %v", err)
		}
		f.phases = append(f.phases, pinnedPropPhase{name: name, pin: pin})
	}
	add := func(label, typ string, seat int64) {
		r, err := g.Rels.Add(ctx, typ, a, b, map[string]any{"seat": seat})
		if err != nil {
			t.Fatalf("add %s: %v", label, err)
		}
		f.rel[label] = r.ID()
		f.name[r.ID()] = label
	}
	update := func(label string, seat int64) {
		if _, err := g.Rels.Update(ctx, f.rel[label], map[string]any{"seat": seat}); err != nil {
			t.Fatalf("update %s: %v", label, err)
		}
	}
	del := func(label string) {
		if err := g.Rels.Delete(ctx, f.rel[label]); err != nil {
			t.Fatalf("delete %s: %v", label, err)
		}
	}
	seal := func() {
		if be.segments {
			if err := g.Admin.SealRelSegments("T"); err != nil {
				t.Fatalf("SealRelSegments: %v", err)
			}
		}
	}

	phase("p0")
	add("A", "T", 1)
	if withIndex && be.wrapped != nil {
		tok, _ := g.relTypes.Lookup("T")
		if err := be.wrapped(g).CreateRelPropertyIndex(tok, "seat"); err != nil {
			t.Fatalf("inner CreateRelPropertyIndex: %v", err)
		}
	}
	phase("p1")
	if warm && withIndex {
		if _, err := g.Rels.ByTypeAndProperty("T", "seat", int64(1), storepkg.QueryOpts{TxPin: f.phases[1].pin}); err != nil {
			t.Fatalf("warm-up lookup: %v", err)
		}
	}
	add("B", "T", 2)
	phase("p2")
	add("C", "T", 1)
	phase("p3")
	// Leave a gap of free stamps after p3 for H's backfilled TxFrom: the
	// transaction clock is wall-dominated but runs ahead of the wall during a
	// burst, so wait until the wall passes p3 by more than one stamp.
	f.hTx = f.phases[len(f.phases)-1].pin + 1
	for time.Now().UnixMilli() <= int64(f.hTx)+1 {
		time.Sleep(time.Millisecond)
	}
	add("D", "T", 1)
	phase("p4")
	add("G", "T", 1)
	phase("p5")
	add("X", "U", 1)
	phase("p6") // the first pin E post-dates
	seal()
	update("A", 2)
	phase("p7")
	update("G", 4)
	phase("p8")
	update("C", 3)
	phase("p9")
	add("E", "T", 1)
	phase("p10")
	update("A", 1)
	phase("p11")
	del("B")
	phase("p12")
	del("C")
	phase("p13")
	update("X", 2)
	phase("p14")
	h, err := g.Rels.AddWithTx(ctx, "T", a, b, map[string]any{"seat": int64(1)}, f.hTx)
	if err != nil {
		t.Fatalf("AddWithTx H: %v", err)
	}
	f.rel["H"] = h.ID()
	f.name[h.ID()] = "H"
	phase("p15")
	seal()
	return f
}

// want builds a literal {rel: version} set from label:version pairs.
func (f *pinnedPropFixture) want(pairs ...any) relVersionSet {
	out := relVersionSet{}
	for i := 0; i < len(pairs); i += 2 {
		out[f.rel[pairs[i].(string)]] = uint32(pairs[i+1].(int))
	}
	return out
}

func (f *pinnedPropFixture) pinOf(t *testing.T, name string) types.Instant {
	t.Helper()
	for _, ph := range f.phases {
		if ph.name == name {
			return ph.pin
		}
	}
	t.Fatalf("no phase %s", name)
	return 0
}

// stampPins returns every TxFrom/TxTo/DeletedAt of every stored row of the
// fixture's rels, each with its +-1 neighbours, plus the phase pins, a pin
// before the first write and the far future.
func (f *pinnedPropFixture) stampPins(t *testing.T) []types.Instant {
	t.Helper()
	set := map[types.Instant]struct{}{1: {}, math.MaxInt64 / 2: {}}
	addStamp := func(s types.Instant) {
		if s <= 0 {
			return
		}
		set[s-1], set[s], set[s+1] = struct{}{}, struct{}{}, struct{}{}
	}
	for _, ph := range f.phases {
		addStamp(ph.pin)
	}
	for _, id := range f.rel {
		hist, err := f.g.Rels.History(id)
		if err != nil {
			t.Fatalf("History(%d): %v", id.SnowflakeID(), err)
		}
		rows := hist
		if cur, err := f.g.Rels.Get(context.Background(), id); err == nil {
			rows = append(rows, cur)
		} else if !errors.Is(err, storepkg.ErrRelNotFound) {
			t.Fatalf("Get(%d): %v", id.SnowflakeID(), err)
		}
		for _, r := range rows {
			if tm := r.Temporal(); tm != nil {
				addStamp(tm.TxFrom)
				addStamp(tm.TxTo)
				addStamp(tm.DeletedAt)
			}
		}
	}
	pins := make([]types.Instant, 0, len(set))
	for p := range set {
		pins = append(pins, p)
	}
	sort.Slice(pins, func(i, j int) bool { return pins[i] < pins[j] })
	return pins
}

// relsAsOfMatching is the independent ground truth: RelsAsOf(pin) (the native
// transaction-time door, which never touches a property index or sidecar)
// filtered to type T and seat == value.
func relsAsOfMatching(t *testing.T, g *Core, pin types.Instant, typ, key string, value any) relVersionSet {
	t.Helper()
	tok, ok := g.relTypes.Lookup(typ)
	if !ok {
		t.Fatalf("type %s not registered", typ)
	}
	rels, err := g.Temporal.RelsAsOf(pin)
	if err != nil {
		t.Fatalf("RelsAsOf(%d): %v", pin, err)
	}
	return filterRelsTypeValue(rels, tok, key, value)
}

func filterRelsTypeValue(rels []*types.Relationship, tok uint16, key string, value any) relVersionSet {
	want := indexpkg.PropertyValueKey(value)
	out := relVersionSet{}
	for _, r := range rels {
		if !r.HasTypeTokenRaw(tok) {
			continue
		}
		if got, found := r.IndexablePropertyValueKey(key); found && got == want {
			out[r.ID()] = r.Version()
		}
	}
	return out
}

// TestPinnedByTypeAndPropertyEqualsPinnedScan is T1. Per pin it asserts the
// literal {rel: version} set and equality with two doors that never consult
// the property path: RelsAsOf(pin) filtered, and ByType{TxPin} filtered.
//
// Faulty implementations it catches (each a mutant run red, evidence under
// tasks/evidence/pinned-property-index/): resolving at the current version
// (A at p7 is v1 with seat 2, its current v2 has seat 1), dropping deleted
// rels (C at p3..p8, B for value 2), recording only the current value of a
// rel (G carried 1 only on v0, C carried 1 only on v0 and is deleted), pruning
// with firstTx >= pin instead of > pin (every phase pin equals the stamp of the
// write it follows when the clock does not move; H at hTx exactly), skipping
// the backfilled row (H), and leaking another type's carriers (X).
func TestPinnedByTypeAndPropertyEqualsPinnedScan(t *testing.T) {
	for _, be := range pinnedPropBackends() {
		for _, withIndex := range []bool{true, false} {
			if withIndex && !be.canIndex {
				continue
			}
			for _, warm := range []bool{false, true} {
				if warm && !withIndex {
					continue
				}
				name := fmt.Sprintf("%s/index=%v/warm=%v", be.name, withIndex, warm)
				t.Run(name, func(t *testing.T) {
					f := buildPinnedPropFixture(t, be, withIndex, warm)
					g := f.g
					lookup := func(pin types.Instant, value int64) relVersionSet {
						t.Helper()
						rels, err := g.Rels.ByTypeAndProperty("T", "seat", value, storepkg.QueryOpts{TxPin: pin})
						if err != nil {
							t.Fatalf("ByTypeAndProperty(seat=%d, TxPin=%d): %v", value, pin, err)
						}
						return relVersionSetOf(rels)
					}

					// Literal sets at the phase pins (seat = 1 and seat = 2).
					literal := []struct {
						phase string
						v1    relVersionSet
						v2    relVersionSet
					}{
						{"p0", f.want(), f.want()},
						{"p1", f.want("A", 0), f.want()},
						{"p2", f.want("A", 0), f.want("B", 0)},
						{"p3", f.want("A", 0, "C", 0), f.want("B", 0)},
						{"p4", f.want("A", 0, "C", 0, "D", 0, "H", 0), f.want("B", 0)},
						{"p5", f.want("A", 0, "C", 0, "D", 0, "G", 0, "H", 0), f.want("B", 0)},
						{"p6", f.want("A", 0, "C", 0, "D", 0, "G", 0, "H", 0), f.want("B", 0)},
						{"p7", f.want("C", 0, "D", 0, "G", 0, "H", 0), f.want("A", 1, "B", 0)},
						{"p8", f.want("C", 0, "D", 0, "H", 0), f.want("A", 1, "B", 0)},
						{"p9", f.want("D", 0, "H", 0), f.want("A", 1, "B", 0)},
						{"p10", f.want("D", 0, "E", 0, "H", 0), f.want("A", 1, "B", 0)},
						{"p11", f.want("A", 2, "D", 0, "E", 0, "H", 0), f.want("B", 0)},
						{"p12", f.want("A", 2, "D", 0, "E", 0, "H", 0), f.want()},
						{"p13", f.want("A", 2, "D", 0, "E", 0, "H", 0), f.want()},
						{"p14", f.want("A", 2, "D", 0, "E", 0, "H", 0), f.want()},
						{"p15", f.want("A", 2, "D", 0, "E", 0, "H", 0), f.want()},
					}
					for _, l := range literal {
						pin := f.pinOf(t, l.phase)
						if got := lookup(pin, 1); !relVersionSetsEqual(got, l.v1) {
							t.Fatalf("%s seat=1: got %v, want %v (names %v)", l.phase, got, l.v1, f.name)
						}
						if got := lookup(pin, 2); !relVersionSetsEqual(got, l.v2) {
							t.Fatalf("%s seat=2: got %v, want %v (names %v)", l.phase, got, l.v2, f.name)
						}
					}
					// H's backfilled TxFrom: absent one stamp before, present at it.
					if got, want := lookup(f.hTx-1, 1), f.want("A", 0, "C", 0); !relVersionSetsEqual(got, want) {
						t.Fatalf("hTx-1 seat=1: got %v, want %v", got, want)
					}
					if got, want := lookup(f.hTx, 1), f.want("A", 0, "C", 0, "H", 0); !relVersionSetsEqual(got, want) {
						t.Fatalf("hTx seat=1: got %v, want %v", got, want)
					}

					// Every stamp +-1: the lookup equals RelsAsOf and ByType{TxPin}
					// filtered, for each value ever carried and a phantom.
					tTok, _ := g.relTypes.Lookup("T")
					for _, pin := range f.stampPins(t) {
						byType, err := g.Rels.ByType("T", storepkg.QueryOpts{TxPin: pin})
						if err != nil {
							t.Fatalf("ByType(TxPin=%d): %v", pin, err)
						}
						for _, value := range []int64{1, 2, 3, 4, 99} {
							got := lookup(pin, value)
							asOf := relsAsOfMatching(t, g, pin, "T", "seat", value)
							if !relVersionSetsEqual(got, asOf) {
								t.Fatalf("pin %d seat=%d: lookup %v != RelsAsOf %v (names %v)", pin, value, got, asOf, f.name)
							}
							if typed := filterRelsTypeValue(byType, tTok, "seat", value); !relVersionSetsEqual(got, typed) {
								t.Fatalf("pin %d seat=%d: lookup %v != ByType %v (names %v)", pin, value, got, typed, f.name)
							}
						}
					}
				})
			}
		}
	}
}

// pinnedLoadBackends are the backends whose native store can carry the
// membership sidecar (T5 asserts its cost; the fold backends have none).
func pinnedLoadBackends() []pinnedPropBackend {
	var out []pinnedPropBackend
	for _, be := range pinnedPropBackends() {
		switch be.name {
		case "memory", "badger", "sharded":
			out = append(out, be)
		}
	}
	return out
}

// countChainLoads runs fn and returns how many version chains the generic
// temporal doors resolved meanwhile.
func countChainLoads(g *Core, fn func()) int {
	n := 0
	g.chainLoadTestHook = func() { n++ }
	defer func() { g.chainLoadTestHook = nil }()
	fn()
	return n
}

// TestPinnedPropertyLookupLoadsOnlyEverMembers is T5 (structural): a pinned
// lookup for one value loads exactly the chains of the rels (nodes) that ever
// carried that value, and that count stays put when churn on other types
// (labels) and other values of the same type (label) grows tenfold.
//
// Faulty implementation it catches: today's fold, which loads every rel
// (node) with a history row across all types, so the count tracks the churn.
func TestPinnedPropertyLookupLoadsOnlyEverMembers(t *testing.T) {
	for _, be := range pinnedLoadBackends() {
		t.Run(be.name+"/rel", func(t *testing.T) {
			g := be.open(t)
			ctx := context.Background()
			if err := g.Index.CreateRelProperty("T", "seat"); err != nil {
				t.Fatalf("CreateRelProperty: %v", err)
			}
			a, _ := g.Nodes.Add(ctx, []string{"P"}, nil)
			b, _ := g.Nodes.Add(ctx, []string{"P"}, nil)
			add := func(typ string, seat int64) types.RelID {
				r, err := g.Rels.Add(ctx, typ, a, b, map[string]any{"seat": seat})
				if err != nil {
					t.Fatalf("add: %v", err)
				}
				return r.ID()
			}
			update := func(id types.RelID, seat int64) {
				if _, err := g.Rels.Update(ctx, id, map[string]any{"seat": seat}); err != nil {
					t.Fatalf("update: %v", err)
				}
			}
			// Ever-members of seat=7 on T: 5 current, 3 moved away, 2 deleted.
			for i := 0; i < 5; i++ {
				add("T", 7)
			}
			for i := 0; i < 3; i++ {
				update(add("T", 7), 8)
			}
			for i := 0; i < 2; i++ {
				if err := g.Rels.Delete(ctx, add("T", 7)); err != nil {
					t.Fatalf("delete: %v", err)
				}
			}
			const everMembers = 10
			churn := func(k int) {
				for i := 0; i < k; i++ {
					update(add("U", 7), 70) // another type, same value
					update(add("T", 9), 10) // same type, other values
				}
			}
			measure := func(stage string) {
				pin, err := g.Temporal.NowTx()
				if err != nil {
					t.Fatalf("NowTx: %v", err)
				}
				for _, opts := range []storepkg.QueryOpts{{TxPin: pin}, {ValidAt: pin, TxAt: pin}} {
					var rels []*types.Relationship
					loads := countChainLoads(g, func() {
						rels, err = g.Rels.ByTypeAndProperty("T", "seat", int64(7), opts)
					})
					if err != nil {
						t.Fatalf("%s lookup: %v", stage, err)
					}
					if len(rels) != 5 {
						t.Fatalf("%s %+v: %d matches, want 5", stage, opts, len(rels))
					}
					if loads != everMembers {
						t.Fatalf("%s %+v: lookup loaded %d chains, want %d (the value's ever-members)", stage, opts, loads, everMembers)
					}
				}
			}
			churn(20)
			measure("churn x1")
			churn(200)
			measure("churn x10")
		})
		t.Run(be.name+"/node", func(t *testing.T) {
			g := be.open(t)
			ctx := context.Background()
			if err := g.Index.CreateProperty("L", "seat"); err != nil {
				t.Fatalf("CreateProperty: %v", err)
			}
			add := func(label string, seat int64) types.NodeID {
				n, err := g.Nodes.Add(ctx, []string{label}, map[string]any{"seat": seat})
				if err != nil {
					t.Fatalf("add: %v", err)
				}
				return n.ID()
			}
			update := func(id types.NodeID, seat int64) {
				if _, err := g.Nodes.Update(ctx, id, map[string]any{"seat": seat}); err != nil {
					t.Fatalf("update: %v", err)
				}
			}
			for i := 0; i < 5; i++ {
				add("L", 7)
			}
			for i := 0; i < 3; i++ {
				update(add("L", 7), 8)
			}
			for i := 0; i < 2; i++ {
				if err := g.Nodes.Delete(ctx, add("L", 7)); err != nil {
					t.Fatalf("delete: %v", err)
				}
			}
			const everMembers = 10
			churn := func(k int) {
				for i := 0; i < k; i++ {
					update(add("M", 7), 70)
					update(add("L", 9), 10)
				}
			}
			measure := func(stage string) {
				pin, err := g.Temporal.NowTx()
				if err != nil {
					t.Fatalf("NowTx: %v", err)
				}
				for _, opts := range []storepkg.QueryOpts{{TxPin: pin}, {ValidAt: pin, TxAt: pin}} {
					var nodes []*types.Node
					loads := countChainLoads(g, func() {
						nodes, err = g.Nodes.ByLabelAndProperty("L", "seat", int64(7), opts)
					})
					if err != nil {
						t.Fatalf("%s lookup: %v", stage, err)
					}
					if len(nodes) != 5 {
						t.Fatalf("%s %+v: %d matches, want 5", stage, opts, len(nodes))
					}
					if loads != everMembers {
						t.Fatalf("%s %+v: lookup loaded %d chains, want %d (the value's ever-members)", stage, opts, loads, everMembers)
					}
				}
			}
			churn(20)
			measure("churn x1")
			churn(200)
			measure("churn x10")
		})
	}
}

// --- T3: the property doors in the bitemporal oracle harness ---

// withoutPropertySidecars runs fn with the property membership sidecars
// declined, so the doors take the full-history fold (the unindexed arm).
func withoutPropertySidecars(g *Core, fn func()) {
	rel, node := g.relPropTxMembers, g.nodePropTxMembers
	g.relPropTxMembers, g.nodePropTxMembers = nil, nil
	defer func() { g.relPropTxMembers, g.nodePropTxMembers = rel, node }()
	fn()
}

// propertyProbeDoors returns the temporal property doors that answer probe p
// (generic always; the named door only without a transaction-time filter).
func propertyProbeDoors[T any](p probe, generic func(storepkg.QueryOpts) ([]T, error), at func(types.Instant) ([]T, error), during func(types.Instant, types.Instant) ([]T, error)) map[string]func() ([]T, error) {
	doors := map[string]func() ([]T, error){}
	switch {
	case p.asOf:
		doors["generic{TxPin}"] = func() ([]T, error) { return generic(storepkg.QueryOpts{TxPin: p.txAt}) }
	case p.interval:
		doors["generic(interval)"] = func() ([]T, error) {
			return generic(storepkg.QueryOpts{ValidStart: p.s, ValidEnd: p.e, TxAt: p.txAt})
		}
		if p.txAt == 0 {
			doors["named During"] = func() ([]T, error) { return during(p.s, p.e) }
		}
	default:
		doors["generic(point)"] = func() ([]T, error) { return generic(storepkg.QueryOpts{ValidAt: p.validAt, TxAt: p.txAt}) }
		if p.txAt == 0 {
			doors["named At"] = func() ([]T, error) { return at(p.validAt) }
		}
	}
	return doors
}

// oracleProbeRow resolves one captured entity at probe p and applies match.
func oracleProbeRow(e *oracleEntity, p probe, match func(oracleRow) bool) (oracleRow, bool) {
	switch {
	case p.asOf:
		row, ok := e.asOfVisible(p.txAt)
		return row, ok && match(row)
	case p.interval:
		return e.intervalVisible(p.s, p.e, p.txAt, match)
	default:
		row, ok := e.pointVisible(p.validAt, p.txAt)
		return row, ok && match(row)
	}
}

// runPropertyProbe cross-checks every temporal property door against the
// oracle at one probe, twice: with the sidecars (R and A have a declared
// index, S, B and C do not) and with them declined (the fold). Point and
// as-of answers compare versions; interval answers compare membership (the
// version an interval door returns is its newest match, as for ByLabel).
func (w *world) runPropertyProbe(backend string, seed uint64, snap *snapshot, p probe) {
	g := w.g
	seatVals := []int64{0, 1, 2, 99}
	for _, typ := range oracleRelTypes {
		for _, v := range seatVals {
			want := indexpkg.PropertyValueKey(v)
			oracle := map[types.RelID]uint32{}
			for id, e := range snap.rels {
				if e.relType != typ {
					continue
				}
				if row, ok := oracleProbeRow(e, p, func(r oracleRow) bool { return r.seatVK == want }); ok {
					oracle[id] = row.version
				}
			}
			doors := propertyProbeDoors(p,
				func(o storepkg.QueryOpts) ([]*types.Relationship, error) {
					return g.Rels.ByTypeAndProperty(typ, "seat", v, o)
				},
				func(at types.Instant) ([]*types.Relationship, error) {
					return g.Temporal.RelsByTypePropertyAt(typ, "seat", v, at)
				},
				func(s, e types.Instant) ([]*types.Relationship, error) {
					return g.Temporal.RelsByTypePropertyDuring(typ, "seat", v, s, e)
				})
			for name, door := range doors {
				for _, arm := range []string{"indexed", "unindexed"} {
					var got []*types.Relationship
					var err error
					if arm == "indexed" {
						got, err = door()
					} else {
						withoutPropertySidecars(g, func() { got, err = door() })
					}
					if err != nil {
						w.t.Fatalf("%s rel %s(%s, seat=%d) %s: %v", backend, name, typ, v, arm, err)
					}
					gm := relSetVer(got)
					if (p.interval && !relKeysEqual(oracle, gm)) || (!p.interval && !relMapsEqual(oracle, gm)) {
						w.harnessFail(backend, seed, p, fmt.Sprintf("rel %s(%s, seat=%d) %s", name, typ, v, arm), fmtRelVer(oracle), fmtRelVer(gm))
					}
				}
			}
		}
	}
	for _, label := range oracleNodeLabels {
		for _, v := range seatVals {
			want := indexpkg.PropertyValueKey(v)
			oracle := map[types.NodeID]uint32{}
			for id, e := range snap.nodes {
				if row, ok := oracleProbeRow(e, p, func(r oracleRow) bool { return hasLabel(r, label) && r.seatVK == want }); ok {
					oracle[id] = row.version
				}
			}
			doors := propertyProbeDoors(p,
				func(o storepkg.QueryOpts) ([]*types.Node, error) {
					return g.Nodes.ByLabelAndProperty(label, "seat", v, o)
				},
				func(at types.Instant) ([]*types.Node, error) {
					return g.Temporal.NodesByLabelPropertyAt(label, "seat", v, at)
				},
				func(s, e types.Instant) ([]*types.Node, error) {
					return g.Temporal.NodesByLabelPropertyDuring(label, "seat", v, s, e)
				})
			for name, door := range doors {
				for _, arm := range []string{"indexed", "unindexed"} {
					var got []*types.Node
					var err error
					if arm == "indexed" {
						got, err = door()
					} else {
						withoutPropertySidecars(g, func() { got, err = door() })
					}
					if err != nil {
						w.t.Fatalf("%s node %s(%s, seat=%d) %s: %v", backend, name, label, v, arm, err)
					}
					gm := nodeSetVer(got)
					if (p.interval && !nodeKeysEqual(oracle, gm)) || (!p.interval && !nodeMapsEqual(oracle, gm)) {
						w.harnessFail(backend, seed, p, fmt.Sprintf("node %s(%s, seat=%d) %s", name, label, v, arm), fmtNodeVer(oracle), fmtNodeVer(gm))
					}
				}
			}
		}
	}
}
