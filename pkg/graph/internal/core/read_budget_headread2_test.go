package core

import (
	"context"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	shardedpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R2-5 HEADREAD, second round (design tasks/evidence/R2CUMUL/design-headread.txt
// § 4, T3-T10): the memory store, the tiered history token's shard open, the
// From door's saving, the miss, a refused miss's entry, a winner gone on a
// hit, and two entities under one meter. Every count is checked against an
// exact unlimited control and a native counter the meter does not keep.

// hr2Backend is one store a test runs on; persistent stores also run after a
// reopen (rows flushed, entity caches cold). The disk factories share
// rbdBackends' disabled flush timer: a background flush landing inside a
// read would decode the rows it moved twice (the overlay captured before it,
// then the committed rows) and change the exact counts the tests pin
// (TestBudgetedTimeline_FillInCommitWindow covers that window on purpose).
type hr2Backend struct {
	name       string
	open       func(t testing.TB, dir string) (*Core, func())
	persistent bool
}

func hr2Backends() []hr2Backend {
	out := []hr2Backend{{"memory", func(t testing.TB, _ string) (*Core, func()) {
		t.Helper()
		g, err := New(Config{Store: memory.New(), UntimedValidStart: UntimedFromTxFrom})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return g, func() { _ = g.Close() }
	}, false}}
	for _, be := range rbdBackends() {
		out = append(out, hr2Backend{be.name, be.open, true})
	}
	return out
}

// hr2Native is the native count of history versions read for the nodes and
// rels, kept by the stores, not by the meter: on badger, sharded and tiered
// the history rows the badger stores holding them decoded, in full or to
// their temporal skeleton (HistoryRowDecodesForTest +
// HistoryMetaDecodesForTest), each store counted once; on memory, which
// decodes nothing, the history rows and skeletons its read methods handed out
// (HistoryRowsReadForTest + HistoryMetasListedForTest).
func hr2Native(t testing.TB, g *Core, nodes []types.NodeID, rels []types.RelID) int64 {
	t.Helper()
	if ms, ok := g.store.(*memory.Store); ok {
		return ms.HistoryRowsReadForTest() + ms.HistoryMetasListedForTest()
	}
	seen := map[*badger.Store]bool{}
	var sum int64
	add := func(bs *badger.Store, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		if bs == nil || seen[bs] {
			return
		}
		seen[bs] = true
		sum += bs.HistoryRowDecodesForTest() + bs.HistoryMetaDecodesForTest()
	}
	switch st := g.store.(type) {
	case *badger.Store:
		add(st, nil)
	case *shardedpkg.Store:
		for _, id := range nodes {
			add(st.ShardForIDForTest(id.SnowflakeID()))
		}
		for _, id := range rels {
			add(st.ShardForIDForTest(id.SnowflakeID()))
		}
	case *tiered.Store:
		for _, id := range nodes {
			add(st.ShardForNodeIDForTest(id))
		}
		for _, id := range rels {
			add(st.ShardForRelIDForTest(id))
		}
	default:
		t.Fatalf("no native counter for %T", g.store)
	}
	return sum
}

// hr2Read is one read's figures: the answer, the meter's work, the reads the
// head entry answered (Core.headHits) and the native count (hr2Native).
type hr2Read struct {
	answer string
	err    error
	work   storepkg.ReadWork
	hits   uint64
	native int64
}

// hr2Run runs read under ctx's budget (nil meter: the plain door) and records
// its figures.
func hr2Run(t testing.TB, g *Core, node types.NodeID, rel types.RelID, meter *storepkg.ReadMeter, read func() (string, error)) hr2Read {
	t.Helper()
	var nodes []types.NodeID
	var rels []types.RelID
	if node != 0 {
		nodes = append(nodes, node)
	}
	if rel != 0 {
		rels = append(rels, rel)
	}
	hits, native := g.headHits.Load(), hr2Native(t, g, nodes, rels)
	got, err := read()
	r := hr2Read{answer: got, err: err, hits: g.headHits.Load() - hits, native: hr2Native(t, g, nodes, rels) - native}
	if meter != nil {
		r.work = meter.Work()
	}
	return r
}

// hr2Budget runs door d under a fresh budget b.
func hr2Budget(t testing.TB, g *Core, node types.NodeID, rel types.RelID, d hrDoor, b storepkg.ReadBudget) hr2Read {
	t.Helper()
	ctx, meter := storepkg.WithReadBudget(context.Background(), b)
	return hr2Run(t, g, node, rel, meter, func() (string, error) { return d.ctx(ctx) })
}

// hr2Key is the head-cache key door d reads.
func hr2Key(d hrDoor, node types.NodeID, rel types.RelID) headKey {
	if d.name == "rel timeline" || d.name == "rel timelines" {
		return headKey{rel: true, id: rel.SnowflakeID()}
	}
	return headKey{id: node.SnowflakeID()}
}

// hr2Phases runs probe on each backend's fresh graph (pending) and, for a
// persistent one, again after a reopen (flushed, entity caches evicted by the
// probe where it asks).
func hr2Phases(t *testing.T, build func(t *testing.T, g *Core) any, probe func(t *testing.T, be hr2Backend, g *Core, w any, flushed bool)) {
	for _, be := range hr2Backends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			w := build(t, g)
			t.Run("pending", func(t *testing.T) { probe(t, be, g, w, false) })
			shut()
			if !be.persistent {
				return
			}
			g, shut = be.open(t, dir)
			defer shut()
			t.Run("flushed", func(t *testing.T) { probe(t, be, g, w, true) })
		})
	}
}

// hr2World is a node and a relationship of each size (hrNode, hrRel).
type hr2World struct {
	nodes map[int]types.NodeID
	rels  map[int]types.RelID
}

func hr2Build(t *testing.T, g *Core) any {
	w := &hr2World{nodes: map[int]types.NodeID{}, rels: map[int]types.RelID{}}
	for _, n := range []int{10, 80} {
		w.nodes[n], w.rels[n] = hrNode(t, g, n), hrRel(t, g, n)
	}
	return w
}

// hr2MissCost is a miss's HistoryExamined, the same with the rows pending or
// flushed (each row is examined once where it lives, REOPENBUF): N skeletons
// plus two winners; sharded reads the full chain, capture and decode 2N.
func hr2MissCost(be hr2Backend, versions int) uint64 {
	n := uint64(versions - 1) // #nosec G115 -- a test size
	if !hr2Skeletons(be) {
		return 2 * n
	}
	return n + 2
}

// hr2Skeletons reports whether be's fold lists the history skeletons before
// it reads the winners (store.TemporalMetaHistoryCapability).
func hr2Skeletons(be hr2Backend) bool { return be.name != "sharded" }

// hr2MissNative counts actual rows read or decoded, in full or to a skeleton.
// Raw capture and merge examinations do not add native decodes.
func hr2MissNative(be hr2Backend, versions int) int64 {
	n := int64(versions - 1)
	if !hr2Skeletons(be) {
		return n
	}
	return n + 2
}

// TestBudgetedTimeline_HeadMiss (design T6): on every backend, rows pending
// and flushed, each head door after its entity's entry is dropped: the
// unlimited budgeted read gives the plain door's exact answer, is not a head
// hit, and counts each capture, merge and decode phase (hr2MissCost), with
// exact native decodes/listed rows (hr2MissNative); it fills the entry, so the next
// budgeted read is a head hit at the W = 2 winners, the native counter 2.
func TestBudgetedTimeline_HeadMiss(t *testing.T) {
	t.Parallel()
	hr2Phases(t, hr2Build, func(t *testing.T, be hr2Backend, g *Core, wa any, flushed bool) {
		w := wa.(*hr2World)
		pin := g.peekNow()
		for i, d0 := range hrDoors(g, 0, 0, pin)[:5] {
			t.Run(d0.name, func(t *testing.T) {
				for _, n := range []int{10, 80} {
					node, rel := w.nodes[n], w.rels[n]
					d := hrDoors(g, node, rel, pin)[i]
					want, err := d.plain()
					if err != nil || want == "" {
						t.Fatalf("%d versions: plain = %q, %v", n, want, err)
					}
					g.heads.put(hr2Key(d, node, rel), nil) // the entry dropped
					if flushed {
						rbdEvict(t, g, []types.NodeID{node}, []types.RelID{rel})
					}
					miss := hr2Budget(t, g, node, rel, d, storepkg.ReadBudget{})
					if flushed {
						rbdEvict(t, g, []types.NodeID{node}, []types.RelID{rel})
					}
					hit := hr2Budget(t, g, node, rel, d, storepkg.ReadBudget{})
					t.Logf("%d versions: miss HistoryExamined %d, native %d, DecodedBytes %d, hits %d; then hit %d / %d / %d / %d",
						n, miss.work.HistoryExamined, miss.native, miss.work.DecodedBytes, miss.hits, hit.work.HistoryExamined, hit.native, hit.work.DecodedBytes, hit.hits)
					if miss.err != nil || miss.answer != want || miss.hits != 0 {
						t.Fatalf("%d versions: miss = %q, %v, %d head hits; want the plain answer %q, no hit", n, miss.answer, miss.err, miss.hits, want)
					}
					if c, nat := hr2MissCost(be, n), hr2MissNative(be, n); miss.work.HistoryExamined != c || miss.native != nat {
						t.Errorf("%d versions: miss HistoryExamined %d, native %d; want %d and %d", n, miss.work.HistoryExamined, miss.native, c, nat)
					}
					if hit.err != nil || hit.answer != want || hit.hits != 1 || hit.work.HistoryExamined != 2 || hit.native != 2 {
						t.Errorf("%d versions: next read = %q, %v, %d head hits, HistoryExamined %d, native %d; want the plain answer from the entry at 2 and 2",
							n, hit.answer, hit.err, hit.hits, hit.work.HistoryExamined, hit.native)
					}
					if be.name == "memory" && (miss.work.DecodedBytes != 0 || hit.work.DecodedBytes != 0) {
						t.Errorf("memory decoded %d / %d bytes; it decodes nothing", miss.work.DecodedBytes, hit.work.DecodedBytes)
					}
				}
			})
		}
	})
}

// TestBudgetedTimeline_RefusedMissLeavesExactEntry (design T7): on a miss
// under a HistoryExamined limit one below the miss's cost, the read is
// refused. Where the fold reads the winners after the skeleton list (memory,
// badger, tiered) the refusal comes among the winners and the entry the fold
// put before hydrating them is the one a plain fold builds; the retry under
// the same limit is admitted from it (the 2 winners) and gives the plain
// door's answer. On sharded (no skeleton list) the refusal comes inside the
// history list: no fold ran and no entry is kept.
func TestBudgetedTimeline_RefusedMissLeavesExactEntry(t *testing.T) {
	t.Parallel()
	hr2Phases(t, hr2Build, func(t *testing.T, be hr2Backend, g *Core, wa any, flushed bool) {
		w := wa.(*hr2World)
		pin := g.peekNow()
		for i, d0 := range hrDoors(g, 0, 0, pin)[:5] {
			t.Run(d0.name, func(t *testing.T) {
				node, rel := w.nodes[80], w.rels[80]
				d := hrDoors(g, node, rel, pin)[i]
				k := hr2Key(d, node, rel)
				want, err := d.plain()
				if err != nil || want == "" {
					t.Fatalf("plain = %q, %v", want, err)
				}
				fold := g.heads.get(k)
				if fold == nil {
					t.Fatal("the plain door kept no entry")
				}
				g.heads.put(k, nil)
				limit := storepkg.ReadBudget{HistoryExamined: hr2MissCost(be, 80) - 1}
				if flushed {
					rbdEvict(t, g, []types.NodeID{node}, []types.RelID{rel})
				}
				refused := hr2Budget(t, g, node, rel, d, limit)
				if !isBudget(refused.err, "history examined") || refused.answer != "" {
					t.Fatalf("limit %d on a miss = %q, %v; want a refusal and no answer", limit.HistoryExamined, refused.answer, refused.err)
				}
				kept := g.heads.get(k)
				t.Logf("limit %d: refused after %d examined, native %d; entry kept %v", limit.HistoryExamined, refused.work.HistoryExamined, refused.native, kept != nil)
				if !hr2Skeletons(be) {
					if kept != nil {
						t.Fatal("a refusal inside the history list kept an entry")
					}
					return
				}
				if kept == nil {
					t.Fatal("a refusal among the winners kept no entry")
				}
				a, b := *kept, *fold
				a.gens, b.gens = nil, nil
				if !reflect.DeepEqual(a, b) {
					t.Fatalf("the entry a refused read kept differs from the plain fold's\n kept %+v\n fold %+v", a, b)
				}
				if flushed {
					rbdEvict(t, g, []types.NodeID{node}, []types.RelID{rel})
				}
				retry := hr2Budget(t, g, node, rel, d, limit)
				if retry.err != nil || retry.answer != want || retry.hits != 1 || retry.work.HistoryExamined != 2 || retry.native != 2 {
					t.Fatalf("retry = %q, %v, %d head hits, HistoryExamined %d, native %d; want the plain answer %q from the entry at 2 and 2",
						retry.answer, retry.err, retry.hits, retry.work.HistoryExamined, retry.native, want)
				}
			})
		}
	})
}

// TestBudgetedTimeline_HeadWinnerGone (design T8): an entry naming a winner
// the store no longer has (its second history piece points at a version that
// does not exist; the history token still matches) is not a hit under a
// budget: the read gives the plain door's exact answer from the fold and
// counts both what the head path read and the fold's chain (hr2MissCost).
// The head path read the first winner (1) and looked for the gone one, which
// memory counts before the lookup (1) and a disk store's snapshot does not
// (nothing decoded); the native counter sees the first winner and the fold's
// reads (hr2MissNative).
//
// Design L1, the other miss arm: the entry names an existing version that is
// not visible at the pin. On the 80-version entity a version is written after
// the pin (one more in each phase); the entry a plain read at a later pin
// keeps (its second history piece that new version) is edited to answer from
// the earlier pin (maxTx moved down to it). The head path reads the first
// winner and the new version, each counted (2), finds it not visible at the
// pin and folds:
// HistoryExamined is the fold's plus at most W more (2), the native counter
// agrees, a HistoryExamined limit one below the sum refuses, and a second
// read at the pin gives the plain answer again (the edited entry never
// answers it).
func TestBudgetedTimeline_HeadWinnerGone(t *testing.T) {
	t.Parallel()
	hr2Phases(t, hr2Build, func(t *testing.T, be hr2Backend, g *Core, wa any, flushed bool) {
		g.headSuffixOff = true // the counts below are the whole fold's (TestHeadTail_ColdReadExaminesItsSuffix)
		w := wa.(*hr2World)
		pin := g.peekNow()
		for _, c := range []struct {
			name    string
			version uint32 // 0: the version written after the pin
		}{{"gone", 9999}, {"not visible", 0}} {
			versions, pin, fillPin, node, rel := 10, pin, pin, w.nodes[10], w.rels[10]
			if c.version == 0 {
				node, rel = w.nodes[80], w.rels[80]
				pin = g.peekNow()
				time.Sleep(2 * time.Millisecond) // the write's TxFrom lands after the pin
				ctx := context.Background()
				n, err := g.Temporal.SetNodeVersionInterval(ctx, node, 3000, 4000, r10Props(80))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := g.Temporal.SetRelVersionInterval(ctx, rel, 3000, 4000, r10Props(80)); err != nil {
					t.Fatal(err)
				}
				c.version, versions, fillPin = n.Version(), int(n.Version())+1, g.peekNow()
			}
			for i, d0 := range hrDoors(g, 0, 0, pin)[:5] {
				t.Run(c.name+"/"+d0.name, func(t *testing.T) {
					d := hrDoors(g, node, rel, pin)[i]
					k := hr2Key(d, node, rel)
					want, err := d.plain()
					if err != nil || want == "" {
						t.Fatalf("plain = %q, %v", want, err)
					}
					if _, err := hrDoors(g, node, rel, fillPin)[i].plain(); err != nil { // the entry at the head
						t.Fatal(err)
					}
					e := g.heads.get(k)
					if e == nil || len(e.pieces) != 3 {
						t.Fatalf("entry %+v; want 3 pieces", e)
					}
					edited := *e
					edited.pieces = slices.Clone(e.pieces)
					edited.pieces[2].version = c.version // the second history winner (v versions-1) replaced
					if c.version != 9999 {
						if e.pieces[2].version != c.version {
							t.Fatalf("entry at the head %+v; want its second history piece version %d", e.pieces, c.version)
						}
						edited.maxTx, edited.gens = pin, nil // answers from the earlier pin
					}
					g.heads.put(k, &edited)
					if flushed {
						rbdEvict(t, g, []types.NodeID{node}, []types.RelID{rel})
					}
					r := hr2Budget(t, g, node, rel, d, storepkg.ReadBudget{})
					t.Logf("%s: HistoryExamined %d, native %d, DecodedBytes %d, hits %d", c.name, r.work.HistoryExamined, r.native, r.work.DecodedBytes, r.hits)
					if r.err != nil || r.answer != want || r.hits != 0 {
						t.Fatalf("= %q, %v, %d head hits; want the plain answer %q from the fold", r.answer, r.err, r.hits, want)
					}
					head, headNat := uint64(2), int64(2) // the first winner and the replaced one, each read
					if c.version == 9999 {
						head, headNat = 1, 1 // the gone one is not read; memory counts the lookup
						if be.name == "memory" {
							head = 2
						}
					}
					sum := hr2MissCost(be, versions) + head
					if nat := hr2MissNative(be, versions) + headNat; r.work.HistoryExamined != sum || r.native != nat {
						t.Errorf("HistoryExamined %d, native %d; want %d (the head path's, then the fold's) and %d (the head path's rows, then the fold's)",
							r.work.HistoryExamined, r.native, sum, nat)
					}
					if c.version == 9999 {
						if e := g.heads.get(k); e == nil || e.pieces[2].version == c.version {
							t.Error("the fold did not put the exact entry back")
						}
					} else {
						if flushed {
							rbdEvict(t, g, []types.NodeID{node}, []types.RelID{rel})
						}
						if again := hr2Budget(t, g, node, rel, d, storepkg.ReadBudget{}); again.err != nil || again.answer != want {
							t.Errorf("second read = %q, %v; want the plain answer %q", again.answer, again.err, want)
						}
					}
					g.heads.put(k, &edited)
					if flushed {
						rbdEvict(t, g, []types.NodeID{node}, []types.RelID{rel})
					}
					if r := hr2Budget(t, g, node, rel, d, storepkg.ReadBudget{HistoryExamined: sum - 1}); !isBudget(r.err, "history examined") || r.answer != "" {
						t.Errorf("limit %d = %q, %v; want a refusal and no answer", sum-1, r.answer, r.err)
					}
				})
			}
		}
	})
}

// TestBudgetedTimeline_HeadTwoEntitiesOneMeter (design T9): two entities
// read under one meter whose HistoryExamined limit (3) each head read fits
// (2 winners) and the two do not: the first is admitted, the second refused;
// the unlimited control under one meter counts the sum of the two (4), as
// the native counter does.
func TestBudgetedTimeline_HeadTwoEntitiesOneMeter(t *testing.T) {
	t.Parallel()
	hr2Phases(t, hr2Build, func(t *testing.T, be hr2Backend, g *Core, wa any, flushed bool) {
		w := wa.(*hr2World)
		pin := g.peekNow()
		for i, d0 := range hrDoors(g, 0, 0, pin)[:5] {
			t.Run(d0.name, func(t *testing.T) {
				a := hrDoors(g, w.nodes[10], w.rels[10], pin)[i]
				b := hrDoors(g, w.nodes[80], w.rels[80], pin)[i]
				wantA, errA := a.plain()
				wantB, errB := b.plain()
				if errA != nil || errB != nil || wantA == "" || wantB == "" {
					t.Fatalf("plain = %q, %v; %q, %v", wantA, errA, wantB, errB)
				}
				evict := func() {
					if flushed {
						rbdEvict(t, g, []types.NodeID{w.nodes[10], w.nodes[80]}, []types.RelID{w.rels[10], w.rels[80]})
					}
				}
				native := func() int64 {
					return hr2Native(t, g, []types.NodeID{w.nodes[10], w.nodes[80]}, []types.RelID{w.rels[10], w.rels[80]})
				}
				both := func(b0 storepkg.ReadBudget) (string, string, storepkg.ReadWork, int64, error, error) {
					evict()
					ctx, meter := storepkg.WithReadBudget(context.Background(), b0)
					n0 := native()
					gotA, errA := a.ctx(ctx)
					gotB, errB := b.ctx(ctx)
					return gotA, gotB, meter.Work(), native() - n0, errA, errB
				}
				gotA, gotB, work, nat, errA, errB := both(storepkg.ReadBudget{})
				if errA != nil || errB != nil || gotA != wantA || gotB != wantB || work.HistoryExamined != 4 || nat != 4 {
					t.Fatalf("unlimited = %q, %v; %q, %v; HistoryExamined %d, native %d; want the plain answers at 4 and 4", gotA, errA, gotB, errB, work.HistoryExamined, nat)
				}
				gotA, gotB, work, nat, errA, errB = both(storepkg.ReadBudget{HistoryExamined: 3})
				t.Logf("limit 3: first %v, second %v; HistoryExamined %d, native %d", errA, errB, work.HistoryExamined, nat)
				if errA != nil || gotA != wantA || !isBudget(errB, "history examined") || gotB != "" || work.HistoryExamined != 4 || nat != 3 {
					t.Fatalf("limit 3 = %q, %v; %q, %v; HistoryExamined %d, native %d; want the first admitted, the second refused at its second winner (4 counted, 3 read)",
						gotA, errA, gotB, errB, work.HistoryExamined, nat)
				}
			})
		}
	})
}

// hr2Six builds a node of versions rows whose timeline is six segments: one
// row each of [1000, 2000) (the node's current row) ... [5000, 6000), then
// versions-5 history rows of [6000, 7000), each superseding the last.
func hr2Six(tb testing.TB, g *Core, versions int) types.NodeID {
	tb.Helper()
	ctx := context.Background()
	p := r10Props(0)
	p["tkg_valid_from"], p["tkg_valid_to"] = int64(1000), int64(2000)
	n, err := g.Nodes.Add(ctx, []string{"Derived"}, p)
	if err != nil {
		tb.Fatalf("Add: %v", err)
	}
	for i := 1; i < versions; i++ {
		from := types.Instant(1000 * (min(i, 5) + 1))
		if _, err := g.Temporal.SetNodeVersionInterval(ctx, n.ID(), from, from+1000, r10Props(i)); err != nil {
			tb.Fatalf("SetNodeVersionInterval: %v", err)
		}
	}
	return n.ID()
}

// TestBudgetedTimeline_HeadFromReadsLess (design T5): a node whose timeline
// is six segments: the current row (version 0, [1000, 2000), as
// SetNodeVersionInterval keeps it) and five history winners. From 6500 only
// the [6000, 7000) winner reaches; from the entry the From door reads that
// one winner, the whole timeline all five: HistoryExamined and the native
// count strictly below the whole timeline's, the same at 10 and 80 versions,
// the answers the plain doors'.
func TestBudgetedTimeline_HeadFromReadsLess(t *testing.T) {
	t.Parallel()
	hr2Phases(t, func(t *testing.T, g *Core) any {
		return map[int]types.NodeID{10: hr2Six(t, g, 10), 80: hr2Six(t, g, 80)}
	}, func(t *testing.T, be hr2Backend, g *Core, wa any, flushed bool) {
		nodes := wa.(map[int]types.NodeID)
		pin := g.peekNow()
		tp := g.Temporal
		type pair struct{ whole, from hr2Read }
		runs := map[int]pair{}
		for _, n := range []int{10, 80} {
			id := nodes[n]
			whole := hrDoor{name: "node timeline",
				plain: func() (string, error) { s, err := tp.NodeEffectiveTimeline(id, pin); return sbSegs(s), err },
				ctx: func(c context.Context) (string, error) {
					s, err := tp.NodeEffectiveTimelineCtx(c, id, pin)
					return sbSegs(s), err
				}}
			from := hrDoor{name: "node timeline from",
				plain: func() (string, error) { s, err := tp.NodeEffectiveTimelineFrom(id, 6500, pin); return sbSegs(s), err },
				ctx: func(c context.Context) (string, error) {
					s, err := tp.NodeEffectiveTimelineFromCtx(c, id, 6500, pin)
					return sbSegs(s), err
				}}
			var p pair
			for _, d := range []hrDoor{whole, from} {
				want, err := d.plain() // fills the entry
				if err != nil || want == "" {
					t.Fatalf("%d versions, %s: plain = %q, %v", n, d.name, want, err)
				}
				if flushed {
					rbdEvict(t, g, []types.NodeID{id}, nil)
				}
				r := hr2Budget(t, g, id, 0, d, storepkg.ReadBudget{})
				if r.err != nil || r.answer != want || r.hits != 1 {
					t.Fatalf("%d versions, %s: unlimited = %q, %v, %d head hits; want the plain answer %q from the entry", n, d.name, r.answer, r.err, r.hits, want)
				}
				if d.name == "node timeline" {
					p.whole = r
				} else {
					p.from = r
				}
			}
			runs[n] = p
			t.Logf("%d versions: whole HistoryExamined %d, native %d, DecodedBytes %d; from 6500 %d, %d, %d; %s",
				n, p.whole.work.HistoryExamined, p.whole.native, p.whole.work.DecodedBytes, p.from.work.HistoryExamined, p.from.native, p.from.work.DecodedBytes, p.from.answer)
			if p.from.work.HistoryExamined >= p.whole.work.HistoryExamined || p.from.native >= p.whole.native {
				t.Errorf("%d versions: from HistoryExamined %d, native %d; want both strictly below the whole timeline's %d, %d",
					n, p.from.work.HistoryExamined, p.from.native, p.whole.work.HistoryExamined, p.whole.native)
			}
			if p.whole.work.HistoryExamined != 5 || p.whole.native != 5 || p.from.work.HistoryExamined != 1 || p.from.native != 1 {
				t.Errorf("%d versions: whole %d / %d, from %d / %d; want 5 / 5 and 1 / 1 (the winners each reads)",
					n, p.whole.work.HistoryExamined, p.whole.native, p.from.work.HistoryExamined, p.from.native)
			}
		}
		// Forward iterator skips are candidates and depend on key gaps.
		// Hydration, output buffers and the From door remain constant.
		whole10, whole80 := runs[10].whole.work, runs[80].whole.work
		if whole10.Candidates < 1 || whole80.Candidates < 1 {
			t.Fatal("whole timeline did not count its entity candidate")
		}
		whole10.Candidates, whole80.Candidates = 0, 0
		if runs[10].from.work != runs[80].from.work || whole10 != whole80 {
			t.Errorf("work at 10 versions %+v / %+v, at 80 %+v / %+v; want equal hydration and From work", runs[10].whole.work, runs[10].from.work, runs[80].whole.work, runs[80].from.work)
		}
	})
}

// hr2TieredWorld is a tiered graph over two event shards, each holding two
// live nodes and a relationship between them (the first node and the
// relationship updated twice), both shards demoted to cold. Under
// MaxOpenColdShards 0 a cold shard closes as soon as its read is done, so
// every read of an entity reopens its shard; under 1 the shard a read opened
// stays open.
type hr2TieredWorld struct {
	g     *Core
	ts    *tiered.Store
	nodes []types.NodeID // one per shard
	rels  []types.RelID  // one per shard
	cold  []*tiered.EventShard
}

func hr2BuildTieredCold(t *testing.T, maxCold int) *hr2TieredWorld {
	t.Helper()
	ts, err := tiered.New(tiered.Config{
		DataDir:           t.TempDir(),
		RefLabels:         []string{"Ref"},
		ShardWindow:       7 * 24 * time.Hour,
		FlushInterval:     1<<63 - 1,
		MaxOpenColdShards: maxCold,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := New(Config{Store: ts, UntimedValidStart: UntimedFromTxFrom})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close(); _ = ts.Close() })
	ctx := context.Background()
	w := &hr2TieredWorld{g: g, ts: ts}
	const shards = 2
	for i := range shards {
		a, err := g.Nodes.Add(ctx, []string{sbLabel}, map[string]any{"i": int64(i)})
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes.Add(ctx, []string{sbLabel}, map[string]any{"i": int64(i)})
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels.AddByID(ctx, sbType, a.ID(), b.ID(), map[string]any{"i": int64(i)})
		if err != nil {
			t.Fatal(err)
		}
		for v := range 2 {
			if _, err := g.Nodes.Update(ctx, a.ID(), map[string]any{"v": int64(v)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"v": int64(v)}); err != nil {
				t.Fatal(err)
			}
		}
		w.nodes = append(w.nodes, a.ID())
		w.rels = append(w.rels, r.ID())
		time.Sleep(2 * time.Millisecond)
		if err := ts.RotateHotShard(); err != nil {
			t.Fatal(err)
		}
	}
	ts.MuForTest().RLock()
	hot := ts.HotShardForTest()
	for _, es := range ts.EventShardsForTest() {
		if es != hot {
			w.cold = append(w.cold, es)
		}
	}
	ts.MuForTest().RUnlock()
	if len(w.cold) < shards {
		t.Fatalf("%d cold shards, want %d", len(w.cold), shards)
	}
	for _, es := range w.cold {
		demoteToCold(ts, es.Name())
	}
	return w
}

func (w *hr2TieredWorld) closeCold(t *testing.T) {
	t.Helper()
	for _, es := range w.cold {
		closeEventShardStore(t, es)
	}
}

func (w *hr2TieredWorld) opens() uint64 { return w.ts.ShardAccessStats().ColdOpens }

// hr2TieredOpens is the shards one point timeline of a live entity on a
// closed cold shard opens (cap 0: each read reopens it): the current row, the
// presence read, the skeleton list and the two winners. The history token
// opens none: the shard is closed, so there is no token (0) and the read
// folds and cannot reuse an entry with a zero token.
const hr2TieredOpens = 5

// TestBudgetedTimeline_HeadTokenShardOpens (design T4): on a tiered store
// whose cold event shards are closed before each read and close again after
// every read (MaxOpenColdShards 0), a head door over a live entity on a cold
// shard: the unlimited budgeted read gives the plain door's answer and counts
// in ShardOpens exactly the shards the store opened
// (ShardAccessStats.ColdOpens, a counter the meter does not keep), the
// plain door opening as many (hr2TieredOpens: the history token opens none
// and is 0, so no head entry is kept); a ShardOpens limit one below refuses
// with no answer and opens no more than the limit. Two entities on two cold
// shards under one meter whose ShardOpens limit each read fits: the first is
// admitted, the second refused before its first open. With
// MaxOpenColdShards 1 (TestBudgetedTimeline_HeadTokenRetainedShard) the
// entry is kept and the next budgeted read is a head hit that opens nothing.
func TestBudgetedTimeline_HeadTokenShardOpens(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	w := hr2BuildTieredCold(t, 0)
	pin := w.g.peekNow()
	for i, d0 := range hrDoors(w.g, 0, 0, pin)[:5] {
		t.Run(d0.name, func(t *testing.T) {
			d := hrDoors(w.g, w.nodes[0], w.rels[0], pin)[i]
			w.closeCold(t)
			opens := w.opens()
			want, err := d.plain()
			plainOpens := w.opens() - opens
			if err != nil || want == "" {
				t.Fatalf("plain = %q, %v", want, err)
			}
			w.closeCold(t)
			opens = w.opens()
			ctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
			got, err := d.ctx(ctx)
			opened := w.opens() - opens
			t.Logf("plain opened %d; unlimited: %d shards opened, %d counted", plainOpens, opened, meter.Work().ShardOpens)
			if err != nil || got != want || opened != hr2TieredOpens || plainOpens != hr2TieredOpens || meter.Work().ShardOpens != opened {
				t.Fatalf("unlimited = %q, %v, %d shards opened (plain %d), %d counted; want the plain answer %q, %d opens each, every open counted",
					got, err, opened, plainOpens, meter.Work().ShardOpens, want, hr2TieredOpens)
			}
			if e := w.g.heads.get(hr2Key(d, w.nodes[0], w.rels[0])); e != nil {
				t.Fatalf("a head entry kept with the shard closed: %+v; want none (no token)", e)
			}

			w.closeCold(t)
			opens = w.opens()
			ctx, _ = storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{ShardOpens: opened - 1})
			got, err = d.ctx(ctx)
			bounded := w.opens() - opens
			if !isBudget(err, "shard opens") || got != "" || bounded > opened-1 {
				t.Fatalf("ShardOpens %d = %q, %v, %d shards opened; want a refusal, no answer, at most %d opens", opened-1, got, err, bounded, opened-1)
			}

			// Two entities on two cold shards, one meter: each read fits
			// the limit, the two do not.
			e := hrDoors(w.g, w.nodes[1], w.rels[1], pin)[i]
			wantE, err := e.plain()
			if err != nil || wantE == "" {
				t.Fatalf("second entity plain = %q, %v", wantE, err)
			}
			w.closeCold(t)
			opens = w.opens()
			ctx, meter = storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{ShardOpens: opened})
			gotD, errD := d.ctx(ctx)
			gotE, errE := e.ctx(ctx)
			both := w.opens() - opens
			t.Logf("two entities under ShardOpens %d: %v, %v; %d shards opened, %d counted", opened, errD, errE, both, meter.Work().ShardOpens)
			if errD != nil || gotD != want || !isBudget(errE, "shard opens") || gotE != "" || both != opened {
				t.Fatalf("ShardOpens %d over two entities = %q, %v; %q, %v; %d shards opened; want the first admitted, the second refused before its first open (%d opens)",
					opened, gotD, errD, gotE, errE, both, opened)
			}
		})
	}
}

// TestBudgetedTimeline_HeadTokenRetainedShard (design T4, cap 1 variant): the
// cold shard a read opens stays open (MaxOpenColdShards 1), so the history
// token is read from it: the plain door keeps the head entry, and a budgeted
// read of each head door after it is a head hit with the plain answer that
// opens no shard (ShardOpens 0, ColdOpens unchanged).
func TestBudgetedTimeline_HeadTokenRetainedShard(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	w := hr2BuildTieredCold(t, 1)
	pin := w.g.peekNow()
	for i, d0 := range hrDoors(w.g, 0, 0, pin)[:5] {
		t.Run(d0.name, func(t *testing.T) {
			d := hrDoors(w.g, w.nodes[0], w.rels[0], pin)[i]
			w.closeCold(t)
			if _, err := d.plain(); err != nil { // opens the shard, which stays open
				t.Fatal(err)
			}
			want, err := d.plain() // the token read from the open shard: the entry kept
			if err != nil || want == "" {
				t.Fatalf("plain = %q, %v", want, err)
			}
			if w.g.heads.get(hr2Key(d, w.nodes[0], w.rels[0])) == nil {
				t.Fatal("no head entry kept with the shard open")
			}
			opens, hits := w.opens(), w.g.headHits.Load()
			ctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
			got, err := d.ctx(ctx)
			opened, hit := w.opens()-opens, w.g.headHits.Load()-hits
			t.Logf("budgeted, shard retained: %d opened, %d counted, %d head hits", opened, meter.Work().ShardOpens, hit)
			if err != nil || got != want || opened != 0 || meter.Work().ShardOpens != 0 || hit != 1 {
				t.Fatalf("budgeted = %q, %v, %d opened, %d counted, %d head hits; want the plain answer %q from the entry, nothing opened",
					got, err, opened, meter.Work().ShardOpens, hit, want)
			}
		})
	}
}
