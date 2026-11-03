package core

import (
	"context"
	"errors"
	"slices"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// HEADLEND (sigma, 3 Nov): on memory the effective timeline doors hand out
// the stored frozen history row of each winning version, not a per-read
// DeepCopy of it (sigma's paged read-back: 1.2 GB of 5.5 GB). The segment
// row was frozen before as well (temporal/segment.go), so callers see the
// same contract; only the copy is gone.

const headLendVersions = 6

// headLendGraph is a memory graph with a node and a relationship of
// headLendVersions versions, version i valid from 1000*(i+1) with k = i, so
// every version wins one segment.
func headLendGraph(t *testing.T) (*Core, types.NodeID, types.RelID) {
	t.Helper()
	return headLendGraphOn(t, memory.New())
}

func headLendGraphOn(t *testing.T, store storepkg.MandatoryStore) (*Core, types.NodeID, types.RelID) {
	t.Helper()
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()
	// The page door (ForEachNodeEffectiveByLabelAndProperties) reads through
	// a composite index; only the node n carries its values.
	if err := g.Index.CreateComposite("Fact", []string{"ns", "grp"}); err != nil {
		t.Fatal(err)
	}
	n, err := g.Nodes.Add(ctx, []string{"Fact"}, map[string]any{"tkg_valid_from": types.Instant(1000), "k": int64(0), "ns": "n", "grp": "g"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(ctx, []string{"Fact"}, map[string]any{"tkg_valid_from": types.Instant(1000)})
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add(ctx, "R", n, b, map[string]any{"tkg_valid_from": types.Instant(1000), "k": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < headLendVersions; i++ {
		vf := types.Instant(1000 * (i + 1))
		if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"tkg_valid_from": vf, "k": int64(i)}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"tkg_valid_from": vf, "k": int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	return g, n.ID(), r.ID()
}

// checkNodeSegs asserts the exact answer: one segment per version, version
// i from 1000*(i+1), k = i, the last open.
func checkNodeSegs(t *testing.T, door string, segs []temporal.NodeSegment) {
	t.Helper()
	if len(segs) != headLendVersions {
		t.Fatalf("%s: %d segments, want %d", door, len(segs), headLendVersions)
	}
	for i, s := range segs {
		k := propK(s.Node.GetProperty("k"))
		wantTo := types.Instant(1000 * (i + 2))
		if i == headLendVersions-1 {
			wantTo = 0
		}
		if s.ValidFrom != types.Instant(1000*(i+1)) || s.ValidTo != wantTo || k != int64(i) {
			t.Errorf("%s: segment %d = [%d, %d) k=%d (v%d), want [%d, %d) k=%d", door, i, s.ValidFrom, s.ValidTo, k, s.Node.Version(), 1000*(i+1), wantTo, i)
		}
		if !s.Node.IsFrozen() {
			t.Errorf("%s: segment %d row not frozen", door, i)
		}
	}
}

func checkRelSegs(t *testing.T, door string, segs []temporal.RelSegment) {
	t.Helper()
	if len(segs) != headLendVersions {
		t.Fatalf("%s: %d segments, want %d", door, len(segs), headLendVersions)
	}
	for i, s := range segs {
		k := propK(s.Rel.GetProperty("k"))
		if s.ValidFrom != types.Instant(1000*(i+1)) || k != int64(i) {
			t.Errorf("%s: segment %d = [%d, %d) k=%d, want from %d k=%d", door, i, s.ValidFrom, s.ValidTo, k, 1000*(i+1), i)
		}
		if !s.Rel.IsFrozen() {
			t.Errorf("%s: segment %d row not frozen", door, i)
		}
	}
}

// propK is property k as an int64 (-1 when absent).
func propK(v any, ok bool) int64 {
	k, isInt := v.(int64)
	if !ok || !isInt {
		return -1
	}
	return k
}

// copiedNodeRows counts the history segments (all but the current row) whose
// row is a different pointer in b than in a: a copy per read.
func copiedNodeRows(a, b []temporal.NodeSegment) int {
	n := 0
	for i := 0; i < len(a)-1 && i < len(b)-1; i++ {
		if a[i].Node != b[i].Node {
			n++
		}
	}
	return n
}

func copiedRelRows(a, b []temporal.RelSegment) int {
	n := 0
	for i := 0; i < len(a)-1 && i < len(b)-1; i++ {
		if a[i].Rel != b[i].Rel {
			n++
		}
	}
	return n
}

// TestEffectiveDoorsLendHistoryRows: every effective door hands out the same
// stored row for a history winner on two reads (no copy), the answers are
// exact, and the rows are frozen. Mutations: GetNodeVersion's DeepCopy at
// any of the six hydration sites makes the pointers differ.
func TestEffectiveDoorsLendHistoryRows(t *testing.T) {
	t.Parallel()
	g, nid, rid := headLendGraph(t)
	pin := g.readNow()

	nodeDoors := map[string]func() []temporal.NodeSegment{
		"NodeEffectiveTimeline": func() []temporal.NodeSegment {
			s, err := g.Temporal.NodeEffectiveTimeline(nid, pin)
			if err != nil {
				t.Fatal(err)
			}
			return s
		},
		"NodeEffectiveTimelineFrom": func() []temporal.NodeSegment {
			s, err := g.Temporal.NodeEffectiveTimelineFrom(nid, 0, pin)
			if err != nil {
				t.Fatal(err)
			}
			return s
		},
		"NodeEffectiveTimelines": func() []temporal.NodeSegment {
			s, err := g.Temporal.NodeEffectiveTimelines(nid, []types.Instant{pin})
			if err != nil || len(s) != 1 {
				t.Fatal(s, err)
			}
			return s[0]
		},
		"ForEachNodeEffective": func() []temporal.NodeSegment {
			var out []temporal.NodeSegment
			if err := g.Temporal.ForEachNodeEffective(pin, func(s temporal.NodeSegment) bool {
				if s.Node.ID() == nid {
					out = append(out, s)
				}
				return true
			}); err != nil {
				t.Fatal(err)
			}
			return out
		},
		"ForEachNodeEffectiveByLabelAndProperties": func() []temporal.NodeSegment {
			var out []temporal.NodeSegment
			calls := 0
			if _, err := g.Temporal.ForEachNodeEffectiveByLabelAndProperties(context.Background(), "Fact",
				map[string]any{"ns": "n", "grp": "g"}, pin, temporal.EffectivePageOpts{},
				func(id types.NodeID, segs []temporal.NodeSegment) bool {
					calls++
					if id == nid {
						out = segs
					}
					return true
				}); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("page door matched %d nodes, want 1", calls)
			}
			return out
		},
	}
	for name, read := range nodeDoors {
		a, b := read(), read()
		checkNodeSegs(t, name, a)
		checkNodeSegs(t, name, b)
		if n := copiedNodeRows(a, b); n != 0 {
			t.Errorf("%s: %d of %d history rows copied per read, want lent", name, n, headLendVersions-1)
		}
	}
	// The singular door's second read above was a head hit (nodeHeadSegments).
	if g.headHits.Load() == 0 {
		t.Fatal("no head hit: the head path went untested")
	}

	relDoors := map[string]func() []temporal.RelSegment{
		"RelEffectiveTimeline": func() []temporal.RelSegment {
			s, err := g.Temporal.RelEffectiveTimeline(rid, pin)
			if err != nil {
				t.Fatal(err)
			}
			return s
		},
		"RelEffectiveTimelines": func() []temporal.RelSegment {
			s, err := g.Temporal.RelEffectiveTimelines(rid, []types.Instant{pin})
			if err != nil || len(s) != 1 {
				t.Fatal(s, err)
			}
			return s[0]
		},
		"ForEachRelEffective": func() []temporal.RelSegment {
			var out []temporal.RelSegment
			if err := g.Temporal.ForEachRelEffective(pin, func(s temporal.RelSegment) bool {
				if s.Rel.ID() == rid {
					out = append(out, s)
				}
				return true
			}); err != nil {
				t.Fatal(err)
			}
			return out
		},
	}
	hits := g.headHits.Load()
	for name, read := range relDoors {
		a, b := read(), read()
		checkRelSegs(t, name, a)
		checkRelSegs(t, name, b)
		if n := copiedRelRows(a, b); n != 0 {
			t.Errorf("%s: %d of %d history rows copied per read, want lent", name, n, headLendVersions-1)
		}
	}
	if g.headHits.Load() == hits {
		t.Fatal("no rel head hit: relHeadSegments went untested")
	}
}

// TestEffectiveLentRowsRefuseMutation: a lent history row refuses a write,
// and neither the attempt nor a later write changes what the store or a
// later read answers (the two-phase check: read, write, read again).
func TestEffectiveLentRowsRefuseMutation(t *testing.T) {
	t.Parallel()
	g, nid, _ := headLendGraph(t)
	pin := g.readNow()
	segs, err := g.Temporal.NodeEffectiveTimeline(nid, pin)
	if err != nil {
		t.Fatal(err)
	}
	row := segs[0].Node
	if err := row.SetProperty("k", int64(99)); !errors.Is(err, types.ErrFrozenNode) {
		t.Fatalf("SetProperty on a lent row: %v, want ErrFrozenNode", err)
	}
	if _, err := g.Nodes.Update(context.Background(), nid, map[string]any{"tkg_valid_from": types.Instant(50_000), "k": int64(50)}); err != nil {
		t.Fatal(err)
	}
	if k := propK(row.GetProperty("k")); k != 0 {
		t.Fatalf("the lent row changed after a write: k = %v", k)
	}
	again, err := g.Temporal.NodeEffectiveTimeline(nid, pin)
	if err != nil {
		t.Fatal(err)
	}
	checkNodeSegs(t, "NodeEffectiveTimeline at the old pin", again)
	v, err := g.store.GetNodeVersion(nid, row.Version())
	if err != nil {
		t.Fatal(err)
	}
	if v.IsFrozen() || v == row {
		t.Fatal("GetNodeVersion no longer returns the caller's own mutable copy")
	}
	if err := v.SetProperty("k", int64(7)); err != nil {
		t.Fatalf("GetNodeVersion copy refused a write: %v", err)
	}
	if k := propK(row.GetProperty("k")); k != 0 {
		t.Fatalf("a write to GetNodeVersion's copy reached the stored row: k = %v", k)
	}
	// The point state doors keep returning the caller's own mutable row.
	at, err := g.Temporal.NodeAtTx(nid, 1500, pin)
	if err != nil {
		t.Fatal(err)
	}
	if at == row || at.IsFrozen() || propK(at.GetProperty("k")) != 0 {
		t.Fatalf("NodeAtTx = %p frozen=%v; want a mutable copy of version 1", at, at.IsFrozen())
	}
	_, rid := headLendRel(t, g)
	rat, err := g.Temporal.RelAtTx(rid, 1500, g.readNow())
	if err != nil {
		t.Fatal(err)
	}
	if rat.IsFrozen() || propK(rat.GetProperty("k")) != 0 {
		t.Fatalf("RelAtTx frozen=%v; want a mutable copy of version 1", rat.IsFrozen())
	}
}

// headLendRel is headLendGraph's relationship (the first one stored).
func headLendRel(t *testing.T, g *Core) (*types.Relationship, types.RelID) {
	t.Helper()
	var out *types.Relationship
	if err := g.Temporal.ForEachRelEffective(g.readNow(), func(s temporal.RelSegment) bool {
		out = s.Rel
		return false
	}); err != nil || out == nil {
		t.Fatal("no relationship", err)
	}
	return out, out.ID()
}

// metaWrapStore is a memory store behind a wrapper: not trusted (its rows
// are validated and copied), but declaring the skeleton reads so the
// effective doors take the hydrating path. It inherits LendNodeVersion.
type metaWrapStore struct{ *memory.Store }

func (s *metaWrapStore) NodeHistoryTemporalMeta(id types.NodeID) ([]storepkg.VersionTemporalMeta, error) {
	return s.Store.NodeHistoryTemporalMeta(id)
}

func (s *metaWrapStore) RelHistoryTemporalMeta(id types.RelID) ([]storepkg.VersionTemporalMeta, error) {
	return s.Store.RelHistoryTemporalMeta(id)
}

// TestEffectiveUntrustedStoreCopiesVersions: a store the core does not trust
// lends nothing, even when it offers the lending capability: each read hands
// out its own validated copy. Mutation: dropping the trust check in
// lentNodeVersion / lentRelVersion lends the wrapper's rows.
func TestEffectiveUntrustedStoreCopiesVersions(t *testing.T) {
	t.Parallel()
	g, nid, rid := headLendGraphOn(t, &metaWrapStore{memory.New()})
	if g.storeRowsTrust || g.temporalMetaHistory == nil {
		t.Fatalf("trust=%v meta=%v: the wrapper does not test the untrusted hydrating path", g.storeRowsTrust, g.temporalMetaHistory != nil)
	}
	pin := g.readNow()
	read := func() []temporal.NodeSegment {
		s, err := g.Temporal.NodeEffectiveTimelines(nid, []types.Instant{pin})
		if err != nil {
			t.Fatal(err)
		}
		return s[0]
	}
	a, b := read(), read()
	checkNodeSegs(t, "untrusted NodeEffectiveTimelines", a)
	if n := copiedNodeRows(a, b); n != headLendVersions-1 {
		t.Errorf("untrusted store: %d of %d history rows copied, want all", n, headLendVersions-1)
	}
	readRel := func() []temporal.RelSegment {
		s, err := g.Temporal.RelEffectiveTimelines(rid, []types.Instant{pin})
		if err != nil {
			t.Fatal(err)
		}
		return s[0]
	}
	ra, rb := readRel(), readRel()
	checkRelSegs(t, "untrusted RelEffectiveTimelines", ra)
	if n := copiedRelRows(ra, rb); n != headLendVersions-1 {
		t.Errorf("untrusted store: %d of %d rel history rows copied, want all", n, headLendVersions-1)
	}
}

// TestEffectiveLentReadsCounted: a lent version read is a history read of one
// row (Core.historyRead, the seam the growth tests count folds with): a
// head-hit read of each entity reports one per history winner.
func TestEffectiveLentReadsCounted(t *testing.T) {
	g, nid, rid := headLendGraph(t)
	pin := g.readNow()
	if _, err := g.Temporal.NodeEffectiveTimeline(nid, pin); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Temporal.RelEffectiveTimeline(rid, pin); err != nil {
		t.Fatal(err)
	}
	var reads []int
	g.historyRead = func(rows int) { reads = append(reads, rows) }
	defer func() { g.historyRead = nil }()
	if _, err := g.Temporal.NodeEffectiveTimeline(nid, pin); err != nil {
		t.Fatal(err)
	}
	if want := slices.Repeat([]int{1}, headLendVersions-1); !slices.Equal(reads, want) {
		t.Errorf("node head read reported history reads %v, want %v", reads, want)
	}
	reads = nil
	if _, err := g.Temporal.RelEffectiveTimeline(rid, pin); err != nil {
		t.Fatal(err)
	}
	if want := slices.Repeat([]int{1}, headLendVersions-1); !slices.Equal(reads, want) {
		t.Errorf("rel head read reported history reads %v, want %v", reads, want)
	}
	// A failed lend is no read (the seam counts rows returned).
	reads = nil
	rd := entityReads{c: g}
	if _, err := rd.lentNodeVersion(nid, 99); !errors.Is(err, storepkg.ErrVersionNotFound) {
		t.Fatalf("lentNodeVersion(missing) = %v", err)
	}
	if _, err := rd.lentRelVersion(rid, 99); !errors.Is(err, storepkg.ErrVersionNotFound) {
		t.Fatalf("lentRelVersion(missing) = %v", err)
	}
	if len(reads) != 0 {
		t.Errorf("failed lends reported history reads %v, want none", reads)
	}
}

// markSnap is a read snapshot answering every version read with its marker
// rows (the rest of the interface is never called).
type markSnap struct {
	storepkg.ReadSnapshot
	n *types.Node
	r *types.Relationship
}

func (s markSnap) GetNodeVersion(types.NodeID, uint32) (*types.Node, error)       { return s.n, nil }
func (s markSnap) GetRelVersion(types.RelID, uint32) (*types.Relationship, error) { return s.r, nil }

// TestEffectiveLendReadsThroughSnapshot: a read through a store snapshot
// takes its versions from the snapshot, not from the store's lending
// capability (a scan's rows come from one moment). Mutation: lentNodeVersion /
// lentRelVersion lending from the store while a snapshot is set.
func TestEffectiveLendReadsThroughSnapshot(t *testing.T) {
	t.Parallel()
	g, nid, rid := headLendGraph(t)
	snap := markSnap{n: types.NewNode(nid, 0, nil), r: types.NewRelationship(rid, 0, 0, 0)}
	rd := entityReads{c: g, snap: snap}
	if n, err := rd.lentNodeVersion(nid, 1); err != nil || n != snap.n {
		t.Errorf("lentNodeVersion through a snapshot = %p, %v; want the snapshot's row", n, err)
	}
	if r, err := rd.lentRelVersion(rid, 1); err != nil || r != snap.r {
		t.Errorf("lentRelVersion through a snapshot = %p, %v; want the snapshot's row", r, err)
	}
}

// TestEffectiveHeadReadAllocs: a head-hit read of a node with
// headLendVersions-1 history winners allocates no row. Before HEADLEND each
// winner's DeepCopy cost 4 allocations (row, properties, temporal block,
// integrity): 25 a read here, 5 after it (the segment slice and fixed
// overhead, independent of the versions). The threshold leaves 3 of slack
// and still fails on a single copied winner.
func TestEffectiveHeadReadAllocs(t *testing.T) {
	g, nid, _ := headLendGraph(t)
	pin := g.readNow()
	if _, err := g.Temporal.NodeEffectiveTimeline(nid, pin); err != nil { // fills the head entry
		t.Fatal(err)
	}
	hits := g.headHits.Load()
	allocs := testing.AllocsPerRun(50, func() {
		if _, err := g.Temporal.NodeEffectiveTimeline(nid, pin); err != nil {
			t.Fatal(err)
		}
	})
	if g.headHits.Load() == hits {
		t.Fatal("no head hit")
	}
	t.Logf("head-hit NodeEffectiveTimeline, %d versions: %.0f allocs/read", headLendVersions, allocs)
	if limit := float64(8); allocs > limit {
		t.Errorf("%.0f allocs per head read, want <= %.0f (a copy per history winner)", allocs, limit)
	}
}

// TestEffectiveLentRowsSurviveOverwriteAndTrim (run it with -race): readers
// holding lent history rows keep reading them while a writer overwrites one
// of those versions in the store and then trims the history; the lent rows
// never change (the store replaces or drops a row, never writes it), and the
// store answers the overwrite afterwards.
func TestEffectiveLentRowsSurviveOverwriteAndTrim(t *testing.T) {
	t.Parallel()
	g, nid, _ := headLendGraph(t)
	ms := g.store.(*memory.Store)
	pin := g.readNow()
	segs, err := g.Temporal.NodeEffectiveTimeline(nid, pin)
	if err != nil {
		t.Fatal(err)
	}
	checkNodeSegs(t, "before", segs)
	held := segs[1].Node // version 2, k = 1
	if lent, err := ms.LendNodeVersion(nid, held.Version()); err != nil || lent != held {
		t.Fatalf("segment row %p is not the stored row (%p, %v): nothing is lent", held, lent, err)
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	const readers = 4
	bad := make(chan string, readers)
	for range readers {
		go func() {
			defer func() { done <- struct{}{} }()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i, s := range segs[:headLendVersions-1] {
					if k := propK(s.Node.GetProperty("k")); k != int64(i) {
						bad <- "lent row changed under a write"
						return
					}
					if f, _, ok := s.Node.ValidRange(); !ok || f != types.Instant(1000*(i+1)) {
						bad <- "lent row's valid start changed under a write"
						return
					}
				}
			}
		}()
	}
	over := held.DeepCopy()
	if err := over.SetProperty("k", int64(77)); err != nil {
		t.Fatal(err)
	}
	for range 200 {
		if err := ms.PutNodeVersion(nid, held.Version(), over); err != nil {
			t.Fatal(err)
		}
	}
	if err := ms.TrimNodeHistoryFrom(nid, held.Version()); err != nil {
		t.Fatal(err)
	}
	close(stop)
	for range readers {
		<-done
	}
	select {
	case msg := <-bad:
		t.Fatal(msg)
	default:
	}
	checkNodeSegs(t, "held after the writes", segs)
	if _, err := ms.LendNodeVersion(nid, held.Version()); !errors.Is(err, storepkg.ErrVersionNotFound) {
		t.Fatalf("trimmed version still lent: %v", err)
	}
}
