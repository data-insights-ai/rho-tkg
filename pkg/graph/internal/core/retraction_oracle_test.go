package core

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"sort"
	"strconv"
	"testing"
	"time"

	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRetractionOracle_NeverRecordedWorld is the brute-force oracle of
// backlog 43, written apart from the resolver: it never asks how a retraction
// is resolved, only what it MEANS.
//
// Definition: a retraction at T of a life L says L was never true. So at any
// pin p, the graph must answer exactly what a graph answers that holds the
// same rows except that
//
//   - every life retracted at T <= p was never recorded at all (its rows are
//     physically absent), and
//   - every life retracted at T > p was ended by a plain Delete at T (its
//     tombstone without the marker: at p < T the retraction is not known).
//
// That second graph ("the never-recorded world") is built from the first
// one's export, rows removed or unmarked directly at the store, so it holds
// NO retraction marker and is answered by the Delete semantics alone — which
// the bitemporal and W5 oracles pin independently. Every state door (point,
// interval, generic ByLabel/ByType with ValidAt+TxAt, the effective
// timelines) and record door (as-of sets) is compared at every pin of the
// sequence and a grid of valid instants.
//
// The generator draws creates, links, updates, cascades (corrections below
// the first valid-from included), deletes, re-imports of deleted IDs valid
// from below their earlier life (a life whose start lies inside another), and
// node / relationship retractions through every door family, plain and at a
// caller instant — so a retracted life sits next to an earlier life it
// superseded while it was believed. Catches: a retraction capped rather than
// dropped (it still supersedes or bounds another life), a marker leaking into
// a pre-T answer, a door that disagrees with the definition anywhere.
func TestRetractionOracle_NeverRecordedWorld(t *testing.T) {
	t.Parallel()
	seeds, nOps := 6, 36
	if !testing.Short() {
		seeds, nOps = 16, 44
	}
	if isRaceEnabled() {
		seeds = 3
	}
	// A longer sweep: RT_ORACLE_SEEDS=200 go test -run TestRetractionOracle_NeverRecordedWorld
	if n, err := strconv.Atoi(os.Getenv("RT_ORACLE_SEEDS")); err == nil && n > 0 {
		seeds = n
	}
	const base uint64 = 0x43_2E_7AC7 // "43.retract"
	x0 := types.Instant(time.Now().Add(time.Hour).UnixMilli()/1_000_000*1_000_000 + 1_000_000)
	var retractions, lives, compared int
	for _, be := range txbBackends() {
		for i := 0; i < seeds; i++ {
			seed := base + uint64(i)
			t.Run(fmt.Sprintf("%s/seed=%d", be.name, seed), func(t *testing.T) {
				r, l, c := runNeverRecorded(t, be, seed, nOps, x0)
				retractions += r
				lives += l
				compared += c
			})
		}
	}
	if t.Failed() {
		return
	}
	if retractions == 0 || lives == 0 {
		t.Fatalf("generator drew %d retractions, %d retractions next to an earlier life: the oracle exercised nothing", retractions, lives)
	}
	t.Logf("%d retractions (%d next to an earlier life), %d answers compared", retractions, lives, compared)
}

// rtoRetraction is one retracted life: the entity, the instant, and whether
// the entity had an earlier life.
type rtoRetraction struct {
	isNode bool
	nid    types.NodeID
	rid    types.RelID
	at     types.Instant
}

type rtoWorld struct {
	*txbOracle
	retracted   []rtoRetraction
	retractedID map[int64]bool
}

func runNeverRecorded(t *testing.T, be txbBackend, seed uint64, nOps int, x0 types.Instant) (nRetract, nLives, nCompared int) {
	g := be.open(t, true)
	clk := &switchableClock{}
	g.SetClockForTest(t, clk.Now)
	rng := rand.New(rand.NewPCG(seed, seed^0x6A09E667F3BCC909))
	o := &rtoWorld{
		txbOracle:   &txbOracle{world: newWorld(t, g, rng), be: be.name, clk: clk, accepted: map[string]int{}},
		retractedID: map[int64]bool{},
	}
	o.x = x0
	o.tick()
	o.setup()
	for i := 0; i < nOps; i++ {
		o.tick()
		switch o.rng.IntN(12) {
		case 0:
			o.addNode()
		case 1:
			o.linkNodes()
		case 2:
			o.updateNode()
		case 3:
			o.updateRel()
		case 4:
			o.cascadeNode()
		case 5:
			o.cascadeRel()
		case 6:
			o.deleteNode()
		case 7:
			o.deleteRel()
		case 8, 9:
			o.reimport()
		case 10:
			o.retractNode()
		default:
			o.retractRel()
		}
	}
	// Make sure every seed holds a retraction next to an earlier life: delete,
	// re-import below the first valid-from, update inside the earlier span,
	// retract.
	o.tick()
	o.livesFixture()
	o.tick()

	ret := o.g
	var exported bytes.Buffer
	if err := ret.IO.Export(&exported); err != nil {
		t.Fatalf("Export: %v", err)
	}
	sort.Slice(o.retracted, func(i, j int) bool { return o.retracted[i].at < o.retracted[j].at })
	snap := o.capture()
	for _, r := range o.retracted {
		if o.hadEarlierLife(snap, r) {
			nLives++
		}
	}

	// The pins: every recorded instant of every row and ±1, each retraction's
	// T-1 / T, 0 (no tx filter) and the clock. The valid grid: every valid
	// stamp of every row and ±1 (sampled to bound the cost), plus 1.
	pinSet := map[types.Instant]bool{0: true}
	validSet := map[types.Instant]bool{1: true}
	for _, e := range append(entities(snap.nodes), entities(snap.rels)...) {
		for _, r := range e.rows {
			for _, v := range []types.Instant{r.txFrom, r.txTo, r.deletedAt, r.updatedAt} {
				if v > 0 {
					pinSet[v-1], pinSet[v] = true, true
				}
			}
			for _, v := range []types.Instant{r.validFrom, r.validTo, r.updatedAt} {
				if v > 1 {
					validSet[v-1], validSet[v] = true, true
				}
			}
		}
		validSet[e.sfFallback] = true
	}
	peek, err := ret.Temporal.PeekTx()
	if err != nil {
		t.Fatalf("PeekTx: %v", err)
	}
	pinSet[peek] = true
	for p := range pinSet {
		if p > peek {
			delete(pinSet, p) // the timelines refuse a pin ahead of the commit clock
		}
	}
	pins := sortedInstants(pinSet)
	valids := sortedInstants(validSet)
	if len(valids) > 40 {
		sampled := make([]types.Instant, 0, 40)
		for i := 0; i < 40; i++ {
			sampled = append(sampled, valids[i*len(valids)/40])
		}
		valids = sampled
	}
	if len(pins) > 60 {
		keep := map[types.Instant]bool{0: true, peek: true}
		for _, r := range o.retracted {
			keep[r.at-1], keep[r.at] = true, true
		}
		for i := 0; i < 50; i++ {
			keep[pins[i*len(pins)/50]] = true
		}
		pins = sortedInstants(keep)
	}

	alts := map[int]*Core{}
	altFor := func(k int) *Core {
		if a, ok := alts[k]; ok {
			return a
		}
		a := o.neverRecordedWorld(exported.Bytes(), k, clk)
		alts[k] = a
		return a
	}
	nodeIDs, relIDs := o.allNodeIDs(), o.relIDs
	for _, pin := range pins {
		k := len(o.retracted)
		if pin > 0 {
			k = sort.Search(len(o.retracted), func(i int) bool { return o.retracted[i].at > pin })
		}
		alt := altFor(k)
		got := rtoRender(t, ret, pin, valids, nodeIDs, relIDs)
		want := rtoRender(t, alt, pin, valids, nodeIDs, relIDs)
		if len(got) != len(want) {
			t.Fatalf("pin %d: %d answers, never-recorded world %d", pin, len(got), len(want))
		}
		for i := range got {
			nCompared++
			if got[i] != want[i] {
				o.fail("pin %d (retractions known: %d of %d):\n got  %s\n want %s\n retractions %v", pin, k, len(o.retracted), got[i], want[i], o.retracted)
			}
		}
	}
	return len(o.retracted), nLives, nCompared
}

// nodeRetractionInstant is the instant of the retraction tombstone in a
// node's history (a cascade row may sit above it in version order).
func nodeRetractionInstant(t *testing.T, g *Core, id types.NodeID) types.Instant {
	t.Helper()
	h, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	var at types.Instant
	for _, r := range h {
		if tm := r.Temporal(); tm != nil && tm.Retracted {
			at = max(at, tm.DeletedAt)
		}
	}
	if at == 0 {
		t.Fatalf("node %v: no retraction tombstone", id)
	}
	return at
}

func relRetractionInstant(t *testing.T, g *Core, id types.RelID) types.Instant {
	t.Helper()
	h, err := g.Rels.History(id)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	var at types.Instant
	for _, r := range h {
		if tm := r.Temporal(); tm != nil && tm.Retracted {
			at = max(at, tm.DeletedAt)
		}
	}
	if at == 0 {
		t.Fatalf("relationship %v: no retraction tombstone", id)
	}
	return at
}

func entities[K comparable](m map[K]*oracleEntity) []*oracleEntity {
	out := make([]*oracleEntity, 0, len(m))
	for _, e := range m {
		out = append(out, e)
	}
	return out
}

func sortedInstants(set map[types.Instant]bool) []types.Instant {
	out := make([]types.Instant, 0, len(set))
	for v := range set {
		if v >= 0 {
			out = append(out, v)
		}
	}
	slices.Sort(out)
	return out
}

// hadEarlierLife: the retracted entity's chain holds a plain delete's
// tombstone (an earlier life).
func (o *rtoWorld) hadEarlierLife(s *snapshot, r rtoRetraction) bool {
	var e *oracleEntity
	if r.isNode {
		e = s.nodes[r.nid]
	} else {
		e = s.rels[r.rid]
	}
	for _, row := range e.rows {
		if row.deletedAt != 0 && !row.retracted {
			return true
		}
	}
	return false
}

// reimport brings a deleted (never retracted) node or relationship back as a
// new life, valid from a random instant that may lie inside or below its
// earlier life.
func (o *rtoWorld) reimport() {
	if o.rng.IntN(2) == 0 {
		for _, id := range o.nodeIDs {
			if o.nodeAlive[id] || o.retractedID[int64(id)] {
				continue
			}
			_, err := o.g.Nodes.Import(o.ctx, id, o.pickLabels(), map[string]any{"tkg_valid_from": o.cascadeVF(), "k": o.seqno})
			o.seqno++
			o.record(fmt.Sprintf("reimportNode(%v)", id), err)
			if err == nil {
				o.nodeAlive[id] = true
			}
			return
		}
		return
	}
	for _, id := range o.relIDs {
		if o.retractedID[int64(id)] {
			continue
		}
		if _, err := o.g.Rels.Get(o.ctx, id); err == nil {
			continue
		}
		h, err := o.g.Rels.History(id)
		if err != nil || len(h) == 0 {
			continue
		}
		s, err1 := o.g.Nodes.Get(o.ctx, h[0].StartNodeID())
		e, err2 := o.g.Nodes.Get(o.ctx, h[0].EndNodeID())
		if err1 != nil || err2 != nil {
			continue
		}
		_, err = o.g.Rels.Import(o.ctx, id, o.relType[id], s, e, map[string]any{"tkg_valid_from": o.cascadeVF(), "k": o.seqno})
		o.seqno++
		o.record(fmt.Sprintf("reimportRel(%v)", id), err)
		if err == nil {
			o.relAlive[id] = true
		}
		return
	}
}

// retractNode retracts a live regular node through a random door family,
// plain or at a placeable caller instant, and records every life it ends.
func (o *rtoWorld) retractNode() {
	alive := o.aliveNodes()
	if len(alive) == 0 {
		return
	}
	id := alive[o.rng.IntN(len(alive))]
	hood := txbHoodRels(o.t, o.g, id)
	ents := []*oracleEntity{captureNode(o.t, o.g, id)}
	for _, r := range hood {
		ents = append(ents, captureRel(o.t, o.g, r, ""))
	}
	withTx := o.rng.IntN(2) == 0
	d := rtxbDoor(o.rng.IntN(5), withTx)
	var at types.Instant
	if withTx {
		lo, _ := txbBounds(ents...)
		if lo >= o.x {
			return
		}
		at = lo + 1 + types.Instant(o.rng.Int64N(int64(o.x-lo)))
	}
	err := d.node(o.t, o.g, id, at)
	o.record(fmt.Sprintf("%s/RetractNode(%v,t=%d) hood=%v", d.name, id, at, hood), err)
	if err != nil {
		o.fail("RetractNode: %v", err)
	}
	o.nodeAlive[id] = false
	at = nodeRetractionInstant(o.t, o.g, id)
	o.retracted = append(o.retracted, rtoRetraction{isNode: true, nid: id, at: at})
	o.retractedID[int64(id)] = true
	for _, r := range hood {
		o.relAlive[r] = false
		o.retracted = append(o.retracted, rtoRetraction{rid: r, at: at})
		o.retractedID[int64(r)] = true
	}
}

func (o *rtoWorld) retractRel() {
	live := o.liveRels()
	if len(live) == 0 {
		return
	}
	id := live[o.rng.IntN(len(live))]
	withTx := o.rng.IntN(2) == 0
	d := rtxbDoor(o.rng.IntN(5), withTx)
	var at types.Instant
	if withTx {
		lo, _ := txbBounds(captureRel(o.t, o.g, id, ""))
		if lo >= o.x {
			return
		}
		at = lo + 1 + types.Instant(o.rng.Int64N(int64(o.x-lo)))
	}
	err := d.rel(o.t, o.g, id, at)
	o.record(fmt.Sprintf("%s/RetractRelationship(%v,t=%d)", d.name, id, at), err)
	if err != nil {
		o.fail("RetractRelationship: %v", err)
	}
	o.relAlive[id] = false
	o.retracted = append(o.retracted, rtoRetraction{rid: id, at: relRetractionInstant(o.t, o.g, id)})
	o.retractedID[int64(id)] = true
}

// livesFixture: a node and a relationship each with an earlier life the
// retracted one superseded while it was believed (see
// TestRetraction_RetractedLifeShapesNoOtherLife).
func (o *rtoWorld) livesFixture() {
	n, err := o.g.Nodes.Add(o.ctx, []string{"A"}, map[string]any{"k": -1, "tkg_valid_from": types.Instant(1000)})
	if err != nil {
		o.fail("fixture Add: %v", err)
	}
	r, err := o.g.Rels.AddByID(o.ctx, "R", o.anchors[0], o.anchors[1], map[string]any{"k": -1, "tkg_valid_from": types.Instant(1000)})
	if err != nil {
		o.fail("fixture AddByID: %v", err)
	}
	o.nodeIDs = append(o.nodeIDs, n.ID())
	o.relIDs = append(o.relIDs, r.ID())
	o.relType[r.ID()] = "R"
	o.tick()
	if err := o.g.Nodes.Delete(o.ctx, n.ID()); err != nil {
		o.fail("fixture Delete: %v", err)
	}
	if err := o.g.Rels.Delete(o.ctx, r.ID()); err != nil {
		o.fail("fixture Rels.Delete: %v", err)
	}
	o.tick()
	if _, err := o.g.Nodes.Import(o.ctx, n.ID(), []string{"A"}, map[string]any{"k": -2, "tkg_valid_from": types.Instant(500)}); err != nil {
		o.fail("fixture Import: %v", err)
	}
	s, _ := o.g.Nodes.Get(o.ctx, o.anchors[0])
	e, _ := o.g.Nodes.Get(o.ctx, o.anchors[1])
	if _, err := o.g.Rels.Import(o.ctx, r.ID(), "R", s, e, map[string]any{"k": -2, "tkg_valid_from": types.Instant(500)}); err != nil {
		o.fail("fixture Rels.Import: %v", err)
	}
	o.tick()
	if _, err := o.g.Nodes.Update(o.ctx, n.ID(), map[string]any{"k": -3, "tkg_valid_from": types.Instant(2000)}); err != nil {
		o.fail("fixture Update: %v", err)
	}
	if _, err := o.g.Rels.Update(o.ctx, r.ID(), map[string]any{"k": -3, "tkg_valid_from": types.Instant(2000)}); err != nil {
		o.fail("fixture Rels.Update: %v", err)
	}
	o.tick()
	if err := o.g.Nodes.Retract(o.ctx, n.ID()); err != nil {
		o.fail("fixture Retract: %v", err)
	}
	o.retracted = append(o.retracted, rtoRetraction{isNode: true, nid: n.ID(), at: nodeRetractionInstant(o.t, o.g, n.ID())})
	o.retractedID[int64(n.ID())] = true
	o.tick()
	if err := o.g.Rels.Retract(o.ctx, r.ID()); err != nil {
		o.fail("fixture Rels.Retract: %v", err)
	}
	o.retracted = append(o.retracted, rtoRetraction{rid: r.ID(), at: relRetractionInstant(o.t, o.g, r.ID())})
	o.retractedID[int64(r.ID())] = true
}

// neverRecordedWorld imports the export into a fresh memory graph and edits
// its stores: the lives of the first k retractions (by instant) are removed,
// every later one becomes a plain Delete (the marker cleared). The life of a
// retraction is the rows whose first delete recorded at or after them is the
// retraction's tombstone; it is the newest life of its entity (the generator
// never re-imports a retracted ID), so its rows are a version suffix.
func (o *rtoWorld) neverRecordedWorld(export []byte, k int, clk *switchableClock) *Core {
	t := o.t
	alt, err := New(Config{AllowTxBackfill: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = alt.Close() })
	alt.SetClockForTest(t, clk.Now) // the same test clock: the same pins are admissible
	if err := alt.IO.Import(bytes.NewReader(export), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import into the never-recorded world: %v", err)
	}
	trim, ok := alt.store.(storepkg.HistoryRollbackTrimCapability)
	if !ok {
		t.Fatal("memory store without HistoryRollbackTrimCapability")
	}
	for i, r := range o.retracted {
		if r.isNode {
			rows, err := alt.store.GetNodeHistory(r.nid)
			if err != nil {
				t.Fatalf("GetNodeHistory: %v", err)
			}
			life, minV := retractedLifeOf(t, rows, r.at)
			if err := trim.TrimNodeHistoryFrom(r.nid, minV); err != nil {
				t.Fatalf("TrimNodeHistoryFrom: %v", err)
			}
			if i >= k {
				for _, row := range life {
					cp := row.DeepCopy()
					cp.Temporal().Retracted = false
					if err := alt.store.PutNodeVersion(r.nid, cp.Version(), cp); err != nil {
						t.Fatalf("PutNodeVersion: %v", err)
					}
				}
			}
			continue
		}
		rows, err := alt.store.GetRelHistory(r.rid)
		if err != nil {
			t.Fatalf("GetRelHistory: %v", err)
		}
		life, minV := retractedLifeOf(t, rows, r.at)
		if err := trim.TrimRelHistoryFrom(r.rid, minV); err != nil {
			t.Fatalf("TrimRelHistoryFrom: %v", err)
		}
		if i >= k {
			for _, row := range life {
				cp := row.DeepCopy()
				cp.Temporal().Retracted = false
				if err := alt.store.PutRelVersion(r.rid, cp.Version(), cp); err != nil {
					t.Fatalf("PutRelVersion: %v", err)
				}
			}
		}
	}
	return alt
}

// retractedLifeOf returns the rows of the life the retraction at `at` ended
// and its lowest version, checking that they are a version suffix.
func retractedLifeOf[T interface {
	Version() uint32
	Temporal() *types.TemporalMetadata
}](t *testing.T, rows []T, at types.Instant) ([]T, uint32) {
	t.Helper()
	var deaths []types.Instant
	for _, r := range rows {
		if tm := r.Temporal(); tm != nil && tm.DeletedAt != 0 {
			deaths = append(deaths, tm.DeletedAt)
		}
	}
	slices.Sort(deaths)
	var life []T
	minV := uint32(1<<32 - 1)
	for _, r := range rows {
		i, _ := slices.BinarySearch(deaths, r.Temporal().TxFrom)
		if i < len(deaths) && deaths[i] == at {
			life = append(life, r)
			minV = min(minV, r.Version())
		}
	}
	if len(life) == 0 {
		t.Fatalf("no rows of the life retracted at %d", at)
	}
	for _, r := range rows {
		if r.Version() >= minV && !slices.ContainsFunc(life, func(x T) bool { return x.Version() == r.Version() }) {
			t.Fatalf("the retracted life (from v%d) is not a version suffix: v%d belongs to another life", minV, r.Version())
		}
	}
	return life, minV
}

// rtoRender renders what the state and record doors answer at pin, one line
// per door and valid instant, rows in full (version, every stamp, the
// marker, properties).
func rtoRender(t *testing.T, g *Core, pin types.Instant, valids []types.Instant, nodeIDs []types.NodeID, relIDs []types.RelID) []string {
	t.Helper()
	ns := func(nodes []*types.Node, err error) string {
		if err != nil {
			t.Fatalf("node door at pin %d: %v", pin, err)
		}
		return nodesAnswer(nodes).String()
	}
	rs := func(rels []*types.Relationship, err error) string {
		if err != nil {
			t.Fatalf("rel door at pin %d: %v", pin, err)
		}
		return relsAnswer(rels).String()
	}
	var out []string
	for _, v := range valids {
		out = append(out,
			fmt.Sprintf("NodesAtTx(%d,%d)=%s", v, pin, ns(g.Temporal.NodesAtTx(v, pin))),
			fmt.Sprintf("RelsAtTx(%d,%d)=%s", v, pin, rs(g.Temporal.RelsAtTx(v, pin))),
			fmt.Sprintf("ByLabel(A){%d,%d}=%s", v, pin, ns(g.Nodes.ByLabel("A", storepkg.QueryOpts{ValidAt: v, TxAt: pin}))),
			fmt.Sprintf("ByType(R){%d,%d}=%s", v, pin, rs(g.Rels.ByType("R", storepkg.QueryOpts{ValidAt: v, TxAt: pin}))),
			fmt.Sprintf("NodesDuringTx(%d,+300,%d)=%s", v, pin, ns(g.Temporal.NodesDuringTx(v, v+300, pin))),
			fmt.Sprintf("RelsDuringTx(%d,+300,%d)=%s", v, pin, rs(g.Temporal.RelsDuringTx(v, v+300, pin))),
		)
	}
	if pin == 0 {
		return out
	}
	out = append(out,
		fmt.Sprintf("NodesAsOf(%d)=%s", pin, ns(g.Temporal.NodesAsOf(pin))),
		fmt.Sprintf("RelsAsOf(%d)=%s", pin, rs(g.Temporal.RelsAsOf(pin))),
	)
	for _, id := range nodeIDs {
		segs, err := g.Temporal.NodeEffectiveTimeline(id, pin)
		if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
			t.Fatalf("NodeEffectiveTimeline(%v,%d): %v", id, pin, err)
		}
		out = append(out, fmt.Sprintf("NodeEffectiveTimeline(%v,%d)=%s", id, pin, nodeSegmentsString(segs)))
	}
	for _, id := range relIDs {
		segs, err := g.Temporal.RelEffectiveTimeline(id, pin)
		if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
			t.Fatalf("RelEffectiveTimeline(%v,%d): %v", id, pin, err)
		}
		out = append(out, fmt.Sprintf("RelEffectiveTimeline(%v,%d)=%s", id, pin, relSegmentsString(segs)))
	}
	return out
}
