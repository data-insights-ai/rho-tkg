package core

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R15 — the cross-backend bitemporal oracle with backdated ends and
// supersessions (tasks/handover-tx-backfill-delete-update-20261009.md §5 R15).
//
// A seeded random sequence interleaves plain creates, AddWithTx backfills,
// Update, Delete, CloseVersion, SetVersionInterval and label changes with the
// caller-instant doors — node and relationship DeleteWithTx / UpdateWithTx,
// each through a randomly chosen door (standalone, GraphTx, Batch, ingest
// strong, ingest concurrent) — on memory, badger (in-memory), sharded and
// tiered (anchor "Ref" on the reference shard, so relationships to it cross
// shards). The clock is a test clock that moves 1000 ms per step, so a caller
// instant t is placed between the entity's own last stamp and the clock:
// backdated against every other entity's later writes.
//
// What is asserted, and what each assertion catches:
//
//   - Per caller-instant op (the oracle's independent half: t is the caller's
//     number, not a stamp read back): a delete's tombstone carries
//     TxTo = DeletedAt = t on the entity and on every relationship a node
//     delete cascades; an update's new version carries TxFrom = UpdatedAt = t
//     and its predecessor TxTo = t; the as-of, TxPin and TxAt doors at pin t
//     already answer the new belief (absent, or the new version) and at pin
//     t-1 the old one. Catches a door stubbed to the plain stamp (the clock
//     lands above t, so pin t still sees the old belief), an off-by-one at the
//     pin, and a cascade that stamps its relationships with the clock.
//   - A t at or below the entity's recorded TxFrom/TxTo is refused on every
//     door with errors.Is(ErrTxOrder) and errors.Is(ErrInvalidTxFrom), and
//     nothing changes (R11's ordering rule in random positions).
//   - After the sequence: every intent's pin t-1 / t answers are unchanged by
//     the later writes (rule 15: the past is remembered), every point, during,
//     TxAt, as-of, TxPin, ByLabel(opts) and ByType(opts) door equals the
//     bitemporal oracle of bitemporaloracle_test.go on the captured chains, and
//     the four backends hold identical chains and give identical answers
//     (entities compared by creation ordinal).

type txbOracle struct {
	*world
	be  string
	clk *switchableClock
	x   types.Instant // the test clock's current reading

	intents  []txbIntent
	accepted map[string]int
	refused  int
	// aboveCurrent counts caller-instant ops on an entity whose chain held a
	// row above its current version (a cascade that left the current row in
	// place) — the shapes the removed W5 skips excluded.
	aboveCurrent int
}

// txbIntent is one accepted caller-instant op: what pins t-1 and t answered
// right after it, re-checked at the end of the sequence.
type txbIntent struct {
	desc         string
	isNode       bool
	nid          types.NodeID
	rid          types.RelID
	at           types.Instant
	prev, atView string
}

// txbRelFamily is one door family for the relationship caller-instant doors
// (the node twin is txbNodeFamilies).
type txbRelFamily struct {
	name string
	del  func(t *testing.T, g *Core, id types.RelID, at types.Instant) error
	upd  func(t *testing.T, g *Core, id types.RelID, m map[string]any, at types.Instant) error
}

func txbRelFamilies() []txbRelFamily {
	ctx := context.Background()
	ingest := func(name string, opts IngestOptions) txbRelFamily {
		return txbRelFamily{name,
			func(t *testing.T, g *Core, id types.RelID, at types.Instant) error {
				return txbIngestDo(t, g, opts, func(s *Session) error { return s.DeleteRelationshipWithTx(id, at) })
			},
			func(t *testing.T, g *Core, id types.RelID, m map[string]any, at types.Instant) error {
				return txbIngestDo(t, g, opts, func(s *Session) error { return s.UpdateRelationshipWithTx(id, m, at) })
			}}
	}
	return []txbRelFamily{
		{"standalone",
			func(t *testing.T, g *Core, id types.RelID, at types.Instant) error {
				return g.Rels.DeleteWithTx(ctx, id, at)
			},
			func(t *testing.T, g *Core, id types.RelID, m map[string]any, at types.Instant) error {
				_, err := g.Rels.UpdateWithTx(ctx, id, m, at)
				return err
			}},
		{"graphtx",
			func(t *testing.T, g *Core, id types.RelID, at types.Instant) error {
				return txbTxDo(t, g, func(tx *GraphTx) error { return tx.DeleteRelationshipWithTx(id, at) })
			},
			func(t *testing.T, g *Core, id types.RelID, m map[string]any, at types.Instant) error {
				return txbTxDo(t, g, func(tx *GraphTx) error { _, err := tx.UpdateRelationshipWithTx(id, m, at); return err })
			}},
		{"batch",
			func(t *testing.T, g *Core, id types.RelID, at types.Instant) error {
				return txbBatchDo(t, g, func(b *BatchBuilder) error { return b.DeleteRelationshipWithTx(id, at) })
			},
			func(t *testing.T, g *Core, id types.RelID, m map[string]any, at types.Instant) error {
				return txbBatchDo(t, g, func(b *BatchBuilder) error { return b.UpdateRelationshipWithTx(id, m, at) })
			}},
		ingest("ingest_sync", IngestOptions{Sync: true}),
		ingest("ingest_concurrent", IngestOptions{Concurrent: true}),
	}
}

func (o *txbOracle) tick() {
	o.x += 1000
	o.clk.at.Store(int64(o.x))
}

func (o *txbOracle) fail(format string, args ...any) {
	o.t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "[%s] ", o.be)
	fmt.Fprintf(&b, format, args...)
	fmt.Fprintf(&b, "\nop log (%d):\n", len(o.log))
	for i, l := range o.log {
		fmt.Fprintf(&b, "  %2d. %s\n", i, l)
	}
	o.t.Fatal(b.String())
}

// setup: anchor 0 is a "Ref" node (the reference shard on tiered), anchor 1 an
// "A" node, so the anchor relationships cross shards; then two regular nodes,
// two anchor relationships and one relationship between the regular nodes.
func (o *txbOracle) setup() {
	for i, l := range []string{"Ref", "A"} {
		n, err := o.g.Nodes.Add(o.ctx, []string{l}, map[string]any{"tkg_valid_from": o.nextVF(), "anchor": i})
		if err != nil {
			o.t.Fatalf("add anchor: %v", err)
		}
		o.anchors = append(o.anchors, n.ID())
	}
	o.addNode()
	o.addNode()
	o.addRel()
	o.addRel()
	o.linkNodes()
}

// backfill is world.backfillNode on the test clock: a TxFrom up to a minute
// below the clock, so every backend gets the same stamp.
func (o *txbOracle) backfill() {
	props, _ := o.maybeValidTo(o.nextVF())
	txFrom := o.x - types.Instant(1000+o.rng.IntN(60000))
	n, err := o.g.Nodes.AddWithTx(o.ctx, o.pickLabels(), props, txFrom)
	o.record(fmt.Sprintf("backfillNode(tx=%d)", txFrom), err)
	if err == nil {
		o.nodeIDs = append(o.nodeIDs, n.ID())
		o.nodeAlive[n.ID()] = true
	}
}

// linkNodes adds a relationship from a live regular node (or the "Ref"
// anchor: cross-shard on tiered) to another live regular node, so node
// deletes cascade into tracked relationships.
func (o *txbOracle) linkNodes() {
	alive := o.aliveNodes()
	if len(alive) < 1 {
		return
	}
	e := alive[o.rng.IntN(len(alive))]
	s := o.anchors[0]
	if len(alive) > 1 && o.rng.IntN(2) == 0 {
		for s = e; s == e; {
			s = alive[o.rng.IntN(len(alive))]
		}
	}
	typ := oracleRelTypes[o.rng.IntN(len(oracleRelTypes))]
	props, _ := o.maybeValidTo(o.nextVF())
	r, err := o.g.Rels.AddByID(o.ctx, typ, s, e, props)
	o.record(fmt.Sprintf("linkNodes(%s %v->%v)", typ, s, e), err)
	if err == nil {
		o.relIDs = append(o.relIDs, r.ID())
		o.relType[r.ID()] = typ
		o.relAlive[r.ID()] = true
	}
}

func (o *txbOracle) closeVersion() {
	if o.rng.IntN(2) == 0 {
		if alive := o.aliveNodes(); len(alive) > 0 {
			id := alive[o.rng.IntN(len(alive))]
			at := o.nextVF()
			o.record(fmt.Sprintf("closeNode(%v,%d)", id, at), o.g.Nodes.CloseVersion(o.ctx, id, at))
		}
		return
	}
	if alive := o.liveRels(); len(alive) > 0 {
		id := alive[o.rng.IntN(len(alive))]
		at := o.nextVF()
		o.record(fmt.Sprintf("closeRel(%v,%d)", id, at), o.g.Rels.CloseVersion(o.ctx, id, at))
	}
}

// liveRels: tracked relationships that still have a current row (a node
// delete cascades without updating relAlive).
func (o *txbOracle) liveRels() []types.RelID {
	var out []types.RelID
	for _, id := range o.relIDs {
		if _, err := o.g.Rels.Get(o.ctx, id); err == nil {
			out = append(out, id)
		}
	}
	return out
}

func (o *txbOracle) step() {
	o.tick()
	switch o.rng.IntN(16) {
	case 0:
		o.addNode()
	case 1:
		o.backfill()
	case 2:
		o.linkNodes()
	case 3:
		o.updateNode()
	case 4:
		o.updateRel()
	case 5:
		o.addLabel()
	case 6:
		o.cascadeNode()
	case 7:
		o.cascadeRel()
	case 8:
		o.deleteNode()
	case 9:
		o.deleteRel()
	case 10:
		o.closeVersion()
	default: // 11-15: a caller-instant op
		o.callerOp()
	}
}

// txbBounds returns lo, the largest stamp of any kind on rows (and the
// snowflake fallback a derived valid-from would use): every t above it is
// placeable; and maxTx, the largest TxFrom/TxTo: every t at or below it is
// refused.
func txbBounds(es ...*oracleEntity) (lo, maxTx types.Instant) {
	for _, e := range es {
		lo = max(lo, e.sfFallback)
		for _, r := range e.rows {
			maxTx = max(maxTx, r.txFrom, r.txTo)
			lo = max(lo, r.txFrom, r.txTo, r.validFrom, r.updatedAt, r.deletedAt)
		}
	}
	return lo, maxTx
}

// pickT returns a placeable t in (lo, clock], or (with probability 1/5) a t
// at or below maxTx, which every door must refuse.
func (o *txbOracle) pickT(lo, maxTx types.Instant) (types.Instant, bool) {
	if o.rng.IntN(5) == 0 {
		return max(1, maxTx-types.Instant(o.rng.IntN(3))), false
	}
	if lo >= o.x {
		o.fail("fixture: entity bound %d not below the clock %d", lo, o.x)
	}
	return lo + 1 + types.Instant(o.rng.Int64N(int64(o.x-lo))), true
}

// nodeView is the belief about node id at pin through the as-of door, the
// as-of set door, the TxPin generic door and the TxAt point door (valid time
// far in the future; every valid interval in the sequence lies below it or is
// open).
func (o *txbOracle) nodeView(id types.NodeID, pin types.Instant) string {
	var b strings.Builder
	if n, err := o.g.Temporal.NodeAsOf(id, pin); err == nil {
		fmt.Fprintf(&b, "asof=v%d", n.Version())
	} else if errors.Is(err, ErrNoVersionAsOf) {
		b.WriteString("asof=absent")
	} else {
		o.fail("NodeAsOf(%v,%d): %v", id, pin, err)
	}
	set, err := o.g.Temporal.NodesAsOf(pin)
	if err != nil {
		o.fail("NodesAsOf(%d): %v", pin, err)
	}
	fmt.Fprintf(&b, " set=%s", txbVerIn(nodeSetVer(set), id))
	all, err := o.g.Nodes.All(storepkg.QueryOpts{TxPin: pin})
	if err != nil {
		o.fail("Nodes.All{TxPin}: %v", err)
	}
	fmt.Fprintf(&b, " pin=%s", txbVerIn(nodeSetVer(all), id))
	at, err := o.g.Temporal.NodesAtTx(txbFarValid, pin)
	if err != nil {
		o.fail("NodesAtTx: %v", err)
	}
	fmt.Fprintf(&b, " txat=%s", txbVerIn(nodeSetVer(at), id))
	return b.String()
}

func (o *txbOracle) relView(id types.RelID, pin types.Instant) string {
	var b strings.Builder
	if r, err := o.g.Temporal.RelAsOf(id, pin); err == nil {
		fmt.Fprintf(&b, "asof=v%d", r.Version())
	} else if errors.Is(err, ErrNoVersionAsOf) {
		b.WriteString("asof=absent")
	} else {
		o.fail("RelAsOf(%v,%d): %v", id, pin, err)
	}
	set, err := o.g.Temporal.RelsAsOf(pin)
	if err != nil {
		o.fail("RelsAsOf(%d): %v", pin, err)
	}
	fmt.Fprintf(&b, " set=%s", txbVerIn(relSetVer(set), id))
	all, err := o.g.Rels.All(storepkg.QueryOpts{TxPin: pin})
	if err != nil {
		o.fail("Rels.All{TxPin}: %v", err)
	}
	fmt.Fprintf(&b, " pin=%s", txbVerIn(relSetVer(all), id))
	at, err := o.g.Temporal.RelsAtTx(txbFarValid, pin)
	if err != nil {
		o.fail("RelsAtTx: %v", err)
	}
	fmt.Fprintf(&b, " txat=%s", txbVerIn(relSetVer(at), id))
	return b.String()
}

// txbFarValid is a valid instant above every synthetic valid time of the
// sequence (the world's valid clock counts from 1000 in steps below 500).
const txbFarValid = types.Instant(10_000_000)

func txbVerIn[K comparable](m map[K]uint32, id K) string {
	if v, ok := m[id]; ok {
		return fmt.Sprintf("v%d", v)
	}
	return "absent"
}

// txbWantView is what the as-of, as-of set and TxPin doors must answer for
// one belief: version v, or absent when v == 0. (The TxAt door answers the
// version only when it is valid at txbFarValid; checkIntent holds it to the
// far-future pin instead.)
func txbWantView(v uint32) string {
	s := "absent"
	if v != 0 {
		s = fmt.Sprintf("v%d", v)
	}
	return fmt.Sprintf("asof=%s set=%s pin=%s", s, s, s)
}

// txbLive returns the live (current) row's version, 0 for a deleted entity.
// Every chain gets the strict per-op checks: the W5 skips for chains the plain
// doors left with a cascade row above the current one, two rows with one
// version, or a retraction stamp on an appended row (txbTangled) and for the
// GraphTx rollback of such chains (txbNoTxRollback) were removed with the
// fixes for backlog 14, 18 and 19.
func txbLive(e *oracleEntity) uint32 {
	if !e.currentAlive {
		return 0
	}
	return e.rows[len(e.rows)-1].version
}

// noteAbove counts an op on a live entity whose chain holds a row above its
// current version.
func (o *txbOracle) noteAbove(e *oracleEntity) {
	cur := txbLive(e)
	if !e.currentAlive {
		return
	}
	for _, r := range e.rows {
		if r.version > cur {
			o.aboveCurrent++
			return
		}
	}
}

// txbTombstoneRel / txbTombstoneNode return the stamps of row version v (the
// former current row a delete tombstoned).
func txbTombstoneRel(o *txbOracle, id types.RelID, v uint32) types.TemporalMetadata {
	for _, r := range txbChain(o.t, o.g, id) {
		if r.Version() == v {
			return relTemporalCopy(r)
		}
	}
	o.fail("rel %v: no row v%d", id, v)
	return types.TemporalMetadata{}
}

func txbTombstoneNode(o *txbOracle, id types.NodeID, v uint32) types.TemporalMetadata {
	for _, n := range txbNodeChain(o.t, o.g, id) {
		if n.Version() == v {
			return nodeTemporalCopy(n)
		}
	}
	o.fail("node %v: no row v%d", id, v)
	return types.TemporalMetadata{}
}

func (o *txbOracle) callerOp() {
	fam := o.rng.IntN(5)
	switch o.rng.IntN(4) {
	case 0:
		o.nodeCallerDelete(fam)
	case 1:
		o.nodeCallerUpdate(fam)
	case 2:
		o.relCallerDelete(fam)
	default:
		o.relCallerUpdate(fam)
	}
}

// refusedOrder asserts a refusal of the ordering rule: errors.Is both
// sentinels, never a store invariant error.
func (o *txbOracle) refusedOrder(desc string, err error) {
	o.t.Helper()
	if !errors.Is(err, ErrTxOrder) || !errors.Is(err, ErrInvalidTxFrom) || errors.Is(err, storepkg.ErrInvalidStoreMutation) {
		o.fail("%s: err = %v; want ErrTxOrder wrapping ErrInvalidTxFrom", desc, err)
	}
	o.refused++
}

func (o *txbOracle) nodeCallerDelete(fam int) {
	alive := o.aliveNodes()
	if len(alive) == 0 {
		return
	}
	id := alive[o.rng.IntN(len(alive))]
	hood := txbHoodRels(o.t, o.g, id)
	node := captureNode(o.t, o.g, id)
	ents := []*oracleEntity{node}
	for _, r := range hood {
		ents = append(ents, captureRel(o.t, o.g, r, ""))
	}
	f := txbNodeFamilies()[fam]
	vers := make([]uint32, len(ents))
	for i, e := range ents {
		vers[i] = txbLive(e)
		o.noteAbove(e)
	}
	lo, _ := txbBounds(ents...)
	_, nodeMaxTx := txbBounds(node)
	at, ok := o.pickT(lo, nodeMaxTx)
	desc := fmt.Sprintf("%s/Nodes.DeleteWithTx(%v,t=%d) hood=%v", f.name, id, at, hood)
	before := snapHood(o.t, o.g, id, hood...)
	beforeR := o.render(id, hood...)
	prevNode := o.nodeView(id, at-1)
	prevRels := make([]string, len(hood))
	for i, r := range hood {
		prevRels[i] = o.relView(r, at-1)
	}
	err := f.del(o.t, o.g, id, at)
	o.record(desc, err)
	if !ok {
		o.refusedOrder(desc, err)
		o.unchanged(desc, beforeR, o.render(id, hood...))
		assertHoodUnchanged(o.t, o.g, desc, before, id)
		return
	}
	if err != nil {
		o.fail("%s: %v", desc, err)
	}
	o.nodeAlive[id] = false
	o.accepted[f.name+"/node-delete"]++
	if tm := txbTombstoneNode(o, id, vers[0]); tm.TxTo != at || tm.DeletedAt != at {
		o.fail("%s: node tombstone v%d TxTo=%d DeletedAt=%d; want both %d", desc, vers[0], tm.TxTo, tm.DeletedAt, at)
	}
	o.checkIntent(txbIntent{desc: desc, isNode: true, nid: id, at: at}, prevNode, txbWantView(0))
	for i, r := range hood {
		if tm := txbTombstoneRel(o, r, vers[i+1]); tm.TxTo != at || tm.DeletedAt != at {
			o.fail("%s: cascaded rel %v tombstone v%d TxTo=%d DeletedAt=%d; want both %d", desc, r, vers[i+1], tm.TxTo, tm.DeletedAt, at)
		}
		o.checkIntent(txbIntent{desc: desc + fmt.Sprintf(" cascaded %v", r), rid: r, at: at}, prevRels[i], txbWantView(0))
	}
}

func (o *txbOracle) nodeCallerUpdate(fam int) {
	alive := o.aliveNodes()
	if len(alive) == 0 {
		return
	}
	id := alive[o.rng.IntN(len(alive))]
	node := captureNode(o.t, o.g, id)
	o.noteAbove(node)
	f := txbNodeFamilies()[fam]

	lo, maxTx := txbBounds(node)
	at, ok := o.pickT(lo, maxTx)
	cur, err := o.g.Nodes.Get(o.ctx, id)
	if err != nil {
		o.fail("Get(node %v): %v", id, err)
	}
	closed := isClosedTemporal(cur.Temporal())
	desc := fmt.Sprintf("%s/Nodes.UpdateWithTx(%v,t=%d) closed=%v", f.name, id, at, closed)
	before := snapNode(o.t, o.g, id)
	beforeR := o.render(id)
	prev := o.nodeView(id, at-1)
	err = f.upd(o.t, o.g, id, map[string]any{"k": o.seqno}, at)
	o.seqno++
	o.record(desc, err)
	switch {
	case !ok:
		o.refusedOrder(desc, err)
		o.unchanged(desc, beforeR, o.render(id))
		assertNodeUnchanged(o.t, desc, before, snapNode(o.t, o.g, id))
		return
	case closed:
		if !errors.Is(err, ErrAlreadyClosed) {
			o.fail("%s: err = %v; want ErrAlreadyClosed", desc, err)
		}
		o.unchanged(desc, beforeR, o.render(id))
		assertNodeUnchanged(o.t, desc, before, snapNode(o.t, o.g, id))
		return
	case err != nil:
		o.fail("%s: %v", desc, err)
	}
	o.accepted[f.name+"/node-update"]++
	now, err := o.g.Nodes.Get(o.ctx, id)
	if err != nil {
		o.fail("%s: Get: %v", desc, err)
	}
	if tm := now.Temporal(); tm.TxFrom != at || tm.UpdatedAt != at || now.Version() <= cur.Version() {
		o.fail("%s: new version v%d TxFrom=%d UpdatedAt=%d; want a version above v%d at %d", desc, now.Version(), tm.TxFrom, tm.UpdatedAt, cur.Version(), at)
	}
	for _, h := range snapNode(o.t, o.g, id).hist {
		if h.Version() == cur.Version() && h.Temporal().TxTo != at {
			o.fail("%s: superseded v%d TxTo=%d; want %d", desc, h.Version(), h.Temporal().TxTo, at)
		}
	}
	o.checkIntent(txbIntent{desc: desc, isNode: true, nid: id, at: at}, prev, txbWantView(now.Version()))
}

func (o *txbOracle) relCallerDelete(fam int) {
	live := o.liveRels()
	if len(live) == 0 {
		return
	}
	id := live[o.rng.IntN(len(live))]
	rel := captureRel(o.t, o.g, id, "")
	o.noteAbove(rel)
	f := txbRelFamilies()[fam]
	ver := txbLive(rel)
	lo, maxTx := txbBounds(rel)
	at, ok := o.pickT(lo, maxTx)
	desc := fmt.Sprintf("%s/Rels.DeleteWithTx(%v,t=%d)", f.name, id, at)
	before := snapRel(o.t, o.g, id)
	beforeR := o.render(0, id)
	prev := o.relView(id, at-1)
	err := f.del(o.t, o.g, id, at)
	o.record(desc, err)
	if !ok {
		o.refusedOrder(desc, err)
		o.unchanged(desc, beforeR, o.render(0, id))
		assertRelUnchanged(o.t, desc, before, snapRel(o.t, o.g, id))
		return
	}
	if err != nil {
		o.fail("%s: %v", desc, err)
	}
	o.relAlive[id] = false
	o.accepted[f.name+"/rel-delete"]++
	if tm := txbTombstoneRel(o, id, ver); tm.TxTo != at || tm.DeletedAt != at {
		o.fail("%s: tombstone v%d TxTo=%d DeletedAt=%d; want both %d", desc, ver, tm.TxTo, tm.DeletedAt, at)
	}
	o.checkIntent(txbIntent{desc: desc, rid: id, at: at}, prev, txbWantView(0))
}

func (o *txbOracle) relCallerUpdate(fam int) {
	live := o.liveRels()
	if len(live) == 0 {
		return
	}
	id := live[o.rng.IntN(len(live))]
	rel := captureRel(o.t, o.g, id, "")
	o.noteAbove(rel)
	f := txbRelFamilies()[fam]

	lo, maxTx := txbBounds(rel)
	at, ok := o.pickT(lo, maxTx)
	cur, err := o.g.Rels.Get(o.ctx, id)
	if err != nil {
		o.fail("Get(rel %v): %v", id, err)
	}
	closed := isClosedTemporal(cur.Temporal())
	desc := fmt.Sprintf("%s/Rels.UpdateWithTx(%v,t=%d) closed=%v", f.name, id, at, closed)
	before := snapRel(o.t, o.g, id)
	beforeR := o.render(0, id)
	prev := o.relView(id, at-1)
	err = f.upd(o.t, o.g, id, map[string]any{"k": o.seqno}, at)
	o.seqno++
	o.record(desc, err)
	switch {
	case !ok:
		o.refusedOrder(desc, err)
		o.unchanged(desc, beforeR, o.render(0, id))
		assertRelUnchanged(o.t, desc, before, snapRel(o.t, o.g, id))
		return
	case closed:
		if !errors.Is(err, ErrAlreadyClosed) {
			o.fail("%s: err = %v; want ErrAlreadyClosed", desc, err)
		}
		o.unchanged(desc, beforeR, o.render(0, id))
		assertRelUnchanged(o.t, desc, before, snapRel(o.t, o.g, id))
		return
	case err != nil:
		o.fail("%s: %v", desc, err)
	}
	o.accepted[f.name+"/rel-update"]++
	now, err := o.g.Rels.Get(o.ctx, id)
	if err != nil {
		o.fail("%s: Get: %v", desc, err)
	}
	if tm := now.Temporal(); tm.TxFrom != at || tm.UpdatedAt != at || now.Version() <= cur.Version() {
		o.fail("%s: new version v%d TxFrom=%d UpdatedAt=%d; want a version above v%d at %d", desc, now.Version(), tm.TxFrom, tm.UpdatedAt, cur.Version(), at)
	}
	for _, h := range snapRel(o.t, o.g, id).hist {
		if h.Version() == cur.Version() && h.Temporal().TxTo != at {
			o.fail("%s: superseded v%d TxTo=%d; want %d", desc, h.Version(), h.Temporal().TxTo, at)
		}
	}
	o.checkIntent(txbIntent{desc: desc, rid: id, at: at}, prev, txbWantView(now.Version()))
}

// render is every row of node nid (if non-zero) and of rels, stamps and
// version, for the nothing-changed checks (their failure prints the op log).
func (o *txbOracle) render(nid types.NodeID, rels ...types.RelID) string {
	var b strings.Builder
	row := func(r oracleRow) {
		fmt.Fprintf(&b, "\n  v%d vf=%d vt=%d tx=%d..%d del=%d upd=%d", r.version, r.validFrom, r.validTo, r.txFrom, r.txTo, r.deletedAt, r.updatedAt)
	}
	if nid != 0 {
		e := captureNode(o.t, o.g, nid)
		fmt.Fprintf(&b, "\n node %v alive=%v", nid, e.currentAlive)
		for _, r := range e.rows {
			row(r)
		}
	}
	for _, id := range rels {
		e := captureRel(o.t, o.g, id, "")
		fmt.Fprintf(&b, "\n rel %v alive=%v", id, e.currentAlive)
		for _, r := range e.rows {
			row(r)
		}
	}
	return b.String()
}

func (o *txbOracle) unchanged(desc, before, after string) {
	o.t.Helper()
	if before != after {
		o.fail("%s: a refused op changed the chain:\n before%s\n after%s", desc, before, after)
	}
}

// chainString renders the entity's captured chain for a failure message.
func (o *txbOracle) chainString(in txbIntent) string {
	e := (*oracleEntity)(nil)
	if in.isNode {
		e = captureNode(o.t, o.g, in.nid)
	} else {
		e = captureRel(o.t, o.g, in.rid, "")
	}
	var b strings.Builder
	for _, r := range e.rows {
		fmt.Fprintf(&b, "\n  v%d vf=%d vt=%d tx=%d..%d del=%d upd=%d", r.version, r.validFrom, r.validTo, r.txFrom, r.txTo, r.deletedAt, r.updatedAt)
	}
	fmt.Fprintf(&b, "\n  currentAlive=%v", e.currentAlive)
	return b.String()
}

func (o *txbOracle) view(in txbIntent, pin types.Instant) string {
	if in.isNode {
		return o.nodeView(in.nid, pin)
	}
	return o.relView(in.rid, pin)
}

// checkIntent: right after the op, pin t answers what the far-future pin
// answers (the new belief) through every door, pin t-1 still answers the
// belief before the op, and pin t answers exactly wantAt.
func (o *txbOracle) checkIntent(in txbIntent, prevBefore, wantAt string) {
	o.t.Helper()
	in.prev = o.view(in, in.at-1)
	if in.prev != prevBefore {
		o.fail("%s: pin t-1 changed by the op:\n before %s\n after  %s\n chain %s", in.desc, prevBefore, in.prev, o.chainString(in))
	}
	in.atView = o.view(in, in.at)
	future := o.view(in, o.x+1_000_000)
	if in.atView != future {
		o.fail("%s: pin t does not answer the new belief:\n pin t      %s\n far future %s", in.desc, in.atView, future)
	}
	if !strings.HasPrefix(in.atView, wantAt+" ") {
		o.fail("%s: pin t answers %s; want %s", in.desc, in.atView, wantAt)
	}
	o.intents = append(o.intents, in)
}

// txbCanon is a backend-independent rendering of the captured chains: each
// entity by creation ordinal, every row's stamps, version and labels.
func (o *txbOracle) canon(s *snapshot) string {
	var b strings.Builder
	row := func(r oracleRow) {
		fmt.Fprintf(&b, " [v%d vf=%d vt=%d tx=%d..%d del=%d upd=%d %v]", r.version, r.validFrom, r.validTo, r.txFrom, r.txTo, r.deletedAt, r.updatedAt, r.labels)
	}
	for i, id := range o.allNodeIDs() {
		fmt.Fprintf(&b, "n%d:", i)
		for _, r := range s.nodes[id].rows {
			row(r)
		}
		b.WriteByte('\n')
	}
	for i, id := range o.relIDs {
		fmt.Fprintf(&b, "r%d(%s):", i, o.relType[id])
		for _, r := range s.rels[id].rows {
			row(r)
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// txbLineDiff renders the lines of b that differ from a (by position).
func txbLineDiff(a, b string) string {
	al, bl := strings.Split(a, "\n"), strings.Split(b, "\n")
	var out strings.Builder
	for i := 0; i < max(len(al), len(bl)); i++ {
		var x, y string
		if i < len(al) {
			x = al[i]
		}
		if i < len(bl) {
			y = bl[i]
		}
		if x != y {
			fmt.Fprintf(&out, "\n want %s\n got  %s", x, y)
		}
	}
	return out.String()
}

func (o *txbOracle) allNodeIDs() []types.NodeID {
	return append(append([]types.NodeID(nil), o.anchors...), o.nodeIDs...)
}

// answers renders what the main doors answer at every probe, entities by
// creation ordinal, for the cross-backend comparison.
func (o *txbOracle) answers(probes []probe, pins []types.Instant) []string {
	nOrd := map[types.NodeID]int{}
	for i, id := range o.allNodeIDs() {
		nOrd[id] = i
	}
	rOrd := map[types.RelID]int{}
	for i, id := range o.relIDs {
		rOrd[id] = i
	}
	nodes := func(ns []*types.Node, err error) string {
		if err != nil {
			o.fail("node door: %v", err)
		}
		out := make([]string, 0, len(ns))
		for _, n := range ns {
			out = append(out, fmt.Sprintf("n%d#v%d", nOrd[n.ID()], n.Version()))
		}
		sort.Strings(out)
		return strings.Join(out, ",")
	}
	rels := func(rs []*types.Relationship, err error) string {
		if err != nil {
			o.fail("rel door: %v", err)
		}
		out := make([]string, 0, len(rs))
		for _, r := range rs {
			out = append(out, fmt.Sprintf("r%d#v%d", rOrd[r.ID()], r.Version()))
		}
		sort.Strings(out)
		return strings.Join(out, ",")
	}
	var out []string
	g := o.g
	for _, p := range probes {
		if p.interval {
			out = append(out, fmt.Sprintf("%s NodesDuringTx=%s RelsDuringTx=%s", p,
				nodes(g.Temporal.NodesDuringTx(p.s, p.e, p.txAt)), rels(g.Temporal.RelsDuringTx(p.s, p.e, p.txAt))))
			continue
		}
		out = append(out, fmt.Sprintf("%s NodesAtTx=%s RelsAtTx=%s ByLabel(A)=%s ByType(R)=%s", p,
			nodes(g.Temporal.NodesAtTx(p.validAt, p.txAt)), rels(g.Temporal.RelsAtTx(p.validAt, p.txAt)),
			nodes(g.Nodes.ByLabel("A", storepkg.QueryOpts{ValidAt: p.validAt, TxAt: p.txAt})),
			rels(g.Rels.ByType("R", storepkg.QueryOpts{ValidAt: p.validAt, TxAt: p.txAt}))))
	}
	for _, pin := range pins {
		out = append(out, fmt.Sprintf("pin=%d NodesAsOf=%s RelsAsOf=%s ByLabel(B){TxPin}=%s ByType(S){TxPin}=%s", pin,
			nodes(g.Temporal.NodesAsOf(pin)), rels(g.Temporal.RelsAsOf(pin)),
			nodes(g.Nodes.ByLabel("B", storepkg.QueryOpts{TxPin: pin})),
			rels(g.Rels.ByType("S", storepkg.QueryOpts{TxPin: pin}))))
	}
	return out
}

// txbOracleRun drives one seeded sequence on one backend and returns its
// canonical chains and door answers; probes and pins come from the first
// backend's run (nil: build them here).
func txbOracleRun(t *testing.T, be txbBackend, seed uint64, nOps int, x0 types.Instant, probes []probe, pins []types.Instant) (canon string, answers []string, outProbes []probe, outPins []types.Instant, o *txbOracle) {
	g := be.open(t, true)
	if !g.bitemporalMigrated {
		t.Fatalf("%s: expected bitemporalMigrated=true; oracle assumptions invalid", be.name)
	}
	clk := &switchableClock{}
	g.SetClockForTest(t, clk.Now)
	rng := rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))
	o = &txbOracle{world: newWorld(t, g, rng), be: be.name, clk: clk, accepted: map[string]int{}}
	// x0 lies above the wall clock (snowflake mint times, the derived
	// valid-from fallback) and is the same for every backend of a seed.
	o.x = x0
	o.tick()
	o.setup()
	for i := 0; i < nOps; i++ {
		o.step()
	}
	o.tick()
	// Rule 15: what pins t-1 and t answered right after each op survives every
	// later write.
	for _, in := range o.intents {
		if got := o.view(in, in.at-1); got != in.prev {
			o.fail("%s: pin t-1 forgotten after later writes:\n then %s\n now  %s", in.desc, in.prev, got)
		}
		if got := o.view(in, in.at); got != in.atView {
			o.fail("%s: pin t forgotten after later writes:\n then %s\n now  %s", in.desc, in.atView, got)
		}
	}
	snap := o.capture()
	var maxStamp types.Instant
	for _, v := range snap.interestingInstants() {
		maxStamp = max(maxStamp, v)
	}
	farFuture := maxStamp + 1_000_000
	if probes == nil {
		probes = snap.buildProbes(rand.New(rand.NewPCG(seed^0xD1B54A32D192ED03, seed)), 40, farFuture)
		seen := map[types.Instant]bool{}
		add := func(p types.Instant) {
			if p > 0 && !seen[p] {
				seen[p] = true
				pins = append(pins, p)
			}
		}
		for _, in := range o.intents {
			add(in.at - 1)
			add(in.at)
		}
		for _, p := range probes {
			add(p.txAt)
		}
		add(farFuture)
	}
	for _, p := range probes {
		o.runProbe(be.name, seed, snap, p)
	}
	for _, pin := range pins {
		o.runAsOfProbe(be.name, seed, snap, pin)
	}
	return o.canon(snap), o.answers(probes, pins), probes, pins, o
}

func TestTxBackfillOracle_CrossBackend(t *testing.T) {
	t.Parallel()
	seeds, nOps := 16, 40
	if !testing.Short() {
		seeds, nOps = 48, 48
	}
	// A longer sweep: TXB_ORACLE_SEEDS=300 go test -run TestTxBackfillOracle_CrossBackend
	if n, err := strconv.Atoi(os.Getenv("TXB_ORACLE_SEEDS")); err == nil && n > 0 {
		seeds = n
	}
	const base uint64 = 0x7B_0D_15 // "tx-bf-15"
	// One test clock origin for every backend: read per backend, the wall
	// clock could cross a rounding boundary between two runs of a seed and
	// shift every stamp of the later backend.
	x0 := types.Instant(time.Now().Add(time.Hour).UnixMilli()/1_000_000*1_000_000 + 1_000_000)
	accepted := map[string]int{}
	refused, intents, above := 0, 0, 0
	for i := 0; i < seeds; i++ {
		seed := base + uint64(i)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			var (
				refCanon   string
				refAnswers []string
				probes     []probe
				pins       []types.Instant
			)
			for bi, be := range txbBackends() {
				canon, answers, p, pn, o := txbOracleRun(t, be, seed, nOps, x0, probes, pins)
				if bi == 0 {
					refCanon, refAnswers, probes, pins = canon, answers, p, pn
					for k, v := range o.accepted {
						accepted[k] += v
					}
					refused += o.refused
					intents += len(o.intents)
					above += o.aboveCurrent
					continue
				}
				if canon != refCanon {
					o.fail("chains differ from %s:%s", txbBackends()[0].name, txbLineDiff(refCanon, canon))
				}
				if len(answers) != len(refAnswers) {
					o.fail("%d answers; %s gave %d", len(answers), txbBackends()[0].name, len(refAnswers))
				}
				for j := range answers {
					if answers[j] != refAnswers[j] {
						o.fail("answer differs from %s:\n %s\n %s", txbBackends()[0].name, refAnswers[j], answers[j])
					}
				}
			}
		})
	}
	if t.Failed() {
		return
	}
	// Coverage of the generator itself: every door family accepted at least
	// one op of each kind, and refusals happened.
	for _, f := range txbNodeFamilies() {
		for _, k := range []string{"/node-delete", "/node-update", "/rel-delete", "/rel-update"} {
			if accepted[f.name+k] == 0 {
				t.Errorf("generator never accepted %s%s over %d seeds", f.name, k, seeds)
			}
		}
	}
	if refused == 0 {
		t.Errorf("generator never drew a refused t over %d seeds", seeds)
	}
	// The shapes the W5 skips once excluded — a caller-instant op on a chain
	// with a cascade row above its current row — must be reached.
	if above == 0 {
		t.Errorf("generator never drew a caller-instant op on a chain with a row above the current one over %d seeds", seeds)
	}
	t.Logf("accepted %v, refused %d, pin checks %d (%d on a chain with a row above the current one)", accepted, refused, intents, above)
}
