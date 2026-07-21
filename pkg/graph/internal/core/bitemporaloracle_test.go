package core

// — Bitemporal reference oracle + generative cross-door harness.
//
// This is the keystone correctness investment for the bitemporal resolver. It
// pairs a small, deliberately-DUMB reference model of rho-tkg's bitemporal
// query semantics (the "oracle") with a randomized harness that drives the real
// engine through random op sequences and asserts BOTH temporal query doors
// (named: NodesAtTx/NodeAtTx/NodesDuring/NodesAt/NodesAsOf; generic:
// ByLabel/All/ByType with QueryOpts{ValidAt|ValidStart/ValidEnd, TxAt}) on BOTH
// backends (memory, badger) agree with the oracle. It permanently retires the
// "the two doors disagreed" bug class behind lessons 32/33/42/43/46/60.
//
// Design — why this is trustworthy:
//
//   - The oracle never PREDICTS engine state. After running an op sequence it
//     reads the FULL version chain (History ++ current) back from the engine,
//     capturing the REAL stamps (TxFrom / UpdatedAt / ValidFrom / ValidTo /
//     DeletedAt / Version) the engine assigned. This sidesteps the two
//     test-clock hazards documented in bitemporal_tombstone_test.go: we never
//     guess a monotonic-floor stamp, and every probe is a fixed integer, so a
//     given captured model yields deterministic oracle answers regardless of
//     wall-clock jitter.
//
//   - The oracle then re-derives every query answer from the captured rows
//     using its OWN independent O(n) implementation of the contract — no engine
//     function is called for resolution. Each rule is one obvious clause with a
//     comment naming its lesson. Because the engine and the oracle both resolve
//     from the SAME captured rows via SEPARATE code, a divergence in the
//     resolver surfaces as a harness mismatch (the mutation checks in the WP
//     confirm this: reintroducing lesson 60/43/33 in the engine turns the
//     harness red).
//
//   - Probes always carry a valid-time component (ValidAt for points, finite
//     ValidStart/ValidEnd for intervals), so the generic door never takes the
//     TxAt-only "valid at wall now" path (the RT-2 footgun) whose answer depends
//     on the wall clock — keeping the harness deterministic. TxAt values are
//     0 (no filter), around recorded stamps (boundary and boundary±1), and a
//     far-future pin.
//
// The op set (WP deliverable 1): add node/rel (optional explicit valid-from /
// valid-to), backfill-add via AddWithTx (Config.AllowTxBackfill), update-props
// (optional explicit valid-from), add-label / remove-label, set-version-interval
// (append-only cascade), and hard-delete. Relationships connect a fixed pool of
// anchor nodes (never mutated / deleted) so endpoints stay stable and a node
// delete never cascades into a tracked relationship.

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"
	"time"

	snowflakepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/snowflake"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	shardedpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// The oracle: a dumb slice-of-versions model + independent resolution.
// =============================================================================

// oracleRow is one captured version row of an entity's chain. It carries only
// the fields the bitemporal resolver consumes; labels are per-version because a
// node's label set changes across versions (add-label / remove-label).
type oracleRow struct {
	validFrom types.Instant // raw ValidFromRaw (0 = unset)
	validTo   types.Instant // raw ValidToRaw (0 = open)
	txFrom    types.Instant
	txTo      types.Instant
	deletedAt types.Instant
	updatedAt types.Instant
	version   uint32
	labels    []string // node label set on this version ("" set for rels)
}

// oracleEntity is the captured chain for one entity, in engine chain order:
// history rows (ascending version) followed by the current live row (if any).
type oracleEntity struct {
	rows         []oracleRow
	currentAlive bool          // true iff a live current row exists (last of rows)
	sfFallback   types.Instant // snowflake-ID effective valid-from fallback
	relType      string        // "" for nodes; the immutable type for rels
}

// effVF is a VERSION's effective valid-from used to ORDER it within the chain
// and as its tile start. Mirrors the engine's nodeSortValidFrom (temporal.go):
// explicit ValidFrom, else a non-genesis row's UpdatedAt, else the snowflake
// fallback. (Contract: "effective valid-from of that version".)
func (e *oracleEntity) effVF(r oracleRow) types.Instant {
	if r.validFrom != 0 {
		return r.validFrom
	}
	if r.version != 0 && r.updatedAt != 0 {
		return r.updatedAt
	}
	return e.sfFallback
}

// eclipsedRow reports a cascade zero-width sentinel row (lesson 35): ValidTo ==
// ValidFrom+1. Such rows are invisible to valid-time resolution and must not
// contribute to a neighbor's vEnd.
func eclipsedRow(r oracleRow) bool {
	return r.validFrom != 0 && r.validTo != 0 && r.validTo == r.validFrom+1
}

// bounds computes the effective [vStart, vEnd) for rows[i] over a chain already
// ordered by (effVF asc, version asc). Faithful, independent re-statement of
// nodeVersionBounds/relVersionBounds (temporal.go):
//
//   - vStart: genesis → effective valid-from; non-genesis → UpdatedAt (else
//     effective valid-from); then an explicit ValidFrom overrides absolutely
//     (lesson 33 — on a migrated store every non-zero ValidFrom is
//     caller-supplied, so no inheritance heuristic is needed).
//   - vEnd: the NEXT non-eclipsed version's effective valid-from (its explicit
//     ValidFrom, else its UpdatedAt, else the snowflake fallback — lesson 32:
//     the next VALID-time boundary, never the supersede TX time as a rule);
//     then an explicit ValidTo overrides absolutely; 0 = open.
func (e *oracleEntity) bounds(chain []oracleRow, i int) (types.Instant, types.Instant) {
	r := chain[i]
	var vStart, vEnd types.Instant

	if r.version == 0 {
		if r.validFrom != 0 {
			vStart = r.validFrom
		} else {
			vStart = e.sfFallback
		}
	} else {
		switch {
		case r.updatedAt != 0:
			vStart = r.updatedAt
		case r.validFrom != 0:
			vStart = r.validFrom
		default:
			vStart = e.sfFallback
		}
	}

	for j := i + 1; j < len(chain); j++ {
		next := chain[j]
		if eclipsedRow(next) {
			continue
		}
		switch {
		case next.validFrom != 0:
			vEnd = next.validFrom
		case next.updatedAt != 0:
			vEnd = next.updatedAt
		default:
			vEnd = e.sfFallback
		}
		break
	}

	if r.validFrom != 0 { // explicit ValidFrom override (migrated store)
		vStart = r.validFrom
	}
	if r.validTo != 0 { // explicit ValidTo override
		vEnd = r.validTo
	}
	return vStart, vEnd
}

// txFilter reproduces filter{Node,Rel}ChainByTxAt (temporal.go). txAt == 0 → no
// filter, no normalization (returns the chain verbatim). Otherwise keep rows
// RECORDED-BY-THEN (TxFrom <= txAt; TxTo never bounds visibility — lesson 43),
// and un-apply a post-pin delete tombstone (lesson 60): a surviving row whose
// DeletedAt post-dates txAt is normalized to the belief state as of the pin — if
// its ValidTo equals the DeletedAt stamp the interval re-opens (0). Rows are
// copied by value, so the captured model is never mutated.
func (e *oracleEntity) txFilter(txAt types.Instant) []oracleRow {
	if txAt == 0 {
		return append([]oracleRow(nil), e.rows...)
	}
	out := make([]oracleRow, 0, len(e.rows))
	for _, r := range e.rows {
		if r.txFrom != 0 && r.txFrom > txAt { // lesson 43: recorded-by-then only
			continue
		}
		if r.deletedAt != 0 && r.deletedAt > txAt { // lesson 60: tombstone not yet known
			if r.validTo == r.deletedAt {
				r.validTo = 0
			}
			r.deletedAt = 0
			if r.txTo > txAt {
				r.txTo = 0
			}
		}
		out = append(out, r)
	}
	return out
}

// sortChain orders rows by (effVF asc, version asc) — the order the engine's
// resolver imposes before deriving bounds — and reports whether the ORIGINAL
// order already satisfied it (no adjacent inversion). "needed==false" is the
// engine's fast path (resolveNodeVersionAt: monotonic chain, newest-covering
// wins); "needed==true" is the slow path (append-only cascade: newest belief
// wins on overlap). See temporal.go sortNodeChainForResolve.
func (e *oracleEntity) sortChain(rows []oracleRow) (sorted []oracleRow, needed bool) {
	for i := 1; i < len(rows); i++ {
		a, b := e.effVF(rows[i-1]), e.effVF(rows[i])
		if a > b || (a == b && rows[i-1].version > rows[i].version) {
			needed = true
			break
		}
	}
	sorted = append([]oracleRow(nil), rows...)
	sort.SliceStable(sorted, func(i, j int) bool {
		vi, vj := e.effVF(sorted[i]), e.effVF(sorted[j])
		if vi != vj {
			return vi < vj
		}
		return sorted[i].version < sorted[j].version
	})
	return sorted, needed
}

// beliefNewer reports whether a is a strictly later belief than b (higher
// TxFrom, then version) — the newer-belief tiebreak on a valid-time overlap
// (lesson 46).
func beliefNewer(a, b oracleRow) bool {
	if a.txFrom != b.txFrom {
		return a.txFrom > b.txFrom
	}
	return a.version > b.version
}

// ownBounds returns [vStart, vEnd) using r's OWN asserted end (validTo, 0 ==
// open) rather than the positional next-row bound bounds() derives. Mirrors
// the engine's nodeOwnBounds/relOwnBounds (temporal.go) — used ONLY by
// pointVisible's slow (cascade) path, for the identical BACKLOG 10b reason:
// an untouched older row's ValidFrom must never truncate a newer, wider-
// reaching correction. vStart is effVF(r) — the SAME effective-start key
// bounds() derives for a non-genesis row without the inheritance heuristic
// (a migrated-store assumption already baked into this harness's op set,
// which never exercises the pre-migration inheritance heuristic — see
// runBitemporalMigrationBestEffort). bounds()/intervalVisible/asOfVisible and
// pointVisible's fast path are UNCHANGED — see the BACKLOG 10b CHANGELOG
// entry for why this one-clause change is required rather than optional: the
// oracle's positional bounds() shares the exact same bug as the engine's
// (pre-fix) nodeVersionBounds, which is why this harness could not catch 10b
// before this change.
func (e *oracleEntity) ownBounds(r oracleRow) (types.Instant, types.Instant) {
	return e.effVF(r), r.validTo
}

// pointVisible resolves the version covering validAt as known at txAt, or
// (row, false) when the entity has no covering version. Mirrors
// resolveNodeVersionAt's fast/slow dispatch exactly. validAt is covered by a
// version when vStart <= validAt && (vEnd == 0 || vEnd > validAt).
func (e *oracleEntity) pointVisible(validAt, txAt types.Instant) (oracleRow, bool) {
	chain, needed := e.sortChain(e.txFilter(txAt))
	if len(chain) == 0 {
		return oracleRow{}, false
	}
	if !needed {
		// Fast path: newest covering version (highest effVF) wins.
		for i := len(chain) - 1; i >= 0; i-- {
			if eclipsedRow(chain[i]) {
				continue
			}
			vs, ve := e.bounds(chain, i)
			if vs <= validAt && (ve == 0 || ve > validAt) {
				return chain[i], true
			}
		}
		return oracleRow{}, false
	}
	// Slow path (cascade): newest BELIEF covering version wins. BACKLOG 10b:
	// own-interval bounds, not positional — see ownBounds.
	best := -1
	for i := range chain {
		if eclipsedRow(chain[i]) {
			continue
		}
		vs, ve := e.ownBounds(chain[i])
		if vs <= validAt && (ve == 0 || ve > validAt) {
			if best < 0 || beliefNewer(chain[i], chain[best]) {
				best = i
			}
		}
	}
	if best < 0 {
		return oracleRow{}, false
	}
	return chain[best], true
}

// intervalVisible resolves whether ANY tx-visible version overlaps [s, e) AND
// satisfies pred (predicate-anywhere, never most-recent-only — lesson 42), and
// returns the matched version (the engine scans newest-first and returns the
// first such match). Overlap: vStart < e && (vEnd == 0 || vEnd > s). Mirrors
// findNodeVersionMatchingDuringTx.
func (e *oracleEntity) intervalVisible(s, end, txAt types.Instant, pred func(oracleRow) bool) (oracleRow, bool) {
	chain, _ := e.sortChain(e.txFilter(txAt)) // interval path always sorts
	for i := len(chain) - 1; i >= 0; i-- {
		if eclipsedRow(chain[i]) {
			continue
		}
		vs, ve := e.bounds(chain, i)
		if vs < end && (ve == 0 || ve > s) {
			if pred == nil || pred(chain[i]) {
				return chain[i], true
			}
		}
	}
	return oracleRow{}, false
}

// asOfVisible resolves the named as-of SNAPSHOT door (NodesAsOf/RelsAsOf): the
// record recorded-CURRENT at txTime. It is a distinct resolver from the
// point/interval doors (tx-time only, no valid-time), so it has its own oracle
// clause. Mirrors the aligned backends (memory nodeAsOfLocked, badger native
// reverse-scan, core fallback) exactly:
//
//   - current arm: a live current row wins iff it is open in tx-time and already
//     committed at txTime (TxFrom>0 && TxFrom<=txTime && TxTo==0). It wins even
//     when a later-TxFrom HISTORY row exists (a bounded cascade can append a
//     higher-version row while leaving the live current unchanged) — both
//     backends short-circuit on the live current before scanning history.
//   - history arm (current absent OR not committed by txTime): the newest BELIEF
//     recorded by txTime — the highest VERSION among history rows with
//     TxFrom<=txTime. Recency is by version, NOT by TxFrom: an Update derives its
//     TxFrom via validInstantAfter and can bump it ABOVE a later cascade row's
//     plain c.now() stamp, so version order (allocation order) — not TxFrom order
//     — is authoritative, mirroring the badger native reverse-scan. That belief
//     is decisive: if it was retracted/deleted by txTime (TxTo!=0 && TxTo<=txTime,
//     or DeletedAt!=0 && DeletedAt<=txTime) the entity is ABSENT — the resolver
//     must NOT fall through to an older open-TxTo row (lesson 62). This is the
//     bug the WP fixed: an append-only cascade demotes the prior current to
//     history WITHOUT stamping its TxTo, so a hard delete leaving the corrected
//     tile tombstoned used to resurrect the open-TxTo genesis on memory/tiered
//     while badger correctly reported absent.
func (e *oracleEntity) asOfVisible(txTime types.Instant) (oracleRow, bool) {
	n := len(e.rows)
	if e.currentAlive {
		cur := e.rows[n-1]
		if cur.txFrom > 0 && cur.txFrom <= txTime && cur.txTo == 0 {
			return cur, true
		}
		n-- // history arm scans everything except the live current row
	}
	best := -1
	for i := 0; i < n; i++ {
		r := e.rows[i]
		if r.txFrom == 0 || r.txFrom > txTime {
			continue
		}
		if best < 0 || r.version > e.rows[best].version {
			best = i
		}
	}
	if best < 0 {
		return oracleRow{}, false
	}
	b := e.rows[best]
	if b.txTo != 0 && b.txTo <= txTime { // decisive belief superseded by the pin
		return oracleRow{}, false
	}
	if b.deletedAt != 0 && b.deletedAt <= txTime { // decisive belief deleted by the pin
		return oracleRow{}, false
	}
	return b, true
}

// hasLabel reports whether a captured node version carries label l.
func hasLabel(r oracleRow, l string) bool {
	for _, x := range r.labels {
		if x == l {
			return true
		}
	}
	return false
}

// =============================================================================
// Model capture — read the REAL stamps back from the engine.
// =============================================================================

func snowflakeFallbackNode(id types.NodeID) types.Instant {
	return types.Instant(snowflakepkg.Layout.CreatedAt(id.SnowflakeID()).UnixMilli())
}

func snowflakeFallbackRel(id types.RelID) types.Instant {
	return types.Instant(snowflakepkg.Layout.CreatedAt(id.SnowflakeID()).UnixMilli())
}

func nodeRow(g *Core, n *types.Node) oracleRow {
	tm := n.Temporal()
	r := oracleRow{version: n.Version(), labels: g.Nodes.Labels(n)}
	if tm != nil {
		r.validFrom, r.validTo = tm.ValidFrom, tm.ValidTo
		r.txFrom, r.txTo = tm.TxFrom, tm.TxTo
		r.deletedAt, r.updatedAt = tm.DeletedAt, tm.UpdatedAt
	}
	return r
}

func relRow(r *types.Relationship) oracleRow {
	tm := r.Temporal()
	out := oracleRow{version: r.Version()}
	if tm != nil {
		out.validFrom, out.validTo = tm.ValidFrom, tm.ValidTo
		out.txFrom, out.txTo = tm.TxFrom, tm.TxTo
		out.deletedAt, out.updatedAt = tm.DeletedAt, tm.UpdatedAt
	}
	return out
}

func captureNode(t *testing.T, g *Core, id types.NodeID) *oracleEntity {
	t.Helper()
	hist, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("History(node %v): %v", id, err)
	}
	e := &oracleEntity{sfFallback: snowflakeFallbackNode(id)}
	for _, h := range hist {
		e.rows = append(e.rows, nodeRow(g, h))
	}
	cur, err := g.Nodes.Get(context.Background(), id)
	switch {
	case err == nil:
		e.rows = append(e.rows, nodeRow(g, cur))
		e.currentAlive = true
	case errors.Is(err, storepkg.ErrNodeNotFound):
		// deleted — history-only
	default:
		t.Fatalf("Get(node %v): %v", id, err)
	}
	return e
}

func captureRel(t *testing.T, g *Core, id types.RelID, relType string) *oracleEntity {
	t.Helper()
	hist, err := g.Rels.History(id)
	if err != nil {
		t.Fatalf("History(rel %v): %v", id, err)
	}
	e := &oracleEntity{sfFallback: snowflakeFallbackRel(id), relType: relType}
	for _, h := range hist {
		e.rows = append(e.rows, relRow(h))
	}
	cur, err := g.Rels.Get(context.Background(), id)
	switch {
	case err == nil:
		e.rows = append(e.rows, relRow(cur))
		e.currentAlive = true
	case errors.Is(err, storepkg.ErrRelNotFound):
	default:
		t.Fatalf("Get(rel %v): %v", id, err)
	}
	return e
}

// =============================================================================
// Random op sequence — the world.
// =============================================================================

var (
	oracleNodeLabels = []string{"A", "B", "C"}
	oracleRelTypes   = []string{"R", "S"}
)

type world struct {
	t   *testing.T
	g   *Core
	rng *rand.Rand
	ctx context.Context

	anchors []types.NodeID // stable rel endpoints (create-only)

	nodeIDs   []types.NodeID
	nodeAlive map[types.NodeID]bool

	relIDs   []types.RelID
	relType  map[types.RelID]string
	relAlive map[types.RelID]bool

	vclock types.Instant // monotonically increasing logical valid-time cursor
	seqno  int
	log    []string
}

func newWorld(t *testing.T, g *Core, rng *rand.Rand) *world {
	return &world{
		t: t, g: g, rng: rng, ctx: context.Background(),
		nodeAlive: map[types.NodeID]bool{},
		relType:   map[types.RelID]string{},
		relAlive:  map[types.RelID]bool{},
		vclock:    1000,
	}
}

func (w *world) nextVF() types.Instant {
	w.vclock += types.Instant(100 + w.rng.IntN(400))
	return w.vclock
}

func (w *world) record(desc string, err error) {
	if err != nil {
		w.log = append(w.log, desc+" -> "+err.Error())
	} else {
		w.log = append(w.log, desc+" -> ok")
	}
}

func (w *world) pickLabels() []string {
	n := 1 + w.rng.IntN(2)
	seen := map[string]bool{}
	var out []string
	for len(out) < n {
		l := oracleNodeLabels[w.rng.IntN(len(oracleNodeLabels))]
		if !seen[l] {
			seen[l] = true
			out = append(out, l)
		}
	}
	return out
}

func (w *world) maybeValidTo(vf types.Instant) (map[string]any, types.Instant) {
	props := map[string]any{"tkg_valid_from": vf, "k": w.seqno}
	w.seqno++
	if w.rng.IntN(100) < 30 {
		vt := vf + types.Instant(100+w.rng.IntN(2000))
		props["tkg_valid_to"] = vt
		return props, vt
	}
	return props, 0
}

func (w *world) aliveNodes() []types.NodeID {
	var out []types.NodeID
	for _, id := range w.nodeIDs {
		if w.nodeAlive[id] {
			out = append(out, id)
		}
	}
	return out
}

func (w *world) aliveRels() []types.RelID {
	var out []types.RelID
	for _, id := range w.relIDs {
		if w.relAlive[id] {
			out = append(out, id)
		}
	}
	return out
}

func (w *world) setup() {
	// Two stable anchor nodes for relationship endpoints.
	for i := 0; i < 2; i++ {
		props := map[string]any{"tkg_valid_from": w.nextVF(), "anchor": i}
		n, err := w.g.Nodes.Add(w.ctx, []string{oracleNodeLabels[i%len(oracleNodeLabels)]}, props)
		if err != nil {
			w.t.Fatalf("add anchor: %v", err)
		}
		w.anchors = append(w.anchors, n.ID())
	}
	// A couple of mutable regular nodes.
	for i := 0; i < 2; i++ {
		w.addNode()
	}
	// A couple of relationships between anchors.
	for i := 0; i < 2; i++ {
		w.addRel()
	}
}

func (w *world) addNode() {
	props, _ := w.maybeValidTo(w.nextVF())
	n, err := w.g.Nodes.Add(w.ctx, w.pickLabels(), props)
	w.record("addNode", err)
	if err == nil {
		w.nodeIDs = append(w.nodeIDs, n.ID())
		w.nodeAlive[n.ID()] = true
	}
}

func (w *world) backfillNode() {
	props, _ := w.maybeValidTo(w.nextVF())
	txFrom := types.Instant(time.Now().UnixMilli()) - types.Instant(1000+w.rng.IntN(60000))
	n, err := w.g.Nodes.AddWithTx(w.ctx, w.pickLabels(), props, txFrom)
	w.record(fmt.Sprintf("backfillNode(tx=%d)", txFrom), err)
	if err == nil {
		w.nodeIDs = append(w.nodeIDs, n.ID())
		w.nodeAlive[n.ID()] = true
	}
}

func (w *world) addRel() {
	if len(w.anchors) < 2 {
		return
	}
	typ := oracleRelTypes[w.rng.IntN(len(oracleRelTypes))]
	props, _ := w.maybeValidTo(w.nextVF())
	s, e := w.anchors[0], w.anchors[1]
	if w.rng.IntN(2) == 0 {
		s, e = e, s
	}
	r, err := w.g.Rels.AddByID(w.ctx, typ, s, e, props)
	w.record("addRel("+typ+")", err)
	if err == nil {
		w.relIDs = append(w.relIDs, r.ID())
		w.relType[r.ID()] = typ
		w.relAlive[r.ID()] = true
	}
}

func (w *world) updateNode() {
	alive := w.aliveNodes()
	if len(alive) == 0 {
		return
	}
	id := alive[w.rng.IntN(len(alive))]
	updates := map[string]any{"k": w.seqno}
	w.seqno++
	if w.rng.IntN(2) == 0 {
		updates["tkg_valid_from"] = w.nextVF()
	}
	_, err := w.g.Nodes.Update(w.ctx, id, updates)
	w.record(fmt.Sprintf("updateNode(%v)", id), err)
}

func (w *world) updateRel() {
	alive := w.aliveRels()
	if len(alive) == 0 {
		return
	}
	id := alive[w.rng.IntN(len(alive))]
	updates := map[string]any{"k": w.seqno}
	w.seqno++
	if w.rng.IntN(2) == 0 {
		updates["tkg_valid_from"] = w.nextVF()
	}
	_, err := w.g.Rels.Update(w.ctx, id, updates)
	w.record(fmt.Sprintf("updateRel(%v)", id), err)
}

func (w *world) addLabel() {
	alive := w.aliveNodes()
	if len(alive) == 0 {
		return
	}
	id := alive[w.rng.IntN(len(alive))]
	l := oracleNodeLabels[w.rng.IntN(len(oracleNodeLabels))]
	err := w.g.Nodes.AddLabel(w.ctx, id, l)
	w.record(fmt.Sprintf("addLabel(%v,%s)", id, l), err)
}

func (w *world) removeLabel() {
	alive := w.aliveNodes()
	if len(alive) == 0 {
		return
	}
	id := alive[w.rng.IntN(len(alive))]
	l := oracleNodeLabels[w.rng.IntN(len(oracleNodeLabels))]
	err := w.g.Nodes.RemoveLabel(w.ctx, id, l)
	w.record(fmt.Sprintf("removeLabel(%v,%s)", id, l), err)
}

// cascadeVF picks a validFrom that is sometimes EARLIER than existing versions
// (a backdated correction — exercises the resolver's non-monotonic slow path).
func (w *world) cascadeVF() types.Instant {
	return types.Instant(500 + w.rng.IntN(int(w.vclock)))
}

func (w *world) cascadeNode() {
	alive := w.aliveNodes()
	if len(alive) == 0 {
		return
	}
	id := alive[w.rng.IntN(len(alive))]
	vf := w.cascadeVF()
	var vt types.Instant
	if w.rng.IntN(2) == 0 {
		vt = vf + types.Instant(100+w.rng.IntN(2000))
	}
	_, err := w.g.Temporal.SetNodeVersionInterval(w.ctx, id, vf, vt, map[string]any{"k": w.seqno})
	w.seqno++
	w.record(fmt.Sprintf("cascadeNode(%v,[%d,%d))", id, vf, vt), err)
}

func (w *world) cascadeRel() {
	alive := w.aliveRels()
	if len(alive) == 0 {
		return
	}
	id := alive[w.rng.IntN(len(alive))]
	vf := w.cascadeVF()
	var vt types.Instant
	if w.rng.IntN(2) == 0 {
		vt = vf + types.Instant(100+w.rng.IntN(2000))
	}
	_, err := w.g.Temporal.SetRelVersionInterval(w.ctx, id, vf, vt, map[string]any{"k": w.seqno})
	w.seqno++
	w.record(fmt.Sprintf("cascadeRel(%v,[%d,%d))", id, vf, vt), err)
}

func (w *world) deleteNode() {
	alive := w.aliveNodes()
	if len(alive) == 0 {
		return
	}
	id := alive[w.rng.IntN(len(alive))]
	err := w.g.Nodes.Delete(w.ctx, id)
	w.record(fmt.Sprintf("deleteNode(%v)", id), err)
	if err == nil {
		w.nodeAlive[id] = false
	}
}

func (w *world) deleteRel() {
	alive := w.aliveRels()
	if len(alive) == 0 {
		return
	}
	id := alive[w.rng.IntN(len(alive))]
	err := w.g.Rels.Delete(w.ctx, id)
	w.record(fmt.Sprintf("deleteRel(%v)", id), err)
	if err == nil {
		w.relAlive[id] = false
	}
}

func (w *world) step() {
	switch w.rng.IntN(11) {
	case 0:
		w.addNode()
	case 1:
		w.backfillNode()
	case 2:
		w.addRel()
	case 3:
		w.updateNode()
	case 4:
		w.updateRel()
	case 5:
		w.addLabel()
	case 6:
		w.removeLabel()
	case 7:
		w.cascadeNode()
	case 8:
		w.cascadeRel()
	case 9:
		w.deleteNode()
	case 10:
		w.deleteRel()
	}
}

// =============================================================================
// Captured snapshot of the whole graph.
// =============================================================================

type snapshot struct {
	nodes map[types.NodeID]*oracleEntity
	rels  map[types.RelID]*oracleEntity
}

func (w *world) capture() *snapshot {
	s := &snapshot{nodes: map[types.NodeID]*oracleEntity{}, rels: map[types.RelID]*oracleEntity{}}
	for _, id := range w.anchors {
		s.nodes[id] = captureNode(w.t, w.g, id)
	}
	for _, id := range w.nodeIDs {
		s.nodes[id] = captureNode(w.t, w.g, id)
	}
	for _, id := range w.relIDs {
		s.rels[id] = captureRel(w.t, w.g, id, w.relType[id])
	}
	return s
}

// =============================================================================
// Probe grid.
// =============================================================================

type probe struct {
	interval bool
	validAt  types.Instant // point
	s, e     types.Instant // interval
	txAt     types.Instant
}

func (p probe) String() string {
	if p.interval {
		return fmt.Sprintf("interval[%d,%d) txAt=%d", p.s, p.e, p.txAt)
	}
	return fmt.Sprintf("point validAt=%d txAt=%d", p.validAt, p.txAt)
}

// interestingInstants collects every recorded stamp across the snapshot. Used
// ONLY for the txAt probe dimension (addT / txSubset in buildProbes) — that
// dimension is intentionally left untouched by the flake fix below; see
// boundaryInstants for the validAt/interval boundary-value split.
func (s *snapshot) interestingInstants() []types.Instant {
	set := map[types.Instant]struct{}{}
	add := func(v types.Instant) {
		if v > 0 {
			set[v] = struct{}{}
		}
	}
	for _, e := range s.nodes {
		for _, r := range e.rows {
			add(r.validFrom)
			add(r.validTo)
			add(r.txFrom)
			add(r.txTo)
			add(r.deletedAt)
			add(r.updatedAt)
		}
	}
	for _, e := range s.rels {
		for _, r := range e.rows {
			add(r.validFrom)
			add(r.validTo)
			add(r.txFrom)
			add(r.txTo)
			add(r.deletedAt)
			add(r.updatedAt)
		}
	}
	out := make([]types.Instant, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// tombstoneValidTo reports whether row r's ValidTo was DELETE-stamped rather
// than test-asserted: a hard delete stamps DeletedAt/ValidTo/TxTo to the SAME
// instant in place (node_delete.go/relationship_delete.go), so ValidTo ==
// DeletedAt (both non-zero) is the tombstone signature. A ValidTo the test
// supplied via tkg_valid_to (or a cascade's vt) never coincides with
// DeletedAt this way.
func tombstoneValidTo(r oracleRow) bool {
	return r.deletedAt != 0 && r.validTo != 0 && r.validTo == r.deletedAt
}

// boundaryInstants splits every recorded stamp into two PROVENANCE buckets,
// consumed by buildProbes to generate validAt/interval boundary VALUES (the
// txAt dimension keeps using the flat interestingInstants pool above,
// unaffected by this split — the fix below scopes to validAt/interval
// candidate generation only):
//
//   - explicit: tkg_valid_from / tkg_valid_to values the TEST chose (world's
//     synthetic vclock/cascadeVF sequence numbers, never wall-clock derived)
//     plus a SetNodeVersionInterval/SetRelVersionInterval cascade's caller-
//     supplied vf/vt. Exact-boundary probing against these is fully
//     deterministic — there is no clock underneath them.
//   - system: TxFrom / TxTo / DeletedAt / UpdatedAt (all minted by Core.now(),
//     context.go — wall-dominated with only a >=1ms MONOTONIC FLOOR) plus a
//     delete tombstone's ValidTo (stamped identically to DeletedAt — see
//     tombstoneValidTo) and the snowflake-derived effective valid-from
//     fallback. A burst of ops can stamp several DIFFERENT entities within
//     the same or an adjacent wall-clock millisecond, so an EXACT-equality
//     probe against one of these inherits the ms-truncation clock hazards
//     documented in bitemporal_tombstone_test.go's header: the oracle (built
//     from the captured, ground-truth stamps) and the engine's own
//     wall-clock-adjacent resolution can legitimately disagree right AT that
//     instant. Verified flake: seeds 47645253332 / 47645253148, both failing
//     a point/interval probe pinned exactly on a deleted entity's tombstone
//     ValidTo. See buildProbes for how the two buckets become probe values.
func (s *snapshot) boundaryInstants() (explicit, system []types.Instant) {
	explicitSet := map[types.Instant]struct{}{}
	systemSet := map[types.Instant]struct{}{}
	addE := func(v types.Instant) {
		if v > 0 {
			explicitSet[v] = struct{}{}
		}
	}
	addS := func(v types.Instant) {
		if v > 0 {
			systemSet[v] = struct{}{}
		}
	}
	collect := func(e *oracleEntity) {
		addS(e.sfFallback) // snowflake-derived effective valid-from fallback
		for _, r := range e.rows {
			addE(r.validFrom) // always test/cascade-chosen in this harness
			if tombstoneValidTo(r) {
				addS(r.validTo)
			} else {
				addE(r.validTo)
			}
			addS(r.txFrom)
			addS(r.txTo)
			addS(r.deletedAt)
			addS(r.updatedAt)
		}
	}
	for _, e := range s.nodes {
		collect(e)
	}
	for _, e := range s.rels {
		collect(e)
	}
	toSorted := func(set map[types.Instant]struct{}) []types.Instant {
		out := make([]types.Instant, 0, len(set))
		for v := range set {
			out = append(out, v)
		}
		sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
		return out
	}
	return toSorted(explicitSet), toSorted(systemSet)
}

// buildProbes generates a deterministic, bounded probe set clustered around the
// recorded stamps (exact boundaries, boundary±1, a small validAt floor, and a
// far-future pin). Point probes keep validAt >= 1 so the generic door never
// takes the TxAt-only wall-now path.
//
// Flake fix: the validAt/interval boundary VALUES below are drawn from the
// two boundaryInstants provenance buckets, not a single flat pool (the txAt
// dimension — addT/txSubset — is untouched, still built from the flat
// interestingInstants pool):
//
//   - explicit stamps keep exact-boundary AND boundary±1 probes (unchanged
//     behavior) — they are plain test-chosen integers, never wall-clock
//     derived, so exact equality is safe and deterministic.
//   - system-derived stamps NEVER get an exact-equality probe. Instead each
//     contributes stamp-2ms and stamp+2ms (safely past the ambiguous
//     millisecond on either side), and a shifted candidate that happens to
//     land exactly on a DIFFERENT op's system-derived stamp is dropped (that
//     would just reintroduce the same ms-truncation hazard one hop over).
//
// See boundaryInstants for the full rationale and the verified flake seeds.
func (s *snapshot) buildProbes(rng *rand.Rand, n int, farFuture types.Instant) []probe {
	pool := s.interestingInstants() // txAt dimension only — unaffected by this fix
	explicitStamps, systemStamps := s.boundaryInstants()
	systemSet := make(map[types.Instant]struct{}, len(systemStamps))
	for _, v := range systemStamps {
		systemSet[v] = struct{}{}
	}
	// addSystemBoundary appends the two safe-shifted candidates for a
	// system-derived stamp p to dst via add, dropping any candidate that
	// collides with a DIFFERENT op's system-derived stamp (see
	// boundaryInstants doc comment).
	addSystemBoundary := func(p types.Instant, add func(types.Instant)) {
		for _, cand := range [2]types.Instant{p - 2, p + 2} {
			if _, collide := systemSet[cand]; collide {
				continue
			}
			add(cand)
		}
	}

	var validAts []types.Instant
	seenV := map[types.Instant]bool{}
	addV := func(v types.Instant) {
		if v >= 1 && !seenV[v] {
			seenV[v] = true
			validAts = append(validAts, v)
		}
	}
	addV(1)
	addV(farFuture)
	for _, p := range explicitStamps {
		addV(p - 1)
		addV(p)
		addV(p + 1)
	}
	for _, p := range systemStamps {
		addSystemBoundary(p, addV)
	}

	var txAts []types.Instant
	seenT := map[types.Instant]bool{}
	addT := func(v types.Instant) {
		if v >= 0 && !seenT[v] {
			seenT[v] = true
			txAts = append(txAts, v)
		}
	}
	addT(0)
	addT(farFuture)
	for _, p := range pool {
		addT(p)
		addT(p + 1)
	}

	// Point probes.
	var points []probe
	for _, va := range validAts {
		for _, ta := range txAts {
			points = append(points, probe{validAt: va, txAt: ta})
		}
	}
	rng.Shuffle(len(points), func(i, j int) { points[i], points[j] = points[j], points[i] })

	// Interval probes: pairs (s,e) with s<e drawn from the boundary-safe pool
	// (explicit stamps exact, system stamps stamp±2ms — see above), various
	// txAt (txSubset keeps drawing from the flat, unaffected pool).
	var intervalBounds []types.Instant
	seenB := map[types.Instant]bool{}
	addB := func(v types.Instant) {
		if v > 0 && !seenB[v] {
			seenB[v] = true
			intervalBounds = append(intervalBounds, v)
		}
	}
	for _, p := range explicitStamps {
		addB(p)
	}
	for _, p := range systemStamps {
		addSystemBoundary(p, addB)
	}
	sort.Slice(intervalBounds, func(i, j int) bool { return intervalBounds[i] < intervalBounds[j] })

	var intervals []probe
	if len(intervalBounds) >= 2 {
		txSubset := []types.Instant{0, farFuture}
		if len(pool) > 0 {
			txSubset = append(txSubset, pool[rng.IntN(len(pool))])
			txSubset = append(txSubset, pool[rng.IntN(len(pool))]+1)
		}
		for i := 0; i < len(intervalBounds); i++ {
			for j := i + 1; j < len(intervalBounds); j++ {
				for _, ta := range txSubset {
					intervals = append(intervals, probe{interval: true, s: intervalBounds[i], e: intervalBounds[j], txAt: ta})
				}
			}
		}
	}
	rng.Shuffle(len(intervals), func(i, j int) { intervals[i], intervals[j] = intervals[j], intervals[i] })

	half := n / 2
	if half < 1 {
		half = 1
	}
	var out []probe
	for i := 0; i < half && i < len(points); i++ {
		out = append(out, points[i])
	}
	for i := 0; i < (n-half) && i < len(intervals); i++ {
		out = append(out, intervals[i])
	}
	return out
}

// =============================================================================
// Comparison: oracle vs both doors.
// =============================================================================

func nodeSetVer(ns []*types.Node) map[types.NodeID]uint32 {
	m := make(map[types.NodeID]uint32, len(ns))
	for _, n := range ns {
		m[n.ID()] = n.Version()
	}
	return m
}

func relSetVer(rs []*types.Relationship) map[types.RelID]uint32 {
	m := make(map[types.RelID]uint32, len(rs))
	for _, r := range rs {
		m[r.ID()] = r.Version()
	}
	return m
}

// harnessFail dumps everything a maintainer needs to reproduce a divergence.
func (w *world) harnessFail(backend string, seed uint64, p probe, kind string, want, got string) {
	var b strings.Builder
	fmt.Fprintf(&b, "MISMATCH [%s] seed=%d backend=%s\n", kind, seed, backend)
	fmt.Fprintf(&b, "probe: %s\n", p)
	fmt.Fprintf(&b, "oracle: %s\n", want)
	fmt.Fprintf(&b, "door:   %s\n", got)
	fmt.Fprintf(&b, "op log (%d):\n", len(w.log))
	for i, l := range w.log {
		fmt.Fprintf(&b, "  %2d. %s\n", i, l)
	}
	w.t.Fatal(b.String())
}

func fmtNodeVer(m map[types.NodeID]uint32) string {
	ids := make([]types.NodeID, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%v#v%d", id, m[id]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func fmtRelVer(m map[types.RelID]uint32) string {
	ids := make([]types.RelID, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%v#v%d", id, m[id]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func nodeMapsEqual(a, b map[types.NodeID]uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func relMapsEqual(a, b map[types.RelID]uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func nodeKeysEqual(a, b map[types.NodeID]uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

func relKeysEqual(a, b map[types.RelID]uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

// runProbe compares the oracle against every applicable door for one probe.
func (w *world) runProbe(backend string, seed uint64, snap *snapshot, p probe) {
	g := w.g
	if p.interval {
		w.runIntervalProbe(backend, seed, snap, p)
		return
	}
	w.runPointProbe(backend, seed, snap, p)
	_ = g
}

func (w *world) runPointProbe(backend string, seed uint64, snap *snapshot, p probe) {
	g := w.g
	// --- Nodes ---
	oracleAll := map[types.NodeID]uint32{}
	oracleByLabel := map[string]map[types.NodeID]uint32{}
	for _, l := range oracleNodeLabels {
		oracleByLabel[l] = map[types.NodeID]uint32{}
	}
	for id, e := range snap.nodes {
		row, ok := e.pointVisible(p.validAt, p.txAt)
		if !ok {
			continue
		}
		oracleAll[id] = row.version
		for _, l := range oracleNodeLabels {
			if hasLabel(row, l) {
				oracleByLabel[l][id] = row.version
			}
		}
	}

	// NodesAtTx (named)
	got, err := g.Temporal.NodesAtTx(p.validAt, p.txAt)
	if err != nil {
		w.t.Fatalf("NodesAtTx: %v", err)
	}
	if gm := nodeSetVer(got); !nodeMapsEqual(oracleAll, gm) {
		w.harnessFail(backend, seed, p, "NodesAtTx", fmtNodeVer(oracleAll), fmtNodeVer(gm))
	}

	// Nodes.All (generic point)
	gotAll, err := g.Nodes.All(storepkg.QueryOpts{ValidAt: p.validAt, TxAt: p.txAt})
	if err != nil {
		w.t.Fatalf("Nodes.All(point): %v", err)
	}
	if gm := nodeSetVer(gotAll); !nodeMapsEqual(oracleAll, gm) {
		w.harnessFail(backend, seed, p, "Nodes.All(point)", fmtNodeVer(oracleAll), fmtNodeVer(gm))
	}

	// NodesAt == NodesAtTx(_,0)
	if p.txAt == 0 {
		gotNA, err := g.Temporal.NodesAt(p.validAt)
		if err != nil {
			w.t.Fatalf("NodesAt: %v", err)
		}
		if gm := nodeSetVer(gotNA); !nodeMapsEqual(oracleAll, gm) {
			w.harnessFail(backend, seed, p, "NodesAt", fmtNodeVer(oracleAll), fmtNodeVer(gm))
		}
	}

	// Nodes.ByLabel (generic point)
	for _, l := range oracleNodeLabels {
		gotBL, err := g.Nodes.ByLabel(l, storepkg.QueryOpts{ValidAt: p.validAt, TxAt: p.txAt})
		if err != nil {
			w.t.Fatalf("ByLabel(%s,point): %v", l, err)
		}
		if gm := nodeSetVer(gotBL); !nodeMapsEqual(oracleByLabel[l], gm) {
			w.harnessFail(backend, seed, p, "Nodes.ByLabel("+l+")", fmtNodeVer(oracleByLabel[l]), fmtNodeVer(gm))
		}
	}

	// NodeAtTx per node (strong per-entity check).
	for id, e := range snap.nodes {
		row, ok := e.pointVisible(p.validAt, p.txAt)
		n, err := g.Temporal.NodeAtTx(id, p.validAt, p.txAt)
		if !ok {
			if err == nil {
				w.harnessFail(backend, seed, p, fmt.Sprintf("NodeAtTx(%v) want-absent", id),
					"absent", fmt.Sprintf("v%d", n.Version()))
			}
			if !errors.Is(err, storepkg.ErrNoVersionValidAt) && !errors.Is(err, storepkg.ErrNodeNotFound) {
				w.t.Fatalf("NodeAtTx(%v): unexpected error %v", id, err)
			}
			continue
		}
		if err != nil {
			w.harnessFail(backend, seed, p, fmt.Sprintf("NodeAtTx(%v) want-v%d", id, row.version),
				fmt.Sprintf("v%d", row.version), "err="+err.Error())
		}
		if n.Version() != row.version {
			w.harnessFail(backend, seed, p, fmt.Sprintf("NodeAtTx(%v)", id),
				fmt.Sprintf("v%d", row.version), fmt.Sprintf("v%d", n.Version()))
		}
	}

	// --- Rels ---
	oracleRelAll := map[types.RelID]uint32{}
	oracleByType := map[string]map[types.RelID]uint32{}
	for _, ty := range oracleRelTypes {
		oracleByType[ty] = map[types.RelID]uint32{}
	}
	for id, e := range snap.rels {
		row, ok := e.pointVisible(p.validAt, p.txAt)
		if !ok {
			continue
		}
		oracleRelAll[id] = row.version
		oracleByType[e.relType][id] = row.version
	}

	gotR, err := g.Temporal.RelsAtTx(p.validAt, p.txAt)
	if err != nil {
		w.t.Fatalf("RelsAtTx: %v", err)
	}
	if gm := relSetVer(gotR); !relMapsEqual(oracleRelAll, gm) {
		w.harnessFail(backend, seed, p, "RelsAtTx", fmtRelVer(oracleRelAll), fmtRelVer(gm))
	}

	gotRAll, err := g.Rels.All(storepkg.QueryOpts{ValidAt: p.validAt, TxAt: p.txAt})
	if err != nil {
		w.t.Fatalf("Rels.All(point): %v", err)
	}
	if gm := relSetVer(gotRAll); !relMapsEqual(oracleRelAll, gm) {
		w.harnessFail(backend, seed, p, "Rels.All(point)", fmtRelVer(oracleRelAll), fmtRelVer(gm))
	}

	for _, ty := range oracleRelTypes {
		gotBT, err := g.Rels.ByType(ty, storepkg.QueryOpts{ValidAt: p.validAt, TxAt: p.txAt})
		if err != nil {
			w.t.Fatalf("ByType(%s,point): %v", ty, err)
		}
		if gm := relSetVer(gotBT); !relMapsEqual(oracleByType[ty], gm) {
			w.harnessFail(backend, seed, p, "Rels.ByType("+ty+")", fmtRelVer(oracleByType[ty]), fmtRelVer(gm))
		}
	}

	for id, e := range snap.rels {
		row, ok := e.pointVisible(p.validAt, p.txAt)
		r, err := g.Temporal.RelAtTx(id, p.validAt, p.txAt)
		if !ok {
			if err == nil {
				w.harnessFail(backend, seed, p, fmt.Sprintf("RelAtTx(%v) want-absent", id),
					"absent", fmt.Sprintf("v%d", r.Version()))
			}
			if !errors.Is(err, storepkg.ErrNoVersionValidAt) && !errors.Is(err, storepkg.ErrRelNotFound) {
				w.t.Fatalf("RelAtTx(%v): unexpected error %v", id, err)
			}
			continue
		}
		if err != nil {
			w.harnessFail(backend, seed, p, fmt.Sprintf("RelAtTx(%v) want-v%d", id, row.version),
				fmt.Sprintf("v%d", row.version), "err="+err.Error())
		}
		if r.Version() != row.version {
			w.harnessFail(backend, seed, p, fmt.Sprintf("RelAtTx(%v)", id),
				fmt.Sprintf("v%d", row.version), fmt.Sprintf("v%d", r.Version()))
		}
	}
}

func (w *world) runIntervalProbe(backend string, seed uint64, snap *snapshot, p probe) {
	g := w.g
	// --- Nodes ---
	oracleAll := map[types.NodeID]uint32{}
	oracleByLabel := map[string]map[types.NodeID]uint32{}
	for _, l := range oracleNodeLabels {
		oracleByLabel[l] = map[types.NodeID]uint32{}
	}
	for id, e := range snap.nodes {
		if row, ok := e.intervalVisible(p.s, p.e, p.txAt, nil); ok {
			oracleAll[id] = row.version
		}
		for _, l := range oracleNodeLabels {
			label := l
			if row, ok := e.intervalVisible(p.s, p.e, p.txAt, func(r oracleRow) bool { return hasLabel(r, label) }); ok {
				oracleByLabel[l][id] = row.version
			}
		}
	}

	gotAll, err := g.Nodes.All(storepkg.QueryOpts{ValidStart: p.s, ValidEnd: p.e, TxAt: p.txAt})
	if err != nil {
		w.t.Fatalf("Nodes.All(interval): %v", err)
	}
	if gm := nodeSetVer(gotAll); !nodeKeysEqual(oracleAll, gm) {
		w.harnessFail(backend, seed, p, "Nodes.All(interval)", fmtNodeVer(oracleAll), fmtNodeVer(gm))
	}

	// NodesDuringTx (named bitemporal-interval door, sigma ask 2) — must equal the
	// same interval-visible-at-txAt oracle for EVERY txAt (0 included, where it
	// collapses onto NodesDuring). This is the named-door half of the rule-17
	// equivalence: NodesDuringTx == Nodes.All(interval) == oracle, on every backend
	// the harness runs.
	gotDurTx, err := g.Temporal.NodesDuringTx(p.s, p.e, p.txAt)
	if err != nil {
		w.t.Fatalf("NodesDuringTx: %v", err)
	}
	if gm := nodeSetVer(gotDurTx); !nodeKeysEqual(oracleAll, gm) {
		w.harnessFail(backend, seed, p, "NodesDuringTx", fmtNodeVer(oracleAll), fmtNodeVer(gm))
	}

	if p.txAt == 0 {
		gotDur, err := g.Temporal.NodesDuring(p.s, p.e)
		if err != nil {
			w.t.Fatalf("NodesDuring: %v", err)
		}
		if gm := nodeSetVer(gotDur); !nodeKeysEqual(oracleAll, gm) {
			w.harnessFail(backend, seed, p, "NodesDuring", fmtNodeVer(oracleAll), fmtNodeVer(gm))
		}
	}

	for _, l := range oracleNodeLabels {
		gotBL, err := g.Nodes.ByLabel(l, storepkg.QueryOpts{ValidStart: p.s, ValidEnd: p.e, TxAt: p.txAt})
		if err != nil {
			w.t.Fatalf("ByLabel(%s,interval): %v", l, err)
		}
		if gm := nodeSetVer(gotBL); !nodeKeysEqual(oracleByLabel[l], gm) {
			w.harnessFail(backend, seed, p, "Nodes.ByLabel("+l+",interval)", fmtNodeVer(oracleByLabel[l]), fmtNodeVer(gm))
		}
	}

	// --- Rels ---
	oracleRelAll := map[types.RelID]uint32{}
	oracleByType := map[string]map[types.RelID]uint32{}
	for _, ty := range oracleRelTypes {
		oracleByType[ty] = map[types.RelID]uint32{}
	}
	for id, e := range snap.rels {
		if row, ok := e.intervalVisible(p.s, p.e, p.txAt, nil); ok {
			oracleRelAll[id] = row.version
			oracleByType[e.relType][id] = row.version
		}
	}

	gotRAll, err := g.Rels.All(storepkg.QueryOpts{ValidStart: p.s, ValidEnd: p.e, TxAt: p.txAt})
	if err != nil {
		w.t.Fatalf("Rels.All(interval): %v", err)
	}
	if gm := relSetVer(gotRAll); !relKeysEqual(oracleRelAll, gm) {
		w.harnessFail(backend, seed, p, "Rels.All(interval)", fmtRelVer(oracleRelAll), fmtRelVer(gm))
	}

	// RelsDuringTx (named) — mirror of NodesDuringTx, every txAt.
	gotRDurTx, err := g.Temporal.RelsDuringTx(p.s, p.e, p.txAt)
	if err != nil {
		w.t.Fatalf("RelsDuringTx: %v", err)
	}
	if gm := relSetVer(gotRDurTx); !relKeysEqual(oracleRelAll, gm) {
		w.harnessFail(backend, seed, p, "RelsDuringTx", fmtRelVer(oracleRelAll), fmtRelVer(gm))
	}

	if p.txAt == 0 {
		gotDur, err := g.Temporal.RelsDuring(p.s, p.e)
		if err != nil {
			w.t.Fatalf("RelsDuring: %v", err)
		}
		if gm := relSetVer(gotDur); !relKeysEqual(oracleRelAll, gm) {
			w.harnessFail(backend, seed, p, "RelsDuring", fmtRelVer(oracleRelAll), fmtRelVer(gm))
		}
	}

	for _, ty := range oracleRelTypes {
		gotBT, err := g.Rels.ByType(ty, storepkg.QueryOpts{ValidStart: p.s, ValidEnd: p.e, TxAt: p.txAt})
		if err != nil {
			w.t.Fatalf("ByType(%s,interval): %v", ty, err)
		}
		if gm := relSetVer(gotBT); !relKeysEqual(oracleByType[ty], gm) {
			w.harnessFail(backend, seed, p, "Rels.ByType("+ty+",interval)", fmtRelVer(oracleByType[ty]), fmtRelVer(gm))
		}
	}
}

// runAsOfProbe cross-checks the named as-of snapshot door (NodesAsOf/RelsAsOf
// and the per-entity NodeAsOf/RelAsOf) against the as-of oracle for one txAt.
// Now possible on BOTH backends because the WP aligned the memory-native /
// core-fallback resolver with the badger-native reverse-scan (lesson 62).
func (w *world) runAsOfProbe(backend string, seed uint64, snap *snapshot, txAt types.Instant) {
	g := w.g
	p := probe{txAt: txAt} // validAt=0 marks an as-of probe in diagnostics

	// --- Nodes ---
	oracleAll := map[types.NodeID]uint32{}
	oracleByLabel := map[string]map[types.NodeID]uint32{}
	for _, l := range oracleNodeLabels {
		oracleByLabel[l] = map[types.NodeID]uint32{}
	}
	for id, e := range snap.nodes {
		if row, ok := e.asOfVisible(txAt); ok {
			oracleAll[id] = row.version
			for _, l := range oracleNodeLabels {
				if hasLabel(row, l) {
					oracleByLabel[l][id] = row.version
				}
			}
		}
	}
	got, err := g.Temporal.NodesAsOf(txAt)
	if err != nil {
		w.t.Fatalf("NodesAsOf(%d): %v", txAt, err)
	}
	if gm := nodeSetVer(got); !nodeMapsEqual(oracleAll, gm) {
		w.harnessFail(backend, seed, p, "NodesAsOf", fmtNodeVer(oracleAll), fmtNodeVer(gm))
	}

	// TxPin generic door: Nodes.All{TxPin} must equal the as-of oracle (the
	// belief state, NO valid-time filter) — the same reference the named
	// NodesAsOf door is checked against. Skipped at txAt==0: 0 is the QueryOpts
	// "disabled" sentinel for TxPin (a full current-state scan), whereas the
	// named NodesAsOf(0) treats 0 as a literal empty pin — the two doors
	// intentionally diverge at exactly the zero value.
	if txAt != 0 {
		gotPinAll, err := g.Nodes.All(storepkg.QueryOpts{TxPin: txAt})
		if err != nil {
			w.t.Fatalf("Nodes.All{TxPin=%d}: %v", txAt, err)
		}
		if gm := nodeSetVer(gotPinAll); !nodeMapsEqual(oracleAll, gm) {
			w.harnessFail(backend, seed, p, "Nodes.All{TxPin}", fmtNodeVer(oracleAll), fmtNodeVer(gm))
		}
		for _, l := range oracleNodeLabels {
			gotPinBL, err := g.Nodes.ByLabel(l, storepkg.QueryOpts{TxPin: txAt})
			if err != nil {
				w.t.Fatalf("Nodes.ByLabel(%s){TxPin=%d}: %v", l, txAt, err)
			}
			if gm := nodeSetVer(gotPinBL); !nodeMapsEqual(oracleByLabel[l], gm) {
				w.harnessFail(backend, seed, p, "Nodes.ByLabel("+l+"){TxPin}", fmtNodeVer(oracleByLabel[l]), fmtNodeVer(gm))
			}
		}
	}
	for id, e := range snap.nodes {
		row, ok := e.asOfVisible(txAt)
		n, err := g.Temporal.NodeAsOf(id, txAt)
		if !ok {
			if err == nil {
				w.harnessFail(backend, seed, p, fmt.Sprintf("NodeAsOf(%v) want-absent", id),
					"absent", fmt.Sprintf("v%d", n.Version()))
			}
			if !errors.Is(err, ErrNoVersionAsOf) {
				w.t.Fatalf("NodeAsOf(%v): unexpected error %v", id, err)
			}
			continue
		}
		if err != nil {
			w.harnessFail(backend, seed, p, fmt.Sprintf("NodeAsOf(%v) want-v%d", id, row.version),
				fmt.Sprintf("v%d", row.version), "err="+err.Error())
		}
		if n.Version() != row.version {
			w.harnessFail(backend, seed, p, fmt.Sprintf("NodeAsOf(%v)", id),
				fmt.Sprintf("v%d", row.version), fmt.Sprintf("v%d", n.Version()))
		}
	}

	// --- Rels ---
	oracleRelAll := map[types.RelID]uint32{}
	oracleByType := map[string]map[types.RelID]uint32{}
	for _, ty := range oracleRelTypes {
		oracleByType[ty] = map[types.RelID]uint32{}
	}
	for id, e := range snap.rels {
		if row, ok := e.asOfVisible(txAt); ok {
			oracleRelAll[id] = row.version
			oracleByType[e.relType][id] = row.version
		}
	}
	gotR, err := g.Temporal.RelsAsOf(txAt)
	if err != nil {
		w.t.Fatalf("RelsAsOf(%d): %v", txAt, err)
	}
	if gm := relSetVer(gotR); !relMapsEqual(oracleRelAll, gm) {
		w.harnessFail(backend, seed, p, "RelsAsOf", fmtRelVer(oracleRelAll), fmtRelVer(gm))
	}

	// TxPin generic door: Rels.All{TxPin} / ByType{TxPin} vs the as-of oracle
	// (skipped at txAt==0 — see the node block above for why the zero value
	// intentionally diverges).
	if txAt != 0 {
		gotPinRAll, err := g.Rels.All(storepkg.QueryOpts{TxPin: txAt})
		if err != nil {
			w.t.Fatalf("Rels.All{TxPin=%d}: %v", txAt, err)
		}
		if gm := relSetVer(gotPinRAll); !relMapsEqual(oracleRelAll, gm) {
			w.harnessFail(backend, seed, p, "Rels.All{TxPin}", fmtRelVer(oracleRelAll), fmtRelVer(gm))
		}
		for _, ty := range oracleRelTypes {
			gotPinBT, err := g.Rels.ByType(ty, storepkg.QueryOpts{TxPin: txAt})
			if err != nil {
				w.t.Fatalf("Rels.ByType(%s){TxPin=%d}: %v", ty, txAt, err)
			}
			if gm := relSetVer(gotPinBT); !relMapsEqual(oracleByType[ty], gm) {
				w.harnessFail(backend, seed, p, "Rels.ByType("+ty+"){TxPin}", fmtRelVer(oracleByType[ty]), fmtRelVer(gm))
			}
		}
	}
	for id, e := range snap.rels {
		row, ok := e.asOfVisible(txAt)
		r, err := g.Temporal.RelAsOf(id, txAt)
		if !ok {
			if err == nil {
				w.harnessFail(backend, seed, p, fmt.Sprintf("RelAsOf(%v) want-absent", id),
					"absent", fmt.Sprintf("v%d", r.Version()))
			}
			if !errors.Is(err, ErrNoVersionAsOf) {
				w.t.Fatalf("RelAsOf(%v): unexpected error %v", id, err)
			}
			continue
		}
		if err != nil {
			w.harnessFail(backend, seed, p, fmt.Sprintf("RelAsOf(%v) want-v%d", id, row.version),
				fmt.Sprintf("v%d", row.version), "err="+err.Error())
		}
		if r.Version() != row.version {
			w.harnessFail(backend, seed, p, fmt.Sprintf("RelAsOf(%v)", id),
				fmt.Sprintf("v%d", row.version), fmt.Sprintf("v%d", r.Version()))
		}
	}
}

// =============================================================================
// The harness driver.
// =============================================================================

func runOracleSequence(t *testing.T, seed uint64, nOps, nProbes int) {
	backends := []struct {
		name string
		cfg  Config
		// newStore builds a fresh store per sequence (so state never leaks across
		// seeds). nil => the cfg's own backend (memory/badger) is used.
		newStore func(t *testing.T) storepkg.MandatoryStore
	}{
		{"memory", Config{AllowTxBackfill: true}, nil},
		{"badger", Config{BadgerInMemory: true, AllowTxBackfill: true}, nil},
		// ADR-0007 S2 oracle arm: the sharded backend, BaseSlot 0 / SlotCount 2 —
		// core still mints legacy dual-generator IDs (nodes on slot 0, rels on
		// slot 1 with SnowflakeNodeID=0), so two slots cover both raw values and
		// every relationship is cross-shard from its endpoints. Sharded declines
		// TransactionTimeQueryCapability (S3/S5), so the as-of / TxPin doors
		// resolve through core's generic chain resolver rather than a native
		// reverse scan — the arm still cross-checks every probe class the harness
		// runs on memory/badger (point, interval, as-of, TxPin) against the same
		// oracle; nothing the harness needs is skipped.
		{"sharded", Config{AllowTxBackfill: true}, func(t *testing.T) storepkg.MandatoryStore {
			st, err := shardedpkg.New(shardedpkg.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				t.Fatalf("sharded.New: %v", err)
			}
			return st
		}},
	}
	for _, be := range backends {
		cfg := be.cfg
		if be.newStore != nil {
			cfg.Store = be.newStore(t)
		}
		g, err := New(cfg)
		if err != nil {
			t.Fatalf("New(%s): %v", be.name, err)
		}
		// The oracle assumes a migrated store (no legacy inherited-ValidFrom
		// demotion — lesson 33/36). Fresh memory/badger stores migrate at New.
		if !g.bitemporalMigrated {
			g.Close()
			t.Fatalf("backend %s: expected bitemporalMigrated=true; oracle assumptions invalid", be.name)
		}

		rng := rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))
		w := newWorld(t, g, rng)
		w.setup()
		for i := 0; i < nOps; i++ {
			w.step()
		}

		snap := w.capture()

		// Far-future pin: beyond every recorded stamp.
		var maxStampV types.Instant
		for _, v := range snap.interestingInstants() {
			if v > maxStampV {
				maxStampV = v
			}
		}
		farFuture := maxStampV + 1_000_000

		probeRng := rand.New(rand.NewPCG(seed^0xD1B54A32D192ED03, seed))
		probes := snap.buildProbes(probeRng, nProbes, farFuture)
		if len(probes) == 0 {
			t.Fatalf("backend %s: probe-count sanity check failed: buildProbes returned 0 probes", be.name)
		}
		seenTxAt := map[types.Instant]struct{}{}
		asOfProbeCount := 0  // number of distinct txAt pins the as-of door was cross-checked at
		txPinProbeCount := 0 // subset of the above with txAt != 0 — the ones that also ran the TxPin generic-door checks
		for _, p := range probes {
			w.runProbe(be.name, seed, snap, p)
			// As-of depends only on txAt; cross-check each distinct pin once.
			if _, done := seenTxAt[p.txAt]; !done {
				seenTxAt[p.txAt] = struct{}{}
				w.runAsOfProbe(be.name, seed, snap, p.txAt)
				asOfProbeCount++
				if p.txAt != 0 {
					txPinProbeCount++
				}
			}
		}
		// Always include the far-future pin (every entity's newest belief).
		if _, done := seenTxAt[farFuture]; !done {
			w.runAsOfProbe(be.name, seed, snap, farFuture)
			asOfProbeCount++
			if farFuture != 0 {
				txPinProbeCount++
			}
		}
		// Probe-count sanity check: the validAt/interval boundary-value
		// filtering (buildProbes/boundaryInstants) must not starve the as-of /
		// TxPin probe classes — those depend only on the DISTINCT txAt values a
		// probe carries, a dimension this WP deliberately left untouched, but
		// assert it rather than trust it.
		if asOfProbeCount == 0 {
			t.Fatalf("backend %s: probe-count sanity check failed: 0 as-of probes executed (probe filtering starved the as-of probe class)", be.name)
		}
		if txPinProbeCount == 0 {
			t.Fatalf("backend %s: probe-count sanity check failed: 0 TxPin-eligible (txAt!=0) as-of probes executed (probe filtering starved the TxPin probe class)", be.name)
		}

		g.Close()
	}
}

// TestBitemporalOracleHarness is the generative cross-door harness. It is
// deterministic from the printed seed: a failure names the seed, prints the full
// op log with recorded stamps, the probe, and both the oracle and door answers.
func TestBitemporalOracleHarness(t *testing.T) {
	t.Parallel()

	sequences, probesPer, nOps := 30, 20, 12
	if !testing.Short() {
		sequences, probesPer, nOps = 200, 40, 18
	}

	const base uint64 = 0xB17E_0DE_10 // "bit-code-10"
	for i := 0; i < sequences; i++ {
		seed := base + uint64(i)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runOracleSequence(t, seed, nOps, probesPer)
		})
	}
}
