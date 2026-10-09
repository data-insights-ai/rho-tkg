package core

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Fixtures for the re-import tests (backlog 38 and the 2026-09-24 "re-import of
// a deleted ID" entry). They extend the cascade-correctness adapter (ccEnt):
// one scenario drives a node or a relationship on every backend, every read is
// an exact tracked set, and the expected set comes from the brute-force
// oracle of bitemporaloracle_test.go (oracleEntity.pointVisible / asOfVisible
// over the rows read back), never from the engine's resolver.

// reimport imports a tracked entity's ID again (Nodes.Import / Rels.Import)
// and returns the new current row's version.
func (e *ccEnt) reimport(id int64, props map[string]any) (uint32, error) {
	if e.rel {
		s, err := e.g.Nodes.Get(e.ctx, e.start)
		if err != nil {
			return 0, err
		}
		en, err := e.g.Nodes.Get(e.ctx, e.end)
		if err != nil {
			return 0, err
		}
		r, err := e.g.Rels.Import(e.ctx, types.RelID(id), ccType, s, en, props)
		if err != nil {
			return 0, err
		}
		return r.Version(), nil
	}
	n, err := e.g.Nodes.Import(e.ctx, types.NodeID(id), []string{ccLabel}, props)
	if err != nil {
		return 0, err
	}
	return n.Version(), nil
}

func (e *ccEnt) mustReimport(id int64, props map[string]any) uint32 {
	e.t.Helper()
	v, err := e.reimport(id, props)
	if err != nil {
		e.t.Fatalf("%s re-import(%d): %v", e.kind(), id, err)
	}
	return v
}

// historyRows fingerprints every stored history row of id by version: the
// version, x, every temporal stamp and both hashes. History rows are
// immutable, so a fingerprint that changes or disappears is a lost row.
func (e *ccEnt) historyRows(id int64) map[uint32]string {
	e.t.Helper()
	out := map[uint32]string{}
	add := func(v uint32, x any, tm *types.TemporalMetadata, hash, prev string) {
		var t types.TemporalMetadata
		if tm != nil {
			t = *tm
		}
		fp := fmt.Sprintf("x=%v vf=%d vt=%d tx=%d..%d del=%d upd=%d hash=%.12s prev=%.12s",
			x, t.ValidFrom, t.ValidTo, t.TxFrom, t.TxTo, t.DeletedAt, t.UpdatedAt, hash, prev)
		if old, dup := out[v]; dup {
			e.t.Fatalf("%s %s: two history rows with version %d:\n  %s\n  %s", e.kind(), e.names[id], v, old, fp)
		}
		out[v] = fp
	}
	if e.rel {
		hist, err := e.g.Rels.History(types.RelID(id))
		if err != nil {
			e.t.Fatalf("Rels.History: %v", err)
		}
		for _, h := range hist {
			x, _ := h.Properties().Get("x")
			ig := h.Integrity()
			add(h.Version(), x, h.Temporal(), ig.Hash, ig.PrevHash)
		}
		return out
	}
	hist, err := e.g.Nodes.History(types.NodeID(id))
	if err != nil {
		e.t.Fatalf("Nodes.History: %v", err)
	}
	for _, h := range hist {
		x, _ := h.Properties().Get("x")
		ig := h.Integrity()
		add(h.Version(), x, h.Temporal(), ig.Hash, ig.PrevHash)
	}
	return out
}

// keepsRows fails when a history row recorded in before is gone or changed.
func (e *ccEnt) keepsRows(phase string, id int64, before map[uint32]string) map[uint32]string {
	e.t.Helper()
	after := e.historyRows(id)
	var bad []string
	vs := make([]int, 0, len(before))
	for v := range before {
		vs = append(vs, int(v))
	}
	sort.Ints(vs)
	for _, v := range vs {
		if after[uint32(v)] != before[uint32(v)] {
			bad = append(bad, fmt.Sprintf("v%d: %s -> %q", v, before[uint32(v)], after[uint32(v)]))
		}
	}
	if len(bad) > 0 {
		e.t.Fatalf("[%s %s] a history row was lost or overwritten:\n  %s\n chain:%s", e.kind(), phase, strings.Join(bad, "\n  "), e.chainString(id))
	}
	return after
}

// oracleOf captures id's chain for the brute-force oracle.
func (e *ccEnt) oracleOf(id int64) *oracleEntity {
	e.t.Helper()
	if e.rel {
		return captureRel(e.t, e.g, types.RelID(id), ccType)
	}
	return captureNode(e.t, e.g, types.NodeID(id))
}

// wantAt is the brute-force answer of the bitemporal point doors at
// (validAt, pin) over every tracked entity (pin 0: no TX filter).
func (e *ccEnt) wantAt(validAt, pin types.Instant) string {
	e.t.Helper()
	m := map[int64]uint32{}
	for _, id := range e.tracked() {
		if row, ok := e.oracleOf(id).pointVisible(validAt, pin); ok {
			m[id] = row.version
		}
	}
	return e.render(m)
}

// wantAsOf is the brute-force answer of the as-of doors at pin.
func (e *ccEnt) wantAsOf(pin types.Instant) string {
	e.t.Helper()
	m := map[int64]uint32{}
	for _, id := range e.tracked() {
		if row, ok := e.oracleOf(id).asOfVisible(pin); ok {
			m[id] = row.version
		}
	}
	return e.render(m)
}

// agree asserts every door against the brute-force oracle at every pin and
// valid instant: the as-of doors (named point and set, TxPin set and count),
// the bitemporal point doors (named point and set, ValidAt+TxAt set and
// count), the valid-time doors without a pin, the effective timeline against
// the point door pointwise (etChecker), the timeline scan forms against the
// per-entity timeline, HasHistory against History, and the hash chain.
func (e *ccEnt) agree(phase string, pins, valids []types.Instant) {
	e.t.Helper()
	e.agreeReads(phase, pins, valids)
	for _, id := range e.tracked() {
		e.verifies(phase, id)
	}
}

// agreeReads is agree without the hash-chain check.
func (e *ccEnt) agreeReads(phase string, pins, valids []types.Instant) {
	e.t.Helper()
	ids := e.tracked()
	for _, p := range pins {
		e.expect(fmt.Sprintf("%s as-of pin=%d", phase, p), e.asOfDoors(p), e.wantAsOf(p), ids...)
		for _, va := range valids {
			e.expect(fmt.Sprintf("%s valid=%d pin=%d", phase, va, p), e.atTxDoors(va, p), e.wantAt(va, p), ids...)
		}
		e.scansMatchTimelines(phase, p)
	}
	for _, va := range valids {
		e.expect(fmt.Sprintf("%s valid=%d", phase, va), e.validDoors(va), e.wantAt(va, 0), ids...)
	}
	maxPin := e.pin()
	k := &etChecker{t: e.t, g: e.g, label: e.kind() + " " + phase}
	for _, id := range ids {
		if e.rel {
			k.rel(types.RelID(id), pins, maxPin)
		} else {
			k.node(types.NodeID(id), pins, maxPin)
		}
		e.hasHistoryMatches(phase, id)
	}
}

// scansMatchTimelines asserts the timeline scan form (ForEachNodeEffectiveByLabel
// / ForEachRelEffectiveByType) yields exactly the per-entity timeline of every
// tracked entity at pin.
func (e *ccEnt) scansMatchTimelines(phase string, pin types.Instant) {
	e.t.Helper()
	scan := map[int64][]etSeg{}
	var err error
	if e.rel {
		err = e.g.Temporal.ForEachRelEffectiveByType(ccType, pin, func(s temporal.RelSegment) bool {
			id := int64(s.Rel.ID())
			scan[id] = append(scan[id], etRelSegs([]temporal.RelSegment{s})...)
			return true
		})
	} else {
		err = e.g.Temporal.ForEachNodeEffectiveByLabel(ccLabel, pin, func(s temporal.NodeSegment) bool {
			id := int64(s.Node.ID())
			scan[id] = append(scan[id], etNodeSegs([]temporal.NodeSegment{s})...)
			return true
		})
	}
	if err != nil {
		e.t.Fatalf("[%s %s pin=%d] effective scan: %v", e.kind(), phase, pin, err)
	}
	for _, id := range e.tracked() {
		var one []etSeg
		if e.rel {
			ss, err := e.g.Temporal.RelEffectiveTimeline(types.RelID(id), pin)
			if err != nil {
				e.t.Fatalf("RelEffectiveTimeline: %v", err)
			}
			one = etRelSegs(ss)
		} else {
			ss, err := e.g.Temporal.NodeEffectiveTimeline(types.NodeID(id), pin)
			if err != nil {
				e.t.Fatalf("NodeEffectiveTimeline: %v", err)
			}
			one = etNodeSegs(ss)
		}
		if got, want := etSegsString(scan[id]), etSegsString(one); got != want {
			e.t.Fatalf("[%s %s pin=%d] %s: scan form%s\n timeline%s", e.kind(), phase, pin, e.names[id], got, want)
		}
	}
}

func (e *ccEnt) hasHistoryMatches(phase string, id int64) {
	e.t.Helper()
	var has bool
	var err error
	if e.rel {
		has, err = e.g.Rels.HasHistory(types.RelID(id))
	} else {
		has, err = e.g.Nodes.HasHistory(types.NodeID(id))
	}
	if err != nil {
		e.t.Fatalf("HasHistory: %v", err)
	}
	if want := len(e.historyRows(id)) > 0; has != want {
		e.t.Fatalf("[%s %s] %s: HasHistory = %v, History holds rows = %v", e.kind(), phase, e.names[id], has, want)
	}
}

// verifies asserts the entity's hash chain verifies across every life.
func (e *ccEnt) verifies(phase string, id int64) {
	e.t.Helper()
	var ok bool
	var err error
	if e.rel {
		ok, err = e.g.Hash.VerifyRelChain(types.RelID(id))
	} else {
		ok, err = e.g.Hash.VerifyNodeChain(types.NodeID(id))
	}
	if err != nil || !ok {
		e.t.Fatalf("[%s %s] %s: Verify*Chain = %v, %v; chain:%s", e.kind(), phase, e.names[id], ok, err, e.chainString(id))
	}
}

// tombstoneAt returns the DeletedAt of the newest tombstone on id's chain.
func (e *ccEnt) tombstoneAt(id int64) types.Instant {
	e.t.Helper()
	var d types.Instant
	for _, r := range e.chain(id) {
		d = max(d, r.tm.DeletedAt)
	}
	if d == 0 {
		e.t.Fatalf("%s: no tombstone on the chain:%s", e.names[id], e.chainString(id))
	}
	return d
}

// maxStamp is the largest TxFrom / TxTo / DeletedAt on id's chain.
func (e *ccEnt) maxStamp(id int64) types.Instant {
	var m types.Instant
	for _, r := range e.chain(id) {
		m = max(m, r.tm.TxFrom, r.tm.TxTo, r.tm.DeletedAt)
	}
	return m
}

// isAbsent reports whether err is the kind's not-found sentinel.
func (e *ccEnt) isAbsent(err error) bool {
	return errors.Is(err, e.notFound())
}

// rlOldImport writes the row a pre-fix import (v4.43–v4.47) wrote for a
// deleted ID: version 0, an empty PrevHash, TxFrom = txFrom (0: the clock; a
// pre-fix backfilled import could carry any instant), straight to the store.
// Chains holding such a row exist in stored data; the read side must keep
// reading them.
func rlOldImport(e *ccEnt, id int64, props map[string]any, vf, txFrom types.Instant) {
	e.t.Helper()
	if e.rel {
		oldImportRel(e.t, e.g, types.RelID(id), ccType, e.start, e.end, props, vf, txFrom)
		return
	}
	oldImportNode(e.t, e.g, types.NodeID(id), ccLabel, props, vf, txFrom)
}

func oldImportNode(t testing.TB, g *Core, id types.NodeID, label string, props map[string]any, vf, txFrom types.Instant) {
	t.Helper()
	ps, err := types.NewOwnedPropertySlice(props)
	if err != nil {
		t.Fatalf("props: %v", err)
	}
	if txFrom == 0 {
		txFrom = g.now()
	}
	tok, ok := g.labels.Lookup(label)
	if !ok {
		t.Fatalf("label token %q", label)
	}
	n := types.NewNode(id, tok, nil)
	if err := n.SetOwnedProperties(ps); err != nil {
		t.Fatalf("props: %v", err)
	}
	h, err := integrity.ComputeNodeHashChecked(n, []string{label})
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	n.SetIntegrity(&types.NodeIntegrity{Hash: h})
	n.SetTemporal(&types.TemporalMetadata{TxFrom: txFrom, ValidFrom: vf})
	if err := g.store.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
}

func oldImportRel(t testing.TB, g *Core, id types.RelID, typ string, start, end types.NodeID, props map[string]any, vf, txFrom types.Instant) {
	t.Helper()
	ps, err := types.NewOwnedPropertySlice(props)
	if err != nil {
		t.Fatalf("props: %v", err)
	}
	if txFrom == 0 {
		txFrom = g.now()
	}
	tok, ok := g.relTypes.Lookup(typ)
	if !ok {
		t.Fatalf("type token %q", typ)
	}
	r := types.NewRelationship(id, tok, start, end)
	if err := r.SetOwnedProperties(ps); err != nil {
		t.Fatalf("props: %v", err)
	}
	h, err := integrity.ComputeRelHashChecked(r, typ)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	r.SetIntegrity(&types.RelIntegrity{Hash: h})
	r.SetTemporal(&types.TemporalMetadata{TxFrom: txFrom, ValidFrom: vf})
	if err := g.store.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
}
